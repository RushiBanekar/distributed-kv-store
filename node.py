"""One node of the distributed key-value store.

Any node can act as COORDINATOR for a client request:
  * PUT/DELETE /kv/<key>  -> write to the N replicas on the ring, succeed after W acks
  * GET        /kv/<key>  -> read from the N replicas, succeed after R replies, return newest
Replica-to-replica traffic uses /internal/<key>.

Run:  python node.py node1
"""
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutTimeout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlparse

import config
from common import http_json
from hashring import HashRing
from storage import Storage


class Node:
    def __init__(self, node_id):
        self.id = node_id
        self.host, self.port = config.NODES[node_id]
        self.ring = HashRing(config.NODES.keys(), config.VNODES)
        os.makedirs(config.DATA_DIR, exist_ok=True)
        self.store = Storage(os.path.join(config.DATA_DIR, f"{node_id}.wal"), fsync=config.FSYNC)
        self.alive = {n: True for n in config.NODES}     # failure detector's view
        self.pool = ThreadPoolExecutor(max_workers=64)
        self.stop_event = threading.Event()
        self.stats = {"writes": 0, "reads": 0, "read_repairs": 0, "quorum_failures": 0}

    # ---------------- logging ----------------
    def log(self, msg):
        print(f"[{time.strftime('%H:%M:%S')}] {self.id}: {msg}", flush=True)

    # ---------------- failure detector ----------------
    def heartbeat_loop(self):
        """Ping every peer periodically; remember who is alive."""
        while not self.stop_event.is_set():
            for nid, (host, port) in config.NODES.items():
                if nid == self.id:
                    continue
                try:
                    status, _ = http_json("GET", host, port, "/health", timeout=0.5)
                    up = status == 200
                except OSError:
                    up = False
                if up != self.alive[nid]:
                    self.log(f"peer {nid} is now {'UP' if up else 'DOWN'}")
                self.alive[nid] = up
            self.stop_event.wait(config.HEARTBEAT_INTERVAL)

    # ---------------- replica-level operations ----------------
    def replica_write(self, nid, key, rec):
        """Write one record to replica `nid`. Returns True if acknowledged."""
        if nid == self.id:
            self.store.apply(key, rec["value"], rec["ts"], rec["deleted"])
            return True
        if not self.alive.get(nid, False):
            return False                                  # known dead: fail fast
        host, port = config.NODES[nid]
        try:
            status, _ = http_json("PUT", host, port, "/internal/" + quote(key, safe=""),
                                  rec, config.REQUEST_TIMEOUT)
            return status == 200
        except OSError:
            self.alive[nid] = False
            return False

    def replica_read(self, nid, key):
        """Returns (ok, record_or_None). ok=False means the replica was unreachable."""
        if nid == self.id:
            return True, self.store.get(key)
        if not self.alive.get(nid, False):
            return False, None
        host, port = config.NODES[nid]
        try:
            status, body = http_json("GET", host, port, "/internal/" + quote(key, safe=""),
                                     None, config.REQUEST_TIMEOUT)
            if status == 200:
                return True, body
            if status == 404:
                return True, None
            return False, None
        except OSError:
            self.alive[nid] = False
            return False, None

    # ---------------- coordinator logic ----------------
    def coordinate_write(self, key, value, deleted=False):
        self.stats["writes"] += 1
        rec = {"value": value, "ts": time.time_ns(), "deleted": deleted}
        replicas = self.ring.get_nodes(key, config.N)
        futures = [self.pool.submit(self.replica_write, n, key, rec) for n in replicas]
        acks = 0
        try:
            for f in as_completed(futures, timeout=config.REQUEST_TIMEOUT * 2):
                if f.result():
                    acks += 1
                if acks >= config.W:
                    break                                  # quorum reached; others finish in background
        except FutTimeout:
            pass
        ok = acks >= config.W
        if not ok:
            self.stats["quorum_failures"] += 1
        return ok, acks, replicas

    def coordinate_read(self, key):
        self.stats["reads"] += 1
        replicas = self.ring.get_nodes(key, config.N)
        fut2nid = {self.pool.submit(self.replica_read, n, key): n for n in replicas}
        responses, seen = {}, set()
        try:
            for f in as_completed(fut2nid, timeout=config.REQUEST_TIMEOUT * 2):
                seen.add(f)
                ok, rec = f.result()
                if ok:
                    responses[fut2nid[f]] = rec
                if len(responses) >= config.R:
                    break
        except FutTimeout:
            pass
        if len(responses) < config.R:
            self.stats["quorum_failures"] += 1
            return None, len(responses), replicas
        # Background read-repair: once all replicas answered, push the newest value to stale ones.
        remaining = {f: n for f, n in fut2nid.items() if f not in seen}
        threading.Thread(target=self._read_repair, args=(key, dict(responses), remaining),
                         daemon=True).start()
        newest = self._newest(responses.values())
        return newest, len(responses), replicas

    @staticmethod
    def _newest(records):
        recs = [r for r in records if r]
        return max(recs, key=lambda r: r["ts"]) if recs else None

    def _read_repair(self, key, responses, remaining):
        try:
            for f in as_completed(remaining, timeout=config.REQUEST_TIMEOUT * 2):
                ok, rec = f.result()
                if ok:
                    responses[remaining[f]] = rec
        except FutTimeout:
            pass
        newest = self._newest(responses.values())
        if not newest:
            return
        for nid, rec in responses.items():
            if rec is None or rec["ts"] < newest["ts"]:
                if self.replica_write(nid, key, newest):
                    self.stats["read_repairs"] += 1
                    self.log(f"read-repair: updated {nid} for key '{key}'")

    # ---------------- client-facing handlers ----------------
    def handle_put(self, key, value, deleted=False):
        ok, acks, replicas = self.coordinate_write(key, value, deleted)
        if ok:
            return 200, {"ok": True, "acks": acks, "replicas": replicas, "coordinator": self.id}
        return 503, {"ok": False, "error": "write quorum not reached", "acks": acks,
                     "needed": config.W, "replicas": replicas}

    def handle_get(self, key):
        rec, got, replicas = self.coordinate_read(key)
        if got < config.R:
            return 503, {"ok": False, "error": "read quorum not reached", "replies": got,
                         "needed": config.R, "replicas": replicas}
        if rec is None or rec["deleted"]:
            return 404, {"ok": False, "error": "not found"}
        return 200, {"ok": True, "key": key, "value": rec["value"], "ts": rec["ts"]}

    def status(self):
        view = dict(self.alive)
        view[self.id] = True
        return {"id": self.id, "port": self.port, "keys": self.store.count(),
                "alive_view": view, "N": config.N, "R": config.R, "W": config.W,
                "stats": dict(self.stats)}


class FastServer(ThreadingHTTPServer):
    request_queue_size = 256
    daemon_threads = True


def make_handler(node):
    class Handler(BaseHTTPRequestHandler):
        server_version = "DKV/1.0"

        def log_message(self, *args):      # silence default per-request logging
            pass

        def _send(self, code, obj):
            data = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n == 0:
                return {}
            try:
                return json.loads(self.rfile.read(n))
            except ValueError:
                return None

        def _parse(self):
            u = urlparse(self.path)
            parts = u.path.split("/", 2)
            kind = parts[1] if len(parts) > 1 else ""
            key = unquote(parts[2]) if len(parts) > 2 else None
            return kind, key, parse_qs(u.query)

        def do_GET(self):
            kind, key, q = self._parse()
            if kind == "health":
                return self._send(200, {"id": node.id, "status": "ok"})
            if kind == "status":
                return self._send(200, node.status())
            if kind == "ring":
                k = q.get("key", [""])[0]
                return self._send(200, {"key": k, "replicas": node.ring.get_nodes(k, config.N)})
            if kind == "kv" and key:
                return self._send(*node.handle_get(key))
            if kind == "internal" and key:
                rec = node.store.get(key)
                return self._send(200, rec) if rec else self._send(404, {"error": "not found"})
            self._send(404, {"error": "bad path"})

        def do_PUT(self):
            kind, key, _ = self._parse()
            body = self._body()
            if body is None or not key:
                return self._send(400, {"error": "bad request"})
            if kind == "kv":
                if "value" not in body:
                    return self._send(400, {"error": "missing 'value'"})
                return self._send(*node.handle_put(key, str(body["value"])))
            if kind == "internal":
                if config.REPLICA_DELAY_MS:
                    time.sleep(config.REPLICA_DELAY_MS / 1000.0)
                node.store.apply(key, body["value"], body["ts"], body.get("deleted", False))
                return self._send(200, {"ok": True})
            self._send(404, {"error": "bad path"})

        def do_DELETE(self):
            kind, key, _ = self._parse()
            if kind == "kv" and key:
                return self._send(*node.handle_put(key, None, deleted=True))
            self._send(404, {"error": "bad path"})

    return Handler


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in config.NODES:
        print(f"usage: python node.py <{'|'.join(config.NODES)}>")
        sys.exit(1)
    node = Node(sys.argv[1])
    server = FastServer((node.host, node.port), make_handler(node))
    threading.Thread(target=node.heartbeat_loop, daemon=True).start()
    node.log(f"listening on {node.host}:{node.port}  (N={config.N} R={config.R} W={config.W}, "
             f"{node.store.count()} keys recovered from WAL)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop_event.set()
        server.server_close()
        node.store.close()


if __name__ == "__main__":
    main()
