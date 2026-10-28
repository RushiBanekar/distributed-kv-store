"""Client library + CLI.

CLI:
  python client.py put  mykey myvalue
  python client.py get  mykey
  python client.py delete mykey
  python client.py status          # show every node's state
  python client.py ring mykey      # which nodes own this key?
"""
import argparse
import itertools
import json
import time
from urllib.parse import quote

import config
from common import http_json


class QuorumError(Exception):
    """Raised when the cluster could not reach the read/write quorum."""


class DKVClient:
    def __init__(self, endpoints=None, timeout=3.0):
        self.endpoints = list(endpoints or config.NODES.values())
        self.timeout = timeout
        self._counter = itertools.count()
        self._down_until = {}          # endpoint -> time before which we skip it

    def _request(self, method, path, body=None):
        """Round-robin over nodes; skip recently-failed nodes; fail over on errors."""
        start = next(self._counter)
        n = len(self.endpoints)
        order = [self.endpoints[(start + k) % n] for k in range(n)]
        now = time.time()
        healthy = [e for e in order if self._down_until.get(e, 0) <= now]
        candidates = healthy + [e for e in order if e not in healthy]   # last resort
        for host, port in candidates:
            try:
                result = http_json(method, host, port, path, body, self.timeout)
                self._down_until.pop((host, port), None)
                return result
            except OSError:
                self._down_until[(host, port)] = time.time() + 5.0
        raise ConnectionError("no node reachable")

    def put(self, key, value):
        status, body = self._request("PUT", "/kv/" + quote(key, safe=""), {"value": value})
        if status == 503:
            raise QuorumError(body)
        return status == 200

    def get(self, key):
        """Return the value, or None if the key does not exist."""
        status, body = self._request("GET", "/kv/" + quote(key, safe=""))
        if status == 200:
            return body["value"]
        if status == 503:
            raise QuorumError(body)
        return None

    def delete(self, key):
        status, body = self._request("DELETE", "/kv/" + quote(key, safe=""))
        if status == 503:
            raise QuorumError(body)
        return status == 200

    def replicas(self, key):
        _, body = self._request("GET", "/ring?key=" + quote(key, safe=""))
        return body["replicas"]


def main():
    ap = argparse.ArgumentParser(description="Distributed KV store client")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("put"); p.add_argument("key"); p.add_argument("value")
    p = sub.add_parser("get"); p.add_argument("key")
    p = sub.add_parser("delete"); p.add_argument("key")
    p = sub.add_parser("ring"); p.add_argument("key")
    sub.add_parser("status")
    args = ap.parse_args()
    c = DKVClient()
    try:
        if args.cmd == "put":
            print("OK" if c.put(args.key, args.value) else "FAILED")
        elif args.cmd == "get":
            v = c.get(args.key)
            print(v if v is not None else "(not found)")
        elif args.cmd == "delete":
            print("OK" if c.delete(args.key) else "FAILED")
        elif args.cmd == "ring":
            print("replicas:", c.replicas(args.key))
        elif args.cmd == "status":
            for nid, (host, port) in config.NODES.items():
                try:
                    _, s = http_json("GET", host, port, "/status", timeout=1.0)
                    print(f"{nid} :{port}  UP    keys={s['keys']:<5} stats={json.dumps(s['stats'])}")
                except OSError:
                    print(f"{nid} :{port}  DOWN")
    except QuorumError as e:
        print("QUORUM NOT REACHED:", e)
    except ConnectionError as e:
        print("ERROR:", e)


if __name__ == "__main__":
    main()
