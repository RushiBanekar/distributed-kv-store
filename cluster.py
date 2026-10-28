"""Start/stop a local cluster of node processes.

CLI:  python cluster.py start [--nodes 5] [--N 3] [--R 2] [--W 2] [--clean]
Also used as a library by demo_failure.py, benchmark.py and the tests.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time

from common import http_json

ROOT = os.path.dirname(os.path.abspath(__file__))
NODE_PY = os.path.join(ROOT, "node.py")


class Cluster:
    def __init__(self, num_nodes=5, N=3, R=2, W=2, base_port=8001,
                 replica_delay_ms=0, data_dir=None):
        self.node_ids = [f"node{i + 1}" for i in range(num_nodes)]
        self.endpoints = [("127.0.0.1", base_port + i) for i in range(num_nodes)]
        self.port = {nid: base_port + i for i, nid in enumerate(self.node_ids)}
        self._tmp = data_dir is None
        self.data_dir = data_dir or tempfile.mkdtemp(prefix="dkv_")
        self.log_dir = os.path.join(self.data_dir, "logs")
        os.makedirs(self.log_dir, exist_ok=True)
        self.env = {"DKV_NUM_NODES": str(num_nodes), "DKV_BASE_PORT": str(base_port),
                    "DKV_N": str(N), "DKV_R": str(R), "DKV_W": str(W),
                    "DKV_DATA_DIR": self.data_dir,
                    "DKV_REPLICA_DELAY_MS": str(replica_delay_ms)}
        self.procs = {}

    def start_node(self, nid, wait=True):
        log = open(os.path.join(self.log_dir, f"{nid}.log"), "a")
        env = os.environ.copy()
        env.update(self.env)
        self.procs[nid] = subprocess.Popen([sys.executable, NODE_PY, nid], cwd=ROOT, env=env,
                                           stdout=log, stderr=subprocess.STDOUT)
        if wait:
            self.wait_ready(nid)

    def wait_ready(self, nid, timeout=10.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.procs[nid].poll() is not None:
                raise RuntimeError(f"{nid} exited early; see {self.log_dir}/{nid}.log")
            try:
                status, _ = http_json("GET", "127.0.0.1", self.port[nid], "/health", timeout=0.3)
                if status == 200:
                    return
            except OSError:
                pass
            time.sleep(0.1)
        raise RuntimeError(f"{nid} did not become ready")

    def start_all(self):
        for nid in self.node_ids:
            self.start_node(nid, wait=False)
        for nid in self.node_ids:
            self.wait_ready(nid)

    def stop_node(self, nid):
        """SIGKILL: simulates a crash (no graceful shutdown)."""
        p = self.procs.pop(nid, None)
        if p:
            p.kill()
            p.wait()

    def stop_all(self):
        for nid in list(self.procs):
            self.stop_node(nid)
        if self._tmp:
            shutil.rmtree(self.data_dir, ignore_errors=True)

    def __enter__(self):
        self.start_all()
        return self

    def __exit__(self, *exc):
        self.stop_all()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["start"])
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--N", type=int, default=3)
    ap.add_argument("--R", type=int, default=2)
    ap.add_argument("--W", type=int, default=2)
    ap.add_argument("--base-port", type=int, default=8001)
    ap.add_argument("--clean", action="store_true", help="delete old data/WAL files first")
    a = ap.parse_args()
    data_dir = os.path.join(ROOT, "data")
    if a.clean:
        shutil.rmtree(data_dir, ignore_errors=True)
    cl = Cluster(a.nodes, a.N, a.R, a.W, a.base_port, data_dir=data_dir)
    cl.start_all()
    print(f"Cluster up: {a.nodes} nodes, N={a.N} R={a.R} W={a.W}")
    for nid, (h, p) in zip(cl.node_ids, cl.endpoints):
        print(f"  {nid}: http://{h}:{p}")
    print(f"Logs: {cl.log_dir}\nPress Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nstopping...")
    finally:
        cl.stop_all()


if __name__ == "__main__":
    main()
