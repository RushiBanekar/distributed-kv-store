"""Scripted demo: crash nodes and show the cluster keeps working (and heals).
Run:  python demo_failure.py
"""
import time

from client import DKVClient, QuorumError
from cluster import Cluster
from common import http_json


def banner(msg):
    print(f"\n=== {msg} ===")


def count_reads(c, keys):
    ok = 0
    for k in keys:
        try:
            if c.get(k) == f"value-{k}":
                ok += 1
        except QuorumError:
            pass
    return ok


def count_writes(c, keys):
    ok = 0
    for k in keys:
        try:
            ok += bool(c.put(k, f"value-{k}"))
        except QuorumError:
            pass
    return ok


def main():
    with Cluster(num_nodes=5, N=3, R=2, W=2, base_port=8101) as cl:
        c = DKVClient(cl.endpoints)

        banner("1. Healthy cluster: write 50 keys, read them back")
        keys = [f"user:{i}" for i in range(50)]
        print(f"writes ok: {count_writes(c, keys)}/50")
        print(f"reads  ok: {count_reads(c, keys)}/50")

        probe = "user:7"
        replicas = c.replicas(probe)
        print(f"key '{probe}' is stored on {replicas}")

        banner(f"2. Crash {replicas[0]} (kill -9). N=3, W=2, R=2 -> still available")
        cl.stop_node(replicas[0])
        time.sleep(2.5)                                   # let heartbeats notice
        print(f"reads  ok: {count_reads(c, keys)}/50")
        new_keys = [f"new:{i}" for i in range(30)]
        print(f"writes ok (new keys while node is down): {count_writes(c, new_keys)}/30")

        banner(f"3. Crash {replicas[1]} too -> only 1 of 3 replicas of '{probe}' alive")
        cl.stop_node(replicas[1])
        time.sleep(2.5)
        try:
            c.put(probe, "will-fail")
            print("write unexpectedly succeeded")
        except QuorumError as e:
            print("write rejected (quorum not reachable):", e)
        try:
            c.get(probe)
            print("read unexpectedly succeeded")
        except QuorumError as e:
            print("read rejected (quorum not reachable):", e)
        print("-> consistency is preferred over availability when the quorum is lost")

        banner("4. Restart both nodes: WAL replay + read repair")
        cl.start_node(replicas[0])
        cl.start_node(replicas[1])
        time.sleep(2.5)
        others = [k for k in keys if k != probe]
        print(f"reads ok after recovery (excluding '{probe}'): {count_reads(c, others)}/49")
        c.get(probe)
        time.sleep(1.0)                                   # let read repair converge
        print(f"'{probe}' now returns {c.get(probe)!r}: the 'failed' write survived on 1 replica")
        print(f"new keys readable (triggers read-repair): {count_reads(c, new_keys)}/30")
        time.sleep(1.0)
        print(f"\nPer-replica copy of '{new_keys[0]}' (written while nodes were down):")
        for nid in c.replicas(new_keys[0]):
            _, rec = http_json("GET", "127.0.0.1", cl.port[nid], "/internal/" + new_keys[0])
            print(f"  {nid}: {rec if rec else 'MISSING'}")
        print("\nDemo finished.")


if __name__ == "__main__":
    main()
