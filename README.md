# Mini Dynamo: A Distributed Key-Value Store (OS + Distributed Systems course project)

Pure Python 3.9+, standard library only (matplotlib optional, for plots).

## Architecture
```
 client.py (CLI/library, round-robin + failover)
      |  HTTP/JSON
      v
 +---------+   +---------+   +---------+   ...   (any node can be the COORDINATOR)
 | node1   |<->| node2   |<->| node3   |
 | HashRing|   | HashRing|   | HashRing|   consistent hashing (100 vnodes/node)
 | Storage |   | Storage |   | Storage |   in-memory dict + write-ahead log (WAL)
 | Heartbt |   | Heartbt |   | Heartbt |   failure detector (1s pings)
 +---------+   +---------+   +---------+
```
- **Partitioning**: consistent hashing with virtual nodes (`hashring.py`).
- **Replication**: every key lives on N=3 nodes (the next 3 distinct nodes clockwise).
- **Quorums**: write succeeds after W acks, read after R replies. R + W > N gives strong (read-your-writes) consistency.
- **Conflict resolution**: last-write-wins using nanosecond timestamps; deletes are tombstones.
- **Durability**: every write is appended to a WAL *before* updating memory; on restart the WAL is replayed (`storage.py`). A torn last line after a crash is ignored.
- **Failure detection**: each node pings its peers every second and fast-fails requests to dead peers.
- **Recovery**: read repair. After a read, the coordinator pushes the newest value to stale or empty replicas.

## Files
| File | Purpose |
|---|---|
| `config.py` | N, R, W, ports, timeouts (env-overridable) |
| `common.py` | HTTP+JSON helper |
| `hashring.py` | Consistent hash ring |
| `storage.py` | Memory store + WAL + LWW + tombstones |
| `node.py` | Node server: coordinator logic, replication, heartbeat, read repair |
| `client.py` | Client library and CLI |
| `cluster.py` | Start/stop local clusters (CLI and library) |
| `demo_failure.py` | Scripted crash/recovery demo |
| `benchmark.py` | Throughput/latency/consistency experiments, CSV + PNG output |
| `tests/` | Unit and integration tests |

## How to run
```bash
# Terminal 1: start 5 nodes (N=3, R=2, W=2)
python cluster.py start --clean

# Terminal 2: use it
python client.py put name alice
python client.py get name
python client.py ring name          # which nodes own the key
python client.py status
python client.py delete name
```
Manual failure test: find the replicas with `ring`, kill one node process, and `get`/`put` still work.
Stop that node's process (Ctrl+C or kill) and start it again with `python node.py node2`.
The cluster's own log files are in `data/logs/`.

```bash
python demo_failure.py                          # best live demo for the viva
python -m unittest discover -s tests -v         # 11 tests
python benchmark.py --experiment quorum         # throughput/latency vs (R,W)
python benchmark.py --experiment scale          # vs cluster size
python benchmark.py --experiment consistency    # stale reads vs (R,W)
```
Results land in `results/` (CSV and PNG graphs for the report).

## Design decisions / likely viva questions
- **Why consistent hashing?** Adding/removing a node moves only ~1/N of the keys instead of rehashing everything. Virtual nodes even out the load.
- **What if W + R <= N?** A read may miss the latest write (stale reads). `--experiment consistency` demonstrates this.
- **What if two clients write the same key at once?** The higher timestamp wins (LWW). Downside: it relies on clocks, and one write is silently lost. Vector clocks would detect the conflict instead.
- **What happens when a replica is down?** With N=3, W=2, one dead replica is tolerated. With two dead replicas of the same key, writes and reads for that key are rejected (CP-leaning behavior).
- **How do crashed nodes recover?** WAL replay restores their own data. Read repair brings them up to date on writes they missed.
- **Where does the OS come in?** Processes (one per node), sockets and threads (`ThreadingHTTPServer`, thread pool), locks (storage mutex), disk I/O and `fsync` (WAL), `SIGKILL` crash simulation.

## Limitations / future work
Static membership; no hinted handoff or anti-entropy (Merkle trees); no WAL compaction; LWW instead of vector clocks; HTTP instead of a binary protocol; all nodes on localhost.
