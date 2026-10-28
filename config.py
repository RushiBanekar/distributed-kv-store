"""Central configuration. Every value can be overridden with an environment variable
(cluster.py uses this to launch nodes with different settings)."""
import os

ROOT = os.path.dirname(os.path.abspath(__file__))

HOST = "127.0.0.1"
NUM_NODES = int(os.environ.get("DKV_NUM_NODES", 5))
BASE_PORT = int(os.environ.get("DKV_BASE_PORT", 8001))
NODES = {f"node{i + 1}": (HOST, BASE_PORT + i) for i in range(NUM_NODES)}

# Replication / quorum settings
N = int(os.environ.get("DKV_N", 3))   # replicas per key
R = int(os.environ.get("DKV_R", 2))   # read quorum
W = int(os.environ.get("DKV_W", 2))   # write quorum

VNODES = int(os.environ.get("DKV_VNODES", 100))        # virtual nodes per physical node
HEARTBEAT_INTERVAL = 1.0                               # seconds between health pings
REQUEST_TIMEOUT = 1.0                                  # seconds for node-to-node calls
DATA_DIR = os.environ.get("DKV_DATA_DIR", os.path.join(ROOT, "data"))
FSYNC = os.environ.get("DKV_FSYNC", "0") == "1"        # fsync every WAL write (slower, safer)
REPLICA_DELAY_MS = int(os.environ.get("DKV_REPLICA_DELAY_MS", 0))  # artificial lag (experiments)
