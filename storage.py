"""Per-node storage engine: in-memory dict + append-only write-ahead log (WAL).
Conflict resolution is last-write-wins (LWW) using a timestamp on every record.
Deletes are stored as tombstones so they replicate and win over older writes."""
import json
import os
import threading


class Storage:
    def __init__(self, wal_path, fsync=False):
        self.wal_path = wal_path
        self.fsync = fsync
        self._data = {}                 # key -> {"value", "ts", "deleted"}
        self._lock = threading.Lock()
        self._replay()
        self._wal = open(wal_path, "a", encoding="utf-8")

    def _replay(self):
        """Crash recovery: rebuild memory state from the log."""
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except ValueError:
                    continue            # torn (half-written) last line after a crash
                cur = self._data.get(e["k"])
                if cur is None or cur["ts"] < e["ts"]:
                    self._data[e["k"]] = {"value": e["v"], "ts": e["ts"], "deleted": e["d"]}

    def apply(self, key, value, ts, deleted=False):
        """Write a record if it is newer than what we have. Returns True if applied."""
        with self._lock:
            cur = self._data.get(key)
            if cur is not None and cur["ts"] >= ts:
                return False
            # Write-ahead: log first, then update memory.
            self._wal.write(json.dumps({"k": key, "v": value, "ts": ts, "d": deleted}) + "\n")
            self._wal.flush()
            if self.fsync:
                os.fsync(self._wal.fileno())
            self._data[key] = {"value": value, "ts": ts, "deleted": deleted}
            return True

    def get(self, key):
        """Return the raw record (may be a tombstone) or None."""
        with self._lock:
            rec = self._data.get(key)
            return dict(rec) if rec else None

    def count(self):
        with self._lock:
            return sum(1 for r in self._data.values() if not r["deleted"])

    def close(self):
        self._wal.close()
