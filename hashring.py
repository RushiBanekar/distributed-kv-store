"""Consistent hashing ring with virtual nodes."""
import bisect
import hashlib


def _hash(s):
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


class HashRing:
    def __init__(self, node_ids=(), vnodes=100):
        self.vnodes = vnodes
        self._ring = {}      # hash position -> node id
        self._keys = []      # sorted hash positions
        self._nodes = set()
        for n in node_ids:
            self.add_node(n)

    def add_node(self, node_id):
        for i in range(self.vnodes):
            self._ring[_hash(f"{node_id}#{i}")] = node_id
        self._keys = sorted(self._ring)
        self._nodes.add(node_id)

    def remove_node(self, node_id):
        self._ring = {h: n for h, n in self._ring.items() if n != node_id}
        self._keys = sorted(self._ring)
        self._nodes.discard(node_id)

    def get_nodes(self, key, n):
        """Return the first n DISTINCT physical nodes clockwise from the key's position
        (the 'preference list'; element 0 is the primary owner)."""
        if not self._keys:
            return []
        n = min(n, len(self._nodes))
        idx = bisect.bisect(self._keys, _hash(key)) % len(self._keys)
        result = []
        while len(result) < n:
            node = self._ring[self._keys[idx]]
            if node not in result:
                result.append(node)
            idx = (idx + 1) % len(self._keys)
        return result
