import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from hashring import HashRing


class TestHashRing(unittest.TestCase):
    def test_returns_distinct_nodes(self):
        ring = HashRing(["a", "b", "c", "d", "e"], vnodes=50)
        for i in range(200):
            nodes = ring.get_nodes(f"key{i}", 3)
            self.assertEqual(len(nodes), 3)
            self.assertEqual(len(set(nodes)), 3)

    def test_deterministic(self):
        ring = HashRing(["a", "b", "c"])
        self.assertEqual(ring.get_nodes("x", 2), ring.get_nodes("x", 2))

    def test_reasonably_balanced(self):
        ring = HashRing(["a", "b", "c", "d"], vnodes=100)
        counts = {n: 0 for n in "abcd"}
        for i in range(4000):
            counts[ring.get_nodes(f"k{i}", 1)[0]] += 1
        for c in counts.values():
            self.assertGreater(c, 600)      # ideal is 1000

    def test_minimal_movement_on_node_removal(self):
        ring = HashRing(["a", "b", "c", "d"], vnodes=100)
        before = {f"k{i}": ring.get_nodes(f"k{i}", 1)[0] for i in range(2000)}
        ring.remove_node("d")
        for k, owner in before.items():
            if owner != "d":
                self.assertEqual(ring.get_nodes(k, 1)[0], owner)


if __name__ == "__main__":
    unittest.main()
