import os, sys, time, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from client import DKVClient, QuorumError
from cluster import Cluster


class TestCluster(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cl = Cluster(num_nodes=3, N=3, R=2, W=2, base_port=9301)
        cls.cl.start_all()
        cls.c = DKVClient(cls.cl.endpoints)

    @classmethod
    def tearDownClass(cls):
        cls.cl.stop_all()

    def test_1_put_get_overwrite_delete(self):
        self.assertTrue(self.c.put("a", "1"))
        self.assertEqual(self.c.get("a"), "1")
        self.c.put("a", "2")
        self.assertEqual(self.c.get("a"), "2")
        self.c.delete("a")
        self.assertIsNone(self.c.get("a"))
        self.assertIsNone(self.c.get("never-existed"))

    def test_2_survives_one_node_failure(self):
        self.c.put("survivor", "yes")
        self.cl.stop_node("node2")
        time.sleep(2.0)
        self.assertEqual(self.c.get("survivor"), "yes")
        self.assertTrue(self.c.put("while-down", "ok"))

    def test_3_fails_without_quorum(self):
        self.cl.stop_node("node3")                     # node2 already down from previous test
        time.sleep(2.0)
        with self.assertRaises(QuorumError):
            self.c.put("x", "y")


if __name__ == "__main__":
    unittest.main()
