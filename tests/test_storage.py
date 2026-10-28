import os, sys, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.wal = os.path.join(self.dir, "t.wal")
        self._opened = []

    def tearDown(self):
        for s in self._opened:
            s.close()

    def open_store(self):
        s = Storage(self.wal)
        self._opened.append(s)
        return s

    def test_last_write_wins(self):
        s = self.open_store()
        s.apply("k", "new", 200)
        self.assertFalse(s.apply("k", "old", 100))
        self.assertEqual(s.get("k")["value"], "new")

    def test_wal_recovery(self):
        s = self.open_store()
        s.apply("a", "1", 1)
        s.apply("b", "2", 2)
        s.close()
        s2 = self.open_store()                         # "restart"
        self.assertEqual(s2.get("a")["value"], "1")
        self.assertEqual(s2.get("b")["value"], "2")

    def test_tombstone(self):
        s = self.open_store()
        s.apply("k", "v", 1)
        s.apply("k", None, 2, deleted=True)
        self.assertTrue(s.get("k")["deleted"])
        self.assertEqual(s.count(), 0)

    def test_torn_wal_line_ignored(self):
        s = self.open_store()
        s.apply("a", "1", 1)
        s.close()
        with open(self.wal, "a") as f:
            f.write('{"k": "b", "v"')                  # simulated crash mid-write
        s2 = self.open_store()
        self.assertEqual(s2.get("a")["value"], "1")
        self.assertIsNone(s2.get("b"))


if __name__ == "__main__":
    unittest.main()