import unittest
import queue
from mesos_stats.carbon import Carbon

class CarbonTest(unittest.TestCase):
    def test_add_prefix(self):
        c = Carbon('127.0.0.1', 'myprefix.abc')
        a = "a.b 123 1111"
        self.assertEqual(c._add_prefix(a), 'myprefix.abc.a.b 123 1111')


    def test_add_prefix_pickle(self):
        c = Carbon('127.0.0.1', 'myprefix.abc', pickle=True)
        a = ('a.b', (123, 1111))
        self.assertEqual(c._add_prefix(a),
                         ('myprefix.abc.a.b', (123, 1111))
        )

    def test_get_chunk_from_queue(self):
        c = Carbon('127.0.0.1', prefix=None, pickle=False)
        q = queue.Queue()
        for i in [1, 2, 3, 4, 5]:
            q.put(i, block=False)
        a = c._get_chunk_from_queue(q, 2)
        self.assertEqual(len(a), 2)
        a = c._get_chunk_from_queue(q, 100)
        self.assertEqual(len(a), 3)

