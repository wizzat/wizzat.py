import unittest, random
from serialization import *
from testutil import *

class TestSerialization(unittest.TestCase):

    @skip_unfinished
    def test_write_int_set(self):
        self.assertEqual(write_int_set(set), '')
        self.assertEqual(write_int_set({ 1, 2, 3 }, False), '')
        self.assertEqual(write_int_set({ 1, 2, 3 }, False), '')

    @skip_unfinished
    def test_read_int_set(self):
        pass

    @skip_unfinished
    def test_int_set__randoms(self):
        random_list = [ random.randint(1, 2**38-1) for _ in xrange(100000) ]

        for _ in xrange(1000):
            s = { random.choice(random_list) for _ in xrange(10000) }
            self.assertEqual(read_int_set(write_int_set(s, False), False), s)
            self.assertEqual(read_int_set(write_int_set(s, True), True), s)

    @skip_unfinished
    def test_pack_iterable(self):
        self.assertEqual(pack_iterable([ 1, 2, 3, 3], 'H'), '')
        self.assertEqual(pack_iterable([ 1, 2048, 3, 3], 'H'), '')

    @skip_unfinished
    def test_unpack_iterable(self):
        self.assertEqual(unpack_iterable('', 'H'), [ 1, 2, 3, 3])
        self.assertEqual(unpack_iterable('', 'H'), [ 1, 2048, 3, 3])

    @skip_unfinished
    def test_iterable__random(self):
        random_list = [ random.randint(1, 2**64-1) for _ in xrange(25000) ]

        for _ in xrange(100):
            s = [ random.choice(random_list) for _ in xrange(10000) ]
            self.assertEqual(unpack_iterable(pack_iterable(s, 'Q', False), 'Q', False), s)
            self.assertEqual(unpack_iterable(pack_iterable(s, 'Q', True), 'Q', True), s)

    @skip_unfinished
    def test_serialization(self):
        s = { 1, 2, 3, 4, 2**30-1 }
        for name, func_tpl in self.funcs.iteritems():
            write_func, read_func = func_tpl
            self.assertEqual(read_func(write_func(s)), s, name + str(s) + str(read_func(write_func(s))))
