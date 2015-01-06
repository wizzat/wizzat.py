from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from wizzat.serialization import *
from wizzat.testutil import *
import random, zlib, struct

class TestIntSet(TestCase):
    def test_write_int_set(self):
        self.assertEqual(write_int_set(set()), '\x00\x00\x00\x00')
        self.assertEqual(write_int_set({ 1, 2, 3 }, False), ''.join([
            '\x01\x00\x00\x00',                 # Num bitmasks
            '\x0e\x00\x00\x00\x00\x00\x00\x00', # Bitmask, 0b1110
            '\x01\x00\x00\x00',                 # Num Indexes
            '\x00\x00\x00\x00'                  # First Index
        ]))
        self.assertEqual(write_int_set({ 1, 2, 3 }, True), zlib.compress(''.join([
            '\x01\x00\x00\x00',                 # Num bitmasks
            '\x0e\x00\x00\x00\x00\x00\x00\x00', # Bitmask, 0b1110
            '\x01\x00\x00\x00',                 # Num Indexes
            '\x00\x00\x00\x00'                  # First Index
        ])))

    def test_read_int_set(self):
        self.assertEqual(read_int_set(''.join([
            '\x01\x00\x00\x00',                 # Num bitmasks
            '\x0e\x00\x00\x00\x00\x00\x00\x00', # Bitmask, 0b1110
            '\x01\x00\x00\x00',                 # Num Indexes
            '\x00\x00\x00\x00'                  # First Index
        ]), False), { 1, 2, 3 })

        self.assertEqual(read_int_set(zlib.compress(''.join([
            '\x01\x00\x00\x00',                 # Num bitmasks
            '\x0e\x00\x00\x00\x00\x00\x00\x00', # Bitmask, 0b1110
            '\x01\x00\x00\x00',                 # Num Indexes
            '\x00\x00\x00\x00'                  # First Index
        ])), True), { 1, 2, 3 })

    def test_serialization_limits(self):
        self.assert_(write_int_set({ 0, 2**38-1 }))
        self.assertRaises(struct.error, lambda: write_int_set({ 0, 2**38 })) # Maximum serializable value is 2**38-1
        self.assertRaises(struct.error, lambda: write_int_set({ -1, 0, 1 })) # Can't serialize negatives

    def test_int_set_compression_exceptions(self):
        self.assertRaises(zlib.error, lambda: read_int_set(''.join([
            '\x01\x00\x00\x00',                 # Num bitmasks
            '\x0e\x00\x00\x00\x00\x00\x00\x00', # Bitmask, 0b1110
            '\x01\x00\x00\x00',                 # Num Indexes
            '\x00\x00\x00\x00'                  # First Index
        ]), True))

    def test_int_set__randoms(self):
        random_list = [ random.randint(1, 2**38-1) for _ in xrange(100000) ]

        for _ in xrange(100):
            s = { random.choice(random_list) for _ in xrange(100) }
            self.assertEqual(read_int_set(write_int_set(s, False), False), s)
            self.assertEqual(read_int_set(write_int_set(s, True), True), s)

class TestPackIterable(TestCase):
    def test_pack_iterable(self):
        self.assertEqual(pack_iterable([ 1, 2, 3, 3], 'H', False), ''.join([
            '\x01\x00', # 1
            '\x02\x00', # 2
            '\x03\x00', # 3
            '\x03\x00', # 3
        ]))

        self.assertEqual(pack_iterable([ 1, 2, 3, 3], 'H', True), zlib.compress(''.join([
            '\x01\x00', # 1
            '\x02\x00', # 2
            '\x03\x00', # 3
            '\x03\x00', # 3
        ])))

    def test_unpack_iterable(self):
        self.assertEqual(unpack_iterable(''.join([
            '\x01\x00', # 1
            '\x02\x00', # 2
            '\x03\x00', # 3
            '\x03\x00', # 3
        ]), 'H', False), [ 1, 2, 3, 3 ])

        self.assertEqual(unpack_iterable(zlib.compress(''.join([
            '\x01\x00', # 1
            '\x02\x00', # 2
            '\x03\x00', # 3
            '\x03\x00', # 3
        ])), 'H', True), [ 1, 2, 3, 3 ])

    def test_iterable__random(self):
        random_list = [ random.randint(1, 2**64-1) for _ in xrange(100000) ]

        for _ in xrange(100):
            s = [ random.choice(random_list) for _ in xrange(100) ]
            self.assertEqual(unpack_iterable(pack_iterable(s, 'Q', False), 'Q', False), s)
            self.assertEqual(unpack_iterable(pack_iterable(s, 'Q', True), 'Q', True), s)

    def test_iterable__compression_exception(self):
        self.assertRaises(zlib.error, lambda: unpack_iterable(''.join([
            '\x01\x00', # 1
            '\x02\x00', # 2
            '\x03\x00', # 3
            '\x03\x00', # 3
        ]), 'H', True))
