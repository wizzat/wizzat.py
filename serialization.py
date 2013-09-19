import collections, struct, cPickle, ujson as json
import bitarray, array
from decorators import *
from util import chunks

@benchmark
def write_bitset_array(itr):
    return zlib.compress(array.array('d', itr).tostring())

@benchmark
def read_bitset_array(s):
    return set(array.array.fromstring(zlib.decompress(s)))

@benchmark
def write_bitset_json(itr):
    return zlib.compress(json.dumps(list(itr)))

@benchmark
def read_bitset_json(s):
    return set(json.loads(zlib.decompress(s)))

@benchmark
def write_bitset_pickle(itr):
    return zlib.compress(cPickle.dumps(itr, -1))

@benchmark
def read_bitset_pickle(s):
    return cPickle.loads(zlib.decompress(s))

@benchmark
def write_bitset_naive(itr):
    l = []
    for chunk in chunks(list(itr), 250):
        l.append(struct.pack('<{}Q'.format(len(chunk)), *chunk))

    return zlib.compress("".join(l))

@benchmark
def read_bitset_naive(s):
    s = zlib.decompress(s)
    output_set = set()
    i = 0
    for chunk in chunks(range(len(s)//8), 250):
        chunk_set = struct.unpack_from('<{}Q'.format(len(chunk)), s, i)

        output_set.update(chunk_set)

        i += len(chunk) * 8

    return output_set

@benchmark
def write_bitset(itr):
    parts   = collections.defaultdict(int)
    buckets = collections.defaultdict(list)

    for element in itr:
        part_idx = element // 64
        offset   = element % 64
        parts[part_idx] |= (1 << offset)

    maxlen = 0
    last_bitmask = None
    for part_idx, bitmask in parts.iteritems():
        l = buckets[bitmask]
        l.append(part_idx)
        maxlen = max(len(l), maxlen)

    l = [ struct.pack('<I', len(buckets)) ]

    for bitmask, part_indexes in buckets.iteritems():
        l.append( struct.pack('<QI', bitmask, len(part_indexes)) )
        i = 0
        while i < len(part_indexes):
            n = min(len(part_indexes) - i, 250)
            l.append( struct.pack('<{}I'.format(n), *part_indexes[i:i+n]) )
            i += n

    return "".join(l)

@benchmark
def read_bitset(s):
    output_set = set()

    num_elements, = struct.unpack('<I', s[0:4])
    ptr = 4

    for _ in xrange(num_elements):
        nptr = ptr + 12
        bitmask, num_indexes = struct.unpack('<QI', s[ptr:nptr])
        bitmask_offsets = list(get_bits(bitmask))
        ptr = nptr

        i = 0
        while i < num_indexes:
            n = min(num_indexes - i, 250)
            nptr = ptr+n*4
            bases = struct.unpack('<{}I'.format(n), s[ptr:nptr])
            for base in bases:
                base *= 64
                for offset in bitmask_offsets:
                    output_set.add(base+offset)
            ptr = nptr
            i += n

    return output_set

def get_bits(bitmask):
    idx = 0
    while bitmask != 0:
        if bitmask & 1:
            yield idx
        bitmask >>= 1
        idx += 1

if __name__ == '__main__':
    import unittest, random, zlib

    class TestBitset(unittest.TestCase):
        funcs = {
            'naive' : [ write_bitset_naive, read_bitset_naive ],
            'pickle' : [ write_bitset_pickle, read_bitset_pickle ],
            'json' : [ write_bitset_json, read_bitset_json ],
            'ser'  : [ write_bitset, read_bitset ],
        }

        def test_serialization(self):
            s = { 1, 2, 3, 4 }
            for name, func_tpl in self.funcs.iteritems():
                write_func, read_func = func_tpl
                self.assertEqual(read_func(write_func(s)), s, name)
                self.assertEqual(read_func(write_func(s)), s, name)

        def test_random_serialization_and_deserialization(self):
            r = [ random.randint(1, 2**35-1) for _ in xrange(20000) ]
            for _ in xrange(100):
                s = { random.choice(r) for _ in xrange(1000) }
                for name, func_tpl in self.funcs.iteritems():
                    write_func, read_func = func_tpl
                    self.assertEqual(read_func(write_func(s)), s, name)
                    self.assertEqual(read_func(write_func(s)), s, name)

        def test_performance(self):
            BenchResults.clear()
            r = { random.randint(1, 250000) for _ in xrange(100000) }

            for name, func_tpl in self.funcs.iteritems():
                write_func, read_func = func_tpl
                for _ in xrange(50):
                    s = write_func(r)
                    read_func(s)

                print 'raw', name, len(s)
                print 'zlib', name, len(zlib.compress(s))
                print

            print BenchResults.format_stats()

    unittest.main()

# Pre messing with bytearray
# +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+
# | write_bitset_array  | 0            | 0     |
# +---------------------+--------------+-------+
# | read_bitset_json    | 0.581        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.607        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_naive   | 0.725        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_pickle | 2.579        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_naive  | 2.923        | 50    |
# +---------------------+--------------+-------+
# | write_bitset        | 3.266        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_json   | 3.438        | 50    |
# +---------------------+--------------+-------+
# | read_bitset         | 5.001        | 50    |
# +---------------------+--------------+-------+


# Stripped out cStringIO and took out bytearray 
# +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+
# | read_bitset_array   | 0            | 0     |
# +---------------------+--------------+-------+
# | write_bitset_array  | 0            | 0     |
# +---------------------+--------------+-------+
# | read_bitset_json    | 0.556        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.617        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_naive   | 0.720        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_pickle | 2.597        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_naive  | 2.935        | 50    |
# +---------------------+--------------+-------+
# | write_bitset        | 3.142        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_json   | 3.425        | 50    |
# +---------------------+--------------+-------+
# | read_bitset         | 4.992        | 50    |
# +---------------------+--------------+-------+
# 

# +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+
# | read_bitset_array   | 0            | 0     |
# +---------------------+--------------+-------+
# | write_bitset_array  | 0            | 0     |
# +---------------------+--------------+-------+
# | read_bitset_json    | 0.571        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.595        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_naive   | 0.690        | 50    |
# +---------------------+--------------+-------+
# | write_bitset        | 2.477        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_pickle | 2.568        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_naive  | 2.922        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_json   | 3.414        | 50    |
# +---------------------+--------------+-------+
# | read_bitset         | 4.991        | 50    |
# +---------------------+--------------+-------+


# +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+
# | read_bitset_array   | 0            | 0     |
# +---------------------+--------------+-------+
# | write_bitset_array  | 0            | 0     |
# +---------------------+--------------+-------+
# | read_bitset_json    | 0.615        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.632        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_naive   | 0.694        | 50    |
# +---------------------+--------------+-------+
# | write_bitset        | 2.405        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_pickle | 2.603        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_naive  | 2.910        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_json   | 3.537        | 50    |
# +---------------------+--------------+-------+
# | read_bitset         | 4.931        | 50    |
# +---------------------+--------------+-------+
# 
