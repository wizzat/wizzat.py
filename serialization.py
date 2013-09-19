import collections, struct, cPickle
import bitarray, array
from decorators import *
from util import chunks

try:
    import ujson as json
except ImportError:
    import json

@benchmark
def write_bitset_array(itr):
    return zlib.compress(array.array('d', itr).tostring())

@benchmark
def read_bitset_array(s):
    a = array.array('d')
    a.fromstring(zlib.decompress(s))
    return set(a)

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

qstructs = [ struct.Struct('<{}Q'.format(x)) for x in xrange(251) ]
@benchmark
def write_bitset_naive(itr):
    l = []
    for chunk in chunks(list(itr), 250):
        l.append( qstructs[len(chunk)].pack(*chunk) )

    return zlib.compress("".join(l))

@benchmark
def read_bitset_naive(s):
    s = zlib.decompress(s)
    output_set = set()
    i = 0
    for chunk in chunks(range(len(s)//8), 250):
        reader = qstructs[len(chunk)]
        output_set.update(reader.unpack_from(s, i))
        i += reader.size

    return output_set

bmstruct = struct.Struct('<QI')
istructs = [ struct.Struct('<{}I'.format(x)) for x in xrange(251) ]
@benchmark
def write_bitset(itr):
    parts   = collections.defaultdict(int)
    buckets = collections.defaultdict(list)

    for element in itr:
        part_idx = element // 64
        offset   = element % 64
        parts[part_idx] |= (1 << offset)

    for part_idx, bitmask in parts.iteritems():
        buckets[bitmask].append(part_idx)

    output_list = [ istructs[1].pack(len(buckets)) ]

    for bitmask, part_indexes in buckets.iteritems():
        output_list.append( bmstruct.pack(bitmask, len(part_indexes)) )
        for chunk in chunks(part_indexes, 250):
            output_list.append( istructs[len(chunk)].pack(*chunk) )

    return zlib.compress("".join(output_list))

@benchmark
def read_bitset(s):
    s = zlib.decompress(s)
    output_set = set()

    ptr = 0
    num_elements, = istructs[1].unpack_from(s, 0)
    ptr += istructs[1].size
    xr64 = xrange(64)

    for _ in xrange(num_elements):
        bitmask, num_indexes = bmstruct.unpack_from(s, ptr)
        ptr += bmstruct.size
        bitmask_offsets = [ x for x in xr64 if bitmask & (1<<x) ]

        i = 0
        while i < num_indexes:
            n = min(num_indexes - i, 250)
            bases = istructs[n].unpack_from(s, ptr)
            for base in bases:
                base *= 64
                output_set.update(base+offset for offset in bitmask_offsets)
            ptr += istructs[n].size
            i += n

    return output_set

if __name__ == '__main__':
    import unittest, random, zlib

    class TestBitset(unittest.TestCase):
        funcs = {
            'array'  : [ write_bitset_array, read_bitset_array ],
            'naive'  : [ write_bitset_naive, read_bitset_naive ],
            'pickle' : [ write_bitset_pickle, read_bitset_pickle ],
            'json'   : [ write_bitset_json, read_bitset_json ],
            'ser'    : [ write_bitset, read_bitset ],
        }

        def test_serialization(self):
            s = { 1, 2, 3, 4, 2**64-1 }
            for name, func_tpl in self.funcs.iteritems():
                write_func, read_func = func_tpl
                self.assertEqual(read_func(write_func(s)), s, name + str(s) + str(read_func(write_func(s))))

        def test_random_serialization_and_deserialization(self):
            r = [ random.randint(1, 2**35-1) for _ in xrange(20000) ]
            for _ in xrange(100):
                s = { random.choice(r) for _ in xrange(5000) }
                for name, func_tpl in self.funcs.iteritems():
                    write_func, read_func = func_tpl
                    self.assertEqual(read_func(write_func(s)), s, name)
                    self.assertEqual(read_func(write_func(s)), s, name)

        def test_performance(self):
            self.skipTest("abc")
            BenchResults.clear()
            r = { random.randint(1, 250000) for _ in xrange(100000) }

            for name, func_tpl in self.funcs.iteritems():
                write_func, read_func = func_tpl
                for _ in xrange(50):
                    s = write_func(r)
                    read_func(s)

                #print 'raw', name, len(s)
                #print 'zlib', name, len(zlib.compress(s))
                #print

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

# Before qstructs
# +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+
# | read_bitset_array   | 0.454        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_json    | 0.567        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.588        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_naive   | 0.804        | 50    |
# +---------------------+--------------+-------+
# | write_bitset        | 2.326        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_pickle | 2.566        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_naive  | 3.000        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_array  | 3.253        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_json   | 3.478        | 50    |
# +---------------------+--------------+-------+
# | read_bitset         | 4.888        | 50    |
# +---------------------+--------------+-------+


# Add qstructs, update to pack_into/from, eliminate get_bits
# +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+
# | read_bitset_array   | 0.443        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_json    | 0.574        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.587        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_naive   | 0.703        | 50    |
# +---------------------+--------------+-------+
# | write_bitset        | 2.322        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_pickle | 2.552        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_naive  | 2.894        | 50    |
# +---------------------+--------------+-------+
# | read_bitset         | 3.148        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_array  | 3.227        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_json   | 3.464        | 50    |
# +---------------------+--------------+-------+



# First pypy build, uses json instead of ujson
# +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+
# | write_bitset        | 0.554        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_array   | 0.903        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.964        | 50    |
# +---------------------+--------------+-------+
# | read_bitset         | 1.310        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_naive   | 1.563        | 50    |
# +---------------------+--------------+-------+
# | read_bitset_json    | 2.363        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_array  | 2.692        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_naive  | 2.887        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_pickle | 3.085        | 50    |
# +---------------------+--------------+-------+
# | write_bitset_json   | 3.432        | 50    |
# +---------------------+--------------+-------+

# With zlib compress/decompress
# Python-2.7.3                                     Pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |   |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+   +=====================+==============+=======+
# | read_bitset_array   | 0.432        | 50    |   | read_bitset_array   | 0.913        | 50    | -> 2x slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset_json    | 0.586        | 50    |   | read_bitset_json    | 2.356        | 50    | -> 4.5x slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.603        | 50    |   | read_bitset_pickle  | 0.941        | 50    | -> 50% slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset_naive   | 0.789        | 50    |   | read_bitset_naive   | 1.567        | 50    | -> 2x slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset        | 2.396        | 50    |   | write_bitset        | 0.574        | 50    | -> 4x faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset_pickle | 2.589        | 50    |   | write_bitset_pickle | 3.081        | 50    | -> 20% slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset_naive  | 2.922        | 50    |   | write_bitset_naive  | 2.876        | 50    | -> Approximately equal
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset         | 2.982        | 50    |   | read_bitset         | 1.282        | 50    | -> 2.5x faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset_array  | 3.278        | 50    |   | write_bitset_array  | 2.710        | 50    | -> 30% faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset_json   | 3.493        | 50    |   | write_bitset_json   | 3.455        | 50    | -> Approximately equal
# +---------------------+--------------+-------+   +---------------------+--------------+-------+

# Without zlib compress/decompress
# Python 2.7.3                                     Pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# |      Function       | Sum Duration | Calls |   |      Function       | Sum Duration | Calls |
# +=====================+==============+=======+   +=====================+==============+=======+
# | write_bitset_pickle | 0.254        | 50    |   | write_bitset_pickle | 0.778        | 50    | -> 3x slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset_array   | 0.281        | 50    |   | read_bitset_array   | 0.622        | 50    | -> 2x slower with pypy-2.0.1 
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset_json   | 0.371        | 50    |   | write_bitset_json   | 0.310        | 50    | -> 20% faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset_naive  | 0.409        | 50    |   | write_bitset_naive  | 0.364        | 50    | -> 10% faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset_json    | 0.428        | 50    |   | read_bitset_json    | 2.148        | 50    | -> 5x slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset_pickle  | 0.478        | 50    |   | read_bitset_pickle  | 0.850        | 50    | -> 75% slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset_naive   | 0.590        | 50    |   | read_bitset_naive   | 1.438        | 50    | -> 3x slower with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset_array  | 0.766        | 50    |   | write_bitset_array  | 0.279        | 50    | -> 5x faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | write_bitset        | 2.358        | 50    |   | write_bitset        | 0.590        | 50    | -> 4x faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+
# | read_bitset         | 3.009        | 50    |   | read_bitset         | 1.353        | 50    | -> 2x faster with pypy-2.0.1
# +---------------------+--------------+-------+   +---------------------+--------------+-------+


























