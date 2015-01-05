import collections, struct, zlib
from decorators import *
from util import chunks

__all__ = [
    'pack_iterable',
    'unpack_iterable',
    'write_int_set',
    'read_int_set',
]

@memoize()
def structs(code):
    return [ struct.Struct('<{}{}'.format(ct, code)) for ct in xrange(251) ]

def pack_iterable(itr, type_code, compress = False):
    """
    This is a wrapper around struct.pack for packing arrays of homogeneous data.
    """
    l = []
    readers = structs(type_code)
    for chunk in chunks(list(itr), 250):
        l.append( readers[len(chunk)].pack(*chunk) )

    s = "".join(l)
    return s if not compress else zlib.compress(s)

def unpack_iterable(s, type_code, compressed = False):
    """
    This is a wrapper around struct.unpack for unpacking arrays of homogeneous data.
    """
    if compressed:
        s = zlib.decompress(s)

    readers = structs(type_code)
    size = readers[1].size
    output = []

    i = 0
    while i < len(s):
        reader = readers[min((len(s)-i)//size, 250)]
        output.extend(reader.unpack_from(s, i))
        i += reader.size

    return output

bmstruct = struct.Struct('<QI')
istructs = structs('I')
def write_int_set(itr, compress = False):
    """
    This is a space efficient serialization format for integer iterables.
    """
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

    s = "".join(output_list)
    return s if not compress else zlib.compress(s)

def read_int_set(s, compressed = False):
    """
    This is a space efficient serialization format for integer iterables.
    """
    if compressed:
        s = zlib.decompress(s)
    output_set = set()

    ptr = 0
    num_elements, = istructs[1].unpack_from(s, 0)
    ptr += istructs[1].size

    for _ in xrange(num_elements):
        bitmask, num_indexes = bmstruct.unpack_from(s, ptr)
        ptr += bmstruct.size
        bitmask_offsets = [ x for x in xrange(64) if bitmask & (1<<x) ]

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
