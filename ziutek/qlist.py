#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
>>> pack([]).encode('hex')
'10deadbeefdeadbaaf00'
>>> pack([1,2,3,4]).encode('hex')
'10deadbeefdeadbaaf8181818100'

>>> a = pack([1, 256L, 65536L, 16777216L, 4294967295L])
>>> b = pack([3, 256])
>>> unpack( do_or(a, b) )
array('L', [1L, 3L, 256L, 65536L, 16777216L, 4294967295L])

>>> unpack( do_and(a, b) )
array('L', [256L])
>>> unpack( do_and(b, a) )
array('L', [256L])

>>> unpack( do_andnot(a, b) )
array('L', [1L, 65536L, 16777216L, 4294967295L])
>>> unpack( do_andnot(b, a) )
array('L', [3L])
'''
import array
try:
    from . import _qlist
except ImportError:
    import _qlist

def pack(arr, sort=False, typecode='L'):
    '''
    >>> pack([1]).encode('hex')
    '10deadbeefdeadbaaf8100'
    >>> pack(array.array('L', [1])).encode('hex')
    '10deadbeefdeadbaaf8100'
    '''
    # Is arr a sequence?
    if sort:
        arr = sorted(arr)
    # list() is used by default instead of array. It appears to be twice faster!
    #if not isinstance(arr, array.array):
    arr = array.array(typecode, arr)
    #assert arr.typecode in 'LI'
    return _qlist.pack_array(arr.tostring(), arr.itemsize)

def unpack(qbuf, reverse=False, typecode='L'):
    '''
    '''
    arr = array.array(typecode)
    s = _qlist.unpack_to_array(qbuf, arr.itemsize)
    arr.fromstring(s)
    if reverse:
        return array.array(typecode, reversed(arr))
    return arr

def do_or(qbuf_a, qbuf_b):
    '''
    '''
    return _qlist.do_or(qbuf_a, qbuf_b)

def do_and(qbuf_a, qbuf_b):
    '''
    '''
    return _qlist.do_and(qbuf_a, qbuf_b)

def do_andnot(qbuf_a, qbuf_b):
    '''
    '''
    return _qlist.do_andnot(qbuf_a, qbuf_b)

def is_valid(qbuf):
    return is_qbuf(qbuf)

def is_qbuf(qbuf):
    return isinstance(qbuf, str) and qbuf.startswith('\x10\xde\xad\xbe\xef\xde\xad\xba\xaf')


if __name__ == "__main__":
    import doctest
    doctest.testmod()
