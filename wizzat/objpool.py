from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

import collections
import contextlib
import itertools
import threading
from wizzat.util import set_strict_defaults

__all__ = [
    'ObjPool',
    'ObjPoolError',
    'ObjPoolExhausted',
    'ObjPoolNameError',
    'ObjPoolOwnershipError',
]

class ObjPoolError(Exception):
    pass


class ObjPoolExhausted(ObjPoolError):
    """
    All objects the pool owns are in use.
    """
    pass


class ObjPoolNameError(ObjPoolError):
    """
    The object name is invalid (either already in use or non-existent)
    """
    pass


class ObjPoolOwnershipError(ObjPoolError):
    """
    The object is not owned by the object pool
    """
    pass


class ObjPool(object):
    def __init__(self, **kwargs):
        """
            An object pool with naming support.
            Paramters:
                min_objs: The minimum number of objects to have available
                max_objs: The maximum number of objects to have available
        """
        kwargs = set_strict_defaults(kwargs,
            min_objs = 0,
            max_objs = 1,
        )

        self.lock     = threading.RLock()
        self.ready    = collections.deque()
        self.in_use   = set()
        self.names    = set()
        self.min_objs = kwargs['min_objs']
        self.max_objs = kwargs['max_objs']

        if self.max_objs < self.min_objs:
            raise ValueError('max_objs {} < min_objs {}'.format(
                self.max_objs,
                self.min_objs,
            ))

        objs = [ self.new_obj() for _ in range(self.min_objs) ]
        for obj in objs:
            self.yield_obj(obj)

    def new_func(self):
        return object()

    def put_func(self, obj):
        return

    @contextlib.contextmanager
    def obj_from_pool(self):
        """
            Get an object from the pool, and put it back when finished.
        """
        obj = self.new_obj()
        yield obj
        self.yield_obj(obj)

    def name(self, name, obj = None):
        """
            Ensure that an object is available at pool.{name}.
            If you do not have an object, one will be provided for you.
        """
        with self.lock:
            existing_obj = getattr(self, name, None)

            if not obj and existing_obj:
                return existing_obj
            elif existing_obj and existing_obj != obj:
                raise ObjPoolNameError(name)

            if not obj:
                obj = self.new_obj()

            if obj not in self.in_use:
                raise ObjPoolOwnershipError()

            self.names.add(name)
            setattr(self, name, obj)
            return obj

    def unname(self, name):
        """
            Remove the object from pool.{name} and yield it back
            to the pool for reuse.
        """
        with self.lock:
            obj = getattr(self, name, None)
            if not obj or obj not in self.in_use:
                raise ObjPoolNameError(name)

            self.names.remove(name)
            delattr(self, name)
            self.yield_obj(obj)

    def new_obj(self):
        """
            Gets an object from the pool.
        """
        with self.lock:
            if self.ready:
                obj = self.ready.popleft()
            elif len(self.in_use) < self.max_objs:
                obj = self.new_func()
            else:
                raise ObjPoolExhausted()

            self.in_use.add(obj)
            return obj

    def yield_obj(self, obj):
        """
            Puts an object back in the pool
        """
        with self.lock:
            try:
                self.in_use.remove(obj)
                self.put_func(obj)
                self.ready.append(obj)
            except KeyError:
                raise ObjPoolOwnershipError()

    def yield_all(self):
        with self.lock:
            for name in list(self.names):
                self.unname(name)

            for obj in list(self.in_use):
                self.yield_obj(obj)

    def foreach(self, func, in_use=True):
        if in_use:
            objs = itertools.chain(self.ready, self.in_use)
        else:
            objs = self.ready

        for obj in objs:
            func(obj)
