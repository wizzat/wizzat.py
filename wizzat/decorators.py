from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import functools
import itertools
import os
import six
import sys
import threading
import time
import wizzat.textutil
from wizzat.util import (
    swallow,
    OfflineError,
    assert_online,
    set_defaults
)

__all__ = [
    'BenchResults',
    'MemoizeResults',
    'benchmark',
    'coroutine',
    'memoize',
    'memoize_property',
    'skip_offline',
    'skip_performance',
    'skip_unfinished',
    'skip_unless_env',
    'create_cache_obj',
]

def coroutine(func):
    """
    Coroutine generator
    Original source: http://wiki.python.org/moin/Concurrency/99Bottles
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        gen.next() # advance to the first yield
        return gen
    return wrapper

class MemoizeResults(object):
    """
    Shared state memoize() result container.
    """
    caches = {}
    stats  = {}

    @classmethod
    def clear(cls, stats = False):
        """
            Clear all memoize() caches.  Optionally clear stats as well (default False)
            Most useful with unit testing teardowns.
        """
        for cache in cls.caches.values():
            cache.clear()

        if stats:
            for stat in cls.stats.values():
                stat.clear()

    @classmethod
    def format_stats(cls):
        """
            Calculates the statistics for all memoized() things.
            Returns a text table formatted string containing:
            - Function name
            - Calls
            - Hits
            - Misses
        """

        rows = []
        for func, stats in sorted(six.iteritems(cls.stats), key=lambda x: x[1]['call']):
            rows.append([
                func.__name__,                                                      # 'Function Name',
                stats['call'],                                                      # 'Calls',
                stats['call'] - stats['miss'],                                      # 'Hits',
                stats['miss'],                                                      # 'Misses',
            ])

        table = wizzat.textutil.text_table([
            'Function Name',
            'Calls',
            'Hits',
            'Misses',
        ], rows)

        return "Memoize Stats By Function\n\n" + table

    @classmethod
    def format_csv(cls):
        """
            Calculates the statistics for all memoized() things.
            Returns a csv formatted string containing:
            - Function name
            - Calls
            - Hits
            - Misses
        """
        fp = six.moves.cStringIO()
        fp.write(",".join([
            'Function Name',
            'Calls',
            'Hits',
            'Misses',
        ]))
        fp.write("\n")

        for func, stats in sorted(six.iteritems(cls.stats), key=lambda x: x[1]['call']):
            fp.write(",".join([ str(x) for x in [
                func.__name__,                                                      # 'Function Name',
                stats['call'],                                                      # 'Calls',
                stats['call'] - stats['miss'],                                      # 'Hits',
                stats['miss'],                                                      # 'Misses',
            ] ]))
            fp.write("\n")

        return fp.getvalue()

def release(lock):
    swallow(RuntimeError, lock.release)

def construct_cache_func_definition(threads, disable_kw, obj, verbose, **kwargs):
    if threads:
        threadlock   = "lock.acquire()"
        threadunlock = "release(lock)"
    else:
        threadlock   = ""
        threadunlock = "pass"

    if disable_kw:
        setup_key = "(func, args)"
    else:
        setup_key = "(func, args, tuple(sorted(izip(iteritems(kwargs)))))"

    if obj:
        generate_cache = 'if not hasattr(args[0], "__memoize_cache__"): setattr(args[0], "__memoize_cache__", gen_cache())'
        get_cache = 'cache = args[0].__memoize_cache__'
    else:
        generate_cache = ''
        get_cache = ''

    result = """
@functools.wraps(func)
def memo_func(*args, **kwargs):
    {generate_cache}
    {get_cache}
    stats['call'] += 1
    key = {setup_key}
    {threadlock}
    try:
        return cache[key]
    except KeyError:
        stats['miss'] += 1
        value = cache[key] = func(*args, **kwargs)
        return value
    finally:
        {threadunlock}
""".format(**locals())

    if verbose:
        print(result)

    return result

def construct_cache_obj_definition(max_size, max_bytes, until, ignore_nulls, verbose, **kwargs):
    if max_size or max_bytes:
        superclass     = 'collections.OrderedDict'
        remove_old_key = "if key in self: self.pop(key)" # Enables LRU behavior
    else:
        superclass     = 'dict'
        remove_old_key = ''

    if max_bytes:
        byte_filter = "while self and self.current_size > self.max_bytes: self.popitem(False)"
        bytes_incr  = "self.current_size += sys.getsizeof(value)"
        bytes_decr  = "self.current_size -= sys.getsizeof(value)"
    else:
        byte_filter = ""
        bytes_incr = ""
        bytes_decr = ""

    if max_size:
        size_filter = "while self and len(self) > self.max_size: self.popitem(False)"
    else:
        size_filter = ""

    if ignore_nulls:
        null_filter = "if value != None: "
    else:
        null_filter = ""

    if until:
        until_check = "if expiration < time.time(): del self[key]; raise KeyError(key)"
        until_call  = "expiration = self.expire_func()"
        result_expr = "(expiration, value)"
    else:
        until_check = ""
        until_call  = ""
        result_expr = "value"

    definition = """
class Cache({superclass}):
    current_size = 0
    max_bytes    = max_bytes
    max_size     = max_size
    expire_func  = staticmethod(expire_func)

    def __delitem__(self, key):
        value = {superclass}.__getitem__(self, key)
        {bytes_decr}
        {superclass}.__delitem__(self, key)

    def __getitem__(self, key):
        {result_expr} = {superclass}.__getitem__(self, key)
        {until_check}
        return value

    def __setitem__(self, key, value):
        {remove_old_key}
        {until_call}
        {null_filter}{superclass}.__setitem__(self, key, {result_expr}); {bytes_incr}
        {byte_filter}
        {size_filter}

    if sys.version_info.major >= 3:
        def popitem(self, last=True):
            key, value = {superclass}.popitem(self, last)
            {bytes_decr}
            return key, value
""".format(**locals())

    if verbose:
        print(definition)

    return definition

def create_cache_obj(**kwargs):
    kwargs = expand_memoize_args(kwargs)

    definition = construct_cache_obj_definition(
        kwargs['max_size'],
        kwargs['max_bytes'],
        kwargs['until'],
        kwargs['ignore_nulls'],
        kwargs['verbose'],
    )

    namespace = {
        'expire_func' : kwargs['until'],
        'max_size'    : kwargs['max_size'],
        'max_bytes'   : kwargs['max_bytes'],
        'collections' : collections,
        'sys'         : sys,
        'time'        : time,
    }

    six.exec_(definition, namespace)
    return namespace['Cache']()

def create_cache_func(func, **kwargs):
    kwargs = expand_memoize_args(kwargs)
    cache_obj = func.cache = MemoizeResults.caches[func] = create_cache_obj(**kwargs)
    stats_obj = func.stats = MemoizeResults.stats[func]  = collections.Counter()

    definition = construct_cache_func_definition(**kwargs)
    namespace = {
        'functools'   : functools,
        'release'     : release,
        'func'        : func,
        'stats'       : stats_obj,
        'cache'       : cache_obj,
        'izip'        : six.moves.zip,
        'iteritems'   : six.iteritems,
        'lock'        : threading.RLock(),
        'gen_cache'   : lambda: create_cache_obj(**kwargs),
    }

    six.exec_(definition, namespace)
    return namespace['memo_func']

memoize_default_options = {
    'until'        : None,
    'disable_kw'   : False,
    'ignore_nulls' : False,
    'verbose'      : False,
    'threads'      : False,
    'obj'          : False,
    'disabled'     : False,
    'max_size'     : 0,
    'max_bytes'    : 0,
}

def expand_memoize_args(kwargs):
    global memoize_default_options
    kwargs = set_defaults(kwargs, memoize_default_options)
    if any(x not in memoize_default_options for x in six.iterkeys(kwargs)):
        raise TypeError("Received unexpected arguments to @memoize")

    return kwargs


def memoize(**kwargs):
    """
    Memoize Function.
    Arguments:
        until:        func, memoize until time specified (seconds, using time.time)
        disable_kw    bool, do not memoize around kwargs.  This is a significant performance benefit.
        ignore_nulls: bool, do not store null values in the cache.  This can cause later lookups for the same key.
        verbose:      bool, print the constructed memoize function and cache obj
        threads:      bool, thread safety locks around updating cache
        obj:          bool, memoize to the first argument (generally, self) instead of the global cache.
                            This cache can be cleared by calling obj.__memoize_cache__.clear(), and will not be
                            cleared when clearing the global cache.
        max_bytes:    int,  maximum number of bytes to keep in the cache, as calculated by sys.getsizeof(result).
                            Items are evicted in LRU order.
        max_size      int,  maximum number of items to keep in the cache.  Items are evicted in LRU order.
        disabled      bool, disable memoization and return the original function instead of the memoize wrapper

    Examples:

    # Memoize for an hour
    @memoize(until = lambda: time.time()+3600)
    def func(): pass

    # Respect kwargs for memoize
    @memoize(disable_kw=True)
    def func(arg, **kwargs): time.sleep(10)

    func(1, a = 1) # waits 10 seconds
    func(1, b = 1) # returns instantly

    # Respect kwargs for memoize, thread safe, memoize for an hour
    @memoize(until = lambda: time.time()+3600, threads=True)
    def func(*args, **kwargs): pass

    # Memoize to the first argument (self) instead
    class Foo(object):
        @memoize(obj=True)
        def method(self, arg1, arg2, **kwargs):
            return arg1 + arg2 + len(kwargs)
    """
    if kwargs.get('disabled', None):
        def wrap(func):
            return func
    else:
        def wrap(func):
            return create_cache_func(func, **kwargs)
    return wrap

memoize_property = memoize(obj=True)

class BenchResults(object):
    """
        Acts as a storage container for all benchmark results.

        Expected usage:

        @benchmark
        def foo():
            print 'called foo!'

        for x in range(100):
            foo()

        print BenchResults().format_stats() # Text table pretty
        print BenchResults().format_csv() # For CSV
    """
    results = {}

    @classmethod
    def format_stats(cls, skip_no_calls = False):
        rows = []
        for k, v in sorted(six.iteritems(cls.results), key=lambda x: x[1][1]):
            if skip_no_calls and calls == 0:
                continue

            func, cume_duration, calls = v
            rows.append([ k.__name__, cume_duration, calls ])

        table = wizzat.textutil.text_table([
            'Function',
            'Sum Duration',
            'Calls',
        ], rows)

        return "Benchmark Results\n\n" + table

    @classmethod
    def format_csv(cls, skip_no_calls = False):
        fp = six.StringIO.StringIO()

        fp.write(", ".join([
            'Function',
            'Sum Duration',
            'Calls',
        ]))
        fp.write("\n")

        for k, v in sorted(six.iteritems(cls.results), key=lambda x: x[1][1]):
            if skip_no_calls and calls == 0:
                continue

            func, cume_duration, calls = v
            fp.write(", ".join([ str(x) for x in [ k.__name__, cume_duration, calls ] ]))
            fp.write("\n")

    @classmethod
    def clear(cls):
        for func, stats in six.iteritems(cls.results):
            stats[1] = 0
            stats[2] = 0

def benchmark(obj):
    """
        Decorator for capturing function call duration and number of calls.

        Works with BenchResults for display purposes.
    """
    bench_results = obj.bench_results = BenchResults.results[obj] = [ str(obj), 0, 0]

    @functools.wraps(obj)
    def benchmarker(*args, **kwargs):
        start_time = time.time()

        retval = obj(*args, **kwargs)

        delta = time.time() - start_time

        bench_results[1] += delta
        bench_results[2] += 1

        return retval

    return benchmarker

class TailRecurseException(Exception):
    def __init__(self, args, kwargs):
        self.args   = args
        self.kwargs = kwargs

def tail_call_optimized(obj):
    """
    This function decorates a function with tail call
    optimization. It does this by throwing an exception
    if it is it's own grandparent, and catching such
    exceptions to fake the tail call optimization.

    This function fails if the decorated
    function recurses in a non-tail context.
    """
    @functools.wraps(obj)
    def func(*args, **kwargs):
        f = sys._getframe()
        if f.f_back and f.f_back.f_back and f.f_back.f_back.f_code == f.f_code:
            raise TailRecurseException(args, kwargs)
        else:
            while 1:
                try:
                    return obj(*args, **kwargs)
                except TailRecurseException as e:
                    args   = e.args
                    kwargs = e.kwargs

    return func

def skip_offline(func):
    """
    This decorator is meant for tests.  It will catch OfflineError and issue a skipTest for you.
    """
    @functools.wraps(func)
    def wrapper(self):
        try:
            assert_online()
            retval = func(self)
        except OfflineError:
            self.skipTest("----- OFFLINE TEST -----")

        return retval
    return wrapper

def skip_unfinished(func):
    """
    This decorator is meant for tests.  It automatically issues a skipTest.
    """

    @functools.wraps(func)
    def wrapper(self):
        self.skipTest("----- UNFINISHED TEST -----")
    return wrapper

def skip_unless_env(env_var):
    """
    This decorator is meant for tests.  It automatically issues a skipTest if
    """

    def skip_unless_env(func):
        @functools.wraps(func)
        def wrapper(self):
            if not os.environ.get(env_var, False):
                self.skipTest("----- SKIP ENV {} -----".format(env_var))
            else:
                return func(self)
        return wrapper
    return skip_unless_env

skip_performance = skip_unless_env('PERFORMANCE_TEST')
