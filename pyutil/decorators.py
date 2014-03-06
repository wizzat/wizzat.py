import functools, time, traceback, logging, sys, threading, StringIO
from util import swallow, OfflineError, assert_online

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
    class MemoStat(object):
        calls     = 0
        miss      = 0
        nulls     = 0
        expire    = 0
        exec_time = 0

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
    def cache_size(cls, stats):
        """
            Returns the cache size for native python objects.
            Does not work on Pypy.
        """
        # Pypy compatibility
        try:
            return sys.getsizeof(stats.cache)
        except TypeError:
            return 0

    @classmethod
    def format_stats(cls):
        """
            Calculates the statistics for all memoized() things.
            Returns a text table formatted string containing:
            - Function name
            - Calls
            - Hits
            - Misses
            - Nulls
            - Miss Time
            - Max Time Saved
            - Cache Items
            - Cache Size
        """
        import texttable
        table = texttable.Texttable(0)
        table.header([
            'Function Name',
            'Calls',
            'Hits',
            'Misses',
            'Nulls',
            'Miss Time',
            'Max Time Saved',
            'Cache Items',
            'Cache Size',
        ])

        def cache_size(stats):
            try:
                return sys.getsizeof(stats.cache)
            except TypeError:
                return 0

        for func, stats in sorted(cls.stats.iteritems(), key=lambda x: x[1].calls):
            table.add_row([
                func.__name__,                                                      # 'Function Name',
                stats.calls,                                                        # 'Calls',
                stats.calls - stats.miss,                                           # 'Hits',
                stats.miss,                                                         # 'Misses',
                stats.nulls,                                                        # 'Nulls',
                stats.exec_time,                                                    # 'Miss Time',
                (stats.calls - stats.miss) * (stats.exec_time / (stats.miss or 1)), # 'Max Time Saved',
                len(stats.cache),                                                   # 'Cache Items',
                cls.cache_size(stats),                                              # 'Cache Size',
            ])

        return "Memoize Stats By Function\n\n" + table.draw()

    @classmethod
    def format_csv(cls):
        """
            Calculates the statistics for all memoized() things.
            Returns a csv formatted string containing:
            - Function name
            - Calls
            - Hits
            - Misses
            - Nulls
            - Miss Time
            - Max Time Saved
            - Cache Items
            - Cache Size
        """
        fp = StringIO.StringIO()
        fp.write(",".join([
            'Function Name',
            'Calls',
            'Hits',
            'Misses',
            'Nulls',
            'Miss Time',
            'Max Time Saved',
            'Cache Items',
            'Cache Size',
        ]))
        fp.write("\n")

        for func, stats in sorted(cls.stats.iteritems(), key=lambda x: x[1].calls):
            fp.write(",".join([ str(x) for x in [
                func.__name__,                                                      # 'Function Name',
                stats.calls,                                                        # 'Calls',
                stats.calls - stats.miss,                                           # 'Hits',
                stats.miss,                                                         # 'Misses',
                stats.nulls,                                                        # 'Nulls',
                stats.exec_time,                                                    # 'Miss Time',
                (stats.calls - stats.miss) * (stats.exec_time / (stats.miss or 1)), # 'Max Time Saved',
                len(stats.cache),                                                   # 'Cache Items',
                cls.cache_size(stats),                                              # 'Cache Size',
            ] ]))
            fp.write("\n")

        return fp.getvalue()

def construct_memo_func(stats = False, until = False, kw = False, ignore_nulls = False, threads = False, verbose = False, sync = False):
    if stats:
        log_call      = "stats.calls     += 1"
        log_miss      = "stats.miss      += 1"
        log_expire    = "stats.expire    += 1"
        log_exec_time = "stats.exec_time += time.time() - start"
    else:
        log_call      = ""
        log_miss      = ""
        log_expire    = ""
        log_exec_time = ""

    if ignore_nulls and stats:
        log_nulls = "if value == None: stats.nulls += 1"
    else:
        log_nulls = ""

    if threads:
        threadlock   = "lock.acquire()"
        threadunlock = "release(lock)"
    else:
        threadlock   = ""
        threadunlock = "pass"

    if sync:
        sync       = "lock.acquire()"
        syncunlock = "release(lock)"
    else:
        sync       = ""
        syncunlock = "pass"

    if until:
        test_value  = "ts, value  = cache[key]"
        store_value = "cache[key] = (ts, value)"
        template    = """
@functools.wraps(func)
def memo_func(*args, **kwargs):
    {log_call}
    {setup_key}
    {start_clock}
    {sync}
    try:
        {test_value}
        if ts < start:
            {log_expire}
            {log_miss}
            ts    = expire_func()
            value = func(*args, **kwargs)
            {log_exec_time}
            {threadlock}
            {store_value}
            {log_nulls}
        return value
    except KeyError:
        {log_miss}
        ts    = expire_func()
        value = func(*args, **kwargs)
        {log_exec_time}
        {threadlock}
        {store_value}
        {log_nulls}
        return value
    finally:
        {threadunlock}
        {syncunlock}
"""
    else:
        test_value  = "value      = cache[key]"
        store_value = "cache[key] = value"
        template    = """
@functools.wraps(func)
def memo_func(*args, **kwargs):
    {log_call}
    {setup_key}
    {start_clock}
    {sync}
    try:
        {test_value}
        return value
    except KeyError:
        {log_miss}
        value = func(*args, **kwargs)
        {log_exec_time}
        {threadlock}
        {store_value}
        {log_nulls}
        return value
    finally:
        {threadunlock}
        {syncunlock}
"""

    if until or stats:
        start_clock = "start = time.time()"
    else:
        start_clock = ""

    if ignore_nulls:
        store_value = "if value != None: {}".format(store_value)

    if kw:
        setup_key = "key = (args, tuple(sorted(zip(kwargs.iteritems()))))"
    else:
        setup_key = "key = args"

    if verbose:
        print template.format(**locals())

    return template.format(**locals())

def memo_func(func, Stats, Cache, stats = False, until = False, kw = False, ignore_nulls = False, verbose = False, threads = False, sync = False):
    template = construct_memo_func(stats = stats, until = until, kw = kw, ignore_nulls = ignore_nulls, verbose = verbose, threads = threads, sync = sync)

    def release(lock):
        swallow(RuntimeError, lock.release)

    namespace = {
        '__name__'    : 'memoize_func_{}'.format(func.__name__),
        'functools'   : functools,
        'time'        : time,
        'expire_func' : until,
        'release'     : release,
        'func'        : func,
        'stats'       : Stats,
        'cache'       : Cache,
        'lock'        : threading.RLock(),
    }

    exec template.format(**locals()) in namespace

    return namespace['memo_func']

def memoize(until = None, kw = None, ignore_nulls = None, stats = None, verbose = False, threads = False, sync = False):
    """
    Memoize Function.
    Arguments:
        until:        memoize until time specified (seconds, using time.time)
        stats:        collect memoize stats around hits, misses, and execution time
        kw:           memoize around kwargs.  There is a hashing performance penalty here.
        ignore_nulls: don't memoize null return values
        verbose:      print the constructed memoize function
        threads:      thread safety locks around updating cache
        sync:         syncronize around lookup as well

    Examples:

    # Memoize for an hour
    @memoize(until = lambda: time.time()+3600)
    def func(): pass

    # Respect kwargs for memoize
    @memoize(until = lambda: time.time()+3600, kw=True)
    def func(**kwargs): pass

    # Respect kwargs for memoize, syncronize, memoize for an hour
    @memoize(until = lambda: time.time()+3600, kw=True, sync=True)
    def func(**kwargs): pass

    Inspired by http://wiki.python.org/moin/PythonDecoratorLibrary
    """
    def wrap(func):
        Cache = func.cache = MemoizeResults.caches[func] = {}
        Stats = func.stats = MemoizeResults.stats[func]  = MemoizeResults.MemoStat()
        Stats.cache = Cache

        return memo_func(func, Stats, Cache,
            until        = until,
            kw           = kw,
            ignore_nulls = ignore_nulls,
            stats        = stats,
            verbose      = verbose,
            sync         = sync,
        )
    return wrap

def memoize_property(func):
    cache_name = '__cache_{}'.format(func.__name__)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        obj = args[0]
        if not hasattr(obj, cache_name):
            rv = func(*args, **kwargs)
            setattr(obj, cache_name, rv)
        return getattr(obj, cache_name)
    return wrapper

class BenchResults(object):
    """
        Acts as a storage container for all benchmark results.

        Expected usage:

        @benchmark
        def foo():
            print 'called foo!'

        for x in xrange(100):
            foo()

        print BenchResults().format_stats() # Text table pretty
        print BenchResults().format_csv() # For CSV
    """
    results = {}

    @classmethod
    def format_stats(cls, skip_no_calls = False):
        import texttable
        table = texttable.Texttable(0)
        table.header([
            'Function',
            'Sum Duration',
            'Calls',
        ])

        for k, v in sorted(cls.results.iteritems(), key=lambda x: x[1][1]):
            if skip_no_calls and calls == 0:
                continue

            func, cume_duration, calls = v
            table.add_row([ k.__name__, cume_duration, calls ])

        return "Benchmark Results\n\n" + table.draw()

    @classmethod
    def format_csv(cls, skip_no_calls = False):
        fp = StringIO.StringIO()

        fp.write(", ".join([
            'Function',
            'Sum Duration',
            'Calls',
        ]))
        fp.write("\n")

        for k, v in sorted(cls.results.iteritems(), key=lambda x: x[1][1]):
            if skip_no_calls and calls == 0:
                continue

            func, cume_duration, calls = v
            fp.write(", ".join([ str(x) for x in [ k.__name__, cume_duration, calls ] ]))
            fp.write("\n")

    @classmethod
    def clear(cls):
        for func, stats in cls.results.iteritems():
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

def skip_performance(func):
    """
    This decorator is meant for tests.  It checks for $ENV{PERFORMANCE_TEST} and will issue skipTest without it.
    """
    @functools.wraps(func)
    def wrapper(self):
        if not os.environ.get('PERFORMANCE_TEST', False):
            self.skipTest("----- PERFORMANCE TEST -----")
        else:
            return func(self)

    return wrapper

def skip_unfinished(func):
    """
    This decorator is meant for tests.  It automatically issues a skipTest.
    """

    @functools.wraps(func)
    def wrapper(self):
        self.skipTest("----- UNFINISHED TEST -----")
    return wrapper
