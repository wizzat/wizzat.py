import functools, time, traceback, logging, texttable, sys, threading

__all__ = [
    'MemoizeResults',
    'memoize',
    'coroutine',
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
    def format_stats(cls):
        print 'Memoize Stats By Function'

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
                sys.getsizeof(stats.cache),                                         # 'Cache Size',
            ])

        return table.draw()

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
        try:
            lock.release()
        except RuntimeError:
            pass

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

    Modified from http://wiki.python.org/moin/PythonDecoratorLibrary
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

if __name__ == '__main__':
    @memoize(ignore_nulls = 1, stats = 1, threads = 1, sync=1)
    def a_func(some, value, *args, **kwargs):
        time.sleep(.25)
        print some, value, args, kwargs

    a_func(1, 2, a = 1)
    a_func(1, 2, a = 1)
    a_func(1, 2, a = 1)
    time.sleep(1)
    a_func(1, 2, a = 1)

    print MemoizeResults.format_stats()
