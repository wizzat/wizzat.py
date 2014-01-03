from pyutil.decorators import *
from pyutil.testutil   import *
import time

class MemoizePropertyTest(TestCase):
    def test_memoizes(self):
        class F(object):
            def __init__(self):
                self.counter = 0

            @memoize_property
            def foo(self):
                self.counter += 1
                return self.counter

        f = F()
        self.assertEqual(f.foo(), 1)
        self.assertEqual(f.foo(), 1)

    def test_memoize_on_only_obj(self):
        class F(object):
            def __init__(self):
                self.counter = 0

            @memoize_property
            def foo(self):
                self.counter += 1
                return self.counter

        f1 = F()
        f2 = F()
        f2.counter = 200
        self.assertEqual(f1.foo(), 1)
        self.assertEqual(f1.foo(), 1)
        self.assertEqual(f2.foo(), 201)
        self.assertEqual(f2.foo(), 201)
        self.assertEqual(f1.foo(), 1)

    def test_memoize_on_classobj(self):
        class F(object):
            counter = 0

            @classmethod
            @memoize_property
            def foo(cls):
                cls.counter += 1
                return cls.counter

        self.assertEqual(F.counter, 0)
        self.assertEqual(F.foo(), 1)
        self.assertEqual(F.foo(), 1)
        self.assertEqual(F.counter, 1)

        F.counter = 2
        self.assertEqual(F.foo(), 1)

class MemoizeTest(TestCase):
    options = {
        'stats'        : (False, True),
        'kw'           : (False, True),
        'ignore_nulls' : (False, True),
        'threads'      : (False, True),
        'sync'         : (False, True),
        'until'        : (False, lambda: time.time() + 1),
    }

    def gen_options(self, **kw):
        for k in kw:
            if k not in self.options:
                raise ValueError()

        if len(kw) == len(self.options):
            yield kw
        else:
            for k, v in sorted(self.options.iteritems()):
                if k in kw:
                    continue

                for opt_value in v:
                    kw[k] = opt_value
                    for option in self.gen_options(**kw):
                        yield option
                break

    def test_stats(self):
        for option in self.gen_options(stats = True):
            self.called = 0

            @memoize(**option)
            def func(*args, **kwargs):
                self.called += 1
                return True

            func(1, 2, 3, a = 1, b = 2)
            func(1, 2, 3, a = 1, b = 2)
            func(1, 2, 3, a = 1, b = 3)

            self.assertEqual(func.stats.calls, 3)
            if option['kw']:
                self.assertEqual(func.stats.miss, 2, option)
                self.assertEqual(self.called, 2, option)
            else:
                self.assertEqual(func.stats.miss, 1, option)
                self.assertEqual(self.called, 1, option)

    def test_until(self):
        # This test may fail if the computer is under load due to short cache timeouts
        for option in self.gen_options(until = lambda: time.time() + .01):
            self.called = 0

            @memoize(**option)
            def func(*args, **kwargs):
                self.called += 1

                return True

            func(1, 2, 3, a = 1, b = 2) # Called
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 3) # Cached (Called kw)
            time.sleep(.02)
            func(1, 2, 3, a = 1, b = 2) # Called
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 3) # Cached (Called kw)

            if option['kw']:
                self.assertEqual(self.called, 4, (self.called, option))
            else:
                self.assertEqual(self.called, 2, (self.called, option))

    def test_kw(self):
        for option in self.gen_options(kw = True):
            self.called = 0
            @memoize(**option)
            def func(*args, **kwargs):
                self.called += 1
                return True

            func(1, 2, 3, a = 1, b = 2) # Called
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 3) # Cached (Called kw)
            func(1, 2, 3, a = 1, b = 2) # Called
            func(1, 2, 3, a = 1, b = 2) # Cached
            func(1, 2, 3, a = 1, b = 3) # Cached (Called kw)

            self.assertEqual(self.called, 2)

    def test_ignore_nulls(self):
        for option in self.gen_options(ignore_nulls = True):
            self.called = 0
            @memoize(**option)
            def func(*args, **kwargs):
                self.called += 1
                return self.called if self.called >= 3 else None

            self.assertEqual(func(1,2,3, a = 1, b = 2), None) # Called = 1
            self.assertEqual(func(1,2,3, a = 1, b = 2), None) # Called = 2
            self.assertEqual(func(1,2,3, a = 1, b = 2), 3) # Called = 3
            self.assertEqual(func(1,2,3, a = 1, b = 2), 3) # Cached

            if option['kw']:
                self.assertEqual(func(1,2,3, a = 1, b = 3), 4) # Cached (Called kw)
                self.assertEqual(self.called, 4, (self.called, option))
            else:
                self.assertEqual(func(1,2,3, a = 1, b = 3), 3) # Cached (Called kw)
                self.assertEqual(self.called, 3, (self.called, option))

    def test_compiles_for_all_options(self):
        for option in self.gen_options():
            @memoize(**option)
            def func(*args, **kwargs):
                return True

            func(1, 2, 3, a = 1, b = 2)
            func(1, 2, 3, a = 1, b = 3)
            func(1, 2, 4, a = 1, b = 2)
            func(1, 2, 3, a = 1, b = 3)

    def test_memoize_results(self):
        @memoize()
        def func(*args, **kwargs):
            return True

        self.assertNotEqual(MemoizeResults.format_csv(), None)
        self.assertNotEqual(MemoizeResults.format_stats(), None)
