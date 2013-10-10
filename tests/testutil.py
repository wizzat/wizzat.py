from testcase import PyUtilTestCase
from pyutil.testutil import *
from pyutil.util import *
import datetime, time

class TestUtil(PyUtilTestCase):
    def test_import_class(self):
        ospath = import_class('os.path')
        self.assertEqual(ospath.join('a', 'b', 'c'), 'a/b/c')

    def test_import_class__import_error(self):
        with self.assertRaises(ImportError):
            ospath = import_class('something.that.doesnt.exist')

    def test_swallow(self):
        def throw_ValueError(arg):
            raise ValueError()

        def throw_IndexError():
            raise IndexError()

        def normal_func():
            return 1

        with self.assertRaises(IndexError):
            swallow(ValueError, throw_IndexError)

        with self.assertRaises(ValueError):
            swallow(IndexError, throw_ValueError, 1)

        swallow(ValueError, throw_ValueError, 1)
        swallow(IndexError, throw_IndexError)
        swallow(IndexError, normal_func)

    def test_merge_dicts(self):
        merged = merge_dicts(
            { 'a' : 'b' },
            { 'a' : 'c' },
            { 'b' : 'd' },
        )

        self.assertEqual(merged, {
            'a' : 'c',
            'b' : 'd',
        })

    def test_set_defaults__kwargs(self):
        def func(**kwargs):
            return set_defaults(kwargs,
                a = 1,
                b = 2,
                c = 3,
            )

        self.assertEqual(func(a = 2, c = 4), {
            'a' : 2,
            'b' : 2,
            'c' : 4,
        })

    def test_set_defaults__real_dicts_dont_get_overriden(self):
        real_dict = {
            'a' : 1,
            'b' : 2,
            'c' : 3,
        }
        def func(**kwargs):
            return set_defaults(kwargs, real_dict)

        self.assertEqual(func(a = 2, c = 4), {
            'a' : 2,
            'b' : 2,
            'c' : 4,
        })

        self.assertEqual(real_dict, {
            'a' : 1,
            'b' : 2,
            'c' : 3,
        })

    def test_chunks(self):
        iterable = range(100)

        all_chunks = []
        for chunk in chunks(iterable, 10):
            all_chunks.append(chunk)

        self.assertEqual(all_chunks, [
            range( 0,  10),
            range(10,  20),
            range(20,  30),
            range(30,  40),
            range(40,  50),
            range(50,  60),
            range(60,  70),
            range(70,  80),
            range(80,  90),
            range(90, 100),
        ])
