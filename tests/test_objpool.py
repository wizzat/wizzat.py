from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

from wizzat.objpool import *
from wizzat.testutil import *

class ObjPoolTest(TestCase):
    def test_min_objs(self):
        pool = ObjPool(min_objs = 5, max_objs = 5)
        self.assertEqual(len(pool.ready), 5)
        self.assertEqual(len(pool.in_use), 0)

    def test_invalid_boundaries(self):
        with self.assertRaises(ValueError):
            ObjPool(min_objs = 5, max_objs = 1)

    def test_obj_exhaustion(self):
        pool = ObjPool(max_objs = 2)
        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 0)

        obj1 = pool.new_obj()
        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 1)

        obj2 = pool.new_obj()
        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 2)

        with self.assertRaises(ObjPoolExhausted):
            pool.new_obj()

        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 2)

    def test_yield_multiple_times(self):
        pool = ObjPool()
        obj = pool.new_obj()
        pool.yield_obj(obj)

        with self.assertRaises(ObjPoolOwnershipError):
            pool.yield_obj(obj)

    def test_custom_put_func(self):
        class F(object):
            putted = False

        class Pool(ObjPool):
            def new_func(self):
                return F()

            def put_func(self, obj):
                obj.putted = True

        pool = Pool(max_objs = 2)

        obj = pool.new_obj()
        self.assertEqual(obj.putted, False)

        pool.yield_obj(obj)
        self.assertEqual(obj.putted, True)

    def test_name(self):
        pool = ObjPool()
        obj1 = pool.name('abc')
        obj2 = pool.name('abc')
        self.assertIs(pool.abc, obj1)
        self.assertIs(obj1, obj2)

    def test_naming_with_old_references(self):
        pool = ObjPool()
        obj = pool.new_obj()
        pool.yield_obj(obj)

        # You can't hold onto references like that.
        with self.assertRaises(ObjPoolOwnershipError):
            pool.name('abc', obj)

    def test_naming_picks_up_from_ready_queue(self):
        pool = ObjPool()
        obj1 = pool.new_obj()
        pool.yield_obj(obj1)
        obj2 = pool.name('abc')

        self.assertIs(obj1, obj2)

    def test_naming_with_objs_that_already_exist(self):
        pool = ObjPool()
        obj1 = pool.new_obj()
        obj2 = pool.name('abc', obj1)

        self.assertIs(obj1, obj2)

    def test_unname(self):
        pool = ObjPool()
        obj = pool.name('abc')

        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 1)

        pool.unname('abc')

        self.assertEqual(len(pool.ready), 1)
        self.assertEqual(len(pool.in_use), 0)

    def test_unname__invalid_name(self):
        pool = ObjPool()
        obj = pool.new_obj()

        with self.assertRaises(ObjPoolNameError):
            pool.unname('abc')

    def test_obj_from_pool(self):
        pool = ObjPool()
        with pool.obj_from_pool() as obj:
            self.assertEqual(len(pool.ready), 0)
            self.assertEqual(len(pool.in_use), 1)

        self.assertEqual(len(pool.ready), 1)
        self.assertEqual(len(pool.in_use), 0)

    def test_yield_all(self):
        pool = ObjPool(max_objs = 2)
        obj1 = pool.new_obj()
        obj2 = pool.name('abc')

        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 2)

        pool.yield_all()
        self.assertEqual(len(pool.ready), 2)
        self.assertEqual(len(pool.in_use), 0)
        self.assertFalse(hasattr(pool, 'abc'))

    def test_foreach(self):
        class F(object):
            foreached = False

        class Pool(ObjPool):
            def new_func(self):
                return F()

        def f(obj):
            obj.foreached = True

        pool = Pool(max_objs = 2)

        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 0)

        pool.foreach(f)

        self.assertEqual(len(pool.ready), 0)
        self.assertEqual(len(pool.in_use), 0)

        obj1 = pool.new_obj()
        obj2 = pool.new_obj()
        pool.yield_obj(obj2)

        pool.foreach(f)

        self.assertTrue(obj1.foreached)
        self.assertTrue(obj2.foreached)

    def test_foreach__not_in_use(self):
        class F(object):
            foreached = False

        class Pool(ObjPool):
            def new_func(self):
                return F()

        def f(obj):
            obj.foreached = True

        pool = Pool(max_objs = 2)

        obj1 = pool.new_obj()
        obj2 = pool.new_obj()
        pool.yield_obj(obj2)

        pool.foreach(f, in_use = False)

        self.assertFalse(obj1.foreached)
        self.assertTrue(obj2.foreached)
