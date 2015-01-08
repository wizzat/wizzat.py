from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime, time, os, uuid
from wizzat.testutil import *
from wizzat.util import *

class TestUtil(TestCase):
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

    def test_update_env(self):
        self.assertTrue('ABRA' not in os.environ)
        self.assertTrue('CAD' not in os.environ)
        self.assertTrue('PWD' in os.environ)

        with update_env(ABRA = 'abc', CAD = 'def', PWD='bar'):
            self.assertEqual(os.environ['ABRA'], 'abc')
            self.assertEqual(os.environ['CAD'], 'def')
            self.assertEqual(os.environ['PWD'], 'bar')

        self.assertTrue('ABRA' not in os.environ)
        self.assertTrue('CAD' not in os.environ)
        self.assertNotEqual(os.environ['PWD'], 'bar')

    def test_tmpdir(self):
        with tmpdir() as d:
            self.assertTrue(os.path.exists(d))
            self.assertTrue(os.path.isdir(d))

        self.assertFalse(os.path.exists(d))

    def test_invert_dict(self):
        self.assertEqual(invert_dict({ 1 : 2, 2 : 2, 3 : 2 }, many=True), { 2 : [ 1, 2, 3 ] })
        self.assertEqual(invert_dict({ 1 : 2, 3 : 4 }), { 2 : 1, 4 : 3 })

        # Undefined behavior from invert_dict when multiple keys point to same value and many=False
        self.assertTrue(invert_dict({ 1 : 2, 2 : 2, 3 : 2 }) in [
            { 2 : 1 },
            { 2 : 2 },
            { 2 : 3 },
        ])

    def test_first_existing_path__ignores_paths_that_do_not_exist(self):
        doesnt_exist1 = str(uuid.uuid4())
        doesnt_exist2 = str(uuid.uuid4())

        path = first_existing_path(doesnt_exist1, doesnt_exist2, __file__, 'testdateutil.py')
        self.assertEqual(path, __file__)

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

    def test_set_strict_defaults__kwargs(self):
        self.assertEqual({ 'a' : 1 }, set_strict_defaults({}, a = 1))
        self.assertEqual({ 'a' : 2 }, set_strict_defaults({ 'a' : 2 }, a = 1))

        with self.assertRaises(TypeError):
            set_strict_defaults({'d' : 4}, { 'a' : 1 })

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

    def test_filter_keys(self):
        self.assertEqual(filter_keys([ 'a', 'b' ], { 'a' : 1, 'b' : 2, 'c' : 3 }), {
            'a' : 1,
            'b' : 2,
        })

        self.assertEqual(filter_keys([ 'a', 'b' ], { 'a' : 1, 'b' : 2, 'c' : 3 }, True), {
            'a' : 1,
            'b' : 2,
        })

        self.assertEqual(filter_keys([ 'a', 'b' ], { 'a' : 1, 'b' : 2, 'c' : 3 }, False), {
            'a' : 1,
            'b' : 2,
        })

        self.assertEqual(filter_keys([ 'a', 'b', 'asdf' ], { 'a' : 1, 'b' : 2, 'c' : 3 }, False), {
            'a' : 1,
            'b' : 2,
        })

        with self.assertRaises(KeyError):
            self.assertEqual(filter_keys([ 'a', 'b', 'asdf' ], { 'a' : 1, 'b' : 2, 'c' : 3 }), {
                'a' : 1,
                'b' : 2,
            })

    def test_unique(self):
        self.assertEqual(unique([ 'a', 'b', 'b', 'c', 'd', 'a' ]), [ 'a', 'b', 'c', 'd' ])
        self.assertEqual(unique(list(reversed(range(5))) + range(6)), [ 4, 3, 2, 1, 0, 5 ])

        with self.assertRaises(TypeError):
            self.assertEqual(unique([ {'a' : 1}, {'a' : 2} ]), [ 'a', 'b', 'c', 'd' ])

    def test_json_copy(self):
        obj = {
            'key1' : '123',
            'key2' : {
                'inner_key1' : {
                    'inner_inner_key1' : '123',
                    'inner_inner_key2' : '123',
                }
            },
            'key3' : [
                { 'inner_key1' : '123' },
                '123',
            ]
        }

        new_obj = json_copy(obj)
        self.assertFalse(new_obj is obj)
        self.assertFalse(new_obj['key2'] is obj['key2'])
        self.assertFalse(new_obj['key2']['inner_key1'] is obj['key2']['inner_key1'])
        self.assertFalse(new_obj['key3'] is obj['key3'])
        self.assertFalse(new_obj['key3'][0] is obj['key3'][0])

        # The data is unchanged
        self.assertTrue(new_obj['key2']['inner_key1']['inner_inner_key1'] is obj['key2']['inner_key1']['inner_inner_key1'])
        self.assertTrue(new_obj['key2']['inner_key1']['inner_inner_key2'] is obj['key2']['inner_key1']['inner_inner_key2'])
        self.assertTrue(new_obj['key3'][0]['inner_key1'] is obj['key3'][0]['inner_key1'])
