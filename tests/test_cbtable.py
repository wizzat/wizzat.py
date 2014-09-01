import couchbase
from pyutil.cbtable import *
from pyutil.testutil import *
from pyutil.util import *
from testcase import DBTestCase

class CBTableTest(DBTestCase):
    def setUp(self):
        self.conn = couchbase.Couchbase().connect(
            host    = 'localhost',
            port    = 8091,
            bucket  = 'default',
            timeout = 5.0,
        )

        self.conn.delete('tbl/1/2', quiet=True)

    def new_subclass(self, keys = None, data_fields = None, memoize_cls = False, tbl_name = 'tbl'):
        class C(CBTable):
            table_name = tbl_name
            memoize    = memoize_cls
            conn       = self.conn
            key_fields = keys or [
                'key1',
                'key2',
            ]

            fields = data_fields or [
                'key1',
                'key2',
                'data1',
                'data2',
            ]

            def default_data2(self):
                return self.key1 + self.key2

        return C

    def test_add(self):
        cls = self.new_subclass()
        self.assertEqual(cls.find_by_key(1, 2), None)
        expected_data = {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'abc',
            'data2' : 'def',
        }

        self.conn.set('tbl/1/2', expected_data)

        with self.assertRaises(couchbase.exceptions.KeyExistsError):
            cls.create(1,2,
                data1 = 'abc',
                data2 = 'def',
            )

    def test_checks_cas_values(self):
        cls = self.new_subclass()
        self.assertEqual(cls.find_by_key(1, 2), None)
        expected_data = {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'abc',
            'data2' : 'def',
        }

        obj = cls.find_or_create(1,2)
        self.conn.set('tbl/1/2', expected_data)

        obj.data1 = 'ha ha ha'

        with self.assertRaises(couchbase.exceptions.KeyExistsError):
            obj.update()

    def test_find_by_key(self):
        cls = self.new_subclass()
        self.assertEqual(cls.find_by_key(1, 2), None)
        expected_data = {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'abc',
            'data2' : 'def',
        }

        self.conn.set('tbl/1/2', expected_data)

        self.assertEqual(cls.find_by_key(1, 2)._data, expected_data)

    def test_find_or_create(self):
        cls = self.new_subclass()

        obj = cls.find_or_create(1,2)

        self.assertEqual(obj._key, 'tbl/1/2')
        self.assertJSONEqual(obj._data, {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : None,
            'data2' : 3,
        })

        self.assertEqual(obj.key1, 1)
        self.assertEqual(obj.key2, 2)
        self.assertEqual(obj.data1, None)
        self.assertEqual(obj.data2, 3)
        self.assertEqual(obj._changed, False)

    def test_memoize(self):
        cls = self.new_subclass(memoize_cls = True)
        obj1 = cls.find_or_create(1,2,
            data1 = 'abc',
            data2 = 'def',
        )

        expected_data = {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'abc',
            'data2' : 'def',
        }

        self.assertJSONEqual(cls.key_cache['tbl/1/2']._data, expected_data)
        self.assertEqual(cls.key_cache.keys(), [ 'tbl/1/2' ])

        obj2 = cls.find_or_create(1,2,
            data1 = 'abc',
            data2 = 'def',
        )

        self.assertTrue(obj1 is obj2)
        cls.clear_cache()

        self.assertEqual(cls.key_cache.keys(), [])

        obj3 = cls.find_or_create(1,2,
            data1 = 'abc',
            data2 = 'def',
        )

        self.assertTrue(obj1 is not obj3)
