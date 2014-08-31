from pyutil.kvtable import *
from pyutil.testutil import *
from pyutil.util import *
from testcase import DBTestCase


class KVTableTest(DBTestCase):
    def new_subclass(self, keys = None, data_fields = None, memoize_cls = False, tbl_name = 'tbl'):
        class DictKVTable(KVTable):
            table_name = tbl_name
            memoize    = memoize_cls
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

            kv_store = {}

            @classmethod
            def _find_by_key(cls, key):
                data = cls.kv_store.get(key, None)

                if data:
                    return True, data
                else:
                    return False, None

            def _insert(self, force=False):
                self.kv_store[self._key] = self._data

            def _update(self, force=False):
                self.kv_store[self._key] = self._data

            def _delete(self, force=False):
                del self.kv_store[self._key]

            def default_data2(self):
                return self.key1 + self.key2

        return DictKVTable

    def test_key_func(self):
        cls = self.new_subclass()
        self.assertEqual(cls.key_func([ 1, 2 ]), 'tbl/1/2')
        self.assertEqual(cls.key_func([ 1, 2, 3 ]), 'tbl/1/2')

        with self.assertRaises(ValueError):
            cls.key_func([ 1, ])

    def test_keys_must_be_fields(self):
        with self.assertRaises(KVTableConfigError):
            cls = self.new_subclass([ 'key', ], [ 'data' ])

    def test_key_types(self):
        with self.assertRaises(KVTableConfigError):
            cls = self.new_subclass({ 'key' : 'abc' })

    def test_data_types(self):
        with self.assertRaises(KVTableConfigError):
            cls = self.new_subclass(None, { 'data' : 'abc' })

    def test_table_name_data_type(self):
        with self.assertRaises(KVTableConfigError):
            cls = self.new_subclass(tbl_name = None)

        with self.assertRaises(KVTableConfigError):
            cls = self.new_subclass(tbl_name = { 'abc' })

        with self.assertRaises(KVTableConfigError):
            cls = self.new_subclass(tbl_name = 1)

    def test_find_by_key(self):
        cls = self.new_subclass()
        self.assertEqual(cls.find_by_key(1, 2), None)

        cls.kv_store['tbl/1/2'] = {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'abc',
            'data2' : 'def',
        }

        self.assertEqual(cls.find_by_key(1, 2)._data, cls.kv_store['tbl/1/2'])

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

    def test_changed_on_update(self):
        cls = self.new_subclass()
        obj = cls.find_or_create(1,2, data1 = 'abc')
        obj.data1 = 'abc'
        self.assertEqual(obj._changed, False)
        obj.data1 = 'def'
        self.assertEqual(obj._changed, True)
        obj.update()

        self.assertEqual(obj.kv_store['tbl/1/2'], {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'def',
            'data2' : 3,
        })

    def test_update__rejects_key_changes(self):
        cls = self.new_subclass()
        obj = cls.find_or_create(1,2, data1 = 'abc')
        with self.assertRaises(KVTableImmutableFieldError):
            obj.key1 = 2

    def test_memoize(self):
        cls = self.new_subclass(memoize_cls = True)
        data = {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'abc',
            'data2' : 'def',
        }

        cls.kv_store['tbl/1/2'] = data

        self.assertEqual(cls.find_by_key(1, 2)._data, data)
        self.assertEqual(cls.key_cache['tbl/1/2']._data, data)
        self.assertEqual(cls.key_cache.keys(), [ 'tbl/1/2' ])
        cls.kv_store.clear()

        self.assertEqual(cls.find_by_key(1, 2)._data, data)
