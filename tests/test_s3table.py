import boto, boto.exception, boto.s3, boto.s3.key, json, time
from pyutil.s3table import *
from pyutil.testutil import *
from pyutil.util import *
from pyutil.decorators import *
from testcase import DBTestCase

class S3TableTest(DBTestCase):
    @skip_unless_env('TEST_S3')
    def setUp(self):
        data = load_json_paths('~/.test_s3.cfg')
        self.s3_conn = boto.connect_s3(
            data['s3_access_key'],
            data['s3_secret_key'],
        )

        self.bucket_name = data['s3.default_bucket']
        self.s3_bucket = self.s3_conn.get_bucket(self.bucket_name)
        self.purge_key('tbl/1/2')
        self.purge_key('tbl/1/3')
        self.purge_key('tbl/1/4')

    def purge_key(self, key):
        try:
            self.s3_bucket.delete_key(key)
        except boto.exception.S3ResponseError:
            pass


    def new_subclass(self, keys = None, data_fields = None, memoize_cls = False, tbl_name = 'tbl'):
        class C(S3Table):
            table_name = tbl_name
            memoize    = memoize_cls
            conn       = self.s3_conn
            bucket     = self.bucket_name
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

    def test_find_by_key(self):
        cls = self.new_subclass()
        self.assertEqual(cls.find_by_key(1, 2), None)
        expected_data = {
            'key1'  : 1,
            'key2'  : 2,
            'data1' : 'abc',
            'data2' : 'def',
        }

        boto.s3.key.Key(self.s3_bucket, 'tbl/1/2').set_contents_from_string(json.dumps(expected_data))
        time.sleep(.25) # S3 is eventually consistent

        self.assertEqual(cls.find_by_key(1, 2)._data, expected_data)

    def test_find_or_create(self):
        cls = self.new_subclass()

        obj = cls.find_or_create(1,3)

        self.assertEqual(obj._key, 'tbl/1/3')
        self.assertJSONEqual(obj._data, {
            'key1'  : 1,
            'key2'  : 3,
            'data1' : None,
            'data2' : 4,
        })

        self.assertEqual(obj.key1, 1)
        self.assertEqual(obj.key2, 3)
        self.assertEqual(obj.data1, None)
        self.assertEqual(obj.data2, 4)
        self.assertEqual(obj._changed, False)

    def test_memoize(self):
        cls = self.new_subclass(memoize_cls = True)
        obj1 = cls.find_or_create(1,4,
            data1 = 'abc',
            data2 = 'def',
        )

        expected_data = {
            'key1'  : 1,
            'key2'  : 4,
            'data1' : 'abc',
            'data2' : 'def',
        }

        self.assertJSONEqual(cls.key_cache['tbl/1/4']._data, expected_data)
        self.assertEqual(cls.key_cache.keys(), [ 'tbl/1/4' ])

        obj2 = cls.find_or_create(1,4,
            data1 = 'abc',
            data2 = 'def',
        )

        self.assertTrue(obj1 is obj2)
        cls.clear_cache()

        self.assertEqual(cls.key_cache.keys(), [])

        obj3 = cls.find_or_create(1,4,
            data1 = 'abc',
            data2 = 'def',
        )

        self.assertTrue(obj1 is not obj3)
