import psycopg2
import unittest
from pyutil import pghelper

class ConnMgrTest(unittest.TestCase):
    connection_info = {
        'host'     : 'localhost',
        'port'     : 5432,
        'user'     : 'pyutil',
        'password' : 'pyutil',
        'database' : 'pyutil_testdb',
        'minconn'  : 0,
        'maxconn'  : 2,
    }

    def tearDown(self):
        super(ConnMgrTest, self).tearDown()

        while pghelper.ConnMgr.all_mgrs:
            pghelper.ConnMgr.all_mgrs.pop().closeall()

    def test_creation(self):
        mgr = pghelper.ConnMgr(**self.connection_info)
        self.assertEqual(mgr.connections, {})

    def test_setdefault(self):
        mgr1 = pghelper.ConnMgr(**self.connection_info)
        mgr2 = pghelper.ConnMgr(**self.connection_info)

        self.assertNotEqual(mgr1, mgr2)
        self.assertEqual(pghelper.ConnMgr.default(), None)

        mgr1.setdefault()
        self.assertEqual(pghelper.ConnMgr.default(), mgr1)
        mgr2.setdefault()
        self.assertEqual(pghelper.ConnMgr.default(), mgr2)

    def test_attribute_delegation(self):
        mgr = pghelper.ConnMgr(**self.connection_info)
        conn = mgr.getconn("abc")

        self.assertEqual(conn, mgr.abc)

        self.assertEqual(list(pghelper.fetch_results(mgr.abc, "select 1")), [[1]])

    def test_putconn_removes_attribute_delegation(self):
        mgr = pghelper.ConnMgr(**self.connection_info)
        conn = mgr.getconn("abc")
        mgr.putconn("abc")

        self.assertFalse(hasattr(mgr, 'abc'))

    def test_getconn_is_different_connections(self):
        mgr = pghelper.ConnMgr(**self.connection_info)
        mgr.getconn("a")
        mgr.getconn("b")

        pghelper.execute(mgr.a, "drop table if exists foobar")
        mgr.a.commit()

        pghelper.execute(mgr.a, "create table foobar (a integer)");
        self.assertEqual(pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(pghelper.table_exists(mgr.b, "foobar"), False)

    def test_commit(self):
        mgr = pghelper.ConnMgr(**self.connection_info)
        mgr.getconn("a")
        mgr.getconn("b")

        pghelper.execute(mgr.a, "drop table if exists foobar")
        pghelper.execute(mgr.a, "drop table if exists barfoo")
        mgr.a.commit()

        pghelper.execute(mgr.a, "create table foobar (a integer)")
        pghelper.execute(mgr.b, "create table barfoo (a integer)")
        self.assertEqual(pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(pghelper.table_exists(mgr.b, "foobar"), False)

        self.assertEqual(pghelper.table_exists(mgr.a, "barfoo"), False)
        self.assertEqual(pghelper.table_exists(mgr.b, "barfoo"), True)

        mgr.commit()

        self.assertEqual(pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(pghelper.table_exists(mgr.b, "foobar"), True)
        self.assertEqual(pghelper.table_exists(mgr.a, "barfoo"), True)
        self.assertEqual(pghelper.table_exists(mgr.b, "barfoo"), True)

    def test_maxconn(self):
        mgr = pghelper.ConnMgr(**self.connection_info)
        mgr.getconn("a")
        mgr.getconn("b")

        with self.assertRaises(psycopg2.pool.PoolError):
            mgr.getconn("c")

    def test_putconn_invalid_name(self):
        mgr = pghelper.ConnMgr(**self.connection_info)
        with self.assertRaises(KeyError):
            mgr.putconn("abc")
