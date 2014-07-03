import pyutil.pghelper
from pyutil.testutil import *
from testcase import DBTestCase

class ConnMgrTest(DBTestCase):
    def test_creation(self):
        mgr = pyutil.pghelper.ConnMgr(**self.db_info)
        self.assertEqual(mgr.connections, {})

    def test_setdefault(self):
        mgr1 = pyutil.pghelper.ConnMgr(**self.db_info)
        mgr2 = pyutil.pghelper.ConnMgr(**self.db_info)

        self.assertNotEqual(mgr1, mgr2)
        self.assertEqual(pyutil.pghelper.ConnMgr.default(), self.db_mgr)

        mgr1.setdefault()
        self.assertEqual(pyutil.pghelper.ConnMgr.default(), mgr1)
        mgr2.setdefault()
        self.assertEqual(pyutil.pghelper.ConnMgr.default(), mgr2)

    def test_attribute_delegation(self):
        mgr = pyutil.pghelper.ConnMgr(**self.db_info)
        conn = mgr.getconn("abc")

        self.assertEqual(conn, mgr.abc)

        self.assertEqual(list(pyutil.pghelper.fetch_results(mgr.abc, "select 1")), [[1]])

    def test_putconn_removes_attribute_delegation(self):
        mgr = pyutil.pghelper.ConnMgr(**self.db_info)
        conn = mgr.getconn("abc")
        mgr.putconn("abc")

        self.assertFalse(hasattr(mgr, 'abc'))

    def test_getconn_is_different_connections(self):
        mgr = pyutil.pghelper.ConnMgr(**self.db_info)
        mgr.getconn("a")
        mgr.getconn("b")

        pyutil.pghelper.execute(mgr.a, "drop table if exists foobar")
        mgr.a.commit()

        pyutil.pghelper.execute(mgr.a, "create table foobar (a integer)");
        self.assertEqual(pyutil.pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(pyutil.pghelper.table_exists(mgr.b, "foobar"), False)

    def test_commit(self):
        mgr = pyutil.pghelper.ConnMgr(**self.db_info)
        mgr.getconn("a")
        mgr.getconn("b")

        pyutil.pghelper.execute(mgr.a, "drop table if exists foobar")
        pyutil.pghelper.execute(mgr.a, "drop table if exists barfoo")
        mgr.a.commit()

        pyutil.pghelper.execute(mgr.a, "create table foobar (a integer)")
        pyutil.pghelper.execute(mgr.b, "create table barfoo (a integer)")
        self.assertEqual(pyutil.pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(pyutil.pghelper.table_exists(mgr.b, "foobar"), False)

        self.assertEqual(pyutil.pghelper.table_exists(mgr.a, "barfoo"), False)
        self.assertEqual(pyutil.pghelper.table_exists(mgr.b, "barfoo"), True)

        mgr.commit()

        self.assertEqual(pyutil.pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(pyutil.pghelper.table_exists(mgr.b, "foobar"), True)
        self.assertEqual(pyutil.pghelper.table_exists(mgr.a, "barfoo"), True)
        self.assertEqual(pyutil.pghelper.table_exists(mgr.b, "barfoo"), True)

    def test_maxconn(self):
        mgr = pyutil.pghelper.ConnMgr(**self.db_info)
        mgr.getconn("a")
        mgr.getconn("b")
        mgr.getconn("c")

        with self.assertRaises(pyutil.pghelper.PgPoolError):
            mgr.getconn("d")

    def test_putconn_invalid_name(self):
        mgr = pyutil.pghelper.ConnMgr(**self.db_info)
        with self.assertRaises(KeyError):
            mgr.putconn("abc")
