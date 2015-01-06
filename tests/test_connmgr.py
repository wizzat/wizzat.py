from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import wizzat.pghelper
from wizzat.testutil import *
from testcase import DBTestCase

class ConnMgrTest(DBTestCase):
    def test_creation(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        self.assertEqual(mgr.connections, {})

    def test_setdefault(self):
        mgr1 = wizzat.pghelper.ConnMgr(**self.db_info)
        mgr2 = wizzat.pghelper.ConnMgr(**self.db_info)

        self.assertNotEqual(mgr1, mgr2)
        self.assertEqual(wizzat.pghelper.ConnMgr.default(), self.db_mgr)

        mgr1.setdefault()
        self.assertEqual(wizzat.pghelper.ConnMgr.default(), mgr1)
        mgr2.setdefault()
        self.assertEqual(wizzat.pghelper.ConnMgr.default(), mgr2)

    def test_attribute_delegation(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        conn = mgr.getconn("abc")

        self.assertEqual(conn, mgr.abc)

        self.assertEqual(list(wizzat.pghelper.fetch_results(mgr.abc, "select 1")), [[1]])

    def test_putconn_removes_attribute_delegation(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        conn = mgr.getconn("abc")
        mgr.putconn("abc")

        self.assertFalse(hasattr(mgr, 'abc'))

    def test_getconn_is_different_connections(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        mgr.getconn("a")
        mgr.getconn("b")

        wizzat.pghelper.execute(mgr.a, "drop table if exists foobar")
        mgr.a.commit()

        wizzat.pghelper.execute(mgr.a, "create table foobar (a integer)");
        self.assertEqual(wizzat.pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(wizzat.pghelper.table_exists(mgr.b, "foobar"), False)

    def test_str_and_unicode_are_same_connection(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        c1 = mgr.getconn(str("a"))
        c2 = mgr.getconn(unicode("a"))

        self.assertTrue(c1 is c2)

    def test_commit(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        mgr.getconn("a")
        mgr.getconn("b")

        wizzat.pghelper.execute(mgr.a, "drop table if exists foobar")
        wizzat.pghelper.execute(mgr.a, "drop table if exists barfoo")
        mgr.a.commit()

        wizzat.pghelper.execute(mgr.a, "create table foobar (a integer)")
        wizzat.pghelper.execute(mgr.b, "create table barfoo (a integer)")
        self.assertEqual(wizzat.pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(wizzat.pghelper.table_exists(mgr.b, "foobar"), False)

        self.assertEqual(wizzat.pghelper.table_exists(mgr.a, "barfoo"), False)
        self.assertEqual(wizzat.pghelper.table_exists(mgr.b, "barfoo"), True)

        mgr.commit()

        self.assertEqual(wizzat.pghelper.table_exists(mgr.a, "foobar"), True)
        self.assertEqual(wizzat.pghelper.table_exists(mgr.b, "foobar"), True)
        self.assertEqual(wizzat.pghelper.table_exists(mgr.a, "barfoo"), True)
        self.assertEqual(wizzat.pghelper.table_exists(mgr.b, "barfoo"), True)

    def test_maxconn(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        mgr.getconn("a")
        mgr.getconn("b")
        mgr.getconn("c")

        with self.assertRaises(wizzat.pghelper.PgPoolError):
            mgr.getconn("d")

    def test_putconn_invalid_name(self):
        mgr = wizzat.pghelper.ConnMgr(**self.db_info)
        with self.assertRaises(KeyError):
            mgr.putconn("abc")
