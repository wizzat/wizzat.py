import psycopg2
import unittest
import pghelper
from util import *
from pghelper import *
from testutil import *

# You probably want to define these in a centralized location somewhere like a models.py
class FooTable(DBTable):
    table_name = 'foo'
    key_field  = None
    conn       = None
    fields     = (
        'a',
        'b',
    )

class BarTable(DBTable):
    table_name = 'bar'
    key_field  = 'a'
    conn       = None
    fields     = (
        'a',
        'b',
        'c',
    )

class DBTableTest(unittest.TestCase, AssertSQLMixin):
    conn = None
    def setUp(self):
        super(DBTableTest, self).setUp()

        if not self.conn:
            self.conn = psycopg2.connect(
                host     = 'localhost',
                port     = 5432,
                user     = 'pyutil',
                password = 'pyutil',
                database = 'pyutil_testdb',
            )
            self.conn.autocommit = False
        FooTable.conn = self.conn
        BarTable.conn = self.conn

        execute(self.conn, "drop table if exists foo")
        execute(self.conn, "create table foo (a integer, b integer)")

        execute(self.conn, "drop table if exists bar")
        execute(self.conn, "create table bar (a integer primary key, b integer, c integer)")
        self.conn.commit()

    def tearDown(self):
        super(DBTableTest, self).tearDown()
        swallow(Exception, self.conn.close)
        FooTable.conn = None
        BarTable.conn = None
        del self.conn

    def test_find_by(self):
        execute(self.conn, "insert into foo (a, b) values (1, 2)")
        execute(self.conn, "insert into foo (a, b) values (1, 3)")
        execute(self.conn, "insert into foo (a, b) values (2, 3)")

        self.assertEqual(sorted([ x.get_dict() for x in FooTable.find_by(a = 1) ]), sorted([
            { 'a' : 1, 'b' : 2 },
            { 'a' : 1, 'b' : 3 },
        ]))

    def test_insert(self):
        f1 = FooTable(a = 1, b = 2).insert()
        f2 = FooTable(a = 1, b = 2).insert()
        f3 = FooTable(a = 1, b = 3).insert()
        f4 = FooTable(a = 2, b = 4).insert()

        self.assertEqual(f1._is_in_db, True)
        self.assertEqual(f2._is_in_db, True)
        self.assertEqual(f3._is_in_db, True)
        self.assertEqual(f4._is_in_db, True)

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM foo
            ORDER BY a, b
        """,
            [ 'a', 'b', ],
            [   1,   2, ],
            [   1,   2, ],
            [   1,   3, ],
            [   2,   4, ],
        )

    def test_insert__already_exists(self):
        with self.assertRaises(psycopg2.IntegrityError):
            f1 = BarTable(a = 1, b = 2, c = 3).insert()
            f2 = BarTable(a = 1, b = 2, c = 3).insert()
        self.conn.rollback()

        with self.assertRaises(psycopg2.IntegrityError):
            f1 = BarTable(a = 1, b = 2, c = 3).insert()
            f1.insert()
        self.conn.rollback()

    def test_update(self):
        f1 = FooTable(a = 1, b = 2).insert()
        f1.b = 3
        f1.update()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM foo
            ORDER BY a, b
        """,
            [ 'a', 'b', ],
            [   1,   3, ],
        )

    def test_update__does_not_exist(self):
        with self.assertRaises(AssertionError):
            f1 = FooTable(a = 1, b = 2)
            f1.update()

    def test_update__changes_primary_key_and_causes_integrity_violation(self):
        with self.assertRaises(psycopg2.IntegrityError):
            f1 = BarTable(a = 1, b = 2, c = 3).insert()
            f2 = BarTable(a = 2, b = 2, c = 3).insert()

            f2.a = f1.a
            f2.update()

    @skip_unfinished
    def test_lock_for_processing(self):
        pass
