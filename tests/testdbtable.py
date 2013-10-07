import psycopg2
import unittest
from pyutil.util import *
from pyutil.pghelper import *
from pyutil.testutil import *

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
    conn2 = None
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

        if not self.conn2:
            self.conn2 = psycopg2.connect(
                host     = 'localhost',
                port     = 5432,
                user     = 'pyutil',
                password = 'pyutil',
                database = 'pyutil_testdb',
            )
            self.conn2.autocommit = False


        FooTable.conn = self.conn
        BarTable.conn = self.conn

        execute(self.conn, "DROP TABLE IF EXISTS foo")
        execute(self.conn, "CREATE TABLE foo (a INTEGER, b INTEGER)")

        execute(self.conn, "DROP TABLE IF EXISTS bar")
        execute(self.conn, "CREATE TABLE bar (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER)")
        self.conn.commit()
        self.conn2.commit()

    def tearDown(self):
        super(DBTableTest, self).tearDown()
        swallow(Exception, self.conn.close)
        swallow(Exception, self.conn2.close)
        FooTable.conn = None
        BarTable.conn = None
        del self.conn
        del self.conn2

    def test_find_by(self):
        execute(self.conn, "INSERT INTO foo (a, b) VALUES (1, 2)")
        execute(self.conn, "INSERT INTO foo (a, b) VALUES (1, 3)")
        execute(self.conn, "INSERT INTO foo (a, b) VALUES (2, 3)")

        self.assertEqual(sorted([ x.get_dict() for x in FooTable.find_by(a = 1) ]), sorted([
            { 'a' : 1, 'b' : 2 },
            { 'a' : 1, 'b' : 3 },
        ]))

    def test_insert(self):
        f1 = FooTable(a = 1, b = 2).update()
        f2 = FooTable(a = 1, b = 2).update()
        f3 = FooTable(a = 1, b = 3).update()
        f4 = FooTable(a = 2, b = 4).update()

        self.assertEqual(f1.db_fields, { "a" : 1, "b" : 2, })
        self.assertEqual(f2.db_fields, { "a" : 1, "b" : 2, })
        self.assertEqual(f3.db_fields, { "a" : 1, "b" : 3, })
        self.assertEqual(f4.db_fields, { "a" : 2, "b" : 4, })

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
            f1 = BarTable(a = 1, b = 2, c = 3).update()
            f2 = BarTable(a = 1, b = 2, c = 3).update()
        self.conn.rollback()

    def test_update(self):
        f1 = FooTable(a = 1, b = 2).update()
        f2 = FooTable(a = 1, b = 3).update()
        f3 = FooTable(a = 2, b = 4).update()
        f1.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM foo
            ORDER BY a, b
        """,
            [ 'a', 'b', ],
            [  1,   2,  ],
            [  1,   3,  ],
            [  2,   4,  ],
        )

        self.sql = None
        def log_func(sql):
            self.sql = sql

        f1.b = 3
        f1.update()
        f1.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM foo
            ORDER BY a, b
        """,
            [ 'a', 'b', ],
            [  1,   3,  ],
            [  1,   3,  ],
            [  2,   4,  ],
        )


    def test_update__inserts_when_not_in_database(self):
        f1 = FooTable(a = 1, b = 2)
        f1.update()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM foo
            ORDER BY a, b
        """,
            [ 'a', 'b', ],
            [  1,   2,  ],
        )

    def test_update__catches_primary_key_changes(self):
        with self.assertRaises(AssertionError):
            f1 = BarTable(a = 1, b = 2, c = 3).update()
            f2 = BarTable(a = 2, b = 2, c = 3).update()

            f2.a = f1.a
            f2.update()

    def test_lock_for_processing(self):
        f1 = BarTable(a = 1, b = 2, c = 3).update()
        f2 = BarTable(a = 2, b = 2, c = 3).update()
        f3 = BarTable(a = 3, b = 2, c = 3).update()
        self.conn.commit()

        f1.lock_for_processing()

        with self.assertRaises(psycopg2.OperationalError):
            execute(self.conn2, "select * from bar for update nowait")
