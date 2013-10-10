from testcase import PyUtilTestCase
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.util import *
import psycopg2

# In some kind of project, you would probably want to define these in a centralized location, such as models.py
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

class DBTableTest(AssertSQLMixin, PyUtilTestCase):
    def setUp(self):
        super(DBTableTest, self).setUp()
        FooTable.conn = self.conn
        BarTable.conn = self.conn

        execute(self.conn, "DROP TABLE IF EXISTS foo")
        execute(self.conn, "CREATE TABLE foo (a INTEGER, b INTEGER DEFAULT 1)")

        execute(self.conn, "DROP TABLE IF EXISTS bar")
        execute(self.conn, "CREATE TABLE bar (a INTEGER PRIMARY KEY, b INTEGER, c INTEGER)")
        self.conn.commit()

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

    def test_insert_default_values(self):
        f1 = FooTable(a = 1).update()
        self.assertEqual(f1.b, 1)

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

    def test_delete__deletes_all_rows_that_look_like_the_deleted_row(self):
        f1 = FooTable(a = 1, b = 2).update()
        f2 = FooTable(a = 1, b = 2).update()
        self.assertEqual(len(f1.delete()), 2)
        f1.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM foo
            ORDER BY a, b
        """,
            [ 'a', 'b', ],
        )

    def test_deletes_based_on_db_fields(self):
        f1 = FooTable(a = 1, b = 2).update()
        f2 = FooTable(a = 1, b = 2).update()
        f1.b = 3

        self.assertEqual(len(f1.delete()), 2)
        f1.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM foo
            ORDER BY a, b
        """,
            [ 'a', 'b', ],
        )

    def test_deletes_based_on_primary_key_and_db_fields(self):
        f1 = BarTable(a = 1, b = 2).update()
        f2 = BarTable(a = 2, b = 3).update()
        f1.a = 2
        f1.delete()
        f1.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM bar
            ORDER BY a, b
        """,
            [ 'a',  'b',  ],
            [ 2,    3,    ],
        )

    def test_delete_does_not_error_if_not_in_db(self):
        f1 = FooTable(a = 1, b = 2)
        self.assertEqual(f1.delete(), [])

    def test_rowlock(self):
        f1 = BarTable(a = 1, b = 2, c = 3).update()
        f2 = BarTable(a = 2, b = 2, c = 3).update()
        f3 = BarTable(a = 3, b = 2, c = 3).update()
        self.conn.commit()

        f1.rowlock()

        with self.assertRaises(psycopg2.OperationalError):
            execute(self.mgr.getconn("conn2"), "select * from bar for update nowait")
