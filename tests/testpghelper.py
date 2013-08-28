import psycopg2
import unittest
import pghelper

class PgHelperTest(unittest.TestCase):
    connection_info = {
        'host'     : 'localhost',
        'port'     : 5432,
        'user'     : 'pyutil',
        'password' : 'pyutil',
        'database' : 'pyutil_testdb',
    }

    def test_connection(self):
        with psycopg2.connect(**self.connection_info) as conn:
            pass

    def test_execute(self):
        with psycopg2.connect(**self.connection_info) as conn:
            results = [ x for x in pghelper.execute(conn, "SELECT 1 AS foobar") ]
            self.assertEqual(results[0]['foobar'], 1)
            self.assertEqual(results[0][0], 1)

    def test_iteration(self):
        with psycopg2.connect(**self.connection_info) as conn:
            result = pghelper.execute(conn, """
                SELECT 1 AS foobar
                UNION ALL
                SELECT 2 AS foobar
                UNION ALL
                SELECT 3 AS foobar
                UNION ALL
                SELECT 4 AS foobar
            """)

            self.assertEqual(result.next()["foobar"], 1)
            self.assertEqual(result.next()["foobar"], 2)
            self.assertEqual(result.next()["foobar"], 3)
            self.assertEqual(result.next()["foobar"], 4)

    def test_fetch_result_rows(self):
        with psycopg2.connect(**self.connection_info) as conn:
            result1 = pghelper.fetch_result_rows(conn, """
                SELECT 1 AS foobar
                UNION ALL
                SELECT 2 AS foobar
                UNION ALL
                SELECT 3 AS foobar
                UNION ALL
                SELECT 4 AS foobar
            """)

            result2 = pghelper.fetch_result_rows(conn, """
                SELECT 2 AS foobar
                UNION ALL
                SELECT 4 AS foobar
                UNION ALL
                SELECT 6 AS foobar
                UNION ALL
                SELECT 8 AS foobar
            """)

            # We would expect accessing result1 to fail if it isn't cached.
            self.assertEqual([ x["foobar"] for x in result1 ], [ 1, 2, 3, 4 ])
            self.assertEqual([ x["foobar"] for x in result2 ], [ 2, 4, 6, 8 ])

    def test_table_exists(self):
        with psycopg2.connect(**self.connection_info) as conn:
            pghelper.execute(conn, "drop table if exists foobar")
            self.assertEqual(pghelper.table_exists(conn, "foobar"), False);
            conn.commit()

            pghelper.execute(conn, "create table foobar (a integer)")
            conn.commit()
            self.assertEqual(pghelper.table_exists(conn, "foobar"), True);

if __name__ == '__main__':
    unittest.main()
