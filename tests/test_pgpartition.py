try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass

import psycopg2
from pyutil.testutil import *
from pyutil import pghelper
from pyutil.pgtestutil import *
from pyutil.pgpartition import *
from pyutil.dateutil import *
from pyutil.pghelper import *

class PgPartitionTest(PgTestCase):
    setup_database = True

    def test_partition_sql__timestamp(self):
        sql = generate_partition_sql('part_table', 'part_table_20140505', range_values = {
            'field' : 'datefield',
            'start' : coerce_date('2014-05-05 00:00:00'),
            'stop' : coerce_date('2014-05-06 00:00:00'),
        })

        self.assertTrue('part_table_20140505' in sql)
        self.assertTrue('INHERITS (part_table)' in sql)
        self.assertTrue("datefield >= '2014-05-05 00:00:00'::timestamp" in sql)
        self.assertTrue("datefield <  '2014-05-06 00:00:00'::timestamp" in sql)

    def test_partition_sql__date(self):
        sql = generate_partition_sql('part_table', 'part_table_20140505', range_values = dict(
            field = 'datefield',
            start = coerce_day(coerce_date('2014-05-05 00:00:00')),
            stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
        ))

        self.assertTrue('part_table_20140505' in sql)
        self.assertTrue('INHERITS (part_table)' in sql)
        self.assertTrue("datefield >= '2014-05-05'::date" in sql)
        self.assertTrue("datefield <  '2014-05-06'::date" in sql)

    def test_partition_sql__actually_works(self):
        self.create_test_table()

        create_partition(self.conn, 'part_table', 'part_table_20140505', range_values = dict(
            field = 'datefield',
            start = coerce_day(coerce_date('2014-05-05 00:00:00')),
            stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
        ))

        self.assertTrue(table_exists(self.conn, 'part_table_20140505'))

    def test_partitions__date_check_constraints_allow_insertion(self):
        self.create_test_table()

        create_partition(self.conn, 'part_table', 'part_table_20140505', range_values = dict(
            field = 'datefield',
            start = coerce_day(coerce_date('2014-05-05 00:00:00')),
            stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
        ))

        for date in [ '2014-05-05 00:00:00', '2014-05-05 12:00:00', '2014-05-05 23:59:59', ]:
            self.insert_row('part_table_20140505', date, 1)

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM part_table
            ORDER BY datefield
        """,
            [ 'datefield',            'meta',  ],
            [ '2014-05-05 00:00:00',  1,        ],
            [ '2014-05-05 12:00:00',  1,        ],
            [ '2014-05-05 23:59:59',  1,        ],
        )

    def test_partitions__date_check_constraints_work(self):
        self.create_test_table()

        create_partition(self.conn, 'part_table', 'part_table_20140505', range_values = dict(
            field = 'datefield',
            start = coerce_day(coerce_date('2014-05-05 00:00:00')),
            stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
        ))

        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_20140505', '2014-05-06 00:00:00', 2)

    def test_partitions__kv_check_constraints_allow_insertion(self):
        self.create_test_table()

        create_partition(self.conn, 'part_table', 'part_table_1', range_values = dict(
            field = 'meta',
            start = 1,
            stop  = 2,
        ))

        create_partition(self.conn, 'part_table', 'part_table_2', range_values = dict(
            field = 'meta',
            start = 2,
            stop  = 3,
        ))

        for date in range(3):
            self.insert_row('part_table_1', None, 1)

        for date in range(3):
            self.insert_row('part_table_2', None, 2)

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM part_table
            ORDER BY meta, datefield
        """,
            [ 'meta',  ],
            [ 1,       ],
            [ 1,       ],
            [ 1,       ],
            [ 2,       ],
            [ 2,       ],
            [ 2,       ],
        )

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM only part_table_1
            ORDER BY meta, datefield
        """,
            [ 'meta',  ],
            [ 1,       ],
            [ 1,       ],
            [ 1,       ],
        )

    def test_partitions__kv_check_constraints_work(self):
        self.create_test_table()

        create_partition(self.conn, 'part_table', 'part_table_1', range_values = dict(
            field = 'meta',
            start = 1,
            stop  = 2,
        ))

        create_partition(self.conn, 'part_table', 'part_table_2', range_values = dict(
            field = 'meta',
            start = 2,
            stop  = 3,
        ))

        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_1', None, 2)

    def test_partitions__multiple_constraints(self):
        self.create_test_table()

        create_partition(self.conn, 'part_table', 'part_table_1', range_values = [
            dict(
                field = 'datefield',
                start = coerce_day(coerce_date('2014-05-05 00:00:00')),
                stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
            ),
            dict(
                field = 'meta',
                start = 1,
                stop  = 2,
            ),
        ])

    def test_partitions__multiple_constraints_work(self):
        self.create_test_table()

        create_partition(self.conn, 'part_table', 'part_table_20140505_1', range_values = [
            dict(
                field = 'datefield',
                start = coerce_day(coerce_date('2014-05-05 00:00:00')),
                stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
            ),
            dict(
                field = 'meta',
                start = 1,
                stop  = 2,
            ),
        ])
        self.conn.commit()

        self.insert_row('part_table_20140505_1', '2014-05-05 01:02:03', 1)
        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_20140505_1', '2014-05-05 01:02:03', 2)
        self.conn.commit()

        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_20140505_1', '2014-05-06 01:02:03', 1)
        self.conn.commit()

    def create_test_table(self):
        execute(self.conn, """
            DROP TABLE IF EXISTS part_table;
            CREATE TABLE part_table (
                datefield TIMESTAMP,
                meta      INT
            );
        """)
        self.conn.commit()

    def insert_row(self, partition_name, datefield = None, meta = None):
        sql = """
            INSERT INTO {partition_name} (
                datefield,
                meta
            ) VALUES (
                %(datefield)s,
                %(meta)s
            )
        """.format(partition_name = partition_name)

        execute(self.conn, sql,
            datefield = datefield,
            meta      = meta,
        )
