try:
    from psycopg2cffi import compat
    compat.register()
except ImportError:
    pass

import psycopg2
from wizzat.testutil import *
from wizzat.pgpartition import *
from wizzat.dateutil import *
from wizzat.pghelper import *
from testcase import DBTestCase

class PartitionerTestCase(DBTestCase):
    setup_database = True

    def create_test_table(self):
        execute(self.conn(), """
            DROP TABLE IF EXISTS part_table CASCADE;
            DROP SCHEMA IF EXISTS partitions;
            CREATE SCHEMA partitions;
            CREATE TABLE part_table (
                datefield TIMESTAMP,
                meta      INT
            );
        """)

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

        execute(self.conn(), sql,
            datefield = datefield,
            meta      = meta,
        )

class PgPartitionTest(PartitionerTestCase):
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

        create_partition(self.conn(), 'part_table', 'part_table_20140505', range_values = dict(
            field = 'datefield',
            start = coerce_day(coerce_date('2014-05-05 00:00:00')),
            stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
        ))

        self.assertTrue(table_exists(self.conn(), 'part_table_20140505'))

    def test_partitions__date_check_constraints_allow_insertion(self):
        self.create_test_table()

        create_partition(self.conn(), 'part_table', 'part_table_20140505', range_values = dict(
            field = 'datefield',
            start = coerce_day(coerce_date('2014-05-05 00:00:00')),
            stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
        ))

        for date in [ '2014-05-05 00:00:00', '2014-05-05 12:00:00', '2014-05-05 23:59:59', ]:
            self.insert_row('part_table_20140505', date, 1)

        self.assertSqlResults(self.conn(), """
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

        create_partition(self.conn(), 'part_table', 'part_table_20140505', range_values = dict(
            field = 'datefield',
            start = coerce_day(coerce_date('2014-05-05 00:00:00')),
            stop  = coerce_day(coerce_date('2014-05-06 00:00:00')),
        ))

        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_20140505', '2014-05-06 00:00:00', 2)

    def test_partitions__kv_check_constraints_allow_insertion(self):
        self.create_test_table()

        create_partition(self.conn(), 'part_table', 'part_table_1', range_values = dict(
            field = 'meta',
            start = 1,
            stop  = 2,
        ))

        create_partition(self.conn(), 'part_table', 'part_table_2', range_values = dict(
            field = 'meta',
            start = 2,
            stop  = 3,
        ))

        for date in range(3):
            self.insert_row('part_table_1', None, 1)

        for date in range(3):
            self.insert_row('part_table_2', None, 2)

        self.assertSqlResults(self.conn(), """
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

        self.assertSqlResults(self.conn(), """
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

        create_partition(self.conn(), 'part_table', 'part_table_1', range_values = dict(
            field = 'meta',
            start = 1,
            stop  = 2,
        ))

        create_partition(self.conn(), 'part_table', 'part_table_2', range_values = dict(
            field = 'meta',
            start = 2,
            stop  = 3,
        ))

        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_1', None, 2)

    def test_partitions__multiple_constraints(self):
        self.create_test_table()

        create_partition(self.conn(), 'part_table', 'part_table_1', range_values = [
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

        create_partition(self.conn(), 'part_table', 'part_table_20140505_1', range_values = [
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

        self.insert_row('part_table_20140505_1', '2014-05-05 01:02:03', 1)
        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_20140505_1', '2014-05-05 01:02:03', 2)

        with self.assertRaises(psycopg2.IntegrityError):
            self.insert_row('part_table_20140505_1', '2014-05-06 01:02:03', 1)

class PgDatePartitionerTest(PartitionerTestCase):
    def test_partition_name__day_partitioner(self):
        partitioner = DayPartitioner('part_table', 'date_field')
        self.assertEqual(partitioner.partition_name('2014-04-04 01:02:03'), 'public.part_table_20140404')
        self.assertEqual(partitioner.partition_name('2014-05-04 01:02:03'), 'public.part_table_20140504')

    def test_partition_name__week_partitioner(self):
        partitioner = WeekPartitioner('part_table', 'date_field')
        self.assertEqual(partitioner.partition_name('2014-04-04 01:02:03'), 'public.part_table_20140331')
        self.assertEqual(partitioner.partition_name('2014-05-04 01:02:03'), 'public.part_table_20140428')

    def test_partition_name__month_partitioner(self):
        partitioner = MonthPartitioner('part_table', 'date_field')
        self.assertEqual(partitioner.partition_name('2014-04-04 01:02:03'), 'public.part_table_20140401')
        self.assertEqual(partitioner.partition_name('2014-05-04 01:02:03'), 'public.part_table_20140501')

    def test_partition_name__schema_support(self):
        partitioner = DayPartitioner('part_table', 'date_field', part_schema = 'partitions')
        self.assertEqual(partitioner.partition_name('2014-04-04 01:02:03'), 'partitions.part_table_20140404')
        self.assertEqual(partitioner.partition_name('2014-05-04 01:02:03'), 'partitions.part_table_20140504')

    def test_valid_partition__no_retention_period(self):
        set_now("2014-04-04 00:01:23")
        partitioner = DayPartitioner('part_table', 'date_field', retention_period = None)

        self.assertEqual(partitioner.valid_partition('1960-01-01 00:00:00'), True)
        self.assertEqual(partitioner.valid_partition('2014-04-04 23:59:59'), True)
        self.assertEqual(partitioner.valid_partition('2014-04-05 00:00:00'), True)
        self.assertEqual(partitioner.valid_partition('2014-04-06 00:00:00'), False)

    def test_valid_partition__day_partitioner__retention_period(self):
        set_now('2014-04-04 00:01:23')

        partitioner = DayPartitioner('part_table', 'date_field', retention_period = days(90))

        self.assertEqual(partitioner.valid_partition('1960-01-01 00:00:00'), False)
        self.assertEqual(partitioner.valid_partition('2014-01-03 00:00:00'), False)
        self.assertEqual(partitioner.valid_partition('2014-01-04 00:00:00'), True)
        self.assertEqual(partitioner.valid_partition('2014-04-04 23:59:59'), True)
        self.assertEqual(partitioner.valid_partition('2014-04-04 23:59:59'), True)
        self.assertEqual(partitioner.valid_partition('2014-04-05 00:00:00'), True)
        self.assertEqual(partitioner.valid_partition('2014-04-06 00:00:00'), False)

    def test_complains_if_base_table_does_not_exist(self):
        partitioner = DayPartitioner('part_table', 'date_field')

        execute(self.conn(), """
            DROP TABLE IF EXISTS part_table CASCADE;
        """)

        with self.assertRaises(PgProgrammingError):
            partitioner.find_or_create_partition(self.conn(), '2014-04-04 23:59:59')

    def test_subpartitioning_support(self):
        self.create_test_table()
        master_partitioner = DayPartitioner('part_table', 'datefield', part_schema = 'partitions')
        new_partition = master_partitioner.find_or_create_partition(self.conn(), "2014-05-05 00:00:00")

        part_schema, part_name = new_partition.split('.')

        subpartitioner = DayPartitioner(part_name, 'datefield', table_schema = part_schema, part_schema = 'partitions')
        subpartitioner.find_or_create_partition(self.conn(), "2014-05-05 00:00:00")

    def test_actually_creates_partition(self):
        self.create_test_table()
        partitioner = DayPartitioner('part_table', 'datefield', part_schema = 'partitions')

        for date in [ '2014-05-05 00:00:00', '2014-05-05 23:59:59', '2014-05-06 00:00:01', ]:
            self.insert_row(partitioner.find_or_create_partition(self.conn(), date), date, 1)

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM only part_table
            ORDER BY datefield
        """,
            [ 'datefield',            'meta',  ],
        )

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM part_table
            ORDER BY datefield
        """,
            [ 'datefield',            'meta',  ],
            [ '2014-05-05 00:00:00',  1,       ],
            [ '2014-05-05 23:59:59',  1,       ],
            [ '2014-05-06 00:00:01',  1,       ],
        )

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM partitions.part_table_20140505
            ORDER BY datefield
        """,
            [ 'datefield',            'meta',  ],
            [ '2014-05-05 00:00:00',  1,       ],
            [ '2014-05-05 23:59:59',  1,       ],
        )

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM partitions.part_table_20140506
            ORDER BY datefield
        """,
            [ 'datefield',            'meta',  ],
            [ '2014-05-06 00:00:01',  1,       ],
        )
