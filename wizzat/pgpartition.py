import collections
import datetime
import wizzat.pghelper
from wizzat.dateutil import *
from wizzat.pghelper import execute, table_exists

__all__ = [
    'create_partition',
    'generate_partition_sql',
    'UnretainedPartitionError',
    'DatePartitioner',
    'DayPartitioner',
    'WeekPartitioner',
    'MonthPartitioner',
]

class UnretainedPartitionError(Exception): pass


class DatePartitioner(object):
    def __init__(self, table_name, date_field,
        date_type        = 'timestamp',
        date_fmt         = "%Y%m%d",
        table_schema     = 'public',
        part_schema      = 'public',
        reject_future    = True,
        retention_period = None
    ):
        self.table_name       = table_name
        self.table_schema     = table_schema
        self.part_schema      = part_schema
        self.date_field       = date_field
        self.date_type        = date_type
        self.date_fmt         = date_fmt
        self.reject_future    = reject_future
        self.retention_period = retention_period

    def find_or_create_partition(self, conn, date):
        date = self.trunc_func(coerce_date(date))

        if not self.valid_partition(date):
            raise UnretainedPartitionError((date, self.retention_period))

        partition_name = self.partition_name(date)

        wizzat.pgpartition.create_partition(conn, self.full_table_name, partition_name, range_values = [{
            'field' : self.date_field,
            'start' : date,
            'stop'  : date + self.interval,
        }])

        return partition_name

    @property
    def full_table_name(self):
        return '{}.{}'.format(self.table_schema, self.table_name)

    def valid_partition(self, date):
        date = coerce_date(date)
        invalid = False

        if self.retention_period:
            earliest_date = self.trunc_func(now() - self.retention_period)
            invalid = invalid or date < earliest_date

        if self.reject_future:
            latest_date = self.trunc_func(now() + self.interval)
            invalid = invalid or date > latest_date

        return not invalid

    def partition_name(self, date):
        start_date = self.trunc_func(coerce_date(date))
        date_str   = start_date.strftime(self.date_fmt)

        return '{}.{}_{}'.format(self.part_schema, self.table_name, date_str)


class DayPartitioner(DatePartitioner):
    interval   = days(1)

    def trunc_func(self, date):
        return to_day(date)


class WeekPartitioner(DatePartitioner):
    interval   = days(7)

    def trunc_func(self, date):
        return to_week(date)

class MonthPartitioner(DatePartitioner):
    interval   = days(32)

    def trunc_func(self, date):
        return to_month(date)


def create_partition(conn, table_name, partition_name, range_values = None, key_values = None):
    if table_exists(conn, partition_name):
        return

    sql = generate_partition_sql(table_name, partition_name, range_values, key_values)
    execute(conn, sql)

def generate_partition_sql(table_name, partition_name, range_values = None, key_values = None):
    check_constraints = []
    key_values = key_values or {}

    if range_values:
        if isinstance(range_values, list):
            for data in range_values:
                check_constraints += _generate_range_check(
                    data['field'],
                    data['start'],
                    data['stop'],
                )
        else:
            check_constraints += _generate_range_check(
                range_values['field'],
                range_values['start'],
                range_values['stop'],
            )

    for field_name, value in key_values.iteritems():
        check_constraints += _generate_kv_check(field_name, value)

    return """
        CREATE TABLE {partition_name} (
            CHECK ({check_constraints})
        ) INHERITS ({base_table})
    """.format(
        partition_name    = partition_name,
        base_table        = table_name,
        check_constraints = '\n    AND '.join(check_constraints),
    )


def _part_format_value(value):
    if isinstance(value, datetime.datetime):
        return "'{}'::timestamp".format(format_date(value))
    elif isinstance(value, datetime.date):
        return "'{}'::date".format(format_day(value))
    elif isinstance(value, basestring):
        return "'{}'".format(value)
    else:
        return value


def _generate_range_check(field_name, start_value, end_value):
    return [
        "{} >= {}".format(field_name, _part_format_value(start_value)),
        "{} <  {}".format(field_name, _part_format_value(end_value)),
    ]


def _generate_kv_check(field_name, value):
    if isinstance(value, (list, tuple)):
        return [ '{} IN ({})'.format(
            field_name,
            ', '.join(_part_format_value(x) for x in value),
        ) ]
    else:
        return [ "{} = {}".format(field_name, _part_format_value(value)) ]

