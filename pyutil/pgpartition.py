import datetime
from pyutil.dateutil import format_day, format_date
from pyutil.pghelper import execute

__all__ = [
    'create_partition',
    'generate_partition_sql',
]

def create_partition(conn, table_name, partition_name, range_values = None, key_values = None):
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

