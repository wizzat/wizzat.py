from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *
from future.utils import iteritems

import io

from wizzat.util import set_strict_defaults
import wizzat.decorators

__all__ = [
    'AmbiguousFormatError',
    'set_formatter',
    'format_value',
    'text_table',
]

class AmbiguousFormatError(Exception): pass

format_rules = {}
def set_formatter(obj_type, func):
    """
        Adds a formatting rule.

        Example usage:

        def format_date(x):
            return x.strftime('%Y-%m-%d %H:%M:%S')

        add_format_rule(format_date)

        May raise AmbiguousFormatError if the format is already specified for super class.
    """
    global format_rules
    for old_type in format_rules:
        if isinstance(obj_type, old_type):
            raise AmbiguousFormatError()

    format_rules[obj_type] = func

def format_value(obj, trunc_len = None):
    global format_rules
    for rule_type, rule_func in sorted(iteritems(format_rules)):
        if isinstance(obj, rule_type):
            formatted_value = rule_func(obj)
            break
    else:
        formatted_value = str(obj)

    if trunc_len and len(formatted_value) > trunc_len:
        return formatted_value[:trunc_len]
    else:
        return formatted_value

def text_table(header, rows, **options):
    options = set_strict_defaults(options,
        row_dicts = False,
        col_trunc_len = None,
    )

    if not header:
        return '[]'

    fmt_rows = []
    field_sizes = [ len(name) for name in header ]

    for row in rows:
        if options['row_dicts']:
            fmt_row = [ format_value(row.get(name, '')) for name in header ]
        else:
            if len(row) != len(header):
                raise TypeError("Expected row of length {}, got {}".format(
                    len(header),
                    len(row)),
                )

            fmt_row = [ format_value(value) for value in row ]

        _update_sizes(field_sizes, fmt_row)
        fmt_rows.append(fmt_row)

    header_sep = _format_hline(field_sizes, '=', '+')
    row_sep = _format_hline(field_sizes, '-', '+')

    fp = io.StringIO()
    fp.write(row_sep)
    fp.write(_format_row(field_sizes, header))
    fp.write(header_sep)

    for row in fmt_rows:
        fp.write(_format_row(field_sizes, row))
        fp.write(row_sep)

    return fp.getvalue()

def _update_sizes(field_sizes, row):
    for (idx, size), value in zip(enumerate(field_sizes), row):
        if len(value) > size:
            field_sizes[idx] = len(value)

def _format_row(field_sizes, row):
    @wizzat.decorators.memoize()
    def lfmt(size):
        return ' {:<' + str(size) + '} '

    return '|{}|\n'.format('|'.join(lfmt(sz).format(x) for sz, x in zip(field_sizes, row)))

def _format_hline(field_sizes, fillchar, splitchar):
    mid = splitchar.join(fillchar * (x+2) for x in field_sizes)
    return splitchar + mid + splitchar + "\n"
