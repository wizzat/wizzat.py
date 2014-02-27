import texttable

__all__ = [
    'AmbiguousFormatError',
    'add_format_rule',
    'tableize_grid',
    'tableize_obj_list',
]

class AmbiguousFormatError(Exception): pass

format_rules = {}
def add_format_rule(obj_type, func):
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

def formatted_value(obj):
    global format_rules
    for rule_type, rule_func in sorted(format_rules.iteritems()):
        if isinstance(obj, rule_type):
            return rule_func(obj)

    return obj

def tableize_grid(header, rows):
    """
        Turns
            [ 'abc',                   'def'          ],
            [ datetime.datetime(...),  Complex(0, 1), ],

        Into
            [ 'abc',                  'def',  ],
            [ '2013-03-03 01:02:03',  '1i',   ],
    """
    if not rows:
        return "[]"

    table = texttable.Texttable()
    table.header(header)
    for row in rows:
        table.add_row([ formatted_value(x) for x in row ])

    return table.draw()

def tableize_obj_list(fields, obj_list):
    """
        Turns
            { 'abc' : 1, 'def' : 2 },
            { 'abc' : 2, 'def' : 4 },
            { 'abc' : 4, 'def' : 6 },

        Into
            [ 'abc',  'def',  ],
            [ 1,      2,      ],
            [ 2,      4,      ],
            [ 4,      6,      ],
    """
    if not obj_list or not fields:
        return "[]"

    table = texttable.Texttable()
    table.header(fields)
    for obj in obj_list:
        table.add_row([ formatted_value(obj[field]) for field in fields ])

    return table.draw()
