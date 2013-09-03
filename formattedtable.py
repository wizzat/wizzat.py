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
    if not rows:
        return "[]"

    table = texttable.Texttable()
    table.header(header)
    for row in rows:
        table.add_row([ formatted_value(x) for x in row ])

    return table.draw()

def tableize_obj_list(fields, obj_list):
    if not obj_list or not fields:
        return "[]"

    table = texttable.Texttable()
    table.header(fields)
    for obj in obj_list:
        table.add_row([ formatted_value(obj[field]) for field in fields ])

    return table.draw()

