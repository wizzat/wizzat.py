import unittest, difflib, texttable

class AssertSQLMixin(object):
    """
    Mixin for assertSqlResults
    """
    def assertSqlResults(conn, sql, *rows):
        header, rows = rows[0], rows[1:]
        results = fetch_result_rows(conn, sql)

        expected = tableize_grid(header, rows)
        actual   = tableize_obj_list(header, results)

        diff = difflib.unified_diff(expected, actual)
        if diff:
            raise AssertionError("Assert failed for sql\n{sql}\n\n{diff}".format(
                sql = sql,
                diff = diff,
            ))
