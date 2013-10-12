import unittest, difflib, texttable, functools, os
from pghelper import ConnMgr, fetch_results
from util import assert_online, OfflineError
from formattedtable import *

__all__ = [
    'AssertSQLMixin',
    'OfflineError',
    'skip_offline',
    'skip_unfinished',
    'skip_performance',
]

def skip_offline(func):
    """
    This decorator is meant for tests.  It will catch OfflineError and issue a skipTest for you.
    """
    @functools.wraps(func)
    def wrapper(self):
        try:
            assert_online()
            retval = func(self)
        except OfflineError:
            self.skipTest("----- OFFLINE TEST -----")

        return retval
    return wrapper

def skip_performance(func):
    """
    This decorator is meant for tests.  It checks for $ENV{PERFORMANCE_TEST} and will issue skipTest without it.
    """
    @functools.wraps(func)
    def wrapper(self):
        if not os.environ.get('PERFORMANCE_TEST', False):
            self.skipTest("----- PERFORMANCE TEST -----")
        else:
            return func(self)

    return wrapper

def skip_unfinished(func):
    """
    This decorator is meant for tests.  It automatically issues a skipTest.
    """

    @functools.wraps(func)
    def wrapper(self):
        self.skipTest("----- UNFINISHED TEST -----")
    return wrapper


class AssertSQLMixin(object):
    """
    Mixin for assertSqlResults
    """
    def setup_connections(self):
        self.mgr = ConnMgr.default_from_info(**self.db_info)
        self.conn = self.mgr.getconn("conn")

    def teardown_connections(self):
        # First tear down all the other connection pools
        while ConnMgr.all_mgrs:
            mgr = ConnMgr.all_mgrs.pop()
            if mgr != self.mgr:
                mgr.rollback()
                mgr.closeall()

        # Now tear ours down and put it back
        self.mgr.rollback()
        ConnMgr.all_mgrs.append(self.mgr)

    def assertSqlResults(self, conn, sql, *rows):
        header, rows = rows[0], rows[1:]
        results = fetch_results(conn, sql)

        expected = tableize_grid(header, rows)
        actual   = tableize_obj_list(header, results)

        diff = list(difflib.unified_diff(expected.split('\n'), actual.split('\n')))
        if diff:
            raise AssertionError("Assert failed for sql\n{sql}\n\n{diff}".format(
                sql = sql,
                diff = '\n'.join(diff),
            ))
