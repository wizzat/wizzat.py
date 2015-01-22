from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

import unittest, difflib, functools, os, json
from wizzat.sqlhelper import fetch_results
from wizzat.util import assert_online, OfflineError, reset_online, set_online, is_online
from wizzat.dateutil import reset_now
from wizzat.decorators import *
from wizzat.textutil import text_table

__all__ = [
    'TestCase',
    'OfflineError',
    'skip_offline',
    'skip_unfinished',
    'skip_performance',
    'expected_failure',
    'expectedFailure',
]

expected_failure = unittest.expectedFailure
expectedFailure  = unittest.expectedFailure

class TestCase(unittest.TestCase):
    """
    This is a default test case.
    """
    requires_online  = True
    setup_database   = False
    setup_queries    = {}
    teardown_queries = {}

    def setUp(self):
        super(TestCase, self).setUp()
        reset_now()
        reset_online()

        if self.requires_online and not is_online():
            self.skipTest("----- OFFLINE TEST -----")
        elif not self.requires_online:
            set_online(False)

        self.setup_connections()

    def tearDown(self):
        super(TestCase, self).tearDown()
        self.teardown_connections()

    def setup_connections(self):
        if self.setup_database:
            for db_name, query in self.setup_queries.items():
                assert isinstance(v, (list, tuple)), "setup_queries is of the form { 'db_name' : [ 'query1', 'query2' ] }"
                db_conn = self.conn(db_name)
                for query in queries:
                    execute(db_conn, query)
                    db_conn.commit()

    def teardown_connections(self):
        if self.setup_database:
            try:
                import pghelper
                for mgr in pghelper.ConnMgr.all_mgrs:
                    mgr.rollback()
            except ImportError:
                pass

            for db_name, query in self.teardown_queries.items():
                assert isinstance(v, (list, tuple)), "teardown_queries is of the form { 'db_name' : [ 'query1', 'query2' ] }"
                db_conn = self.conn(db_name)
                for query in queries:
                    execute(db_conn, query)
                    db_conn.commit()

    def assertJSONEqual(self, obj1, obj2):
        expected = json.dumps(obj1, indent=4, sort_keys=True)
        actual = json.dumps(obj2, indent=4, sort_keys=True)

        diff = list(difflib.unified_diff(expected.split('\n'), actual.split('\n'), n=20))
        if diff:
            raise AssertionError("Assert JSON failed:\n{diff}".format(
                diff = '\n'.join(diff),
            ))

    def assertSqlResults(self, conn, sql, *rows):
        """
            Example:

            self.assertSqlResults(self.db_conn, '''
                select *
                from some_table
                order by logdate
            ''',
                [ 'logdate',  'col1',  'col2',  'metadata',  ],
                [ time1,      'abc1',  'def',   1,           ],
                [ time1,      'abc1',  'def',   1,           ],
                [ time2,      'abc2',  'def',   1,           ],
                [ time3,      'abc1',  'def',   1,           ],
            )

        """
        header, rows = rows[0], rows[1:]
        results = fetch_results(conn, sql)

        expected = text_table(header, rows)
        actual   = text_table(header, results, row_dicts = True)

        diff = list(difflib.unified_diff(expected.split('\n'), actual.split('\n'), n=20))
        if diff:
            raise AssertionError("Assert failed for sql\n{sql}\n\n{diff}".format(
                sql = sql,
                diff = '\n'.join(diff),
            ))

