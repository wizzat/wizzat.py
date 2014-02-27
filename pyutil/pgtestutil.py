import difflib
from testutil import *
from formattedtable import *
from pghelper import ConnMgr, fetch_results

__all__ = [
    'PgTestCase',
]

class PgTestCase(TestCase):
    """
    Postgres based test util
    """
    setup_database = False
    db_info = {
        'host'     : 'localhost',
        'port'     : 5432,
        'user'     : 'pyutil',
        'password' : 'pyutil',
        'database' : 'pyutil_testdb',
        'minconn'  : 0,
        'maxconn'  : 2,
    }

    def setUp(self):
        super(PgTestCase, self).setUp()

        self.mgr  = None
        self.conn = None
        self.setup_connections()

    def tearDown(self):
        super(PgTestCase, self).tearDown()
        self.teardown_connections()

    def setup_connections(self):
        if self.setup_database:
            self.mgr = ConnMgr.default_from_info(**self.db_info)
            self.conn = self.mgr.getconn("conn")

    def teardown_connections(self):
        # First tear down all the other connection pools
        if self.setup_database:
            while ConnMgr.all_mgrs:
                mgr = ConnMgr.all_mgrs.pop()
                if mgr != self.mgr:
                    mgr.rollback()
                    mgr.closeall()

            # Now tear ours down and put it back
            if self.mgr:
                self.mgr.rollback()
                ConnMgr.all_mgrs.append(self.mgr)

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

        expected = tableize_grid(header, rows)
        actual   = tableize_obj_list(header, results)

        diff = list(difflib.unified_diff(expected.split('\n'), actual.split('\n')))
        if diff:
            raise AssertionError("Assert failed for sql\n{sql}\n\n{diff}".format(
                sql = sql,
                diff = '\n'.join(diff),
            ))

