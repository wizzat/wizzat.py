import wizzat.testutil
import wizzat.pghelper

class DBTestCase(wizzat.testutil.TestCase):
    db_info = {
        'host'     : 'localhost',
        'port'     : 5432,
        'user'     : 'wizzatpy',
        'password' : 'wizzat',
        'database' : 'wizzatpy_testdb',
        'minconn'  : 0,
        'maxconn'  : 3,
    }

    db_mgr = wizzat.pghelper.ConnMgr.default_from_info(**db_info)

    def conn(self, name = 'testconn'):
        conn = self.db_mgr.getconn(name)
        conn.autocommit = True

        return conn
