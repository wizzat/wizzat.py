import wizzat.testutil
import wizzat.pghelper

class DBTestCase(wizzat.testutil.TestCase):
    db_info = {
        'host'     : 'localhost',
        'port'     : 5432,
        'user'     : 'wizzat',
        'password' : 'wizzat',
        'database' : 'wizzat_testdb',
        'minconn'  : 0,
        'maxconn'  : 3,
    }

    db_mgr = wizzat.pghelper.ConnMgr.default_from_info(**db_info)

    def conn(self, name = 'testconn'):
        conn = self.db_mgr.getconn(name)
        conn.autocommit = True

        return conn
