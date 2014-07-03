import pyutil.testutil
import pyutil.pghelper

class DBTestCase(pyutil.testutil.TestCase):
    db_info = {
        'host'     : 'localhost',
        'port'     : 5432,
        'user'     : 'pyutil',
        'password' : 'pyutil',
        'database' : 'pyutil_testdb',
        'minconn'  : 0,
        'maxconn'  : 3,
    }

    db_mgr = pyutil.pghelper.ConnMgr.default_from_info(**db_info)

    def conn(self, name = 'testconn'):
        conn = self.db_mgr.getconn(name)
        conn.autocommit = True

        return conn
