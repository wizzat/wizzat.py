from __future__ import (absolute_import, division, print_function, unicode_literals)
from builtins import *

import wizzat.testutil
import wizzat.pghelper

class DBTestCase(wizzat.testutil.TestCase):
    db_info = {
        'host'       : 'localhost',
        'port'       : 5432,
        'user'       : 'wizzat',
        'password'   : 'wizzat',
        'database'   : 'wizzatpy_testdb',
        'autocommit' : False,
    }

    db_mgr = wizzat.pghelper.ConnMgr(db_info,
        max_objs = 3,
    )

    def conn(self, name = 'testconn'):
        conn = self.db_mgr.name(name)
        conn.autocommit = True

        return conn
