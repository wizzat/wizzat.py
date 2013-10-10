import unittest

__all__ = [
    'PyUtilTestCase',
]

class PyUtilTestCase(unittest.TestCase):
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
        super(PyUtilTestCase, self).setUp()
        self.setup_connections()

    def tearDown(self):
        super(PyUtilTestCase, self).tearDown()
        self.teardown_connections()

    def setup_connections(self):
        pass

    def teardown_connections(self):
        pass
