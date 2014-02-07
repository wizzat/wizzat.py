import unittest, difflib, texttable, functools, os
from util import assert_online, OfflineError
from dateutil import reset_now
from decorators import *

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
expectedFailure = unittest.expectedFailure

class TestCase(unittest.TestCase):
    """
    This is a default test case.
    """
    def setUp(self):
        super(TestCase, self).setUp()
        reset_now()

        self.setup_connections()

    def tearDown(self):
        super(TestCase, self).tearDown()
        self.teardown_connections()

    def setup_connections(self):
        pass

    def teardown_connections(self):
        pass
