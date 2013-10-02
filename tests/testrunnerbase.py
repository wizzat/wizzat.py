from pyutil.testutil import *
from pyutil.runner import *
import unittest, datetime, time


class TestRunnerBase(unittest.TestCase):
    def test_basic(self):
        class TestRunner(RunnerBase):
            def run(self):
                self.output = range(5)

        runner = TestRunner().run()
        self.assertEqual(runner.output, range(5))
