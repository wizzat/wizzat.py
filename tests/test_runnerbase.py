from pyutil.testutil import *
from pyutil.runner import *
from pyutil.util import *
import datetime, time, os, uuid, logging

class TestRunnerBase(TestCase):
    def test_basic(self):
        class TestRunner(RunnerBase):
            def _run(self):
                self.output = range(5)

        runner = TestRunner().run()
        self.assertEqual(runner.output, range(5))

    def test_does_not_run_if_should_not_run(self):
        class TestRunner(RunnerBase):
            def should_run(self):
                return False

            def _run(self):
                self.output = range(5)

        with self.assertRaises(AttributeError):
            runner = TestRunner().run()
            runner.output

    def test_exception_handling(self):
        class Runner(RunnerBase):
            def _run(self):
                1 / 0

        runner = Runner()
        with self.assertRaises(ZeroDivisionError):
            runner = runner.run()

        self.assertTrue('ZeroDivisionError' in slurp(runner.log_file))
