from pyutil.testutil import *
from pyutil.runner import *
from pyutil.util import *
import unittest, datetime, time, os, uuid, logging

class TestRunnerBase(unittest.TestCase):
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

    def test_log_files(self):
        class TestRunner(RunnerBase):
            log_root = '/tmp'
            process_name = 'bad'
            def _run(self):
                self.uuid = str(uuid.uuid4())
                logging.info(self.uuid)

        runner = TestRunner().run()

        self.assertEqual(runner.uuid in slurp("/tmp/bad.log"), True)
        swallow(OSError, os.unlink, '/tmp/bad.log')

    def test_multiple_runners_with_different_log_files(self):
        class RunnerA(RunnerBase):
            def _run(self):
                self.uuid = str(uuid.uuid4())
                logging.info(self.uuid)

        class RunnerB(RunnerA):
            pass

        runner1 = RunnerA().run()
        runner2 = RunnerB().run()

        self.assertNotEqual(runner1.log_file, runner2.log_file)
        self.assertEqual(runner1.uuid in slurp(runner1.log_file), True)
        self.assertEqual(runner2.uuid in slurp(runner2.log_file), True)
