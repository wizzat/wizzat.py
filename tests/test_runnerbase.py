import os, tempfile
from wizzat.testutil import *
from wizzat.runner import *
from wizzat.util import *

class TestRunnerBase(TestCase):
    def test_basic(self):
        class TestRunner(RunnerBase):
            log_root = tempfile.mkdtemp()

            def _run(self):
                self.output = range(5)

        runner = TestRunner().run()
        self.assertEqual(runner.output, range(5))

    def test_does_not_run_if_should_not_run(self):
        class TestRunner(RunnerBase):
            log_root = tempfile.mkdtemp()

            def should_run(self):
                return False

            def _run(self):
                self.output = range(5)

        with self.assertRaises(AttributeError):
            runner = TestRunner().run()
            runner.output

    def test_exception_handling(self):
        class Runner(RunnerBase):
            log_root = tempfile.mkdtemp()
            def _run(self):
                1 / 0

        runner = Runner()
        with self.assertRaises(ZeroDivisionError):
            runner = runner.run()

        self.assertTrue('ZeroDivisionError' in slurp(runner.log_file))

    def test_pidfile(self):
        pidfile = tempfile.NamedTemporaryFile().name
        class Runner(RunnerBase):
            log_root = tempfile.mkdtemp()

            def pidfile(self):
                return pidfile

            def _run(self):
                self.ran = True

        r = Runner()

        # Test when pidfile exists but doesn't contain anything
        self.assertEqual(r.check_pidfile(), True)

        # Test when pidfile doesn't exist
        os.unlink(pidfile)
        self.assertEqual(r.check_pidfile(), True)

        # Test when pidfile exists with a valid pid
        with open(pidfile, 'w') as fp:
            fp.write(str(os.getpid()))
        self.assertEqual(r.check_pidfile(), False)


        # Test when pidfile exists with a non-parseable value
        with open(pidfile, 'w') as fp:
            fp.write("abc")
        self.assertEqual(r.check_pidfile(), True)
