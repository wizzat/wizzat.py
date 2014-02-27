import logging, signal, os, os.path
from util import mkdirp, slurp

__all__ = [
    'RunnerBase',
]

class RunnerBase(object):
    """
        This is a base class for runners.  It supports:
        - Setting up logging
        - Resetting logging for tests
        - Signal handling
        - Hooks for common operations like setup_connections or should_run
    """
    log_root = '/mnt/logs'
    process_name = None
    sig_handlers = {
        signal.SIGTERM : 'sig_term',
        signal.SIGINT  : 'sig_int',
        signal.SIGHUP  : 'sig_hup',
    }

    def __init__(self, **params):
        self.__dict__.update(params)
        self.terminated  = False
        self.interrupted = False

        self.setup_connections()
        self.setup_logging()

        for sig, func in self.sig_handlers.iteritems():
            signal.signal(sig, getattr(self, func))

    def run(self):
        """
        This method provides should_run() and automatic exception handling/logging.
        """
        if not self.should_run():
            return

        try:
            self._run()
            return self
        except Exception:
            logging.exception("Caught exception")
            raise

    def pidfile(self):
        """
        This method can be overridden to return a full file path, which will be checked as a pidfile.
        If the pidfile exists and the process also exists, the process will be flagged as should_run = False.
        """
        return False

    def check_pidfile(self):
        pidfile = self.pidfile()
        if pidfile:
            logging.info("Checking pidfile: %s", pidfile)
            mkdirp(os.path.dirname(pidfile))
            if os.path.exists(pidfile):
                try:
                    # Does the process exist and can we signal it?
                    pid = int(slurp(pidfile).strip())
                    logging.info("Pidfile %s exists, checking pid %d", pidfile, pid)
                    os.kill(pid, 0)
                    logging.info("Pidfile exists and process can be signaled, aborting")
                    return False
                except (ValueError, OSError):
                    logging.info("Pidfile exists but process cannot be signaled, continuing")

            logging.info("Writing new pidfile %s (%d)", pidfile, os.getpid())
            with open(pidfile, 'w') as fp:
                fp.write(str(os.getpid()))

        return True

    def setup_connections(self):
        """
        Stub for overriding.  Called during init()
        """
        pass

    def should_run(self):
        """
        Should implement logic for determining whether the process should run.
        Memory constraints, CPU constraints, pidfiles, etc go here.

        Called before _run()
        """
        if not self.check_pidfile():
            return False

        return True

    def setup_logging(self):
        cls = type(self)
        self.process_name = self.process_name or "{}.{}".format(cls.__module__, cls.__name__)
        self.log_file = os.path.join(self.log_root, self.process_name + '.log')

        mkdirp(self.log_root)

        # http://stackoverflow.com/questions/1943747/python-logging-before-you-run-logging-basicconfig
        # This lets you run these guys in tests with a different logging conf per runner
        root = logging.getLogger()
        if root.handlers:
            for handler in root.handlers:
                root.removeHandler(handler)

        logging.basicConfig(
            format   = '%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            filename = self.log_file,
            level    = logging.INFO,
        )

    def sig_term(self, signal, frame):
        """
        By default, sig_term sets the `terminated` flag.  This can be used for main loop control.
        """
        self.terminated = True

    def sig_int(self, signal, frame):
        """
        By default, sig_int sets the `interrupted` flag.  This can be used for main loop control.
        """
        self.interrupted = True

    def sig_hup(self, signal, frame):
        """
        By default, sig_hup will close and reopen log files (for log rotation)
        """
        self.setup_logging()
