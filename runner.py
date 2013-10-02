import logging, signal

__all__ = [
    'RunnerBase',
]

class RunnerBase(object):
    log_root = '/mnt/logs'
    process_name = None
    sig_handlers = {
        signal.SIGTERM : 'sig_term',
        signal.SIGINT  : 'sig_int',
        signal.SIGHUP  : 'sig_hup',
    }

    def __init__(self, **params):
        self.__dict__.update(params)
        self.should_run  = True
        self.terminated  = False
        self.interrupted = False

        self.setup_connections()
        self.setup_logging()

        for sig, func in self.sig_handlers.iteritems():
            signal.signal(sig, getattr(self, func))

    def run(self):
        if not self.check_if_should_run():
            return
        self._run()

        return self

    def setup_connections(self):
        pass

    def should_run(self):
        return True

    def setup_logging(self):
        cls = type(self)
        self.process_name = self.process_name or "{}.{}".format(cls.__module__, cls.__name__)
        log_file = os.path.join(self.log_root, self.process_name + '.log')

        # http://stackoverflow.com/questions/1943747/python-logging-before-you-run-logging-basicconfig
        # This lets you run these guys in tests with a different logging conf per runner
        root = logging.getLogger()
        if root.handlers:
            for handler in root.handlers:
                root.removeHandler(handler)

        logging.basicConfig(
            format   = '%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            filename = log_file,
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
