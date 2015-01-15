from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import abc
import logging
import os
import signal
import time
import threading

import wizzat.util

class StartupFailureError(Exception): pass

class AsyncFileTailer(threading.Thread):
    """
    Sets up a thread which reads from a file handle and directs the output to a log function.
    This is particularly useful when multiprocessing.

    Note that wait_for calls start() for you.
    """
    daemon = True

    def __init__(self, filename, log_func):
        super(AsyncFileTailer, self).__init__()

        self.filename = filename
        self.log_func = log_func

    def run(self):
        offset = 0
        mtime = new_mtime = os.stat(self.filename).st_mtime

        while True:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as fp:
                    fp.seek(offset)

                    for line in fp:
                        line = line[:-1] # Trim the newline
                        self.log_func(line)

                    offset = fp.tell()

            while mtime != new_mtime:
                time.sleep(0.1)
                new_mtime = os.stat(self.filename).st_mtime


class Service(object):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.log_func = self.kwargs.get('log_func', logging.info)

    def setup_service(self):
        pass

    @abc.abstractmethod
    def cmd(self):
        pass

    def teardown_service(self):
        pass

    def validate_startup(self, filename):
        pass


class SpawnedService(threading.Thread):
    def __init__(self, service):
        super(SpawnedService, self).__init__()
        self.service = service

        self.shutting_down = False
        self.child = None
        self.wait_for_pattern = None
        self.validated = False

    def run(self):
        try:
            self.service.setup_service()
            self.child, self.log_file = wizzat.util.run_daemon(*self.service.cmd())
            self.service.validate_startup(self.log_file)
            self.service.log_func('%s -- %s', self.service.name, self.log_file)
            self.validated = True

            while self.child.poll() is None:
                time.sleep(0.1)

            if not self.shutting_down:
                raise RuntimeError("Subprocess has died.")
        finally:
            self.service.teardown_service()

    def signal(self, sig):
        if sig in (signal.SIGTERM, signal.SIGKILL):
            self.shutting_down = True

        self.child.send_signal(sig)

    def kill(self):
        self.signal(signal.SIGKILL)

    def terminate(self):
        self.signal(signal.SIGTERM)

    def pause(self):
        self.signal(signal.SIGSTOP)

    def resume(self):
        self.signal(signal.SIGCONT)

    def wait_for_validation(self, timeout):
        t1 = time.time()

        while not self.validated:
            if time.time() - t1 > timeout:
                raise StartupFailureError(self.service.name)
            time.sleep(0.1)


class SpawnedCluster(object):
    def __init__(self, *args, **kwargs):
        self.args     = args
        self.kwargs   = kwargs
        self.services = {}
        self.log_func = kwargs.get('log_func', logging.info)

        self.setup_cluster()

    def setup_cluster(self):
        pass

    def add_service(self, service, timeout):
        self.services[service.name] = ss = SpawnedService(service)
        ss.start()
        ss.wait_for_validation(timeout)
        setattr(self, service.name, ss)

    def add_services(self, services, timeout):
        end_time = time.time() + timeout

        for service in services:
            self.services[service.name] = SpawnedService(service)
            self.services[service.name].start()

            setattr(self, service.name, self.services[service.name])

        for service in services:
            self.services[service.name].wait_for_validation(end_time - time.time())

    def kill_all(self, termination_timeout):
        self.signal_all(signal.SIGKILL, termination_timeout)

    def stop_all(self, termination_timeout):
        self.signal_all(signal.SIGTERM, termination_timeout)

    def signal_all(self, signal, termination_timeout = None):
        end_time = time.time() + termination_timeout
        for name, ss in self.services.items():
            ss.signal(signal)

        if termination_timeout:
            for name, ss in self.services.items():
                ss.join(time.time() - termination_timeout)
                return False

        return True
