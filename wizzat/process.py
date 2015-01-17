from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import abc
import errno
import logging
import os
import os.path
import signal
import six
import subprocess
import tempfile
import threading
import time

subprocess_lock = threading.RLock()

def run_cmd(cmdline, env = None, shell = False):
    """
    Executes a command, returns the return code and the merged stdout/stderr contents.
    """
    global subprocess_lock
    try:
        fp = tempfile.TemporaryFile()
        with subprocess_lock:
            child = subprocess.Popen(cmdline,
                env     = env,
                shell   = shell,
                bufsize = 2,
                stdout  = fp,
                stderr  = fp,
            )

        return_code = child.wait()
        fp.seek(0, 0)
        output = fp.read()

        return return_code, output
    except OSError as e:
        if e.errno == errno.ENOENT:
            e.msg += '\n' + ' '.join(cmdline)
        raise


def run_daemon(cmdline, env = None, shell = False):
    """
    Executes a command, returns the subprocess object and the log file
    """
    global subprocess_lock
    try:
        fp = tempfile.NamedTemporaryFile(delete = False)
        with subprocess_lock:
            child = subprocess.Popen(cmdline,
                env     = env,
                shell   = shell,
                bufsize = 2,
                stdout  = fp,
                stderr  = fp,
            )

        return child, fp.name
    except OSError as e:
        if e.errno == errno.ENOENT:
            e.msg += '\n' + ' '.join(cmdline)
        raise


def log_file_updates(log_func, filename, mtime = None, offset = None):
    if not log_func or not filename or not os.path.exists(filename):
        return mtime, offset

    new_mtime = os.stat(filename).st_mtime

    if new_mtime > mtime:
        mtime = new_mtime
        with open(filename, 'r') as fp:
            fp.seek(offset)

            for line in fp:
                log_func(line[:-1])

            offset = fp.tell()

    return mtime, offset


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
        while True:
            self.mtime, self.offset = log_file_updates(
                self.log_func,
                self.filename,
                self.mtime,
                self.offset
            )

            time.sleep(0.1)


class Service(object):
    global_log = True

    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.log_file = None
        self.log_offset = 0
        self.last_mtime = None
        self.shutdown = None

    def poll(self):
        if not self.child:
            return

        logger = logging.getLogger(self.name)
        self.log_mtime, self.log_offset = log_file_updates(
            logger.info,
            self.log_file,
            self.log_mtime,
            self.log_offset,
        )

        self.update_log()
        exit_code = self.child.poll()

        if exit_code and not self.shutdown:
            logging.critical('Service failed: %d', exit_code)
            raise ServiceFailureError(self.name)

    def update_log(self):
        if not self.global_log:
            return

    def setup_service(self):
        pass

    @abc.abstractmethod
    def cmd(self):
        pass

    def teardown_service(self):
        pass

    def validate_startup(self):
        pass


class Cluster(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(Cluster, self).__init__()

        self.args = args
        self.kwargs = kwargs
        self.services = {}
        self.running = True
        self.kill = True

        self.setup_cluster()

    def run(self):
        while self.running:
            self.check_services()
            time.sleep(0.1)

        if self.kill:
            self.kill_all()
        else:
            self.stop_all()

    def join(self, timeout, kill = True):
        end_time = now() + seconds(timeout)

        self.kill = kill
        self.running = False

        while len(self.services) > 0 and now() < end_time:
            time.sleep(0.1)

    def check_services(self):
        for name, service in self.services.items():
            service.check()
            service.update_log()

    def setup_cluster(self):
        pass

    def add_service(self, service, timeout = None):
        self.run_service(service)

        self.services[service.name] = service
        self.validate_services([service], timeout)

    def add_services(self, services, timeout = None):
        for service in services:
            self.run_service(service)
            self.services[service.name] = service

        self.validate_services(services, timeout)

    def run_service(self, service):
        service.setup_service()
        cmd, env = self.service.cmd()
        service.child, service.log_file = run_daemon(cmd, env)

    def validate_services(self, services, timeout):
        end_time = now() + seconds(timeout)
        services = collections.deque(services)

        while services and now() < end_time:
            service = services.popleft()
            if not service.validate_startup():
                services.append(service)

        return len(services) == 0

    def kill_all(self):
        self.signal_all(signal.SIGKILL)

    def stop_all(self):
        self.signal_all(signal.SIGTERM)

    def signal_all(self, signal):
        for name in self.services:
            self.signal(name, signal)

    def signal(self, name, signal):
        if self.services[name].child:
            self.services[name].child.signal(signal)
