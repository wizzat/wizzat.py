import Queue, gzip, time, threading, os, fcntl, shutil, json
from util import mkdirp

__all__ = [
    'QueueFile',
]

class QueueFile(object):
    """
        Thread and multiprocess safe queued line writer.

        Arguments:
            filename    - the file to be processed.  Repeatedly opening the same file should have no effect.
            murder_time - the time at which to stop writing to this file and begin throwing IOErrors.

        Important Methods:
            qf.write(s) - Write the string to the file
            qf.write_json(js) - json.dump and write the resulting string to the file
            qf.close() - Close the underlying file and terminate the threads

        Example usage:

        class SpammerThread(threading.Thread):
            def run(self):
                r = range(10)
                chunk = "".join([ random.choice(r)) for _ in xrange(2048) ])
                f = QueueFile("asdf")
                for _ in xrange(10000):
                    f.write(chunk)

            @classmethod
            def launch(cls):
                thread = cls()
                thread.start()
                return thread

        threads = [ SpammerThread.launch() for x in xrange(20) ]
        for t in threads:
            t.join()
        sort asdf | uniq -c


    """
    class QueueWriter(threading.Thread):
        daemon = True
        def run(self):
            while self.enabled:
                self.flush_queue()
                time.sleep(.25)

            self.flush_queue()

        def flush_queue(self):
            fp = open(self.filename, 'a')
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
            try:
                while True:
                    s = self.write_queue.get(False)
                    fp.write(s)
                    fp.write("\n")
            except Queue.Empty as e:
                pass
            finally:
                fp.flush()
                fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
                fp.close()

    class WriterKiller(threading.Thread):
        daemon = True
        def run(self):
            time.sleep(self.murder_time - time.time())
            self.victim.close()

    init_lock = threading.RLock()
    writers   = {}
    killers   = {}

    def __init__(self, output_filename, murder_time = None):
        self.init_lock.acquire()
        if output_filename not in self.writers:
            mkdirp(os.path.dirname(output_filename))
            self.writers[output_filename] = writer = self.QueueWriter()

            writer.write_queue = Queue.Queue()
            writer.filename    = output_filename
            writer.daemon      = True
            writer.enabled     = True

            if murder_time and murder_time > time.time():
                self.killers[output_filename] = killer = self.WriterKiller()
                killer.murder_time = murder_time
                killer.daemon      = True
                killer.victim      = self
                killer.start()

            writer.start()
        self.init_lock.release()

        self.writer          = self.writers[output_filename]
        self.output_filename = output_filename

    def write(self, obj):
        if not self.writer.enabled:
            raise IOError("Closed file")
        self.writer.write_queue.put(obj)

    def write_json(self, obj):
        if not self.writer.enabled:
            raise IOError("Closed file")
        self.write(json.dumps(obj))

    def close(self):
        self.close_file(self.output_filename)

    @classmethod
    def close_all(cls):
        for filename in cls.writers:
            cls.close_file(filename)

    @classmethod
    def close_file(cls, filename):
        writer         = cls.writers[filename]
        writer.enabled = False
        writer.join()

        if filename.endswith('.gz'):
            read_fp = open(filename, 'r')
            gzip_fp = gzip.open(filename + '.tmp', 'wb')
            gzip_fp.writelines(read_fp)
            gzip_fp.close()
            read_fp.close()
            shutil.move(gzip_fp.name, filename)
