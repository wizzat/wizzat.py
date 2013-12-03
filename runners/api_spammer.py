import time, threading, requests, sys, collections, random, json, time
import pyutil.runner
from pyutil.mathutil import Percentile
from multiprocessing.pool import ThreadPool

class RqStats(object):
    responses    = Percentile()
    status_codes = collections.defaultdict(int)
    add_value    = responses.add_value

    def __init__(self):
        self.lock = threading.RLock()
        self.start_time = time.time()

    def end_test(self):
        self.end_time = time.time()

        duration = self.end_time - self.start_time
        print """
Test Duration:\t\t\t{duration:.2f}
Requests:\t\t\t{requests}
Req/Sec:\t\t\t{req_sec:.2f}
Pct   0:\t\t\t{pct_0}ms
Pct  25:\t\t\t{pct_25}ms
Pct  50:\t\t\t{pct_50}ms
Pct  75:\t\t\t{pct_75}ms
Pct  98:\t\t\t{pct_98}ms
Pct 100:\t\t\t{pct_100}ms
        """.format(
            duration = duration,
            requests = self.responses.num_values,
            req_sec  = self.responses.num_values / duration,
            pct_0    = self.responses.percentile(0.0) / 1000,
            pct_25   = self.responses.percentile(0.25) / 1000,
            pct_50   = self.responses.percentile(0.50) / 1000,
            pct_75   = self.responses.percentile(0.75) / 1000,
            pct_98   = self.responses.percentile(0.98) / 1000,
            pct_100  = self.responses.percentile(1.00) / 1000,
        )

        print 'Status Codes'
        for code, ct in self.status_codes.iteritems():
            print "\t{code}\t\t\t{ct}".format(code = code, ct = ct)

class APISpammer(pyutil.runner.RunnerBase):
    """
    The API spammer takes a series of files with the following format:
{ "url" : "http://localhost:8888/test_endpoint", "post_body" : "{\"abc\":1,\"def\":2}" }
{ "url" : "http://localhost:8888/test_endpoint", "post_body" : "{\"ghi\":3,\"jkl\":4}" }
{ "url" : "http://localhost:8888/test_endpoint", "post_body" : "{\"ghi\":3,\"jkl\":4}" }
...

    There is an optional header element per row which should contain a dictionary of HTTP headers.

    The default pool size is 20 and the default number of requests is 10000.

    An example output:

Test Duration:                  19.50
Requests:                       10000
Req/Sec:                        512.76
Pct   0:                        1ms
Pct  25:                        32ms
Pct  50:                        35ms
Pct  75:                        38ms
Pct  98:                        46ms
Pct 100:                        259ms

Status Codes
        200                     10000
    """
    pool_size = 20
    num_requests = 10000
    def _run(self):
        self.setup_data()
        self.stats = RqStats()

        self.pool = ThreadPool(self.pool_size)
        self.pool.map(self.make_request, self.new_request())
        self.pool.close()
        self.pool.join()

        self.stats.end_test()

    def setup_data(self):
        dt = collections.namedtuple('data', 'url header data')
        self.datas = []

        for name in self.data_names:
            with open(name, 'r') as fp:
                for row in fp:
                    row = json.loads(row)
                    self.datas.append(dt(
                        url    = row['url'],
                        header = row.get('header', {}),
                        data   = json.dumps(row.get('post_body', {}))
                    ))

    def new_request(self):
        ct = 0
        while not self.interrupted and not self.terminated and ct < self.num_requests:
            yield random.choice(self.datas)
            ct += 1

    def make_request(self, data):
        url, headers, data = data

        requested = False
        while not requested:
            try:
                rq = requests.post(url, data = data, headers = headers)
                requested = True
                with self.stats.lock:
                    self.stats.status_codes[rq.status_code] += 1
                    self.stats.add_value(rq.elapsed.microseconds)
            except (IOError, requests.ConnectionError), e:
                self.stats.status_codes['retry'] += 1

    def sig_term(self, signal, frame):
        self.terminated = True
        self.pool.terminate()

    def sig_int(self, signal, frame):
        self.interrupted = True
        self.pool.terminate()

if __name__ == '__main__':
    url, args = sys.argv[1], sys.argv[2:]
    APISpammer(
        data_names = sys.argv[1:],
    ).run()
