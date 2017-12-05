import socket
import struct
import time
import os
import sys
import pickle
from mesos_stats.util import log
from mesos_stats.metric import Metric

CHUNK_SIZE = 5000 # Maximum number of stats to send to Carbon in one go

class Carbon:
    def __init__(self, host, prefix, pickle=False, port=2003,
                 pickle_port=2004, dry_run=False):
        self.host = host
        self.prefix = prefix
        self.port = port
        self.sock = None
        self.pickle = pickle
        self.pickle_port = pickle_port
        self.dry_run = dry_run
        self.timeout = 30.0

    def connect(self, port):
        if self.sock != None:
            raise Exception("Attempt to connect an already connected socket.")
        self.port = port
        self.sock = socket.socket()
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

    def ensure_connected(self, port):
        if self.sock == None:
            self.connect(port)
        elif self.port != port:
            self.close()
            self.sock.connect(port)

    def close(self):
        self.sock.close()
        self.sock = None

    def send_metrics(self, metrics, timeout, timestamp):
        num_datapoints = sum([len(m.data) if isinstance(m, Metric) else 1
                              for m in metrics])
        log('Sending {} datapoints to Carbon'.format(num_datapoints))
        if self.dry_run: # Don't do anything in test mode
            log('DRY RUN - Not sending stats to carbon')
            return
        self.timeout = timeout
        ts = int(timestamp)
        if self.pickle:
            self.send_metrics_pickle(metrics, ts)
            return
        else:
            self.send_metrics_plaintext(metrics, ts)

    def _chunks(self, l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def send_metrics_plaintext(self, metrics, ts):
        log('Send metrics via Plaintext')
        try:
            self.ensure_connected(self.port)
            self.all_stats = []
            for m in metrics:
                for k, v in m.Results():
                    self.all_stats.append("{}.{} {} {}".format(self.prefix,
                                                               k, v, ts))
            l = len(self.all_stats)
            iterations = 0
            for chunk in self._chunks(self.all_stats, CHUNK_SIZE):
                iterations += 1
                data = "\n".join(chunk) + '\n'
                sent = self.sock.sendall(data.encode())
                if sent == 0:
                    raise RuntimeError("socket connection broken")
            if iterations != 1:
                log("INFO: Send took %s iterations" % iterations)
        finally:
            self.close()

    def send_metrics_pickle(self, metrics, ts):
        log('Send metrics via Pickle')
        try:
            self.ensure_connected(self.pickle_port)
            tuples = []
            ts = int(time.time())
            def addToTuple(k, v):
                tuples.append((k, (ts, v)))
            self.forEachPrefixedMetric(metrics, addToTuple)
            payload = pickle.dumps(tuples, protocol=2)
            header = struct.pack("!L", len(payload))
            message = header + payload
            self.sock.send(message)
        finally:
            self.close()

    def forEachPrefixedMetric(self, metrics, f):
        for m in metrics:
            for r in m.Results():
                k, v = r
                f(self.prefix + "." + k, v)
