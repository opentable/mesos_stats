import socket
import struct
import pickle
import queue
from mesos_stats.util import log

CHUNK_SIZE = 500  # Maximum number of stats to send to Carbon in one go


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
        if self.sock is not None:
            raise Exception("Attempt to connect an already connected socket.")
        self.port = port
        self.sock = socket.socket()
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

    def ensure_connected(self, port):
        if self.sock is None:
            self.connect(port)
        elif self.port != port:
            self.close()
            self.sock.connect(port)

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def send_metrics(self, metrics, timeout):
        self.timeout = timeout
        iterations = 1
        total = 0
        while True:
            chunk = self._get_chunk_from_queue(metrics, CHUNK_SIZE)
            if not self.dry_run:
                if self.pickle:
                    self.send_metrics_pickle(chunk)
                else:
                    self.send_metrics_plaintext(chunk)
            iterations += 1
            total += len(chunk)
            if len(chunk) < CHUNK_SIZE:
                break
        if iterations != 1:
            log("INFO: Send took %s iterations" % iterations)
        log('Sent {} datapoints to Carbon'.format(total))
        self.close()

    def _add_prefix(self, metric):
        if self.pickle:
            return ('{}.{}'.format(self.prefix, metric[0]),
                    (metric[1][0], metric[1][1]))
        else:
            return '{}.{}'.format(self.prefix, metric)

    def _get_chunk_from_queue(self, q, n):
        '''
        returns list with n number of metrics from queue
        if queue has less than n metrics, it will return everything
        optionally adds a prefix to the metric name
        '''
        res = []
        while True:
            try:
                m = q.get(block=False)
            except queue.Empty:
                break
            if self.prefix:
                m = self._add_prefix(m)
            res.append(m)
            if len(res) == n:
                break
        return res

    def send_metrics_plaintext(self, metrics_list):
        log('Sending {} metrics via Plaintext'.format(len(metrics_list)))
        self.ensure_connected(self.port)

        data = []
        data = "\n".join(metrics_list) + '\n'
        try:
            sent = self.sock.sendall(data.encode())
        except socket.error as e:  # Just retry once
            log('Error during send, Reconnecting')
            self.connect(self.port)
            sent = self.sock.sendall(data.encode())
        if sent == 0:
            raise RuntimeError("socket connection broken")
        return

    def send_metrics_pickle(self, metrics_list):
        log('Send metrics via Pickle')
        self.ensure_connected(self.pickle_port)
        assert(isinstance(metrics_list[0], tuple))
        payload = pickle.dumps(metrics_list, protocol=2)
        header = struct.pack("!L", len(payload))
        message = header + payload
        try:
            self.sock.send(message)
        except socket.error as e:  # Just retry once
            log('Error during send, Reconnecting')
            self.connect(self.port)
            sent = self.sock.send(message)
        if sent == 0:
            raise RuntimeError("socket connection broken")
        return
