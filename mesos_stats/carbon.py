import socket
import time
import os

class Carbon:
    def __init__(self, host, prefix, pickle=False):
        self.host = host
        self.prefix = prefix
        self.port = None
        self.sock = None
        self.pickle = pickle

    def connect(self, port):
        if self.sock != None:
            raise Exception("Attempt to connect an already connected socket.")
        self.port = port
        self.sock = socket.socket()
        self.sock.connect((self.host, self.port))

    def ensure_connected(self, port):
        if self.sock == None:
            self.connect(port)
        elif self.port == port:
            return
        else:
            self.close()
            self.sock.connect(port)

    def close(self):
        self.sock.close()
        self.sock = None

    def send_metrics(self, metrics):
        ts = int(time.time())
        # Confusing control-flow...
        # If the send_metrics_pickle fails on socket.error, we
        # simply pass through to the send_metrics_plaintext.
        # If the send_metrics_pickle succeeds, we return early.
        # Well, I hope that's how it works.
        if self.pickle:
            try:
                self.send_metrics_pickle(metrics, ts)
                return
            except socket.error:
                pass
        
        self.send_metrics_plaintext(metrics, ts)

    def send_metrics_plaintext(self, metrics, ts):
        try:
            self.ensure_connected(2003)
            def send(k, v):
                print "SEND: %s = %s" % (k, v[0])
                try:
                    pkt = "%s %f %d\n" % (k, float(v[0]), ts)
                    self.sock.send(pkt)
                except TypeError:
                    print "Got %s; want float" % v
            self.forEachPrefixedMetric(metrics, send)
        finally:
            self.close()

    def send_metrics_pickle(self, metrics, ts):
        try:
            self.ensure_connected(2004)
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
