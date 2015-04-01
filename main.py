#!/usr/bin/env python
import json
import requests
import socket
import time
import sys
import pickle
import struct

if len(sys.argv) < 4:
    log("Usage: %s <mesos master host> <carbon host> <graphite prefix> [period seconds=60]" % sys.argv[0])
    sys.exit(1)

masterHost = sys.argv[1]
carbonHost = sys.argv[2]
graphitePrefix = sys.argv[3]
try:
    period = float(sys.argv[4])
except:
    period = 60

class carbon:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = socket.socket()
        self.sock.connect((host, port))
    def send(self, message):
        self.sock.send(message)
    def close(self):
        self.sock.close()

def send_metrics_slow(metrics):
    ts = int(time.time())
    c = carbon(carbonHost, 2003)
    for m in metrics:
        for r in m.Results():
            k, v = r
            #print "%s = %d" % (k, v)
            c.send("%s %d %d\n" % (k, v, ts))
    c.close()

def send_metrics(metrics):
    tuples = []
    ts = int(time.time())
    for m in metrics:
        for r in m.Results():
            k, v = r
            tuples.append((k, (ts, v))) 
    #print tuples
    payload = pickle.dumps(tuples, protocol=2)
    header = struct.pack("!L", len(payload))
    message = header + payload
    c = carbon(carbonHost, 2004)
    c.send(message)
    c.close()

def try_get_json(url):
    try:
        return json.loads(requests.get(url).text)
    except requests.exceptions.ConnectionError as e:
        log("GET %s failed: %s" % (url, e))
        return None

def get_slave_stats(slavePID):
    return try_get_json("http://%s/metrics/snapshot" % slavePID)

def get_cluster_metrics(leaderPID):
    return try_get_json("http://%s/metrics/snapshot" % leaderPID)

def get_master_state(masterPID):
    url = "http://%s/state.json" % masterPID
    log("Getting master state from: %s" % url)
    return try_get_json(url)

def get_leader_state(masterurl):
    masterstate = get_master_state(masterurl)
    if masterstate == None:
        return None
    if masterstate["pid"] == masterstate["leader"]:
        return masterstate
    return get_master_state(masterstate["leader"])
    

class Metric:
    def __init__(self, path, name, *measurements):
        self.name = graphitePrefix + "." + name
        self.path = path
        self.measurements = measurements
        self.data = []

    def Add(self, slave): self.data.append(slave[self.path])

    def Sum(self): return sum(self.data)

    def DatapointName(self, dp):
        return self.name.replace("[]", dp)

    def Datapoint(self, name, value):
        return (self.DatapointName(name), value)

    def Results(self):
        results = []
        for f in self.measurements:
            results += f(self)
        return results

def Sum(metric):
    result = sum(metric.data)    
    return [metric.Datapoint("sum", result)]

def Mean(metric):
    d = metric.data
    result = float(sum(d))/len(d) if len(d) > 0 else float('nan')
    return [metric.Datapoint("mean", result)]

def Each(scale=1):
    def Each_scale(metric):
        results = []
        for i, d in enumerate(metric.data):
            results.append(metric.Datapoint("{0}".format(i), metric.data[i]*scale))
        return results
    return Each_scale

def makeSlaveMetrics():
    return [
        Metric("slave/mem_total",       "slave.[].mem.total",               Each()),
        Metric("slave/mem_used",        "slave.[].mem.used",                Each()),
        Metric("slave/mem_percent",     "slave.[].mem.percent",             Each(scale=100)),
        Metric("slave/cpus_total",      "slave.[].cpus.total",              Each()),
        Metric("slave/cpus_used",       "slave.[].cpus.used",               Each()),
        Metric("slave/cpus_percent",    "slave.[].cpus.percent",            Each(scale=100)),
        Metric("slave/disk_total",      "slave.[].disk.total",              Each()),
        Metric("slave/disk_used",       "slave.[].disk.total",              Each()),
        Metric("slave/tasks_running",   "slave.[].tasks.running",           Each()),
        Metric("slave/tasks_staging",   "slave.[].tasks.staging",           Each()),
        Metric("system/load_1min",      "slave.[].system.load.1min",        Each(scale=1000)),
        Metric("system/load_5min",      "slave.[].system.load.5min",        Each(scale=1000)),
        Metric("system/load_15min",     "slave.[].system.load.15min",       Each(scale=1000)),
        Metric("system/mem_free_bytes", "slave.[].system.mem.free.bytes",   Each()),
        Metric("system/mem_total_bytes","slave.[].system.mem.total.bytes",  Each()),
    ]

def makeClusterMetrics():
    return [
            Metric("master/cpus_percent",     "cluster.cpus.percent",     Each(scale=100)),
            Metric("master/cpus_total",       "cluster.cpus.total",       Each()),
            Metric("master/cpus_used",        "cluster.cpus.used",        Each()),
            Metric("master/mem_percent",      "cluster.mem.percent",      Each(scale=100)),
            Metric("master/mem_total",        "cluster.mem.total",        Each()),
            Metric("master/mem_used",         "cluster.mem.used",         Each()),
            Metric("master/disk_percent",     "cluster.disk.percent",     Each(scale=100)),
            Metric("master/disk_total",       "cluster.disk.total",       Each()),
            Metric("master/disk_used",        "cluster.disk.used",        Each()),
            Metric("master/slaves_connected", "cluster.slaves.connected", Each()),
            Metric("master/tasks_failed",     "cluster.tasks.failed",     Each()),
            Metric("master/tasks_finished",   "cluster.tasks.finished",   Each()),
            Metric("master/tasks_killed",     "cluster.tasks.killed",     Each()),
            Metric("master/tasks_lost",       "cluster.tasks.lost",       Each()),
            Metric("master/tasks_running",    "cluster.tasks.running",    Each()),
            Metric("master/tasks_staging",    "cluster.tasks.staging",    Each()),
            Metric("master/tasks_starting",   "cluster.tasks.starting",   Each()),
    ]



def get_cluster_stats(masterPID):
    leader = get_leader_state(masterPID)
    if leader == None:
        log("No leader found")
        return None
    
    clusterMetrics = makeClusterMetrics()
    cluster = get_cluster_metrics(leader["pid"])
    for m in clusterMetrics:
        m.Add(cluster)

    slaveMetrics = makeSlaveMetrics()

    for s in leader["slaves"]:
        log("Getting stats for %s" % s["pid"])
        slave = get_slave_stats(s["pid"])
        if slave == None:
            log("Slave lost")
            continue
        for m in slaveMetrics:
            m.Add(slave)

    allMetrics = clusterMetrics + slaveMetrics

    try:
        send_metrics(allMetrics)
    except socket.error:
        log("WARNING: Unable to send pickled stats on port 2004; Attempting plaintext (slow) on 2003...")
        try:
            send_metrics_slow(allMetrics)
        except socket.error:
            log("ERROR: Unable to send plantext on port 2003")

    log("Metrics sent.")
    return leader["pid"]
    
def log(message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print '%s %s' % (ts, message)

try:
    while True:
        # get_cluster_stats returns the leader pid, so we don't have to repeat leader
        # lookup each time
        masterHostPort = masterHost + ":5050"
        leaderHostPort = get_cluster_stats(masterHostPort)
        if leaderHostPort != None:
            masterHostPort = leaderHostPort
        else:
            log("No stats this time; sleeping")
        time.sleep(period)
except KeyboardInterrupt, SystemExit:
    print "Bye!"
