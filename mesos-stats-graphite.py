#!/usr/bin/env python
import json
import requests
import socket
import time
import sys

if len(sys.argv) < 4:
    print "Usage: %s <mesos master host> <carbon host> <graphite prefix> [period seconds=60]" % sys.argv[0]
    sys.exit(1)

masterHost = sys.argv[1]
carbonHost = sys.argv[2]
graphitePrefix = sys.argv[3]
try:
    period = float(sys.argv[4])
except:
    period = 60
carbonPort = 2003

def collect_metric(name, value, timestamp):
    sock = socket.socket()
    sock.connect((carbonHost, carbonPort))
    name = graphitePrefix + "." + name
    sock.send("%s %d %d\n" % (name, value, timestamp))
    sock.close()

def try_get_json(url):
    try:
        return json.loads(requests.get(url).text)
    except requests.exceptions.ConnectionError as e:
        print "GET %s failed: %s" % (url, e)
        return None

def get_slave_stats(slavePID):
    return try_get_json("http://%s/stats.json" % slavePID)

def get_master_state(masterPID):
    url = "http://%s/state.json" % masterPID
    print "Getting master state from: %s" % url
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
        self.name = name
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

def Each(metric):
    results = []
    for i, d in enumerate(metric.data):
        results.append(metric.Datapoint("{0}".format(i), metric.data[i]))
    return results

def makeMetrics():
    return [
        Metric("slave/mem_total",       "slave.[].mem.total",               Each),
        Metric("slave/mem_used",        "slave.[].mem.used",                Each),
        Metric("slave/mem_percent",     "slave.[].mem.percent",             Each),
        Metric("slave/cpus_total",      "slave.[].cpus.total",              Each),
        Metric("slave/cpus_used",       "slave.[].cpus.used",               Each),
        Metric("slave/cpus_percent",    "slave.[].cpus.percent",            Each),
        Metric("slave/disk_total",      "slave.[].disk.total",              Each),
        Metric("slave/disk_used",       "slave.[].disk.total",              Each),
        Metric("slave/tasks_running",   "slave.[].tasks.running",           Each),
        Metric("staged_tasks",          "slave.[].tasks.staged",            Each),
        Metric("system/load_1min",      "slave.[].system.load.1min",        Each),
        Metric("system/load_5min",      "slave.[].system.load.5min",        Each),
        Metric("system/load_15min",     "slave.[].system.load.15min",       Each),
        Metric("system/mem_free_bytes", "slave.[].system.mem.free.bytes",   Each),
        Metric("system/mem_total_bytes","slave.[].system.mem.total.bytes",  Each),
    ]


def get_cluster_stats(masterPID):
    leader = get_leader_state(masterPID)
    if leader == None:
        print "No leader found"
        return None
    
    totalMem, usedMem, totalCPU, usedCPU, totalDisk, usedDisk = (0, 0, 0, 0, 0, 0)

    metrics = makeMetrics()

    for s in leader["slaves"]:
        print "Getting stats for %s" % s["pid"]
        slave = get_slave_stats(s["pid"])
        if slave == None:
            print "Slave lost"
            continue
        for m in metrics:
            m.Add(slave)

    ts = int(time.time())
    for m in metrics:
        for r in m.Results():
            k, v = r
            print "{0} = {1}".format(k, v)
            collect_metric(k, v, ts)

    return leader["pid"]
    
try:
    while True:
        # get_cluster_stats returns the leader pid, so we don't have to repeat leader
        # lookup each time
        masterHostPort = masterHost + ":5050"
        leaderHostPort = get_cluster_stats(masterHostPort)
        if leaderHostPort != None:
            masterHostPort = leaderHostPort
        else:
            print "No stats this time; sleeping"
        time.sleep(period)
except KeyboardInterrupt, SystemExit:
    print "Bye!"
