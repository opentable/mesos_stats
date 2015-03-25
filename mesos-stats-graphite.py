#!/usr/bin/env python
import json
import requests
import socket
import time
import sys

if len(sys.argv) < 5:
    print "Usage: <mesos master host> <graphite host> <graphite port> <graphite prefix> [period seconds=60]"
    sys.exit(1)

masterHost = sys.argv[1] + ":5050"
graphiteHost = sys.argv[2]
graphitePort = int(sys.argv[3])
graphitePrefix = sys.argv[4]
try:
    period = float(sys.argv[5])
except:
    period = 60

def collect_metric(name, value, timestamp):
    sock = socket.socket()
    sock.connect((graphiteHost, graphitePort))
    name = graphitePrefix + "." + name
    sock.send("%s %d %d\n" % (name, value, timestamp))
    sock.close()


def get_slave_stats(slavePID):
    return json.loads(requests.get("http://%s/stats.json" % slavePID).text)

def get_master_state(masterPID):
    url = "http://%s/state.json" % masterPID
    print "Getting master state from: %s" % url
    return json.loads(requests.get(url).text)

def get_leader_state(masterurl):
    masterstate = get_master_state(masterurl)
    if masterstate["pid"] == masterstate["leader"]:
        return masterstate
    return get_master_state(masterstate["leader"])
    

def get_cluster_stats(masterPID):
    leader = get_leader_state(masterPID)
    totalMem, usedMem, totalCPU, usedCPU, totalDisk, usedDisk = (0, 0, 0, 0, 0, 0)

    for s in leader["slaves"]:
        print "Getting stats for %s" % s["pid"]
        url = "http://%s/stats.json" % s["pid"]
        slave = get_slave_stats(s["pid"])
        totalMem += slave["slave/mem_total"]
        usedMem  += slave["slave/mem_used"]
        totalCPU += slave["slave/cpus_total"]
        usedCPU  += slave["slave/cpus_used"]
        totalDisk+= slave["slave/disk_total"]
        usedDisk += slave["slave/disk_used"]

    print "MEM %s/%s; CPU %s/%s; Disk %s/%s" % (usedMem,totalMem,usedCPU,totalCPU,usedDisk,totalDisk)

    # hierarchy: mesos.qa-uswest2.mem.total
    #            mesos.qa-uswest2.mem.used
    ts = int(time.time())
    collect_metric("mem.total", totalMem, ts)
    collect_metric("mem.used", usedMem, ts)
    collect_metric("cpu.total", totalCPU, ts)
    collect_metric("cpu.used", usedCPU, ts)
    collect_metric("disk.total", totalDisk, ts)
    collect_metric("disk.used", usedDisk, ts)
    
    return leader["pid"]
    
try:
    while True:
        # get_cluster_stats returns the leader pid, so we don't have to repeat leader
        # lookup each time
        masterHostPort = get_cluster_stats(masterHostPort)
        time.sleep(period)
except KeyboardInterrupt, SystemExit:
    print "Bye!"
