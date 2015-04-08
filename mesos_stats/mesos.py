from metric import Metric, Each
from mesos_stats import log, try_get_json
import requests

class Mesos:
    def __init__(self, master_pid):
        self.master_pid = master_pid
        self.leader_pid = master_pid

    def get_slave_stats(self, slave_pid):
        return try_get_json("http://%s/metrics/snapshot" % slave_pid)
    
    def get_master_state(self):
        url = "http://%s/state.json" % self.master_pid
        log("Getting master state from: %s" % url)
        return try_get_json(url)
    
    def get_leader_state(self):
        master = self.get_master_state()
        if master == None:
            return None
        if master["pid"] == master["leader"]:
            return master
        self.master_pid = master["pid"]
        return self.get_leader_state()
    
    def get_cluster_metrics(self):
        leader = self.get_leader_state()
        if leader == None:
            raise Exception("No leader found.")

        clusterMetrics = self.makeClusterMetrics()
        url = "http://%s/metrics/snapshot" % self.leader_pid
        cluster = try_get_json(url)
        for m in clusterMetrics:
            m.Add(cluster)
    
        slaveMetrics = self.makeSlaveMetrics()
        for s in leader["slaves"]:
            log("Getting stats for %s" % s["pid"])
            slave = self.get_slave_stats(s["pid"])
            if slave == None:
                log("Slave lost")
                continue
            for m in slaveMetrics:
                m.Add(slave)

        allMetrics = clusterMetrics + slaveMetrics

        return allMetrics
    
        #try:
        #    send_metrics(allMetrics)
        #except socket.error:
        #    log("WARNING: Unable to send pickled stats on port 2004; Attempting plaintext (slow) on 2003...")
        #    try:
        #        send_metrics_slow(allMetrics)
        #    except socket.error:
        #        log("ERROR: Unable to send plantext on port 2003")
    
        log("Metrics sent.")
        return leader["pid"]
        
    def makeSlaveMetrics(self):
        return [
            Metric("slave/mem_total",        "slave.[].mem.total",               Each()),
            Metric("slave/mem_used",         "slave.[].mem.used",                Each()),
            Metric("slave/mem_percent",      "slave.[].mem.percent",             Each(scale=100)),
            Metric("slave/cpus_total",       "slave.[].cpus.total",              Each()),
            Metric("slave/cpus_used",        "slave.[].cpus.used",               Each()),
            Metric("slave/cpus_percent",     "slave.[].cpus.percent",            Each(scale=100)),
            Metric("slave/disk_total",       "slave.[].disk.total",              Each()),
            Metric("slave/disk_used",        "slave.[].disk.total",              Each()),
            Metric("slave/tasks_running",    "slave.[].tasks.running",           Each()),
            Metric("slave/tasks_staging",    "slave.[].tasks.staging",           Each()),
            Metric("system/load_1min",       "slave.[].system.load.1min",        Each(scale=1000)),
            Metric("system/load_5min",       "slave.[].system.load.5min",        Each(scale=1000)),
            Metric("system/load_15min",      "slave.[].system.load.15min",       Each(scale=1000)),
            Metric("system/mem_free_bytes",  "slave.[].system.mem.free.bytes",   Each()),
            Metric("system/mem_total_bytes", "slave.[].system.mem.total.bytes",  Each()),
        ]
    
    def makeClusterMetrics(self):
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
    
    
