from metric import Metric, Each
from mesos_stats import log, try_get_json
import requests

class Mesos:
    def __init__(self, master_pid):
        self.leader_pid = master_pid
        self.leader_state = None
        self.slave_states = None

    def reset(self):
        self.leader_state = None
        self.slave_states = None

    def get_master_state(self):
        url = "http://%s/state.json" % self.leader_pid
        log("Getting master state from: %s" % url)
        return try_get_json(url)
    
    def state(self):
        if self.leader_state != None:
            return self.leader_state
        master = self.get_master_state()
        if master == None:
            return None
        if master["pid"] == master["leader"]:
            self.leader_state = master
        else:
            self.leader_pid = master["leader"]
        return self.state()

    def get_cluster_stats(self):
        return try_get_json("http://%s/metrics/snapshot" % self.state()["leader"])

    def get_slave(self, slave_pid):
        return try_get_json("http://%s/metrics/snapshot" % slave_pid)

    def get_slave_statistics(self, slave_pid):
        return try_get_json("http://%s/monitor/statistics.json" % slave_pid)
    
    def slaves(self):
        if self.slave_states != None:
            return self.slave_states
        self.slave_states = {}
        for slave in self.state()["slaves"]:
            slave_pid = slave["pid"]
            log("Getting stats for %s" % slave_pid)
            slave_state = self.get_slave(slave_pid)
            if slave == None:
                log("Slave lost: %s" % slave_pid)
                continue
            tasks = self.get_slave_statistics(slave_pid)
            self.slave_states[slave_pid] = slave_state
            self.slave_states[slave_pid]["task_details"] = tasks

        return self.slave_states

def slave_metrics(mesos):
    metrics = [
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

    for pid in mesos.slaves():
        slave = mesos.slaves()[pid]
        for m in metrics:
            m.Add(slave, [pid])

    return metrics

def slave_task_metrics(mesos):
    ms = []
    for pid in mesos.slaves():
        slave = mesos.slaves()[pid]
        prefix = "slave.[].executors.singularity.tasks.[]"
        metrics = [
            Metric("cpus_system_time_secs", prefix + ".cpus.system_time_secs", Each()),
            Metric("cpus_user_time_secs",   prefix + ".cpus.user_time_secs",   Each()),
            Metric("cpus_limit",            prefix + ".cpus.limit",            Each()),
            Metric("mem_limit_bytes",       prefix + ".mem.limit_bytes",       Each()),
            Metric("mem_rss_bytes",         prefix + ".mem.rss_bytes",         Each()),
        ]
        for m in metrics:
            for t in slave["task_details"]:
                # TODO:m.Add(t, "key1", "key2", ...)
                m.Add(t["statistics"], [pid, t["executor_id"]])

        ms += metrics

    return ms


def cluster_metrics(mesos):
    metrics = [
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

    for m in metrics:
        m.Add(mesos.get_cluster_stats())

    return metrics
   

    
