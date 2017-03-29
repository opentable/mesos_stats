from .metric import Metric, Each
from .util import log, try_get_json
import sys

class Mesos:
    def __init__(self, master_pid):
        self.original_master_pid = master_pid
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
        try:
            master = self.get_master_state()
        except:
            if self.leader_pid == self.original_master_pid:
                log("Already using originally configured mesos master pid.")
                raise MesosStatsException("Unable to locate any Mesos master at %s" % self.original_master_pid)
            log("Unable to hit last known leader, falling back to configured master: %s" % self.original_master_pid)
            self.leader_pid = self.original_master_pid
            return self.state()
        if master == None:
            return None
        if master["pid"] == master["leader"]:
            self.leader_state = master
            return self.leader_state
        else:
            self.leader_pid = master["leader"]
        return self.state()

    def get_cluster_stats(self):
        if self.state() == None:
            return None
        return try_get_json("http://%s/metrics/snapshot" % self.state()["leader"])

    def get_slave(self, slave_pid):
        return try_get_json("http://%s/metrics/snapshot" % slave_pid)

    def get_slave_statistics(self, slave_pid):
        return try_get_json("http://%s/monitor/statistics.json" % slave_pid)
    
    def slaves(self):
        if self.slave_states != None:
            return self.slave_states
        self.slave_states = {}
        if self.state() == None:
            return []
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

class MesosStatsException(Exception):
    pass

def slave_metrics(mesos):
    metrics = [
        Metric("slave/mem_total",        "slave.[].mem.total"),
        Metric("slave/mem_used",         "slave.[].mem.used"),
        Metric("slave/mem_percent",      "slave.[].mem.percent", Each(scale=100)),
        Metric("slave/cpus_total",       "slave.[].cpus.total"),
        Metric("slave/cpus_used",        "slave.[].cpus.used"),
        Metric("slave/cpus_percent",     "slave.[].cpus.percent", Each(scale=100)),
        Metric("slave/disk_total",       "slave.[].disk.total"),
        Metric("slave/disk_used",        "slave.[].disk.total"),
        Metric("slave/tasks_running",    "slave.[].tasks.running"),
        Metric("slave/tasks_staging",    "slave.[].tasks.staging"),
        Metric("system/load_1min",       "slave.[].system.load.1min"),
        Metric("system/load_5min",       "slave.[].system.load.5min"),
        Metric("system/load_15min",      "slave.[].system.load.15min"),
        Metric("system/mem_free_bytes",  "slave.[].system.mem.free.bytes"),
        Metric("system/mem_total_bytes", "slave.[].system.mem.total.bytes"),
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
            Metric("cpus_system_time_secs", prefix + ".cpus.system_time_secs"),
            Metric("cpus_user_time_secs",   prefix + ".cpus.user_time_secs"),
            Metric("cpus_limit",            prefix + ".cpus.limit"),
            Metric("mem_limit_bytes",       prefix + ".mem.limit_bytes"),
            Metric("mem_rss_bytes",         prefix + ".mem.rss_bytes"),
        ]
        for m in metrics:
            for t in slave["task_details"]:
                # TODO:m.Add(t, "key1", "key2", ...)
                m.Add(t["statistics"], [pid, t["executor_id"]])

        ms += metrics

    return ms


def cluster_metrics(mesos):
    metrics = [
        Metric("master/cpus_percent",     "cluster.cpus.percent", Each(scale=100)),
        Metric("master/cpus_total",       "cluster.cpus.total"),
        Metric("master/cpus_used",        "cluster.cpus.used"),
        Metric("master/mem_percent",      "cluster.mem.percent", Each(scale=100)),
        Metric("master/mem_total",        "cluster.mem.total"),
        Metric("master/mem_used",         "cluster.mem.used"),
        Metric("master/disk_percent",     "cluster.disk.percent", Each(scale=100)),
        Metric("master/disk_total",       "cluster.disk.total"),
        Metric("master/disk_used",        "cluster.disk.used"),
        Metric("master/slaves_connected", "cluster.slaves.connected"),
        Metric("master/tasks_failed",     "cluster.tasks.failed"),
        Metric("master/tasks_finished",   "cluster.tasks.finished"),
        Metric("master/tasks_killed",     "cluster.tasks.killed"),
        Metric("master/tasks_lost",       "cluster.tasks.lost"),
        Metric("master/tasks_running",    "cluster.tasks.running"),
        Metric("master/tasks_staging",    "cluster.tasks.staging"),
        Metric("master/tasks_starting",   "cluster.tasks.starting"),
    ]

    for m in metrics:
        m.Add(mesos.get_cluster_stats())

    return metrics
   

    
