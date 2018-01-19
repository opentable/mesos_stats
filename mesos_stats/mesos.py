import sys
import re
import functools
from multiprocessing import Pool
from .metric import Metric, Each
from .util import log, try_get_json

POOL_SIZE = 10 # Number of parallel processes to query Mesos

class Mesos:
    def __init__(self, master_pid):
        self.original_master_pid = master_pid
        self.leader_pid = master_pid
        self.leader_state = None
        self.slave_states = None


    def show_cache_info(self):
        log("get_slave cache : {}".format(self.get_slave.cache_info()))
        log("get_slave_stats cache : {}".format(self.get_slave.cache_info()))
        log("get_cluster_stats cache : {}".format(self.get_slave.cache_info()))


    def reset(self):
        self.show_cache_info()
        self.leader_state = None
        self.slave_states = None
        self.get_slave.cache_clear()
        self.get_slave_statistics.cache_clear()
        self.get_cluster_stats.cache_clear()


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
                raise MesosStatsException("Unable to locate any Mesos master \
                                    at {}".format(self.original_master_pid))
            log("Unable to hit last known leader, falling back to configured \
                master: {}".format(self.original_master_pid))
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


    @functools.lru_cache(maxsize=None)
    def get_cluster_stats(self):
        if self.state() == None:
            return None
        return try_get_json("http://%s/metrics/snapshot" \
                            % self.state()["leader"])

    @functools.lru_cache(maxsize=None)
    def get_slave(self, slave_pid):
        return try_get_json("http://%s/metrics/snapshot" % slave_pid)


    @functools.lru_cache(maxsize=None)
    def get_slave_statistics(self, slave_pid):
        return try_get_json("http://%s/monitor/statistics.json" % slave_pid)


    def _update_slave_state(self, slave_state):
        slave_pid = slave_state['pid']
        if slave_state == None:
            log("Slave lost: %s" % slave_pid)
            return
        slave_state = self.get_slave(slave_pid)
        tasks = self.get_slave_statistics(slave_pid)
        return(slave_pid, slave_state, tasks)


    def slaves(self):
        if self.slave_states != None:
            return self.slave_states
        self.slave_states = {}
        if self.state() == None:
            return []
        with Pool(processes=POOL_SIZE, maxtasksperchild=1) as pool:
            result = pool.map(self._update_slave_state,
                              self.state()["slaves"], chunksize=1)
        '''
        with Pool(processes=POOL_SIZE) as pool:
            result = pool.map(self._update_slave_state,
                              self.state()["slaves"])
        '''
        for (slave_pid, slave_state, tasks) in result:
            self.slave_states[slave_pid] = slave_state
            self.slave_states[slave_pid]["task_details"] = tasks

        return self.slave_states


class MesosStatsException(Exception):
    pass


def slave_metrics(mesos):
    metrics = [
        Metric("slave/mem_total",        "slave.[].mem.total"),
        Metric("slave/mem_used",         "slave.[].mem.used"),
        Metric("slave/mem_percent",      "slave.[].mem.percent",
               Each(scale=100)),
        Metric("slave/cpus_total",       "slave.[].cpus.total"),
        Metric("slave/cpus_used",        "slave.[].cpus.used"),
        Metric("slave/cpus_percent",     "slave.[].cpus.percent",
               Each(scale=100)),
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


def old_slave_task_metrics(mesos):
    '''
    Add old style task metrics so we are backward compatible
    '''
    prefix = "slave.[].executors.singularity.tasks.[]"
    metrics = [
        Metric("cpus_system_time_secs", prefix + ".cpus.system_time_secs"),
        Metric("cpus_user_time_secs",   prefix + ".cpus.user_time_secs"),
        Metric("cpus_limit",            prefix + ".cpus.limit"),
        Metric("mem_limit_bytes",       prefix + ".mem.limit_bytes"),
        Metric("mem_rss_bytes",         prefix + ".mem.rss_bytes"),
    ]
    for pid in mesos.slaves():
        slave = mesos.slaves()[pid]
        # This is the old schema to retain for backwards compatibility
        # The full task names are used
        for m in metrics:
            for t in slave["task_details"]:
                m.Add(t["statistics"], [pid, t["executor_id"]])
    num_metrics = sum([len(m.data) for m in metrics])
    log('Number of old-style task metrics : {}'.format(num_metrics))
    return metrics


def new_slave_task_metrics(mesos, requests_json=None):
    '''
    This is the new schema
    Create a metrics schema that is independent of slave IDs and Teamcity
    version numbers.
    e.g. mesos_stats.(env).tasks.(task_name)-(instance).mem.limit_bytes
    '''
    # We rely on 2 ways to get the request ID to use in the Graphite schema
    # The first way relies no regex which covers 99% of the cases
    regex = re.compile(r'(?P<task_name>\S+)-'
                       'teamcity\S+-(?P<instance_no>\d{1,2})'
                       '-mesos_slave\S+'
    )
    # The second way just compares the task name to see if it has any of the
    # request names as a prefix
    if requests_json:
        requests = [r['request']['id'] for r in requests_json]
    else:
        requests = []

    tc_prefix = "tasks.[]"
    tc_metrics = [
        Metric("cpus_system_time_secs", tc_prefix + ".cpus.system_time_secs"),
        Metric("cpus_user_time_secs", tc_prefix + ".cpus.user_time_secs"),
        Metric("cpus_limit", tc_prefix + ".cpus.limit"),
        Metric("mem_limit_bytes", tc_prefix + ".mem.limit_bytes"),
        Metric("mem_rss_bytes", tc_prefix + ".mem.rss_bytes"),
    ]
    for pid in mesos.slaves():
        slave = mesos.slaves()[pid]
        for m in tc_metrics:
            for t in slave["task_details"]:
                match = regex.search(t["executor_id"])
                if match:
                    r = match.groupdict()
                    task_name = r['task_name']
                    instance_no = r['instance_no']
                else:
                    # Try and match the task name with one of the requests
                    # We'll then use the request name as the task name
                    for r in requests:
                        if t['executor_id'].startswith(r):
                            task_name = r
                            break
                    else:
                        # We should never get here but just in case
                        task_instance = t['executor_id'].split('-mesos-slave', 1)[0]
                        task_name = task_instance.rsplit('-', 1)[0]

                    instance_no = t['executor_id'].split('-mesos-slave', 1)[0][-1]

                task = "{}_{}".format(task_name, instance_no)
                m.Add(t["statistics"], [task,])
    num_metrics = sum([len(m.data) for m in tc_metrics])
    log('Number of new-style task metrics : {}'.format(num_metrics))
    return tc_metrics


def slave_task_metrics(mesos, requests_json=None):
    ms = []
    ms.extend(old_slave_task_metrics(mesos))
    ms.extend(new_slave_task_metrics(mesos, requests_json))
    return ms


def cluster_metrics(mesos):
    metrics = [
        Metric("master/cpus_percent",     "cluster.cpus.percent",
               Each(scale=100)),
        Metric("master/cpus_total",       "cluster.cpus.total"),
        Metric("master/cpus_used",        "cluster.cpus.used"),
        Metric("master/mem_percent",      "cluster.mem.percent",
               Each(scale=100)),
        Metric("master/mem_total",        "cluster.mem.total"),
        Metric("master/mem_used",         "cluster.mem.used"),
        Metric("master/disk_percent",     "cluster.disk.percent",
               Each(scale=100)),
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

