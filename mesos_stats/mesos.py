import sys
import re
import functools
import requests
from multiprocessing import Pool
from .metric import Metric, Each
from .util import log, try_get_json

POOL_SIZE = 10 # Number of parallel processes to query Mesos

class Mesos:
    """
        Mesos class to retrieve and store metrics
    """
    def __init__(self, master_list):
        self.master_list = master_list
        self.master_state = None
        self.slave_states = None
        self.master = None
        self.cluster_metrics = [
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

        # You can make an API call to any of the Master hosts
        # They will do a HTTP 307 redirect to the acting leader
        # We just need to be concerned that a host might be down and cause
        # all subsequent API calls to fail.
        # Let's test each master host in turn to get a working one
        for master in master_list:
            try:
                url = "http://{}/version".format(master)
                a = try_get_json(url)
                # HTTP call works, use this host from now on
                self.master = master
            except requests.exceptions.RequestException as e:
                print(str(e))
                continue
            break
        else: # We've failed to reach all masters, quit.
            raise MesosStatsException('Unable to reach Mesos Masters')

        self.slaves = try_get_json("http://%s/slaves" % self.master)['slaves']

    def update(self):
        """ Retrieves slave and master metrics"""
        self.cluster_metrics = self._get_cluster_metrics()
        self.slave_metrics = self._get_slave_metrics()
        self.executors = self._get_executors()


    def _get_cluster_metrics(self):
        return try_get_json("http://{}/metrics/snapshot".format(self.master))


    def _get_slave_metrics(self):
        if not self.slaves:
            return
        res = {}
        for slave in self.slaves:
            metric = try_get_json("http://{}:{}/metrics/snapshot"\
                                 .format(slave['hostname'], slave['port']))
            res[slave.get('hostname')] = metric
        return res


    def _get_executors(self):
        if not self.slaves:
            return
        res = {}
        for slave in self.slaves:
            executors = try_get_json("http://{}:{}/monitor/statistics.json"\
                                 .format(slave['hostname'], slave['port']))
            res[slave.get('hostname')] = executors
        return res


    def reset(self):
        self.slaves = None
        self.cluster_metrics = None
        self.slave_metrics = None
        self.executors = None



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
    ms = []
    for pid in mesos.slaves():
        slave = mesos.slaves()[pid]
        # This is the old schema to retain for backwards compatibility
        # The full task names are used
        for m in metrics:
            for t in slave["task_details"]:
                m.Add(t["statistics"], [pid, t["executor_id"]])
        ms.extend(metrics)
    num_metrics = sum([len(m.data) for m in ms])
    log('Number of old-style task metrics : {}'.format(num_metrics))
    return ms


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
    ms = []
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
        ms.extend(tc_metrics)
    num_metrics = sum([len(m.data) for m in ms])
    log('Number of new-style task metrics : {}'.format(num_metrics))
    return ms


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

