import sys
import re
import time
import requests
from multiprocessing import Pool
from .metric import Metric, Each
from .util import log, try_get_json

POOL_SIZE = 10 # Number of parallel processes to query Mesos

class Mesos:
    '''
        Mesos class to retrieve and store metrics
    '''
    def __init__(self, master_list):
        self.master_list = master_list
        self.master = self._get_master()
        self.slaves = try_get_json("http://%s/slaves" % self.master)\
                        .get('slaves', None)


    def _get_master(self):
        ''' Get a working master hostname '''
        for master in self.master_list:
            # Let's test each master host in turn to get a working one
            try:
                url = "http://{}/metrics/snapshot".format(master)
                res = try_get_json(url)
                try:
                    if res['master/elected']:
                        return master
                except KeyError:
                    pass
            except requests.exceptions.RequestException as e:
                print(str(e))
                continue
        else: # We've failed to reach all masters, quit.
            raise MesosStatsException('Unable to reach Mesos Masters')


    def update(self):
        ''' Retrieves slave and master metrics '''
        self.cluster_metrics = self._get_cluster_metrics()
        # Let's make sure we are still connected to the master
        if not self.cluster_metrics['master/elected']:
            self._get_master()
            self.cluster_metrics = self._get_cluster_metrics()

        self.update_ts = int(time.time())

        if self.slaves:
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
        self.update_ts = None


class MesosStatsException(Exception):
    pass


class MesosCarbon:
    '''
        Convert Mesos metrics into Carbon compatible metrics
        and flushes them into the given queue
    '''
    master_metric_mapping = {
        "master/cpus_percent":      "cluster.cpus.percent",
        "master/cpus_total":        "cluster.cpus.total",
        "master/cpus_used":         "cluster.cpus.used",
        "master/mem_percent":       "cluster.mem.percent",
        "master/mem_total":         "cluster.mem.total",
        "master/mem_used":          "cluster.mem.used",
        "master/disk_percent":      "cluster.disk.percent",
        "master/disk_total":        "cluster.disk.total",
        "master/disk_used":         "cluster.disk.used",
        "master/slaves_connected":  "cluster.slaves.connected",
        "master/tasks_failed":      "cluster.tasks.failed",
        "master/tasks_error":       "cluster.tasks.error",
        "master/tasks_finished":    "cluster.tasks.finished",
        "master/tasks_killed":      "cluster.tasks.killed",
        "master/tasks_lost":        "cluster.tasks.lost",
        "master/tasks_running":     "cluster.tasks.running",
        "master/tasks_staging":     "cluster.tasks.staging",
        "master/tasks_starting":    "cluster.tasks.starting",
        "master/tasks_unreachable": "cluster.tasks.unreachable",
        "master/dropped_messages":  "cluster.messages.dropped",
    }

    slave_metric_mapping = {
        "slave/mem_total":          "slave.{}.mem.total",
        "slave/mem_used":           "slave.{}.mem.used",
        "slave/mem_percent":        "slave.{}.mem.percent",
        "slave/cpus_total":         "slave.{}.cpus.total",
        "slave/cpus_used":          "slave.{}.cpus.used",
        "slave/cpus_percent":       "slave.{}.cpus.percent",
        "slave/disk_total":         "slave.{}.disk.total",
        "slave/disk_used":          "slave.{}.disk.total",
        "slave/tasks_running":      "slave.{}.tasks.running",
        "slave/tasks_staging":      "slave.{}.tasks.staging",
        "system/load_1min":         "slave.{}.system.load.1min",
        "system/load_5min":         "slave.{}.system.load.5min",
        "system/load_15min":        "slave.{}.system.load.15min",
        "system/mem_free_bytes":    "slave.{}.system.mem.free.bytes",
        "system/mem_total_bytes":   "slave.{}.system.mem.total.bytes",
    }

    eprefix = "slave.{}.executors.singularity.tasks.{}"
    executor_metric_mapping = {
        "cpus_system_time_secs": eprefix + ".cpus.system_time_secs",
        "cpus_user_time_secs":   eprefix + ".cpus.user_time_secs",
        "cpus_limit":            eprefix + ".cpus.limit",
        "mem_limit_bytes":       eprefix + ".mem.limit_bytes",
        "mem_rss_bytes":         eprefix + ".mem.rss_bytes",
    }


    def __init__(self, mesos, queue, singularity=None, pickle=False):
        self.mesos = mesos
        self.pickle = pickle
        self.queue = queue
        self.singularity = singularity


    def _convert(self, metric_name, value):
        ''' We use this to clean up or do any custom conversions '''
        # Scale all percentages since Mesos reports percent as 0.0 - 0.1
        if 'percent' in metric_name:
            value = value * 100.0
        return (metric_name, value)


    def flush_slave_metrics(self):
        counter = 0
        for slave_name, metrics in self.mesos.slave_metrics.items():
            for k, v in metrics.items():
                try:
                    metric_name = self.slave_metric_mapping[k]\
                                    .format(slave_name)
                except KeyError: # Skip metrics that are not defined above
                    continue
                (metric_name, v) = self._convert(metric_name, v)
                self._add_to_queue(metric_name, v)
                counter += 1
        log('flushed {} slave metrics'.format(counter))
        self.mesos.slave_metrics = None


    def flush_cluster_metrics(self):
        counter = 0
        for k, v in self.mesos.cluster_metrics.items():
            try:
                metric_name = self.master_metric_mapping[k]
            except KeyError: # Skip metrics that are not defined above
                continue
            (metric_name, v) = self._convert(metric_name, v)
            self._add_to_queue(metric_name, v)
            counter += 1
        log('flushed {} cluster metrics'.format(counter))
        self.mesos.cluster_metrics = None


    def flush_executor_metrics(self):
        counter = 0
        for slave_name, executors in self.mesos.executor_metrics.items():
            for e in executors:
                for k, v in e['statistics'].items():
                    metric_name = self.executor_metric_mapping[k]\
                            .format(slave_name, e['executor_id'])
                    self._add_to_queue(metric_name, v)
                counter += 1
        log('flushed {} executor metrics'.format(counter))
        self.mesos.executor_metrics = None



    def send_alternate_executor_metrics(self):
        '''
            This method is similar to flush_executor_metrics but avoids having
            the slave names in the metric_name. All task metrics will be
            populated under {prefix}.tasks.task_name.*.*

            Another feature of this method is that task names will be
            shortened to just their Singularity request name and their
            respective instance numbers
        '''
        mapping = {}
        for k, v in self.executor_metric_mapping.items():
            mapping[k] = v.replace('slave.{}.executors.singularity.tasks.{}',
                                  'tasks.{}')

        sing_lookup = self.singularity.get_singularity_lookup()
        counter = 0
        for slave_name, executors in self.mesos.executors.items():
            for e in executors:
                if e['framework_id'] == 'Singularity':
                    task_name = sing_lookup.get(e['executor_id'],
                                               e['executor_id'])
                else: # Use mesos task names for non singularity tasks
                    task_name = e['executor_id']

                for k, v in e['statistics'].items():
                    try:
                        metric_name = mapping[k].format(task_name)
                    except KeyError:
                        continue
                    self._add_to_queue(metric_name, v)
                counter += 1
        log('Sent {} alternate executor metrics'.format(counter))


    def _add_to_queue(self, metric_name, metric_value):
        # The carbon plaintext protocol for metrics are
        # <metric path> <metric value> <metric timestamp>
        # The pickle protocol accepts a tuple
        # [(path, (timestamp, value)), ...]
        if not self.pickle:
            self.queue.append('{} {} {}'.format(metric_name, metric_value,
                                                self.mesos.update_ts))
        else:
            self.queue.append((metric_name,
                               (self.mesos.update_ts, metric_value)))


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

