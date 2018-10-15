import time
import re
import requests
from concurrent import futures
from .util import log, try_get_json

POOL_SIZE = 10  # Number of parallel threads to query Mesos


class Mesos:
    '''
        Mesos class to retrieve and store metrics
    '''
    def __init__(self, master_list):
        self.master_list = master_list
        self.master = self._get_master()
        self.slaves = try_get_json("http://%s/slaves" % self.master)\
            .get('slaves', None)
        self.slave_metrics = {}
        self.executors = []

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
        else:  # We've failed to reach all masters, quit.
            raise MesosStatsException('Unable to reach Mesos Masters')

    def update(self):
        ''' Retrieves slave and master metrics '''
        self.cluster_metrics = self._get_cluster_metrics()
        # Let's make sure we are still connected to the master
        if not self.cluster_metrics['master/elected']:
            self._get_master()
            self.cluster_metrics = self._get_cluster_metrics()

        self.framework_metrics = self._get_framework_metrics()

        self.slaves = try_get_json("http://%s/slaves" % self.master)\
            .get('slaves', None)
        self.update_ts = int(time.time())
        if self.slaves:
            self.slave_metrics = self._get_slave_metrics()
            self.executors = self._get_executors()
            log('Total number of executors = {}'.format(sum(len(e)
                for e in self.executors)))

    def _get_framework_metrics(self):
        return try_get_json("http://{}/frameworks".format(self.master))

    def _get_cluster_metrics(self):
        return try_get_json("http://{}/metrics/snapshot".format(self.master))

    def _get_slave_metrics(self):
        if not self.slaves:
            return

        def task(slave):
            metric = try_get_json("http://{}:{}/metrics/snapshot"
                                  .format(slave['hostname'], slave['port']))
            return(slave.get('hostname'), metric)

        ex = futures.ThreadPoolExecutor(max_workers=POOL_SIZE)
        results = ex.map(task, self.slaves)
        return {r[0]: r[1] for r in results}

    def _get_executors(self):
        if not self.slaves:
            return

        def task(slave):
            executors = try_get_json("http://{}:{}/monitor/statistics.json"
                                     .format(slave['hostname'], slave['port']))
            return(slave.get('hostname'), executors)

        ex = futures.ThreadPoolExecutor(max_workers=POOL_SIZE)
        results = ex.map(task, self.slaves)
        return {r[0]: r[1] for r in results}

    def reset(self):
        self.cluster_metrics = {}
        self.slaves = {}
        self.slave_metrics = {}
        self.executors = []
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

    framework_metric_mapping = {
        "disk": "frameworks.{}.resources.disk",
        "mem":  "frameworks.{}.resources.mem",
        "cpus": "frameworks.{}.resources.cpus",
    }

    fw_task_metric_mapping = {
        "disk": "frameworks.{}.tasks.{}.disk",
        "mem":  "frameworks.{}.tasks.{}.mem",
        "cpus": "frameworks.{}.tasks.{}.cpus",
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

    def flush_all(self):
        self.flush_cluster_metrics()
        self.flush_slave_metrics()
        if self.singularity:
            self.send_alternate_executor_metrics()
        self.flush_executor_metrics()
        self.flush_framework_metrics()
        self.flush_framework_task_metrics()

    def _clean_metric_name(self, name):
        name = name.replace('.', '_')
        return name.replace(' ', '_')

    def flush_slave_metrics(self):
        counter = 0
        for slave_name, metrics in self.mesos.slave_metrics.items():
            for k, v in metrics.items():
                slave_name = self._clean_metric_name(slave_name)
                try:
                    metric_name = self.slave_metric_mapping[k]\
                                    .format(slave_name)
                except KeyError:  # Skip metrics that are not defined above
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
            except KeyError:  # Skip metrics that are not defined above
                continue
            (metric_name, v) = self._convert(metric_name, v)
            self._add_to_queue(metric_name, v)
            counter += 1
        log('flushed {} cluster metrics'.format(counter))
        self.mesos.cluster_metrics = None

    def flush_executor_metrics(self):
        counter = 0
        for slave_name, executors in self.mesos.executors.items():
            for e in executors:
                for k, v in e['statistics'].items():
                    task_name = self._clean_metric_name(e['executor_id'])
                    sn = self._clean_metric_name(slave_name)
                    try:
                        metric_name = self.executor_metric_mapping[k]\
                                .format(sn, task_name)
                    except KeyError:
                        continue
                    self._add_to_queue(metric_name, v)
                counter += 1
        log('flushed {} executor metrics'.format(counter))
        self.mesos.executor_metrics = None

    def flush_framework_metrics(self):
        counter = 0
        for framework in self.mesos.framework_metrics['frameworks']:
            for k, v in framework['used_resources'].items():
                fw_name = self._clean_metric_name(framework['name'])
                try:
                    metric_name = self.framework_metric_mapping[k]\
                            .format(fw_name)
                except KeyError:
                    continue
                self._add_to_queue(metric_name, v)
            counter += 1
        log('flushed {} framework metrics'.format(counter))

    def flush_framework_task_metrics(self):
        counter = 0
        for framework in self.mesos.framework_metrics['frameworks']:
            if framework['id'] == 'Singularity':
                continue
            for task in framework['tasks']:
                for k, v in task['resources'].items():
                    fw_name = self._clean_metric_name(framework['name'])
                    task_name = self._clean_metric_name(task['name'])
                    try:
                        metric_name = self.fw_task_metric_mapping[k]\
                                .format(fw_name, task_name)
                    except KeyError:
                        continue
                    self._add_to_queue(metric_name, v)
                counter += 1
        log('flushed {} framework task metrics'.format(counter))
        self.mesos.framework_metrics = None

    def _best_guess_req_name(self, name):
        '''
            Used when we can't match a task name to any existing Request
        '''
        if '---' in name:
            # matches login_service--eu-pp_sf-7712aab4c9c893696d
            pattern = '(\S+)---(\w+_\w+)-\S+-(\d+)[-\w+_\w+]*[_|-]mesos_\S+'
            match = re.search(pattern, name)
            if match:
                task_name, env, instance = match.groups()
                log('guessed task name : {}-{}_{}'.format(task_name, env,
                                                          instance))
                return '{}-{}_{}'.format(task_name, env, instance)
        if '-teamcity_' in name:
            # matches ci-nei-teamcity_2018_01_04T12_4-1-mesos_slave4_qa_sf
            pattern = '(\S+)-teamcity\S+-(\d+)[-\w+_\w+]*[_|-]mesos_\S+'
            match = re.search(pattern, name)
            if match:
                task_name, instance = match.groups()
                log('guessed task name : {}_{}'.format(task_name, instance))
                return '{}_{}'.format(task_name, instance)
        if 'opentable.com' in name:
            pattern = '(\S+)(_|-\.)20\d{2}.\S+-(\d+)[-\w+_\w+]*[_|-]mesos_\S+'
            # Matches task_name_2018-01-02-1-mesos-slave-14.opentable.com
            # OR
            # Matches task_name-.2018-01-02-1-mesos-slave-14.opentable.com
            match = re.search(pattern, name)
            if match:
                task_name, _, instance = match.groups()
                log('guessed task name : {}_{}'.format(task_name, instance))
                return '{}_{}'.format(task_name, instance)
        log('Could not guess task name for : {}'.format(name))
        return name

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
                    task_name = sing_lookup.get(e['executor_id'], None)
                    if not task_name or '---' in task_name:
                        log('Could not match task name: {}'
                            .format(e['executor_id']))
                        task_name = self._best_guess_req_name(e['executor_id'])
                else:  # Use mesos task names for non singularity tasks
                    log('Non Singularity tasks : {}'.format(e['executor_id']))
                    task_name = e['executor_id']

                task_name = self._clean_metric_name(task_name)
                # have instance numbers be a separate directory
                # this converts task_name_3 to task_name.3
                task_name = re.sub('_(\d+$)', '.\g<1>', task_name)

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
            self.queue.put('{} {} {}'.format(metric_name, metric_value,
                                             self.mesos.update_ts))
        else:
            self.queue.put((metric_name,
                            (self.mesos.update_ts, metric_value)))
