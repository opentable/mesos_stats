import time
from .util import log, try_get_json


class Singularity:
    def __init__(self, host):
        self.host = host
        self.state = {}
        self.active_requests = []
        self.disaster_stats = {}
        self.active_tasks = []
        self.update()

    def reset(self):
        self.state = {}
        self.active_requests = []
        self.disasters_stats = {}
        self.active_tasks = []

    def update(self):
        self.state = self.get_state()
        self.active_requests = self.get_active_requests()
        self.disasters_stats = self.get_disasters_stats()
        self.active_tasks = self.get_active_tasks()

    def get_disasters_stats(self):
        return self._get("/disasters/stats")

    def get_state(self):
        state = self._get("/state")
        state['decommissionedSlaves'] = len(self.get_decommisioned_slaves())
        return state

    def get_active_requests(self):
        return self._get("/requests")

    def get_active_tasks(self):
        return self._get("/tasks/active")

    def get_decommisioned_slaves(self):
        return self._get("/slaves?state=DECOMMISSIONED")

    def get_scheduled_tasks(self):
        return self._get("/tasks/scheduled")

    def _get(self, uri):
        url = "http://%s/api%s" % (self.host, uri)
        return try_get_json(url)

    def get_singularity_lookup(self):
        '''
            return a lookup dict so we can quickly map mesos tasks to their
            respective singularity request names and instance number
        '''
        lookup = {}
        for t in self.active_tasks:
            request_name = t['taskId']['requestId']
            instance = t['taskId']['instanceNo']
            mesos_task_name = t['mesosTask']['taskId']['value']
            lookup[mesos_task_name] = '{}_{}'.format(request_name, instance)
        return lookup


class SingularityCarbon:
    '''
        Convert Singularity metrics into Carbon compatible metrics
        and flushes them into the give queue
    '''
    metric_mapping = {
        "activeTasks":              "singularity.tasks.active",
        "scheduledTasks":           "singularity.tasks.scheduled",
        "lateTasks":                "singularity.tasks.late",
        "launchingTasks":           "singularity.tasks.launching",
        "cleaningTasks":            "singularity.tasks.cleaning",
        "futureTasks":              "singularity.tasks.future",
        "numLostTasks":             "singularity.tasks.lost",
        "avgStatusUpdateDelayMs":   "singularity.tasks.avgStatusUpdateDelayMs",
        "activeRequests":           "singularity.requests.active",
        "maxTaskLag":               "singularity.requests.max_task_lag",
        "cooldownRequests":         "singularity.requests.cooldown",
        "pausedRequests":           "singularity.requests.paused",
        "pendingRequests":          "singularity.requests.pending",
        "cleaningRequests":         "singularity.requests.cleaning",
        "activeSlaves":             "singularity.slaves.active",
        "deadSlaves":               "singularity.slaves.dead",
        "numLostSlaves":            "singularity.slaves.lost",
        "decommissioningSlaves":    "singularity.slaves.decommissioning",
        "decommissionedSlaves":     "singularity.slaves.decommissioned",
    }

    def __init__(self, singularity, queue, pickle=False):
        self.singularity = singularity
        self.queue = queue
        self.pickle = pickle

    def flush_all(self):
        counter = 0
        # flush state metrics
        for k, v in self.singularity.state.items():
            try:
                metric_name = self.metric_mapping[k]
            except KeyError:
                continue
            ts = int(time.time())
            self._add_to_queue(metric_name, v, ts)
            counter += 1
        # flush disaster metrics
        latest_stat = self.singularity.disasters_stats.get('stats')[0]
        ts = latest_stat.get('timestamp')
        for k, v in latest_stat.items():
            try:
                metric_name = self.metric_mapping[k]
            except KeyError:
                continue
            self._add_to_queue(metric_name, v, ts)
            counter += 1

        log('flushed {} singularity metrics'.format(counter))

    def _add_to_queue(self, metric_name, metric_value, ts):
        # The carbon plaintext protocol for metrics are
        # <metric path> <metric value> <metric timestamp>
        # The pickle protocol accepts a tuple
        # [(path, (timestamp, value)), ...]
        if not self.pickle:
            self.queue.put('{} {} {}'.format(metric_name, metric_value, ts))
        else:
            self.queue.put((metric_name, (ts, metric_value)))
