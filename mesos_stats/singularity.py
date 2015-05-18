from metric import Metric, Each
from util import log, try_get_json
import time

class Singularity:
    def __init__(self, host):
        self.host = host
        self.reset()

    def state(self):
        return self.__get("/state")

    def active_requests(self):
        return self.__get("/requests")

    def pending_deploys(self):
        return self.__get("/deploys/pending")

    # scheduled_tasks should nod be confused with the common concept of scheduled
    # tasks. This indicates any task which is scheduled to be run, including tasks
    # which have been invoked to run immediately, and long-running services
    # or workers. All of these task types become a scheduled task prior to being
    # run on a slave.
    def scheduled_tasks(self):
        return self.__get("/tasks/scheduled")

    def __get(self, uri):
        if self.__cache.get(uri) == None:
            url = "http://%s/api%s" % (self.host, uri)
            log("Getting %s" % url)
            self.__cache[uri] = try_get_json(url)
        return self.__cache[uri]

    def reset(self):
        self.__cache = {}

class DigestedMetric:
    def __init__(self, key, value):
        self.key = key
        self.value = value
    def Results(self):
        return [(self.key, self.value)]

def singularity_metrics(singularity):
    # TODO: Figure out why deploy state is never OVERDUE, even when the deploy is
    # overdue.
    pending_deploys = singularity.pending_deploys()
    #overdue_deploys = [d for d in pending_deploys if d["currentDeployState"] == "OVERDUE"]

    # Alternative method to determine overdue tasks: look at scheduled start time
    # and set to overdue if it is in the past. This is how the Singularity UI
    # calculates it.
    scheduled_tasks = singularity.scheduled_tasks()
    overdue_tasks = [t for t in scheduled_tasks if
            int(t["pendingTask"]["pendingTaskId"]["nextRunAt"]) <
            int(round(time.time() * 1000))]

    overdue_deploys = overdue_tasks
    digested_metrics = [
        DigestedMetric("singularity.deploys.pending.total", len(pending_deploys)),
        DigestedMetric("singularity.deploys.pending.overdue", len(overdue_deploys)),
    ]
    return digested_metrics

