import time
import functools
from multiprocessing import Pool
from .metric import Metric, Each
from .util import log, try_get_json

POOL_SIZE = 20 # Number of parallel processes to query singularity

class Singularity:
    def __init__(self, host):
        self.host = host
        self.reset()


    def show_cache_info(self):
        log("_get cache info : {}".format(self._get.cache_info()))


    def state(self):
        return self._get("/state")


    def active_requests(self):
        return self._get("/requests")


    def pending_deploys(self):
        return self._get("/deploys/pending")


    def failed_tasks(self, requestId):
        return self._get("/history/request/%s/tasks" % requestId)


    # scheduled_tasks should nod be confused with the common concept of scheduled
    # tasks. This indicates any task which is scheduled to be run, including tasks
    # which have been invoked to run immediately, and long-running services
    # or workers. All of these task types become a scheduled task prior to being
    # run on a slave.
    def scheduled_tasks(self):
        return self._get("/tasks/scheduled")


    @functools.lru_cache(maxsize=None)
    def _get(self, uri):
        url = "http://%s/api%s" % (self.host, uri)
        log("Getting %s" % url)
        return try_get_json(url)


    def reset(self):
        self.show_cache_info()
        self._get.cache_clear()


class DigestedMetric:
    def __init__(self, key, value):
        self.key = key
        self.value = value


    def Results(self):
        return [(self.key, self.value)]

def get_failed_count(request, singularity):
    failed_tasks = singularity.failed_tasks(request['request']['id'])
    count = 0
    for h in failed_tasks:
        # TODO: we should store the timestamp of the last scrape so we
        # only filter by updatedAt > last_scrape_timestamp instead of
        # hardcoding this to 1500 ms.
        if all([h['lastTaskState'] == 'TASK_FAILED',
               h['updatedAt'] > int(round((time.time() - 1500)* 1000)),
               h['updatedAt'] < int(round((time.time())* 1000))]):
            count += 1
    return (request['request']['id'], count)


def singularity_metrics(singularity):
    # TODO: Figure out why deploy state is never OVERDUE, even when the deploy
    # is overdue.
    pending_deploys = singularity.pending_deploys()
    #overdue_deploys = [d for d in pending_deploys
    # if d["currentDeployState"] == "OVERDUE"]

    with Pool(processes=POOL_SIZE) as pool:
        result = pool.map(functools.partial(get_failed_count,
                                            singularity=singularity),
                          singularity.active_requests())
    '''
    for t in singularity.active_requests():
        failed_tasks = singularity.failed_tasks(t['request']['id'])
        for h in failed_tasks:
            if h['lastTaskState'] == 'TASK_FAILED' and h['updatedAt'] > int(round((time.time() - 1500)* 1000)) and h['updatedAt'] < int(round((time.time())* 1000)):
                if h['taskId']['requestId'] in task_failed_count:
                    task_failed_count[h['taskId']['requestId']] += 1
                else:
                    task_failed_count[h['taskId']['requestId']] = 1
    '''
    scheduled_tasks = singularity.scheduled_tasks()

    # Alternative method to determine overdue tasks: look at scheduled start
    # time and set to overdue if it is in the past. This is how the
    # Singularity UI calculates it.
    overdue_tasks = [t for t in scheduled_tasks if
            int(t["pendingTask"]["pendingTaskId"]["nextRunAt"]) <
            int(round(time.time() * 1000))]

    overdue_deploys = overdue_tasks
    dm = [
        DigestedMetric("singularity.deploys.pending.total",
                       len(pending_deploys)),
        DigestedMetric("singularity.deploys.pending.overdue",
                       len(overdue_deploys)),
    ]

    for (requestId, count) in result:
         dm.append(DigestedMetric("singularity.tasks.history.failure.%s"
                                                        % requestId, count))

    return dm
