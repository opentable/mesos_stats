from metric import Metric, Each
from util import log, try_get_json

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
    pending_deploys = singularity.pending_deploys()
    overdue_deploys = [d for d in pending_deploys if d["currentDeployState"] == "OVERDUE"]
    digested_metrics = [
        DigestedMetric("singularity.deploys.pending.total", len(pending_deploys)),
        DigestedMetric("singularity.deploys.pending.overdue", len(overdue_deploys)),
    ]
    return digested_metrics

