import json
import time
import requests

def try_get_json(url):
    with timer("GET %s" % url):     
        t = time.time()
        try:
            return json.loads(requests.get(url, timeout=20).text)
        except requests.exceptions.Timeout:
            log("GET %s timed out after %s." % (url, time.time()-t))
            raise
        except requests.exceptions.ConnectionError as e:
            log("GET %s failed: %s" % (url, e))
            raise
        except object as e:
            log("GET %s returned a '%s' exception: %s" % (url, type(e), e))
            raise
   
def log(message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print '%s %s' % (ts, message)

class timer:
    def __init__(self, name):
        self.name = name
    def __enter__(self):
        self.time = time.time()
    def __exit__(self, type, value, traceback):
        return log("%s took %ss" % (self.name, time.time() - self.time))

