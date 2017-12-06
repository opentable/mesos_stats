import json
import time
import sys
import requests

def try_get_json(url, timeout=20):
    t = time.time()
    try:
        response = requests.get(url, timeout=timeout)
    except requests.exceptions.Timeout:
        log("GET %s timed out after %s." % (url, time.time()-t))
        raise
    except requests.exceptions.MissingSchema:
        log("%s is not a valid URL" % url)
        raise
    except requests.exceptions.ConnectionError as e:
        log("GET %s failed: %s" % (url, e))
        raise
    except:
        log("Unexpected error from %s : %s" % (url, sys.exc_info()[0]))
        raise

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        log("GET %s failed - Non 200 HTTP Error" % url)
        return False

def log(message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print('%s %s' % (ts, message))

class Timer:
    def __init__(self, name):
        self.name = name
    def __enter__(self):
        self.time = time.time()
    def __exit__(self, type, value, traceback):
        return log("%s took %ss" % (self.name, time.time() - self.time))
