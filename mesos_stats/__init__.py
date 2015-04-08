import json
import time
import requests

__version__ = "0.0.0"

def try_get_json(url):
    try:
        return json.loads(requests.get(url).text)
    except requests.exceptions.ConnectionError as e:
        log("GET %s failed: %s" % (url, e))
        return None
   
def log(message):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print '%s %s' % (ts, message)

