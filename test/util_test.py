import time
import unittest
import requests
import httpretty

from mesos_stats.util import try_get_json

class UtilTest(unittest.TestCase):
    def test_try_get_json_invalid_url(self):
        self.assertRaises(requests.exceptions.MissingSchema,
                          try_get_json, 'not a valid url')

    @httpretty.activate
    def test_try_get_json_timeout(self):
        timeout = 2
        def request_callback(request, uri, headers):
            time.sleep(timeout + 2)
            return (200, headers, 'response')

        httpretty.register_uri(
            method=httpretty.GET,
            uri='http://www.someurl.com',
            status=200,
            body=request_callback
        )

        self.assertRaises(requests.exceptions.Timeout,
                          try_get_json, 'http://www.someurl.com',
                          timeout=timeout)

    @httpretty.activate
    def test_try_get_json_success(self):
        httpretty.register_uri(
            method=httpretty.GET,
            uri='http://www.someurl.com',
            status=200,
            body='{"success": true}',
            content_type='text/json'
        )

        resp = try_get_json('http://www.someurl.com')
        self.assertTrue(resp['success'])

    @httpretty.activate
    def test_try_get_json_http_error(self):
        httpretty.register_uri(
            method=httpretty.GET,
            uri='http://www.someurl.com',
            status=503,
            body='{"success": false}',
            content_type='text/json'
        )

        resp = try_get_json('http://www.someurl.com')
        self.assertFalse(resp)
