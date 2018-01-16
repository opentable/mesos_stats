import time
import unittest
import requests
import requests_mock

from mesos_stats.mesos import Mesos, MesosStatsException

'''
import sys
import logging
logger = logging.getLogger()
logger.level = logging.DEBUG
stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
'''

class MesosTest(unittest.TestCase):
    def setUp(self):
        self.slaves_api = {
            'slaves': [
                {
                    'hostname': 'slave1',
                    'port': '5051',
                }
            ]
        }


    def test_mesos_raises_exception_for_no_master(self):
        master_list = ['mesos4', 'mesos5', 'mesos6']
        self.assertRaises(MesosStatsException, Mesos, master_list)


    def test_mesos_chooses_working_master(self):
        with requests_mock.Mocker(real_http=True) as m:
            m.register_uri('GET', 'http://mesos2/slaves',
                           json=self.slaves_api, status_code=200)
            m.register_uri('GET', 'http://mesos2/version',
                           json={'a': 'b'}, status_code=200)
            mesos = Mesos(master_list=['mesos1', 'mesos2', 'mesos3'])
        self.assertEqual(mesos.master, 'mesos2')


    def test_get_slave_metrics(self):
        with requests_mock.Mocker(real_http=True) as m:
            res = {
                "slave/tasks_finished": 4367,
                "slave/cpus_total": 32,
                "slave/executors_preempted": 0,
            }
            m.register_uri('GET', 'http://slave1:5051/metrics/snapshot',
                           json=res, status_code=200)
            m.register_uri('GET', 'http://mesos1/slaves',
                           json=self.slaves_api, status_code=200)
            m.register_uri('GET', 'http://mesos1/version',
                           json={'a': 'b'}, status_code=200)
            mesos = Mesos(master_list=['mesos1'])
            slave_metrics = mesos._get_slave_metrics()

            self.assertTrue(isinstance(slave_metrics, dict))
            self.assertTrue('slave1' in slave_metrics.keys())
            self.assertTrue(isinstance(slave_metrics['slave1'], dict))
            self.assertTrue(slave_metrics['slave1']['slave/cpus_total'], 32)


    def test_get_executors(self):
        with requests_mock.Mocker(real_http=True) as m:
            res = [
                {
                    "executor_id": "mytask",
                    "executor_name": "mytask command",
                    "framework_id": "Singularity",
                    "source": "mytask",
                    "statistics": {
                        "cpus_limit": 0.13,
                        "cpus_system_time_secs": 22.56,
                        "cpus_user_time_secs": 104.88,
                        "mem_limit_bytes": 301989888,
                        "mem_rss_bytes": 87113728,
                        "timestamp": 1516124496.89259
                    }
                }
            ]
            m.register_uri('GET', 'http://slave1:5051/monitor/statistics.json',
                           json=res, status_code=200)
            m.register_uri('GET', 'http://mesos1/slaves',
                           json=self.slaves_api, status_code=200)
            m.register_uri('GET', 'http://mesos1/version',
                           json={'a': 'b'}, status_code=200)
            mesos = Mesos(master_list=['mesos1'])
            executors = mesos._get_executors()

            self.assertTrue(isinstance(executors, dict))
            self.assertTrue('slave1' in executors.keys())
            self.assertTrue(isinstance(executors['slave1'], list))
            self.assertEqual(executors['slave1'][0]['executor_id'], "mytask")



