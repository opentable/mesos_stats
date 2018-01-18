import time
import unittest
import requests
import requests_mock

from mesos_stats.mesos import Mesos, MesosStatsException, MesosCarbon
from mesos_stats.singularity import Singularity

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
            # mesos1 is unreachable, mesos2 is not a master
            # mesos3 is the master
            m.register_uri('GET', 'http://mesos3/slaves',
                           json=self.slaves_api, status_code=200)
            m.register_uri('GET', 'http://mesos2/metrics/snapshot',
                           json={'master/elected': 0}, status_code=200)
            m.register_uri('GET', 'http://mesos3/metrics/snapshot',
                           json={'master/elected': 1}, status_code=200)
            mesos = Mesos(master_list=['mesos1', 'mesos2', 'mesos3'])
        self.assertEqual(mesos.master, 'mesos3')


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
            m.register_uri('GET', 'http://mesos1/metrics/snapshot',
                           json={'master/elected': 1}, status_code=200)
            mesos = Mesos(master_list=['mesos1'])
            slave_metrics = mesos._get_slave_metrics()

            self.assertTrue(isinstance(slave_metrics, dict))
            self.assertTrue('slave1' in slave_metrics.keys())
            self.assertTrue(isinstance(slave_metrics['slave1'], dict))
            self.assertTrue(slave_metrics['slave1']['slave/cpus_total'], 32)


    def test_mesoscarbon(self):
        with requests_mock.Mocker(real_http=True) as m:
            res = {
                "slave/cpus_total": 32,
                "slave/cpus_percent": 0.1,
            }
            res2 = [
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
            res3 = {
                "master/cpus_total": 32,
                "master/cpus_percent": 0.1,
                "master/elected": 1,
            }
            m.register_uri('GET', 'http://slave1:5051/metrics/snapshot',
                           json=res, status_code=200)
            m.register_uri('GET', 'http://slave1:5051/monitor/statistics.json',
                           json=res2, status_code=200)
            m.register_uri('GET', 'http://mesos1/slaves',
                           json=self.slaves_api, status_code=200)
            m.register_uri('GET', 'http://mesos1/metrics/snapshot',
                           json=res3, status_code=200)
            mesos = Mesos(master_list=['mesos1'])
            mesos.update()

        q = []
        mc = MesosCarbon(mesos, q, pickle=False)
        mc.flush_slave_metrics()

        self.assertTrue(q)
        self.assertEqual(q[0].split()[0], 'slave.slave1.cpus.total')
        self.assertEqual(q[0].split()[1], '32')

        # Test that the percent is scaled up by 100, 0.1 * 100 = 10.0
        self.assertEqual(q[1].split()[1], '10.0')

        # Test slave metrics is empty after flushing
        self.assertFalse(mesos.slave_metrics)

        q2 = []
        mc2 = MesosCarbon(mesos, q2, pickle=False)
        mc2.flush_cluster_metrics()

        self.assertTrue(q2)
        self.assertEqual(q2[0].split()[0], 'cluster.cpus.total')
        self.assertEqual(q2[0].split()[1], '32')

        # Test that the percent is scaled up by 100, 0.1 * 100 = 10.0
        self.assertEqual(q2[1].split()[1], '10.0')

        # Test cluster metrics is empty after flushing
        self.assertFalse(mesos.cluster_metrics)

        # Test pickle and non-pickle output
        q3 = []
        mc3 = MesosCarbon(mesos, q3, pickle=False)
        mc3._add_to_queue('test.testing', 123.0)
        self.assertTrue(q3)
        self.assertEqual(len(q3[0].split()), 3)

        q4 = []
        mc4 = MesosCarbon(mesos, q4, pickle=True)
        mc4._add_to_queue('test.testing', 123.0)
        self.assertTrue(q4)
        self.assertIsInstance(q4[0], tuple)
        self.assertIsInstance(q4[0][1], tuple)
        self.assertEqual(q4[0][0], 'test.testing')
        self.assertEqual(q4[0][1][1], 123.0)

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
            m.register_uri('GET', 'http://mesos1/metrics/snapshot',
                           json={'master/elected': 1}, status_code=200)
            mesos = Mesos(master_list=['mesos1'])
            executors = mesos._get_executors()

            self.assertTrue(isinstance(executors, dict))
            self.assertTrue('slave1' in executors.keys())
            self.assertTrue(isinstance(executors['slave1'], list))
            self.assertEqual(executors['slave1'][0]['executor_id'], "mytask")

    def test_send_alternate_executor_metrics(self):
        with requests_mock.Mocker(real_http=True) as m:
            tasks_api = [
                {
                   "taskId":{
                      "requestId":"my-request",
                      "deployId":"teamcity_2018_01_17T00_04_40",
                      "startedAt":1516147481669,
                      "instanceNo":2,
                      "host":"mesos_slave21_qa_sf.qasql.opentable.com",
                      "sanitizedHost":"mesos_slave21_qa_sf.qasql.opentable.com",
                      "sanitizedRackId":"FIXME",
                      "rackId":"FIXME",
                      "id":"my-mesos-task"
                   },
                   "mesosTask":{
                      "taskId":{
                         "value":"my-mesos-task"
                      },
                      "name":"pp-promoted-inventory-service",
                    }
                }
            ]

            res = [
                {
                    "executor_id": "my-mesos-task",
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
            m.register_uri('GET', 'http://slave1:5051/metrics/snapshot',
                           json={}, status_code=200)
            m.register_uri('GET', 'http://mesos1/slaves',
                           json=self.slaves_api, status_code=200)
            m.register_uri('GET', 'http://mesos1/metrics/snapshot',
                           json={'master/elected': 1}, status_code=200)
            m.register_uri('GET', 'http://server/api/state',
                           json={}, status_code=200)
            m.register_uri('GET', 'http://server/api/requests',
                           json=[], status_code=200)
            m.register_uri('GET', 'http://server/api/disasters/stats',
                           json={}, status_code=200)
            m.register_uri('GET', 'http://server/api/tasks/active',
                           json=tasks_api, status_code=200)

            s = Singularity('server')
            mesos = Mesos(master_list=['mesos1'])
            mesos.update()
            q = []
            mc = MesosCarbon(mesos, q, singularity=s)
            mc.send_alternate_executor_metrics()

            self.assertTrue(q)
            self.assertEqual(len(q), 5)
            self.assertTrue(q[0].split()[0].startswith('tasks.my-request_2.'))


