import unittest
import requests
import requests_mock

from mesos_stats.singularity import Singularity, SingularityCarbon

class MesosTest(unittest.TestCase):
    def setUp(self):
        self.state_api = {
            "activeTasks": 817,
            "launchingTasks": 0,
            "activeRequests": 654,
            "cooldownRequests": 7,
            "pausedRequests": 58,
            "scheduledTasks": 72,
            "pendingRequests": 0,
            "lbCleanupTasks": 0,
            "lbCleanupRequests": 0,
            "cleaningRequests": 0,
            "activeSlaves": 28,
            "deadSlaves": 0,
            "decommissioningSlaves": 1,
            "activeRacks": 1,
            "deadRacks": 0,
            "decommissioningRacks": 0,
            "cleaningTasks": 0,
            "oldestDeploy": 2062,
            "numDeploys": 1,
            "oldestDeployStep": 2062,
            "lateTasks": 0,
            "futureTasks": 72,
            "maxTaskLag": 0,
            "generatedAt": 1516279668086,
            "overProvisionedRequests": 0,
            "underProvisionedRequests": 1,
            "finishedRequests": 0,
            "unknownRacks": 0,
            "unknownSlaves": 0,
            "authDatastoreHealthy": 'true',
            "avgStatusUpdateDelayMs": 0,
            "decomissioningSlaves": 1,
            "decomissioningRacks": 0,
            "allRequests": 719
        }

        self.disaster_api = {
            "stats": [
                {
                    "timestamp": 1516279808269,
                    "numActiveTasks": 833,
                    "numPendingTasks": 56,
                    "numLateTasks": 0,
                    "avgTaskLagMillis": 0,
                    "numLostTasks": 0,
                    "numActiveSlaves": 29,
                    "numLostSlaves": 0
                },
                {
                    "timestamp": 1516279798264,
                    "numActiveTasks": 817,
                    "numPendingTasks": 72,
                    "numLateTasks": 0,
                    "avgTaskLagMillis": 0,
                    "numLostTasks": 0,
                    "numActiveSlaves": 29,
                    "numLostSlaves": 0
                },
            ],
        }

        self.requests_api = [
            {
                "request": {
                    "id": "pp-freetext-api",
                    "requestType": "SERVICE",
                    "owners": [
                        "user@mail.com",
                    ],
                    "instances": 1
                },
                "state": "ACTIVE",
                "requestDeployState": {
                    "requestId": "pp-freetext-api",
                    "activeDeploy": {
                        "requestId": "pp-freetext-api",
                        "deployId": "teamcity_2018_01_17T01_28_48",
                        "timestamp": 1516152528958
                    }
                }
            },
            {
                "request": {
                    "id": "coresvc_cancel_queue--eu-pp_sf-6288de",
                    "requestType": "SERVICE",
                    "owners": [
                        "user@mail.com",
                    ],
                    "instances": 1
                },
                "state": "ACTIVE",
                "requestDeployState": {
                    "requestId": "coresvc_cancel_queue--eu-pp_sf-6288de",
                    "pendingDeploy": {
                        "requestId": "coresvc_cancel_queue--eu-pp_sf-6288de",
                        "deployId": "1_0_22_60df718d2136416c80e8d504ed9f6002",
                        "timestamp": 1516279862745
                    }
                }
            },

        ]


    def test_singularity_carbon(self):
        with requests_mock.Mocker(real_http=True) as m:
            m.register_uri('GET', 'http://server/api/state',
                           json=self.state_api, status_code=200)
            m.register_uri('GET', 'http://server/api/requests',
                           json=self.requests_api, status_code=200)
            m.register_uri('GET', 'http://server/api/disasters/stats',
                           json=self.disaster_api, status_code=200)
            s = Singularity('server')
            q = []
            sc = SingularityCarbon(s, q)
            sc.flush_metrics()

            # Queue should be populated
            self.assertTrue(q)

            # Test plaintext protocol
            self.assertEqual(len(q[0].split()), 3)

            # Make sure that every metric has been captured
            all_metric_names = set(sc.metric_mapping.values())
            metric_names_from_q = set([m.split()[0] for m in q])
            self.assertEqual(all_metric_names, metric_names_from_q)

            # Test pickle protocol
            q2 = []
            sc2 = SingularityCarbon(s, q2, pickle=True)
            sc2.flush_metrics()

            self.assertTrue(q2)
            self.assertIsInstance(q2[0], tuple)
            self.assertIsInstance(q2[0][1], tuple)


