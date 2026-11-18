import unittest
from types import SimpleNamespace

from xgovernor_mcts.engine.mcts_protocol import ProtocolError, notification
from xgovernor_mcts.bridge.job_registry import JobRegistry
from xgovernor_mcts.bridge.rpc_transport import MockTransport
from xgovernor_mcts.bridge.server import LifecycleServer


class MultiRunIsolationTests(unittest.TestCase):
    def test_job_registry_rejects_unregistered_run(self):
        jobs = JobRegistry()
        jobs.register_run("r1")
        jobs.register_run("r2")
        jobs.accept("r1", "j1")

        with self.assertRaises(ProtocolError):
            jobs.apply(
                "host.job.started",
                {"run_id": "r3", "job_id": "j3", "sequence": 1},
            )

        with self.assertRaises(ProtocolError):
            jobs.get("r3", "j3")

    def test_job_notifications_do_not_cross_run(self):
        jobs = JobRegistry()
        jobs.register_run("r1")
        jobs.register_run("r2")
        jobs.accept("r1", "j1")

        jobs.apply(
            "host.job.started",
            {"run_id": "r1", "job_id": "j1", "sequence": 1},
        )

        self.assertEqual(jobs.get("r1", "j1").status, "running")
        with self.assertRaises(ProtocolError):
            jobs.get("r2", "j1")

    def test_server_routes_job_notification_by_run_id(self):
        class RecordingPeer:
            def __init__(self):
                self.messages = []

            def dispatch(self, message):
                self.messages.append(message)

        server = LifecycleServer(MockTransport())
        run1 = RecordingPeer()
        run2 = RecordingPeer()
        server.executors = {
            "run-1": SimpleNamespace(host=SimpleNamespace(peer=run1)),
            "run-2": SimpleNamespace(host=SimpleNamespace(peer=run2)),
        }

        message = notification(
            "host.job.started",
            {"run_id": "run-2", "job_id": "job-9", "sequence": 1},
        )
        server._dispatch_notification(message)

        self.assertEqual(run1.messages, [])
        self.assertEqual(run2.messages, [message])


if __name__ == "__main__":
    unittest.main()
