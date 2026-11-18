import unittest

from xgovernor_mcts.bridge.mock_host import FakeHostCapabilities
from xgovernor_mcts.engine.agents import host_agent_turn
from xgovernor_tasks.common.host_benchmark import HostTaskWorkspace


class P2HostAgentTests(unittest.TestCase):
    def test_agent_and_task_workspace_use_branch_only(self):
        host = FakeHostCapabilities()
        result = host_agent_turn(
            host,
            branch_id="branch-root",
            text="try fix",
            timeout_ms=1,
            idempotency_key="turn-1",
            wait_job=lambda job_id: {"status": "completed", "output": "<solution>ok</solution>", "usage": {"tokens": 3}},
        )
        self.assertEqual(result.branch_id, "branch-root")
        self.assertNotIn("runtime_id", result.__dict__)
        ws = HostTaskWorkspace("branch-root")
        ws.write(host, "/tmp/a", b"ok", idempotency_key="w1")
        self.assertEqual(ws.read(host, "/tmp/a"), b"ok")


if __name__ == "__main__": unittest.main()
