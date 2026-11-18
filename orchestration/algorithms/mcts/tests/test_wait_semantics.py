import threading
import time
import unittest

from xgovernor_mcts.engine.result_builder import ResultBuilder
from xgovernor_mcts.bridge.lifecycle import RunRegistry


def init_params():
    return {
        "plugin_id": "mcts",
        "supported_api_versions": ["mcts.rpc.v1"],
        "host_version": "fake",
        "available_capabilities": [
            "snapshot",
            "fork",
            "agent_turn",
            "inspect",
            "exec",
            "file_read",
            "file_write",
            "diff",
            "release",
        ],
    }


def start_params(run_id="r1"):
    return {
        "plugin_id": "mcts",
        "run_id": run_id,
        "session_id": "s",
        "turn_id": "t",
        "root_state_id": "state-root",
        "goal": "fix",
        "content": "ctx",
        "budget": {"max_iters": 5, "branching": 3, "max_time_ms": 1, "max_tokens": 1},
        "granted_capabilities": ["snapshot", "fork", "agent_turn"],
    }


class WaitSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.registry = RunRegistry()
        self.registry.initialize(init_params())
        self.registry.start(start_params())

    def test_after_sequence_zero_returns_immediately(self):
        started = time.monotonic()
        state = self.registry.wait("r1", after_sequence=0, timeout_ms=0)
        self.assertLess(time.monotonic() - started, 0.2)
        self.assertEqual(state["status"], "accepted")
        self.assertEqual(state["sequence"], 0)

    def test_wait_blocks_until_sequence_advances(self):
        def advance():
            time.sleep(0.05)
            self.registry.emit(
                "plugin.progress",
                {
                    "plugin_id": "mcts",
                    "run_id": "r1",
                    "sequence": 2,
                    "summary": "expanded",
                },
            )

        thread = threading.Thread(target=advance)
        thread.start()
        state = self.registry.wait("r1", after_sequence=1, timeout_ms=1000)
        thread.join(timeout=1)
        self.assertGreaterEqual(state["sequence"], 2)
        self.assertEqual(state["summary"], "expanded")

    def test_wait_timeout_returns_current_state_without_cancelling(self):
        state = self.registry.wait("r1", after_sequence=10, timeout_ms=50)
        self.assertEqual(state["sequence"], 0)
        self.assertEqual(state["status"], "accepted")
        self.assertEqual(self.registry.runs["r1"].status, "accepted")

    def test_wait_returns_terminal_result_immediately(self):
        self.registry.emit(
            "plugin.started",
            {
                "plugin_id": "mcts",
                "run_id": "r1",
                "sequence": 1,
                "summary": "started",
            },
        )
        result = ResultBuilder(root_state_id="state-root").build(
            answer="ok",
            summary="completed",
            stop_reason="budget_exhausted",
        )
        self.registry.emit(
            "plugin.completed",
            {
                "plugin_id": "mcts",
                "run_id": "r1",
                "sequence": 2,
                "result": result,
            },
        )
        state = self.registry.wait("r1", after_sequence=99, timeout_ms=50)
        self.assertEqual(state["status"], "completed")
        self.assertIn("result", state)


if __name__ == "__main__":
    unittest.main()
