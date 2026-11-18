import unittest
import threading

from xgovernor_mcts.engine.host_api import FakeClock, RunContext
from xgovernor_mcts.engine.mcts_coordinator import Node
from xgovernor_mcts.engine.mcts_coordinator import Expansion, Coordinator as HostCoordinator
from xgovernor_mcts.bridge.mock_host import FakeHostCapabilities


class P2PortableCoreTests(unittest.TestCase):
    def test_fake_host_runs_without_xgovernor_or_http(self):
        host = FakeHostCapabilities()
        root = Node("root", state_id="state-root")
        run = RunContext("mcts", "run-1", "state-root", set(host.granted))

        def step(parent, child, capabilities):
            receipt = capabilities.agent_turn(child.branch_id, f"expand {child.node_id}", timeout_ms=1, idempotency_key=child.node_id)
            child.job_id = receipt["job_id"]
            # This coordinator contract test supplies an arbitrary mock reward;
            # it does not represent a benchmark result.
            return Expansion(0.75, "synthetic mock reward", output="candidate", query="")

        best = HostCoordinator(root, host=host, run=run, step=step, max_expansions=2, branching=2, clock=FakeClock()).run()
        self.assertIn(best.node_id, {"root", "root/c0", "root/c0/c0"})
        self.assertEqual(len(root.children), 2)
        self.assertTrue(all(node.state_id for node in root.children))
        self.assertTrue(any(call[0] == "agent_turn" for call in host.calls))
        self.assertEqual(len(host.branches), 1)  # root branch remains; children released

    def test_cancellation_stops_new_expansion(self):
        host = FakeHostCapabilities()
        root = Node("root", state_id="state-root")
        run = RunContext("mcts", "run-2", "state-root", set(host.granted), cancelled=True)
        best = HostCoordinator(root, host=host, run=run, step=lambda *_: Expansion(1, "1"), max_expansions=3).run()
        self.assertIs(best, root)
        self.assertEqual(host.calls, [])

    def test_concurrent_queue_runs_sibling_rollouts_in_parallel(self):
        host = _TimingHostCapabilities()
        root = Node("root", state_id="state-root")
        run = RunContext("mcts", "run-concurrent", "state-root", set(host.granted))
        barrier = threading.Barrier(3)
        active = 0
        max_active = 0
        lock = threading.Lock()

        def step(parent, child, capabilities):
            nonlocal active, max_active
            barrier.wait(timeout=2.0)
            with lock:
                active += 1
                max_active = max(max_active, active)
            threading.Event().wait(0.02)
            with lock:
                active -= 1
            return Expansion(1.0, "synthetic", output="candidate", query="")

        HostCoordinator(
            root,
            host=host,
            run=run,
            step=step,
            max_expansions=1,
            branching=3,
            max_concurrent=3,
        ).run()
        self.assertEqual(len(root.children), 3)
        self.assertGreaterEqual(max_active, 3)


class _TimingHostCapabilities:
    granted = set()

    def fork(self, parent_state_id, *, timeout_ms, idempotency_key, offline=False):
        return {"branch_id": f"branch-{idempotency_key}"}

    def snapshot(self, branch_id, *, timeout_ms, idempotency_key):
        return {"state_id": f"state-{idempotency_key}"}

    def release(self, branch_id, *, timeout_ms, idempotency_key):
        return None


if __name__ == "__main__":
    unittest.main()
