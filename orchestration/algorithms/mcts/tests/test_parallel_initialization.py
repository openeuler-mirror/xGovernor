from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock

from xgovernor_mcts.engine.mcts_coordinator import Coordinator, Node
from xgovernor_mcts.engine.executor import BenchmarkStrategy
from xgovernor_mcts.tests.test_benchmark_strategy import ExecutorStub, ScriptedHost, Task


ORACLE_PATH = "/home/user/workspace/repo/testcases.py"
ORACLE_BYTES = b"def test_generated():\n    assert True\n"


def _wait_until(predicate, *, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class ParallelInitializationTests(unittest.TestCase):
    def test_dynamic_sibling_replays_blind_solver_before_initial_author_finishes(self):
        finish_author = threading.Event()
        sibling_started = threading.Event()
        both_solvers_finished = threading.Event()
        solver_inputs = []
        solver_jobs = []
        author_waits = []
        scored = []

        class BlockingExecutor(ExecutorStub):
            def wait_job(self, receipt, *, on_progress=None):
                kind, branch, prompt, profile = self.host.job_data[receipt["job_id"]]
                if kind == "agent" and profile == "author-init":
                    author_waits.append(receipt["job_id"])
                    if not finish_author.wait(5):
                        raise TimeoutError("test did not release Author")
                if kind == "agent" and profile == "solver-init":
                    if (branch, ORACLE_PATH) in self.host.files:
                        raise AssertionError("first-layer Solver can see the Author oracle")
                    solver_inputs.append((prompt, profile))
                    if len(solver_inputs) == 1:
                        on_progress({"sequence": 1, "tools": [{"name": "read"}]})
                        if not sibling_started.wait(5):
                            raise TimeoutError("dynamic sibling did not start")
                    else:
                        sibling_started.set()
                result = super().wait_job(receipt)
                if kind == "agent" and profile == "solver-init":
                    solver_jobs.append(receipt["job_id"])
                    if len(solver_jobs) == 2:
                        both_solvers_finished.set()
                return result

        with tempfile.TemporaryDirectory() as tmp:
            host = ScriptedHost()
            executor = BlockingExecutor(host)
            task = Task(Path(tmp) / "testcases.py")

            def score(host, branch, *args, **kwargs):
                self.assertTrue(finish_author.is_set())
                self.assertIn(b"test_generated", task.oracle_host_path.read_bytes())
                scored.append(branch)
                return 1.0, "tested"

            strategy = BenchmarkStrategy(task, score)
            strategy.prepare(executor)
            root = Node("root", state_id=strategy.search_root_state_id)
            self.assertNotIn(ORACLE_PATH, host.snapshots[root.state_id])

            author_thread = threading.Thread(
                target=strategy._author_init, args=(executor,), daemon=True
            )
            author_thread.start()

            coordinator = Coordinator(
                root, host=host, run_context=executor.context,
                step=lambda parent, child, host: strategy.expand(executor, parent, child, host),
                max_expansions=1, max_concurrent=2,
                should_fork=lambda context, turn_id: True,
            )
            executor.coordinator = coordinator
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(coordinator.run)
                try:
                    self.assertTrue(both_solvers_finished.wait(2))
                    self.assertFalse(scored, "Scoring must wait for the initial tests")
                finally:
                    finish_author.set()
                result.result(timeout=2)

            self.assertEqual(coordinator.dynamic_fork_count, 1)
            first, sibling = sorted(root.children, key=lambda node: node.node_id)
            self.assertIs(first.solver_input, sibling.solver_input)
            sibling_fork = next(
                args for method, args in host.calls
                if method == "fork" and args["idempotency_key"] == "run:root/c1:fork"
            )
            self.assertEqual(sibling_fork["parent_state_id"], first.solver_input.state_id)
            self.assertNotEqual(sibling_fork["parent_state_id"], root.state_id)
            self.assertTrue(sibling_fork["offline"])
            self.assertEqual(solver_inputs, [(task.init_solver(), "solver-init")] * 2)
            self.assertEqual([first.oracle_version, sibling.oracle_version], [0, 0])

            author_thread.join(timeout=2)
            self.assertFalse(author_thread.is_alive())
            self.assertEqual(len(author_waits), 1)
            self.assertEqual(len(scored), 2)
            self.assertTrue(strategy.oracle_ready.is_set())
            self.assertNotEqual(strategy.author_state_id, root.state_id)

    def test_initial_author_failure_prevents_all_scoring(self):
        class FailedAuthorExecutor(ExecutorStub):
            def wait_job(self, receipt):
                if self.host.job_data[receipt["job_id"]][3] == "author-init":
                    raise RuntimeError("initial Author failed")
                return super().wait_job(receipt)

        with tempfile.TemporaryDirectory() as tmp:
            host = ScriptedHost()
            executor = FailedAuthorExecutor(host)
            scorer = Mock()
            strategy = BenchmarkStrategy(Task(Path(tmp) / "tests.py"), scorer)
            strategy.prepare(executor)
            strategy._author_init(executor)
            self.assertTrue(strategy.oracle_ready.is_set())
            self.assertIsNotNone(strategy.author_error)

            root = Node("root", state_id=strategy.search_root_state_id)
            for index in range(2):
                branch = host.fork(root.state_id, timeout_ms=1000, idempotency_key=f"child-{index}")
                child = Node(f"root/c{index}", parent=root, branch_id=branch["branch_id"])
                with self.assertRaisesRegex(RuntimeError, "initial Author failed"):
                    strategy.expand(executor, root, child, host)
            scorer.assert_not_called()

    def test_stopping_before_scoring_cancels_and_releases_initial_author(self):
        author_started = threading.Event()
        release_author = threading.Event()

        class BlockingExecutor(ExecutorStub):
            def wait_job(self, receipt):
                if self.host.job_data[receipt["job_id"]][3] == "author-init":
                    author_started.set()
                    if not release_author.wait(5):
                        raise TimeoutError("test did not release Author")
                return super().wait_job(receipt)

        with tempfile.TemporaryDirectory() as tmp:
            host = ScriptedHost()
            host.cancel_job = Mock(wraps=host.cancel_job)
            executor = BlockingExecutor(host)
            strategy = BenchmarkStrategy(Task(Path(tmp) / "tests.py"), Mock())
            strategy.prepare(executor)
            author_thread = threading.Thread(
                target=strategy._author_init, args=(executor,), daemon=True
            )
            author_thread.start()
            self.assertTrue(author_started.wait(2))
            self.assertTrue(_wait_until(lambda: strategy._initial_author is not None))
            branch, receipt = strategy._initial_author

            strategy.stop_author_worker(executor)
            self.assertEqual(host.cancel_job.call_args.args, (receipt["job_id"],))
            self.assertNotIn(branch, host.branches)
            self.assertIsNone(strategy._initial_author)

            release_author.set()
            author_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
