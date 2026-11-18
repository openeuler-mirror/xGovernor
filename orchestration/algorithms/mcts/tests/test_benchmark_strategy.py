from __future__ import annotations

import tempfile
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

from xgovernor_mcts.engine.host_api import RunContext
from xgovernor_mcts.engine.mcts_coordinator import Node
from xgovernor_mcts.engine.executor import BenchmarkStrategy
from xgovernor_mcts.bridge.mock_host import FakeHostCapabilities
from xgovernor_tasks.swe_bench.scorer import pytest as swe_pytest


# Deliberately synthetic scorer output.  It verifies the pass-rate parser only;
# it is not produced by a model, benchmark instance, or real pytest process.
SYNTHETIC_PYTEST_SUMMARY = "================ 3 passed, 1 failed in 0.2s ================"


@dataclass
class Spec:
    oracle_rel_path: str = "testcases.py"


class Task:
    spec = Spec()
    def __init__(self, oracle_path): self.oracle_host_path = oracle_path
    def bootstrap_prepare(self, host, branch_id): pass
    def materialize_script(self): return "materialize"
    def init_author(self): return "author: create tests"
    def init_solver(self): return "solver: implement solution"
    def step_solver(self, prior):
        if not prior.get("pytest_output"):
            raise ValueError("parent retest evidence required")
        return "solver: use benchmark evidence\n" + prior["pytest_output"]
    def step_author(self, prior, node_id=""):
        return "author: revise tests\n" + prior["memory"]


class ScriptedHost(FakeHostCapabilities):
    def __init__(self):
        super().__init__()
        self.counter = 0
        self.job_data = {}
        self.snapshots = {}

    def fork(self, parent_state_id, *, timeout_ms, idempotency_key, offline=False):
        result = super().fork(parent_state_id, timeout_ms=timeout_ms, idempotency_key=idempotency_key, offline=offline)
        self.files.update({(result["branch_id"], path): data for (branch, path), data in list(self.files.items()) if branch == self.states[parent_state_id]})
        return result

    def snapshot(self, branch_id, *, timeout_ms, idempotency_key):
        result = super().snapshot(branch_id, timeout_ms=timeout_ms, idempotency_key=idempotency_key)
        self.snapshots[result["state_id"]] = dict((path, data) for (branch, path), data in self.files.items() if branch == branch_id)
        return result

    def agent_turn(self, branch_id, text, *, timeout_ms, idempotency_key, profile=None):
        receipt = super().agent_turn(branch_id, text, timeout_ms=timeout_ms, idempotency_key=idempotency_key, profile=profile)
        self.job_data[receipt["job_id"]] = ("agent", branch_id, text, profile)
        return receipt

    def exec(self, branch_id, argv, *, cwd, timeout_ms, idempotency_key):
        receipt = super().exec(branch_id, argv, cwd=cwd, timeout_ms=timeout_ms, idempotency_key=idempotency_key)
        self.job_data[receipt["job_id"]] = ("exec", branch_id, argv, cwd)
        return receipt


class ExecutorStub:
    def __init__(self, host):
        self.host = host
        self.context = RunContext("mcts", "run", "state-root", set(host.granted))
        self.deadline = time.monotonic() + 60
        self.tokens = 0
        self.events = []

    def remaining_ms(self): return max(1, int((self.deadline-time.monotonic())*1000))
    def check(self): pass
    def emit(self, method, params): self.events.append((method, params))

    def wait_job(self, receipt):
        self.assert_valid_receipt(receipt)
        kind, branch_id, payload, extra = self.host.job_data[receipt["job_id"]]
        if kind == "agent":
            if extra == "author-init":
                self.host.files[(branch_id, "/home/user/workspace/repo/testcases.py")] = b"def test_generated():\n    assert True\n"
                output = "oracle created"
            elif extra == "author-step":
                self.host.files[(branch_id, "/home/user/workspace/repo/testcases.py")] = b"def test_generated():\n    assert 1 == 1\n"
                output = "oracle revised"
            else:
                output = "<solution>patch</solution><reflection>tested</reflection><query></query>"
            self.host.jobs[receipt["job_id"]].update(status="completed", output=output, usage={"total_tokens": 7})
            return self.host.jobs[receipt["job_id"]]
        command = " ".join(payload)
        output = "" if "pytest" not in command else SYNTHETIC_PYTEST_SUMMARY
        code = 0 if "pytest" not in command else 1
        self.host.jobs[receipt["job_id"]].update(status="completed", output=output, exit_code=code, usage={})
        return self.host.jobs[receipt["job_id"]]

    @staticmethod
    def assert_valid_receipt(receipt):
        if not isinstance(receipt, dict) or not receipt.get("job_id"):
            raise AssertionError("host exec/agent_turn must return a job receipt")


class BenchmarkStrategyTests(unittest.TestCase):
    def test_author_solver_and_pytest_scoring_use_isolated_branches(self):
        with tempfile.TemporaryDirectory() as tmp:
            oracle = Path(tmp) / "testcases.py"
            task = Task(oracle)
            host = ScriptedHost()
            executor = ExecutorStub(host)
            strategy = BenchmarkStrategy(task, swe_pytest, oracle=oracle, evaluation_version="sweverified:pytest:v1")
            strategy.initialize(executor)
            root = Node("root", state_id=strategy.search_root_state_id)
            child_branch = host.fork(root.state_id, timeout_ms=1000, idempotency_key="child")
            child = Node("root/c0", branch_id=child_branch["branch_id"], parent=root)
            expansion = strategy.expand(executor, root, child, host)

            self.assertAlmostEqual(expansion.score, 0.75)
            self.assertEqual(expansion.output, "patch")
            self.assertEqual(expansion.oracle_version, 0)
            self.assertNotEqual(expansion.state_id, child.branch_id)
            self.assertNotIn("/home/user/workspace/repo/testcases.py", host.snapshots[expansion.state_id])
            calls = host.calls
            solver = [args for name, args in calls if name == "agent_turn" and args.get("profile") == "solver-init"]
            self.assertEqual(len(solver), 1)
            self.assertNotIn(":evaluate", str(calls))
            execs = [args for name, args in calls if name == "exec"]
            self.assertTrue(any("run_in_instance.sh" in " ".join(args["argv"]) for args in execs))
            self.assertTrue(all(args["branch_id"] != child.branch_id for args in execs))
            score_branch = next(args["branch_id"] for name, args in calls if name == "file_write" and args["path"].endswith("testcases.py"))
            self.assertNotEqual(score_branch, child.branch_id)

    def test_oracle_version_change_forces_parent_retest(self):
        with tempfile.TemporaryDirectory() as tmp:
            oracle = Path(tmp) / "testcases.py"
            task = Task(oracle)
            host = ScriptedHost()
            executor = ExecutorStub(host)
            strategy = BenchmarkStrategy(task, swe_pytest, oracle=oracle)
            strategy.initialize(executor)
            root = Node("root", state_id=strategy.search_root_state_id)
            first_branch = host.fork(root.state_id, timeout_ms=1000, idempotency_key="first-child")
            first = Node("root/c0", parent=root, branch_id=first_branch["branch_id"])
            result = strategy.expand(executor, root, first, host)
            first.state_id, first.score, first.feedback = result.state_id, result.score, result.feedback
            first.oracle_version = result.oracle_version

            strategy.oracle_version = 1
            oracle.write_bytes(b"# xiaoo-testcases-version: 1\ndef test_updated(): assert True\n")
            strategy.oracle_bytes = oracle.read_bytes()
            second_branch = host.fork(first.state_id, timeout_ms=1000, idempotency_key="second-child")
            second = Node("root/c0/c0", parent=first, branch_id=second_branch["branch_id"])
            strategy.expand(executor, first, second, host)

            pytest_runs = [args for name, args in host.calls if name == "exec" and "pytest" in " ".join(args["argv"])]
            self.assertEqual(len(pytest_runs), 3)  # first candidate, parent retest, new candidate
            self.assertEqual(first.oracle_version, 1)
            solver_prompts = [args["input"] for name, args in host.calls if name == "agent_turn" and args.get("profile") == "solver-step"]
            self.assertIn("Test result", solver_prompts[-1])

    def test_job_receipt_is_not_a_command_result(self):
        from xgovernor_tasks.common.host_benchmark import BenchmarkBranch
        host = FakeHostCapabilities()
        def bad_wait(receipt): return receipt
        with self.assertRaisesRegex(RuntimeError, "completed job result"):
            BenchmarkBranch(host, "branch-root", bad_wait).exec("pytest testcases.py")

    def test_zsoft_search_reward_keeps_preflight_verify_formula(self):
        from xgovernor_tasks.zsoft.scorer import parse_verify_output, score_attempt
        ratio, _ = parse_verify_output("PASS trigger\nPASS cleanup\nFAIL negative-control")
        self.assertAlmostEqual(ratio, 2 / 3)
        score, evidence = score_attempt(0, ratio, "verify evidence")
        self.assertAlmostEqual(score, 0.85 + 0.15 * (2 / 3))
        self.assertIn("preflight=trigger", evidence)
        self.assertIn("verify=0.67", evidence)
        score, _ = score_attempt(1, 0.5)
        self.assertAlmostEqual(score, 0.475)
        score, _ = score_attempt(3, 0.5)
        self.assertAlmostEqual(score, 0.05)

    def test_oracle_version_change_forces_parent_retest(self):
        with tempfile.TemporaryDirectory() as tmp:
            oracle = Path(tmp) / "testcases.py"
            task = Task(oracle)
            host = ScriptedHost()
            executor = ExecutorStub(host)
            strategy = BenchmarkStrategy(task, swe_pytest, oracle=oracle)
            strategy.initialize(executor)
            root = Node("root", state_id=strategy.search_root_state_id)
            first_branch = host.fork(root.state_id, timeout_ms=1000, idempotency_key="first-child")
            first = Node("root/c0", parent=root, branch_id=first_branch["branch_id"])
            result = strategy.expand(executor, root, first, host)
            first.state_id, first.score, first.feedback = result.state_id, result.score, result.feedback
            first.oracle_version = result.oracle_version
            strategy.oracle_version = 1
            oracle.write_bytes(b"# xiaoo-testcases-version: 1\ndef test_updated(): assert True\n")
            strategy.oracle_bytes = oracle.read_bytes()
            second_branch = host.fork(first.state_id, timeout_ms=1000, idempotency_key="second-child")
            second = Node("root/c0/c0", parent=first, branch_id=second_branch["branch_id"])
            strategy.expand(executor, first, second, host)
            pytest_runs = [args for name, args in host.calls if name == "exec" and "pytest" in " ".join(args["argv"])]
            self.assertEqual(len(pytest_runs), 3)
            self.assertEqual(first.oracle_version, 1)
            prompts = [args["input"] for name, args in host.calls if name == "agent_turn" and args.get("profile") == "solver-step"]
            self.assertIn("benchmark evidence", prompts[-1])

    def test_zsoft_search_reward_keeps_preflight_verify_formula(self):
        from xgovernor_tasks.zsoft.scorer import parse_verify_output, score_attempt
        ratio, _ = parse_verify_output("PASS trigger\nPASS cleanup\nFAIL negative-control")
        self.assertAlmostEqual(ratio, 2 / 3)
        score, evidence = score_attempt(0, ratio, "verify evidence")
        self.assertAlmostEqual(score, 0.85 + 0.15 * (2 / 3))
        self.assertIn("preflight=trigger", evidence)
        self.assertIn("verify=0.67", evidence)
        score, _ = score_attempt(1, 0.5)
        self.assertAlmostEqual(score, 0.475)
        score, _ = score_attempt(3, 0.5)
        self.assertAlmostEqual(score, 0.05)


if __name__ == "__main__":
    unittest.main()
