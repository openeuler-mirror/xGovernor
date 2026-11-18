"""Finishing remains available after the managed exploration allowance is spent."""
import tempfile
from pathlib import Path
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from xgovernor_mcts.bridge.mock_host import FakeHostCapabilities
from xgovernor_mcts.engine.executor import BenchmarkStrategy, RunExecutor
from xgovernor_mcts.engine.mcts_coordinator import Expansion
from xgovernor_mcts.tests.test_plugin_safety_gates import _params
from xgovernor_mcts.tests.test_benchmark_strategy import ExecutorStub, ScriptedHost, Task


class ManagedBudgetTests(unittest.TestCase):
    def test_budget_stop_selects_exports_and_evaluates(self):
        for budget_kind in ("token", "time"):
            with self.subTest(budget=budget_kind):
                self._run_budget_case(budget_kind)

    def _run_budget_case(self, budget_kind):
        events, expanded, finished = [], [], []
        clock = [0.0]
        strategy = SimpleNamespace(
            task=object(), oracle_ready=threading.Event(), author_error=None,
            prepare=lambda executor: None,
            _author_init=lambda executor: strategy.oracle_ready.set(),
        )
        executor = RunExecutor(_params(100), None,
                               lambda method, data: events.append((method, data)),
                               strategy=strategy,
                               managed_parameters={"max_concurrent": 1})
        managed_peer = executor.host.peer
        executor.host = FakeHostCapabilities()

        def expand(run, parent, child, host):
            expanded.append(child.node_id)
            notification = {"jsonrpc": "2.0", "method": "host.job.completed", "params": {
                "run_id": "r1", "job_id": "solver", "sequence": 2,
                "output": "fixed", "usage": {"total_tokens": 100 if budget_kind == "token" else 0}, "truncated": False}}
            managed_peer.dispatch(notification)
            managed_peer.dispatch(notification)
            if budget_kind == "time":
                clock[0] = 0.95
            return Expansion(1.0, "checked", output="fixed")

        def export(node):
            self.assertGreater(executor.remaining_ms(), 0)
            executor.check()
            finished.append("export")
            return "patch"

        strategy.expand = expand
        with patch("xgovernor_mcts.engine.executor.time.monotonic", side_effect=lambda: clock[0]), \
             patch("xgovernor_mcts.engine.executor.select_node", side_effect=lambda root, task, fallback, **kw: finished.append("select") or fallback), \
             patch.object(executor, "_official_target", return_value=None), \
             patch.object(executor, "_export_final_artifact", side_effect=export), \
             patch.object(executor, "_export_final_files", return_value={}), \
             patch.object(executor, "_run_official", side_effect=lambda node: finished.append("official") or (True, "report")), \
             patch.object(executor, "_write_search_trace"):
            executor.run()
        self.assertEqual(expanded, ["root/c0"])
        self.assertEqual(finished, ["select", "export", "official"])
        method, data = events[-1]
        self.assertEqual(method, "plugin.completed")
        self.assertEqual(data["result"]["stop_reason"], budget_kind + "_budget")
        self.assertEqual(data["result"]["selected_candidate_id"], "root/c0")
        self.assertEqual(data["result"]["answer"], "patch")

    def test_failed_turns_keep_candidates_and_finish_without_fabricating_any(self):
        class Jobs(dict):
            def get(self, run_id, job_id=None):
                return SimpleNamespace(**self[job_id]) if job_id else super().get(run_id)

        class Host(ScriptedHost):
            def __init__(self, failed_profile):
                super().__init__()
                self.jobs = Jobs()
                self.failed_profile = failed_profile
                self.failed = False
                self.stub = ExecutorStub(self)

            def agent_turn(self, *args, **kwargs):
                receipt = super().agent_turn(*args, **kwargs)
                if kwargs.get("profile") == self.failed_profile and not self.failed:
                    self.failed = True
                    self.jobs[receipt["job_id"]].update(status="failed", failure={"code": "timeout", "message": "turn timed out"}, usage={"total_tokens": 1})
                else:
                    self.stub.wait_job(receipt)
                return receipt

            def exec(self, *args, **kwargs):
                receipt = super().exec(*args, **kwargs)
                self.stub.wait_job(receipt)
                return receipt

        # One failed Solver must not discard its successful sibling. A failed
        # initial Author leaves no evaluated candidate, even if Solver finished.
        for profile, expected_count in (("solver-init", 1), ("author-init", 0)):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as tmp:
                events, finished = [], []
                strategy = BenchmarkStrategy(Task(Path(tmp) / "tests.py"), lambda *a, **kw: (1.0, "checked"))
                params = _params(100)
                params["budget"].update(max_iters=0, branching=2, max_time_ms=30_000)
                run = RunExecutor(params, None, lambda m, d: events.append((m, d)), strategy=strategy,
                                  managed_parameters={"max_concurrent": 2})
                run.host = Host(profile)
                with patch("xgovernor_mcts.engine.executor.select_node", side_effect=lambda root, task, fallback, **kw: finished.append("select") or fallback), \
                     patch.object(run, "_official_target", return_value=None), \
                     patch.object(run, "_export_final_artifact", side_effect=lambda node: finished.append("export") or "patch"), \
                     patch.object(run, "_export_final_files", return_value={}), \
                     patch.object(run, "_run_official", side_effect=lambda node: finished.append("official") or (True, "report")), \
                     patch.object(run, "_write_search_trace"):
                    run.run()
                self.assertEqual(events[-1][0], "plugin.completed")
                result = events[-1][1]["result"]
                self.assertEqual(len(result["candidates"]), expected_count)
                self.assertEqual(finished, ["select", "export", "official"] if expected_count else [])
                self.assertTrue(any('"status": "failed"' in d.get("summary", "") for m, d in events))
                if not expected_count:
                    self.assertIsNone(result["selected_candidate_id"])
                    self.assertIsNone(result["apply_plan"])
                    self.assertIn("No candidates", result["summary"])
