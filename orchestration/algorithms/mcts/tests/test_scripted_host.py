from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from xgovernor_mcts.tests.mock_search import run_mock_search
from xgovernor_mcts.benchmarks.scripted_host import ScriptedHost


class ScriptedHostTest(unittest.TestCase):
    def test_fork_and_agent_turn_complete_by_default(self) -> None:
        host = ScriptedHost()
        branch = host.fork(
            "state-root",
            timeout_ms=1000,
            idempotency_key="test-fork",
        )
        receipt = host.agent_turn(
            branch["branch_id"],
            "solve",
            timeout_ms=1000,
            idempotency_key="test-agent",
            profile="solver-step",
        )
        job = host.jobs[receipt["job_id"]]
        self.assertEqual(job["status"], "completed")
        self.assertIn("<solution>mock patch</solution>", job["output"])
        self.assertIn("job-", receipt["job_id"])

    def test_job_script_overrides_default_completion(self) -> None:
        host = ScriptedHost()
        host.schedule_job(
            "job-2",
            [
                ("host.job.started", {"job_id": "job-2", "sequence": 1}),
                (
                    "host.job.failed",
                    {
                        "job_id": "job-2",
                        "sequence": 2,
                        "failure": {"code": "mock_failure", "message": "boom"},
                        "usage": {},
                    },
                ),
            ],
        )
        branch = host.fork(
            "state-root",
            timeout_ms=1000,
            idempotency_key="test-script-fork",
        )
        receipt = host.agent_turn(
            branch["branch_id"],
            "solve",
            timeout_ms=1000,
            idempotency_key="test-script-agent",
        )
        job = host.jobs[receipt["job_id"]]
        self.assertEqual(job["status"], "failed")
        self.assertEqual(job["failure"]["message"], "boom")

    def test_pytest_exec_receives_synthetic_summary(self) -> None:
        host = ScriptedHost(differentiated_scores=False)
        receipt = host.exec(
            "branch-root",
            ["bash", "-lc", "pytest testcases.py"],
            cwd="/home/user/workspace/repo",
            timeout_ms=1000,
            idempotency_key="test-pytest",
        )
        job = host.jobs[receipt["job_id"]]
        self.assertEqual(job["status"], "completed")
        self.assertIn("3 passed", job["output"])


class MockBenchmarkRunTest(unittest.TestCase):
    def test_mock_search_writes_protocol_and_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result, host, events = run_mock_search(
                max_iters=2,
                output_dir=Path(tmp),
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(host.live_branch_count, 0)
            self.assertTrue((Path(tmp) / "protocol.jsonl").is_file())
            self.assertTrue((Path(tmp) / "result.json").is_file())
            self.assertTrue((Path(tmp) / "search.log").is_file())

            protocol = [
                json.loads(line)
                for line in (Path(tmp) / "protocol.jsonl").read_text().splitlines()
            ]
            methods = [event["method"] for event in protocol]
            self.assertIn("plugin.started", methods)
            self.assertIn("plugin.progress", methods)
            self.assertIn("plugin.completed", methods)
            self.assertEqual(methods[0], "plugin.started")
            self.assertEqual(methods[-1], "plugin.completed")
            self.assertGreaterEqual(len(events), 3)
            trace = (Path(tmp) / "search.log").read_text()
            self.assertIn("▶ choose", trace)
            self.assertIn("▶ selected", trace)

    def test_mock_search_produces_candidate_with_state(self) -> None:
        result, host, _ = run_mock_search(max_iters=1, branching=1)
        self.assertEqual(host.live_branch_count, 0)
        self.assertGreaterEqual(len(result["candidates"]), 1)
        self.assertIsNotNone(result["selected_candidate_id"])
        self.assertIsNotNone(result["apply_plan"])

    def test_mock_search_branching_expands_expected_candidates(self) -> None:
        result, host, _ = run_mock_search(max_iters=1, branching=2)
        self.assertEqual(host.live_branch_count, 0)
        self.assertEqual(len(result["candidates"]), 4)

    def test_mock_search_has_differentiated_scores(self) -> None:
        result, host, _ = run_mock_search(max_iters=1, branching=4)
        scores = {candidate["score"] for candidate in result["candidates"]}
        self.assertEqual(host.live_branch_count, 0)
        self.assertGreater(len(scores), 1)

    def test_mock_search_max_iters_expands_beyond_initialization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result, host, _ = run_mock_search(
                max_iters=2,
                branching=1,
                output_dir=Path(tmp),
            )
            trace = (Path(tmp) / "search.log").read_text()
            self.assertEqual(host.live_branch_count, 0)
            self.assertGreaterEqual(len(result["candidates"]), 3)
            self.assertIn("▶ choose  iter=0  node=root", trace)
            self.assertIn("▶ choose  iter=1  node=root/c0", trace)
            self.assertIn("▶ choose  iter=2  node=root/c0/c0", trace)


if __name__ == "__main__":
    unittest.main()
