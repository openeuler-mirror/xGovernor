from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from unittest import mock

from xgovernor_mcts.engine.mcts_coordinator import Node
from xgovernor_mcts.bridge.mock_host import FakeHostCapabilities
from xgovernor_tasks.swe_bench.official import evaluate as swe_evaluate
from xgovernor_tasks.swe_bench.scorer import pytest as score_swe
from xgovernor_tasks.zsoft.official import evaluate as zsoft_evaluate


@dataclass
class Spec:
    id: str = "mock"
    oracle_rel_path: str = "testcases.py"

    def artifact_reader_fn(self):
        from xgovernor_tasks.swe_bench.artifact import git_diff
        return git_diff


class _Task:
    spec = Spec()
    testbench = "sweverified"
    instance_id = "mock__case"
    dataset_name = "SWE-bench/SWE-bench_Verified"
    split = "test"
    task_id = "zsoft-mock"
    poc_filename = "poc.py"
    poc_sandbox_path = "/home/user/workspace/poc.py"


class OfficialHost(FakeHostCapabilities):
    """Fake host that makes every command a completed asynchronous job."""
    def __init__(self, *, verdict=True, zsoft=False):
        super().__init__()
        self.verdict = verdict
        self.zsoft = zsoft
        self.waited = []

    def wait(self, receipt):
        self.waited.append(receipt["job_id"])
        job = self.jobs[receipt["job_id"]]
        argv = " ".join(job.get("argv", []))
        if "git status" in argv:
            job.update(status="completed", output=" M app.py", exit_code=0)
        elif "git diff" in argv:
            job.update(status="completed", output="diff --git a/app.py b/app.py", exit_code=0)
        elif "zsoft-poc evaluate" in argv:
            payload = {"task_id": "zsoft-mock", "attempt_id": "a1", "result": {
                "status": "completed", "success": self.verdict,
                "summary": "mock judge", "checks": [], "details": {}}}
            job.update(status="completed", output=json.dumps(payload), exit_code=0)
        else:
            job.update(status="completed", output="", exit_code=0)
        return job

    def file_read(self, branch_id, path, *, timeout_ms):
        if path.endswith("poc.py"):
            self._call("file_read", branch_id=branch_id, path=path, timeout_ms=timeout_ms)
            return b"print('poc')\n"
        if path.endswith("verdict.json"):
            self._call("file_read", branch_id=branch_id, path=path, timeout_ms=timeout_ms)
            return json.dumps({"passed": self.verdict}).encode()
        return super().file_read(branch_id, path, timeout_ms=timeout_ms)


class OfficialMockTests(unittest.TestCase):
    def test_scoring_and_delivery_keys_isolate_branches_and_survive_retry(self):
        class StrictHost(OfficialHost):
            def __init__(self):
                super().__init__()
                self.identities = {}

            def _call(self, capability, **params):
                if "idempotency_key" in params:
                    key = (capability, params["idempotency_key"])
                    payload = {k: v for k, v in params.items() if k != "timeout_ms"}
                    if key in self.identities and self.identities[key] != payload:
                        raise RuntimeError("IdempotencyConflict: arguments changed")
                    self.identities[key] = payload
                super()._call(capability, **params)

            def wait(self, receipt):
                job = super().wait(receipt)
                if " pytest " in " ".join(job.get("argv", [])):
                    job["output"] = "======= 1 passed in 0.1s ======="
                return job

        host, task = StrictHost(), _Task()
        with tempfile.TemporaryDirectory() as tmp:
            for branch in ("score-left", "score-right"):
                node = Node("root/" + branch, state_id="state-root")

                def finish():
                    self.assertEqual(score_swe(host, branch, task, wait_job=host.wait)[0], 1.0)
                    passed, patch = swe_evaluate(node, task, Path(tmp) / branch,
                                                host=host, branch_id=branch, wait_job=host.wait)
                    self.assertTrue(passed)
                    self.assertIn("diff --git", patch.read_text())

                finish()
                identities = dict(host.identities)
                finish()
                self.assertEqual(host.identities, identities, "retry changed operation identities")
        # ensure, pytest, artifact status/diff, official preparation/run, prediction
        self.assertEqual(len(host.identities), 14)

    def test_swe_official_waits_for_receipts_and_persists_verdict(self):
        host = OfficialHost(verdict=True)
        node = Node("root/c0", state_id="state-root", branch_id="branch-root")
        task = _Task()
        with tempfile.TemporaryDirectory() as tmp:
            passed, artifact = swe_evaluate(node, task, Path(tmp), host=host,
                                            branch_id="branch-root", wait_job=host.wait)
            self.assertTrue(passed)
            self.assertEqual(artifact.read_text(), "diff --git a/app.py b/app.py")
            out = Path(tmp)
            self.assertTrue((out / "official_result.json").exists())
            self.assertGreaterEqual(len(host.waited), 3)
            self.assertTrue(any("swebench.harness.run_evaluation" in " ".join(c[1]["argv"])
                                for c in host.calls if c[0] == "exec"))

    def test_swe_verdict_false_is_not_synthetic_success(self):
        host = OfficialHost(verdict=False)
        node = Node("root/c0", state_id="state-root", branch_id="branch-root")
        with tempfile.TemporaryDirectory() as tmp:
            passed, _ = swe_evaluate(node, _Task(), Path(tmp), host=host,
                                     branch_id="branch-root", wait_job=host.wait)
            self.assertFalse(passed)

    def test_zsoft_official_reads_poc_and_waits_for_host_judge(self):
        host = OfficialHost(verdict=True, zsoft=True)
        node = Node("root/c0", state_id="state-root", branch_id="branch-root")
        result = {
            "task_id": "zsoft-mock",
            "attempt_id": "a1",
            "status": "completed",
            "success": True,
            "summary": "mock judge",
            "checks": [],
            "details": {},
        }
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch(
                "xgovernor_tasks.zsoft.official._local",
                return_value=(result, "{}", ""),
            ):
                passed, artifact = zsoft_evaluate(node, _Task(), Path(tmp), host=host,
                                                  branch_id="branch-root", wait_job=host.wait)
            self.assertTrue(passed)
            self.assertEqual(artifact.read_bytes(), b"print('poc')\n")
            self.assertTrue((Path(tmp) / "official_result.json").exists())
            self.assertTrue((Path(tmp) / "official.stdout.log").exists())
            self.assertTrue((Path(tmp) / "official.stderr.log").exists())

    def test_zsoft_noncompleted_status_fails(self):
        host = OfficialHost(verdict=True)
        node = Node("root/c0", state_id="state-root", branch_id="branch-root")
        result = {
            "task_id": "zsoft-mock",
            "status": "running",
            "success": True,
            "summary": "",
            "checks": [],
            "details": {},
        }
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch(
                "xgovernor_tasks.zsoft.official._local",
                return_value=(result, "{}", ""),
            ):
                passed, _ = zsoft_evaluate(node, _Task(), Path(tmp), host=host,
                                           branch_id="branch-root", wait_job=host.wait)
            self.assertFalse(passed)


if __name__ == "__main__":
    unittest.main()
