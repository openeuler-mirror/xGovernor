from __future__ import annotations

import time
import unittest
from unittest import mock

from xgovernor_mcts.engine.mcts_coordinator import Node
from xgovernor_mcts.bridge import launch_spec
from xgovernor_mcts.engine.executor import RunExecutor
from xgovernor_tasks.zsoft import trial


class _TrialTask:
    task_id = "mock-task"
    manifest = type("Manifest", (), {"tags": []})()

    def issue_text(self) -> str:
        return "contract"


class _Spec:
    def __init__(self, official_fn):
        self._official_fn = official_fn

    def official_fn(self):
        return self._official_fn


class _Task:
    def __init__(self, official_fn=None):
        self.spec = _Spec(official_fn)


class _Strategy:
    def __init__(self, task):
        self.task = task


class _OfficialHost:
    def __init__(self, fork_error=None):
        self.fork_error = fork_error
        self.fork_calls = 0
        self.release_calls = 0

    def fork(self, *args, **kwargs):
        self.fork_calls += 1
        if self.fork_error is not None:
            raise self.fork_error
        return {"branch_id": "branch-official"}

    def release(self, *args, **kwargs):
        self.release_calls += 1


class _Job:
    def __init__(self, status="completed", usage=None, sequence=2):
        self.status = status
        self.usage = usage or {}
        self.sequence = sequence
        self.failure = None
        self.output = ""


def _params(host_budget=1000):
    return {
        "plugin_id": "mcts",
        "run_id": "r1",
        "root_state_id": "state-root",
        "granted_capabilities": ["snapshot", "fork", "agent_turn", "exec", "file_read", "file_write", "release"],
        "budget": {"max_iters": 5, "branching": 3, "max_time_ms": 1000, "max_tokens": host_budget},
    }


class TrialDefaultOffTests(unittest.TestCase):
    def test_empty_url_disables_submission(self):
        task = _TrialTask()
        with mock.patch.object(trial, "trial_server_url", return_value=""):
            self.assertFalse(trial.trial_enabled(task))
            with mock.patch.object(trial, "record_trial"):
                with mock.patch.object(trial, "_fetch_trial") as fetch:
                    result = trial.run_judge_trial(
                        task, b"poc", host=object(), branch_id="b",
                        wait_job=lambda receipt: {"output": ""},
                    )
            self.assertIsNone(result)
            fetch.assert_not_called()

    def test_explicit_trial_fetch_failure_degrades_to_none(self):
        task = _TrialTask()
        with mock.patch.object(trial, "trial_server_url", return_value="http://127.0.0.1:8666"):
            with mock.patch.object(trial, "record_trial"):
                with mock.patch.object(trial, "_fetch_trial", return_value=None):
                    result = trial.run_judge_trial(
                        task, b"poc", host=object(), branch_id="b",
                        wait_job=lambda receipt: {"output": ""},
                    )
        self.assertIsNone(result)


class OfficialOnceGateTests(unittest.TestCase):
    def test_official_runs_at_most_once(self):
        calls = []
        task = _Task(lambda *args, **kwargs: calls.append(args[0].node_id) or (True, "passed"))
        executor = RunExecutor(_params(), None, lambda *args: None, strategy=_Strategy(task))
        executor.host = _OfficialHost()
        selected = Node("root/c0", state_id="state-1")

        self.assertEqual(executor._run_official(selected), (True, "passed"))
        self.assertIsNone(executor._run_official(selected))
        self.assertEqual(calls, ["root/c0"])
        self.assertTrue(executor.official_started)
        self.assertTrue(executor.official_done)

    def test_failed_official_is_not_retried(self):
        calls = []
        def official(*args, **kwargs):
            calls.append(args[0].node_id)
            raise RuntimeError("official boom")
        executor = RunExecutor(_params(), None, lambda *args: None, strategy=_Strategy(_Task(official)))
        executor.host = _OfficialHost()
        selected = Node("root/c0", state_id="state-1")

        with self.assertRaises(RuntimeError):
            executor._run_official(selected)
        self.assertIsNone(executor._run_official(selected))
        self.assertEqual(calls, ["root/c0"])
        self.assertTrue(executor.official_started)
        self.assertFalse(executor.official_done)

    def test_fork_failure_does_not_count_as_official_attempt(self):
        executor = RunExecutor(_params(), None, lambda *args: None, strategy=_Strategy(_Task(lambda *args, **kwargs: None)))
        executor.host = _OfficialHost(fork_error=RuntimeError("fork boom"))
        selected = Node("root/c0", state_id="state-1")

        with self.assertRaises(RuntimeError):
            executor._run_official(selected)
        self.assertFalse(executor.official_started)
        self.assertFalse(executor.official_done)


class TokenUsageDedupTests(unittest.TestCase):
    def test_completed_job_usage_is_counted_once(self):
        executor = RunExecutor(_params(), None, lambda *args: None)
        job = _Job(status="completed", usage={"total_tokens": 7})
        executor.host = type("Host", (), {
            "jobs": type("Jobs", (), {"get": lambda self, run_id, job_id: job})(),
        })()
        executor.deadline = time.monotonic() + 10

        executor.wait_job({"job_id": "j1"})
        executor.wait_job({"job_id": "j1"})
        self.assertEqual(executor.tokens, 7)

    def test_failed_job_usage_is_counted_once_before_raising(self):
        executor = RunExecutor(_params(), None, lambda *args: None)
        job = _Job(status="failed", usage={"input_tokens": 3, "output_tokens": 2})
        job.failure = {"code": "host-error", "message": "bad"}
        executor.host = type("Host", (), {
            "jobs": type("Jobs", (), {"get": lambda self, run_id, job_id: job})(),
        })()
        executor.deadline = time.monotonic() + 10

        with self.assertRaises(RuntimeError):
            executor.wait_job({"job_id": "j2"})
        with self.assertRaises(RuntimeError):
            executor.wait_job({"job_id": "j2"})
        self.assertEqual(executor.tokens, 5)


class TokenLimitTests(unittest.TestCase):
    def test_host_budget_wins_over_env_fuse(self):
        executor = RunExecutor(_params(host_budget=100), None, lambda *args: None)
        with mock.patch.dict("os.environ", {"XIAOO_MCTS_TOKEN_FUSE_LIMIT": "50"}, clear=False):
            self.assertEqual(executor.token_limit, 100)

    def test_env_fuse_used_when_host_budget_is_zero_or_missing(self):
        params = _params(host_budget=0)
        with mock.patch.dict("os.environ", {"XIAOO_MCTS_TOKEN_FUSE_LIMIT": "200"}, clear=False):
            executor = RunExecutor(params, None, lambda *args: None)
            self.assertEqual(executor.token_limit, 200)
            del params["budget"]["max_tokens"]
            executor = RunExecutor(params, None, lambda *args: None)
            self.assertEqual(executor.token_limit, 200)


class RunDeadlineTests(unittest.TestCase):
    def test_zero_time_budget_starts_without_a_deadline(self):
        params = _params()
        params["budget"]["max_time_ms"] = 0
        strategy = mock.Mock()
        events = []
        executor = RunExecutor(params, None, lambda method, payload: events.append(method), strategy=strategy)

        def prepare(run):
            run.check()
            self.assertGreater(run.remaining_ms(), 0)
            run.cancel("test complete")
            run.check()

        strategy.prepare.side_effect = prepare
        executor.run()
        self.assertEqual(events, ["plugin.started", "plugin.cancelled"])


class NoLegacyLlmEnvTests(unittest.TestCase):
    def test_launch_spec_excludes_legacy_llm_env(self):
        self.assertFalse(
            any(key.startswith("XGOVERNOR_LLM_") for key in launch_spec.required_env())
        )
        command = launch_spec.build_command("zsoft", {"task_id": "mock-task"})
        self.assertFalse(any(part.startswith("XGOVERNOR_LLM_") for part in command))

    def test_executor_boots_without_legacy_llm_env(self):
        with mock.patch.dict(
            "os.environ",
            {
                "XGOVERNOR_LLM_PROVIDER": "",
                "XGOVERNOR_LLM_MODEL": "",
                "XGOVERNOR_LLM_API_BASE": "",
                "XGOVERNOR_LLM_KEY_ENV": "",
            },
            clear=False,
        ):
            executor = RunExecutor(_params(), None, lambda *args: None)
        self.assertEqual(executor.tokens, 0)
        self.assertFalse(executor.thread.is_alive())


if __name__ == "__main__":
    unittest.main()
