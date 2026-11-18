"""OpenCode harness tests: agent_turn, exec, path mapping, dual-tower expand."""
from __future__ import annotations

import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from xgovernor_mcts.engine.mcts_coordinator import Node
from xgovernor_mcts.engine.host_api import RunContext
from xgovernor_opencode_host.opencode_adapter import FakeSessionApi, OpenCodeAdapter, load_agent_profiles
from xgovernor_mcts.engine.submission_selector import collect_candidates
from xgovernor_opencode_host.run_opencode_mcts import (
    OpenCodeGenericTask,
    build_parser,
    opencode_generic_test,
)
from xgovernor_mcts.engine.executor import BenchmarkStrategy


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _init_repo(root: Path) -> Path:
    source = root / "source"
    source.mkdir()
    _git(source, "init")
    _git(source, "config", "user.name", "tester")
    _git(source, "config", "user.email", "tester@example.com")
    (source / "app.py").write_text("print('hi')\n", encoding="utf-8")
    (source / "test_ok.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (source / "test_ok.sh").chmod(0o755)
    _git(source, "add", ".")
    _git(source, "commit", "-m", "initial")
    return source


class _WaitHost:
    """Tiny stand-in for RunExecutor.wait_job using adapter job events."""

    def __init__(self, adapter: OpenCodeAdapter) -> None:
        self.adapter = adapter
        self.events: dict[str, list[dict]] = {}
        self.cond = threading.Condition()

        def emit(method: str, params: dict) -> None:
            job_id = params["job_id"]
            with self.cond:
                self.events.setdefault(job_id, []).append({"method": method, "params": params})
                self.cond.notify_all()

        adapter.job_emitter = emit

    def wait_job(self, receipt: dict):
        job_id = receipt["job_id"]
        deadline = time.monotonic() + 5
        with self.cond:
            while True:
                events = self.events.get(job_id, [])
                for event in events:
                    if event["method"] in {"host.job.completed", "host.job.failed"}:
                        params = event["params"]
                        if event["method"] == "host.job.failed":
                            raise RuntimeError(params["failure"])
                        return type(
                            "Job",
                            (),
                            {
                                "status": "completed",
                                "output": params.get("output", ""),
                                "exit_code": params.get("exit_code"),
                                "usage": params.get("usage", {}),
                            },
                        )()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(job_id)
                self.cond.wait(remaining)


class AgentTurnExecTests(unittest.TestCase):
    def test_selector_reads_latest_text_from_current_turn(self) -> None:
        final = '{"evaluations": []}'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_repo(root)
            sessions = FakeSessionApi(assistant_reply="Interim reply")
            adapter = OpenCodeAdapter(session_api=sessions, profiles=load_agent_profiles("generic"))
            waiter = _WaitHost(adapter)
            state = adapter.create_root_snapshot(repo=str(source), output_dir=str(root / "out"))
            solver = adapter.fork(state, timeout_ms=1000, idempotency_key="solver")["branch_id"]
            session_id = adapter.branches[solver].session_id
            sessions.sessions[session_id].messages.extend(
                {"id": f"old-{i}", "type": "assistant", "text": "Old Solver reply"}
                for i in range(125)
            )
            candidate = adapter.snapshot(solver, timeout_ms=1000, idempotency_key="candidate")
            selector = adapter.fork(candidate["state_id"], timeout_ms=1000, idempotency_key="selector")

            def finish(session_id):
                sessions.sessions[session_id].messages.extend([
                    {"id": "final", "type": "assistant", "content": [{"type": "text", "text": final}]},
                    {"id": "reasoning", "type": "assistant", "content": [{"type": "reasoning", "text": "Thinking"}]},
                ])

            sessions.on_wait = finish
            receipt = adapter.agent_turn(
                selector["branch_id"], "Choose a submission", timeout_ms=1000,
                idempotency_key="select", profile="submission-selector",
            )
            self.assertEqual(waiter.wait_job(receipt).output, final)

            sessions.on_wait = None
            sessions.assistant_reply = ""
            receipt = adapter.agent_turn(
                selector["branch_id"], "Choose again", timeout_ms=1000,
                idempotency_key="select-again", profile="submission-selector",
            )
            self.assertEqual(waiter.wait_job(receipt).output, "")

    def test_agent_turn_receipt_then_completion_and_exec_scores(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_repo(root)
            sessions = FakeSessionApi(assistant_reply="patched")
            adapter = OpenCodeAdapter(session_api=sessions, model="opencode/big-pickle")
            waiter = _WaitHost(adapter)
            state = adapter.create_root_snapshot(repo=str(source), output_dir=str(root / "out"))
            branch = adapter.fork(state, timeout_ms=1000, idempotency_key="f1", run_id="run")
            bid = branch["branch_id"]

            receipt = adapter.agent_turn(
                bid,
                "fix the bug",
                timeout_ms=1000,
                idempotency_key="turn-1",
                run_id="run",
            )
            self.assertEqual(receipt["status"], "accepted")
            job = waiter.wait_job(receipt)
            self.assertEqual(job.output, "patched")
            self.assertTrue(
                any(c[0] == "set_permissions" for c in sessions.calls),
                sessions.calls,
            )

            # Candidate snapshot then local-subprocess score on the same worktree.
            snap = adapter.snapshot(bid, timeout_ms=1000, idempotency_key="s1", run_id="run")
            self.assertTrue(snap["state_id"])
            exec_receipt = adapter.exec(
                bid,
                ["/bin/sh", "-c", "exit 0"],
                cwd=".",
                timeout_ms=1000,
                idempotency_key="e0",
                run_id="run",
            )
            exec_job = waiter.wait_job(exec_receipt)
            self.assertEqual(exec_job.exit_code, 0)

            fail_receipt = adapter.exec(
                bid,
                ["/bin/sh", "-c", "exit 7"],
                cwd=".",
                timeout_ms=1000,
                idempotency_key="e7",
                run_id="run",
            )
            fail_job = waiter.wait_job(fail_receipt)
            self.assertEqual(fail_job.exit_code, 7)

            # wait() should receive timeout_ms from agent_turn
            wait_calls = [c for c in sessions.calls if c[0] == "wait"]
            self.assertTrue(
                any(c[1].get("timeout_ms") == 1000 for c in wait_calls),
                wait_calls,
            )

            adapter.release(bid, timeout_ms=1000, idempotency_key="rb", run_id="run")

    def test_sandbox_paths_map_into_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_repo(root)
            adapter = OpenCodeAdapter(session_api=FakeSessionApi())
            state = adapter.create_root_snapshot(repo=str(source), output_dir=str(root / "out"))
            branch = adapter.fork(state, timeout_ms=1000, idempotency_key="f", run_id="run")
            bid = branch["branch_id"]
            written = adapter.file_write(
                bid,
                "/home/user/workspace/repo/testcases.py",
                b"def test_ok():\n    assert True\n",
                timeout_ms=1000,
                idempotency_key="w",
                run_id="run",
            )
            self.assertGreater(written, 0)
            data = adapter.file_read(
                bid,
                "/home/user/workspace/repo/testcases.py",
                timeout_ms=1000,
                run_id="run",
            )
            self.assertIn(b"assert True", data)
            # Relative path should see the same file.
            self.assertEqual(
                adapter.file_read(bid, "testcases.py", timeout_ms=1000, run_id="run"),
                data,
            )
            adapter.release(bid, timeout_ms=1000, idempotency_key="r", run_id="run")

    def test_benchmark_strategy_expand_uses_generic_verify_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_repo(root)
            sessions = FakeSessionApi(assistant_reply="solver: patch applied")
            adapter = OpenCodeAdapter(session_api=sessions)
            waiter = _WaitHost(adapter)
            state = adapter.create_root_snapshot(repo=str(source), output_dir=str(root / "out"))
            branch = adapter.fork(state, timeout_ms=1000, idempotency_key="f", run_id="run")

            task = OpenCodeGenericTask(
                goal="make tests pass",
                oracle_dir=root / "oracles",
                task_id="expand-case",
            )
            task.oracle_host_path.write_text(
                "#!/bin/sh\necho 'PASS ok'\nexit 0\n", encoding="utf-8"
            )
            strategy = BenchmarkStrategy(
                task,
                opencode_generic_test,
                oracle=task.oracle_host_path,
                evaluation_version="opencode-local:generic_test:v1",
                inherit_scoring_history=False,
            )
            strategy.search_root_state_id = state
            strategy.oracle_bytes = task.oracle_host_path.read_bytes()
            strategy.oracle_version = 0
            # This test pre-materializes an oracle and calls expand() directly;
            # it never runs the background _author_init thread.
            strategy.oracle_ready.set()

            class _Exec:
                def __init__(self) -> None:
                    self.params = {
                        "goal": "make tests pass",
                        "content": "",
                        "plugin_id": "mcts",
                        "run_id": "run",
                        "root_state_id": state,
                        "granted_capabilities": [
                            "snapshot",
                            "fork",
                            "agent_turn",
                            "exec",
                            "file_read",
                            "file_write",
                            "release",
                        ],
                        "budget": {
                            "max_iters": 1,
                            "branching": 2,
                            "max_time_ms": 60_000,
                            "max_tokens": 1000,
                        },
                    }
                    self.context = RunContext(
                        "mcts",
                        "run",
                        state,
                        set(self.params["granted_capabilities"]),
                    )
                    self.events = []

                def check(self) -> None:
                    return None

                def remaining_ms(self) -> int:
                    return 30_000

                def wait_job(self, receipt):
                    return waiter.wait_job(receipt)

                def emit(self, method, payload):
                    self.events.append((method, payload))

            parent = Node("root", state_id=state)
            child = Node("root/c0", parent=parent, depth=1)
            child.branch_id = branch["branch_id"]
            with patch.object(adapter, "fork", wraps=adapter.fork) as fork:
                expansion = strategy.expand(_Exec(), parent, child, adapter)
            self.assertFalse(fork.call_args.kwargs["inherit_history"])
            self.assertEqual(expansion.score, 1.0)
            self.assertTrue(expansion.state_id)
            # Dual-tower Expansion.output is harvested <solution> text; FakeSession
            # replies are plain assistant prose, so the solution slot stays empty.
            self.assertEqual(expansion.output, "")
            self.assertIn("verification author", task.init_author())
            self.assertIn("implement or fix the product code", task.init_solver())
            # Follow-up expand should include oracle feedback from the parent.
            child.score = expansion.score
            child.feedback = expansion.feedback
            child.pytest_output = expansion.feedback
            child.state_id = expansion.state_id
            child.oracle_version = expansion.oracle_version
            strategy.oracle_version += 1
            child2 = Node("root/c0/c0", parent=child, depth=2)
            child2.branch_id = adapter.fork(
                expansion.state_id, timeout_ms=1000, idempotency_key="f2", run_id="run"
            )["branch_id"]
            with patch.object(adapter, "fork", wraps=adapter.fork) as fork:
                expansion2 = strategy.expand(_Exec(), child, child2, adapter)
            self.assertEqual(
                [call.kwargs["idempotency_key"].rsplit(":", 1)[-1] for call in fork.call_args_list],
                ["parent-retest-fork", "score-fork"],
            )
            self.assertTrue(all(not call.kwargs["inherit_history"] for call in fork.call_args_list))
            self.assertEqual(expansion2.score, 1.0)
            prompt_calls = [c for c in sessions.calls if c[0] == "prompt"]
            self.assertTrue(
                any("Verification result" in c[1]["text"] for c in prompt_calls),
                prompt_calls,
            )

    def test_spec_exposes_selector_and_search_trace_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            task = OpenCodeGenericTask(
                goal="fix greet prefix",
                oracle_dir=Path(tmp) / "oracles",
                task_id="demo-case",
            )
            self.assertEqual(task.spec.id, "opencode-generic")
            self.assertEqual(task.instance_id, "demo-case")
            self.assertTrue(callable(task.spec.artifact_reader_fn))
            reader = task.spec.artifact_reader_fn()
            self.assertTrue(callable(reader))
            self.assertIsNone(task.spec.official_fn())
            # Same attribute path RunExecutor._write_search_trace uses.
            out_dir = Path("output") / task.spec.id / task.instance_id
            self.assertEqual(out_dir, Path("output/opencode-generic/demo-case"))

    def test_opencode_task_and_profiles_reference_the_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            task = OpenCodeGenericTask(goal="Fix the issue", oracle_dir=Path(tmp))
            profiles = load_agent_profiles("generic", prompt_repo=task.prompt_repo)
            self.assertIn("Repository: .", task.init_solver())
            followup = task.step_solver({"feedback": "FAIL example"})
            self.assertIn("Repository: .", followup)
            self.assertIn("FAIL example", followup)
            self.assertIn("./verify.sh", task.init_author())
            for name in ("author-init", "author-step", "solver-init"):
                self.assertNotIn("/home/user/workspace/repo", profiles[name].prompt)
            self.assertIn("./verify.sh", profiles["author-init"].prompt)

    def test_collect_candidates_uses_spec_artifact_reader(self) -> None:
        """Regression: missing artifact_reader_fn used to yield candidates=0."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_repo(root)
            adapter = OpenCodeAdapter(session_api=FakeSessionApi())
            waiter = _WaitHost(adapter)
            state = adapter.create_root_snapshot(
                repo=str(source), output_dir=str(root / "out")
            )
            branch = adapter.fork(state, timeout_ms=1000, idempotency_key="f", run_id="run")
            bid = branch["branch_id"]
            # Dirty the worktree so git_diff returns a non-empty artifact.
            wt = adapter.branches[bid].worktree
            (wt / "app.py").write_text("print('fixed')\n", encoding="utf-8")
            snap = adapter.snapshot(bid, timeout_ms=1000, idempotency_key="s", run_id="run")
            adapter.release(bid, timeout_ms=1000, idempotency_key="r", run_id="run")

            task = OpenCodeGenericTask(
                goal="fix app",
                oracle_dir=root / "oracles",
                task_id="selector-case",
            )
            node = Node("root/c0", state_id=snap["state_id"])
            node.score = 1.0
            tree = Node("root", state_id=state)
            tree.children.append(node)
            node.parent = tree

            candidates = collect_candidates(
                tree, task, min_score=1.0, host=adapter, wait_job=waiter.wait_job
            )
            self.assertEqual(len(candidates), 1, candidates)
            self.assertEqual(candidates[0].node.node_id, "root/c0")
            self.assertTrue(candidates[0].artifact.strip())


class RunParserDefaultsTests(unittest.TestCase):
    def test_run_defaults_to_full_search(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "run",
                "--repo",
                "/tmp/repo",
                "--goal-file",
                "/tmp/goal.md",
                "--output-dir",
                "/tmp/out",
                "--model",
                "deepseek/deepseek-flash",
            ]
        )
        self.assertFalse(args.handshake_only)

    def test_handshake_only_opt_in(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "run",
                "--repo",
                "/tmp/repo",
                "--goal-file",
                "/tmp/goal.md",
                "--output-dir",
                "/tmp/out",
                "--model",
                "deepseek/deepseek-flash",
                "--handshake-only",
            ]
        )
        self.assertTrue(args.handshake_only)

    def test_full_scaffold_start_keeps_full_search(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "run",
                "--repo",
                "/tmp/repo",
                "--goal-file",
                "/tmp/goal.md",
                "--output-dir",
                "/tmp/out",
                "--model",
                "deepseek/deepseek-flash",
                "--full-scaffold-start",
            ]
        )
        self.assertFalse(args.handshake_only)


if __name__ == "__main__":
    unittest.main()
