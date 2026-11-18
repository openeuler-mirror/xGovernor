"""Step-2 tests for OpenCode root snapshot, fork, and snapshot isolation."""
from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call

from xgovernor_opencode_host.opencode_adapter import (
    FakeSessionApi,
    HttpSessionApi,
    OpenCodeAdapter,
    OpenCodeError,
)

from xgovernor_opencode_host.run_opencode_mcts import StdioHostDriver


def _git(repo: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def _init_source(root: Path) -> Path:
    source = root / "source"
    source.mkdir()
    _git(source, "init")
    _git(source, "config", "user.name", "tester")
    _git(source, "config", "user.email", "tester@example.com")
    (source / "keep.txt").write_text("keep-v1\n", encoding="utf-8")
    (source / "gone.txt").write_text("will-delete\n", encoding="utf-8")
    (source / "subdir").mkdir()
    (source / "subdir" / "nested.txt").write_text("nested\n", encoding="utf-8")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "initial")
    return source


class RootSnapshotTests(unittest.TestCase):
    def test_root_snapshot_captures_modified_deleted_and_new_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            # dirty workspace: modify, delete, add untracked
            (source / "keep.txt").write_text("keep-v2\n", encoding="utf-8")
            (source / "gone.txt").unlink()
            (source / "new.txt").write_text("brand-new\n", encoding="utf-8")
            (source / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
            (source / "ignored.txt").write_text("should-not-appear\n", encoding="utf-8")

            out = root / "out"
            adapter = OpenCodeAdapter(session_api=FakeSessionApi())
            state_id = adapter.create_root_snapshot(repo=str(source), output_dir=str(out))
            self.assertEqual(state_id, "state-root")
            private = out / "private"
            self.assertTrue(private.is_dir())
            self.assertEqual((private / "keep.txt").read_text(encoding="utf-8"), "keep-v2\n")
            self.assertFalse((private / "gone.txt").exists())
            self.assertEqual((private / "new.txt").read_text(encoding="utf-8"), "brand-new\n")
            self.assertFalse((private / "ignored.txt").exists())
            # history preserved: root commit has a parent
            parents = _git(private, "rev-list", "--count", "HEAD").strip()
            self.assertGreaterEqual(int(parents), 2)
            commit = adapter.states[state_id].commit
            self.assertEqual(commit, _git(private, "rev-parse", "HEAD").strip())

    def test_root_snapshot_rejects_submodules(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            child = root / "child"
            child.mkdir()
            _git(child, "init")
            _git(child, "config", "user.name", "tester")
            _git(child, "config", "user.email", "tester@example.com")
            (child / "x.txt").write_text("x\n", encoding="utf-8")
            _git(child, "add", ".")
            _git(child, "commit", "-m", "child")
            # record as gitlink without requiring network submodule add
            _git(
                source,
                "update-index",
                "--add",
                "--cacheinfo",
                "160000",
                _git(child, "rev-parse", "HEAD").strip(),
                "vendor",
            )
            _git(source, "commit", "-m", "add submodule gitlink")

            adapter = OpenCodeAdapter(session_api=FakeSessionApi())
            with self.assertRaises(OpenCodeError):
                adapter.create_root_snapshot(repo=str(source), output_dir=str(root / "out"))

    def test_root_snapshot_rejects_merge_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            base = _git(source, "rev-parse", "--abbrev-ref", "HEAD").strip()
            _git(source, "checkout", "-b", "feature")
            (source / "keep.txt").write_text("feature\n", encoding="utf-8")
            _git(source, "add", "keep.txt")
            _git(source, "commit", "-m", "feature")
            _git(source, "checkout", base)
            (source / "keep.txt").write_text("mainline\n", encoding="utf-8")
            _git(source, "add", "keep.txt")
            _git(source, "commit", "-m", "mainline")
            merge = subprocess.run(
                ["git", "-C", str(source), "merge", "feature", "--no-commit", "--no-ff"],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(merge.returncode, 0)
            adapter = OpenCodeAdapter(session_api=FakeSessionApi())
            with self.assertRaises(OpenCodeError):
                adapter.create_root_snapshot(repo=str(source), output_dir=str(root / "out"))


class ForkSnapshotIsolationTests(unittest.TestCase):
    def test_sibling_forks_are_write_isolated_and_snapshot_preserves_commit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            sessions = FakeSessionApi()
            adapter = OpenCodeAdapter(session_api=sessions)
            state_id = adapter.create_root_snapshot(
                repo=str(source), output_dir=str(root / "out")
            )

            left = adapter.fork(
                state_id, timeout_ms=1000, idempotency_key="fork-left", run_id="run"
            )
            right = adapter.fork(
                state_id, timeout_ms=1000, idempotency_key="fork-right", run_id="run"
            )
            self.assertNotEqual(left["branch_id"], right["branch_id"])

            # Root expansions must create sessions, not fork empty history.
            create_calls = [c for c in sessions.calls if c[0] == "create"]
            fork_calls = [c for c in sessions.calls if c[0] == "fork"]
            self.assertEqual(len(create_calls), 2)
            self.assertEqual(fork_calls, [])

            left_branch = left["branch_id"]
            right_branch = right["branch_id"]
            adapter.file_write(
                left_branch,
                "only-left.txt",
                b"L\n",
                timeout_ms=1000,
                idempotency_key="w-left",
                run_id="run",
            )
            adapter.file_write(
                right_branch,
                "only-right.txt",
                b"R\n",
                timeout_ms=1000,
                idempotency_key="w-right",
                run_id="run",
            )

            left_tree = adapter.branches[left_branch].worktree
            right_tree = adapter.branches[right_branch].worktree
            self.assertTrue((left_tree / "only-left.txt").is_file())
            self.assertFalse((left_tree / "only-right.txt").exists())
            self.assertTrue((right_tree / "only-right.txt").is_file())
            self.assertFalse((right_tree / "only-left.txt").exists())

            # Give left session history so snapshot can freeze via fork.
            sessions.append_message(adapter.branches[left_branch].session_id, "turn-1")
            snap = adapter.snapshot(
                left_branch, timeout_ms=1000, idempotency_key="snap-left", run_id="run"
            )
            child_state = adapter.states[snap["state_id"]]
            self.assertFalse(child_state.empty_history)
            self.assertIsNotNone(child_state.frozen_session_id)

            # Sibling fork from the snapshot should HTTP-fork frozen session and move.
            before_forks = len([c for c in sessions.calls if c[0] == "fork"])
            child = adapter.fork(
                snap["state_id"],
                timeout_ms=1000,
                idempotency_key="fork-child",
                run_id="run",
            )
            after_forks = len([c for c in sessions.calls if c[0] == "fork"])
            self.assertGreater(after_forks, before_forks)
            child_tree = adapter.branches[child["branch_id"]].worktree
            self.assertEqual(
                (child_tree / "only-left.txt").read_text(encoding="utf-8"), "L\n"
            )
            self.assertFalse((child_tree / "only-right.txt").exists())

            # The stdio request can copy the same files without the conversation.
            driver = StdioHostDriver(Mock(), adapter)
            before = len(sessions.calls)
            score = driver._dispatch_host("host.fork", {
                "parent_state_id": snap["state_id"],
                "timeout_ms": 1000,
                "idempotency_key": "fork-score",
                "run_id": "run",
                "inherit_history": False,
            })
            score_branch = adapter.branches[score["branch_id"]]
            self.assertEqual((score_branch.worktree / "only-left.txt").read_text(), "L\n")
            self.assertEqual(sessions.sessions[score_branch.session_id].directory, str(score_branch.worktree))
            self.assertEqual(sessions.sessions[score_branch.session_id].messages, [])
            self.assertTrue({method for method, _ in sessions.calls[before:]}.isdisjoint({"fork", "move"}))
            adapter.release(
                score["branch_id"], timeout_ms=1000, idempotency_key="rel-score", run_id="run"
            )

            # Scoring-like junk on a branch must not appear in a prior snapshot's tree.
            adapter.file_write(
                left_branch,
                "score-cache.txt",
                b"cache\n",
                timeout_ms=1000,
                idempotency_key="w-score",
                run_id="run",
            )
            show = _git(
                adapter.private_repo,  # type: ignore[arg-type]
                "show",
                f"{child_state.commit}:only-left.txt",
            )
            self.assertEqual(show, "L\n")
            missing = subprocess.run(
                [
                    "git",
                    "-C",
                    str(adapter.private_repo),
                    "cat-file",
                    "-e",
                    f"{child_state.commit}:score-cache.txt",
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(missing.returncode, 0)

            adapter.release(
                left_branch, timeout_ms=1000, idempotency_key="rel-left", run_id="run"
            )
            adapter.release(
                right_branch, timeout_ms=1000, idempotency_key="rel-right", run_id="run"
            )
            adapter.release(
                child["branch_id"],
                timeout_ms=1000,
                idempotency_key="rel-child",
                run_id="run",
            )
            self.assertEqual(adapter.branches, {})


class HttpSessionForkTests(unittest.TestCase):
    def test_fork_reimports_history_at_new_directory_before_starting_a_turn(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            old_dir = str(Path(root) / "old")
            new_dir = str((Path(root) / "new").resolve())
            parent_id, child_id = "ses_parent", "ses_child"
            child = {"id": child_id, "location": {"directory": old_dir}}
            history = [{"type": "shell", "command": "pwd"}]
            restored = {"id": child_id, "location": {"directory": new_dir}}
            request = Mock(side_effect=[
                {"data": child},
                {"data": {"info": child, "messages": history}},
                {}, {"data": restored},
            ])

            result = HttpSessionApi(Mock(request_json=request)).fork(parent_id, directory=new_dir)

            self.assertEqual(result, {"data": restored})
            self.assertEqual(request.call_args_list, [
                call("POST", f"/api/session/{parent_id}/fork", {}),
                call("GET", f"/api/experimental/session/{child_id}/export"),
                call("DELETE", f"/api/session/{child_id}"),
                call("POST", "/api/experimental/session/import", {
                    "info": child, "messages": history, "location": {"directory": new_dir},
                }),
            ])


class IdempotencyTests(unittest.TestCase):
    def test_fork_idempotency_returns_same_branch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            adapter = OpenCodeAdapter(session_api=FakeSessionApi())
            state_id = adapter.create_root_snapshot(
                repo=str(source), output_dir=str(root / "out")
            )
            first = adapter.fork(
                state_id, timeout_ms=1000, idempotency_key="same", run_id="run"
            )
            second = adapter.fork(
                state_id, timeout_ms=1000, idempotency_key="same", run_id="run"
            )
            self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
