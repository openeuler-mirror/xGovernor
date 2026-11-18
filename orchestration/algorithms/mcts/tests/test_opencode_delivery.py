"""Step-4 tests for deliver_selected artifacts and cleanup_run reclaim."""
from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from xgovernor_opencode_host.opencode_adapter import (
    FakeSessionApi,
    OpenCodeAdapter,
    OpenCodeError,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _init_source(root: Path) -> Path:
    source = root / "source"
    source.mkdir()
    _git(source, "init")
    _git(source, "config", "user.name", "tester")
    _git(source, "config", "user.email", "tester@example.com")
    (source / "keep.txt").write_text("keep-v1\n", encoding="utf-8")
    _git(source, "add", ".")
    _git(source, "commit", "-m", "initial")
    return source


class DeliveryTests(unittest.TestCase):
    def test_deliver_selected_writes_artifacts_and_patch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            out = root / "out"
            sessions = FakeSessionApi()
            adapter = OpenCodeAdapter(session_api=sessions)

            root_id = adapter.create_root_snapshot(repo=str(source), output_dir=str(out))
            self.assertTrue((out / "run_record.json").is_file())
            record = json.loads((out / "run_record.json").read_text(encoding="utf-8"))
            self.assertEqual(record["root_state_id"], root_id)
            self.assertEqual(record["root_commit"], adapter.root_commit)

            left = adapter.fork(
                root_id, timeout_ms=1000, idempotency_key="fork-left", run_id="run"
            )
            right = adapter.fork(
                root_id, timeout_ms=1000, idempotency_key="fork-right", run_id="run"
            )
            left_branch = left["branch_id"]
            right_branch = right["branch_id"]
            left_wt = adapter.branches[left_branch].worktree
            right_wt = adapter.branches[right_branch].worktree

            (left_wt / "selected.txt").write_text("winner\n", encoding="utf-8")
            (right_wt / "noise.txt").write_text("loser\n", encoding="utf-8")

            # Non-empty history so freeze can fork a retained session.
            sessions.append_message(adapter.branches[left_branch].session_id, "turn-left")
            sessions.append_message(adapter.branches[right_branch].session_id, "turn-right")

            left_snap = adapter.snapshot(
                left_branch, timeout_ms=1000, idempotency_key="snap-left", run_id="run"
            )
            right_snap = adapter.snapshot(
                right_branch, timeout_ms=1000, idempotency_key="snap-right", run_id="run"
            )
            selected_state = left_snap["state_id"]
            other_state = right_snap["state_id"]
            selected_frozen = adapter.states[selected_state].frozen_session_id
            other_frozen = adapter.states[other_state].frozen_session_id
            self.assertIsNotNone(selected_frozen)
            self.assertIsNotNone(other_frozen)

            apply_plan = {
                "kind": "continue_from_state",
                "state_id": selected_state,
                "base_state_id": root_id,
            }
            result = {
                "status": "completed",
                "summary": "picked left",
                "answer": "winner",
                "stop_reason": "node_budget",
                "selected_candidate_id": "node-left",
                "candidates": [
                    {
                        "candidate_id": "node-left",
                        "state_id": selected_state,
                        "score": 1.0,
                        "evaluation_version": "test:v1",
                        "summary": "ok",
                        "evidence_refs": ["ev-1"],
                    }
                ],
                "evidence": [
                    {
                        "evidence_id": "ev-1",
                        "kind": "test_report",
                        "content": "exit_code=0 score=1",
                    }
                ],
                "limitations": [],
            }

            delivery = adapter.deliver_selected(
                output_dir=str(out),
                apply_plan=apply_plan,
                result=result,
            )

            selected_dir = out / "selected"
            self.assertTrue(selected_dir.is_dir())
            self.assertEqual(
                (selected_dir / "selected.txt").read_text(encoding="utf-8"), "winner\n"
            )
            self.assertFalse((selected_dir / "noise.txt").exists())
            self.assertTrue((selected_dir / "session.export.json").is_file())

            patch = (out / "selected.patch").read_text(encoding="utf-8")
            self.assertIn("selected.txt", patch)
            self.assertNotIn("noise.txt", patch)

            result_payload = json.loads((out / "result.json").read_text(encoding="utf-8"))
            self.assertEqual(result_payload["apply_plan"]["state_id"], selected_state)
            self.assertEqual(result_payload["delivery"]["session_id"], selected_frozen)
            self.assertEqual(
                result_payload["delivery"]["worktree_path"], str(selected_dir.resolve())
            )
            self.assertIn("continue_session_command", result_payload["delivery"])
            self.assertIn(selected_frozen, result_payload["delivery"]["continue_session_command"])
            self.assertEqual(result_payload["evidence"][0]["content"], "exit_code=0 score=1")

            # Live branches reclaimed; selected frozen session retained.
            self.assertEqual(adapter.branches, {})
            self.assertFalse(left_wt.exists())
            self.assertFalse(right_wt.exists())
            sessions.get(selected_frozen)  # still readable
            with self.assertRaises(OpenCodeError):
                sessions.get(other_frozen)

            self.assertEqual(delivery["state_id"], selected_state)
            self.assertEqual(delivery["base_commit"], adapter.root_commit)

    def test_cleanup_reclaims_from_run_record_keeps_selected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            out = root / "out"
            sessions = FakeSessionApi()
            adapter = OpenCodeAdapter(session_api=sessions)

            root_id = adapter.create_root_snapshot(repo=str(source), output_dir=str(out))
            forked = adapter.fork(
                root_id, timeout_ms=1000, idempotency_key="fork", run_id="run"
            )
            branch_id = forked["branch_id"]
            worktree = adapter.branches[branch_id].worktree
            (worktree / "picked.txt").write_text("picked\n", encoding="utf-8")
            sessions.append_message(adapter.branches[branch_id].session_id, "turn")
            snap = adapter.snapshot(
                branch_id, timeout_ms=1000, idempotency_key="snap", run_id="run"
            )
            state_id = snap["state_id"]
            frozen = adapter.states[state_id].frozen_session_id
            self.assertIsNotNone(frozen)

            adapter.deliver_selected(
                output_dir=str(out),
                apply_plan={
                    "kind": "continue_from_state",
                    "state_id": state_id,
                    "base_state_id": root_id,
                },
                result={"status": "completed", "evidence": [], "summary": "done"},
            )

            # Cold adapter: cleanup must work from run_record.json alone.
            cleaner = OpenCodeAdapter(session_api=sessions)
            summary = cleaner.cleanup_run(output_dir=str(out), keep_selected=True)

            self.assertFalse((out / "private").exists())
            self.assertFalse((out / "worktrees").exists())
            self.assertFalse((out / "frozen_sessions").exists())
            self.assertTrue((out / "selected").is_dir())
            self.assertTrue((out / "selected.patch").is_file())
            self.assertTrue((out / "result.json").is_file())
            self.assertEqual(summary["kept_session_id"], frozen)
            sessions.get(frozen)  # retained selected session still readable

            record = json.loads((out / "run_record.json").read_text(encoding="utf-8"))
            self.assertIn("cleanup", record)
            self.assertEqual(record["frozen_session_ids"], [frozen])
            self.assertIsNone(record["private_repo"])

    def test_cleanup_can_drop_selected_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_source(root)
            out = root / "out"
            sessions = FakeSessionApi()
            adapter = OpenCodeAdapter(session_api=sessions)
            root_id = adapter.create_root_snapshot(repo=str(source), output_dir=str(out))
            forked = adapter.fork(
                root_id, timeout_ms=1000, idempotency_key="fork", run_id="run"
            )
            branch_id = forked["branch_id"]
            (adapter.branches[branch_id].worktree / "x.txt").write_text("x\n", encoding="utf-8")
            sessions.append_message(adapter.branches[branch_id].session_id, "turn")
            snap = adapter.snapshot(
                branch_id, timeout_ms=1000, idempotency_key="snap", run_id="run"
            )
            frozen = adapter.states[snap["state_id"]].frozen_session_id
            adapter.deliver_selected(
                output_dir=str(out),
                apply_plan={
                    "kind": "continue_from_state",
                    "state_id": snap["state_id"],
                    "base_state_id": root_id,
                },
            )
            summary = adapter.cleanup_run(output_dir=str(out), keep_selected=False)
            self.assertIsNone(summary["kept_session_id"])
            with self.assertRaises(OpenCodeError):
                sessions.get(frozen)
            # selected/ artifacts remain on disk even when session is dropped
            self.assertTrue((out / "selected").is_dir())


if __name__ == "__main__":
    unittest.main()
