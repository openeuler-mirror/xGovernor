from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from unittest import mock

from xgovernor_mcts.benchmarks import xg_root_state_prepare as cli
from xgovernor_mcts.benchmarks.root_state import RootStateError


class XgRootStatePrepareCliTests(unittest.TestCase):
    def run_cli(self, argv: list[str]) -> tuple[int, str, str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = cli.run(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_success_prints_single_json_line_to_stdout(self):
        fake_client = object()
        details = {
            "root_state_id": "ckpt-1",
            "checkpoint_name": "mcts-root-demo",
            "mode": "zsoft",
        }

        with mock.patch.object(cli, "RootStateClient", return_value=fake_client), \
             mock.patch.object(cli, "prepare_root_state", return_value=details) as prepare:
            with tempfile.TemporaryDirectory() as tmp:
                code, stdout, stderr = self.run_cli(
                    ["--mode", "zsoft", "--task-id", "civetweb-1", "--output-dir", tmp]
                )

        self.assertEqual(code, 0)
        self.assertEqual(stderr, "")
        lines = stdout.strip().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0]), {
            "root_state_id": "ckpt-1",
            "checkpoint_id": "ckpt-1",
            "output_dir": tmp,
        })
        prepare.assert_called_once()
        _, kwargs = prepare.call_args
        self.assertEqual(kwargs["task_id"], "civetweb-1")
        self.assertEqual(kwargs["mode"], "zsoft")

    def test_transport_failure_returns_nonzero_and_json_error(self):
        fake_client = object()
        with mock.patch.object(cli, "RootStateClient", return_value=fake_client), \
             mock.patch.object(
                 cli, "prepare_root_state", side_effect=RootStateError("boom")
             ) as prepare:
            with tempfile.TemporaryDirectory() as tmp:
                code, stdout, stderr = self.run_cli(
                    ["--task-id", "civetweb-1", "--output-dir", tmp]
                )

        self.assertNotEqual(code, 0)
        self.assertEqual(stderr, "")
        payload = json.loads(stdout.strip())
        self.assertEqual(payload["error"], "prepare_failed")
        self.assertIn("boom", payload["message"])
        prepare.assert_called_once()

    def test_unexpected_failure_still_emits_json_stdout(self):
        fake_client = object()
        with mock.patch.object(cli, "RootStateClient", return_value=fake_client), \
             mock.patch.object(cli, "prepare_root_state", side_effect=RuntimeError("kaboom")):
            with tempfile.TemporaryDirectory() as tmp:
                code, stdout, stderr = self.run_cli(
                    ["--task-id", "civetweb-1", "--output-dir", tmp]
                )

        self.assertNotEqual(code, 0)
        payload = json.loads(stdout.strip())
        self.assertEqual(payload["error"], "prepare_failed")
        self.assertIn("kaboom", payload["message"])
        self.assertIn("unexpected failure", stderr)

    def test_sweverified_mode_uses_instance_id_and_split(self):
        fake_client = object()
        details = {
            "root_state_id": "ckpt-swe",
            "checkpoint_name": "mcts-root-swe",
            "mode": "sweverified",
        }
        with mock.patch.object(cli, "RootStateClient", return_value=fake_client), \
             mock.patch.object(cli, "prepare_root_state", return_value=details) as prepare:
            with tempfile.TemporaryDirectory() as tmp:
                code, stdout, _ = self.run_cli(
                    [
                        "--mode", "sweverified",
                        "--task-id", "astropy__astropy-12907",
                        "--testbench", "sweverified",
                        "--split", "test",
                        "--output-dir", tmp,
                    ]
                )

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout)["root_state_id"], "ckpt-swe")
        prepare.assert_called_once()
        _, kwargs = prepare.call_args
        self.assertEqual(kwargs["task_id"], "astropy__astropy-12907")
        self.assertEqual(kwargs["mode"], "sweverified")
        self.assertEqual(kwargs["testbench"], "sweverified")
        self.assertEqual(kwargs["split"], "test")

    def test_generic_mode_forwards_goal_and_task_spec(self):
        fake_client = object()
        details = {
            "root_state_id": "ckpt-generic",
            "checkpoint_name": "mcts-root-generic",
            "mode": "generic",
        }
        with mock.patch.object(cli, "RootStateClient", return_value=fake_client), \
             mock.patch.object(cli, "prepare_root_state", return_value=details) as prepare:
            with tempfile.TemporaryDirectory() as tmp:
                spec_path = tmp + "/spec.json"
                with open(spec_path, "w", encoding="utf-8") as fh:
                    fh.write('{"files": {"calc.py": "def add(a, b): return a + b"}}')
                code, stdout, _ = self.run_cli(
                    [
                        "--mode", "generic",
                        "--task-id", "generic-001",
                        "--goal", "implement add",
                        "--task-spec", spec_path,
                        "--output-dir", tmp,
                    ]
                )

        self.assertEqual(code, 0)
        self.assertEqual(json.loads(stdout)["root_state_id"], "ckpt-generic")
        prepare.assert_called_once()
        _, kwargs = prepare.call_args
        self.assertEqual(kwargs["task_id"], "generic-001")
        self.assertEqual(kwargs["mode"], "generic")
        self.assertEqual(kwargs["goal"], "implement add")
        self.assertEqual(kwargs["task_spec"], spec_path)



if __name__ == "__main__":
    unittest.main()
