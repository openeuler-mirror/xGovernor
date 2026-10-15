"""Unit tests for batch isolation, failure status, input validation, and patch export."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parent))
import run_swebench as runner

FIRST = "astropy__astropy-12907"
SECOND = "astropy__astropy-14995"


class InputTests(unittest.TestCase):
    def test_multiple_sources_are_ordered_and_deduplicated(self):
        with tempfile.TemporaryDirectory() as directory:
            ids = Path(directory) / "ids.txt"
            ids.write_text(f"# selection\n{SECOND}, {FIRST}\n")
            self.assertEqual(runner.parse_instance_ids([FIRST], ids), [FIRST, SECOND])

    def test_all_selects_every_dataset_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "dataset.json"
            dataset.write_text(json.dumps([{"instance_id": iid, "base_commit": "a" * 40}
                                           for iid in (FIRST, SECOND)]))
            args = runner.parser().parse_args(["--all", "--dataset-file", str(dataset)])
            rows = runner.load_instances(args, None)
            self.assertEqual(list(rows), [FIRST, SECOND])

    def test_all_cannot_be_combined_with_instance_ids(self):
        # This is a semantic validation performed by main(), rather than an
        # argparse syntax error (both options are individually valid).
        with patch.object(runner.sys, "version_info", (3, 11)), \
             self.assertRaisesRegex(ValueError, "--all cannot be combined"):
            runner.main(["--all", "--instance-ids", FIRST])

    def test_paths_and_shell_fragments_are_not_instance_ids(self):
        for value in ["../astropy__astropy-12907", "$(touch /tmp/no)", "a__b-1;rm", "a__b"]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.parse_instance_ids([value])

    def test_predictions_accept_single_json_and_jsonl(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "predictions.json"
            prediction = {"instance_id": FIRST, "model_patch": "diff"}
            path.write_text(json.dumps(prediction))
            self.assertEqual(runner.read_records(path), [prediction])
            path.write_text(json.dumps(prediction) + "\n" + json.dumps({**prediction, "instance_id": SECOND}))
            self.assertEqual(len(runner.read_records(path)), 2)

    def test_noninteractive_key_is_required_without_echo(self):
        args = runner.parser().parse_args([FIRST])
        with patch.dict(os.environ, {}, clear=True), patch("sys.stdin.isatty", return_value=False):
            with self.assertRaisesRegex(ValueError, "non-interactive"):
                runner.read_key(args)

    def test_saved_relay_key_is_bound_to_endpoint_and_explicit_env_wins(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            keyfile = root / ".config/xgovernor/az-gptplus5.key"
            keyfile.parent.mkdir(parents=True)
            keyfile.write_text("fake-relay-key")
            with patch.object(runner, "REPO", root / "repo"), patch.dict(os.environ, {}, clear=True), \
                 patch("sys.stdin.isatty", return_value=False), patch.object(runner, "SECRETS", []):
                args = runner.parser().parse_args([FIRST])
                self.assertEqual(runner.read_key(args), "fake-relay-key")
                args.api_base = "https://different.example/v1"
                with self.assertRaises(ValueError):
                    runner.read_key(args)
                args.api_base = "https://az.gptplus5.com/v1"
                args.api_key_env = "EXPLICIT_KEY"
                with self.assertRaises(ValueError):
                    runner.read_key(args)
                with patch.dict(os.environ, {"EXPLICIT_KEY":"explicit-key"}):
                    self.assertEqual(runner.read_key(args), "explicit-key")

    def test_key_redaction_in_artifacts(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(runner, "SECRETS", ["private-key"]):
            path = Path(directory) / "result.json"
            runner.save(path, {"error": "server echoed private-key"})
            self.assertNotIn("private-key", path.read_text())
            self.assertIn("[REDACTED]", path.read_text())


class ImageTests(unittest.TestCase):
    def test_agent_uses_official_image_without_building(self):
        instance = runner.Runner.__new__(runner.Runner)
        instance.ensure_image = Mock(return_value="sha256:official")
        instance.docker_cmd = Mock(side_effect=AssertionError("unexpected Docker mutation"))
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            spec = types.SimpleNamespace(instance_image_key="swebench/official:latest")
            self.assertEqual(instance.agent_image(spec, {}, folder), "sha256:official")
            instance.ensure_image.assert_called_once_with("swebench/official:latest", folder)
            self.assertEqual(list(folder.iterdir()), [])


class BatchTests(unittest.TestCase):
    def batch(self, root, outcomes, mode="baseline", run_id="test", jobs=1):
        dataset = root / "dataset.json"
        dataset.write_text(json.dumps([{"instance_id": iid, "base_commit": "a" * 40,
            "problem_statement": "issue", "patch": "DO NOT EXPOSE", "test_patch": "HELD OUT"}
            for iid in (FIRST, SECOND)]))
        workspaces = []
        active = 0
        max_active = 0
        active_lock = threading.Lock()

        class FakeRunner:
            def __init__(self, *args):
                pass

            def docker_cmd(self, *args, **kwargs):
                return None

            def agent_image(self, spec, row, folder):
                (folder / "image-pull.log").write_text("pull output")
                return "image"

            def run_agent(self, row, image, folder):
                (folder / "trace.jsonl").write_text('{"message":{"role":"user","content":"issue"}}\n')
                (folder / "server.log").write_text("server output")
                (folder / "private").mkdir()
                (folder / "private/state.json").write_text("{}")
                return {"instance_id": row["instance_id"], "model_patch": "diff"}

            def evaluate(self, spec, row, prediction, folder):
                nonlocal active, max_active
                workspaces.append(folder)
                (folder / "result.json").write_text("{}")
                with active_lock:
                    active += 1
                    max_active = max(max_active, active)
                time.sleep(0.02)
                with active_lock:
                    active -= 1
                value = outcomes[row["instance_id"]]
                if isinstance(value, BaseException):
                    raise value
                return {"resolved": value, "baseline_valid": not value}

        module = types.ModuleType("swebench.harness.test_spec.test_spec")
        module.make_test_spec = lambda row, **kwargs: row
        arguments = ["--mode", mode, "--dataset-file", str(dataset), "--output-dir", str(root),
                     "--run-id", run_id, "--jobs", str(jobs), "--instance-ids", FIRST, SECOND, "--skip-model-check"]
        if mode == "evaluate":
            predictions = root / "predictions.jsonl"
            predictions.write_text("\n".join(json.dumps({"instance_id": iid, "model_patch": "diff"})
                                              for iid in (FIRST, SECOND)))
            arguments.extend(["--predictions", str(predictions)])
        output = io.StringIO()
        with patch.dict(sys.modules, {module.__name__: module}), \
             patch.object(runner.importlib.metadata, "version", return_value=runner.HARNESS_VERSION), \
             patch.object(runner, "Runner", FakeRunner), \
             patch.object(runner, "read_key", return_value="fake-key") as read_key, \
             patch.object(runner.sys, "version_info", (3, 11)), \
             contextlib.redirect_stdout(output):
            code = runner.main(arguments)
            if mode != "run":
                read_key.assert_not_called()
        self.assertTrue(all(not path.exists() for path in workspaces))
        return code, output.getvalue(), max_active

    def test_failure_does_not_prevent_the_next_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, output, _ = self.batch(root, {FIRST: RuntimeError("cannot apply patch"), SECOND: False})
            self.assertEqual(code, 1)
            self.assertIn("baseline_verified", output)
            self.assertIn("Summary: 1/2 passed", output)
            summary = json.loads((root / "test/summary.json").read_text())
            self.assertEqual(summary["failed"], 1)
            self.assertTrue((root / "test/summary.md").is_file())

    def test_unresolved_evaluation_returns_nonzero_without_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, output, _ = self.batch(root, {FIRST: True, SECOND: False}, mode="evaluate")
            self.assertEqual(code, 1)
            self.assertIn("Summary: 1/2 passed", output)
            self.assertTrue((root / "test/summary.json").is_file())

    def test_baseline_failure_is_the_expected_success_condition(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.batch(Path(directory), {FIRST: False, SECOND: False})[0], 0)

    def test_jobs_two_runs_at_most_two_cases_concurrently(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, _, max_active = self.batch(root, {FIRST: False, SECOND: False}, jobs=2)
            self.assertEqual(code, 0)
            self.assertEqual(max_active, 2)
            summary = json.loads((root / "test/summary.json").read_text())
            self.assertEqual(summary["jobs"], 2)
            self.assertEqual(summary["total"], 2)
            self.assertIn("Wall time", (root / "test/summary.md").read_text())

    def test_success_keeps_only_pi_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, _, _ = self.batch(root, {FIRST: True, SECOND: True}, mode="run")
            self.assertEqual(code, 0)
            files = sorted(str(path.relative_to(root / "test")) for path in (root / "test").rglob("*") if path.is_file())
            self.assertEqual(files, [f"{FIRST}/trace.jsonl", f"{SECOND}/trace.jsonl", "summary.json", "summary.md"])

    def test_interruption_preserves_context_and_removes_work_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, output, _ = self.batch(root, {FIRST: KeyboardInterrupt(), SECOND: True}, mode="run")
            self.assertEqual(code, 130)
            self.assertIn("1 not run", output)
            files = [path for path in (root / "test").rglob("*") if path.is_file()]
            self.assertIn(root / "test/summary.json", files)
            self.assertIn(root / "test/summary.md", files)

    def test_grading_error_still_preserves_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code, _, _ = self.batch(root, {FIRST: RuntimeError("grading failed"), SECOND: True}, mode="run")
            self.assertEqual(code, 1)
            files = sorted(str(path.relative_to(root / "test")) for path in (root / "test").rglob("*") if path.is_file())
            self.assertEqual(files, [f"{FIRST}/trace.jsonl", f"{SECOND}/trace.jsonl", "summary.json", "summary.md"])

    def test_existing_run_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.batch(root, {FIRST: True, SECOND: True}, mode="run")
            with self.assertRaises(FileExistsError):
                self.batch(root, {FIRST: True, SECOND: True}, mode="run")


class PatchTests(unittest.TestCase):
    def test_patch_includes_new_staged_modified_and_deleted_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.DEVNULL)

            git("init")
            git("config", "user.email", "test@example.invalid")
            git("config", "user.name", "Test")
            (root / "modified.txt").write_text("before\n")
            (root / "deleted.txt").write_text("before\n")
            git("add", ".")
            git("commit", "-m", "base")
            (root / "modified.txt").write_text("after\n")
            git("add", "modified.txt")
            (root / "new.txt").write_text("new\n")
            (root / "deleted.txt").unlink()
            git("add", "-N", "--", ".")
            diff = git("diff", "--binary", "HEAD", "--", ".")
            git("reset", "--hard", "HEAD")
            git("clean", "-fd")
            subprocess.run(["git", "-C", str(root), "apply"], input=diff, check=True)
            self.assertEqual((root / "modified.txt").read_text(), "after\n")
            self.assertEqual((root / "new.txt").read_text(), "new\n")
            self.assertFalse((root / "deleted.txt").exists())


class AgentLifecycleTests(unittest.TestCase):
    def exercise(self, root, network="none", stream_error=None, workspace="/testbed"):
        args = runner.parser().parse_args([FIRST])
        instance = object.__new__(runner.Runner)
        instance.args, instance.key = args, "test-private-key"
        instance.server = root / "server"
        instance.worker = root / "pi-worker"
        instance.pi = root / "pi"
        process = Mock(pid=12345)
        process.poll.return_value = None
        calls = []
        requests = []
        spawned = {}

        def spawn(*args, **kwargs):
            spawned.update(kwargs)
            return process

        def api(url, token, payload, timeout):
            path = url.split("/api/v1", 1)[1]
            calls.append(path)
            requests.append(payload)
            if path == "/sessions/open":
                return {"runtime_id": "runtime-test"}
            if path == "/sessions/exec":
                stdout = "a" * 40 + "\n" if "rev-parse" in payload["command"][-1] else "diff --git a/new b/new\n"
                return {"stdout": stdout, "exit_code": 0, "timed_out": False}
            if path == "/sessions/turns":
                return {"turn_id": "turn-test"}
            return {}

        def docker(*args, **kwargs):
            if args[0] == "inspect":
                return types.SimpleNamespace(stdout=json.dumps([{"HostConfig": {"NetworkMode": network},
                    "Config": {"User": "10001:10001", "WorkingDir": workspace}, "Mounts": []}]))
            return types.SimpleNamespace(stdout="container-test\n" if "-q" in args else "")

        def export(*args):
            calls.append("trace-export")
            return True

        instance.docker_cmd = docker
        instance.export_trace = export
        instance.stream = Mock(side_effect=stream_error,
            return_value={"kind": "turn_completed", "outcome": "complete"})
        with patch.object(runner, "http_json", side_effect=api), \
             patch.object(runner.subprocess, "Popen", side_effect=spawn), \
             patch.object(runner, "free_ports", return_value=(1234, 1235)), \
             patch.object(runner.os, "killpg") as kill, patch.object(runner, "SECRETS", []):
            try:
                result = instance.run_agent({"instance_id": FIRST, "base_commit": "a" * 40,
                    "problem_statement": "Fix the bug"}, "test-image", root)
            except (RuntimeError, TimeoutError) as exc:
                result = exc
            self.assertTrue(kill.called)
            self.assertTrue(process.wait.called)
        return result, calls, requests, spawned, instance

    def test_success_exports_before_close_and_keeps_key_out_of_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, calls, requests, spawned, _ = self.exercise(root)
            self.assertIsInstance(result, dict)
            self.assertLess(calls.index("trace-export"), calls.index("/sessions/close"))
            self.assertEqual(spawned["env"]["XGOV_SWE_LLM_KEY"], "test-private-key")
            self.assertNotIn("test-private-key", json.dumps(requests))
            self.assertFalse((root / "private/tenants.toml").exists())
            for path in root.rglob("*"):
                if path.is_file():
                    self.assertNotIn("test-private-key", path.read_text())

    def test_networked_container_never_receives_prompt(self):
        with tempfile.TemporaryDirectory() as directory:
            result, calls, _, _, instance = self.exercise(Path(directory), network="bridge")
            self.assertIsInstance(result, RuntimeError)
            self.assertNotIn("/sessions/turns", calls)
            self.assertIn("/sessions/close", calls)
            instance.stream.assert_not_called()

    def test_wrong_workspace_never_receives_prompt(self):
        with tempfile.TemporaryDirectory() as directory:
            result, calls, _, _, instance = self.exercise(Path(directory), workspace="/workspace")
            self.assertIsInstance(result, RuntimeError)
            self.assertNotIn("/sessions/turns", calls)
            instance.stream.assert_not_called()

    def test_stream_failure_cancels_exports_and_closes_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, calls, _, _, _ = self.exercise(root, stream_error=TimeoutError("stream interrupted"))
            self.assertIsInstance(result, TimeoutError)
            self.assertIn("/sessions/cancel", calls)
            self.assertIn("trace-export", calls)
            self.assertIn("/sessions/close", calls)
            self.assertFalse((root / "model.patch").exists())


if __name__ == "__main__":
    unittest.main()
