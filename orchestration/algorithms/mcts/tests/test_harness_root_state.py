from __future__ import annotations

import argparse
import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from xgovernor_mcts.benchmarks import run_plugin_benchmark
from xgovernor_mcts.benchmarks.root_state import (
    HttpResponse,
    RootStateClient,
    RootStateError,
    prepare_root_state,
    read_root_state_id,
)


class _FakeTask:
    is_kernel = False

    def bundle_bytes(self) -> bytes:
        return b"fake-bundle"

    def materialize_script(self) -> str:
        return "echo ZSOFT_TASK_READY"


class _FakeSweTask:
    def materialize_script(self) -> str:
        return "echo SWEBENCH_REPO_READY"


class _FakeGenericTask:
    task_spec = {
        "goal": "implement add",
        "files": {"calc.py": "1 + 1"},
        "test_command": "pytest -q",
    }

    def task_json(self) -> dict:
        return {
            "task_id": "generic-001",
            "goal": self.task_spec["goal"],
            "test_command": self.task_spec["test_command"],
        }


class RootStateContractTests(unittest.TestCase):
    def test_open_request_uses_zsoft_runtime_and_template(self):
        seen = []

        def transport(path, payload):
            seen.append((path, payload))
            return HttpResponse(200, {"runtime_id": "rt-root"})

        client = RootStateClient(transport=transport)
        client.open_runtime(mode="zsoft", task_id="civetweb-1")

        self.assertEqual(seen[0][0], "/api/v1/sessions/open")
        payload = seen[0][1]
        self.assertEqual(payload["runtime_kind"], "pi")
        self.assertEqual(
            payload["deployment"]["options"]["provider_options"]["template_id"],
            "zsoft-docker",
        )
        self.assertEqual(payload["ext"]["runtime_pi"]["backend_id"], "e2b")
        self.assertEqual(payload["workspace"], {"kind": "daemon_default"})
        self.assertNotIn("api_key", payload)

    def test_open_request_routes_generic_to_docker(self):
        seen = []

        def transport(path, payload):
            seen.append((path, payload))
            return HttpResponse(200, {"runtime_id": "rt-generic"})

        client = RootStateClient(transport=transport)
        client.open_runtime(mode="generic", task_id="generic-001")

        payload = seen[0][1]
        self.assertEqual(payload["ext"]["runtime_pi"]["backend_id"], "docker")
        provider_options = payload["deployment"]["options"]["provider_options"]
        self.assertEqual(provider_options["workspace_root"], "/home/user/workspace")
        self.assertTrue(provider_options["bootstrap"])
        self.assertNotIn("template_id", provider_options)

    def test_prepare_root_state_materializes_checkpoints_and_verifies(self):
        events = []

        def transport(path, payload):
            events.append(path)
            if path == "/api/v1/sessions/open":
                return HttpResponse(200, {"runtime_id": "rt-root"})
            if path == "/api/v1/sessions/heartbeat":
                return HttpResponse(200, {})
            if path == "/api/v1/sessions/files/write":
                return HttpResponse(200, {"bytes_written": len(payload["content_base64"])})
            if path == "/api/v1/sessions/exec":
                command = payload["command"][2]
                if "ZSOFT_ROOT_STATE_OK" in command:
                    return HttpResponse(200, {"exit_code": 0, "stdout": "ZSOFT_ROOT_STATE_OK\n"})
                return HttpResponse(200, {"exit_code": 0, "stdout": "ready"})
            if path == "/api/v1/sessions/checkpoint":
                return HttpResponse(200, {"checkpoint_id": "ckpt-1"})
            if path == "/api/v1/sessions/load":
                return HttpResponse(200, {"runtime_id": "rt-verify"})
            if path == "/api/v1/sessions/close":
                return HttpResponse(200, {})
            raise AssertionError(f"unexpected path: {path}")

        with tempfile.TemporaryDirectory() as tmp:
            client = RootStateClient(transport=transport)
            details = prepare_root_state(
                client,
                task_id="civetweb-1",
                output_dir=tmp,
                task_factory=lambda _task_id: _FakeTask(),
            )

            self.assertEqual(details["root_state_id"], "ckpt-1")
            self.assertEqual(read_root_state_id(Path(tmp) / "root_state_id"), "ckpt-1")
            self.assertTrue((Path(tmp) / "root_state.json").exists())
            self.assertEqual(
                events,
                [
                    "/api/v1/sessions/open",
                    "/api/v1/sessions/heartbeat",
                    "/api/v1/sessions/files/write",
                    "/api/v1/sessions/exec",
                    "/api/v1/sessions/exec",
                    "/api/v1/sessions/checkpoint",
                    "/api/v1/sessions/load",
                    "/api/v1/sessions/exec",
                    "/api/v1/sessions/close",
                    "/api/v1/sessions/close",
                ],
            )

    def test_prepare_root_state_materializes_sweverified_without_bundle(self):
        events = []

        def transport(path, payload):
            events.append(path)
            if path == "/api/v1/sessions/open":
                return HttpResponse(200, {"runtime_id": "rt-root"})
            if path == "/api/v1/sessions/heartbeat":
                return HttpResponse(200, {})
            if path == "/api/v1/sessions/exec":
                command = payload["command"][2]
                if "SWEBENCH_REPO_READY" in command:
                    return HttpResponse(200, {"exit_code": 0, "stdout": "SWEBENCH_REPO_READY\n"})
                return HttpResponse(200, {"exit_code": 0, "stdout": "ready"})
            if path == "/api/v1/sessions/checkpoint":
                return HttpResponse(200, {"checkpoint_id": "ckpt-swe"})
            if path == "/api/v1/sessions/load":
                return HttpResponse(200, {"runtime_id": "rt-verify"})
            if path == "/api/v1/sessions/close":
                return HttpResponse(200, {})
            raise AssertionError(f"unexpected path: {path}")

        with tempfile.TemporaryDirectory() as tmp:
            client = RootStateClient(transport=transport)
            details = prepare_root_state(
                client,
                task_id="astropy__astropy-12907",
                output_dir=tmp,
                mode="sweverified",
                task_factory=lambda _instance_id: _FakeSweTask(),
            )

            self.assertEqual(details["root_state_id"], "ckpt-swe")
            self.assertNotIn("/api/v1/sessions/files/write", events)
            self.assertEqual(
                events,
                [
                    "/api/v1/sessions/open",
                    "/api/v1/sessions/heartbeat",
                    "/api/v1/sessions/exec",
                    "/api/v1/sessions/checkpoint",
                    "/api/v1/sessions/load",
                    "/api/v1/sessions/exec",
                    "/api/v1/sessions/close",
                    "/api/v1/sessions/close",
                ],
            )

    def test_prepare_root_state_materializes_generic_files_and_task_json(self):
        events = []
        writes = []

        def transport(path, payload):
            events.append(path)
            if path == "/api/v1/sessions/open":
                return HttpResponse(200, {"runtime_id": "rt-generic"})
            if path == "/api/v1/sessions/heartbeat":
                return HttpResponse(200, {})
            if path == "/api/v1/sessions/files/write":
                import base64

                writes.append(
                    (
                        payload["path"],
                        base64.b64decode(payload["content_base64"]).decode("utf-8"),
                    )
                )
                return HttpResponse(200, {"bytes_written": len(payload["content_base64"])})
            if path == "/api/v1/sessions/exec":
                command = payload["command"][2]
                if "GENERIC_ROOT_STATE_OK" in command:
                    return HttpResponse(200, {"exit_code": 0, "stdout": "GENERIC_ROOT_STATE_OK\n"})
                return HttpResponse(200, {"exit_code": 0, "stdout": "ready"})
            if path == "/api/v1/sessions/checkpoint":
                return HttpResponse(200, {"checkpoint_id": "ckpt-generic"})
            if path == "/api/v1/sessions/load":
                return HttpResponse(200, {"runtime_id": "rt-verify"})
            if path == "/api/v1/sessions/close":
                return HttpResponse(200, {})
            raise AssertionError(f"unexpected path: {path}")

        with tempfile.TemporaryDirectory() as tmp:
            client = RootStateClient(transport=transport)
            details = prepare_root_state(
                client,
                task_id="generic-001",
                output_dir=tmp,
                mode="generic",
                task_factory=lambda _task_id: _FakeGenericTask(),
            )

            self.assertEqual(details["root_state_id"], "ckpt-generic")
            self.assertEqual(
                [path for path, _content in writes],
                [
                    "/home/user/workspace/repo/calc.py",
                    "/home/user/workspace/.mcts/task.json",
                ],
            )
            task_json = dict(writes)["/home/user/workspace/.mcts/task.json"]
            import json

            self.assertEqual(json.loads(task_json)["test_command"], "pytest -q")
            self.assertEqual(
                events,
                [
                    "/api/v1/sessions/open",
                    "/api/v1/sessions/heartbeat",
                    "/api/v1/sessions/files/write",
                    "/api/v1/sessions/files/write",
                    "/api/v1/sessions/checkpoint",
                    "/api/v1/sessions/load",
                    "/api/v1/sessions/exec",
                    "/api/v1/sessions/close",
                    "/api/v1/sessions/close",
                ],
            )

    def test_prepare_root_state_fails_when_checkpoint_verification_fails(self):
        def transport(path, payload):
            if path == "/api/v1/sessions/open":
                return HttpResponse(200, {"runtime_id": "rt-root"})
            if path == "/api/v1/sessions/heartbeat":
                return HttpResponse(200, {})
            if path == "/api/v1/sessions/files/write":
                return HttpResponse(200, {"bytes_written": 11})
            if path == "/api/v1/sessions/exec":
                command = payload["command"][2]
                if "ZSOFT_ROOT_STATE_OK" in command:
                    return HttpResponse(200, {"exit_code": 1, "stdout": "", "timed_out": False})
                return HttpResponse(200, {"exit_code": 0, "stdout": "ready"})
            if path == "/api/v1/sessions/checkpoint":
                return HttpResponse(200, {"checkpoint_id": "ckpt-1"})
            if path == "/api/v1/sessions/checkpoint/delete":
                return HttpResponse(200, {})
            if path == "/api/v1/sessions/load":
                return HttpResponse(200, {"runtime_id": "rt-verify"})
            if path == "/api/v1/sessions/close":
                return HttpResponse(200, {})
            raise AssertionError(f"unexpected path: {path}")

        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(RootStateError):
                prepare_root_state(
                    RootStateClient(transport=transport),
                    task_id="civetweb-1",
                    output_dir=tmp,
                    task_factory=lambda _task_id: _FakeTask(),
                )

    def test_read_root_state_id_rejects_empty_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "root_state_id"
            path.write_text("\n", encoding="utf-8")
            with self.assertRaises(RootStateError):
                read_root_state_id(path)


class RunPluginBenchmarkTests(unittest.TestCase):
    def test_help_exits_zero(self):
        with self.assertRaises(SystemExit) as raised:
            with contextlib.redirect_stdout(io.StringIO()):
                run_plugin_benchmark.main(["--help"])
        self.assertEqual(raised.exception.code, 0)

    def test_build_mcts_request_has_no_llm_credentials(self):
        request = run_plugin_benchmark.build_mcts_request(
            task_id="civetweb-1",
            root_state_id="ckpt-1",
            mode="zsoft",
            run_id="run-1",
            goal="goal",
            content="content",
            budget={"max_iters": 5, "branching": 3, "max_time_ms": 1000, "max_tokens": 100},
            granted_capabilities=["snapshot", "fork"],
        )
        self.assertEqual(request["root_state_id"], "ckpt-1")
        self.assertEqual(request["granted_capabilities"], ["snapshot", "fork"])
        self.assertNotIn("llm", request)
        self.assertFalse(any("XGOVERNOR_LLM_" in key for key in request))

    def test_build_mcts_request_uses_instance_id_for_sweverified(self):
        request = run_plugin_benchmark.build_mcts_request(
            task_id="astropy__astropy-12907",
            root_state_id="ckpt-swe",
            mode="sweverified",
            run_id="run-swe",
            goal="goal",
            content="content",
            budget={"max_iters": 2, "branching": 2, "max_time_ms": 1000, "max_tokens": 100},
            granted_capabilities=["snapshot"],
            split="test",
            testbench="sweverified",
        )
        self.assertEqual(request["instance_id"], "astropy__astropy-12907")
        self.assertNotIn("task_id", request)
        self.assertEqual(request["split"], "test")
        self.assertEqual(request["testbench"], "sweverified")

    def test_call_mcts_polls_and_saves_completion(self):
        created_request = []

        def transport(path, payload):
            created_request.append((path, payload))
            if path == "/api/v1/mcts/runs":
                return HttpResponse(201, {"status": "Accepted", "run_id": "run-1"})
            raise AssertionError(f"unexpected path: {path}")

        def get_transport(path):
            self.assertEqual(path, "/api/v1/mcts/runs/run-1")
            return HttpResponse(
                200,
                {
                    "status": "Completed",
                    "run_id": "run-1",
                    "outcome": {"result": {"status": "completed", "answer": "answer"}},
                    "protocol_jsonl": '{"jsonrpc":"2.0","id":"mcts-run-start","method":"plugin.start"}\n',
                },
            )

        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / "root_state_id").write_text("ckpt-1\n", encoding="utf-8")
            client = RootStateClient(transport=transport, get_transport=get_transport)
            args = argparse.Namespace(
                task_id="civetweb-1",
                output_dir=str(out),
                root_state_id_file=str(out / "root_state_id"),
                mode="zsoft",
                run_id="run-1",
                goal=None,
                content=None,
                max_iters=5,
                branching=3,
                max_time_ms=1000,
                max_tokens=100,
                capabilities=None,
                timeout_s=10,
                poll_interval_s=0,
            )

            self.assertEqual(run_plugin_benchmark.call_mcts(args, client), 0)
            self.assertTrue((out / "completion.json").exists())
            self.assertTrue((out / "run.json").exists())
            self.assertTrue((out / "protocol.jsonl").exists())
            self.assertFalse((out / "protocol_unavailable.txt").exists())
            self.assertTrue((out / "protocol.jsonl").read_text().startswith('{"jsonrpc":"2.0"'))
            request = created_request[0][1]
            self.assertFalse(any("XGOVERNOR_LLM_" in key for key in request))


if __name__ == "__main__":
    unittest.main()
