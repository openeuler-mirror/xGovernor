#!/usr/bin/env python3
"""Run SWE-bench Verified instances with xGovernor's Docker backend and pi.

Host-side preparation and grading use SWE-bench 4.1.0. Only the problem
statement and the base repository are exposed to the agent. No API key is
written to disk, passed in argv, or injected into the tool container.
"""
from __future__ import annotations

import argparse
import collections
import contextlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import getpass
import importlib.metadata
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid

REPO = Path(__file__).resolve().parents[1]
HARNESS_VERSION = "4.1.0"
TOOLS = {"read", "write", "edit", "bash", "ls", "find", "grep"}
SECRETS: list[str] = []


def redact(value: str) -> str:
    for secret in SECRETS:
        if secret:
            value = value.replace(secret, "[REDACTED]")
    return value


def save(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(redact(json.dumps(value, indent=2, ensure_ascii=False)) + "\n")


def log(message: str) -> None:
    # print(redact(message), flush=True)
    pass


def command(argv, *, log_path=None, timeout=1800, check=True, env=None):
    argv = [str(x) for x in argv]
    # Stream large build/test logs to disk instead of retaining them in memory.
    if log_path:
        with Path(log_path).open("a") as out:
            result = subprocess.run(argv, stdout=out, stderr=subprocess.STDOUT,
                                    timeout=timeout, env=env, text=True)
    else:
        result = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=timeout, env=env, text=True)
    if check and result.returncode:
        detail = f"see {log_path}" if log_path else redact((result.stdout or "")[-3000:])
        raise RuntimeError(f"Command failed ({result.returncode}): {shlex.join(argv)}\n{detail}")
    return result


def parse_instance_ids(values, filename=None):
    entries = list(values)
    if filename:
        entries.extend(Path(filename).read_text().splitlines())
    ids = []
    for entry in entries:
        for item in re.split(r"[,\s]+", entry.split("#", 1)[0].strip()):
            if not item:
                continue
            if not re.fullmatch(r"[A-Za-z0-9_.-]+__[A-Za-z0-9_.-]+-\d+", item):
                raise ValueError(f"Invalid instance ID: {item}")
            if item not in ids:
                ids.append(item)
    if not ids:
        raise ValueError("Provide instance IDs or --instances-file")
    return ids


def read_records(path):
    path = Path(path)
    if path.suffix == ".arrow":
        from datasets import Dataset
        return Dataset.from_file(str(path))
    text = path.read_text()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = [json.loads(line) for line in text.splitlines() if line.strip()]
    if isinstance(data, dict):
        data = [data] if "instance_id" in data else list(data.values())
    return data


def load_instances(args, ids=None):
    if args.dataset_file:
        records = read_records(args.dataset_file)
    else:
        from datasets import load_dataset
        # Prefer the existing Arrow cache, including in an offline installation.
        cache = args.dataset_cache
        slug = args.dataset.replace("/", "___").lower()
        arrows = []
        if cache and cache.is_dir():
            for folder in cache.iterdir():
                if folder.is_dir() and folder.name.lower() == slug:
                    arrows.extend(folder.rglob(f"*-{args.split}.arrow"))
        if arrows:
            records = read_records(max(arrows, key=lambda p: p.stat().st_mtime))
        else:
            records = load_dataset(args.dataset, split=args.split,
                                   cache_dir=str(cache) if cache else None)
    wanted = set(ids) if ids is not None else None
    selected = {row["instance_id"]: dict(row) for row in records
                if wanted is None or row["instance_id"] in wanted}
    if not selected:
        raise ValueError("Dataset selection is empty")
    if ids is None:
        ids = list(selected)
    missing = set(ids) - selected.keys()
    if missing:
        raise ValueError(f"Instances not found in dataset: {', '.join(sorted(missing))}")
    for row in selected.values():
        if not re.fullmatch(r"[0-9a-f]{40}", row["base_commit"]):
            raise ValueError(f"Invalid base commit for {row['instance_id']}")
    return selected


def read_key(args):
    if args.api_key_file:
        key = args.api_key_file.read_text().strip()
    else:
        env_name = args.api_key_env or "MODEL_API_KEY"
        key = os.environ.get(env_name, "").strip()
        saved = REPO.parent / ".config/xgovernor/az-gptplus5.key"
        if (not key and args.api_key_env is None
                and args.api_base.rstrip("/") == "https://az.gptplus5.com/v1"
                and saved.is_file()):
            key = saved.read_text().strip()
        if not key:
            if not sys.stdin.isatty():
                raise ValueError(f"Set {env_name} or pass --api-key-file in non-interactive mode")
            key = getpass.getpass("API key (hidden): ").strip()
    if not key:
        raise ValueError("API key is empty")
    SECRETS.append(key)
    return key


def http_json(url, token, payload=None, timeout=60):
    request = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(),
                                     headers={"Authorization": "Bearer " + token,
                                              "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {redact(exc.read().decode(errors='replace'))}") from None


def free_ports():
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(2)]
        for item in sockets:
            item.bind(("127.0.0.1", 0))
        return [item.getsockname()[1] for item in sockets]


def choose_executable(explicit, candidates, label):
    if explicit:
        candidates = [explicit]
    for path in candidates:
        if path and Path(path).is_file() and os.access(path, os.X_OK):
            return Path(path).absolute()
    raise ValueError(f"{label} not found. Specify its CLI path; see docs/swebench_runner.md")


def handle_interrupt(*_):
    raise KeyboardInterrupt


class Runner:
    def __init__(self, args, root, key):
        self.args, self.root, self.key = args, root, key
        self.docker = [args.docker, "--host", f"unix://{args.docker_socket}"]
        if args.mode == "run":
            cache = REPO.parent / ".cache/xgovernor-pi-target/debug"
            self.server = choose_executable(args.server_bin,
                [REPO / "target/release/xgovernor-server", REPO / "target/debug/xgovernor-server",
                 cache / "xgovernor-server"], "xgovernor-server")
            self.worker = choose_executable(args.worker_bin, [self.server.parent / "pi-worker"], "pi-worker")
            self.pi = choose_executable(args.pi, [args.extension_dir / "node_modules/.bin/pi",
                                                  shutil.which("pi")], "pi")
            if not (args.extension_dir / "index.ts").is_file():
                raise ValueError("pi bridge extension not found")


    def docker_cmd(self, *args, **kwargs):
        return command([*self.docker, *args], **kwargs)

    def ensure_image(self, image, folder):
        if self.docker_cmd("image", "inspect", image, check=False).returncode:
            log(f"Pulling {image}")
            self.docker_cmd("pull", image, log_path=folder / "image-pull.log", timeout=self.args.prepare_timeout)
        return json.loads(self.docker_cmd("image", "inspect", image).stdout)[0]["Id"]

    def agent_image(self, spec, row, folder):
        # Pin the official image by content ID; initialization only changes the
        # disposable container writable layer, never creates a derived image.
        return self.ensure_image(spec.instance_image_key, folder)

    def export_trace(self, state, rid, folder):
        files = list((state / "pi-sessions" / rid).glob("*.jsonl")) if rid else []
        if not files:
            return False
        source = max(files, key=lambda p: p.stat().st_mtime)
        trace = folder / "trace.jsonl"
        trace.write_text(redact(source.read_text()))
        messages = [r["message"] for line in trace.read_text().splitlines()
                    if "message" in (r := json.loads(line))]
        calls = collections.Counter(c["name"] for m in messages for c in m.get("content", [])
                                    if isinstance(c, dict) and c.get("type") == "toolCall")
        if set(calls) - TOOLS:
            raise RuntimeError(f"Unexpected agent tools: {sorted(set(calls) - TOOLS)}")
        return True

    def run_agent(self, row, image, folder):
        private = folder / "private"
        private.mkdir(mode=0o700)
        state = private / "state"
        token = secrets.token_urlsafe(32)
        SECRETS.append(token)
        tenants = private / "tenants.toml"
        # These are local control-plane tokens, not model credentials.
        tenants.write_text(f'[admin]\ntokens = ["{token}"]\n\n[[tenant]]\ntenant_id = "swebench"\ntokens = ["{secrets.token_urlsafe(32)}"]\n')
        tenants.chmod(0o600)
        wrapper = private / "pi.sh"
        wrapper.write_text("#!/bin/sh\nexport PI_OFFLINE=1 PI_TELEMETRY=0\nexec " + shlex.quote(str(self.pi)) +
            " --no-extensions --no-skills --no-prompt-templates --no-themes --no-context-files"
            " --tools read,write,edit,bash,ls,find,grep --exclude-tools webfetch,websearch,web_search \"$@\"\n")
        wrapper.chmod(0o700)
        admin, tenant = free_ports()
        base = f"http://127.0.0.1:{admin}/api/v1"
        deployment = "swe-" + uuid.uuid4().hex[:20]
        env = dict(os.environ)
        env.update({"XGOVERNOR_DATA_DIR": str(state), "XGOVERNOR_TENANTS_CONFIG_PATH": str(tenants),
            "XGOVERNOR_BIND_ADDR": f"127.0.0.1:{admin}", "XGOVERNOR_TENANT_BIND_ADDR": f"127.0.0.1:{tenant}",
            "XGOVERNOR_DEFAULT_WORKSPACE_ROOT": "/testbed", "XGOVERNOR_PI_WORKER": str(self.worker),
            "XGOVERNOR_PI_EXECUTABLE": str(wrapper), "XGOVERNOR_DOCKER_ENABLED": "1",
            "XGOVERNOR_DOCKER_CLI": self.args.docker, "XGOVERNOR_DOCKER_SOCKET": self.args.docker_socket,
            "XGOVERNOR_DOCKER_IMAGE": image, "XGOVERNOR_DOCKER_DEPLOYMENT": deployment,
            "XGOVERNOR_DOCKER_CPUS": str(self.args.cpus), "XGOVERNOR_DOCKER_MEMORY_MB": str(self.args.memory_mb),
            "XGOVERNOR_DOCKER_PIDS": "512", "XGOVERNOR_DOCKER_DISK_BYTES": str(8 * 1024**3),
            "XGOVERNOR_DOCKER_OUTPUT_BYTES": str(16 * 1024**2),
            "XGOVERNOR_ORPHAN_THRESHOLD_SECS": str(self.args.agent_timeout + 1800),
            "XGOV_SWE_LLM_KEY": self.key, "PI_OFFLINE": "1", "PI_TELEMETRY": "0", "RUST_LOG": "info"})
        # Isolate this daemon from unrelated backends/configuration in the host environment.
        for name in ("E2B_API_KEY", "XGOVERNOR_DOCKER_STATE_DIR", "XGOVERNOR_DB_PATH"):
            env.pop(name, None)
        api = lambda path, payload=None, timeout=60: http_json(base + path, token, payload, timeout)
        rid, process, trace_saved = None, None, False
        cleanup_errors = []
        try:
            with (folder / "server.log").open("a") as out:
                process = subprocess.Popen([str(self.server)], cwd=REPO, env=env,
                    stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
            save(private / "process.json", {"pid": process.pid, "deployment": deployment, "base": base})
            for _ in range(60):
                if process.poll() is not None:
                    raise RuntimeError("xGovernor exited during startup; see server.log")
                try:
                    api("/health", timeout=2)
                    break
                except (OSError, RuntimeError):
                    time.sleep(1)
            else:
                raise TimeoutError("xGovernor startup timed out")
            system = ("You are solving a SWE-bench issue. Work in /testbed at the provided base commit. "
                "Inspect the repository, reproduce the issue, implement a minimal correct fix, and run relevant tests. "
                "Python dependencies are installed in the testbed conda environment. "
                "Run Python/tests with conda run --no-capture-output -n testbed, or activate that environment. "
                "Do not commit. Do not use the web, external solutions, or reference patches. "
                "Web tools are disabled and the container has no network. Finish with changes and test results.")
            request = {"runtime_kind": "pi", "conversation_id": deployment, "sender_id": "swebench-runner",
                "workspace": {"kind": "daemon_default"},
                "llm": {"provider": self.args.provider, "model": self.args.model,
                        "api_base": self.args.api_base, "api_key_env": "XGOV_SWE_LLM_KEY"},
                "deployment": {"options": {"provider_options": {"network_mode": "none", "workspace_root": "/testbed", "bootstrap": True}}},
                "ext": {"runtime_pi": {"backend_id": "docker", "system_prompt": system,
                    "max_turns": self.args.max_turns, "tools_enabled": True,
                    "extension_dir": str(self.args.extension_dir)}}}
            opened = api("/sessions/open", request, timeout=960)
            rid = opened["runtime_id"]
            save(folder / "session-request.json", request)
            save(folder / "session.json", opened)
            ids = self.docker_cmd("ps", "-q", "--filter", f"label=io.xgovernor.docker-prototype={deployment}").stdout.split()
            if len(ids) != 1:
                raise RuntimeError("Cannot identify exactly one task container")
            inspected = json.loads(self.docker_cmd("inspect", ids[0]).stdout)[0]
            verified = {"container_id": ids[0], "network_mode": inspected["HostConfig"]["NetworkMode"],
                        "mounts": inspected["Mounts"], "user": inspected["Config"]["User"],
                        "workspace_root": inspected["Config"].get("WorkingDir")}
            save(folder / "container.json", verified)
            if verified["network_mode"] != "none" or verified["mounts"] or verified["user"] != "10001:10001" or verified["workspace_root"] != "/testbed":
                raise RuntimeError("Container isolation verification failed; refusing to submit a prompt")
            def execute(shell):
                result = api("/sessions/exec", {"runtime_id": rid, "command": ["bash", "-c", shell],
                                                "timeout_ms": 120000}, timeout=160)
                if result.get("exit_code") != 0 or result.get("timed_out"):
                    raise RuntimeError(f"Sandbox command failed: {result}")
                return result["stdout"]
            execute(f"cd /testbed && git reset --hard {shlex.quote(row['base_commit'])} && git clean -fd")
            baseline = execute("cd /testbed && git rev-parse HEAD && git status --porcelain")
            if baseline.strip() != row["base_commit"]:
                raise RuntimeError(f"Repository is not a clean base checkout: {baseline}")
            prompt = "Resolve this issue in the checked-out repository:\n\n" + row["problem_statement"]
            (folder / "prompt.txt").write_text(prompt)
            receipt = api("/sessions/turns", {"runtime_id": rid, "text": prompt})
            save(folder / "turn.json", receipt)
            terminal = self.stream(api, base, token, rid, receipt["turn_id"], folder)
            save(folder / "turn-result.json", terminal)
            if terminal.get("kind") != "turn_completed" or terminal.get("outcome") != "complete":
                raise RuntimeError(f"Agent did not complete successfully: {terminal}")
            # HEAD includes staged changes; intent-to-add includes new non-ignored files.
            patch = execute("cd /testbed && git add -N -- . && git diff --binary HEAD -- .")
            (folder / "model.patch").write_text(patch)
            prediction = {"instance_id": row["instance_id"], "model_name_or_path": self.args.model,
                          "model_patch": patch}
            save(folder / "prediction.json", prediction)
            trace_saved = self.export_trace(state, rid, folder)
            if not trace_saved:
                raise RuntimeError("Agent completed but no pi trace was found")
            return prediction
        finally:
            if rid:
                with contextlib.suppress(Exception):
                    api("/sessions/cancel", {"runtime_id": rid}, timeout=15)
                try:
                    if not trace_saved:
                        self.export_trace(state, rid, folder)
                except Exception as exc:
                    cleanup_errors.append(f"trace: {exc}")
                try:
                    api("/sessions/close", {"runtime_id": rid}, timeout=60)
                except Exception as exc:
                    cleanup_errors.append(f"close: {exc}")
            if process and process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            # Reclaim only this invocation's containers, including failed session opens.
            try:
                leftover = self.docker_cmd("ps", "-aq", "--filter",
                    f"label=io.xgovernor.docker-prototype={deployment}").stdout.split()
                if leftover:
                    self.docker_cmd("rm", "-f", *leftover)
            except Exception as exc:
                cleanup_errors.append(f"containers: {exc}")
            tenants.unlink(missing_ok=True)
            save(folder / "cleanup.json", {"errors": cleanup_errors})
            if cleanup_errors:
                log(f"Cleanup warnings for {row['instance_id']}: {cleanup_errors}")

    def stream(self, api, base, token, rid, tid, folder):
        request = urllib.request.Request(base + f"/sessions/{rid}/turns/{tid}/events",
                                         headers={"Authorization": "Bearer " + token})
        deadline = time.monotonic() + self.args.agent_timeout
        with urllib.request.urlopen(request, timeout=65) as response:
            for line in response:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Agent turn exceeded --agent-timeout")
                text = redact(line.decode())
                if text.startswith("data:"):
                    event = json.loads(text[5:])
                    if event.get("kind") in ("turn_completed", "turn_failed"):
                        return event
                    if event.get("kind") == "tool_activity" and event.get("phase") == "begin":
                        log(f"  tool: {event.get('name')}")
        raise RuntimeError("Event stream ended without a terminal event; partial trace retained")

    def evaluate(self, spec, row, prediction, folder):
        from swebench.harness.grading import get_eval_report
        evaluation = folder / "evaluation"
        evaluation.mkdir()
        image = self.ensure_image(spec.instance_image_key, folder)
        name = "swe-eval-" + uuid.uuid4().hex[:24]
        (evaluation / "eval.sh").write_text(spec.eval_script)
        (evaluation / "model.patch").write_text(prediction["model_patch"] or "")
        save(evaluation / "image.json", {"name": spec.instance_image_key, "digest": image})
        created = False
        try:
            self.docker_cmd("create", "--name", name, "--network", "none",
                "--cpus", str(self.args.cpus), "--memory", f"{self.args.memory_mb}m", "--pids-limit", "512",
                "--entrypoint", "/usr/bin/sleep", image, "infinity")
            created = True
            self.docker_cmd("start", name)
            self.docker_cmd("exec", "-w", "/testbed", name, "git", "reset", "--hard", row["base_commit"])
            self.docker_cmd("exec", "-w", "/testbed", name, "git", "clean", "-fd")
            for filename in ("eval.sh", "model.patch"):
                self.docker_cmd("cp", evaluation / filename, f"{name}:/tmp/{filename}")
            if prediction["model_patch"]:
                self.docker_cmd("exec", "-w", "/testbed", name, "git", "apply", "--verbose", "/tmp/model.patch",
                                log_path=evaluation / "apply-patch.log")
            result = self.docker_cmd("exec", name, "bash", "/tmp/eval.sh",
                log_path=evaluation / "test-output.log", timeout=self.args.test_timeout, check=False)
            report = get_eval_report(spec, prediction, str(evaluation / "test-output.log"), include_tests_status=True)
            save(evaluation / "report.json", report)
            detail = report[row["instance_id"]]
            status = detail.get("tests_status", {})
            baseline_valid = (len(status.get("FAIL_TO_PASS", {}).get("failure", [])) == len(spec.FAIL_TO_PASS)
                and not status.get("PASS_TO_PASS", {}).get("failure", [])
                and detail.get("patch_successfully_applied", False))
            output = {"resolved": bool(detail.get("resolved")), "eval_exit_code": result.returncode,
                      "tests_status": status, "baseline_valid": baseline_valid}
            save(folder / "result.json", output)
            return output
        finally:
            if created:
                self.docker_cmd("rm", "-f", name, timeout=60)


def parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("instances", nargs="*", help="Instance IDs (spaces or commas)")
    p.add_argument("--instance-ids", nargs="+", action="extend", default=[])
    p.add_argument("--instances-file", type=Path, help="One ID per line; commas and # comments supported")
    p.add_argument("--all", action="store_true", help="Run every instance in the selected dataset split")
    p.add_argument("--jobs", type=int, choices=(1, 2), default=1,
                   help="Maximum concurrent cases (limited to 2)")
    p.add_argument("--mode", choices=["run", "evaluate", "baseline"], default="run")
    p.add_argument("--predictions", type=Path, help="JSON/JSONL predictions for evaluate mode")
    p.add_argument("--patch", type=Path, help="Single-instance patch for evaluate mode")
    p.add_argument("--dataset", default="SWE-bench/SWE-bench_Verified")
    p.add_argument("--split", default="test")
    p.add_argument("--dataset-file", type=Path, help="Local JSON, JSONL, or Arrow dataset")
    cached = REPO.parent / ".cache/huggingface/datasets"
    p.add_argument("--dataset-cache", type=Path, default=cached if cached.is_dir() else None)
    p.add_argument("--output-dir", type=Path, default=REPO.parent / "pi-trace")
    p.add_argument("--run-id", help="New output subdirectory; existing directories are never overwritten")
    p.add_argument("--model", default="deepseek-flash")
    p.add_argument("--provider", default="openai", help="pi provider for OpenAI-compatible endpoints")
    p.add_argument("--api-base", default="https://az.gptplus5.com/v1")
    keys = p.add_mutually_exclusive_group()
    keys.add_argument("--api-key-env", help="Environment variable; otherwise use MODEL_API_KEY, saved relay key, or prompt")
    keys.add_argument("--api-key-file", type=Path, help="Read a key from an existing private file")
    p.add_argument("--skip-model-check", action="store_true", help="Skip GET /models for providers without that endpoint")
    p.add_argument("--server-bin", type=Path)
    p.add_argument("--worker-bin", type=Path)
    p.add_argument("--pi", type=Path)
    p.add_argument("--extension-dir", type=Path, default=REPO / "apps/runtime-pi/extension")
    p.add_argument("--docker", default=shutil.which("docker") or "docker")
    p.add_argument("--docker-socket", default="/var/run/docker.sock")
    p.add_argument("--arch", choices=["x86_64", "arm64"], default="x86_64")
    p.add_argument("--max-turns", type=int, default=80)
    p.add_argument("--cpus", type=int, default=2)
    p.add_argument("--memory-mb", type=int, default=4096)
    p.add_argument("--agent-timeout", type=int, default=1800, help="Agent turn deadline in seconds")
    p.add_argument("--test-timeout", type=int, default=1800)
    p.add_argument("--prepare-timeout", type=int, default=1800)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    if sys.version_info < (3, 11):
        raise ValueError("Python 3.11 or newer is required")
    requested = args.instances + args.instance_ids
    if args.all and (requested or args.instances_file):
        raise ValueError("--all cannot be combined with instance IDs or --instances-file")
    if not args.all and not requested and not args.instances_file:
        raise ValueError("Provide instance IDs, --instances-file, or --all")
    ids = None if args.all else parse_instance_ids(requested, args.instances_file)
    if args.mode != "evaluate" and (args.patch or args.predictions):
        raise ValueError("--patch/--predictions require --mode evaluate")
    if args.mode == "evaluate" and bool(args.patch) == bool(args.predictions):
        raise ValueError("evaluate mode requires exactly one of --patch or --predictions")
    if args.patch and ids is None:
        raise ValueError("--patch cannot be combined with --all; use --predictions")
    if args.patch and len(ids) != 1:
        raise ValueError("--patch requires exactly one instance; use --predictions for multiple instances")
    for name in ("cpus", "max_turns", "agent_timeout", "test_timeout", "prepare_timeout"):
        if getattr(args, name) <= 0:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")
    if args.memory_mb < 256:
        raise ValueError("--memory-mb must be at least 256")
    args.api_base = args.api_base.rstrip("/")
    if not args.api_base.startswith("https://"):
        raise ValueError("--api-base must use HTTPS")
    if importlib.metadata.version("swebench") != HARNESS_VERSION:
        raise ValueError(f"Install scripts/swebench-requirements.txt (swebench=={HARNESS_VERSION})")
    from swebench.harness.test_spec.test_spec import make_test_spec
    rows = load_instances(args, ids)
    if ids is None:
        ids = list(rows)
    predictions = {}
    if args.predictions:
        predictions = {row["instance_id"]: row for row in read_records(args.predictions)}
        for iid in ids:
            if iid not in predictions or not isinstance(predictions[iid].get("model_patch"), str):
                raise ValueError(f"Missing/invalid prediction for {iid}")
    key = read_key(args) if args.mode == "run" else None
    if key and not args.skip_model_check:
        available = http_json(args.api_base + "/models", key)
        names = {row["id"] for row in available.get("data", [])}
        if args.model not in names:
            raise ValueError(f"Requested model {args.model!r} not advertised by provider; available: {sorted(names)}")
    name = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name):
        raise ValueError("--run-id must be a simple directory name")
    root = args.output_dir.expanduser().absolute() / name
    runner = Runner(args, root, key)
    runner.docker_cmd("info", "--format", "{{.OSType}}", timeout=30)
    root.mkdir(parents=True, mode=0o700, exist_ok=False)
    log(f"Context output: {root}")
    try:
        return run_batch(args, root, runner, rows, ids, predictions, make_test_spec)
    finally:
        # Evaluation-only and pre-agent failures have no context to retain.
        with contextlib.suppress(OSError):
            root.rmdir()


def run_batch(args, root, runner, rows, ids, predictions, make_test_spec):
    batch_start = time.monotonic()
    results = {}
    completed = 0
    interruption = threading.Event()

    def run_one(index, iid):
        row = rows[iid]
        log(f"[{index}/{len(ids)}] {iid}")
        start = time.monotonic()
        result = {"instance_id": iid, "status": "error", "resolved": False}
        # Each concurrent case owns its daemon state, temp directory, and trace path.
        with tempfile.TemporaryDirectory(prefix="xgovernor-swe-") as temporary:
            folder = Path(temporary)
            try:
                spec = make_test_spec(row, namespace="swebench", arch=args.arch)
                if args.mode == "run":
                    image = runner.agent_image(spec, row, folder)
                    prediction = runner.run_agent(row, image, folder)
                else:
                    prediction = predictions.get(iid) or {"instance_id": iid, "model_name_or_path": args.mode,
                        "model_patch": args.patch.read_text() if args.patch else ""}
                result.update(runner.evaluate(spec, row, prediction, folder))
                passed = result["baseline_valid"] if args.mode == "baseline" else result["resolved"]
                result["status"] = "baseline_verified" if args.mode == "baseline" and passed else "resolved" if passed else "unresolved"
                cleanup = folder / "cleanup.json"
                if cleanup.exists() and (errors := json.loads(cleanup.read_text())["errors"]):
                    result["status"] = "error"
                    result["error"] = f"Cleanup was incomplete: {errors}"
            except KeyboardInterrupt:
                result.update(status="interrupted", error="Interrupted by user")
                interruption.set()
            except Exception as exc:
                result["error"] = redact(str(exc))
            finally:
                trace = folder / "trace.jsonl"
                if trace.is_file():
                    destination = root / iid
                    destination.mkdir(mode=0o700, exist_ok=True)
                    shutil.copyfile(trace, destination / "trace.jsonl")
                    (destination / "trace.jsonl").chmod(0o600)
                    log(f"[{index}/{len(ids)}] Context: {destination / 'trace.jsonl'}")
            result["elapsed_seconds"] = round(time.monotonic() - start, 2)
            if "error" in result:
                logs = sorted(folder.rglob("*.log"), key=lambda path: path.stat().st_mtime)
                for path in logs[-2:]:
                    with path.open("rb") as source:
                        source.seek(max(0, path.stat().st_size - 2000))
                        log(f"[{index}/{len(ids)}] {path.name}:\n{source.read().decode(errors='replace')}")
        return result

    pool = ThreadPoolExecutor(max_workers=args.jobs, thread_name_prefix="swebench")
    futures = {pool.submit(run_one, index, iid): iid for index, iid in enumerate(ids, 1)}
    interrupted = False
    try:
        for future in as_completed(futures):
            iid = futures[future]
            try:
                results[iid] = future.result()
            except KeyboardInterrupt:
                interrupted = True
                results[iid] = {"instance_id": iid, "status": "interrupted", "error": "Interrupted by user"}
                raise
            except Exception as exc:
                results[iid] = {"instance_id": iid, "status": "error", "resolved": False,
                                "error": redact(str(exc))}
            completed += 1
            item = results[iid]
            log(f"[{completed}/{len(ids)} done] {iid}: {item['status']} ({item.get('elapsed_seconds', 0)}s)"
                + (f": {item['error']}" if "error" in item else ""))
            if interruption.is_set():
                raise KeyboardInterrupt
    except KeyboardInterrupt:
        interrupted = True
        for future, iid in futures.items():
            if future.cancel():
                results[iid] = {"instance_id": iid, "status": "not_run", "resolved": False}
    finally:
        pool.shutdown(wait=True, cancel_futures=interrupted)
    if interrupted:
        for iid in ids:
            results.setdefault(iid, {"instance_id": iid, "status": "not_run", "resolved": False})

    ordered = [results.get(iid, {"instance_id": iid, "status": "not_run", "resolved": False}) for iid in ids]
    passed = sum(r["status"] in ("resolved", "baseline_verified") for r in ordered)
    batch_elapsed = round(time.monotonic() - batch_start, 2)
    summary = {
        "run_id": root.name, "dataset": args.dataset, "split": args.split, "mode": args.mode,
        "jobs": args.jobs, "total": len(ids),
        "completed": sum(r["status"] != "not_run" for r in ordered),
        "passed": passed,
        "failed": sum(r["status"] in ("error", "unresolved", "interrupted") for r in ordered),
        "not_run": sum(r["status"] == "not_run" for r in ordered),
        "elapsed_seconds": batch_elapsed, "cases": ordered,
    }
    save(root / "summary.json", summary)
    (root / "summary.json").chmod(0o600)
    write_summary_markdown(root / "summary.md", summary)
    log(f"Summary: {passed}/{len(ids)} passed; {summary['failed']} failed; "
        f"{summary['not_run']} not run in {batch_elapsed}s")
    if any(r["status"] == "interrupted" for r in ordered):
        return 130
    return 0 if passed == len(ids) else 1


def write_summary_markdown(path, summary):
    lines = [f"# SWE-bench run: {summary['run_id']}", "",
             f"- Dataset: `{summary['dataset']}` ({summary['split']})",
             f"- Mode: `{summary['mode']}`; concurrency: **{summary['jobs']}**",
             f"- Result: **{summary['passed']}/{summary['total']} passed**, "
             f"{summary['failed']} failed, {summary['not_run']} not run",
             f"- Wall time: **{summary['elapsed_seconds']} s**", "",
             "| Instance | Status | Time (s) | Error |", "|---|---|---:|---|"]
    for case in summary["cases"]:
        error = redact(str(case.get("error", ""))).replace("|", "\\|").replace("\n", " ")
        lines.append(f"| `{case['instance_id']}` | {case['status']} | "
                     f"{case.get('elapsed_seconds', '')} | {error} |")
    Path(path).write_text("\n".join(lines) + "\n")
    Path(path).chmod(0o600)


if __name__ == "__main__":
    os.umask(0o077)
    signal.signal(signal.SIGTERM, handle_interrupt)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except Exception as exc:
        log(f"ERROR: {exc}")
        sys.exit(2)
