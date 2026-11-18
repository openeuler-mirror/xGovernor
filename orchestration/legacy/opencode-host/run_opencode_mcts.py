"""Local OpenCode MCTS host entry (dual-tower harness).

``run`` connects to a user-started OpenCode 2.2.6 server, checks readiness, and
spawns the MCTS plugin over JSON-RPC v1 stdio using the shared Author/Solver
BenchmarkStrategy. Host ``host.*`` is implemented in :mod:`opencode_adapter`;
this module wires CLI, lifecycle, delivery on completion, and ``cleanup``.

Example::

    python -m xgovernor_opencode_host.run_opencode_mcts run \\
      --repo /path/to/repo --goal-file /path/to/goal.md \\
      --output-dir /path/to/new-run --model provider/model \\
      -- python3 -m unittest discover
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from xgovernor_mcts.engine.fork_policy import dynamic_fork_enabled
from xgovernor_mcts.engine.mcts_protocol import (
    PROTOCOL_VERSION,
    error,
    request as rpc_request,
    result as rpc_result,
)
from xgovernor_opencode_host.opencode_adapter import (
    DEFAULT_OPENCODE_URL,
    EXPECTED_OPENCODE_VERSION,
    OpenCodeAdapter,
    load_agent_profiles,
    OpenCodeError,
)
from xgovernor_mcts.engine.executor import BenchmarkStrategy, RunExecutor
from xgovernor_mcts.bridge.lifecycle import RunRegistry
from xgovernor_mcts.bridge.server import LifecycleServer
from xgovernor_mcts.bridge.stdio_transport import StdioTransport
from xgovernor_tasks.common.benchmark_util import (
    completed_output,
    wait_completed,
)
from xgovernor_tasks.common.mode import MODES
from xgovernor_tasks.generic.scorer import generic_test as opencode_generic_test
from xgovernor_tasks.generic.task import GenericTask

ALL_CAPABILITIES = [
    "snapshot",
    "fork",
    "agent_turn",
    "inspect",
    "exec",
    "file_read",
    "file_write",
    "release",
    "cancel_job",
]
# job_progress is granted only after the OpenCode SSE listener is actually up.

HOST_VERSION = "opencode-mcts-adapter-scaffold"
DEFAULT_MAX_ITERS = 1  # executor expands max_iters + 1 → two rounds
DEFAULT_BRANCHING = 2
DEFAULT_MAX_CONCURRENT = 2
DEFAULT_MAX_TIME_MS = 1_800_000
DEFAULT_MAX_TOKENS = 3_000_000
DEFAULT_PLUGIN_RPC_TIMEOUT_S = 30.0


_ARTIFACT_SKIP_DIRS = (
    "/node_modules/",
    "/.venv/",
    "/venv/",
    "/__pycache__/",
    "/target/",
    "/dist/",
    "/build/",
    "/.mypy_cache/",
    "/.pytest_cache/",
)
_MAX_ARTIFACT_FILE_CHARS = 64_000
_MAX_ARTIFACT_TOTAL_CHARS = 512_000
_MAX_ARTIFACT_FILE_BYTES = 512_000


def opencode_generic_repo(
    node: Any,
    task: Any,
    *,
    host: Any,
    branch_id: str,
    wait_job: Any = None,
    **kwargs: Any,
) -> tuple[str, str]:
    """OpenCode-local variant of ``generic_repo``.

    The shared reader runs ``find /home/user/workspace/repo`` inside a shell,
    which does not exist on the host. Here the adapter already maps ``cwd``, so
    use ``find .`` and read relative paths through the same adapter.
    """
    del task, kwargs
    if wait_job is None:
        raise RuntimeError("opencode_generic_repo requires wait_job")
    find_parts = ["find . -type f"]
    for skipped in _ARTIFACT_SKIP_DIRS:
        find_parts.append(f"-not -path '*{skipped}*'")
    find_parts.append("-print | sort")
    receipt = host.exec(
        branch_id,
        ["bash", "-lc", " ".join(find_parts)],
        cwd="/home/user/workspace/repo",
        timeout_ms=120_000,
        idempotency_key=f"artifact:list:{node.node_id}",
    )
    listing = completed_output(wait_completed(receipt, wait_job))
    paths = sorted({line.strip() for line in listing.splitlines() if line.strip()})

    parts: list[str] = []
    omitted: list[str] = []
    total = 0
    for path in paths:
        try:
            data = host.file_read(branch_id, path, timeout_ms=120_000)
        except Exception:
            continue
        if b"\x00" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if not text.strip():
            continue
        truncated = False
        if len(text) > _MAX_ARTIFACT_FILE_CHARS:
            text = text[:_MAX_ARTIFACT_FILE_CHARS] + "\n... (truncated)"
            truncated = True
        remaining = _MAX_ARTIFACT_TOTAL_CHARS - total
        if remaining <= 0:
            omitted.append(path)
            continue
        if len(text) > remaining:
            text = text[:remaining] + "\n... (truncated)"
            truncated = True
        if truncated:
            omitted.append(path)
        parts.append(f"### {path}\n{text.rstrip()}")
        total += len(text) + 8
    status = f"{len(parts)} file(s)"
    if omitted:
        preview = ", ".join(omitted[:20])
        status += f"; omitted/truncated: {preview}"
    return status, "\n\n".join(parts)


def opencode_generic_files(
    node: Any,
    task: Any,
    *,
    host: Any,
    branch_id: str,
    wait_job: Any = None,
    **kwargs: Any,
) -> dict[str, str]:
    """OpenCode-local variant of ``generic_files`` for final artifact write-back."""
    del task, kwargs
    if wait_job is None:
        raise RuntimeError("opencode_generic_files requires wait_job")
    find_parts = ["find . -type f"]
    for skipped in _ARTIFACT_SKIP_DIRS:
        find_parts.append(f"-not -path '*{skipped}*'")
    find_parts.append("-print | sort")
    receipt = host.exec(
        branch_id,
        ["bash", "-lc", " ".join(find_parts)],
        cwd="/home/user/workspace/repo",
        timeout_ms=120_000,
        idempotency_key=f"artifact-files:list:{node.node_id}",
    )
    listing = completed_output(wait_completed(receipt, wait_job))
    files: dict[str, str] = {}
    for line in listing.splitlines():
        path = line.strip()
        if not path:
            continue
        rel = path[2:] if path.startswith("./") else path
        if not rel or rel in {".", ".."}:
            continue
        try:
            data = host.file_read(branch_id, path, timeout_ms=120_000)
        except Exception:
            continue
        if b"\x00" in data or len(data) > _MAX_ARTIFACT_FILE_BYTES:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        files[rel] = text
    return files


class _OpenCodeGenericSpec:
    id = "opencode-generic"
    name = "OpenCode generic code task"
    oracle_rel_path = "verify.sh"
    oracle_host_name = "verify.sh"
    artifact_rel_path = ""
    prompts_dir = "generic/prompts"
    materialize = "generic-noop"

    def official_fn(self):
        return None

    def artifact_reader_fn(self):
        return opencode_generic_repo

    def artifact_files_fn(self):
        return opencode_generic_files


class OpenCodeGenericTask(GenericTask):
    """Generic-mode task surface for local OpenCode runs.

    Reuses the language-neutral ``verify.sh`` contract from the generic task
    plugin instead of the SWE-style ``testcases.py + pytest`` harness. The local
    spec additionally swaps in OpenCode-safe artifact readers that use adapter
    ``cwd`` mapping instead of sandbox-style absolute paths.
    """

    prompt_repo = "."

    def __init__(
        self,
        *,
        goal: str,
        oracle_dir: Path,
        task_id: str = "opencode-local",
    ) -> None:
        self._oracle_dir = Path(oracle_dir)
        self._oracle_dir.mkdir(parents=True, exist_ok=True)
        # Ensure the generic plugin's materializer/scorer registries are loaded.
        super().__init__(
            MODES.get("generic"),
            task_id=(task_id or "opencode-local").strip() or "opencode-local",
            goal=(goal or "").strip() or "Solve the coding task in this repository.",
        )
        self.spec = _OpenCodeGenericSpec()

    @property
    def oracle_host_path(self) -> Path:
        return self._oracle_dir / self.spec.oracle_host_name


def _strip_remainder(command: list[str]) -> list[str]:
    if command and command[0] == "--":
        return list(command[1:])
    return list(command)


def _read_goal(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        raise SystemExit(f"goal file is empty: {path}")
    return text


def _require_git_repo(repo: Path) -> None:
    """Fail fast when ``--repo`` is not a git working tree.

    The adapter performs the same check inside ``create_root_snapshot``, but
    that happens after the OpenCode readiness probe and output-dir creation.
    Checking here gives the caller an immediate, clear error before any side
    effects. Worktrees (where ``.git`` is a file) are accepted.
    """
    if not repo.is_dir():
        raise SystemExit(f"--repo is not a directory: {repo}")
    if (repo / ".git").exists():
        return
    probe = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--git-dir"],
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0:
        detail = (probe.stderr or probe.stdout or "").strip()
        raise SystemExit(f"--repo is not a git working tree: {repo}\ngit: {detail}")


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def build_start_params(
    *,
    run_id: str,
    root_state_id: str,
    goal: str,
    model: str,
    test_command: list[str],
    max_iters: int,
    branching: int,
    max_time_ms: int,
    max_tokens: int,
    grant_job_progress: bool = False,
    opencode_version: str = EXPECTED_OPENCODE_VERSION,
) -> dict[str, Any]:
    content_lines = [
        f"model={model}",
        f"test_command_hint={json.dumps(test_command, ensure_ascii=False)}",
        "scoring=generic_verify",
        "towers=author+solver",
        f"adapter=opencode-{opencode_version}",
    ]
    return {
        "plugin_id": "mcts",
        "run_id": run_id,
        "session_id": f"opencode-{run_id}",
        "turn_id": "turn-1",
        "root_state_id": root_state_id,
        "goal": goal,
        "content": "\n".join(content_lines),
        "budget": {
            "max_iters": max_iters,
            "branching": branching,
            "max_time_ms": max_time_ms,
            "max_tokens": max_tokens,
        },
        "granted_capabilities": list(ALL_CAPABILITIES)
        + (["job_progress"] if grant_job_progress else []),
    }


def serve_plugin(args: argparse.Namespace) -> int:
    """Plugin-side stdio entry: LifecycleServer + RunExecutor + dual-tower strategy."""
    try:
        test_command = json.loads(args.test_command_json)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid --test-command-json: {exc}") from exc
    if not isinstance(test_command, list) or not all(isinstance(part, str) for part in test_command):
        raise SystemExit("--test-command-json must be a JSON list of strings")
    oracle_dir = (
        Path(args.oracle_dir).expanduser().resolve()
        if str(args.oracle_dir).strip()
        else Path(os.environ.get("TMPDIR", "/tmp")) / "xiaoo-mcts-oracles" / "opencode"
    )
    spec = MODES.get(args.mode)

    def strategy_factory(params: dict[str, Any]) -> BenchmarkStrategy:
        goal = str(params.get("goal") or "")
        run_id = str(params.get("run_id") or "")
        task_args = dict(goal=goal, oracle_dir=oracle_dir, task_id=run_id or "opencode-local")
        if spec.opencode_adapter is None:
            task = OpenCodeGenericTask(**task_args)
            evaluation_version = "opencode-local:generic_test:v1"
        else:
            task = spec.loader()(spec, root_state_id=params["root_state_id"], **task_args)
            evaluation_version = f"{spec.id}:{spec.scorer}:v1"
        return BenchmarkStrategy(
            task,
            spec.scorer_fn(),
            oracle=task.oracle_host_path,
            evaluation_version=evaluation_version,
            inherit_scoring_history=False,
        )

    LifecycleServer(
        StdioTransport(),
        RunRegistry(),
        executor_factory=RunExecutor,
        strategy_factory=strategy_factory,
    ).serve_forever()
    return 0


class StdioHostDriver:
    """Host-side stdio loop for plugin lifecycle + host.* dispatch.

    Handles initialize / start / wait / cancel / shutdown and answers host.*
    by calling :class:`OpenCodeAdapter`. Worker threads handle host.* so a
    blocking plugin.wait cannot stall capability replies.
    """

    def __init__(
        self,
        process: subprocess.Popen[str],
        adapter: OpenCodeAdapter,
        *,
        timeout_s: float = DEFAULT_PLUGIN_RPC_TIMEOUT_S,
    ) -> None:
        self.process = process
        self.adapter = adapter
        self.timeout_s = timeout_s
        self._write_lock = threading.Lock()
        self._pending: dict[str | int, dict[str, Any]] = {}
        self._pending_cond = threading.Condition()
        self._reader_error: BaseException | None = None
        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, name="opencode-host-reader", daemon=True)
        # Plugin waits on host.job.* notifications for agent_turn/exec receipts.
        self.adapter.job_emitter = self._emit_job_notification

    def _emit_job_notification(self, method: str, params: dict[str, Any]) -> None:
        from xgovernor_mcts.engine.mcts_protocol import notification

        self._send(notification(method, params))

    def start(self) -> None:
        self._reader.start()

    def _send(self, message: dict[str, Any]) -> None:
        assert self.process.stdin is not None
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
        with self._write_lock:
            self.process.stdin.write(payload)
            self.process.stdin.flush()

    def _read_loop(self) -> None:
        assert self.process.stdout is not None
        try:
            while not self._closed:
                line = self.process.stdout.readline()
                if not line:
                    break
                message = json.loads(line)
                if "method" in message and "id" in message:
                    # Plugin → host capability request.
                    threading.Thread(
                        target=self._handle_host_request,
                        args=(message,),
                        name=f"host-{message.get('method', 'rpc')}",
                        daemon=True,
                    ).start()
                    continue
                if "method" in message and "id" not in message:
                    # Plugin notification (plugin.started / progress / ...).
                    print(
                        json.dumps({"notification": message}, ensure_ascii=False),
                        file=sys.stderr,
                    )
                    continue
                request_id = message.get("id")
                with self._pending_cond:
                    self._pending[request_id] = message
                    self._pending_cond.notify_all()
        except BaseException as exc:  # noqa: BLE001 - surface to callers
            self._reader_error = exc
            with self._pending_cond:
                self._pending_cond.notify_all()

    def _handle_host_request(self, message: dict[str, Any]) -> None:
        method = message["method"]
        params = message.get("params") or {}
        request_id = message["id"]
        try:
            value = self._dispatch_host(method, params)
            self._send(rpc_result(request_id, value, method=method))
        except NotImplementedError as exc:
            self._send(error(request_id, -32603, f"not implemented: {exc}"))
        except Exception as exc:  # noqa: BLE001
            self._send(error(request_id, -32603, str(exc) or type(exc).__name__))

    def _dispatch_host(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        adapter = self.adapter
        if method == "host.snapshot":
            return adapter.snapshot(
                params["branch_id"],
                timeout_ms=int(params["timeout_ms"]),
                idempotency_key=params["idempotency_key"],
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
        if method == "host.fork":
            return adapter.fork(
                params["parent_state_id"],
                timeout_ms=int(params["timeout_ms"]),
                idempotency_key=params["idempotency_key"],
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
                inherit_history=params.get("inherit_history", True),
            )
        if method == "host.agent_turn":
            return adapter.agent_turn(
                params["branch_id"],
                params["input"],
                timeout_ms=int(params["timeout_ms"]),
                idempotency_key=params["idempotency_key"],
                profile=params.get("profile"),
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
        if method == "host.inspect":
            content = adapter.inspect(
                params["branch_id"],
                view=params["view"],
                timeout_ms=int(params["timeout_ms"]),
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
            return {"content": content}
        if method == "host.exec":
            return adapter.exec(
                params["branch_id"],
                list(params["argv"]),
                cwd=params["cwd"],
                timeout_ms=int(params["timeout_ms"]),
                idempotency_key=params["idempotency_key"],
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
        if method == "host.file_read":
            data = adapter.file_read(
                params["branch_id"],
                params["path"],
                timeout_ms=int(params["timeout_ms"]),
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
            import base64

            return {"content_base64": base64.b64encode(data).decode("ascii")}
        if method == "host.file_write":
            import base64

            written = adapter.file_write(
                params["branch_id"],
                params["path"],
                base64.b64decode(params["content_base64"]),
                timeout_ms=int(params["timeout_ms"]),
                idempotency_key=params["idempotency_key"],
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
            return {"written_bytes": written}
        if method == "host.diff":
            return {"content": adapter.diff(params["branch_id"], params["base_state_id"])}
        if method == "host.release":
            adapter.release(
                params["branch_id"],
                timeout_ms=int(params["timeout_ms"]),
                idempotency_key=params["idempotency_key"],
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
            return {"released": True}
        if method == "host.cancel_job":
            return adapter.cancel_job(
                params["job_id"],
                reason=params["reason"],
                timeout_ms=int(params["timeout_ms"]),
                idempotency_key=params["idempotency_key"],
                plugin_id=params.get("plugin_id"),
                run_id=params.get("run_id"),
            )
        raise ValueError(f"unsupported host method: {method}")

    def call(self, method: str, params: dict[str, Any], *, timeout_s: float | None = None) -> dict[str, Any]:
        if self._reader_error is not None:
            raise RuntimeError(f"plugin reader failed: {self._reader_error}")
        req = rpc_request(f"host-{uuid4().hex}", method, params)
        request_id = req["id"]
        with self._pending_cond:
            self._pending.pop(request_id, None)
        self._send(req)
        deadline = time.monotonic() + (self.timeout_s if timeout_s is None else timeout_s)
        with self._pending_cond:
            while request_id not in self._pending:
                if self._reader_error is not None:
                    raise RuntimeError(f"plugin reader failed: {self._reader_error}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"timed out waiting for {method}")
                self._pending_cond.wait(remaining)
            response = self._pending.pop(request_id)
        if "error" in response:
            err = response["error"]
            raise RuntimeError(f"{method} failed: {err.get('code')} {err.get('message')}")
        return response["result"]

    def close(self) -> None:
        self._closed = True
        if self.process.stdin:
            try:
                self.process.stdin.close()
            except Exception:
                pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


def _plugin_command(test_command: list[str], *, oracle_dir: Path, mode: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "xgovernor_opencode_host.run_opencode_mcts",
        "serve-plugin",
        "--mode",
        mode,
        "--test-command-json",
        json.dumps(test_command, ensure_ascii=False),
        "--oracle-dir",
        str(oracle_dir),
    ]


def _discover_opencode_url(cli_url: str) -> str:
    """Prefer a live ``opencode service`` URL when the CLI default is unchanged."""
    default = DEFAULT_OPENCODE_URL.rstrip("/")
    requested = (cli_url or default).rstrip("/")
    if requested != default:
        return requested
    try:
        completed = subprocess.run(
            ["opencode", "service", "status"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:
        return requested
    if completed.returncode != 0:
        return requested
    for line in (completed.stdout or "").splitlines():
        value = line.strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value.rstrip("/")
    return requested


def cmd_run(args: argparse.Namespace) -> int:
    test_command = _strip_remainder(list(args.test_command or []))
    repo = Path(args.repo).expanduser().resolve()
    goal_file = Path(args.goal_file).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    args.opencode_url = _discover_opencode_url(args.opencode_url)

    _require_git_repo(repo)
    if not goal_file.is_file():
        raise SystemExit(f"--goal-file is not a file: {goal_file}")
    # Generic scoring uses the Author-generated ``verify.sh``. A trailing `-- cmd`
    # is only an optional Author hint, not the scorer itself.
    goal = _read_goal(goal_file)
    output_dir.mkdir(parents=True, exist_ok=True)
    oracle_dir = output_dir / "oracles"
    oracle_dir.mkdir(parents=True, exist_ok=True)

    spec = MODES.get(args.mode)
    adapter_class = spec.opencode_adapter or OpenCodeAdapter
    adapter = adapter_class(
        base_url=args.opencode_url,
        expected_version=args.expect_version,
        model=args.model,
        profiles=load_agent_profiles(spec.id, prompt_repo=OpenCodeGenericTask.prompt_repo),
    )

    try:
        status = adapter.check_ready()
    except OpenCodeError as exc:
        print(f"[run_opencode_mcts] OpenCode readiness check failed: {exc}", file=sys.stderr)
        return 2

    run_id = args.run_id or f"opencode-mcts-{uuid4().hex[:12]}"
    dynamic_fork = dynamic_fork_enabled()
    try:
        stream_ok = False
        if dynamic_fork:
            stream_ok = adapter.start_event_stream()
            if stream_ok:
                print("[run_opencode_mcts] OpenCode SSE event stream connected", file=sys.stderr)
            else:
                print(
                    "[run_opencode_mcts] OpenCode SSE event stream unavailable; "
                    "dynamic fork disabled",
                    file=sys.stderr,
                )
        try:
            root_state_id = adapter.create_root_snapshot(repo=str(repo), output_dir=str(output_dir))
        except OpenCodeError as exc:
            print(f"[run_opencode_mcts] create_root_snapshot failed: {exc}", file=sys.stderr)
            return 2
        print(
            json.dumps({"root_state_id": root_state_id}, ensure_ascii=False, indent=2)
        )
        start_params = build_start_params(
            run_id=run_id,
            root_state_id=root_state_id,
            goal=goal,
            model=args.model,
            test_command=test_command,
            max_iters=args.max_iters,
            branching=args.branching,
            max_time_ms=args.max_time_ms,
            max_tokens=args.max_tokens,
            grant_job_progress=stream_ok,
            opencode_version=status.version or args.expect_version,
        )
        available_capabilities = list(start_params["granted_capabilities"])

        plan = {
            "status": "scaffold",
            "created_at": _utc_now_iso(),
            "opencode": status.as_dict(),
            "repo": str(repo),
            "goal_file": str(goal_file),
            "output_dir": str(output_dir),
            "model": args.model,
            "test_command": test_command,
            "budget": start_params["budget"],
            "plugin_start": start_params,
            "towers": ["author", "solver"],
            "mode": spec.id,
            "scoring": "generic_verify",
            "oracle_dir": str(oracle_dir),
            "next_steps": [
                "optional live smoke with branching=2 depth=2",
                "use cleanup --output-dir after inspecting selected/",
            ],
        }
        plan_path = output_dir / "scaffold_plan.json"
        plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"opencode": status.as_dict(), "plan": str(plan_path)}, ensure_ascii=False, indent=2))

        if args.dry_run:
            print("[run_opencode_mcts] dry-run: skipped plugin spawn", file=sys.stderr)
            return 0

        env = os.environ.copy()
        env.setdefault("XIAOO_MCTS_MAX_CONCURRENT", str(args.max_concurrent))

        command = _plugin_command(test_command, oracle_dir=oracle_dir, mode=spec.id)
        print(f"[run_opencode_mcts] spawn plugin: {command!r}", file=sys.stderr)
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
            cwd=str(output_dir),
        )

        # Drain plugin stderr so a blocked pipe cannot stall LifecycleServer.
        def _drain_stderr() -> None:
            assert process.stderr is not None
            for line in process.stderr:
                sys.stderr.write(f"[plugin] {line}")
                sys.stderr.flush()

        threading.Thread(target=_drain_stderr, name="opencode-plugin-stderr", daemon=True).start()

        rpc_timeout_s = (
            DEFAULT_PLUGIN_RPC_TIMEOUT_S if args.plugin_timeout_s is None else args.plugin_timeout_s
        )
        driver = StdioHostDriver(process, adapter, timeout_s=rpc_timeout_s)
        driver.start()
        try:
            init = driver.call(
                "plugin.initialize",
                {
                    "plugin_id": "mcts",
                    "supported_api_versions": [PROTOCOL_VERSION],
                    "host_version": HOST_VERSION,
                    "available_capabilities": available_capabilities,
                },
            )
            print(json.dumps({"plugin.initialize": init}, ensure_ascii=False, indent=2))
            if args.handshake_only:
                shutdown = driver.call(
                    "plugin.shutdown",
                    {"plugin_id": "mcts", "grace_period_ms": 0},
                )
                print(json.dumps({"plugin.shutdown": shutdown}, ensure_ascii=False, indent=2))
                print(
                    "[run_opencode_mcts] handshake-only complete; "
                    "skipped search (pass without --handshake-only for a full run)",
                    file=sys.stderr,
                )
                return 0

            accepted = driver.call("plugin.start", start_params)
            print(json.dumps({"plugin.start": accepted}, ensure_ascii=False, indent=2))
            view: dict[str, Any] = {"status": "accepted"}
            after_sequence = 0
            # The executor owns the run deadline. An explicit CLI timeout can
            # still cap the total wait for callers that need a separate limit.
            wait_deadline = None
            if args.plugin_timeout_s is not None:
                wait_deadline = time.monotonic() + max(args.plugin_timeout_s, 30.0)
            while True:
                # The protocol bounds each long poll to 30 seconds.
                slice_ms = 30_000
                if wait_deadline is not None:
                    remaining_ms = max(1, int((wait_deadline - time.monotonic()) * 1000))
                    slice_ms = min(slice_ms, remaining_ms)
                view = driver.call(
                    "plugin.wait",
                    {
                        "plugin_id": "mcts",
                        "run_id": run_id,
                        "after_sequence": after_sequence,
                        "timeout_ms": slice_ms,
                    },
                    # RPC call timeout must cover the wait slice itself.
                    timeout_s=max(rpc_timeout_s, slice_ms / 1000.0 + 15.0),
                )
                print(json.dumps({"plugin.wait": view}, ensure_ascii=False, indent=2))
                status = str(view.get("status") or "")
                after_sequence = int(view.get("sequence") or after_sequence)
                if status in {"completed", "cancelled", "failed"}:
                    break
                if wait_deadline is not None and time.monotonic() >= wait_deadline:
                    try:
                        driver.call(
                            "plugin.cancel",
                            {
                                "plugin_id": "mcts",
                                "run_id": run_id,
                                "reason": "host wait budget exhausted",
                            },
                        )
                    except Exception as exc:  # noqa: BLE001
                        print(f"[run_opencode_mcts] cancel after wait budget: {exc}", file=sys.stderr)
                    break

            delivery = None
            if str(view.get("status") or "") == "completed":
                result = view.get("result") if isinstance(view.get("result"), dict) else {}
                apply_plan = result.get("apply_plan") if isinstance(result, dict) else None
                if isinstance(apply_plan, dict) and apply_plan.get("state_id"):
                    try:
                        delivery = adapter.deliver_selected(
                            output_dir=str(output_dir),
                            apply_plan=apply_plan,
                            result=result,
                        )
                        print(json.dumps({"delivery": delivery}, ensure_ascii=False, indent=2))
                    except OpenCodeError as exc:
                        print(f"[run_opencode_mcts] deliver_selected failed: {exc}", file=sys.stderr)
                else:
                    print(
                        "[run_opencode_mcts] completed without apply_plan; skipped delivery",
                        file=sys.stderr,
                    )
                adapter.write_run_record(output_dir)

            shutdown = driver.call(
                "plugin.shutdown",
                {"plugin_id": "mcts", "grace_period_ms": 0},
            )
            print(json.dumps({"plugin.shutdown": shutdown}, ensure_ascii=False, indent=2))
            print(
                "[run_opencode_mcts] run finished"
                + ("; selected artifacts written" if delivery else ""),
                file=sys.stderr,
            )
            return 0 if str(view.get("status") or "") == "completed" else 1
        finally:
            driver.close()

    finally:
        if dynamic_fork:
            adapter.stop_event_stream()

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Local OpenCode MCTS adapter host.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="check OpenCode and run MCTS search over stdio")
    modes = ["generic", *(spec.id for spec in MODES.all().values() if spec.opencode_adapter)]
    run.add_argument("--mode", choices=modes, default="generic")
    run.add_argument("--repo", required=True, help="trusted local git repository path")
    run.add_argument("--goal-file", required=True, help="UTF-8 task description file")
    run.add_argument("--output-dir", required=True, help="directory for run artifacts")
    run.add_argument("--model", required=True, help="OpenCode model id (provider/model)")
    run.add_argument(
        "--opencode-url",
        default=os.environ.get("OPENCODE_URL", DEFAULT_OPENCODE_URL),
        help=f"OpenCode server base URL (default {DEFAULT_OPENCODE_URL})",
    )
    run.add_argument(
        "--expect-version",
        default=EXPECTED_OPENCODE_VERSION,
        help=f"required OpenCode version when /api/info exposes one (default {EXPECTED_OPENCODE_VERSION})",
    )
    run.add_argument("--run-id", default=None)
    run.add_argument("--max-iters", type=int, default=DEFAULT_MAX_ITERS)
    run.add_argument("--branching", type=int, default=DEFAULT_BRANCHING)
    run.add_argument("--max-concurrent", type=int, default=DEFAULT_MAX_CONCURRENT)
    run.add_argument(
        "--max-time-ms", type=int, default=DEFAULT_MAX_TIME_MS,
        help="Run time budget in milliseconds (0 omits the run deadline)",
    )
    run.add_argument(
        "--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
        help="Run token budget (0 uses XIAOO_MCTS_TOKEN_FUSE_LIMIT)",
    )
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="check OpenCode readiness and write scaffold_plan.json without spawning the plugin",
    )
    run.set_defaults(handshake_only=False)
    run.add_argument(
        "--handshake-only",
        action="store_true",
        dest="handshake_only",
        help="spawn plugin, run plugin.initialize, then shutdown (skip search)",
    )
    run.add_argument(
        "--full-scaffold-start",
        action="store_false",
        dest="handshake_only",
        help="run full search through completion and deliver selected artifacts (default)",
    )
    run.add_argument(
        "--plugin-timeout-s", type=float,
        help="Override the RPC timeout and cap the total wait (minimum 30 seconds for the total wait)",
    )
    run.add_argument(
        "test_command",
        nargs=argparse.REMAINDER,
        help="optional Author hint after -- (scoring uses Author-generated verify.sh)",
    )

    serve = sub.add_parser(
        "serve-plugin",
        help="plugin-side stdio server (LifecycleServer + RunExecutor); spawned by run",
    )
    serve.add_argument("--mode", choices=modes, default="generic")
    serve.add_argument("--test-command-json", default="[]")
    serve.add_argument("--oracle-dir", default="")

    cleanup = sub.add_parser(
        "cleanup",
        help="reclaim retained OpenCode sessions/worktrees listed in run_record.json",
    )
    cleanup.add_argument("--output-dir", required=True, help="run output directory")
    cleanup.add_argument(
        "--opencode-url",
        default=os.environ.get("OPENCODE_URL", DEFAULT_OPENCODE_URL),
        help=f"OpenCode server base URL (default {DEFAULT_OPENCODE_URL})",
    )
    cleanup.add_argument(
        "--expect-version",
        default=EXPECTED_OPENCODE_VERSION,
        help=f"required OpenCode version when /api/info exposes one (default {EXPECTED_OPENCODE_VERSION})",
    )
    cleanup.add_argument(
        "--drop-selected-session",
        action="store_true",
        help="also remove the retained selected session (default keeps it)",
    )

    return parser


def cmd_cleanup(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    if not output_dir.is_dir():
        raise SystemExit(f"--output-dir is not a directory: {output_dir}")
    args.opencode_url = _discover_opencode_url(args.opencode_url)
    adapter = OpenCodeAdapter(
        base_url=args.opencode_url,
        expected_version=args.expect_version,
    )
    try:
        adapter.check_ready()
    except OpenCodeError as exc:
        print(f"[run_opencode_mcts] OpenCode readiness check failed: {exc}", file=sys.stderr)
        return 2
    try:
        summary = adapter.cleanup_run(
            output_dir=str(output_dir),
            keep_selected=not bool(args.drop_selected_session),
        )
    except OpenCodeError as exc:
        print(f"[run_opencode_mcts] cleanup failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps({"cleanup": summary}, ensure_ascii=False, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "serve-plugin":
        return serve_plugin(args)
    if args.command == "run":
        return cmd_run(args)
    if args.command == "cleanup":
        return cmd_cleanup(args)
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
