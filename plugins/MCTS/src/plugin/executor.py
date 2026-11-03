"""Run orchestration: branch/job lifecycle around an injected benchmark strategy."""
from __future__ import annotations

import ast
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

from src.core.host_api import RunContext
from src.core.mcts_coordinator import (
    Coordinator,
    Expansion,
    Node,
    iter_tree,
    max_concurrent_from_env,
)
from src.core.result_builder import ResultBuilder
from src.core.retest_gate import RetestGate, retest_gate_enabled
from src.core.token_budget import token_fuse_limit_from_env
from src.core.submission_selector import select_node
from src.memory import MemoryPool
from src.memory.merger import SemanticMerger
from src.res.benchmark_util import harvest_triplet, stamp_testcases_version, strip_testcases_version
from .memory_host import AuthorWorker, HostMergeClient
from .host_client import RpcHostCapabilities


class RunCancelled(Exception):
    pass


def _job_field(job, name, default=None):
    return job.get(name, default) if isinstance(job, dict) else getattr(job, name, default)


def _is_valid_python_source(data: bytes) -> bool:
    """Return true when bytes are a non-trivial, syntactically valid Python file."""
    if not data.strip():
        return False
    try:
        source = data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    try:
        ast.parse(source)
    except SyntaxError:
        return False
    return True


def _extract_python_source(text: str) -> str | None:
    """Extract a Python file body from an Author response that chose prose
    instead of using the file-write tool.

    Prefer markdown fences; fall back to a raw source candidate. The detector is
    intentionally conservative because later byte-for-byte writes become the
    benchmark oracle.
    """
    if not text:
        return None
    fence = re.search(r"```(?:python|py)?[ \t]*\n(.*?)```", text, re.S)
    if fence:
        body = fence.group(1).strip("\n")
        if body:
            return body + "\n"

    marker = re.search(r"<<<VERIFY_PY_BEGIN>>>\s*\n(.*?)\n<<<VERIFY_PY_END>>>", text, re.S)
    if marker:
        body = marker.group(1).strip("\n")
        if body:
            return body + "\n"
    return None


def author_materialize_attempts() -> int:
    """Maximum Author rescue turns before the run fails hard.

    Mirrors the legacy ``author_init`` contract: if the Author replies with
    code in chat instead of writing ``repo/verify.py`` through a tool, ask it
    again with an explicit hard-failure instruction before giving up.
    """
    raw = os.environ.get("XIAOO_AUTHOR_MATERIALIZE_ATTEMPTS", "3")
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "XIAOO_AUTHOR_MATERIALIZE_ATTEMPTS must be a positive integer, "
            f"got {raw!r}"
        ) from exc
    if value <= 0:
        raise RuntimeError(
            "XIAOO_AUTHOR_MATERIALIZE_ATTEMPTS must be a positive integer, "
            f"got {raw!r}"
        )
    return value


def solver_materialize_attempts() -> int:
    """Maximum Solver rescue turns before a missing PoC is treated as unscored."""
    raw = os.environ.get("XIAOO_SOLVER_MATERIALIZE_ATTEMPTS", "3")
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            "XIAOO_SOLVER_MATERIALIZE_ATTEMPTS must be a positive integer, "
            f"got {raw!r}"
        ) from exc
    if value <= 0:
        raise RuntimeError(
            "XIAOO_SOLVER_MATERIALIZE_ATTEMPTS must be a positive integer, "
            f"got {raw!r}"
        )
    return value


def memory_pool_enabled() -> bool:
    """Whether Solver findings go through MemoryPool instead of sync Author turns."""
    raw = os.environ.get("XIAOO_MCTS_MEMORY_POOL", "1")
    return raw.strip().lower() not in {"0", "false", "off", "no"}


class SearchStrategy(Protocol):
    evaluation_version: str
    def expand(self, executor: "RunExecutor", parent: Node, child: Node, host: Any) -> Expansion: ...


@dataclass
class BenchmarkStrategy:
    """Task-specific expansion and scoring.

    The strategy is deliberately small: task plugins own prompt/oracle semantics;
    this class only connects them to the host branch/job boundary.
    """
    task: Any
    scorer: Callable[..., tuple[float, str]]
    oracle: Any = None
    evaluation_version: str = "benchmark-v1"
    solver_init_profile: str = "solver-init"
    solver_profile: str = "solver-step"
    author_state_id: str | None = None
    oracle_version: int = 0
    oracle_bytes: bytes = b""
    search_root_state_id: str | None = None
    _oracle_lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)
    _memory: MemoryPool | None = field(default=None, init=False, repr=False)
    _author_worker: AuthorWorker | None = field(default=None, init=False, repr=False)
    _retest_gate: RetestGate | None = field(default=None, init=False, repr=False)
    _last_memory_json: str = field(default="{}", init=False, repr=False)

    def initialize(self, executor: "RunExecutor") -> None:
        """Materialize one isolated task root and have Author create its oracle."""
        host = executor.host
        self._retest_gate = RetestGate() if retest_gate_enabled() else None
        branch = host.fork(executor.context.root_state_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:task-root:fork")
        branch_id = branch["branch_id"]
        try:
            prepare = getattr(self.task, "bootstrap_prepare", None)
            if prepare:
                prepare(host, branch_id)
            materialize = host.exec(branch_id, ["bash", "-lc", self.task.materialize_script()], cwd="/home/user/workspace", timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:materialize")
            materialized = executor.wait_job(materialize)
            exit_code = _job_field(materialized, "exit_code")
            if exit_code != 0:
                output = _job_field(materialized, "output", "")
                raise RuntimeError(f"task materialization failed (exit {exit_code}): {output[-4000:]}")
            post = getattr(self.task, "bootstrap_post", None)
            if post:
                post(host, branch_id, wait_job=executor.wait_job)
            receipt = host.agent_turn(branch_id, self.task.init_author(), timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:init", profile="author-init")
            author_job = executor.wait_job(receipt)
            author_output = _job_field(author_job, "output", "")
            oracle_path = self.task.oracle_host_path
            repo_path = f"/home/user/workspace/repo/{self.task.spec.oracle_rel_path}"
            data = b""
            author_output = author_output or ""
            for attempt in range(1, author_materialize_attempts() + 1):
                try:
                    data = host.file_read(branch_id, repo_path, timeout_ms=executor.remaining_ms())
                except Exception as exc:
                    executor.emit(
                        "plugin.progress",
                        {
                            "summary": (
                                f"Author file missing on attempt {attempt}; "
                                f"will request a file-writing retry ({exc})"
                            )
                        },
                    )
                    data = b""
                if _is_valid_python_source(data):
                    break
                problem = "does not exist" if not data.strip() else "exists but is not syntactically valid Python"
                rescue = (
                    f"HARD FAILURE ({attempt}): {repo_path} {problem}. "
                    f"Create exactly {repo_path} now with a file-writing tool. It must be "
                    f"syntactically valid Python and follow the Author contract. "
                    f"Run `python3 -m py_compile {repo_path}` before finishing. "
                    "Do not return code only in chat."
                )
                receipt = host.agent_turn(
                    branch_id,
                    rescue,
                    timeout_ms=executor.remaining_ms(),
                    idempotency_key=f"{executor.context.run_id}:author:init:rescue:{attempt}",
                    profile="author-init",
                )
                author_job = executor.wait_job(receipt)
                author_output = _job_field(author_job, "output", "") or author_output
            if not _is_valid_python_source(data):
                generated = _extract_python_source(author_output)
                if generated and _is_valid_python_source(generated.encode("utf-8")):
                    data = generated.encode("utf-8")
                    host.file_write(
                        branch_id,
                        repo_path,
                        data,
                        timeout_ms=executor.remaining_ms(),
                        idempotency_key=f"{executor.context.run_id}:author:init:materialize",
                    )
                    executor.emit("plugin.progress", {"summary": "Materialized Author-generated verify.py from response text"})
                else:
                    detail = author_output[-4000:] if author_output else "(no author turn output captured)"
                    executor.emit("plugin.progress", {"summary": f"Author produced no valid Python oracle. Output: {detail}"})
                    raise RuntimeError(f"author did not create valid Python oracle: {repo_path}")
            oracle_path.parent.mkdir(parents=True, exist_ok=True)
            oracle_path.write_bytes(data)
            stamp_testcases_version(oracle_path, 0)
            self.oracle_bytes = oracle_path.read_bytes()
            host.file_write(branch_id, repo_path, self.oracle_bytes, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:oracle:v0")
            self.search_root_state_id = host.snapshot(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:task-root:snapshot")["state_id"]
            self.author_state_id = self.search_root_state_id
        finally:
            host.release(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:task-root:release")

    def _sync_author(self, executor: "RunExecutor", query: str) -> None:
        if not query.strip() or not self.author_state_id:
            return
        with self._oracle_lock:
            host = executor.host
            version = self.oracle_version
            branch = host.fork(self.author_state_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:{version}:fork")
            branch_id = branch["branch_id"]
            repo_path = f"/home/user/workspace/repo/{self.task.spec.oracle_rel_path}"
            try:
                prompt = self.task.step_author({"memory": query}, node_id="solver-query")
                receipt = host.agent_turn(branch_id, prompt, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:{version}:step", profile="author-step")
                executor.wait_job(receipt)
                updated = host.file_read(branch_id, repo_path, timeout_ms=executor.remaining_ms())
                if _is_valid_python_source(updated) and strip_testcases_version(updated) != strip_testcases_version(self.oracle_bytes):
                    self.oracle_version = version + 1
                    path = self.task.oracle_host_path
                    path.write_bytes(updated)
                    stamp_testcases_version(path, self.oracle_version)
                    self.oracle_bytes = path.read_bytes()
                    host.file_write(branch_id, repo_path, self.oracle_bytes, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:oracle:v{self.oracle_version}")
                    self.author_state_id = host.snapshot(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:v{version + 1}:snapshot")["state_id"]
                    executor.emit("plugin.progress", {"summary": f"Author updated benchmark oracle to v{self.oracle_version}"})
            finally:
                host.release(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:{version}:release")

    def start_author_worker(self, executor: "RunExecutor") -> None:
        """Start asynchronous MemoryPool consumption, falling back to sync Author."""
        if not memory_pool_enabled():
            return
        try:
            client = HostMergeClient(
                host=executor.host,
                wait_job=executor.wait_job,
                run_id=executor.context.run_id,
                author_state_id=lambda: self.author_state_id,
                profile="memory-merge",
                timeout_ms=executor.remaining_ms,
            )
            self._memory = MemoryPool(_merger=SemanticMerger(client.complete))
            self._author_worker = AuthorWorker(self, executor)
            self._author_worker.start()
        except Exception as exc:
            self._memory = None
            self._author_worker = None
            executor.emit(
                "plugin.progress",
                {"summary": f"MemoryPool disabled, falling back to sync Author turns: {exc}"},
            )

    def stop_author_worker(self, executor: "RunExecutor") -> None:
        worker = self._author_worker
        if worker is not None:
            worker.stop()
        memory = self._memory
        if memory is not None:
            try:
                memory.close(cancel_pending=True)
            except Exception:
                pass
        self._author_worker = None
        self._memory = None

    def ingest_memory(self, executor: "RunExecutor", query: str) -> None:
        """Route a Solver ``<query>`` through MemoryPool or sync Author."""
        if not query or not query.strip():
            return
        if self._memory is None:
            self._sync_author(executor, query)
            return
        self._memory.add(query)
        if self._author_worker is not None:
            self._author_worker.wake()

    @staticmethod
    def _memory_delta(snapshot: str, previous: str) -> str:
        """Return only versions added since the previous fetched snapshot."""
        try:
            current = json.loads(snapshot or "{}")
        except Exception:
            return snapshot
        try:
            prior = json.loads(previous or "{}")
        except Exception:
            prior = {}
        if not isinstance(current, dict):
            return snapshot
        delta = {key: value for key, value in current.items() if key not in prior}
        return json.dumps(delta, ensure_ascii=False)

    def _consume_memory(self, executor: "RunExecutor") -> None:
        if self._memory is None:
            return
        snapshot = self._memory.fetch()
        if snapshot == self._last_memory_json:
            return
        delta = self._memory_delta(snapshot, self._last_memory_json)
        self._last_memory_json = snapshot
        if delta and delta != "{}":
            self._sync_author(executor, delta)

    def _ensure_solver_artifact(
        self,
        executor: "RunExecutor",
        child: Node,
        solver_output: str,
        *,
        profile: str | None = None,
    ) -> str:
        """Guarantee the Solver deliverable exists in its branch before scoring.

        Like the Author oracle path, a model sometimes returns the requested file
        only as chat text.  Ask it once more with an explicit file-writing
        contract, then fall back to materializing a fenced Python source.
        """
        target = getattr(self.task, "poc_sandbox_path", None)
        if not target:
            return solver_output
        host = executor.host
        require_python = target.endswith(".py")
        data = b""
        for attempt in range(1, solver_materialize_attempts() + 1):
            try:
                data = host.file_read(child.branch_id, target, timeout_ms=executor.remaining_ms())
            except Exception:
                data = b""
            valid = _is_valid_python_source(data) if require_python else bool(data.strip())
            if valid:
                return solver_output
            problem = (
                "does not exist" if not data.strip()
                else "exists but is not syntactically valid Python"
            )
            contract = "syntactically valid Python" if require_python else "non-empty and task-correct"
            check = (
                f"Run `python3 -m py_compile {target}` before finishing."
                if require_python
                else f"Run `test -s {target}` before finishing."
            )
            rescue = (
                f"HARD FAILURE ({attempt}): REQUIRED DELIVERABLE IS STILL INVALID. "
                f"{target} {problem}. Create exactly {target} now with a "
                f"file-writing tool. It must be {contract}. {check} "
                "Do not return code only in chat."
            )
            receipt = host.agent_turn(
                child.branch_id,
                rescue,
                timeout_ms=executor.remaining_ms(),
                idempotency_key=f"{executor.context.run_id}:{child.node_id}:solve:rescue:{attempt}",
                profile=profile or self.solver_profile,
            )
            job = executor.wait_job(receipt)
            solver_output = _job_field(job, "output", "") or solver_output
        generated = _extract_python_source(solver_output)
        if generated and (not require_python or _is_valid_python_source(generated.encode("utf-8"))):
            host.file_write(
                child.branch_id,
                target,
                generated.encode("utf-8"),
                timeout_ms=executor.remaining_ms(),
                idempotency_key=f"{executor.context.run_id}:{child.node_id}:solve:materialize",
            )
            executor.emit("plugin.progress", {"summary": f"Materialized Solver-generated {target} from response text"})
        elif require_python and not generated:
            executor.emit(
                "plugin.progress",
                {"summary": f"{child.node_id} still has no valid Python deliverable {target}; scoring will gate it"},
            )
        return solver_output

    def _prompt(self, parent: Node) -> str:
        prior = {
            "feedback": str(parent.score),
            "pytest_output": parent.pytest_output or parent.feedback,
            "solution": parent.solution,
            "reflection": parent.reflection,
            "query": parent.query,
        }
        if parent.parent is None and not parent.solution:
            return self.task.init_solver()
        return self.task.step_solver(prior)

    def expand(self, executor: "RunExecutor", parent: Node, child: Node, host: Any) -> Expansion:
        executor.check()
        with self._oracle_lock:
            current_version = self.oracle_version
            oracle_bytes = self.oracle_bytes
        prior_feedback = parent.feedback
        prior_output = parent.pytest_output
        if parent.parent is not None and parent.state_id and parent.oracle_version != current_version:
            def retest_parent():
                retest_branch = host.fork(parent.state_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:{child.node_id}:parent-retest-fork")
                try:
                    return self.scorer(host, retest_branch["branch_id"], self.task, self.task.oracle_host_path, wait_job=executor.wait_job)
                finally:
                    host.release(retest_branch["branch_id"], timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:{child.node_id}:parent-retest-release")

            gate = self._retest_gate
            if gate is not None:
                score, evidence = gate.run(parent.state_id, current_version, retest_parent)
            else:
                score, evidence = retest_parent()
            parent.score, parent.feedback, parent.pytest_output, parent.oracle_version = float(score), str(evidence), str(evidence), current_version
            prior_feedback, prior_output = parent.feedback, parent.pytest_output
        if parent.parent is not None and oracle_bytes:
            host.file_write(child.branch_id, f"/home/user/workspace/repo/{self.task.spec.oracle_rel_path}", oracle_bytes, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:{child.node_id}:oracle-v{current_version}")
        remaining = executor.remaining_ms()
        solver_profile = (
            self.solver_init_profile
            if parent.parent is None and not parent.solution
            else self.solver_profile
        )
        receipt = host.agent_turn(
            child.branch_id, self._prompt(parent), timeout_ms=remaining,
            idempotency_key=f"{executor.context.run_id}:{child.node_id}:solve",
            profile=solver_profile,
        )
        child.job_id = receipt["job_id"]
        job = executor.wait_job(receipt)
        harvested = harvest_triplet(job)
        self._ensure_solver_artifact(
            executor, child, _job_field(job, "output", ""), profile=solver_profile
        )
        # Snapshot the product immediately after the solver turn.  Destructive
        # benchmark evaluation happens on a fork of this immutable snapshot.
        solution_snapshot = host.snapshot(
            child.branch_id, timeout_ms=executor.remaining_ms(),
            idempotency_key=f"{executor.context.run_id}:{child.node_id}:solution",
        )
        score_branch = host.fork(
            solution_snapshot["state_id"], timeout_ms=executor.remaining_ms(),
            idempotency_key=f"{executor.context.run_id}:{child.node_id}:score-fork",
        )
        try:
            score, evidence = self.scorer(
                host, score_branch["branch_id"], self.task, self.oracle,
                wait_job=executor.wait_job,
            )
        finally:
            host.release(
                score_branch["branch_id"], timeout_ms=executor.remaining_ms(),
                idempotency_key=f"{executor.context.run_id}:{child.node_id}:score-release",
            )
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not 0 <= score <= 1:
            raise ValueError("benchmark scorer must return score in [0, 1]")
        executor.emit("plugin.progress", {"summary": f"Evaluated {child.node_id}: {score}"})
        self.ingest_memory(executor, harvested.get("query", ""))
        return Expansion(
            float(score), str(evidence), harvested.get("solution", ""),
            harvested.get("query", ""), state_id=solution_snapshot["state_id"],
            oracle_version=current_version,
        )


class RunExecutor:
    def __init__(self, params, transport, emit, *, strategy: SearchStrategy | None = None):
        self.params = params
        self.transport = transport
        self.emit = emit
        self.context = RunContext(params["plugin_id"], params["run_id"], params["root_state_id"], set(params["granted_capabilities"]))
        self.host = RpcHostCapabilities(
            transport, plugin_id=params["plugin_id"], run_id=params["run_id"],
            granted=self.context.granted_capabilities, external_dispatch=True,
        )
        self.strategy = strategy
        self.thread = threading.Thread(target=self.run, name=f"mcts-{params['run_id']}", daemon=True)
        self.reason = "cancelled by host"
        self.deadline = 0.0
        self.tokens = 0
        self._accounted_jobs: set[tuple[str, int]] = set()
        self._usage_lock = threading.Lock()
        self._search_lock = threading.Lock()
        self._search_events: list[tuple[str, str, float, int | None]] = []
        self.official_started = False
        self.official_done = False

    def cancel(self, reason):
        self.reason = reason
        self.context.cancelled = True

    def check(self):
        if self.context.cancelled:
            raise RunCancelled(self.reason)
        if time.monotonic() >= self.deadline:
            raise TimeoutError("run time budget exhausted")

    def remaining_ms(self) -> int:
        return max(1, int((self.deadline - time.monotonic()) * 1000))

    def wait_job(self, receipt):
        job_id = receipt["job_id"]
        while True:
            self.check()
            job = self.host.jobs.get(self.context.run_id, job_id)
            if job.status == "failed":
                self._record_job_usage(job_id, job)
                raise RuntimeError(f"host job failed: {job.failure}")
            if job.status == "completed":
                self._record_job_usage(job_id, job)
                return job
            time.sleep(.01)

    def _record_job_usage(self, job_id: str, job) -> None:
        """Count one terminal host job's usage exactly once per run.

        The registry already drops out-of-order notifications, but a defensive
        dedup here prevents the same terminal event from being charged twice
        when a caller polls the same receipt more than once.
        """
        key = (job_id, getattr(job, "sequence", 0))
        usage = job.usage or {}
        delta = int(usage.get("total_tokens", usage.get("input_tokens", 0) + usage.get("output_tokens", 0)))
        with self._usage_lock:
            if key in self._accounted_jobs:
                return
            self._accounted_jobs.add(key)
            self.tokens += delta

    @property
    def token_limit(self) -> int | None:
        """Effective token stop threshold for this run.

        The host per-run budget takes priority when positive; otherwise the
        plugin falls back to ``XIAOO_MCTS_TOKEN_FUSE_LIMIT`` (default 30M,
        ``0`` disables the token stop entirely).
        """
        budget = self.params.get("budget") or {}
        host_limit = int(budget.get("max_tokens", 0) or 0)
        if host_limit > 0:
            return host_limit
        env_limit = token_fuse_limit_from_env()
        return env_limit if env_limit > 0 else None

    def _token_exhausted(self) -> bool:
        limit = self.token_limit
        return limit is not None and self.tokens >= limit

    def _official_target(self):
        spec = getattr(getattr(self.strategy, "task", None), "spec", None)
        if spec is None:
            return None
        official_fn = getattr(spec, "official_fn", None)
        return official_fn() if official_fn is not None else None

    def _run_official(self, selected):
        """Run the official evaluation at most once per run_id.

        ``official_started`` is set only immediately before entering the
        official function, so a fork failure does not count as an official
        attempt, while a cancelled/failed attempt is never retried.
        """
        official = self._official_target()
        if selected is None or official is None or self.official_started:
            return None
        official_branch = self.host.fork(
            selected.state_id, timeout_ms=self.remaining_ms(),
            idempotency_key=f"{self.context.run_id}:{selected.node_id}:official-fork",
        )
        try:
            poc_target = getattr(self.strategy.task, "poc_sandbox_path", None)
            poc_filename = getattr(self.strategy.task, "poc_filename", "")
            if poc_target and poc_filename.endswith(".py"):
                try:
                    data = self.host.file_read(
                        official_branch["branch_id"], poc_target,
                        timeout_ms=self.remaining_ms(),
                    )
                except Exception:
                    data = b""
                if not _is_valid_python_source(data):
                    self.emit(
                        "plugin.progress",
                        {"summary": f"Skipping official evaluation for {selected.node_id}: invalid Python PoC"},
                    )
                    return None
            self.official_started = True
            result = official(
                selected, self.strategy.task, Path("output"),
                host=self.host, branch_id=official_branch["branch_id"], wait_job=self.wait_job,
            )
            self.official_done = True
            return result
        finally:
            self.host.release(
                official_branch["branch_id"], timeout_ms=self.remaining_ms(),
                idempotency_key=f"{self.context.run_id}:{selected.node_id}:official-release",
            )

    def step(self, parent, child, host):
        if self.strategy is None:
            raise RuntimeError("no benchmark strategy configured; start the plugin with a task mode")
        expansion = self.strategy.expand(self, parent, child, host)
        with self._search_lock:
            self._search_events.append(
                (parent.node_id, child.node_id, expansion.score, expansion.oracle_version)
            )
        return expansion

    def _write_search_trace(self, root: Node, selected: Node | None) -> None:
        """Persist a human-readable MCTS search tree next to official output."""

        task = getattr(self.strategy, "task", None)
        mode = getattr(getattr(task, "spec", None), "id", "unknown")
        task_id = (
            getattr(task, "instance_id", "")
            or getattr(task, "task_id", "")
            or ""
        )
        lines = [
            f"▶ task  mode={mode}  task_id={task_id}",
            f"▶ startup  mode={mode}",
        ]

        grouped: dict[str, list[tuple[str, float, int | None]]] = {}
        parent_order: list[str] = []
        with self._search_lock:
            for parent_id, child_id, score, oracle_version in self._search_events:
                if parent_id not in grouped:
                    grouped[parent_id] = []
                    parent_order.append(parent_id)
                grouped[parent_id].append((child_id, score, oracle_version))
        for iteration, parent_id in enumerate(parent_order):
            lines.append(f"▶ choose  iter={iteration}  node={parent_id}")
            for child_id, score, oracle_version in grouped[parent_id]:
                suffix = (
                    f"  oracle_v={oracle_version}"
                    if oracle_version is not None
                    else ""
                )
                lines.append(f"    {child_id} score={score:.3f}{suffix}")

        lines.extend(["", "▶ tree"])
        for node in iter_tree(root):
            indent = "  " * node.depth
            if node is root:
                lines.append(
                    f"{indent}root  visits={node.visits}  value={node.value:.3f}"
                )
                continue
            lines.append(
                f"{indent}{node.node_id}"
                f"    score={node.score:.3f}  visits={node.visits}  value={node.value:.3f}"
            )

        lines.extend(
            [
                "",
                "▶ first_layer  "
                + "  ".join(f"{c.node_id}={c.score:.3f}" for c in root.children),
                "▶ selected  "
                + (
                    f"node={selected.node_id} score={selected.score:.3f}"
                    if selected
                    else "none"
                ),
            ]
        )

        try:
            if task is not None and getattr(task, "spec", None) is not None:
                out_dir = Path("output") / task.spec.id / task.instance_id
            else:
                out_dir = Path("output")
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "search.log").write_text(
                "\n".join(lines) + "\n", encoding="utf-8"
            )
        except Exception as exc:
            self.emit(
                "plugin.progress",
                {"summary": f"Failed to write search trace: {exc}"},
            )

    def run(self):
        self.deadline = time.monotonic() + self.params["budget"]["max_time_ms"] / 1000
        try:
            self.check()
            if self.strategy is None:
                raise RuntimeError("no benchmark strategy configured; plugin.start has no task mode")
            self.emit("plugin.started", {"summary": "MCTS search started"})
            initialize = getattr(self.strategy, "initialize", None)
            if initialize:
                initialize(self)
            start_author_worker = getattr(self.strategy, "start_author_worker", None)
            if start_author_worker:
                start_author_worker(self)
            root = Node("root", state_id=getattr(self.strategy, "search_root_state_id", None) or self.context.root_state_id)
            Coordinator(
                root, host=self.host, run_context=self.context, step=self.step,
                max_expansions=self.params["budget"]["max_iters"] + 1,
                branching=self.params["budget"]["branching"],
                should_stop=lambda: self._token_exhausted(),
                max_concurrent=max_concurrent_from_env(),
                timeout_ms=self.remaining_ms(),
            ).run()
            self.check()
            nodes = [n for n in iter_tree(root) if n is not root and n.state_id]
            selected = None
            if nodes:
                best_score = max(n.score for n in nodes)
                tied = [n for n in nodes if n.score == best_score]
                fallback = tied[0]
                selected = select_node(
                    root, self.strategy.task, fallback,
                    min_score=best_score,
                    host=self.host, wait_job=self.wait_job,
                    on_event=self.emit,
                )
            official_result = None
            if selected is not None:
                official_result = self._run_official(selected)
                if official_result is not None:
                    self.emit("plugin.progress", {"summary": f"Official evaluation: {official_result[0]}"})
            version = getattr(self.strategy, "evaluation_version", "benchmark-v1")
            evidence = [{"evidence_id": n.node_id, "kind": "test_report", "content": n.feedback} for n in nodes]
            if official_result is not None:
                evidence.append({
                    "evidence_id": f"official:{selected.node_id if selected else 'none'}",
                    "kind": "official_report",
                    "content": f"passed={official_result[0]}",
                })
            candidates = [{"candidate_id": n.node_id, "state_id": n.state_id, "score": n.score, "evaluation_version": f"{version}:oracle-v{n.oracle_version}" if n.oracle_version is not None else version, "summary": n.feedback, "evidence_refs": [n.node_id]} for n in nodes]
            limitations = []
            if official_result is None:
                limitations.append("official evaluation was not configured")
            elif not official_result[0]:
                limitations.append("official evaluation completed with a failing verdict")
            retest_gate = getattr(self.strategy, "_retest_gate", None)
            if retest_gate is not None:
                self.emit("plugin.progress", {"summary": f"RetestGate stats: {retest_gate.stats()}"})
            self._write_search_trace(root, selected)
            result = ResultBuilder(root_state_id=self.context.root_state_id).build(
                answer=selected.solution if selected else "",
                summary=f"Evaluated {len(nodes)} candidates",
                stop_reason="token_budget" if self._token_exhausted() else "node_budget",
                candidates=candidates, evidence=evidence,
                selected_candidate_id=selected.node_id if selected else None,
                apply_plan={"kind": "continue_from_state", "state_id": selected.state_id, "base_state_id": root.state_id} if selected else None,
                limitations=limitations,
            )
            self.emit("plugin.completed", {"result": result})
        except RunCancelled as exc:
            self.emit("plugin.cancelled", {"reason": str(exc), "summary": str(exc)})
        except Exception as exc:
            if self.context.cancelled:
                self.emit("plugin.cancelled", {"reason": self.reason, "summary": self.reason})
            else:
                message = str(exc) or type(exc).__name__
                self.emit("plugin.failed", {"failure": {"code": type(exc).__name__, "message": message}, "summary": message})
        finally:
            stop_author_worker = getattr(self.strategy, "stop_author_worker", None)
            if stop_author_worker is not None:
                try:
                    stop_author_worker(self)
                except Exception:
                    pass
