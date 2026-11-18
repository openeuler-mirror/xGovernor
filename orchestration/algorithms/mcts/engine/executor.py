"""Run orchestration: branch/job lifecycle around an injected benchmark strategy."""
from __future__ import annotations

import ast
import json
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol

from xgovernor_mcts.engine.host_api import RunContext
from xgovernor_mcts.engine.fork_judge import LLMForkPolicy
from xgovernor_mcts.engine.fork_policy import DEFAULT_FORK_THRESHOLD, dynamic_fork_enabled, token_hesitation_policy
from xgovernor_mcts.engine.mcts_coordinator import (
    Coordinator,
    Expansion,
    ExpansionFailed,
    Node,
    SolverInput,
    iter_tree,
    max_concurrent_from_env,
)
from xgovernor_mcts.engine.result_builder import ResultBuilder
from xgovernor_mcts.engine.retest_gate import RetestGate, retest_gate_enabled
from xgovernor_mcts.engine.token_budget import token_fuse_limit_from_env
from xgovernor_mcts.engine.submission_selector import select_node
from xgovernor_mcts.memory import MemoryPool
from xgovernor_mcts.memory.merger import SemanticMerger
from xgovernor_tasks.common.benchmark_util import harvest_triplet, operation_key, stamp_testcases_version, strip_testcases_version
from xgovernor_tasks.generic.task import SANDBOX_REPO
from xgovernor_mcts.bridge.memory_host import AuthorWorker, HostMergeClient
from xgovernor_mcts.bridge.host_client import RpcHostCapabilities


# Host RPC still requires a finite operation timeout for runs without a deadline.
DEFAULT_HOST_OPERATION_TIMEOUT_MS = 1_800_000


class HostJobFailed(ExpansionFailed):
    """A terminal failed host job; cancellation and protocol errors stay distinct."""


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


def _oracle_kind(task) -> str:
    """Return the generic oracle kind: python source vs. plain script.

    Modes whose oracle file ends in ``.py`` keep the strict Python syntax gate;
    generic modes can use a language-neutral ``verify.sh`` entrypoint instead.
    """
    path = getattr(getattr(task, "spec", None), "oracle_rel_path", "testcases.py")
    return "python" if str(path).endswith(".py") else "script"


def _oracle_is_valid(task, data: bytes) -> bool:
    if _oracle_kind(task) == "python":
        return _is_valid_python_source(data)
    return bool(data.strip())


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
    inherit_scoring_history: bool = True
    _oracle_lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)
    oracle_ready: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    author_error: BaseException | None = field(default=None, init=False, repr=False)
    _memory: MemoryPool | None = field(default=None, init=False, repr=False)
    _author_worker: AuthorWorker | None = field(default=None, init=False, repr=False)
    _retest_gate: RetestGate | None = field(default=None, init=False, repr=False)
    _last_memory_json: str = field(default="{}", init=False, repr=False)
    _initial_author: tuple[str, dict] | None = field(default=None, init=False, repr=False)
    _initial_author_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def prepare(self, executor: "RunExecutor") -> None:
        """Materialize the task root and snapshot a clean search root (no oracle)."""
        host = executor.host
        self._retest_gate = RetestGate() if retest_gate_enabled() else None
        branch = host.fork(executor.context.root_state_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:task-root:fork")
        branch_id = branch["branch_id"]
        try:
            prepare = getattr(self.task, "bootstrap_prepare", None)
            if prepare:
                prepare(host, branch_id)
            materialize = host.exec(branch_id, ["bash", "-lc", self.task.materialize_script()], cwd="/testbed" if getattr(self.task, "prepared_root", False) else "/home/user/workspace", timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:materialize")
            materialized = executor.wait_job(materialize)
            exit_code = _job_field(materialized, "exit_code")
            if exit_code != 0:
                output = _job_field(materialized, "output", "")
                raise RuntimeError(f"task materialization failed (exit {exit_code}): {output[-4000:]}")
            post = getattr(self.task, "bootstrap_post", None)
            if post:
                post(host, branch_id, wait_job=executor.wait_job)
            self.search_root_state_id = host.snapshot(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:task-root-clean")["state_id"]
        finally:
            host.release(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:task-root:release")

    def _author_init(self, executor: "RunExecutor") -> None:
        """Background entry point for the author oracle.

        Success and failure are both published through ``oracle_ready``; a
        failure is retained in ``author_error`` so the scoring path can fail
        fast instead of hanging.
        """
        try:
            self._author_init_once(executor)
        except BaseException as exc:
            self.author_error = exc
        finally:
            self.oracle_ready.set()

    def _author_init_once(self, executor: "RunExecutor") -> None:
        """Create the author oracle on an isolated fork of the clean search root."""
        host = executor.host
        branch = host.fork(self.search_root_state_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:init:fork", offline=True)
        branch_id = branch["branch_id"]
        try:
            receipt = host.agent_turn(branch_id, self.task.init_author(), timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:init", profile="author-init")
            with self._initial_author_lock:
                self._initial_author = (branch_id, receipt)
            author_job = executor.wait_job(receipt)
            author_output = _job_field(author_job, "output", "")
            oracle_path = self.task.oracle_host_path
            oracle_rel_path = self.task.spec.oracle_rel_path
            repo_path = f"{SANDBOX_REPO}/{oracle_rel_path}"
            prompt_path = f"{getattr(self.task, 'prompt_repo', SANDBOX_REPO)}/{oracle_rel_path}"
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
                if _oracle_is_valid(self.task, data):
                    break
                if _oracle_kind(self.task) == "python":
                    problem = "does not exist" if not data.strip() else "exists but is not syntactically valid Python"
                    check = f"Run `python3 -m py_compile {prompt_path}` before finishing if shell access is available."
                else:
                    problem = "does not exist" if not data.strip() else "exists but is empty or not a valid executable script"
                    check = f"Run `bash -n {prompt_path}` before finishing if shell access is available."
                rescue = (
                    f"HARD FAILURE ({attempt}): {prompt_path} {problem}. "
                    f"Create exactly {prompt_path} now with a file-writing tool. It must "
                    f"satisfy the Author contract and be runnable by the scorer. {check} "
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
            if not _oracle_is_valid(self.task, data):
                generated = None
                if _oracle_kind(self.task) == "python":
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
                    executor.emit("plugin.progress", {"summary": "Materialized Author-generated oracle from response text"})
                else:
                    detail = author_output[-4000:] if author_output else "(no author turn output captured)"
                    executor.emit("plugin.progress", {"summary": f"Author produced no valid oracle. Output: {detail}"})
                    raise RuntimeError(f"author did not create valid oracle: {repo_path}")
            oracle_path.parent.mkdir(parents=True, exist_ok=True)
            oracle_path.write_bytes(data)
            stamp_testcases_version(oracle_path, 0)
            oracle_bytes = oracle_path.read_bytes()
            with self._oracle_lock:
                self.oracle_bytes = oracle_bytes
            host.file_write(branch_id, repo_path, oracle_bytes, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:oracle:v0")
            self.author_state_id = host.snapshot(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:v0:snapshot")["state_id"]
        finally:
            with self._initial_author_lock:
                self._initial_author = None
            host.release(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:author:init:release")
        self.start_author_worker(executor)

    def initialize(self, executor: "RunExecutor") -> None:
        """Prepare the clean search root, then create the author oracle."""
        self.prepare(executor)
        self._author_init(executor)
        if self.author_error is not None:
            raise self.author_error

    def _sync_author(self, executor: "RunExecutor", query: str) -> None:
        try:
            self._sync_author_once(executor, query)
        except HostJobFailed as exc:
            executor.event("node", node_id="author", parents=["root"], role="author", status="failed")
            executor.extension("turn_failed", node_id="author", reason=str(exc))

    def _sync_author_once(self, executor: "RunExecutor", query: str) -> None:
        if not query.strip() or not self.author_state_id:
            return
        with self._oracle_lock:
            host = executor.host
            version = self.oracle_version
            scope = operation_key(self.author_state_id, "author-revision", query)
            branch = host.fork(self.author_state_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{scope}:fork", offline=True)
            branch_id = branch["branch_id"]
            repo_path = f"/home/user/workspace/repo/{self.task.spec.oracle_rel_path}"
            try:
                prompt = self.task.step_author({"memory": query}, node_id="solver-query")
                receipt = host.agent_turn(branch_id, prompt, timeout_ms=executor.remaining_ms(), idempotency_key=f"{scope}:step", profile="author-step")
                executor.wait_job(receipt)
                updated = host.file_read(branch_id, repo_path, timeout_ms=executor.remaining_ms())
                if _oracle_is_valid(self.task, updated) and strip_testcases_version(updated) != strip_testcases_version(self.oracle_bytes):
                    self.oracle_version = version + 1
                    path = self.task.oracle_host_path
                    path.write_bytes(updated)
                    stamp_testcases_version(path, self.oracle_version)
                    self.oracle_bytes = path.read_bytes()
                    host.file_write(branch_id, repo_path, self.oracle_bytes, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:oracle:v{self.oracle_version}")
                    self.author_state_id = host.snapshot(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{scope}:snapshot")["state_id"]
                    executor.emit("plugin.progress", {"summary": f"Author updated benchmark oracle to v{self.oracle_version}"})
                    executor.extension("oracle", node_id="author", revision=self.oracle_version)
            finally:
                host.release(branch_id, timeout_ms=executor.remaining_ms(), idempotency_key=f"{scope}:release")

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
        # A run may stop before any Solver reaches the scoring boundary. If the
        # background author still owns a branch/job, cancel and release it.
        with self._initial_author_lock:
            initial_author = self._initial_author
            self._initial_author = None
        if initial_author is not None:
            branch_id, receipt = initial_author
            try:
                executor.host.cancel_job(
                    receipt["job_id"],
                    reason="run stopped before initial scoring",
                    timeout_ms=30_000,
                    idempotency_key=f"{executor.context.run_id}:author:init:cancel",
                )
            finally:
                executor.host.release(
                    branch_id,
                    timeout_ms=30_000,
                    idempotency_key=f"{executor.context.run_id}:author:init:release",
                )

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

    def _fork_for_scoring(self, executor: "RunExecutor", host: Any, state_id: str, key: str) -> dict[str, Any]:
        options = {} if self.inherit_scoring_history else {"inherit_history": False}
        return host.fork(
            state_id,
            timeout_ms=executor.remaining_ms(),
            idempotency_key=key,
            offline=True,
            **options,
        )

    def expand(self, executor: "RunExecutor", parent: Node, child: Node, host: Any) -> Expansion:
        executor.check()
        coordinator = getattr(executor, "coordinator", None)
        dynamic_fork = coordinator is not None and coordinator.should_fork is not None
        replay = child.solver_input
        if replay is None:
            with self._oracle_lock:
                current_version = self.oracle_version
                oracle_bytes = self.oracle_bytes
            if parent.parent is not None and parent.state_id and parent.oracle_version != current_version:
                def retest_parent():
                    retest_branch = self._fork_for_scoring(
                        executor, host, parent.state_id,
                        f"{executor.context.run_id}:{child.node_id}:parent-retest-fork",
                    )
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
            if parent.parent is not None and oracle_bytes:
                host.file_write(child.branch_id, f"/home/user/workspace/repo/{self.task.spec.oracle_rel_path}", oracle_bytes, timeout_ms=executor.remaining_ms(), idempotency_key=f"{executor.context.run_id}:{child.node_id}:oracle-v{current_version}")
            solver_profile = (
                self.solver_init_profile
                if parent.parent is None and not parent.solution
                else self.solver_profile
            )
            prompt = self._prompt(parent)
            if dynamic_fork:
                head = host.snapshot(child.branch_id, timeout_ms=executor.remaining_ms(),
                                     idempotency_key=f"{executor.context.run_id}:{child.node_id}:solver-head")
                child.solver_input = SolverInput(head["state_id"], prompt, solver_profile, current_version)
        else:
            prompt, solver_profile, current_version = replay.text, replay.profile, replay.oracle_version
        remaining = executor.remaining_ms()
        receipt = host.agent_turn(
            child.branch_id, prompt, timeout_ms=remaining,
            idempotency_key=f"{executor.context.run_id}:{child.node_id}:solve",
            profile=solver_profile,
        )
        child.job_id = receipt["job_id"]
        if dynamic_fork:
            child.solver_running = True
            try:
                job = executor.wait_job(receipt, on_progress=lambda event: coordinator.on_progress(child, event))
            finally:
                child.solver_running = False
        else:
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
        if not self.oracle_ready.wait(timeout=executor.remaining_ms() / 1000.0):
            raise TimeoutError("timed out waiting for author oracle")
        if self.author_error is not None:
            raise self.author_error
        score_branch = self._fork_for_scoring(
            executor, host, solution_snapshot["state_id"],
            f"{executor.context.run_id}:{child.node_id}:score-fork",
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
    def __init__(self, params, transport, emit, *, strategy: SearchStrategy | None = None, should_fork=None, managed_parameters=None):
        self.params = params
        self.transport = transport
        self.emit = emit
        self.managed_parameters = managed_parameters
        self.context = RunContext(params["plugin_id"], params["run_id"], params["root_state_id"], set(params["granted_capabilities"]))
        self.host = RpcHostCapabilities(
            transport, plugin_id=params["plugin_id"], run_id=params["run_id"],
            granted=self.context.granted_capabilities, external_dispatch=True,
        )
        self.strategy = strategy
        self.should_fork = should_fork
        self._fork_disabled_reason = None
        if self.should_fork is None and dynamic_fork_enabled():
            if "job_progress" in self.context.granted_capabilities:
                threshold = float(os.environ.get("XIAOO_MCTS_FORK_SCORE_THRESHOLD", str(DEFAULT_FORK_THRESHOLD)))
                self.should_fork = LLMForkPolicy(token_hesitation_policy(threshold), self._complete_fork_judge, self.emit)
            else:
                self._fork_disabled_reason = "host did not grant job_progress"
        self.coordinator = None
        self.thread = threading.Thread(target=self.run, name=f"mcts-{params['run_id']}", daemon=True)
        self.reason = "cancelled by host"
        self.deadline = 0.0
        self.tokens = 0
        self._accounted_jobs: set[str | tuple[str, int]] = set()
        self._usage_lock = threading.Lock()
        self._search_lock = threading.Lock()
        self._search_events: list[tuple[str, str, float, int | None]] = []
        self.official_started = False
        self.official_done = False
        if self.managed_parameters is not None:
            self.host.operation_timeout_ms = self.remaining_ms
            managed_host = self.host
            previous = managed_host.peer.on_notification

            def on_notification(message):
                previous(message)
                if message["method"] in {"host.job.completed", "host.job.failed"}:
                    payload = message["params"]
                    self._record_job_usage(payload["job_id"], managed_host.jobs.get(
                        self.context.run_id, payload["job_id"]))

            self.host.peer.on_notification = on_notification

    def _complete_fork_judge(self, context, prompt):
        # One tools-disabled model request on an isolated head copy. It never
        # enters the search queue or consumes a candidate slot.
        client = HostMergeClient(
            host=self.host, wait_job=self.wait_job, run_id=self.context.run_id,
            author_state_id=lambda: context.head_state_id,
            profile="fork-judge", timeout_ms=self.remaining_ms,
        )
        return client.complete([{"role": "user", "content": prompt}])

    def cancel(self, reason):
        self.reason = reason
        self.context.cancelled = True

    def check(self):
        if self.context.cancelled:
            raise RunCancelled(self.reason)
        if time.monotonic() >= self.deadline:
            raise TimeoutError("run time budget exhausted")

    def remaining_ms(self) -> int:
        if self.deadline == math.inf:
            return DEFAULT_HOST_OPERATION_TIMEOUT_MS
        return max(1, int((self.deadline - time.monotonic()) * 1000))

    def wait_job(self, receipt, *, on_progress=None):
        job_id = receipt["job_id"]
        progress_sequence = 0
        while True:
            self.check()
            if on_progress is not None:
                for event in self.host.jobs.progress_since(self.context.run_id, job_id, progress_sequence):
                    on_progress(event)
                    progress_sequence = event["sequence"]
            job = self.host.jobs.get(self.context.run_id, job_id)
            if job.status == "failed":
                self._record_job_usage(job_id, job)
                raise HostJobFailed(f"host job failed: {job.failure}")
            if job.status == "completed":
                self._record_job_usage(job_id, job)
                return job
            time.sleep(.01)

    def _record_job_usage(self, job_id: str, job) -> None:
        """Count one terminal host job's usage exactly once per run.

        Managed jobs have one immutable terminal usage record, deduplicated by
        job ID. Legacy hosts retain their sequence-based accounting convention.
        """
        key = job_id if self.managed_parameters is not None else (job_id, getattr(job, "sequence", 0))
        usage = job.usage or {}
        fallback = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)
        if self.managed_parameters is not None:
            fallback = usage.get("tokens", fallback)
        delta = int(usage.get("total_tokens", fallback))
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
        if self.managed_parameters is not None:
            return int(budget["max_tokens"])
        host_limit = int(budget.get("max_tokens", 0) or 0)
        if host_limit > 0:
            return host_limit
        env_limit = token_fuse_limit_from_env()
        return env_limit if env_limit > 0 else None

    def _token_exhausted(self) -> bool:
        limit = self.token_limit
        if self.managed_parameters is not None:
            reserve = math.ceil(limit * self.managed_parameters.get("finish_reserve_ratio", 0.1))
            return self.remaining_tokens <= reserve
        return limit is not None and self.tokens >= limit

    def _time_exhausted(self) -> bool:
        if self.managed_parameters is None:
            return False
        reserve_ms = math.ceil(self.params["budget"]["max_time_ms"] *
                               self.managed_parameters.get("finish_reserve_ratio", 0.1))
        return (self.deadline - time.monotonic()) * 1000 <= reserve_ms

    def _exploration_exhausted(self) -> bool:
        return self._token_exhausted() or self._time_exhausted()

    def _stop_reason(self) -> str:
        if self._token_exhausted():
            return "token_budget"
        return "time_budget" if self._time_exhausted() else "node_budget"

    @property
    def remaining_tokens(self) -> int:
        with self._usage_lock:
            return max(0, self.token_limit - self.tokens)

    def event(self, kind, **payload):
        if self.managed_parameters is not None:
            self.emit("plugin.progress", {"summary": "mcts.event.v1 " + json.dumps(
                {"type": kind, "payload": payload}, ensure_ascii=False, allow_nan=False)})

    def extension(self, name, **data):
        self.event("extension", namespace="mcts", version="1",
                   schema="urn:xgovernor:mcts:events:1", name=name, data=data)

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

    def _export_final_artifact(self, selected):
        """Fork the selected candidate and export its artifact to the output dir.

        Modes without an official evaluator (generic) need this so the final
        product code is persisted instead of only living inside the sandbox.
        Returns the artifact text (may be empty on failure).
        """
        task = getattr(self.strategy, "task", None)
        if selected is None or task is None:
            return ""
        spec = getattr(task, "spec", None)
        reader_fn = getattr(spec, "artifact_reader_fn", None) if spec is not None else None
        if reader_fn is None or not selected.state_id:
            return ""
        # The artifact is the user's final deliverable. A nearly-exhausted run
        # budget must not silently drop it: grant a short grace window and give
        # the fork/release round-trip a sane floor so the read can complete.
        if self.managed_parameters is None:
            self.deadline = max(self.deadline, time.monotonic() + 180.0)
        export_timeout_ms = self.remaining_ms() if self.managed_parameters is not None else max(60_000, self.remaining_ms())
        branch = self.host.fork(
            selected.state_id, timeout_ms=export_timeout_ms,
            idempotency_key=f"{self.context.run_id}:{selected.node_id}:artifact-export-fork",
        )
        try:
            _, artifact = reader_fn()(
                selected, task, host=self.host,
                branch_id=branch["branch_id"], wait_job=self.wait_job,
            )
        except Exception as exc:
            self.emit("plugin.progress", {"summary": f"Final artifact export failed: {exc}"})
            return ""
        finally:
            self.host.release(
                branch["branch_id"], timeout_ms=export_timeout_ms,
                idempotency_key=f"{self.context.run_id}:{selected.node_id}:artifact-export-release",
            )
        artifact = (artifact or "").strip()
        if not artifact:
            return ""
        out_dir = Path("output") / spec.id / task.instance_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "artifact.txt"
        out_file.write_text(artifact + "\n", encoding="utf-8")
        self.emit("plugin.progress", {"summary": f"Final artifact exported to {out_file}"})
        return artifact

    def _export_final_files(self, selected):
        """Fork the selected candidate and read its deliverable files.

        ``artifact_files_fn`` returns ``{workspace-relative path: content}`` on
        modes that produce live code (generic). The host then writes these back
        into the originating session before reporting completion.
        """
        task = getattr(self.strategy, "task", None)
        if selected is None or task is None or not selected.state_id:
            return {}
        spec = getattr(task, "spec", None)
        files_fn = getattr(spec, "artifact_files_fn", None) if spec is not None else None
        if files_fn is None:
            return {}
        resolver = files_fn()
        if resolver is None:
            return {}
        if self.managed_parameters is None:
            self.deadline = max(self.deadline, time.monotonic() + 180.0)
        export_timeout_ms = self.remaining_ms() if self.managed_parameters is not None else max(60_000, self.remaining_ms())
        branch = self.host.fork(
            selected.state_id,
            timeout_ms=export_timeout_ms,
            idempotency_key=f"{self.context.run_id}:{selected.node_id}:artifact-files-export-fork",
        )
        try:
            files = resolver(
                selected, task, host=self.host,
                branch_id=branch["branch_id"], wait_job=self.wait_job,
            ) or {}
        except Exception as exc:
            self.emit("plugin.progress", {"summary": f"Final artifact files export failed: {exc}"})
            return {}
        finally:
            self.host.release(
                branch["branch_id"], timeout_ms=export_timeout_ms,
                idempotency_key=f"{self.context.run_id}:{selected.node_id}:artifact-files-export-release",
            )
        return files if isinstance(files, dict) else {}

    def step(self, parent, child, host):
        if self.strategy is None:
            raise RuntimeError("no benchmark strategy configured; start the plugin with a task mode")
        self.event("node", node_id=child.node_id, parents=[parent.node_id], role="solver", status="running")
        self.extension("expansion", node_id=child.node_id, parent_id=parent.node_id)
        try:
            expansion = self.strategy.expand(self, parent, child, host)
        except HostJobFailed as exc:
            self.event("node", node_id=child.node_id, parents=[parent.node_id], role="solver", status="failed")
            self.extension("turn_failed", node_id=child.node_id, reason=str(exc))
            raise
        self.event("node", node_id=child.node_id, parents=[parent.node_id], role="solver", status="completed")
        self.event("score", node_id=child.node_id, metrics={"score": expansion.score},
                   evaluation_version=f"{getattr(self.strategy, 'evaluation_version', 'benchmark-v1')}:oracle-v{expansion.oracle_version}")
        self.extension("oracle", node_id=child.node_id, revision=expansion.oracle_version)
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
        max_time_ms = self.params["budget"]["max_time_ms"]
        self.deadline = (time.monotonic() + max_time_ms / 1000
                         if max_time_ms or self.managed_parameters is not None else math.inf)
        try:
            self.check()
            if self.strategy is None:
                raise RuntimeError("no benchmark strategy configured; plugin.start has no task mode")
            self.emit("plugin.started", {"summary": "MCTS search started"})
            if self.should_fork is not None and "job_progress" not in self.context.granted_capabilities:
                raise RuntimeError("dynamic siblings require host job_progress")
            if self._fork_disabled_reason:
                self.emit("plugin.progress", {"summary": f"Dynamic fork disabled: {self._fork_disabled_reason}; using fixed branching"})
            strategy = self.strategy
            prepare = getattr(strategy, "prepare", None)
            author_init = getattr(strategy, "_author_init", None)
            if prepare is None or author_init is None:
                raise RuntimeError("benchmark strategy must expose prepare() and _author_init()")
            strategy.oracle_ready.clear()
            strategy.author_error = None
            prepare(self)
            def initialize_author():
                self.event("node", node_id="author", parents=["root"], role="author", status="running")
                author_init(self)
                self.event("node", node_id="author", parents=["root"], role="author",
                           status="failed" if getattr(strategy, "author_error", None) else "completed")
                if isinstance(strategy.author_error, HostJobFailed):
                    self.extension("turn_failed", node_id="author", reason=str(strategy.author_error))
                self.extension("oracle", node_id="author", revision=getattr(strategy, "oracle_version", None))

            author_thread = threading.Thread(
                target=initialize_author,
                name=f"mcts-author-init-{self.context.run_id}", daemon=True,
            )
            root = Node("root", state_id=getattr(strategy, "search_root_state_id", None) or self.context.root_state_id)
            self.event("node", node_id="root", parents=[], role="root", status="prepared")
            author_thread.start()
            self.coordinator = Coordinator(
                root, host=self.host, run_context=self.context, step=self.step,
                max_expansions=self.params["budget"]["max_iters"] + 1,
                branching=self.params["budget"]["branching"],
                should_stop=self._exploration_exhausted,
                max_concurrent=(self.managed_parameters.get("max_concurrent", 3)
                                if self.managed_parameters is not None else max_concurrent_from_env()),
                timeout_ms=self.remaining_ms(),
                should_fork=self.should_fork,
            )
            self.coordinator.run()
            self.check()
            author_thread.join(timeout=30.0)
            if author_thread.is_alive():
                raise TimeoutError("author init did not complete after search")
            if strategy.author_error is not None and not isinstance(strategy.author_error, HostJobFailed):
                raise strategy.author_error
            nodes = [n for n in iter_tree(root) if n is not root and n.state_id]
            selected = None
            if nodes:
                best_score = max(n.score for n in nodes)
                tied = [n for n in nodes if n.score == best_score]
                # Stable tie-break by node_id so local OpenCode runs are safe if
                # the LLM selector is unavailable.
                tied.sort(key=lambda n: n.node_id)
                fallback = tied[0]
                task = getattr(self.strategy, "task", None)
                if task is None:
                    selected = fallback
                else:
                    selected = select_node(
                        root, task, fallback,
                        min_score=best_score,
                        host=self.host, wait_job=self.wait_job,
                        on_event=self.emit,
                    )
            self.event("selection", candidate_ids=[selected.node_id] if selected else [],
                       reason="highest score; submission selector with stable fallback" if selected else "No evaluated candidates available")
            self.extension("search_stopped", reason=self._stop_reason(),
                           remaining_tokens=self.remaining_tokens if self.managed_parameters is not None else None)
            final_artifact = ""
            final_files = {}
            if selected is not None and self._official_target() is None:
                final_artifact = self._export_final_artifact(selected)
                final_files = self._export_final_files(selected)
            official_result = None
            if selected is not None:
                official_result = self._run_official(selected)
                if official_result is not None:
                    status = getattr(strategy.task, "official_status", None) or ("passed" if official_result[0] else "failed")
                    self.emit("plugin.progress", {"summary": f"Official evaluation: {status}; {getattr(strategy.task, 'official_reason', '')}"})
            version = getattr(self.strategy, "evaluation_version", "benchmark-v1")
            evidence = [{"evidence_id": n.node_id, "kind": "test_report", "content": n.feedback} for n in nodes]
            if official_result is not None:
                evidence.append({
                    "evidence_id": f"official:{selected.node_id if selected else 'none'}",
                    "kind": "official_report",
                    "content": f"status={getattr(strategy.task, 'official_status', None) or ('passed' if official_result[0] else 'failed')}; {getattr(strategy.task, 'official_reason', '')}",
                })
            candidates = [{"candidate_id": n.node_id, "state_id": n.state_id, "score": n.score, "evaluation_version": f"{version}:oracle-v{n.oracle_version}" if n.oracle_version is not None else version, "summary": n.feedback, "evidence_refs": [n.node_id]} for n in nodes]
            limitations = []
            if not nodes:
                limitations.append("No candidates: no Solver path completed with a valid evaluation")
            for node in iter_tree(root):
                if node.failed:
                    limitations.append(f"Failed node {node.node_id}: {node.feedback}")
            if strategy.author_error is not None:
                limitations.append(f"Author failed: {strategy.author_error}")
            if self._official_target() is not None and official_result is None:
                limitations.append("official evaluation not run: no candidate" if not nodes else "official evaluation was not configured")
            elif official_result is not None and official_result[0] is None:
                limitations.append("official evaluation not run: " + getattr(strategy.task, "official_reason", "unavailable"))
            elif official_result is not None and not official_result[0]:
                limitations.append("official evaluation completed with a failing verdict")
            retest_gate = getattr(self.strategy, "_retest_gate", None)
            if retest_gate is not None:
                self.emit("plugin.progress", {"summary": f"RetestGate stats: {retest_gate.stats()}"})
            self._write_search_trace(root, selected)
            result = ResultBuilder(root_state_id=self.context.root_state_id).build(
                answer=final_artifact or (selected.solution if selected else ""),
                summary=f"Evaluated {len(nodes)} candidates" if nodes else "No candidates: all evaluation paths failed or search stopped before evaluation",
                stop_reason=self._stop_reason() if nodes else "no_candidates",
                candidates=candidates, evidence=evidence,
                selected_candidate_id=selected.node_id if selected else None,
                apply_plan={"kind": "continue_from_state", "state_id": selected.state_id, "base_state_id": root.state_id} if selected else None,
                limitations=limitations,
                files=final_files,
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
