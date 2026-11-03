"""Host-backed semantic merge and asynchronous Author worker for plugin runs."""

from __future__ import annotations

import threading
from typing import Any, Callable
from uuid import uuid4


def _job_output(job: Any) -> str:
    if isinstance(job, dict):
        return str(job.get("output", "") or "")
    return str(getattr(job, "output", "") or "")


class HostMergeClient:
    """Adapts ``SemanticMerger.complete`` to the plugin host's ``agent_turn``."""

    def __init__(
        self,
        *,
        host: Any,
        wait_job: Callable[[Any], Any],
        run_id: str,
        author_state_id: Callable[[], str | None],
        profile: str = "memory-merge",
        timeout_ms: Callable[[], int] | None = None,
    ) -> None:
        self.host = host
        self.wait_job = wait_job
        self.run_id = run_id
        self.author_state_id = author_state_id
        self.profile = profile
        self.timeout_ms = timeout_ms or (lambda: 300_000)

    def complete(self, messages: list[dict[str, str]]) -> str:
        state_id = self.author_state_id()
        if not state_id:
            raise RuntimeError("memory merge requires an author state")
        user = messages[-1]["content"] if messages else ""
        timeout = self.timeout_ms()
        branch = self.host.fork(
            state_id,
            timeout_ms=timeout,
            idempotency_key=f"{self.run_id}:memory-merge:{uuid4().hex}",
        )
        branch_id = branch["branch_id"]
        try:
            receipt = self.host.agent_turn(
                branch_id,
                user,
                timeout_ms=timeout,
                idempotency_key=f"{self.run_id}:memory-merge:{uuid4().hex}:turn",
                profile=self.profile,
            )
            job = self.wait_job(receipt)
            return _job_output(job)
        finally:
            self.host.release(
                branch_id,
                timeout_ms=30_000,
                idempotency_key=f"{self.run_id}:memory-merge:{uuid4().hex}:release",
            )


class AuthorWorker:
    """Consumes settled memory snapshots on a dedicated daemon thread."""

    def __init__(
        self,
        strategy: Any,
        executor: Any,
        *,
        poll_interval: float = 0.05,
    ) -> None:
        self.strategy = strategy
        self.executor = executor
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"mcts-author-{executor.context.run_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def stop(self, *, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout)

    def _has_pending(self) -> bool:
        memory = getattr(self.strategy, "_memory", None)
        if memory is None:
            return False
        try:
            return memory.has_unfetched() and not memory.is_merge_pending()
        except Exception:
            return False

    def _run(self) -> None:
        while True:
            if self._stop.is_set() and not self._has_pending():
                return
            if self._has_pending():
                self.strategy._consume_memory(self.executor)
            else:
                self._wake.wait(self.poll_interval)
                self._wake.clear()
