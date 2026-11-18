"""Deterministic host job event tracking."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Mapping

from xgovernor_mcts.engine.mcts_protocol import ProtocolError, validate_notification_params


@dataclass
class JobState:
    run_id: str
    job_id: str
    sequence: int = 0
    status: str = "accepted"
    output: str = ""
    usage: dict[str, int] = field(default_factory=dict)
    failure: dict[str, str] | None = None
    exit_code: int | None = None
    truncated: bool = False
    progress: list[dict[str, Any]] = field(default_factory=list)


class JobRegistry:
    def __init__(self) -> None:
        self.jobs: dict[tuple[str, str], JobState] = {}
        self.run_ids: set[str] = set()
        self._lock = threading.RLock()

    def register_run(self, run_id: str) -> None:
        with self._lock:
            self.run_ids.add(run_id)

    def accept(self, run_id: str, job_id: str) -> JobState:
        with self._lock:
            self.run_ids.add(run_id)
            key = (run_id, job_id)
            if key in self.jobs:
                return self.jobs[key]
            self.jobs[key] = JobState(run_id, job_id)
            return self.jobs[key]

    def apply(self, method: str, params: Mapping[str, Any]) -> JobState:
        validate_notification_params(method, params)
        run_id, job_id, sequence = params["run_id"], params["job_id"], params["sequence"]
        with self._lock:
            if run_id not in self.run_ids:
                raise ProtocolError(f"job notification for unregistered run: {run_id}")
            job = self.jobs.get((run_id, job_id))
            if job is None:
                job = self.accept(run_id, job_id)
            if sequence <= job.sequence:
                return job
            if job.status in {"completed", "failed"}:
                raise ProtocolError(f"job {job_id} emitted event after terminal state")
            job.sequence = sequence
            if method == "host.job.started":
                job.status = "running"
            elif method == "host.job.output_delta":
                job.output += params["delta"]
            elif method == "host.job.progress":
                job.progress.append(dict(params))
                job.usage = dict(params["usage"])
            elif method == "host.job.completed":
                job.status = "completed"
                job.output = params["output"]
                job.usage = dict(params["usage"])
                job.exit_code = params.get("exit_code")
                job.truncated = params.get("truncated", False)
            elif method == "host.job.failed":
                job.status = "failed"
                job.failure = dict(params["failure"])
                job.usage = dict(params["usage"])
            return job

    def progress_since(self, run_id: str, job_id: str, after_sequence: int = 0) -> list[dict[str, Any]]:
        """Read each accepted batch boundary once without scheduling any work."""
        from copy import deepcopy
        with self._lock:
            job = self.get(run_id, job_id)
            return deepcopy([event for event in job.progress if event["sequence"] > after_sequence])

    def get(self, run_id: str, job_id: str) -> JobState:
        with self._lock:
            try:
                return self.jobs[(run_id, job_id)]
            except KeyError as exc:
                raise ProtocolError(f"unknown job: {job_id}") from exc
