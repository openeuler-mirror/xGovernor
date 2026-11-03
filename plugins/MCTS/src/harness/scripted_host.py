"""In-memory host with reusable job event scripts.

This mirrors the branch/state/job shape used by the portable MCTS core, but
completes jobs locally instead of going through xGovernor or HTTP.
"""
from __future__ import annotations

import re
from typing import Any

from src.plugin.mock_host import FakeHostCapabilities

AGENT_OUTPUT = (
    "<solution>mock patch</solution>"
    "<reflection>mock reflection</reflection>"
    "<query></query>"
)
PYTEST_SUMMARY = (
    "============================= 3 passed, 0 failed in 0.1s "
    "=============================="
)
SCORE_SEQUENCE = (0.0, 0.25, 0.5, 0.75, 1.0)
_BRANCH_NUMBER_RE = re.compile(r"(\d+)\s*$")


def pytest_summary_for_score(score: float) -> str:
    total = 4
    passed = max(0, min(total, round(score * total)))
    failed = total - passed
    return f"{'=' * 41} {passed} passed, {failed} failed in 0.1s {'=' * 41}"


class ScriptedHost(FakeHostCapabilities):
    def __init__(
        self,
        *,
        run_id: str = "mock-run",
        agent_output: str = AGENT_OUTPUT,
        pytest_summary: str = PYTEST_SUMMARY,
        exec_output: str = "",
        differentiated_scores: bool = True,
        score_sequence: tuple[float, ...] = SCORE_SEQUENCE,
    ) -> None:
        super().__init__()
        self.run_id = run_id
        self.agent_output = agent_output
        self.pytest_summary = pytest_summary
        self.exec_output = exec_output
        self.differentiated_scores = differentiated_scores
        self.score_sequence = score_sequence
        self.job_scripts: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        self.job_events: list[dict[str, Any]] = []

    def schedule_job(
        self, job_id: str, events: list[tuple[str, dict[str, Any]]]
    ) -> None:
        self.job_scripts[job_id] = list(events)

    def _job(self, job_id: str) -> dict[str, Any]:
        job = self.jobs.get(job_id)
        if job is None:
            job = {"job_id": job_id, "branch_id": None}
            self.jobs[job_id] = job
        return job

    def _apply_event(
        self,
        job_id: str,
        method: str,
        params: dict[str, Any],
    ) -> None:
        job = self._job(job_id)
        job["sequence"] = int(params.get("sequence", 1))
        if method == "host.job.started":
            job["status"] = "running"
        elif method == "host.job.output_delta":
            job["status"] = "running"
            job["output"] = str(job.get("output", "")) + str(params.get("delta", ""))
        elif method == "host.job.completed":
            job.update(
                {
                    "status": "completed",
                    "output": str(params.get("output", "")),
                    "usage": dict(params.get("usage", {})),
                    "exit_code": params.get("exit_code"),
                    "truncated": params.get("truncated", False),
                }
            )
        elif method == "host.job.failed":
            job.update(
                {
                    "status": "failed",
                    "failure": dict(params.get("failure", {})),
                    "usage": dict(params.get("usage", {})),
                }
            )
        self.job_events.append(
            {
                "job_id": job_id,
                "method": method,
                "params": {**params, "job_id": job_id, "run_id": self.run_id},
            }
        )

    def _run_job_script(
        self,
        job_id: str,
        fallback: list[tuple[str, dict[str, Any]]],
    ) -> None:
        script = self.job_scripts.pop(job_id, None)
        events = script if script is not None else fallback
        for method, params in events:
            self._apply_event(job_id, method, {**params, "sequence": int(params.get("sequence", 1))})

    def _pytest_output(self, branch_id: str) -> str:
        if not self.differentiated_scores:
            return self.pytest_summary
        match = _BRANCH_NUMBER_RE.search(branch_id)
        number = int(match.group(1)) if match else 0
        score = self.score_sequence[number % len(self.score_sequence)]
        return pytest_summary_for_score(score)

    def agent_turn(
        self,
        branch_id: str,
        text: str,
        *,
        timeout_ms: int,
        idempotency_key: str,
        profile: str | None = None,
    ) -> dict[str, Any]:
        receipt = super().agent_turn(
            branch_id,
            text,
            timeout_ms=timeout_ms,
            idempotency_key=idempotency_key,
            profile=profile,
        )
        job_id = receipt["job_id"]
        self._run_job_script(
            job_id,
            [
                ("host.job.started", {"job_id": job_id, "sequence": 1}),
                (
                    "host.job.completed",
                    {
                        "job_id": job_id,
                        "sequence": 2,
                        "output": self.agent_output,
                        "exit_code": 0,
                        "usage": {"total_tokens": 7},
                    },
                ),
            ],
        )
        return receipt

    def exec(
        self,
        branch_id: str,
        argv: list[str],
        *,
        cwd: str,
        timeout_ms: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        receipt = super().exec(
            branch_id,
            argv,
            cwd=cwd,
            timeout_ms=timeout_ms,
            idempotency_key=idempotency_key,
        )
        job_id = receipt["job_id"]
        has_pytest = any("pytest" in part.split("/")[-1] for part in argv)
        output = self._pytest_output(branch_id) if has_pytest else self.exec_output
        self._run_job_script(
            job_id,
            [
                ("host.job.started", {"job_id": job_id, "sequence": 1}),
                (
                    "host.job.completed",
                    {
                        "job_id": job_id,
                        "sequence": 2,
                        "output": output,
                        "exit_code": 0,
                        "usage": {},
                    },
                ),
            ],
        )
        return receipt

    @property
    def live_branch_count(self) -> int:
        return len(self.branches)
