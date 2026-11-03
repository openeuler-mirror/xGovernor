"""Host capability interface and an in-memory fake used by P1 tests."""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Any

from src.core.mcts_protocol import ProtocolError, require_capability


@dataclass
class FakeHostCapabilities:
    granted: set[str] = field(default_factory=lambda: {"snapshot", "fork", "agent_turn", "inspect", "exec", "file_read", "file_write", "diff", "release"})
    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    files: dict[tuple[str, str], bytes] = field(default_factory=dict)
    states: dict[str, str] = field(default_factory=lambda: {"state-root": "branch-root"})
    branches: set[str] = field(default_factory=lambda: {"branch-root"})
    jobs: dict[str, dict[str, Any]] = field(default_factory=dict)
    _counter: int = 0

    def _call(self, capability: str, **params: Any) -> None:
        require_capability(self.granted, capability)
        self.calls.append((capability, params))

    def snapshot(self, branch_id: str, *, timeout_ms: int, idempotency_key: str) -> dict[str, Any]:
        self._call("snapshot", branch_id=branch_id, timeout_ms=timeout_ms, idempotency_key=idempotency_key)
        self._counter += 1
        state = f"state-{self._counter}"
        self.states[state] = branch_id
        return {"state_id": state, "expires_at": "2030-01-01T00:00:00Z"}

    def fork(self, parent_state_id: str, *, timeout_ms: int, idempotency_key: str) -> dict[str, Any]:
        self._call("fork", parent_state_id=parent_state_id, timeout_ms=timeout_ms, idempotency_key=idempotency_key)
        if parent_state_id not in self.states:
            raise ProtocolError(f"unknown state: {parent_state_id}")
        self._counter += 1
        branch = f"branch-{self._counter}"
        self.branches.add(branch)
        return {"branch_id": branch}

    def agent_turn(self, branch_id: str, text: str, *, timeout_ms: int, idempotency_key: str, profile: str | None = None) -> dict[str, Any]:
        self._call("agent_turn", branch_id=branch_id, input=text, timeout_ms=timeout_ms, idempotency_key=idempotency_key, **({"profile": profile} if profile else {}))
        self._counter += 1
        job = f"job-{self._counter}"
        self.jobs[job] = {"branch_id": branch_id, "input": text}
        return {"job_id": job, "status": "accepted"}

    def inspect(self, branch_id: str, *, view: str, timeout_ms: int) -> str:
        self._call("inspect", branch_id=branch_id, view=view, timeout_ms=timeout_ms)
        return f"{view}:{branch_id}"

    def exec(self, branch_id: str, argv: list[str], *, cwd: str, timeout_ms: int, idempotency_key: str) -> dict[str, Any]:
        self._call("exec", branch_id=branch_id, argv=argv, cwd=cwd, timeout_ms=timeout_ms, idempotency_key=idempotency_key)
        self._counter += 1
        job = f"job-{self._counter}"
        self.jobs[job] = {"branch_id": branch_id, "argv": argv, "cwd": cwd}
        return {"job_id": job, "status": "accepted"}

    def file_read(self, branch_id: str, path: str, *, timeout_ms: int) -> bytes:
        self._call("file_read", branch_id=branch_id, path=path, timeout_ms=timeout_ms)
        return self.files.get((branch_id, path), b"")

    def file_write(self, branch_id: str, path: str, data: bytes, *, timeout_ms: int, idempotency_key: str) -> int:
        self._call("file_write", branch_id=branch_id, path=path, timeout_ms=timeout_ms, idempotency_key=idempotency_key)
        self.files[(branch_id, path)] = bytes(data)
        return len(data)

    def diff(self, branch_id: str, base_state_id: str, *, timeout_ms: int) -> str:
        self._call("diff", branch_id=branch_id, base_state_id=base_state_id, timeout_ms=timeout_ms)
        return ""

    def release(self, branch_id: str, *, timeout_ms: int, idempotency_key: str) -> None:
        self._call("release", branch_id=branch_id, timeout_ms=timeout_ms, idempotency_key=idempotency_key)
        self.branches.discard(branch_id)

