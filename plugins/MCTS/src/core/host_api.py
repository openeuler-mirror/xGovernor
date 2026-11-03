"""Dependency-injection contracts used by the portable MCTS core."""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Any, Callable, Protocol, Sequence


@dataclass(frozen=True)
class BranchHandle:
    branch_id: str
    state_id: str | None = None


@dataclass(frozen=True)
class JobReceipt:
    job_id: str
    status: str = "accepted"


@dataclass
class RunContext:
    plugin_id: str
    run_id: str
    root_state_id: str
    granted_capabilities: set[str] = field(default_factory=set)
    cancelled: bool = False


class HostCapabilities(Protocol):
    def snapshot(self, branch_id: str, *, timeout_ms: int, idempotency_key: str) -> dict[str, Any]: ...
    def fork(self, parent_state_id: str, *, timeout_ms: int, idempotency_key: str) -> dict[str, Any]: ...
    def agent_turn(self, branch_id: str, text: str, *, timeout_ms: int, idempotency_key: str, profile: str | None = None) -> dict[str, Any]: ...
    def inspect(self, branch_id: str, *, view: str, timeout_ms: int) -> str: ...
    def exec(self, branch_id: str, argv: list[str], *, cwd: str, timeout_ms: int, idempotency_key: str) -> dict[str, Any]: ...
    def file_read(self, branch_id: str, path: str, *, timeout_ms: int) -> bytes: ...
    def file_write(self, branch_id: str, path: str, data: bytes, *, timeout_ms: int, idempotency_key: str) -> int: ...
    def diff(self, branch_id: str, base_state_id: str, *, timeout_ms: int) -> str: ...
    def release(self, branch_id: str, *, timeout_ms: int, idempotency_key: str) -> None: ...


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    monotonic = staticmethod(time.monotonic)
    sleep = staticmethod(time.sleep)


class FakeClock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += max(0.0, seconds)

    def advance(self, seconds: float) -> None:
        self.value += seconds

