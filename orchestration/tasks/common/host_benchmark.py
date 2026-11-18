"""Task-plugin helpers that operate on HostCapabilities only.

These helpers are the P2 migration target for SWE/ZSoft scorers. They keep
task code unaware of 宿主 JSON-RPC runtime/session IDs and are easy to exercise
with ``FakeHostCapabilities``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from .benchmark_util import operation_key

from xgovernor_mcts.engine.host_api import HostCapabilities


@dataclass(frozen=True)
class CommandResult:
    output: str
    exit_code: int
    stdout: bytes
    stderr: bytes = b""
    timed_out: bool = False


class BenchmarkBranch:
    """Synchronous benchmark IO over asynchronous host jobs.

    Protocol v1 combines stdout/stderr in ``output``. Keep that complete evidence
    in stdout rather than pretending it is two independently captured streams.
    Failed jobs propagate; a command's nonzero exit is interpreted by the scorer.
    """
    def __init__(self, host, branch_id, wait_job):
        self.host, self.branch_id, self.wait_job = host, branch_id, wait_job

    def exec(self, command: str, *, cwd="/home/user/workspace", timeout_ms=300_000):
        receipt = self.host.exec(self.branch_id, ["bash", "-lc", command], cwd=cwd,
                                 timeout_ms=timeout_ms, idempotency_key=operation_key(self.branch_id, "benchmark:exec", cwd, command))
        job = self.wait_job(receipt)
        get = job.get if isinstance(job, dict) else lambda key, default=None: getattr(job, key, default)
        if get("status") != "completed":
            raise RuntimeError("benchmark command has no completed job result")
        if get("truncated", False):
            raise RuntimeError("benchmark command output was truncated")
        code = get("exit_code")
        if isinstance(code, bool) or not isinstance(code, int):
            raise RuntimeError("benchmark command completed without exit_code")
        output = get("output", "")
        return CommandResult(output, code, output.encode("utf-8"))

    def read(self, path):
        return self.host.file_read(self.branch_id, path, timeout_ms=120_000)

    def write(self, path, data):
        return self.host.file_write(self.branch_id, path, data, timeout_ms=120_000,
                                    idempotency_key=operation_key(self.branch_id, "benchmark:write", path, data.hex()))


@dataclass(frozen=True)
class HostTaskWorkspace:
    branch_id: str
    cwd: str = "/home/user/workspace/repo"

    def read(self, host: HostCapabilities, path: str, *, timeout_ms: int = 30_000) -> bytes:
        return host.file_read(self.branch_id, path, timeout_ms=timeout_ms)

    def write(self, host: HostCapabilities, path: str, data: bytes, *, timeout_ms: int = 30_000, idempotency_key: str = "write") -> int:
        return host.file_write(self.branch_id, path, data, timeout_ms=timeout_ms, idempotency_key=operation_key(self.branch_id, idempotency_key, path, data.hex()))

    def execute(self, host: HostCapabilities, argv: list[str], *, timeout_ms: int = 300_000, idempotency_key: str = "exec") -> dict[str, Any]:
        return host.exec(self.branch_id, argv, cwd=self.cwd, timeout_ms=timeout_ms, idempotency_key=operation_key(self.branch_id, idempotency_key, self.cwd, argv))

    def diff(self, host: HostCapabilities, base_state_id: str, *, timeout_ms: int = 30_000) -> str:
        return host.diff(self.branch_id, base_state_id, timeout_ms=timeout_ms)
