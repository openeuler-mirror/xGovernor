"""RPC backed implementation of the Core ``HostCapabilities`` boundary."""

from __future__ import annotations

import base64
import shlex
import time
from uuid import uuid4
from typing import Any

from src.core.mcts_protocol import ProtocolError, require_capability, validate_result
from src.plugin.job_registry import JobRegistry
from src.plugin.rpc_transport import MessageTransport, RpcPeer


_FILE_WRITE_CHUNK_SIZE = 1_000_000
_JOB_POLL_INTERVAL_S = 0.01


class RpcHostCapabilities:
    def __init__(self, transport: MessageTransport, *, plugin_id: str, run_id: str, granted: set[str], jobs: JobRegistry | None = None, external_dispatch: bool = False) -> None:
        self.plugin_id = plugin_id
        self.run_id = run_id
        self.granted = set(granted)
        self.jobs = jobs or JobRegistry()
        self.jobs.register_run(self.run_id)
        self.peer = RpcPeer(transport, external_dispatch=external_dispatch, on_notification=self._on_notification)

    def _on_notification(self, message: dict[str, Any]) -> None:
        if message["method"].startswith("host.job."):
            self.jobs.apply(message["method"], message["params"])

    def _call(self, capability: str, method: str, params: dict[str, Any]) -> dict[str, Any]:
        require_capability(self.granted, capability)
        common = {"plugin_id": self.plugin_id, "run_id": self.run_id}
        response = self.peer.call(method, {**common, **params}, timeout=params.get("timeout_ms", 30000) / 1000)
        if "error" in response:
            error = response["error"]
            raise ProtocolError(f"host RPC {method} failed: {error['code']} {error['message']}")
        validate_result(method, response["result"])
        return response["result"]

    def snapshot(self, branch_id: str, *, timeout_ms: int, idempotency_key: str) -> dict[str, Any]:
        return self._call("snapshot", "host.snapshot", {"timeout_ms": timeout_ms, "branch_id": branch_id, "idempotency_key": idempotency_key})

    def fork(self, parent_state_id: str, *, timeout_ms: int, idempotency_key: str) -> dict[str, Any]:
        return self._call("fork", "host.fork", {"timeout_ms": timeout_ms, "parent_state_id": parent_state_id, "idempotency_key": idempotency_key})

    def agent_turn(self, branch_id: str, text: str, *, timeout_ms: int, idempotency_key: str, profile: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"timeout_ms": timeout_ms, "branch_id": branch_id, "input": text, "idempotency_key": idempotency_key}
        if profile is not None: params["profile"] = profile
        receipt = self._call("agent_turn", "host.agent_turn", params)
        self.jobs.accept(self.run_id, receipt["job_id"])
        return receipt

    def inspect(self, branch_id: str, *, view: str, timeout_ms: int) -> str:
        return self._call("inspect", "host.inspect", {"timeout_ms": timeout_ms, "branch_id": branch_id, "view": view})["content"]

    def exec(self, branch_id: str, argv: list[str], *, cwd: str, timeout_ms: int, idempotency_key: str) -> dict[str, Any]:
        receipt = self._call("exec", "host.exec", {"timeout_ms": timeout_ms, "branch_id": branch_id, "argv": argv, "cwd": cwd, "idempotency_key": idempotency_key})
        self.jobs.accept(self.run_id, receipt["job_id"])
        return receipt

    def file_read(self, branch_id: str, path: str, *, timeout_ms: int) -> bytes:
        value = self._call("file_read", "host.file_read", {"timeout_ms": timeout_ms, "branch_id": branch_id, "path": path})
        try:
            return base64.b64decode(value["content_base64"], validate=True)
        except Exception as exc:
            raise ProtocolError("host.file_read returned invalid base64") from exc

    def file_write(self, branch_id: str, path: str, data: bytes, *, timeout_ms: int, idempotency_key: str) -> int:
        if len(data) <= _FILE_WRITE_CHUNK_SIZE:
            return self._file_write_direct(
                branch_id, path, data, timeout_ms=timeout_ms, idempotency_key=idempotency_key
            )
        return self._file_write_chunked(
            branch_id, path, data, timeout_ms=timeout_ms, idempotency_key=idempotency_key
        )

    def _file_write_direct(self, branch_id: str, path: str, data: bytes, *, timeout_ms: int, idempotency_key: str) -> int:
        value = self._call("file_write", "host.file_write", {
            "timeout_ms": timeout_ms,
            "branch_id": branch_id,
            "path": path,
            "content_base64": base64.b64encode(data).decode("ascii"),
            "idempotency_key": idempotency_key,
        })
        return value["written_bytes"]

    def _file_write_chunked(self, branch_id: str, path: str, data: bytes, *, timeout_ms: int, idempotency_key: str) -> int:
        suffix = uuid4().hex
        parts: list[str] = []
        for offset in range(0, len(data), _FILE_WRITE_CHUNK_SIZE):
            part_path = f"{path}.{suffix}.part{offset}"
            self._file_write_direct(
                branch_id,
                part_path,
                data[offset : offset + _FILE_WRITE_CHUNK_SIZE],
                timeout_ms=timeout_ms,
                idempotency_key=f"{idempotency_key}:{offset}",
            )
            parts.append(shlex.quote(part_path))

        assemble = f"cat {' '.join(parts)} > {shlex.quote(path)} && rm -f {' '.join(parts)}"
        receipt = self.exec(
            branch_id,
            ["bash", "-lc", assemble],
            cwd="/home/user/workspace",
            timeout_ms=timeout_ms,
            idempotency_key=f"{idempotency_key}:assemble",
        )
        self._wait_for_job(receipt["job_id"], timeout_ms)
        return len(data)

    def _wait_for_job(self, job_id: str, timeout_ms: int) -> None:
        deadline = time.monotonic() + timeout_ms / 1000
        while True:
            job = self.jobs.get(self.run_id, job_id)
            if job.status == "completed":
                return
            if job.status == "failed":
                failure = job.failure or {}
                raise ProtocolError(f"host job {job_id} failed: {failure.get('message', failure)}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for host job {job_id}")
            time.sleep(_JOB_POLL_INTERVAL_S)

    def diff(self, branch_id: str, base_state_id: str, *, timeout_ms: int) -> str:
        return self._call("diff", "host.diff", {"timeout_ms": timeout_ms, "branch_id": branch_id, "base_state_id": base_state_id})["content"]

    def release(self, branch_id: str, *, timeout_ms: int, idempotency_key: str) -> None:
        self._call("release", "host.release", {"timeout_ms": timeout_ms, "branch_id": branch_id, "idempotency_key": idempotency_key})
