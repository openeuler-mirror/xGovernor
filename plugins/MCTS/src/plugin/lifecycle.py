"""In-memory plugin lifecycle registry for the P1 mock boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import threading
import time
from typing import Any, Mapping

from src.core.mcts_protocol import PluginManifest, ProtocolError, PROTOCOL_VERSION, result, notification, validate_params


@dataclass
class RunContext:
    params_hash: str
    params: dict[str, Any]
    status: str = "accepted"
    sequence: int = 0
    summary: str = "accepted"
    terminal_event: dict[str, Any] | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    condition: threading.Condition = field(default_factory=threading.Condition, repr=False)


class RunRegistry:
    def __init__(self, manifest: PluginManifest | None = None) -> None:
        self.manifest = manifest or PluginManifest()
        self.initialized = False
        self.runs: dict[str, RunContext] = {}
        self.shutdown = False

    @staticmethod
    def _hash(params: Mapping[str, Any]) -> str:
        encoded = json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        return hashlib.sha256(encoded).hexdigest()

    def initialize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        validate_params("plugin.initialize", params)
        if PROTOCOL_VERSION not in params["supported_api_versions"]:
            raise ProtocolError("no compatible protocol version")
        required = set(self.manifest.required_capabilities)
        available = set(params["available_capabilities"])
        if not required <= available:
            raise ProtocolError(f"missing required capabilities: {sorted(required - available)}")
        self.initialized = True
        return {"api_version": PROTOCOL_VERSION, "plugin_version": self.manifest.version, "required_host_capabilities": list(self.manifest.required_capabilities), "optional_host_capabilities": list(self.manifest.optional_capabilities)}

    def start(self, params: Mapping[str, Any]) -> dict[str, Any]:
        validate_params("plugin.start", params)
        if self.shutdown or not self.initialized:
            raise ProtocolError("plugin is not initialized")
        run_id = params["run_id"]
        digest = self._hash(params)
        existing = self.runs.get(run_id)
        if existing:
            if existing.params_hash != digest:
                raise ProtocolError("run_id already exists with different parameters")
            return {"run_id": run_id, "status": "accepted"}
        self.runs[run_id] = RunContext(digest, dict(params))
        return {"run_id": run_id, "status": "accepted"}

    def emit(self, method: str, params: Mapping[str, Any]) -> dict[str, Any]:
        run_id = params.get("run_id")
        if run_id not in self.runs:
            raise ProtocolError(f"unknown run: {run_id}")
        run = self.runs[run_id]
        event = notification(method, params)
        sequence = params["sequence"]
        if sequence <= run.sequence:
            return event
        if run.terminal_event is not None:
            raise ProtocolError("run already has a terminal event")
        run.sequence = sequence
        run.events.append(event)
        run.summary = params.get("summary", run.summary)
        if method == "plugin.started":
            run.status = "running"
        elif method == "plugin.completed":
            run.status, run.terminal_event = "completed", event
        elif method == "plugin.cancelled":
            run.status, run.terminal_event = "cancelled", event
        elif method == "plugin.failed":
            run.status, run.terminal_event = "failed", event
        with run.condition:
            run.condition.notify_all()
        return event

    def _snapshot(self, run: RunContext) -> dict[str, Any]:
        value: dict[str, Any] = {
            "run_id": run.params.get("run_id", ""),
            "status": run.status,
            "sequence": run.sequence,
            "summary": run.summary,
        }
        if run.terminal_event:
            params = run.terminal_event["params"]
            if run.status == "completed":
                value["result"] = params["result"]
            elif run.status == "cancelled":
                value["reason"] = params["reason"]
            elif run.status == "failed":
                value["failure"] = params["failure"]
        return value

    def wait(self, run_id: str, after_sequence: int = 0, timeout_ms: int = 0) -> dict[str, Any]:
        run = self.runs.get(run_id)
        if run is None:
            raise ProtocolError(f"unknown run: {run_id}")
        deadline = time.monotonic() + (timeout_ms / 1000 if timeout_ms > 0 else 0)
        with run.condition:
            while run.sequence <= after_sequence and run.terminal_event is None:
                if timeout_ms <= 0:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                run.condition.wait(remaining)
            return self._snapshot(run)

    def cancel(self, run_id: str, reason: str) -> dict[str, Any]:
        run = self.runs.get(run_id)
        if run is None:
            raise ProtocolError(f"unknown run: {run_id}")
        if run.status in {"completed", "cancelled", "failed"}:
            return {"run_id": run_id, "status": run.status}
        run.status = "cancelling"
        return {"run_id": run_id, "status": "cancelling"}

    def close(self) -> dict[str, Any]:
        self.shutdown = True
        return {"plugin_id": self.manifest.plugin_id, "status": "stopped"}


    def complete(self, run_id: str, result: dict[str, Any], *, cleanup=None) -> dict[str, Any]:
        run = self.runs[run_id]
        try:
            event = self.emit('plugin.completed', {'plugin_id': self.manifest.plugin_id, 'run_id': run_id, 'sequence': run.sequence + 1, 'result': result})
            return event
        finally:
            if cleanup:
                cleanup()

    def fail(self, run_id: str, code: str, message: str, *, cleanup=None) -> dict[str, Any]:
        try:
            return self.emit('plugin.failed', {'plugin_id': self.manifest.plugin_id, 'run_id': run_id, 'sequence': self.runs[run_id].sequence + 1, 'failure': {'code': code, 'message': message}})
        finally:
            if cleanup: cleanup()
