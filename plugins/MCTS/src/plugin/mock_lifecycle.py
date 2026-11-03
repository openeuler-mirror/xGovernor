"""Stdio lifecycle server for the local ``--mock`` plugin mode."""

from __future__ import annotations

import threading
from typing import Any

from src.harness.run_mock_benchmark import run_mock_search
from src.plugin.lifecycle import RunRegistry
from src.plugin.server import LifecycleServer
from src.plugin.stdio_transport import StdioTransport


class MockRunExecutor:
    """Runs one in-process mock search and emits plugin lifecycle events."""

    def __init__(self, params: dict[str, Any], transport: Any, emit: Any) -> None:
        self.params = params
        self.emit = emit
        self.reason = "cancelled by host"
        self.host = type("Host", (), {"peer": None})()
        self.thread = threading.Thread(
            target=self.run,
            name=f"mcts-mock-{params.get('run_id', 'run')}",
            daemon=True,
        )

    def run(self) -> None:
        self.emit("plugin.started", {"summary": "MCTS mock search started"})
        try:
            budget = self.params.get("budget", {}) or {}
            max_iters = max(1, int(budget.get("max_iters", 2) or 2))
            branching = max(1, int(budget.get("branching", 3) or 3))
            result, _host, _events = run_mock_search(
                max_iters=max_iters,
                branching=branching,
                output_dir=None,
            )
            self.emit("plugin.completed", {"result": result})
        except Exception as exc:
            code = type(exc).__name__
            message = str(exc) or code
            self.emit(
                "plugin.failed",
                {
                    "failure": {"code": code, "message": message},
                    "summary": message,
                },
            )

    def cancel(self, reason: str) -> None:
        self.reason = reason


class MockLifecycleServer(LifecycleServer):
    def _make_executor(self, params: dict[str, Any]):
        emit = lambda method, payload: self._emit(params["run_id"], method, payload)
        return MockRunExecutor(params, self.transport, emit)


def serve_mock_stdio() -> None:
    MockLifecycleServer(StdioTransport(), RunRegistry()).serve_forever()
