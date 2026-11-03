"""Small JSON-RPC transport primitives used by the plugin and its tests.

The production adapter can replace ``MockTransport`` with stdio or a socket;
the peer and request bookkeeping are deliberately transport agnostic.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import itertools
import threading
from typing import Any, Callable, Mapping, Protocol

from src.core.mcts_protocol import ProtocolError, decode, error, validate_message


class MessageTransport(Protocol):
    def send(self, message: Mapping[str, Any]) -> None: ...
    def recv(self, timeout: float | None = None) -> dict[str, Any] | None: ...
    def close(self) -> None: ...


class MockTransport:
    """In-memory transport with deterministic injection and inspection."""

    def __init__(self) -> None:
        self._queue: deque[dict[str, Any]] = deque()
        self._condition = threading.Condition()
        self.sent: list[dict[str, Any]] = []
        self._sent_cursor = 0
        self.closed = False

    def send(self, message: Mapping[str, Any]) -> None:
        wire = validate_message(message)
        with self._condition:
            if self.closed:
                raise ConnectionError("transport is closed")
            self.sent.append(wire)
            self._condition.notify_all()

    def take_sent(self, timeout: float | None = None) -> dict[str, Any] | None:
        """Return the next outbound message for a fake peer."""
        with self._condition:
            if not self.sent and not self.closed:
                self._condition.wait(timeout)
            if self._sent_cursor < len(self.sent):
                message = self.sent[self._sent_cursor]
                self._sent_cursor += 1
                return message
            return None

    def inject(self, message: Mapping[str, Any] | str | bytes) -> None:
        wire = decode(message)
        with self._condition:
            if self.closed:
                raise ConnectionError("transport is closed")
            self._queue.append(wire)
            self._condition.notify()

    def recv(self, timeout: float | None = None) -> dict[str, Any] | None:
        with self._condition:
            if not self._queue and not self.closed:
                self._condition.wait(timeout)
            if self._queue:
                return self._queue.popleft()
            return None

    def close(self) -> None:
        with self._condition:
            self.closed = True
            self._condition.notify_all()


@dataclass
class PendingCall:
    request_id: str | int
    method: str
    event: threading.Event = field(default_factory=threading.Event)
    response: dict[str, Any] | None = None


_REQUEST_IDS = itertools.count(1)


class RpcPeer:
    """Request/response multiplexer over a message transport.

    ``call`` sends a request and waits while dispatching notifications through
    ``on_notification``. Tests can inject a response from another thread or
    use ``serve_once`` to implement a tiny fake host.
    """

    def __init__(self, transport: MessageTransport, *, external_dispatch: bool = False, on_notification: Callable[[dict[str, Any]], None] | None = None) -> None:
        self.transport = transport
        self.external_dispatch = external_dispatch
        self.on_notification = on_notification
        self._ids = _REQUEST_IDS
        self._pending: dict[str | int, PendingCall] = {}
        self._pending_lock = threading.Lock()

    def call(self, method: str, params: Mapping[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
        request_id = next(self._ids)
        request = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": dict(params)}
        validate_message(request)
        pending = PendingCall(request_id, method)
        with self._pending_lock:
            self._pending[request_id] = pending
        self.transport.send(request)
        try:
            if self.external_dispatch:
                if not pending.event.wait(timeout):
                    raise TimeoutError(f"timed out waiting for {method}")
                message = pending.response
                if message is None:
                    raise ProtocolError(f"missing response payload for {method}")
            else:
                while True:
                    message = self.transport.recv(timeout)
                    if message is not None and "method" in message:
                        if self.on_notification: self.on_notification(message)
                        continue
                    break
            if message is None:
                raise TimeoutError(f"timed out waiting for {method}")
            if message.get("id") != request_id:
                raise ProtocolError(f"unexpected response id: {message.get('id')!r}")
            return message
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)

    def dispatch(self, message: dict[str, Any]) -> None:
        """Deliver a response received by an outer transport dispatcher."""
        if "method" in message:
            if self.on_notification: self.on_notification(message)
            return
        request_id = message.get("id")
        with self._pending_lock:
            pending = self._pending.get(request_id)
            if pending is None:
                return
            pending.response = message
        pending.event.set()

    def serve_once(self, handler: Callable[[dict[str, Any]], Mapping[str, Any] | None], *, timeout: float | None = None) -> dict[str, Any] | None:
        message = self.transport.recv(timeout)
        if message is None:
            return None
        if "method" not in message or "id" not in message:
            if self.on_notification:
                self.on_notification(message)
            return message
        response = handler(message)
        if response is not None:
            self.transport.send(response)
        return message
