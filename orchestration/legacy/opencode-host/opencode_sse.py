"""OpenCode SSE event stream and mid-turn host.job.progress translation."""
from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode
from urllib import error as urllib_error
from urllib import request as urllib_request


SESSION_PROGRESS_TYPES: tuple[str, ...] = (
    "session.text.delta",
    "session.reasoning.delta",
    "session.tool.called",
    "session.usage.updated",
    "session.step.ended",
    "session.execution.succeeded",
    "session.execution.failed",
    "session.execution.interrupted",
)


class SSEParser:
    """Incremental parser for text/event-stream frames."""

    def __init__(self) -> None:
        self._buf = ""

    def feed(self, chunk: bytes | str) -> list[dict[str, str]]:
        if isinstance(chunk, bytes):
            text = chunk.decode("utf-8", errors="replace")
        else:
            text = chunk
        if not text:
            return []
        self._buf += text
        frames: list[dict[str, str]] = []
        while True:
            sep = "\n\n" if "\n\n" in self._buf else ("\r\n\r\n" if "\r\n\r\n" in self._buf else None)
            if sep is None:
                break
            raw, self._buf = self._buf.split(sep, 1)
            frame = parse_sse_frame(raw)
            if frame is not None:
                frames.append(frame)
        return frames


def parse_sse_frame(raw: str) -> dict[str, str] | None:
    data_lines: list[str] = []
    fields: dict[str, str] = {}
    for line in raw.splitlines():
        if not line or line.startswith(":"):
            continue
        if ":" in line:
            name, value = line.split(":", 1)
            if value.startswith(" "):
                value = value[1:]
        else:
            name, value = line, ""
        if name == "data":
            data_lines.append(value)
        elif name in {"event", "id", "retry"}:
            fields[name] = value
    if data_lines:
        fields["data"] = "\n".join(data_lines)
    return fields or None


def decode_sse_event(frame: Mapping[str, str]) -> dict[str, Any] | None:
    raw = frame.get("data")
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    typ = payload.get("type") if isinstance(payload.get("type"), str) else None
    if typ is None:
        framed = frame.get("event")
        typ = framed if framed and framed != "message" else None
    if typ is None and isinstance(payload.get("event"), str):
        typ = payload["event"]
    data = payload.get("data") if isinstance(payload.get("data"), dict) else None
    if data is None:
        data = payload
    return {
        "id": frame.get("id") or payload.get("id"),
        "type": typ,
        "data": data,
        "payload": payload,
    }


def session_id_of(event: Mapping[str, Any]) -> str | None:
    for block in (event.get("data"), event.get("payload"), event):
        if not isinstance(block, Mapping):
            continue
        for key in ("sessionID", "session_id"):
            value = block.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _as_int(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float):
        return max(0, int(value))
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def usage_from_tokens(tokens: Mapping[str, Any] | None) -> dict[str, int]:
    raw = tokens if isinstance(tokens, Mapping) else {}
    cache = raw.get("cache") if isinstance(raw.get("cache"), Mapping) else {}
    input_tokens = _as_int(raw.get("input"))
    output_tokens = _as_int(raw.get("output"))
    reasoning_tokens = _as_int(raw.get("reasoning"))
    cache_read = _as_int(cache.get("read") if cache is not None else 0)
    cache_write = _as_int(cache.get("write") if cache is not None else 0)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "reasoning_tokens": reasoning_tokens,
        "cache_read_tokens": cache_read,
        "cache_write_tokens": cache_write,
        "total_tokens": input_tokens + output_tokens + reasoning_tokens,
    }


def event_url(base_url: str, types: tuple[str, ...] = SESSION_PROGRESS_TYPES) -> str:
    base = base_url.rstrip("/")
    query = urlencode([("types", item) for item in types])
    return f"{base}/api/event?{query}" if query else f"{base}/api/event"


@dataclass
class JobProgressMapper:
    """Turn OpenCode session events into host.job.progress payloads."""

    run_id: str
    job_id: str
    sequence: int
    usage: dict[str, int] = field(default_factory=lambda: usage_from_tokens({}))
    context_tokens: int = 0
    head_input_tokens: int = 0
    response_count: int = 0
    pending_text: str = ""
    pending_tools: list[dict[str, Any]] | None = None

    def apply(self, event: Mapping[str, Any]) -> dict[str, Any] | None:
        typ = event.get("type")
        data = event.get("data") if isinstance(event.get("data"), Mapping) else {}
        if typ == "session.usage.updated":
            self._set_usage(data.get("tokens") if isinstance(data.get("tokens"), Mapping) else {})
            if self.pending_tools and self._has_output():
                tools = self.pending_tools
                self.pending_tools = None
                return self._emit(self._flush_text(), tools)
            return None
        if typ in ("session.text.delta", "session.reasoning.delta"):
            delta = data.get("delta")
            if isinstance(delta, str) and delta:
                self.pending_text += delta
            return None
        if typ == "session.step.ended":
            self.response_count += 1
            if self.pending_text and not self.pending_tools:
                return self._emit(self._flush_text(), [])
            return None
        if typ == "session.tool.called":
            tools = [_tool_payload(data)]
            if self._has_output():
                return self._emit(self._flush_text(), tools)
            self.pending_tools = tools
            return None
        return None

    def _set_usage(self, tokens: Mapping[str, Any]) -> None:
        self.usage = usage_from_tokens(tokens)
        self.head_input_tokens = self.usage["input_tokens"]
        self.context_tokens = self.usage["input_tokens"] + self.usage["cache_read_tokens"]

    def _has_output(self) -> bool:
        return (self.usage.get("output_tokens", 0) + self.usage.get("reasoning_tokens", 0)) > 0

    def _flush_text(self) -> str:
        text = self.pending_text
        self.pending_text = ""
        return text

    def _emit(self, new_text: str, tools: list[dict[str, Any]]) -> dict[str, Any]:
        params = {
            "run_id": self.run_id,
            "job_id": self.job_id,
            "sequence": self.sequence,
            "response_count": self.response_count,
            "head_input_tokens": self.head_input_tokens,
            "usage": dict(self.usage),
            "new_text": new_text,
            "tools": tools,
            "context_tokens": self.context_tokens,
        }
        self.sequence += 1
        return params


def _tool_payload(data: Mapping[str, Any]) -> dict[str, Any]:
    native = data.get("native") if isinstance(data.get("native"), Mapping) else {}
    name = data.get("name") or native.get("name") or native.get("tool") or data.get("id")
    return {
        "id": data.get("id"),
        "name": name,
        "input": data.get("input") if isinstance(data.get("input"), Mapping) else {},
        "executed": bool(data.get("executed")),
    }


class OpenCodeEventHub:
    """Session-routed event dispatcher with optional live SSE and reconnect."""

    def __init__(
        self,
        *,
        opener: Callable[[str | None], Any] | None = None,
        reconnect: bool = True,
        backoff_s: tuple[float, ...] = (0.5, 1.0, 2.0, 5.0),
    ) -> None:
        self._opener = opener
        self._reconnect = reconnect
        self._backoff_s = backoff_s
        self._lock = threading.Lock()
        self._subscribers: dict[str, list[Callable[[dict[str, Any]], None]]] = {}
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._thread: threading.Thread | None = None
        self._response: Any = None
        self._last_event_id: str | None = None
        self._error: BaseException | None = None
        self._ever_connected = threading.Event()

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def start(self, *, wait_s: float = 5.0) -> bool:
        if self._opener is None:
            # Inject-only mode for tests; the dispatcher is ready immediately.
            self._connected.set()
            self._ever_connected.set()
            return True
        if self._thread is not None and self._thread.is_alive():
            return self._wait_started(wait_s)
        self._stop.clear()
        self._connected.clear()
        self._ever_connected.clear()
        self._error = None
        self._thread = threading.Thread(target=self._run, name="opencode-sse", daemon=True)
        self._thread.start()
        return self._wait_started(wait_s)

    def _wait_started(self, wait_s: float) -> bool:
        """True if the stream came up at least once, even if it later dropped and is reconnecting."""
        deadline = time.monotonic() + max(0.0, wait_s)
        while not self._ever_connected.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self._ever_connected.wait(timeout=remaining)
        return self._ever_connected.is_set()

    def wait_connected(self, wait_s: float) -> bool:
        return self._connected.wait(timeout=max(0.0, wait_s))

    def stop(self) -> None:
        self._stop.set()
        response = self._response
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        self._thread = None
        self._connected.clear()

    def subscribe(self, session_id: str, callback: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        with self._lock:
            self._subscribers.setdefault(session_id, []).append(callback)

        def unsubscribe() -> None:
            with self._lock:
                listeners = self._subscribers.get(session_id) or []
                self._subscribers[session_id] = [item for item in listeners if item is not callback]
                if not self._subscribers[session_id]:
                    self._subscribers.pop(session_id, None)

        return unsubscribe

    def inject(self, event: Mapping[str, Any]) -> None:
        typ = event.get("type")
        data = event.get("data") if isinstance(event.get("data"), Mapping) else event.get("data")
        if typ is None and isinstance(event.get("payload"), Mapping):
            typ = event["payload"].get("type")
        normalized = {
            "id": event.get("id"),
            "type": typ,
            "data": data if isinstance(data, Mapping) else {},
            "payload": event.get("payload") or dict(event),
        }
        self._dispatch(normalized)

    def _dispatch(self, event: dict[str, Any]) -> None:
        session_id = session_id_of(event)
        if not session_id:
            return
        with self._lock:
            listeners = list(self._subscribers.get(session_id) or [])
        for callback in listeners:
            try:
                callback(event)
            except Exception:
                continue

    def _run(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                response = self._opener(self._last_event_id)
                self._response = response
                self._connected.set()
                self._ever_connected.set()
                attempt = 0
                self._consume(response)
            except Exception as exc:
                self._error = exc
                self._connected.clear()
                if self._stop.is_set() or not self._reconnect:
                    return
                delay = self._backoff_s[min(attempt, len(self._backoff_s) - 1)]
                attempt += 1
                self._stop.wait(delay)
            finally:
                response = self._response
                self._response = None
                if response is not None:
                    try:
                        response.close()
                    except Exception:
                        pass
                if not self._reconnect:
                    return

    def _consume(self, response: Any) -> None:
        parser = SSEParser()
        while not self._stop.is_set():
            chunk = response.read(4096)
            if not chunk:
                return
            for frame in parser.feed(chunk):
                if frame.get("id"):
                    self._last_event_id = frame["id"]
                event = decode_sse_event(frame)
                if event is not None:
                    self._dispatch(event)


def open_http_event_stream(
    *,
    base_url: str,
    headers: Mapping[str, str],
    last_event_id: str | None = None,
    types: tuple[str, ...] = SESSION_PROGRESS_TYPES,
    timeout_s: float | None = None,
) -> Any:
    url = event_url(base_url, types)
    request_headers = dict(headers)
    request_headers["Accept"] = "text/event-stream"
    request_headers.pop("Content-Type", None)
    if last_event_id:
        request_headers["Last-Event-ID"] = str(last_event_id)
    request = urllib_request.Request(url, headers=request_headers, method="GET")
    try:
        return urllib_request.urlopen(request, timeout=timeout_s)
    except urllib_error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        detail = raw.strip() or error.reason
        raise RuntimeError(f"OpenCode GET /api/event failed with HTTP {error.code}: {detail}") from error
    except urllib_error.URLError as error:
        raise RuntimeError(f"could not reach OpenCode event stream at {url}: {error.reason}") from error
