# 轻量控制台状态输出。运行过程不在源码树生成日志文件。

from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, TextIO


from src.res.benchmark_util import TAGS

Stream = Literal["solver", "author"]

_log_solver: TextIO | None = None
_log_author: TextIO | None = None
_log_memory: TextIO | None = None
_events: TextIO | None = None
_lock = threading.Lock()
_seq = 0
_t0 = 0.0
_memory_round = 0


def open_logs(
    **_ignored,
) -> None:
    global _seq, _t0, _memory_round
    _seq, _t0, _memory_round = 0, time.monotonic(), 0


def close_logs() -> None:
    global _log_solver, _log_author, _log_memory, _events
    for handle in (_log_solver, _log_author, _log_memory, _events):
        if handle:
            handle.close()
    _log_solver = _log_author = _log_memory = _events = None


def _file(stream: Stream) -> TextIO | None:
    return _log_author if stream == "author" else _log_solver


def _tty_hi(stream: Stream) -> tuple[str, str]:
    if not sys.stdout.isatty() or os.environ.get("NO_COLOR"):
        return "", ""
    code = "95" if stream == "author" else "96"
    return f"\033[1;{code}m", "\033[0m"


def _json_field(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    return value


def event(name: str, *, stream: Stream = "solver", **fields) -> None:
    global _seq
    if not _log_solver and not _log_author and not _events:
        return
    safe_fields = {key: _json_field(val) for key, val in fields.items()}
    with _lock:
        _seq += 1
        payload = {
            "seq": _seq,
            "event": name,
            "stream": stream,
            "ts": datetime.now(timezone.utc).isoformat(),
            "elapsed_ms": round((time.monotonic() - _t0) * 1000, 3),
            "thread": threading.current_thread().name,
            **safe_fields,
        }
        line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        log_file = _file(stream)
        if log_file:
            log_file.write(f"EVENT {line}\n")
            log_file.flush()
        if _events:
            _events.write(line + "\n")
            _events.flush()
    if stream == "author" and name == "memory.add":
        hi, reset = _tty_hi("author")
        print(
            f"{hi}  memory  add  node={fields.get('node', '?')}  "
            f"success={fields.get('success')}{reset}"
        )
    if stream == "author" and name == "memory.fetch":
        hi, reset = _tty_hi("author")
        print(f"{hi}  memory  fetch  bytes={fields.get('bytes', '?')} version={fields.get('version', '?')}{reset}")
    if stream == "author" and name == "author.sync" and fields.get("ok"):
        report_author_update(
            changed=bool(fields.get("changed")),
            testcases_version=fields.get("testcases_version"),
        )


def debug_print(text: str) -> None:
    """Lifecycle debug output: always prints to stdout when enabled."""
    if os.environ.get("XIAOO_LIFECYCLE_DEBUG", "1").strip().lower() not in {"0", "false", "no", "off"}:
        print(text, flush=True)


def write(text: str = "", *, stream: Stream = "solver") -> None:
    log_file = _file(stream)
    if log_file:
        with _lock:
            log_file.write(text + "\n")
            log_file.flush()


def stage(label: str, *, stream: Stream = "solver", **fields) -> None:
    hi, reset = _tty_hi(stream)
    prefix = "[author] " if stream == "author" else ""
    tail = "  " + "  ".join(f"{k}={v}" for k, v in fields.items()) if fields else ""
    print(f"{hi}▶ {prefix}{label}{reset}{tail}")
    event(f"scheduler.{label}", stream=stream, **fields)


def lifecycle(node_id: str, *, stream: Stream = "solver"):
    def emit(ev: str, fields: dict) -> None:
        event(ev, node=node_id, stream=stream, **fields)

    return emit


def write_context_input(node_id: str, text: str, *, stream: Stream = "solver") -> None:
    write(f"── {node_id}  input", stream=stream)
    write(_indent(text), stream=stream)


def write_agent_step_input(phase: str, node_id: str, text: str, *, stream: Stream) -> None:
    write(f"=== {phase} {node_id} ===", stream=stream)
    write_context_input(node_id, text, stream=stream)


class TurnLog:
    def __init__(self, node_id: str, *, stream: Stream = "solver"):
        self.node_id = node_id
        self.stream = stream
        self._cur = None

    def __call__(self, ev: dict) -> None:
        t = ev.get("type")
        if t == "turn_start":
            self._flush()
            self._cur = {"turn": int(ev.get("turn") or 0), "thinking": "", "text": "", "tools": []}
        elif self._cur and t == "thinking_delta":
            self._cur["thinking"] = ev.get("snapshot") or self._cur["thinking"] + str(ev.get("delta") or "")
        elif self._cur and t == "text_delta":
            self._cur["text"] = ev.get("snapshot") or self._cur["text"] + str(ev.get("delta") or "")
        elif self._cur and t == "tool_result":
            name = str(ev.get("tool_name") or ev.get("name") or "tool")
            self._cur["tools"].append(name)
            # Surface file tool outcomes — failures here explain silent no-op edits.
            if name in ("file_edit", "file_write", "file_read") or "file" in name:
                err = ev.get("is_error")
                out = ev.get("output_preview") or ev.get("output") or ""
                if isinstance(out, (dict, list)):
                    out = json.dumps(out, ensure_ascii=False)[:500]
                else:
                    out = str(out)[:500]
                write(
                    f"tool_result: {name}  is_error={err}  output={out!r}",
                    stream=self.stream,
                )
        elif t == "done":
            self._flush()
            write(f"── {self.node_id}  final reply:", stream=self.stream)
            write(_indent(str(ev.get("raw_reply") or ev.get("reply") or "(empty)")), stream=self.stream)

    def _flush(self) -> None:
        if not self._cur:
            return
        write(f"── {self.node_id}  turn {self._cur['turn']}", stream=self.stream)
        if self._cur["thinking"].strip():
            write(_indent(self._cur["thinking"]), stream=self.stream)
        if self._cur["text"].strip():
            write(_indent(self._cur["text"]), stream=self.stream)
        for name in self._cur["tools"]:
            write(f"tool: {name}", stream=self.stream)
        self._cur = None


def _testcases_suffix(testcases_version: int | None) -> str:
    if testcases_version is None:
        return ""
    return f" testcases=v{testcases_version}"


def report_author_update(*, changed: bool, testcases_version: int | None) -> None:
    hi, reset = _tty_hi("author")
    print(f"{hi}  author update changed={changed}{_testcases_suffix(testcases_version)}{reset}")


def report_node_score(
    node_id: str,
    score: float,
    *,
    feedback: str = "",
    testcases_version: int | None = None,
    phase: str = "score",
) -> None:
    fb = feedback if feedback != "" else str(score)
    timing = "before" if phase == "retest" else "after"
    hi, reset = _tty_hi("solver")
    print(
        f"{hi}  {node_id} score={score:.3f} timing={timing}"
        f"{_testcases_suffix(testcases_version)}{reset}"
    )
    event(
        "mcts.node.retest" if phase == "retest" else "mcts.node.score",
        node=node_id,
        score=score,
        feedback=fb,
        timing=timing,
        testcases_version=testcases_version,
    )


def report_retest(
    node_id: str,
    score: float,
    *,
    feedback: str = "",
    testcases_version: int | None = None,
) -> None:
    report_node_score(
        node_id,
        score,
        feedback=feedback,
        testcases_version=testcases_version,
        phase="retest",
    )


def report_rollout(node_id: str, result: object, harvested: dict[str, str], score: float) -> None:
    turns = result.turn_count or len(result.turns)
    tools = sum(len(t.tool_results) for t in result.turns) or len(result.tool_results)
    write(f"── {node_id}  summary  turns={turns} tools={tools}", stream="solver")
    for tag in TAGS:
        if tag in harvested:
            write(f"<{tag}>", stream="solver")
            write(_indent(harvested[tag]), stream="solver")
            write(f"</{tag}>", stream="solver")
    # Screen score is printed by report_node_score at score-ready time.

def report_author_step(node_id: str) -> None:
    write(f"── {node_id}  author step done", stream="author")


def write_memory_input(node_id: str, query: str, *, success: bool, errors: object = ()) -> None:
    """Record one solver → memory.add payload."""
    global _memory_round
    if not _log_memory:
        return
    with _lock:
        _memory_round += 1
        round_id = _memory_round
        _log_memory.write(f"=== memory INPUT round={round_id} node={node_id} success={success} ===\n")
        if errors:
            _log_memory.write(f"errors: {errors}\n")
        _log_memory.write(_indent(query or "(empty)") + "\n")
        _log_memory.write("\n")
        _log_memory.flush()


def write_memory_output(
    *,
    version: int,
    delta: str,
    raw_snapshot: str = "",
) -> None:
    """Record one memory.fetch → author delta (and optional raw snapshot)."""
    global _memory_round
    if not _log_memory:
        return
    with _lock:
        _memory_round += 1
        round_id = _memory_round
        _log_memory.write(f"=== memory OUTPUT round={round_id} version={version} ===\n")
        _log_memory.write("-- delta (fed to author) --\n")
        _log_memory.write(_indent(delta or "(empty)") + "\n")
        if raw_snapshot:
            _log_memory.write("-- raw fetch snapshot --\n")
            _log_memory.write(_indent(raw_snapshot) + "\n")
        _log_memory.write("\n")
        _log_memory.flush()


def write_author_sync(
    *,
    ok: bool,
    changed: bool,
    bytes_count: int,
    sha256: str,
    error: str = "",
    version: int = 0,
    testcases_version: int | None = None,
) -> None:
    """Record host sync outcome after an author step."""
    global _memory_round
    if not _log_memory:
        return
    with _lock:
        _memory_round += 1
        round_id = _memory_round
        tc = f" testcases=v{testcases_version}" if testcases_version is not None else ""
        _log_memory.write(
            f"=== memory SYNC round={round_id} version={version}{tc} "
            f"ok={ok} changed={changed} bytes={bytes_count} ===\n"
        )
        if sha256:
            _log_memory.write(f"sha256: {sha256}\n")
        if error:
            _log_memory.write(f"error: {error}\n")
        _log_memory.write("\n")
        _log_memory.flush()


def _indent(text: str, prefix: str = "    ") -> str:
    return "\n".join(f"{prefix}{line}" for line in text.splitlines())
