"""OpenCode 2.2.6 HTTP host adapter.

Step 2 implements private-repo root snapshots plus ``host.snapshot`` /
``host.fork`` (detached worktrees + session create/fork/move). Agent turn, exec scoring, delivery, and cleanup are implemented.
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from urllib import error as urllib_error
from urllib import request as urllib_request

from xgovernor_opencode_host.opencode_sse import (
    JobProgressMapper,
    OpenCodeEventHub,
    SESSION_PROGRESS_TYPES,
    open_http_event_stream,
)
from xgovernor_mcts.bridge.launch_spec import load_manifest, manifest_mode, resolve_profile_source
from xgovernor_tasks.generic.task import SANDBOX_REPO

EXPECTED_OPENCODE_VERSION = "2.2.6"
DEFAULT_OPENCODE_URL = "http://127.0.0.1:4096"
DEFAULT_TIMEOUT_S = 30.0
ROOT_COMMIT_MESSAGE = "opencode-mcts root workspace snapshot"
BRANCH_COMMIT_MESSAGE = "opencode-mcts branch snapshot"
EXPIRES_FAR = "2099-01-01T00:00:00Z"


class OpenCodeError(RuntimeError):
    """Raised when the OpenCode server is unreachable or rejects a request."""


def _sandbox_to_worktree(worktree: Path, path: str | None) -> Path:
    """Map E2B-style absolute sandbox paths onto a local OpenCode worktree.

    BenchmarkStrategy / scorers still speak ``/home/user/workspace/repo/...``.
    OpenCode is harness-only, so the adapter translates those paths instead of
    changing algorithm-layer path constants.
    """
    if path in {"", ".", None}:
        return worktree.resolve()
    raw = str(path)
    for bare in (
        "/home/user/workspace/repo",
        "/home/user/workspace",
    ):
        if raw == bare:
            return worktree.resolve()
        if raw.startswith(bare + "/"):
            return (worktree / raw[len(bare) + 1 :]).resolve()
    candidate = Path(raw)
    if candidate.is_absolute():
        raise OpenCodeError(f"path escapes worktree mapping: {path}")
    return (worktree / candidate).resolve()


def _ensure_under_worktree(worktree: Path, target: Path) -> Path:
    root = worktree.resolve()
    resolved = target.resolve()
    if resolved != root and root not in resolved.parents:
        raise OpenCodeError(f"path escapes worktree: {target}")
    return resolved




@dataclass(frozen=True)
class ServerStatus:
    """Normalized view of ``GET /api/info`` for readiness checks."""

    raw: dict[str, Any]
    version: str | None
    ready: bool
    url: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "version": self.version,
            "ready": self.ready,
            "raw": self.raw,
        }


@dataclass
class StateRecord:
    state_id: str
    commit: str
    frozen_session_id: str | None
    empty_history: bool
    is_root: bool = False
    frozen_export_path: str | None = None


@dataclass
class BranchRecord:
    branch_id: str
    parent_state_id: str
    commit: str
    worktree: Path
    session_id: str
    busy: bool = False


class SessionApi(Protocol):
    """Minimal OpenCode session surface used by snapshot/fork/turn/exec."""

    def create(self, *, directory: str, title: str | None = None) -> dict[str, Any]: ...

    def fork(
        self, session_id: str, *, before: str | None = None, directory: str | None = None
    ) -> dict[str, Any]: ...

    def move(self, session_id: str, *, directory: str) -> dict[str, Any]: ...

    def get(self, session_id: str) -> dict[str, Any]: ...

    def remove(self, session_id: str) -> None: ...

    def export(self, session_id: str) -> dict[str, Any]: ...

    def set_permissions(self, session_id: str, permissions: list[dict[str, Any]]) -> None: ...

    def switch_model(self, session_id: str, model: Mapping[str, str]) -> None: ...

    def prompt(
        self, session_id: str, text: str, *, metadata: Mapping[str, Any] | None = None
    ) -> dict[str, Any]: ...

    def wait(self, session_id: str, *, timeout_ms: int | None = None) -> None: ...

    def messages(self, session_id: str, *, limit: int = 50) -> list[dict[str, Any]]: ...

    def context(self, session_id: str) -> list[dict[str, Any]]: ...

    def shell(self, session_id: str, command: str) -> None: ...


def _basic_auth_header(password: str) -> str:
    # OpenCode HTTP Basic uses username ``opencode`` + the server password.
    token = base64.b64encode(f"opencode:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _service_json_password() -> str | None:
    config = Path.home() / ".config" / "opencode" / "service.json"
    if not config.is_file():
        return None
    try:
        payload = json.loads(config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    password = payload.get("password")
    return password if isinstance(password, str) and password else None


def _load_password(explicit: str | None) -> str | None:
    """Resolve OpenCode password without logging its value.

    Prefer the local ``opencode service`` password in
    ``~/.config/opencode/service.json`` when present (it matches the running
    background server). Fall back to ``OPENCODE_PASSWORD``.
    """
    if explicit:
        return explicit
    service_password = _service_json_password()
    if service_password:
        return service_password
    return os.environ.get("OPENCODE_PASSWORD") or None


def _parse_model_ref(model: str | None) -> dict[str, str] | None:
    if not model:
        return None
    value = model.strip()
    if not value:
        return None
    if "/" in value:
        provider, model_id = value.split("/", 1)
        provider, model_id = provider.strip(), model_id.strip()
        if provider and model_id:
            return {"providerID": provider, "id": model_id}
    return {"providerID": "opencode", "id": value}


def _permission_rule(action: str, effect: str, resource: str = "*") -> dict[str, str]:
    """OpenCode Permission.Rule: action is the tool name, effect is allow/deny/ask."""
    return {"action": action, "resource": resource, "effect": effect}


FILE_TOOL_PERMISSIONS: list[dict[str, str]] = [
    _permission_rule("read", "allow"),
    _permission_rule("edit", "allow"),
    _permission_rule("write", "allow"),
    _permission_rule("grep", "allow"),
    _permission_rule("glob", "allow"),
    _permission_rule("list", "allow"),
    _permission_rule("bash", "allow"),
    _permission_rule("shell", "allow"),
    _permission_rule("task", "deny"),
    _permission_rule("question", "deny"),
    _permission_rule("ask", "deny"),
    _permission_rule("external_directory", "allow"),
    _permission_rule("webfetch", "deny"),
    _permission_rule("websearch", "deny"),
    _permission_rule("skill", "deny"),
    _permission_rule("command", "deny"),
]

_DISABLED_TOOL_NAMES = (
    "*",
    "read",
    "edit",
    "write",
    "grep",
    "glob",
    "list",
    "bash",
    "shell",
    "task",
    "question",
    "ask",
    "external_directory",
    "webfetch",
    "websearch",
    "skill",
    "command",
    "todowrite",
    "todoread",
)
TOOLS_DISABLED_PERMISSIONS: list[dict[str, str]] = [
    _permission_rule(name, "deny") for name in _DISABLED_TOOL_NAMES
]


@dataclass(frozen=True)
class AgentProfile:
    """Host-side resolution of a plugin manifest profile."""

    name: str
    prompt: str = ""
    tools_enabled: bool = True
    output_mode: str | None = None
    prompt_metadata: Mapping[str, Any] | None = None


def load_agent_profiles(
    mode: str = "sweverified", *, prompt_repo: str = SANDBOX_REPO
) -> dict[str, AgentProfile]:
    """Load prompt text and tool flags for every profile in ``mode``."""
    manifest = load_manifest()
    mode_config = manifest_mode(manifest, mode)
    loaded: dict[str, AgentProfile] = {}
    for name, config in (mode_config.get("profiles") or {}).items():
        if not isinstance(config, dict):
            continue
        source = resolve_profile_source(mode, name, manifest=manifest)
        prompt = source.read_text(encoding="utf-8") if source.is_file() else ""
        output_mode = config.get("output_mode")
        loaded[name] = AgentProfile(
            name=name,
            prompt=prompt.replace(SANDBOX_REPO, prompt_repo),
            tools_enabled=config.get("tools_enabled", True) is not False,
            output_mode=output_mode if isinstance(output_mode, str) else None,
            prompt_metadata=config.get("prompt_metadata"),
        )
    return loaded


def compose_profile_prompt(text: str, profile: AgentProfile | None) -> str:
    if profile is None or not profile.prompt.strip():
        return text
    return f"{profile.prompt.rstrip()}\n\n{text}"


def _message_ids(messages: list[dict[str, Any]]) -> set[str]:
    return {str(item["id"]) for item in messages if isinstance(item, dict) and item.get("id")}


def _new_messages(
    messages: list[dict[str, Any]], before_ids: set[str]
) -> list[dict[str, Any]]:
    """Messages that appeared after a prompt, ignoring inherited history."""
    return [
        item
        for item in messages
        if isinstance(item, dict) and str(item.get("id") or "") not in before_ids
    ]


def _extract_version(payload: Mapping[str, Any]) -> str | None:
    for key in ("version", "serverVersion", "server_version", "opencodeVersion"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lstrip("v")
    for nested_key in ("server", "info"):
        nested = payload.get(nested_key)
        if isinstance(nested, Mapping):
            found = _extract_version(nested)
            if found:
                return found
    return None


def _extract_ready(payload: Mapping[str, Any]) -> bool:
    for key in ("ready", "healthy", "ok"):
        value = payload.get(key)
        if isinstance(value, bool):
            return value
    status = payload.get("status")
    if isinstance(status, str):
        return status.strip().lower() in {"ready", "ok", "healthy", "running"}
    return True


def _unwrap_session_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    """OpenCode often wraps resources as ``{"data": {...}}``."""
    data = payload.get("data")
    if isinstance(data, Mapping) and (
        "id" in data or "sessionID" in data or "location" in data or "directory" in data
    ):
        return data
    return payload


def _session_id_of(payload: Mapping[str, Any]) -> str:
    payload = _unwrap_session_payload(payload)
    for key in ("id", "sessionID", "session_id"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    session = payload.get("session")
    if isinstance(session, Mapping):
        return _session_id_of(session)
    raise OpenCodeError(f"session response missing id: {payload!r}")


def _session_directory_of(payload: Mapping[str, Any]) -> str | None:
    payload = _unwrap_session_payload(payload)
    location = payload.get("location")
    if isinstance(location, Mapping):
        directory = location.get("directory")
        if isinstance(directory, str) and directory:
            return directory
    for key in ("directory", "dir", "path"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    session = payload.get("session")
    if isinstance(session, Mapping):
        return _session_directory_of(session)
    return None


def _git(
    repo: Path,
    args: list[str],
    *,
    check: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        text=True,
        capture_output=True,
        input=input_text,
    )
    if check and completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise OpenCodeError(f"git {' '.join(args)} failed in {repo}: {detail}")
    return completed


def _git_out(repo: Path, args: list[str]) -> str:
    return _git(repo, args).stdout


def _nul_split(text: str) -> list[str]:
    return [part for part in text.split("\0") if part]


def _reject_merge_conflicts(source: Path) -> None:
    unmerged = _git_out(source, ["ls-files", "-u"]).strip()
    if unmerged:
        raise OpenCodeError(f"source repo has merge conflicts: {source}")


def _reject_submodules(source: Path) -> None:
    stage = _git_out(source, ["ls-files", "--stage"])
    for line in stage.splitlines():
        # submodule mode
        if line.startswith("160000 "):
            raise OpenCodeError(f"source repo has git submodules: {source}")
    gitmodules = source / ".gitmodules"
    if gitmodules.is_file() and gitmodules.stat().st_size > 0:
        status = _git(source, ["submodule", "status"], check=False)
        if status.returncode == 0 and status.stdout.strip():
            raise OpenCodeError(f"source repo has git submodules: {source}")


def _copy_path(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        if dst.is_dir() and not dst.is_symlink():
            shutil.rmtree(dst)
        else:
            dst.unlink()
    if src.is_symlink():
        dst.symlink_to(os.readlink(src))
    elif src.is_file():
        shutil.copy2(src, dst)
    else:
        raise OpenCodeError(f"unsupported workspace path type: {src}")


def _materialize_root_commit(
    source: Path, private: Path, *, gitlinks: frozenset[str]
) -> str:
    """Overlay the source working tree onto the private clone and commit it."""
    tracked = _nul_split(_git_out(source, ["ls-files", "-z"]))
    untracked = _nul_split(
        _git_out(source, ["ls-files", "--others", "--exclude-standard", "-z"])
    )

    # Drop files present in the clean clone checkout but deleted in the source WT.
    for rel in tracked:
        if rel in gitlinks:
            continue
        src = source / rel
        dst = private / rel
        if not src.exists() and not src.is_symlink():
            if dst.exists() or dst.is_symlink():
                if dst.is_dir() and not dst.is_symlink():
                    shutil.rmtree(dst)
                else:
                    dst.unlink()
            continue
        _copy_path(src, dst)

    for rel in untracked:
        src = source / rel
        if src.exists() or src.is_symlink():
            _copy_path(src, private / rel)

    _git(private, ["add", "-A"])
    env_commit = [
        "-c",
        "user.name=opencode-mcts",
        "-c",
        "user.email=opencode-mcts@local",
        "commit",
        "--allow-empty",
        "-m",
        ROOT_COMMIT_MESSAGE,
    ]
    _git(private, env_commit)
    return _git_out(private, ["rev-parse", "HEAD"]).strip()


class HttpSessionApi:
    """OpenCode HTTP session client used by snapshot/fork/turn/exec."""

    def __init__(self, adapter: "OpenCodeAdapter") -> None:
        self._adapter = adapter

    def create(self, *, directory: str, title: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"location": {"directory": str(Path(directory).resolve())}}
        if title:
            body["title"] = title
        return self._adapter.request_json("POST", "/api/session", body)

    def fork(
        self, session_id: str, *, before: str | None = None, directory: str | None = None
    ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if before is not None:
            body["before"] = before
        forked = self._adapter.request_json("POST", f"/api/session/{session_id}/fork", body)
        if directory is None:
            return forked

        child_id = _session_id_of(forked)
        directory = str(Path(directory).resolve())
        if _session_directory_of(forked) == directory:
            return forked

        try:
            # A queued move can start execution against the old worktree.
            # Reimport the new fork at its final location before its first turn.
            exported = self.export(child_id)
            data = exported.get("data", exported)
            if not isinstance(data, Mapping):
                raise OpenCodeError(f"invalid export for forked session {child_id}")
            info = data.get("info")
            messages = data.get("messages")
            if (
                not isinstance(info, Mapping)
                or info.get("id") != child_id
                or not isinstance(messages, list)
            ):
                raise OpenCodeError(f"invalid export for forked session {child_id}")
            self.remove(child_id)
            return self._adapter.request_json(
                "POST", "/api/experimental/session/import",
                {"info": dict(info), "messages": messages, "location": {"directory": directory}},
            )
        except Exception:
            try:
                self.remove(child_id)
            except OpenCodeError:
                pass
            raise

    def move(self, session_id: str, *, directory: str) -> dict[str, Any]:
        path = f"/api/session/{session_id}/move"
        body = {"directory": str(Path(directory).resolve())}
        try:
            return self._adapter.request_json("POST", path, body)
        except OpenCodeError as exc:
            message = str(exc)
            if "HTTP 400" not in message or "Missing key" not in message:
                raise
            return self._adapter.request_json("POST", path, {"location": body})

    def get(self, session_id: str) -> dict[str, Any]:
        return self._adapter.request_json("GET", f"/api/session/{session_id}")

    def remove(self, session_id: str) -> None:
        self._adapter.request_json("DELETE", f"/api/session/{session_id}")

    def export(self, session_id: str) -> dict[str, Any]:
        return self._adapter.request_json(
            "GET", f"/api/experimental/session/{session_id}/export"
        )

    def set_permissions(self, session_id: str, permissions: list[dict[str, Any]]) -> None:
        self._adapter.request_json(
            "PATCH", f"/api/session/{session_id}", {"permissions": permissions}
        )

    def switch_model(self, session_id: str, model: Mapping[str, str]) -> None:
        self._adapter.request_json(
            "POST", f"/api/session/{session_id}/model", {"model": dict(model)}
        )

    def prompt(
        self, session_id: str, text: str, *, metadata: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"text": text}
        if metadata is not None:
            body["metadata"] = dict(metadata)
        return self._adapter.request_json("POST", f"/api/session/{session_id}/prompt", body)

    def wait(self, session_id: str, *, timeout_ms: int | None = None) -> None:
        # OpenCode 2.2.6 declares an empty wait body; do not send timeout fields.
        # The previous hard-coded client timeout (DEFAULT_TIMEOUT_S=30) aborted
        # long-polling waits at 30s and made short turns look like they "hung"
        # for a full slice. Align the HTTP client timeout with timeout_ms.
        if timeout_ms is None:
            client_timeout_s = max(float(self._adapter.timeout_s), 30.0)
        else:
            # Small slack so a wait that returns at the deadline is not cut by
            # urllib first; floor keeps tiny unit-test budgets workable.
            client_timeout_s = max(1.0, float(timeout_ms) / 1000.0 + 5.0)
        self._adapter.request_json(
            "POST",
            f"/api/experimental/session/{session_id}/wait",
            None,
            timeout_s=client_timeout_s,
        )

    def messages(self, session_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        payload = self._adapter.request_json(
            "GET", f"/api/session/{session_id}/message?limit={int(limit)}"
        )
        data = payload.get("data", payload)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def context(self, session_id: str) -> list[dict[str, Any]]:
        payload = self._adapter.request_json("GET", f"/api/session/{session_id}/context")
        data = payload.get("data", payload)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

    def shell(self, session_id: str, command: str) -> None:
        self._adapter.request_json(
            "POST", f"/api/session/{session_id}/shell", {"command": command}
        )


@dataclass
class FakeSessionRecord:
    session_id: str
    directory: str
    messages: list[dict[str, Any]] = field(default_factory=list)
    parent_id: str | None = None
    removed: bool = False
    model: dict[str, str] | None = None
    permissions: list[dict[str, Any]] = field(default_factory=list)


class FakeSessionApi:
    """In-memory OpenCode session API for unit tests (no live server)."""

    def __init__(self, *, assistant_reply: str = "mock assistant reply") -> None:
        self.sessions: dict[str, FakeSessionRecord] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.assistant_reply = assistant_reply
        self.shell_exit_code = 0
        self.shell_output = "SCORE_OK\n"
        self.prompt_should_fail = False
        self.prompt_failure_message = "provider.free-limit"
        self.on_wait = None
        self._n = 0
        self._msg_n = 0

    def _new_id(self) -> str:
        self._n += 1
        return f"ses_fake_{self._n}"

    def _new_msg(self) -> str:
        self._msg_n += 1
        return f"msg_fake_{self._msg_n}"

    def _require(self, session_id: str) -> FakeSessionRecord:
        session = self.sessions.get(session_id)
        if session is None or session.removed:
            raise OpenCodeError(f"unknown session: {session_id}")
        return session

    def create(self, *, directory: str, title: str | None = None) -> dict[str, Any]:
        self.calls.append(("create", {"directory": directory, "title": title}))
        session_id = self._new_id()
        self.sessions[session_id] = FakeSessionRecord(
            session_id=session_id, directory=str(Path(directory).resolve())
        )
        return {
            "id": session_id,
            "title": title,
            "location": {"directory": str(Path(directory).resolve())},
        }

    def fork(
        self, session_id: str, *, before: str | None = None, directory: str | None = None
    ) -> dict[str, Any]:
        self.calls.append(("fork", {"session_id": session_id, "before": before}))
        parent = self._require(session_id)
        if not parent.messages:
            raise OpenCodeError(f"ForkEmptyError: session {session_id} has empty history")
        new_id = self._new_id()
        self.sessions[new_id] = FakeSessionRecord(
            session_id=new_id,
            directory=parent.directory,
            messages=[dict(item) for item in parent.messages],
            parent_id=session_id,
            model=dict(parent.model) if parent.model else None,
            permissions=[dict(item) for item in parent.permissions],
        )
        if directory is not None:
            self.move(new_id, directory=directory)
        return {
            "id": new_id,
            "location": {"directory": self.sessions[new_id].directory},
            "parentID": session_id,
        }

    def move(self, session_id: str, *, directory: str) -> dict[str, Any]:
        self.calls.append(("move", {"session_id": session_id, "directory": directory}))
        session = self._require(session_id)
        session.directory = str(Path(directory).resolve())
        return {"id": session_id, "location": {"directory": session.directory}}

    def get(self, session_id: str) -> dict[str, Any]:
        self.calls.append(("get", {"session_id": session_id}))
        session = self._require(session_id)
        return {
            "id": session_id,
            "location": {"directory": session.directory},
            "tokens": {"input": 1, "output": 2, "reasoning": 0, "cache": {"read": 0, "write": 0}},
            "message_count": len(session.messages),
        }

    def remove(self, session_id: str) -> None:
        self.calls.append(("remove", {"session_id": session_id}))
        session = self.sessions.get(session_id)
        if session is None:
            return
        session.removed = True

    def export(self, session_id: str) -> dict[str, Any]:
        self.calls.append(("export", {"session_id": session_id}))
        session = self._require(session_id)
        return {
            "id": session_id,
            "directory": session.directory,
            "messages": [dict(item) for item in session.messages],
        }

    def set_permissions(self, session_id: str, permissions: list[dict[str, Any]]) -> None:
        self.calls.append(("set_permissions", {"session_id": session_id, "permissions": permissions}))
        session = self._require(session_id)
        session.permissions = [dict(item) for item in permissions]

    def switch_model(self, session_id: str, model: Mapping[str, str]) -> None:
        self.calls.append(("switch_model", {"session_id": session_id, "model": dict(model)}))
        session = self._require(session_id)
        session.model = dict(model)

    def prompt(
        self, session_id: str, text: str, *, metadata: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        request = {"session_id": session_id, "text": text}
        if metadata is not None:
            request["metadata"] = dict(metadata)
        self.calls.append(("prompt", request))
        session = self._require(session_id)
        user_id = self._new_msg()
        session.messages.append({"id": user_id, "type": "user", "text": text})
        if self.prompt_should_fail:
            session.messages.append(
                {
                    "id": self._new_msg(),
                    "type": "assistant",
                    "content": [],
                    "finish": "error",
                    "error": {"type": "provider.free-limit", "message": self.prompt_failure_message},
                }
            )
            session.messages.append(
                {
                    "id": self._new_msg(),
                    "type": "idle",
                    "outcome": "failed",
                    "error": {"type": "provider.free-limit", "message": self.prompt_failure_message},
                }
            )
        else:
            session.messages.append(
                {
                    "id": self._new_msg(),
                    "type": "assistant",
                    "content": [{"type": "text", "text": self.assistant_reply}],
                    "text": self.assistant_reply,
                    "finish": "stop",
                }
            )
            session.messages.append({"id": self._new_msg(), "type": "idle", "outcome": "success"})
        return {"id": user_id, "type": "user", "payload": {"text": text}}

    def wait(self, session_id: str, *, timeout_ms: int | None = None) -> None:
        self.calls.append(
            ("wait", {"session_id": session_id, "timeout_ms": timeout_ms})
        )
        self._require(session_id)
        if self.on_wait is not None:
            self.on_wait(session_id)

    def messages(self, session_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
        self.calls.append(("messages", {"session_id": session_id, "limit": limit}))
        session = self._require(session_id)
        items = list(reversed(session.messages))
        return items[:limit]

    def context(self, session_id: str) -> list[dict[str, Any]]:
        self.calls.append(("context", {"session_id": session_id}))
        session = self._require(session_id)
        return [dict(item) for item in session.messages]

    def shell(self, session_id: str, command: str) -> None:
        self.calls.append(("shell", {"session_id": session_id, "command": command}))
        session = self._require(session_id)
        # Run command locally for realistic exit codes when possible.
        exit_code = self.shell_exit_code
        output = self.shell_output
        try:
            completed = subprocess.run(
                command,
                shell=True,
                cwd=session.directory,
                capture_output=True,
                text=True,
                timeout=30,
            )
            exit_code = int(completed.returncode)
            output = (completed.stdout or "") + (completed.stderr or "")
        except Exception as exc:  # noqa: BLE001
            exit_code = 1
            output = str(exc)
        session.messages.append(
            {
                "id": self._new_msg(),
                "type": "shell",
                "command": command,
                "status": "exited",
                "exit": exit_code,
                "output": {"output": output, "truncated": False},
            }
        )

    def append_message(self, session_id: str, text: str = "turn") -> None:
        """Test helper: give a session non-empty history so fork succeeds."""
        session = self.sessions[session_id]
        session.messages.append({"id": self._new_msg(), "type": "user", "text": text})


class OpenCodeAdapter:
    """HTTP client + host capabilities for the local OpenCode MCTS harness."""

    session_permissions = FILE_TOOL_PERMISSIONS

    def __init__(
        self,
        *,
        base_url: str | None = None,
        password: str | None = None,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        expected_version: str = EXPECTED_OPENCODE_VERSION,
        session_api: SessionApi | None = None,
        model: str | None = None,
        profiles: Mapping[str, AgentProfile] | None = None,
    ) -> None:
        self.base_url = (base_url or DEFAULT_OPENCODE_URL).rstrip("/")
        self.password = _load_password(password)
        self.timeout_s = timeout_s
        self.expected_version = expected_version.lstrip("v")
        self.sessions: SessionApi = session_api or HttpSessionApi(self)
        self.model = model
        self.model_ref = _parse_model_ref(model)
        self.profiles = dict(profiles or {})
        self.job_emitter: Callable[[str, dict[str, Any]], None] | None = None
        # job_id -> {"run_id", "cancelled": Event, "terminal": bool}
        self._active_jobs: dict[str, dict[str, Any]] = {}

        self.output_dir: Path | None = None
        self.source_repo: Path | None = None
        self.private_repo: Path | None = None
        self.root_state_id: str | None = None
        self.root_commit: str | None = None
        self.states: dict[str, StateRecord] = {}
        self.branches: dict[str, BranchRecord] = {}
        self._frozen_session_ids: set[str] = set()
        self.delivered: dict[str, Any] | None = None
        self._idempotency: dict[tuple[str, str, str], Any] = {}
        self._session_turn_locks: dict[str, threading.Lock] = {}
        self._lock = threading.RLock()
        self._branch_locks: dict[str, threading.RLock] = {}
        self._counter = 0
        self.events = OpenCodeEventHub()

    # --- HTTP -----------------------------------------------------------------

    def _headers(self, *, content_type: str | None = "application/json") -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        if self.password:
            headers["Authorization"] = _basic_auth_header(self.password)
        return headers

    def request_json(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, Any]:
        url = self.base_url + "/" + path.lstrip("/")
        data = None
        headers = self._headers(
            content_type="application/json" if body is not None else None
        )
        if body is not None:
            data = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode(
                "utf-8"
            )
            headers["Content-Type"] = "application/json"
        request = urllib_request.Request(
            url, data=data, headers=headers, method=method.upper()
        )
        call_timeout = self.timeout_s if timeout_s is None else float(timeout_s)
        try:
            with urllib_request.urlopen(request, timeout=call_timeout) as response:
                raw = response.read().decode("utf-8")
                if not raw.strip():
                    return {}
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise OpenCodeError(
                        f"OpenCode {method.upper()} {path} returned non-JSON body"
                    ) from exc
                if isinstance(parsed, dict):
                    return parsed
                return {"value": parsed}
        except urllib_error.HTTPError as error:
            raw = error.read().decode("utf-8", errors="replace")
            detail = raw.strip() or error.reason
            raise OpenCodeError(
                f"OpenCode {method.upper()} {path} failed with HTTP {error.code}: {detail}"
            ) from error
        except urllib_error.URLError as error:
            raise OpenCodeError(
                f"could not reach OpenCode at {url}: {error.reason}"
            ) from error

    def get_server_info(self) -> dict[str, Any]:
        return self.request_json("GET", "/api/info")

    def check_ready(self) -> ServerStatus:
        info = self.get_server_info()
        version = _extract_version(info)
        ready = _extract_ready(info)
        status = ServerStatus(raw=info, version=version, ready=ready, url=self.base_url)
        if not ready:
            raise OpenCodeError(
                f"OpenCode at {self.base_url} reported not ready: "
                f"{json.dumps(info, ensure_ascii=False)}"
            )
        if version is not None and version != self.expected_version:
            raise OpenCodeError(
                f"OpenCode version mismatch at {self.base_url}: "
                f"expected {self.expected_version}, got {version}"
            )
        return status

    # --- identity helpers -----------------------------------------------------

    def _next(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}-{self._counter}-{uuid.uuid4().hex[:8]}"

    def _idempotent(self, run_id: str | None, method: str, key: str, producer):
        cache_key = (run_id or "", method, key)
        with self._lock:
            if cache_key in self._idempotency:
                return self._idempotency[cache_key]
        value = producer()
        with self._lock:
            prior = self._idempotency.get(cache_key)
            if prior is not None:
                return prior
            self._idempotency[cache_key] = value
            return value

    def _branch_lock(self, branch_id: str) -> threading.RLock:
        with self._lock:
            lock = self._branch_locks.get(branch_id)
            if lock is None:
                lock = threading.RLock()
                self._branch_locks[branch_id] = lock
            return lock

    def _require_private(self) -> Path:
        if self.private_repo is None or self.output_dir is None:
            raise OpenCodeError("create_root_snapshot must be called before host.*")
        return self.private_repo

    def _worktree_path(self, branch_id: str) -> Path:
        assert self.output_dir is not None
        return self.output_dir / "worktrees" / branch_id

    def _frozen_dir(self) -> Path:
        assert self.output_dir is not None
        path = self.output_dir / "frozen_sessions"
        path.mkdir(parents=True, exist_ok=True)
        return path

    # --- root snapshot --------------------------------------------------------

    def _root_gitlinks(self, source: Path) -> frozenset[str]:
        _reject_submodules(source)
        return frozenset()

    def _prepare_worktree(self, worktree: Path) -> None:
        """Prepare dependencies before opening a session in a new worktree."""

    def _validate_snapshot_workspace(self, worktree: Path) -> None:
        """Check mode-specific constraints before persisting the worktree."""

    def create_root_snapshot(self, *, repo: str, output_dir: str) -> str:
        """Build a private Git mirror of ``repo``'s workspace under ``output_dir``.

        Preserves source history via ``git clone --local``, overlays tracked and
        non-ignored untracked workspace files (including modifications and
        deletions), and creates an explicit root commit. Returns ``state_id``.
        """
        source = Path(repo).expanduser().resolve()
        out = Path(output_dir).expanduser().resolve()
        if not source.is_dir():
            raise OpenCodeError(f"repo is not a directory: {source}")
        if not (source / ".git").exists():
            # Allow worktree sources: git rev-parse --git-dir
            probe = subprocess.run(
                ["git", "-C", str(source), "rev-parse", "--git-dir"],
                capture_output=True,
                text=True,
            )
            if probe.returncode != 0:
                raise OpenCodeError(f"repo is not a git working tree: {source}")

        out.mkdir(parents=True, exist_ok=True)
        private = out / "private"
        if private.exists():
            raise OpenCodeError(f"private repo already exists: {private}")

        _reject_merge_conflicts(source)
        gitlinks = self._root_gitlinks(source)

        clone = subprocess.run(
            ["git", "clone", "--local", str(source), str(private)],
            capture_output=True,
            text=True,
        )
        if clone.returncode != 0:
            raise OpenCodeError(
                f"git clone --local failed: {(clone.stderr or clone.stdout).strip()}"
            )

        commit = _materialize_root_commit(source, private, gitlinks=gitlinks)
        state_id = "state-root"
        with self._lock:
            self.source_repo = source
            self.output_dir = out
            self.private_repo = private
            self.root_state_id = state_id
            self.root_commit = commit
            self.states[state_id] = StateRecord(
                state_id=state_id,
                commit=commit,
                frozen_session_id=None,
                empty_history=True,
                is_root=True,
            )
        self.write_run_record()
        return state_id

    # --- host.snapshot / host.fork / host.release -----------------------------

    def snapshot(
        self,
        branch_id: str,
        *,
        timeout_ms: int,
        idempotency_key: str,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        del timeout_ms, plugin_id  # reserved for later deadlines / auth scope

        def produce() -> dict[str, Any]:
            private = self._require_private()
            with self._branch_lock(branch_id):
                with self._lock:
                    branch = self.branches.get(branch_id)
                    if branch is None:
                        raise OpenCodeError(f"unknown branch: {branch_id}")
                    if branch.busy:
                        raise OpenCodeError(f"branch busy: {branch_id}")
                    branch.busy = True
                try:
                    _git(
                        branch.worktree,
                        ["add", "-A"],
                    )
                    self._validate_snapshot_workspace(branch.worktree)
                    _git(
                        branch.worktree,
                        [
                            "-c",
                            "user.name=opencode-mcts",
                            "-c",
                            "user.email=opencode-mcts@local",
                            "commit",
                            "--allow-empty",
                            "-m",
                            BRANCH_COMMIT_MESSAGE,
                        ],
                    )
                    commit = _git_out(branch.worktree, ["rev-parse", "HEAD"]).strip()
                    frozen_session_id, empty_history, export_path = self._freeze_session(
                        branch.session_id
                    )
                    state_id = self._next("state")
                    with self._lock:
                        self.states[state_id] = StateRecord(
                            state_id=state_id,
                            commit=commit,
                            frozen_session_id=frozen_session_id,
                            empty_history=empty_history,
                            is_root=False,
                            frozen_export_path=export_path,
                        )
                        branch.commit = commit
                    self.write_run_record()
                    return {"state_id": state_id, "expires_at": EXPIRES_FAR}
                finally:
                    with self._lock:
                        branch.busy = False

        return self._idempotent(run_id, "host.snapshot", idempotency_key, produce)

    def _freeze_session(self, session_id: str) -> tuple[str | None, bool, str | None]:
        """Freeze a session copy for later fork-from-snapshot.

        Prefer forking a dedicated frozen session (non-empty history). Empty
        history cannot be forked on OpenCode 2.2.6, so we mark ``empty_history``
        and leave ``frozen_session_id`` unset; later forks use create instead.
        """
        export_path: str | None = None
        try:
            exported = self.sessions.export(session_id)
            path = self._frozen_dir() / f"{session_id}.json"
            path.write_text(
                json.dumps(exported, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            export_path = str(path)
        except OpenCodeError:
            export_path = None

        try:
            frozen = self.sessions.fork(session_id)
            frozen_id = _session_id_of(frozen)
            with self._lock:
                self._frozen_session_ids.add(frozen_id)
            return frozen_id, False, export_path
        except OpenCodeError as exc:
            message = str(exc).lower()
            if "forkempty" in message or "empty" in message or "http 400" in message:
                return None, True, export_path
            # Export-only freeze: keep referencing the live session id as frozen
            # marker only when fork is unsupported for another reason.
            with self._lock:
                self._frozen_session_ids.add(session_id)
            return session_id, False, export_path

    def fork(
        self,
        parent_state_id: str,
        *,
        timeout_ms: int,
        idempotency_key: str,
        plugin_id: str | None = None,
        run_id: str | None = None,
        offline: bool = False,
        inherit_history: bool = True,
    ) -> dict[str, Any]:
        del timeout_ms, plugin_id, offline

        def produce() -> dict[str, Any]:
            private = self._require_private()
            with self._lock:
                state = self.states.get(parent_state_id)
                if state is None:
                    raise OpenCodeError(f"unknown state: {parent_state_id}")
                state_view = StateRecord(
                    state_id=state.state_id,
                    commit=state.commit,
                    frozen_session_id=state.frozen_session_id,
                    empty_history=state.empty_history,
                    is_root=state.is_root,
                    frozen_export_path=state.frozen_export_path,
                )
                branch_id = self._next("branch")
                worktree = self._worktree_path(branch_id)

            worktree.parent.mkdir(parents=True, exist_ok=True)
            if worktree.exists():
                raise OpenCodeError(f"worktree path already exists: {worktree}")
            _git(
                private,
                ["worktree", "add", "--detach", str(worktree), state_view.commit],
            )

            session_id = None
            try:
                self._prepare_worktree(worktree)
                session_payload = self._session_for_state(
                    state_view, worktree, inherit_history=inherit_history
                )
                session_id = _session_id_of(session_payload)
                self._ensure_session_directory(session_id, worktree)
                self._configure_session(session_id)
            except Exception:
                if session_id is not None:
                    try:
                        self.sessions.remove(session_id)
                    except OpenCodeError:
                        pass
                self._remove_worktree(worktree)
                raise

            with self._lock:
                self.branches[branch_id] = BranchRecord(
                    branch_id=branch_id,
                    parent_state_id=parent_state_id,
                    commit=state_view.commit,
                    worktree=worktree,
                    session_id=session_id,
                )
            self.write_run_record()
            return {"branch_id": branch_id}

        return self._idempotent(run_id, "host.fork", idempotency_key, produce)

    def _session_for_state(
        self, state: StateRecord, worktree: Path, *, inherit_history: bool
    ) -> dict[str, Any]:
        directory = str(worktree.resolve())
        if not inherit_history or state.is_root or state.empty_history or not state.frozen_session_id:
            return self.sessions.create(directory=directory, title=f"mcts-{state.state_id}")
        return self.sessions.fork(state.frozen_session_id, directory=directory)

    def _configure_session(self, session_id: str) -> None:
        """Restrict tools to file ops; optionally pin the CLI model."""
        try:
            self.sessions.set_permissions(session_id, self.session_permissions)
        except OpenCodeError as exc:
            # Permissions are best-effort on older servers; keep going.
            print(f"[opencode_adapter] set_permissions skipped: {exc}")
        if self.model_ref is not None:
            try:
                self.sessions.switch_model(session_id, self.model_ref)
            except OpenCodeError as exc:
                print(f"[opencode_adapter] switch_model skipped: {exc}")

    def _ensure_session_directory(self, session_id: str, worktree: Path) -> None:
        directory = _session_directory_of(self.sessions.get(session_id))
        expected = worktree.resolve()
        if directory is None or Path(directory).resolve() != expected:
            raise OpenCodeError(
                f"session {session_id} directory mismatch: {directory!r} != {str(expected)!r}"
            )

    def _remove_worktree(self, worktree: Path) -> None:
        private = self._require_private()
        removed = _git(private, ["worktree", "remove", "--force", str(worktree)], check=False)
        if removed.returncode != 0 and worktree.exists():
            shutil.rmtree(worktree, ignore_errors=True)
            _git(private, ["worktree", "prune"], check=False)

    def release(
        self,
        branch_id: str,
        *,
        timeout_ms: int,
        idempotency_key: str,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> None:
        """Minimal release: drop the live worktree/session; keep frozen snapshots."""
        del timeout_ms, plugin_id

        def produce() -> dict[str, Any]:
            with self._branch_lock(branch_id):
                with self._lock:
                    branch = self.branches.get(branch_id)
                    if branch is None:
                        return {"released": True}
                    if branch.busy:
                        raise OpenCodeError(f"branch busy: {branch_id}")
                    session_id = branch.session_id
                    worktree = branch.worktree
                    del self.branches[branch_id]

                keep_session = session_id in self._frozen_session_ids
                if not keep_session:
                    try:
                        self.sessions.remove(session_id)
                    except OpenCodeError:
                        pass
                self._remove_worktree(worktree)
                return {"released": True}

        self._idempotent(run_id, "host.release", idempotency_key, produce)

    # --- still later ----------------------------------------------------------

    def start_event_stream(self, *, wait_s: float = 5.0) -> bool:
        """Start the process-wide OpenCode SSE listener when a live server is used."""
        if self._opener_is_live():
            self.events.stop()
            self.events = OpenCodeEventHub(opener=self._open_event_stream, reconnect=True)
        return self.events.start(wait_s=wait_s)

    def stop_event_stream(self) -> None:
        self.events.stop()

    def _opener_is_live(self) -> bool:
        return not isinstance(self.sessions, FakeSessionApi)

    def _open_event_stream(self, last_event_id: str | None = None):
        headers = self._headers(content_type=None)
        return open_http_event_stream(
            base_url=self.base_url,
            headers=headers,
            last_event_id=last_event_id,
            types=SESSION_PROGRESS_TYPES,
        )

    def _on_job_progress(self, mapper: JobProgressMapper, job_id: str, event: dict[str, Any]) -> None:
        if self._job_cancelled(job_id):
            return
        with self._lock:
            job = self._active_jobs.get(job_id)
            if job is None or job["terminal"]:
                return
        params = mapper.apply(event)
        if params is None:
            return
        if self._job_cancelled(job_id):
            return
        with self._lock:
            job = self._active_jobs.get(job_id)
            if job is None or job["terminal"]:
                return
        self._emit_job("host.job.progress", params)

    def _emit_job(self, method: str, params: dict[str, Any]) -> None:
        emitter = self.job_emitter
        if emitter is None:
            raise OpenCodeError(
                f"job_emitter is not configured; cannot emit {method}"
            )
        emitter(method, params)

    def _claim_job_terminal(self, job_id: str) -> bool:
        """Return True once for the first terminal notification of a job."""
        with self._lock:
            job = self._active_jobs.get(job_id)
            if job is None:
                return True
            if job["terminal"]:
                return False
            job["terminal"] = True
            return True

    def _job_cancelled(self, job_id: str) -> bool:
        with self._lock:
            job = self._active_jobs.get(job_id)
            return bool(job and job["cancelled"].is_set())

    def _session_turn_lock(self, session_id: str) -> threading.Lock:
        with self._lock:
            lock = self._session_turn_locks.get(session_id)
            if lock is None:
                lock = threading.Lock()
                self._session_turn_locks[session_id] = lock
            return lock

    def _assistant_text(self, messages: list[dict[str, Any]]) -> str:
        for item in messages:
            if item.get("type") != "assistant":
                continue
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                return text
            content = item.get("content")
            if isinstance(content, list):
                chunks: list[str] = []
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        value = part.get("text")
                        if isinstance(value, str):
                            chunks.append(value)
                text = "".join(chunks)
                if text.strip():
                    return text
            error = item.get("error")
            if isinstance(error, dict) and error.get("message"):
                return str(error["message"])
        return ""

    def _turn_failed(self, messages: list[dict[str, Any]]) -> str | None:
        for item in messages:
            if item.get("type") == "idle" and item.get("outcome") == "failed":
                error = item.get("error") or {}
                if isinstance(error, dict):
                    return str(error.get("message") or error.get("type") or "turn failed")
                return "turn failed"
            if item.get("type") == "assistant" and item.get("finish") == "error":
                error = item.get("error") or {}
                if isinstance(error, dict):
                    return str(error.get("message") or error.get("type") or "assistant error")
                return "assistant error"
        return None

    def _usage_from_session(self, before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, int]:
        def tokens(payload: Mapping[str, Any]) -> dict[str, int]:
            body = _unwrap_session_payload(payload)
            raw = body.get("tokens") if isinstance(body.get("tokens"), Mapping) else {}
            return {
                "input_tokens": int((raw or {}).get("input", 0) or 0),
                "output_tokens": int((raw or {}).get("output", 0) or 0),
                "reasoning_tokens": int((raw or {}).get("reasoning", 0) or 0),
                "total_tokens": int((raw or {}).get("input", 0) or 0)
                + int((raw or {}).get("output", 0) or 0)
                + int((raw or {}).get("reasoning", 0) or 0),
            }

        before_u = tokens(before)
        after_u = tokens(after)
        return {
            "input_tokens": max(0, after_u["input_tokens"] - before_u["input_tokens"]),
            "output_tokens": max(0, after_u["output_tokens"] - before_u["output_tokens"]),
            "reasoning_tokens": max(0, after_u["reasoning_tokens"] - before_u["reasoning_tokens"]),
            "total_tokens": max(0, after_u["total_tokens"] - before_u["total_tokens"]),
        }

    def _resolve_profile(self, profile: str | None) -> AgentProfile | None:
        if not profile:
            return None
        resolved = self.profiles.get(profile)
        if resolved is None:
            print(f"[opencode_adapter] unknown agent profile {profile!r}; sending turn without it")
        return resolved

    def _apply_profile_permissions(self, session_id: str, profile: AgentProfile | None) -> None:
        if profile is None or profile.tools_enabled:
            return
        try:
            self.sessions.set_permissions(session_id, TOOLS_DISABLED_PERMISSIONS)
        except OpenCodeError as exc:
            print(f"[opencode_adapter] tools-disabled permissions skipped: {exc}")

    def _run_agent_turn_job(
        self,
        *,
        branch_id: str,
        text: str,
        job_id: str,
        run_id: str,
        timeout_ms: int,
        profile: str | None = None,
    ) -> None:
        sequence = 1
        usage = {"input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0, "total_tokens": 0}
        resolved = self._resolve_profile(profile)
        prompt_text = compose_profile_prompt(text, resolved)
        try:
            with self._lock:
                branch = self.branches.get(branch_id)
                if branch is None:
                    raise OpenCodeError(f"unknown branch: {branch_id}")
                session_id = branch.session_id
                if branch.busy:
                    raise OpenCodeError(f"branch busy: {branch_id}")
                branch.busy = True
            turn_lock = self._session_turn_lock(session_id)
            if not turn_lock.acquire(blocking=False):
                raise OpenCodeError(f"session busy: {session_id}")
            try:
                self._emit_job(
                    "host.job.started",
                    {"run_id": run_id, "job_id": job_id, "sequence": sequence},
                )
                sequence += 1
                mapper = JobProgressMapper(run_id, job_id, sequence=sequence)
                unsubscribe = self.events.subscribe(
                    session_id,
                    lambda event, _mapper=mapper, _job_id=job_id: self._on_job_progress(
                        _mapper, _job_id, event
                    ),
                )
                try:
                    before_ids = _message_ids(self.sessions.messages(session_id, limit=100))
                    before = self.sessions.get(session_id)
                    self._apply_profile_permissions(session_id, resolved)
                    if resolved is not None and resolved.prompt_metadata is not None:
                        self.sessions.prompt(session_id, prompt_text, metadata=resolved.prompt_metadata)
                    else:
                        self.sessions.prompt(session_id, prompt_text)
                    self.sessions.wait(session_id, timeout_ms=timeout_ms)
                finally:
                    unsubscribe()
                    sequence = mapper.sequence
                if self._job_cancelled(job_id):
                    return
                messages = self.sessions.messages(session_id, limit=20)
                turn_messages = _new_messages(messages, before_ids)
                after = self.sessions.get(session_id)
                usage = self._usage_from_session(before, after)
                failure = self._turn_failed(turn_messages)
                if failure:
                    if self._claim_job_terminal(job_id):
                        self._emit_job(
                            "host.job.failed",
                            {
                                "run_id": run_id,
                                "job_id": job_id,
                                "sequence": sequence,
                                "failure": {"code": "agent_turn_failed", "message": failure},
                                "usage": usage,
                            },
                        )
                    return
                if resolved is not None and resolved.output_mode == "final_text":
                    # /message returns newest first. The pre-turn scan covers
                    # this entire window, even when a long history is inherited.
                    output = self._assistant_text(turn_messages)
                else:
                    try:
                        context_messages = self.sessions.context(session_id)
                    except OpenCodeError:
                        context_messages = list(reversed(messages))
                    output = self._assistant_text(context_messages) or self._assistant_text(messages)
                if self._claim_job_terminal(job_id):
                    self._emit_job(
                        "host.job.completed",
                        {
                            "run_id": run_id,
                            "job_id": job_id,
                            "sequence": sequence,
                            "output": output,
                            "usage": usage,
                            "exit_code": 0,
                            "truncated": False,
                        },
                    )
            finally:
                turn_lock.release()
                with self._lock:
                    branch = self.branches.get(branch_id)
                    if branch is not None:
                        branch.busy = False
        except Exception as exc:  # noqa: BLE001
            try:
                if not self._job_cancelled(job_id) and self._claim_job_terminal(job_id):
                    self._emit_job(
                        "host.job.failed",
                        {
                            "run_id": run_id,
                            "job_id": job_id,
                            "sequence": sequence,
                            "failure": {
                                "code": type(exc).__name__,
                                "message": str(exc) or type(exc).__name__,
                            },
                            "usage": usage,
                        },
                    )
            finally:
                with self._lock:
                    branch = self.branches.get(branch_id)
                    if branch is not None:
                        branch.busy = False

    def cancel_job(
        self,
        job_id: str,
        *,
        reason: str,
        timeout_ms: int,
        idempotency_key: str,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Best-effort cancel of an in-flight agent_turn/exec job.

        Marks the job cancelled and emits ``host.job.failed`` so plugin waiters
        unblock. The background worker may still finish OpenCode HTTP work; it
        will skip a second terminal notification once this path has fired.
        """
        del timeout_ms, plugin_id

        def produce() -> dict[str, Any]:
            with self._lock:
                job = self._active_jobs.get(job_id)
                if job is None:
                    return {"job_id": job_id, "status": "completed"}
                if job["terminal"]:
                    return {"job_id": job_id, "status": "completed"}
                job["cancelled"].set()
                job["terminal"] = True
                resolved_run_id = str(job.get("run_id") or run_id or "run")
            self._emit_job(
                "host.job.failed",
                {
                    "run_id": resolved_run_id,
                    "job_id": job_id,
                    "sequence": 99,
                    "failure": {"code": "cancelled", "message": reason},
                    "usage": {"input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0, "total_tokens": 0},
                },
            )
            return {"job_id": job_id, "status": "cancelling"}

        return self._idempotent(run_id, "host.cancel_job", idempotency_key, produce)

    def agent_turn(
        self,
        branch_id: str,
        text: str,
        *,
        timeout_ms: int,
        idempotency_key: str,
        profile: str | None = None,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        del plugin_id

        def produce() -> dict[str, Any]:
            with self._lock:
                if branch_id not in self.branches:
                    raise OpenCodeError(f"unknown branch: {branch_id}")
            job_id = self._next("job")
            resolved_run_id = run_id or "run"
            with self._lock:
                self._active_jobs[job_id] = {
                    "run_id": resolved_run_id,
                    "cancelled": threading.Event(),
                    "terminal": False,
                }
            thread = threading.Thread(
                target=self._run_agent_turn_job,
                kwargs={
                    "branch_id": branch_id,
                    "text": text,
                    "job_id": job_id,
                    "run_id": resolved_run_id,
                    "timeout_ms": timeout_ms,
                    "profile": profile,
                },
                name=f"opencode-turn-{job_id}",
                daemon=True,
            )
            thread.start()
            return {"job_id": job_id, "status": "accepted"}

        return self._idempotent(run_id, "host.agent_turn", idempotency_key, produce)

    def _latest_shell_message(
        self, messages: list[dict[str, Any]], command: str
    ) -> dict[str, Any] | None:
        for item in messages:
            if item.get("type") != "shell":
                continue
            if item.get("command") == command:
                return item
        for item in messages:
            if item.get("type") == "shell":
                return item
        return None

    def _run_exec_job(
        self,
        *,
        branch_id: str,
        argv: list[str],
        cwd: str,
        job_id: str,
        run_id: str,
        timeout_ms: int,
    ) -> None:
        """Run argv locally in the branch worktree (no OpenCode shell/wait)."""
        sequence = 1
        try:
            with self._lock:
                branch = self.branches.get(branch_id)
                if branch is None:
                    raise OpenCodeError(f"unknown branch: {branch_id}")
                if branch.busy:
                    raise OpenCodeError(f"branch busy: {branch_id}")
                branch.busy = True
                worktree = branch.worktree
            self._emit_job(
                "host.job.started",
                {"run_id": run_id, "job_id": job_id, "sequence": sequence},
            )
            sequence += 1
            run_cwd_path = _ensure_under_worktree(
                worktree, _sandbox_to_worktree(worktree, cwd)
            )
            if not run_cwd_path.is_dir():
                raise OpenCodeError(f"exec cwd does not exist: {run_cwd_path}")
            timeout_s = None if timeout_ms is None else max(0.1, float(timeout_ms) / 1000.0)
            try:
                completed = subprocess.run(
                    list(argv),
                    cwd=str(run_cwd_path),
                    capture_output=True,
                    text=True,
                    timeout=timeout_s,
                    check=False,
                )
                exit_code = int(completed.returncode)
                output = (completed.stdout or "") + (completed.stderr or "")
                truncated = False
            except subprocess.TimeoutExpired as exc:
                exit_code = 124
                out = ""
                if exc.stdout:
                    out += exc.stdout if isinstance(exc.stdout, str) else exc.stdout.decode(
                        "utf-8", errors="replace"
                    )
                if exc.stderr:
                    out += exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode(
                        "utf-8", errors="replace"
                    )
                output = out or f"timed out after {timeout_s}s"
                truncated = True
            self._emit_job(
                "host.job.completed",
                {
                    "run_id": run_id,
                    "job_id": job_id,
                    "sequence": sequence,
                    "output": output,
                    "usage": {},
                    "exit_code": exit_code,
                    "truncated": truncated,
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._emit_job(
                "host.job.failed",
                {
                    "run_id": run_id,
                    "job_id": job_id,
                    "sequence": sequence,
                    "failure": {
                        "code": type(exc).__name__,
                        "message": str(exc) or type(exc).__name__,
                    },
                    "usage": {},
                },
            )
        finally:
            with self._lock:
                branch = self.branches.get(branch_id)
                if branch is not None:
                    branch.busy = False

    def exec(
        self,
        branch_id: str,
        argv: list[str],
        *,
        cwd: str,
        timeout_ms: int,
        idempotency_key: str,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        del plugin_id

        def produce() -> dict[str, Any]:
            with self._lock:
                if branch_id not in self.branches:
                    raise OpenCodeError(f"unknown branch: {branch_id}")
            job_id = self._next("job")
            resolved_run_id = run_id or "run"
            thread = threading.Thread(
                target=self._run_exec_job,
                kwargs={
                    "branch_id": branch_id,
                    "argv": list(argv),
                    "cwd": cwd,
                    "job_id": job_id,
                    "run_id": resolved_run_id,
                    "timeout_ms": timeout_ms,
                },
                name=f"opencode-exec-{job_id}",
                daemon=True,
            )
            thread.start()
            return {"job_id": job_id, "status": "accepted"}

        return self._idempotent(run_id, "host.exec", idempotency_key, produce)

    def inspect(
        self,
        branch_id: str,
        *,
        view: str,
        timeout_ms: int,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> str:
        raise NotImplementedError("OpenCodeAdapter.inspect lands in a later step")

    def file_read(
        self,
        branch_id: str,
        path: str,
        *,
        timeout_ms: int,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> bytes:
        """Read a file from the branch worktree (needed for isolation tests)."""
        del timeout_ms, plugin_id
        with self._lock:
            branch = self.branches.get(branch_id)
            if branch is None:
                raise OpenCodeError(f"unknown branch: {branch_id}")
            worktree = branch.worktree
        target = _ensure_under_worktree(worktree, _sandbox_to_worktree(worktree, path))
        if not target.is_file():
            raise OpenCodeError(f"file not found: {path}")
        return target.read_bytes()

    def file_write(
        self,
        branch_id: str,
        path: str,
        data: bytes,
        *,
        timeout_ms: int,
        idempotency_key: str,
        plugin_id: str | None = None,
        run_id: str | None = None,
    ) -> int:
        """Write a file into the branch worktree (needed for isolation tests)."""
        del timeout_ms, plugin_id

        def produce() -> dict[str, Any]:
            with self._branch_lock(branch_id):
                with self._lock:
                    branch = self.branches.get(branch_id)
                    if branch is None:
                        raise OpenCodeError(f"unknown branch: {branch_id}")
                    if branch.busy:
                        raise OpenCodeError(f"branch busy: {branch_id}")
                    worktree = branch.worktree
                target = _ensure_under_worktree(
                    worktree, _sandbox_to_worktree(worktree, path)
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                return {"written_bytes": len(data)}

        result = self._idempotent(run_id, "host.file_write", idempotency_key, produce)
        return int(result["written_bytes"])


    def run_record_path(self, output_dir: str | Path | None = None) -> Path:
        base = Path(output_dir) if output_dir is not None else self.output_dir
        if base is None:
            raise OpenCodeError("output_dir is required to locate run_record.json")
        return Path(base).expanduser().resolve() / "run_record.json"

    def build_run_record(self) -> dict[str, Any]:
        with self._lock:
            states = {
                state_id: {
                    "state_id": rec.state_id,
                    "commit": rec.commit,
                    "frozen_session_id": rec.frozen_session_id,
                    "empty_history": rec.empty_history,
                    "is_root": rec.is_root,
                    "frozen_export_path": rec.frozen_export_path,
                }
                for state_id, rec in self.states.items()
            }
            branches = {
                branch_id: {
                    "branch_id": rec.branch_id,
                    "parent_state_id": rec.parent_state_id,
                    "commit": rec.commit,
                    "worktree": str(rec.worktree),
                    "session_id": rec.session_id,
                    "busy": rec.busy,
                }
                for branch_id, rec in self.branches.items()
            }
            record = {
                "output_dir": str(self.output_dir) if self.output_dir else None,
                "source_repo": str(self.source_repo) if self.source_repo else None,
                "private_repo": str(self.private_repo) if self.private_repo else None,
                "root_state_id": self.root_state_id,
                "root_commit": self.root_commit,
                "opencode_url": self.base_url,
                "model": self.model,
                "states": states,
                "branches": branches,
                "frozen_session_ids": sorted(self._frozen_session_ids),
                "delivered": self.delivered,
            }
        return record

    def write_run_record(self, output_dir: str | Path | None = None) -> Path:
        path = self.run_record_path(output_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.build_run_record(), ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return path

    @classmethod
    def load_run_record(cls, output_dir: str | Path) -> dict[str, Any]:
        path = Path(output_dir).expanduser().resolve() / "run_record.json"
        if not path.is_file():
            raise OpenCodeError(f"run_record.json not found: {path}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _checkout_commit_to(self, commit: str, dest: Path) -> None:
        private = self._require_private()
        if dest.exists():
            shutil.rmtree(dest)
        dest.mkdir(parents=True, exist_ok=True)
        archive = subprocess.run(
            ["git", "-C", str(private), "archive", commit],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["tar", "-x", "-C", str(dest)],
            input=archive.stdout,
            check=True,
            capture_output=True,
        )

    def _diff_patch(self, base_commit: str, head_commit: str) -> str:
        private = self._require_private()
        completed = _git(
            private,
            ["diff", "--binary", base_commit, head_commit],
            check=True,
        )
        return completed.stdout

    def diff(self, branch_id: str, base_state_id: str) -> str:
        with self._lock:
            branch = self.branches.get(branch_id)
            base = self.states.get(base_state_id)
            if branch is None or base is None:
                raise OpenCodeError(f"unknown branch or base state: {branch_id}, {base_state_id}")
            base_commit, head_commit = base.commit, branch.commit
        return self._diff_patch(base_commit, head_commit)

    def deliver_selected(
        self,
        *,
        output_dir: str,
        apply_plan: Mapping[str, Any],
        result: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Materialize the selected snapshot and write delivery artifacts.

        Writes ``selected/``, ``selected.patch`` (vs startup root commit), and
        ``result.json``. Reclaims non-selected live branches/sessions while
        retaining the selected frozen session until ``cleanup_run``.
        """
        out = Path(output_dir).expanduser().resolve()
        if self.output_dir is None:
            self.output_dir = out
        if self.private_repo is None:
            candidate = out / "private"
            if candidate.is_dir():
                self.private_repo = candidate
        self._require_private()

        if not isinstance(apply_plan, Mapping):
            raise OpenCodeError("apply_plan must be an object")
        state_id = apply_plan.get("state_id")
        if not isinstance(state_id, str) or not state_id:
            raise OpenCodeError("apply_plan.state_id is required")

        with self._lock:
            state = self.states.get(state_id)
            if state is None:
                record = None
                try:
                    record = self.load_run_record(out)
                except OpenCodeError:
                    record = None
                if record and state_id in (record.get("states") or {}):
                    raw = record["states"][state_id]
                    state = StateRecord(
                        state_id=raw["state_id"],
                        commit=raw["commit"],
                        frozen_session_id=raw.get("frozen_session_id"),
                        empty_history=bool(raw.get("empty_history", False)),
                        is_root=bool(raw.get("is_root", False)),
                        frozen_export_path=raw.get("frozen_export_path"),
                    )
                    self.states[state_id] = state
                    if self.root_commit is None:
                        self.root_commit = record.get("root_commit")
                    if self.root_state_id is None:
                        self.root_state_id = record.get("root_state_id")
                else:
                    raise OpenCodeError(f"unknown selected state: {state_id}")
            selected = StateRecord(
                state_id=state.state_id,
                commit=state.commit,
                frozen_session_id=state.frozen_session_id,
                empty_history=state.empty_history,
                is_root=state.is_root,
                frozen_export_path=state.frozen_export_path,
            )
            root_commit = self.root_commit
            if root_commit is None and self.root_state_id and self.root_state_id in self.states:
                root_commit = self.states[self.root_state_id].commit
            if root_commit is None:
                raise OpenCodeError("root_commit is unknown; cannot build selected.patch")

        selected_dir = out / "selected"
        self._checkout_commit_to(selected.commit, selected_dir)

        session_id = selected.frozen_session_id
        session_export_path = selected_dir / "session.export.json"
        if selected.frozen_export_path and Path(selected.frozen_export_path).is_file():
            shutil.move(str(selected.frozen_export_path), str(session_export_path))
            with self._lock:
                if state_id in self.states:
                    self.states[state_id].frozen_export_path = str(session_export_path)
        elif session_id:
            try:
                exported = self.sessions.export(session_id)
                session_export_path.write_text(
                    json.dumps(exported, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8",
                )
            except OpenCodeError:
                session_export_path.write_text(
                    json.dumps({"id": session_id, "note": "export unavailable"}, indent=2)
                    + "\n",
                    encoding="utf-8",
                )

        if session_id:
            try:
                self.sessions.move(session_id, directory=str(selected_dir.resolve()))
            except OpenCodeError:
                pass

        patch_text = self._diff_patch(root_commit, selected.commit)
        patch_path = out / "selected.patch"
        patch_path.write_text(patch_text, encoding="utf-8")

        continue_cmd = (
            f"opencode --server {self.base_url} -s {session_id} {selected_dir}"
            if session_id
            else f"opencode --server {self.base_url} {selected_dir}"
        )
        delivery = {
            "selected_dir": str(selected_dir),
            "selected_patch": str(patch_path),
            "state_id": selected.state_id,
            "commit": selected.commit,
            "base_commit": root_commit,
            "session_id": session_id,
            "session_export": str(session_export_path) if session_export_path.exists() else None,
            "worktree_path": str(selected_dir),
            "continue_session_command": continue_cmd,
            "apply_plan": dict(apply_plan),
        }

        result_payload = dict(result or {})
        result_payload.setdefault("status", "completed")
        result_payload["apply_plan"] = dict(apply_plan)
        result_payload["delivery"] = delivery
        result_payload.setdefault("evidence", [])
        result_path = out / "result.json"
        result_path.write_text(
            json.dumps(result_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        delivery["result_json"] = str(result_path)

        for branch_id in list(self.branches.keys()):
            try:
                self.release(
                    branch_id,
                    timeout_ms=60_000,
                    idempotency_key=f"deliver:release:{branch_id}",
                    run_id="deliver",
                )
            except OpenCodeError:
                pass

        keep = {session_id} if session_id else set()
        for frozen_id in list(self._frozen_session_ids):
            if frozen_id in keep:
                continue
            try:
                self.sessions.remove(frozen_id)
            except OpenCodeError:
                pass
            self._frozen_session_ids.discard(frozen_id)
        if session_id:
            self._frozen_session_ids.add(session_id)

        with self._lock:
            self.delivered = delivery
        self.write_run_record(out)
        return delivery

    def cleanup_run(self, *, output_dir: str, keep_selected: bool = True) -> dict[str, Any]:
        """Explicitly reclaim retained resources listed in run_record.json."""
        out = Path(output_dir).expanduser().resolve()
        record = self.load_run_record(out)
        if self.output_dir is None:
            self.output_dir = out
        if self.private_repo is None and record.get("private_repo"):
            self.private_repo = Path(record["private_repo"])
        if self.root_commit is None:
            self.root_commit = record.get("root_commit")
        if not self.states and isinstance(record.get("states"), dict):
            for sid, raw in record["states"].items():
                self.states[sid] = StateRecord(
                    state_id=raw["state_id"],
                    commit=raw["commit"],
                    frozen_session_id=raw.get("frozen_session_id"),
                    empty_history=bool(raw.get("empty_history", False)),
                    is_root=bool(raw.get("is_root", False)),
                    frozen_export_path=raw.get("frozen_export_path"),
                )

        removed_sessions: list[str] = []
        removed_worktrees: list[str] = []

        for branch_id in list(self.branches.keys()):
            try:
                self.release(
                    branch_id,
                    timeout_ms=60_000,
                    idempotency_key=f"cleanup:release:{branch_id}",
                    run_id="cleanup",
                )
            except OpenCodeError:
                pass

        worktrees_dir = out / "worktrees"
        if worktrees_dir.is_dir():
            for child in sorted(worktrees_dir.iterdir()):
                removed_worktrees.append(str(child))
                if self.private_repo and self.private_repo.is_dir():
                    _git(
                        self.private_repo,
                        ["worktree", "remove", "--force", str(child)],
                        check=False,
                    )
                if child.exists():
                    shutil.rmtree(child, ignore_errors=True)
            if self.private_repo and self.private_repo.is_dir():
                _git(self.private_repo, ["worktree", "prune"], check=False)
            shutil.rmtree(worktrees_dir, ignore_errors=True)

        keep_session = None
        delivered = record.get("delivered") or {}
        if keep_selected:
            keep_session = delivered.get("session_id")

        session_ids: set[str] = set(record.get("frozen_session_ids") or [])
        for branch in (record.get("branches") or {}).values():
            if isinstance(branch, dict) and branch.get("session_id"):
                session_ids.add(branch["session_id"])
        if delivered.get("session_id"):
            session_ids.add(delivered["session_id"])

        for session_id in sorted(session_ids):
            if keep_selected and session_id == keep_session:
                continue
            try:
                self.sessions.remove(session_id)
            except OpenCodeError:
                pass
            removed_sessions.append(session_id)
            self._frozen_session_ids.discard(session_id)

        frozen_dir = out / "frozen_sessions"
        if frozen_dir.is_dir():
            shutil.rmtree(frozen_dir, ignore_errors=True)

        private = out / "private"
        if private.exists():
            shutil.rmtree(private, ignore_errors=True)
            self.private_repo = None

        summary = {
            "output_dir": str(out),
            "removed_sessions": removed_sessions,
            "removed_worktrees": removed_worktrees,
            "kept_session_id": keep_session if keep_selected else None,
            "kept_selected_dir": (
                str(out / "selected")
                if keep_selected and (out / "selected").exists()
                else None
            ),
            "cleaned_at": datetime.now(timezone.utc).isoformat(),
        }
        record["cleanup"] = summary
        record["branches"] = {}
        record["frozen_session_ids"] = (
            [keep_session] if keep_selected and keep_session else []
        )
        record["private_repo"] = None
        (out / "run_record.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        with self._lock:
            self.branches.clear()
            if not keep_selected:
                self._frozen_session_ids.clear()
                self.delivered = None
        return summary
