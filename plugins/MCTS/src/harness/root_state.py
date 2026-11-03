"""Prepare a recoverable xGovernor root state for the MCTS plugin.

The plugin must never talk directly to xGovernor's public session HTTP API.
This harness is the only pre-run tool allowed to do so: it opens a sandbox,
materializes the task's public bundle, checkpoints it, verifies the checkpoint
can be loaded, and writes the resulting ``root_state_id`` for a later
``POST /api/v1/mcts/runs`` call.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import shlex
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4


ZSOFT_TEMPLATE = "zsoft-docker"
SWEVERIFIED_TEMPLATE = "swepro-docker"
DEFAULT_RUNTIME_KIND = "pi"
DEFAULT_BASE_URL = "http://127.0.0.1:8787"
DEFAULT_EXEC_TIMEOUT_MS = 600_000
SWEVERIFIED_EXEC_TIMEOUT_MS = 1_800_000
HTTP_TIMEOUT_SECONDS = SWEVERIFIED_EXEC_TIMEOUT_MS // 1000 + 60
ROOT_STATE_FILE = "root_state_id"
ROOT_STATE_JSON = "root_state.json"


class RootStateError(RuntimeError):
    """A root-state HTTP or verification step failed."""


class HttpRequestError(RootStateError):
    """The xGovernor HTTP request returned a non-2xx status."""

    def __init__(self, path: str, status: int, body: Any) -> None:
        super().__init__(f"POST {path} failed with HTTP {status}: {body!r}")
        self.path = path
        self.status = status
        self.body = body


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: dict[str, Any]


Transport = Callable[[str, str, dict[str, Any]], HttpResponse]
GetTransport = Callable[[str], HttpResponse]


def _default_json_post(
    base_url: str,
    token: str | None,
    path: str,
    payload: dict[str, Any],
) -> HttpResponse:
    """Minimal JSON HTTP transport used when tests don't inject one."""
    url = base_url.rstrip("/") + "/" + path.lstrip("/")
    data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            return HttpResponse(
                status=response.status,
                body=json.loads(raw) if raw.strip() else {},
            )
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            body = {"raw": raw}
        return HttpResponse(status=error.code, body=body)
    except urllib.error.URLError as error:
        raise RootStateError(f"could not reach xGovernor at {url}: {error}") from error


def _default_json_get(base_url: str, token: str | None, path: str) -> HttpResponse:
    """Minimal JSON HTTP GET transport used when tests don't inject one."""
    url = base_url.rstrip("/") + "/" + path.lstrip("/")
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            raw = response.read().decode("utf-8")
            return HttpResponse(
                status=response.status,
                body=json.loads(raw) if raw.strip() else {},
            )
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            body = {"raw": raw}
        return HttpResponse(status=error.code, body=body)
    except urllib.error.URLError as error:
        raise RootStateError(f"could not reach xGovernor at {url}: {error}") from error


class RootStateClient:
    """Thin wrapper around xGovernor's session HTTP control surface."""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        token: str | None = None,
        transport: Transport | None = None,
        get_transport: GetTransport | None = None,
    ) -> None:
        self.base_url = base_url or os.environ.get(
            "XGOVERNOR_BASE_URL", DEFAULT_BASE_URL
        )
        self.token = token if token is not None else os.environ.get("XGOVERNOR_API_TOKEN", "")
        self.transport = transport
        self.get_transport = get_transport

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self.transport is not None:
            response = self.transport(path, payload)
        else:
            response = _default_json_post(self.base_url, self.token, path, payload)
        if not 200 <= response.status < 300:
            raise HttpRequestError(path, response.status, response.body)
        if not isinstance(response.body, dict):
            raise RootStateError(f"non-object response from POST {path}: {response.body!r}")
        return response.body

    def get(self, path: str) -> dict[str, Any]:
        if self.get_transport is not None:
            response = self.get_transport(path)
        else:
            response = _default_json_get(self.base_url, self.token, path)
        if not 200 <= response.status < 300:
            raise HttpRequestError(path, response.status, response.body)
        if not isinstance(response.body, dict):
            raise RootStateError(f"non-object response from GET {path}: {response.body!r}")
        return response.body

    @staticmethod
    def _lease(client_id: str) -> dict[str, str]:
        return {"client_id": client_id}

    @staticmethod
    def _capabilities() -> dict[str, list[str]]:
        return {
            "sandbox": ["exec", "file_read", "file_write", "snapshot"],
            "runtime": ["fork", "checkpoint", "state_export"],
        }

    @staticmethod
    def _deployment(template: str) -> dict[str, Any]:
        return {"options": {"provider_options": {"template_id": template}}}

    def open_runtime(
        self,
        *,
        mode: str,
        task_id: str | None = None,
        runtime_kind: str | None = None,
        conversation_id: str | None = None,
        sender_id: str = "mcts-harness",
    ) -> dict[str, Any]:
        template = SWEVERIFIED_TEMPLATE if mode == "sweverified" else ZSOFT_TEMPLATE
        ident = task_id or mode
        conversation_id = conversation_id or f"mcts-root:{ident}"
        ext_namespace = "runtime_pi" if (runtime_kind or DEFAULT_RUNTIME_KIND) == "pi" else "xiaoo"
        payload: dict[str, Any] = {
            "runtime_kind": runtime_kind or DEFAULT_RUNTIME_KIND,
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "workspace": {"kind": "daemon_default"},
            "deployment": self._deployment(template),
            "requested_capabilities": self._capabilities(),
            "ext": {ext_namespace: {"backend_id": "e2b"}},
            "lease": self._lease(f"mcts-root:{ident}"),
        }
        return self.post("/api/v1/sessions/open", payload)

    def write_file(
        self,
        runtime_id: str,
        path: str,
        content: bytes,
        *,
        client_id: str,
        create_parents: bool = True,
    ) -> dict[str, Any]:
        """Upload a file, chunking large bundles so the HTTP body stays small.

        The pre-plugin xGovernor client did this for task source bundles. The
        session HTTP body limit is intentionally small, so a one-shot
        base64 JSON POST for a 50MB+ local source tree violates it and the
        server drops the connection (the P4 ``Broken pipe`` failure).
        """
        chunk_size = 1_000_000
        if len(content) <= chunk_size:
            return self._write_file(
                runtime_id,
                path,
                content,
                client_id=client_id,
                create_parents=create_parents,
            )

        suffix = uuid4().hex
        parts: list[str] = []
        for offset in range(0, len(content), chunk_size):
            part_path = f"{path}.{suffix}.part{offset}"
            self._write_file(
                runtime_id,
                part_path,
                content[offset : offset + chunk_size],
                client_id=client_id,
                create_parents=create_parents,
            )
            parts.append(shlex.quote(part_path))

        assemble = (
            f"cat {' '.join(parts)} > {shlex.quote(path)} && "
            f"rm -f {' '.join(parts)}"
        )
        self.exec(runtime_id, assemble, client_id=client_id)
        return {"chunked": True, "parts": len(parts)}

    def _write_file(
        self,
        runtime_id: str,
        path: str,
        content: bytes,
        *,
        client_id: str,
        create_parents: bool = True,
    ) -> dict[str, Any]:
        return self.post(
            "/api/v1/sessions/files/write",
            {
                "runtime_id": runtime_id,
                "path": path,
                "content_base64": base64.b64encode(content).decode("ascii"),
                "create_parents": create_parents,
                "lease": self._lease(client_id),
            },
        )

    def exec(
        self,
        runtime_id: str,
        command: str,
        *,
        client_id: str,
        cwd: str = "/home/user/workspace",
        timeout_ms: int = DEFAULT_EXEC_TIMEOUT_MS,
    ) -> dict[str, Any]:
        return self.post(
            "/api/v1/sessions/exec",
            {
                "runtime_id": runtime_id,
                "command": ["bash", "-lc", command],
                "cwd": cwd,
                "timeout_ms": timeout_ms,
                "lease": self._lease(client_id),
            },
        )

    def checkpoint(
        self,
        runtime_id: str,
        *,
        client_id: str,
        name: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "runtime_id": runtime_id,
            "requested_scope": "full",
            "lease": self._lease(client_id),
        }
        if name:
            payload["name"] = name
        return self.post("/api/v1/sessions/checkpoint", payload)

    def delete_checkpoint(
        self,
        checkpoint_id: str,
        *,
        client_id: str,
    ) -> dict[str, Any]:
        return self.post(
            "/api/v1/sessions/checkpoint/delete",
            {
                "checkpoint_id": checkpoint_id,
                "lease": self._lease(client_id),
            },
        )

    def load_checkpoint(
        self,
        checkpoint_id: str,
        *,
        client_id: str,
        mode: str,
        runtime_id: str | None = None,
    ) -> dict[str, Any]:
        template = SWEVERIFIED_TEMPLATE if mode == "sweverified" else ZSOFT_TEMPLATE
        return self.post(
            "/api/v1/sessions/load",
            {
                "checkpoint_id": checkpoint_id,
                "runtime_id": runtime_id or f"runtime-{uuid4().hex}",
                "conversation_id": f"mcts-root-verify:{checkpoint_id}",
                "sender_id": "mcts-harness",
                "requested_capabilities": self._capabilities(),
                "deployment": self._deployment(template),
                "lease": self._lease(client_id),
            },
        )

    def close(self, runtime_id: str, *, client_id: str) -> dict[str, Any]:
        return self.post(
            "/api/v1/sessions/close",
            {
                "runtime_id": runtime_id,
                "lease": self._lease(client_id),
            },
        )


def _default_zsoft_task(task_id: str):
    from src.res.mode import MODES
    from src.res.task_plugins.zsoft.task import ZSoftTask

    spec = MODES.get("zsoft")
    return ZSoftTask.create(spec, task_id)


def _default_swe_task(
    task_id: str,
    *,
    testbench: str = "sweverified",
    split: str = "test",
):
    from src.res.mode import MODES

    spec = MODES.get("sweverified")
    return spec.loader()(spec, testbench=testbench, instance_id=task_id, split=split)


def _verification_marker(mode: str) -> str:
    if mode == "sweverified":
        return "SWEBENCH_REPO_READY"
    return "ZSOFT_ROOT_STATE_OK"


def _checked_exec(
    client: RootStateClient,
    runtime_id: str,
    command: str,
    *,
    client_id: str,
    what: str,
    timeout_ms: int = DEFAULT_EXEC_TIMEOUT_MS,
) -> dict[str, Any]:
    result = client.exec(runtime_id, command, client_id=client_id, timeout_ms=timeout_ms)
    exit_code = result.get("exit_code")
    if exit_code != 0 or result.get("timed_out"):
        stdout = result.get("stdout", "")
        stderr = result.get("stderr", "")
        raise RootStateError(
            f"{what} failed (exit={exit_code}, timed_out={result.get('timed_out')}): "
            f"stdout={stdout[-2000:]!r} stderr={stderr[-2000:]!r}"
        )
    return result


def prepare_root_state(
    client: RootStateClient,
    *,
    task_id: str,
    output_dir: str | Path,
    mode: str = "zsoft",
    runtime_kind: str | None = None,
    task_factory: Callable[[str], Any] | None = None,
    testbench: str = "sweverified",
    split: str = "test",
) -> dict[str, Any]:
    """Materialize, checkpoint, and verify one recoverable task root state."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ident = task_id
    client_id = f"mcts-root:{ident}"
    open_runtime_id: str | None = None
    verify_runtime_id: str | None = None
    checkpoint_id: str | None = None

    try:
        opened = client.open_runtime(
            mode=mode,
            task_id=task_id,
            runtime_kind=runtime_kind,
        )
        open_runtime_id = opened["runtime_id"]
        if not open_runtime_id:
            raise RootStateError("session open response has no runtime_id")

        if mode == "sweverified":
            task_factory = task_factory or (
                lambda instance_id: _default_swe_task(
                    instance_id, testbench=testbench, split=split
                )
            )
            task = task_factory(task_id)
            materialize = getattr(task, "materialize_script")()
            _checked_exec(
                client,
                open_runtime_id,
                materialize,
                client_id=client_id,
                what="sweverified materialization",
                timeout_ms=SWEVERIFIED_EXEC_TIMEOUT_MS,
            )
        else:
            task_factory = task_factory or _default_zsoft_task
            task = task_factory(task_id)
            bundle = getattr(task, "bundle_bytes")()
            client.write_file(
                open_runtime_id,
                "/home/user/workspace/zsoft-task.tar.gz",
                bundle,
                client_id=client_id,
            )
            materialize = getattr(task, "materialize_script")()
            _checked_exec(
                client,
                open_runtime_id,
                materialize,
                client_id=client_id,
                what="zsoft materialization",
            )
            if not getattr(task, "is_kernel", False):
                _checked_exec(
                    client,
                    open_runtime_id,
                    "cd /home/user/workspace && "
                    "if [ -d repo ] && [ ! -e .zsoft-pristine-repo.tar ]; then "
                    "tar --exclude='.git' -cf .zsoft-pristine-repo.tar -C repo .; fi",
                    client_id=client_id,
                    what="zsoft pristine snapshot",
                )

        checkpoint_name = f"mcts-root-{ident}-{int(time.time())}"
        checkpoint = client.checkpoint(
            open_runtime_id,
            client_id=client_id,
            name=checkpoint_name,
        )
        checkpoint_id = checkpoint["checkpoint_id"]
        if not checkpoint_id:
            raise RootStateError("checkpoint response has no checkpoint_id")

        loaded = client.load_checkpoint(
            checkpoint_id,
            client_id=client_id,
            mode=mode,
        )
        verify_runtime_id = loaded["runtime_id"]
        marker = _verification_marker(mode)
        verification = _checked_exec(
            client,
            verify_runtime_id,
            "test -d /home/user/workspace && "
            f"echo {marker}",
            client_id=client_id,
            what="root-state checkpoint load verification",
        )
        if marker not in str(verification.get("stdout", "")):
            raise RootStateError("checkpoint loaded but verification marker is missing")

        details = {
            "task_id": task_id,
            "mode": mode,
            "runtime_kind": runtime_kind or DEFAULT_RUNTIME_KIND,
            "root_state_id": checkpoint_id,
            "checkpoint_name": checkpoint_name,
            "source_runtime_id": open_runtime_id,
            "verified_runtime_id": verify_runtime_id,
            "created_at_ms": int(time.time() * 1000),
        }
        (output_dir / ROOT_STATE_FILE).write_text(checkpoint_id + "\n", encoding="utf-8")
        (output_dir / ROOT_STATE_JSON).write_text(
            json.dumps(details, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return details
    except Exception:
        if checkpoint_id:
            try:
                client.delete_checkpoint(
                    checkpoint_id,
                    client_id=client_id,
                )
            except RootStateError:
                pass
        raise
    finally:
        for runtime_id in (verify_runtime_id, open_runtime_id):
            if runtime_id:
                try:
                    client.close(runtime_id, client_id=client_id)
                except RootStateError:
                    pass


def read_root_state_id(path: str | Path) -> str:
    value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise RootStateError(f"root_state_id file is empty: {path}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare a recoverable MCTS root state via xGovernor session HTTP."
    )
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--mode", choices=("zsoft", "sweverified"), default="zsoft")
    parser.add_argument("--runtime-kind", default=None)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--token", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = RootStateClient(base_url=args.base_url, token=args.token)
    details = prepare_root_state(
        client,
        task_id=args.task_id,
        output_dir=args.output_dir,
        mode=args.mode,
        runtime_kind=args.runtime_kind,
    )
    print(json.dumps(details, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
