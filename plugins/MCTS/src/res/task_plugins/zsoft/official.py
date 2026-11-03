"""One-shot official ZSoft evaluation.

PoC extraction still goes through the allocated xGovernor branch, but judge
execution happens on the host side, using the same ``zsoft-poc`` executable
(or HTTP submission server) as the legacy runner. The sandbox never sees the
private judge command or credentials.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from .flow import extract_poc


LOCAL_BINARY = "/root/e2b-zsoft-docker-template/.venv-control/bin/zsoft-poc"


def _parse(record: dict) -> dict:
    result = record.get("result") or {}
    return {
        "task_id": str(record.get("task_id") or ""),
        "attempt_id": str(record.get("attempt_id") or ""),
        "status": str(result.get("status") or "infrastructure_error"),
        "success": result.get("success") is True,
        "summary": str(result.get("summary") or ""),
        "checks": result.get("checks") or [],
        "details": result.get("details") or {},
    }


def _post(url: str, task_id: str, filename: str, content: bytes) -> dict:
    boundary = "----zsoft" + uuid.uuid4().hex
    body = bytearray()
    body.extend(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"phase\"\r\n\r\nofficial\r\n".encode()
    )
    body.extend(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n"
        f"Content-Type: application/octet-stream\r\n\r\n".encode()
    )
    body.extend(content)
    body.extend(f"\r\n--{boundary}--\r\n".encode())
    request = urllib.request.Request(
        f"{url.rstrip('/')}/v0.1/tasks/{task_id}/submissions",
        data=bytes(body),
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(request, timeout=3600) as response:
        return _parse(json.loads(response.read()))


def _binary() -> str:
    configured = os.environ.get("ZSOFT_BIN", "").strip()
    if configured:
        return configured
    if Path(LOCAL_BINARY).is_file():
        return LOCAL_BINARY
    found = shutil.which("zsoft-poc")
    if found:
        return found
    raise RuntimeError("zsoft-poc not found; set ZSOFT_BIN or ZSOFT_SERVER_URL")


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
    env.setdefault("HOME", "/root")
    env.setdefault("LANG", "C.UTF-8")
    env.setdefault("LC_ALL", "C.UTF-8")
    env.setdefault("TMPDIR", "/tmp")
    return env


def _local(task, poc: Path) -> tuple[dict, str, str]:
    binary = _binary()
    bench = task.tasks_dir.parent
    state = bench / ".task-state"
    runs = bench / ".runs"
    if not (state / task.task_id / "image-refs.json").is_file():
        for command in (
            [
                binary,
                "fetch-sources",
                "--task-id",
                task.task_id,
                "--tasks-dir",
                str(task.tasks_dir),
            ],
            [
                binary,
                "prepare",
                task.task_id,
                "--tasks-dir",
                str(task.tasks_dir),
                "--state-dir",
                str(state),
            ],
        ):
            completed = subprocess.run(
                command,
                env=_subprocess_env(),
                capture_output=True,
                text=True,
                timeout=3600,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"{Path(command[0]).name} {' '.join(command[1:])} failed "
                    f"({completed.returncode}): {completed.stderr[-2000:]}"
                )
    completed = subprocess.run(
        [
            binary,
            "evaluate",
            task.task_id,
            str(poc),
            "--phase",
            "official",
            "--tasks-dir",
            str(task.tasks_dir),
            "--runs-dir",
            str(runs),
            "--state-dir",
            str(state),
        ],
        env=_subprocess_env(),
        capture_output=True,
        text=True,
        timeout=1800,
    )
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if completed.returncode not in (0, 1):
        raise RuntimeError(
            f"zsoft-poc failed ({completed.returncode}): {stderr[-2000:]}"
        )
    return _parse(json.loads(stdout)), stdout, stderr


def evaluate(node, task, output_root: Path, *, host, branch_id, wait_job) -> tuple[bool, Path]:
    extracted = extract_poc(host, branch_id, task)
    if extracted is None:
        raise RuntimeError(f"no PoC at {task.poc_sandbox_path} for {node.node_id}")
    content, _ = extracted
    out = output_root / task.spec.id / task.instance_id if output_root.name == "output" else output_root
    out.mkdir(parents=True, exist_ok=True)
    artifact = out / task.poc_filename
    artifact.write_bytes(content)
    (out / "node.txt").write_text(node.node_id + "\n", encoding="utf-8")

    server = os.environ.get("ZSOFT_SERVER_URL", "").strip()
    if server:
        result = _post(server, task.task_id, artifact.name, content)
        stdout, stderr = "", ""
    else:
        result, stdout, stderr = _local(task, artifact)

    (out / "official_result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out / "official.stdout.log").write_text(stdout, encoding="utf-8")
    (out / "official.stderr.log").write_text(stderr, encoding="utf-8")
    return result["status"] == "completed" and result["success"], artifact
