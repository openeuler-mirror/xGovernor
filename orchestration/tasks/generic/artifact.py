"""Artifact reader for the generic code-task mode.

Generic tasks are open-ended and have no official evaluator, so the final
deliverable is the repository content itself (product code plus the
author-generated verifier). The sandbox is not required to be a git repository,
therefore this reader lists and reads files instead of relying on ``git diff``.
"""

from __future__ import annotations

import os
from typing import Any

from xgovernor_tasks.common.benchmark_util import completed_output, operation_key, wait_completed
from xgovernor_tasks.common.mode import ARTIFACT_FILES_READERS, ARTIFACT_READERS, register

REPO = "/home/user/workspace/repo"
WORKSPACE = "/home/user/workspace"

# Keep the artifact bounded for all consumers (selector prompt, result.answer,
# artifact.txt). Heavy dependency/cache directories are never part of a code
# deliverable; binary files are skipped; large files are truncated.
_SKIP_DIRS = (
    "/.git/",
    "/node_modules/",
    "/.venv/",
    "/venv/",
    "/__pycache__/",
    "/target/",
    "/dist/",
    "/build/",
    "/.mypy_cache/",
    "/.pytest_cache/",
)
_MAX_FILE_CHARS = 64_000
_MAX_TOTAL_CHARS = 512_000
_MAX_FILE_BYTES = 512_000
_OMMIT_PREVIEW = 20


@register(ARTIFACT_READERS, "generic_repo")
def generic_repo(
    node: Any,
    task: Any,
    *,
    host: Any,
    branch_id: str,
    wait_job: Any = None,
    **kwargs: Any,
) -> tuple[str, str]:
    """Export the repository as a bounded, readable text artifact."""
    if wait_job is None:
        raise RuntimeError("generic_repo requires wait_job for host command receipts")
    find_parts: list[str] = [f"find {REPO} -type f"]
    for skipped in _SKIP_DIRS:
        find_parts.append(f"-not -path '*{skipped}*'")
    find_parts.append("-print | sort")
    receipt = host.exec(
        branch_id,
        ["bash", "-lc", " ".join(find_parts)],
        cwd=REPO,
        timeout_ms=120_000,
        idempotency_key=operation_key(branch_id, "artifact:list", node.node_id),
    )
    listing = completed_output(wait_completed(receipt, wait_job))
    paths = sorted({line.strip() for line in listing.splitlines() if line.strip()})

    parts: list[str] = []
    omitted: list[str] = []
    total = 0
    for path in paths:
        try:
            data = host.file_read(branch_id, path, timeout_ms=120_000)
        except Exception:
            continue
        if b"\x00" in data:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if not text.strip():
            continue

        truncated = False
        if len(text) > _MAX_FILE_CHARS:
            text = text[:_MAX_FILE_CHARS] + "\n... (truncated)"
            truncated = True
        remaining = _MAX_TOTAL_CHARS - total
        if remaining <= 0:
            omitted.append(path)
            continue
        if len(text) > remaining:
            text = text[:remaining] + "\n... (truncated)"
            truncated = True
        if truncated:
            omitted.append(path)

        parts.append(f"### {path}\n{text.rstrip()}")
        total += len(text) + 8

    status = f"{len(parts)} file(s)"
    if omitted:
        preview = ", ".join(omitted[:_OMMIT_PREVIEW])
        status += f"; omitted/truncated: {preview}"
    return status, "\n\n".join(parts)


@register(ARTIFACT_FILES_READERS, "generic_files")
def generic_files(
    node: Any,
    task: Any,
    *,
    host: Any,
    branch_id: str,
    wait_job: Any = None,
    **kwargs: Any,
) -> dict[str, str]:
    """Export the repository as ``{repo-relative path: full content}``.

    Unlike ``generic_repo`` (which produces a bounded prompt/answer string),
    this reader returns the real file contents the host needs to write the
    final deliverable back into the originating session. It reuses the same
    directory exclusions and UTF-8 gate, but does not truncate individual
    files to the prompt-oriented character budget; the host applies its own
    per-file/total byte caps before writing.
    """
    if wait_job is None:
        raise RuntimeError("generic_files requires wait_job for host command receipts")
    find_parts: list[str] = [f"find {REPO} -type f"]
    for skipped in _SKIP_DIRS:
        find_parts.append(f"-not -path '*{skipped}*'")
    find_parts.append("-print | sort")
    receipt = host.exec(
        branch_id,
        ["bash", "-lc", " ".join(find_parts)],
        cwd=REPO,
        timeout_ms=120_000,
        idempotency_key=operation_key(branch_id, "artifact-files:list", node.node_id),
    )
    listing = completed_output(wait_completed(receipt, wait_job))

    files: dict[str, str] = {}
    for line in listing.splitlines():
        path = line.strip()
        if not path:
            continue
        rel = os.path.relpath(path, WORKSPACE)
        if not rel or rel == "." or rel.startswith(".."):
            continue
        try:
            data = host.file_read(branch_id, path, timeout_ms=120_000)
        except Exception:
            continue
        if b"\x00" in data or len(data) > _MAX_FILE_BYTES:
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        files[rel] = text
    return files
