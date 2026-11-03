"""ZSoft L1 agent bundle builder (anti-cheat filtered, public assets only).

The agent bundle transferred into the E2B sandbox contains only the task
``public/`` assets (README, task.json, repo-vul/...). The vulnerable source
working tree is *not* pre-downloaded on the host: it is downloaded directly
inside the E2B sandbox by the materialize script (like SWE-bench pulls its
instance content inside the sandbox).

Judge-private content is rejected with an explicit error:
- judge-private *directories* (``private/``, ``judge/``, task state, runs,
  submission entrypoints such as ``submit.sh``) are blocked only at the task
  top level of the bundle (``task/<name>``) — source-internal directories that
  happen to share a name (e.g. ``task/repo-local/include/private``) are
  legitimate source and allowed;
- judge-private *files* (reference PoC, fix patch, oracle, ``submit.sh``,
  context manifests) and forbidden suffixes are blocked anywhere in the tree,
  since filename matches do not depend on directory depth.

The official judge always runs host-side, so the sandbox never sees those
assets.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

# Judge-private directories / files that must never reach the agent sandbox.
FORBIDDEN_DIR_NAMES = {
    "private",
    "judge",
    ".task-state",
    ".runs",
    ".cache",
    ".work",
    ".git",
    ".hg",
    ".svn",
    "logs",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
}
FORBIDDEN_FILE_NAMES = {
    "vulnerable.fix.patch",
    "reference-poc",
    "negative-poc",
    "source-manifest.json",
    "prepare.context.json",
    "evaluation.context.json",
    "image-refs.json",
    # 绝不把官测 submission 入口交给 agent：server 会返回完整 EvaluationResult
    # （含 vuln/fix 退出码），agent 若拿到 submit.sh 就能直接探测官测。
    "submit.sh",
}
FORBIDDEN_SUFFIXES = {".pyc", ".pyo", ".pyd"}

TASK_JSON_NAME = "task.json"
BUNDLE_TASK_DIR = "task"
BUNDLE_TASK_MD = "TASK.md"


@dataclass(frozen=True)
class TaskManifest:
    task_id: str
    title: str
    description: str
    tags: list[str] = field(default_factory=list)


def load_task_manifest(task_root: Path) -> TaskManifest:
    path = task_root / TASK_JSON_NAME
    if not path.is_file():
        raise FileNotFoundError(f"missing task.json: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    tags = raw.get("tags") or []
    return TaskManifest(
        task_id=str(raw["task_id"]),
        title=str(raw.get("title") or raw["task_id"]),
        description=str(raw.get("description") or ""),
        tags=[str(t) for t in tags] if isinstance(tags, list) else [],
    )


def is_kernel_task(tags: list[str]) -> bool:
    return any(tag.lower() == "kernel" for tag in tags)


def poc_filename_for(manifest: TaskManifest) -> str:
    """PoC filename convention in the sandbox (kernel tasks are single-file C)."""
    return "poc.c" if is_kernel_task(manifest.tags) else "poc.py"


def _copy_tree(src: Path, dst: Path) -> int:
    count = 0
    for entry in src.rglob("*"):
        if entry.is_dir():
            continue
        rel = entry.relative_to(src)
        target = dst / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(entry, target)
        count += 1
    return count


def _forbidden_dir_at_task_top(path: Path) -> bool:
    """True when a forbidden directory name sits at the bundle's task top level.

    Archive/staging layout: <root>/task/<entry>...  Only ``task/`` direct
    children are judge-private structures; the same name inside the source tree
    (e.g. ``task/repo-local/include/private``) is legitimate source content.
    """
    parts = path.parts
    return len(parts) >= 2 and parts[0] == BUNDLE_TASK_DIR and parts[1] in FORBIDDEN_DIR_NAMES


def _assert_no_forbidden(staging: Path) -> None:
    task_root_dir = staging / BUNDLE_TASK_DIR
    if task_root_dir.is_dir():
        for entry in task_root_dir.iterdir():
            if entry.is_dir() and entry.name in FORBIDDEN_DIR_NAMES:
                raise RuntimeError(f"forbidden directory leaked into agent bundle: {entry}")
    for entry in staging.rglob("*"):
        if entry.is_file():
            if entry.name in FORBIDDEN_FILE_NAMES:
                raise RuntimeError(f"forbidden file leaked into agent bundle: {entry}")
            if entry.suffix.lower() in FORBIDDEN_SUFFIXES:
                raise RuntimeError(f"forbidden file leaked into agent bundle: {entry}")


@dataclass(frozen=True)
class AgentBundle:
    task_id: str
    tar_path: Path
    sha256: str
    file_count: int
    poc_filename: str


def build_agent_bundle(
    task_root: Path,
    *,
    out_dir: Path,
    extra_trees: dict[str, Path] | None = None,
) -> AgentBundle:
    """Build the anti-cheat filtered agent bundle tarball for one ZSoft task.

    Only ``public/`` content enters the bundle; the vulnerable source working
    tree is downloaded inside the E2B sandbox at materialize time, unless a
    local scrubbed snapshot is supplied via ``extra_trees`` (arcname -> dir,
    e.g. ``{"repo-local": <cached-snapshot>}``). The snapshot is copied into
    the bundle under ``task/<arcname>`` and is still subject to the same
    anti-cheat filtering as the rest of the bundle.
    """
    manifest = load_task_manifest(task_root)
    public_dir = task_root / "public"
    if not public_dir.is_dir():
        raise FileNotFoundError(f"missing public bundle: {public_dir}")

    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    tar_path = out_dir / f"zsoft-{manifest.task_id}-agent-bundle.tar.gz"

    with tempfile.TemporaryDirectory(prefix="zsoft-bundle-") as tmp:
        staging = Path(tmp)
        task_dst = staging / BUNDLE_TASK_DIR
        task_dst.parent.mkdir(parents=True, exist_ok=True)

        file_count = _copy_tree(public_dir, task_dst)

        for arcname, src_dir in sorted((extra_trees or {}).items()):
            if not src_dir.is_dir():
                raise RuntimeError(f"extra tree missing for {arcname!r}: {src_dir}")
            dst = task_dst / arcname
            shutil.copytree(
                src_dir,
                dst,
                symlinks=True,
                ignore=shutil.ignore_patterns(".source-lock.json"),
            )
            file_count += sum(1 for entry in dst.rglob("*") if not entry.is_dir())

        readme = public_dir / "README.md"
        if readme.is_file():
            (staging / BUNDLE_TASK_MD).write_text(
                readme.read_text(encoding="utf-8"), encoding="utf-8"
            )
            file_count += 1

        task_json = task_root / TASK_JSON_NAME
        if task_json.is_file():
            shutil.copy2(task_json, staging / TASK_JSON_NAME)
            file_count += 1

        _assert_no_forbidden(staging)

        digest = hashlib.sha256()
        with tarfile.open(tar_path, "w:gz") as tar:
            for entry in sorted(staging.rglob("*")):
                if entry.is_dir():
                    continue
                arcname = entry.relative_to(staging).as_posix()
                tar.add(entry, arcname=arcname)
                with open(entry, "rb") as fh:
                    digest.update(fh.read())
    return AgentBundle(
        task_id=manifest.task_id,
        tar_path=tar_path,
        sha256=digest.hexdigest(),
        file_count=file_count,
        poc_filename=poc_filename_for(manifest),
    )


def verify_bundle(tar_path: Path) -> dict:
    """Verify an agent bundle tarball contains no judge-private content."""
    forbidden = []
    file_count = 0
    with tarfile.open(tar_path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue
            file_count += 1
            name = Path(member.name)
            if _forbidden_dir_at_task_top(name):
                forbidden.append(member.name)
            if name.name in FORBIDDEN_FILE_NAMES:
                forbidden.append(member.name)
            if name.suffix.lower() in FORBIDDEN_SUFFIXES:
                forbidden.append(member.name)
    if forbidden:
        raise RuntimeError(
            f"agent bundle contains judge-private content: {forbidden}"
        )
    return {"tar_path": str(tar_path), "file_count": file_count, "ok": True}


def bundle_bytes(tar_path: Path) -> bytes:
    return tar_path.read_bytes()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        sys.exit("usage: python3 zsoft_bundle.py <task_dir> [out_dir]")
    task_dir = Path(sys.argv[1])
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else task_dir.parent / ".bundle-out"
    bundle = build_agent_bundle(
        task_dir,
        out_dir=out_dir,
    )
    print(json.dumps(
        {
            "task_id": bundle.task_id,
            "tar_path": str(bundle.tar_path),
            "sha256": bundle.sha256,
            "file_count": bundle.file_count,
            "poc_filename": bundle.poc_filename,
        },
        indent=2,
    ))
    verify_bundle(bundle.tar_path)
