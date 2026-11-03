"""Pure benchmark helpers and the asynchronous Host adapter shared by scorers."""
from __future__ import annotations

import contextlib
import contextvars
import re
from pathlib import Path
from typing import Any, Callable, Iterator

REPO_DIR = "/home/user/workspace/repo"
REPO_TESTCASES = f"{REPO_DIR}/testcases.py"
ENSURE_INSTANCE = "/home/user/workspace/ensure_instance.sh"
RUN_IN_INSTANCE = "/home/user/workspace/run_in_instance.sh"
TESTCASES_VERSION_MARKER = "# xiaoo-testcases-version:"
_TESTCASES_VERSION_RE = re.compile(r"^\s*#\s*xiaoo-testcases-version:\s*(\d+)\s*$", re.M)


def completed_output(job: Any) -> str:
    if isinstance(job, dict):
        return str(job.get("output", job.get("stdout", "")) or "")
    return str(getattr(job, "output", getattr(job, "stdout", "")) or "")


def completed_exit_code(job: Any) -> int | None:
    value = job.get("exit_code") if isinstance(job, dict) else getattr(job, "exit_code", None)
    return int(value) if isinstance(value, int) and not isinstance(value, bool) else None


def wait_completed(receipt: dict[str, Any], wait_job: Callable[[Any], Any]) -> Any:
    if not isinstance(receipt, dict) or not isinstance(receipt.get("job_id"), str):
        raise ValueError("host operation did not return a valid job receipt")
    return wait_job(receipt)


def parse_pytest_pass_rate(stdout: str, stderr: str = "") -> float:
    """passed/(passed+failed+errors), excluding skipped and xfailed tests."""
    text = f"{stdout}\n{stderr}"
    summaries = re.findall(r"=+\s*(.*?)\s+in\s+[\d.]+s\s*=+", text, re.S)
    blob = summaries[-1] if summaries else text
    def count(name: str) -> int:
        match = re.search(rf"(\d+)\s+{name}\b", blob)
        return int(match.group(1)) if match else 0
    passed, failed = count("passed"), count("failed")
    match = re.search(r"(\d+)\s+errors?\b", blob)
    errors = int(match.group(1)) if match else 0
    total = passed + failed + errors
    return passed / total if total else 0.0


def format_pytest_output(result: Any) -> str:
    return completed_output(result)


def score_pytest(result: Any) -> tuple[float, str]:
    text = format_pytest_output(result)
    return parse_pytest_pass_rate(text), text


def harvest_tag(result: Any, tag: str) -> str:
    text = completed_output(result)
    matches = re.findall(rf"<{re.escape(tag)}>(.*?)</{re.escape(tag)}>", text, re.S | re.I)
    return matches[-1].strip() if matches else ""


def harvest_triplet(result: Any) -> dict[str, str]:
    return {name: harvest_tag(result, name) for name in ("solution", "reflection", "query")}


def pull_testcases(host, branch_id: str, path: Path, *, repo_path: str = REPO_TESTCASES) -> bytes:
    data = host.file_read(branch_id, repo_path, timeout_ms=120_000)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return data


def push_testcases(host, branch_id: str, path: Path, *, repo_path: str = REPO_TESTCASES) -> None:
    if path.is_file():
        host.file_write(branch_id, repo_path, path.read_bytes(), timeout_ms=120_000, idempotency_key=f"oracle:{repo_path}")


def ensure_instance(host, branch_id: str, *, wait_job: Callable[[Any], Any]) -> Any:
    receipt = host.exec(branch_id, ["bash", "-lc", ENSURE_INSTANCE], cwd="/home/user/workspace", timeout_ms=120_000, idempotency_key="instance:ensure")
    return wait_completed(receipt, wait_job)


def run_solver_pytest(host, branch_id: str, *, wait_job: Callable[[Any], Any], oracle: str = "testcases.py") -> Any:
    ensure_instance(host, branch_id, wait_job=wait_job)
    receipt = host.exec(branch_id, ["bash", "-lc", f"{RUN_IN_INSTANCE} pytest {oracle}"], cwd=REPO_DIR, timeout_ms=600_000, idempotency_key=f"pytest:{oracle}")
    return wait_completed(receipt, wait_job)


def read_testcases_version(data_or_path: bytes | str | Path) -> int | None:
    if isinstance(data_or_path, Path):
        if not data_or_path.is_file():
            return None
        text = data_or_path.read_text(encoding="utf-8", errors="replace")
    elif isinstance(data_or_path, bytes):
        text = data_or_path.decode("utf-8", errors="replace")
    else:
        text = str(data_or_path)
    match = _TESTCASES_VERSION_RE.search(text)
    return int(match.group(1)) if match else None


def stamp_testcases_version(path: Path, version: int) -> None:
    if not path.is_file():
        raise RuntimeError(f"missing oracle: {path}")
    text = path.read_text(encoding="utf-8", errors="replace")
    marker = f"{TESTCASES_VERSION_MARKER} {int(version)}\n"
    text = _TESTCASES_VERSION_RE.sub(marker.rstrip("\n"), text, count=1) if _TESTCASES_VERSION_RE.search(text) else marker + text
    path.write_text(text, encoding="utf-8")


def strip_testcases_version(data: bytes) -> bytes:
    text = _TESTCASES_VERSION_RE.sub("", data.decode("utf-8", errors="replace"), count=1)
    return text[1:].encode() if text.startswith("\n") else text.encode()


_SCORING_NODE: contextvars.ContextVar[str | None] = contextvars.ContextVar("scoring_node", default=None)


@contextlib.contextmanager
def set_scoring_node(node_id: str) -> Iterator[None]:
    token = _SCORING_NODE.set(node_id)
    try:
        yield
    finally:
        _SCORING_NODE.reset(token)


def scoring_node() -> str | None:
    return _SCORING_NODE.get()


def query_has_test_bug(query_text: str) -> bool:
    return bool(re.search(r'kind\s*=\s*"test_bug"', query_text or "", re.I))
