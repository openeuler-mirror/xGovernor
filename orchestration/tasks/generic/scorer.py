"""Generic search reward: run the Author-generated verification entrypoint.

The generic checker is a language-neutral executable script at
``/home/user/workspace/repo/verify.sh``. The Author decides which test
framework or build/run commands to invoke (pytest, npm test, mvn test, go test,
gcc + binary run, ...) and reports one ``PASS``/``FAIL`` line per check; the
script exits 0 only when every check passes.
"""

from __future__ import annotations

import re
import shlex
from typing import Any, Callable

from xgovernor_tasks.common.benchmark_util import (
    completed_exit_code,
    completed_output,
    operation_key,
    push_testcases,
    wait_completed,
)
from xgovernor_tasks.common.mode import SCORERS, register

from .task import SANDBOX_REPO

_PASS_RE = re.compile(r"^\s*(?:[^:\n]*:\s*)?PASS\b", re.MULTILINE | re.IGNORECASE)
_FAIL_RE = re.compile(r"^\s*(?:[^:\n]*:\s*)?FAIL\b", re.MULTILINE | re.IGNORECASE)


@register(SCORERS, "generic_test")
def generic_test(
    host: Any,
    branch_id: str,
    task: Any,
    oracle: Any = None,
    *,
    wait_job: Callable[[Any], Any],
) -> tuple[float, str]:
    """Run the Author's verification script in the branch."""
    script = task.spec.oracle_rel_path
    if oracle is not None and getattr(oracle, "is_file", lambda: False)():
        push_testcases(host, branch_id, oracle, repo_path=f"{SANDBOX_REPO}/{script}")
    receipt = host.exec(
        branch_id,
        ["bash", "-lc", f"bash {shlex.quote(script)}"],
        cwd=SANDBOX_REPO,
        timeout_ms=900_000,
        idempotency_key=operation_key(branch_id, "generic-test", script),
    )
    job = wait_completed(receipt, wait_job)
    output = completed_output(job)
    exit_code = completed_exit_code(job)
    passes = len(_PASS_RE.findall(output))
    fails = len(_FAIL_RE.findall(output))
    # A non-zero exit with no FAIL line means the script aborted before it could
    # mark the failing check (for example `set -e`). Never reward that as full.
    if exit_code != 0 and fails == 0 and passes > 0:
        fails = 1
    if passes or fails:
        score = passes / (passes + fails)
    else:
        # No PASS/FAIL markers at all violates the output contract.
        score = 0.0
    return score, output
