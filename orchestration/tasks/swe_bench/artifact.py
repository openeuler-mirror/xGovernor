"""Export candidate changes without writing to the candidate repository."""
import shlex

from xgovernor_tasks.common.benchmark_util import operation_key
from xgovernor_tasks.common.mode import ARTIFACT_READERS, register

REPO = "/home/user/workspace/repo"

_DIFF_SCRIPT = r'''set -euo pipefail
objects=$(git rev-parse --git-path objects)
objects=$(cd -- "$objects" && pwd -P)
case "$objects" in *$'\n'*) echo "unsupported object path" >&2; exit 1 ;; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/mcts-artifact.XXXXXX")
trap 'rm -rf -- "$tmp"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$tmp/objects/info"
# An alternates file treats colons as path characters, not list separators.
printf '%s\n' "$objects" > "$tmp/objects/info/alternates"
export GIT_INDEX_FILE="$tmp/index" GIT_OBJECT_DIRECTORY="$tmp/objects"
unset GIT_ALTERNATE_OBJECT_DIRECTORIES
git() { command git -c core.filemode=true -c core.splitIndex=false "$@"; }
git read-tree HEAD
git add -A -- . ORACLE_PATH
git diff --cached --no-ext-diff --no-textconv --no-renames --binary --full-index HEAD -- . ORACLE_PATH
'''


def _output(job, action):
    def field(name, default=None):
        return job.get(name, default) if isinstance(job, dict) else getattr(job, name, default)

    if (field("status", "completed") != "completed" or field("exit_code") != 0
            or field("truncated", False)):
        raise RuntimeError(f"could not {action}")
    output = field("output", field("stdout", ""))
    if not isinstance(output, str):
        raise RuntimeError(f"could not {action}: invalid command output")
    return output


@register(ARTIFACT_READERS, "git_diff")
def git_diff(node, task, *, host, branch_id, wait_job=None, **kwargs):
    if wait_job is None:
        raise RuntimeError("git_diff requires wait_job for host command receipts")
    status = wait_job(host.exec(branch_id, ["bash", "-lc", "GIT_OPTIONAL_LOCKS=0 git status --short"], cwd=REPO,
                               timeout_ms=60000, idempotency_key=operation_key(branch_id, "artifact:status")))
    status = _output(status, "read product status")
    oracle = shlex.quote(f":(top,exclude,literal){task.spec.oracle_rel_path}")
    script = _DIFF_SCRIPT.replace("ORACLE_PATH", oracle)
    diff = wait_job(host.exec(branch_id, ["bash", "-lc", script], cwd=REPO,
                             timeout_ms=60000, idempotency_key=operation_key(branch_id, "artifact:diff", task.spec.oracle_rel_path)))
    return status, _output(diff, "export product diff")
