from src.res.mode import ARTIFACT_READERS, register
REPO="/home/user/workspace/repo"
@register(ARTIFACT_READERS, "git_diff")
def git_diff(node, task, *, host, branch_id, wait_job=None, **kwargs):
    if wait_job is None:
        raise RuntimeError("git_diff requires wait_job for host command receipts")
    status = wait_job(host.exec(branch_id, ["bash", "-lc", "git status --short"], cwd=REPO,
                               timeout_ms=60000, idempotency_key="artifact:status"))
    diff = wait_job(host.exec(branch_id, ["bash", "-lc", "git diff --no-ext-diff --binary HEAD --"], cwd=REPO,
                             timeout_ms=60000, idempotency_key="artifact:diff"))
    def out(v):
        if isinstance(v, dict):
            return v.get("output", v.get("stdout", ""))
        return getattr(v, "output", getattr(v, "stdout", str(v)))
    return out(status), out(diff)
