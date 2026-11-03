"""SWE search reward: generated oracle pytest pass rate in the instance."""
from src.res.mode import SCORERS, register
from src.res.benchmark_util import push_testcases, run_solver_pytest, score_pytest


@register(SCORERS, 'pytest')
def pytest(host, branch_id, task, oracle=None, *, wait_job):
    # The author writes the oracle in the live branch.  A local oracle path is
    # optional for CLI integrations that materialize it outside the plugin.
    if oracle is not None and getattr(oracle, 'is_file', lambda: False)():
        repo_path = f'/home/user/workspace/repo/{task.spec.oracle_rel_path}'
        push_testcases(host, branch_id, oracle, repo_path=repo_path)
    result = run_solver_pytest(host, branch_id, wait_job=wait_job, oracle=task.spec.oracle_rel_path)
    return score_pytest(result)
