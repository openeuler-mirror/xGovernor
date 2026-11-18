"""DeepSWE tasks supplied by the benchmark runner."""

from pathlib import Path

from xgovernor_tasks.common.mode import ARTIFACT_READERS, LOADERS, register
from xgovernor_tasks.generic.task import GenericTask


@register(LOADERS, "deepswe_task")
class DeepSWETask(GenericTask):
    prompt_repo = "."

    def __init__(self, spec, *, goal: str, oracle_dir: Path, task_id: str, root_state_id: str):
        super().__init__(spec, goal=goal, task_id=task_id)
        self.root_state_id = root_state_id
        self._oracle_dir = oracle_dir

    def init_solver(self) -> str:
        return self.goal

    def step_solver(self, prior=None) -> str:
        # The benchmark hook receives the same raw task text on every turn.
        return self.goal

    @property
    def oracle_host_path(self) -> Path:
        return self._oracle_dir / self.spec.oracle_host_name


@register(ARTIFACT_READERS, "deepswe_patch")
def read_patch(node, task, *, host, branch_id, wait_job=None):
    return "", host.diff(branch_id, task.root_state_id, timeout_ms=60_000)
