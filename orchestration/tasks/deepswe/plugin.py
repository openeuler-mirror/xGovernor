"""DeepSWE benchmark integration for a prepared task image and OpenCode."""

from xgovernor_tasks.common.mode import MODES, ModeSpec
from .opencode import DeepSWEOpenCodeAdapter
from .task import DeepSWETask  # Registers the task loader and artifact reader.


MODES.register(
    ModeSpec(
        id="deepswe",
        name="DeepSWE",
        prompts_dir="deepswe/prompts",
        load_task="deepswe_task",
        scorer="generic_test",
        materialize="generic-noop",
        artifact_reader="deepswe_patch",
        oracle_rel_path="verify.sh",
        oracle_host_name="verify.sh",
        runtime_backend="opencode",
        opencode_adapter=DeepSWEOpenCodeAdapter,
    )
)
