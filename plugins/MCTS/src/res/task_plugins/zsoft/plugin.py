"""ZSoft plugin: verify.py oracle, single-file PoC and judge connector."""

from src.res.mode import MODES, ModeSpec
from src.res.mode import OFFICIALS, register
from . import artifact, loader, materializer, scorer  # noqa: F401
from .official import evaluate


def configure_cli(parser) -> None:
    parser.add_argument("--task-id", help="ZSoft L1 task id")
    parser.add_argument("--tasks-dir", default=None, help="zsoft-l1 tasks directory")
    parser.add_argument("--list-tasks", action="store_true", help="list available task ids")


@register(OFFICIALS, "zsoft_official")
def submit(node, task, output_root, *, host, branch_id, wait_job):
    return evaluate(node, task, output_root, host=host, branch_id=branch_id, wait_job=wait_job)

MODES.register(
    ModeSpec(
        id="zsoft",
        name="ZSoft L1 (CVE PoC)",
        prompts_dir="zsoft/prompts",
        load_task="zsoft_task",
        scorer="zsoft-verify",
        materialize="zsoft-bundle",
        artifact_reader="poc_file",
        official_eval="zsoft_official",
        oracle_rel_path="verify.py",
        oracle_host_name="verify.py",
        env={
            "E2B_TEMPLATE_NAME": "zsoft-docker",
            # judge-trial 评审官 profile（可选角色：仅当此变量存在时 宿主 RPC
            # 才注册 trial-assessor，sweverified 等其他模式不受影响）。
            "XIAOO_PROMPT_TRIAL_ASSESSOR": "zsoft/prompts/prompt_trial_assessor.txt",
        },
        configure_cli=configure_cli,
    )
)
