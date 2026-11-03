"""SWE-bench plugin: testcases.py oracle, patch artifact and official connector."""

from src.res.mode import MODES, ModeSpec
from src.res.mode import OFFICIALS, register
from . import artifact, loader, materializer, scorer  # noqa: F401
from .official import evaluate


def configure_cli(parser) -> None:
    parser.add_argument("--testbench", choices=("swepro", "swelite", "sweverified"), default="sweverified")
    parser.add_argument("--instance-id", required=True, help="Exact SWE-bench instance_id")
    parser.add_argument("--split", default="test")


@register(OFFICIALS, "swe_official")
def submit(node, task, output_root, *, host, branch_id, wait_job):
    return evaluate(node, task, output_root, host=host, branch_id=branch_id, wait_job=wait_job)

MODES.register(
    ModeSpec(
        id="sweverified",
        name="SWE-bench Verified",
        prompts_dir="sweverified/prompts",
        load_task="swe_task",
        scorer="pytest",
        materialize="swe-instance",
        artifact_reader="git_diff",
        official_eval="swe_official",
        oracle_rel_path="testcases.py",
        oracle_host_name="testcases.py",
        env={"E2B_TEMPLATE_NAME": "swepro-docker"},
        configure_cli=configure_cli,
    )
)
