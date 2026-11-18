"""Generic code-task plugin: open-ended tasks run on the local docker backend."""

from xgovernor_tasks.common.mode import MODES, ModeSpec

from . import artifact, loader, materializer, scorer  # noqa: F401


def configure_cli(parser) -> None:
    parser.add_argument("--task-id", help="Generic task id")
    parser.add_argument("--goal", default="", help="Task goal text")
    parser.add_argument("--task-spec", default=None, help="Path to a generic task-spec JSON file")


MODES.register(
    ModeSpec(
        id="generic",
        name="Generic Code Task",
        prompts_dir="generic/prompts",
        load_task="generic_task",
        scorer="generic_test",
        materialize="generic-noop",
        artifact_reader="generic_repo",
        artifact_files_reader="generic_files",
        official_eval=None,
        oracle_rel_path="verify.sh",
        oracle_host_name="verify.sh",
        runtime_backend="docker",
        configure_cli=configure_cli,
    )
)
