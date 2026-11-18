"""Generic code-task implementation of the shared task interface."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from xgovernor_tasks.common.mode import MATERIALIZERS, ModeSpec

SANDBOX_WORKSPACE = "/home/user/workspace"
SANDBOX_REPO = f"{SANDBOX_WORKSPACE}/repo"
VERIFY_SCRIPT = f"{SANDBOX_REPO}/verify.sh"
TASK_JSON = f"{SANDBOX_WORKSPACE}/.mcts/task.json"


class GenericTask:
    """A user-provided open-ended code task with no dataset or official judge."""

    prompt_repo = SANDBOX_REPO

    def __init__(
        self,
        spec: ModeSpec,
        *,
        task_id: str,
        goal: str = "",
        task_spec: dict[str, Any] | None = None,
    ):
        self.spec = spec
        self.task_id = task_id
        self.goal = goal
        self.task_spec = task_spec or {}

    # --- identity --------------------------------------------------------
    @property
    def instance_id(self) -> str:
        return self.task_id

    @property
    def display_name(self) -> str:
        return "Generic Code Task"

    def issue_text(self) -> str:
        return self.goal or self.task_id

    # --- prompts ---------------------------------------------------------
    def init_solver(self) -> str:
        goal = self.issue_text()
        return "\n\n".join(
            [
                f"Repository: {self.prompt_repo}",
                "Task: implement or fix the product code so it satisfies the requirement below.",
                goal,
                "Work inside the repository with the tools available to you. You may run "
                "your own checks, but the final deliverable is the product code under the "
                "repository. Do not modify the Author's verification script.",
            ]
        )

    def step_solver(self, prior: dict[str, Any] | None = None) -> str:
        prior = prior or {}
        output = str(prior.get("pytest_output") or prior.get("feedback") or "")
        goal = self.issue_text()
        return "\n\n".join(
            [
                f"Repository: {self.prompt_repo}",
                "The latest verification finished with the result below. Analyze it and "
                "continue fixing/implementing the product code.",
                "Verification result:",
                output or "(no output)",
                "Original task:",
                goal,
                "Only change product code needed to satisfy the task; if the verification "
                "itself looks wrong, report that concern to the Author instead of gaming it.",
            ]
        )

    def init_author(self, *, reference_example_paths: list[str] | None = None) -> str:
        goal = self.issue_text()
        return "\n\n".join(
            [
                "You are the verification author for a generic code task.",
                f"Repository: {self.prompt_repo}",
                "Write the verification entrypoint at "
                f"{self.prompt_repo}/{self.spec.oracle_rel_path} that checks whether the "
                "product code satisfies the task.",
                "Read the repository as evidence, but write only the verification script.",
                "Task:",
                goal,
            ]
        )

    def step_author(self, prior: dict[str, Any], *, node_id: str = "") -> str:
        snapshot = (prior.get("memory") or prior.get("query") or "").strip()
        return "\n\n".join(
            [
                "Merged solver findings — revise the verification script once using this snapshot.",
                snapshot or "(no new findings)",
                self.issue_text(),
            ]
        )

    # --- materialize -----------------------------------------------------
    def materialize_script(self) -> str:
        return MATERIALIZERS[self.spec.materialize](self)

    @property
    def oracle_host_path(self) -> Path:
        return (
            Path(os.environ.get("TMPDIR", "/tmp"))
            / "xiaoo-mcts-oracles"
            / self.spec.oracle_host_name
        )

    def task_json(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "goal": self.issue_text(),
        }
