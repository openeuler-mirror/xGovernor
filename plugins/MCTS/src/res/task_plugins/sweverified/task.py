"""SWE-bench implementation of the shared task interface."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from src.res.mode import MATERIALIZERS, ModeSpec

SANDBOX_WORKSPACE = "/home/user/workspace"
SANDBOX_REPO = f"{SANDBOX_WORKSPACE}/repo"
SWEPRO_DATASET = "ScaleAI/SWE-bench_Pro"
SWELITE_DATASET = "princeton-nlp/SWE-bench_Lite"
SWEVERIFIED_DATASET = "SWE-bench/SWE-bench_Verified"
FORBIDDEN_COLUMNS = {"patch", "test_patch"}


def _format_field(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def _constraints_block(*lines: str) -> str:
    return "Constraints:\n" + "\n".join(f"- {line}" for line in lines)


# ---------------------------------------------------------------------------
# SWE-bench（swepro / swelite / sweverified）
# ---------------------------------------------------------------------------
class SweTask:
    """SWE-bench 风格任务：author 写 testcases.py，solver 改产品代码，pytest 打分。"""

    def __init__(self, spec: ModeSpec, *, testbench: str, row: dict, split: str):
        self.spec = spec
        self.testbench = testbench
        self.row = row
        self.split = split

    # --- identity --------------------------------------------------------
    @property
    def instance_id(self) -> str:
        return str(self.row["instance_id"])

    @property
    def dataset_name(self) -> str:
        return {
            "swepro": SWEPRO_DATASET,
            "swelite": SWELITE_DATASET,
            "sweverified": SWEVERIFIED_DATASET,
        }[self.testbench]

    @property
    def display_name(self) -> str:
        return {
            "swepro": "SWE-bench Pro",
            "swelite": "SWE-bench Lite",
            "sweverified": "SWE-bench Verified",
        }[self.testbench]

    @property
    def docker_image(self) -> str:
        if self.testbench != "swepro":
            raise AttributeError("docker_image is only defined for swepro")
        return "jefzda/sweap-images:" + str(self.row["dockerhub_tag"])

    @property
    def testcases_path(self) -> str:
        return f"{SANDBOX_REPO}/{self.spec.oracle_rel_path}"

    # --- task text -------------------------------------------------------
    def issue_text(self) -> str:
        if self.testbench in ("swelite", "sweverified"):
            sections = [("PROBLEM_STATEMENT", self.row.get("problem_statement", ""))]
        else:
            sections = [("PROBLEM_STATEMENT", self.row.get("problem_statement", ""))]
            if self.row.get("requirements"):
                sections.append(("REQUIREMENTS", self.row["requirements"]))
            if self.row.get("interface"):
                sections.append(("INTERFACE", self.row["interface"]))
        return "\n\n".join(
            f"[{name}]\n{_format_field(value)}"
            for name, value in sections
            if _format_field(value)
        )

    def init_solver(self) -> str:
        return "\n\n".join(
            [
                f"Repository: {SANDBOX_REPO}",
                "Task: Fix the bug described below so the behavior illustrated in the problem statement is correct.",
                _constraints_block(
                    f"You may modify only product code under {SANDBOX_REPO}/ that is relevant to this bug.",
                    "You may run the repository's own existing tests to verify your fix "
                    "(always name the project's own test dirs/files explicitly, e.g. `pytest tests/`; "
                    "do not run bare `pytest` from the repo root).",
                    "Use /home/user/workspace/run_in_instance.sh for commands inside the instance container.",
                ),
                self.issue_text(),
            ]
        )

    def step_solver(self, prior: dict[str, str] | None = None) -> str:
        if not prior or not prior.get("pytest_output"):
            raise ValueError("step_solver requires parent pytest_output")
        score = prior.get("feedback", "")
        header = f"Test result for {self.testcases_path}"
        if score != "":
            header = f"{header} (score={score})"
        return "\n\n".join(
            [
                f"Repository: {SANDBOX_REPO}",
                f"The orchestrator ran tests on {self.testcases_path}. "
                "Analyze the failure below and fix the product code.",
                f"{header}:\n\n{prior['pytest_output']}",
                f"Task: Analyze the test failure and fix the issue in product code under {SANDBOX_REPO}/. "
                "If the output instead shows a generated-test or execution-mechanism defect, leave product code and "
                "test infrastructure unchanged and report it to Author as test_bug.",
                _constraints_block(
                    f"Do not modify or run {self.testcases_path}. You may READ it to gauge current coverage.",
                    "Base your analysis on the orchestrator test output above; do not re-run "
                    f"{self.testcases_path} to confirm.",
                    f"Avoid bare `pytest` from the repo root which would collect {self.testcases_path}; "
                    "leave its execution to the orchestrator.",
                    "You may run the repository's own existing tests to verify your fix "
                    "(scope the command to the project's own test dirs/files, e.g. `pytest tests/`).",
                    "Do not install packages or adjust test execution infrastructure to rescue generated tests; "
                    "use /home/user/workspace/run_in_instance.sh only for scoped product/repository checks.",
                ),
            ]
        )

    def init_author(self, *, reference_example_paths: list[str] | None = None) -> str:
        ready = (
            f"Repository is at {SANDBOX_REPO} (materialized from the {self.display_name} instance). "
            "Python/pytest/pip run inside the instance container via /home/user/workspace/run_in_instance.sh; "
            f"read the repository as evidence, but write only {self.testcases_path}."
        )
        parts = [ready]
        parts.append(self.issue_text())
        return "\n\n".join(parts)

    def step_author(self, prior: dict[str, str], *, node_id: str = "") -> str:
        snapshot = (prior.get("memory") or prior.get("query") or "").strip()
        header = (
            "Merged solver findings from memory — revise testcases.py once using this "
            "snapshot. Raise the bar for gaps; fix/remove broken oracles for test_bug."
        )
        body = snapshot if snapshot else "(memory snapshot empty — no test revision needed)"
        return f"{header}\n\n{body}\n\n{self.issue_text()}"

    # --- materialize -----------------------------------------------------
    def materialize_script(self) -> str:
        return MATERIALIZERS[self.spec.materialize](self)

    @property
    def oracle_host_path(self) -> Path:
        return Path(os.environ.get("TMPDIR", "/tmp")) / "xiaoo-mcts-oracles" / self.spec.oracle_host_name
