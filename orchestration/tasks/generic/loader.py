"""Task loader for the generic code-task mode."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from xgovernor_tasks.common.mode import LOADERS, ModeSpec, register

from .task import GenericTask


@register(LOADERS, "generic_task")
def generic_task(
    spec: ModeSpec,
    *,
    task_id: str | None = None,
    goal: str = "",
    task_spec: str | None = None,
    **kwargs: Any,
) -> GenericTask:
    """Build a GenericTask from CLI arguments (optionally a task-spec JSON file)."""
    if not task_id:
        raise SystemExit("--task-id is required for the generic mode")
    spec_dict: dict[str, Any] = {}
    if task_spec:
        spec_path = Path(task_spec)
        if not spec_path.is_file():
            raise SystemExit(f"--task-spec is not a file: {task_spec!r}")
        spec_dict = json.loads(spec_path.read_text(encoding="utf-8"))
    if not goal:
        goal = str(spec_dict.get("goal") or "")
    return GenericTask(
        spec,
        task_id=task_id,
        goal=goal,
        task_spec=spec_dict,
    )
