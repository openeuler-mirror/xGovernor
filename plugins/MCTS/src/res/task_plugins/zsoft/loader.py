"""任务加载器注册：把 CLI 参数（testbench+index / task-id）变成 adapter 任务对象。

新模式若任务形状与 SWE/ZSoft 同构（「给一个程序 + 写一个 oracle + 改一个产物」），
直接复用这两个 loader；只有数据源完全不同时才注册新 loader（成本 2，见 README）。
"""

from __future__ import annotations

from typing import Any
from pathlib import Path

from src.res.mode import LOADERS, ModeSpec, register
from .task import ZSoftTask, default_tasks_dir

@register(LOADERS, "zsoft_task")
def zsoft_task(
    spec: ModeSpec, *, task_id: str | None, tasks_dir: str | None = None,
    list_tasks: bool = False, **kwargs: Any,
) -> ZSoftTask:
    """构造 ZSoftTask（manifest + public README + 反作弊 bundle + source lock）。"""
    if list_tasks:
        root = default_tasks_dir() if tasks_dir is None else Path(tasks_dir)
        print("\n".join(sorted(path.name for path in root.iterdir() if path.is_dir())))
        raise SystemExit(0)
    if not task_id:
        raise SystemExit("--task-id is required for the zsoft mode")
    return ZSoftTask.create(spec, task_id=task_id, tasks_dir=tasks_dir)
