"""任务加载器注册：把 CLI 参数（testbench+instance-id / task-id）变成 adapter 任务对象。

新模式若任务形状与 SWE/ZSoft 同构（「给一个程序 + 写一个 oracle + 改一个产物」），
直接复用这两个 loader；只有数据源完全不同时才注册新 loader（成本 2，见 README）。
"""

from __future__ import annotations

from typing import Any

from .task import (
    FORBIDDEN_COLUMNS,
    SWEPRO_DATASET,
    SWELITE_DATASET,
    SWEVERIFIED_DATASET,
    SweTask,
)
from xgovernor_tasks.common.mode import LOADERS, ModeSpec, register


@register(LOADERS, "swe_task")
def swe_task(
    spec: ModeSpec,
    *,
    testbench: str,
    instance_id: str,
    split: str = "test",
    prepared_root: bool = False,
    official_python: str | None = None,
    instance_file: str | None = None,
    **kwargs: Any,
) -> SweTask:
    """从 HuggingFace 数据集加载 SWE-bench 实例，剔除 patch/test_patch 等禁止列。"""
    if testbench not in ("swepro", "swelite", "sweverified"):
        raise SystemExit(f"Unknown testbench: {testbench!r}; use swepro, swelite, or sweverified")
    if testbench == "sweverified" and split != "test":
        raise SystemExit("SWE-bench Verified only has the test split; use --split test")
    if instance_file:
        import json
        from pathlib import Path
        row = json.loads(Path(instance_file).read_text())
        if row.get("instance_id") != instance_id:
            raise ValueError("instance file does not match requested task")
        row = {k: v for k, v in row.items() if k not in FORBIDDEN_COLUMNS}
        return SweTask(spec, testbench=testbench, row=row, split=split,
                       prepared_root=prepared_root, official_python=official_python)
    try:
        from datasets import load_dataset
    except ImportError as exc:
        raise SystemExit(
            "Missing datasets package. Install it with: pip install datasets"
        ) from exc
    dataset = {
        "swepro": SWEPRO_DATASET,
        "swelite": SWELITE_DATASET,
        "sweverified": SWEVERIFIED_DATASET,
    }[testbench]
    ds = load_dataset(dataset, split=split)
    row = next((dict(candidate) for candidate in ds if str(candidate["instance_id"]) == instance_id), None)
    if row is None:
        raise SystemExit(f"Instance {instance_id!r} not found in {dataset} (split={split!r})")
    for column in FORBIDDEN_COLUMNS.intersection(row):
        row.pop(column, None)
    return SweTask(spec=spec, testbench=testbench, row=row, split=split,
                   prepared_root=prepared_root, official_python=official_python)
