"""模式注册层：把「任务模式」的全部差异收敛到一个注册表里。

大代码（MCTS 双塔循环、AuthorWorker、Coordinator、selector）完全不感知模式，
只通过 :func:`get_mode` 拿到一个 :class:`ModeSpec`，再按注册的名字取内置的
loader / scorer / materializer / artifact reader / official evaluator。

新增一个模式的成本（尽量压到最小，详见 README「新增一个模式」）：

- 成本 0（最常见，只改提示词）：任务形状与现有模式同构 ——
  1) 复制 ``res/task_plugins/<现有模式>/``，修改 prompts 下的 5 个 txt；
  2) 在新插件的 ``plugin.py`` 里注册一个 :class:`ModeSpec`（复用已注册的
     loader/scorer/materialize/artifact_reader，通常 8 行左右）；
  3) 无需修改主程序；启动时会自动发现含 ``plugin.py`` 的任务目录。
- 成本 1（改配置）：评分/物化/产物读取换成另一个已注册的内置器，只改字段。
- 成本 2（注册一个小函数，10-50 行）：全新执行形状，往对应注册表注册一个函数，
  核心循环不动。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
import pkgutil

# --- 类型别名（避免循环 import） -------------------------------------------------
# task: 任意“任务对象”，只需满足 adapter 接口（issue_text/init_solver/.../materialize_script）。
Task = Any
# scorer: (sess_or_ckpt, task, host) -> (score, evidence_text)
Scorer = Callable[..., tuple[float, str]]
# materializer: (task) -> str  —— 生成沙箱内物化脚本（或执行物化的可调用对象）
Materializer = Callable[..., Any]
# loader: (spec, **kwargs) -> task
Loader = Callable[..., Task]
# artifact reader: (node, task) -> (path, text) —— selector 读候选产物
ArtifactReader = Callable[..., tuple[str, str]]
# official: (node, task) -> bool —— union 默认不实现官测（None），提交时机逻辑保留
OfficialEval = Callable[..., bool]

RES = Path(__file__).resolve().parent
PROMPTS_ROOT = RES / "task_plugins"


# --- 注册表 ----------------------------------------------------------------------
LOADERS: dict[str, Loader] = {}
SCORERS: dict[str, Scorer] = {}
MATERIALIZERS: dict[str, Materializer] = {}
ARTIFACT_READERS: dict[str, ArtifactReader] = {}
OFFICIALS: dict[str, OfficialEval] = {}


def register(registry: dict, name: str | None = None) -> Callable:
    """装饰器：把一个实现注册进对应注册表（默认用函数名）。"""

    def deco(fn):
        registry[name or fn.__name__] = fn
        return fn

    return deco


@dataclass(frozen=True)
class ModeSpec:
    """一个任务模式的全部配置。除前四个字段外都有合理默认值。"""

    id: str
    name: str
    # 提示词目录名（res/task_plugins/<prompts_dir>/ 下 5 个文件）：
    #   prompt_solver_init.txt / prompt_solver_step.txt
    #   prompt_author_init.txt / prompt_author_step.txt / prompt_submission_selector.txt
    prompts_dir: str
    # 已注册的 loader 名（构造任务对象）。
    load_task: str
    # 已注册的 scorer / materializer / artifact reader / official 名。
    scorer: str = "pytest"
    materialize: str = "swe-instance"
    artifact_reader: str = "git_diff"
    official_eval: str | None = None
    # 沙箱内 oracle 文件（相对 repo 根）；host 侧暂存文件名。
    oracle_rel_path: str = "testcases.py"
    oracle_host_name: str = "testcases.py"
    # 产物文件（相对 repo 根）；"" 表示产物是整个 repo 的 git diff（SWE 风格）。
    artifact_rel_path: str = ""
    # 模式级环境变量（如 E2B_TEMPLATE_NAME），主程序启动时设置。
    env: dict[str, str] = field(default_factory=dict)
    configure_cli: Callable[[Any], None] | None = None

    def prompts(self) -> dict[str, str]:
        """返回 {role: 提示词文件名}，用于设置 XIAOO_PROMPT_* 环境变量。"""
        return {
            "solver_init": f"{self.prompts_dir}/prompt_solver_init.txt",
            "solver_step": f"{self.prompts_dir}/prompt_solver_step.txt",
            "author_init": f"{self.prompts_dir}/prompt_author_init.txt",
            "author_step": f"{self.prompts_dir}/prompt_author_step.txt",
            "submission_selector": f"{self.prompts_dir}/prompt_submission_selector.txt",
        }

    def loader(self) -> Loader:
        return LOADERS[self.load_task]

    def scorer_fn(self) -> Scorer:
        return SCORERS[self.scorer]

    def materializer(self) -> Materializer:
        return MATERIALIZERS[self.materialize]

    def artifact_reader_fn(self) -> ArtifactReader:
        return ARTIFACT_READERS[self.artifact_reader]

    def official_fn(self) -> OfficialEval | None:
        if self.official_eval is None:
            return None
        return OFFICIALS[self.official_eval]


class ModeRegistry:
    def __init__(self) -> None:
        self._modes: dict[str, ModeSpec] = {}

    def register(self, spec: ModeSpec) -> ModeSpec:
        if spec.id in self._modes:
            raise ValueError(f"mode already registered: {spec.id}")
        self._modes[spec.id] = spec
        return spec

    def get(self, mode_id: str) -> ModeSpec:
        _import_all()
        try:
            return self._modes[mode_id]
        except KeyError:
            raise SystemExit(
                f"unknown mode: {mode_id!r}; registered: {sorted(self._modes)}"
            ) from None

    def all(self) -> dict[str, ModeSpec]:
        _import_all()
        return dict(self._modes)

    def __contains__(self, mode_id: str) -> bool:
        return mode_id in self._modes


MODES = ModeRegistry()


def _import_all() -> None:
    """Discover every directory containing plugin.py; no task name list lives here."""
    import importlib
    from src.res import task_plugins

    for item in pkgutil.iter_modules(task_plugins.__path__):
        if item.ispkg:
            importlib.import_module(f"{task_plugins.__name__}.{item.name}.plugin")
