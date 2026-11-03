"""可选、只读的 LLM 提交选择器：在满分/并列最高分节点里挑最终提交物。

union 版：候选产物类型（git diff / PoC 文件）和评分维度都由模式注册层决定——
``task.spec.artifact_reader`` 决定怎么读候选，``prompt_submission_selector.txt``
里的 ``DIMENSIONS:`` 行声明评分维度（名字:上限，逗号分隔），因此新增模式只需
改提示词文件，不必碰本模块。

默认只收集满分节点（score >= 1.0）；``min_score`` 可放低门槛比较并列最高分。
LLM 总是对并列最高分候选做最终挑选；只有候选为空/唯一或调用失败时才回退。
与 MCTS 调度完全解耦。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


PROFILE_ID = "submission-selector"
REPO = "/home/user/workspace/repo"
MAX_ARTIFACT_CHARS = 60_000
MAX_TOTAL_ARTIFACT_CHARS = 360_000
MAX_CANDIDATES = 10
_PROMPTS_ROOT = Path(__file__).resolve().parents[1] / "res" / "task_plugins"
_DIMENSIONS_RE = re.compile(
    r"^\s*DIMENSIONS\s*:\s*(.+?)\s*$", re.MULTILINE | re.IGNORECASE
)
_DEFAULT_DIMENSIONS = [
    ("task_alignment", 35.0),
    ("root_cause", 25.0),
    ("regression_safety", 25.0),
    ("implementation_quality", 15.0),
]


@dataclass(frozen=True)
class Candidate:
    label: str
    node: Any
    artifact_sha256: str
    status: str
    artifact: str


def _iter_tree(node: Any):
    yield node
    for child in node.children:
        yield from _iter_tree(child)


def _node_score(node: Any) -> float:
    return float(node.score)


def _rubric_text() -> str:
    """从模式提示词目录读 selector rubric（含 DIMENSIONS: 行）。"""
    name = os.environ.get("XIAOO_PROMPT_SUBMISSION_SELECTOR", "prompt_submission_selector.txt")
    path = _PROMPTS_ROOT / name
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return (
        "You are the final submission reviewer. Select the single candidate most "
        "likely to satisfy the task. Every candidate passed the self-tests (admission "
        "gate only); judge the actual artifact, not claims. Extract atomic requirements "
        "from the task, evaluate every candidate against every requirement with concrete "
        "evidence, score independently, and mark eligible=false only for direct evidence "
        "of a fatal flaw.\n"
    )


def _dimensions() -> list[tuple[str, float]]:
    """解析模式提示词里的 DIMENSIONS: 行；缺省用 SWE 风格维度。"""
    text = _rubric_text()
    m = _DIMENSIONS_RE.search(text)
    if not m:
        return _DEFAULT_DIMENSIONS
    dims: list[tuple[str, float]] = []
    for part in m.group(1).split(","):
        part = part.strip()
        if not part:
            continue
        bits = re.split(r"\s*[:：]\s*", part, maxsplit=1)
        if len(bits) != 2 or not bits[1].strip().replace(".", "", 1).isdigit():
            continue
        dims.append((bits[0].strip(), float(bits[1].strip())))
    return dims or _DEFAULT_DIMENSIONS


def _read_artifact(node: Any, task: Any, host=None, wait_job=None) -> tuple[str, str]:
    """从节点不可变 state 读候选产物，交给模式注册的 artifact reader。"""
    if host is None: raise RuntimeError("HostCapabilities is required")
    if not node.state_id:
        raise RuntimeError(f"candidate {node.node_id} has no immutable state")
    # Coordinator releases expansion branches after every step.  Materialize a
    # short-lived branch from the immutable candidate state for artifact reads;
    # selector must never depend on a released node.branch_id.
    branch = host.fork(node.state_id, timeout_ms=120_000,
                       idempotency_key=f"selector:{node.node_id}:artifact-fork")
    branch_id = branch["branch_id"]
    try:
        reader = task.spec.artifact_reader_fn()
        status, artifact = reader(node, task, host=host, branch_id=branch_id, wait_job=wait_job)
        if not artifact.strip():
            raise RuntimeError(f"empty artifact for {node.node_id}")
        return status, artifact
    finally:
        host.release(branch_id, timeout_ms=30_000,
                     idempotency_key=f"selector:{node.node_id}:artifact-release")


def collect_candidates(
    root: Any, task: Any, *, min_score: float = 1.0, host=None, wait_job=None
) -> list[Candidate]:
    unique: dict[str, Candidate] = {}
    for node in _iter_tree(root):
        if not node.state_id or _node_score(node) < min_score:
            continue
        if len(unique) >= MAX_CANDIDATES:
            break
        try:
            status, artifact = _read_artifact(node, task, host, wait_job)
        except Exception:
            # 读不到的节点绝不能让选择流程卡死；跳过。
            continue
        digest = hashlib.sha256((status + "\0" + artifact).encode("utf-8")).hexdigest()
        if digest not in unique:
            unique[digest] = Candidate(
                label=f"candidate_{len(unique) + 1:02d}",
                node=node,
                artifact_sha256=digest,
                status=status,
                artifact=artifact,
            )
    return list(unique.values())


def _task_text(task: Any) -> str:
    issue_text = getattr(task, "issue_text", None)
    if callable(issue_text):
        return str(issue_text())
    return str(task)


def _build_prompt(task: Any, candidates: list[Candidate]) -> str:
    dims = _dimensions()
    dim_names = [name for name, _max in dims]
    dim_desc = "; ".join(f"{name} 0-{maximum:g}" for name, maximum in dims)
    parts = [
        _rubric_text().strip(),
        "All candidates carry the tree's top self-test scores — admission gate only; "
        "do not award points for test results or candidate claims. Judge the actual "
        "artifact.",
        f"Score independently (total sums to 100): {dim_desc}.",
        "\n[TASK]\n" + _task_text(task),
    ]
    used = 0
    for candidate in candidates:
        remaining = MAX_TOTAL_ARTIFACT_CHARS - used
        if remaining <= 0:
            break
        artifact = candidate.artifact[:remaining]
        used += len(artifact)
        parts.append(
            f"\n[{candidate.label}]\n"
            f"node={candidate.node.node_id}\n"
            f"status:\n{candidate.status or '(clean except shown artifact)'}\n"
            f"artifact:\n{artifact or '(empty artifact)'}"
        )
    schema_fields = ", ".join(f'"{name}":0' for name in dim_names)
    parts.append(
        "\nReturn JSON only. Evaluate every candidate exactly once. The program will "
        "recompute totals and select by: eligible first, total descending, then "
        + ", ".join(dim_names)
        + ", and finally candidate label ascending. Requirements must be atomic and "
        "each candidate's coverage must address every requirement. "
        "Schema: "
        '{"requirements":[{"id":"R1","text":"..."}],"evaluations":['
        '{"candidate":"candidate_XX","eligible":true,"coverage":['
        '{"requirement":"R1","covered":true,"evidence":"..."}],'
        + schema_fields
        + ',"fatal_flaws":[],"reason":"..."}]}'
    )
    return "\n".join(parts)


def _first_selector_object(text: str) -> dict[str, Any]:
    """Extract the selector JSON object from a model response.

    Selector models frequently emit Markdown fences, reasoning prose, and source
    code snippets before the final JSON object.  A naive ``text.find("{")``
    picks up the first brace in an example, so prefer a fenced block, then the
    known ``{"requirements"`` prefix, then scan every opening brace from the end
    backwards with :func:`json.JSONDecoder.raw_decode`.
    """
    candidates: list[str] = []

    fenced = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.S)
    if fenced:
        candidates.append(fenced.group(1).strip())

    root_pos = text.find('{"requirements"')
    if root_pos >= 0:
        candidates.append(text[root_pos:].strip())

    for match in reversed(list(re.finditer(r"\{", text))):
        candidates.append(text[match.start():].strip())

    errors: list[str] = []
    decoder = json.JSONDecoder()
    for candidate in candidates:
        try:
            data, _ = decoder.raw_decode(candidate)
        except Exception as exc:
            errors.append(str(exc))
            continue
        if isinstance(data, dict) and isinstance(data.get("evaluations"), list):
            return data
        errors.append("decoded value is not a selector object")
    detail = errors[0] if errors else "no JSON object found"
    raise ValueError(f"selector JSON extraction failed: {detail}")

def _parse_winner(raw: str, labels: set[str]) -> str:
    data = _first_selector_object(raw.strip())
    evaluations = data.get("evaluations")
    if not isinstance(evaluations, list):
        raise ValueError("selector response has no evaluations list")

    limits = dict(_dimensions())
    scored: list[tuple[bool, float, tuple[float, ...], str]] = []
    seen: set[str] = set()
    for item in evaluations:
        if not isinstance(item, dict):
            raise ValueError("selector evaluation is not an object")
        label = str(item.get("candidate") or "")
        if label not in labels or label in seen:
            raise ValueError(f"invalid or duplicate candidate {label!r}")
        seen.add(label)
        values: list[float] = []
        for name, maximum in limits.items():
            value = item.get(name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{label} has invalid {name}")
            value = float(value)
            if not 0.0 <= value <= maximum:
                raise ValueError(f"{label} {name} is outside 0..{maximum:g}")
            values.append(value)
        eligible = item.get("eligible")
        if not isinstance(eligible, bool):
            raise ValueError(f"{label} has invalid eligible flag")
        total = sum(values)
        scored.append((eligible, total, tuple(values), label))
    if seen != labels:
        raise ValueError(f"selector omitted candidates: {sorted(labels - seen)}")

    pool = [row for row in scored if row[0]] or scored
    # 并列时按维度顺序（提示词里的声明顺序）降序，最后 label 升序。
    pool.sort(key=lambda row: (-row[1],) + tuple(-v for v in row[2]) + (row[3],))
    return pool[0][3]


def select_node(
    root: Any,
    task: Any,
    fallback: Any,
    *,
    min_score: float | None = None,
    host=None,
    wait_job=None,
    on_event: Callable[[str, dict], None] | None = None,
) -> Any:
    """返回 LLM 选中的节点；任何失败都回退到 fallback。"""
    try:
        gate = 1.0 if min_score is None else min_score
        if host is None or wait_job is None:
            return fallback
        candidates = collect_candidates(root, task, min_score=gate, host=host, wait_job=wait_job)
        if on_event:
            on_event("plugin.progress", {
                "summary": (
                    f"submission selector candidates={len(candidates)} "
                    f"min_score={gate} nodes={[c.node.node_id for c in candidates]}"
                ),
            })
        if not candidates:
            return fallback
        if len(candidates) == 1:
            return candidates[0].node
        prompt = _build_prompt(task, candidates)
        if not getattr(fallback, "state_id", None):
            return fallback
        state = {"state_id": fallback.state_id}
        branch = host.fork(state["state_id"], timeout_ms=300_000, idempotency_key="selector:fork")
        try:
            receipt = host.agent_turn(branch["branch_id"], prompt, profile=PROFILE_ID,
                                      timeout_ms=300_000, idempotency_key="selector:turn")
            completed = wait_job(receipt)
            raw = completed.get("output", "") if isinstance(completed, dict) else getattr(completed, "output", "")
            winner = _parse_winner(raw, {c.label for c in candidates})
        finally:
            try:
                host.release(branch["branch_id"], timeout_ms=30000, idempotency_key="selector:release")
            except Exception:
                pass
        selected = next(c for c in candidates if c.label == winner).node
        if on_event:
            on_event("plugin.progress", {
                "summary": (
                    f"submission selector selected {selected.node_id} "
                    f"winner={winner} fallback={fallback.node_id}"
                ),
            })
        return selected
    except Exception as exc:
        if on_event:
            on_event("plugin.progress", {
                "summary": (
                    f"submission selector fell back after error: {exc}; "
                    f"fallback={fallback.node_id}"
                ),
            })
        return fallback
