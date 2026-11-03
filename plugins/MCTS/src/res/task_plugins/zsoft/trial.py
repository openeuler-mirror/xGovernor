"""Judge-trial：每节点向本地 zsoft-poc serve 提交一次，取公开反馈 + 评审官评分。

设计契约（xiaoo-MCTS-analysis 设计稿）：
- 单一开关 ``ZSOFT_TRIAL_SERVER_URL``（空 = 功能完全关闭，主线零变化）；
- 只走 HTTP 通道：serve 天然把响应裁剪为 PublicSubmissionFeedback
  （evaluation_status / observed_exit_code / output<=8KB），判定细节绝不进搜索；
- 覆盖范围：URL 设置后所有题（含 kernel——判题成本 2~4 分钟/次，超时按题型自适应）；
- 评审官 LLM 综合公开反馈 + （可选）preflight 退出码给出连续 trial_score；
- 降级语义：serve 挂 → 重试一次后 trial=None；LLM 挂 → 规则兜底打分；
  任何情况下不抛出、不杀节点。

Trace 采集（本文件同时是采集器）：
- 一次 MCTS run（= 一个进程 = 一个任务）的全部 trial 追加到
  ``output/zsoft-trials/<RUN_TAG|ad-hoc>/<task_id>-<进程时间戳>.jsonl``；
- 每行归属到节点（node_id 来自 benchmark_util 的评分节点上下文）；
- 成功 / 基础设施失败 / 缓存命中 三条路径都落盘；写失败仅警告；
- 同一 PoC（sha256 相同）进程内只提交一次判题（retest 复用缓存，零重复成本）。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from src.res.benchmark_util import scoring_node

from . import bundle as zb

TRIAL_TIMEOUT_SECS = 300
KERNEL_TRIAL_TIMEOUT_SECS = 900  # kernel 判题 = 编译 + vuln/fix 两次 QEMU 开机，TCG 下 2~10 分钟
TRIAL_PHASE = "mcts-trial"
MAX_OUTPUT_CHARS = 8_192
MAX_EVIDENCE_OUTPUT_CHARS = 2_048
ASSISTANT_PROFILE = "trial-assessor"

# 并发保护：4 核机器上同时最多 2 个判题 trial（首层 3 节点并发时第三个排队）。
_semaphore = threading.Semaphore(2)

# --- trace 采集 ---------------------------------------------------------------

_TRACE_ROOT = Path(__file__).resolve().parents[3] / "output" / "zsoft-trials"
_TRACE_LOCK = threading.Lock()
_RUN_STAMP = time.strftime("%Y%m%d-%H%M%S")  # 模块导入 ≈ 进程启动；一进程 = 一 run
# 进程内 trial 缓存：同一 PoC（sha256）只交一次判题，retest 零成本复用。
_TRIAL_CACHE: dict[str, "TrialResult"] = {}

_MECHANICAL_RE = re.compile(r"^\s*MECHANICAL\s*:\s*(.+?)\s*$", re.MULTILINE)
_TRIAL_SCORE_RE = re.compile(
    r"^\s*TRIAL_SCORE\s*:\s*([0-9]*\.?[0-9]+)\s*$", re.MULTILINE | re.IGNORECASE
)
_ADVICE_RE = re.compile(r"^\s*ADVICE\s*:\s*(.*?)\s*$", re.MULTILINE | re.IGNORECASE)


def trial_server_url() -> str:
    return os.environ.get("ZSOFT_TRIAL_SERVER_URL", "").strip()


def trial_enabled(task) -> bool:
    """URL 已设置即启用（kernel 题也提交；判题成本 2~4 分钟/次，由预算与超时兜底）。

    任何内部错误都视为关闭（保守降级）。
    """
    if not trial_server_url():
        return False
    return True


@dataclass
class TrialResult:
    """一次 judge-trial 的完整结果（公开反馈 + 评审四元组）。"""

    status: str
    exit_code: int | None
    output: str
    attempt_id: str
    mechanical: str
    signals: str
    trial_score: float
    advice: str
    raw_reply: str = ""
    fallback: bool = False
    trial_latency_ms: int = 0
    assessor_latency_ms: int = 0
    poc_sha256: str = ""
    poc_bytes_len: int = 0

    @property
    def broken(self) -> bool:
        return self.mechanical.lower().startswith("broken")

    def evidence(self) -> str:
        """喂给下一轮 Solver 的证据块（追加到评分证据文本尾部）。"""
        head = self.output[:MAX_EVIDENCE_OUTPUT_CHARS].rstrip()
        return (
            "[Judge trial evidence] (public feedback from one real judge run)\n"
            f"status={self.status} exit={self.exit_code}\n"
            f"output:\n{head}\n"
            f"Assessor: MECHANICAL={self.mechanical} "
            f"TRIAL_SCORE={self.trial_score:.2f}\n"
            f"Signals: {self.signals}\n"
            f"Advice: {self.advice}"
        )


def _trial_timeout_secs(task) -> int:
    """按题型选超时：kernel 判题要编译 + 两次 QEMU 开机，需要更长预算。"""
    try:
        if zb.is_kernel_task(task.manifest.tags):
            return KERNEL_TRIAL_TIMEOUT_SECS
    except Exception:
        pass
    return TRIAL_TIMEOUT_SECS


def _submit(server: str, task_id: str, poc_bytes: bytes, timeout_secs: int) -> dict:
    """multipart POST 一笔提交，返回 serve 的 PublicSubmissionFeedback JSON。

    与 official.py 的 _post 同构，但 phase 固定为 mcts-trial（.runs 里与
    official 天然隔离，便于审计与清理）。
    """
    boundary = "----mcts-trial-" + uuid.uuid4().hex
    body = bytearray()
    for name, value in (("phase", TRIAL_PHASE), ("submission_kind", "trial")):
        body.extend(
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n".encode()
        )
    body.extend(
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; filename="poc"\r\n'
        f"Content-Type: application/octet-stream\r\n\r\n".encode()
    )
    body.extend(poc_bytes)
    body.extend(f"\r\n--{boundary}--\r\n".encode())
    request = urllib.request.Request(
        f"{server.rstrip('/')}/v0.1/tasks/{task_id}/submissions",
        data=bytes(body),
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(request, timeout=timeout_secs) as response:
        return json.loads(response.read())


def _fetch_trial(task, poc_bytes: bytes) -> dict | None:
    """提交一次 judge-trial；连接类错误重试一次，其余直接放弃。"""
    server = trial_server_url()
    timeout_secs = _trial_timeout_secs(task)
    for attempt in range(2):
        try:
            return _submit(server, task.task_id, poc_bytes, timeout_secs)
        except urllib.error.HTTPError:
            # serve 明确拒绝（如超 max_bytes）——重试无意义。
            return None
        except (urllib.error.URLError, TimeoutError, OSError):
            if attempt == 0:
                continue
    return None


def _parse_assessor(raw: str) -> tuple[str, str, float, str] | None:
    """解析评审官输出四元组；TRIAL_SCORE 缺失视为解析失败。"""
    score_m = _TRIAL_SCORE_RE.search(raw)
    if not score_m:
        return None
    score = max(0.0, min(1.0, float(score_m.group(1))))
    mech_m = _MECHANICAL_RE.search(raw)
    mechanical = mech_m.group(1).strip() if mech_m else "ok"
    advice_m = _ADVICE_RE.search(raw)
    advice = advice_m.group(1).strip() if advice_m else ""
    signals = "\n".join(
        ln.lstrip(" -\t").strip()
        for ln in raw.splitlines()
        if ln.strip().startswith("-")
    )
    return mechanical, signals, score, advice


def _rule_assess(feedback: dict) -> tuple[str, str, float, str]:
    """LLM 不可用时的规则兜底（保守：现象含糊一律 0.4~0.5）。"""
    exit_code = feedback.get("observed_exit_code")
    output = str(feedback.get("output") or "")
    if exit_code is not None and exit_code != 0:
        return f"broken: trial exit={exit_code}", "", 0.0, ""
    if output.strip():
        return "ok", "", 0.5, "(rule-based: LLM assessor unavailable)"
    return "ok", "", 0.4, "(rule-based: LLM assessor unavailable)"


def _build_assessor_prompt(task, feedback: dict, preflight_exit: int | None) -> str:
    if preflight_exit is None:
        corroboration = (
            "clean-room exit code: ABSENT "
            "(the sandbox cannot reproduce this task's target; judge solely "
            "on the trial feedback)"
        )
    else:
        corroboration = f"clean-room exit code: {preflight_exit}"
    output = str(feedback.get("output") or "")[:MAX_OUTPUT_CHARS]
    return "\n\n".join(
        [
            f"[TASK CONTRACT]\n{task.issue_text()}",
            "[TRIAL FEEDBACK] (public, from one real judge run; no verdict)\n"
            f"status={feedback.get('evaluation_status')} "
            f"exit={feedback.get('observed_exit_code')}\n"
            f"output:\n{output}",
            f"[CORROBORATION]\n{corroboration}",
            "Respond now in the required format "
            "(MECHANICAL / SIGNALS / TRIAL_SCORE / ADVICE).",
        ]
    )


def _assess(task, feedback: dict, preflight_exit: int | None, *, host, branch_id: str, wait_job):
    """评审官 LLM 评分；任何失败回退规则兜底，绝不抛出。

    返回 (mechanical, signals, score, advice, raw_reply, fallback, latency_ms)。
    """
    started = time.monotonic()
    try:
        prompt = _build_assessor_prompt(task, feedback, preflight_exit)
        state = host.snapshot(branch_id, timeout_ms=300_000, idempotency_key="trial:assessor:snapshot")
        assessor_branch = host.fork(state["state_id"], timeout_ms=300_000, idempotency_key="trial:assessor:fork")
        receipt = host.agent_turn(
            assessor_branch["branch_id"], prompt, profile=ASSISTANT_PROFILE,
            timeout_ms=300_000, idempotency_key="trial:assessor:turn",
        )
        try:
            completed = wait_job(receipt)
            raw = completed.get("output", "") if isinstance(completed, dict) else getattr(completed, "output", "")
            parsed = _parse_assessor(raw)
        finally:
            try:
                host.release(assessor_branch["branch_id"], timeout_ms=30000, idempotency_key="trial:release")
            except Exception:
                pass
        latency = int((time.monotonic() - started) * 1000)
        if parsed is not None:
            mechanical, signals, score, advice = parsed
            return mechanical, signals, score, advice, raw, False, latency
    except Exception:
        pass
    latency = int((time.monotonic() - started) * 1000)
    mechanical, signals, score, advice = _rule_assess(feedback)
    return mechanical, signals, score, advice, "", True, latency


def _trace_path(task) -> Path:
    tag = os.environ.get("RUN_TAG", "").strip() or "ad-hoc"
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", tag)
    directory = _TRACE_ROOT / safe
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{task.task_id}-{_RUN_STAMP}.jsonl"


def record_trial(
    task,
    result: TrialResult | None,
    *,
    node_id: str | None,
    verify_ratio: float | None,
    preflight_exit: int | None = None,
    cached: bool = False,
    error: str | None = None,
) -> None:
    """把一次 trial（成功/失败/缓存命中）追加进本 run 的 trace 文件。

    写失败只打 stderr 警告——trace 绝不影响节点评分。
    """
    try:
        record = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "run_stamp": _RUN_STAMP,
            "run_tag": os.environ.get("RUN_TAG", "").strip() or "ad-hoc",
            "task_id": task.task_id,
            "node_id": node_id,
            "cached": cached,
            "phase": TRIAL_PHASE,
            "submission": {
                "poc_sha256": result.poc_sha256 if result else "",
                "poc_bytes": result.poc_bytes_len if result else 0,
                "attempt_id": result.attempt_id if result else "",
                "trial_url": trial_server_url(),
            },
            "error": error,
        }
        if result is not None:
            record["feedback"] = {
                "evaluation_status": result.status,
                "observed_exit_code": result.exit_code,
                "output": result.output,
                "output_truncated": len(result.output) >= MAX_OUTPUT_CHARS,
            }
            record["assessor"] = {
                "raw_reply": result.raw_reply,
                "mechanical": result.mechanical,
                "signals": result.signals,
                "trial_score": result.trial_score,
                "advice": result.advice,
                "fallback": result.fallback,
                "latency_ms": result.assessor_latency_ms,
            }
        record["context"] = {
            "preflight_exit": preflight_exit,
            "verify_ratio": verify_ratio,
            "trial_latency_ms": result.trial_latency_ms if result is not None else 0,
        }
        line = json.dumps(record, ensure_ascii=False)
        path = _trace_path(task)
        with _TRACE_LOCK:
            with path.open("a", encoding="utf-8") as fh:
                fh.write(line + "\n")
    except Exception as exc:  # noqa: BLE001
        print(f"[trial-trace] write failed: {exc}", file=sys.stderr)


def run_judge_trial(
    task,
    poc_bytes: bytes | None,
    preflight_exit: int | None = None,
    verify_ratio: float | None = None,
    *, host, branch_id: str, wait_job,
) -> TrialResult | None:
    """主入口：提交一次 trial 并评分。未启用/无 PoC/基础设施失败 → None。

    同一 PoC（sha256 相同）进程内复用首次结果（retest 零成本），
    但每次调用都在 trace 里落一行（cached=True 标记节点归属）。
    """
    if not poc_bytes:
        return None
    if not trial_enabled(task):
        return None
    node_id = scoring_node()
    sha = hashlib.sha256(poc_bytes).hexdigest()
    cached_result = _TRIAL_CACHE.get(sha)
    if cached_result is not None:
        record_trial(
            task, cached_result,
            node_id=node_id, verify_ratio=verify_ratio,
            preflight_exit=preflight_exit, cached=True,
        )
        return cached_result
    started = time.monotonic()
    with _semaphore:
        feedback = _fetch_trial(task, poc_bytes)
    trial_latency = int((time.monotonic() - started) * 1000)
    if feedback is None:
        record_trial(
            task, None,
            node_id=node_id, verify_ratio=verify_ratio,
            preflight_exit=preflight_exit,
            error="trial fetch failed (serve unreachable or rejected)",
        )
        return None
    mechanical, signals, score, advice, raw_reply, fallback, assessor_latency = _assess(
        task, feedback, preflight_exit, host=host, branch_id=branch_id, wait_job=wait_job
    )
    result = TrialResult(
        status=str(feedback.get("evaluation_status") or ""),
        exit_code=feedback.get("observed_exit_code"),
        output=str(feedback.get("output") or "")[:MAX_OUTPUT_CHARS],
        attempt_id=str(feedback.get("attempt_id") or ""),
        mechanical=mechanical,
        signals=signals,
        trial_score=score,
        advice=advice,
        raw_reply=raw_reply,
        fallback=fallback,
        trial_latency_ms=trial_latency,
        assessor_latency_ms=assessor_latency,
        poc_sha256=sha,
        poc_bytes_len=len(poc_bytes),
    )
    _TRIAL_CACHE[sha] = result
    record_trial(
        task, result,
        node_id=node_id, verify_ratio=verify_ratio,
        preflight_exit=preflight_exit,
    )
    return result
