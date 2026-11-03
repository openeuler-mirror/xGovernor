"""评分器注册：统一的 (sess, task, host) -> (score, evidence_text) 契约。

- pytest：SWE-bench 风格 —— 沙箱内 `pytest <oracle>` 的通过率；
- zsoft-verify：ZSoft L1 风格 —— 纯净工作树 + verify.py 断言 + public-contract
 预检，机制性风险静态扫描封顶；最终官方 judge 由 official evaluator 单独执行。

新模式的「运行某个程序得到评分」若形状同构，优先复用这两个之一（注册名写在
ModeSpec.scorer 里）；完全不同才注册新 scorer（成本 2，见 README）。
"""

from __future__ import annotations

import ast
import re

from src.res.host_benchmark import BenchmarkBranch

from src.res.benchmark_util import push_testcases
from src.res.mode import SCORERS, register
from .flow import apply_pristine_workspace, extract_poc, preflight_poc
from .trial import run_judge_trial

# --- ZSoft verify + preflight -----------------------------------------------

_VERIFY_RATIO_RE = re.compile(r"^\s*VERIFY_RATIO\s*=\s*(\d+(?:\.\d+)?)", re.MULTILINE)
_VERIFY_PASS_RE = re.compile(r"^\s*PASS\s+(.+?)\s*$", re.MULTILINE)
_VERIFY_FAIL_RE = re.compile(r"^\s*FAIL\s+(.+?)\s*$", re.MULTILINE)

# 静态机制性风险扫描：官测镜像是最小 debian:bookworm，PoC 若硬编码链接官方镜像
# 没有的 dev 库或在运行期装包/用 pkill，会在 vuln/fix 两侧都确定性 exit>1。
_SAFE_LINK_LIBS = {"m", "dl", "pthread", "c", "gcc_s", "rt", "stdc++"}
_LINK_FLAG_RE = re.compile(r"(?<![A-Za-z0-9])-l([A-Za-z0-9.+-]{1,16})\b")
_LOCAL_L_SEARCH_RE = re.compile(r"-L\b")
_PKILL_RE = re.compile(r"\bpkill\b|\bpgrep\b")
_PKG_INSTALL_RE = re.compile(
    r"\bapt-get\s+(update|install)\b|\bapt\s+install\b|\byum\s+install\b|"
    r"\bdnf\s+install\b|\bpip\s+install\b"
)


def mechanistic_risk_flags(poc_bytes: bytes) -> list[str]:
    """Best-effort 静态扫描，找出会在官测镜像里确定性 MECH FAIL 的 PoC 特征。

    只报高置信特征，避免误伤；返回值非空只代表风险高，配合评分封顶使用。
    """
    text = poc_bytes.decode("utf-8", errors="replace")
    risks: list[str] = []
    for line in text.splitlines():
        if not _LINK_FLAG_RE.search(line):
            continue
        if _LOCAL_L_SEARCH_RE.search(line):
            continue
        for lib in _LINK_FLAG_RE.findall(line):
            if lib not in _SAFE_LINK_LIBS:
                risks.append(f"hardcoded -l{lib} without -L (dev lib may be missing in judge image)")
    if _PKILL_RE.search(text):
        risks.append("pkill/pgrep (procps not installed in judge image)")
    if _PKG_INSTALL_RE.search(text):
        risks.append("package install at runtime (judge has no network)")
    return risks


def parse_verify_output(stdout: str, stderr: str = "") -> tuple[float | None, str]:
    """解析 verify.py 输出：PASS/FAIL 行 + VERIFY_RATIO 行。返回 (通过率, 证据文本)。"""
    text = f"{stdout}\n{stderr}"
    passes = [m.strip() for m in _VERIFY_PASS_RE.findall(text)]
    fails = [m.strip() for m in _VERIFY_FAIL_RE.findall(text)]
    ratio: float | None = None
    m = _VERIFY_RATIO_RE.search(text)
    if m:
        ratio = max(0.0, min(1.0, float(m.group(1))))
    elif passes or fails:
        ratio = len(passes) / (len(passes) + len(fails))
    log_lines = [f"PASS {p}" for p in passes] + [f"FAIL {f}" for f in fails]
    tail = [ln for ln in text.strip().splitlines() if ln.strip()][-8:]
    log_text = "\n".join(log_lines + (["---"] + tail if tail else []))
    if ratio is None:
        log_text = f"{log_text}\n(no PASS/FAIL/VERIFY_RATIO markers)".strip()
    return ratio, log_text


def is_valid_python_source(data: bytes) -> bool:
    """Conservative python syntax gate for ZSoft Python PoCs."""
    if not data or not data.strip():
        return False
    try:
        ast.parse(data.decode("utf-8"))
    except (UnicodeDecodeError, SyntaxError):
        return False
    return True


def run_verify(
    capabilities,
    branch_id: str,
    task,
    oracle,
    *,
    wait_job,
    timeout_ms: int = 360_000,
) -> tuple[float | None, str]:
    """在沙箱内跑 Author 写的 verify.py。缺失/失败 → (None, 说明)，绝不把
    harness 错误当成 PoC 失败。"""
    sess = BenchmarkBranch(capabilities, branch_id, wait_job)
    try:
        if oracle is not None and getattr(oracle, "is_file", lambda: False)():
            push_testcases(capabilities, branch_id, oracle, repo_path=task.verify_path)
        oracle_bytes = sess.read(task.verify_path)
        if not oracle_bytes or not oracle_bytes.strip():
            return None, "verify.py: missing or empty"
        if not is_valid_python_source(oracle_bytes):
            return None, "verify.py: not syntactically valid python"
        result = sess.exec(
            f"python3 {task.verify_path} {task.poc_sandbox_path}",
            cwd="/home/user/workspace/repo",
            timeout_ms=timeout_ms,
        )
        stdout = (result.stdout or b"").decode("utf-8", errors="replace")
        stderr = (result.stderr or b"").decode("utf-8", errors="replace")
        ratio, log_text = parse_verify_output(stdout, stderr)
        if result.timed_out:
            log_text = f"{log_text}\n(verify.py timed out)".strip()
        return ratio, log_text
    except Exception as exc:
        # Missing file, runtime not found, or host error must not become a
        # false PoC score; callers treat verify=none via the legacy formula.
        return None, f"verify.py: harness read error: {exc}"


def score_attempt(
    preflight_exit: int | None,
    verify_ratio: float | None,
    verify_log: str = "",
    trial=None,
) -> tuple[float, str]:
    """组合评分。

    - trial=None（未启用 judge-trial 或基础设施失败）：预检为主、verify 为辅，
      与历史公式逐字节一致；
    - trial 非 None（judge-trial 已执行）：真环境证据为主（0.50），
      verify 次之（0.35）；评审官判 broken 时一票否决（对齐 preflight>1 档）。
    """
    if verify_ratio is None:
        v = 0.5
        vnote = "verify=none"
    else:
        v = verify_ratio
        vnote = f"verify={verify_ratio:.2f}"
    if preflight_exit is None:
        pnote = "preflight=unverified"
    elif preflight_exit == 0:
        pnote = "preflight=trigger"
    elif preflight_exit == 1:
        pnote = "preflight=no-repro"
    else:
        pnote = f"preflight=error({preflight_exit})"
    if trial is not None:
        if trial.broken:
            score = 0.0 + 0.1 * v
            tnote = f"trial=broken({trial.mechanical})"
        else:
            score = 0.15 + 0.35 * v + 0.50 * trial.trial_score
            tnote = f"trial={trial.trial_score:.2f}"
        score = min(1.0, score)
        evidence = f"{tnote} {pnote} {vnote} {verify_log}".strip()
        return score, evidence
    if preflight_exit is None:
        base = 0.25 + 0.5 * v
    elif preflight_exit == 0:
        base = 0.85 + 0.15 * v
    elif preflight_exit == 1:
        base = 0.35 + 0.25 * v
    else:
        base = 0.0 + 0.1 * v
    score = min(1.0, base)
    evidence = f"{pnote} {vnote} {verify_log}".strip()
    return score, evidence


@register(SCORERS, "zsoft-verify")
def zsoft_verify(capabilities, branch_id: str, task, oracle=None, *, wait_job, assessor=None) -> tuple[float, str]:
    """ZSoft 评分：纯净工作树 + verify.py + public-contract 预检 + 机制风险封顶。

    预检是破坏性的（杀进程、只读化 /workspace/code），因此调用方必须在独立
    checkout 上评分（agents.score_checkpoint 保证），不动工作沙箱。
    """
    sess = BenchmarkBranch(capabilities, branch_id, wait_job)
    apply_pristine_workspace(capabilities, branch_id, wait_job=wait_job)
    sess.exec("ps -eo pid= | tr -d ' ' | sort -n > /tmp/zsoft.pids.baseline", timeout_ms=60_000)
    verify_ratio, verify_log = run_verify(capabilities, branch_id, task, oracle, wait_job=wait_job)
    preflight_exit: int | None = None
    poc_bytes: bytes | None = None
    poc = extract_poc(capabilities, branch_id, task)
    if poc is None:
        verify_log = f"{verify_log}\n(preflight skipped: no PoC at {task.poc_sandbox_path})".strip()
    else:
        poc_bytes, _poc_path = poc
        if not getattr(task, "is_kernel", False) and task.poc_filename.endswith(".py") and not is_valid_python_source(poc_bytes):
            # A syntactically invalid Python PoC can never be judge-ready.
            # Do not let a weak oracle award it the legacy verify=none floor.
            return 0.0, "python_syntax=invalid" + (f" {verify_log}" if verify_log else "")
        preflight_exit, _pout, _perr = preflight_poc(capabilities, branch_id, task, poc_bytes, wait_job=wait_job)
    # judge-trial：host 侧向本地 serve 提交一次真判题，取公开反馈 + 评审官评分。
    # 任何故障降级为 trial=None（回退历史公式），绝不影响节点存活。
    trial_result = None
    if poc_bytes is not None:
        try:
            trial_result = run_judge_trial(
                task, poc_bytes,
                preflight_exit=preflight_exit,
                verify_ratio=verify_ratio,
                host=capabilities, branch_id=branch_id, wait_job=wait_job,
            )
        except Exception as exc:
            verify_log = f"{verify_log}\n(trial harness error: {exc})".strip()
    score, evidence = score_attempt(preflight_exit, verify_ratio, verify_log, trial=trial_result)
    if trial_result is not None:
        evidence = f"{evidence}\n{trial_result.evidence()}".strip()
    risks = mechanistic_risk_flags(poc_bytes or b"")
    if risks:
        evidence = f"{evidence} [mechanistic-risk: {'; '.join(risks)}]".strip()
        # 静态风险封顶：低于 preflight=trigger 的 0.85 基分。
        score = min(score, 0.75)
    return score, evidence
