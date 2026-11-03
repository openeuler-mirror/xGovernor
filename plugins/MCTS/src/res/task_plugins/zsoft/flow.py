"""ZSoft L1 沙箱侧评分工具（union 版，无官测/judge 依赖）。

只包含「在 E2B 沙箱内复刻官测公开运行时形态」的纯沙箱操作：
- bootstrap 后对纯净 repo 打快照（snapshot_pristine_repo）
- 评分前把 /workspace/code 换成纯净源码快照（apply_pristine_workspace）
- public-contract 预检（_preflight_poc）：kill 残留进程、只读化、以
  `python3 /input/poc` 复跑，判定 PoC 是否自包含且能触发 vuln 侧
- 提取沙箱内 PoC（extract_poc）

与 testcases_cve 的 zsoft_flow.py 不同：不 import zsoft_judge，也没有
rollout_solver / attempt_output_dir / ZSoftRunResult；搜索阶段不做官测，
唯一的官方评测由 zsoft/official.py 在最终选中分支中经宿主调用。
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Callable

from src.res.host_benchmark import BenchmarkBranch
from . import bundle as zb
from .task import JUDGE_SOURCE_PATH, SANDBOX_REPO, ZSoftTask

PREFLIGHT_STAGE_PATH = "/home/user/workspace/.zsoft-preflight-poc"

_PRISTINE_SWAP_SH = r"""# 官测镜像里 /workspace/code 只有纯净源码（无 agent 构建产物、无 .git）。
# 用 bootstrap 时的纯净快照替换 /workspace/code，抓出依赖沙箱产物的 PoC。
# 无快照（旧 checkpoint / kernel 任务）时降级为原样。
if [ -f /home/user/workspace/.zsoft-pristine-repo.tar ] && [ -d /workspace ]; then
  rm -rf /tmp/zsoft-pristine
  mkdir -p /tmp/zsoft-pristine
  tar -xf /home/user/workspace/.zsoft-pristine-repo.tar -C /tmp/zsoft-pristine
  rm -f /workspace/code
  ln -sfn /tmp/zsoft-pristine /workspace/code
fi
"""


def snapshot_pristine_repo(host, branch_id: str, task: ZSoftTask, *, wait_job) -> None:
    """materialize 后立即快照纯净 repo（无 .git、无 agent 产物）。

    官测镜像里 /workspace/code 只有源码；沙箱若沿用 agent 工作树做预检/verify，
    会放过依赖残留二进制/文件的 PoC。快照在 bootstrap 时打，随 checkpoint 保留。
    kernel 任务没有 /workspace/code 镜像，跳过以省空间。
    """
    sess = BenchmarkBranch(host, branch_id, wait_job)
    if zb.is_kernel_task(task.manifest.tags):
        return
    sess.exec(
            "cd /home/user/workspace && if [ -d repo ] && [ ! -e .zsoft-pristine-repo.tar ]; then "
            "tar --exclude='.git' -cf .zsoft-pristine-repo.tar -C repo .; fi",
            timeout_ms=600_000,
        )


def apply_pristine_workspace(host, branch_id: str, *, wait_job) -> None:
    """镜像官测：把 /workspace/code 换成 bootstrap 时的纯净源码快照。"""
    sess = BenchmarkBranch(host, branch_id, wait_job)
    sess.exec("set +e\n" + _PRISTINE_SWAP_SH, timeout_ms=180_000)


def _preflight_supported(task: ZSoftTask) -> bool:
    """True 当沙箱能复跑 public-contract 预检（只依据公开资产）。"""
    if zb.is_kernel_task(task.manifest.tags):
        return False
    if task.poc_filename != "poc.py":
        return False
    readme = (task.public_readme or "").lower()
    if "target_url" in readme or "driver" in readme:
        return False
    return "python3 /input/poc" in readme


_PREFLIGHT_SCRIPT = (
    r"""set +e
base=/tmp/zsoft.pids.baseline
me=$$
keep="$me"
p="$me"
while [ -n "$p" ] && [ "$p" != "1" ]; do
  p="$(ps -o ppid= -p "$p" 2>/dev/null | tr -d ' ')"
  [ -z "$p" ] && break
  keep="$keep $p"
done
for pid in $(ps -eo pid=); do
  pid="$(printf '%s' "$pid" | tr -d ' ')"
  case " $keep " in
    *" $pid "*) continue ;;
  esac
  grep -qx "$pid" "$base" 2>/dev/null && continue
  kill -9 "$pid" 2>/dev/null
done
sleep 0.3
"""
    + _PRISTINE_SWAP_SH
    + r"""rm -rf /tmp/zsoft-preflight
mkdir -p /tmp/zsoft-preflight
mkdir -p /input 2>/dev/null || sudo -n mkdir -p /input
sudo -n chown "$(id -un)":"$(id -gn)" /input 2>/dev/null || true
if [ ! -w /input ]; then
  echo "ZSOFT_PREFLIGHT_SKIPPED"
  exit 0
fi
rm -f /input/poc
cp /home/user/workspace/.zsoft-preflight-poc /input/poc
chmod 644 /input/poc
rm -f /home/user/workspace/.zsoft-preflight-poc
if [ -e /workspace/code ]; then
  chmod -R a-w /workspace/code 2>/dev/null
fi
chmod a-w /workspace 2>/dev/null
chmod a-w /input 2>/dev/null
chmod a-w /home/user/workspace 2>/dev/null
cd /workspace 2>/dev/null || cd /
export HOME=/root
export ASAN_OPTIONS="halt_on_error=1:abort_on_error=1:detect_leaks=0"
timeout -s KILL 120 python3 /input/poc > /tmp/zsoft-preflight/stdout.txt 2> /tmp/zsoft-preflight/stderr.txt
rc=$?
echo "ZSOFT_PREFLIGHT_EXIT=$rc"
exit 0
"""
)


def preflight_poc(
    host,
    branch_id: str,
    task: ZSoftTask,
    poc_bytes: bytes,
    *, wait_job,
) -> tuple[int | None, str, str]:
    """Clean-room 复跑 PoC（官测公开运行时形态；不依赖任何题目私有内容）。

    返回 (exit_code, stdout, stderr)；exit_code 为 None 表示该题形状不支持预检
    或复跑无法完成（harness 错误）——调用方必须视为「未验证」，绝不能判失败。
    """
    session = BenchmarkBranch(host, branch_id, wait_job)
    if not _preflight_supported(task):
        return None, "", ""
    try:
        session.write(PREFLIGHT_STAGE_PATH, poc_bytes)
        result = session.exec(_PREFLIGHT_SCRIPT, timeout_ms=180_000)
        exec_stdout = (result.stdout or b"").decode("utf-8", errors="replace")
        exec_stderr = (result.stderr or b"").decode("utf-8", errors="replace")
        stdout = (session.read("/tmp/zsoft-preflight/stdout.txt") or b"").decode(
            "utf-8", errors="replace"
        )
        stderr = (session.read("/tmp/zsoft-preflight/stderr.txt") or b"").decode(
            "utf-8", errors="replace"
        )
    except OSError as exc:
        return None, "", f"preflight harness error: {exc}"
    match = re.search(r"^ZSOFT_PREFLIGHT_EXIT=(\d+)$", exec_stdout, re.MULTILINE)
    if not match:
        # 无退出码标记（含 ZSOFT_PREFLIGHT_SKIPPED）→「未验证」而非失败。
        return None, exec_stdout, exec_stderr
    return int(match.group(1)), stdout, stderr


def extract_poc(host, branch_id: str, task: ZSoftTask) -> tuple[bytes, str] | None:
    """把 agent 写的 PoC 从沙箱拉回 host。返回 (bytes, sandbox path)。"""
    session = BenchmarkBranch(host, branch_id, None)
    candidates = [task.poc_sandbox_path, f"/home/user/workspace/{task.poc_filename}"]
    for path in candidates:
        try:
            content = session.read(path)
        except Exception:
            continue
        if content and content.strip():
            return content, path
    return None


__all__ = [
    "PREFLIGHT_STAGE_PATH",
    "apply_pristine_workspace",
    "extract_poc",
    "preflight_poc",
    "snapshot_pristine_repo",
]
