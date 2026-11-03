"""物化脚本生成器：生成在 E2B 沙箱内把任务内容落到 /home/user/workspace 的 shell。

统一抽象：adapter.materialize_script() 转发到这里注册的生成器；生成器只依赖
adapter 的公开属性（dataset_name/split/instance_id/local_source/policy/...），
不感知具体模式之外的任何东西。
"""

from __future__ import annotations

from .task import (
    JUDGE_SOURCE_PATH,
    SANDBOX_REPO,
    SANDBOX_TASK_DIR,
    SANDBOX_WORKSPACE,
)
from src.res.mode import MATERIALIZERS, register


@register(MATERIALIZERS, "zsoft-bundle")
def zsoft_bundle(task) -> str:
    """ZSoft 物化：解 bundle（public 资产）+ 按策略准备漏洞源码工作树。"""
    parts = [
        "set -euo pipefail",
        f"cd {SANDBOX_WORKSPACE}",
        "tar -xzf zsoft-task.tar.gz -C /home/user/workspace",
        "test -f task.json",
        "test -d task",
    ]
    if task.local_source is not None:
        parts.append("rm -rf repo repo.dl")
        parts.append(f"cp -a {SANDBOX_TASK_DIR}/repo-local repo")
        parts.append(f"echo ZSOFT_REPO_SOURCE=local:{task.source_lock.source_id}")
    elif (task.policy == "public" or task.source_lock is None) and task.has_public_source:
        parts.append(f"rm -rf repo && cp -r {SANDBOX_TASK_DIR}/repo-vul repo")
        parts.append("echo ZSOFT_REPO_SOURCE=public:repo-vul")
    elif task.source_lock is not None:
        parts.append(_download_script(task.source_lock))
    else:
        parts.append(
            "echo 'no working source: task has neither public/repo-vul "
            "nor a source lock' >&2 && exit 1"
        )
    from . import bundle as zb
    if not zb.is_kernel_task(task.manifest.tags):
        # 镜像官测源码路径：judge 容器固定把目标源码放在 /workspace/code。
        parts += [
            "mkdir -p /workspace 2>/dev/null || sudo -n mkdir -p /workspace",
            "sudo -n chown \"$(id -un)\":\"$(id -gn)\" /workspace 2>/dev/null || true",
            "if [ ! -w /workspace ]; then echo 'ZSOFT_MIRROR_UNAVAILABLE' >&2; exit 1; fi",
            "rm -rf /workspace/code",
            f"ln -sfn {SANDBOX_REPO} /workspace/code",
        ]
    parts += [
        f"git config --global --add safe.directory {SANDBOX_REPO} || true",
        f"git config --global --add safe.directory {JUDGE_SOURCE_PATH} || true",
        "echo ZSOFT_TASK_READY",
    ]
    return "\n".join(parts)


def _download_script(lock) -> str:
    """In-sandbox 下载锁定源码到 repo/。"""
    if lock.kind == "archive":
        return f"""rm -rf repo repo.dl
mkdir -p repo.dl
curl -fsSL "{lock.url}" -o /tmp/zsoft-source.tar.gz
tar -xzf /tmp/zsoft-source.tar.gz -C repo.dl
d="$(find repo.dl -mindepth 1 -maxdepth 1 -type d | head -n1)"
[ -n "$d" ]
mv "$d" repo
rm -rf repo.dl /tmp/zsoft-source.tar.gz
echo ZSOFT_REPO_SOURCE=archive:{lock.source_id}"""
    if lock.commit:
        return f"""rm -rf repo repo.dl
git clone "{lock.url}" repo.dl
git -C repo.dl checkout {lock.commit}
mv repo.dl repo
echo ZSOFT_REPO_SOURCE=git_snapshot:{lock.source_id}"""
    ref_part = f"--branch {lock.ref}" if lock.ref else ""
    return f"""rm -rf repo repo.dl
git clone {ref_part} "{lock.url}" repo.dl
mv repo.dl repo
echo ZSOFT_REPO_SOURCE=git_snapshot:{lock.source_id}"""
