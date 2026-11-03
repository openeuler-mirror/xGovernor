#!/usr/bin/env bash
# union 统一启动器（模式注册层驱动，无影子提交，最终节点仅官测一次）。
#
# 用法：
#   SWE：   bash run_union.sh --mode sweverified --instance-id astropy__astropy-12907 --split test
#   ZSoft： bash run_union.sh --mode zsoft --task-id civetweb-002-websocket-deflate-oob
#   ZSoft 任务列表： bash run_union.sh --mode zsoft --list-tasks
#
# 更多参数（max-iters / token-fuse-limit / ...）直接透传给 core/main.py。
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$DIR/config.env" ]]; then
  # shellcheck source=/dev/null
  source "$DIR/config.env"
else
  echo "缺少 config.env：请复制 config.env.example 并填写 宿主 JSON-RPC 地址、访问令牌与模型配置；启动步骤见根目录 README.md。" >&2
  exit 2
fi
# Run as a package from the repository root, independent of the caller cwd.
cd "$DIR/.."

# 宿主 JSON-RPC owns Agent runtimes, credentials, and E2B providers.
export XGOVERNOR_BASE_URL="${XGOVERNOR_BASE_URL:-http://127.0.0.1:8787}"
export XGOVERNOR_RUNTIME="${XGOVERNOR_RUNTIME:-xiaoo}"

# SWE-bench 数据集走官方 Hub
export HF_ENDPOINT="${HF_ENDPOINT:-https://huggingface.co}"

# ZSoft L1 任务目录（按本机部署修改）
export ZSOFT_TASKS_DIR="${ZSOFT_TASKS_DIR:-/home/lcy/muyuan-sec/benchmarks/vulnerability/zsoft-l1/tasks}"

# E2B 模板由模式注册层决定（sweverified→swepro-docker，zsoft→zsoft-docker）；
# 模板配置随 session checkpoint 保存，由 宿主 JSON-RPC 创建与恢复。

python -m src.core.main "$@"
