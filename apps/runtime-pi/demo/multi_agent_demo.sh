#!/usr/bin/env bash
#
# multi_agent_demo.sh — xGovernor 双 Pi agent 并行测试
#
# 起一个 xgovernor-server，同时打开两个互不相关的 Pi 会话：
#   Agent A (repo)：分析本仓库主要作用 —— local backend，工作区 = 仓库目录，只读分析
#   Agent B (stock)：联网查询今日 A 股大盘涨跌 —— e2b backend 时在远程 E2B 沙箱内联网；
#                    未配置 E2B_API_KEY 时退化为 local backend（宿主机网络）
#
# 两个 turn 背靠背提交、两条 SSE 流并发拉取，验证「互不影响、各自执行」：
# 每个会话有独立的 pi 子进程、独立的桥接层 token、独立的沙箱/工作区，
# 唯一共享的是 DeepSeek API 配额与 daemon 主机本身。
#
# 用法：
#   DEEPSEEK_API_KEY=sk-... E2B_API_KEY=e2b_... bash apps/runtime-pi/demo/multi_agent_demo.sh
#   bash apps/runtime-pi/demo/multi_agent_demo.sh --keep-server   # 跑完后保留 server 进程
#
# 可调环境变量：
#   XGOVERNOR_BIND_ADDR / XGOVERNOR_TENANT_BIND_ADDR / XGOVERNOR_ADMIN_TOKEN（写入 tenants.toml 的 admin token，默认 demo-admin-token）
#   XGOVERNOR_REPO_DIR        Agent A 分析的仓库（默认：本仓库根目录）
#   XGOVERNOR_DEMO_TIMEOUT_S  单个 agent 的等待上限（秒，默认 600）
#   XGOVERNOR_DEMO_ROOT       演示数据目录（默认 mktemp）
#   PROMPT_A / PROMPT_B       覆盖两个 agent 的 prompt
#
# 配套文档：apps/runtime-pi/demo/mult_agent_demo.md
set -euo pipefail

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

ADMIN_ADDR="${XGOVERNOR_BIND_ADDR:-127.0.0.1:8787}"
TENANT_ADDR="${XGOVERNOR_TENANT_BIND_ADDR:-127.0.0.1:8788}"
ADMIN_TOKEN="${XGOVERNOR_ADMIN_TOKEN:-demo-admin-token}"
REPO_DIR="${XGOVERNOR_REPO_DIR:-$REPO_ROOT}"
TIMEOUT_S="${XGOVERNOR_DEMO_TIMEOUT_S:-600}"
DEMO_ROOT="${XGOVERNOR_DEMO_ROOT:-$(mktemp -d /tmp/xgovernor-two-agent.XXXXXX)}"
WORK_HOME="$DEMO_ROOT/home"
WORK_ROOT="$DEMO_ROOT/workspace"
SERVER_BIN="$REPO_ROOT/target/debug/xgovernor-server"

KEEP_SERVER=0
if [[ "${1:-}" == "--keep-server" ]]; then KEEP_SERVER=1; fi

PROMPT_A="${PROMPT_A:-请分析当前 git 仓库（工作区根目录）的主要作用：阅读 README.md 和 README.zh-CN.md、crates/ 与 apps/ 的目录结构，然后给出一份简洁的中文总结——这个项目是做什么的、核心架构是什么、当前完成度如何。只读分析：不要修改任何文件，不要运行可能产生副作用的命令。}"
PROMPT_B="${PROMPT_B:-请查询今天 A 股大盘的涨跌情况。用 bash + curl 获取上证指数、深证成指、创业板指的实时行情，例如：curl -s 'https://qt.gtimg.cn/q=sh000001,sz399001,sz399006'（返回 GBK 编码，可用 iconv -f gbk -t utf-8 转码），也可以尝试东方财富等公开行情接口。以实际返回的数据为准报告涨跌点数与百分比；数据拿不到就如实说明，不要编造。}"

API_URL="http://$ADMIN_ADDR/api/v1"
AUTH="Authorization: Bearer $ADMIN_TOKEN"
CT="Content-Type: application/json"

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
say()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m  ✓ %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m  ! %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31m  ✗ %s\033[0m\n' "$*"; }

api_post() { # api_post <path> <json> → stdout
  curl -sS -X POST "$API_URL/$1" -H "$AUTH" -H "$CT" -d "$2"
}

# 从 SSE 文件里提取终态 kind（turn_completed / turn_failed），没有则输出空
# （文件为空时 grep 无匹配返回 1，`|| true` 保证在 set -e 下不中断轮询）
terminal_kind() {
  tr -d '\r' < "$1" | grep '^data: ' | sed 's/^data: //' \
    | jq -r 'select(.kind=="turn_completed" or .kind=="turn_failed") | .kind' | tail -1 || true
}

terminal_error() { # 终态是 turn_failed 时的错误摘要
  tr -d '\r' < "$1" | grep '^data: ' | sed 's/^data: //' \
    | jq -r 'select(.kind=="turn_failed") | "\(.error.code // "?") \(.error.message // "")"' | tail -1 || true
}

final_text() { # 拼接全部 output_delta
  tr -d '\r' < "$1" | grep '^data: ' | sed 's/^data: //' \
    | jq -r 'select(.kind=="output_delta") | .delta' | tr -d '\n' || true
}

tool_activities() { # 工具活动清单
  tr -d '\r' < "$1" | grep '^data: ' | sed 's/^data: //' \
    | jq -r 'select(.kind=="tool_activity") | "\(.phase) \(.name) \(.status)"' || true
}

wait_health() {
  local deadline=$(( $(date +%s) + 60 ))
  while :; do
    if curl -sS -o /dev/null --max-time 2 "$API_URL/health" 2>/dev/null; then return 0; fi
    if [[ $(date +%s) -ge $deadline ]]; then return 1; fi
    sleep 1
  done
}

# ---------------------------------------------------------------------------
# 前置检查
# ---------------------------------------------------------------------------
say "前置检查"
if ! command -v pi >/dev/null 2>&1; then
  fail "找不到 pi 可执行文件，请先 npm install -g @earendil-works/pi-coding-agent"
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then fail "需要 jq"; exit 1; fi
if [[ -z "${DEEPSEEK_API_KEY:-}" ]]; then
  fail "DEEPSEEK_API_KEY 未设置（pi 的 LLM key，见 easydemo.md §0.1）"
  exit 1
fi
ok "pi $(pi --version 2>/dev/null | head -1)"
if [[ -n "${E2B_API_KEY:-}" ]]; then
  ok "E2B_API_KEY 已配置 → Agent B 走 e2b 远程沙箱"
  E2B_CONFIGURED=1
else
  warn "E2B_API_KEY 未配置 → Agent B 退化为 local backend（宿主机网络）；配置后即为完整 e2b 场景"
  E2B_CONFIGURED=0
fi
[[ -d "$REPO_DIR/.git" ]] && ok "Agent A 工作区：$REPO_DIR" || { fail "Agent A 工作区不是 git 仓库：$REPO_DIR"; exit 1; }

mkdir -p "$WORK_HOME" "$WORK_ROOT"

# ---------------------------------------------------------------------------
# 构建 + 启动 server
# ---------------------------------------------------------------------------
say "构建并启动 xgovernor-server"
if [[ ! -x "$SERVER_BIN" ]]; then
  (cd "$REPO_ROOT" && cargo build -p xgovernor-server) 2>&1 | tail -3
fi
mkdir -p "$WORK_HOME/.xgovernor"
cat > "$WORK_HOME/.xgovernor/tenants.toml" <<EOF
[admin]
tokens = ["$ADMIN_TOKEN"]

[[tenant]]
tenant_id = "demo-tenant"
tokens = ["demo-tenant-token"]
EOF

(
  export XGOVERNOR_BIND_ADDR="$ADMIN_ADDR"
  export XGOVERNOR_TENANT_BIND_ADDR="$TENANT_ADDR"
  export XGOVERNOR_DEFAULT_WORKSPACE_ROOT="$WORK_ROOT"
  export XGOVERNOR_DATA_DIR="$WORK_HOME/.xgovernor"
  export HOME="$WORK_HOME"
  # 在仓库根目录启动：pi 子进程的宿主 cwd == Agent A 的 local 工作区，
  # 避免 easydemo.md §11 记录的「ls cwd 锚定」问题（Agent B 的 e2b 工作区在
  # 沙箱内，不受影响，pi 会按已验证的行为自动降级用 bash）。
  cd "$REPO_DIR"
  exec "$SERVER_BIN"
) > "$DEMO_ROOT/server.log" 2>&1 &
SERVER_PID=$!
ok "server pid=$SERVER_PID, 日志=$DEMO_ROOT/server.log"

if ! wait_health; then
  fail "server 未在 60s 内就绪，日志尾部："
  tail -20 "$DEMO_ROOT/server.log"
  kill "$SERVER_PID" 2>/dev/null || true
  exit 1
fi
ok "server 就绪: $API_URL"

CURL_A_PID=""; CURL_B_PID=""
cleanup() {
  [[ -n "$CURL_A_PID" ]] && kill "$CURL_A_PID" 2>/dev/null || true
  [[ -n "$CURL_B_PID" ]] && kill "$CURL_B_PID" 2>/dev/null || true
  if [[ -n "${RID_A:-}" ]]; then
    api_post "sessions/close" "{\"runtime_id\":\"$RID_A\"}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${RID_B:-}" ]]; then
    api_post "sessions/close" "{\"runtime_id\":\"$RID_B\"}" >/dev/null 2>&1 || true
  fi
  if [[ "$KEEP_SERVER" != 1 && -n "${SERVER_PID:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 打开两个会话（背靠背，互不等待）
# ---------------------------------------------------------------------------
say "打开两个会话"

T0=$(date +%s)
open_a=$(api_post "sessions/open" "{
  \"conversation_id\": \"two-agent-repo\",
  \"sender_id\": \"demo-user\",
  \"workspace\": { \"kind\": \"local_path\", \"path\": \"$REPO_DIR\" },
  \"ext\": { \"runtime_pi\": { \"backend_id\": \"local\" } }
}")
RID_A=$(printf '%s' "$open_a" | jq -r '.runtime_id // empty')
if [[ -z "$RID_A" ]]; then fail "open A 失败: $open_a"; exit 1; fi
T_OPEN_A=$(date +%s)
ok "Agent A(repo)  runtime_id=$RID_A  isolation=$(printf '%s' "$open_a" | jq -c '.isolation')"

open_b=$(api_post "sessions/open" "{
  \"conversation_id\": \"two-agent-stock\",
  \"sender_id\": \"demo-user\",
  \"workspace\": { \"kind\": \"daemon_default\" },
  \"ext\": { \"runtime_pi\": { \"backend_id\": \"$([[ $E2B_CONFIGURED == 1 ]] && echo e2b || echo local)\" } }
}")
RID_B=$(printf '%s' "$open_b" | jq -r '.runtime_id // empty')
if [[ -z "$RID_B" ]]; then fail "open B 失败: $open_b"; exit 1; fi
T_OPEN_B=$(date +%s)
ok "Agent B(stock) runtime_id=$RID_B  isolation=$(printf '%s' "$open_b" | jq -c '.isolation')"

# ---------------------------------------------------------------------------
# 背靠背提交两个 turn
# ---------------------------------------------------------------------------
say "提交两个 turn（背靠背，不等对方完成）"
turn_a=$(api_post "sessions/turns" "{\"runtime_id\":\"$RID_A\",\"text\":$(jq -Rn --arg t "$PROMPT_A" '$t')}")
TURN_A=$(printf '%s' "$turn_a" | jq -r '.turn_id // empty')
T_SUBMIT_A=$(date +%s)
ok "turn A id=$TURN_A"

turn_b=$(api_post "sessions/turns" "{\"runtime_id\":\"$RID_B\",\"text\":$(jq -Rn --arg t "$PROMPT_B" '$t')}")
TURN_B=$(printf '%s' "$turn_b" | jq -r '.turn_id // empty')
T_SUBMIT_B=$(date +%s)
ok "turn B id=$TURN_B"

# ---------------------------------------------------------------------------
# 并发订阅两条 SSE 事件流
# ---------------------------------------------------------------------------
say "并发订阅事件流（两条 SSE 同时拉取）"
curl -sS -N "$API_URL/sessions/$RID_A/turns/$TURN_A/events" -H "$AUTH" > "$DEMO_ROOT/events_a.txt" 2>/dev/null &
CURL_A_PID=$!
curl -sS -N "$API_URL/sessions/$RID_B/turns/$TURN_B/events" -H "$AUTH" > "$DEMO_ROOT/events_b.txt" 2>/dev/null &
CURL_B_PID=$!
ok "events: $DEMO_ROOT/events_a.txt / events_b.txt"

# ---------------------------------------------------------------------------
# 等待两个终态
# ---------------------------------------------------------------------------
say "等待两个 agent 完成（上限 ${TIMEOUT_S}s）"
deadline=$(( $(date +%s) + TIMEOUT_S ))
TERM_A=""; TERM_B=""
while :; do
  TERM_A=$(terminal_kind "$DEMO_ROOT/events_a.txt")
  TERM_B=$(terminal_kind "$DEMO_ROOT/events_b.txt")
  if [[ -n "$TERM_A" && -n "$TERM_B" ]]; then break; fi
  if [[ $(date +%s) -ge $deadline ]]; then
    warn "超时：A=${TERM_A:-running} B=${TERM_B:-running}，将取消并收尾"
    api_post "sessions/cancel" "{\"runtime_id\":\"$RID_A\"}" >/dev/null 2>&1 || true
    api_post "sessions/cancel" "{\"runtime_id\":\"$RID_B\"}" >/dev/null 2>&1 || true
    break
  fi
  sleep 2
done
T_TERM_A=$(date +%s); T_TERM_B=$T_TERM_A
[[ -n "$TERM_A" ]] && T_TERM_A=$(date +%s)
[[ -n "$TERM_B" ]] && T_TERM_B=$(date +%s)

# ---------------------------------------------------------------------------
# 汇总
# ---------------------------------------------------------------------------
say "结果汇总"

# 并行性证据
overlap_start=$(( T_SUBMIT_A < T_SUBMIT_B ? T_SUBMIT_A : T_SUBMIT_B ))
overlap_end=$(( T_TERM_A < T_TERM_B ? T_TERM_A : T_TERM_B ))
overlap=$(( overlap_end - overlap_start ))
[[ $overlap -lt 0 ]] && overlap=0
echo "  时间线（秒，相对 t0=${T0}）："
echo "    open A @$((T_OPEN_A-T0))  open B @$((T_OPEN_B-T0))"
echo "    submit A @$((T_SUBMIT_A-T0))  submit B @$((T_SUBMIT_B-T0))"
echo "    terminal A @$((T_TERM_A-T0))  terminal B @$((T_TERM_B-T0))"
echo "    两 turn 同时在飞时长：${overlap}s"
if [[ $overlap -gt 0 ]]; then
  ok "并行性成立：两个 turn 曾同时处于执行中"
else
  fail "未观察到并行窗口（其中一个可能极快结束）"
fi

for agent in A B; do
  eval "rid=\$RID_$agent"; eval "term=\$TERM_$agent"; eval "turn=\$TURN_$agent"
  file="$DEMO_ROOT/events_$(echo $agent | tr A-Z a-z).txt"
  echo ""
  echo "── Agent $agent (runtime_id=$rid, turn=$turn) ──"
  if [[ -z "$term" ]]; then
    fail "无终态（超时或被取消）"
  elif [[ "$term" == "turn_failed" ]]; then
    fail "turn_failed: $(terminal_error "$file")"
  else
    ok "outcome=complete"
  fi
  echo "  工具活动："
  if [[ -n "$(tool_activities "$file")" ]]; then
    tool_activities "$file" | sed 's/^/    /'
  else
    echo "    （无）"
  fi
  echo "  最终输出："
  final_text "$file" | fold -s -w 100 | sed 's/^/    /'
  echo ""
done

# e2b 沙箱生命周期验证（配置了 E2B key 时）
if [[ $E2B_CONFIGURED == 1 ]]; then
  say "E2B 沙箱生命周期"
  before=$(curl -sS "https://api.e2b.dev/sandboxes" -H "X-API-Key: $E2B_API_KEY" | jq 'length')
  echo "  close 前 running 沙箱数：$before"
  api_post "sessions/close" "{\"runtime_id\":\"$RID_A\"}" >/dev/null 2>&1 || true
  api_post "sessions/close" "{\"runtime_id\":\"$RID_B\"}" >/dev/null 2>&1 || true
  RID_A=""; RID_B=""
  sleep 5
  after=$(curl -sS "https://api.e2b.dev/sandboxes" -H "X-API-Key: $E2B_API_KEY" | jq 'length')
  echo "  close 后 running 沙箱数：$after"
fi

say "收尾"
if [[ "$KEEP_SERVER" == 1 ]]; then
  ok "server 保持运行（pid=${SERVER_PID}），demo 数据在 $DEMO_ROOT"
else
  ok "server 已停止，demo 数据在 ${DEMO_ROOT}（保留供复盘）"
fi

if [[ "$TERM_A" == "turn_completed" && "$TERM_B" == "turn_completed" ]]; then
  echo ""
  ok "双 Agent 并行测试通过"
  exit 0
else
  echo ""
  fail "双 Agent 并行测试未完全通过（A=${TERM_A} B=${TERM_B}）"
  exit 1
fi
