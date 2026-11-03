# xiaoO-MCTS

基于 MCTS JSON-RPC v1 的任务搜索插件。插件进程只通过 stdio JSON-RPC 使用宿主能力；当前宿主是 xg。插件不创建 HTTP 客户端，不读取模型/E2B 凭据，也不管理运行时会话。

## 快速验证（本地，不需要模型或 E2B）

前置：插件代码位于 xg 的 `plugins/MCTS`（xg 仓库不包含插件源码；工作区没有该目录时先 clone 到该路径）、Python ≥ 3.11、Rust ≥ 1.89。这条路径用 `mode: "mock"` 拉起真实插件进程，验证 xg → 插件 → stdio 协议整条链路。

```bash
# 1. 安装插件依赖（已有装了 jsonschema 的 Python 可跳过）
python3 -m venv plugins/MCTS/.venv
plugins/MCTS/.venv/bin/pip install -r plugins/MCTS/src/requirements.txt
```

```bash
# 2. 启动 xg：空数据目录 = 开发模式（请求不鉴权），两端口都必须设置
export XGOVERNOR_DATA_DIR=$(mktemp -d)
export XGOVERNOR_TENANT_BIND_ADDR=127.0.0.1:8788
export MCTS_REPO_ROOT=$PWD/plugins/MCTS
export MCTS_PYTHON_BIN=$PWD/plugins/MCTS/.venv/bin/python
cargo run -p xgovernor-server
```

```bash
# 3. 另开终端：提交 mock run 并轮询
curl -sS -X POST http://127.0.0.1:8787/api/v1/mcts/runs \
  -H 'content-type: application/json' \
  -d '{
    "plugin_id": "mcts",
    "mode": "mock",
    "run_id": "smoke-1",
    "root_state_id": "state-root",
    "goal": "smoke",
    "content": "smoke",
    "budget": {"max_iters": 2, "branching": 1, "max_time_ms": 15000, "max_tokens": 1000}
  }'

curl -sS http://127.0.0.1:8787/api/v1/mcts/runs/smoke-1
```

GET 返回 `"status":"completed"`、`outcome.result.candidates` 有内容，就说明接入正常（mock 通常 1 秒内完成，必要时隔一两秒再查）。`mock` 不调用模型和 E2B，结果只是合成夹具，用来验证进程、协议和生命周期。

- 开发模式只在没有 `tenants.toml` 时生效；如果数据目录里已有 token 配置，请加 `-H "authorization: Bearer <admin token>"`。
- 端口冲突时同时改 `XGOVERNOR_BIND_ADDR` / `XGOVERNOR_TENANT_BIND_ADDR` 和 curl 地址。
- mock 必须显式给 `root_state_id`：省略时 xg 会去准备真实根状态，而 mock 没有对应准备器；这个 id 只是占位，不会创建或删除真实 checkpoint。

如果 xg 已经按其它方式在跑，只需要补上 `MCTS_REPO_ROOT` / `MCTS_PYTHON_BIN` 后重启 xg，再执行第 3 步即可验证。

## 跑真实任务（zsoft / sweverified）

真实搜索需要支持快照的 provider——当前开箱即用的是 E2B；宿主机上的 `local` provider 不支持快照，跑不了真实搜索。凭据全部留在 xg 进程，插件只拿到白名单环境变量。

```bash
# xg 服务端（在 xg 仓库根目录）
export XGOVERNOR_TENANT_BIND_ADDR=127.0.0.1:8788
export XGOVERNOR_DATA_DIR=$HOME/.xgovernor      # 生产环境不要用临时目录
export E2B_API_KEY=e2b_...
export XGOVERNOR_LLM_PROVIDER=deepseek
export XGOVERNOR_LLM_MODEL="<model>"
export XGOVERNOR_LLM_KEY_ENV=DEEPSEEK_API_KEY
export DEEPSEEK_API_KEY=...
export MCTS_REPO_ROOT=$PWD/plugins/MCTS
export MCTS_PYTHON_BIN=$PWD/plugins/MCTS/.venv/bin/python
export ZSOFT_TASKS_DIR=/path/to/zsoft-l1/tasks  # 仅 curl 让 xg 自动准备根状态时需要
cargo run -p xgovernor-server
```

```bash
# 另开终端：先准备可恢复根状态，再提交 run（venv 按快速验证第 1 步创建）
cd plugins/MCTS
export XGOVERNOR_BASE_URL=http://127.0.0.1:8787
export XGOVERNOR_API_TOKEN="<admin token>"      # 数据目录里没有 tenants.toml 时留空
export ZSOFT_TASKS_DIR=/path/to/zsoft-l1/tasks  # 准备根状态时在客户端读取任务

.venv/bin/python -m src.harness.run_plugin_benchmark \
  --mode zsoft --task-id "<task-id>" --output-dir /tmp/mcts-zsoft \
  --prepare-root-state

.venv/bin/python -m src.harness.run_plugin_benchmark \
  --mode zsoft --task-id "<task-id>" --output-dir /tmp/mcts-zsoft \
  --call-mcts --max-iters 12 --branching 3
```

`--call-mcts` 从 `<output-dir>/root_state_id` 读取根状态，轮询到终态，并把 `run.json`、`completion.json`、`protocol.jsonl` 写到输出目录；`--capabilities` 缺省授予全部 9 项能力。

SWE-bench Verified 把 `--mode` 换成 `sweverified`、`--task-id` 换成 instance id，并先装数据集依赖：

```bash
cd plugins/MCTS
.venv/bin/pip install datasets
.venv/bin/python -m src.harness.run_plugin_benchmark \
  --mode sweverified --task-id astropy__astropy-12907 --split test \
  --output-dir /tmp/mcts-swe --prepare-root-state
.venv/bin/python -m src.harness.run_plugin_benchmark \
  --mode sweverified --task-id astropy__astropy-12907 --split test \
  --output-dir /tmp/mcts-swe --call-mcts
```

也可以直接一条 curl，让 xg 自己准备根状态（省略 `root_state_id`）：

```bash
curl -sS -X POST http://127.0.0.1:8787/api/v1/mcts/runs \
  -H 'content-type: application/json' \
  -d '{
    "plugin_id": "mcts",
    "mode": "zsoft",
    "task_id": "<task-id>",
    "goal": "修复任务描述中的问题",
    "content": "<任务上下文>",
    "budget": {"max_iters": 12, "branching": 3, "max_time_ms": 3600000, "max_tokens": 20000000},
    "granted_capabilities": ["snapshot", "fork", "agent_turn", "inspect", "exec", "file_read", "file_write", "diff", "release"]
  }'
```

这条路径下根状态由 xg 服务端准备，所以 `ZSOFT_TASKS_DIR`（zsoft）或 `datasets` + HF 访问（sweverified）要配置在 xg 服务端环境里；准备最长阻塞 30 分钟。

### Provider 支持

MCTS 依赖 checkpoint/load 和 snapshot/fork，provider 必须能提供快照：

| provider | 能否跑真实 MCTS | 说明 |
| --- | --- | --- |
| `e2b` | 能（默认） | harness 的根状态准备固定 `backend_id: "e2b"`；需要 `E2B_API_KEY` |
| `local`（宿主机目录） | 不能 | 只有 exec/files/network；带 `snapshot` 能力开会直接 422 `unsupported_capability`，`checkpoint` 也未实现，因此没有分支所需的状态快照 |
| `docker`（宿主机 Docker） | 理论上能，当前未打通 | 设 `XGOVERNOR_DOCKER_SNAPSHOT_ENABLED=1` 后才会声明 snapshot；当前 harness 没有选择 backend 的入口，需自行用会话 API 准备 docker checkpoint 再把 id 传给 `/mcts`，这条路径未经验证 |

插件本身与 provider 无关：只要宿主实现 `host.snapshot` / `host.fork` 就能跑；上表限制来自 xg 当前 provider 能力。`mode: "mock"` 不需要任何 provider，只验证协议链路。

## 接入配置

| 变量 | 作用 | 默认 |
| --- | --- | --- |
| `MCTS_REPO_ROOT` | 插件仓库根目录（不是 xg 仓库根，建议绝对路径）；xg 以它为插件进程 cwd，并读取 `src/plugin/plugin_manifest.json` 与提示词 | `.`（相对 xg 的 cwd） |
| `MCTS_PYTHON_BIN` | 启动插件和根状态准备的解释器，必须是绝对路径 | `python` |
| `XGOVERNOR_BASE_URL` | 省略 `root_state_id` 时，根状态准备访问的 xg 地址 | `http://127.0.0.1:8787` |
| `XGOVERNOR_ADMIN_TOKEN` | 根状态准备使用的 token；未设置时准备 CLI 回落到服务端的 `XGOVERNOR_API_TOKEN`，两者都没有则不带鉴权头 | 无 |

模型配置由 xg 服务端持有（`XGOVERNOR_LLM_PROVIDER` / `XGOVERNOR_LLM_MODEL` / `XGOVERNOR_LLM_API_BASE` / `XGOVERNOR_LLM_KEY_ENV`），插件永远拿不到凭据；请求里的 `llm` 字段不会改变 xg 实际使用的模型。

插件进程以空环境启动，xg 只注入 `apps/server/src/httpserver/mcts.rs` 中 `MCTS_PLUGIN_ENV_ALLOWLIST` 列出的变量：

```
ZSOFT_TASKS_DIR  ZSOFT_BUNDLE_DIR  ZSOFT_BIN  ZSOFT_SERVER_URL  ZSOFT_BENCH_ROOT
ZSOFT_TRIAL_SERVER_URL  PYTHONPATH
XIAOO_MCTS_TOKEN_FUSE_LIMIT  XIAOO_MCTS_MAX_CONCURRENT  XIAOO_MCTS_MEMORY_POOL
HF_ENDPOINT  HF_HOME  HF_DATASETS_CACHE
```

因此 `PATH`、`HOME` 等不会继承（`MCTS_PYTHON_BIN` 必须绝对路径）；zsoft 的 `ZSOFT_TASKS_DIR` 要在 xg 服务端环境里设置；其它调试变量（如 `XIAOO_AUTHOR_MATERIALIZE_ATTEMPTS`、`XIAOO_RETEST_CACHE`）需要先加入该白名单才能到达插件。

`/api/v1/mcts/runs` 挂在 admin/tenant 两类监听面上，调用方 token 需与监听面角色匹配（admin 面用 admin token，tenant 面用 tenant token）。

## `/api/v1/mcts/runs` 契约

请求字段：

| 字段 | 说明 |
| --- | --- |
| `plugin_id` | 固定 `mcts` |
| `mode` | `zsoft` / `sweverified`；`mock` 仅供联调。模式的 profile 必须已在 `plugin_manifest.json` 注册 |
| `run_id` | 留空时由服务端生成 UUID |
| `task_id` / `instance_id` | zsoft 用 `task_id`，sweverified 用 `instance_id`；省略 `root_state_id` 时至少给一个 |
| `split` / `testbench` | sweverified 可选；xg 自动准备根状态时不读取它们 |
| `root_state_id` | 可省略；省略时 xg 自动准备根状态 |
| `goal` / `content` / `budget` | 必填；`budget` 的 `max_iters`、`branching`、`max_time_ms`、`max_tokens` 均为必填整数 |
| `llm` | 可选，当前不改变宿主模型配置 |
| `granted_capabilities` | 生产请求必须显式给出；实际使用 `snapshot` / `fork` / `agent_turn` / `exec` / `file_read` / `file_write` / `release`，`inspect` / `diff` 目前未使用但保留在协议里。xg 不补默认值 |

创建请求在根状态准备完成后返回 201 和 `MctsRunView`（`run_id`、`status: running`、回显的 request 等），搜索在后台进行。之后用同一 token 轮询：

```bash
curl -sS http://127.0.0.1:8787/api/v1/mcts/runs/$RUN_ID \
  -H "authorization: Bearer $XGOVERNOR_ADMIN_TOKEN"
curl -sS -X POST http://127.0.0.1:8787/api/v1/mcts/runs/$RUN_ID/cancel \
  -H "authorization: Bearer $XGOVERNOR_ADMIN_TOKEN"
```

`status` 取值为 `accepted`（未启用 spawn 的测试配置）、`running`、`completed`、`failed`、`cancelled`。终态后 `outcome` 包含 `result` / `failure` / `summary`，顶层 `protocol_jsonl` 是本次 run 的完整协议记录。取消只置位并停止调度，资源清理由 xg 在 run 返回后完成。run 记录保存在 xg 进程内存中，xg 重启后无法再按 `run_id` 查询。

根状态准备：

- 请求带 `root_state_id`：xg 直接采用，不做准备。
- 请求省略：xg 在 `MCTS_REPO_ROOT` 下执行 `python -m src.harness.xg_root_state_prepare`，固定传入 `--mode`、`--task-id`（`task_id` 或 `instance_id`）、`--base-url`（`XGOVERNOR_BASE_URL`）、`--runtime-kind pi`、`--output-dir`（临时目录），设置了 `XGOVERNOR_ADMIN_TOKEN` 时再传 `--token`。
- 该 CLI 用 xg 进程自身的环境运行（继承，不是白名单），zsoft 依赖服务端的 `ZSOFT_TASKS_DIR`。成功后把 `root_state_id` 注入请求再启动 MCTS；失败返回 500，不创建 run；run 终态后 xg 会删除这次准备的 checkpoint。
- 自动准备不转发请求里的 `split` / `testbench`，sweverified 固定使用脚本默认值（`test` / `sweverified`），runtime 也固定为 pi。需要其它组合时，用上面的 harness 自行准备并显式传 `root_state_id`。
- `xg_root_state_prepare` 只向 stdout 输出一行 JSON（`root_state_id` / `checkpoint_id` / `output_dir`），失败输出 `{"error":"prepare_failed","message":...}` 并以非零码退出；诊断信息走 stderr。

## 常见错误

- 创建 run 返回 500 且提示 `failed to build real MCTS host application` / manifest 解析失败：`MCTS_REPO_ROOT` 不是插件仓库根，或其中缺少 `src/plugin/plugin_manifest.json`。
- 插件以 `plugin.failed` 结束且提示 `capability not granted`：请求没有给对应的 `granted_capabilities`。
- 插件立即退出、`status: failed` 附带 `Eof`：`MCTS_PYTHON_BIN` 用了依赖 `PATH` 的名字（插件进程不继承 `PATH`，必须绝对路径），或解释器没装 `src/requirements.txt`。
- zsoft 启动时报 `cannot locate zsoft-l1 tasks`：xg 服务端环境没有设置 `ZSOFT_TASKS_DIR`（插件进程和根状态准备都需要）。
- 省略 `root_state_id` 但返回 `task_id or instance_id is required`：根状态准备至少需要其中一个标识。
- 创建 run 长时间不返回：正在同步准备根状态（最长 30 分钟）。需要进度时改用 harness 的 `--prepare-root-state`（打印人类可读日志），再用 `--call-mcts` 提交。

## 架构

xg 为每个 run 拉起一个插件进程，双方在 stdin/stdout 上交换换行分隔的 JSON-RPC v1 消息。

- 宿主 → 插件：`plugin.initialize/start/wait/cancel/shutdown`
- 插件 → 宿主：`host.fork/snapshot/agent_turn/inspect/exec/file_read/file_write/diff/release`
- 插件 → 宿主：`plugin.started/progress/completed/cancelled/failed`

xg 侧的 `crates/mcts-plugin-host` 负责 manifest 校验、进程隔离、JSON-RPC framing、生命周期、run map、租约心跳和终态清理，并把 `host.*` 映射到 xg session 能力。插件返回结构化结果和 `apply_plan`；未选中的分支在终态清理，选中候选的 state 由结果关联。stdout 只承载协议消息，日志一律写 stderr（由 xg 转到 `mcts.plugin.stderr`）。

关键目录：

- `src/core/host_api.py`：HostCapabilities、RunContext、Clock 契约。
- `src/core/mcts_coordinator.py`：无运行时依赖的 UCT 搜索。
- `src/core/result_builder.py`：结构化 completion result 和引用校验。
- `src/plugin/`：stdio transport、lifecycle、job registry 和宿主能力客户端（`host_client.py`）。
- `src/res/agents.py`、`benchmark_util.py`：通过 branch handle 调用宿主能力。
- `src/plugin/plugin_manifest.json`：mode/profile 与提示词路径清单，xg 在创建 run 时读取。
- `docs/protocols/mcts-rpc-v1.md`：协议说明；Schema 位于 `docs/protocols/mcts-rpc-v1.schema.json`。

## 测试与本地调试

Python 侧的 mock/协议测试不启动模型、沙箱或外部 HTTP 服务；测试目录为 `src/tests`（如果当前检出包含它），在插件仓库根目录执行 `python -m unittest discover -s src/tests -t . -v`。真实评分只有在配置 xg、任务实例和 benchmark 后才会产生。

stdio 握手（不发模型请求）：

```bash
python -m src.harness.stdio_driver --plugin "python -m src.core.main --mock"
```

xg 宿主的 Rust 测试（在 xg 仓库根目录执行）：

```bash
cargo test -p mcts-plugin-host
MCTS_REPO_ROOT=$PWD/plugins/MCTS MCTS_PYTHON_BIN=$(command -v python3) \
  cargo test -p mcts-plugin-host --test mcts_run_real_plugin
MCTS_REPO_ROOT=$PWD/plugins/MCTS MCTS_PYTHON_BIN=$(command -v python3) \
  cargo test -p xgovernor-server http_route_spawns_real_mock_mcts_process
```

后两条会拉起真实插件进程走 `--mock`（内置脚本化宿主，确定性搜索），用来验证 stdio 生命周期、HTTP 入口和进程隔离；它们不调用模型或 E2B。

旧的手工启动方式（`run_union.sh` + `src/config.env`，或直接 `python -m src.core.main --mode ...`）已经不适配当前接入：插件只通过 stdio 等待宿主驱动，该命令只是 xg 拉起插件时使用的 argv，单独运行会等待 stdin 上的 JSON-RPC（或在加载 zsoft 任务时因缺少 `ZSOFT_TASKS_DIR` 直接失败）。本地调试请走上面的 harness/测试入口。

## 依赖门禁

业务代码不得导入旧运行时客户端或使用 runtime/session/checkpoint 标识（在插件仓库根目录执行）：

```bash
rg -n "xgovernor_client|runtime_id|checkpoint_id|AuthorSession|SolverResult" src/core src/memory src/res --glob '*.py'
```

协议层使用 `jsonschema` Draft 2020-12 校验请求、通知和结果。
