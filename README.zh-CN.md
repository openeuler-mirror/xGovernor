# xGovernor

[English](./README.md) | [中文](./README.zh-CN.md)

面向异构 AI agent runtime 的会话控制面（governor）：一个服务端，负责打开、治理、观测 **agent 会话**，而把真正的"思考"交给可插拔的 **agent runtime**（pi、xiaoO、opencode……）。它刻意**不**实现自己的 LLM 决策环路——客户端只面对一份统一的 HTTP + SSE 会话 API；执行环境（本机目录、远程 E2B 沙箱）由沙箱 provider 统一管理；治理（会话租约、孤儿回收、配额、能力门控）内建在服务端。

[License](./License) · [Rust](https://www.rust-lang.org/) · [Version]()

## 安装

**1. Rust 工具链**（编译 server 用，最低 1.74）

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**2. pi（agent runtime，必装）**

```bash
npm install -g @earendil-works/pi-coding-agent
pi --version   # 需要 >= 0.84.2
```

桥接扩展的依赖（Node.js >= 22.19.0）：

```bash
cd apps/runtime-pi/extension && npm install
```

**3. LLM key**（pi 思考时用）

```bash
export DEEPSEEK_API_KEY=sk-...
```

可选：`E2B_API_KEY=e2b_...` 启用远程 E2B 沙箱后端；不设则只有本机 `local` 沙箱。

## 启动

`xgovernor-server` 同时监听两个面，启动时必须配置 token：

| 环境变量 | 默认值 | 含义 |
|---|---|---|
| `XGOVERNOR_BIND_ADDR` | `127.0.0.1:8787` | admin 面监听地址（必须是回环地址） |
| `XGOVERNOR_TENANT_BIND_ADDR` | *(必填，无默认)* | tenant 面监听地址 |
| `XGOVERNOR_TENANTS_CONFIG_PATH` | `$XGOVERNOR_DATA_DIR/tenants.toml` | 声明式凭证/身份策略文件（见下）；**默认路径**缺失 = dev 模式（每个请求隐式获得 admin 身份）；**显式设置**的路径缺失则 fail-closed 拒绝启动 |
| `XGOVERNOR_DATA_DIR` | `~/.xgovernor` | SQLite 数据库所在目录（默认也是 `tenants.toml` 所在目录） |
| `XGOVERNOR_DEFAULT_WORKSPACE_ROOT` | 系统临时目录 | 会话工作区根目录 |
| `E2B_API_KEY` | *(未设置)* | 设置后注册 `e2b` 远程沙箱后端 |
| `DEEPSEEK_API_KEY` 等 | *(未设置)* | 透传给 pi 子进程的 LLM key |

凭证与角色配置在 `tenants.toml` 文件里（`docs/tenancy_design.md` §4），启动时加载，支持 `SIGHUP` 热重载——轮换 token 或新增租户不需要重启：

```toml
[admin]
tokens = ["demo-admin-token"]

[[tenant]]
tenant_id = "demo-tenant"
tokens = ["demo-tenant-token"]
# principal / max_sessions / max_requests_per_minute 均可选
```

最小启动配置：

```bash
export XGOVERNOR_TENANT_BIND_ADDR=127.0.0.1:8788
mkdir -p "${XGOVERNOR_DATA_DIR:-$HOME/.xgovernor}"
cat > "${XGOVERNOR_DATA_DIR:-$HOME/.xgovernor}/tenants.toml" <<'EOF'
[admin]
tokens = ["demo-admin-token"]

[[tenant]]
tenant_id = "demo-tenant"
tokens = ["demo-tenant-token"]
EOF
export DEEPSEEK_API_KEY=sk-...
cargo run -p xgovernor-server
```

## 最小跑通一个 case

### admin 面（管理端）

以下请求打 admin 监听地址（`127.0.0.1:8787`），用 `tenants.toml` 里 `[admin]` 节配置的 admin token。

**1. 打开一个 pi 会话**

`ext.runtime_pi.backend_id` 必填：`local`（本机沙箱，无额外依赖）或 `e2b`（远程沙箱，需配置 `E2B_API_KEY`）。

```bash
curl -s localhost:8787/api/v1/sessions/open -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "conversation_id": "demo",
  "sender_id": "me",
  "workspace": { "kind": "daemon_default" },
  "ext": { "runtime_pi": { "backend_id": "local" } }
}'
# 记下响应里的 runtime_id
```

**2. 提交一个 turn**

```bash
curl -s localhost:8787/api/v1/sessions/turns -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "runtime_id": "<runtime_id>",
  "text": "查看当前目录"
}'
# → { "runtime_id": "...", "turn_id": "...", "accepted_kind": "turn" }
```

**3. 订阅事件流**（SSE，直到收到 `turn_completed` / `turn_failed` 结束）

```bash
curl -N localhost:8787/api/v1/sessions/<runtime_id>/turns/<turn_id>/events \
  -H 'Authorization: Bearer demo-admin-token'
```

看到 `turn_completed` 即跑通。更完整的走查（e2b 远程沙箱、多 agent 并行、`kill -9` 重启复原）见 [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md) 和 [apps/runtime-pi/demo/mult_agent_demo.md](./apps/runtime-pi/demo/mult_agent_demo.md)。

### tenant 面（普通租户用自己的 token）

普通租户请求打 **tenant 监听地址**（`127.0.0.1:8788`），用的是 `tenants.toml` 里**该租户自己的 token**（`demo-tenant-token`），不是 admin token。

租户会话有准入约束，与 admin 不同：

- 工作区必须是 **git 仓库**（`kind: "git"`，仅 https URL，且 URL 不能内嵌用户名密码）；`daemon_default` / `local` 在 tenant 面会被拒。注意目前只支持**公开** https 仓库——沙箱不注入任何凭据，私有仓库无法 clone。
- provider 必须是**沙箱化**的，即 `backend_id: "e2b"`（需配置 `E2B_API_KEY`）；`local` 不是沙箱，tenant 面不可用。
- 同一套 API 路径，只换监听地址和 token。

```bash
curl -s localhost:8788/api/v1/sessions/open -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-tenant-token' -d '{
  "conversation_id": "demo",
  "sender_id": "me",
  "workspace": { "kind": "git", "url": "https://github.com/example/repo.git" },
  "ext": { "runtime_pi": { "backend_id": "e2b" } }
}'
# 记下响应里的 runtime_id；沙箱内会先 git clone 该仓库再作为工作区
```

提交 turn 与订阅事件流和 admin 面完全一样（`POST /api/v1/sessions/turns`、`GET /api/v1/sessions/<runtime_id>/turns/<turn_id>/events`），只把监听地址换成 `8788`、token 换成该租户自己的。每个租户的 token 对应一个 `tenant_id`，会话按租户隔离（配额、访问互不可见）。

## 开发

```bash
cargo test --workspace
```

## 许可证

[MulanPSL-2.0](./License)
