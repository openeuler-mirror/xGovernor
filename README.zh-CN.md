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
| `XGOVERNOR_BEARER_TOKEN` | *(必填)* | admin 面 token |
| `XGOVERNOR_TENANT_TOKENS_JSON` | *(必填)* | tenant 面 token 表：`[{"token":"...","tenant_id":"..."}]` |
| `XGOVERNOR_DATA_DIR` | `~/.xgovernor` | SQLite 数据库所在目录 |
| `XGOVERNOR_DEFAULT_WORKSPACE_ROOT` | 系统临时目录 | 会话工作区根目录 |
| `E2B_API_KEY` | *(未设置)* | 设置后注册 `e2b` 远程沙箱后端 |
| `DEEPSEEK_API_KEY` 等 | *(未设置)* | 透传给 pi 子进程的 LLM key |

最小启动配置：

```bash
export XGOVERNOR_TENANT_BIND_ADDR=127.0.0.1:8788
export XGOVERNOR_BEARER_TOKEN=demo-admin-token
export XGOVERNOR_TENANT_TOKENS_JSON='[{"token":"demo-tenant-token","tenant_id":"demo-tenant"}]'
export DEEPSEEK_API_KEY=sk-...
cargo run -p xgovernor-server
```

## 最小跑通一个 case

以下请求都打 admin 面（`127.0.0.1:8787`，`Authorization: Bearer demo-admin-token`）。

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

## 开发

```bash
cargo test --workspace
```

## 许可证

[MulanPSL-2.0](./License)
