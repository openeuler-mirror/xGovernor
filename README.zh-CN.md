# xGovernor

[English](./README.md) | [中文](./README.zh-CN.md)

面向异构 AI agent runtime 的会话控制面（governor）。

[License](./License)
[Rust](https://www.rust-lang.org/)
[Version]()

## xGovernor 是什么？

xGovernor 是一个服务端控制面：负责打开、治理、观测 **agent 会话**，而把真正的"思考"交给可插拔的 **agent runtime**（pi、xiaoO、opencode……）。它刻意**不**实现自己的 LLM 决策环路——产品边界是经由各 runtime 自己的 SDK/API 去管理它们，并给所有 runtime 同一套生命周期、隔离与线上接口：

- **所有 runtime 共享同一套会话 API。** 客户端只面对一份 HTTP + SSE 合同；runtime 之间的差异表达为*能力*差异与带命名空间的*扩展*载荷，永远不表达为 API 形状差异。
- **沙箱生命周期是一份合同。** 执行环境（本机目录、远程 E2B 沙箱、容器……）的创建、装载、暂停、删除全部经由 provider SPI 完成，由一个纯函数、可执行的生命周期状态机约束。
- **治理内建。** 单写者会话租约与心跳、崩溃客户端的孤儿回收、按 owner 的沙箱配额、能力门控与优雅降级，以及由类型系统保证的凭证卫生（持久化的 LLM 描述符*无法表达* api key）。


## 安装依赖

**Pi（agent runtime，必装）**

```bash
npm install -g @earendil-works/pi-coding-agent
pi --version   # 需要 >= 0.84.2
```

- 全链路端到端验证于 **pi 0.84.2**（2026-08）；桥接扩展（`apps/runtime-pi/extension`）声明 `peerDependencies: ^0.84.2`、typecheck 锁定 0.84.2——更早版本的 RPC 协议未验证。
- pi 本体是 Bun 编译的独立二进制，自己加载并运行扩展，运行时不依赖 Node；Node.js **>= 22.19.0** 仅用于扩展目录的 `npm install` / `tsc` typecheck。
- 安装位置不在 `PATH` 上时，用 `XGOVERNOR_PI_EXECUTABLE` 指定绝对路径，或在 open 时经 `ext.runtime_pi.executable` 按会话指定（见 easydemo.md §1）。

**Rust / Cargo（编译 server）**

- 最低 **Rust 1.74**（edition 2021）；开发验证于 rustc/cargo **1.94.0**。
- 非测试代码最低 1.70（`Option::is_some_and`）；测试套件额外用到 `std::io::Error::other`（1.74+）。
- 关键依赖（workspace 统一钉版）：tokio ≥ 1.35、axum 0.7、rusqlite 0.32（bundled）、reqwest 0.12（rustls）、serde/serde_json 1、uuid 1.6、tracing 0.1。

**可选**

- `E2B_API_KEY` —— 启用 `e2b` 远程沙箱后端（自研 reqwest REST 客户端直连 E2B 平台，无 SDK 依赖）。
- LLM key —— `DEEPSEEK_API_KEY`（验证于 `deepseek-v4-pro`）或 pi 支持的其他 provider。



## 架构

```
          客户端 (TUI / CLI / channel)
              │  HTTP + SSE                 ← session-protocol（wire 合同）
              ▼
   ┌───────────────────────────────────────────┐
   │  xgovernor-server                          │  传输适配层 (axum)
   │  admin 127.0.0.1:8787 / tenant :8788      │
   │  SessionApplication                       │  ← crates/core：域记录、
   │  · 环境归一化                              │     能力门控、投影、
   │  · 租约表 / 孤儿回收                       │     租约与回收器
   └───────┬───────────────────────────┬───────┘
           │ 每会话 spawn                │ provider SPI
           ▼                            ▼
   pi --mode rpc 子进程          InstanceManager (local / e2b)
   + TS 扩展（工具转发）          ← crates/manager：按 owner 配额、
           │ 工具调用，本机 HTTP              全局上限、幂等创建、
           │ 桥（每会话一个 token）           补偿删除、
           ▼                                 pending-release 重试、
   ┌──────────── Bridge ────────────┐         启动 reconcile
   │  operation-protocol 转发       │
   └──────────────┬─────────────────┘
                  ▼
         沙箱（本机目录 / E2B 远程 VM）
```



系统围绕若干条显式边界组织，每条边界由一个合同 crate 拥有：


| Crate / App                 | 合同                                                                                                                                                         |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `crates/session-protocol`   | 客户端 ↔ daemon 的 HTTP/SSE wire 合同。runtime 中立的核心词汇（open / turn / 事件 / 交互 / 操作 / 错误），runtime 专有载荷走带命名空间的 `ext` 袋。                                              |
| `crates/provider-protocol`  | manager ↔ 沙箱 provider 的生命周期合同。以 opaque 的 `owner_ref` 取代业务身份、中性生命周期 reason、纯函数状态机作为可执行规格。                                                                   |
| `crates/operation-protocol` | provider 实例 attach 之后的操作面合同（exec / 文件系统 / 搜索）——`provider-protocol` 的进程内对应物。                                                                                |
| `crates/core`               | 域模型与应用层：`SessionApplication`、持有 **opaque runtime 状态**的会话记录、所有 agent runtime 接入的 `RuntimeAdapter` 接缝、租约表、孤儿回收器、准入闸门、wire 投影。                                |
| `crates/backend`            | 各 adapter 共享的 provider 实现：本机目录沙箱、E2B 远程沙箱、SQLite provider 实例账本。                                                                                            |
| `crates/manager`            | `InstanceManager`：统一的 provider 实例编排——按 owner 配额 + 全局上限、per-runtime_id 创建幂等、attach 失败补偿删除、delete 失败 pending-release 重试队列、创建路径信号量准入 + 退避重试、启动时对账（reconcile）。 |
| `apps/runtime-local`        | 架在**真实**本地沙箱之上的 mock runtime adapter——验证全链路管线，但不假装自己是 LLM。                                                                                                 |
| `apps/runtime-e2b`          | 架在**真实** E2B provider 之上的 mock runtime adapter，含沙箱内 `git clone`（git 工作区）。                                                                                  |
| `apps/runtime-pi`           | **真实** pi runtime adapter：每会话 spawn `pi --mode rpc`，以逐行 JSON 驱动，工具执行经桥接层路由进沙箱（`local` / `e2b`）。                                                            |
| `apps/server`               | 可运行的 daemon（`xgovernor-server`）：双监听器（admin 回环 + tenant）、HTTP/SSE 传输、SQLite 会话仓库、装配。                                                                        |


处处成立的设计规则：wire 核心保持 runtime 中立（任何单一 runtime 的概念都走 `ext`）；runtime 内部状态以 opaque、带版本的 blob 检疫隔离；能力分两族（沙箱 / runtime），在触达 runtime 之前完成门控；所有错误经由唯一的 wire 词汇投影出网；tenant 面只准入「git 工作区（https）+ 沙箱化 provider」的会话——fail-closed。

完整的规范性设计文档见 [docs/protocol_boundaries.md](./docs/protocol_boundaries.md)。

## 当前状态

如实陈述：控制面**闭环已对真实系统证明**——HTTP `open` → `turn` → 真实 pi 子进程 → 真实 E2B 沙箱内的工具执行 → 归一化 SSE 事件，已端到端验证三次（2026-08-15 单会话走查，见 [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md)；2026-08-17 两个互不相关的 pi agent 并行执行，见 [apps/runtime-pi/demo/mult_agent_demo.md](./apps/runtime-pi/demo/mult_agent_demo.md)；2026-08-17 `kill -9` 重启 + 同 runtime_id 惰性会话复原，见同一文档 §10.5）。全工作区 250+ 测试全绿，包括协议 crate 的依赖策略与边界词汇守卫。

今天已经交付的：

- **真实 pi runtime adapter**（`apps/runtime-pi`）——每会话一个 `pi --mode rpc` 子进程 + 桥接扩展；工具执行落在真实沙箱里，不碰 daemon 宿主机文件系统。
- **两个沙箱 provider**——`local`（本机目录）与 `e2b`（远程 VM，`E2B_API_KEY` 可选启用），走完全相同的 provider SPI 与配额管线；`InstanceManager` 提供按 owner 配额（默认 20）、全局上限（默认 1024）、创建幂等、补偿删除与 pending-release 重试队列。
- **持久化**——SQLite 会话仓库 + provider 实例账本共用一个 WAL 文件（默认 `~/.xgovernor/xgovernor.db`，`XGOVERNOR_DATA_DIR` 可覆盖）；启动时 reconcile，重启后能重新附着到仍然存活的沙箱；pi 会话另有**重启惰性复原**：open 时把重新拉起 pi 所需的会话状态持久化进 opaque 的 `SessionRecord.runtime` 坑位，daemon 重启后同 `runtime_id` 透明续上对话（2026-08-17 已用 `kill -9` 端到端验证，见 demo §10.5）。
- **双监听面**——仅回环的 admin 面（`XGOVERNOR_BIND_ADDR`）与 tenant 面（`XGOVERNOR_TENANT_BIND_ADDR`）；启动时必须配置 token；tenant 会话只准入「git 工作区（仅 https，过 URL 卫生检查）+ 沙箱化 provider（`e2b`）」。
- **生命周期与防御**——单写者租约 + 心跳、死亡客户端孤儿回收、真实 turn 取消、带强制退出时限的优雅关闭、传输层超时 / 请求体上限 / 并发上限 / SSE 流 TTL。

诚实的缺口（见 Roadmap）：沙箱已死/会话文件丢失时 fail-closed，绝不静默冷启动假装复原）；操作面尚未暴露为 HTTP 路由；渠道适配器（飞书 / Telegram / cron / MCP）是仓内旧实现，待迁移；xiaoO / opencode 尚未接入；e2b 的 git 工作区只支持**公开 https URL**（按设计不注入凭据，私有仓库无法 clone）。

## 快速开始

```bash
cargo run -p xgovernor-server
```


| 环境变量                               | 默认值              | 含义                                                                        |
| ---------------------------------- | ---------------- | ------------------------------------------------------------------------- |
| `XGOVERNOR_BIND_ADDR`              | `127.0.0.1:8787` | admin 监听地址（必须是回环地址）                                                       |
| `XGOVERNOR_TENANT_BIND_ADDR`       | *(必填，无默认)*       | tenant 监听地址（可对外）                                                          |
| `XGOVERNOR_BEARER_TOKEN`           | *(必填)*           | admin 面 token                                                             |
| `XGOVERNOR_TENANT_TOKENS_JSON`     | *(必填)*           | tenant 面 token 表：`[{"token": "...", "tenant_id": "...", "quota": {...}}]` |
| `XGOVERNOR_DATA_DIR`               | `~/.xgovernor`   | SQLite 数据库所在目录                                                            |
| `XGOVERNOR_DEFAULT_WORKSPACE_ROOT` | 系统临时目录           | `workspace: daemon_default` 使用的工作区根                                       |
| `E2B_API_KEY`                      | *(未设置)*          | 设置后注册 `e2b` 后端（否则仅 local）                                                 |
| `DEEPSEEK_API_KEY` *等*             | *(未设置)*          | 透传给 pi 子进程的 LLM key                                                       |


最小可跑配置与完整的单会话走查见 [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md)（§2–§9）。一句话形态：

```bash
export XGOVERNOR_TENANT_BIND_ADDR=127.0.0.1:8788
export XGOVERNOR_BEARER_TOKEN=demo-admin-token
export XGOVERNOR_TENANT_TOKENS_JSON='[{"token":"demo-tenant-token","tenant_id":"demo-tenant"}]'
export E2B_API_KEY=e2b_... DEEPSEEK_API_KEY=sk-...
cargo run -p xgovernor-server
```

打开一个 pi 会话（每个 pi 会话必须声明 `ext.runtime_pi.backend_id`，`local` 或 `e2b`）：

```bash
curl -s localhost:8787/api/v1/sessions/open -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "conversation_id": "demo",
  "sender_id": "me",
  "workspace": { "kind": "daemon_default" },
  "ext": { "runtime_pi": { "backend_id": "e2b" } }
}'
# → SessionOpenResponse：runtime_id、workspace/isolation 事实（e2b 时 boundary=remote）、生效能力集
```

提交一个 turn 并订阅其事件流：

```bash
curl -s localhost:8787/api/v1/sessions/turns -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "runtime_id": "<runtime_id>",
  "text": "查看当前目录"
}'
# → { "runtime_id": "...", "turn_id": "...", "accepted_kind": "turn" }

curl -N localhost:8787/api/v1/sessions/<runtime_id>/turns/<turn_id>/events \
  -H 'Authorization: Bearer demo-admin-token'
# → SSE：output_delta、tool_activity (begin/end) ... turn_completed | turn_failed
```

想看两个 pi agent 并行跑（仓库分析 + 联网查询，各自独立沙箱），运行 [apps/runtime-pi/demo/multi_agent_demo.sh](./apps/runtime-pi/demo/multi_agent_demo.sh)。

### API 一览


| 路由                                                     | 方法   | 用途                             |
| ------------------------------------------------------ | ---- | ------------------------------ |
| `/api/v1/health`                                       | GET  | 存活探测                           |
| `/api/v1/sessions/open`                                | POST | 打开会话（携带 `runtime_id` 时为幂等重附着）  |
| `/api/v1/sessions/turns`                               | POST | 提交 turn → 回执携带服务端签发的 `turn_id` |
| `/api/v1/sessions/{runtime_id}/turns/{turn_id}/events` | GET  | 单个 turn 的 SSE 事件流              |
| `/api/v1/sessions/interactions`                        | POST | 应答 runtime 发起的交互               |
| `/api/v1/sessions/cancel`                              | POST | 取消活跃（或指定）turn                  |
| `/api/v1/sessions/fork`                                | POST | fork 会话（能力门控）                  |
| `/api/v1/sessions/heartbeat`                           | POST | 维持租约心跳                         |
| `/api/v1/sessions/detach`                              | POST | 释放租约、保留会话                      |
| `/api/v1/sessions/close`                               | POST | 关闭会话（销毁沙箱）                     |




## Roadmap

- **操作面 HTTP 路由** —— exec / 文件读写 / checkpoint-checkout 经 HTTP 暴露，而不只是进程内 SPI。
- **进行中 turn 的跨重启续跑** —— 沙箱账本与已完成的 turn 已能跨重启（惰性复原，2026-08-17 交付）；daemon 死亡时正在跑的 turn 仍按设计丢弃，续跑是剩下的最后一块。
- **e2b git 工作区的受控凭据注入** —— 不把凭据嵌进 URL 也能 clone 私有仓库（必须过现有 URL 卫生闸门）。
- **外部认证 →** `owner_ref` —— 从认证主体推导租户身份；目前由 token 表决定角色。
- **接入适配器** —— 渠道（飞书 / Telegram）、cron 触发、MCP 面，以会话 API 之上的薄适配层形态重建（旧实现仍在仓内，待迁移）。
- **更多 runtime** —— xiaoO / opencode 经 ACP 对齐的归一化事件模型接入。



## 开发

```bash
cargo test --workspace
```

协议 crate 具备自我守卫：依赖策略测试把 `session-protocol` 的依赖闭包钉死在 `serde`/`serde_json`/`thiserror`；边界词汇测试会在业务身份泄入 provider 合同、或实现词汇泄入 wire 合同时使构建失败。协议 crate 的任何 JSON 形态 diff 都应视为 wire 变更来评审。`apps/runtime-pi` 的合同测试用 fake pi 二进制驱动真实 adapter；实跑 demo 还需要 `PATH` 上有 `pi`、一个 LLM key，以及可选的 `E2B_API_KEY`。

## 文档


| 文档                                                                                 | 内容                                                        |
| ---------------------------------------------------------------------------------- | --------------------------------------------------------- |
| [docs/protocol_boundaries.md](./docs/protocol_boundaries.md)                       | 规范：协议 crate 边界、准入判据、能力模型、runtime adapter 接缝规则             |
| [docs/session_orchestration_skeleton.md](./docs/session_orchestration_skeleton.md) | 已落地编排的工作方式：最小闭环、租约表、孤儿回收（A/B/C 组件）                        |
| [docs/http_api.md](./docs/http_api.md)                                             | Wire 参考：全部路由、请求/响应形态、SSE 事件词汇、错误码                         |
| [docs/runtime_adapter_guide.md](./docs/runtime_adapter_guide.md)                   | 接入新 agent runtime：trait 义务、事件映射、能力宣告、检查清单                 |
| [docs/tenancy_design.md](./docs/tenancy_design.md)                                 | 多租户设计：身份链条、admin/租户信任公理、git-only 沙箱工作区、配额                 |
| [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md)                         | 单会话端到端 demo：真实 `pi --mode rpc` + DeepSeek + E2B，实跑记录、`kill -9` 重启 + 惰性复原（§10.5）、踩坑清单 |
| [apps/runtime-pi/demo/mult_agent_demo.md](./apps/runtime-pi/demo/mult_agent_demo.md)           | 双 agent 并行 demo：独立 pi 会话（仓库分析 + 联网查询）、隔离模型、结果判读           |




## 许可证

[MulanPSL-2.0](./License)