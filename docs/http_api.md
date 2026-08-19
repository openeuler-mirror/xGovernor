# HTTP / SSE API 参考

> 性质：descriptive。本文从 `crates/session-protocol` 源码逐字段推导，描述当前已接线的 wire 面。协议的规范性设计见 [protocol_boundaries.md](./protocol_boundaries.md)。
> 任何与本文不符的行为都应视为 bug——要么改代码，要么改本文。

---

## 1. 约定

- Base path：`/api/v1`，请求与响应均为 JSON（`content-type: application/json`）。
- **鉴权**：凭证与身份来自 `tenants.toml` 声明式策略文件（[tenancy_design.md](./tenancy_design.md) §4，默认路径 `$XGOVERNOR_DATA_DIR/tenants.toml`，可用 `XGOVERNOR_TENANTS_CONFIG_PATH` 覆盖，支持 `SIGHUP` 热重载）——`[admin]` 节的 `tokens` 签发 admin 身份，`[[tenant]]` 节的 `tokens` 签发对应 `tenant_id` 的 tenant 身份。配置了该文件（且非空）时，所有路由要求 `Authorization: Bearer <token>`，未知/缺失 token 返回 401。**默认路径**下该文件不存在时视为单机 dev 模式：每个请求隐式获得 admin 身份，行为与历史版本一致；**显式**设置 `XGOVERNOR_TENANTS_CONFIG_PATH` 却指向不存在的文件，或文件存在但解析/校验失败，则 fail-closed 拒绝启动。鉴权解析出的身份是服务端事实（[tenancy_design.md](./tenancy_design.md) §1/§2/§3.1），wire 请求体中不存在、也不接受 tenant_id 字段。
- **跨租户所有权**：非 admin 身份访问不属于自己租户的 `runtime_id` 时，一律返回 `not_found`（404）而非 403——存在性对无权限的调用方不可见（[tenancy_design.md](./tenancy_design.md) §3.2 "404 不泄露存在性信息"）。admin 身份不受此约束。
- **未知字段**：请求 DTO 一律 `deny_unknown_fields`——多传字段是 400 错误，不是静默忽略。唯一例外是 `ext` 扩展袋内部。
- **ext 扩展袋**：`ext` 是 `{命名空间: 任意 JSON}` 的映射，核心协议不解释其内容，由对应 runtime adapter 消费（例如 `ext.runtime_mock`、`ext.runtime_pi`、未来的 `ext.xiaoo`）。
- **lease 声明**：所有控制请求可携带 `lease` 对象：

```json
{ "client_id": "tui-1234", "client_pid": 1234, "client_hostname": "my-host" }
```

  三个字段均可省略。语义见 [session_orchestration_skeleton.md](./session_orchestration_skeleton.md) §4：带 `client_id` 即参与单写者租约；匿名请求在无活跃租约时放行；`daemon:` 前缀保留给 daemon 内部 principal。

## 2. 路由总表


| 路由                                                     | 方法   | 请求体                       | 成功响应                         |
| ------------------------------------------------------ | ---- | ------------------------- | ---------------------------- |
| `/api/v1/health`                                       | GET  | —                         | 200                          |
| `/api/v1/sessions`                                     | GET  | —                         | 200 SessionListResponse      |
| `/api/v1/sessions/open`                                | POST | SessionOpenRequest        | 200 SessionOpenResponse      |
| `/api/v1/sessions/turns`                               | POST | SessionTurnRequest        | 202 SessionSubmitReceipt     |
| `/api/v1/sessions/{runtime_id}/turns/{turn_id}/events` | GET  | —                         | 200 SSE 流                    |
| `/api/v1/sessions/interactions`                        | POST | SessionInteractionRequest | 202 SessionSubmitReceipt     |
| `/api/v1/sessions/cancel`                              | POST | SessionCancelRequest      | 200 SessionControlResponse   |
| `/api/v1/sessions/fork`                                | POST | SessionForkRequest        | 200 SessionOpenResponse      |
| `/api/v1/sessions/heartbeat`                           | POST | SessionHeartbeatRequest   | 200 SessionHeartbeatResponse |
| `/api/v1/sessions/detach`                              | POST | SessionDetachRequest      | 200 SessionControlResponse   |
| `/api/v1/sessions/close`                               | POST | SessionCloseRequest       | 200 SessionControlResponse   |
| `/api/v1/sessions/checkpoint`                         | POST | SessionCheckpointRequest  | 200 SessionCheckpointResult  |
| `/api/v1/sessions/checkpoint/delete`                  | POST | SessionCheckpointDeleteRequest | 200 SessionCheckpointDeleteResult |
| `/api/v1/sessions/load`                               | POST | SessionLoadRequest        | 200 SessionOpenResponse      |
| `/api/v1/admin/tenants`                                | POST | TenantCreateRequest       | 201 TenantCreateResponse     |
| `/api/v1/admin/tenants/{tenant_id}`                    | PATCH| TenantPatchRequest        | 200 TenantPatchResponse      |
| `/api/v1/admin/tenants/{tenant_id}`                    | DELETE | —                        | 200 TenantDeleteResponse     |


### 功能一览


| 路由                                                     | 方法   | 用途                             |
| ------------------------------------------------------ | ---- | ------------------------------ |
| `/api/v1/health`                                       | GET  | 存活探测                           |
| `/api/v1/sessions`                                     | GET  | 自助查询：调用方可见的活跃会话列表 + 配额快照       |
| `/api/v1/sessions/open`                                | POST | 打开会话（携带 `runtime_id` 时为幂等重附着）  |
| `/api/v1/sessions/turns`                               | POST | 提交 turn → 回执携带服务端签发的 `turn_id` |
| `/api/v1/sessions/{runtime_id}/turns/{turn_id}/events` | GET  | 单个 turn 的 SSE 事件流              |
| `/api/v1/sessions/interactions`                        | POST | 应答 runtime 发起的交互               |
| `/api/v1/sessions/cancel`                              | POST | 取消活跃（或指定）turn                  |
| `/api/v1/sessions/fork`                                | POST | fork 会话（能力门控）                  |
| `/api/v1/sessions/heartbeat`                           | POST | 维持租约心跳                         |
| `/api/v1/sessions/detach`                              | POST | 释放租约、保留会话                      |
| `/api/v1/sessions/close`                               | POST | 关闭会话（销毁沙箱）                     |
| `/api/v1/sessions/checkpoint`                         | POST | 创建持久化 checkpoint（沙箱快照 + runtime 状态） |
| `/api/v1/sessions/checkpoint/delete`                  | POST | 用户主动删除 checkpoint 及其归档资源      |
| `/api/v1/sessions/load`                               | POST | 从 checkpoint 创建新会话                 |
| `/api/v1/admin/tenants`                                | POST | 新建租户（生成 token，仅明文返回一次）         |
| `/api/v1/admin/tenants/{tenant_id}`                    | PATCH | 修改租户配额（部分字段更新）                 |
| `/api/v1/admin/tenants/{tenant_id}`                    | DELETE | 删除租户（存在活跃会话则拒绝）               |




## 3. 会话控制面



### 会话列表 / 配额自助查询

`GET /api/v1/sessions`，无请求体。与其余路由同一条共享路由，按调用方身份自动过滤（[tenancy_design.md](./tenancy_design.md) §4）：tenant 身份只看到自己名下的活跃会话，admin 身份看到全部租户。

```json
{
  "sessions": [
    {
      "runtime_id": "runtime-…", "conversation_id": "demo", "sender_id": "me",
      "status": "idle", "runtime_kind": "mock",
      "created_at_ms": 0, "updated_at_ms": 0
    }
  ],
  "has_more": false,
  "quota": { "max_sessions": 10, "active_sessions": 1, "max_requests_per_minute": null }
}
```

- `sessions` 只含**活跃**会话（`opening | idle | running | paused`），从不包含 `failed`/`closed`；按 `updated_at_ms` 降序排列。
- v1 无真正分页：`sessions` 最多返回服务端固定上限（当前 100）条最近更新的记录；`has_more` 为 true 表示调用方真实活跃会话数超过了这个上限。
- `quota.active_sessions` 是调用方可见范围内的**真实计数**（来自 SQLite 查询），不受 `sessions` 截断影响。session 准入计数在 daemon 启动时也从同一 SQLite 聚合恢复；恢复失败时服务拒绝启动，避免重启绕过 `max_sessions`。
- `quota.max_sessions` / `max_requests_per_minute` 直接取自调用方自身的配额配置；admin 身份没有配额上限，两个字段为 `null`。
- 不返回历史/已关闭会话，也不暴露审计日志——这两者是 v1 明确排除的范围（[tenancy_design.md](./tenancy_design.md) §4）。

### open

```json
{
  "runtime_id": null,
  "conversation_id": "demo",
  "sender_id": "me",
  "workspace": { "kind": "local_path", "path": "/work/repo" },
  "deployment": { "profile": null, "resource_class": null, "options": null },
  "requested_capabilities": { "sandbox": ["exec"], "runtime": [] },
  "llm": { "provider": "openai", "model": "gpt-x", "api_key": "sk-..." },
  "ext": { "runtime_mock": { "backend_id": "local" } },
  "lease": { "client_id": "tui-1" }
}
```

- `runtime_id` 省略/为 null 时由服务端签发；携带已存在的 id 为**幂等重附着**（attach + 返回现有会话投影）。
- `workspace.kind`：`daemon_default`（默认）/ `local_path` / `git` / `shared`。当前部署只实现前两种，其余返回 400。
- `requested_capabilities` 从严校验：请求未知能力名是 400；请求了归一化结果/adapter 宣告之外的能力是 422。
- `llm` 为请求期一次性配置（可含明文 api_key），永不持久化、永不回显。

响应 `SessionOpenResponse`：

```json
{
  "runtime_id": "runtime-…", "conversation_id": "demo", "sender_id": "me",
  "status": "idle", "created_at_ms": 0, "updated_at_ms": 0,
  "runtime_kind": "mock",
  "workspace": { "workspace_id": "…", "root": "/work/repo", "access": "read_write", "revision": null, "metadata": null },
  "isolation": { "boundary": "host", "workspace_access": "read_write", "network": "none", "effective_capabilities": ["exec"], "metadata": null },
  "effective_capabilities": { "sandbox": ["exec"], "runtime": [] },
  "llm": null
}
```

- `status`：`opening | idle | running | paused | failed | closed`。
- `llm` 仅在 runtime 宣告 ModelOverride 能力时出现，且形态为 `ResolvedLlmDescriptor`——**类型上不存在 api_key 字段**。
- 响应侧能力集为容忍反序列化：客户端遇到未知能力名按缺席处理，不报错。



### close / detach / heartbeat / cancel

统一形态 `{ "runtime_id": "…", "lease": {…} }`；cancel 额外可带 `turn_id`（`null` 表示取消当前活跃 turn）。

- close / detach / cancel → `SessionControlResponse`：`{ "runtime_id", "status", "updated_at_ms" }`。
- heartbeat → `{ "runtime_id", "accepted": true, "lease_expires_at_ms": … }`。匿名 heartbeat 直接 401 `lease_required`。
- detach 释放租约但保留会话（runtime 保温）；close 终结会话。



### fork（能力门控：Fork + StateExport）

```json
{
  "parent_runtime_id": "runtime-…",
  "conversation_id": null, "sender_id": null,
  "workspace": null, "deployment": null,
  "requested_capabilities": {}, "lease": {}
}
```

省略的字段继承父会话。成功返回新会话的 `SessionOpenResponse`；runtime 不支持时 422 `unsupported_capability`。

### checkpoint / load / checkpoint delete

`POST /api/v1/sessions/checkpoint` 创建一个 checkpoint。完整 checkpoint 同时包含 provider 快照和 runtime opaque state，成功响应携带 `checkpoint_id`。checkpoint 本身不占用 active session 配额。

`POST /api/v1/sessions/load` 根据 `checkpoint_id` 创建一个新的 session；load 按 open/fork 规则计入租户 `max_sessions`，provider load 前同时检查 sandbox 配额，失败会回滚已预占的配额和已创建资源。

```json
{ "checkpoint_id": "checkpoint-…", "conversation_id": null, "sender_id": null, "llm": null }
```

`POST /api/v1/sessions/checkpoint/delete` 由用户主动决定清理时机：

```json
{ "checkpoint_id": "checkpoint-…", "lease": {} }
```

删除会清理 provider snapshot、runtime 归档目录以及 SQLite checkpoint 记录。tenant 只能删除自己的 checkpoint，admin 可删除任意 checkpoint；无权限与不存在统一返回 404。provider 或归档清理失败时保留 SQLite 记录，便于重试。当前不运行自动 GC，也不会因 session close 自动删除 checkpoint；重复删除已不存在的记录返回 404。

### 租户管理（管控面，admin-only）

三条路由，仅 admin 身份可达（§3.1 的角色闸；tenant 身份访问一律 403）。且仅当服务端启动时加载了真实 `tenants.toml`（[tenancy_design.md](./tenancy_design.md) §4）才存在——dev 模式（无 `tenants.toml`，隐式 admin 全开）下这三条路由整体不挂载，请求直接 404，而不是任何 handler 内部判断"鉴权是否已配置"。持久化直接改写 `tenants.toml`（原子写：临时文件 + rename）并复用既有 `SIGHUP` 重载逻辑同一份代码路径热更新内存中的 `TokenTable`，不是另一张 SQLite 表。

**`POST /api/v1/admin/tenants`** — 新建租户。

```json
{ "tenant_id": "acme", "principal": null, "max_sessions": 5, "max_requests_per_minute": null }
```

- `tenant_id` 必填、非空（去除首尾空白后仍为空则 400）；与已有 `[[tenant]]` 块的 `tenant_id` 冲突则 409。
- `principal`、`max_sessions`、`max_requests_per_minute` 均可省略；`principal` 缺省为 `"tenant"`，配额字段缺省为 `null`（不设上限）——与 `tenants.toml` 文件本身的字段缺省语义一致。
- token **由服务端生成**（`xgt_` 前缀 + 40 位随机字母数字，约 238 bit 熵），不接受客户端传入。

响应 201 `TenantCreateResponse`：

```json
{ "tenant_id": "acme", "token": "xgt_...", "principal": "tenant", "max_sessions": 5, "max_requests_per_minute": null }
```

- `token` **仅在这一次响应中以明文返回**——之后任何接口（含未来可能出现的租户查询接口）都不会再回显它；遗失即只能删除重建该租户，当前没有单独的 token 轮换接口。

**`PATCH /api/v1/admin/tenants/{tenant_id}`** — 修改租户配额，**部分字段更新语义**：请求体中缺席的字段保持原值不变；字段存在且为 `null` 表示清空为不设上限；字段存在且为数字表示设置该值。

```json
{ "max_sessions": 20 }
```

```json
{ "max_sessions": null }
```

目标 `tenant_id` 不存在返回 404 `tenant_not_found`。响应 200 `TenantPatchResponse`，形态同 `TenantCreateResponse` 但不含 `token` 字段。

**`DELETE /api/v1/admin/tenants/{tenant_id}`** — 删除租户。

- 该租户存在**活跃会话**（`opening | idle | running | paused`）时拒绝，返回 409 `conflict`——不会强制关闭会话；需先关闭所有会话再删除。
- 删除后若会导致 `tenants.toml` 中**零凭证**（既无 `[admin]` 也无其余 `[[tenant]]`），同样拒绝并返回 409——与 `SIGHUP` 重载路径"reload 产出空表永不应用"的不变式一致，这里是同一条不变式的主动前置检查。
- 目标 `tenant_id` 不存在返回 404 `tenant_not_found`。
- 成功返回 200 `TenantDeleteResponse`：`{ "tenant_id": "acme" }`。

## 4. 会话交互面



### 提交 turn

```json
{
  "runtime_id": "runtime-…",
  "text": "hello",
  "entry": { "entry_kind": "tui", "instance_id": null, "message_id": null, "reply_to_message_id": null },
  "llm": null,
  "reasoning_effort": null,
  "client_request_id": "req-7f3a",
  "ext": {},
  "lease": {}
}
```

- `llm` 需 ModelOverride 能力、`reasoning_effort` 需 ReasoningControl 能力，否则 422。
- 响应 202：`{ "runtime_id", "turn_id", "accepted_kind": "turn" }`。`turn_id` **由服务端签发**，该 turn 的每条 SSE 事件携带同一值——这是回执与事件流之间唯一的相关性合同。
- **单活跃 turn**：同一会话同一时刻最多一个活跃 turn。前一个 turn 未到终态时再次提交返回 409 `conflict`（消息中携带活跃 turn 的 id）；收到终态事件后即可重新提交。
- **幂等重试**：`client_request_id` 为客户端自选的幂等键（会话内唯一）。网络超时后带同一键重试，服务端**重放原回执**（同一 `turn_id`）而不会启动第二个 turn；重放响应不产生新的事件流。省略该字段即每次提交都是新 turn。服务端只比对键本身，同键换正文属客户端错误。每会话保留最近 64 个键，daemon 重启后窗口清空。



### 订阅事件流

`GET /api/v1/sessions/{runtime_id}/turns/{turn_id}/events`，SSE 格式，每条事件的 event 名等于 JSON 的 `kind`。**流是一次性领取的**：同一 turn 的流被取走后再次 GET 返回 404。

### 应答交互（能力门控：Interaction）

```json
{
  "runtime_id": "runtime-…", "turn_id": "turn-…",
  "interaction_id": "interaction-…",
  "answer": { "kind": "text", "value": "yes" },
  "ext": {}, "lease": {}
}
```

`answer.kind`：`text` / `selection`（value 为字符串数组）/ `confirm`（value 为布尔）/ `data`（任意 JSON）/ `cancelled`。响应 202 回执（`accepted_kind: "interaction"`）。

## 5. SSE 事件词汇

所有事件携带 `runtime_id` 与 `turn_id`（`extension` 的 turn_id 可为 null）。


| kind                    | 专有字段                                                                                            | 语义                                                       |
| ----------------------- | ----------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `output_delta`          | `stream_id`、`sequence`、`delta`                                                                  | 增量文本输出                                                   |
| `tool_activity`         | `activity_id`、`phase`（begin/end）、`name`、`status`（running/succeeded/failed/cancelled）、`summary?` | 归一化工具活动                                                  |
| `interaction_requested` | `interaction_id`、`interaction_kind`、`prompt`、`options[]`、`ext`                                  | runtime 请求用户交互；用 §4 的 interactions 路由应答                  |
| `turn_completed`        | `outcome`（complete/max_turns/budget_exhausted/cancelled）、`usage`                                | 软终态                                                      |
| `turn_failed`           | `error{code,message,retryable,details}`、`usage`                                                 | 硬终态。含服务端合成的 `event_stream_closed`（runtime 事件流未发终态即关闭时兜底） |
| `extension`             | `namespace`、`payload`                                                                           | runtime 专有事件的命名空间透传，客户端按 `runtime_kind` 决定是否解释           |


**终态保证**：每个被接受的 turn 的事件流必然以 `turn_completed` 或 `turn_failed` 收尾。

## 6. 错误模型

所有错误响应为单一形态：`SessionWireError`，以 `code` 打标：


| code                     | HTTP | 含义                                                                           |
| ------------------------ | ---- | ---------------------------------------------------------------------------- |
| `invalid_request`        | 400  | 请求形态/字段非法（含未知字段、未支持的 workspace 种类）                                           |
| `lease_required`         | 401  | 需要租约身份（如匿名 heartbeat）                                                        |
| `not_found`              | 404  | 会话或 turn 流不存在                                                                |
| `tenant_not_found`       | 404  | 管控面按 `tenant_id` 查找的租户不存在（与 `not_found` 分属不同标识符空间，见 §3 租户管理）                |
| `conflict`               | 409  | 会话状态冲突，或管控面删除租户被拒绝（存在活跃会话 / 会清空全部凭证）                                        |
| `lease_conflict`         | 409  | 他人活跃持有租约（附 holder_client_id / holder_pid / holder_hostname）                  |
| `unsupported_capability` | 422  | 能力门控拒绝（附 family: sandbox/runtime 与 capability 名，capability 保持字符串以便客户端解码未来能力） |
| `internal`               | 500  | 内部错误                                                                         |
| `unavailable`            | 503  | 暂不可用（含配额超限）                                                                  |
| `timeout`                | 504  | 操作超时                                                                         |




## 7. 演进承诺

- 新增字段一律 `#[serde(default)]`；请求侧未知字段拒绝、响应侧未知能力按缺席处理——客户端可以落后于服务端，反之需同步窗口。
- 协议 crate 的任何 JSON 形态变更都会触发其 schema/边界测试 diff，按 wire 变更评审。
- 操作面（exec / 文件读写 / checkpoint / checkout / pause / resume）的 DTO 已在 session-protocol 定义但**尚未路由**，接线后并入本文 §2。
