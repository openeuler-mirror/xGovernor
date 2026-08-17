# Runtime Adapter 实现指南

> 面向要把一个 agent runtime（xiaoO、pi、opencode……）接入 xGovernor 的实现者。
> 接缝的规范性约束见 [protocol_boundaries.md](./protocol_boundaries.md) §5；可直接照抄的参考实现是 `apps/runtime-local`（LocalMockRuntime）。

---

## 1. 心智模型

xGovernor 不实现 LLM 决策环路。一个 runtime 接入后，governor 负责会话生命周期、租约、沙箱、能力门控与 wire 投影；runtime 负责思考。两者之间只隔一个 trait：

```rust
#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    fn kind(&self) -> &str;                       // 写进 SessionOpenResponse.runtime_kind
    fn capabilities(&self) -> BTreeSet<SessionRuntimeCapability>;

    async fn start(&self, request: RuntimeStartRequest) -> Result<(), SessionDomainError>;
    async fn stop(&self, runtime_id: &str) -> Result<(), SessionDomainError>;
    async fn attach(&self, runtime_id: &str) -> Result<(), SessionDomainError>;
    async fn submit_turn(&self, input: RuntimeTurnInput)
        -> Result<RuntimeEventReceiver, SessionDomainError>;
    async fn answer_interaction(&self, input: RuntimeInteractionInput)
        -> Result<(), SessionDomainError>;
    async fn cancel(&self, runtime_id: &str, turn_id: Option<&str>)
        -> Result<(), SessionDomainError>;

    // 默认返回 UnsupportedCapability——只有宣告 StateExport 时才需要实现
    async fn export_state(&self, runtime_id: &str) -> Result<OpaqueRuntimeState, _>;
    async fn load_state(&self, runtime_id: &str, state: OpaqueRuntimeState) -> Result<(), _>;
}
```

接入新 runtime 不是协议变更：**一组能力宣告 + 一个 adapter crate**，governor 的编排代码与 wire 面一行不动。

## 2. 工程形态

**独立 crate**（如 `apps/runtime-xiaoo`），依赖 `xgovernor-core` 与该 runtime 自己的 SDK/客户端。**禁止**反向让 core 或任何协议 crate 依赖 runtime 类型——这是检疫规则，有边界词汇测试守着。

adapter 内部通向沙箱的管线不要手写，组合 `crates/manager`（独立 crate）现成件：

- `xgovernor_manager::InstanceManager`：`ProviderLifecycle::create` → `OperationAttach::attach` → 以 runtime_id 为键的注册表（start/stop/backend_for），外加并发控制、按 `owner_ref` 的配额（含全局上限）、创建路径重试与 pending-release 队列——完整能力清单见 [session_orchestration_skeleton.md](./session_orchestration_skeleton.md) §3。两个现有 adapter（`apps/runtime-local`、`apps/runtime-e2b`）都是薄封装：各自的 `with_ledger()` 构造一个 `InstanceManager` 并把 `start`/`stop`/`attach`/`submit_turn` 转发给它。
- 沙箱内操作（exec / 文件 / 端口）走 `operation-protocol` 的 `OperationBackend`。

runtime 进程本身应运行在 provider 提供的沙箱内，adapter 经操作面到达它（例如：exec 启动 runtime 的 headless 进程，经 stdio/端口对话）。

**已记录的例外（`apps/runtime-pi`，收窄版）**：`apps/runtime-pi` 现在**组合** `InstanceManager`（`local`/`e2b`，各一个，按 `ext.runtime_pi.backend_id` 选）——Pi 的工具执行（文件读写/编辑、exec、grep/glob）经一个本地 HTTP 桥（`apps/runtime-pi/src/bridge.rs`）转发到 `start_instance` 拿到的真实 `OperationBackend`，与 §2 上面描述的组合路径完全一致，不再是偏离。仍然偏离的只剩 RPC **控制通道**本身：`pi --mode rpc` 子进程由 adapter 用 `tokio::process` 直接在 daemon 本机 spawn，用 LF-分隔 JSON-per-line 在 stdin/stdout 上对话，不经 `operation-protocol`。原因是结构性的，不是省事：`OperationExec` 的合同是一次性、全缓冲的单个请求-响应，结构上无法驱动一个长生命周期、持续双向对话的交互式 stdio 进程。连带后果收窄为：`pi` 子进程本身（不含它附着的沙箱）没有 ledger、没有跨重启 reconcile——它的进程注册表只在 daemon 进程存活期间有效，与 `apps/server` 进程同生共死；它所附着的沙箱则由对应 `InstanceManager` 正常记账、reconcile、配额强制，与 runtime-local/runtime-e2b 无异。**跨重启的会话恢复由 §3（五）的"状态检疫坑位 + 惰性复原"模式补上（2026-08-17 落地，见 [pi_session_restore_plan.md](./pi_session_restore_plan.md)）**：进程注册表死了，但会话本身能重建。这不是新范式：接入下一个 runtime 时默认仍应遵循 §2 的 `InstanceManager` 组合路径（工具执行侧照抄 `apps/runtime-pi` 现在的桥接模式）；只有当目标 runtime 也需要一条长连接交互式控制通道时，才应重新评估是否需要类似的窄口子例外，而不是照抄整个旧偏离。

## 3. 六条合同义务

**（一）事件映射。** runtime 原生事件必须翻译为 `RuntimeEvent` 五原语：OutputDelta、ToolActivity（begin/end + 状态）、InteractionRequested、Completed（outcome + usage）、Failed（结构化错误 + usage）。翻译不了的专有事件包成 `Extension { namespace, payload }` 透传——**不得丢弃，也不得伪装成核心事件**。namespace 用你的 runtime kind（如 `xiaoo`）。

**（二）终态纪律。** 每个被接受的 turn，事件流以 Completed 或 Failed 收尾。做不到时 application 层会合成 `event_stream_closed` 的失败兜底，但那是事故路径，不是你的正常出口。cancel 被调用后也要走到终态（通常是 `Completed { outcome: Cancelled }`）。

**（六）`submit_turn` 立即返回，`cancel` 有真实语义**（2026-08 收紧，`crates/core/src/runtime_adapter.rs` 上 `submit_turn`/`cancel` 的 doc 注释是规范文本）。`submit_turn` 只做同步的准备工作（查 backend、开 channel、登记取消状态），真正驱动 turn 的工作必须放进自己 `tokio::spawn` 的后台任务，**在那个后台任务跑完之前就要把 receiver 返回给调用方**——不能在 `submit_turn` 里同步跑完整个 turn 再返回（有界 channel 下这样写甚至会死锁：调用方要等这次调用返回才能开始排空 channel，你却在等排空发生）。`cancel` 必须有真实中断语义：调用后一个正在跑的 turn 必须尽快走到终态（不能是"返回 `Ok(())` 但 turn 该怎么跑还怎么跑"的空操作）。参考实现 `apps/runtime-local`/`apps/runtime-e2b` 共享 `crates/core::TurnCancellationRegistry` 做取消簿记（`submit_turn` 调 `begin()` 拿 `CancelSignal` 再 spawn，后台任务里 `tokio::select!` 真实执行 future 和 `CancelSignal` 二选一，`cancel` 调 `fire()`），"跑什么"的业务逻辑仍各自实现、不共享——新 adapter 接入真实 runtime（如 xiaoO 的 headless 进程）时，无论是否复用这个 registry，都必须满足同样的"立即返回 + 真实取消"契约，这是能接入任何真实 agent runtime 之前的硬前提，不是 mock 专属的写法。

**（三）ext 命名空间。** runtime 专有输入（引导配置、技能目录、专有开关）从 `RuntimeStartRequest.ext` / `RuntimeTurnInput.ext` 里**只读自己的命名空间**。参考 runtime-local：它从 `ext.runtime_local` 读 `backend_id`，缺失即 `InvalidRequest`。**`owner_ref` 不再是 ext 概念**：它已升级为 `RuntimeStartRequest` 的一等字段，由装配层（`SessionApplication::open_impl`/`fork_impl`）通过 `SecurityContext::owner_ref()` 机械推导（tenant 得 `tenant/{tenant_id}`，admin 得固定哨兵值），adapter 直接读 `request.owner_ref`——两个现有 adapter（runtime-local/runtime-e2b）都已删掉 ext 里的 `owner_ref` 键，不留兼容路径。任何新 runtime adapter 都不应该、也不能再从 ext 读 owner_ref。

**（四）能力宣告即承诺。** `capabilities()` 返回什么，governor 就放行什么：宣告 Interaction 才会有交互应答路由到你；宣告 ModelOverride 才会收到 `llm` 覆写（并且 open 响应才会带 ResolvedLlmDescriptor）；宣告 StateExport 才需要实现 export/load_state，fork 与完整 checkpoint 也依赖它。**不宣告就不必实现，宣告了就必须实现**——宣告而未实现的能力会把 422 变成 500。

**（五）状态所有权。** runtime 内部状态出入 governor 只经 `OpaqueRuntimeState`（`runtime_kind` + `schema_version` + `Value`）。serde 形态归你私有、自带版本号；具体快照类型永不出现在 SessionRecord 的类型签名里。版本升级由你的 `load_state` 自行兼容。

**（七）governor 内部自用的 `export_state`（2026-08-17 起对下一个 runtime 的通用建议）**：`export_state` 不必只为 checkpoint 服务。`apps/runtime-pi` 示范了第二种、governor 内部自用的模式：`start()` 冷路径把「重新拉起所需的一切」（backend_id、per-session 状态目录、可执行文件/扩展覆盖、workspace 元数据）编进 opaque blob，`open_impl` 在 start 成功后自动调 `export_state` 写入 `SessionRecord.runtime`——**不宣告 `StateExport` 能力**（那是 checkpoint 语义的能力门控，与内部复原是两回事，`capabilities()` 不动）。重启后任意请求踩到「SQLite 有行、adapter 无实例」缝隙时，application 层经 `ensure_runtime_attached` 用 `runtime.start(RuntimeStartRequest{ state: Some(blob), .. })` 重放启动的后半段，adapter 在 `start` 内部按 `request.state` 分冷启动/复原两条路。配套约束：复原信息必须**足够自包含**（不依赖 adapter 进程内任何残留状态——进程注册表、内存句柄都已随重启消失）；读不认识的 `schema_version` 必须 fail-closed 报错而不是猜；沙箱已死/状态文件缺失时明确失败（`pi_sandbox_gone`/`pi_session_state_lost`），绝不静默开新会话假装复原。你的 runtime 只要"会话状态能落盘、能从落盘重放"，就能免费获得 daemon 重启后的惰性会话复原。

## 4. 错误映射

runtime/provider 侧错误映射到 `SessionDomainError`，映射决定客户端看到的 HTTP 语义：找不到 → NotFound(404)、载荷非法 → InvalidRequest(400)、资源/配额 → Unavailable(503)、能力缺席 → UnsupportedCapability(422)、其余 → Internal(500)。参考 runtime-local 的 `map_provider_error`。

## 5. 新 adapter 检查清单（以 xiaoO 为例）

1. runtime 侧先有 headless 附着面（serve/RPC 模式）——这是前置条件，不是 adapter 能绕过的。
2. 建 crate，组合 `xgovernor_manager::InstanceManager`，在 `start` 里创建沙箱并拉起 runtime 进程。
3. 写事件映射表：runtime 事件 → 五原语；列出哪些进 `ext.<kind>` 扩展事件。
4. 决定能力集：最小可用通常是空集（纯 turn/事件）；交互、模型覆写、状态导出按 runtime 实际能力逐项宣告。
5. 错误映射函数。
6. 测试三件套（照抄 runtime-local 的模式）：全链路集成测试（open → submit_turn → 断言真实输出流经事件 → 终态）；缺失 ext 命名空间被拒；能力未宣告时对应请求被 422 拒绝。
7. 在 `apps/server` 的装配入口把 adapter 换入/并入 `SessionApplication::new`。

## 6. 今天就能验证的最小路径

不等 runtime 侧就绪时，可先把 adapter 的骨架对着 `LocalMockRuntime` 的测试跑起来：它证明的正是"governor 这半边一切就绪"——你的全部工作量都在 trait 的另一侧。
