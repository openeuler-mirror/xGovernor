# Agent Runtime 接入指南

runtime 直接实现 `agent_runtime_protocol::AgentRuntime`。

## 必须实现的边界

```rust
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    fn runtime_kind(&self) -> &str;
    fn capabilities(&self) -> BTreeSet<RuntimeCapability>;
    async fn start(&self, request: RuntimeStartRequest,
                   context: RuntimeExecutionContext) -> Result<(), RuntimeError>;
    async fn attach(&self, runtime_id: &str,
                    context: RuntimeExecutionContext) -> Result<(), RuntimeError>;
    async fn stop(&self, runtime_id: &str) -> Result<(), RuntimeError>;
    async fn check_alive(&self, runtime_id: &str) -> Result<bool, RuntimeError>;
    async fn submit_turn(&self, request: RuntimeTurnRequest)
        -> Result<RuntimeEventReceiver, RuntimeError>;
    async fn answer_interaction(&self, request: RuntimeInteractionRequest)
        -> Result<(), RuntimeError>;
    async fn cancel(&self, request: RuntimeCancelRequest) -> Result<(), RuntimeError>;
    async fn export_state(&self, runtime_id: &str) -> Result<RuntimeStateSnapshot, RuntimeError>;
    async fn load_state(&self, runtime_id: &str, state: RuntimeStateSnapshot)
        -> Result<(), RuntimeError>;
}
```

`RuntimeExecutionContext` 只有 `operation_backend`。provider 的创建、恢复、停止、checkpoint 和删除由 `SessionApplication` 通过 `InstanceManager` 完成。runtime 可以保存自身 `ext` 中的 backend 标识，但不持有 manager，也不自行创建、恢复或删除 provider。

## Worker

Pi 和 xiaoO 都采用每个 runtime 实例一个 worker 的形态。worker 使用 `WorkerRequest` / `WorkerResponse` NDJSON；runtime supervisor 负责 worker 进程、turn、interaction、取消和事件转发。worker 退出或协议损坏必须产生 `WorkerUnavailable`，已接受的 turn 必须恰好产生一个 terminal event。

`submit_turn` 必须快速返回 receiver，实际 turn 在后台 task/worker 中执行。`cancel` 必须能影响正在执行的 turn，而不是只返回成功。

## Provider 与操作 backend

Application open/restore 流程先通过 `InstanceManager` 获取 backend，再调用 runtime：

```text
normalizer -> InstanceManager::start/attach -> OperationBackend
           -> RuntimeExecutionContext -> AgentRuntime::start
```

runtime 只能通过 `OperationBackend` 执行文件、命令和搜索操作，不能绕过 provider 隔离边界。

## 状态、checkpoint 和 fork

runtime state 是带 `runtime_kind` 与 `schema_version` 的 opaque snapshot。runtime 只实现 export/load；checkpoint snapshot 的创建、恢复、删除属于 provider manager，Application 负责协调和原子持久化。fork 同样由 Application 组合 provider snapshot 与 runtime state，生成隔离的子 session。

## 新 runtime 清单

1. 实现 `AgentRuntime` 和 worker RPC；
2. 定义自己的 `runtime_kind`、`ext` 命名空间和版本化 state；
3. 只从 `RuntimeExecutionContext` 获取 operation backend；
4. 实现 capability、事件、interaction、cancel 和 worker crash 映射；
5. 在 server 注册 runtime，并为其配置 normalizer 与 provider manager；
6. 添加 protocol round-trip、worker NDJSON、state schema、terminal event exactly-once 测试。
