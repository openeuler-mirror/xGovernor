# 协议边界

本文描述当前实现中的协议和职责边界。

## 总体结构

```text
HTTP/SSE client
    |
session-protocol
    |
SessionApplication (xgovernor-core)
    |-- AgentRuntime
    |-- InstanceManager
    |     |-- ProviderLifecycle (provider-protocol)
    |     |-- OperationAttach -> OperationBackend (operation-protocol)
    |-- SessionEnvironmentNormalizer
```

同一个服务进程可以注册多个 runtime。session 创建时确定 `runtime_kind`，之后所有 runtime 操作都按持久化的类型路由。缺省值是 `pi`。

## 协议 crate

### `session-protocol`

这是 daemon 与 HTTP/SSE 客户端之间的 wire contract，包含 session open、turn、interaction、cancel、close、checkpoint、load、fork 的请求、响应、错误和事件。

它不依赖 provider、operation 或 runtime 实现。runtime 专有配置通过 `ext` 命名空间传递；核心字段保持 runtime 中立。

### `agent-runtime-protocol`

这是 host 与 agent runtime 之间的 runtime contract。`AgentRuntime` 负责：

- `start`、`attach`、`stop`、`check_alive`
- `submit_turn`、`answer_interaction`、`cancel`
- `export_state`、`load_state`
- runtime capability 声明和标准化 runtime event

`RuntimeExecutionContext` 只包含 host 注入的 `Arc<dyn operation_protocol::OperationBackend>`。它不包含 provider kind、`InstanceManager`、provider snapshot 或 checkpoint API。

worker 的 NDJSON 控制消息 (`WorkerRequest` / `WorkerResponse`) 也是 runtime protocol 的内部公共合同。worker 崩溃统一映射为 `RuntimeError::WorkerUnavailable`。

### `provider-protocol`

这是 `InstanceManager` 与基础设施 provider 之间的控制面合同：create、load、pause、checkpoint、delete、inspect 及 provider capability。它不认识 session、runtime 或具体 agent。

### `operation-protocol`

这是 provider 实例启动后提供给 runtime 的操作面 trait，包含 exec、filesystem、search、path、export 和 permission 能力。它不负责 provider 生命周期，也不负责 runtime 生命周期。

## Application 的职责

`SessionApplication` 是唯一的编排层：

1. normalizer 校验请求并生成 workspace/isolation facts；
2. `InstanceManager` 创建或恢复 provider，并取得 operation backend；
3. 将 backend 放入 `RuntimeExecutionContext`，调用 `AgentRuntime`；
4. 将 runtime event 投影为 session event；
5. checkpoint 时先 `runtime.export_state()`，再由 manager 创建 provider snapshot，最后原子保存两者；
6. load/fork 时由 manager 恢复或创建 provider，再用 runtime state 启动 runtime。

runtime 不执行 provider create/delete，不持有 `InstanceManager`，也不实现 checkpoint snapshot 删除。

## 状态与能力

session 持久化 `runtime_kind` 和版本化 opaque runtime state。state schema 的解释只属于对应 runtime；未知 runtime kind 或 schema 版本必须失败。

能力分为两组：provider sandbox capability 和 runtime capability。完整 checkpoint 只有 runtime 支持 state export 且 provider 支持 snapshot 时才暴露；Local provider 不支持 snapshot，E2B provider 可以支持。

## 依赖规则

- protocol crate 不依赖 core、manager、backend 或具体 runtime；
- provider protocol 不依赖 runtime 或 session；
- runtime protocol 只依赖 operation protocol 及必要的 wire 类型；
- core/application 负责所有跨协议类型转换；
- Pi、xiaoO 和未来 runtime 都直接实现 `AgentRuntime`。
