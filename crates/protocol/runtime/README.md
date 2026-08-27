# agent-runtime-protocol

`agent-runtime-protocol` 定义 xGovernor host 与 agent runtime/worker 之间的 runtime contract。

`AgentRuntime` 负责 runtime worker 的启动、附着、停止、健康检查、turn、interaction、cancel 和版本化 state export/load。runtime 通过 `RuntimeExecutionContext` 接收 host 注入的 `OperationBackend`，不接触 provider 生命周期、`InstanceManager` 或 provider snapshot。

Pi、xiaoO 以及未来 runtime 都直接实现这个 trait。worker 使用 `WorkerRequest` / `WorkerResponse` 的 NDJSON 编码；worker 崩溃映射为 `WorkerUnavailable`，已接受 turn 必须恰好产生一个 terminal event。

本 crate 不依赖 xGovernor core、manager、backend 或具体 runtime。provider 创建、恢复、checkpoint、删除由 host 的 `InstanceManager` 负责。
