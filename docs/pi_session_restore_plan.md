# Pi Session 恢复契约

Pi session 由 provider state 与 runtime state 两部分组成：

```text
Session = provider instance + Pi runtime state
```

provider 生命周期由 `SessionApplication` 和 `InstanceManager` 管理；Pi 只实现 `AgentRuntime`。

## 持久化状态

Pi 通过 `RuntimeStateSnapshot` 保存版本化 opaque state，其中包含重新拉起 worker/Pi 所需的 runtime 私有信息，例如 session 目录、可执行文件配置和会话文件位置。core 不解析这些字段，只校验和路由 `runtime_kind`。

每次 open 成功后，Application 调用 `export_state` 并保存到 `SessionRecord.runtime`。Pi session JSONL 存在 per-runtime 目录中；恢复前必须校验最新文件完整且最后一条消息是非 pending 的 assistant 终态。

## 惰性恢复

daemon 重启后，provider manager 先通过 ledger reconcile 恢复 provider 实例与 operation backend。第一次 attach/turn/interaction/cancel 到达时，Application 执行：

1. 按 session 中的 `runtime_kind` 选择 Pi runtime；
2. 从对应 `InstanceManager` 取得原 runtime id 的 operation backend；
3. 调用 `AgentRuntime::attach`；
4. worker 不存在时，以 `RuntimeStartRequest.state = Some(saved_state)` 调用 `start`；
5. Pi 校验 session JSONL，启动 `pi-worker` 并让 worker 恢复原 Pi session；
6. attach 成功后继续原请求；遗留的 `Running` 状态恢复为 `Idle`。

provider instance 或 runtime state 不存在时不得创建空白替代 session。恢复失败由 Application 写入 `last_error`，session 状态置为 `Failed`。

## Close

close 顺序为：

1. `AgentRuntime::stop`，worker 不存在按幂等成功处理；
2. `InstanceManager::destroy_by_runtime_id` 清理 provider instance；
3. session 持久化为 `Closed` 并释放 lease、turn gate 和租户配额。

Pi session JSONL 是 runtime 私有持久化数据，不属于 provider lifecycle；当前 close 不要求 provider manager 删除该目录。

## 不变量

- runtime 不持有 `InstanceManager`；
- provider backend 只通过 `RuntimeExecutionContext` 注入；
- provider cleanup 和 checkpoint 不属于 runtime protocol；
- 未知 state schema version 必须 fail-closed；
- worker 崩溃映射为 `WorkerUnavailable`；
- 一个 accepted turn 恰好产生一个 terminal event。
