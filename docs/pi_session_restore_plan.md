# Pi Session 恢复契约

Pi session 由 provider state 与 runtime state 两部分组成：

```text
Session = provider instance + Pi runtime state
```

provider 生命周期由 `SessionApplication` 和 `InstanceManager` 管理；Pi 只实现 `AgentRuntime`。

## 持久化状态

Pi 通过 `RuntimeStateSnapshot` 保存版本化 opaque state，其中包含重新拉起 worker/Pi 所需的 runtime 私有信息，例如 session 目录、可执行文件配置和会话文件位置。core 不解析这些字段，只校验和路由 `runtime_kind`。

每次 open 成功后，Application 调用 `export_state` 并保存到 `SessionRecord.runtime`。Pi session JSONL 存在 per-runtime 目录中；恢复前校验最新文件，最后一条 `message` 记录必须是非 pending 的 assistant 终态。回合结束后 pi 追加的簿记记录（`compaction` / `model_change` / `thinking_level_change` 等）不参与该判定，向后扫描跳过。

SIGKILL 可能把正在写入的 JSONL 截成半行 JSON，或留在该 turn 的 user / toolResult / pending assistant 记录上。恢复路径把这三种“崩溃尾”视为可截断：先回收该 session 遗留的 worker 进程组，再把文件 truncate 到最后一条完整 assistant turn，然后重新校验。没有完整 turn 时删除不完整文件并以空历史继续；目录缺失、I/O 错误等非尾部损坏仍 fail closed。被丢弃的只是未完成的 in-flight turn，不会把它交给新进程续跑。

## 惰性恢复

daemon 重启后，provider manager 先通过 ledger reconcile 恢复 provider 实例与 operation backend。第一次 attach/turn/interaction/cancel 到达时，Application 执行：

1. 按 session 中的 `runtime_kind` 选择 Pi runtime；
2. 从对应 `InstanceManager` 取得原 runtime id 的 operation backend；
3. 调用 `AgentRuntime::attach`；
4. worker 不存在时，以 `RuntimeStartRequest.state = Some(saved_state)` 调用 `start`；
5. Pi 先按 session 目录中的 `worker.pgid` 标记回收遗留 worker 进程组（校验组内命令确属该 session 后才 kill），再校验/截断 session JSONL，启动 `pi-worker` 并让 worker 恢复原 Pi session。若运行环境禁止进程表检查（`ps`/procfs 被 sandbox 拒绝），reaper 会记录 warning 并继续恢复——无法安全判定归属时不会盲杀进程组；此时可能需要环境层面放开检查或人工清理。
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
