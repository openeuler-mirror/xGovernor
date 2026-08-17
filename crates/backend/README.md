# backend

`provider-protocol`(控制面) + `operation-protocol`(操作面) 两份契约的**具体实现**。每个子模块是一个真实的资源供应方：

- **`local/`** — 本地进程沙箱：workspace 目录 + bubblewrap(Linux)/seatbelt(macOS) 隔离策略。没有独立的常驻进程，"实例"本质就是一份目录+策略配置。
- **`e2b/`** — E2B 远程沙箱：通过 E2B 平台 API 创建/查询/删除远程沙箱，操作面经 `envd`(沙箱内 HTTP 服务)转发。
- **`ledger` / `sqlite_ledger.rs`** — provider 实例的持久化账本(SQLite)，记录哪些实例处于活跃状态，供进程重启后的 reconcile 使用。
- **`process_group.rs`** — 本地子进程的进程组管理(批量终止、注册/反注册)，`local` 模块的支撑设施。

每个 provider 模块都要同时实现 `provider_protocol::ProviderLifecycle`(控制面)和本 crate 定义的 `OperationAttach` trait(把一个已创建的 `ProviderInstance` 桥接到 `operation_protocol::OperationBackend`)——`OperationAttach` 之所以定义在这里而不是某个协议 crate 里，是因为 provider-protocol 有意把这一步排除在控制面契约之外，由具体实现自行承担。

**边界**：只做"资源怎么落地"，不做编排/配额/重试——那是 `crates/manager` 的职责；也不做业务语义(pi 会话、turn 生命周期等)——那是 `crates/core` 的职责。目前只有 `local`/`e2b` 两个 provider，其余(如 conch)和多会话共享/checkpoint 编排暂缓。
