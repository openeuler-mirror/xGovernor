# backend

`provider-protocol`(控制面) + `operation-protocol`(操作面) 两份契约的**具体实现**。每个子模块是一个真实的资源供应方：

- **`local/`** — 本地进程沙箱：workspace 目录 + bubblewrap(Linux)/seatbelt(macOS) 隔离策略。没有独立的常驻进程，"实例"本质就是一份目录+策略配置。
- **`docker/`** — 实验性 Linux Docker 工具后端；用受控镜像实现无宿主挂载的容器执行。见 [原型说明](../../scripts/docker-prototype/README.md)。
- **`e2b/`** — E2B 远程沙箱：通过 E2B 平台 API 创建/查询/删除远程沙箱，操作面经 `envd`(沙箱内 HTTP 服务)转发。
- **`ledger` / `sqlite_ledger.rs`** — provider 实例的持久化账本(SQLite)，记录哪些实例处于活跃状态，供进程重启后的 reconcile 使用。
- **`process_group.rs`** — 本地子进程的进程组管理(批量终止、注册/反注册)，`local` 模块的支撑设施。

每个 provider 模块都要同时实现 `provider_protocol::ProviderLifecycle`(控制面)和本 crate 定义的 `OperationAttach` trait(把一个已创建的 `ProviderInstance` 桥接到 `operation_protocol::OperationBackend`)——`OperationAttach` 之所以定义在这里而不是某个协议 crate 里，是因为 provider-protocol 有意把这一步排除在控制面契约之外，由具体实现自行承担。

**边界**：只做"资源怎么落地"，不做编排/配额/重试——那是 `crates/manager` 的职责；也不做业务语义(pi 会话、turn 生命周期等)——那是 `crates/core` 的职责。目前包含 `local`/`e2b` 和实验性 `docker` provider，其余(如 conch)和多会话共享/checkpoint 编排暂缓。

## Docker 后端（实验性）

open 可使用 `ext.runtime_pi.backend_id: "docker"`，workspace 支持 `daemon_default` 和 HTTPS `git`，根目录固定 `/workspace`。`local_path` 和租户 Docker 请求返回 400；不能用 `deployment.options.provider_options` 指定镜像、挂载、特权或 daemon 配置。管理员可通过 `provider_options.network_mode` 选择 `none`、`default` 或 `bridge`，省略时默认为 `default`；`none` 会关闭容器网络并在 PI 隔离投影中标记为 `isolated`。

Docker 会话的工具隔离投影为 `container`，`controller_boundary` 为 `host`；`network_mode: "none"` 时网络隔离为 `isolated` 且不声明 Network capability，`default`/`bridge` 时网络隔离为 `none` 并声明 Network capability。沿用 exec/files/turns/close 接口，不声明 Snapshot；checkpoint/fork 不支持。镜像和资源由服务端控制，原型最多两个容器，每个 1 CPU、1 GiB 内存、128 PID。close 删除容器及工作区，正常服务重启重新附着原容器。完整 turn 取消传播暂未实现。
