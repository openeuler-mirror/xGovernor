# manager (xgovernor-manager)

`InstanceManager`：面向 `provider-protocol` 契约、跨具体 provider 通用的实例编排层。介于 `crates/backend`(具体怎么建一个沙箱)和 `crates/core`(pi 会话业务逻辑)之间。

**做什么**：

- `start_instance`/`stop_instance`：创建/销毁一个 provider 实例，并把结果注册进内存registry + SQLite 账本(`backend::ProviderInstanceLedger`)。
- 配额：`max_sandboxes_per_owner`/`max_sandboxes_global` 两级限制，`admin` 哨兵豁免。
- 失败重试：create 路径有限次重试+补偿删除+配额回滚；delete 失败进入待重试队列，无限重试直到成功（避免资源泄漏）。
- `reconcile()`：daemon 重启后，用账本记录逐条尝试 `attach()` 重建内存 registry——`attach()` 本身的成功/失败就是唯一的存活判据，不再依赖 provider 自己的 `list_instances()` 做门槛判断(2026-08-17 的架构修复，因为 `list_instances()` 对 local provider 而言重启后天然为空，会导致所有活跃行被误判为孤儿)。
- `destroy_by_runtime_id`：绕开 attach，直接按账本行删 provider 实例，用于"重启后 close，还没来得及 reconcile"的场景。

**边界**：完全不知道"pi/turn/会话"这些业务概念，也不关心某个具体 provider 内部怎么实现——它只面向 `provider_protocol::ProviderLifecycle` + 本 crate 从 `backend` 引入的 `OperationAttach` 这两个 trait 编程，可以对着任意 provider(local/e2b/未来新增的)工作而不用改代码。
