# operation-protocol

一个 provider 实例被 `InstanceManager` attach 之后，注入 agent runtime 的**操作面契约**：`OperationBackend` trait，涵盖 exec、文件读写、搜索、导出、路径类型、权限授予/拒绝、diff 等具体能力。

**边界**：这是 `provider-protocol` 特意留白的部分——那边只给一个能力标志位，"实例活着之后到底怎么操作"由这个 crate 说清楚。它不管实例是怎么创建/销毁的(那是 `provider-protocol` 的事)，也不管协议怎么在网络上传输(那是 `session-protocol` 的事)。

**谁依赖它**：`crates/backend`(local/e2b 的具体 backend 实现这个 trait)、`apps/runtime-*`(运行时适配器通过它驱动 exec/文件操作，不关心背后是本地进程还是远程沙箱)。
