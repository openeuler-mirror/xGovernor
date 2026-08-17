# provider-protocol

Governor 与"基础设施 provider"（沙箱/虚拟机/容器等资源的实际供应方，比如本地进程沙箱、E2B 远程沙箱）之间的**控制面契约**：`create`/`load`/`pause`/`delete`/`inspect` 生命周期操作，以及 `ProviderLifecycleStateMachine` 定义的合法状态迁移。

**边界**：只管"资源怎么生、怎么灭、状态是什么"，刻意不管"资源活着之后怎么被操作"——`ProviderOperationCapabilities` 只是一组能力标志位(能不能 exec、能不能读写文件…)，具体怎么操作是 `operation-protocol` 的事，也不是这个 crate 关心的。Provider 相关的业务方(owner)信息和 provider 自定义配置(`provider_options`)都作为不透明值跨界传递，本 crate 不解释其内容。

**谁依赖它**：`crates/backend`(每个具体 provider——local/e2b——实现这里的 trait)、`crates/manager`(`InstanceManager` 面向这个契约做生命周期编排/配额/重试，不关心具体 provider 是谁)。
