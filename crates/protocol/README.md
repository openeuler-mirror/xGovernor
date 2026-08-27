# crates/protocol

三份纯契约(wire/trait)crate 的集合目录，本身不是一个 crate（没有自己的 `Cargo.toml`/代码），只是把"边界定义"归到一起，方便区分于会实现这些契约的 `backend`/`manager`/`core`。

三份契约按"谁跟谁对话"分层，互不依赖：

- **`session/`**(`session-protocol`) — governor daemon 与外部客户端(HTTP/SSE)之间的**线协议**。
- **`provider/`**(`provider-protocol`) — governor 与"基础设施 provider"(local/e2b/未来的其它沙箱)之间的**控制面契约**(create/load/pause/delete/inspect)。
- **`operation/`**(`operation-protocol`) — 一个已 attach 的 provider 实例，对外暴露的**操作面契约**(exec/文件读写/搜索/导出等)。
- **`runtime/`**(`agent-runtime-protocol`) — host 与 agent runtime/worker 之间的 runtime 契约(start/attach/turn/interaction/cancel/state，以及 worker NDJSON)。

四者都刻意不解释对方的内部细节：session 不关心 provider/runtime 怎么实现，provider 不关心 operation/runtime 具体怎么跑，runtime 只接收 host 注入的 operation backend。跨层编排统一由 `crates/core` 的 `SessionApplication` 完成。
