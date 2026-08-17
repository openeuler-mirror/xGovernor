# core (xgovernor-core)

应用/领域层，坐在 `session-protocol`(线协议) 和具体 runtime 实现(`apps/runtime-*`)之间——协议里的类型和运行时里的类型都不直接跨过这一层，全部在这里转换成领域概念(`SessionRecord`/`SessionStatus` 等)再对外暴露。

**核心模块**：

- `application.rs` — `SessionApplication`：open/submit_turn/answer_interaction/cancel/close/fork 等业务动作的主入口，编排 lease 校验、runtime 桥接、状态落库。
- `domain.rs` — 领域类型本身(`SessionRecord`、`SessionStatus`、`WorkspaceFacts` 等)，不依赖协议或具体 runtime。
- `runtime_adapter.rs` — `RuntimeAdapter` trait：本层与具体 runtime(pi/local/e2b)之间的边界，runtime 只需要实现这一个 trait 就能接入。
- `security.rs` — `SecurityContext`：贯穿整个应用层的身份事实，只能由传输层(bearer token 校验)产生，绝不从客户端请求体反序列化。
- `session_lease.rs` — "同一会话同一时刻只有一个写者"的租约表，内存态，daemon 重启即清空。
- `orphan_reaper.rs` — 租约过期但没人显式 close 的会话，后台回收。
- `sqlite_repository.rs` — `SessionRepository` 的 SQLite 落地实现，只管开给定路径的库文件，不决定路径本身。
- `memory_automation.rs` — 可选的长期记忆自动化（写入/召回），召回内容作为不可信系统上下文渲染，失败由调用方兜底。
- `turn_cancellation.rs` / `projection.rs` / `prompt_utils.rs` — turn 取消令牌管理、领域对象→线协议的投影、prompt 拼装的小工具集合。

**边界**：不知道"沙箱怎么建/销毁"这类基础设施细节(那是 `crates/manager`+`crates/backend` 的事)，只通过 `RuntimeAdapter` 这一个窄接口跟 runtime 打交道；也不做 HTTP 序列化(那是 `apps/server` 的事，它依赖这里的类型)。
