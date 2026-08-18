# session-protocol

Governor daemon 与外部客户端之间的 HTTP/SSE **线协议**（wire contract）。定义会话的 open/submit_turn/answer_interaction/cancel/close/fork 等请求/响应/事件类型。

**边界**：只描述"协议长什么样"，不含任何运行时逻辑——不知道 pi/local/e2b 是什么，也不知道请求最终怎么被处理。运行时特有的输入/事件放进 `ext`(`SessionExtensions = BTreeMap<String, Value>`) 这个不透明字典里，对本 crate 而言永远是 opaque JSON，不解析、不校验内容。

**谁依赖它**：`apps/server`(HTTP 层序列化/反序列化)、`crates/core`(领域层用它的类型作为输入/输出契约)。它不依赖 `provider-protocol`/`operation-protocol`——三层协议互相不知道对方存在。
