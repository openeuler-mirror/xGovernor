# MCTS JSON-RPC v1

本文是 宿主 JSON-RPC 管控面托管 MCTS 插件的协议草案，包含三个契约：插件生命周期、宿主执行能力、搜索结果回传。用户在现有 Agent 会话输入 `/mcts`，由 宿主 JSON-RPC 解析、授权并启动插件；插件中的 MCTS Core 接管搜索，通过宿主能力接口执行分支，最后把结果交回 宿主 JSON-RPC，由主 Agent 展示和继续处理。

本文沿用调整后的 `plugin.*` 生命周期、短名称能力和 `content` 上下文字段。[JSON Schema](mcts-rpc-v1.schema.json) 已同步本文的请求、事件与结果结构。当前 Python 编解码层仍是此前的 `mcts.*` / `invocation_id` 草案，尚未同步，不能用于验证本文的业务消息。

## 1. 协议边界与消息约定

| 协议 | 方向 | 方法 | 职责 |
| --- | --- | --- | --- |
| A：插件生命周期 | 宿主 JSON-RPC → Plugin | `plugin.initialize/start/wait/cancel/shutdown` | 协商、启动、等待、取消和退出 |
| B：宿主执行能力 | Plugin → 宿主 JSON-RPC | `host.<能力名>` | 分支与 Agent 的实际执行 |
| B：宿主任务事件 | 宿主 JSON-RPC → Plugin | `host.job.*` | 执行进度与终态 |
| C：搜索信息回传 | Plugin → 宿主 JSON-RPC | `plugin.started/progress/completed/cancelled/failed` | 搜索摘要与结构化结果 |
| C：Agent 交付 | 宿主 JSON-RPC → 主 Agent / UI | 宿主映射为结果消息 / session event | 展示、继续对话及结果应用 |

A、B、C 的插件边界使用双向 JSON-RPC。主 Agent 和 UI 的接入由 宿主 JSON-RPC 适配，插件无需调用 Agent 的独立网络端点。

采用 [JSON-RPC 2.0](https://www.jsonrpc.org/specification) 的消息封装，并约定 v1 只发送单条消息、具名 object 参数和 object 成功结果；这是本协议的使用约束。请求带 `id`，响应使用相同的 `id`，通知不带 `id` 且不返回响应。请求不使用 `null` ID；无法识别请求 ID 的解析或无效请求错误，响应使用 `id: null`。

本文除显式展示 JSON-RPC 封装外，JSON 示例均为 `params` 或 `result` 的内容。

### 标识与作用域

| 字段 | 含义 | 示例 |
| --- | --- | --- |
| `plugin_id` | 安装的插件标识，跨运行保持不变 | `mcts` |
| `run_id` | 宿主 JSON-RPC 为一次 `/mcts` 调用生成的唯一标识 | `mcts-run-42` |
| `session_id` / `turn_id` | 触发搜索的主会话及输入轮次 | `session-abc` / `turn-17` |
| `state_id` / `root_state_id` | 不可变状态快照及本次搜索根状态 | `state-001` |
| `branch_id` | 可执行、可修改的分支 | `branch-7` |
| `job_id` | 一次异步宿主操作 | `job-9` |

`plugin_id` 与 `run_id` 分开，使同一个插件能并行服务多次调用；取消一轮搜索不会取消该插件的其他运行。所有资源 ID 均为 opaque string，宿主校验其所属会话、运行和授权范围，知道 ID 不代表有权访问资源。

## 2. 协议 A：插件规范与生命周期

### 插件 manifest

安装时由 宿主 JSON-RPC 注册。`methods` 列出插件接收的请求，`events` 列出插件发出的通知，两者分别声明。

```json
{
  "plugin_id": "mcts",
  "version": "1.0.0",
  "api_version": "mcts.rpc.v1",
  "triggers": ["/mcts"],
  "methods": [
    "plugin.initialize",
    "plugin.start",
    "plugin.wait",
    "plugin.cancel",
    "plugin.shutdown"
  ],
  "required_host_capabilities": ["snapshot", "fork", "agent_turn"],
  "optional_host_capabilities": [
    "inspect", "exec", "file_read", "file_write", "diff", "release"
  ],
  "events": [
    "plugin.started", "plugin.progress", "plugin.completed",
    "plugin.cancelled", "plugin.failed"
  ]
}
```

`mcts.rpc.v1` 暂作为本插件协议版本标识。`plugin.*` 使用通用命名，为后续复用保留空间，不代表 宿主 JSON-RPC 已有该插件标准的实现。

### `plugin.initialize`

每个插件连接在业务调用之前完成一次协商。

- 请求：`plugin_id`、`supported_api_versions: string[]`、`host_version`、`available_capabilities: string[]`。
- 响应：`api_version`、`plugin_version`、`required_host_capabilities`、`optional_host_capabilities`。
- 无共同版本或缺少必需能力时拒绝启动。可用能力是宿主支持的功能；单次运行实际授权仍以 `plugin.start.granted_capabilities` 为准。

### `plugin.start`

宿主 JSON-RPC 在当前 Agent turn 的安全边界捕获上下文和根快照，再发送：

```json
{
  "jsonrpc": "2.0",
  "id": "xg-1",
  "method": "plugin.start",
  "params": {
    "plugin_id": "mcts",
    "run_id": "mcts-run-42",
    "session_id": "session-abc",
    "turn_id": "turn-17",
    "root_state_id": "state-001",
    "goal": "解决当前错误并给出可应用的修改",
    "content": "当前会话上下文：用户要求修复 parser；已复现空输入导致的异常；修改范围限于 parser 模块。",
    "budget": {
      "max_iters": 5,
      "branching": 3,
      "max_time_ms": 300000,
      "max_tokens": 200000
    },
    "granted_capabilities": [
      "snapshot", "fork", "agent_turn", "inspect", "exec", "diff", "release"
    ]
  }
}
```

`content` 在 v1 是 UTF-8 文本，包含宿主授权提供的会话上下文、约束和已有观察；不包含模型隐藏推理或服务端凭据。宿主若因长度限制提供摘要，必须在内容中说明省略范围。可恢复的执行状态由 `root_state_id` 承载，`content` 不代替快照。

三个预算字段均为正整数；节点预算由 Core 调度控制，时间和模型用量由 宿主 JSON-RPC 同时约束。Token 预算是停止新任务的阈值，在途执行可能产生尾部用量。

`plugin.start` 立即返回 `{ "run_id": "mcts-run-42", "status": "accepted" }`，不阻塞到搜索完成。相同 `run_id` 和相同参数的重试返回已有受理结果；相同 ID、不同参数返回冲突。

### `plugin.wait`

查询或有界等待一次运行的状态变化，不暂停 MCTS 搜索。

```json
{
  "plugin_id": "mcts",
  "run_id": "mcts-run-42",
  "after_sequence": 3,
  "timeout_ms": 30000
}
```

响应包含 `run_id`、`status`、`sequence`、`summary`；完成时还包含本节后文定义的完整 `result`，失败时包含 `failure`。`timeout_ms` 允许 0～30000，0 表示立即查询。状态序号大于 `after_sequence` 或已到终态时立即返回，否则等待至上限并返回当前状态。等待超时不取消搜索。

通知用于及时展示；`plugin.wait` 用于补取最新状态与终态结果，不重放全部日志。终态缓存保留到该连接的 `plugin.shutdown`；v1 不承诺插件进程崩溃后的断点恢复。

### `plugin.cancel` 与 `plugin.shutdown`

`plugin.cancel` 参数为 `plugin_id`、`run_id`、`reason`。宿主先关闭该运行的新任务受理入口，并取消其在途 job；插件停止调度，返回 `cancelling`，清理结束后发布 `plugin.cancelled`。取消是幂等请求，已经终止的运行返回原终态。取消受理不表示远端操作已停止；宿主必须等待执行停止后再回收资源。

`plugin.shutdown` 参数为 `plugin_id`、`grace_period_ms`。它关闭该连接的插件实例，停止所有剩余运行并释放插件自身资源。宿主 JSON-RPC 负责宿主资源清理，超出宽限时间可以终止插件进程。

运行状态：`accepted → running → completed | failed`，取消路径为 `accepted/running → cancelling → cancelled`。每轮只有一个终态；完成与取消竞争时由首次已记录的终态决定，后续消息不得覆盖它。

## 3. 协议 B：Core 请求宿主执行能力

Core 使用插件侧的能力客户端；客户端将操作转为下表中的 RPC。能力名称保持简短，线上方法统一加 `host.` 前缀。

| 能力 | RPC 方法 | 操作参数（不含公共字段） | 成功结果 |
| --- | --- | --- | --- |
| `snapshot` | `host.snapshot` | `branch_id` | `state_id`、`expires_at` |
| `fork` | `host.fork` | `parent_state_id` | `branch_id` |
| `agent_turn` | `host.agent_turn` | `branch_id`、`input`、可选 `profile` | `job_id`、`status: accepted` |
| `inspect` | `host.inspect` | `branch_id`、`view: summary/output` | `content` |
| `exec` | `host.exec` | `branch_id`、`argv: string[]`、`cwd` | `job_id`、`status: accepted` |
| `file_read` | `host.file_read` | `branch_id`、`path` | `content_base64` |
| `file_write` | `host.file_write` | `branch_id`、`path`、`content_base64` | `written_bytes` |
| `diff` | `host.diff` | `branch_id`、`base_state_id` | `content` |
| `release` | `host.release` | `branch_id` | `released: true` |

所有请求携带 `plugin_id`、`run_id`、`timeout_ms`；创建或修改资源的 `snapshot/fork/agent_turn/exec/file_write/release` 还必须携带 `idempotency_key`。去重范围是 `(run_id, method, idempotency_key)`，保留至本次运行回收；同键同参数返回原结果，同键不同参数返回冲突。RPC 请求 ID 只用于匹配响应，不替代幂等键。

宿主必须逐次校验能力授权和资源归属。插件可以申请更小的执行限制，不能自行扩大授权。模型调用和命令执行的用量均由 宿主 JSON-RPC 记录。

### 分支和快照

根快照由 宿主 JSON-RPC 在启动前创建；`host.snapshot` 用于保存搜索分支。快照必须明确保证 Agent 可恢复状态与任务所需环境状态的一致性，不要求复制模型内部缓存。无法提供所需隔离能力时拒绝启动，不静默退化为共享工作目录。

```json
{
  "plugin_id": "mcts",
  "run_id": "mcts-run-42",
  "parent_state_id": "state-001",
  "idempotency_key": "fork-node-7",
  "timeout_ms": 30000
}
```

`fork` 返回新分支。同一 branch 同时只允许一个修改或快照操作；冲突返回 `branch busy`，不同 branch 可并行。`inspect` 是只读观察，执行测试必须走 `exec`。

`release` 只释放已停止的 live branch，保留已创建的快照；重复释放返回成功。插件不需要持有删除根会话或修改主会话的权限。宿主在运行结束时回收未选中资源，选中快照保留至应用、用户放弃或约定过期时间，不能在发出结果后立即销毁。

### Agent turn 与异步 job

`host.agent_turn` 请求示例：

```json
{
  "plugin_id": "mcts",
  "run_id": "mcts-run-42",
  "branch_id": "branch-7",
  "input": "分析当前错误，尝试一种修复方案",
  "profile": "mcts-explorer",
  "timeout_ms": 120000,
  "idempotency_key": "node-7-turn-1"
}
```

`profile` 由宿主解析为已批准的 Agent 配置。分支中的工具调用继续通过 宿主 JSON-RPC 执行；搜索分支不得递归触发 `/mcts`。v1 的分支交互请求以 job 失败结束，回传所缺输入，由主 Agent 在搜索结束后处理。

`agent_turn` 与 `exec` 总是返回 job receipt，统一通过以下通知给出执行结果：

- `host.job.started`：执行开始。
- `host.job.output_delta`：可选输出增量，包含 `stream`、`delta`。
- `host.job.tool_activity`：可选工具摘要。
- `host.job.completed`：完整最终 `output` 和 `usage`；命令还包含 `exit_code`。
- `host.job.failed`：包含 `failure.code/message` 和已发生的 `usage`。

每条通知携带 `run_id`、`job_id`、递增的 `sequence`。终态必须在 receipt 之后发送，每个 job 仅一个终态；重复通知按 `(job_id, sequence)` 去重。关闭连接或输出 EOF 不等于任务成功。v1 连接丢失时终止本次运行，由宿主取消其 job；不自动重试执行有副作用的操作。

非零命令退出码仍是执行完成，由评价器解释；执行超时、传输失败或取消属于 job 失败，不能自动作为候选零分。宿主限制输出大小，截断时在终态明确标记 `truncated: true`。

## 4. 协议 C：搜索信息与最终结果交给 Agent

插件发出的每条通知携带 `plugin_id`、`run_id` 和运行内递增的 `sequence`。`plugin.wait` 返回相同的最新序号，宿主按运行和序号去重。

| 信息 | 交付位置 | 语义 |
| --- | --- | --- |
| `plugin.started` | UI 状态流 | 搜索已启动 |
| `plugin.progress` | UI 状态流 | 节点数、活动分支数、当前候选摘要与用量 |
| 完整树、UCT 数值、原始分支输出 | 诊断存储 | 不默认注入主 Agent 上下文 |
| `plugin.completed` | 主 Agent 的结构化插件结果 + UI 结果卡片 | 建议、证据、候选和产物 |
| `plugin.cancelled/failed` | 主 Agent 结果 + UI 状态 | 停止原因、已验证的有限发现、后续所需输入 |

主 Agent 不消费每次 rollout 的全部输出。最终交付应包含可直接使用的 `summary`、`answer`、证据和局限；搜索分支内容按来源标注为插件结果，不作为新的用户指令或系统指令。

### 完成结果

下面的内容放在 `plugin.completed.params.result` 中，也由终态 `plugin.wait.result.result` 返回：

```json
{
  "status": "completed",
  "summary": "搜索得到一个推荐修复方案，仍有两项测试未通过。",
  "answer": "推荐修改 parser 的状态切换和空输入处理。候选通过 18/20 项测试，剩余两项失败及修改差异见证据。",
  "stop_reason": "budget_exhausted",
  "selected_candidate_id": "node-7",
  "candidates": [
    {
      "candidate_id": "node-7",
      "state_id": "state-007",
      "score": 0.81,
      "evaluation_version": "parser-checks-v1",
      "summary": "修正状态切换和空输入处理",
      "evidence_refs": ["evidence-1", "evidence-2"]
    }
  ],
  "evidence": [
    {"evidence_id": "evidence-1", "kind": "test_report", "content": "18 passed, 2 failed"},
    {"evidence_id": "evidence-2", "kind": "diff", "content": "示例占位：实现必须在此填入真实 diff 内容"}
  ],
  "limitations": ["两项测试未通过；尚未应用到主工作区"],
  "apply_plan": {
    "kind": "continue_from_state",
    "state_id": "state-007",
    "base_state_id": "state-001"
  }
}
```

`score` 是有限的 `[0,1]` 搜索奖励，不能解释成成功概率。`evaluation_version` 标记评价依据；不同版本的评分不默认可比。没有可靠置信度估计时不提供 `confidence`。`completed` 表示搜索正常结束，不代表任务验证通过。没有有效候选时 `selected_candidate_id`、`apply_plan` 为 `null`，`candidates` 可为空，并说明原因；纯文本结果也可以没有应用计划。

所有 `evidence_refs` 必须能在本结果的 `evidence` 中解析。宿主保存最终结果后才释放其依赖资源；v1 优先内联可交付证据，过长内容必须说明截断。

### 宿主 JSON-RPC 的结果交付职责

1. 校验结果中的候选、快照归属和证据引用，保存最终结构化结果。
2. 把 `summary/answer/evidence/limitations` 作为一次插件结果交给主 Agent，恢复主会话，让 Agent 面向用户解释建议和验证情况。
3. UI 可以直接展示进度与候选卡片，不必为每次进度更新启动模型调用。
4. 结果应用由宿主已有的授权策略决定。插件中的 `apply_plan` 是建议，不能自行触发合并或主会话切换。
5. 应用前核对主会话是否仍基于 `base_state_id`；如果用户已继续修改，返回冲突并重新协调，不覆盖新状态。应用完成后由宿主发送应用回执，主 Agent 才能声称修改已生效。

等待用户确认属于宿主结果处理阶段。宿主可以向 UI 发布 `plugin.awaiting_confirmation`，插件自身无需继续占用搜索执行流程，也不在 manifest 中声明该事件。应用策略保留在 宿主 JSON-RPC 内部，不再要求 `plugin.start` 携带 `apply_policy`。

## 5. 错误与资源回收

| Code | 含义 |
| ---: | --- |
| -32700 | parse error |
| -32600 | invalid request |
| -32601 | method not found |
| -32602 | invalid params |
| -32603 | internal error |
| -32001 | capability denied |
| -32002 | run not found |
| -32003 | branch not found |
| -32004 | deadline exceeded |
| -32005 | cancelled |
| -32006 | state expired |
| -32007 | idempotency conflict |
| -32008 | incompatible protocol or required capability unavailable |
| -32009 | branch busy |
| -32010 | budget exhausted |

已识别运行的业务错误 `data` 携带 `plugin_id`、`run_id`；资源相关错误另带 `branch_id/state_id/job_id`。解析失败时不要求不存在的运行信息。错误发生后只有宿主能决定是否安全重试，取消不能撤销已经发生的外部副作用。

宿主终止运行时停止受理新操作，取消并等待 job 停止，再回收分支。终态结果所需的选中状态与证据单独保留，并明确过期时间；插件进程退出不应清理其他插件或其他运行的资源。

## 6. 传输约定

JSON-RPC 定义消息语义，HTTP、Unix socket 或 stdio 提供传输，两者不是互斥选项。

- 首版本机连接使用 stdio 或 Unix socket；UTF-8、每行一条完整 JSON 消息，字符串中的换行转义，日志写 stderr。
- 同一连接允许双向并发请求，等待 `plugin.wait` 或宿主执行结果时仍须接收另一方向的请求和通知。
- 远程传输可复用相同消息，但认证、连接恢复和帧限制需另行协商，本文不宣称已定义远程部署规范。
- 用户入口和 UI 继续使用 宿主 JSON-RPC 的 HTTP / SSE。插件不直接访问公开 session API，Core 的执行操作统一经过宿主能力 RPC。

## 7. Schema 校验入口

Schema 使用 JSON Schema Draft 2020-12。根节点校验单条请求、通知或响应；请求和通知按 `method` 选择对应载荷，未知方法、旧字段及额外字段均不通过校验。

- 插件清单：`#/$defs/manifest`。
- 完成结果：`#/$defs/completionResult`。
- 方法参数和成功结果：通过顶层 `x-methods[method].params/result` 查找 `$ref`。
- 通知载荷：通过 `x-notifications[method].params` 查找 `$ref`。

`x-methods`、`x-notifications` 是查找表注解，不是 JSON Schema 校验关键字。JSON-RPC 响应没有 `method`，根节点只能校验响应封装；接收方必须用响应 `id` 找到原请求，再按该方法的结果定义校验 `result`。校验独立定义时须保留根文档的 `$defs` 引用上下文。

Schema 明确了此前仅用文字描述的载荷：`plugin.cancel` 返回 `run_id/status`；`plugin.shutdown` 返回 `plugin_id/status: stopped`。`plugin.progress` 的 `summary` 必填，节点数、活动分支数、候选标识和用量可选；取消事件要求 `reason/summary`，失败事件要求 `failure/summary`，两者可附 `findings` 和 `required_input`。`host.job.tool_activity` 使用 `summary`；job 完成的 `output` 是文本，`usage` 是宿主提供的非负整数计数表，空对象表示数据不可用。

校验器需要启用 `date-time` 格式检查，并在 JSON 解码时拒绝 NaN/Infinity。资源归属、幂等去重、终态唯一性、候选与证据 ID 的引用关系、能力授权和命令 job 必须带 `exit_code` 等跨消息约束，仍由宿主在运行时检查。

独立的协议测试不依赖现有 Python 编解码层：

```bash
python -m pip install -r docs/protocols/requirements-test.txt
python -m unittest discover -s docs/protocols/tests -v
```
