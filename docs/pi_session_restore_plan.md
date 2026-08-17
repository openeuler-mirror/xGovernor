# Pi 会话场景复原改造 Plan

> 性质：~~实施计划（plan），非现状描述~~ **已落地（2026-08-17，commit `e1789f6` 惰性复原 v0.1 +
> `d2f25ff` 修复 v0.2）**。本文档转为历史记录：设计决策与验收基准留档在此，现状描述以
> `runtime_adapter_guide.md` / `protocol_boundaries.md` / `apps/runtime-pi/demo/easydemo.md` / README 为准。
> 历史正文里的「pi_demo.md」均指现 `apps/runtime-pi/demo/easydemo.md`。
>
> 背景：daemon 重启后，沙箱状态经 `InstanceManager::reconcile()` 从 SQLite 账本恢复，**但 `pi` 子进程
> 及其进程内注册表随 daemon 一起消失**。由于 pi 本身把会话持久化为 JSONL 文件（`--session`/`--session-dir`
> 官方支持重新打开），复原 = 把"重新拉起 pi 并挂回原会话文件"这条路补通。
>
> 已定决策（用户拍板，2026-08-17）：
> 1. **会话文件按 runtime_id 落到 daemon 数据目录**下（`~/.xgovernor/pi-sessions/<runtime_id>/`，
>    经 `XGOVERNOR_DATA_DIR` 可改根路径——与 `xgovernor.db` 同根）。
> 2. **进行到一半的 turn 默认丢弃**，只保留上一个完整状态；不尝试续跑半截 turn。
> 3. **惰性复原**：daemon 启动时不批量拉起 pi 进程，等某个 runtime_id 真正来了请求再复原。

---

## 0. 现状事实（本 plan 的依据，均已核对到行）

| # | 事实 | 位置 |
|---|---|---|
| F1 | sessions 表已有 runtime 状态检疫坑位：`runtime_json` 列（NOT NULL），存 `OpaqueRuntimeState { runtime_kind, schema_version, state: Value }` | `crates/core/src/sqlite_repository.rs:72-98`、`crates/core/src/domain.rs:97-105` |
| F2 | 该坑位**永远是 Null**：`open_impl` 写死 `state: Value::Null`，此后无任何写入点 | `crates/core/src/application.rs:332-336` |
| F3 | `RuntimeStartRequest.state: Option<OpaqueRuntimeState>` 已存在但恒传 `None`；`PiRuntime::start` 不读它 | `crates/core/src/runtime_adapter.rs:16`、`application.rs:304` |
| F4 | `application.rs` 已留 `TODO(state-persistence)` 注释，位置正是 submit_turn 的终态转发处 | `application.rs:458-461` |
| F5 | 重启后带旧 runtime_id 的 open / submit_turn：SQLite 行还在 → `require_session` 通过 → adapter 内存注册表空 → 塌缩成 404 `not_found`，与"从不存在"不可区分；SQLite 行永远停在 idle | `application.rs:215-235, 367-444`、`apps/runtime-pi/src/lib.rs:447-456` |
| F6 | `RuntimeAdapter::attach` 唯一生产调用点是 open 带 runtime_id 的 re-attach 分支；`PiRuntime::attach` 是纯查内存表 | `application.rs:233`、`lib.rs:1028-1030` |
| F7 | `InstanceManager::reconcile()` 启动时已把账本里 active 的沙箱经 `OperationAttach::attach` 恢复进 manager 自己的 registry；但 `ReconcileOutcome` 在 `main.rs` 被丢弃，PiRuntime 拿不到 | `crates/manager/src/lib.rs:699-747`、`apps/server/src/main.rs:116-127` |
| F8 | `SessionRepository` trait 只有 get/save，没有 list——惰性复原**不需要** list（点查即可），schema 零改动 | `application.rs:923-927` |
| F9 | pi 0.84.2 官方支持：`--session <path\|id>` 打开指定会话文件、`--session-dir <dir>` 自定义存储目录（rpc 模式文档明确列出 `--session-dir`）；会话文件为 JSONL 树（v3），按 cwd 分桶命名 `<timestamp>_<uuid>.jsonl` | pi 自带 docs/sessions.md、docs/rpc.md、docs/session-format.md |
| F10 | 我们已删 `.current_dir()`，pi 的 cwd 恒为 daemon cwd → 默认所有会话挤同一个 cwd 桶，靠文件名区分，无法机械认领——这是决策 1 要求显式 `--session-dir` 的直接原因 | `lib.rs:945-961` |
| F11 | 重启后 close 旧 runtime_id 也 404，沙箱泄漏只能等 e2b 过期或手工删（pi_demo.md §11 已知坑）——本改造顺带修复 | `docs/pi_demo.md` §11 |
| F12 | **实测确认**：`pi --mode rpc --session <path>` 真实恢复上下文——起一个 pi 进程发一轮 prompt、SIGKILL、带 `--session <上一份 jsonl 路径>` 起第二个 pi 进程发新 prompt，fake LLM 收到的 messages 数组含两轮 user 消息（历史被完整重发）。相对路径与绝对路径均可用；同一 session-dir 下继续写同一个文件，不产生新文件 | 探针 `pi_probe3.mjs` + 生成的 jsonl（见下方 Phase 0 结论） |
| F13 | 显式传 `--session-dir <dir>` 时，jsonl 直接落在该目录下，**不再**按 cwd 分二级子目录（F10 的担心只适用于默认无 `--session-dir` 的情况）；文件名规律 `<ISO时间戳，冒号转-`\_<uuid>.jsonl`，例如 `2026-08-17T04-03-11-637Z_01a00de3-....jsonl` | 同上 |
| F14 | **超出预期，改写 §1.3 设计**：pi 的 SessionManager 对一个 turn 做整体批量落盘——turn 期间（session 头行/model_change/thinking_level_change/user message 等）在到达终态（assistant message 拿到非 `pending` 的 `stopReason`）之前，**磁盘上不会出现该 turn 的任何字节**，哪怕 user message 早已经在 10 秒前就回显到 RPC stdout。用 `fake_llm_slow.mjs` 制造 10s 的 LLM 挂起窗口，每秒轮询磁盘，文件在整整 12 秒内不存在，LLM 一返回终态就一次性写入全部 5 行。SIGKILL 中途杀 pi，session 文件永远精确停在"上一个完整 turn 结束"那一刻——没有半行、没有悬空 user message、没有半截 assistant message，天然满足决策 2。pi 自带 `docs/session-format.md:120` 只承诺了 assistant message 这一半（"pending 不应出现在 JSONL 里"），实测确认整个 turn 都是这个粒度 | 探针 `pi_probe4.mjs`/`pi_probe5.mjs` + `fake_llm_slow.mjs`（详见 Phase 0 结论） |

---

## 1. 总体设计

一句话：**start 时把"重新拉起所需的一切"写进 SessionRecord.runtime（F1 的坑位），重启后任何请求踩到
"SQLite 有行、adapter 没实例"的缝隙时，用这份状态把 start 的后半段重放一遍（惰性），pi 用 `--session`
自己把上下文加载回来。**

不新增表、不改 schema（F1/F8）；不新增协议字段（F3 的通道本来就是为此留的）；不违反协议分层——
`OpaqueRuntimeState.state` 的内容归 PiRuntime 私有编解码，application 层只经手 opaque blob，
正是 `protocol_boundaries.md` §4 "runtime 状态检疫"的规定用法。

### 1.1 状态 blob 的形状（PiRuntime 私有，schema_version 从此有意义）

```json
{
  "backend_id": "e2b",
  "executable": "/opt/homebrew/bin/pi",        // 仅当当初显式覆盖过才存
  "extension_dir": "/.../extension",           // 同上
  "pi_session_dir": "~/.xgovernor/pi-sessions/<runtime_id>",
  "workspace_metadata": { ... }                // git 元数据原样快照（沙箱重建时才用，见 §4.3）
}
```

`OpaqueRuntimeState { runtime_kind: "pi", schema_version: 1, state: <上面> }`。
版本号语义：blob 形状变更即 bump，PiRuntime 读到不认识的版本 fail-closed 报错（不猜）。

### 1.2 复原时序（惰性，缝隙触发）

```
请求(submit_turn/open-reattach/answer/cancel/close)
  → require_session 通过（SQLite 有行）
  → ensure_runtime_attached(record)               [application 层新增小助手]
       ├─ runtime.attach(runtime_id) Ok → 直接继续（正常热路径，零开销）
       └─ NotFound 且 record.runtime.state != Null
            → runtime.start(RuntimeStartRequest{ state: Some(record.runtime), ..从 record 重建 })
            → 重试 attach → 继续
  → 原有逻辑不变
```

PiRuntime::start 内部按 `request.state` 分两条路：
- state 为 None → 现行冷启动路径，唯一新增动作是 spawn 前创建 per-id session dir 并加
  `--session-dir <dir>` 参数、成功后产出 §1.1 的状态 blob；
- state 为 Some → **复原路径**：不再走 git clone（工作区在沙箱里已存在或已死，见 §4.3），改为向
  `InstanceManager` 要回既有沙箱句柄，重注册 bridge token，然后
  `pi --mode rpc --session <dir 里最新的 .jsonl> --session-dir <dir> -e ...` 拉起。

### 1.3 半截 turn 的丢弃（决策 2）—— 已被 F14 简化

**原计划**（保留在此存档）是写一个裁剪函数：丢弃末尾解析不了的残行，再从尾部回溯到最后一个
带 stopReason 的 assistant message，把之后的半截内容全部裁掉。**F14 实测推翻了这个前提**：pi
自己就是按整个 turn 批量落盘的，SIGKILL 中途杀掉时磁盘上根本不会出现半截 turn 的字节——文件
永远精确停在上一个完整 turn 结束的地方。也就是说决策 2 的效果，pi 自己已经免费保证了，daemon
不需要做主动裁剪。

**改后设计**：复原路径 spawn 前只做一次**防御性校验**（仍是纯函数、独立可测，但不再是"裁剪"，
是"确认"）：

1. 读最新 jsonl，末行必须能完整 parse——parse 不了（理论上不该发生，但不赌）就整份判 corrupt；
2. 末行必须是 `type:"message"` 且 `message.role == "assistant"` 且 `message.stopReason` 存在
   且不是 `"pending"`——不满足同样判 corrupt；
3. 校验不通过（含文件为空/不存在）一律按 §1.4 的 `pi_session_state_lost` fail-closed，
   **不**尝试自己动手裁剪修复（没有必要裁的东西，出现裁不干净的情况说明假设被打破，宁可拒绝
   复原）。

不再需要 `.pre-restore.bak`（没有写操作，谈不上需要备份）；`session_file.rs` 这个模块保留，但
职责从"裁剪"改为"校验+找最新文件"。

服务端侧配套：复原成功后若 SQLite 行 status 停在 `running`，落回 `idle`；被丢弃的那个 turn
无需补发终态事件——重启已使所有 SSE 连接断开、内存 turn_gate 清空，客户端本来就该按断线重查处理。

### 1.4 明确的失败语义（不静默降级）

- **沙箱已死**（e2b endAt 过期 / reconcile 判 orphan）：复原路径向 manager 要句柄拿不到 →
  返回 `Unavailable`，错误信息带稳定前缀 `pi_sandbox_gone`，SQLite 行写 `last_error` 并置
  `failed`。不自动重建空沙箱让 pi 带着错误世界观继续跑（重建留给显式的新 open）。
- **会话文件缺失/裁剪后为空**：同样 fail-closed（`pi_session_state_lost`），不静默开新会话——
  那会是"看起来复原了其实失忆"的最坏结果。
- **close 特例**：close 的目的只是销毁，不值得为它 spawn 一个 pi 再 kill。close 路径在
  adapter NotFound 时改走"按 state 直接清理"：读 blob 里的 backend_id → 对应 manager 的
  `stop_instance(runtime_id)` → 删 session dir → 存档 SQLite 行为 closed。这一条顺带修掉 F11 的
  沙箱泄漏坑。

---

## 2. 分阶段实施

### Phase 0：事实钉死（半天，先做，结论写进本文档再动代码）

| 验证项 | 方法 | 风险如果不成立 |
|---|---|---|
| V1: `pi --mode rpc --session <path>` 可用（rpc docs 只明确列了 `--session-dir`，`--session` 是全局 flag，理论可用但未在 rpc 一节背书） | 真实 pi 手工验证：先跑一轮生成 jsonl，kill，带 `--session` 重启，发 prompt 看是否延续上下文 | 若不可用，退路：`--session-dir <per-id dir>` + `-c`（continue most recent；桶里只有这一个会话，语义等价）——两条路都验，取更稳的 |
| V2: per-id `--session-dir` 下文件布局（是否仍按 cwd 分桶一层子目录） | 同上观察磁盘 | 只影响"找最新 jsonl"的 glob 写法 |
| V3: v3 JSONL 里"完整 turn 锚点"的准确判据（stopReason 字段名/位置） | 读真实生成的文件 + pi docs/session-format.md 对照 | 裁剪函数判据要改，接口不变 |

### Phase 0 结论（2026-08-17，实测已做，三项全部过关）

用真实 pi 0.84.2 二进制（无网络环境，靠 `pi.registerProvider` 注册一个本地假 OpenAI-completions
provider 打到本机 fake HTTP server，绕开真实 API key 依赖）做的实测：

- **V1 通过**：`--session <path>` 在 rpc 模式下确实恢复上下文（F12）。
- **V2 通过**：`--session-dir` 下文件是平铺的，不再按 cwd 分桶，文件名规律已记录（F13）。
- **V3 通过，且有意外收获**：turn 锚点判据确认是 `message.stopReason`（F9 假设成立），但更重要的是
  发现 pi 按整 turn 批量落盘（F14）——这直接把 §1.3 的"裁剪函数"降级成"校验函数"，工作量和风险
  都比原计划小。§1.3 正文已按此改写。

结论已回填进 §0（F12/F13/F14）与 §1.3；Phase 1 可以开工。

### Phase 1：写入路径（冷启动即持久化，约 1 天）

1. `apps/runtime-pi/src/lib.rs`
   - `PiRuntimeExt` 不动（决策来源仍是 ext）；新增私有 `PiPersistedState`（§1.1 形状）+
     `to/from OpaqueRuntimeState` 编解码（带 schema_version 校验，fail-closed）。
   - `start()` 冷路径：spawn 前 `create_dir_all(<data_dir>/pi-sessions/<runtime_id>)`，
     spawn 参数加 `--session-dir`；session dir 根路径由 `PiRuntime::new` 新参数注入
     （`main.rs` 从 `xgovernor_db_path()` 同根推导），不从环境变量在 adapter 里现读——
     配置单一确定路径（feedback_avoid_toggle_config_complexity）。
   - `PiInstance` 增持 `persisted_state`；实现 `export_state()`（trait 现成缺省方法，F3 旁边）
     返回它。**不**宣告 StateExport capability——那是 checkpoint 语义的能力门控，这里是
     governor 内部自用，两回事，capabilities() 不动。
2. `crates/core/src/application.rs`
   - `open_impl` 在 `runtime.start` 成功后调 `runtime.export_state(runtime_id)`：
     `Ok(state)` → 写进 record.runtime 再 save；`Err(UnsupportedCapability)` → 保持 Null
     （其他 runtime 不受影响，机制通用）。
3. 测试：contract 测试断言 open 后 SQLite 行的 runtime_json 非 Null 且含 backend_id；
   fake_pi 增加对 `--session-dir` 参数的记录断言。

### Phase 2：惰性复原路径（核心，约 2 天）

1. `crates/manager`：新增 `InstanceManager::resume_instance(runtime_id) -> Result<Arc<dyn OperationBackend>, _>`
   ——从 reconcile 恢复出的 registry（F7）里按 runtime_id 取既有 `BoundInstance` 的 backend 句柄；
   没有则 `NotFound`（即 §1.4 的沙箱已死信号）。只读不建，与 `start_instance` 语义分开。
2. `apps/runtime-pi/src/lib.rs`：`start()` 复原分支（§1.2）：解码 state → 选 manager →
   `resume_instance` → 校验最新 JSONL（§1.3，独立模块 `session_file.rs`，纯函数，找最新文件 +
   末行校验，不裁剪）→ 注册 bridge token → spawn 带 `--session`。注册表冲突检查复用现有逻辑，
   天然防止并发双复原（先到者 insert，后到者 attach 命中）。
3. `crates/core/src/application.rs`：新增 `ensure_runtime_attached(ctx, record)`，接入四个缝隙点：
   `open_impl` re-attach 分支（F6）、`submit_turn_impl`、`answer_interaction`、`cancel`。
   复原成功后 status `running` → `idle` 落库。
4. close 特例（§1.4）：adapter NotFound 时按 state 直接清理。实现放 application 层
   （它有 record）+ PiRuntime 暴露一个窄方法 `cleanup_from_state(runtime_id, state)`，
   或对称起见直接让 `stop()` 接受"registry miss 但 state 可用"的第二输入——实施时取改动
   面更小者，倾向前者。
5. 测试（全走 fake_pi，不依赖真实 pi）：
   - 重启模拟：构造 PiRuntime A → start → drop A（杀 pi）→ 构造 PiRuntime B（同 session dir、
     同 manager 底座）→ submit_turn 触发复原 → 断言 fake_pi 收到 `--session` 指向最新文件；
   - 校验函数单测（F14 后不再是"裁剪"，是"确认"）：正常文件（通过）、空文件（corrupt）、
     末行 parse 失败（corrupt）、末行是 user message 而非 assistant（corrupt，理论上不该由
     pi 产生，但作为 fail-closed 兜底必须测到）五类样本；
   - 沙箱已死 → `pi_sandbox_gone`、状态置 failed；
   - close-after-restart → 沙箱销毁 + session dir 删除 + 行 closed。

### Phase 3：真实端到端验证 + 文档收口（约半天）

1. 真实 pi + e2b 复现 pi_demo §10 流程，中途 kill -9 daemon，重启后同 runtime_id 直接
   submit_turn 问"我们刚才在做什么"，验证上下文延续；再验 close 路径沙箱真实销毁。
2. 文档同步（老规矩，例外记录三处一起看）：
   - `apps/runtime-pi/src/lib.rs` 模块文档：Scope cut 段落改写——"pi 进程注册表不跨重启"仍真，
     但补"经 SessionRecord.runtime + 惰性复原可重建"；
   - `docs/pi_demo.md` §11：划掉 F5/F11 两条坑，写明复原语义与 fail-closed 场景；
   - `docs/runtime_adapter_guide.md`：`export_state` 的这种 governor 内部自用模式写成
     对下一个 runtime 的通用建议；
   - `docs/protocol_boundaries.md` §4 表格"会话治理"行补一句状态检疫坑位的实际用法。
3. 记忆文件更新（xgovernor-runtime-pi-landed 或新条目）。

### Phase 3 结论（2026-08-17，真实 E2E 已做，先红后绿）

**第一轮（v0.1，红）**：真实 E2E 复现 pi_demo §10 流程并中途 `kill -9` daemon 后，同 runtime_id
submit_turn 得到 `pi_sandbox_gone` —— 沙箱明明活着（E2B 平台可见），fail-closed 却误报。抓到两个根因：

- **根因 A（reconcile 未按 backend 隔离）**：`provider_instances` 账本表被 local/e2b 两个
  `InstanceManager` 共享，但 `list_active()` 无 provider 过滤。daemon 重启时先跑的 local manager
  reconcile 把 e2b 行判成"provider 未上报"（该分支静默、不打日志）→ `record_deleted`；e2b manager
  又反向误杀 local 行。任何双后端 daemon 重启后所有会话都会被判孤儿。
- **根因 B（provider `attach` 仅内存注册表）**：`E2bProvider`/`LocalProvider` 的 `attach` 都只在
  本进程内存注册表里查——重启后 provider 是全新实例、注册表为空，即使 `list_instances()` 如实上报
  沙箱存活也必然 NotFound → reconcile 走"re-attach failed" → 仍判孤儿。e2b 侧代码自己就标注了
  "cross-restart e2b re-attach is not implemented yet"（当时 `provider.rs` 实况测试显式断言
  attach 必须失败）。F7 的"reconcile 已把账本里 active 的沙箱经 attach 恢复进 registry"前提对真实
  provider 不成立。
- 连带：close-after-restart 走 §1.4 特例时 `manager.stop_instance` 因注册表为空而 NotFound，
  `cleanup_from_state` 的 `let _ =` 吞掉错误 → close 返回 closed 但**沙箱真实泄漏**（F11 的新形态）。

**第二轮（v0.2，绿，commit `d2f25ff`）**，三个修复 + 复验全部通过：

- **Fix A**：`ledger.list_active(&self.kind)` 按 provider 过滤（`crates/backend/src/ledger.rs`、
  `sqlite_ledger.rs`、`crates/manager/src/lib.rs`），两个 manager 各管各的行。
- **Fix B**：e2b 新增 `reattach_from_persisted_instance` —— 从账本 `metadata.provider_options` 重建
  `E2bBackendState`，经 `fetch_sandbox_detail` 重新拉取 **envd access token** 并校验沙箱存活（非
  `running` 即 `NotFound` fail-closed）；local 从 `metadata.provider_options`（create 时新增持久化）
  重建后端，工作区目录已消失则 fail-closed。`attach` 语义变为"注册表快路径 + 账本重建慢路径"。
- **Fix C**：`InstanceManager::destroy_by_runtime_id` —— 注册表 miss 时按账本行直接
  `lifecycle.delete`；`cleanup_from_state` 不再吞错（失败打 warn 留痕）。
- 复验矩阵（真实 pi 0.84.2 + DeepSeek + E2B，`kill -9` 硬杀）：e2b 重启后同 runtime_id 问
  "我们刚才在做什么" → pi 完整回忆上一轮 marker 任务（上下文延续 ✅）；同一 sandbox id 复用、
  marker 文件仍在（沙箱连续性 ✅）；close 后 E2B 平台 running 归零（真实销毁 ✅）；local 后端
  同样延续 ✅；外部删除沙箱后的重启 → reconcile 正确判 orphan（warn 日志）+ `pi_sandbox_gone`
  + 行 `failed`（§1.4 fail-closed ✅）。单测 59 例全绿（manager 47 + runtime-pi 12）。

**遗留观察（非阻塞）**：正常 close 路径（实例已挂回）不删 per-runtime 会话目录（jsonl 留档），
与 `cleanup_from_state` 特例删除的行为不对称——留档有价值，统一策略留待产品决策。

---

## 3. 刻意不做（本轮范围外，防蔓延）

- **不做启动期批量复原**（决策 3 已否）；也不做后台预热。
- **不做半截 turn 的续跑/重放**（决策 2 已否）。
- **不做沙箱已死时的自动重建**（§1.4，显式失败，重建走新 open）。
- **不给 SessionRepository 加 list**——惰性模型点查够用（F8）；等将来需要"列出可复原会话"
  的产品面再说。
- **不动协议**：session-protocol / provider-protocol / operation-protocol 零变更；
  wire 上唯一可感知的变化是"重启后旧 runtime_id 不再 404"。
- **不实现 StateExport 能力宣告**（checkpoint 语义另案）。

## 4. 风险与开口

1. **V1 不成立**（`--session` 在 rpc 模式失效）：退路 `-c` 已备（Phase 0 表格），不阻塞。
2. **裁剪判据漂移**：pi 升级可能改 session 格式（有 version 字段护栏）；裁剪函数读到
   不认识的 version → fail-closed 报 `pi_session_state_lost`，宁可拒绝复原不可错误裁剪。
3. **workspace_metadata 快照的用途**（§1.1）：本轮只存不用（沙箱死了就 fail），存它是为了
   将来若做"显式重建"时不用改 blob 形状——一次 schema 版本躲一次迁移。
4. **local 后端的"沙箱"其实是宿主机目录**：daemon 重启不影响其存活，`resume_instance`
   应恒命中；e2b 才有真实的过期风险。测试矩阵两个后端都要覆盖。
