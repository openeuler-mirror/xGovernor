# 会话编排骨架（Session Orchestration Skeleton）

> 性质：descriptive。本文如实描述当前已落地的会话编排实现——最小闭环与三个治理组件（A 绑定 / B 租约 / C 回收）。代码注释中对 "Component A/B/C" 的引用均指向本文。
> 协议与边界的规范性定义见 [protocol_boundaries.md](./protocol_boundaries.md)。

---

## 1. 总览

会话编排的核心是 `xgovernor_core::SessionApplication`：它把 wire 请求翻译为对六个端口的编排调用，自身不持有任何具体实现。

| 端口 | 职责 | 当前生产实现 |
|---|---|---|
| `RuntimeAdapter` | agent runtime 接缝（边界三） | `LocalMockRuntime`（apps/runtime-local） |
| `SessionRepository` | 会话记录存取 | `SqliteSessionRepository`（crates/core，`~/.xgovernor/xgovernor.db`，重启后记账仍在） |
| `SessionEnvironmentNormalizer` | open 时的环境归一化 | `LocalWorkspaceEnvironment`（仅支持 daemon_default / local_path） |
| `TurnIdGenerator` / `RuntimeIdGenerator` | 服务端 id 签发 | UUID |
| `Clock` | 时间源 | 系统时钟 |

公开方法：`open`、`submit_turn`、`answer_interaction`、`close`、`detach`、`heartbeat`、`cancel`、`fork`；通过 `with_lease_table` 挂接租约表后，控制路径全部经过租约守卫。

## 2. 最小闭环（Phase 1）

已被集成测试证明的路径：HTTP `open` → `turn` → 真实本地沙箱 `exec` → 归一化 SSE 事件。

**open 流程**（唯一入口，规范 §4 的环境归一化落点）：

1. `runtime_id` 缺省时由服务端签发；携带已存在的 `runtime_id` 时为幂等重附着（`RuntimeAdapter::attach` + 投影返回，不重复创建）。
2. `SessionEnvironmentNormalizer::normalize` 产出 workspace/isolation 事实与沙箱能力集。
3. 两族能力门控：requested sandbox 能力对照归一化结果、requested runtime 能力对照 adapter 宣告，任一缺席即 `UnsupportedCapability` 拒绝——**在触达 runtime 之前**。
4. `RuntimeAdapter::start`（runtime 专有引导走 `ext` 命名空间）。
5. 组装 `SessionRecord`（runtime 状态为 opaque blob：`runtime_kind` + `schema_version` + `Value`）并保存；保存失败时补偿调用 `RuntimeAdapter::stop`，不留半开会话。
6. 经 `project_session` 投影为 `SessionOpenResponse` 出网——域记录永不直接序列化上线。

**submit_turn 流程**：

1. 会话存在性检查 + 能力门控（`llm` 覆写需 ModelOverride、`reasoning_effort` 需 ReasoningControl）。
2. **幂等重放**：请求携带 `client_request_id` 且命中该会话的回执窗口（每会话保留最近 64 键，进程内存态）时，直接重放原回执（同一 `turn_id`、无新事件流），adapter 不会收到第二次提交。只有被 runtime 真实接受的提交才会记入窗口。
3. **单活跃 turn 占位（TurnGate）**：以会话为粒度在触达 adapter *之前*占位，占用中再次提交返回 Conflict（携带活跃 turn id）；adapter 拒绝提交时立即释放占位。
4. 服务端签发 `turn_id`，随回执返回；同一值贴在该 turn 的每一条 SSE 事件上（相关性合同）。
5. adapter 返回的事件流经 `project_runtime_event` 逐条投影为 wire 事件转发；占位在转发任务结束时释放（带 turn_id 比对，防止迟到任务误释放后继 turn 的占位）。
6. **终态保证**：若 adapter 事件流在未发出 `Completed`/`Failed` 前关闭，转发任务合成一条 `turn_failed`（code = event_stream_closed）——客户端永远能等到终态。
7. **弃流防御**：单条事件的转发发送有 8 秒上限（FORWARD_SEND_TIMEOUT，2026-08 从 60 秒下调）。订阅者消失（从未领取流或领取后死亡）导致发送超时/失败时，转发任务尽力取消该 turn（`RuntimeAdapter::cancel`）、排空 runtime 事件流后释放占位——没有这条防御，弃流会把单活跃 turn 占位永久卡死。下调到 8 秒是因为 `cancel` 现在有真实语义（见下方 `submit_turn`/`cancel` 合同小节），超时后真的能让 runtime 的在飞工作停下来，不再需要等一整分钟才触发。
8. **`submit_turn`/`cancel` 合同**（2026-08 收紧，`crates/core/src/runtime_adapter.rs` 的 trait 文档注释是规范文本）：`submit_turn` 必须**立即**返回事件接收端——真正的执行放进 adapter 自己 `tokio::spawn` 的后台任务，不能在返回前把整个 turn 跑完（旧实现这么做过，用有界 channel 甚至能死锁：调用方要等这次调用返回才能开始排空，发送方却卡在等排空的满 channel 上）。`cancel` 必须有真实中断语义：调用后一个正在跑的 turn 必须尽快走到终态（约定用 `Completed { outcome: Cancelled }`），不能是"返回 `Ok(())` 但 turn 该怎么跑还怎么跑"的空操作。两个 mock adapter（`apps/runtime-local`、`apps/runtime-e2b`）共享 `crates/core::TurnCancellationRegistry`（`Mutex<HashMap<runtime_id, (turn_id, oneshot::Sender)>>`）做取消簿记：`submit_turn` 调 `begin()` 拿到 `CancelSignal` 再 spawn 后台任务，后台任务里 `tokio::select!` 真实执行 future 和 `CancelSignal` 二选一；`cancel` 调 `fire()` 触发信号。两个 adapter 各自的"跑什么"业务逻辑（目前都是硬编码的一次 `exec`）仍然各自实现，不共享——只有取消簿记这个并发原语共享，理由见该类型的模块文档。合同用确定性测试锁定（不依赖 sleep/timing）：`#[tokio::test]` 默认单线程 runtime，`submit_turn` 返回后立即 `try_recv()` 必须是 `Empty`（后台任务还没机会跑）；`submit_turn` 返回后立即调用 `cancel`（其 body 是纯同步的锁+oneshot send，不会让出）能确定性地让后续 `select!` 选中已经触发的取消分支，而不是 50/50 的真实竞态。

## 3. Component A：Provider 编排（crates/manager，独立 crate）

> 2026-08 改造：本节描述的编排逻辑原先分散在 `crates/backend` 的 `ProviderBoundRuntime`（binding.rs）与 `QuotaEnforcedLifecycle`（quota.rs）两个类型里——前者只管 create+attach+registry，没有并发控制、没有配额；后者只管 per-owner 配额，不知道 registry、没有全局上限。二者都已删除，责任收拢进独立 crate `crates/manager` 的单一编排组件 `xgovernor_manager::InstanceManager`（`crates/manager/src/lib.rs`），这也是 [protocol_boundaries.md](./protocol_boundaries.md) §2.3/§4 现在指向的真实实现。下文按能力而非旧类型划分。

`InstanceManager` 是所有 adapter 共用的 provider 管线：`ProviderLifecycle::create` → `OperationAttach::attach` → 以 `runtime_id` 为键的实例注册表，以及对应的 stop/查询，外加旧实现没有的五项能力：

1. **per-`runtime_id` 幂等锁**：并发的两个 `start_instance` 用同一个 `runtime_id` 不再互相覆盖注册表条目（旧 `ProviderBoundRuntime` 明确记录过、判定为范围外未修的并发 bug——现已随本次改造关闭）；后到的一方直接拿到 `Conflict`，不排队不合并。
2. **attach 失败补偿删除**：从旧 `ProviderBoundRuntime::rollback_create` 原样搬入，行为不变。
3. **delete 失败 pending-release 重试队列**：`stop_instance` 里 `lifecycle.delete` 失败不再只是把条目留在注册表等一次可能永远不会来的手工重试，而是入队交给 `spawn_retry_loop()` 做指数退避的自动后台重试（`apps/server/src/main.rs` 在装配完 runtime 后 `runtime.spawn_pending_release_retry_loop()` 拉起，与孤儿回收器同一"不阻塞关闭的周期性 sweep"惯例）。
4. **全局（跨 owner）沙箱总数上限**：旧 `QuotaEnforcedLifecycle` 只按单个 `owner_ref` 限流；`InstanceManager` 额外维护一个不分 owner 的进程级总量上限（`InstanceManagerConfig::max_sandboxes_global`）。
5. **创建路径信号量准入 + 对 retryable 错误的退避重试**：create 路径原来没有任何限流（多少个 `start_instance` 同时到达就同时打到 provider），也没有对瞬时 provider 错误（`Transport`/`Timeout`）的容错。现在经一个准入信号量（`DEFAULT_MAX_CONCURRENT_CREATES`）节流，并对 `is_retryable` 判定为可重试的错误按 `RetryPolicy`（默认 3 次、指数退避、封顶 2 秒）重试。

配额仍按 `owner_ref` 实施（runtime-local 默认每 owner 4 个，加上第 4 点的全局上限），在 create 路径强制，超限映射为 `Unavailable`。配额计数器仍是纯内存态，`rehydrate(entries)` 方法（接受 `(owner_ref, instance_id)` 对）负责在启动时从对账结果种回计数。

**Provider 实例账本**（`ProviderInstanceLedger` port，`crates/backend/src/ledger.rs`——这个 trait 定义仍留在 `crates/backend`，`InstanceManager` 依赖它而非反过来；SQLite 实现 `SqliteProviderInstanceLedger`，`crates/backend/src/sqlite_ledger.rs`）：`InstanceManager::start_instance`/`stop_instance` 在每次创建/删除后落一条记账到 `~/.xgovernor/xgovernor.db` 的 `provider_instances` 表（与 `SqliteSessionRepository` 共享同一物理文件，WAL 模式下各自持有独立连接，互不冲突）。

**启动时对账**（`InstanceManager::reconcile`，`crates/manager/src/lib.rs`）：`ledger.list_active()` 取出账本认为还活着的每一行，与 `lifecycle.list_instances()`（`ProviderLifecycle` trait 方法，每个 provider 自报"我这边认为哪些实例还活着"）按 `instance_id` 比对。确认还在的（`confirmed`）会重新 `attach` 一次、把 `(runtime_id → BoundInstance)` 写回内存注册表，让重启前就存在的会话立刻能 `backend_for`/`stop_instance`；账本有但 provider 说没有的（`orphaned`，含"provider 报告还在但重新 attach 失败"的情况）会 `ledger.record_deleted` 软删，修掉漂移。返回的 `ReconcileOutcome{confirmed, orphaned}` 里的 `confirmed` 就是喂给配额 `rehydrate` 的输入——两步合起来是 `LocalMockRuntime`/`E2bMockRuntime` 的 `reconcile_on_startup()`，`apps/server/src/main.rs` 在装配完 runtime、开始服务真实流量之前调用一次（best-effort，失败只打日志，不 fail-closed——单进程本地账本的对账失败不该拖垮整个 daemon 启动）。

**明确没做的**（见 `crates/manager/src/lib.rs` 模块文档同一份记录）：`load`/`pause`（快照恢复）没有任何调用路径接入，旧实现也没有真正用到过；[protocol_boundaries.md](./protocol_boundaries.md) §2.4 的状态机强制点（`ProviderLifecycleState` 的 begin/complete_success/complete_failure）没有接入——`InstanceManager` 的 create/delete 仍是直接按 `Result` 分支，不经状态机；per-owner/全局上限仍是构造参数，不是可配置的策略存储。这三点不是本次改造的范围，特意留痕以免"crate 存在了"被误读成"顺带也修好了"。
## 4. Component B：租约表（crates/core/session_lease.rs）

单写者约束："一个会话同一时刻只有一个写入客户端"。

- **结构**：16 分片的内存表（按 session_id 哈希分片，不同会话的租约操作互不争锁）。daemon 重启即清空，客户端经下一次 open/heartbeat 重新获取。
- **陈旧阈值**：`STALE_LEASE_THRESHOLD_MS` = 45 秒（15 秒心跳间隔的 3 倍，容忍瞬时网络抖动）。超过阈值的租约可被接管。
- **获取语义**（`LeaseAcquireOutcome`）：无先前租约 / 本就持有（刷新）/ 接管陈旧租约 → Acquired；他人活跃持有 → Busy（附 holder 身份与 stale 提示，供客户端决定是否重试）；系统时钟早于 UNIX_EPOCH → ClockSkew，宁可拒绝也不静默放松单写者。
- **匿名调用者**：不带 `client_id` 的请求在无活跃租约时放行；但 `heartbeat` 对匿名请求直接以 `LeaseRequired` 拒绝——没有身份就没有可记录心跳的对象。
- **daemon 内部 principal**：`daemon:` 前缀（`daemon:cron`、`daemon:hook:<id>`、`daemon:channel:<id>`）。这类协作型后台调用者不获取租约、显式绕过持有者检查并留审计日志——取代旧世界"client_id 为空即绕过"的不可审计约定。
- **租约不锁定进行中的 turn**：旧持有者已启动的 turn 跑完为止，门控的只是*新的*提交。
- **detach 的语义**：整条移除租约表条目，含义是"runtime 保温、等下一次 open"——这是一个独立的生命周期状态，不是孤儿（见 Component C 的豁免）。

## 5. Component C：孤儿回收器（crates/core/orphan_reaper.rs）

客户端崩溃、网络分区、或从不调用 detach/close 的调用者会留下"租约老化但 runtime 仍在跑"的泄漏。回收器是唯一的兜底：

- **候选来源**：`SessionLeaseTable::snapshot()`，而非 SessionRepository——"谁最后心跳、何时"这一事实本来就住在租约表里。
- **阈值**：`ORPHAN_SESSION_THRESHOLD_MS` = 2 小时（刻意保守：detach 后一小时回来的用户仍应找到温热的会话）。
- **节奏**：`REAPER_INTERVAL` = 10 分钟一轮；首轮在启动一个间隔之后才跑，避免与 daemon 启动时仍在 open 途中的会话竞速。
- **TOCTOU 防护**：对每个候选在动手前重查 `has_live_lease`——snapshot 与执行之间落地的心跳会让该会话被放过。
- **关闭路径**：以匿名 `SessionLeaseClaim` 调用 `SessionApplication::close`；close 内部的租约检查经陈旧放行臂（45 秒阈值）通过——能走到这里的候选已经越过了严格得多的 2 小时线。
- **detach 豁免**：detach 过的会话没有租约表条目，永不被扫——这是设计而非缺口。
- **失败容忍**：单条 close 失败（例如会话已经由其他路径关闭）记日志跳过，不阻塞整轮扫描。

## 6. 已知边界与后续工作

- **持久化 + 启动时对账已落地**（本次改造，两轮）：`SessionRepository`（`SqliteSessionRepository`）与 provider 实例账本（`SqliteProviderInstanceLedger`）都是 SQLite-backed，共享 `~/.xgovernor/xgovernor.db`（可用 `XGOVERNOR_DATA_DIR` 覆盖）。进程重启后，`LocalMockRuntime::reconcile_on_startup`（`apps/server/src/main.rs` 在服务真实流量前调用）会把账本记录与 provider 实际状态对账、修复漂移（孤儿实例软删）、并把配额计数器从对账结果里 `rehydrate` 回来——见上面 §3 "启动时对账"的完整机制说明。`E2bProvider::list_instances()` 也已改为真实调用 v1 的 `GET /sandboxes`（不再是内存快照），e2b 的对账现在和 local 一样忠实，唯一保留的范围局限是单一环境变量身份、看不到多账号下的其他 sandbox（见 §3）。
- **manager 层已收拢为独立 crate（`crates/manager`）**（本次改造）：`ProviderBoundRuntime`/`QuotaEnforcedLifecycle` 两个旧类型及其所在的 `binding.rs`/`quota.rs` 已删除，责任统一进 `xgovernor_manager::InstanceManager`——完整能力清单见上面 §3。旧列在这里的两个已知 bug 现已随之关闭：**并发 `start_instance` 覆盖注册表条目**（现由 per-`runtime_id` 幂等锁防止）、**delete 失败导致配额/账本永久漂移**（现由 pending-release 重试队列自动收敛，不再只能等一次可能不会来的手工重试）。**仍然明确没有做的**：
  - **`reconcile()` 与并发的 `start_instance`/`stop_instance` 之间没有互斥**：设计上假定它在"装配完成、开始服务流量之前"跑一次；如果未来有代码路径允许两者并发（例如热重载而非冷启动），需要重新评估。这一条从旧 `ProviderBoundRuntime` 原样继承，`InstanceManager` 没有改变这个假设。
  - **状态机强制点未接入**（[protocol_boundaries.md](./protocol_boundaries.md) §2.4）：`InstanceManager` 的 create/delete 仍是直接按 provider 调用的 `Result` 分支，不经过 `ProviderLifecycleState` 的 begin/complete_success/complete_failure 转换。
  - **`load`/`pause`（快照恢复）没有调用路径**：旧实现也从未真正接线过，本次改造判定为独立能力，未来 `RuntimeAdapter` 需要时再设计。
  - **per-owner/全局上限仍是构造参数，不是策略存储**：与 `docs/tenancy_design.md` §7 步骤 4 已记录的局限一致。
- **传输层四道防线已落地**（本次改造）：streams 表 TTL/主动清理/全局条目上限（本文提及的 SSE streams 表本身）、per-tenant 请求速率限制、tower 请求体/超时/并发三层（含一处 `tower::limit::ConcurrencyLimitLayer` 与 axum `Router::layer` 的兼容性坑及修复）、优雅关闭（OS 信号 + 强制退出兜底）。完整描述见 [transport_defenses.md](./transport_defenses.md)，不在本文重复。
- turn 终态前的 runtime 状态导出/落库尚未接线（submit_turn 内有 TODO 注记）：export adapter 状态并写入 SessionRecord，这样重启后 `reconcile_on_startup` confirm 的会话不仅能恢复 provider 实例本身，也能恢复会话应用层状态。
- 操作面（exec / 文件 / checkpoint）的 HTTP 路由未暴露；wire DTO 已在 session-protocol 中就绪。
- **`owner_ref` 已从客户端 ext 载荷改为装配层机械推导**（本次改造，§7 步骤 4）：`SecurityContext::owner_ref()`（`crates/core/src/security.rs`）为 tenant 推导 `tenant/{tenant_id}`，为 admin 返回固定哨兵值 `ADMIN_OWNER_REF`（`"admin"`）。`RuntimeStartRequest` 新增一等字段 `owner_ref`，`SessionApplication::open_impl`/`fork_impl` 用 `ctx.owner_ref()` 填充；`apps/runtime-local`、`apps/runtime-e2b` 两个 adapter 的 ext 命名空间（`runtime_local`/`runtime_e2b`）不再包含 `owner_ref` 键，只保留 `backend_id`——旧的从客户端 ext 读 owner_ref 的路径已整条删除，不留兼容/双路径。`InstanceManager`（`crates/manager/src/lib.rs`，原 `QuotaEnforcedLifecycle` 的职责所在）对 `owner_ref == "admin"` 这个哨兵值完全豁免 `max_per_owner` 与全局上限（仍计数用于诊断，只是不拒绝）——admin "百无禁忌"。`crates/manager` 与 `xgovernor-core` 之间没有共同依赖（`manager` 依赖 `backend`，不依赖 `core`），`ADMIN_OWNER_REF` 因此以字面量形式在两处各自定义，靠交叉引用注释保持同步，而非新增跨层依赖。
