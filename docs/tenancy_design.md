# 多租户与工作区安全设计

> 性质：normative design。本文是多租户边界的目标态设计，尚未实现；实现时以此为验收基准。
> 规模假设：数十个租户、1000–2000 并发 agent、单写者控制面（见 architecture 评审结论：不做分布式控制面）。
> 关联：[protocol_boundaries.md](./protocol_boundaries.md)（协议边界）、[session_orchestration_skeleton.md](./session_orchestration_skeleton.md)（编排组件）、[http_api.md](./http_api.md)（wire 面）。

---

## 0. 场景公理（安全模型的出发点）

系统只存在两个信任世界，各自的信任锚不同：


|          | World A：管理员       | World B：租户                                    |
| -------- | ----------------- | --------------------------------------------- |
| 谁        | 本地部署的管理员          | 其余全部用户                                        |
| 信任锚      | **身份**（确认身份后百无禁忌） | **隔离边界**（不信任其代码与输入）                           |
| provider | 可用 local（宿主机执行）   | 仅沙箱 provider（e2b / 容器 / microVM），**严禁 local** |
| 工作区来源    | 任意（含宿主机路径）        | **仅 git 仓库**，在沙箱内物化                           |
| 安全依赖     | admin 凭证不泄露       | 沙箱边界不被穿透                                      |


由此得到全文唯一的承重不变式：

> **非 admin 会话：IsolationState.boundary ≠ Host，且 workspace 来源 ≠ LocalPath。**

这条公理把安全模型从"处处路径校验"压缩为"一处强制断言"——它不是实现细节，是决策，后文一切设计由它推导。

---



## 1. 总原则：身份是服务端事实

延续协议的"意图与事实分离"哲学：**wire 请求中永远不出现 tenant_id 字段**。客户端出示凭证（credential），租户身份是 daemon 从凭证解析出的事实。谁在请求体里"声称"自己属于哪个租户，谁就已经打穿了边界。

推论：session-protocol 对多租户**几乎零改动**——鉴权走 HTTP header，租户上下文在传输层解析后作为内部参数下传；协议 crate 唯一的变更是错误面新增配额码（§6）。多租户改造不动 wire 合同，这是分层正确性的验收标准之一。

---



## 2. 身份链条

四层，自外向内，各司其职：


| 层          | 是什么                     | 说明                                                                         |
| ---------- | ----------------------- | -------------------------------------------------------------------------- |
| credential | per-tenant bearer token | 替换现有全局共享单 token；每租户可配多个 token 以支持轮换                                        |
| principal  | 认证主体（谁在调用）              | 三类：human（租户用户）、service（渠道 ingress / cron，即现有 `daemon:` 前缀 principal）、admin |
| tenant     | 治理边界                    | 配额、计费、隔离、审计的挂载点；一个 tenant 有多个 principal                                    |
| owner_ref  | provider 层 opaque 业主    | 由装配层从 tenant **机械推导**（形如 `tenant/{tenant_id}`），彻底取代从客户端 ext 读取的过渡形态        |


owner_ref 推导落地后，`QuotaEnforcedLifecycle` 现成的按 owner 沙箱配额自动成为按租户配额，provider 层零改动——这是当初把 owner_ref 设计为 opaque 的回报。

> 2026-08 后续更新：`QuotaEnforcedLifecycle` 与同段提到的 `ProviderBoundRuntime`（binding.rs）已删除，责任收拢进独立 crate `crates/manager` 的 `xgovernor_manager::InstanceManager`——本节及下文引用的 `crates/backend/src/quota.rs`/`crates/backend/src/binding.rs` 路径均已不存在，按owner/租户配额的行为本身未变，只是搬了家。完整现状见 [protocol_boundaries.md](./protocol_boundaries.md) §2.3/§4 与 [session_orchestration_skeleton.md](./session_orchestration_skeleton.md) §3。本节以下保留原文，作为该决策发生时的真实记录，不再逐处修改路径。

---



## 3. 分层落位



### 3.1 传输层（apps/server）

- 认证中间件将 Authorization header 解析为 `SecurityContext { tenant_id, principal, role }`，失败 401。
- SecurityContext 以 axum extension 注入，handler 将其作为**独立参数**传给 SessionApplication 各方法——不合并进请求 DTO，保持"凭证证明的事实"与"客户端声明的意图"类型分居。
- **admin 凭证面收窄**：admin 面只绑 loopback / unix socket，不随租户 API 暴露在网络上。"百无禁忌"把全部安全重量压在身份确认上，因此网络上根本不应存在能打到宿主机的凭证。一行 bind 配置，消除一整类风险。



### 3.2 应用层（SessionApplication）

- `require_session(ctx, runtime_id)`：记录的 tenant 与 ctx 不符时返回 **NotFound 而非 Forbidden**——404 不泄露"存在但不属于你"的存在性信息，跨租户探测拿不到回声。此规则适用于所有路由，不允许个别 handler 写成 403。
- open 准入：租户级会话数上限、并发 turn 上限（TurnGate 之上加按租户计数）。
- **已落地**（§7 步骤 4 追加）：`owner_ref` 现为 `RuntimeStartRequest` 一等字段，`SecurityContext::owner_ref()` 机械推导，双运行时（local/e2b）的 ext 均已删除 `owner_ref` 键。



### 3.3 域模型层

- SessionRecord 增加 `tenant_id`、`created_by`（principal）两个字段。
- runtime_id 保持全局 UUID，不做租户前缀——隔离靠所有权校验，不靠命名规则。



### 3.4 协议层（唯一 wire 变更）

- SessionWireError 新增 `QuotaExceeded { scope, limit }` → HTTP 429。现有 Unavailable(503) 的语义是"服务端问题，稍后再试"；配额超限是"你的问题，别再试了"——混用会教坏客户端重试逻辑。枚举加值属非破坏演进。

---



## 4. 租户策略：一个文件，不是一个子系统

数十租户规模的正确形态是声明式配置（tenants.toml），每租户一节：


| 字段                                                  | 含义                                            |
| --------------------------------------------------- | --------------------------------------------- |
| tokens                                              | 凭证列表（支持轮换）                                    |
| role                                                | admin / tenant                                |
| max_sessions / max_concurrent_turns / max_sandboxes | 三级配额（max_sandboxes 喂给 provider 配额）            |
| allowed_workspace_kinds                             | World B 恒为 `["git"]`；admin 不限                 |
| git_host_allowlist                                  | 可选：租户可访问的 git host（企业租户通常只该访问自家 GitLab）       |
| capability_ceiling                                  | 能力天花板：requested_capabilities 先对天花板裁剪，再进现有能力门控 |
| min_isolation_boundary                              | World B 恒 ≥ Container；§0 不变式的策略表达             |
| allowed_runtimes                                    | 未来多 runtime 时按租户圈定                            |


启动加载 + SIGHUP 热重载。租户数上百、需要自助开通时再考虑数据库与管理 API——现在建即是过度工程。

---



## 5. 工作区与路径安全



### 5.1 场景公理如何消解路径圈禁问题

在 §0 公理下，原本需要四层的路径圈禁分析发生塌缩：


| 原方案层                                            | 命运                                                        |
| ----------------------------------------------- | --------------------------------------------------------- |
| BYO 路径校验管线（canonicalize、组件级前缀比对、标记文件、allowlist） | **整层删除**。租户没有提交宿主机路径的语法位；admin 不需要被校验（他有 shell，路径检查对他是仪式） |
| cap-std / openat2 句柄式操作面                        | **降级为可选加固**（防 admin 手滑，P3）。租户文件操作发生在沙箱内部，逃逸只污染沙箱          |
| 宿主机托管工作区树                                       | **被吸收**。托管工作区的物理位置移进沙箱内部——git clone 出的目录即工作区，宿主机上无对应物     |
| 隔离下限策略                                          | **升格为唯一承重墙**（§0 不变式）                                      |


安全性质从"每层都做对"变为"一处断言不被绕过"——更简单，也更坚固。

### 5.2 World B（租户）规则

1. **工作区 = git 仓库，在沙箱内物化。** clone 在**沙箱内**执行，不在宿主机——宿主机 clone 会把 git 攻击面（file:// 走私宿主机路径、指向内网的 SSRF、恶意仓库内容落盘）重新引回宿主机；沙箱内 clone 把这一切关在沙箱里。
2. **两段式 bootstrap。** 租户策略要求网络隔离时，时序为：引导期开网 clone → 收网 → 开始执行。此时序应是 provider 层的显式阶段，不是临时约定。
3. **git URL 卫生，三条纯函数检查**（在 normalizer 中，无文件系统交互、无 TOCTOU）：scheme 仅 https（file:// 是路径走私、ssh:// 引入密钥管理复杂度，均拒绝）；可选 per-tenant git host allowlist；submodule 默认关闭（submodule URL 是二次注入面）。
4. **私有仓库凭证**：租户 deploy token 注入沙箱环境完成 clone。复用 ResolvedLlmDescriptor 的类型模式——SessionRecord 记"凭证来源标识"，类型上无法表达 token 本体。
5. **出口即通道**：租户数据只有两条合法出路——git push 回去、经操作面 export 导出。数据流天然收窄为两个可审计的口。
6. **并发写被消解**：每会话 fresh clone，会话间无共享可变目录；并发写退化为 git 合并问题，git 本来就是为此而生。



### 5.3 World A（管理员）规则

- 确认身份后不做路径限制——对拥有宿主机 shell 的人做路径校验是安全剧场。
- 配套义务见 §3.1：admin 凭证面绑 loopback / unix socket。
- 可选加固（非安全边界）：操作面文件 API 改用 cap-std 受限句柄，防手滑而非防攻击。



### 5.4 强制点

全部规则收敛为 normalizer 中一个三元组校验：

```
match (role, workspace_spec, provider):
    (Admin,  _,        _      ) => 放行
    (Tenant, Git(url), sandbox) => URL 卫生检查后放行
    (Tenant, _,        _      ) => 拒绝（InvalidRequest / UnsupportedCapability）
```

外加 open 完成前的 fail-closed 断言：非 admin 会话的归一化结果必须满足 §0 不变式，不满足即拒绝。两处强制点均须有测试锁定。

### 5.5 磁盘配额

沙箱内工作区随沙箱生命周期释放，宿主机无长期占用。沙箱磁盘上限走 ProviderResourceLimits（协议已备 disk_mb 字段）。admin 场景的宿主机磁盘不设防——同 §5.3 逻辑。

---



## 6. 审计

每个控制面操作记一条：principal、tenant、操作、runtime_id、结果。租约表对 daemon principal 绕过已留审计日志，把该习惯推广到全部路径。usage 计量（token/成本按租户聚合）依赖事件聚合管线，属后续里程碑；审计日志不依赖任何前置，应随身份链条同批落地。

---



## 7. 落地顺序

1. **SecurityContext 链条**：token 表 → 中间件 → require_session 所有权校验（404 语义）——边界本体。
  **落地状态：已实现。** `crates/core/src/security.rs`（`SecurityContext`/`Role`，`owns()` 承载 admin 百无禁忌 / tenant 仅本租户）；
   `apps/server/src/httpserver/auth.rs`（`TokenTable::from_env()` 读 `XGOVERNOR_BEARER_TOKEN`(→admin，兼容旧部署) 与
   `XGOVERNOR_TENANT_TOKENS_JSON`(→ tenant 数组)，`security_layer` 中间件无条件挂载——未配置 token 表时注入隐式
   `SecurityContext::admin("unauthenticated-dev")`，today's "全开" dev 模式零回归）；`SessionApplication` 全部 8 个方法
   与 `require_session`/新增 `check_visible` 已按 `ctx.owns(record.tenant_id)` 强制 404 语义（`crates/core/src/application.rs`）；
   `open`/`fork` 落盘时按 ctx 戳 `tenant_id`/`created_by`；`apps/server/src/httpserver/session.rs` 全部 8 个 HTTP handler
   注入 `Extension<SecurityContext>`，`stream_turn_events` 在触碰 stream 表前先 `check_visible` 防止跨租户探测拿到不同回声。
   跨租户 HTTP 集成测试见 `session.rs` 测试模块（`a_foreign_tenant_gets_not_found_not_forbidden_on_someone_elses_session`
   等）。尚未落地：`tenants.toml` 完整策略文件（§4，token 表只是凭证→身份映射，非配额/workspace_kind 等策略）、
   §6 审计日志。（§3.1 admin 面绑 loopback 已随步骤2落地，见下。）
2. **场景公理落地**：normalizer 三元组校验 + fail-closed 断言 + 测试；admin 面绑 loopback。
  **落地状态：已实现。** 两处独立强制点：(a) `crates/core/src/application.rs` 新增 `enforce_workspace_axiom(ctx, workspace,  provider_is_sandbox)` 自由函数，落实 §5.4 三元组匹配（admin 放行 / tenant+sandbox+git(https，无嵌入凭证) 放行 / 其余拒绝），  
   由每个 `SessionEnvironmentNormalizer::normalize` 实现调用（`normalize` 签名新增 `ctx: &SecurityContext` 首参，全仓库 7 处实现  
   同步改签名）；(b) `SessionApplication::open` 在 `normalize()` 之后、`runtime.start` 之前插入独立的 fail-closed 断言——不复用  
   (a) 的逻辑，直接从 `normalized.isolation.boundary`/`request.workspace` 重新校验 §0 不变式（非 admin 会话不得落到 Host 边界  
   或 LocalPath 工作区），专门用一个"有 bug 的 normalizer"测试替身（`BuggyHostBoundaryEnvironment`）证明两处强制点互相独立、  
   任一失手另一处仍能拦下。admin 面绑 loopback 经历过两次迭代，现状是**强制双监听器、无开关**：`apps/server/src/main.rs`  
   的 `main()` 永远同时起两个 `axum::serve` 任务，没有任何"单监听器模式"分支——`XGOVERNOR_BIND_ADDR`（默认  
   `127.0.0.1:8787`）是 admin-only 面，无条件要求 loopback；`XGOVERNOR_TENANT_BIND_ADDR` 是 tenant-only 面，**必须显式配置**  
   （不设默认值，未设置直接 fail-closed 拒绝启动并说明原因——公网绑定地址不该有"悄悄生效"的默认值）；token 表  
   （`XGOVERNOR_BEARER_TOKEN`/`XGOVERNOR_TENANT_TOKENS_JSON`）同样**始终强制**（没配 = 每个请求隐式 admin，会被 tenant 面的  
   角色闸无差别拒绝，与其让它悄悄"全 403"不如启动时直接拒绝并说明原因）。三项检查（admin 地址必须 loopback、tenant 地址必须  
   已配置、token 表必须已配置）都在绑定任何端口之前跑完，任一不满足即 `exit(1)` 附清晰原因。两个监听器共享同一个  
   `SessionApplication`（`.clone()`，内部全是 `Arc`，廉价）——但各自有独立的 `SessionHttpState`，其 SSE `streams` 表**不跨监听器  
   共享**：admin 面暂时接不到 tenant 面刚提交的 turn 的实时流（记在 `main` 的文档注释里，未来若需要跨监听器 admin 实时订阅再  
   解决）。角色隔离在路由层再加一道闸——`apps/server/src/httpserver/auth.rs` 新增 `require_role(router, Role)` 中间件（必须包  
   在 `security_layer` **里面**，即先 `require_role` 后 `security_layer`，让 `security_layer` 作为外层先解析出  
   `Extension<SecurityContext>`，`require_role` 才有东西可读——顺序写反的失败模式是"全部请求都 403"，不是更危险的"校验被跳  
   过"，仍有专门测试锁定顺序）；`apps/server/src/httpserver/router.rs` 的 `create_router` 保留 `role_gate: Option<Role>` 参数  
   （`main.rs` 两处调用都传 `Some(Role::Admin)`/`Some(Role::Tenant)`；`None` 分支作为 `create_router` 自身契约的一部分继续被  
   测试覆盖，即便生产代码不再触达）。**这是第二次迭代**：第一版是"是否设置 `XGOVERNOR_TENANT_BIND_ADDR`"作为单/双监听器的开关  
   （`run_single_listener`/`run_dual_listener` 两条路径），用户认为开关本身就是不必要的复杂度，要求双监听器成为唯一路径——  
   `run_single_listener` 及其专用的 `TokenTable::has_admin_entry()` 已随之删除。尚未落地：非 https 传输的 git 卫生检查细节以外  
   的 workspace_spec 扩展、跨监听器 SSE 共享（如需要）。
3. **沙箱内 clone 与两段式 bootstrap**：与 e2b/容器 provider 落地天然并轨。
  **落地状态：窄切片已实现（仅沙箱内 clone，不含运行时断网）。** 新增 `apps/runtime-e2b`（crate 名 `xgovernor-runtime-e2b`）：`E2bMockRuntime`  
   复用 `crates/backend/src/binding.rs` 的 `ProviderBoundRuntime`（与 `apps/runtime-local` 的 `LocalMockRuntime` 完全相同的组合方式）包一层  
   `QuotaEnforcedLifecycle<E2bProvider>`；`GitSandboxWorkspaceEnvironment`（`SessionEnvironmentNormalizer` 实现）只接受  
   `WorkspaceSpec::Git`，复用既有的 `enforce_workspace_axiom(ctx, workspace, true)` 三元组闸门，产出 `IsolationBoundary::VirtualMachine`  
   + `NetworkIsolation::None`（诚实上报——本切片全程不断网，不敢谎报 `Restricted`/`Isolated`）。git url/reference/subdirectory 经  
   `WorkspaceFacts.metadata` 传递，不走 `ext`——这暴露了一处此前遗漏：`RuntimeStartRequest` 此前只带 `workspace_root: String`，  
   normalizer 产出的 `WorkspaceFacts.metadata` 在 `SessionApplication::open`/`fork` 里被悄悄丢弃。首次修补时图省事加了个独立的  
   `workspace_metadata: Value` 字段，与已有的 `workspace_root: String` 并列传递——用户当场指出这是偷懒：`root` 与 `metadata` 描述的是  
   同一个工作区，拆成两个独立字段会有脱节风险（例如未来某处只更新其中一个）。改为单一字段 `workspace: WorkspaceFacts`，把  
   normalizer 产出的 `WorkspaceFacts` 整体转发给 runtime adapter，root/metadata 天然不脱节（`ext` 仍然留给客户端自带的运行时专属  
   输入，`workspace` 是应用层自己校验过的工作区事实，两个通道故意分开，见该字段的文档注释）。`E2bMockRuntime::start` 创建沙箱后，  
   若 `request.workspace.metadata` 非空则反序列化并在沙箱内 `exec()` 跑 `git clone [--branch  
   <reference>] <url> <workspace_root>`（clone 目标复用 e2b 自身 envd 引导已创建好的 `/home/user/workspace` 空目录），clone 失败则回滚  
   （`stop_instance`）已创建的沙箱。**尚未落地**：两段式网络断开（e2b 沙箱创建后能否动态收网未经确认，本次范围显式排除，  
   `allow_internet_access` 只在创建时静态设一次）、私有仓库凭证注入（deploy token，见 §5.2 第 4 条）、subdirectory-scoped checkout  
   （`GitWorkspaceMetadata.subdirectory` 已解析但未消费）、把 `E2bMockRuntime`/`GitSandboxWorkspaceEnvironment` 接入 `apps/server` 的运行时路由（目前  
   `SessionApplication` 只持有一个固定的 `RuntimeAdapter`+`SessionEnvironmentNormalizer`，尚无按角色/租户分发的机制）。编译/测试：  
   `cargo build --workspace`/`cargo test --workspace` 全绿（新增用例含一个 `#[ignore]` 的真实 e2b 集成测试，需 `E2B_API_KEY` 与出网）。
4. **配额准入与 429**：租户三级配额 + QuotaExceeded 错误码。
  **落地状态：窄切片已实现（仅 `max_sessions` 一级）。** §4 原定三级配额（`max_sessions`/`max_concurrent_turns`/`max_sandboxes`）
   落地时用户当场收窄范围："active turn 限制没必要，只要活跃 session 数就行"——`max_concurrent_turns` 因此不实现（活跃 session 数
   已经界定了租户的总体footprint，再加一层turn并发计数只是为同一件事重复记账）；`max_sandboxes` 因另一个原因搁置：它天然要接
   `crates/backend/src/quota.rs` 既有的 `QuotaEnforcedLifecycle`（owner-scoped），但那要求 `owner_ref` 先从 `tenant_id` 机械派生
   （§2），今天 `owner_ref` 仍读客户端自带的 `ext`——用客户端自报的身份去撑一个租户级配额，等于让 §1"wire 请求不带 tenant_id"的
   边界白设，所以留给 `owner_ref` 派生工作，不在此处伪造。

   **owner_ref 派生已于同日后续追加落地**（用户指示"owner_ref 改为装配层从 SecurityContext 机械推导，从
   RuntimeStartRequest 一等字段下传，ext 里的这条路径直接删除……不留开关、不留兼容路径"）：`SecurityContext::owner_ref()`
   （`crates/core/src/security.rs`）为 tenant 推导 `tenant/{tenant_id}`，为 admin 返回固定哨兵 `ADMIN_OWNER_REF`
   （`"admin"`，用户明确要求"admin 给个哨兵值，然后对 admin 百无禁忌，不需要 max 限制"）；`RuntimeStartRequest` 新增一等
   字段 `owner_ref`，`open_impl`/`fork_impl` 用 `ctx.owner_ref()` 填充；`apps/runtime-local`/`apps/runtime-e2b` 的 ext 结构体
   （`LocalRuntimeExt`/`E2bRuntimeExt`）删掉 `owner_ref` 字段，只保留 `backend_id`——旧的客户端 ext 路径整条删除，无双路径。
   `QuotaEnforcedLifecycle::reserve`（`crates/backend/src/quota.rs`）对 `owner_ref == "admin"` 直接跳过 `max_per_owner`
   拒绝分支（仍计数，只是不拦截）。**至此，本节开头说的"`max_sandboxes` 天然要接 QuotaEnforcedLifecycle，但那要求
   owner_ref 先机械派生"这个前置条件已经满足**——line 56 预告的"owner_ref 推导落地后，QuotaEnforcedLifecycle 现成的按
   owner 沙箱配额自动成为按租户配额，provider 层零改动"现在成立：`apps/runtime-local`/`apps/runtime-e2b` 已有的
   `DEFAULT_MAX_SANDBOXES_PER_OWNER`（当前硬编码 4）现在就是按租户生效的并发沙箱上限，而不是按可伪造的客户端字符串生效。
   **仍未做的**：`max_sandboxes` 尚未做成可配置的租户级策略数字（即没有从 `TenantTokenEntry` 或某个 tenants 策略表读取
   每租户不同的上限，仍是编译期常量 4，对所有租户一视同仁）——如果要做到"按租户配置不同并发沙箱数"，还需要把这个常量
   替换成从 `SecurityContext`/token 表读出的每租户值，这是本次范围之外的下一步。跨 crate 依赖问题：`crates/backend` 与
   `xgovernor-core` 互不依赖，`ADMIN_OWNER_REF` 因此以字面量形式在两处各自定义（`crates/core/src/security.rs` +
   `crates/backend/src/quota.rs`），靠交叉引用注释保持同步，而非新增依赖边——`apps/runtime-e2b` 早先给
   `E2B_WORKSPACE_ROOT` 开的同样先例。`cargo check --workspace --tests` 全绿；逐 crate `cargo test`：`xgovernor-core`
   58/58、`backend` 88/88（含新增 `admin_owner_ref_is_exempt_from_the_cap`）、`xgovernor-runtime-local` 2/2、
   `xgovernor-runtime-e2b` 6/6（1 ignored）、`xgovernor-server` 26/26。

   `TenantQuota { max_sessions: Option<u32> }`
   （`crates/core/src/security.rs`）挂在 `SecurityContext.quota` 上，`None`（默认）= 不设上限，与既有 `admin(..)`/`tenant(..)`
   构造点零回归；`apps/server/src/httpserver/auth.rs` 的 `TenantTokenEntry` 加一个可选 `max_sessions` 字段，走既有的
   `XGOVERNOR_TENANT_TOKENS_JSON` token 表通道，不另开 `tenants.toml`（§4 完整策略文件仍未落地，这里只是把配额数字塞进已有的
   凭证条目）。准入机制是"先占后确认"：`SessionApplication` 新增 `reserve_tenant_session`/`release_tenant_session`
   （内部状态 `tenant_sessions: Arc<Mutex<HashMap<String, usize>>>`），check-then-increment 在同一次加锁内完成，防止同租户两个
   并发 `open` 都从"还差一个"这个读数溜过去；`open_impl`/`fork_impl` 在真正建 runtime 之前调用 `reserve_tenant_session`，超限
   直接返回 `SessionDomainError::QuotaExceeded { scope, limit }`（不触碰 runtime/repository）；`close_impl` 释放槽位。
   `QuotaExceeded` 早已是 `SessionDomainError`/`SessionWireError` 的既有变体（`crates/session-protocol/src/error.rs:70`
   `Self::QuotaExceeded { .. } => 429`），本步骤只是第一次真正产出它，映射链路本身无需新增代码。admin 会话永远不占用租户配额
   （`SecurityContext::admin` 的 `tenant_id()` 恒为 `None`，`reserve_tenant_session` 只按 `tenant_id` 记账）。测试：
   `fork_counts_against_the_same_tenant_session_quota` 等锁定"open 占额、close 放额、fork 与 open 共享同一计数"。
5. **审计日志**。
  **落地状态：已实现。** `crates/core/src/application.rs` 新增 `audit_log<T>(ctx, operation, runtime_id, result)` 自由函数：
   每次调用发一条 `tracing::info!(target: "audit", principal, tenant, operation, runtime_id, result = "ok"/"error", [error])`——
   选 `tracing` 现有事件流而非自建持久化子系统，因为 §6 只要求"这些事实可查询"，不要求这个 crate 自己管存储/轮转/查询；
   谁运营这个daemon，就用运营 `tracing` 输出的老办法（文件/syslog/OTel...）挂订阅者到 `"audit"` 这个 target 上。8 个
   `SessionApplication` 公开方法（`open`/`submit_turn`/`answer_interaction`/`close`/`detach`/`heartbeat`/`cancel`/`fork`）
   全部改造成薄壳 `pub async fn` 包一层同名 `*_impl`，壳里算完结果后调用 `audit_log`——`check_visible` 是唯一例外（只读所有权
   探针，供 transport 层内部用，本身不算一次控制面操作）。`session_lease.rs` 里此前留着一句"daemon principal 绕过会被记审计"
   的文档承诺，核实后是空头支票：`is_daemon_principal`/`daemon_*_principal` 一族从未被本 crate 任何调用点触达，其文档描述的
   router+`SessionActor` 架构在这个 crate 里根本不存在（今天的租约检查是 `SessionApplication::check_lease_holder`）；文档已
   改写为如实描述现状（未接线的遗留代码），并把"若真要接，绕过本身也该照 `audit_log` 的 `target = "audit"` 惯例发一条事件"
   记在文档里，而不是继续留一句假的"已记录"。测试用了一个手搓的最小 `tracing::Subscriber`（本仓库没有
   `tracing-subscriber`/`tracing-mock` 依赖，为三个断言引入一整个依赖不划算）——第一版按每个测试各自
   `tracing::subscriber::set_default`（线程局部 guard）写，编译能过、单跑也能过，但循环跑 5 次会挂 3 次：`tracing-core` 的
   per-callsite `Interest` 缓存和配套的全局 max-level 提示是进程级共享状态，由"谁恰好在这一刻构造了一个 `Dispatch`"的那个
   线程负责重算，而重算逻辑在"进程里目前只注册过一个 dispatcher"这个阶段会读**当前线程**的默认值——不同测试各自
   `set_default`/guard 释放的时序交错，会让某个测试的 `Dispatch::new` 内部触发的重算读到一个尚未安装完成的默认值，从而把
   全局 max-level 提示瞬间打低，殃及另一个正在并发跑的测试的事件。改为进程级单例：behind 一个 `OnceLock`，全程只
   `set_global_default` 一次（不是线程局部的 `set_default`），事件按捕获线程的 `ThreadId` 分桶（`cargo test` 每个测试独立
   OS 线程，`#[tokio::test]` 默认 current-thread runtime 不会把一个 await 跨线程迁移），彻底消掉"多次注册互相竞争"的根因——
   循环跑 30 次（10+20 两轮）全绿后才认定修好，单跑一次绿不算数（之前那版"看似修好"的单跑同样是假阳性）。

五步均不依赖持久化——内存态同样成立；持久化落地时 tenant_id 已在记录中。