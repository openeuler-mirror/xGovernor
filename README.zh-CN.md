# xGovernor

[English](./README.md) | [中文](./README.zh-CN.md)

xGovernor是一款面向生产、易于使用的多Agent控制面，提供兼容不同异构Agent Runtime 和热插拔各类沙箱后端、插件的灵活能力。

[License](./License) · [Rust](https://www.rust-lang.org/) · [Version]()

## 安装

**1. Rust 工具链**（编译 server 用，最低 1.74）

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**2. pi（agent runtime，必装）**

```bash
npm install -g @earendil-works/pi-coding-agent
pi --version
```

桥接扩展的依赖（Node.js >= 22.19.0）：

```bash
cd apps/runtime-pi/extension && npm install
```

**3. LLM 配置**在创建 session 时传入；server 启动不需要绑定 DeepSeek 或其他 LLM key。

可选：`E2B_API_KEY=e2b_...` 启用远程 E2B 沙箱后端；不设则只有本机 `local` 沙箱。

## 启动

`xgovernor-server` 启动时根据权限能力拆分为两套监听端口，启动时必须配置环境 token：


| 环境变量                               | 默认值        | 含义                  |
| ---------------------------------- | ---------- | ------------------- |
| `XGOVERNOR_TENANT_BIND_ADDR`       | *(必填，无默认)* | tenant 面监听地址        |
| `XGOVERNOR_DEFAULT_WORKSPACE_ROOT` | *(必填，无默认)* | 会话工作区根目录            |
| `E2B_API_KEY`                      | *(未设置)*    | 设置后注册 `e2b` 远程沙箱后端  |


凭证与角色配置在 `tenants.toml` 文件里，默认位置~/.xgovernor/tenants.toml，启动时加载，支持 `SIGHUP` 热重载——轮换 token 或新增租户不需要重启：

```toml
[admin]
tokens = ["demo-admin-token"]   # admin token，可配多个用于轮换

[[tenant]]
tenant_id = "demo-tenant"        # 必填，全局唯一
tokens = ["demo-tenant-token"]   # 必填，至少一个；全局唯一
principal = "demo-ops"           # 可选，默认 "tenant"
max_sessions = 20                # 可选，默认不限
max_requests_per_minute = 120    # 可选，默认不限
```

最小启动配置：

```bash
export XGOVERNOR_TENANT_BIND_ADDR=127.0.0.1:8788
mkdir -p "${XGOVERNOR_DATA_DIR:-$HOME/.xgovernor}"
cat > "${XGOVERNOR_DATA_DIR:-$HOME/.xgovernor}/tenants.toml" <<'EOF'
[admin]
tokens = ["demo-admin-token"]

[[tenant]]
tenant_id = "demo-tenant"
tokens = ["demo-tenant-token"]
EOF
cargo run -p xgovernor-server
```



## 最小跑通一个 case



### admin 面（管理端）

以下请求打 admin 监听地址（`127.0.0.1:8787`），用 `tenants.toml` 里 `[admin]` 节配置的 admin token。

**1. 打开一个 pi 会话**

`ext.runtime_pi.backend_id` 必填：`local`（本机沙箱，无额外依赖）或 `e2b`（远程沙箱，需配置 `E2B_API_KEY`）。

```bash
curl -s localhost:8787/api/v1/sessions/open -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "conversation_id": "demo",
  "sender_id": "me",
  "workspace": { "kind": "daemon_default" },
  "llm": {
    "provider": "openai",
    "model": "gpt-4.1-mini",
    "api_key": "sk-..."
  },
  "ext": { "runtime_pi": { "backend_id": "local" } }
}'
# 记下响应里的 runtime_id
```

**2. 提交一个 turn**

```bash
curl -s localhost:8787/api/v1/sessions/turns -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "runtime_id": "<runtime_id>",
  "text": "查看当前目录"
}'
# → { "runtime_id": "...", "turn_id": "...", "accepted_kind": "turn" }
```

**3. 订阅事件流**（SSE，直到收到 `turn_completed` / `turn_failed` 结束）

```bash
curl -N localhost:8787/api/v1/sessions/<runtime_id>/turns/<turn_id>/events \
  -H 'Authorization: Bearer demo-admin-token'
```

看到 `turn_completed` 即跑通。更完整的走查（e2b 远程沙箱、多 agent 并行、`kill -9` 重启复原）见 [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md) 和 [apps/runtime-pi/demo/mult_agent_demo.md](./apps/runtime-pi/demo/mult_agent_demo.md)。

### tenant 面（普通租户用自己的 token）

普通租户请求打 **tenant 监听地址**（`127.0.0.1:8788`），用的是 `tenants.toml` 里**该租户自己的 token**（`demo-tenant-token`），不是 admin token。

租户会话有准入约束，与 admin 不同：

- 工作区必须是 **git 仓库**（`kind: "git"`，仅 https URL，且 URL 不能内嵌用户名密码）；`daemon_default` / `local` 在 tenant 面会被拒。注意目前只支持**公开** https 仓库——沙箱不注入任何凭据，私有仓库无法 clone。
- provider 必须是**沙箱化**的，即 `backend_id: "e2b"`（需配置 `E2B_API_KEY`）；`local` 不是沙箱，tenant 面不可用。
- 同一套 API 路径，只换监听地址和 token。

```bash
curl -s localhost:8788/api/v1/sessions/open -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-tenant-token' -d '{
  "conversation_id": "demo",
  "sender_id": "me",
  "workspace": { "kind": "git", "url": "https://github.com/example/repo.git" },
  "llm": { "provider": "openai", "model": "gpt-4.1-mini", "api_key": "sk-..." },
  "ext": { "runtime_pi": { "backend_id": "e2b" } }
}'
# 记下响应里的 runtime_id；沙箱内会先 git clone 该仓库再作为工作区
```

提交 turn 与订阅事件流和 admin 面完全一样（`POST /api/v1/sessions/turns`、`GET /api/v1/sessions/<runtime_id>/turns/<turn_id>/events`），只把监听地址换成 `8788`、token 换成该租户自己的。每个租户的 token 对应一个 `tenant_id`，会话按租户隔离（配额、访问互不可见）。

### 新增一个租户

没有运行时管理 API——新增租户就是改配置文件 + 热重载：

1. **编辑** `tenants.toml`（默认 `~/.xgovernor/tenants.toml`），加一个 `[[tenant]]` 块：
  ```toml
   [[tenant]]
   tenant_id = "acme"                        # 必填，不能与其他租户重复
   tokens = ["acme-token-1", "acme-token-2"] # 必填，至少一个；全局唯一（不能与任何 admin token 重复）
   principal = "acme-ops"                    # 可选，默认 "tenant"
   max_sessions = 20                         # 可选，默认不限
   max_requests_per_minute = 120             # 可选，默认不限
  ```
   一个租户可以配多个 token（轮换用）；同一个 token 只能属于一个身份。
2. **热重载，无需重启**：
  ```bash
   kill -HUP <xgovernor-server pid>
  ```
   服务重读文件并整体替换 token 表，两个监听面同时生效。重载失败（TOML 解析错误、token / tenant_id 重复、空 tokens 列表）时**保留旧配置**并打错误日志；Windows 或 dev 模式启动的服务没有热重载，需重启。
3. **验证新租户**：
  ```bash
   curl -s localhost:8788/api/v1/health -H 'Authorization: Bearer acme-token-1'
   # → 200 生效；401 说明 token 未被识别
  ```

删除租户 = 删掉对应的 `[[tenant]]` 块再热重载（注意整体替换语义：确认新文件完整再触发重载）。

## 开发

```bash
cargo test --workspace
```



## 许可证

[MulanPSL-2.0](./License)
