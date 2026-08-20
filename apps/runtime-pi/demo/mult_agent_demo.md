# 双 Pi Agent 并行测试（multi_agent_demo）

> 回答一个问题：**xGovernor 能不能同时跑多个互不影响的 Pi agent？**
> 测试方法：一个 server、两个会话、背靠背提交两个 turn、并发订阅两条 SSE 流，
> 一个 agent 分析本仓库，另一个联网查 A 股大盘，各自独立执行到终态。
> 配套脚本：[`apps/runtime-pi/demo/multi_agent_demo.sh`](./multi_agent_demo.sh)。
> 前置知识：单会话端到端流程见 [`easydemo.md`](./easydemo.md)（本测试不重复它的 §0 前置条件与坑）。

## 测试的两个 agent

| | Agent A (repo) | Agent B (stock) |
|---|---|---|
| 任务 | 分析本 git 仓库的主要作用（只读） | 联网查询今日 A 股大盘涨跌 |
| `ext.runtime_pi.backend_id` | `local` | `e2b`（未配 `E2B_API_KEY` 时退化为 `local`） |
| workspace | `{kind:"local_path", path:<本仓库>}` | `{kind:"daemon_default"}` |
| 工具执行位置 | 宿主机仓库目录（桥接层转发） | E2B 远程沙箱 `/home/user/workspace`（空工作区，有公网） |
| isolation | `boundary=host, provider_is_sandbox=false` | `boundary=remote, provider_is_sandbox=true` |

为什么 Agent A 用 `local` 而不是 `e2b`：e2b 的 git 工作区在沙箱内做 `git clone`，
而 `clone_git_workspace` 只支持**公开 https URL**（无凭据传递，`validate_git_url_hygiene`
也拒绝内嵌凭据）。本仓库在 gitcode 上是私有的，公开 https 克隆不通；`local_path`
把宿主机上的仓库目录直接当作工作区，读分析照常进行。若仓库公开，把 A 的
`workspace` 换成 `{"kind":"git","url":"https://..."}` + `backend_id:"e2b"` 即可，
其余不变。

Agent B 的「网络搜索」：pi 0.84.2 内置工具只有 bash/ls/find/grep/read/write/edit
七个（全部被桥接扩展转发到沙箱），**没有内置 web 搜索工具**，所以 prompt 明确指示它
用 `bash + curl` 拉腾讯/东方财富行情接口——这在 E2B 沙箱内（默认有公网）执行。
拿不到数据时 agent 应如实说明，禁止编造。

## 独立性来自哪里

每个会话自打开起就是一条独立链路，互不共享状态：

```
客户端 (HTTP+SSE)
   ├─ 会话 A ── pi 子进程 A ── 桥 A(独立token) ── local 工作区（仓库目录）
   └─ 会话 B ── pi 子进程 B ── 桥 B(独立token) ── E2B 沙箱 B（远程）
```

- 每个会话一个独立的 `pi --mode rpc` 子进程（`PiRuntime.instances` 按 runtime_id 注册）；
- 每个会话一个独立的桥接层实例与 bearer token（`apps/runtime-pi/src/bridge.rs`）；
- 每个会话一个独立的沙箱实例（`InstanceManager` 按 owner 配额管理）；
- turn 级取消、终态、事件流都按 runtime_id/turn_id 隔离。

**「完全不互相影响」的准确边界**：执行与状态完全隔离；共享的只有外部资源——
LLM API 配额/限速（两个 agent 共用同一个 DeepSeek key）与 E2B 账号并发沙箱上限。
想证明并行，脚本记录两条流的时间线并计算「两 turn 同时在飞」的时长。

## 运行

```bash
# 前置：pi 已装；LLM provider/model/key 由 demo 的 session open 请求传入，E2B_API_KEY 可选但推荐
XGOVERNOR_DEMO_LLM_PROVIDER=openai XGOVERNOR_DEMO_LLM_MODEL=gpt-4.1-mini \
XGOVERNOR_DEMO_LLM_KEY=sk-... E2B_API_KEY=e2b_... bash apps/runtime-pi/demo/multi_agent_demo.sh

# 跑完保留 server 进程便于手工复现
bash apps/runtime-pi/demo/multi_agent_demo.sh --keep-server
```

脚本会：构建并起 server（admin :8787 / tenant :8788，token `demo-admin-token`，
数据与 HOME 隔离在临时目录）→ 背靠背打开两个会话 → 背靠背提交两个 turn →
并发订阅两条 SSE 流 → 等终态（默认 600s 上限）→ 汇总时间线、工具活动、最终输出 →
close 会话并（配置了 E2B key 时）核对 E2B 沙箱已销毁。

可调变量见脚本头部注释（`XGOVERNOR_REPO_DIR`、`XGOVERNOR_DEMO_TIMEOUT_S`、
`PROMPT_A`/`PROMPT_B` 等）。

## 预期结果与判读

- 两个 `open` 的 isolation 事实不同（A：host/本地目录；B：remote/E2B），证明同一
  server 上异构沙箱并存；
- 时间线上 `submit A < terminal B` 且 `submit B < terminal A` → 并行窗口 > 0；
- Agent A 的最终输出是一份仓库总结（README + crates/apps 结构）；
- Agent B 的最终输出包含以 curl 实拉数据为准的指数涨跌；若接口不可达，
  它应如实说明而不是编数字；
- close 后 E2B 平台上 running 沙箱数归零。

## 已知边界（沿用 easydemo.md §11）

- pi 子进程的宿主 cwd 锚定问题：脚本在仓库根目录起 server，使 Agent A 的
  local 工作区与 pi cwd 对齐；Agent B 的 e2b 路径不存在于宿主机，pi 会按已验证
  的行为自动降级用 bash（首条 `ls` 可能报 failed，属预期）。
- daemon 重启后旧会话的 pi 进程注册表清空，其远程沙箱需手工清理或等 E2B 过期。
- 两个 agent 都写 `~/.pi/agent/sessions`（各自的会话目录），HOME 必须是可写目录
  （脚本已隔离 HOME）。
