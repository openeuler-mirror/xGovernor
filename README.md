# xGovernor

[English](./README.md) | [中文](./README.zh-CN.md)

A session control plane ("governor") for heterogeneous AI agent runtimes.

[License](./License)
[Rust](https://www.rust-lang.org/)
[Version]()

## What is xGovernor?

xGovernor is a server-side control plane that opens, governs, and observes **agent sessions**, while delegating the actual thinking to pluggable **agent runtimes** (pi, xiaoO, opencode, ...). It deliberately does **not** implement an LLM decision loop of its own — its product scope is managing other agent runtimes through their own SDKs/APIs, and giving all of them the same lifecycle, isolation, and wire surface:

- **One session API for every runtime.** Clients speak a single HTTP + SSE contract. Runtime differences surface as *capability* differences and namespaced *extension* payloads — never as API-shape differences.
- **Sandbox lifecycle as a contract.** Execution environments (local directory, remote E2B sandbox, container, ...) are created, loaded, paused, and deleted through a provider SPI governed by a pure, executable lifecycle state machine.
- **Governance built in.** Single-writer session leases with heartbeats, orphan reaping for crashed clients, per-owner sandbox quotas, capability gating with graceful degradation, and credential hygiene enforced by types (a persisted LLM descriptor *cannot represent* an API key).

## Architecture

```
          Client (TUI / CLI / channel)
              │  HTTP + SSE                 ← session-protocol (wire contract)
              ▼
   ┌───────────────────────────────────────────┐
   │  xgovernor-server                          │  transport adapter (axum)
   │  admin 127.0.0.1:8787 / tenant :8788      │
   │  SessionApplication                       │  ← crates/core: domain records,
   │  · environment normalization              │     capability gates, projections,
   │  · lease table / orphan reaper            │     lease & reaper
   └───────┬───────────────────────────┬───────┘
           │ shared worker NDJSON        │ provider SPI
           ▼                             ▼
   pi-worker → pi --mode rpc      InstanceManager (local / e2b)
   + TS extension (tools)         ← crates/manager: per-owner quota,
           │  tool calls, local HTTP               global ceiling, idempotent
           │  bridge (per-session token)          create, compensating delete,
           ▼                                      pending-release retry
   ┌──────────── Bridge ────────────┐             startup reconcile
   │  operation-protocol forwarding │
   └──────────────┬─────────────────┘
                  ▼
         sandbox (host dir / E2B remote VM)
```

How a real turn flows today (verified end-to-end on 2026-08 with pi 0.84.2 + DeepSeek + E2B): the client opens a session via the session API; the server spawns a per-session `pi-worker` and talks to it through the same `agent-runtime-protocol` NDJSON envelope used by xiaoO. The worker owns the nested `pi --mode rpc` process and translates Pi's native JSON-RPC events. Pi loads the TypeScript extension in `apps/runtime-pi/extension`, which routes its seven built-in tools (read/write/edit/bash/ls/find/grep) over a local HTTP bridge into the sandbox selected by `ext.runtime_pi.backend_id` — a host directory (`local`) or a remote E2B VM (`e2b`). Worker and Pi control processes stay on the daemon host; only tool *execution* crosses into the sandbox. Multiple sessions remain independent: separate workers, Pi processes, bridge tokens, and sandboxes.

The system is organized around explicit boundaries, each owned by a contract crate:

| Crate / App | Contract |
|---|---|
| `crates/session-protocol` | Client ↔ daemon HTTP/SSE wire contract. Runtime-neutral core vocabulary (open / turn / events / interaction / operation / errors) plus namespaced `ext` bags for runtime-specific payloads. |
| `crates/provider-protocol` | Manager ↔ sandbox-provider lifecycle contract. Opaque `owner_ref` instead of business identity, neutral lifecycle reasons, and a pure state machine as the executable spec. |
| `crates/operation-protocol` | The operation plane (exec / filesystem / search) available once a provider instance is attached — the in-process counterpart to `provider-protocol`. |
| `crates/core` | Domain + application layer: `SessionApplication`, session records with **opaque runtime state**, multi-runtime registration through `AgentRuntime`, lease table, orphan reaper, admission gates, wire projections. |
| `crates/backend` | Provider implementations used by `InstanceManager`: local directory sandbox, E2B remote sandbox, SQLite provider-instance ledger. |
| `crates/manager` | `InstanceManager`: unified provider-instance orchestration — per-owner quota + global ceiling, per-runtime_id create idempotency, attach-failure compensating delete, delete-failure pending-release retry queue, create-path semaphore admission with backoff, startup reconcile against the ledger. |
| `apps/runtime-mock` | A mock `AgentRuntime` over **real** local and E2B providers (dispatched by `ext.runtime_mock.backend_id`) — proves the full plumbing, including in-sandbox `git clone` for git workspaces, without pretending to be an LLM. |
| `apps/runtime-pi` | The **real** Pi runtime and worker: the runtime supervises a per-session worker over shared NDJSON; the worker owns Pi's native RPC process; tools route through the bridge into `local` / `e2b`. |
| `apps/runtime-xiaoo` | The xiaoO `AgentRuntime` and worker: xiaoO tools execute through the same host-injected operation backend and Local/E2B provider boundary. |
| `apps/server` | The runnable daemon (`xgovernor-server`): two listeners (admin loopback + tenant), HTTP/SSE transport, SQLite session repository, assembly. |

Design rules that hold everywhere: the wire core stays runtime-neutral (any single runtime's concept lives in `ext`); runtime internal state is quarantined as an opaque, versioned blob; capabilities come in two families (sandbox vs runtime) and gate requests before they reach a runtime; every error leaves through one projected wire vocabulary; on the tenant surface, sessions are only admitted with a git workspace (https) and a sandboxed provider — fail-closed.

The full normative specification lives in [docs/protocol_boundaries.md](./docs/protocol_boundaries.md).

## Status

Honest edition: the control plane **closed loop is proven against the real thing** — HTTP `open` → `turn` → a real pi subprocess → tool execution in a real E2B sandbox → normalized SSE events, verified end-to-end three times (2026-08-15 single-session walkthrough in [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md); 2026-08-17 two independent pi agents running in parallel in [apps/runtime-pi/demo/mult_agent_demo.md](./apps/runtime-pi/demo/mult_agent_demo.md); 2026-08-17 `kill -9` restart + lazy session restoration on the same `runtime_id`, same document §10.5). The workspace carries 250+ tests, all green, including dependency-policy and boundary-vocabulary guards on the protocol crates.

What ships today:

- **Real Pi runtime** (`apps/runtime-pi`) — per-session worker + nested `pi --mode rpc` + bridge extension; tool execution lands in a real sandbox, never the daemon host filesystem.
- **Two sandbox providers** — `local` (host directory) and `e2b` (remote VM, optional via `E2B_API_KEY`), behind the same provider SPI and quota plumbing; `InstanceManager` gives per-owner quota (default 20), a global ceiling (default 1024), create idempotency, compensating deletes, and a pending-release retry queue.
- **Durable state** — SQLite session repository + provider-instance ledger in one WAL-mode file (`~/.xgovernor/xgovernor.db`, override with `XGOVERNOR_DATA_DIR`); startup reconcile re-attaches to surviving sandboxes after a restart, and pi sessions get **lazy restoration**: the per-session state needed to re-spawn `pi` is persisted into the opaque `SessionRecord.runtime` slot at open, so after a daemon restart the same `runtime_id` transparently resumes its conversation (verified end-to-end with `kill -9` on 2026-08-17, see the demo's §10.5).
- **Two listening surfaces** — loopback-only admin (`XGOVERNOR_BIND_ADDR`) and tenant (`XGOVERNOR_TENANT_BIND_ADDR`); both require tokens at startup; tenant sessions are admitted only as git-workspace + sandboxed-provider (`e2b`), with https-only URL hygiene.
- **Lifecycle & defense** — single-writer leases with heartbeats, orphan reaper for dead clients, real turn cancellation, graceful shutdown with a forced-exit deadline, request timeouts / body limits / concurrency ceilings / SSE stream TTL on the transport.

Honest gaps (see Roadmap): the operation plane is not yet exposed as HTTP routes; the ingress adapters (Feishu / Telegram / cron / MCP) are legacy in-tree code pending migration; opencode is not attached; the e2b git workspace only supports **public https URLs** (no credential injection, by design — private repos cannot be cloned).

## Requirements

| Component | Version | Notes |
|---|---|---|
| Rust toolchain | **≥ 1.74** (edition 2021) | developed and verified with rustc/cargo **1.94.0**; non-test code needs ≥ 1.70 (`Option::is_some_and`), the test suite additionally uses `std::io::Error::other` (1.74+) |
| pi | **≥ 0.84.2** | `npm install -g @earendil-works/pi-coding-agent`; the bridge extension (`apps/runtime-pi/extension`) declares `peerDependencies: ^0.84.2` and type-checks against exactly 0.84.2; the full pipeline is verified end-to-end on pi **0.84.2** |
| Node.js | **≥ 22.19.0** | only needed to `npm install` / typecheck the TS bridge extension; the `pi` binary itself (Bun-compiled) loads and runs the extension |
| E2B | no SDK dependency | the e2b provider is a self-contained reqwest client against the E2B REST API (`api.e2b.dev` + the sandbox's envd HTTP); requires `E2B_API_KEY` at runtime |
| LLM | any provider/model pi supports | selected per session through `llm`; the daemon does not require a fixed LLM key at startup |

Key Rust dependencies (workspace-pinned): tokio ≥ 1.35, axum 0.7, rusqlite 0.32 (bundled), reqwest 0.12 (rustls), serde/serde_json 1, uuid 1.6, tracing 0.1.

## Quick start

```bash
cargo run -p xgovernor-server
```

| Environment variable | Default | Meaning |
|---|---|---|
| `XGOVERNOR_BIND_ADDR` | `127.0.0.1:8787` | Admin listener (must be loopback) |
| `XGOVERNOR_TENANT_BIND_ADDR` | *(required, no default)* | Tenant listener (may be public) |
| `XGOVERNOR_TENANTS_CONFIG_PATH` | `$XGOVERNOR_DATA_DIR/tenants.toml` | Declarative token/identity policy file (see below); missing at the *default* path means dev mode (every request resolves to implicit admin), missing at an explicitly-set path is a fail-closed startup error |
| `XGOVERNOR_DATA_DIR` | `~/.xgovernor` | Directory holding the SQLite database (and, by default, `tenants.toml`) |
| `XGOVERNOR_DEFAULT_WORKSPACE_ROOT` | OS temp dir | Workspace root for `workspace: daemon_default` |
| `XGOVERNOR_LEASE_STALE_SECS` | `45` | Heartbeat staleness window for lease takeover/expiry |
| `XGOVERNOR_ORPHAN_THRESHOLD_SECS` | `1800` | No-heartbeat duration before the orphan reaper force-closes a session |
| `XGOVERNOR_ORPHAN_REAPER_INTERVAL_SECS` | `600` | Orphan reaper polling interval |
| `XGOVERNOR_RECLAIM_SWEEP_INTERVAL_SECS` | `300` | Provider sandbox liveness sweep interval |
| `E2B_API_KEY` | *(unset)* | If set, the `e2b` backend is registered (otherwise local-only) |
| `XGOVERNOR_E2B_TIMEOUT_SECS` | `3600` | Default E2B sandbox timeout; explicit provider `timeout_secs` overrides it |
| `XGOVERNOR_E2B_ACTIVITY_REFRESH_SECS` | `60` | Minimum spacing between E2B timeout refresh requests |
| `E2B_API_URL` | `https://api.e2b.app` | E2B control-plane URL; set this for self-hosted E2B |
| `E2B_DOMAIN` | `e2b.app` | Sandbox hostname suffix, without a URL scheme; a port is allowed |
| `E2B_ENVD_SCHEME` | `https` | Sandbox envd URL scheme (`http` or `https`) |

Credentials and roles live in a `tenants.toml` file (`docs/tenancy_design.md` §4), loaded at startup and hot-reloaded on `SIGHUP` — no restart needed to rotate tokens or add a tenant:

```toml
[admin]
tokens = ["demo-admin-token"]

[[tenant]]
tenant_id = "demo-tenant"
tokens = ["demo-tenant-token"]
# principal, max_sessions, max_requests_per_minute are all optional
```

Minimal working configuration and a full single-session walkthrough are in [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md) (§2–§9). The one-line shape of it:

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
export E2B_API_KEY=e2b_...
cargo run -p xgovernor-server
```

Open a pi session (every pi session must declare `ext.runtime_pi.backend_id` — `local` or `e2b`):

```bash
curl -s localhost:8787/api/v1/sessions/open -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "conversation_id": "demo",
  "sender_id": "me",
  "workspace": { "kind": "daemon_default" },
  "llm": { "provider": "openai", "model": "gpt-4.1-mini", "api_key": "sk-..." },
  "ext": { "runtime_pi": { "backend_id": "e2b" } }
}'
# → SessionOpenResponse: runtime_id, workspace/isolation facts (boundary=remote for e2b), capabilities
```

Submit a turn and stream its events:

```bash
curl -s localhost:8787/api/v1/sessions/turns -H 'content-type: application/json' \
  -H 'Authorization: Bearer demo-admin-token' -d '{
  "runtime_id": "<runtime_id>",
  "text": "查看当前目录"
}'
# → { "runtime_id": "...", "turn_id": "...", "accepted_kind": "turn" }

curl -N localhost:8787/api/v1/sessions/<runtime_id>/turns/<turn_id>/events \
  -H 'Authorization: Bearer demo-admin-token'
# → SSE: output_delta, tool_activity (begin/end) ... turn_completed | turn_failed
```

To see two pi agents running in parallel (repo analysis + live web lookup, independent sandboxes), run [apps/runtime-pi/demo/multi_agent_demo.sh](./apps/runtime-pi/demo/multi_agent_demo.sh).

### API surface

| Route | Method | Purpose |
|---|---|---|
| `/api/v1/health` | GET | Liveness |
| `/api/v1/sessions/open` | POST | Open (idempotent re-attach with `runtime_id`) |
| `/api/v1/sessions/turns` | POST | Submit a turn → receipt with server-assigned `turn_id` |
| `/api/v1/sessions/{runtime_id}/turns/{turn_id}/events` | GET | SSE event stream for one turn |
| `/api/v1/sessions/interactions` | POST | Answer a runtime-initiated interaction |
| `/api/v1/sessions/cancel` | POST | Cancel the active (or a specific) turn |
| `/api/v1/sessions/fork` | POST | Fork a session (capability-gated) |
| `/api/v1/sessions/heartbeat` | POST | Keep a lease alive |
| `/api/v1/sessions/detach` | POST | Release the lease, keep the session |
| `/api/v1/sessions/close` | POST | Close the session (destroys the sandbox) |

## Roadmap

- **Operation-plane HTTP routes** — exec / file read-write / checkpoint-checkout exposed over HTTP instead of only through the in-process SPI.
- **In-flight turn resumption across restarts** — the sandbox ledger and completed turns already survive a restart (lazy restoration, shipped 2026-08-17); a turn that was mid-flight when the daemon died is still dropped by design, and resuming it is the remaining piece.
- **Controlled credentials for e2b git workspaces** — clone private repos without embedding credentials in URLs (must pass the existing hygiene gate).
- **External auth → `owner_ref`** — derive tenant identity from an authenticated principal; today the token table decides the role.
- **Ingress adapters** — channels (Feishu / Telegram), cron triggers, and an MCP surface, rebuilt as thin adapters on top of the session API (legacy implementations exist in-tree and are pending migration).
- **More runtimes** — opencode and additional runtimes via `AgentRuntime` and the normalized event model.

## Development

```bash
cargo test --workspace
```

The protocol crates are self-guarding: a dependency-policy test pins `session-protocol`'s dependency closure to `serde`/`serde_json`/`thiserror`, and boundary-vocabulary tests fail the build if business identity leaks into the provider contract or implementation vocabulary leaks into the wire contract. Treat any diff in protocol-crate JSON shapes as a wire change and review it as such. `apps/runtime-pi`'s contract tests drive a fake `pi` binary against the real adapter; the live demos additionally require `pi` on `PATH`, an LLM key, and optionally `E2B_API_KEY`.

## Documentation

| Document | What it covers |
|---|---|
| [docs/protocol_boundaries.md](./docs/protocol_boundaries.md) | Current protocol scopes, Application/provider/runtime responsibilities, state and capability rules |
| [docs/session_orchestration_skeleton.md](./docs/session_orchestration_skeleton.md) | How the shipped orchestration works: minimal closed loop, lease table, orphan reaper (Components A/B/C) |
| [docs/http_api.md](./docs/http_api.md) | Wire reference: every route, request/response shape, SSE event vocabulary, error codes |
| [docs/agent_runtime_guide.md](./docs/agent_runtime_guide.md) | How to attach a new agent runtime: protocol obligations, worker RPC, state, capabilities, checklist |
| [docs/tenancy_design.md](./docs/tenancy_design.md) | Multi-tenancy design: identity chain, the admin/tenant trust axiom, git-only sandboxed workspaces, quotas |
| [apps/runtime-pi/demo/easydemo.md](./apps/runtime-pi/demo/easydemo.md) | Single-session end-to-end demo: real `pi --mode rpc` + DeepSeek + E2B, verified run log, `kill -9` restart + lazy restoration (§10.5), known pitfalls |
| [apps/runtime-pi/demo/mult_agent_demo.md](./apps/runtime-pi/demo/mult_agent_demo.md) | Two-agent parallel demo: independent pi sessions (repo analysis + live web lookup), isolation model, how to read the results |

## License

[MulanPSL-2.0](./License)
