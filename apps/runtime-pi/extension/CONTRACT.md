# xGovernor Pi bridge: wire contract

This is the contract `index.ts` (this extension) and `apps/runtime-pi/src/bridge.rs`
(the Rust HTTP server on the other end) both implement. **No changes were made
to the contract as originally specified** — `apps/runtime-pi/src/bridge.rs`
already implements and has passing tests (`apps/runtime-pi/src/bridge.rs`'s
`#[cfg(test)] mod tests`) against exactly the 8 endpoints and field shapes
below, confirmed by reading that file while building this extension. Where
this extension had to make a judgment call not fully pinned down by the wire
shapes themselves, that is called out under "Implementation notes" — those
are choices made *within* the contract, not changes *to* it.

## Environment variables

Set by `apps/runtime-pi::PiRuntime::start` on the spawned `pi --mode rpc`
child process. All three are required; `index.ts` throws a clear startup
error if any are missing.

| Variable | Meaning |
|---|---|
| `XGOVERNOR_BRIDGE_URL` | Base URL of the bridge, e.g. `http://127.0.0.1:54321` |
| `XGOVERNOR_BRIDGE_TOKEN` | Bearer token for this session; sent as `Authorization: Bearer <token>` on every request |
| `XGOVERNOR_WORKSPACE_ROOT` | Opaque path string used as the `basePath`/`cwd` passed to every Pi tool factory, so the LLM's relative paths resolve correctly before being sent to the bridge |

## Endpoints

All endpoints are `POST`, JSON request/response bodies, binary content
base64-encoded. Non-2xx responses use `{"error": {"kind": string, "message": string}}`.
`index.ts`'s `BridgeClient.post()` turns any non-2xx response into a thrown
`Error` whose message includes both `kind` and `message`.

### `POST /v1/workspace-root`
No body. Response: `{"path": string}`. Implemented for completeness /
potential debugging use; not called during normal operation since
`XGOVERNOR_WORKSPACE_ROOT` already supplies the same value directly.

### `POST /v1/stat`
Request: `{"path": string}`
Response: `{"exists": boolean, "kind": "file"|"directory"|"symlink"|"other"|null, "size_bytes": number|null, "modified_at_ms": number|null}`

### `POST /v1/read`
Request: `{"path": string}`
Response: `{"content_base64": string}`

### `POST /v1/write`
Request: `{"path": string, "content_base64": string, "mode": "create"|"overwrite"|"atomic_overwrite"}`
Response: `{"path": string}`

### `POST /v1/mkdir`
Request: `{"path": string}`
Response: `{}`. Recursive (matches `operation_protocol::OperationFileSystem::create_dir_all`, confirmed in `bridge.rs`'s `mkdir_handler`).

### `POST /v1/exec`
Request: `{"command": string, "args": string[], "cwd": string|null, "env": Record<string,string>|null, "timeout_ms": number|null, "shell": string|null}`
Response: `{"stdout_base64": string, "stderr_base64": string, "exit_code": number|null, "timed_out": boolean}`

### `POST /v1/glob`
Request: `{"pattern": string, "base_dir": string|null, "limit": number|null}`
Response: `{"paths": string[]}`

### `POST /v1/grep`
Request: `{"query": string, "base_dir": string, "include": string|null, "mode": "files_with_matches"|"content"|"count", "head_limit": number|null}`
Response: `{"entries": string[]}`

## Implementation notes (choices made within the contract, not changes to it)

These were resolved by reading `apps/runtime-pi/src/bridge.rs` and the
`operation-protocol`/`backend` crates it delegates to (`crates/operation-protocol/src/capability/{exec,filesystem,search}.rs`,
`crates/backend/src/local/exec.rs`), not guessed blind:

1. **`/v1/exec`'s `command`/`args`/`shell` split.** Pi's `BashOperations.exec`
   hands this extension a single full shell command-line string (e.g.
   `"ls -la | grep foo && echo done"`), not a program + argv. This extension
   always sends `command: <full line>`, `args: []`, `shell: "bash"` —
   confirmed to be exactly how `operation_protocol::capability::exec::ExecRequest`
   is already used elsewhere in this codebase, e.g.
   `crates/backend/src/local/exec.rs`'s own tests
   (`command: command.to_string(), args: vec![], shell: Some("bash".to_string())`).

2. **Bash execution is not truly streamed.** `/v1/exec` is a single blocking
   HTTP round trip — the bridge only returns once the command has finished.
   Pi's `BashOperations.exec` wants an `onData` callback invoked as output
   streams in; this extension instead calls `onData` at most twice (once
   with the complete stdout buffer, once with the complete stderr buffer)
   after the bridge responds. Live/interactive output is not visible to the
   LLM until the command completes. This is a known limitation, not
   something fixable purely on the extension side — true streaming would
   need a different transport (e.g. SSE/chunked) on `/v1/exec`, which is out
   of scope here.

3. **`write`/`edit`'s write mode is always `"atomic_overwrite"`.** Pi's
   `WriteOperations`/`EditOperations` interfaces don't tell the extension
   whether a given `writeFile()` call is creating a new file or overwriting
   an existing one — both call the same method the same way. `atomic_overwrite`
   is a safe default for both cases (it still creates the file if absent).

4. **`ls`'s directory listing has no dedicated endpoint.** There is no
   `readdir`/`list_dir` capability anywhere in this contract, and — more
   fundamentally — none in `operation_protocol::capability::filesystem::OperationFileSystem`
   either (it only has `stat`, `read_bytes`, `write_bytes`, `create_dir_all`,
   `temp_path`). Adding one would mean adding a new capability across every
   backend implementation (local, e2b), which is well outside this
   TypeScript-only piece of work. Instead, `LsOperations.readdir` is
   synthesized via `POST /v1/glob` with `pattern: "*"` scoped to the target
   directory, taking the basename of each returned path. This assumes the
   bridge's glob implementation treats a bare `"*"` the way shell globs
   conventionally do (matches within one path segment, does not cross `/`).
   If that assumption doesn't hold, `ls` may show extra/duplicate entries
   from nested directories — not a crash, but worth confirming against the
   real backend implementation. **Flagging this explicitly for the Rust
   side**: either guarantee non-recursive `"*"` semantics, or consider adding
   a dedicated list-directory endpoint later.

5. **`find`'s `ignore` list is filtered client-side.** Pi's `FindOperations.glob`
   is always called with an `ignore: string[]` option (e.g. `.git`,
   `node_modules`), but `operation_protocol::capability::search::GlobRequest`
   has no `ignore` field and the wire contract's `/v1/glob` request has no
   equivalent either. This extension fetches results from `/v1/glob` and
   filters out any path matching an ignore pattern using a small local
   glob-to-regex matcher (supports `*`, `**`, `?`; no brace/character-class
   expansion). This is best-effort and may be less efficient than
   server-side filtering for large trees, but requires no contract change.

6. **`read`'s image MIME detection is a pure client-side heuristic**
   (file-extension based: `.png`/`.jpg`/`.jpeg`/`.gif`/`.webp`/`.bmp`/`.svg`).
   The bridge has no MIME-detection endpoint, and `ReadOperations.detectImageMimeType`
   is optional, so this is not a gap — just noting the implementation choice.

7. **The hand-rolled `grep` tool's parameter schema is narrower than Pi's
   built-in `grep` tool.** It exposes `pattern`, `path`, `glob`, `mode`,
   `limit` — mapping directly onto `/v1/grep`'s fields — but not
   `ignoreCase`, `literal`, or `context`, since `/v1/grep`'s contract (and
   `operation_protocol::capability::search::GrepRequest`) has no equivalent
   fields and matching happens entirely server-side. Pi's SDK does expose a
   `GrepOperations` interface (`isDirectory` + `readFile`) for
   `createGrepTool()`, but that seam is for delegating file I/O to a remote
   filesystem while Pi's own tool still walks directories and matches
   patterns locally — it doesn't fit a backend where matching is already
   fully server-side. This extension instead hand-rolls the `grep` tool via
   `pi.registerTool()`, same as Pi's own official Gondolin example extension
   does for the same reason.

8. **Host-cwd paths are rewritten onto the sandbox workspace root before
   forwarding.** Pi's session cwd is the spawned `pi` process's
   `process.cwd()` (the daemon's working directory), and pi puts that host
   path into its system prompt — the model then hands it back verbatim as
   tool arguments. Observed in the real E2B run: `ls` was called with
   `/Users/hypo/Github/xGovernor` and failed with `Path not found` inside
   the sandbox. Every file/exec Operations implementation therefore passes
   incoming paths through `rewriteHostCwdPath` first: paths at/under the
   host `process.cwd()` are remapped onto `XGOVERNOR_WORKSPACE_ROOT`
   (preserving the relative structure), paths already inside the sandbox
   workspace root pass through unchanged, and relative paths (an unexpected
   fallback, since pi resolves before calling Operations) are anchored to
   the workspace root. This is a substitution on the extension side only —
   the wire contract is unaffected.
