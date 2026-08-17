# xgovernor-bridge-extension

A [Pi](https://github.com/badlogic/pi-mono) extension (`@earendil-works/pi-coding-agent`)
that overrides Pi's built-in tools (`read`, `write`, `edit`, `bash`, `ls`,
`find`, `grep`) so that every file/exec operation is proxied over a local
HTTP bridge instead of touching this `pi` process's own local filesystem.
See `CONTRACT.md` for the exact wire contract and the three required
environment variables.

## How it's loaded

This directory is part of `apps/runtime-pi`, this repo's (`xGovernor`) Rust
crate that spawns and drives `pi --mode rpc` as a `RuntimeAdapter`
(`apps/runtime-pi/src/lib.rs`). It is loaded as:

```
pi -e apps/runtime-pi/extension --mode rpc
```

`apps/runtime-pi::PiRuntime::start` sets `XGOVERNOR_BRIDGE_URL`,
`XGOVERNOR_BRIDGE_TOKEN`, and `XGOVERNOR_WORKSPACE_ROOT` on the spawned
process before this extension's default export runs.

**This extension is not meant to be used standalone.** Without those three
env vars (and a running `apps/runtime-pi::Bridge` HTTP server on the other
end), it will throw immediately on load.

## Status

Modeled closely on Pi's own official "Gondolin" example extension
(`packages/coding-agent/examples/extensions/gondolin/index.ts` in
`badlogic/pi-mono`), and type-checked against the real, published
`@earendil-works/pi-coding-agent@0.84.2` package (its actual `.d.ts` files
were read directly to get `ReadOperations`/`WriteOperations`/`EditOperations`/
`BashOperations`/`LsOperations`/`FindOperations`/`GrepOperations`,
`ExtensionAPI`, `ToolDefinition`, and the `createXTool()` factory signatures
right — not inferred from the Gondolin example alone).

That said, this has **not been run against a real `pi` install or a real
`apps/runtime-pi::Bridge`** — the repo owner doesn't have `pi` installed
locally, so end-to-end behavior (in particular the `ls`/`find` glob-based
workarounds documented in `CONTRACT.md`, and the non-streaming bash exec)
is unverified. Treat it as best-effort against Pi's public extension API
until it's exercised against the real thing.
