# ThinkThread Management client

This crate provides a Host-side client for the pinned alpha8 Management SDK
(`2.2.0`, protocol v2, Git revision
`378fd41df1773e00dc01e4ddb5b28ac1df987719`). It requires Rust 1.89 or newer.
It is not registered with the server, `AgentRuntime`, `Provider`, or the public
Session protocol.

Construct `ManagementConfig` with an explicit Management Host executable,
state/runtime/config/cache roots, working directory, filesystem root, home,
executable search directories, selected base environment, and a positive
connection timeout. Paths must be absolute. Validation does not create paths.
The selected environment is copied into the Host process; diagnostic `Debug`
output contains environment keys, not values.

`config.connect().await` returns the supplier's `ThinkThreadClient` after its
exact protocol/fingerprint handshake and immutable context binding. Use the
SDK's typed operation groups directly. A missing executable, incompatible
handshake, rejected context, or connection timeout fails without selecting
another executable or runtime. Preserve SDK delivery certainty, especially
`SentUnknown`; durable operation IDs and reconciliation belong to the future
integration owner. Connection closure terminates the SDK Host connection, not
a ThinkThread tree.

Offline contract tests use a Python 3 subprocess fixture on Unix:

```bash
cargo test -p xgovernor-thinkthread-client
```

The fixture covers connection framing and failure boundaries. It provides no
native Runtime, Ready, ownership, checkpoint, isolation, model, or production
readiness evidence.
