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

## Lifecycle probe

The Unix example `lifecycle_probe` uses the fixed SDK to create a run-owned Root,
check inactive/running projections, start twice, execute two distinct managed
Messages, read stable terminal projections, stop twice, remove the Root, and
verify absence. This is a lifecycle subset; it does not prove native checkpoint
Ready, paired save/restore, queued/active cancellation, or production readiness.

```bash
cargo run -p xgovernor-thinkthread-client --example lifecycle_probe -- \
  /absolute/protected-config.json /absolute/new-journal.ndjson
```

The JSON configuration contains `host_executable`, `wait_timeout_ms` (1–300000),
`context` in the public SDK ClientContext shape, and `create` in the public SDK
ThinkThreadCreateRequest shape. Every context root, home and search directory
must be explicit and absolute. `create.fs` must equal `context.fsRoot`. Profile,
model and executable references must already be provisioned in an isolated
matching deployment. Environment values are read only for the named
`context.environmentKeys`; do not put credential values in the JSON.

Use a matching alpha8 installation and the Linux 6.12+ validation baseline.
The probe does not install a Runtime, create profiles, select a fallback Host,
or infer binary provenance from a filename. Keep the selected environment and
model credential files protected.

The new journal is mode 0600. It syncs each operation UUID before sending a
mutation and records only public operation/target IDs, checks and safe error
categories, never native reply text or environment values. Request errors,
nonterminal/truncated replies, partial batch outcomes, or unconfirmed absence
fail the probe. Unknown delivery is retained without automatic mutation retry,
global resource discovery, or destructive force-stop. Failed runs can retain
resources: use the journal's exact run-owned IDs and public SDK operation queries
for explicit reconciliation before cleanup; closing the Management client does
not stop a native Runtime.

Default lifecycle/checkpoint tests use a Python transport fixture. They verify
SDK framing, caller checks, exact revision and reconnect identity, not native
Session/Fs state. A restore main result can be committed even when execution
startup/continuation fails; preserve that partial result and the failed outcome.

## Durable operation bridge

`durable_operation::DurableManagementOperation` connects persisted joint intents
in `SessionRepository` to typed `thinkthread.create`, `thinkthread.start` and
`get_operation`. It reads the original admission and native step from storage,
checks canonical request parameters and their digest against the bound SDK
context, then atomically claims the sole initial send before invoking a mutation.
The claim remains unknown if the caller crashes or loses its response.

A later caller queries the original UUID and verifies its method, scope and
digest. Unknown delivery, missing operation records and observation failures do
not authorize another send or release ownership. Partial create results retain
their Root identity. Start requires a Root already confirmed by a completed
creation in the same binding. The bridge does not discover targets, replace
configuration, repair Roots, publish Sessions or register a server backend.
This is durable send admission, not a network-level exactly-once guarantee.

The SQLite recovery test uses the existing transport fixture, drops the create
response, reopens the repository and reconnects, then proves only one create was
sent and recovery queried its original UUID. It is a storage/SDK contract test;
it supplies no native Runtime, filesystem or isolation proof.
