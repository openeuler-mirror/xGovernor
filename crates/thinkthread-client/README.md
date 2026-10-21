# ThinkThread Management client

This crate provides a Host-side client for the pinned alpha11 Management SDK
(`2.2.0`, protocol v2, Git revision
`881ff5d49395bfe53904c2da2f8d6403db1b42fd`). It requires Rust 1.89 or newer.
The default server does not assemble it as an execution backend.

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

Use a matching alpha11 installation and the Linux 6.12+ validation baseline.
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
in `SessionRepository` to typed lifecycle and Message operations, with
`get_operation` for recovery. It reads the original admission and native step from storage,
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

## Root lifecycle owner

`root_owner::RootSessionOwner` implements a deliberately limited joint lifecycle
for an explicitly configured administrator deployment. It accepts no tenant
execution, requested LLM override, arbitrary extension parameters or advertised
runtime/sandbox capabilities. The configured filesystem must match the normalized
workspace. A durable workspace claim prevents another binding from taking the
same configured path. This Root/direct profile provides no private Child,
checkpoint, Host operations or isolation guarantee.

Open persists sanitized publication input, budgets and native intent before
create/start. A verified running Root with a committed Ready session is required
before atomic Session publication. The runtime state contains an opaque binding
reference. Reconciliation uses the original persisted inputs and operation IDs;
configuration changes do not replace a pending creation. Attachment and health
checks resolve native identity from the durable binding rather than public IDs.

Unpublished compensation atomically fences ordinary execution and publication
before stop/remove. It uses only the binding's exact known Root, never force or
resource discovery, and requires a separate public absence observation before
releasing ownership. Failed create without a trustworthy residual identity stays
pending. Public close, lease/reaper cleanup and retained checkpoints remain
unsupported.

`SqliteSessionRepository::with_joint_transactions()` explicitly enables the
storage seam for assembly with a verified owner. Its default remains disabled;
this crate adds no server registration or selectable production profile. After a
successful commit, closing the Management connection cannot change the business
result. Credentials and raw request extension/configuration are not persisted.

## Durable Message turns

An explicitly assembled `RootSessionOwner` can admit terminal-only Message
turns for its administrator-owned Root. It accepts an empty policy/capability
profile; model overrides, reasoning controls, entry metadata and extensions are
rejected before Message dispatch. Production token-budget work is unsupported.

The owner persists the public turn, idempotency key, input digest and native
operation UUID before sending. A disconnected send is recovered by querying that
UUID, without another mutation. Message identity, admitted text and owned Root
must match. A successful reply also requires native Session Ready. Truncated,
failed or missing replies fail; an observation timeout or EOF retains the durable
source fence for reconciliation. Client disconnection does not cancel execution.

Terminal state and source-fence release commit together. The application emits
one full reply delta and one terminal, with explicit unknown metering. Replaying
an idempotency key returns the original receipt; changed input conflicts. These
operations do not provide active cancellation, streaming, policy enforcement,
checkpoints or public close.

## Server-side execution profiles

The default pi/xiaoo daemon does not acquire a deployment guard or inspect a
`deployment-locks` directory. Deployment locks apply to explicit ThinkThread
profile assembly.

On Linux, `profile` provides an explicit research assembly around the existing
joint lifecycle. Parse `ExecutionProfile` strictly, verify it against an
independently trusted `DeploymentManifest`, then construct frozen `ProfileRevision`
adapters. The manifest binds the pinned SDK, ARM RPM, exact Management contract,
built-in xiaoO extension, endside/session-format evidence, Host executable digest,
Context, model and secret-version references. Provision those inputs separately
from Session requests. Changing an immutable revision requires a new revision.

Acquire a core `DeploymentGuard` before assembling a `ProfileRouter`. Provision
its private lock namespace and managed directories first. The guard holds OS
object locks on the database, managed roots and their ancestors for its lifetime;
consistent database copies retain the same identity and lock namespace. It accepts
only supported local Linux filesystems and rejects changed object identities.
Shared ancestor locks prevent nested resource claims and detect parent replacement;
the filesystem allowlist applies to exclusively protected objects. A bound
repository checks identity before joint and split writes, including snapshot
intents and checkpoints. Detected identity changes permanently invalidate that
holder. Keep the guard alive until daemon exit. Locks establish local writer ownership;
they provide no evidence that a previous backend operation stopped.

Supply a `DeploymentBudgetConfiguration` with a stable policy reference, finite
resource totals and recovery reserves. Missing capacities are zero. Every joint
connection must use the holder's exact policy. `operation` bounds retained pending
operations, including Host cleanup-pending; `record_bytes` counts the serialized
column bytes of owned SQLite records. It bounds logical record content, excluding
SQLite page/index/WAL overhead and the fixed deployment identity row. Neither unit
measures backend storage. Reducing budgets preserves existing records and refuses
growth beyond available ordinary capacity. Recovery reserves are not ordinary
capacity and do not introduce a new recovery allocation protocol.

Register server bearer/database/operations secrets with
`guard.protect_server_values` before assembly. They are prohibited from selected
Host environments as well as stored content. Public error projections and audit
fields redact registered values even when Session lookup fails before profile
admission. Registrations expire with their owning deployment value sets. Management/model secret values stay
in the explicitly selected Host environment; the profile records only environment
keys and secret-version references. The guard rejects known protected values in
request content and SQLite writes, including nested JSON and encoded binary
results. It fails closed on excessive nesting. It does not sanitize arbitrary
third-party logs or make unknown secrets safe to forward.

Both profile enablement and joint transactions require explicit opt-in.
`disabled` is the default admission mode. `experiment` requires a nonexpired
allowlist entry for the authenticated principal, tenant, revision, release,
workload and capability subset. Administrator status alone grants no exception.
Each Message open/turn and Host operation must itself appear in both the fixed
declaration and its matching permit, even when workload requirements are empty.
Evidence references must identify the fixed alpha11 release.
Research configuration must explicitly disable token fuse and retain finite wall
and expansion bounds. Default MCTS workloads and `production_candidate` are
refused. Required usage, hard policies, isolation, images, complete event streams
and backend storage guarantees remain unsupported.

The router preserves the original revision for existing Sessions and pending
intents. A changed default affects new opens; disabling a revision preserves its
readers. Missing original dependencies fail without selecting another revision.
The Root adapter supports only its existing administrator Message subset and
terminal-only unknown metering. The Host adapter supports only the existing
private-candidate read, overwrite and exec mapping with its original limits.
The ordinary-cancel declaration records the supplier evidence and checkpoint
requirement; this router exposes no new public cancellation path. Candidate
capture/load and public close retain their existing unsupported behavior.
No production backend registration is added.

An enrolled database retains its deployment protection across connection and
process lifetimes. Unbound session connections can read existing schema but do
not initialize, migrate, or write it. Split and joint writes check the holder
inside an immediate SQLite transaction, serialized with deployment enrollment.
The SQLite provider ledger has no deployment-holder binding and therefore
refuses schema initialization and all writes to an enrolled database; existing
ledger rows remain readable. Ordinary databases without deployment registration
retain their legacy session and provider-ledger behavior. The default server
uses one database file for session records and all provider ledgers. No separate
provider-ledger file is selected by the explicit joint profile implementation.
