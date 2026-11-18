# Orchestration contract

This crate contains shared types and the `execution::ExecutionBackend` trait.
It does not implement execution or run management. `run` defines the five shared
entry actions: start, status, cancel, result and list. `event` defines durable,
cursor-addressable run events and namespaced algorithm extensions.

`rpc` maps execution requests and job notifications to the existing
`mcts-rpc-v1` JSON-RPC shape. `plugin.*` remains the algorithm lifecycle wire
spelling. Algorithm-specific configuration belongs in
`config.algorithm.parameters`, under a versioned namespace and schema.
The backend's `execution::ForkRequest.network_policy` is a required
`NetworkPolicy` with no default. Run management loads the effective policy from
the persisted run record (resolved task default or user override).
`rpc::HostForkParams` alone retains the legacy `offline` boolean.
`into_backend_request(&recorded_config)` clamps it to the run policy: an offline
run stays offline when the algorithm sends false or omits the field. An online
run may request an offline branch. Run management must authorize the run ID and
load its record before mapping; algorithm-supplied configuration is not trusted.
Host-initiated branches, including scoring and official evaluation, use
`RunConfig::fork_request` with that same recorded configuration, regardless of
whether conversation history is inherited. There is no evaluation exception.

On the wire, `Unsupported` is classified as `INCOMPATIBLE_PROTOCOL` (-32008):
the existing vocabulary includes required capability unavailable in this class.
It does not redefine -32008 as a new error. `data.kind = "rejected"` and
`data.code = "unsupported"` identify the backend capability rejection, while
protocol/version incompatibility retains the existing class meaning.

Mutation keys are scoped by run and method, survive uncertain results, and are
retained until run reclamation. Same key with changed arguments conflicts.
An unknown outcome carries its operation identity; the host uses `reconcile`
with that identity before retry or cleanup. `Pending` and `Unknown` are not
success or evidence of absence. `wait_job` and `reconcile` are backend-side
queries, not additional algorithm RPCs.

Agent turns and commands return receipts, followed by ordered `host.job.*`
notifications. Nonzero command exit is completed execution; execution timeout,
transport failure and cancellation cannot be converted into a zero score.
Cancellation acceptance is distinct from confirmed stop. Cleanup waits for
confirmed stop, retains unresolved resource identities, and preserves selected
snapshots and artifacts for their recorded lifetime.

Implementations enforce ownership, capability admission, branch exclusivity,
finite deadlines, snapshot consistency and the requested network policy. A
capability declaration describes actual guarantees, including deployment
isolation. Unsupported requirements are rejected without fallback. Model
credentials belong to the host/Agent adapter and must not enter branch files,
conversation state, event payloads or logs.

Run management also uses four host-only capabilities; they are not algorithm
RPC methods. `PrepareRoot` accepts an image/template reference, Git repository
and immutable commit or existing snapshot, working directory, and the recorded
network policy. It returns an immutable snapshot owned by this run. Workspace
packages are reserved and currently rejected as Unsupported. Initial task root
descriptions live in run-manager configuration (SWE-bench image naming rule,
`/testbed`, offline; DeepSWE allows internet); an independent task layer will
supply them later.

`RenewRunLease` establishes the lease before root provisioning and renews it
periodically with positive `ttl_ms`. Each renewal uses a new idempotency key;
retrying an old key returns its original expiry. The backend independently stops
jobs and reclaims all run resources after expiry. `ReclaimRun` waits for in-flight
jobs to stop before release, returning complete, unknown or failed. Unknown and
failed results retain resource identities; a complete result has none. Artifacts
already exported to the run directory survive reclamation. A backend without
leases omits `RenewRunLease`; admission checks the run requirements and never
silently ignores a required lease. Default trait implementations explicitly
reject these added capabilities as Unsupported.

`ExportWorkspace` takes a run-owned snapshot or branch and a manager-authorized
run directory, relative destination and byte limit. It writes a consistent,
complete tar archive containing tracked and new files, excluding `.git` by
default; `include_vcs_metadata` explicitly includes repository metadata. Paths
must stay within the run directory without symlink escape. Publish atomically;
never overwrite another operation's destination. Return archive size, regular
file count and lowercase SHA-256. Exceeding the archive limit returns
`ExportTooLarge` (wire invalid params, `data.code=export_too_large`) without a
partial destination. The existing `diff` continues to supply patches.

All four entries use mutation contexts and the existing idempotency, timeout,
ownership and outcome-unknown reconciliation rules. Their method identities are
`host.prepare_root`, `host.renew_run_lease`, `host.reclaim_run` and
`host.export_workspace`; typed successful reconciliation results are included in
`MutationResult`. They do not change existing methods' behavior.

The algorithm wire uses `rpc::WireCapability`, including `job_progress`, while
backend declarations use `execution::Capability`. Host-only root, lease, reclaim
and export capabilities never enter algorithm handshakes. `job_progress` is only
advertised by a host that actually delivers progress; its payload matches the
algorithm registry's usage and batch accounting. Output deltas require a nonempty
stream name. Backend cancellation distinguishes cancelling, stopped, completed
and failed; the wire maps stopped to failed (confirmed cancellation), preserves
completed for a job that already finished, and never sends `stopped`.

`WireBudget` is the closed existing four-field shape: positive `max_iters` and
`branching`, plus `max_time_ms` and `max_tokens`. The two search settings are
read from algorithm parameters and never added to common run configuration.
Algorithm lifecycle response types retain their existing status vocabulary;
`AlgorithmRunStatus` excludes the manager-only interrupted state. Wait results
preserve optional reason, findings and required_input.

An uncertain mutation is reconciled under its original identity within bounded
backend-call deadlines. If still unresolved, the worker terminates the algorithm
and records a failed run and the unresolved operation/resource identities before
cleanup. It sends no ordinary RPC error that could encourage a new-key retry.
`RpcError::try_from` rejects OutcomeUnknown so callers must handle this explicitly.

Run admission rejects with `run::RunError` (`code`, `message`) and no run ID.
CLI emits that JSON; MCP and session adapters carry the same JSON rejection.
Backend selection, capability mismatch, budget/workspace approval mismatch and
start-key conflicts have stable codes. Isolation satisfaction is ordered none,
workspace, container, sandbox: bare worktrees have no security isolation, Docker
shares the host kernel (container), E2B supplies the sandbox boundary. Declarations
must reflect deployment guarantees. Requirements include `can_reset_history`;
false-history scoring must be rejected at admission if the backend lacks it.
