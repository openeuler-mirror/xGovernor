# Run management

`RunManager` implements the five typed `RunClient` actions used by the MCP
adapter. It provides durable admission, start-key deduplication, budget approval,
capability admission, cancellation requests, event cursors and terminal records.
`service::Service` durably accepts a run and launches a detached worker using
the assembled executable. `worker::execute` claims it and runs the configured
algorithm over stdio. An unclaimed run becomes interrupted after 30 seconds.
The backend is injected as an ExecutionBackend; the library does not implement
a production backend.

SQLite WAL with full synchronous commits is authoritative. Each run has a
directory containing `config.json`, `state.json`, `usage.json`, `events.jsonl`
and `result.json`. These projections are repaired from committed records before
an entry action returns; concurrent projections serialize with database writers.
Run IDs are generated locally and never used as caller-controlled filesystem
paths. The deployment is single-user and its run directory must be private.

The worker claims ownership with its PID, kernel boot ID and process start
ticks. A missing, reused or zombie process becomes `interrupted`, with cleanup
`unknown`; execution is not resumed. Cancellation first records `cancelling`.
Only the owner may confirm `cancelled` after stopping execution. The first
recorded terminal is immutable. Cleanup status describes actual knowledge and
is separate from execution status.

Backend registrations contain actual capabilities supplied by each configured
backend. Auto selection uses ThinkProcess, E2B, Docker, worktree order, chooses
the first configured backend and then checks admission. Failed admission never
falls through to another backend. Workspace uploads are unsupported.

`network_policy(source, override)` resolves SWE-bench to offline and DeepSWE to
allow internet; unknown task defaults require an explicit policy. The common
start request already contains the required effective policy and a supplied
value is an explicit override. Persist it before creating any branch. Execution
callers must load that record and use the contract's RPC fork mapping.

Budget approval must match the exact requested limits. Zero is zero allowance.
`account_usage` accepts cumulative monotonic token and cost usage, records tail
usage and returns whether new work may be admitted. Replayed job usage must be
deduplicated by the worker before computing cumulative totals. Budget exhaustion
does not fabricate a successful execution or an evaluation score.

Configuration is an explicit JSON file read with `Configuration::load`. Relative
paths resolve against its directory. It contains `runs_dir`, `algorithms_dir`
and named `models` with `provider`, `model`, `api_base`, `credential_ref`.
Credentials are resolved by the host adapter and never persisted in run records
or passed into an algorithm. No legacy environment or `config.env` is read.

| Previous configuration | File configuration |
| --- | --- |
| `XGOVERNOR_MCTS_DEFAULT_MODEL`, `XIAOO_MCTS_MODEL` | `models.<name>.model` |
| `XGOVERNOR_LLM_PROVIDER`, `XGOVERNOR_LLM_API_BASE`, `XGOVERNOR_LLM_KEY_ENV` | `models.<name>.provider`, `.api_base`, `.credential_ref` |
| `XIAOO_MCTS_TOKEN_FUSE_LIMIT` | approved run `config.budget.max_tokens` |
| `XIAOO_MCTS_*` search settings | `config.algorithm.parameters` |
| `XIAOO_PROMPT_*` | versioned prompts inside the selected algorithm package |
| `config.env` | explicit JSON configuration file and typed run request |

The original session host continues to use its own configuration. This library
does not modify that host or offer compatibility environment aliases.

## Execution

The worker establishes a backend lease before root provisioning, renews at one
third of `lease_ttl_ms`, and fails execution if renewal cannot be confirmed.
Backend-owned expiry handles orphan resources after worker disappearance.
A backend lacking leases must omit the capability; runs requiring
`renew_run_lease` are rejected at admission. Runs explicitly permitting such a
backend receive no orphan guarantee from the manager. Reclamation waits for
backend-confirmed job stop; unknown/failed cleanup retains `execution.json`
resource and operation identities plus `cleanup.json`. Cancellation becomes
effective only after the algorithm is stopped and backend cleanup is complete.

Algorithms run as `python3 -u <algorithms_dir>/<namespace>/__main__.py` with a
cleared environment and bounded newline JSON frames. The worker negotiates
`mcts.rpc.v1`, starts the algorithm, dispatches authorized run-owned host calls,
publishes job terminal notifications after receipts, and deduplicates job usage.
RPC deadlines are clamped to remaining run time and an operation ceiling:
120 seconds for root preparation, fork and snapshot; configured `job_timeout_ms`
(default 1800000, 30 minutes) for agent turns and exec jobs; 30 seconds for other calls. Run reclamation has a separate bounded
120-second deadline so an exhausted search budget does not prevent cleanup.
Internal lease, reconciliation and cleanup control calls retain bounded deadlines. Mutation intents are journaled before
dispatch; uncertain outcomes are reconciled with the same identity. Unresolved
operations stay journaled through cleanup. The algorithm process group is
stopped before recording a terminal. Linux parent-death signaling also stops
the algorithm leader when its worker is killed.

Budget time starts at worker claim. Time/cost limits stop new execution admission
and request algorithm cancellation; the worker allows a bounded two-second stop
window and records budget exhaustion without inventing scores. The token limit
rejects new model turns while allowing selection and export operations. Completed job
usage is summed once; empty usage is unavailable rather than evidence of zero
model use. In-flight tail usage is retained when reported before cleanup.

Selection payloads are preserved in `selection.json`. A selected branch or
snapshot must be owned by this run; snapshots are forked using the recorded
network policy for diff/export. `workspace.tar`, `workspace-summary.json`,
`selected.patch` and event records remain in the run directory after backend
reclamation. Files are never applied to a caller workspace. Artifact IDs are
relative names scoped to the returned run ID, not arbitrary host paths.

Selected-result diff, snapshot restoration and workspace export each use
`job_timeout_ms` (default 30 minutes), clamped to the remaining run time.
Restoring the baseline and reading two workspaces can take longer than ordinary
queries; delivery therefore shares the configurable long-operation ceiling.
Lease renewal continues while these delivery calls wait. They do not extend the
approved run budget. If delivery fails or times out, the
run is failed with a delivery limitation, while its result retains the selected
candidate, algorithm stop reason, already published artifacts and `selection.json`
reference. That file preserves the original algorithm result, including official
evaluation evidence; delivery failure does not change the evaluation verdict.

## Task root configuration

The initial task descriptions live in `tasks` in the unified configuration.
Defaults are `swe_bench` with image
`swebench/sweb.eval.x86_64.{reference}:latest`, `/testbed`, offline, and `deepswe`
with image `deepswe/{reference}:latest`, `/testbed`, internet allowed. Deployments
can replace these image naming rules. Each `instances.<reference>` supplies
`repository` and full immutable Git `commit`; missing task data fails preparation.
The independent task layer will supply these descriptions later. Snapshot
sources use the backend's `existing-snapshot` template reference. Package upload
is reserved and unsupported.

Additional configuration fields are `backends` (kind and assembled driver),
`lease_ttl_ms` (default 30000, minimum 300), `export_max_bytes` (default 256 MiB),
`job_timeout_ms` (default 1800000, positive), optional `package_directory`, and `tasks`.
Agent turns and commands use the smaller of this job limit, the caller deadline,
and the remaining run time. Configure the Agent adapter timeout consistently.
Named model descriptors remain on the host side; the backend assembly is responsible for resolving `credential_ref`
and injecting credentials into its Agent adapter. No model credential is sent
to the algorithm or workspace. The CLI owns the native OwnBackend assembly and uses its actual capability declaration.

`python_executable` selects the private installed interpreter (default `python3`).
It is used for algorithms and plugin installation; algorithms still receive a
cleared environment. Relative interpreter paths containing a directory resolve
against the config file. Optional `backends[].e2b` holds connection descriptors
(`api_url`, `domain`, `envd_scheme`, `credential_ref`, `timeout_secs`) for backend
assembly. E2B also accepts model (default solver) and an absolute opencode_executable.
The CLI assembles the own driver; missing host-only backend capabilities remain
explicit admission/Unsupported failures. See the [installation guide](../README.md) and its config sample.

For MCTS finishing, token exhaustion alone does not cancel the algorithm.
After recording all reported usage, the worker rejects new `agent_turn` calls
at the hard token limit; commands, files, forks, snapshots and export remain
available for selection and evaluation. Time/cost exhaustion and user
cancellation retain the bounded cancellation window. MCTS reserves a configured
fraction of its start budget before stopping expansion, using the same terminal
job usage notifications as the manager. The time limit includes root preparation and startup. The worker passes only
the remaining time in `plugin.start`. MCTS stops expansion when remaining time
reaches its finishing reserve (the same configurable ratio as the token reserve,
default 10% of the starting remaining time). The manager's hard time deadline
still cancels the algorithm, with the existing two-second stop window; finishing
must fit inside the approved time, and already admitted work may spend the reserve.
The worker runs the algorithm in its
private run directory, which contains the persisted startup configuration.

MCTS structured events use the first-version `plugin.progress.summary` carrier:
`mcts.event.v1 ` followed by a JSON event envelope. Before recording it, a
run-owned validator process in the installed algorithm package strictly checks
`resources/events-v1.schema.json`, including public payloads and MCTS extensions.
Invalid envelopes fail execution before event persistence; they do not fall back
to public events. Ordinary MCTS notifications are validated as extensions too.
A dedicated event transport requires a future RPC protocol version.

For prepared SWE-bench E2B roots, `backends[].e2b.image_templates` explicitly
maps configured task image names to administrator-built E2B template IDs. The
host registers these mappings before root preparation. Optional `swebench_python`
selects the host Python containing the SWE-bench harness; absent prerequisites
produce an official-evaluation `not_run` record, independent of search success.

Query commands (`status`, `result`, `list`) open the run database read-only, with a
10-second busy timeout, and do not assemble execution backends. Writers use WAL
and the same timeout. Queries report a vanished worker as interrupted without
repairing JSON projections; the next mutation persists that observation.

Workers disconnect stdio and start a new session. When launching inside a service
manager, place the launcher/worker in a persistent unit separate from polling or
monitoring units: a cgroup-wide stop also kills detached processes in that cgroup.
