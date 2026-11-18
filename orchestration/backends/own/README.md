# Own execution backend

`OwnBackend` implements `orchestration_contract::execution::ExecutionBackend`
through the existing `SessionApplication`, `InstanceManager` and E2B provider.
It does not use session HTTP. The Agent runtime is supplied by the caller;
the real compatibility fixture uses the installed Pi runtime and worker.

Create one backend and SQLite store per run. Pass the authenticated platform
owner, persisted run/plugin identities, admitted grants and network policy,
model configuration, Agent bootstrap extensions and run lease claim to `open`.
The root is read-only to algorithm calls; forks enforce the admitted policy.
Renew execution leases with `heartbeat` from the run supervisor, and keep the
existing manager retry/reaper tasks running. Use host environment credentials (`api_key_env`); keys must not enter the
sandbox or persisted run records. Strict E2B process cleanup is required.

Establish `renew_run_lease` before provisioning. The backend persists expiry and
runs its own reaper; the owning host must reopen durable runs after process
restart so expired resources and unresolved operations can be reclaimed.

Use `prepare_root` with an E2B template, exact Git commit, workspace directory
and admitted policy. Offline templates must already contain that commit and
task dependencies. OCI images require an explicit host-resolved mapping through
`register_image_template`; a local Docker image ID cannot be used by E2B.
Alternatively open the root execution instance through the coordinator, then
call `register_root`. The root transfers to this run's ownership. `fork` enforces
exactly the admitted network policy. `inherit_history=false` starts a fresh
Agent on the restored filesystem without inspecting opaque Agent state.
Snapshot metadata expires after 24 hours; call `cleanup` from run reclamation
to delete platform snapshots and stop execution instances. A process crash
requires reclamation, not resuming old jobs. Unresolved operations and resource
identities remain in SQLite; a deadline does not authorize a replacement key.

Mutations are exclusive per branch. Jobs retain their guard until the runtime
and process supervisor report completion. Cancellation returns `Cancelling`
until the job publishes a terminal. Repeated mutation keys replay the original
result and job identity; changed payloads are rejected. Unknown token usage is
an empty usage map.

`diff` restores its explicit base snapshot and compares workspace contents,
including untracked files. Export is bounded to 16 MiB and 10,000 regular files;
non-regular files fail explicitly. `.git` directories are excluded. Temporary
base branches remain tracked until deletion completes, even if the read times
out.

The ignored `e2b` integration test invokes the shared compatibility cases on
real E2B. It requires `E2B_API_KEY`, `OWN_E2B_STATE_DIR`, `OWN_PI_WORKER`,
`DEEPSEEK_LLM_MODEL`, `XGOVERNOR_LLM_API_BASE` and `MODEL_API_KEY`. Run it on the
E2B validation server under its heavy lock and resource limits. Normal workspace
tests do not start cloud resources or a model.

`export_workspace` publishes a complete tar with a byte limit and SHA-256, using
descriptor-relative paths to reject traversal and symlink destinations. It
includes new files and symlinks, excludes `.git` unless requested, and rejects
special files. Over-limit exports never publish a partial destination. Exported
artifacts survive `reclaim_run`. Reclamation waits for running work and retains
unknown resource and operation identities.

Hosts can register OpenCode with `runtime::opencode_registration`. The existing
coordinator supplies the run-owned operation backend; the adapter controls
session/turn generations. Restores receive the current host model configuration
and environment credential reference explicitly; credentials are not restored
from snapshots. Model overrides are not sent during a turn.
