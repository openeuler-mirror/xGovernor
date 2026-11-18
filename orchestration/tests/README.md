# Execution backend compatibility

The library exports one async case for each execution capability and one for
unknown outcomes. Backend integration tests construct `Fixture` with a fresh
run, an immutable root snapshot, an allowed workspace-relative test path and
commands for their environment, then call these cases with their backend.
Each case owns the resources it creates; use a separate run per case and reclaim
the run after the case, including when assertions fail. Tests require all ten
capabilities and the fixture's network policy to be advertised.

`outcome_unknown` requires the fixture to lose the first fork response **after
the fork takes effect**. Reconciliation must eventually return that original
branch. The hook is supplied by the backend integration test, not this library.
It must not turn an actual failure into simulated success.

Commands must finish within the fixture timeout: `exec_argv` prints
`compat-exec` and exits 7; `cancellable_argv` stays active until cancelled.
`turn_input` asks the Agent to emit nonempty output. The root is quiescent and
allows writing `test_path`. This suite checks interface behavior; provider
network enforcement, process-tree stopping and joint snapshot guarantees also
need the platform's real environment/Agent integration evidence.

`cargo test -p orchestration-compat` runs the cases on a test-only in-memory
backend. It executes no commands and invokes no Agent, and is only a smoke test
of the reusable assertions, not proof of any production backend capability.
