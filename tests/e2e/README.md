# Installed entry scenarios

These two opt-in tests require a production bundle installed into a private
`MCTS_E2E_HOME`, an E2B template for each task, host Docker instance images,
OpenCode 2.3.0, a model credential in the environment and a configured host
`swebench_python`. They never substitute a backend, algorithm or OpenCode.
Missing explicit inputs skip both scenarios with a reason.

Set `MCTS_E2E_REQUEST` and `MCTS_E2E_OPENCODE_REQUEST` to two distinct approved
SWE-bench Verified requests, and `MCTS_E2E_OPENCODE` to the absolute OpenCode
executable. Set `MCTS_E2E_TIMEOUT` to the allowed monitor duration in seconds
(default 3000), then run:

```sh
python3 -m unittest discover -s tests/e2e -v
```

Reuse each request’s start_key to inspect an existing run without launching new
execution. Both scenarios require completed delivery and an official verdict.

The CLI scenario starts a worker in a persistent systemd unit, waits for the
launcher to exit, polls status/result, and checks patch, workspace, record,
official verdict and complete cleanup. The OpenCode scenario installs the
production plugin, starts the real OpenCode server and creates a session. A
local probe invokes its discovered MCP tools to start the second task, observe
execution, and retrieve completed delivery and confirmed cleanup. The probe makes no
entry-model call; the solver uses the configured real model.

The host needs permission to create and stop systemd units. Hold the server's
heavy lock for both scenarios, limit the worker and monitoring units together
to the available CPU/memory, and keep worker units separate from monitor units.
Keep the private home and run records until evidence and resource identities
have been collected; the tests do not remove them on failure.

The contract compatibility crate retains its small in-memory backend for isolated
interface unit tests. It is not a CLI driver or a distributable execution option.
