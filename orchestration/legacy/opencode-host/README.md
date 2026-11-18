# OpenCode host

`xgovernor-opencode-host` connects the Python algorithm to an OpenCode v2 HTTP service, managing private Git snapshots, worktrees, sessions and selected-result delivery. Install it together with the [algorithm and task packages](../../algorithms/mcts/README.md).

```sh
python3 -m xgovernor_opencode_host.run_opencode_mcts run \
  --repo /path/to/repository --goal-file /path/to/goal.md \
  --output-dir /path/to/run --model provider/model \
  --opencode-url http://127.0.0.1:4096 --expect-version 2.3.0 \
  --max-iters 1 --branching 1 --max-tokens 100000 --max-time-ms 600000
```

The OpenCode service owns model configuration and credentials. The host does not provide sandbox isolation: use it only with a trusted repository and a suitable outer execution boundary. Result delivery writes the selected workspace, patch and session continuation information to the run directory. To release every retained session and worktree after inspecting the result:

```sh
python3 -m xgovernor_opencode_host.run_opencode_mcts cleanup \
  --output-dir /path/to/run --opencode-url http://127.0.0.1:4096 \
  --expect-version 2.3.0 --drop-selected-session
```
