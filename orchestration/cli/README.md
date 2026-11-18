# mcts command

Build `cargo build -p orchestration-cli`. Use `mcts --config /path/config.json`
with `run --request request.json`, `status <run_id> [--after-sequence N]`,
`cancel <run_id>`, `result <run_id>` or `list`. Responses are JSON. `run` accepts
the contract's StartRequest, including exact budget approval and effective
network policy. It returns after durable acceptance and worker launch; closing
the entry process leaves the worker running. Repeated starts return the same ID.

`mcts mcp` serves the same actions over stdio. Without `--config`, the file is
`~/.config/mcts/config.json`, allowing the OpenCode plugin's `mcts mcp` command
to use this application directly. No old configuration variables are read.
`mcts install --target opencode [--config-dir DIR] [--uninstall]` invokes the
bundled OpenCode installer under the configuration's `package_directory`, which
contains `plugins/opencode` and `skills/mcts`.

Backend drivers must be linked into the distribution at its assembly point.
This crate assembles `driver: "own"` for E2B using its actual capability
declaration, per-run SQLite stores and the configured OpenCode runtime. It never
substitutes a test backend for one. Current backend host-only capability gaps
are described in the installation guide. The `test-support`
feature explicitly enables `driver: "memory-test"` for integration testing only.
The test fixture uses `kind: "e2b"` as a selection slot, without claiming E2B
isolation or joint snapshot capabilities. Do not deploy that feature.

For a user-directory bundle including the private Python environment, plugin
assets and configuration, follow the [installation guide](../README.md).
