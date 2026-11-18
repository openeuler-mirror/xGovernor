# Installing and using mcts

The native bundle installs the `mcts` command, a private Python environment with
`xgovernor-mcts` and `xgovernor-tasks`, and the OpenCode plugin and Skill assets.
It also includes `xgovernor-opencode-host` because task registration still imports
its DeepSWE adapter. The session server is distributed separately.

## Build a bundle

On the target Linux architecture, install Rust/Cargo 1.94+, Python 3.11+ with
venv/pip, a C compiler and the repository's native build prerequisites. From
a clean checkout, with Internet access for Cargo and Python dependencies:

```sh
python3 orchestration/cli/distribution/build.py --output "$PWD/mcts-linux.tar.gz"
```

The build uses Cargo.lock with `--locked`. It collects Python wheels and their
dependencies; installing the resulting archive needs no package index. Bundles
are native and require a compatible Linux/glibc host. Build on the oldest host
you intend to support. Downloads and building need no root when prerequisites
are already available. Build the repository only from a trusted revision.

## Install into your user directory

The receiving host needs the same Python major/minor version used to build the
bundle (3.11 or later), with venv support. Use a fresh directory:

```sh
mkdir -p "$HOME/mcts-download"
tar -xzf mcts-linux.tar.gz -C "$HOME/mcts-download"
python3 "$HOME/mcts-download/mcts/install.py"
export PATH="$HOME/.local/share/mcts/bin:$PATH"
mcts --help
mcts install --target opencode
# Other entry agents:
mcts install --target xiaoo
mcts install --target dsh
```

The installer creates
`~/.local/share/mcts` and `~/.config/mcts/config.json` (mode 0600); it preserves
existing destinations by refusing an overwrite. `--prefix PATH --config PATH`
select other new paths. An alternate configuration must be passed to `mcts
--config PATH`; the standard plugin uses the default configuration. Keep the
prefix in place: its private Python environment has absolute interpreter paths.
Add its `bin` directory to your shell startup file and the environment used to
launch OpenCode. You may remove the unpacked archive after installation.

The OpenCode command installs the plugin and Skill in automatically discovered
directories. The plugin registers the local `mcts mcp` server at startup;
it does not edit existing JSON/JSONC settings. Restart OpenCode after installing.
Use OpenCode 2.3.0; no OpenCode download is included in the bundle. Existing
server name `mcts` or modified plugin files cause an explicit conflict. See the
[plugin guide](plugins/opencode/README.md) for removal and custom config directories.

To remove a user installation, stop its runs, run
`mcts install --target opencode --uninstall`, then remove only the prefix and
configuration you created. Retain or export your run directories first.

## Configuration and credentials

The [configuration sample](cli/examples/config.json) is the unified file schema.
The installer fills absolute paths for the run directory, private Python,
algorithm entries and packaged plugin assets. Config-relative paths are also
accepted. Keep the runs directory private; each run persists its config, state,
events, usage, result and selected artifacts.

`models.solver` and `models.reviewer` describe model/provider/API endpoint and
credential sources. Replace the `.invalid` endpoint and model names for your
deployment. `credential_ref` uses `env:NAME` or `file:/absolute/path`; put the
secret only in that environment variable or a private file, never in JSON.
For example, read an existing secret interactively without storing it in history:

```sh
read -r -s -p 'Solver API key: ' MCTS_SOLVER_API_KEY; echo
export MCTS_SOLVER_API_KEY
read -r -s -p 'E2B API key: ' E2B_API_KEY; echo
export E2B_API_KEY
```

E2B connection settings are `backends[].e2b.api_url`, `domain`, `envd_scheme`,
`credential_ref`, `timeout_secs`, `model`, and `opencode_executable`. Configure the endpoint/domain and template
availability for your E2B deployment. Task entries define an image naming rule,
working directory, default network policy and `instances` repository/full Git
commit pairs. SWE-bench defaults offline; DeepSWE defaults allow_internet. An
explicit effective `config.network_policy` is required in each start request
and is persisted; algorithm requests cannot relax it.

The CLI assembles `driver: "own"`, `kind: "e2b"` using OwnBackend, E2B and
OpenCodeRuntime. `e2b.model` selects a named model (default solver), and
`e2b.opencode_executable` must name an absolute OpenCode 2.3.0 executable.
Credentials are resolved only inside the worker: env references remain host
references; file references are read into a private worker environment variable.
They are never passed into the algorithm environment.

Register each task image's prepared E2B template in `e2b.image_templates`
(image reference → template ID). Offline templates must already contain the
repository, its immutable base commit and test dependencies. The task's
`instances` entry supplies the HTTPS repository and full base commit. Root
preparation, leases, workspace export and the algorithm execution capabilities
are supplied by OwnBackend. Backend auto selection uses the first configured
entry in ThinkProcess, E2B, Docker, worktree order; the selected backend must
satisfy all requirements. See [legacy configuration mapping](run-manager/README.md)
for replaced environment variables.

### Host-side official SWE-bench evaluation

Official evaluation runs on the host, using Docker and the instance images
already present there. It does not run the harness inside the E2B sandbox.
Create a separate private harness environment and set top-level
`swebench_python` to its absolute Python path:

```sh
python3 -m venv "$HOME/.local/share/swebench-harness"
"$HOME/.local/share/swebench-harness/bin/pip" install 'swebench==4.1.0'
```

This installs the harness and its Python dependencies; do not download task
images as part of installation. Keep the harness separate from the bundled
algorithm environment. Version 4.1.0 supports the image namespace and cache
options used by the official evaluator. Missing harness, Docker or instance images produce an
explicit `not_run` reason, rather than a passing evaluation. See
[task guide](tasks/README.md) for result semantics.

## Run from the command line

Prepare an approved JSON request using the
[SWE-bench request sample](cli/examples/swebench-request.json). Set its task
instance, goal, matching repository/base commit in configuration, and the
algorithm's `task.arguments.instance_file` to an absolute local instance JSON
(without the gold patch/test patch). Approve the exact budget before starting:

```sh
mcts run --request request.json
# Save the returned run_id, then use it below.
mcts status RUN_ID
mcts result RUN_ID
mcts list
mcts cancel RUN_ID --reason 'stop requested'
```

The start response is durable acceptance; the background worker survives the
entry command. Use a new start_key for a new run and reuse the same key after
an uncertain acceptance. Budget approval must match config.budget exactly.
Total token budgets account for repeated input context as well as output;
in-flight turns may report tail usage above the allowance. Start with one
iteration, two branches and serial execution. A turn's model-request cap is
configured by the OpenCode runtime; use a bounded overall wall-clock budget too.
When using systemd, launch execution in a persistent unit separate from any
polling script's unit (see [worker lifetime](run-manager/README.md)).

`result` gives a summary and artifact references. Read
`<runs_dir>/<run_id>/selected.patch`, `workspace.tar`, `workspace-summary.json`
and `events.jsonl` after completion. Review the patch before applying it yourself;
the tool never applies it automatically. Cancellation acceptance is separate
from a confirmed cancelled terminal and cleanup. Interrupted/unknown cleanup
requires resource investigation, not assuming success.

## Run from OpenCode

Launch OpenCode with the installed command on PATH. Ask it to use the MCTS
Skill, agree the goal, code source and exact budget, and provide the approved
request's fields. Its tools are `mcts_mcts_start`,
`mcts_mcts_status`, `mcts_mcts_cancel`, `mcts_mcts_result`, and `mcts_mcts_list`.
Save the returned run ID and retrieve the result with the same tools or CLI.
OpenCode is only the entry; the configured run worker owns execution.

The two [installed entry scenarios](../tests/e2e/README.md) exercise the CLI and
real OpenCode MCP catalog with E2B and the configured real solver model. They
check complete delivery/official evaluation and cancellation/cleanup. No
fallback backend is used when production prerequisites are missing.
