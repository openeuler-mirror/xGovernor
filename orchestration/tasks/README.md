# Coding tasks

`xgovernor-tasks` contains task loading, materialization, scoring, artifact export and official evaluation. Install it together with the algorithm and legacy host packages as described in [the algorithm README](../algorithms/mcts/README.md).

| Directory | Mode | Scope |
| --- | --- | --- |
| `generic/` | `generic` | Author-generated verify.sh and repository artifacts |
| `swe_bench/` | `sweverified` | Verified, Lite and Pro loading; official evaluation supports Verified and Lite |
| `zsoft/` | `zsoft` | External task bundles, trial and official evaluation; configure ZSOFT_TASKS_DIR |
| `deepswe/` | `deepswe` | OpenCode task-image workflow and submodule fixtures |
| `common/` | — | Registration and execution/scoring helpers |

The current task interface includes algorithm node objects, role prompts and a DeepSWE adapter derived from the legacy OpenCode host. Install all three packages together to load every registered mode.

Execution idempotency keys use a branch or immutable-state scope and a stable
logical operation identity. Scoring and artifact reads run on dedicated forks;
retries on the same fork reuse their keys, while a new evaluation uses a new
fork. Command payloads, paths and Author requests distinguish operations within
one scope; wait budgets and wall-clock timestamps do not identify an operation.
Official evaluation uses a stable branch/node run ID so retrying it preserves
the harness command. Backend argument-conflict checks remain authoritative.
