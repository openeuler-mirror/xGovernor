# MCTS algorithm

Python 3.11+ package `xgovernor-mcts`. Search scheduling is in `engine/`, shared memory in `memory/`, stdio lifecycle and host clients in `bridge/`, and offline drivers in `benchmarks/`.

From the repository root, install the three Python packages together:

```sh
python3 -m pip install ./orchestration/tasks ./orchestration/algorithms/mcts ./orchestration/legacy/opencode-host
python3 -m unittest discover -s orchestration/algorithms/mcts/tests -v
```

The host starts `python3 -m xgovernor_mcts.engine.main --mode sweverified --instance-id <id>` over stdio. SWE dataset loading additionally requires `datasets`. The algorithm uses host capabilities for execution and does not hold model or sandbox credentials. Task registration and profile resources use the installed `xgovernor-tasks` package.

The optional `run_union.sh` wrapper reads `config.env` beside the script; start it with the Python environment containing these installed packages.

For durable runs, set `algorithms_dir` to `orchestration/algorithms` and install
all three packages in the `python3` environment used by run management. The
manager invokes `mcts/__main__.py` in the private run directory. It reads that
run's persisted `config.json`; no legacy environment or `config.env` is read.
Use `config.algorithm.namespace: "mcts"`, `version: "1"`,
`schema: "urn:xgovernor:mcts:run-config:1"` and parameters such as:

```json
{
  "max_iters": 1,
  "branching": 1,
  "max_concurrent": 1,
  "finish_reserve_ratio": 0.1,
  "task": {"mode": "generic", "arguments": {"task_id": "repair"}}
}
```

The parameter schema is [run-config-v1.schema.json](resources/run-config-v1.schema.json).
Task arguments go to the existing task loader; a generic task uses the run goal.
Search and evaluation continue to use the existing task implementations.

Run management owns the budget. In managed runs, remaining tokens equal the
`plugin.start` budget minus usage from distinct `host.job.completed/failed`
notifications, the same source used by the manager. Each managed job has one
immutable terminal usage record, so its job ID is charged once even if the
notification or receipt is replayed; legacy hosts retain sequence-based accounting. There is no environment
fallback or separate algorithm token allowance. Expansion stops when remaining
tokens or remaining time reach the finishing reserve (default 10% of each
starting allowance, configurable from 0 to 1 via `finish_reserve_ratio`). The
manager subtracts root preparation and startup time before sending the time
allowance in `plugin.start`; the algorithm reserves time from that allowance.
Already admitted work may report tail usage. Selection, export and official
evaluation continue; the manager refuses new model turns at the token hard
limit, while file/command/snapshot operations remain available for finishing.
Selection retains its existing fallback if a model turn is refused. The run's
time limit and cancellation remain authoritative, including during finishing.

The first stdio version transports structured events in the existing progress
summary with the prefix
`mcts.event.v1 ` followed by a JSON contract event payload. A dedicated event
transport awaits a future protocol version. Before persistence the manager
strictly validates the entire envelope against the installed events schema;
invalid or unknown payloads fail the run and are never written as public events.
The manager persists
node, score, selection and artifact events using the common contract types;
MCTS expansion, oracle revisions and stop details use version 1 of the
[MCTS event extension schema](resources/events-v1.schema.json). Legacy progress
notifications remain namespaced extensions. Lifecycle events belong to the
manager. Artifact references are published by the manager after export.

Managed host call deadlines are bounded by the run's remaining time: forks and
snapshots allow up to 120 seconds and other short calls up to 30 seconds.
Agent turns and exec jobs use the manager's `job_timeout_ms` ceiling (default
1800000, 30 minutes), with no additional algorithm-side ten-minute cap.
Accepted jobs still run under the overall time budget.
The manager prepares the root with up to 120 seconds within that same budget.
The hard time deadline cancels execution, including finishing; the reserve is
an early expansion stop, not an extension of the approved deadline. Already
admitted work can consume that reserve, so export is not guaranteed after the
hard deadline or explicit cancellation.

A failed host job during an Author or Solver turn is recorded as a failed node
with a `turn_failed` extension event. Failed expansion paths have no candidate
state or score and are not expanded again; other paths continue. A failed Author
revision retains the previous oracle. If the initial Author cannot produce an
oracle, Solver paths cannot be evaluated. Finishing selects only evaluated
candidates and uses the usual export and official evaluation steps. With none,
the result completes with `stop_reason=no_candidates`, an empty candidate list,
and the failure reasons; it does not invent a solution or test score.
