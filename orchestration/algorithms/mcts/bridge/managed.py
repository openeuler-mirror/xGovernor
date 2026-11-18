"""Run configuration and event bridge for the durable stdio host."""
from __future__ import annotations

import json
import os
from pathlib import Path

import jsonschema

from xgovernor_mcts.bridge.lifecycle import RunRegistry
from xgovernor_mcts.bridge.launch_spec import resolve_profile_source
from xgovernor_mcts.bridge.server import LifecycleServer
from xgovernor_mcts.bridge.stdio_transport import StdioTransport
from xgovernor_mcts.engine.executor import BenchmarkStrategy, RunExecutor
from xgovernor_tasks.common.mode import MODES

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "resources" / "run-config-v1.schema.json"


def main():
    request = json.loads(Path("config.json").read_text())
    algorithm = request["config"]["algorithm"]
    if (algorithm["namespace"] != "mcts" or algorithm["version"] != "1"
            or algorithm["schema"] != "urn:xgovernor:mcts:run-config:1"):
        raise ValueError("expected MCTS configuration version 1")
    parameters = algorithm["parameters"]
    jsonschema.validate(parameters, json.loads(SCHEMA_PATH.read_text()))
    temporary = Path("tmp").resolve()
    temporary.mkdir(exist_ok=True)
    os.environ["TMPDIR"] = str(temporary)
    task_config = parameters["task"]
    spec = MODES.get(task_config["mode"])

    def strategy_factory(params):
        arguments = dict(task_config.get("arguments", {}))
        arguments.setdefault("goal", params["goal"])
        arguments.setdefault("task_id", params["run_id"])
        if spec.id == "sweverified":
            arguments.setdefault("testbench", "sweverified")
            arguments["prepared_root"] = True
            runtime = json.loads(Path("task-runtime.json").read_text())
            arguments["official_python"] = runtime.get("swebench_python")
        if spec.id == "deepswe":
            arguments.update(oracle_dir=Path("oracles"), root_state_id=params["root_state_id"])
        task = spec.loader()(spec, **arguments)
        return BenchmarkStrategy(task, spec.scorer_fn(), oracle=getattr(task, "oracle_host_path", None),
                                 evaluation_version=f"{spec.id}:{spec.scorer}:v1")

    def executor_factory(params, transport, emit, *, strategy):
        executor = RunExecutor(params, transport, emit, strategy=strategy, managed_parameters=parameters)
        executor.host.profile_prompt = lambda profile: resolve_profile_source(spec.id, profile).read_text()
        return executor

    LifecycleServer(StdioTransport(), RunRegistry(), executor_factory=executor_factory,
                    strategy_factory=strategy_factory).serve_forever()
