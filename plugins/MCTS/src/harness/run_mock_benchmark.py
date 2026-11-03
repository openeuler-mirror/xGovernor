"""Run a local mock benchmark through the portable MCTS core.

Unlike the stdio plugin, this harness keeps the host in-process so the whole
search is deterministic and has no xGovernor dependency.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from src.core.host_api import RunContext
from src.core.mcts_coordinator import Coordinator, Node, iter_tree
from src.core.mcts_protocol import notification
from src.core.result_builder import ResultBuilder
from src.plugin.executor import BenchmarkStrategy
from src.res.task_plugins.sweverified.scorer import pytest as swe_pytest

from .scripted_host import ScriptedHost


class MockBenchmarkTask:
    class Spec:
        oracle_rel_path = "testcases.py"

    spec = Spec()

    def __init__(self) -> None:
        self.oracle_host_path = Path("/tmp/xiaoo-day3-mock/testcases.py")

    def bootstrap_prepare(self, host, branch_id) -> None:
        return None

    def materialize_script(self) -> str:
        return "echo materialized"

    def init_author(self) -> str:
        return "author: create tests"

    def init_solver(self) -> str:
        return "solver: initial solution"

    def step_solver(self, prior: dict[str, Any]) -> str:
        return "solver: use benchmark evidence"

    def step_author(self, prior: dict[str, Any], node_id: str = "") -> str:
        return "author: revise tests"


class MockExecutor:
    def __init__(
        self,
        params: dict[str, Any],
        host: ScriptedHost,
        strategy: BenchmarkStrategy,
    ) -> None:
        self.params = params
        self.host = host
        self.strategy = strategy
        self.budget = params["budget"]
        self.context = RunContext(
            params["plugin_id"],
            params["run_id"],
            params["root_state_id"],
            set(params["granted_capabilities"]),
        )
        self.deadline = time.monotonic() + self.budget["max_time_ms"] / 1000
        self.tokens = 0
        self.events: list[dict[str, Any]] = []

    def remaining_ms(self) -> int:
        return max(1, int((self.deadline - time.monotonic()) * 1000))

    def check(self) -> None:
        if self.context.cancelled:
            raise RuntimeError("mock run cancelled")
        if time.monotonic() >= self.deadline:
            raise TimeoutError("run time budget exhausted")

    def emit(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        event = notification(
            method,
            {
                "plugin_id": self.context.plugin_id,
                "run_id": self.context.run_id,
                "sequence": len(self.events) + 1,
                **payload,
            },
        )
        self.events.append(event)
        return event

    def wait_job(self, receipt: dict[str, Any]) -> dict[str, Any]:
        self.check()
        job = self.host.jobs.get(receipt["job_id"])
        if not isinstance(job, dict):
            raise RuntimeError(f"unknown job: {receipt}")
        if job.get("status") == "failed":
            raise RuntimeError(f"host job failed: {job.get('failure')}")
        if job.get("status") != "completed":
            raise RuntimeError(f"host job did not complete: {job}")
        usage = job.get("usage") or {}
        self.tokens += int(
            usage.get(
                "total_tokens",
                usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
            )
        )
        return job

    def step(self, parent: Node, child: Node, host: ScriptedHost) -> Any:
        return self.strategy.expand(self, parent, child, host)


def mock_params(max_iters: int, branching: int) -> dict[str, Any]:
    return {
        "plugin_id": "mcts",
        "run_id": "mock-run",
        "session_id": "session-mock",
        "turn_id": "turn-mock",
        "root_state_id": "state-root",
        "goal": "mock search",
        "content": "mock content",
        "budget": {
            "max_iters": max_iters,
            "branching": branching,
            "max_time_ms": 60_000,
            "max_tokens": 1_000_000,
        },
        "granted_capabilities": [
            "snapshot",
            "fork",
            "agent_turn",
            "inspect",
            "exec",
            "file_read",
            "file_write",
            "diff",
            "release",
        ],
    }


def run_mock_search(
    *,
    max_iters: int = 3,
    branching: int = 1,
    output_dir: str | Path | None = None,
) -> tuple[dict[str, Any], ScriptedHost, list[dict[str, Any]]]:
    if max_iters <= 0 or branching <= 0:
        raise ValueError("max_iters and branching must be positive")
    params = mock_params(max_iters, branching)
    host = ScriptedHost(run_id=params["run_id"])
    strategy = BenchmarkStrategy(
        MockBenchmarkTask(),
        swe_pytest,
        oracle=None,
        evaluation_version="mock-zsoft:v1",
    )
    executor = MockExecutor(params, host, strategy)
    trace_lines = [
        "▶ task  mode=mock-zsoft  strategy=mock-zsoft",
        "▶ startup  mode=mock-zsoft",
    ]

    executor.emit("plugin.started", {"summary": "MCTS mock search started"})
    root = Node("root", state_id=params["root_state_id"])
    try:
        last_parent = [None]
        iteration = [-1]

        def logged_step(parent: Node, child: Node, search_host: ScriptedHost):
            if parent.node_id != last_parent[0]:
                last_parent[0] = parent.node_id
                iteration[0] += 1
                trace_lines.append(
                    f"▶ choose  iter={iteration[0]}  node={parent.node_id}"
                )
            expansion = executor.step(parent, child, search_host)
            trace_lines.append(
                f"    {child.node_id} score={expansion.score:.3f}"
            )
            return expansion

        best = Coordinator(
            root,
            host=host,
            run_context=executor.context,
            step=logged_step,
            max_expansions=max_iters + 1,
            branching=branching,
        ).run()

        nodes = [node for node in iter_tree(root) if node is not root and node.state_id]
        selected = max(nodes, key=lambda node: node.score, default=None)
        trace_lines.append(
            "▶ first_layer  "
            + "  ".join(
                f"{node.node_id}={node.score:.3f}" for node in root.children
            )
        )
        trace_lines.append(
            "▶ selected  "
            + (
                f"node={selected.node_id} score={selected.score:.3f}"
                if selected
                else "none"
            )
        )
        evidence = [
            {
                "evidence_id": node.node_id,
                "kind": "test_report",
                "content": node.feedback,
            }
            for node in nodes
        ]
        candidates = [
            {
                "candidate_id": node.node_id,
                "state_id": node.state_id,
                "score": node.score,
                "evaluation_version": "mock-zsoft:v1",
                "summary": node.feedback,
                "evidence_refs": [node.node_id],
            }
            for node in nodes
        ]
        result = ResultBuilder(root_state_id=params["root_state_id"]).build(
            answer=selected.solution if selected else best.solution,
            summary=f"Evaluated {len(nodes)} mock candidates",
            stop_reason="node_budget",
            candidates=candidates,
            evidence=evidence,
            selected_candidate_id=selected.node_id if selected else None,
            apply_plan=(
                {
                    "kind": "continue_from_state",
                    "state_id": selected.state_id,
                    "base_state_id": params["root_state_id"],
                }
                if selected
                else None
            ),
            limitations=["local mock harness run"],
        )
        executor.emit("plugin.completed", {"result": result})

        if output_dir is not None:
            out = Path(output_dir)
            out.mkdir(parents=True, exist_ok=True)
            (out / "search.log").write_text(
                "\n".join(trace_lines) + "\n",
                encoding="utf-8",
            )
            (out / "protocol.jsonl").write_text(
                "\n".join(json.dumps(event, ensure_ascii=False) for event in executor.events)
                + "\n",
                encoding="utf-8",
            )
            (out / "result.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        return result, host, executor.events
    finally:
        if "branch-root" in host.branches:
            host.release(
                "branch-root",
                timeout_ms=executor.remaining_ms(),
                idempotency_key=f"{params['run_id']}:root:release",
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", choices=("mock-zsoft",), default="mock-zsoft")
    parser.add_argument("--max-iters", type=int, default=3)
    parser.add_argument("--branching", type=int, default=1)
    parser.add_argument(
        "--output-dir",
        default="src/output/mock-zsoft/ad-hoc",
    )
    args = parser.parse_args(argv)

    result, host, _ = run_mock_search(
        max_iters=args.max_iters,
        branching=args.branching,
        output_dir=args.output_dir,
    )
    if host.live_branch_count != 0:
        raise SystemExit(f"live branches remained after mock run: {host.live_branch_count}")
    print(json.dumps({"status": result["status"], "answer": result["answer"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
