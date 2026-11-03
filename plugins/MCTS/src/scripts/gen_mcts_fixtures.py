"""Generate the v1 golden fixtures shared by MCTS and xGovernor.

Run from the MCTS repository root:

    ./.venv/bin/python -m src.scripts.gen_mcts_fixtures \
      --schema docs/protocols/mcts-rpc-v1.schema.json \
      --out docs/protocols/fixtures
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from src.core.mcts_protocol import notification, request, result


CAPABILITIES = [
    "snapshot",
    "fork",
    "agent_turn",
    "inspect",
    "exec",
    "file_read",
    "file_write",
    "diff",
    "release",
]


def build_messages() -> list[tuple[str, dict[str, Any]]]:
    completion = {
        "status": "completed",
        "summary": "搜索得到一个推荐修复方案，仍有两项测试未通过。",
        "answer": "推荐修改 parser 的状态切换和空输入处理。候选通过 18/20 项测试，剩余两项失败及修改差异见证据。",
        "stop_reason": "budget_exhausted",
        "selected_candidate_id": "node-7",
        "candidates": [
            {
                "candidate_id": "node-7",
                "state_id": "state-007",
                "score": 0.81,
                "evaluation_version": "parser-checks-v1",
                "summary": "修正状态切换和空输入处理",
                "evidence_refs": ["evidence-1", "evidence-2"],
            }
        ],
        "evidence": [
            {
                "evidence_id": "evidence-1",
                "kind": "test_report",
                "content": "18 passed, 2 failed",
            },
            {
                "evidence_id": "evidence-2",
                "kind": "diff",
                "content": "diff --git a/parser.py b/parser.py",
            },
        ],
        "limitations": ["仍有 2 项测试未通过"],
        "apply_plan": {
            "kind": "continue_from_state",
            "state_id": "state-007",
            "base_state_id": "state-001",
        },
    }

    return [
        (
            "plugin.initialize.request.json",
            request(
                "fixture-init",
                "plugin.initialize",
                {
                    "plugin_id": "mcts",
                    "supported_api_versions": ["mcts.rpc.v1"],
                    "host_version": "xgovernor-dev",
                    "available_capabilities": CAPABILITIES,
                },
            ),
        ),
        (
            "plugin.start.request.json",
            request(
                "fixture-start",
                "plugin.start",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "session_id": "session-abc",
                    "turn_id": "turn-17",
                    "root_state_id": "state-001",
                    "goal": "解决当前错误并给出可应用的修改",
                    "content": "当前会话上下文：用户要求修复 parser；已复现空输入导致的异常；修改范围限于 parser 模块。",
                    "budget": {
                        "max_iters": 5,
                        "branching": 3,
                        "max_time_ms": 1_800_000,
                        "max_tokens": 3_000_000,
                    },
                    "granted_capabilities": CAPABILITIES,
                },
            ),
        ),
        (
            "plugin.wait.request.json",
            request(
                "fixture-wait",
                "plugin.wait",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "after_sequence": 0,
                    "timeout_ms": 30_000,
                },
            ),
        ),
        (
            "plugin.completed.notification.json",
            notification(
                "plugin.completed",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "sequence": 7,
                    "result": completion,
                },
            ),
        ),
        (
            "host.fork.request.json",
            request(
                "fixture-fork",
                "host.fork",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "parent_state_id": "state-001",
                    "idempotency_key": "fork-node-7",
                },
            ),
        ),
        (
            "host.agent_turn.request.json",
            request(
                "fixture-turn",
                "host.agent_turn",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 120_000,
                    "branch_id": "branch-7",
                    "input": "分析当前错误，尝试一种修复方案",
                    "profile": "solver-step",
                    "idempotency_key": "node-7-turn-1",
                },
            ),
        ),
        (
            "host.job.completed.notification.json",
            notification(
                "host.job.completed",
                {
                    "run_id": "mcts-run-42",
                    "job_id": "job-9",
                    "sequence": 3,
                    "output": "18 passed, 2 failed",
                    "usage": {"input_tokens": 123, "output_tokens": 17},
                    "exit_code": 0,
                    "truncated": False,
                },
            ),
        ),
        (
            "host.snapshot.request.json",
            request(
                "fixture-snapshot",
                "host.snapshot",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "branch_id": "branch-7",
                    "idempotency_key": "snapshot-node-7",
                },
            ),
        ),
        (
            "host.snapshot.result.json",
            result(
                "fixture-snapshot",
                {"state_id": "state-007", "expires_at": "2026-12-31T23:59:59Z"},
                method="host.snapshot",
            ),
        ),
        (
            "host.fork.result.json",
            result(
                "fixture-fork",
                {"branch_id": "branch-7"},
                method="host.fork",
            ),
        ),
        (
            "host.agent_turn.result.json",
            result(
                "fixture-turn",
                {"job_id": "job-9", "status": "accepted"},
                method="host.agent_turn",
            ),
        ),
        (
            "host.inspect.request.json",
            request(
                "fixture-inspect",
                "host.inspect",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "branch_id": "branch-7",
                    "view": "summary",
                },
            ),
        ),
        (
            "host.inspect.result.json",
            result(
                "fixture-inspect",
                {"content": "selected branch summary"},
                method="host.inspect",
            ),
        ),
        (
            "host.exec.request.json",
            request(
                "fixture-exec",
                "host.exec",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "branch_id": "branch-7",
                    "argv": ["pytest", "-q"],
                    "cwd": "/workspace",
                    "idempotency_key": "exec-node-7",
                },
            ),
        ),
        (
            "host.exec.result.json",
            result(
                "fixture-exec",
                {"job_id": "job-8", "status": "accepted"},
                method="host.exec",
            ),
        ),
        (
            "host.file_read.request.json",
            request(
                "fixture-file-read",
                "host.file_read",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "branch_id": "branch-7",
                    "path": "/workspace/README.md",
                },
            ),
        ),
        (
            "host.file_read.result.json",
            result(
                "fixture-file-read",
                {"content_base64": "bW9jay1maWxlLWNvbnRlbnQ="},
                method="host.file_read",
            ),
        ),
        (
            "host.file_write.request.json",
            request(
                "fixture-file-write",
                "host.file_write",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "branch_id": "branch-7",
                    "path": "/workspace/output.txt",
                    "content_base64": "aGVsbG8=",
                    "idempotency_key": "file-write-node-7",
                },
            ),
        ),
        (
            "host.file_write.result.json",
            result(
                "fixture-file-write",
                {"written_bytes": 5},
                method="host.file_write",
            ),
        ),
        (
            "host.diff.request.json",
            request(
                "fixture-diff",
                "host.diff",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "branch_id": "branch-7",
                    "base_state_id": "state-001",
                },
            ),
        ),
        (
            "host.diff.result.json",
            result(
                "fixture-diff",
                {"content": "diff --git a/parser.py b/parser.py"},
                method="host.diff",
            ),
        ),
        (
            "host.release.request.json",
            request(
                "fixture-release",
                "host.release",
                {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "timeout_ms": 30_000,
                    "branch_id": "branch-7",
                    "idempotency_key": "release-node-7",
                },
            ),
        ),
        (
            "host.release.result.json",
            result(
                "fixture-release",
                {"released": True},
                method="host.release",
            ),
        ),
        (
            "invalid.missing_timeout.json",
            {
                "jsonrpc": "2.0",
                "id": "fixture-invalid",
                "method": "host.fork",
                "params": {
                    "plugin_id": "mcts",
                    "run_id": "mcts-run-42",
                    "parent_state_id": "state-001",
                    "idempotency_key": "fork-node-7",
                },
            },
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    if not args.schema.is_file():
        raise SystemExit(f"schema not found: {args.schema}")

    args.out.mkdir(parents=True, exist_ok=True)
    for filename, message in build_messages():
        target = args.out / filename
        target.write_text(
            json.dumps(message, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"wrote {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
