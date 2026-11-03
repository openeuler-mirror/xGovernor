"""Two-stage plugin benchmark driver.

Stage 1 prepares an xGovernor checkpoint that is safe for MCTS to use as
``root_state_id``. Stage 2 submits that checkpoint to the xg ``/mcts`` HTTP
endpoint and waits for the plugin process to reach a terminal state.

The driver never embeds private judge credentials or LLM credentials in the
plugin request. Those remain in the xg process, which owns the plugin's
granted capabilities and LLM policy.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import timezone
from datetime import datetime as dt
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.harness.root_state import (
    RootStateClient,
    RootStateError,
    prepare_root_state,
    read_root_state_id,
)


TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
COMPLETION_FILE = "completion.json"
RUN_VIEW_FILE = "run.json"
PROTOCOL_FILE = "protocol.jsonl"
PROTOCOL_UNAVAILABLE_FILE = "protocol_unavailable.txt"


def _utc_now_iso() -> str:
    return dt.now(timezone.utc).isoformat()


def _default_capabilities() -> list[str]:
    return [
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


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def build_mcts_request(
    *,
    task_id: str,
    root_state_id: str,
    mode: str,
    run_id: str,
    goal: str,
    content: str,
    budget: dict[str, int],
    granted_capabilities: list[str],
    split: str | None = None,
    testbench: str | None = None,
) -> dict[str, Any]:
    """Build the //mcts request consumed by xg's ``MctsRunRequest``."""
    request: dict[str, Any] = {
        "plugin_id": "mcts",
        "mode": mode,
        "run_id": run_id,
        "root_state_id": root_state_id,
        "goal": goal,
        "content": content,
        "budget": budget,
        "granted_capabilities": granted_capabilities,
    }
    if mode == "zsoft":
        request["task_id"] = task_id
    elif mode == "sweverified":
        request["instance_id"] = task_id
        if split:
            request["split"] = split
        if testbench:
            request["testbench"] = testbench
    return request


def prepare_root_state_cli(
    args: argparse.Namespace,
    client: RootStateClient,
) -> None:
    details = prepare_root_state(
        client,
        task_id=args.task_id,
        output_dir=args.output_dir,
        mode=args.mode,
        runtime_kind=args.runtime_kind,
        testbench=getattr(args, "testbench", "sweverified"),
        split=getattr(args, "split", "test"),
    )
    print(json.dumps(details, ensure_ascii=False, indent=2))


def call_mcts(
    args: argparse.Namespace,
    client: RootStateClient,
) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    root_state_id = read_root_state_id(args.root_state_id_file)
    run_id = args.run_id or f"mcts-{uuid4()}"
    budget = {
        "max_iters": args.max_iters,
        "branching": args.branching,
        "max_time_ms": args.max_time_ms,
        "max_tokens": args.max_tokens,
    }
    request = build_mcts_request(
        task_id=args.task_id,
        root_state_id=root_state_id,
        mode=args.mode,
        run_id=run_id,
        goal=args.goal or f"Solve {args.task_id}",
        content=args.content or "Run the MCTS search in the prepared root state.",
        budget=budget,
        granted_capabilities=args.capabilities.split(",")
        if getattr(args, "capabilities", None)
        else _default_capabilities(),
        split=getattr(args, "split", None),
        testbench=getattr(args, "testbench", None),
    )

    created = client.post("/api/v1/mcts/runs", request)
    run_path = f"/api/v1/mcts/runs/{run_id}"
    deadline = time.monotonic() + args.timeout_s
    poll_interval = args.poll_interval_s
    view = created

    while True:
        status = str(view.get("status", "")).lower()
        if status in TERMINAL_STATUSES:
            break
        if time.monotonic() >= deadline:
            try:
                client.post(f"{run_path}/cancel", {})
            except Exception:
                pass
            raise RootStateError(
                f"timed out waiting for MCTS run {run_id}; last status={view.get('status')}"
            )
        time.sleep(poll_interval)
        view = client.get(run_path)

    status = str(view.get("status", "")).lower()
    _write_json(output_dir / RUN_VIEW_FILE, view)
    completion: dict[str, Any] = {
        "status": status,
        "run_id": run_id,
        "completed_at": _utc_now_iso(),
        "outcome": view.get("outcome"),
        "result": (view.get("outcome") or {}).get("result"),
        "request": request,
    }
    _write_json(output_dir / COMPLETION_FILE, completion)

    protocol_text = view.get("protocol_jsonl")
    if isinstance(protocol_text, str) and protocol_text:
        (output_dir / PROTOCOL_FILE).write_text(protocol_text, encoding="utf-8")
    else:
        (output_dir / PROTOCOL_UNAVAILABLE_FILE).write_text(
            "xg /api/v1/mcts/runs/:run_id did not return a protocol_jsonl field.\n",
            encoding="utf-8",
        )

    if status == "completed":
        return 0
    raise RootStateError(json.dumps(view.get("outcome") or view, ensure_ascii=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare a root state and/or run the MCTS plugin via xGovernor /mcts."
    )
    parser.add_argument("--mode", choices=("zsoft", "sweverified"), default="zsoft")
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--runtime-kind", default=None)
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--testbench", choices=("swepro", "swelite", "sweverified"), default="sweverified")
    parser.add_argument("--split", default="test")

    stage = parser.add_mutually_exclusive_group(required=True)
    stage.add_argument("--prepare-root-state", action="store_true")
    stage.add_argument("--call-mcts", action="store_true")

    parser.add_argument(
        "--root-state-id-file",
        default=None,
        help="file written by --prepare-root-state; defaults to <output-dir>/root_state_id",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--goal", default=None)
    parser.add_argument("--content", default=None)
    parser.add_argument("--max-iters", type=int, default=5)
    parser.add_argument("--branching", type=int, default=3)
    parser.add_argument("--max-time-ms", type=int, default=1_800_000)
    parser.add_argument("--max-tokens", type=int, default=3_000_000)
    parser.add_argument("--capabilities", default=None)
    parser.add_argument("--timeout-s", type=int, default=1_800)
    parser.add_argument("--poll-interval-s", type=float, default=2.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = RootStateClient(base_url=args.base_url, token=args.token)
    if args.root_state_id_file is None:
        args.root_state_id_file = str(Path(args.output_dir) / "root_state_id")
    if args.prepare_root_state:
        prepare_root_state_cli(args, client)
        return 0
    if args.call_mcts:
        return call_mcts(args, client)
    raise AssertionError("no stage selected")


if __name__ == "__main__":
    raise SystemExit(main())
