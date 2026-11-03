"""Non-interactive root-state preparation CLI for the xg /mcts endpoint.

Unlike ``run_plugin_benchmark --prepare-root-state``, this entrypoint is owned by
the xGovernor server, not by a human debugging the plugin. It prints exactly one
JSON object to stdout so xg can parse the prepared checkpoint without scraping
human-readable logs.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.harness.root_state import (
    RootStateClient,
    RootStateError,
    prepare_root_state,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare a recoverable MCTS root state for the xg /mcts host."
    )
    parser.add_argument("--mode", choices=("zsoft", "sweverified"), default="zsoft")
    parser.add_argument("--task-id", required=True, help="zsoft task id or SWE-bench instance id")
    parser.add_argument("--testbench", choices=("swepro", "swelite", "sweverified"), default="sweverified")
    parser.add_argument("--split", default="test")
    parser.add_argument("--base-url", default=None)
    parser.add_argument("--token", default=None)
    parser.add_argument("--runtime-kind", default=None)
    parser.add_argument("--output-dir", required=True)
    return parser


def _emit_success(details: dict, output_dir: str) -> None:
    payload = {
        "root_state_id": details["root_state_id"],
        "checkpoint_id": details["root_state_id"],
        "output_dir": output_dir,
    }
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def _emit_error(message: str) -> None:
    payload = {"error": "prepare_failed", "message": message}
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


def run(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = str(Path(args.output_dir).resolve())
    try:
        client = RootStateClient(base_url=args.base_url, token=args.token)
        details = prepare_root_state(
            client,
            task_id=args.task_id,
            output_dir=output_dir,
            mode=args.mode,
            runtime_kind=args.runtime_kind,
            testbench=args.testbench,
            split=args.split,
        )
    except RootStateError as error:
        _emit_error(str(error))
        return 1
    except Exception as error:  # keep stdout JSON-only even for unexpected failures
        print(f"xg_root_state_prepare unexpected failure: {error}", file=sys.stderr)
        _emit_error(str(error))
        return 1

    _emit_success(details, output_dir)
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(argv)


if __name__ == "__main__":
    raise SystemExit(main())
