"""Minimal host driver for local MCTS stdio handshake."""

from __future__ import annotations

import argparse
import json
import select
import shlex
import subprocess
import sys

from src.core.mcts_protocol import PROTOCOL_VERSION

ALL_CAPABILITIES = [
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


def initialize_request(plugin_id: str) -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": "stdio-init",
        "method": "plugin.initialize",
        "params": {
            "plugin_id": plugin_id,
            "supported_api_versions": [PROTOCOL_VERSION],
            "host_version": "xgovernor-dev",
            "available_capabilities": ALL_CAPABILITIES,
        },
    }


def read_line(stream, timeout_s: float) -> str:
    ready, _, _ = select.select([stream], [], [], timeout_s)
    if not ready:
        raise TimeoutError(f"plugin did not respond within {timeout_s:.3f}s")
    line = stream.readline()
    if not line:
        raise EOFError("plugin stdout closed before returning a response")
    return line


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plugin", required=True, help="shell command to launch the plugin")
    parser.add_argument("--plugin-id", default="mcts")
    parser.add_argument("--timeout-ms", type=int, default=30_000)
    args = parser.parse_args(argv)

    command = shlex.split(args.plugin)
    print(f"[stdio-driver] spawn: {command!r}", file=sys.stderr)
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    try:
        request = initialize_request(args.plugin_id)
        payload = json.dumps(request, separators=(",", ":")) + "\n"
        assert process.stdin is not None
        process.stdin.write(payload)
        process.stdin.flush()

        assert process.stdout is not None
        line = read_line(process.stdout, args.timeout_ms / 1000)
        response = json.loads(line)
        print(json.dumps(response, ensure_ascii=False, indent=2))
        if response.get("id") != request["id"]:
            raise RuntimeError(f"unexpected response id: {response.get('id')}")
        if "error" in response:
            return 1
        return 0
    finally:
        if process.stdin:
            try:
                process.stdin.close()
            except Exception:
                pass
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()


if __name__ == "__main__":
    raise SystemExit(main())
