#!/usr/bin/env python3
"""Install the xiaoO MCTS MCP entry and user Skill; preserve unrelated settings."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile

HERE = Path(__file__).resolve().parent
STATE = ".xgovernor-mcts-install.json"
SKILL = HERE.parents[1] / "skills/mcts/SKILL.md"


def safe(path: Path) -> Path:
    for part in [path, *path.parents]:
        if part.is_symlink():
            raise ValueError(f"Refusing symlink: {part}")
    return path


def read(path: Path) -> bytes | None:
    safe(path)
    return path.read_bytes() if path.exists() else None


def replace(path: Path, data: bytes) -> None:
    safe(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".mcts-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_state(path: Path, platform: str):
    data = read(path)
    state = json.loads(data) if data is not None else None
    if state is not None and state.get("package") != "xgovernor-mcts-" + platform:
        raise ValueError("Unrecognized MCTS installation manifest")
    return state


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", type=Path)
    parser.add_argument("--uninstall", action="store_true")
    args = parser.parse_args()
    try:
        install(args.config_dir, uninstall=args.uninstall)
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, f"{error}\n")


def install(config_dir: Path | None = None, *, uninstall: bool = False) -> None:
    root = (Path(config_dir) if config_dir is not None else Path.home() / ".config/xiaoo").expanduser().absolute()
    state_path = root / STATE
    state = load_state(state_path, "xiaoo")
    config_path = root / "mcp.json"
    original = read(config_path)
    config = json.loads(original) if original is not None else {}
    servers = config.setdefault("mcpServers", {})
    if not isinstance(servers, dict):
        raise ValueError("mcpServers must be an object")
    entry = json.loads((HERE / "mcp.json").read_text())["mcpServers"]["mcts"]
    skill = Path.home() / ".xiaoo/skills/mcts/SKILL.md"
    current_skill = read(skill)
    # Preflight every owned destination before changing any of them.
    if state is None:
        if uninstall:
            return
        if "mcts" in servers or current_skill is not None:
            raise ValueError("Preserving existing mcts server or Skill")
        toml_path = root / "config.toml"
        if read(toml_path) is not None:
            import tomllib
            toml = tomllib.loads(toml_path.read_text())
            if any(s.get("name") == "mcts" for s in toml.get("mcp", {}).get("servers", [])):
                raise ValueError("Preserving existing TOML mcts server")
    else:
        if servers.get("mcts") != state["entry"] or (current_skill is not None and digest(current_skill) != state["skill"]):
            raise ValueError("Preserving modified mcts server or Skill")
    if uninstall:
        del servers["mcts"]
        # Restore exact original bytes when no unrelated JSON values changed.
        if config == state["before"]:
            if state["original"] is None:
                config_path.unlink()
            else:
                replace(config_path, state["original"].encode())
        else:
            replace(config_path, (json.dumps(config, indent=2) + "\n").encode())
        safe(skill).unlink(missing_ok=True)
        state_path.unlink()
        return
    data = SKILL.read_bytes()
    before = state["before"] if state else json.loads(json.dumps(config))
    servers["mcts"] = entry
    replace(config_path, (json.dumps(config, indent=2) + "\n").encode())
    replace(skill, data)
    replace(state_path, (json.dumps({"package": "xgovernor-mcts-xiaoo", "entry": entry,
        "skill": digest(data), "before": before,
        "original": state["original"] if state else (original.decode() if original is not None else None)}, indent=2) + "\n").encode())


if __name__ == "__main__":
    main()
