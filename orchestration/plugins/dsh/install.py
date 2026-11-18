#!/usr/bin/env python3
"""Install the dsh MCTS MCP patch and Skill, preserving other patch bytes."""
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


BEGIN = "# BEGIN xgovernor-mcts\n"
END = "# END xgovernor-mcts\n"


def install(config_dir: Path | None = None, *, uninstall: bool = False) -> None:
    root = (Path(config_dir) if config_dir is not None else Path(os.environ.get("DSH_HOME", Path.home() / ".dsh"))).expanduser().absolute()
    state_path = root / STATE
    state = load_state(state_path, "dsh")
    patch_path = root / "cordis.patch.yml"
    original = read(patch_path)
    patch = original.decode() if original is not None else ""
    skill = root / "skills/mcts/SKILL.md"
    current_skill = read(skill)
    block = BEGIN + (HERE / "cordis.patch.yml").read_text() + END
    if state is None:
        if uninstall:
            return
        if current_skill is not None or BEGIN in patch or "mcp-mcts" in patch or "serverName: mcts" in patch or '"serverName": "mcts"' in patch:
            raise ValueError("Preserving existing mcts entry or Skill")
        # An empty YAML sequence cannot be followed by additional sequence rows.
        lines = patch.splitlines(keepends=True)
        meaningful = [line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#")]
        if meaningful and meaningful != ["[]"] and not meaningful[0].startswith("-"):
            raise ValueError("Use a block sequence for the dsh patch before installing")
        base = "".join(line for line in lines if line.strip() != "[]") if meaningful == ["[]"] else patch
        if base and not base.endswith("\n"):
            base += "\n"
    else:
        owned = state["block"]
        if patch.count(owned) != 1 or (current_skill is not None and digest(current_skill) != state["skill"]):
            raise ValueError("Preserving modified mcts entry or Skill")
        base = patch.replace(owned, "", 1)
    if uninstall:
        if base == state["base"]:
            if state["original"] is None:
                patch_path.unlink()
            else:
                replace(patch_path, state["original"].encode())
        else:
            replace(patch_path, base.encode())
        safe(skill).unlink(missing_ok=True)
        state_path.unlink()
        return
    data = SKILL.read_bytes()
    replace(patch_path, (base + block).encode())
    replace(skill, data)
    replace(state_path, (json.dumps({"package": "xgovernor-mcts-dsh", "block": block,
        "skill": digest(data), "base": state["base"] if state else base,
        "original": state["original"] if state else (original.decode() if original is not None else None)}, indent=2) + "\n").encode())


if __name__ == "__main__":
    main()
