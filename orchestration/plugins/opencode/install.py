#!/usr/bin/env python3
"""Install or remove the local OpenCode MCTS plugin without editing user config."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile

HERE = Path(__file__).resolve().parent
FILES = {
    "plugins/mcts.js": HERE / "mcts.js",
    "skills/mcts/SKILL.md": HERE.parents[1] / "skills/mcts/SKILL.md",
}
STATE = ".xgovernor-mcts-install.json"


def default_config_dir() -> Path:
    return Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "opencode"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_path(root: Path, relative: str) -> Path:
    path = root / relative
    for part in [path, *path.parents]:
        if part.is_symlink():
            raise ValueError(f"Refusing symlink: {part}")
        if part == root:
            break
    return path


def replace(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".mcts-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def install(config_dir: Path | None = None, *, uninstall: bool = False) -> None:
    root = Path(config_dir) if config_dir is not None else default_config_dir()
    root = root.expanduser().absolute()
    state_path = safe_path(root, STATE)
    state = json.loads(state_path.read_text()) if state_path.exists() else None
    if state is not None and (
        state.get("package") != "xgovernor-mcts"
        or not isinstance(state.get("files"), dict)
        or set(state["files"]) != set(FILES)
    ):
        raise ValueError("Unrecognized MCTS installation manifest")
    # Check every destination before changing any file, including on uninstall.
    for relative in FILES:
        path = safe_path(root, relative)
        if path.exists():
            if state is None or digest(path.read_bytes()) != state["files"][relative]:
                raise ValueError(f"Preserving existing or modified file: {path}")
    if uninstall:
        if state is None:
            return
        for relative in FILES:
            safe_path(root, relative).unlink(missing_ok=True)
        state_path.unlink()
        return
    contents = {relative: source.read_bytes() for relative, source in FILES.items()}
    for relative, data in contents.items():
        replace(safe_path(root, relative), data)
    replace(state_path, (json.dumps({
        "package": "xgovernor-mcts",
        "files": {relative: digest(data) for relative, data in contents.items()},
    }, indent=2) + "\n").encode())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-dir", type=Path)
    parser.add_argument("--uninstall", action="store_true")
    args = parser.parse_args()
    try:
        install(args.config_dir, uninstall=args.uninstall)
    except (OSError, ValueError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
