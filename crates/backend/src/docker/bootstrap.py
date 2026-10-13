"""Container-only initialization, invoked by the provider before exposing tools."""
import os
import sys
from pathlib import Path

workspace = Path(sys.argv[1])
# Bootstrap deliberately does not recursively change ownership of system trees.
if workspace == Path('/') or workspace.is_symlink():
    raise ValueError('bootstrap requires a non-root, non-symlink workspace directory')
workspace.mkdir(parents=True, exist_ok=True)
if not workspace.is_dir():
    raise ValueError('workspace is not a directory')

helper = Path('/usr/local/lib/xgovernor/helper.py')
helper.parent.mkdir(parents=True, exist_ok=True)
# Root owns this directory in supported images; the agent cannot replace the helper.
if helper.is_symlink():
    helper.unlink()
helper.write_bytes(sys.stdin.buffer.read())
helper.chmod(0o555)
python = Path('/usr/local/bin/python3')
python.parent.mkdir(parents=True, exist_ok=True)
if os.path.realpath(python) != os.path.realpath(sys.executable):
    if python.exists() or python.is_symlink():
        python.unlink()
    python.symlink_to(sys.executable)

home = Path('/home/agent')
home.mkdir(parents=True, exist_ok=True)
for root in (workspace, home):
    # Never follow repository symlinks into other parts of the image.
    paths = [root]
    for directory, dirs, files in os.walk(root, followlinks=False):
        paths.extend(Path(directory) / name for name in dirs + files)
    for path in paths:
        info = path.lstat()
        if info.st_uid != 10001 or info.st_gid != 10001:
            os.chown(path, 10001, 10001, follow_symlinks=False)
print('xgovernor bootstrap complete')
