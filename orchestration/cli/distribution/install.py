#!/usr/bin/env python3
"""Install this native mcts bundle into a new directory owned by the current user."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prefix', type=Path, default=Path.home() / '.local/share/mcts')
    parser.add_argument('--config', type=Path, default=Path.home() / '.config/mcts/config.json')
    args = parser.parse_args()
    if sys.version_info < (3, 11):
        parser.error('Python 3.11+ with venv is required')
    source = Path(__file__).resolve().parent
    prefix = args.prefix.expanduser().absolute()
    config_path = args.config.expanduser().absolute()
    manifest = json.loads((source / 'distribution.json').read_text())
    if manifest['python'] != list(sys.version_info[:2]):
        parser.error('bundle requires the same Python major/minor version used to build its wheels')
    if manifest['machine'] != os.uname().machine:
        parser.error('bundle architecture does not match this host')
    if prefix.exists() or prefix.is_symlink() or config_path.exists() or config_path.is_symlink():
        parser.error('prefix and configuration must be new paths; existing files are preserved')
    config_created = False
    prefix_created = False
    try:
        prefix.mkdir(parents=True, mode=0o700)
        prefix_created = True
        for name in ['bin', 'wheelhouse', 'plugins', 'skills', 'examples']:
            shutil.copytree(source / name, prefix / name)
        shutil.copy2(source / 'distribution.json', prefix / 'distribution.json')
        subprocess.run([sys.executable, '-m', 'venv', str(prefix / 'python')], check=True)
        python = prefix / 'python/bin/python3'
        subprocess.run([str(python), '-m', 'pip', 'install', '--no-index', '--find-links', str(prefix / 'wheelhouse'),
                        'xgovernor-mcts==0.1.0', 'xgovernor-tasks==0.1.0', 'xgovernor-opencode-host==0.1.0'], check=True)
        algorithm = prefix / 'algorithms/mcts'
        algorithm.mkdir(parents=True)
        (algorithm / '__main__.py').write_text('from xgovernor_mcts.bridge.managed import main\nmain()\n')
        launcher = prefix / 'bin/mcts'
        launcher.rename(prefix / 'bin/mcts-native')
        launcher.write_text('#!/bin/sh\nexport PATH=' + shlex.quote(str(prefix / 'python/bin')) + ':"$PATH"\nexec ' +
                            shlex.quote(str(prefix / 'bin/mcts-native')) + ' "$@"\n')
        launcher.chmod(0o755)
        configuration = json.loads((prefix / 'examples/config.json').read_text())
        configuration.update(runs_dir=str(prefix / 'runs'), algorithms_dir=str(prefix / 'algorithms'),
                             python_executable=str(python), package_directory=str(prefix))
        config_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with open(config_path, 'x', opener=lambda path, flags: os.open(path, flags, 0o600)) as stream:
            config_created = True
            stream.write(json.dumps(configuration, indent=2) + '\n')
    except BaseException:
        if config_created:
            config_path.unlink()
        if prefix_created:
            shutil.rmtree(prefix)
        raise
    print('Installed mcts into ' + str(prefix))
    print('Add to your shell: export PATH=' + shlex.quote(str(prefix / 'bin')) + ':"$PATH"')
    print('Configuration: ' + str(config_path))


if __name__ == '__main__':
    main()
