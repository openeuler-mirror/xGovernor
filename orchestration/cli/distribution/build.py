#!/usr/bin/env python3
"""Build a native, offline-installable mcts distribution from this checkout."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--profile', choices=['dev', 'release'], default='release')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        parser.error('output already exists')
    command = ['cargo', 'build', '--locked', '-p', 'orchestration-cli', '--profile', args.profile]
    subprocess.run(command, cwd=root, check=True)
    metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--locked', '--no-deps', '--format-version', '1'], cwd=root))
    target = Path(metadata['target_directory']) / ('debug' if args.profile == 'dev' else 'release')
    with tempfile.TemporaryDirectory(prefix='.mcts-build-', dir=output.parent) as temporary:
        package = Path(temporary) / 'mcts'
        package.mkdir()
        (package / 'bin').mkdir()
        shutil.copy2(target / 'mcts', package / 'bin/mcts')
        shutil.copy2(Path(__file__).with_name('install.py'), package / 'install.py')
        shutil.copytree(root / 'orchestration/cli/examples', package / 'examples')
        for directory in ['plugins/opencode', 'plugins/xiaoo', 'plugins/dsh', 'skills/mcts']:
            shutil.copytree(root / 'orchestration' / directory, package / directory, ignore=shutil.ignore_patterns('__pycache__'))
        (package / 'wheelhouse').mkdir()
        # Registration still imports the legacy DeepSWE task adapter.
        sources = []
        for name, relative in [('tasks', 'tasks'), ('algorithm', 'algorithms/mcts'), ('legacy', 'legacy/opencode-host')]:
            destination = Path(temporary) / name
            shutil.copytree(root / 'orchestration' / relative, destination,
                            ignore=shutil.ignore_patterns('__pycache__', '*.egg-info', 'build'))
            sources.append(str(destination))
        subprocess.run([os.sys.executable, '-m', 'pip', 'wheel', '--wheel-dir', str(package / 'wheelhouse'), *sources], check=True)
        (package / 'distribution.json').write_text(json.dumps({
            'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
            'machine': os.uname().machine, 'python': list(os.sys.version_info[:2]),
        }, indent=2) + '\n')
        with tarfile.open(output, 'w:gz') as archive:
            archive.add(package, arcname='mcts')
    print(output)


if __name__ == '__main__':
    main()
