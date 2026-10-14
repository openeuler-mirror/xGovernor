#!/usr/bin/env bash
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(dirname -- "$script_dir")"
venv_dir="${XGOVERNOR_SWE_VENV:-$(dirname -- "$repo_dir")/.cache/swebench-runner-venv}"
runner_python="${XGOVERNOR_SWE_PYTHON:-$venv_dir/bin/python}"

if [[ ! -x "$runner_python" ]]; then
    mkdir -p "$(dirname -- "$venv_dir")"
    if command -v uv >/dev/null 2>&1; then
        UV_CACHE_DIR="$(dirname -- "$venv_dir")/uv" uv venv --python '>=3.11' "$venv_dir"
    else
        python3 -m venv "$venv_dir"
    fi
fi
"$runner_python" -c 'import sys; sys.exit("Python 3.11+ is required") if sys.version_info < (3, 11) else None'
if ! "$runner_python" -c 'import importlib.metadata; assert importlib.metadata.version("swebench") == "4.1.0"' >/dev/null 2>&1; then
    if command -v uv >/dev/null 2>&1; then
        UV_CACHE_DIR="$(dirname -- "$venv_dir")/uv" uv pip install --python "$runner_python" -r "$script_dir/swebench-requirements.txt"
    else
        "$runner_python" -m pip install -r "$script_dir/swebench-requirements.txt"
    fi
fi
exec "$runner_python" "$script_dir/run_swebench.py" "$@"
