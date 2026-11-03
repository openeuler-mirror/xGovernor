"""物化脚本生成器：生成在 E2B 沙箱内把任务内容落到 /home/user/workspace 的 shell。

统一抽象：adapter.materialize_script() 转发到这里注册的生成器；生成器只依赖
adapter 的公开属性（dataset_name/split/instance_id/local_source/policy/...），
不感知具体模式之外的任何东西。
"""

from __future__ import annotations

import json
import os

from src.res.mode import MATERIALIZERS, register


def _hf_export_line() -> str:
    endpoint = os.environ.get("HF_ENDPOINT", "").strip()
    if not endpoint:
        return ""
    return "export HF_ENDPOINT=" + json.dumps(endpoint) + "\n"


def _instance_runtime_script(*, image_setup: str, container_mount: str) -> str:
    """启动 SWE-bench 实例容器（bind-mount repo）和 helper 脚本。"""
    mount = container_mount
    return image_setup + f"""
INSTANCE_CONTAINER="swe_env"
REPO="/home/user/workspace/repo"
CONTAINER_MOUNT="{mount}"
echo "$INSTANCE_IMAGE" > /home/user/workspace/instance_image.txt
echo "$CONTAINER_MOUNT" > /home/user/workspace/instance_mount.txt
docker rm -f "$INSTANCE_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$INSTANCE_CONTAINER" -v "$REPO:$CONTAINER_MOUNT" --entrypoint /bin/bash "$INSTANCE_IMAGE" -c "exec tail -f /dev/null"

PYTHON_BIN="$(docker exec "$INSTANCE_CONTAINER" bash -lc 'command -v python3 || command -v python' 2>/dev/null | head -n1 | tr -d '\\r')"
if [ -z "$PYTHON_BIN" ]; then
  PYTHON_BIN="$(docker exec "$INSTANCE_CONTAINER" bash -c 'command -v python3 || command -v python' 2>/dev/null | head -n1 | tr -d '\\r')"
fi
if [ -n "$PYTHON_BIN" ]; then
  echo "$PYTHON_BIN" > /home/user/workspace/instance_python.txt
  echo "INSTANCE_PYTHON=$PYTHON_BIN"
else
  rm -f /home/user/workspace/instance_python.txt
  echo "INSTANCE_PYTHON=(login shell default)"
fi

cat > /home/user/workspace/run_in_instance.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
py=""
if [ -f /home/user/workspace/instance_python.txt ]; then
  py="$(tr -d '\\n\\r' < /home/user/workspace/instance_python.txt)"
fi
if [ -z "$py" ]; then
  py="python3"
fi
mount="/testbed"
if [ -f /home/user/workspace/instance_mount.txt ]; then
  mount="$(tr -d '\\n\\r' < /home/user/workspace/instance_mount.txt)"
fi
prefix="export PATH=\"$(dirname "$py"):\\$PATH\"; "
args=("$@")
if [ "${{args[0]:-}}" = "pytest" ]; then
  args=("$py" -m pytest "${{args[@]:1}}")
fi
quoted="$(printf '%q ' "${{args[@]}}")"
docker exec -w "$mount" swe_env bash -lc "${{prefix}}${{quoted}}"
EOF
chmod +x /home/user/workspace/run_in_instance.sh

cat > /home/user/workspace/ensure_instance.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
REPO="/home/user/workspace/repo"
IMAGE_FILE="/home/user/workspace/instance_image.txt"
MOUNT_FILE="/home/user/workspace/instance_mount.txt"
CONTAINER="swe_env"
[ -f "$IMAGE_FILE" ] || exit 0
IMAGE="$(tr -d "\\n" < "$IMAGE_FILE")"
[ -n "$IMAGE" ] || exit 0
MOUNT="/testbed"
if [ -f "$MOUNT_FILE" ]; then
  MOUNT="$(tr -d "\\n\\r" < "$MOUNT_FILE")"
fi
running="$(docker inspect -f "{{{{.State.Running}}}}" "$CONTAINER" 2>/dev/null || echo false)"
if [ "$running" = "true" ]; then
  exit 0
fi
docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER" -v "$REPO:$MOUNT" --entrypoint /bin/bash "$IMAGE" -c "exec tail -f /dev/null"
EOF
chmod +x /home/user/workspace/ensure_instance.sh
echo INSTANCE_RUNTIME_READY
"""


@register(MATERIALIZERS, "swe-instance")
def swe_instance(task) -> str:
    """SWE-bench 三款物化：从实例镜像把 repo 拷进沙箱 + 起实例容器。"""
    if task.testbench == "swelite":
        return _swelite_materialize(task)
    if task.testbench == "sweverified":
        return _sweverified_materialize(task)
    return _swepro_materialize(task)


def _swepro_materialize(task) -> str:
    image = task.docker_image
    runtime = _instance_runtime_script(image_setup=f'INSTANCE_IMAGE="{image}"', container_mount="/app")
    return f"""set -euo pipefail
cd /home/user/workspace
rm -rf repo
mkdir -p repo
docker image inspect {image} >/dev/null 2>&1 || docker pull {image}
container="$(docker create {image})"
cleanup() {{
  docker rm -f "$container" >/dev/null 2>&1 || true
}}
trap cleanup EXIT
found=0
for src in /testbed /workspace /app; do
  if docker cp "$container:$src/." repo/ 2>/tmp/swepro-docker-cp.err; then
    found=1
    echo "SWEPRO_REPO_SOURCE=$src"
    break
  fi
done
if [ "$found" != "1" ]; then
  cat /tmp/swepro-docker-cp.err >&2 || true
  echo "Could not copy repository from Docker image." >&2
  exit 1
fi
git config --global --add safe.directory /home/user/workspace/repo || true
cd /home/user/workspace/repo
git rev-parse --show-toplevel || true
{runtime}
echo SWEPRO_REPO_READY
"""


def _swe_instance_materialize(task, *, venv: str, container_tag: str) -> str:
    dataset_name = json.dumps(task.dataset_name)
    split = json.dumps(task.split)
    instance_id = json.dumps(task.instance_id)
    hf_export = _hf_export_line()
    image_setup = 'INSTANCE_IMAGE="$(tr -d "\\n" < /home/user/workspace/instance_image.txt)"'
    runtime = _instance_runtime_script(image_setup=image_setup, container_mount="/testbed")
    return f"""set -euo pipefail
cd /home/user/workspace
rm -rf repo
mkdir -p repo

VENV="{venv}"
if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/python" -m pip install -U pip
  "$VENV/bin/python" -m pip install "swebench==4.1.0" datasets docker
fi

{hf_export}"$VENV/bin/python" - <<'PY'
import re
import shutil
import subprocess
import sys
from pathlib import Path

from datasets import load_dataset
from swebench.harness.test_spec.test_spec import make_test_spec

dataset_name = {dataset_name}
split = {split}
instance_id = {instance_id}

rows = load_dataset(dataset_name, split=split)
row = None
for candidate in rows:
    if str(candidate["instance_id"]) == instance_id:
        row = dict(candidate)
        break
if row is None:
    sys.exit(f"missing instance: {{instance_id}}")

spec = make_test_spec(row, namespace="swebench", instance_image_tag="latest")
image = spec.instance_image_key
container = "{container_tag}_" + re.sub(r"[^a-zA-Z0-9_.-]", "_", instance_id.lower())
repo = "/home/user/workspace/repo"

subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
if subprocess.run(["docker", "image", "inspect", image], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode != 0:
    subprocess.run(["docker", "pull", image], check=True)

try:
    subprocess.run(["docker", "create", "--name", container, image, "tail", "-f", "/dev/null"], check=True)
    shutil.rmtree(repo, ignore_errors=True)
    subprocess.run(["mkdir", "-p", repo], check=True)
    subprocess.run(["docker", "cp", f"{{container}}:/testbed/.", repo], check=True)
    Path("/home/user/workspace/instance_image.txt").write_text(image, encoding="utf-8")
finally:
    subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
PY

git config --global --add safe.directory /home/user/workspace/repo || true
cd /home/user/workspace/repo
git rev-parse --show-toplevel || true
{runtime}
echo SWEBENCH_REPO_READY
"""


def _swelite_materialize(task) -> str:
    return _swe_instance_materialize(
        task,
        venv="/home/user/workspace/swebench-harness/.venv",
        container_tag="xiaoo_swelite",
    )


def _sweverified_materialize(task) -> str:
    return _swe_instance_materialize(
        task,
        venv="/home/user/workspace/swebench-harness-verified/.venv",
        container_tag="xiaoo_sweverified",
    )

