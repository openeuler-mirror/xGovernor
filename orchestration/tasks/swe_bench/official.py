"""One-shot official SWE-bench evaluation over HostCapabilities."""
from __future__ import annotations

import json
import shlex
from xgovernor_tasks.common.benchmark_util import operation_key
from pathlib import Path

MODEL = "xiaoo-mcts"
REMOTE = "/home/user/workspace/official_eval"


def _run_id(node_id: str, branch_id: str) -> str:
    safe = node_id.replace("/", "__").replace(":", "_")
    return f"{safe}-{operation_key(branch_id, 'official', node_id).split(':')[-1]}"


def _field(job, name, default=""):
    return job.get(name, default) if isinstance(job, dict) else getattr(job, name, default)


def _read(host, branch_id, path, *, wait_job, default=b""):
    try:
        return host.file_read(branch_id, path, timeout_ms=120_000)
    except Exception:
        return default


def _host_evaluate(node, task, out, *, host, branch_id, wait_job):
    import shutil
    import subprocess

    status, patch = task.spec.artifact_reader_fn()(node, task, host=host, branch_id=branch_id, wait_job=wait_job)
    patch_file = out / "artifact.patch"
    patch_file.write_text(patch, encoding="utf-8")
    (out / "status.txt").write_text(status, encoding="utf-8")
    report = {"status": "not_run", "passed": None, "reason": ""}

    def finish(reason=""):
        report["reason"] = reason
        task.official_status = report["status"]
        task.official_reason = reason
        (out / "official_result.json").write_text(json.dumps(report), encoding="utf-8")
        return report["passed"], patch_file

    python = getattr(task, "official_python", None)
    if not python:
        return finish("swebench_python is not configured")
    if not shutil.which("docker"):
        return finish("host Docker is unavailable")
    image = f"swebench/sweb.eval.x86_64.{task.instance_id.replace('__', '_1776_')}:latest"
    check = subprocess.run(["docker", "image", "inspect", image], capture_output=True)
    if check.returncode:
        return finish(f"host task image is unavailable: {image}")
    try:
        check = subprocess.run([python, "-c", "import swebench.harness.run_evaluation"], capture_output=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return finish("configured SWE-bench Python is unavailable")
    if check.returncode:
        return finish("configured Python has no SWE-bench harness")
    predictions = out / "predictions.jsonl"
    predictions.write_text(json.dumps({"instance_id": task.instance_id, "model_name_or_path": MODEL,
                                       "model_patch": patch}) + "\n", encoding="utf-8")
    command = [python, "-m", "swebench.harness.run_evaluation", "--dataset_name", task.dataset_name,
               "--split", task.split, "--predictions_path", str(predictions.resolve()),
               "--instance_ids", task.instance_id, "--max_workers", "1", "--run_id", _run_id(node.node_id, branch_id),
               "--namespace", "swebench", "--cache_level", "instance", "--clean", "False",
               "--timeout", "600"]
    with (out / "official.stdout.log").open("wb") as stdout, (out / "official.stderr.log").open("wb") as stderr:
        try:
            result = subprocess.run(command, cwd=out, stdout=stdout, stderr=stderr,
                                    timeout=660, env={**__import__("os").environ,
                                                     "HF_HUB_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1"})
        except (OSError, subprocess.TimeoutExpired) as exc:
            return finish(f"host harness did not finish: {type(exc).__name__}")
    (out / "official.exit_code.txt").write_text(str(result.returncode))
    passed = False
    found = False
    for path in out.rglob("*.json"):
        if path.name in {"instance.json", "official_result.json"}:
            continue
        try:
            value = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(value, dict) and task.instance_id in value.get("resolved_ids", []):
            passed, found = True, True
        if isinstance(value, dict) and isinstance(value.get(task.instance_id), dict):
            verdict = value[task.instance_id].get("resolved")
            if isinstance(verdict, bool):
                passed, found = verdict, True
        if isinstance(value, dict) and task.instance_id in value.get("unresolved_ids", []):
            found = True
    if not found:
        return finish("host harness produced no verdict; inspect official stdout/stderr")
    report.update(status="passed" if passed else "failed", passed=passed)
    return finish()


def evaluate(node, task, output_root: Path, *, host=None, branch_id=None, wait_job=None):
    """Run the official harness once and persist patch, logs and verdict."""
    if host is None or branch_id is None or wait_job is None:
        raise RuntimeError("official SWE evaluation requires host, branch and wait_job")
    if task.testbench not in {"sweverified", "swelite"}:
        raise RuntimeError("official evaluator currently supports sweverified and swelite")
    out = output_root / task.spec.id / task.instance_id if output_root.name == "output" else output_root
    out.mkdir(parents=True, exist_ok=True)
    if getattr(task, "prepared_root", False):
        return _host_evaluate(node, task, out.resolve(), host=host, branch_id=branch_id, wait_job=wait_job)
    status, patch = task.spec.artifact_reader_fn()(node, task, host=host, branch_id=branch_id, wait_job=wait_job)
    patch_file = out / "artifact.patch"
    patch_file.write_text(patch, encoding="utf-8")
    (out / "status.txt").write_text(status or "", encoding="utf-8")
    (out / "node.txt").write_text(node.node_id + "\n", encoding="utf-8")

    run_id = _run_id(node.node_id, branch_id)
    prediction = json.dumps({"instance_id": task.instance_id, "model_name_or_path": MODEL, "model_patch": patch}, ensure_ascii=False) + "\n"
    prep = wait_job(host.exec(branch_id, ["bash", "-lc", f"mkdir -p {shlex.quote(REMOTE)}/results"], cwd="/home/user/workspace", timeout_ms=120_000, idempotency_key=operation_key(branch_id, "official:prepare", node.node_id)))
    if _field(prep, "exit_code") not in (0, None):
        raise RuntimeError("cannot prepare official SWE evaluation directory")
    host.file_write(branch_id, f"{REMOTE}/predictions.jsonl", prediction.encode(), timeout_ms=120_000, idempotency_key=operation_key(branch_id, "official:prediction", node.node_id))
    dataset, split, instance, qrun = map(shlex.quote, (task.dataset_name, task.split, task.instance_id, run_id))
    venv = "/home/user/workspace/swebench-harness-verified/.venv" if task.testbench == "sweverified" else "/home/user/workspace/swebench-harness/.venv"
    script = f'''set -u
ROOT={shlex.quote(REMOTE)}; VENV={shlex.quote(venv)}
mkdir -p "$ROOT/results"
docker rm -f "sweb.eval.{task.instance_id}.{run_id}" >/dev/null 2>&1 || true
set +e
"$VENV/bin/python" -m swebench.harness.run_evaluation --dataset_name {dataset} --split {split} \\
  --predictions_path "$ROOT/predictions.jsonl" --instance_ids {instance} --max_workers 1 --run_id {qrun} \\
  --namespace swebench --cache_level instance --clean False --timeout 1800 > "$ROOT/stdout.log" 2> "$ROOT/stderr.log"
code=$?
printf '%s' "$code" > "$ROOT/exit_code.txt"
set -e
"$VENV/bin/python" - <<'PY'
import json
from pathlib import Path
root=Path({REMOTE!r}); instance={task.instance_id!r}; passed=False; reports=[]
for path in root.rglob("*.json"):
    try: data=json.loads(path.read_text(encoding="utf-8"))
    except Exception: continue
    if isinstance(data,dict) and instance in data.get("resolved_ids",[]): passed=True
    if isinstance(data,dict) and isinstance(data.get(instance),dict) and data[instance].get("resolved") is True: passed=True
    if instance in path.as_posix(): reports.append(str(path))
(root/"verdict.json").write_text(json.dumps({{"passed":passed,"reports":reports}}))
PY
'''
    result = wait_job(host.exec(branch_id, ["bash", "-lc", script], cwd=f"{REMOTE}/results", timeout_ms=3_600_000, idempotency_key=operation_key(branch_id, "official:run", node.node_id)))
    stdout = _read(host, branch_id, f"{REMOTE}/stdout.log", wait_job=wait_job, default=str(_field(result, "output", "")).encode())
    stderr = _read(host, branch_id, f"{REMOTE}/stderr.log", wait_job=wait_job)
    verdict_raw = _read(host, branch_id, f"{REMOTE}/verdict.json", wait_job=wait_job)
    (out / "official.stdout.log").write_bytes(stdout)
    (out / "official.stderr.log").write_bytes(stderr)
    (out / "official.exit_code.txt").write_bytes(_read(host, branch_id, f"{REMOTE}/exit_code.txt", wait_job=wait_job, default=str(_field(result, "exit_code", "")).encode()))
    if not verdict_raw:
        raise RuntimeError("official SWE evaluator did not produce verdict.json")
    (out / "official_result.json").write_bytes(verdict_raw)
    return bool(json.loads(verdict_raw).get("passed")), patch_file
