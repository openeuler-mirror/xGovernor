"""One-shot official SWE-bench evaluation over HostCapabilities."""
from __future__ import annotations

import json
import shlex
from datetime import datetime, timezone
from pathlib import Path

MODEL = "xiaoo-mcts"
REMOTE = "/home/user/workspace/official_eval"


def _run_id(node_id: str) -> str:
    safe = node_id.replace("/", "__").replace(":", "_")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{safe}-{stamp}"


def _field(job, name, default=""):
    return job.get(name, default) if isinstance(job, dict) else getattr(job, name, default)


def _read(host, branch_id, path, *, wait_job, default=b""):
    try:
        return host.file_read(branch_id, path, timeout_ms=120_000)
    except Exception:
        return default


def evaluate(node, task, output_root: Path, *, host=None, branch_id=None, wait_job=None):
    """Run the official harness once and persist patch, logs and verdict."""
    if host is None or branch_id is None or wait_job is None:
        raise RuntimeError("official SWE evaluation requires host, branch and wait_job")
    if task.testbench not in {"sweverified", "swelite"}:
        raise RuntimeError("official evaluator currently supports sweverified and swelite")
    out = output_root / task.spec.id / task.instance_id if output_root.name == "output" else output_root
    out.mkdir(parents=True, exist_ok=True)
    status, patch = task.spec.artifact_reader_fn()(node, task, host=host, branch_id=branch_id, wait_job=wait_job)
    patch_file = out / "artifact.patch"
    patch_file.write_text(patch, encoding="utf-8")
    (out / "status.txt").write_text(status or "", encoding="utf-8")
    (out / "node.txt").write_text(node.node_id + "\n", encoding="utf-8")

    run_id = _run_id(node.node_id)
    prediction = json.dumps({"instance_id": task.instance_id, "model_name_or_path": MODEL, "model_patch": patch}, ensure_ascii=False) + "\n"
    prep = wait_job(host.exec(branch_id, ["bash", "-lc", f"mkdir -p {shlex.quote(REMOTE)}/results"], cwd="/home/user/workspace", timeout_ms=120_000, idempotency_key="official:prepare"))
    if _field(prep, "exit_code") not in (0, None):
        raise RuntimeError("cannot prepare official SWE evaluation directory")
    host.file_write(branch_id, f"{REMOTE}/predictions.jsonl", prediction.encode(), timeout_ms=120_000, idempotency_key="official:prediction")
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
    result = wait_job(host.exec(branch_id, ["bash", "-lc", script], cwd=f"{REMOTE}/results", timeout_ms=3_600_000, idempotency_key=f"official:run:{run_id}"))
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
