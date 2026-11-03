"""Read the selected ZSoft PoC through HostCapabilities."""
from __future__ import annotations
from src.res.mode import ARTIFACT_READERS, register
from .flow import extract_poc

MAX_DIFF_CHARS = 60_000

@register(ARTIFACT_READERS, "poc_file")
def poc_file(node, task, *, host, branch_id, **kwargs):
    poc = extract_poc(host, branch_id, task)
    if poc is None:
        raise RuntimeError(f"no PoC at {task.poc_sandbox_path} for {node.node_id}")
    data, _path = poc
    return task.poc_filename, data.decode("utf-8", errors="replace")[:MAX_DIFF_CHARS]
