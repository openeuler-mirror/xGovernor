"""Task agent orchestration through HostCapabilities."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from src.core.host_api import HostCapabilities

@dataclass(frozen=True)
class AgentTurn:
    branch_id: str
    job_id: str
    output: str = ''
    usage: dict[str,int] | None = None
    activity: list[dict[str,Any]] | None = None

def host_agent_turn(host: HostCapabilities, branch_id: str, prompt: str = "", *, text: str | None = None, wait_job=None, timeout_ms: int=300_000, profile: str|None=None, idempotency_key: str='agent:turn', jobs=None) -> AgentTurn:
    prompt = text if text is not None else prompt
    receipt=host.agent_turn(branch_id,prompt,timeout_ms=timeout_ms,idempotency_key=idempotency_key,profile=profile)
    job_id=receipt['job_id']
    state = wait_job(job_id) if wait_job else (jobs.get(job_id) if jobs and hasattr(jobs,'get') else None)
    return AgentTurn(branch_id,job_id,(state.get('output','') if isinstance(state,dict) else getattr(state,'output','')) if state else receipt.get('output',''),(state.get('usage') if isinstance(state,dict) else getattr(state,'usage',None)) if state else receipt.get('usage'),[])

def solver_step(host, branch_id: str, prompt: str, **kwargs): return host_agent_turn(host,branch_id,prompt,**kwargs)
def author_step(host, branch_id: str, prompt: str, **kwargs): return host_agent_turn(host,branch_id,prompt,**kwargs)
