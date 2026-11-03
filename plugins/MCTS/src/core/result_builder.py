"""P3 structured completion result builder and cross-reference validation."""
from __future__ import annotations
from typing import Any
from .mcts_protocol import validate_completion_result, ProtocolError
class ResultBuilder:
    def __init__(self, *, root_state_id: str): self.root_state_id=root_state_id
    def build(self, *, answer='', summary='', stop_reason='completed', candidates=None, evidence=None, selected_candidate_id=None, limitations=None, apply_plan=None):
        candidates=list(candidates or []); evidence=list(evidence or []); limitations=list(limitations or [])
        cids=[c.get('candidate_id') for c in candidates]; eids=[e.get('evidence_id') for e in evidence]
        if len(set(cids)) != len(cids) or len(set(eids)) != len(eids): raise ProtocolError('duplicate candidate/evidence id')
        emap=set(eids); cmap={c.get('candidate_id'):c for c in candidates}
        for c in candidates:
            if any(ref not in emap for ref in c.get('evidence_refs', [])): raise ProtocolError('candidate evidence reference is unresolved')
        if selected_candidate_id is not None:
            if selected_candidate_id not in cmap: raise ProtocolError('selected candidate is unresolved')
            if apply_plan is None or apply_plan.get('state_id') != cmap[selected_candidate_id].get('state_id'): raise ProtocolError('apply plan state does not match selected candidate')
        elif apply_plan is not None: raise ProtocolError('apply plan requires selected candidate')
        result={'status':'completed','summary':summary,'answer':answer,'stop_reason':stop_reason,'selected_candidate_id':selected_candidate_id,'candidates':candidates,'evidence':evidence,'limitations':limitations,'apply_plan':apply_plan}
        validate_completion_result(result); return result
