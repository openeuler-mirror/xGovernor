"""The cheap gate decides whether to ask; only a strict LLM boolean permits a fork."""
import json
from typing import Callable
from .fork_policy import ForkContext, ForkDecision


def parse_fork_answer(text: str) -> bool:
    answer = text.strip()
    if answer == 'true':
        return True
    if answer == 'false':
        return False
    raise ValueError('fork judge must return exactly true or false')


class LLMForkPolicy:
    def __init__(self, gate: ForkDecision, complete: Callable[[ForkContext, str], str], on_event=None):
        self.gate, self.complete, self.on_event = gate, complete, on_event

    def __call__(self, context: ForkContext, turn_id: int | None) -> bool:
        if not self.gate(context, turn_id):
            return False
        payload = json.dumps({
            'turn_id': turn_id,
            'task_input': context.prompt,
            'head_state_id': context.head_state_id,
            'visible_solver_output': context.visible_text,
            'latest_tools': context.event.get('tools', []),
            'usage': context.event.get('usage', {}),
        }, ensure_ascii=False)
        try:
            raw = self.complete(context, payload)
            decision = parse_fork_answer(raw)
        except Exception as exc:
            if self.on_event:
                preview = " ".join(str(locals().get("raw", "")).split())
                if len(preview) > 200:
                    preview = preview[:200] + "…"
                detail = f"{type(exc).__name__}"
                if preview:
                    detail = f"{detail}: {preview!r}"
                self.on_event('plugin.progress', {'summary': f'Fork judge rejected ({detail}); no sibling created'})
            return False
        if self.on_event:
            self.on_event('plugin.progress', {'summary': f'Fork judge {context.attempt_id}: {str(decision).lower()}'})
        return decision
