"""Two-signal v1 gate; path limits and scheduling stay in Coordinator."""
from dataclasses import dataclass
from functools import partial
import json
import math
import os
from pathlib import Path
import re
from typing import Callable

PARAMS = json.loads(Path(__file__).with_name('fork_policy_params.json').read_text())
DEFAULT_FORK_THRESHOLD = PARAMS['threshold']
HESITATION = re.compile(r'\b(?:'+'|'.join(re.escape(w) for w in PARAMS['words'])+r')\b', re.I)


@dataclass(frozen=True)
class ForkContext:
    attempt_id: str
    head_state_id: str
    prompt: str
    profile: str | None
    context_tokens: int | None
    event: dict
    visible_text: str = ''


ForkDecision = Callable[[ForkContext, int | None], bool]


def dynamic_fork_enabled() -> bool:
    return os.environ.get("XIAOO_MCTS_FORK_AFTER_TOOLS") == "1"


def _generation_tokens(usage: dict) -> int:
    def _count(value) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            return 0
        return value
    return _count(usage.get("output_tokens")) + _count(usage.get("reasoning_tokens"))


def token_hesitation_score(context: ForkContext) -> float | None:
    usage = context.event.get("usage") or {}
    output = _generation_tokens(usage if isinstance(usage, dict) else {})
    if output <= 0:
        return None
    frequency = len(HESITATION.findall(context.visible_text)) * 1000 / output
    # Match the 0.025 feature bins used by the offline fit.
    tokens = round(min(1., output / PARAMS['output_scale']) * 40) / 40
    hesitation = round(min(1., frequency / PARAMS['hesitation_scale']) * 40) / 40
    return PARAMS['token_weight'] * tokens + PARAMS['hesitation_weight'] * hesitation


def should_fork(context: ForkContext, turn_id: int | None, *, threshold: float = DEFAULT_FORK_THRESHOLD) -> bool:
    """Cheap ask gate: True permits an LLM query, not a sibling by itself."""
    score = token_hesitation_score(context)
    return score is not None and score >= threshold


def token_hesitation_policy(threshold: float = DEFAULT_FORK_THRESHOLD) -> ForkDecision:
    if not math.isfinite(threshold):
        raise ValueError('fork score threshold must be finite')
    return partial(should_fork, threshold=threshold)
