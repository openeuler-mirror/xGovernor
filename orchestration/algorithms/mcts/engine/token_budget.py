# Per-instance token fuse: stop exploration and submit the best node so far.
# token_budget.py 是 token 用量监控与熔断器，用于累计用量达到阈值后停止搜索、提交当前候选。
# - 默认阈值： 3000 万 token。
# - 配置方式： --token-fuse-limit 或环境变量 XIAOO_MCTS_TOKEN_FUSE_LIMIT，设为 0 禁用。
# - 计数来源：本次 host job 终止事件的 usage（含 Author/Solver、memory、selector）。
# - 触发行为： 通知 Coordinator 收尾，经过 selector 选优，再执行一次官测。
# 它属于软上限：通过定期检查触发，正在执行的请求和后续 selector 仍可能消耗 token，因此不保证严格卡在阈值内。
# 终止事件按 job_id 去重；恢复父会话不会重复计费。无需 trace 数据库。

from __future__ import annotations

import os
import threading
import time


DEFAULT_TOKEN_FUSE_LIMIT = 30_000_000


def token_fuse_limit_from_env() -> int:
    raw = os.environ.get("XIAOO_MCTS_TOKEN_FUSE_LIMIT")
    if raw is None or raw == "":
        return DEFAULT_TOKEN_FUSE_LIMIT
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"XIAOO_MCTS_TOKEN_FUSE_LIMIT must be an integer token count, got {raw!r}"
        ) from exc


class TokenBudget:
    """Track cumulative turn tokens for one MCTS instance run."""

    def __init__(self, limit: int, *, since_ms: int | None = None, usage_reader=None):
        if limit < 0:
            raise ValueError(f"token fuse limit must be >= 0, got {limit}")
        self.limit = limit
        self.usage_reader = usage_reader or (lambda: 0)
        self.since_ms = since_ms if since_ms is not None else int(time.time() * 1000)
        self._total = 0
        self._tripped = False
        self._lock = threading.Lock()

    @classmethod
    def maybe_enabled(cls, limit: int | None = None, *, since_ms: int | None = None) -> TokenBudget | None:
        effective = token_fuse_limit_from_env() if limit is None else limit
        if effective <= 0:
            return None
        return cls(effective, since_ms=since_ms)

    @property
    def enabled(self) -> bool:
        return self.limit > 0

    @property
    def total(self) -> int:
        with self._lock:
            return self._total

    @property
    def tripped(self) -> bool:
        with self._lock:
            return self._tripped

    def refresh(self) -> int:
        total = int(self.usage_reader())
        with self._lock:
            self._total = total
        return total

    def should_fuse(self) -> bool:
        """Return True once when total tokens reach the limit."""
        if not self.enabled:
            return False
        with self._lock:
            if self._tripped:
                return False
        total = self.refresh()
        if total < self.limit:
            return False
        with self._lock:
            if self._tripped:
                return False
            self._tripped = True
        return True
