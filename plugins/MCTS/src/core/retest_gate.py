"""Coalesce concurrent retest work for identical parent snapshots.

When a parent node's oracle is newer than the cached oracle an expansion was
built against, every sibling retests that same parent.  With concurrent
expansion workers this can fork the same state and run the same scorer several
times in parallel.  ``RetestGate`` lets the first caller run the retest while
the rest wait on the same result; completed results are intentionally not
cached so a later iteration may retest again with fresh evidence.
"""

from __future__ import annotations

import os
import threading
from concurrent.futures import Future
from typing import Callable


def retest_gate_enabled() -> bool:
    """Whether concurrent siblings should share one parent retest.

    Reads ``XIAOO_RETEST_CACHE`` once per call so tests can patch the
    environment.  Defaults to enabled.
    """

    raw = os.environ.get("XIAOO_RETEST_CACHE", "1").strip()
    return raw.lower() not in {"0", "false", "no", "off"}


class RetestGate:
    """Single-flight gate keyed by ``(state_id, oracle_version)``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inflight: dict[tuple[str, int | None], Future] = {}
        self.computes = 0
        self.coalesced = 0

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"computes": self.computes, "coalesced": self.coalesced}

    def run(
        self,
        state_id: str,
        version: int | None,
        compute: Callable[[], tuple[float, str]],
    ) -> tuple[float, str]:
        """Run ``compute`` once per key; concurrent callers share the result."""

        key = (state_id, version)
        with self._lock:
            future = self._inflight.get(key)
            if future is None:
                future = Future()
                self._inflight[key] = future
                owner = True
            else:
                owner = False
                self.coalesced += 1

        if not owner:
            return future.result()

        try:
            value = compute()
            with self._lock:
                self.computes += 1
            future.set_result(value)
            return value
        except BaseException as exc:
            future.set_exception(exc)
            raise
        finally:
            with self._lock:
                self._inflight.pop(key, None)
