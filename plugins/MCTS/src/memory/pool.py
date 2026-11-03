"""Asynchronous semantic memory backed by a per-instance SQLite database."""

from __future__ import annotations

import queue
import sys
import threading
import time
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .merger import MergeExhaustedError, SemanticMerger, SourceItem
from .models import AddResult, SUPPORTED_KINDS
from .parser import parse_query
from .store import MemoryStore, SQLiteStore, StoredItem


@dataclass(frozen=True)
class _QueuedItem:
    """A parsed item waiting for its kind-specific merge worker."""

    source_id: str
    kind: str
    data: dict[str, str | bool]
    sequence: int


class MemoryPool:
    """Accept Solver findings asynchronously and publish Author-ready JSON.

    A pool owns exactly one fresh SQLite database by default.  It has no dependency
    on MCTS coordination or author execution: the host only calls ``add`` and
    ``fetch``.  Storage failures permanently disable this one memory instance so
    they cannot interrupt the host workflow.
    """

    def __init__(
        self,
        *,
        _merger: Any | None = None,
        _store: MemoryStore | None = None,
    ) -> None:
        self._run_id = str(uuid.uuid4())
        self._temp_dir = tempfile.TemporaryDirectory(prefix="xiaoo-memory-")
        self._db_path = Path(self._temp_dir.name) / "memory.sqlite"
        if _merger is None:
            _merger = SemanticMerger(lambda messages: "{}")

        self._merger = _merger
        self._lock = threading.RLock()
        self._fetch_idle = threading.Condition(self._lock)
        self._close_lock = threading.Lock()
        self._next_sequence = 0
        self._active_fetches = 0
        self._disabled_reason: str | None = None
        self._closing = False
        self._closed = False
        self._discard_pending = False
        self._last_fetch_json = "{}"

        if _store is None:
            try:
                self._store: MemoryStore | None = SQLiteStore(self._db_path)
            except Exception as exc:
                self._store = None
                self._disable(exc)
        else:
            self._store = _store
            store_path = _store.path
            if store_path is not None:
                self._db_path = store_path

        self._queues: dict[str, queue.Queue[tuple[_QueuedItem, ...] | None]] = {
            kind: queue.Queue() for kind in SUPPORTED_KINDS
        }
        self._worker_active: dict[str, bool] = {kind: False for kind in SUPPORTED_KINDS}
        self._workers = [
            threading.Thread(
                target=self._run_worker,
                args=(kind,),
                name=f"memory-pool-{kind}",
                daemon=True,
            )
            for kind in SUPPORTED_KINDS
        ]
        for worker in self._workers:
            worker.start()

    @property
    def db_path(self) -> Path:
        """The fresh database assigned to this pool, even if initialization failed."""

        return self._db_path

    def add(self, query_text: str) -> AddResult:
        """Parse and enqueue supported items without waiting for LLM merging."""

        return self._add(query_text)

    def _add(self, query_text: str) -> AddResult:
        """Implement add while the public boundary records its input and result."""

        with self._lock:
            unavailable = self._unavailable_add_result_locked()
        if unavailable is not None:
            return unavailable

        parsed = parse_query(query_text)
        accepted = [
            item
            for item in parsed.items
            if isinstance(item.data.get("kind"), str)
            and item.data["kind"] in SUPPORTED_KINDS
        ]

        # Allocation and publication share one critical section to preserve FIFO for
        # separate add calls of the same kind.  No LLM or SQLite call occurs here.
        with self._lock:
            unavailable = self._unavailable_add_result_locked()
            if unavailable is not None:
                return unavailable
            batches: dict[str, list[_QueuedItem]] = {
                kind: [] for kind in SUPPORTED_KINDS
            }
            for item in accepted:
                kind = item.data["kind"]
                assert isinstance(kind, str)
                batches[kind].append(
                    _QueuedItem(
                        source_id=str(uuid.uuid4()),
                        kind=kind,
                        data=dict(item.data),
                        sequence=self._next_sequence,
                    )
                )
                self._next_sequence += 1
            for kind, batch in batches.items():
                if batch:
                    self._queues[kind].put(tuple(batch))

        return AddResult(
            success=bool(accepted),
            kind_error=parsed.kind_error,
            errors=parsed.errors,
        )

    def has_unfetched(self) -> bool:
        """True when the store has canonical items not yet included in a fetch."""

        with self._lock:
            if self._closed or self._closing or self._disabled_reason is not None:
                return False
            store = self._store
        if store is None:
            return False
        try:
            return store.has_unfetched()
        except Exception:
            return False

    def is_merge_pending(self) -> bool:
        """True when queued or in-flight merge work has not settled yet."""

        with self._lock:
            if self._closed or self._closing or self._disabled_reason is not None:
                return False
            active = any(self._worker_active.values())
        pending = any(
            work_queue.unfinished_tasks for work_queue in self._queues.values()
        )
        return active or pending

    def fetch(self) -> str:
        """Publish one full JSON version without waiting for merge workers or an LLM."""

        with self._lock:
            result = self._last_fetch_json
        return self._fetch()

    def _fetch(self) -> str:
        """Implement fetch while the public boundary records its returned JSON."""

        with self._fetch_idle:
            if self._closed or self._closing or self._disabled_reason is not None:
                return self._last_fetch_json
            store = self._store
            if store is None:
                return self._last_fetch_json
            self._active_fetches += 1

        try:
            try:
                serialized = store.publish_fetch()
            except Exception as exc:
                with self._lock:
                    closing = self._closing or self._closed
                if not closing:
                    self._disable(exc)
                with self._lock:
                    return self._last_fetch_json

            with self._lock:
                # The store operation runs without the pool lock.  A concurrent
                # worker may have disabled memory, or close() may have begun, while
                # it ran.  In either case this publication cannot become the last
                # caller-visible successful result.
                if self._closed or self._closing or self._disabled_reason is not None:
                    return self._last_fetch_json
                self._last_fetch_json = serialized
                return self._last_fetch_json
        finally:
            with self._fetch_idle:
                self._active_fetches -= 1
                if self._active_fetches == 0:
                    self._fetch_idle.notify_all()

    def close(self, *, cancel_pending: bool = False) -> None:
        """Stop workers and close storage; final run cleanup may discard queued merges."""

        with self._close_lock:
            with self._fetch_idle:
                if self._closed:
                    return
                self._closing = True
                self._discard_pending = cancel_pending
                while self._active_fetches:
                    self._fetch_idle.wait()
                queues = tuple(self._queues.values())
                workers = tuple(self._workers)

            for work_queue in queues:
                work_queue.join()
            for work_queue in queues:
                work_queue.put(None)
            for worker in workers:
                worker.join()

            with self._lock:
                store = self._store
            if store is not None:
                try:
                    store.close()
                except Exception as exc:
                    self._disable(exc)
            with self._lock:
                self._closed = True
                self._closing = False
            self._temp_dir.cleanup()

    def __enter__(self) -> MemoryPool:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        self.close()
        return False

    def _run_worker(self, kind: str) -> None:
        work_queue = self._queues[kind]
        while True:
            batch = work_queue.get()
            if batch is None:
                work_queue.task_done()
                return

            with self._lock:
                self._worker_active[kind] = True
            try:
                if self._discard_pending or self._is_disabled():
                    continue
                self._merge_batch(kind, batch)
            except Exception as exc:
                # A worker exception must never leak into or block the MCTS thread.
                # SemanticMerger normally converts LLM/contract failures into
                # MergeExhaustedError; this protects an injected or future merger.
                self._store_failed_batch(kind, batch, exc)
                print(str(exc), file=sys.stderr)
            finally:
                with self._lock:
                    self._worker_active[kind] = False
                work_queue.task_done()

    def _merge_batch(self, kind: str, batch: tuple[_QueuedItem, ...]) -> None:
        store = self._active_store()
        if store is None:
            return
        try:
            old_items = store.list_kind(kind)
        except Exception as exc:
            self._disable(exc)
            return

        old = tuple(
            SourceItem(item.source_id, False, dict(item.data)) for item in old_items
        )
        new = tuple(SourceItem(item.source_id, True, dict(item.data)) for item in batch)
        try:
            groups = self._merger.merge(kind, old, new)
        except MergeExhaustedError as exc:
            self._store_failed_batch(kind, batch, exc)
            print(str(exc), file=sys.stderr)
            return

        self._apply_groups(store, kind, old_items, batch, groups)

    def _store_failed_batch(
        self,
        kind: str,
        batch: tuple[_QueuedItem, ...],
        error: Exception,
    ) -> None:
        store = self._active_store()
        if store is None:
            return
        self._write_to_active_store(
            store,
            lambda: store.append_items(
                StoredItem(
                    source_id=str(uuid.uuid4()),
                    kind=item.kind,
                    data=dict(item.data),
                    sequence=item.sequence,
                    merge_failed=str(error),
                )
                for item in batch
            ),
        )

    def _apply_groups(
        self,
        store: MemoryStore,
        kind: str,
        old_items: tuple[StoredItem, ...],
        batch: tuple[_QueuedItem, ...],
        groups: Any,
    ) -> None:
        old_by_id = {item.source_id: item for item in old_items}
        new_by_id = {item.source_id: item for item in batch}
        replacement: list[StoredItem] = []
        inherited_fetched_ids: dict[str, tuple[str, ...]] = {}

        for group in groups:
            group_old = [
                old_by_id[source_id]
                for source_id in group.source_ids
                if source_id in old_by_id
            ]
            group_new = [
                new_by_id[source_id]
                for source_id in group.source_ids
                if source_id in new_by_id
            ]
            if group_old:
                retained = min(group_old, key=lambda item: item.sequence)
                source_id = retained.source_id
                sequence = retained.sequence
                inherited_fetched_ids[source_id] = tuple(
                    item.source_id for item in group_old
                )
            else:
                if not group_new:
                    raise ValueError("merge group does not contain a known source item")
                source_id = str(uuid.uuid4())
                sequence = min(item.sequence for item in group_new)
                inherited_fetched_ids[source_id] = ()
            replacement.append(
                StoredItem(
                    source_id=source_id,
                    kind=kind,
                    data=dict(group.item),
                    sequence=sequence,
                )
            )

        replacement.sort(key=lambda item: item.sequence)
        self._write_to_active_store(
            store,
            lambda: store.replace_kind(kind, replacement, inherited_fetched_ids),
        )

    def _write_to_active_store(self, store: MemoryStore, operation: Any) -> bool:
        """Serialize a final write with disable state to prevent post-failure writes."""

        with self._lock:
            if (
                self._disabled_reason is not None
                or self._store is not store
                or self._closed
            ):
                return False
            try:
                operation()
                return True
            except Exception as exc:
                self._disable(exc)
                return False

    def _active_store(self) -> MemoryStore | None:
        with self._lock:
            if self._disabled_reason is not None:
                return None
            return self._store

    def _is_disabled(self) -> bool:
        with self._lock:
            return self._disabled_reason is not None

    def _unavailable_add_result_locked(self) -> AddResult | None:
        if self._disabled_reason is not None:
            return AddResult(
                success=False,
                kind_error=False,
                errors=(f"memory disabled: {self._disabled_reason}",),
            )
        if self._closing or self._closed:
            return AddResult(
                success=False,
                kind_error=False,
                errors=("memory pool is closed",),
            )
        return None

    def _disable(self, error: Exception) -> None:
        reason = str(error) or error.__class__.__name__
        with self._lock:
            if self._disabled_reason is not None:
                return
            self._disabled_reason = reason
        print(f"memory disabled: {reason}", file=sys.stderr)

    def _wait_for_idle(self, timeout_secs: float = 1.0) -> bool:
        """Return whether all queued merges settled before the deadline (test helper)."""

        deadline = time.monotonic() + max(0.0, timeout_secs)
        while True:
            with self._lock:
                active = any(self._worker_active.values())
            pending = any(
                work_queue.unfinished_tasks for work_queue in self._queues.values()
            )
            if not active and not pending:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.005)
