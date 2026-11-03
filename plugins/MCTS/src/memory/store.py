"""Persistence backends for the standalone memory pool.

``MemoryPool`` owns parsing, merge scheduling, and LLM calls.  Stores only own
canonical item state and published fetch versions, so the storage mechanism can
change without coupling this package to the MCTS coordinator or author worker.
"""

from __future__ import annotations

import copy
import json
import sqlite3
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class StoredItem:
    """One canonical item persisted by a memory store."""

    source_id: str
    kind: str
    data: dict[str, str | bool]
    sequence: int
    fetched: bool = False
    merge_failed: str | None = None


class MemoryStore(Protocol):
    """The narrow persistence contract used by :class:`MemoryPool`."""

    @property
    def path(self) -> Path | None:
        """The backing database path, if this store has one."""

    def list_kind(self, kind: str) -> tuple[StoredItem, ...]:
        """Return one kind's current canonical items in insertion order."""

    def list_all(self) -> tuple[StoredItem, ...]:
        """Return the complete current canonical state in insertion order."""

    def has_unfetched(self) -> bool:
        """True when at least one canonical item has not yet been included in a fetch."""

    def append_items(self, items: Iterable[StoredItem]) -> None:
        """Append fallback items after an exhausted semantic merge."""

    def replace_kind(
        self,
        kind: str,
        items: Iterable[StoredItem],
        inherited_fetched_ids: Mapping[str, Iterable[str]],
    ) -> None:
        """Atomically replace one kind's canonical representation."""

    def publish_fetch(self) -> str:
        """Create and return the next cumulative fetch-version JSON document."""

    def close(self) -> None:
        """Release any resources held by the store."""


class InMemoryStore:
    """The original RAM-only representation, retained for a future fallback mode.

    ``MemoryPool`` does not select this store automatically in the first SQLite
    version.  It is intentionally standalone so a later fallback policy does not
    need to reimplement the state model.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._items: dict[str, list[StoredItem]] = {}
        self._versions: dict[int, list[dict[str, str | bool]]] = {}
        self._closed = False

    @property
    def path(self) -> Path | None:
        return None

    def list_kind(self, kind: str) -> tuple[StoredItem, ...]:
        with self._lock:
            self._ensure_open()
            return tuple(self._copy_item(item) for item in self._items.get(kind, ()))

    def list_all(self) -> tuple[StoredItem, ...]:
        with self._lock:
            self._ensure_open()
            return tuple(
                self._copy_item(item)
                for item in sorted(
                    (item for items in self._items.values() for item in items),
                    key=lambda item: item.sequence,
                )
            )

    def has_unfetched(self) -> bool:
        with self._lock:
            self._ensure_open()
            return any(
                not item.fetched
                for items in self._items.values()
                for item in items
            )

    def append_items(self, items: Iterable[StoredItem]) -> None:
        normalized = tuple(self._copy_item(item) for item in items)
        with self._lock:
            self._ensure_open()
            for item in normalized:
                self._items.setdefault(item.kind, []).append(item)
            for kind in {item.kind for item in normalized}:
                self._items[kind].sort(key=lambda item: item.sequence)

    def replace_kind(
        self,
        kind: str,
        items: Iterable[StoredItem],
        inherited_fetched_ids: Mapping[str, Iterable[str]],
    ) -> None:
        normalized = tuple(self._copy_item(item) for item in items)
        inherited = {
            source_id: tuple(old_ids)
            for source_id, old_ids in inherited_fetched_ids.items()
        }
        with self._lock:
            self._ensure_open()
            old_fetched = {
                item.source_id for item in self._items.get(kind, ()) if item.fetched
            }
            replacement = [
                replace(
                    item,
                    fetched=any(
                        inherited_id in old_fetched
                        for inherited_id in inherited.get(item.source_id, ())
                    ),
                )
                for item in normalized
            ]
            replacement.sort(key=lambda item: item.sequence)
            self._items[kind] = replacement

    def publish_fetch(self) -> str:
        with self._lock:
            self._ensure_open()
            selected = sorted(
                (
                    item
                    for items in self._items.values()
                    for item in items
                    if not item.fetched
                ),
                key=lambda item: item.sequence,
            )
            version = len(self._versions) + 1
            candidate = copy.deepcopy(self._versions)
            candidate[version] = [copy.deepcopy(item.data) for item in selected]
            serialized = json.dumps(
                {f"v{number}": items for number, items in candidate.items()},
                ensure_ascii=False,
            )
            self._versions = candidate
            selected_ids = {item.source_id for item in selected}
            for kind, items in self._items.items():
                self._items[kind] = [
                    (
                        replace(item, fetched=True)
                        if item.source_id in selected_ids
                        else item
                    )
                    for item in items
                ]
            return serialized

    def close(self) -> None:
        with self._lock:
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("memory store is closed")

    @staticmethod
    def _copy_item(item: StoredItem) -> StoredItem:
        return replace(item, data=dict(item.data))


class SQLiteStore:
    """A thread-safe SQLite store for one ``MemoryPool`` instance."""

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS memory_items (
        source_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL CHECK (kind IN ('gap', 'test_bug')),
        data_json TEXT NOT NULL,
        sequence INTEGER NOT NULL UNIQUE,
        fetched INTEGER NOT NULL CHECK (fetched IN (0, 1)),
        merge_failed TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_memory_items_kind_sequence
        ON memory_items(kind, sequence);
    CREATE INDEX IF NOT EXISTS idx_memory_items_fetched_sequence
        ON memory_items(fetched, sequence);
    CREATE TABLE IF NOT EXISTS fetch_versions (
        version INTEGER PRIMARY KEY CHECK (version > 0),
        items_json TEXT NOT NULL
    );
    """

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._lock = threading.RLock()
        self._closed = False
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(str(self._path), check_same_thread=False)
        try:
            connection.executescript(self._SCHEMA)
        except Exception:
            connection.close()
            raise
        self._connection = connection

    @property
    def path(self) -> Path:
        return self._path

    def list_kind(self, kind: str) -> tuple[StoredItem, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT source_id, kind, data_json, sequence, fetched, merge_failed
                FROM memory_items
                WHERE kind = ?
                ORDER BY sequence
                """,
                (kind,),
            ).fetchall()
            return tuple(self._row_to_item(row) for row in rows)

    def list_all(self) -> tuple[StoredItem, ...]:
        with self._lock:
            self._ensure_open()
            rows = self._connection.execute(
                """
                SELECT source_id, kind, data_json, sequence, fetched, merge_failed
                FROM memory_items
                ORDER BY sequence
                """
            ).fetchall()
            return tuple(self._row_to_item(row) for row in rows)

    def has_unfetched(self) -> bool:
        with self._lock:
            self._ensure_open()
            row = self._connection.execute(
                "SELECT 1 FROM memory_items WHERE fetched = 0 LIMIT 1"
            ).fetchone()
            return row is not None

    def append_items(self, items: Iterable[StoredItem]) -> None:
        normalized = tuple(self._copy_item(item) for item in items)
        if not normalized:
            return
        with self._lock:
            self._ensure_open()
            with self._connection:
                self._connection.executemany(
                    """
                    INSERT INTO memory_items(
                        source_id, kind, data_json, sequence, fetched, merge_failed
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [self._item_values(item) for item in normalized],
                )

    def replace_kind(
        self,
        kind: str,
        items: Iterable[StoredItem],
        inherited_fetched_ids: Mapping[str, Iterable[str]],
    ) -> None:
        normalized = tuple(self._copy_item(item) for item in items)
        inherited = {
            source_id: tuple(old_ids)
            for source_id, old_ids in inherited_fetched_ids.items()
        }
        if any(item.kind != kind for item in normalized):
            raise ValueError("replacement items must match the replaced kind")

        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN")
                fetched_ids = {
                    row[0]
                    for row in self._connection.execute(
                        """
                        SELECT source_id FROM memory_items
                        WHERE kind = ? AND fetched = 1
                        """,
                        (kind,),
                    )
                }
                replacement = [
                    replace(
                        item,
                        fetched=any(
                            inherited_id in fetched_ids
                            for inherited_id in inherited.get(item.source_id, ())
                        ),
                    )
                    for item in normalized
                ]
                self._connection.execute(
                    "DELETE FROM memory_items WHERE kind = ?", (kind,)
                )
                self._connection.executemany(
                    """
                    INSERT INTO memory_items(
                        source_id, kind, data_json, sequence, fetched, merge_failed
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [self._item_values(item) for item in replacement],
                )
                self._connection.commit()
            except Exception:
                self._rollback()
                raise

    def publish_fetch(self) -> str:
        """Persist the next full version, then mark exactly its new items fetched."""

        with self._lock:
            self._ensure_open()
            try:
                self._connection.execute("BEGIN")
                selected = self._connection.execute(
                    """
                    SELECT source_id, kind, data_json, sequence, fetched, merge_failed
                    FROM memory_items
                    WHERE fetched = 0
                    ORDER BY sequence
                    """
                ).fetchall()
                version = self._connection.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 FROM fetch_versions"
                ).fetchone()[0]
                history = {
                    f"v{number}": json.loads(items_json)
                    for number, items_json in self._connection.execute(
                        "SELECT version, items_json FROM fetch_versions ORDER BY version"
                    )
                }
                history[f"v{version}"] = [
                    self._row_to_item(row).data for row in selected
                ]
                serialized = json.dumps(history, ensure_ascii=False)
                self._connection.execute(
                    "INSERT INTO fetch_versions(version, items_json) VALUES (?, ?)",
                    (version, json.dumps(history[f"v{version}"], ensure_ascii=False)),
                )
                if selected:
                    self._connection.executemany(
                        "UPDATE memory_items SET fetched = 1 WHERE source_id = ?",
                        [(row[0],) for row in selected],
                    )
                self._connection.commit()
                return serialized
            except Exception:
                self._rollback()
                raise

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("memory store is closed")

    def _rollback(self) -> None:
        try:
            self._connection.rollback()
        except sqlite3.Error:
            pass

    @staticmethod
    def _item_values(item: StoredItem) -> tuple[str, str, str, int, int, str | None]:
        return (
            item.source_id,
            item.kind,
            json.dumps(item.data, ensure_ascii=False),
            item.sequence,
            int(item.fetched),
            item.merge_failed,
        )

    @staticmethod
    def _row_to_item(row: tuple[object, ...]) -> StoredItem:
        source_id, kind, data_json, sequence, fetched, merge_failed = row
        data = json.loads(data_json)
        if type(data) is not dict:
            raise ValueError("stored memory item data must be a JSON object")
        return StoredItem(
            source_id=str(source_id),
            kind=str(kind),
            data=dict(data),
            sequence=int(sequence),
            fetched=bool(fetched),
            merge_failed=None if merge_failed is None else str(merge_failed),
        )

    @staticmethod
    def _copy_item(item: StoredItem) -> StoredItem:
        return replace(item, data=dict(item.data))
