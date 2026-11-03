"""LLM-backed, stateless semantic merging for canonical memory items."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from math import frexp, isfinite, ldexp
from numbers import Real
from typing import Any, Protocol

from .models import REQUIRED_FIELDS, SUPPORTED_KINDS


_MAX_BACKOFF_SECS = 60.0


@dataclass(frozen=True)
class SourceItem:
    """One canonical item supplied to a semantic merge request."""

    source_id: str
    is_new: bool
    data: dict[str, str | bool]


@dataclass(frozen=True)
class MergeGroup:
    """One validated merged item and all source items it represents."""

    source_ids: tuple[str, ...]
    item: dict[str, str | bool]


class MergeExhaustedError(RuntimeError):
    """Raised when every merge completion attempt violates its contract."""


class _MergeResponseError(ValueError):
    """An LLM response did not satisfy the merge response contract."""


class MergeAttemptRecorder(Protocol):
    """Receive the raw boundary data for every completion attempt."""

    def start_llm_attempt(
        self,
        kind: str,
        attempt: int,
        messages: list[dict[str, str]],
    ) -> str: ...

    def finish_llm_attempt(
        self,
        attempt_id: str,
        kind: str,
        attempt: int,
        content: object,
        error: Exception | None,
        stage: str,
    ) -> None: ...


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build one decoded JSON object while rejecting duplicate member names."""

    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _MergeResponseError(f"response JSON contains duplicate key: {key!r}")
        result[key] = value
    return result


def _unwrap_json_markdown_fence(content: str) -> str:
    """Remove one exact outer ``json`` fence while rejecting surrounding prose."""

    stripped = content.strip()
    lines = stripped.splitlines()
    if len(lines) >= 3 and lines[0].strip() == "```json" and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1])
    return content


class SemanticMerger:
    """Request and validate one semantic merge without retaining any state."""

    def __init__(
        self,
        complete: Callable[[list[dict[str, str]]], str],
        *,
        max_attempts: int = 3,
        backoff_secs: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        attempt_recorder: MergeAttemptRecorder | None = None,
    ) -> None:
        if type(max_attempts) is not int or not 1 <= max_attempts <= 3:
            raise ValueError("max_attempts must be a native integer from 1 through 3")
        if isinstance(backoff_secs, bool) or not isinstance(backoff_secs, Real):
            raise ValueError("backoff_secs must be a finite non-negative real number")
        try:
            normalized_backoff = float(backoff_secs)
        except OverflowError as exc:
            raise ValueError(
                "backoff_secs must be a finite non-negative real number"
            ) from exc
        if not isfinite(normalized_backoff) or normalized_backoff < 0:
            raise ValueError("backoff_secs must be a finite non-negative real number")

        self._complete = complete
        self._max_attempts = max_attempts
        self._backoff_secs = normalized_backoff
        self._sleep = sleep
        self._attempt_recorder = attempt_recorder

    def merge(
        self,
        kind: str,
        old: tuple[SourceItem, ...],
        new: tuple[SourceItem, ...],
    ) -> tuple[MergeGroup, ...]:
        """Merge one kind's existing and newly parsed canonical items."""

        inputs = self._validate_inputs(kind, old, new)
        last_error: Exception | None = None

        for attempt_index in range(self._max_attempts):
            attempt = attempt_index + 1
            messages = self._messages(kind, inputs)
            attempt_id = (
                self._attempt_recorder.start_llm_attempt(kind, attempt, messages)
                if self._attempt_recorder is not None
                else None
            )
            content: object = None
            completion_returned = False
            try:
                content = self._complete(messages)
                completion_returned = True
                result = self._validate_response(content, kind, inputs)
            except Exception as exc:
                last_error = exc
                if self._attempt_recorder is not None and attempt_id is not None:
                    self._attempt_recorder.finish_llm_attempt(
                        attempt_id,
                        kind,
                        attempt,
                        content,
                        exc,
                        "validation" if completion_returned else "completion",
                    )
                if attempt_index == self._max_attempts - 1:
                    break
                self._sleep(self._backoff_delay(attempt_index))
                continue

            if self._attempt_recorder is not None and attempt_id is not None:
                self._attempt_recorder.finish_llm_attempt(
                    attempt_id,
                    kind,
                    attempt,
                    content,
                    None,
                    "validated",
                )
            return result

        assert last_error is not None
        raise MergeExhaustedError(
            f"semantic merge exhausted after {self._max_attempts} attempts: {last_error}"
        ) from last_error

    def _backoff_delay(self, attempt_index: int) -> float:
        if self._backoff_secs == 0:
            return 0.0

        base_mantissa, base_exponent = frexp(self._backoff_secs)
        cap_mantissa, cap_exponent = frexp(_MAX_BACKOFF_SECS)
        scaled_exponent = base_exponent + attempt_index
        if scaled_exponent > cap_exponent or (
            scaled_exponent == cap_exponent and base_mantissa >= cap_mantissa
        ):
            return _MAX_BACKOFF_SECS
        return ldexp(self._backoff_secs, attempt_index)

    @staticmethod
    def _validate_inputs(
        kind: str, old: tuple[SourceItem, ...], new: tuple[SourceItem, ...]
    ) -> tuple[SourceItem, ...]:
        if type(old) is not tuple or type(new) is not tuple:
            raise ValueError("old and new inputs must be native tuples")
        if type(kind) is not str or kind not in SUPPORTED_KINDS:
            raise ValueError(f"unsupported merge kind: {kind!r}")
        inputs = old + new
        if not inputs:
            raise ValueError("merge requires at least one source item")

        seen_ids: set[str] = set()
        for expected_is_new, sources in ((False, old), (True, new)):
            for source in sources:
                if not isinstance(source, SourceItem):
                    raise ValueError("merge sources must be SourceItem instances")
                if type(source.source_id) is not str or not source.source_id:
                    raise ValueError("source_id must be a non-empty native str")
                if source.source_id in seen_ids:
                    raise ValueError(f"duplicate source_id: {source.source_id!r}")
                seen_ids.add(source.source_id)
                if source.is_new is not expected_is_new:
                    raise ValueError(
                        "old and new source is_new flags do not match their input"
                    )
                SemanticMerger._validate_source_data(source.data, kind)
        return inputs

    @staticmethod
    def _validate_source_data(data: object, kind: str) -> None:
        if type(data) is not dict:
            raise ValueError("source item data must be a native dict")
        for key, value in data.items():
            if type(key) is not str or not key:
                raise ValueError(
                    "source item data keys must be non-empty native str values"
                )
            if type(value) not in (str, bool):
                raise ValueError(
                    "source item data values must be native str or bool values"
                )
        if type(data.get("kind")) is not str or data["kind"] != kind:
            raise ValueError("every source item kind must exactly match the merge kind")

        complete = all(
            type(data.get(field)) is str and data[field].strip()
            for field in REQUIRED_FIELDS[kind]
        )
        if complete:
            if "invalid" in data:
                raise ValueError("complete source item data must not contain invalid")
        elif type(data.get("invalid")) is not bool or data["invalid"] is not True:
            raise ValueError("incomplete source item data must contain invalid=True")

    @staticmethod
    def _messages(kind: str, inputs: tuple[SourceItem, ...]) -> list[dict[str, str]]:
        system = (
            "You merge canonical memory findings of one requested kind. Merge sources "
            "only when they describe the same actual problem; items with the same kind "
            "must not merge automatically. Preserve complementary information. An "
            "unrelated old item must remain unchanged and alone. Output only this JSON "
            'shape: {"groups":[{"source_ids":["..."],"item":{"kind":"..."}}]}.'
        )
        user = json.dumps(
            {
                "kind": kind,
                "inputs": [
                    {
                        "source_id": source.source_id,
                        "is_new": source.is_new,
                        "item": source.data,
                    }
                    for source in inputs
                ],
            },
            ensure_ascii=False,
        )
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    @staticmethod
    def _validate_response(
        content: str,
        kind: str,
        inputs: tuple[SourceItem, ...],
    ) -> tuple[MergeGroup, ...]:
        if type(content) is not str or not content.strip():
            raise _MergeResponseError("completion content must be non-empty JSON text")
        normalized_content = _unwrap_json_markdown_fence(content)
        try:
            payload = json.loads(
                normalized_content,
                object_pairs_hook=_object_without_duplicate_keys,
            )
        except (TypeError, json.JSONDecodeError) as exc:
            raise _MergeResponseError("completion content is not valid JSON") from exc
        if not isinstance(payload, dict) or set(payload) != {"groups"}:
            raise _MergeResponseError("response must be an object with exactly groups")
        raw_groups = payload["groups"]
        if not isinstance(raw_groups, list) or not raw_groups:
            raise _MergeResponseError("response groups must be a non-empty list")

        sources_by_id = {source.source_id: source for source in inputs}
        input_order = {source.source_id: index for index, source in enumerate(inputs)}
        seen_ids: set[str] = set()
        validated: list[MergeGroup] = []

        for raw_group in raw_groups:
            if not isinstance(raw_group, dict) or set(raw_group) != {
                "source_ids",
                "item",
            }:
                raise _MergeResponseError(
                    "each group must be an object with exactly source_ids and item"
                )
            raw_ids = raw_group["source_ids"]
            if not isinstance(raw_ids, list) or not raw_ids:
                raise _MergeResponseError("group source_ids must be a non-empty list")
            source_ids: list[str] = []
            for source_id in raw_ids:
                if type(source_id) is not str:
                    raise _MergeResponseError(
                        "group source_ids must contain native str values"
                    )
                if source_id not in sources_by_id:
                    raise _MergeResponseError(
                        f"group contains unknown source_id: {source_id!r}"
                    )
                if source_id in seen_ids:
                    raise _MergeResponseError(
                        f"source_id appears more than once: {source_id!r}"
                    )
                seen_ids.add(source_id)
                source_ids.append(source_id)

            item = SemanticMerger._validate_item(raw_group["item"], kind)
            group_sources = tuple(sources_by_id[source_id] for source_id in source_ids)
            if not any(source.is_new for source in group_sources):
                if len(group_sources) != 1 or item != group_sources[0].data:
                    raise _MergeResponseError(
                        "old-only groups must contain exactly one unchanged old item"
                    )
            validated.append(MergeGroup(tuple(source_ids), item))

        expected_ids = set(sources_by_id)
        if seen_ids != expected_ids:
            missing = sorted(expected_ids - seen_ids)
            raise _MergeResponseError(f"response omits source_ids: {missing!r}")

        validated.sort(
            key=lambda group: min(
                input_order[source_id] for source_id in group.source_ids
            )
        )
        return tuple(validated)

    @staticmethod
    def _validate_item(raw_item: Any, kind: str) -> dict[str, str | bool]:
        if not isinstance(raw_item, dict):
            raise _MergeResponseError("group item must be a JSON object")
        item: dict[str, str | bool] = {}
        for key, value in raw_item.items():
            if type(key) is not str or not key:
                raise _MergeResponseError(
                    "item keys must be non-empty native str values"
                )
            if type(value) not in (str, bool):
                raise _MergeResponseError(
                    "item values must be native str or bool values"
                )
            item[key] = value
        if type(item.get("kind")) is not str or item["kind"] != kind:
            raise _MergeResponseError("item kind must exactly match the requested kind")

        if any(
            type(item.get(field)) is not str or not item[field].strip()
            for field in REQUIRED_FIELDS[kind]
        ):
            item["invalid"] = True
        else:
            item.pop("invalid", None)
        return item
