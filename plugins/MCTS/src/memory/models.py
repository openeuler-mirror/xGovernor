"""Data types shared by the standalone memory package."""

from __future__ import annotations

from dataclasses import dataclass


SUPPORTED_KINDS = ("gap", "test_bug")

REQUIRED_FIELDS = {
    "gap": (
        "finding",
        "why_missing",
        "requested_test",
        "discriminator",
        "snippet",
    ),
    "test_bug": ("test_name", "problem", "fix_request", "evidence"),
}


@dataclass(frozen=True)
class AddResult:
    success: bool
    kind_error: bool
    errors: tuple[str, ...]


@dataclass(frozen=True)
class ParsedItem:
    data: dict[str, str | bool]


@dataclass(frozen=True)
class ParseResult:
    items: tuple[ParsedItem, ...]
    kind_error: bool
    errors: tuple[str, ...]
