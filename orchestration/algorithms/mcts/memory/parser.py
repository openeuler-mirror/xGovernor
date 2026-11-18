"""Parser for the item-formatted query emitted by Solver prompts."""

from __future__ import annotations

import re

from .models import ParseResult, ParsedItem, REQUIRED_FIELDS, SUPPORTED_KINDS


_ITEM_TAG_START_PATTERN = re.compile(
    r"<\s*(?P<closing>/\s*)?item(?=\s|>)", re.IGNORECASE
)
_ITEM_CLOSE_PATTERN = re.compile(r"<\s*/\s*item\s*>", re.IGNORECASE)
_ATTRIBUTE_NAME_PATTERN = re.compile(r"[A-Za-z_:][A-Za-z0-9:_.-]*")
_FIELD_PATTERN = re.compile(
    r"^\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*:\s?(?P<value>.*)$"
)


def parse_query(text: str) -> ParseResult:
    """Parse supported ``<item>`` blocks from one Solver query.

    Invalid or absent ``kind`` attributes make only that item ineligible; valid
    sibling items are still returned.
    """

    item_blocks = _extract_item_blocks(text)
    if not item_blocks:
        return ParseResult(items=(), kind_error=False, errors=("no <item> found",))

    items: list[ParsedItem] = []
    errors: list[str] = []
    kind_error = False

    for index, (attributes, body) in enumerate(item_blocks, start=1):
        kind = _parse_kind(attributes)
        if kind is None:
            errors.append(f"item {index}: missing kind attribute")
            kind_error = True
            continue
        if not kind:
            errors.append(f"item {index}: blank kind attribute")
            kind_error = True
            continue
        if kind not in SUPPORTED_KINDS:
            errors.append(f"item {index}: unsupported kind {kind!r}")
            kind_error = True
            continue

        fields = _parse_fields(body)
        data: dict[str, str | bool] = {"kind": kind}
        data.update(
            name_value for name_value in fields.items() if name_value[0] != "kind"
        )
        if any(not fields.get(name, "").strip() for name in REQUIRED_FIELDS[kind]):
            data["invalid"] = True
        items.append(ParsedItem(data=data))

    return ParseResult(items=tuple(items), kind_error=kind_error, errors=tuple(errors))


def _extract_item_blocks(text: str) -> tuple[tuple[str, str], ...]:
    """Extract item attributes and bodies without treating quoted ``>`` as tags."""

    blocks: list[tuple[str, str]] = []
    active_attributes: str | None = None
    body_start = 0
    position = 0

    while True:
        tag_start = _ITEM_TAG_START_PATTERN.search(text, position)
        if tag_start is None:
            break

        if tag_start.group("closing") is not None:
            closing = _ITEM_CLOSE_PATTERN.match(text, tag_start.start())
            if closing is None:
                position = tag_start.end()
                continue
            if not _is_line_item_tag(text, tag_start.start()):
                position = closing.end()
                continue
            if active_attributes is not None:
                blocks.append((active_attributes, text[body_start : tag_start.start()]))
                active_attributes = None
            position = closing.end()
            continue

        if not _is_line_item_tag(text, tag_start.start()):
            position = tag_start.end()
            continue

        opening_end = _find_open_tag_end(text, tag_start.end())
        if opening_end is None:
            break

        active_attributes = text[tag_start.end() : opening_end]
        body_start = opening_end + 1
        position = body_start

    return tuple(blocks)


def _is_line_item_tag(text: str, position: int) -> bool:
    """Return whether an item tag starts a line after optional whitespace."""

    line_start = text.rfind("\n", 0, position) + 1
    return all(character.isspace() for character in text[line_start:position])


def _find_open_tag_end(text: str, position: int) -> int | None:
    """Find an opening tag's closing bracket while honoring quoted values."""

    quote: str | None = None
    while position < len(text):
        character = text[position]
        if quote is not None:
            if character == quote:
                quote = None
        elif character in ('"', "'"):
            quote = character
        elif character == ">":
            return position
        position += 1
    return None


def _parse_kind(attributes: str) -> str | None:
    """Return the first normalized, quoted ``kind`` attribute, if present."""

    position = 0
    needs_separator = False

    while position < len(attributes):
        token_start = position
        while position < len(attributes) and attributes[position].isspace():
            position += 1
        if position == len(attributes):
            break
        if needs_separator and position == token_start:
            position = _skip_to_whitespace(attributes, position)
            continue

        name_match = _ATTRIBUTE_NAME_PATTERN.match(attributes, position)
        if name_match is None:
            position = _skip_to_whitespace(attributes, position)
            needs_separator = True
            continue

        name = name_match.group()
        position = name_match.end()

        equals_position = position
        while (
            equals_position < len(attributes) and attributes[equals_position].isspace()
        ):
            equals_position += 1
        if equals_position == len(attributes) or attributes[equals_position] != "=":
            needs_separator = True
            continue

        position = equals_position + 1
        while position < len(attributes) and attributes[position].isspace():
            position += 1
        if position == len(attributes):
            break

        quote = attributes[position]
        if quote not in ('"', "'"):
            position = _skip_to_whitespace(attributes, position)
            needs_separator = True
            continue

        value_start = position + 1
        value_end = attributes.find(quote, value_start)
        if value_end == -1:
            break
        if name.lower() == "kind":
            return attributes[value_start:value_end].strip().lower()

        position = value_end + 1
        needs_separator = True

    return None


def _skip_to_whitespace(text: str, position: int) -> int:
    """Advance to the first whitespace character after a malformed token."""

    while position < len(text) and not text[position].isspace():
        position += 1
    return position


def _parse_fields(body: str) -> dict[str, str]:
    """Parse field lines and append each following non-field line to it."""

    fields: dict[str, str] = {}
    current_name: str | None = None

    for line in body.strip().splitlines():
        field = _FIELD_PATTERN.match(line)
        if field:
            current_name = field.group("name")
            fields[current_name] = field.group("value").strip()
        elif current_name is not None:
            fields[current_name] = f"{fields[current_name]}\n{line}"

    return fields
