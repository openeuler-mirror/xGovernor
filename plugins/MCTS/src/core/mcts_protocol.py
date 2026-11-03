"""MCTS JSON-RPC v1 wire contract.

Transport, HTTP/SSE, 宿主 JSON-RPC sessions and MCTS scheduling are intentionally
outside this module. It validates the v1 envelope, method payloads, events and
cross-message completion-result invariants.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping

try:
    import jsonschema
    from jsonschema.exceptions import best_match
except ImportError as exc:  # pragma: no cover - exercised by installation checks
    raise RuntimeError(
        "MCTS RPC v1 requires jsonschema; install src/requirements.txt"
    ) from exc

JSONRPC_VERSION = "2.0"
PROTOCOL_VERSION = "mcts.rpc.v1"
PLUGIN_ID = "mcts"

_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "docs" / "protocols" / "mcts-rpc-v1.schema.json"
try:
    _SCHEMA = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
except OSError as exc:  # pragma: no cover - packaging/integrity failure
    raise RuntimeError(f"MCTS RPC v1 schema is unavailable: {_SCHEMA_PATH}") from exc

_FORMAT_CHECKER = jsonschema.FormatChecker()
_ROOT_VALIDATOR = jsonschema.Draft202012Validator(_SCHEMA, format_checker=_FORMAT_CHECKER)
jsonschema.Draft202012Validator.check_schema(_SCHEMA)

PLUGIN_METHODS = frozenset(_SCHEMA["x-methods"])
HOST_METHODS = frozenset(name for name in PLUGIN_METHODS if name.startswith("host."))
PLUGIN_METHODS = frozenset(name for name in PLUGIN_METHODS if name.startswith("plugin."))
PLUGIN_EVENTS = frozenset(name for name in _SCHEMA["x-notifications"] if name.startswith("plugin."))
HOST_JOB_EVENTS = frozenset(name for name in _SCHEMA["x-notifications"] if name.startswith("host.job."))
CAPABILITIES = frozenset(_SCHEMA["$defs"]["capability"]["enum"])
ALL_METHODS = PLUGIN_METHODS | HOST_METHODS
ALL_NOTIFICATIONS = PLUGIN_EVENTS | HOST_JOB_EVENTS

_METHOD_SCHEMAS = {
    "request_params": {
        method: {"$schema": _SCHEMA["$schema"], "$defs": _SCHEMA["$defs"], "$ref": spec["params"]["$ref"]}
        for method, spec in _SCHEMA["x-methods"].items()
    },
    "notification_params": {
        method: {"$schema": _SCHEMA["$schema"], "$defs": _SCHEMA["$defs"], "$ref": spec["params"]["$ref"]}
        for method, spec in _SCHEMA["x-notifications"].items()
    },
    "results": {
        method: {"$schema": _SCHEMA["$schema"], "$defs": _SCHEMA["$defs"], "$ref": spec["result"]["$ref"]}
        for method, spec in _SCHEMA["x-methods"].items()
    },
}
_SUBSCHEMA_VALIDATORS = {
    kind: {name: jsonschema.Draft202012Validator(schema, format_checker=_FORMAT_CHECKER)
           for name, schema in schemas.items()}
    for kind, schemas in _METHOD_SCHEMAS.items()
}
_MANIFEST_VALIDATOR = jsonschema.Draft202012Validator(
    {"$schema": _SCHEMA["$schema"], "$defs": _SCHEMA["$defs"], "$ref": "#/$defs/manifest"},
    format_checker=_FORMAT_CHECKER,
)
_ERROR_VALIDATOR = jsonschema.Draft202012Validator(
    {"$schema": _SCHEMA["$schema"], "$defs": _SCHEMA["$defs"], "$ref": "#/$defs/rpcError"},
    format_checker=_FORMAT_CHECKER,
)
_COMPLETION_VALIDATOR = jsonschema.Draft202012Validator(
    {"$schema": _SCHEMA["$schema"], "$defs": _SCHEMA["$defs"], "$ref": "#/$defs/completionResult"},
    format_checker=_FORMAT_CHECKER,
)


class ProtocolError(ValueError):
    """Raised when a JSON-RPC v1 message violates the wire contract."""


class RpcErrorCode:
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603
    CAPABILITY_DENIED = -32001
    RUN_NOT_FOUND = -32002
    BRANCH_NOT_FOUND = -32003
    DEADLINE_EXCEEDED = -32004
    CANCELLED = -32005
    STATE_EXPIRED = -32006
    IDEMPOTENCY_CONFLICT = -32007
    INCOMPATIBLE_PROTOCOL = -32008
    BRANCH_BUSY = -32009
    BUDGET_EXHAUSTED = -32010


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ProtocolError(f"{name} must be a non-empty string")
    return value


def _object(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError(f"{name} must be an object")
    return value


def _id(value: Any, *, null: bool = False) -> str | int | None:
    if value is None and null:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int)) or (isinstance(value, str) and not value):
        raise ProtocolError("request id must be a non-empty string or integer")
    return value


def _schema_error(errors: Any, name: str) -> ProtocolError:
    error = best_match(errors)
    if error is None:
        return ProtocolError(f"{name} does not match the MCTS RPC v1 schema")
    path = ".".join(str(part) for part in error.absolute_path)
    location = f" at {name}.{path}" if path else f" in {name}"
    return ProtocolError(f"{location}: {error.message}")


def _validate(validator: Any, value: Any, name: str) -> None:
    errors = validator.iter_errors(value)
    first = next(errors, None)
    if first is not None:
        raise _schema_error(iter((first, *errors)), name)


def _validate_method_payload(kind: str, method: str, value: Any, name: str) -> None:
    try:
        validator = _SUBSCHEMA_VALIDATORS[kind][method]
    except KeyError as exc:
        raise ProtocolError(f"unknown {kind.replace('_', ' ')} method: {method}") from exc
    _validate(validator, value, name)


def request(request_id: str | int, method: str, params: Mapping[str, Any]) -> dict[str, Any]:
    _id(request_id)
    _string(method, "method")
    if not isinstance(params, Mapping):
        raise ProtocolError("params must be an object")
    payload = _object(dict(params), "params")
    validate_params(method, payload)
    message = {"jsonrpc": JSONRPC_VERSION, "id": request_id, "method": method, "params": payload}
    _validate(_ROOT_VALIDATOR, message, "request")
    return message


def notification(method: str, params: Mapping[str, Any]) -> dict[str, Any]:
    _string(method, "method")
    if not isinstance(params, Mapping):
        raise ProtocolError("params must be an object")
    payload = _object(dict(params), "params")
    validate_notification_params(method, payload)
    message = {"jsonrpc": JSONRPC_VERSION, "method": method, "params": payload}
    _validate(_ROOT_VALIDATOR, message, "notification")
    return message


def result(response_id: str | int, value: Mapping[str, Any], *, method: str | None = None) -> dict[str, Any]:
    _id(response_id)
    if not isinstance(value, Mapping):
        raise ProtocolError("result must be an object")
    payload = _object(dict(value), "result")
    if method:
        validate_result(method, payload)
    message = {"jsonrpc": JSONRPC_VERSION, "id": response_id, "result": payload}
    _validate(_ROOT_VALIDATOR, message, "response")
    return message


def error(response_id: str | int | None, code: int, message: str, data: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(code, bool) or not isinstance(code, int):
        raise ProtocolError("error code must be an integer")
    _string(message, "error message")
    payload: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        payload["data"] = _object(dict(data), "error data")
    _validate(_ERROR_VALIDATOR, payload, "error")
    wire = {"jsonrpc": JSONRPC_VERSION, "id": response_id, "error": payload}
    _validate(_ROOT_VALIDATOR, wire, "response")
    return wire


def decode(raw: str | bytes | Mapping[str, Any]) -> dict[str, Any]:
    """Decode one v1 message using the published Draft 2020-12 schema."""
    if isinstance(raw, (str, bytes)):
        try:
            message = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ProtocolError("message is not valid JSON") from exc
    elif isinstance(raw, Mapping):
        message = dict(raw)
    else:
        raise ProtocolError("JSON-RPC v1 message must be an object")
    return validate_message(message)


def validate_message(message: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one already-decoded v1 message and return a plain dict.

    Structural validation is delegated to the published JSON Schema. The
    method-specific and completion-result checks add only runtime invariants
    that cannot be represented by a standalone message schema.
    """
    if not isinstance(message, Mapping):
        raise ProtocolError("JSON-RPC v1 message must be an object")
    message = dict(message)
    if not isinstance(message, dict):
        raise ProtocolError("JSON-RPC v1 message must be an object")
    _validate(_ROOT_VALIDATOR, message, "message")
    if "method" in message:
        method = message["method"]
        if "id" in message:
            validate_params(method, message["params"])
        else:
            validate_notification_params(method, message["params"])
    return message

def validate_params(method: str, params: Mapping[str, Any]) -> None:
    """Validate request params against the method's schema definition."""
    if not isinstance(params, Mapping):
        raise ProtocolError("params must be an object")
    _validate_method_payload("request_params", method, dict(params), f"{method}.params")


def validate_notification_params(method: str, params: Mapping[str, Any]) -> None:
    """Validate notification params against the method's schema definition."""
    if not isinstance(params, Mapping):
        raise ProtocolError("params must be an object")
    _validate_method_payload("notification_params", method, dict(params), f"{method}.params")
    if method == "plugin.completed":
        validate_completion_result(params["result"])


def validate_result(method: str, value: Mapping[str, Any]) -> None:
    """Validate a successful response using the request method's result schema."""
    if not isinstance(value, Mapping):
        raise ProtocolError("result must be an object")
    _validate_method_payload("results", method, dict(value), f"{method}.result")
    if method == "plugin.wait" and "result" in value:
        validate_completion_result(value["result"])


def validate_completion_result(value: Any) -> None:
    """Validate completion shape with the schema and cross-reference IDs."""
    _validate(_COMPLETION_VALIDATOR, value, "completion result")
    obj = value
    evidence_ids = [item["evidence_id"] for item in obj["evidence"]]
    if len(set(evidence_ids)) != len(evidence_ids):
        raise ProtocolError("evidence IDs must be unique")
    evidence_set = set(evidence_ids)

    candidate_ids = [item["candidate_id"] for item in obj["candidates"]]
    if len(set(candidate_ids)) != len(candidate_ids):
        raise ProtocolError("candidate IDs must be unique")
    candidates = {item["candidate_id"]: item for item in obj["candidates"]}
    for candidate in obj["candidates"]:
        unknown = set(candidate["evidence_refs"]) - evidence_set
        if unknown:
            raise ProtocolError(f"unknown evidence reference: {sorted(unknown)[0]}")

    selected = obj["selected_candidate_id"]
    if selected is not None:
        if selected not in candidates:
            raise ProtocolError("selected candidate does not exist")
        plan = obj["apply_plan"]
        if plan["state_id"] != candidates[selected]["state_id"]:
            raise ProtocolError("apply_plan.state_id must match selected candidate")


@dataclass(frozen=True)
class PluginManifest:
    plugin_id: str = PLUGIN_ID
    version: str = "1.0.0"
    triggers: tuple[str, ...] = ("/mcts",)
    methods: tuple[str, ...] = tuple(sorted(PLUGIN_METHODS))
    required_capabilities: tuple[str, ...] = ("snapshot", "fork", "agent_turn")
    optional_capabilities: tuple[str, ...] = tuple(sorted(CAPABILITIES - {"snapshot", "fork", "agent_turn"}))
    events: tuple[str, ...] = tuple(sorted(PLUGIN_EVENTS))
    api_version: str = PROTOCOL_VERSION

    def to_dict(self) -> dict[str, Any]:
        value = {"plugin_id": self.plugin_id, "version": self.version, "api_version": self.api_version, "triggers": list(self.triggers), "methods": list(self.methods), "required_host_capabilities": list(self.required_capabilities), "optional_host_capabilities": list(self.optional_capabilities), "events": list(self.events)}
        _validate(_MANIFEST_VALIDATOR, value, "manifest")
        if set(self.required_capabilities) & set(self.optional_capabilities): raise ProtocolError("required and optional capabilities overlap")
        return value


def require_capability(granted: tuple[str, ...] | list[str] | set[str], capability: str) -> None:
    if capability not in CAPABILITIES: raise ProtocolError(f"unknown capability: {capability}")
    if capability not in granted: raise ProtocolError(f"capability not granted: {capability}")


__all__ = [
    "ALL_METHODS", "ALL_NOTIFICATIONS", "CAPABILITIES", "HOST_JOB_EVENTS", "HOST_METHODS", "PLUGIN_EVENTS", "PLUGIN_ID", "PLUGIN_METHODS", "JSONRPC_VERSION", "PROTOCOL_VERSION", "PluginManifest", "ProtocolError", "RpcErrorCode", "decode", "error", "notification", "request", "require_capability", "result", "validate_completion_result", "validate_message", "validate_notification_params", "validate_params", "validate_result",
]
