"""External launch contract for the MCTS plugin.

The plugin process is configured by argv (mode/task), but xg and the local
harness both need one source of truth for how to build that argv and which
profile templates the host is allowed to resolve.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

PLUGIN_ID = "mcts"
ENTRYPOINT = ["python", "-m", "src.core.main"]
REQUIRED_ENV = (
    "ZSOFT_TASKS_DIR",
    "PYTHONPATH",
    "XIAOO_MCTS_MAX_CONCURRENT",
)
FORBIDDEN_ENV_PREFIX = "XGOVERNOR_LLM_"

PLUGIN_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = PLUGIN_DIR.parent
REPO_ROOT = PACKAGE_ROOT.parent
MANIFEST_PATH = PLUGIN_DIR / "plugin_manifest.json"


class LaunchSpecError(ValueError):
    """Raised when a launch spec or plugin manifest is invalid."""


def _require_relative_plugin_source(source: str) -> str:
    path = Path(source)
    if path.is_absolute():
        raise LaunchSpecError(f"profile source must be relative: {source}")
    if ".." in path.parts:
        raise LaunchSpecError(f"profile source must not contain '..': {source}")
    value = path.as_posix()
    if not value.startswith("res/task_plugins/"):
        raise LaunchSpecError(
            f"profile source must stay under res/task_plugins/: {source}"
        )
    return value


def validate_manifest(payload: Any) -> dict[str, Any]:
    """Validate the profile manifest and return it if valid."""

    if not isinstance(payload, dict):
        raise LaunchSpecError("plugin manifest must be a JSON object")
    if payload.get("plugin_id") != PLUGIN_ID:
        raise LaunchSpecError("plugin_manifest.plugin_id must be 'mcts'")

    resource_root = payload.get("resource_root")
    if not isinstance(resource_root, str) or not resource_root:
        raise LaunchSpecError("plugin_manifest.resource_root is required")
    if Path(resource_root).is_absolute() or ".." in Path(resource_root).parts:
        raise LaunchSpecError("plugin_manifest.resource_root must be relative")

    modes = payload.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise LaunchSpecError("plugin_manifest.modes must be a non-empty object")

    for mode_id, mode_config in modes.items():
        if not isinstance(mode_id, str) or not mode_id:
            raise LaunchSpecError("plugin_manifest mode names must be non-empty")
        if not isinstance(mode_config, dict):
            raise LaunchSpecError(f"mode {mode_id!r} must be an object")

        task_plugin_root = mode_config.get("task_plugin_root")
        if not isinstance(task_plugin_root, str) or not task_plugin_root:
            raise LaunchSpecError(f"mode {mode_id!r} needs task_plugin_root")
        _require_relative_plugin_source(f"{task_plugin_root}/")

        profiles = mode_config.get("profiles")
        if not isinstance(profiles, dict) or not profiles:
            raise LaunchSpecError(f"mode {mode_id!r} needs profiles")

        seen_sources: set[str] = set()
        for profile_id, profile in profiles.items():
            if not isinstance(profile_id, str) or not profile_id:
                raise LaunchSpecError("profile names must be non-empty")
            if not isinstance(profile, dict):
                raise LaunchSpecError(f"profile {profile_id!r} must be an object")
            if profile.get("owner") != "xgovernor":
                raise LaunchSpecError(
                    f"profile {profile_id!r} must be owned by xgovernor"
                )
            if not isinstance(profile.get("role"), str) or not profile["role"]:
                raise LaunchSpecError(f"profile {profile_id!r} needs a role")

            source = profile.get("source")
            if not isinstance(source, str) or not source:
                raise LaunchSpecError(f"profile {profile_id!r} needs a source")
            normalized = _require_relative_plugin_source(source)
            if normalized in seen_sources:
                raise LaunchSpecError(f"duplicate profile source: {normalized}")
            seen_sources.add(normalized)

    return payload


def load_manifest(path: str | Path | None = None) -> dict[str, Any]:
    path = Path(path) if path is not None else MANIFEST_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    return validate_manifest(payload)


def manifest_mode(manifest: Mapping[str, Any], mode: str) -> dict[str, Any]:
    if not isinstance(manifest, Mapping):
        raise LaunchSpecError("invalid plugin manifest")
    modes = manifest.get("modes", {})
    if mode not in modes:
        raise LaunchSpecError(f"unknown mode: {mode}")
    mode_config = modes[mode]
    if not isinstance(mode_config, Mapping):
        raise LaunchSpecError(f"mode {mode!r} must be an object")
    return dict(mode_config)


def required_env() -> tuple[str, ...]:
    return REQUIRED_ENV


def build_command(
    mode: str,
    request: Mapping[str, Any] | None = None,
    *,
    manifest: Mapping[str, Any] | None = None,
) -> list[str]:
    """Build the plugin argv used by xg/harness.

    ``mode`` must exist in the approved plugin manifest. ``request`` contains
    bench-specific fields such as ``task_id`` or ``instance_id``. The returned
    command deliberately contains no LLM credentials.
    """

    request = dict(request or {})
    manifest = manifest or load_manifest()
    manifest_mode(manifest, mode)

    if mode == "zsoft":
        task_id = request.get("task_id")
        if not task_id:
            raise LaunchSpecError("zsoft launch requires task_id")
        return [
            *ENTRYPOINT,
            "--mode",
            "zsoft",
            "--task-id",
            str(task_id),
        ]

    if mode == "sweverified":
        instance_id = request.get("instance_id")
        if not instance_id:
            raise LaunchSpecError("sweverified launch requires instance_id")
        split = request.get("split", "test")
        command = [
            *ENTRYPOINT,
            "--mode",
            "sweverified",
            "--instance-id",
            str(instance_id),
            "--split",
            str(split),
        ]
        if "testbench" in request:
            command.extend(["--testbench", str(request["testbench"])])
        return command

    raise LaunchSpecError(f"no launch command configured for mode: {mode}")


def resolve_profile_source(
    mode: str,
    profile: str,
    *,
    manifest: Mapping[str, Any] | None = None,
) -> Path:
    """Return the absolute filesystem path for a profile template.

    This is used to prove the source path resolves inside the approved
    ``res/task_plugins`` root. xg applies the same rule when it reads this
    manifest before spawning the plugin process.
    """

    manifest = manifest or load_manifest()
    mode_config = manifest_mode(manifest, mode)
    profiles = mode_config.get("profiles", {})
    if profile not in profiles:
        raise LaunchSpecError(f"unknown profile {profile!r} for mode {mode!r}")
    source = profiles[profile]["source"]
    normalized = _require_relative_plugin_source(source)
    resource_root = manifest["resource_root"]
    base = (REPO_ROOT / resource_root).resolve()
    return (base / normalized).resolve()
