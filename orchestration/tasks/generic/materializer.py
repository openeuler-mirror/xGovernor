"""Materializer for generic mode: the root-state prepare step already materialized files."""

from xgovernor_tasks.common.mode import MATERIALIZERS, register


@register(MATERIALIZERS, "generic-noop")
def generic_noop(task) -> str:
    return "echo 'generic root state already materialized'"
