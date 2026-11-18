"""Restore image-provided submodules in isolated DeepSWE worktrees."""

from pathlib import Path

from xgovernor_opencode_host.opencode_adapter import (
    FILE_TOOL_PERMISSIONS,
    OpenCodeAdapter,
    OpenCodeError,
    _git,
    _git_out,
    _nul_split,
)


def _gitlinks(repo: Path, revision: str) -> dict[str, str]:
    links = {}
    for entry in _nul_split(_git_out(repo, ["ls-tree", "-rz", revision])):
        metadata, path = entry.split("\t", 1)
        mode, _, commit = metadata.split()
        if mode == "160000":
            links[path] = commit
    return links


def _indexed_gitlinks(repo: Path) -> dict[str, str]:
    links = {}
    for entry in _nul_split(_git_out(repo, ["ls-files", "--stage", "-z"])):
        metadata, path = entry.split("\t", 1)
        mode, commit, _ = metadata.split()
        if mode == "160000":
            links[path] = commit
    return links


def _restore_submodules(source: Path, worktree: Path) -> None:
    for path, commit in _gitlinks(worktree, "HEAD").items():
        original = source / path
        target = worktree / path
        # Git otherwise walks up to the superproject for an empty checkout.
        if not (original / ".git").exists():
            raise OpenCodeError(f"DeepSWE task image has an uninitialized submodule: {original}")
        _git(worktree, ["clone", "--local", "--no-checkout", "--", str(original), str(target)])
        _git(target, ["checkout", "--detach", "--no-recurse-submodules", commit])
        _restore_submodules(original, target)


class DeepSWEOpenCodeAdapter(OpenCodeAdapter):
    # The benchmark image is the tool isolation boundary for this mode.
    session_permissions = [
        {**rule, "effect": "allow"}
        if rule["action"] in {"bash", "shell", "external_directory"}
        else rule
        for rule in FILE_TOOL_PERMISSIONS
    ]

    def _root_gitlinks(self, source: Path) -> frozenset[str]:
        committed_links = _gitlinks(source, "HEAD")
        if _indexed_gitlinks(source) != committed_links:
            raise OpenCodeError("DeepSWE expects the task image's committed submodule revisions")
        return frozenset(committed_links)

    def _prepare_worktree(self, worktree: Path) -> None:
        assert self.source_repo is not None
        _restore_submodules(self.source_repo, worktree)

    def _validate_snapshot_workspace(self, worktree: Path) -> None:
        links = _gitlinks(worktree, "HEAD")
        if _indexed_gitlinks(worktree) != links:
            raise OpenCodeError("DeepSWE submodule revisions must remain unchanged")
        if links:
            diff = _git(
                worktree, ["diff", "--quiet", "--ignore-submodules=untracked", "HEAD", "--", *links],
                check=False,
            )
            if diff.returncode != 0:
                raise OpenCodeError("DeepSWE submodule fixtures must remain unchanged")

    def _diff_patch(self, base_commit: str, head_commit: str) -> str:
        return _git_out(self._require_private(), [
            "diff", "--binary", base_commit, head_commit, "--", ".", ":(exclude,literal)verify.sh",
        ])
