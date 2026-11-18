"""SWE patches must reproduce the candidate without changing its repository."""
from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from xgovernor_tasks.swe_bench import artifact


class LocalHost:
    def __init__(self, repo, tmpdir, *, object_results=False):
        self.repo = repo
        self.env = {
            **os.environ, "TMPDIR": str(tmpdir),
            "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_AUTHOR_NAME": "Test", "GIT_AUTHOR_EMAIL": "test@example.invalid",
            "GIT_COMMITTER_NAME": "Test", "GIT_COMMITTER_EMAIL": "test@example.invalid",
        }
        self.object_results = object_results
        self.jobs = []

    def exec(self, branch_id, argv, *, cwd, timeout_ms, idempotency_key):
        assert branch_id == "candidate"
        assert cwd == artifact.REPO
        result = subprocess.run(
            argv, cwd=self.repo, env=self.env, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=timeout_ms / 1000,
        )
        job = dict(status="completed", exit_code=result.returncode,
                   output=result.stdout.decode(), truncated=False)
        self.jobs.append(job)
        return {"job_id": len(self.jobs) - 1}

    def wait(self, receipt):
        job = self.jobs[receipt["job_id"]]
        return SimpleNamespace(**job) if self.object_results else job


def snapshot(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            result[str(path.relative_to(root))] = ("link", os.readlink(path))
        elif path.is_file():
            result[str(path.relative_to(root))] = (
                path.stat().st_mode, hashlib.sha256(path.read_bytes()).hexdigest(),
            )
    return result


class SweArtifactTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="swe-artifact-")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name) / "colon: and space"
        self.repo = self.root / "repo: candidate"
        self.tmpdir = self.root / "temporary: objects"
        self.repo.mkdir(parents=True)
        self.tmpdir.mkdir()
        self.host = LocalHost(self.repo, self.tmpdir)
        self.task = SimpleNamespace(spec=SimpleNamespace(oracle_rel_path="testcases.py"))
        self.git("init", "-q")
        (self.repo / ".gitignore").write_text("*.log\nbuild/\n")
        (self.repo / "app.py").write_text("value = 1\n")
        (self.repo / "obsolete.py").write_text("old = True\n")
        (self.repo / "existing.bin").write_bytes(bytes(range(256)))
        self.git("add", "-A")
        self.git("commit", "-qm", "base")

    def git(self, *args, repo=None, data=None):
        return subprocess.run(
            ["git", *args], cwd=repo or self.repo, env=self.host.env,
            input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            check=True, timeout=15,
        ).stdout

    def export(self, host=None):
        host = host or self.host
        before = snapshot(self.root)
        head = self.git("rev-parse", "HEAD")
        result = artifact.git_diff(None, self.task, host=host, branch_id="candidate",
                                   wait_job=host.wait)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(isinstance(item, str) for item in result))
        self.assertEqual(snapshot(self.root), before)
        self.assertEqual(self.git("rev-parse", "HEAD"), head)
        self.assertEqual(list(self.tmpdir.iterdir()), [])
        return result

    def apply(self, patch):
        clean = self.root / "clean"
        self.git("clone", "-q", "--no-hardlinks", str(self.repo), str(clean))
        self.git("apply", "--binary", repo=clean, data=patch.encode())
        return clean

    def test_product_changes_apply_to_clean_head(self):
        (self.repo / "app.py").write_text("from extra import value\n")
        (self.repo / "obsolete.py").unlink()
        (self.repo / "existing.bin").write_bytes(bytes(reversed(range(256))))
        additions = {
            "extra.py": b"value = 42\n",
            "blob.bin": bytes(range(256)) * 4,
            "colon: file.txt": b"new file\n",
            "tool.sh": b"#!/bin/sh\necho ok\n",
        }
        for name, data in additions.items():
            (self.repo / name).write_bytes(data)
        (self.repo / "tool.sh").chmod(0o755)
        (self.repo / "alias").symlink_to("extra.py")
        (self.repo / "testcases.py").write_text("oracle\n")
        (self.repo / "run.log").write_text("ignored\n")
        (self.repo / "build").mkdir()
        (self.repo / "build" / "generated").write_text("ignored\n")
        status, patch = self.export()
        self.assertIn("?? extra.py", status)
        for mode in ("100644", "100755", "120000"):
            self.assertIn(f"new file mode {mode}", patch)
        self.assertIn("deleted file mode 100644", patch)
        self.assertIn("GIT binary patch", patch)
        for excluded in ("testcases.py", "run.log", "build/generated"):
            self.assertNotIn(excluded, patch)
        clean = self.apply(patch)
        for name, data in additions.items():
            self.assertEqual((clean / name).read_bytes(), data)
        self.assertEqual((clean / "app.py").read_text(), "from extra import value\n")
        self.assertEqual((clean / "existing.bin").read_bytes(), bytes(reversed(range(256))))
        self.assertFalse((clean / "obsolete.py").exists())
        self.assertEqual(os.readlink(clean / "alias"), "extra.py")
        self.assertTrue((clean / "tool.sh").stat().st_mode & 0o111)

    def test_new_file_is_present_in_exported_patch(self):
        (self.repo / "extra.py").write_text("value = 42\n")
        _, patch = artifact.git_diff(None, self.task, host=self.host,
                                     branch_id="candidate", wait_job=self.host.wait)
        self.assertIn("diff --git a/extra.py b/extra.py\nnew file mode 100644", patch)
        self.assertEqual((self.apply(patch) / "extra.py").read_text(), "value = 42\n")

    def test_staged_and_unstaged_changes_preserve_real_index(self):
        (self.repo / "staged.py").write_text("new = True\n")
        (self.repo / "app.py").write_text("value = 2\n")
        self.git("add", "staged.py", "app.py")
        (self.repo / "app.py").write_text("value = 3\n")
        _, patch = self.export(LocalHost(self.repo, self.tmpdir, object_results=True))
        clean = self.apply(patch)
        self.assertEqual((clean / "app.py").read_text(), "value = 3\n")
        self.assertEqual((clean / "staged.py").read_text(), "new = True\n")

    def test_oracle_excluded_when_tracked_modified_or_deleted(self):
        oracle = self.repo / "testcases.py"
        oracle.write_text("original\n")
        self.git("add", "testcases.py")
        self.git("commit", "-qm", "track oracle")
        (self.repo / "nested").mkdir()
        (self.repo / "nested" / "testcases.py").write_text("product file\n")
        for delete in (False, True):
            with self.subTest(delete=delete):
                if delete:
                    oracle.unlink()
                else:
                    oracle.write_text("changed\n")
                    self.git("add", "testcases.py")
                _, patch = self.export()
                self.assertNotIn("a/testcases.py", patch)
                self.assertNotIn("b/testcases.py", patch)
                self.assertIn("b/nested/testcases.py", patch)

    def test_oracle_path_is_literal_and_shell_quoted(self):
        name = "oracle: [x] 'file.py"
        self.task.spec.oracle_rel_path = name
        (self.repo / name).write_text("oracle\n")
        (self.repo / "oracle: x 'file.py").write_text("product\n")
        _, patch = self.export()
        self.assertNotIn(name, patch)
        self.assertIn("product", patch)

    def test_empty_product_patch(self):
        self.assertEqual(self.export()[1], "")
        (self.repo / "testcases.py").write_text("oracle\n")
        (self.repo / "run.log").write_text("ignored\n")
        self.assertEqual(self.export()[1], "")

    def test_executable_modes_with_filemode_disabled(self):
        self.git("config", "core.filemode", "false")
        (self.repo / "app.py").chmod(0o755)
        (self.repo / "new.sh").write_text("#!/bin/sh\n")
        (self.repo / "new.sh").chmod(0o755)
        _, patch = self.export()
        self.assertIn("old mode 100644\nnew mode 100755", patch)
        self.assertIn("new file mode 100755", patch)

    def test_linked_worktree_and_packed_shared_split_index(self):
        self.git("gc", "-q")
        worktree = self.root / "work: tree"
        shared = self.root / "shared: clone"
        self.git("worktree", "add", "-q", "-b", "candidate", str(worktree))
        self.git("clone", "-q", "--shared", str(self.repo), str(shared))
        self.git("update-index", "--split-index")
        for repo in (self.repo, worktree, shared):
            with self.subTest(repo=repo.name):
                (repo / "new.py").write_text("value = 2\n")
                _, patch = self.export(LocalHost(repo, self.tmpdir))
                self.assertIn("new file mode 100644", patch)
                self.assertIn("b/new.py", patch)

    def test_git_failure_cleans_temporary_files(self):
        # A failing clean filter exercises cleanup after temporary staging starts.
        self.git("config", "filter.broken.clean", "false")
        self.git("config", "filter.broken.required", "true")
        (self.repo / ".gitattributes").write_text("*.new filter=broken\n")
        (self.repo / "candidate.new").write_text("new\n")
        before = snapshot(self.root)
        with self.assertRaisesRegex(RuntimeError, "export.*diff"):
            artifact.git_diff(None, self.task, host=self.host, branch_id="candidate",
                              wait_job=self.host.wait)
        self.assertEqual(snapshot(self.root), before)
        self.assertEqual(list(self.tmpdir.iterdir()), [])

    def test_failed_truncated_or_unknown_job_is_rejected(self):
        for object_results in (False, True):
            for command in (0, 1):
                for fields in ({"exit_code": 1}, {"truncated": True},
                               {"exit_code": None}, {"status": "failed"}):
                    with self.subTest(object_results=object_results, command=command,
                                      fields=fields):
                        jobs = [dict(status="completed", exit_code=0,
                                     output="partial patch", truncated=False) for _ in range(2)]
                        jobs[command].update(fields)
                        results = [SimpleNamespace(**job) if object_results else job
                                   for job in jobs]
                        with self.assertRaises(RuntimeError):
                            artifact.git_diff(None, self.task, host=self.host,
                                              branch_id="candidate", wait_job=mock.Mock(side_effect=results))

    def test_wait_job_is_required(self):
        with self.assertRaisesRegex(RuntimeError, "requires wait_job"):
            artifact.git_diff(None, self.task, host=self.host, branch_id="candidate")

    def test_selector_reads_complete_patch_and_releases_branch(self):
        from xgovernor_mcts.engine.submission_selector import _read_artifact

        (self.repo / "new.py").write_text("value = 42\n")
        self.task.spec.artifact_reader_fn = lambda: artifact.git_diff
        self.host.fork = mock.Mock(return_value={"branch_id": "candidate"})
        self.host.release = mock.Mock()
        node = SimpleNamespace(node_id="solution", state_id="snapshot")
        status, patch = _read_artifact(node, self.task, self.host, self.host.wait)
        self.assertIn("?? new.py", status)
        self.assertIn("new file mode 100644", patch)
        self.host.release.assert_called_once_with(
            "candidate", timeout_ms=30_000,
            idempotency_key="selector:solution:artifact-release",
        )

    def test_missing_head_fails_without_creating_real_index(self):
        empty = self.root / "unborn"
        empty.mkdir()
        self.git("init", "-q", repo=empty)
        (empty / "new.py").write_text("new\n")
        host = LocalHost(empty, self.tmpdir)
        before = snapshot(self.root)
        with self.assertRaisesRegex(RuntimeError, "export.*diff"):
            artifact.git_diff(None, self.task, host=host, branch_id="candidate",
                              wait_job=host.wait)
        self.assertEqual(snapshot(self.root), before)
        self.assertEqual(list(self.tmpdir.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
