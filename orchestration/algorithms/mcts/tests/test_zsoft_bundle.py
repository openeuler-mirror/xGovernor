#!/usr/bin/env python3
from __future__ import annotations

import io
import tarfile
import tempfile
import unittest
from pathlib import Path

from xgovernor_tasks.zsoft import bundle as zb


class ZSoftBundleTest(unittest.TestCase):
    def _tar(self, files: dict[str, bytes]) -> Path:
        tmp = Path(tempfile.mkdtemp(prefix="zsoft-bundle-test-"))
        tar_path = tmp / "bundle.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            for name, data in files.items():
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
        return tar_path

    def test_source_internal_private_dir_is_allowed(self) -> None:
        # libxml2 源码内合法目录：include/private，不得误判为泄漏。
        tar = self._tar({
            "task/repo-local/include/private/parser.h": b"#define XML_PARSER_H",
            "task/repo-local/parser.c": b"int main(void){return 0;}",
        })
        result = zb.verify_bundle(tar)
        self.assertTrue(result["ok"])

    def test_task_top_level_private_dir_is_rejected(self) -> None:
        tar = self._tar({"task/private/token.txt": b"secret"})
        with self.assertRaisesRegex(RuntimeError, "judge-private"):
            zb.verify_bundle(tar)

    def test_forbidden_file_rejected_anywhere(self) -> None:
        # reference-poc 无论在哪层（包括源码内部）都必须拦截。
        tar = self._tar({"task/repo-local/tests/reference-poc": b"#!/bin/sh"})
        with self.assertRaisesRegex(RuntimeError, "judge-private"):
            zb.verify_bundle(tar)

    def test_assert_no_forbidden_source_internal_ok(self) -> None:
        with tempfile.TemporaryDirectory(prefix="zsoft-staging-") as tmp:
            staging = Path(tmp)
            task_dir = staging / zb.BUNDLE_TASK_DIR
            inner = task_dir / "repo-local" / "include" / "private"
            inner.mkdir(parents=True)
            (inner / "parser.h").write_text("#define XML_PARSER_H", encoding="utf-8")
            zb._assert_no_forbidden(staging)  # 不应抛异常

    def test_assert_no_forbidden_task_top_private_raises(self) -> None:
        with tempfile.TemporaryDirectory(prefix="zsoft-staging-") as tmp:
            staging = Path(tmp)
            task_dir = staging / zb.BUNDLE_TASK_DIR
            (task_dir / "private").mkdir(parents=True)
            (task_dir / "private" / "token.txt").write_text("secret", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "forbidden directory"):
                zb._assert_no_forbidden(staging)


if __name__ == "__main__":
    unittest.main()
