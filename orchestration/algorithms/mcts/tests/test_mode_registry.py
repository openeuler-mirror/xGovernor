#!/usr/bin/env python3
"""模式注册层最小测试：两个内置模式注册完整、字段正确、可实例化 adapter。"""
from __future__ import annotations

import argparse
import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest import mock

from xgovernor_tasks.common import mode
from xgovernor_tasks.swe_bench import loader as swe_loader
from xgovernor_tasks.swe_bench import scorer as swe_scorer
from xgovernor_tasks.zsoft import scorer as zsoft_scorer


class ModeRegistryTest(unittest.TestCase):
    def test_builtin_modes_registered(self) -> None:
        self.assertEqual(sorted(mode.MODES.all()), ["deepswe", "generic", "sweverified", "zsoft"])

    def test_generic_spec(self) -> None:
        spec = mode.MODES.get("generic")
        self.assertEqual(spec.load_task, "generic_task")
        self.assertEqual(spec.scorer, "generic_test")
        self.assertEqual(spec.materialize, "generic-noop")
        self.assertEqual(spec.artifact_reader, "generic_repo")
        self.assertEqual(spec.oracle_rel_path, "verify.sh")
        self.assertEqual(spec.oracle_host_name, "verify.sh")
        self.assertIsNone(spec.official_eval)
        self.assertEqual(spec.runtime_backend, "docker")
        for filename in spec.prompts().values():
            self.assertTrue((mode.PROMPTS_ROOT / filename).is_file(), filename)

    def test_sweverified_spec(self) -> None:
        spec = mode.MODES.get("sweverified")
        self.assertEqual(spec.load_task, "swe_task")
        self.assertEqual(spec.scorer, "pytest")
        self.assertEqual(spec.materialize, "swe-instance")
        self.assertEqual(spec.artifact_reader, "git_diff")
        self.assertEqual(spec.oracle_rel_path, "testcases.py")
        self.assertEqual(spec.oracle_host_name, "testcases.py")
        self.assertIn("E2B_TEMPLATE_NAME", spec.env)
        # 提示词文件齐全
        for filename in spec.prompts().values():
            self.assertTrue((mode.PROMPTS_ROOT / filename).is_file(), filename)

    def test_zsoft_spec(self) -> None:
        spec = mode.MODES.get("zsoft")
        self.assertEqual(spec.load_task, "zsoft_task")
        self.assertEqual(spec.scorer, "zsoft-verify")
        self.assertEqual(spec.materialize, "zsoft-bundle")
        self.assertEqual(spec.artifact_reader, "poc_file")
        self.assertEqual(spec.oracle_rel_path, "verify.py")
        for filename in spec.prompts().values():
            self.assertTrue((mode.PROMPTS_ROOT / filename).is_file(), filename)

    def test_registry_names_resolve(self) -> None:
        spec = mode.MODES.get("sweverified")
        self.assertIsNotNone(spec.scorer_fn())
        self.assertIsNotNone(spec.materializer())
        self.assertIsNotNone(spec.artifact_reader_fn())
        self.assertIsNotNone(spec.loader())
        self.assertIsNotNone(spec.official_fn())

    def test_unknown_mode_raises(self) -> None:
        with self.assertRaises(SystemExit):
            mode.MODES.get("no-such-mode")

    def test_swe_task_loader_strips_forbidden_columns(self) -> None:
        spec = mode.MODES.get("sweverified")
        fake_ds = [
            {"instance_id": "other-task", "problem_statement": "different bug"},
            {
                "instance_id": "demo-1",
                "problem_statement": "bug",
                "patch": "secret",
                "test_patch": "secret",
            }
        ]
        fake_module = mock.Mock()
        fake_module.load_dataset.return_value = fake_ds
        with mock.patch.dict(sys.modules, {"datasets": fake_module}):
            task = swe_loader.swe_task(spec, testbench="sweverified", instance_id="demo-1", split="test")
        self.assertEqual(task.instance_id, "demo-1")
        self.assertNotIn("patch", task.row)
        self.assertNotIn("test_patch", task.row)
        self.assertEqual(task.testcases_path, "/home/user/workspace/repo/testcases.py")
        self.assertTrue(task.materialize_script().startswith("set -euo pipefail"))
        self.assertIn("SWEBENCH_REPO_READY", task.materialize_script())

    def test_swe_task_loader_unknown_instance(self) -> None:
        spec = mode.MODES.get("sweverified")
        fake_module = mock.Mock()
        fake_module.load_dataset.return_value = [{"instance_id": "demo-1"}]
        with mock.patch.dict(sys.modules, {"datasets": fake_module}):
            with self.assertRaisesRegex(SystemExit, "missing-task.*not found"):
                swe_loader.swe_task(spec, testbench="sweverified", instance_id="missing-task")

    def test_swe_cli_requires_instance_id(self) -> None:
        parser = argparse.ArgumentParser()
        mode.MODES.get("sweverified").configure_cli(parser)
        args = parser.parse_args(["--instance-id", "astropy__astropy-12907"])
        self.assertEqual(args.instance_id, "astropy__astropy-12907")
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args([])
            with self.assertRaises(SystemExit):
                parser.parse_args(["--instance-id", "demo-1", "--index", "0"])




if __name__ == "__main__":
    unittest.main()
