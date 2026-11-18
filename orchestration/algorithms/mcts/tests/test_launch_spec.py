from __future__ import annotations

import copy
import unittest

from xgovernor_mcts.bridge import launch_spec


class LaunchCommandTest(unittest.TestCase):
    def test_zsoft_command_injects_task_id(self) -> None:
        command = launch_spec.build_command(
            "zsoft", {"task_id": "civetweb-002-websocket-deflate-oob"}
        )
        self.assertEqual(command[: len(launch_spec.ENTRYPOINT)], launch_spec.ENTRYPOINT)
        self.assertIn("--mode", command)
        self.assertIn("zsoft", command)
        self.assertIn("--task-id", command)
        self.assertIn("civetweb-002-websocket-deflate-oob", command)

    def test_sweverified_command_injects_instance_and_split(self) -> None:
        command = launch_spec.build_command(
            "sweverified",
            {"instance_id": "astropy__astropy-12907", "split": "train"},
        )
        self.assertIn("--mode", command)
        self.assertIn("sweverified", command)
        self.assertIn("--instance-id", command)
        self.assertIn("astropy__astropy-12907", command)
        self.assertIn("--split", command)
        self.assertIn("train", command)

    def test_unknown_mode_raises(self) -> None:
        with self.assertRaises(launch_spec.LaunchSpecError):
            launch_spec.build_command("no-such-mode", {"task_id": "x"})

    def test_zsoft_requires_task_id(self) -> None:
        with self.assertRaises(launch_spec.LaunchSpecError):
            launch_spec.build_command("zsoft", {})

    def test_sweverified_requires_instance_id(self) -> None:
        with self.assertRaises(launch_spec.LaunchSpecError):
            launch_spec.build_command("sweverified", {})


class RequiredEnvTest(unittest.TestCase):
    def test_required_env_is_allow_list_only(self) -> None:
        env = launch_spec.required_env()
        self.assertEqual(
            set(env),
            {"ZSOFT_TASKS_DIR", "PYTHONPATH", "XIAOO_MCTS_MAX_CONCURRENT"},
        )

    def test_command_does_not_contain_llm_credentials(self) -> None:
        command = launch_spec.build_command(
            "zsoft", {"task_id": "civetweb-002-websocket-deflate-oob"}
        )
        self.assertFalse(
            any(
                part.startswith("XGOVERNOR_LLM_")
                or part.startswith("--llm")
                for part in command
            )
        )


class ManifestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest = launch_spec.load_manifest()

    def test_manifest_has_builtin_modes(self) -> None:
        self.assertEqual(set(self.manifest["modes"]), {"deepswe", "generic", "zsoft", "sweverified"})

    def test_manifest_profiles_are_complete(self) -> None:
        expected = {
            "author-init",
            "author-step",
            "solver-step",
            "submission-selector",
        }
        for mode in self.manifest["modes"]:
            self.assertTrue(expected.issubset(self.manifest["modes"][mode]["profiles"]))
        self.assertIn(
            "trial-assessor", self.manifest["modes"]["zsoft"]["profiles"]
        )

    def test_all_profile_sources_exist_on_disk(self) -> None:
        for mode, mode_config in self.manifest["modes"].items():
            for profile in mode_config["profiles"]:
                path = launch_spec.resolve_profile_source(mode, profile)
                self.assertTrue(path.is_file(), f"missing template {path}")

    def test_manifest_rejects_absolute_source(self) -> None:
        payload = copy.deepcopy(self.manifest)
        payload["modes"]["zsoft"]["profiles"]["solver-step"]["source"] = "/etc/passwd"
        with self.assertRaises(launch_spec.LaunchSpecError):
            launch_spec.validate_manifest(payload)

    def test_manifest_rejects_parent_traversal(self) -> None:
        payload = copy.deepcopy(self.manifest)
        payload["modes"]["zsoft"]["profiles"]["solver-step"]["source"] = (
            "../secret/prompts/prompt_solver_step.txt"
        )
        with self.assertRaises(launch_spec.LaunchSpecError):
            launch_spec.validate_manifest(payload)

    def test_manifest_rejects_path_outside_task_plugins(self) -> None:
        payload = copy.deepcopy(self.manifest)
        payload["modes"]["zsoft"]["profiles"]["solver-step"]["source"] = (
            "engine/prompts/prompt_solver_step.txt"
        )
        with self.assertRaises(launch_spec.LaunchSpecError):
            launch_spec.validate_manifest(payload)

    def test_manifest_requires_xgovernor_owner(self) -> None:
        payload = copy.deepcopy(self.manifest)
        payload["modes"]["zsoft"]["profiles"]["solver-step"]["owner"] = "plugin"
        with self.assertRaises(launch_spec.LaunchSpecError):
            launch_spec.validate_manifest(payload)


if __name__ == "__main__":
    unittest.main()
