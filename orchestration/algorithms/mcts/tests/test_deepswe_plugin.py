from pathlib import Path
import tempfile
import unittest

from xgovernor_opencode_host.opencode_adapter import FakeSessionApi, OpenCodeError, load_agent_profiles
from xgovernor_opencode_host.run_opencode_mcts import build_parser
from xgovernor_tasks.common.mode import MODES
from xgovernor_mcts.tests.test_opencode_agent_turn import _WaitHost, _git, _init_repo


class DeepSWEPluginTests(unittest.TestCase):
    def test_solver_uses_raw_goal_and_benchmark_metadata(self):
        args = build_parser().parse_args(["serve-plugin", "--mode", "deepswe"])
        spec = MODES.get(args.mode)
        profiles = load_agent_profiles(spec.id, prompt_repo=".")
        generic_profiles = load_agent_profiles("generic")
        goal = "  Fix app.py.\n"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_repo(root)
            task = spec.loader()(spec, goal=goal, oracle_dir=root, task_id="case", root_state_id="state-root")
            self.assertIn("./verify.sh", task.init_author())
            sessions = FakeSessionApi()
            adapter = spec.opencode_adapter(session_api=sessions, profiles=profiles)
            waiter = _WaitHost(adapter)
            state = adapter.create_root_snapshot(repo=str(source), output_dir=str(root / "out"))
            branch = adapter.fork(state, timeout_ms=1_000, idempotency_key="fork")["branch_id"]
            session_id = adapter.branches[branch].session_id
            permissions = {rule["action"]: rule["effect"] for rule in sessions.sessions[session_id].permissions}
            self.assertEqual(permissions["bash"], "allow")
            self.assertEqual(permissions["external_directory"], "allow")
            turns = {"solver-init": task.init_solver(), "solver-step": task.step_solver({"feedback": "previous"})}
            for profile, text in turns.items():
                with self.subTest(profile=profile):
                    receipt = adapter.agent_turn(branch, text, timeout_ms=1_000, idempotency_key=profile, profile=profile)
                    waiter.wait_job(receipt)
                    prompt = [payload for method, payload in sessions.calls if method == "prompt"][-1]
                    self.assertEqual(prompt["text"], goal)
                    self.assertEqual(prompt["metadata"], {"mcts_deepswe_solver": True})
                    self.assertIsNone(generic_profiles[profile].prompt_metadata)

    def test_submodules_are_restored_and_patch_omits_oracle(self):
        spec = MODES.get("deepswe")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = _init_repo(root)
            fixture_root = root / "fixture"
            fixture_root.mkdir()
            fixture = _init_repo(fixture_root)
            _git(source, "-c", "protocol.file.allow=always", "submodule", "add", str(fixture), "vendor/fixture")
            _git(source, "commit", "-m", "add test fixture")

            adapter = spec.opencode_adapter(session_api=FakeSessionApi())
            out = root / "out"
            state = adapter.create_root_snapshot(repo=str(source), output_dir=str(out))
            branch = adapter.fork(state, timeout_ms=1_000, idempotency_key="fork")["branch_id"]
            worktree = adapter.branches[branch].worktree
            original = (fixture / "app.py").read_text()
            submodule_file = worktree / "vendor/fixture/app.py"
            self.assertEqual(submodule_file.read_text(), original)
            submodule_file.write_text("dirty fixture\n")
            with self.assertRaisesRegex(OpenCodeError, "submodule fixtures must remain unchanged"):
                adapter.snapshot(branch, timeout_ms=1_000, idempotency_key="dirty")
            submodule_file.write_text(original)

            (worktree / "app.py").write_text("print('fixed')\n")
            (worktree / "verify.sh").write_text("echo PASS\n")
            candidate = adapter.snapshot(branch, timeout_ms=1_000, idempotency_key="candidate")["state_id"]
            adapter.release(branch, timeout_ms=1_000, idempotency_key="release")
            restored = adapter.fork(candidate, timeout_ms=1_000, idempotency_key="restore")["branch_id"]
            restored_fixture = adapter.branches[restored].worktree / "vendor/fixture/app.py"
            self.assertEqual(restored_fixture.read_text(), original)
            patch = adapter.diff(restored, state)
            self.assertIn("print('fixed')", patch)
            self.assertNotIn("verify.sh", patch)
            adapter.deliver_selected(output_dir=str(out), apply_plan={"state_id": candidate})
            self.assertEqual((out / "selected.patch").read_text(), patch)
            _git(source, "apply", "--check", str(out / "selected.patch"))
