from pathlib import Path
import tempfile
import unittest

from install import FILES, STATE, install


class InstallationTest(unittest.TestCase):
    def test_repeat_install_uninstall_preserves_user_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = root / "opencode.jsonc"
            original = b'{ // keep comments and unrelated settings\n "model": "custom/model",\n}\n'
            config.write_bytes(original)
            other = root / "plugins/other.js"
            other.parent.mkdir()
            other.write_text("export default async () => ({})\n")
            install(root)
            installed = {name: (root / name).read_bytes() for name in FILES}
            install(root)
            self.assertEqual(installed, {n: (root / n).read_bytes() for n in FILES})
            skill = root / "skills/mcts/SKILL.md"
            skill.write_text("user customization\n")
            with self.assertRaisesRegex(ValueError, "Preserving"):
                install(root, uninstall=True)
            self.assertTrue((root / "plugins/mcts.js").exists())
            skill.write_bytes(installed["skills/mcts/SKILL.md"])
            install(root, uninstall=True)
            install(root, uninstall=True)
            self.assertEqual(config.read_bytes(), original)
            self.assertEqual(other.read_text(), "export default async () => ({})\n")
            self.assertFalse((root / STATE).exists())
            self.assertTrue(all(not (root / n).exists() for n in FILES))
            (root / "plugins/mcts.js").write_text("user plugin")
            with self.assertRaisesRegex(ValueError, "Preserving"):
                install(root)
            self.assertFalse(skill.exists())
