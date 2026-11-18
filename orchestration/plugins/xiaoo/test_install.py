from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import patch

from install import install, STATE


class InstallationTest(unittest.TestCase):
    def test_repeat_install_uninstall_preserves_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            config = home / "config"
            config.mkdir()
            path = config / 'mcp.json'
            original = '{"mcpServers": {"other": {"command": "other"}}}\n'
            path.write_text(original)
            with patch.dict(os.environ, HOME=str(home)):
                install(config)
                installed = path.read_bytes()
                install(config)
                self.assertEqual(path.read_bytes(), installed)
                skill = home / ".xiaoo/skills/mcts/SKILL.md"
                content = skill.read_bytes()
                skill.write_text("user customization")
                with self.assertRaisesRegex(ValueError, "Preserving"):
                    install(config, uninstall=True)
                self.assertEqual(path.read_bytes(), installed)
                skill.write_bytes(content)
                install(config, uninstall=True)
                install(config, uninstall=True)
                self.assertEqual(path.read_text(), original)
                self.assertFalse(skill.exists())
                self.assertFalse((config / STATE).exists())
                install(config)
                # Unrelated edits made after installation survive uninstall.
                import json
                value = json.loads(path.read_text())
                value["mcpServers"]["later"] = {"command": "later"}
                path.write_text(json.dumps(value))
                install(config, uninstall=True)
                self.assertIn("later", path.read_text())
                skill.parent.mkdir(parents=True, exist_ok=True)
                skill.write_text("preexisting skill")
                before = path.read_bytes()
                with self.assertRaisesRegex(ValueError, "Preserving"):
                    install(config)
                self.assertEqual(path.read_bytes(), before)
