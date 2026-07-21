from __future__ import annotations

import configparser
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DeploymentConsoleStreamlitConfigTests(unittest.TestCase):
    def test_unc_safe_streamlit_watcher_settings_are_checked_in(self) -> None:
        path = ROOT / ".streamlit" / "config.toml"
        parser = configparser.ConfigParser()
        self.assertEqual(parser.read(path, encoding="utf-8"), [str(path)])
        self.assertEqual(parser.get("server", "filewatchertype"), '"none"')
        self.assertFalse(parser.getboolean("server", "runonsave"))

    def test_gitignore_keeps_config_but_excludes_local_streamlit_secrets(self) -> None:
        lines = {
            line.strip()
            for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        }
        self.assertIn(".streamlit/*", lines)
        self.assertIn("!.streamlit/config.toml", lines)
        self.assertNotIn("!.streamlit/secrets.toml", lines)

    def test_documented_command_remains_unchanged(self) -> None:
        documentation = (ROOT / "docs" / "deployment_console.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("streamlit run sr_deployment_console.py", documentation)
        self.assertIn("fileWatcherType", documentation)
        self.assertIn("runOnSave", documentation)


if __name__ == "__main__":
    unittest.main()
