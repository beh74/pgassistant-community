import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from apps.home.config import get_config_bool, init_or_load_env, update_llm_config

class LlmConfigInitializationTests(unittest.TestCase):
    def test_ssl_verification_defaults_to_enabled_and_can_be_disabled(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            with patch("apps.home.config.CONFIG_PATH", str(config_path)):
                self.assertTrue(get_config_bool("LLM_VERIFY_SSL", True))
                update_llm_config(
                    llm_verify_ssl=False,
                    config_path=str(config_path),
                )
                self.assertFalse(get_config_bool("LLM_VERIFY_SSL", True))

    def test_creates_first_start_config_from_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            with patch.dict(
                os.environ,
                {
                    "LOCAL_LLM_URI": "http://ollama:11434/v1/",
                    "OPENAI_API_MODEL": "local-model",
                },
                clear=True,
            ):
                init_or_load_env(
                    config_path=config_path,
                    keys=["LOCAL_LLM_URI", "OPENAI_API_MODEL"],
                )

            saved = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["LOCAL_LLM_URI"], "http://ollama:11434/v1/")
            self.assertEqual(saved["OPENAI_API_MODEL"], "local-model")

    def test_repairs_empty_existing_config_from_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            config_path.write_text(
                json.dumps({"OPENAI_API_KEY": "", "OPENAI_API_MODEL": ""}),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"OPENAI_API_KEY": "secret", "OPENAI_API_MODEL": "gpt-model"},
                clear=True,
            ):
                init_or_load_env(
                    config_path=config_path,
                    keys=["OPENAI_API_KEY", "OPENAI_API_MODEL"],
                )

            saved = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["OPENAI_API_KEY"], "secret")
            self.assertEqual(saved["OPENAI_API_MODEL"], "gpt-model")

    def test_saved_settings_remain_authoritative(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            config_path.write_text(
                json.dumps({"OPENAI_API_MODEL": "saved-model"}),
                encoding="utf-8",
            )
            with patch.dict(
                os.environ,
                {"OPENAI_API_MODEL": "environment-model"},
                clear=True,
            ):
                init_or_load_env(
                    config_path=config_path,
                    keys=["OPENAI_API_MODEL"],
                )
                self.assertEqual(os.environ["OPENAI_API_MODEL"], "saved-model")

            saved = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["OPENAI_API_MODEL"], "saved-model")


if __name__ == "__main__":
    unittest.main()
