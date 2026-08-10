import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from apps.home import llm


class OpenAiCompatibleLlmTests(unittest.TestCase):
    def test_query_uses_saved_openai_compatible_settings(self):
        completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content="RFC review complete."),
                    finish_reason="stop",
                )
            ]
        )
        client = MagicMock()
        client.chat.completions.create.return_value = completion
        settings = {
            "OPENAI_API_KEY": "secret",
            "LOCAL_LLM_URI": "https://provider.example/openai/v1",
            "OPENAI_API_MODEL": "qwen3",
        }

        with (
            patch.object(llm, "get_config_value", side_effect=lambda key, default=None: settings.get(key, default)),
            patch.object(llm, "get_config_bool", return_value=True),
            patch.object(llm, "check_ollama_status", return_value="not_ollama") as ollama_status,
            patch.object(llm, "OpenAI", return_value=client) as openai_client,
        ):
            result = llm.query_chatgpt("Review this table.", render_html=False)

        self.assertEqual(result, "RFC review complete.")
        openai_client.assert_called_once_with(
            api_key="secret",
            base_url="https://provider.example/openai/v1/",
            timeout=600.0,
        )
        client.chat.completions.create.assert_called_once_with(
            model="qwen3",
            messages=[
                {"role": "system", "content": "You are a Postgresql database expert"},
                {"role": "user", "content": "Review this table."},
            ],
        )
        ollama_status.assert_called_once_with(
            "https://provider.example/openai/v1",
            verify_ssl=True,
        )

    def test_missing_model_has_actionable_error(self):
        settings = {
            "OPENAI_API_KEY": "secret",
            "LOCAL_LLM_URI": "https://provider.example/openai/v1",
            "OPENAI_API_MODEL": "",
        }
        with (
            patch.object(llm, "get_config_value", side_effect=lambda key, default=None: settings.get(key, default)),
            patch.object(llm, "get_config_bool", return_value=True),
            patch.object(llm, "check_ollama_status", return_value="not_ollama"),
        ):
            with self.assertRaisesRegex(ValueError, "No LLM model is configured"):
                llm.query_chatgpt("Review this table.", render_html=False)

    def test_ssl_verification_can_be_explicitly_disabled(self):
        completion = SimpleNamespace(
            choices=[SimpleNamespace(
                message=SimpleNamespace(content="OK"),
                finish_reason="stop",
            )]
        )
        client = MagicMock()
        client.chat.completions.create.return_value = completion
        settings = {
            "OPENAI_API_KEY": "secret",
            "LOCAL_LLM_URI": "https://internal.example/v1",
            "OPENAI_API_MODEL": "local-model",
        }
        insecure_http_client = object()

        with (
            patch.object(llm, "get_config_value", side_effect=lambda key, default=None: settings.get(key, default)),
            patch.object(llm, "get_config_bool", return_value=False),
            patch.object(llm, "check_ollama_status", return_value="not_ollama"),
            patch.object(llm, "DefaultHttpxClient", return_value=insecure_http_client) as http_client,
            patch.object(llm, "OpenAI", return_value=client) as openai_client,
        ):
            self.assertEqual(
                llm.query_chatgpt("Review.", render_html=False),
                "OK",
            )

        http_client.assert_called_once_with(verify=False)
        openai_client.assert_called_once_with(
            api_key="secret",
            base_url="https://internal.example/v1/",
            timeout=600.0,
            http_client=insecure_http_client,
        )


if __name__ == "__main__":
    unittest.main()
