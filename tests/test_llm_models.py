import unittest

from apps.home.route_api import _extract_openai_model_ids, _extract_upstream_error


class _Response:
    status_code = 403
    text = ""

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class LlmModelListTests(unittest.TestCase):
    def test_extracts_standard_openai_model_list(self):
        self.assertEqual(
            _extract_openai_model_ids({"object": "list", "data": [{"id": "qwen3"}]}),
            ["qwen3"],
        )

    def test_accepts_infomaniak_single_model_object(self):
        self.assertEqual(
            _extract_openai_model_ids({"object": "list", "data": {"id": "qwen3"}}),
            ["qwen3"],
        )

    def test_extracts_nested_provider_error(self):
        response = _Response({
            "error": {
                "message": "Your API key lacks the required permissions."
            }
        })

        self.assertEqual(
            _extract_upstream_error(response),
            "Your API key lacks the required permissions.",
        )


if __name__ == "__main__":
    unittest.main()
