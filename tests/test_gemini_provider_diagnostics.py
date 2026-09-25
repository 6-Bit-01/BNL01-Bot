import json
import os
import unittest
from types import SimpleNamespace
from unittest import mock

import httpx
from google.genai.errors import ClientError, ServerError

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
from bnl_gemini_routing import GeminiImagePart, GeminiImageRequest, policy_for_route, provider_server_diagnostics


def server_error(message="The model is overloaded. Please try again later."):
    return ServerError(503, {"error": {
        "code": 503, "status": "UNAVAILABLE", "message": message,
        "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                     "reason": "MODEL_CAPACITY_EXHAUSTED", "domain": "googleapis.com",
                     "metadata": {"private": "private-metadata-sentinel"}}],
        "request": {"contents": "private-body-sentinel"},
    }}, httpx.Response(503, headers={
        "x-request-id": "test-request-123", "retry-after": "30",
        "set-cookie": "private-cookie-sentinel",
    }))


class ProviderDiagnosticTests(unittest.TestCase):
    def test_sdk_error_keeps_provider_message_reason_and_request_id(self):
        detail = provider_server_diagnostics(server_error())
        self.assertEqual(detail["message"], "The model is overloaded. Please try again later.")
        self.assertEqual(detail["status"], "UNAVAILABLE")
        self.assertEqual(detail["x-request-id"], "test-request-123")
        self.assertEqual(detail["retry_after_seconds"], 30)
        self.assertEqual(detail["error_info"], [{"reason": "MODEL_CAPACITY_EXHAUSTED", "domain": "googleapis.com"}])
        self.assertNotIn("private-", json.dumps(detail))

    def test_credentials_urls_and_control_characters_are_scrubbed(self):
        error = server_error('Unavailable\n https://example.test/?key=url-secret '
                             'api_key="inline-secret" Authorization: Bearer bearer-secret '
                             'token=token-secret known-key\x1b')
        detail = provider_server_diagnostics(error, secrets=("known-key",))
        message = detail["message"]
        for value in ("url-secret", "inline-secret", "bearer-secret", "token-secret", "known-key", "\n", "\x1b"):
            self.assertNotIn(value, message)
        self.assertIn("Unavailable", message)

    def test_payload_like_messages_and_oversized_messages_are_omitted(self):
        for message, reason in (("Failure contents: private-prompt-sentinel", "request_payload_marker"),
                                ('{"prompt":"private-prompt-sentinel"}', "request_payload_marker"),
                                ("x" * 8193, "oversized")):
            detail = provider_server_diagnostics(server_error(message))
            self.assertEqual(detail["message_omitted"], reason)
            self.assertNotIn("message", detail)
        detail = provider_server_diagnostics(server_error("x" * 700))
        self.assertEqual(len(detail["message"]), 600)
        self.assertTrue(detail["message_truncated"])

    def test_unstructured_exception_does_not_dump_exception_text(self):
        self.assertEqual(provider_server_diagnostics(RuntimeError("private-exception-sentinel")),
                         {"details_available": False})

    def test_invalid_metadata_and_headers_are_not_copied(self):
        error = server_error()
        error.status = "UNAVAILABLE\nprivate-status-sentinel"
        error.details["error"]["details"][0]["reason"] = "private reason sentinel"
        error.response.headers["x-request-id"] = "private request sentinel"
        error.response.headers["retry-after"] = "private-retry-sentinel"
        detail = provider_server_diagnostics(error)
        for key in ("status", "error_info", "x-request-id", "retry_after_seconds"):
            self.assertNotIn(key, detail)

    def run_failed_request(self, route, error, diagnostic=None):
        generate = mock.Mock(side_effect=error)
        client = SimpleNamespace(models=SimpleNamespace(generate_content=generate))
        helper = diagnostic or provider_server_diagnostics
        with (mock.patch.object(bot, "record_failed_generation_attempt") as record,
              mock.patch.object(bot, "provider_server_diagnostics", side_effect=helper),
              self.assertLogs(level="WARNING") as logs):
            with self.assertRaises(ServerError) as raised:
                bot._generate_model_with_retry(
                    client, model_name="gemini-3.6-flash", contents="private-prompt-sentinel",
                    route=route, policy=policy_for_route(route),
                    budget_reservation_id="test-reservation-123")
        self.assertIs(raised.exception, error)
        self.assertEqual(generate.call_count, 1)
        record.assert_called_once()
        self.assertIs(record.call_args.args[0], error)
        return "\n".join(logs.output)

    def test_relay_and_journal_log_detail_once_without_changing_failure(self):
        for route in ("website_relay_event", "bnl_journal_generation"):
            with self.subTest(route=route):
                logs = self.run_failed_request(route, server_error())
                self.assertEqual(logs.count("gemini_provider_server_error"), 1)
                self.assertIn("The model is overloaded.", logs)
                self.assertIn("test-request-123", logs)
                self.assertIn("test-reservation-123", logs)
                self.assertIn("status=503", logs)
                self.assertNotIn("private-", logs)

    def test_diagnostic_failure_preserves_provider_error_and_accounting(self):
        logs = self.run_failed_request("website_relay_event", server_error(),
                                       diagnostic=mock.Mock(side_effect=ValueError("private-error-sentinel")))
        self.assertIn("gemini_provider_diagnostics_unavailable", logs)
        self.assertNotIn("private-", logs)

    def test_client_errors_do_not_enter_server_diagnostics(self):
        error = ClientError(400, {"error": {"code": 400, "status": "INVALID_ARGUMENT",
                                            "message": "private-client-message-sentinel"}})
        client = SimpleNamespace(models=SimpleNamespace(generate_content=mock.Mock(side_effect=error)))
        with (mock.patch.object(bot, "record_failed_generation_attempt"),
              mock.patch.object(bot, "provider_server_diagnostics") as detail):
            with self.assertRaises(ClientError) as raised:
                bot._generate_model_with_retry(client, model_name="gemini-3.6-flash", contents="text",
                                               route="website_relay_event", policy=policy_for_route("website_relay_event"))
        self.assertIs(raised.exception, error)
        detail.assert_not_called()
        self.assertEqual(client.models.generate_content.call_count, 1)

    def test_images_keep_the_existing_private_error_path(self):
        contents = GeminiImageRequest("private-image-prompt-sentinel", (
            GeminiImagePart(b"private-image-bytes-sentinel", "image/png", "current image", 256),))
        client = SimpleNamespace(models=SimpleNamespace(generate_content=mock.Mock(side_effect=server_error())))
        with (mock.patch.object(bot, "record_failed_generation_attempt"),
              mock.patch.object(bot, "provider_server_diagnostics") as detail):
            with self.assertRaises(RuntimeError) as raised:
                bot._generate_model_with_retry(client, model_name="gemini-3.6-flash", contents=contents,
                                               route="website_relay_event", policy=policy_for_route("website_relay_event"))
        detail.assert_not_called()
        self.assertNotIn("private-", str(raised.exception))
        self.assertEqual(client.models.generate_content.call_count, 1)


if __name__ == "__main__":
    unittest.main()
