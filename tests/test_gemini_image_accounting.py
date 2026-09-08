"""Current-message image bytes stay paired and inside existing budget owners."""

from contextlib import ExitStack
from dataclasses import FrozenInstanceError, replace
from datetime import datetime
from decimal import Decimal
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from zoneinfo import ZoneInfo

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
from bnl_gemini_routing import (
    GeminiImagePart,
    GeminiImageRequest,
    estimate_gemini_prompt_tokens,
    estimated_generation_reservation,
    policy_for_route,
    single_attempt_reservation,
)


def image_request(data=b"transient-image-payload"):
    return GeminiImageRequest(
        text="Compare these current attachments.",
        images=(
            GeminiImagePart(
                data=data,
                mime_type="image/png",
                source_label="Current attachment: Test Member A; message 101.",
                estimated_tokens=4096,
            ),
            GeminiImagePart(
                data=b"second-transient-image-payload",
                mime_type="image/jpeg",
                source_label="Current attachment: Test Member B; message 102.",
                estimated_tokens=8192,
            ),
        ),
    )


def provider_response(*, usage=True):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=SimpleNamespace(
            parts=[SimpleNamespace(text="Attachment comparison.")],
        ))],
        usage_metadata=(SimpleNamespace(
            total_token_count=10_010,
            prompt_token_count=10_000,
            candidates_token_count=10,
        ) if usage else None),
    )


class GeminiImageAccountingTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.tempdir = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.db_path = str(Path(self.tempdir) / "usage.sqlite")
        self.stack.enter_context(mock.patch.object(bot, "DB_FILE", self.db_path))
        self.stack.enter_context(mock.patch.object(
            bot, "_pacific_now",
            return_value=datetime(2026, 8, 31, 12, tzinfo=ZoneInfo(
                "America/Los_Angeles",
            )),
        ))
        self.stack.enter_context(mock.patch.dict(os.environ, {
            "BNL_GEMINI_BUDGET_ENFORCEMENT_ENABLED": "true",
            "BNL_GEMINI_MONTHLY_TARGET_USD": "100",
            "BNL_GEMINI_MONTHLY_HARD_LIMIT_USD": "100",
            "BNL_GEMINI_DAILY_SOFT_LIMIT_USD": "100",
            "BNL_GEMINI_BILLING_LAG_BUFFER_USD": "0",
            "BNL_GEMINI_JOURNAL_RESERVE_USD": "0",
            "BNL_GEMINI_INTERACTIVE_RESERVE_USD": "0",
            "BNL_GEMINI_JOURNAL_PROTECTED_TOKENS": "0",
            "BNL_GEMINI_RELAY_PROTECTED_TOKENS": "0",
            "BNL_GEMINI_PROVIDER_RETRIES": "1",
        }, clear=False))
        self.stack.enter_context(mock.patch.object(bot, "DAILY_TOKEN_LIMIT", 1_000_000))
        self.original_reserved = bot._token_budget_reserved_tokens
        self.original_lanes = dict(bot._token_budget_reserved_by_lane)
        with bot._token_budget_reservation_lock:
            bot._token_budget_reserved_tokens = 0
            bot._token_budget_reserved_by_lane.clear()
        self.addCleanup(self.restore_reservations)

    def restore_reservations(self):
        with bot._token_budget_reservation_lock:
            bot._token_budget_reserved_tokens = self.original_reserved
            bot._token_budget_reserved_by_lane.clear()
            bot._token_budget_reserved_by_lane.update(self.original_lanes)

    def test_payload_is_immutable_and_absent_from_text_and_diagnostics(self):
        request = image_request()
        self.assertEqual(str(request), request.text)
        self.assertNotIn("transient-image-payload", repr(request))
        self.assertNotIn("transient-image-payload", str(request))
        with self.assertRaises(FrozenInstanceError):
            request.text = "modified"
        with self.assertRaises(FrozenInstanceError):
            request.images[0].data = b"modified"
        with self.assertRaises(ValueError):
            replace(request.images[0], data=bytearray(b"mutable"))
        with self.assertRaises(ValueError):
            replace(request.images[0], estimated_tokens=0)
        with self.assertRaises(ValueError):
            replace(request, images=list(request.images))

    def test_text_estimates_stay_unchanged_and_image_byte_size_is_not_tokens(self):
        for text in ("", "a", "abcd", "Test \u0130\U0001f680"):
            with self.subTest(text=text):
                self.assertEqual(
                    estimate_gemini_prompt_tokens(text),
                    max(1, (len(text) + 2) // 3),
                )
                self.assertEqual(
                    estimate_gemini_prompt_tokens(text, conservative_utf8=True),
                    max(1, len(text.encode("utf-8"))),
                )
        request = image_request()
        expanded = image_request(b"\x00" * 1_000_000)
        for conservative in (False, True):
            with self.subTest(conservative=conservative):
                expected = estimate_gemini_prompt_tokens(
                    request.text, conservative_utf8=conservative,
                ) + sum(
                    estimate_gemini_prompt_tokens(
                        image.source_label, conservative_utf8=conservative,
                    ) + image.estimated_tokens
                    for image in request.images
                )
                self.assertEqual(estimate_gemini_prompt_tokens(
                    request, conservative_utf8=conservative,
                ), expected)
                self.assertEqual(estimate_gemini_prompt_tokens(
                    expanded, conservative_utf8=conservative,
                ), expected)

    def test_native_parts_keep_each_speaker_label_with_its_bytes_on_all_attempts(self):
        request = image_request()
        response = provider_response()
        generate = mock.Mock(side_effect=[
            RuntimeError("503 service unavailable"),
            RuntimeError("503 service unavailable"),
            response,
        ])
        client = SimpleNamespace(models=SimpleNamespace(generate_content=generate))
        with (
            mock.patch.object(bot, "get_gemini_client", return_value=client),
            mock.patch.object(bot.time, "sleep"),
        ):
            routed = bot._generate_gemini_content_with_fallback(
                request, "normal_chat",
            )
        self.assertTrue(routed.fallback_used)
        self.assertEqual(generate.call_count, 3)
        contents = [call.kwargs["contents"] for call in generate.call_args_list]
        self.assertIs(contents[0], contents[1])
        self.assertEqual(contents[0], contents[2])
        for parts in contents:
            self.assertEqual(parts[0], request.text)
            for index, image in enumerate(request.images):
                self.assertEqual(parts[1 + index * 2].text, image.source_label)
                self.assertEqual(parts[2 + index * 2].inline_data.data, image.data)
                self.assertEqual(parts[2 + index * 2].inline_data.mime_type, image.mime_type)
        self.assertEqual(
            generate.call_args_list[0].kwargs["model"],
            "models/" + bot.GEMINI_MODEL,
        )
        self.assertEqual(
            generate.call_args_list[2].kwargs["model"],
            "models/" + bot.GEMINI_FALLBACK_MODEL,
        )
        self.assertEqual(bot._token_budget_reserved_tokens, 0)

    def test_string_provider_contents_remain_the_original_string(self):
        text = "An ordinary text-only turn."
        generate = mock.Mock(return_value=provider_response())
        client = SimpleNamespace(models=SimpleNamespace(generate_content=generate))
        with mock.patch.object(bot, "get_gemini_client", return_value=client):
            bot._generate_gemini_content_with_fallback(text, "normal_chat")
        self.assertIs(generate.call_args.kwargs["contents"], text)

    def test_image_provider_errors_retain_category_without_payload_in_logs(self):
        request = image_request()
        leaked_detail = "payload-secret https://example.invalid/private-image"
        generate = mock.Mock(side_effect=RuntimeError(
            "400 invalid argument: " + leaked_detail,
        ))
        client = SimpleNamespace(models=SimpleNamespace(generate_content=generate))
        with (
            mock.patch.object(bot, "get_gemini_client", return_value=client),
            self.assertLogs(level="INFO") as captured,
        ):
            with self.assertRaises(RuntimeError) as raised:
                bot._generate_gemini_content_with_fallback(request, "normal_chat")
        self.assertNotIn(leaked_detail, str(raised.exception))
        self.assertNotIn(leaked_detail, "\n".join(captured.output))
        self.assertEqual(bot.classify_generation_error(raised.exception)[:2], (
            bot.GENERATION_ERROR_PROVIDER_INVALID_REQUEST, "400",
        ))
        self.assertTrue(raised.exception.__suppress_context__)
        self.assertEqual(generate.call_count, 1)

    def test_native_part_conversion_error_does_not_disclose_payload(self):
        request = image_request()
        client = SimpleNamespace(models=SimpleNamespace(generate_content=mock.Mock()))
        with (
            mock.patch.object(bot, "get_gemini_client", return_value=client),
            mock.patch.object(bot.genai.types.Part, "from_bytes", side_effect=ValueError(
                "conversion failed: transient-image-payload",
            )),
        ):
            with self.assertRaises(RuntimeError) as raised:
                bot._generate_gemini_content_with_fallback(request, "normal_chat")
        self.assertNotIn("transient-image-payload", str(raised.exception))
        client.models.generate_content.assert_not_called()

    def test_real_daily_and_monthly_reservations_include_image_bounds(self):
        request = image_request()
        policy = policy_for_route("normal_chat")
        expected_tokens = estimated_generation_reservation(request, policy)
        text_tokens = estimated_generation_reservation(request.text, policy)
        expected_cost = bot._estimated_request_cost_nanos(request, "normal_chat")
        text_cost = bot._estimated_request_cost_nanos(request.text, "normal_chat")
        self.assertGreater(expected_tokens, text_tokens)
        self.assertGreater(expected_cost, text_cost)
        self.assertEqual(expected_cost, bot._estimated_request_cost_nanos(
            image_request(b"\x00" * 1_000_000), "normal_chat",
        ))
        reservation = bot.reserve_local_model_budget(request, "normal_chat")
        try:
            self.assertEqual(int(reservation), expected_tokens)
            self.assertEqual(bot._token_budget_reserved_tokens, expected_tokens)
            self.assertEqual(reservation.estimated_cost_nanos, expected_cost)
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT estimated_cost_nanos FROM gemini_budget_reservations "
                    "WHERE reservation_id=?",
                    (reservation.cost_reservation_id,),
                ).fetchone()
            self.assertEqual(row, (expected_cost,))
        finally:
            bot.release_local_model_budget(reservation)

    def test_image_daily_budget_exhaustion_prevents_any_provider_access(self):
        request = image_request()
        route = "ordinary_chat_single_packet_canary"
        limit = estimated_generation_reservation(request, policy_for_route(route)) - 1
        with mock.patch.object(bot, "DAILY_TOKEN_LIMIT", limit):
            text_reservation = bot.reserve_local_model_budget(request.text, route)
            bot.release_local_model_budget(text_reservation)
            with mock.patch.object(bot, "get_gemini_client") as client:
                with self.assertRaises(bot.LocalModelBudgetExhausted):
                    bot._generate_gemini_content_with_fallback(request, route)
                client.assert_not_called()

    def test_image_monthly_budget_exhaustion_prevents_any_provider_access(self):
        request = image_request()
        route = "ordinary_chat_single_packet_canary"
        text_cost = bot._estimated_request_cost_nanos(request.text, route)
        image_cost = bot._estimated_request_cost_nanos(request, route)
        limit = Decimal(text_cost + image_cost) / Decimal(2_000_000_000)
        with mock.patch.dict(os.environ, {
            "BNL_GEMINI_MONTHLY_HARD_LIMIT_USD": str(limit),
            "BNL_GEMINI_MONTHLY_TARGET_USD": str(limit),
        }, clear=False):
            text_reservation = bot.reserve_local_model_budget(request.text, route)
            bot.release_local_model_budget(text_reservation)
            with mock.patch.object(bot, "get_gemini_client") as client:
                with self.assertRaisesRegex(bot.LocalModelBudgetExhausted, "monthly_hard_limit"):
                    bot._generate_gemini_content_with_fallback(request, route)
                client.assert_not_called()

    def test_missing_usage_metadata_retains_the_image_estimate_in_usage_ledger(self):
        request = image_request()
        route = "ordinary_chat_single_packet_canary"
        expected = single_attempt_reservation(request, policy_for_route(route))
        generate = mock.Mock(return_value=provider_response(usage=False))
        client = SimpleNamespace(models=SimpleNamespace(generate_content=generate))
        with mock.patch.object(bot, "get_gemini_client", return_value=client):
            bot._generate_gemini_content_with_fallback(request, route)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT route, total_tokens FROM token_usage_events",
            ).fetchone()
        self.assertEqual(row, (route + ".usage_estimated", expected))


if __name__ == "__main__":
    unittest.main()
