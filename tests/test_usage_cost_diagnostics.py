from datetime import datetime
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock


os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class FakeResponse:
    def __init__(self):
        self.embed = None
        self.ephemeral = None

    async def send_message(self, *, embed, ephemeral):
        self.embed = embed
        self.ephemeral = ephemeral


class FakeInteraction:
    def __init__(self):
        self.response = FakeResponse()


class UsageCostDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "usage.sqlite")
        self.original_db = bnl01_bot.DB_FILE
        bnl01_bot.DB_FILE = self.db_path
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            bnl01_bot._ensure_token_usage_schema(cursor)
            cursor.execute(
                """
                UPDATE token_usage
                SET tokens_used_today=1800, last_reset_date='2026-08-22'
                WHERE id=1
                """
            )
            cursor.executemany(
                """
                INSERT INTO token_usage_events (
                    usage_date, recorded_at, route, model,
                    prompt_tokens, candidate_tokens, thought_tokens,
                    cached_tokens, total_tokens, estimated_cost_nanos,
                    cost_priced, pricing_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, '')
                """,
                [
                    (
                        "2026-08-22",
                        "2026-08-22T18:00:00+00:00",
                        "website_relay_event",
                        "gemini-3.6-flash",
                        1000,
                        100,
                        200,
                        0,
                        1300,
                    ),
                    (
                        "2026-08-22",
                        "2026-08-22T18:01:00+00:00",
                        "legacy_unknown.failed_server",
                        "future-unpriced-model",
                        400,
                        50,
                        50,
                        0,
                        500,
                    ),
                ],
            )
            cursor.execute(
                """
                INSERT INTO model_generation_attempts (
                    usage_date, recorded_at, route, model, outcome,
                    total_tokens, attempt_number, is_retry, is_fallback,
                    estimated_cost_nanos, cost_priced, pricing_version
                ) VALUES (
                    '2026-08-22', '2026-08-22T18:01:00+00:00',
                    'legacy_unknown', 'future-unpriced-model', 'failure',
                    0, 1, 0, 0, NULL, 0, ''
                )
                """
            )

    async def asyncTearDown(self):
        bnl01_bot.DB_FILE = self.original_db
        self.tempdir.cleanup()

    async def test_usage_embed_exposes_cost_reserves_attempts_and_unpriced_model(self):
        interaction = FakeInteraction()
        now = bnl01_bot.PACIFIC_TZ.localize(
            datetime(2026, 8, 22, 17, 0)
        )
        with mock.patch.object(bnl01_bot, "_pacific_now", return_value=now):
            diagnostics = bnl01_bot.get_usage_breakdown()
            await bnl01_bot.usage.callback(interaction)

        self.assertTrue(interaction.response.ephemeral)
        fields = {
            field.name: field.value
            for field in interaction.response.embed.fields
        }
        self.assertIn("Protected Token Capacity", fields)
        self.assertIn("Daily soft floor", fields["Monthly Dollar Guardrail"])
        self.assertIn("effective hard", fields["Monthly Dollar Guardrail"])
        self.assertIn("Physical attempts", fields["Attempts and Restrictions"])
        self.assertIn("future-unpriced-model", fields["Attempts and Restrictions"])
        self.assertIn("500 tokens", fields["Attempts and Restrictions"])
        self.assertIn(
            "`legacy_unknown.failed_server` — unpriced / "
            "1 provider attempt(s)",
            fields["Top Routes This Month"],
        )
        self.assertNotIn(
            "`legacy_unknown.failed_server` — $0.0000",
            fields["Top Routes This Month"],
        )
        unknown_route = next(
            item
            for item in diagnostics["cost_routes"]
            if item["route"] == "legacy_unknown.failed_server"
        )
        self.assertEqual(unknown_route["provider_attempts"], 1)
        ordinary_cost = diagnostics["route_restrictions"]["ordinary"][
            "representative_request_cost_usd"
        ]
        expected_ordinary_nanos = bnl01_bot._estimated_request_cost_nanos(
            "usage diagnostic representative request",
            bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
        )
        self.assertEqual(
            ordinary_cost,
            bnl01_bot._nanos_to_usd(expected_ordinary_nanos),
        )
        for value in fields.values():
            self.assertLessEqual(len(value), 1024)


if __name__ == "__main__":
    unittest.main()
