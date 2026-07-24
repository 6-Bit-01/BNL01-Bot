import os
import unittest
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock


os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot


def diagnostic_data():
    return (
        {
            "enabled": False,
            "url_configured": False,
            "cache_present": False,
            "cache_age_seconds": None,
            "last_section_counts": {},
            "canon_source_contract": {},
            "rd_classifier_enabled": False,
        },
        None,
        None,
        0,
        0,
        True,
        {},
        {},
        0,
        {},
        {
            "configured_limits": {},
            "effective_limits": {},
            "conversation_rows_retained": 0,
            "last_consolidation_result": {},
            "prompt_diagnostics": {},
            "recent_skip_reasons": {},
        },
        {
            "trust_counts": {},
            "policy_counts": {},
            "legacy_unknown_count": 0,
            "source_safe_count": 0,
            "consolidated_count": 0,
        },
        True,
        "source-gated",
        False,
        0,
        "",
        "",
        None,
        "",
        0,
        0,
        None,
        {
            "schemaVersion": "memory_ledger_v1",
            "insertedLedgerEntries": 0,
        },
    )


def interaction():
    return SimpleNamespace(
        user=SimpleNamespace(id=999),
        guild=SimpleNamespace(id=123, name="BARCODE Network"),
        channel=None,
        response=SimpleNamespace(
            defer=AsyncMock(),
            send_message=AsyncMock(),
            is_done=mock.Mock(return_value=True),
        ),
        followup=SimpleNamespace(send=AsyncMock()),
    )


class MemoryDiagnosticCommandTests(unittest.IsolatedAsyncioTestCase):
    async def test_owner_command_defers_before_offloaded_collection_and_send(self):
        item = interaction()
        events = []

        async def defer(**_kwargs):
            events.append("defer")

        async def to_thread(function, **kwargs):
            events.append("collect")
            self.assertIs(
                function,
                bnl01_bot._collect_bnl_memory_diagnostic_data,
            )
            self.assertEqual(
                kwargs,
                {
                    "guild_id": 123,
                    "user_id": 999,
                    "policy": "sealed_test",
                },
            )
            return diagnostic_data()

        async def send(_interaction, text, limit=0):
            events.append("send")
            self.assertIn("BNL Memory Diagnostic", text)
            self.assertEqual(limit, 1700)

        item.response.defer.side_effect = defer
        with (
            mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999),
            mock.patch.object(
                bnl01_bot,
                "is_owner_operator",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "resolve_channel_policy",
                return_value="sealed_test",
            ),
            mock.patch.object(
                bnl01_bot.asyncio,
                "to_thread",
                side_effect=to_thread,
            ),
            mock.patch.object(
                bnl01_bot,
                "send_safe_ephemeral_chunks",
                side_effect=send,
            ),
        ):
            await bnl01_bot.bnl_memory_check.callback(item)

        self.assertEqual(events, ["defer", "collect", "send"])
        item.response.defer.assert_awaited_once_with(ephemeral=True)
        item.response.send_message.assert_not_awaited()

    async def test_collection_failure_is_visible_after_immediate_defer(self):
        item = interaction()
        events = []

        async def defer(**_kwargs):
            events.append("defer")

        async def fail_collection(*_args, **_kwargs):
            events.append("collect")
            raise RuntimeError("database unavailable")

        async def send(_interaction, text, limit=0):
            events.append("send_error")
            self.assertIn("could not be built", text)
            self.assertIn("No memory or gate changes were made", text)
            self.assertEqual(limit, 1700)

        item.response.defer.side_effect = defer
        with (
            mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 999),
            mock.patch.object(
                bnl01_bot,
                "is_owner_operator",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "resolve_channel_policy",
                return_value="sealed_test",
            ),
            mock.patch.object(
                bnl01_bot.asyncio,
                "to_thread",
                side_effect=fail_collection,
            ),
            mock.patch.object(
                bnl01_bot,
                "send_safe_ephemeral_chunks",
                side_effect=send,
            ),
        ):
            await bnl01_bot.bnl_memory_check.callback(item)

        self.assertEqual(events, ["defer", "collect", "send_error"])
        item.response.defer.assert_awaited_once_with(ephemeral=True)


if __name__ == "__main__":
    unittest.main()
