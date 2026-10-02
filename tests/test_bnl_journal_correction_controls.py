"""Historical correction stays owner-controlled, private and source-bound."""
import os
import json
import unittest
from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl01_bot as bot


def control_snapshot():
    now = datetime.now(timezone.utc)
    return journal.JournalControlSnapshot(
        snapshot_version=1, revision="2026-09-30T01:00:00Z", digest="a" * 64,
        observed_at=now.isoformat(), fresh_until=(now + timedelta(minutes=5)).isoformat(),
        fresh_for_seconds=300, public_excluded_entry_ids=("test-hidden",),
        memory_excluded_entry_ids=("test-memory-excluded",),
    )


class JournalCorrectionControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.message = SimpleNamespace(
            author=SimpleNamespace(id=10, bot=False), reply=AsyncMock(),
            guild=SimpleNamespace(id=1, get_member=lambda _id: None),
            channel=SimpleNamespace(name="bnl-testing", send=AsyncMock()),
        )
        self.command = "!bnl journal correct journal-test | revision=1 | hash=" + "b" * 64 + " | note=Clarifies the recipient and timing."
        self.snapshot = control_snapshot()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(bot, "BNL_OWNER_USER_ID", 10))
        self.stack.enter_context(patch.object(bot, "resolve_channel_policy", return_value="sealed_test"))
        self.fetch = self.stack.enter_context(patch.object(
            bot, "_journal_publication_control_snapshot_sync", return_value=(self.snapshot, "valid")))
        self.generator = Mock()
        self.stack.enter_context(patch.object(bot, "_bounded_journal_preparation_generator", return_value=self.generator))
        self.draft = self.stack.enter_context(patch.object(bot, "generate_journal_correction_draft", return_value=
            journal.JournalResult(True, "draft", "", "journal-test", 2, "c" * 64)))
        for name in ("approve_journal_draft", "deliver_approved_journal", "release_prepared_journal_entry",
                     "run_journal_automation_once", "_journal_control_flags_for_guild"):
            self.stack.enter_context(patch.object(bot, name, side_effect=AssertionError(name)))

    async def handle(self, command=None):
        return await bot.maybe_handle_journal_command(self.message, command or self.command)

    async def test_owner_creates_only_bound_draft_with_current_source_controls(self):
        self.assertTrue(await self.handle())
        self.draft.assert_called_once()
        args, kwargs = self.draft.call_args
        self.assertEqual((bot.DB_FILE, 1, "journal-test", self.generator), args)
        self.assertEqual(1, kwargs["previous_revision"])
        self.assertEqual("b" * 64, kwargs["previous_content_hash"])
        self.assertEqual({"test-hidden", "test-memory-excluded"}, kwargs["excluded_history_entry_ids"])
        self.assertIs(kwargs["original_source_controls"], bot._public_conversation_recall_controls)
        self.assertEqual(self.snapshot.authority_identity, kwargs["control_authority_identity"])
        self.assertEqual("", kwargs["generation_guard"]())
        self.assertNotIn("hours", kwargs)
        self.assertIn("published entry is unchanged", self.message.reply.await_args.args[0])
        self.message.channel.send.assert_not_called()

    async def test_non_owner_operator_cannot_start_correction(self):
        self.message.author.id = 11
        with patch.object(bot, "can_send_dossier_recommendation", return_value=True):
            self.assertTrue(await self.handle())
        self.fetch.assert_not_called()
        self.draft.assert_not_called()

    async def test_public_channel_cannot_start_correction(self):
        self.message.channel.name = "general"
        with patch.object(bot, "resolve_channel_policy", return_value="public_home"), \
             patch.object(bot, "is_operator_authority_context", return_value=False):
            self.assertTrue(await self.handle())
        self.fetch.assert_not_called()
        self.draft.assert_not_called()

    async def test_missing_base_identity_and_current_window_overrides_stop_before_generation(self):
        for command in (
            "!bnl journal correct journal-test",
            self.command.replace("revision=1", "revision=0"),
            self.command.replace("b" * 64, "invalid"),
            self.command + " | hours=24",
            self.command + " | window=72",
        ):
            with self.subTest(command=command):
                self.assertTrue(await self.handle(command))
        self.fetch.assert_not_called()
        self.draft.assert_not_called()

    async def test_control_outage_and_hidden_target_do_not_generate(self):
        for snapshot in (None, replace(self.snapshot, public_excluded_entry_ids=("journal-test",))):
            with self.subTest(snapshot=snapshot):
                self.fetch.return_value = (snapshot, "unavailable")
                self.assertTrue(await self.handle())
        self.draft.assert_not_called()

    async def test_failed_correction_reports_hold_without_delivery(self):
        self.draft.return_value = journal.JournalResult(False, "no_draft", "correction_base_changed")
        self.assertTrue(await self.handle())
        self.assertIn("correction_base_changed", self.message.reply.await_args.args[0])

    async def test_approval_passes_saved_proof_revalidation(self):
        with patch.object(bot, "approve_journal_draft", return_value=journal.JournalResult(True, "approved")) as approve:
            self.assertTrue(await self.handle("!bnl journal approve journal-test | hash=" + "c" * 64))
        self.assertIs(approve.call_args.kwargs["correction_guard"], bot._journal_saved_correction_control_guard)
        self.assertIs(approve.call_args.kwargs["original_source_controls"], bot._public_conversation_recall_controls)
        self.draft.assert_not_called()

    async def test_manual_retry_passes_saved_proof_after_scheduler_defers(self):
        with patch.object(bot, "BNL_API_KEY", "test-key"), \
             patch.object(bot, "_journal_website_base_url", return_value="https://example.invalid"), \
             patch.object(bot, "get_bnl_control_flags", return_value={}), \
             patch.object(bot, "_journal_control_request_sync", return_value=({}, "valid")), \
             patch.object(bot, "_journal_control_flags_for_guild", return_value={}), \
             patch.object(bot, "release_prepared_journal_entry", return_value=None) as scheduled, \
             patch.object(bot, "deliver_approved_journal", return_value=journal.JournalResult(True, "published")) as deliver:
            self.assertTrue(await self.handle("!bnl journal retry journal-test"))
        scheduled.assert_called_once()
        self.assertIs(deliver.call_args.kwargs["correction_guard"], bot._journal_saved_correction_control_guard)
        self.assertIs(deliver.call_args.kwargs["original_source_controls"], bot._public_conversation_recall_controls)
        self.draft.assert_not_called()

    def test_saved_identity_survives_json_and_detects_later_change(self):
        context = json.loads(json.dumps({"entryId": "journal-test", "controlAuthorityIdentity": self.snapshot.authority_identity}))
        self.assertEqual("", bot._journal_saved_correction_control_guard(context))
        self.fetch.return_value = (replace(self.snapshot, revision="2026-09-30T03:00:00Z"), "valid")
        self.assertEqual("correction_controls_changed", bot._journal_saved_correction_control_guard(context))
        self.assertEqual("correction_controls_unavailable", bot._journal_saved_correction_control_guard({}))

    def test_control_guard_revalidates_current_authority_and_freshness(self):
        guard = bot._journal_correction_control_guard("journal-test", self.snapshot)
        cases = (
            (self.snapshot, ""),
            (replace(self.snapshot, observed_at=datetime.now(timezone.utc).isoformat()), ""),
            (replace(self.snapshot, revision="2026-09-30T02:00:00Z"), "correction_controls_changed"),
            (replace(self.snapshot, memory_excluded_entry_ids=("new-exclusion",)), "correction_controls_changed"),
            (replace(self.snapshot, public_excluded_entry_ids=("journal-test",)), "correction_publication_hidden"),
            (replace(self.snapshot, fresh_until="2000-01-01T00:00:00Z"), "correction_controls_unavailable"),
            (None, "correction_controls_unavailable"),
        )
        for snapshot, expected in cases:
            with self.subTest(expected=expected):
                self.fetch.return_value = (snapshot, "valid")
                self.assertEqual(expected, guard())


if __name__ == "__main__":
    unittest.main()
