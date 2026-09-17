import asyncio
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_source_store as sources
import bnl01_bot as bot


def article_for(packet):
    return json.dumps({
        "title": "A Chorus Takes the Long Way Home",
        "excerpt": "BNL follows a public demo discussion through its unexpected chorus turn.",
        "sections": [{
            "heading": "That second listen",
            "body": " ".join([
                "BNL watches the public music room fold a fresh rhythm into community mischief and careful BARCODE lore."
                for _ in range(27)
            ]),
            "sourceRefIds": [s["refId"] for s in packet["safeSources"]],
        }],
        "metadata": {"topicTags": ["music"], "subjectRefs": [], "continuityNotes": [],
                     "unresolvedQuestions": [], "confidenceFlags": ["grounded"], "safetyFlags": []},
    })


class JournalTestPreviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = str(Path(self.temp.name) / "test.db")
        journal.ensure_schema(self.db)
        sources.ensure_schema(self.db)
        for key, kind, subject, name in (
            ("test-1", "discord_message", "discord_user:10", "Test Composer"),
            ("test-2", "discord_message", "discord_user:20", "Test Listener"),
            ("test-relay", "website_relay", "", ""),
        ):
            sources.record_source_event(
                self.db, guild_id=1, source_kind=kind, source_key=key,
                occurred_at_ms=sources.timestamp_to_epoch_ms("2026-09-17T12:00:00Z"),
                raw_text="The demo chorus uses a new bass rhythm.",
                sanitized_summary="The demo chorus uses a new bass rhythm.",
                subject_ref=subject, private_display_name=name,
                channel_policy="public_relay" if kind == "website_relay" else "public_home",
                public_usable=True,
            )
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE bnl_journal_source_archive_state SET activated_at_ms=? WHERE guild_id=1",
                         (sources.timestamp_to_epoch_ms("2026-09-01T00:00:00Z"),))

    def preview(self, generator):
        return journal.generate_test_preview(self.db, 1, 24, generator, now="2026-09-18T00:00:00Z")

    def test_real_source_read_and_generation_never_write_or_backfill(self):
        before = hashlib.sha256(Path(self.db).read_bytes()).hexdigest()
        calls, connections = [], []
        connect = sqlite3.connect

        def read_connection(*args, **kwargs):
            self.assertIn("mode=ro", args[0])
            self.assertTrue(kwargs.get("uri"))
            conn = connect(*args, **kwargs)
            connections.append(conn)
            return conn

        def generate(packet, prompt):
            self.assertEqual("daily", packet["entryKind"])
            self.assertIn("Test Composer", prompt)
            self.assertIn("Test Listener", prompt)
            self.assertIn('"sourceKind": "relay"', prompt)
            calls.append(prompt)
            return article_for(packet)

        with patch.object(sources, "backfill_legacy_sources", side_effect=AssertionError("backfill")), \
             patch.object(sources, "ensure_schema", side_effect=AssertionError("source schema")), \
             patch.object(journal, "ensure_schema", side_effect=AssertionError("journal schema")), \
             patch.object(sqlite3, "connect", side_effect=read_connection):
            result = self.preview(generate)
        for conn in connections:
            conn.close()
        self.assertTrue(result["ok"], result)
        self.assertEqual(1, len(calls))
        self.assertEqual(before, hashlib.sha256(Path(self.db).read_bytes()).hexdigest())
        self.assertEqual({"title", "excerpt", "sections"}, set(result["article"]))
        self.assertNotIn("sourceRefIds", json.dumps(result["article"]))
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0])
            self.assertEqual(0, conn.execute("SELECT COUNT(*) FROM bnl_journal_private_metadata").fetchone()[0])

    def test_invalid_response_budget_refusal_and_advisory_never_retry(self):
        for answer, error, expected in (
            ("not-json", None, False),
            (None, RuntimeError("local_model_budget_exhausted"), False),
            (None, None, True),
        ):
            generate = Mock(side_effect=error or (lambda packet, prompt: answer or article_for(packet)))
            with patch.object(journal, "validate_article", side_effect=lambda *a, **kw: "" if kw.get("blocking_only") else "flat_report_voice"):
                result = self.preview(generate)
            self.assertEqual(expected, result["ok"], result)
            generate.assert_called_once()
            if expected:
                self.assertTrue(result["editorialAdvisory"])

    def test_incomplete_archive_does_not_spend_budget(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE bnl_journal_source_archive_state SET activated_at_ms=?",
                         (sources.timestamp_to_epoch_ms("2026-09-17T23:00:00Z"),))
        generate = Mock()
        result = self.preview(generate)
        self.assertEqual("incomplete_source_window", result["reason"])
        generate.assert_not_called()

    def test_blocking_output_is_not_returned_or_rewritten(self):
        def unsafe_article(packet, prompt):
            article = json.loads(article_for(packet))
            article["excerpt"] += " https://example.test/private-marker"
            return json.dumps(article)
        generate = Mock(side_effect=unsafe_article)
        result = self.preview(generate)
        self.assertFalse(result["ok"])
        self.assertEqual("public_leak_pattern", result["reason"])
        self.assertNotIn("article", result)
        self.assertNotIn("private-marker", json.dumps(result))
        generate.assert_called_once()

    def test_missing_database_is_not_created(self):
        missing = str(Path(self.temp.name) / "missing.db")
        generate = Mock()
        result = journal.generate_test_preview(missing, 1, 24, generate)
        self.assertEqual("preview_sources_unavailable", result["reason"])
        self.assertFalse(Path(missing).exists())
        generate.assert_not_called()


class JournalTestCommandTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        bot._journal_test_locks_by_guild.clear()
        self.message = SimpleNamespace(
            content="!bnl journal test | hours=24", reply=AsyncMock(),
            author=SimpleNamespace(id=10, send=AsyncMock(), bot=False),
            guild=SimpleNamespace(id=1, get_member=lambda user_id: None),
            channel=SimpleNamespace(name="bnl-testing", send=AsyncMock()),
        )
        self.preview = {
            "ok": True, "reason": "", "editorialVersion": journal.JOURNAL_EDITORIAL_VERSION,
            "sourceWindowStart": "start", "sourceWindowEnd": "end",
            "article": {"title": "Test Preview Title", "excerpt": "Test preview excerpt.",
                        "sections": [{"heading": "Test heading", "body": "Test preview body."}]},
        }
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.real_permission_denial = bot.journal_command_permission_denial
        self.denial = self.stack.enter_context(patch.object(bot, "journal_command_permission_denial", return_value=""))
        self.stack.enter_context(patch.object(bot, "resolve_channel_policy", return_value="sealed_test"))
        self.control = self.stack.enter_context(patch.object(bot, "_journal_publication_control_snapshot_sync", return_value=(
            SimpleNamespace(public_excluded_entry_ids=("test-hidden",), memory_excluded_entry_ids=("test-forgotten",)), "valid")))
        self.generator = self.stack.enter_context(patch.object(bot, "generate_journal_test_preview", return_value=self.preview))
        # Any accidental lifecycle/publication operation fails this test.
        for name in ("generate_and_store_journal_draft", "approve_journal_draft", "deliver_approved_journal",
                     "run_journal_automation_once", "_journal_control_flags_for_guild"):
            self.stack.enter_context(patch.object(bot, name, side_effect=AssertionError(name)))

    async def handle(self):
        return await bot.maybe_handle_journal_command(self.message, self.message.content)

    async def test_result_goes_only_to_dm_and_respects_live_exclusions(self):
        self.assertTrue(await self.handle())
        self.generator.assert_called_once()
        self.assertEqual({"test-hidden", "test-forgotten"}, self.generator.call_args.kwargs["excluded_history_entry_ids"])
        dm_text = "\n".join(c.args[0] for c in self.message.author.send.call_args_list)
        self.assertIn("Test Preview Title", dm_text)
        self.assertIn("Test preview body.", dm_text)
        self.message.channel.send.assert_not_called()
        self.assertNotIn("Test preview body.", str(self.message.reply.call_args_list))

    async def test_closed_dms_do_not_generate(self):
        self.message.author.send.side_effect = bot.discord.Forbidden(SimpleNamespace(status=403, reason="Forbidden"), "closed")
        await self.handle()
        self.generator.assert_not_called()
        self.message.channel.send.assert_not_called()

    async def test_control_outage_does_not_generate(self):
        self.control.return_value = (None, "control_snapshot_unavailable")
        await self.handle()
        self.generator.assert_not_called()

    async def test_owner_and_operator_channel_restrictions_still_apply(self):
        self.denial.return_value = "configured_owner_required"
        await self.handle()
        self.denial.return_value = ""
        with patch.object(bot, "resolve_channel_policy", return_value="public_home"):
            await self.handle()
        self.generator.assert_not_called()
        self.message.author.send.assert_not_called()

    async def test_duplicate_click_does_not_start_second_generation(self):
        bot._journal_test_locks_by_guild[1] = asyncio.Lock()
        await bot._journal_test_locks_by_guild[1].acquire()
        try:
            await self.handle()
        finally:
            bot._journal_test_locks_by_guild[1].release()
        self.generator.assert_not_called()

    async def test_ingress_returns_before_any_conversation_or_memory_capture(self):
        with patch.object(bot, "maybe_handle_declared_canon_command", new=AsyncMock(return_value=False)), \
             patch.object(bot, "_register_direct_conversation_ingress", side_effect=AssertionError("ingress")), \
             patch.object(bot, "upsert_user_profile", side_effect=AssertionError("profile")), \
             patch.object(bot, "record_recent_room_event_from_message", side_effect=AssertionError("room context")):
            await bot.on_message(self.message)
        self.generator.assert_called_once()

    def test_test_generation_is_owner_only_even_though_no_memory_is_written(self):
        with patch.object(bot, "configured_owner_control_denial_reason", return_value="configured_owner_required"):
            self.assertEqual("configured_owner_required", self.real_permission_denial(
                "test", self.message.author, None, self.message.guild))


if __name__ == "__main__":
    unittest.main()
