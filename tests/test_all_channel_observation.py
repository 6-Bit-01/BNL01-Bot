"""Observe every readable text surface without widening participation."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import unittest
from types import SimpleNamespace
from unittest import mock

import test_channel_audit_passive_capture as fixtures
from test_channel_audit_passive_capture import FakeAuthor, FakeChannel, FakeMessage, FakePerms
import bnl01_bot as bot
import bnl_journal as journal
from bnl_journal_source_store import purge_user_discord_sources_on_connection


class AllChannelObservationTests(unittest.TestCase):
    def setUp(self):
        fixtures.ChannelAuditPassiveCaptureTests.setUp(self)
        bot.ensure_journal_source_schema(self.tmp.name)

    tearDown = fixtures.ChannelAuditPassiveCaptureTests.tearDown
    make_guild_channel = fixtures.ChannelAuditPassiveCaptureTests.make_guild_channel

    def test_dynamic_channel_adapter_cannot_walk_parents_forever(self):
        self.assertFalse(bot.is_community_image_channel(mock.Mock()))

    def room(self, name="new-art-room", *, public=True, kind="text"):
        guild, channel = self.make_guild_channel(name, 904, channel_type=kind)
        guild.default_role = object()
        channel.permissions_for = lambda member: FakePerms(
            view=public if member is guild.default_role else True,
        )
        return guild, channel

    def events(self):
        with sqlite3.connect(self.tmp.name) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute("SELECT * FROM bnl_journal_source_events")]

    def test_new_public_room_joins_shared_memory_without_reply_permission(self):
        guild, channel = self.room()
        message = FakeMessage("I finished the chorus arrangement today.", channel)
        self.assertTrue(bot.record_additional_channel_observation(message))
        self.assertTrue(bot.record_additional_channel_observation(message))
        with sqlite3.connect(self.tmp.name) as conn:
            rows = conn.execute("SELECT channel_policy,route_mode,timestamp FROM conversations").fetchall()
        self.assertEqual(rows, [("public_selective", "channel_observation", "2026-05-31 00:00:00")])
        self.assertEqual(bot.resolve_channel_policy(channel), "unknown")
        self.assertEqual(bot.conversation_surface_for_channel_policy("unknown"), "protected_or_silent")
        self.assertEqual(len(self.events()), 1)
        self.assertEqual(json.loads(self.events()[0]["metadata_json"])["observation"]["participationPolicy"], "unknown")

    def test_concurrent_gateway_replay_does_not_duplicate_shared_memory(self):
        guild, channel = self.room()
        message = FakeMessage("I finished the next track.", channel)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(bot.record_additional_channel_observation, [message, message]))
        self.assertEqual(results, [True, True])
        with sqlite3.connect(self.tmp.name) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0], 1)

    def test_owner_observation_never_reads_or_persists_account_display_fields(self):
        class OwnerAuthor:
            id = 42
            bot = False

            @property
            def display_name(self):
                raise AssertionError("Account display fields must not be read")

            @property
            def name(self):
                raise AssertionError("Account display fields must not be read")

        with mock.patch.object(bot, "BNL_OWNER_USER_ID", 42):
            for public in (True, False):
                guild, channel = self.room(public=public)
                self.assertTrue(bot.record_additional_channel_observation(
                    FakeMessage("A new observation.", channel, author=OwnerAuthor())))
        self.assertEqual([event["private_display_name"] for event in self.events()], ["6 Bit", "6 Bit"])
        with sqlite3.connect(self.tmp.name) as conn:
            self.assertEqual(conn.execute("SELECT user_name FROM conversations").fetchall(), [("6 Bit",)])

    def test_observation_does_not_enqueue_source_refresh_but_normal_capture_still_can(self):
        guild, channel = self.room()
        with mock.patch.object(bot, "mark_subject_dirty_for_evidence") as dirty:
            self.assertTrue(bot.record_additional_channel_observation(
                FakeMessage("I released my first complete album today.", channel)))
            dirty.assert_not_called()
            bot.save_user_message(43, "Test Member", guild.id, "I released an album today.",
                                  channel_policy="public_selective")
            dirty.assert_called_once()

    def test_private_named_and_new_rooms_never_become_public_activity(self):
        for name in ("new-private-room", "research-and-development", "rules", "welcome"):
            with self.subTest(name=name):
                guild, channel = self.room(name, public=False)
                self.assertTrue(bot.record_additional_channel_observation(FakeMessage("Private planning note.", channel)))
        self.assertTrue(all(not event["public_usable"] for event in self.events()))
        with sqlite3.connect(self.tmp.name) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0], 0)
        with mock.patch.object(journal, "build_packet_from_sources", return_value={}) as build:
            journal.build_source_packet_between(self.tmp.name, 123, "2026-05-30T00:00:00Z", "2026-06-01T00:00:00Z")
        self.assertEqual(build.call_args.args[4:6], ([], []))
        self.assertEqual(build.call_args.kwargs["aggregate_counts"]["channels"], 0)

    def test_bot_output_is_observed_with_attribution_and_no_human_memory(self):
        guild, channel = self.room("ai-image-generator")
        message = FakeMessage("Generated art", channel, author=FakeAuthor(77, "Test Image Tool", bot=True))
        message.attachments = [SimpleNamespace(filename="art.png", content_type="image/png", url="https://example.invalid/art.png")]
        self.assertTrue(bot.record_additional_channel_observation(message))
        event = self.events()[0]
        self.assertFalse(event["public_usable"])
        self.assertEqual(json.loads(event["metadata_json"])["authorKind"], "bot")
        self.assertFalse(json.loads(event["metadata_json"])["mediaPixelsRead"])
        with sqlite3.connect(self.tmp.name) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0], 0)

    def test_image_tool_ingress_never_dispatches_commands_replies_or_models(self):
        guild, channel = self.room("ai-image-generator")
        for content in ("BNL draw me a picture", "!bnl audit channels", "<@999> hello", "/setup"):
            message = FakeMessage(content, channel)
            with mock.patch.object(bot, "maybe_handle_declared_canon_command", new=mock.AsyncMock()) as command, \
                 mock.patch.object(bot, "get_gemini_client") as provider:
                asyncio.run(bot.on_message(message))
                command.assert_not_called()
                provider.assert_not_called()
                self.assertEqual(message.replies, [])
                self.assertEqual(channel.sent, [])

    def test_image_tool_thread_is_silent_in_any_guild(self):
        guild, parent = self.room("ai-image-generator")
        thread = FakeChannel("a-picture", 905, guild=guild, channel_type="public_thread")
        thread.parent = parent
        with mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", 456):
            self.assertEqual(bot.resolve_channel_policy(thread), "ai_image_tool")
        self.assertFalse(asyncio.run(bot.tree.interaction_check(SimpleNamespace(channel=thread))))

    def test_private_thread_does_not_inherit_public_reuse(self):
        guild, parent = self.room()
        thread = FakeChannel("private-thread", 905, guild=guild, channel_type="private_thread")
        thread.parent = parent
        message = FakeMessage("This is a private thread.", thread)
        self.assertTrue(bot.record_additional_channel_observation(message))
        self.assertFalse(self.events()[0]["public_usable"])

    def test_commands_missing_access_and_own_messages_are_not_archived(self):
        guild, channel = self.room("ai-image-generator")
        for content in ("!bnl journal test", "   !BNL canon declare secret", "/imagine secret"):
            self.assertFalse(bot.record_additional_channel_observation(FakeMessage(content, channel)))
        channel.permissions_for = lambda _: FakePerms(view=False)
        self.assertFalse(bot.record_additional_channel_observation(FakeMessage("unavailable", channel)))
        self.assertEqual(self.events(), [])

    def test_voice_text_observed_and_active_threads_appear_in_audit(self):
        guild, channel = self.room("voice-chat", kind="voice")
        self.assertTrue(bot.record_additional_channel_observation(FakeMessage("Voice text chat only.", channel)))
        thread = FakeChannel("thread", 906, guild=guild, channel_type="public_thread")
        guild.threads = [thread]
        rows = bot.build_channel_audit_rows(guild)
        self.assertEqual({row["channel_id"] for row in rows}, {904, 906})
        self.assertTrue(all(row["observation_expected"] for row in rows))

    def test_forgetting_removes_private_observations_too(self):
        guild, channel = self.room(public=False)
        bot.record_additional_channel_observation(FakeMessage("Private note.", channel))
        with sqlite3.connect(self.tmp.name) as conn:
            self.assertEqual(purge_user_discord_sources_on_connection(conn, 123, 42), 1)
        self.assertEqual(self.events(), [])
