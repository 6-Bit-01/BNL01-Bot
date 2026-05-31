import asyncio
import os
os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl01_bot


class FakePerms:
    def __init__(self, view=True, history=True, send=False, react=True):
        self.view_channel = view
        self.read_message_history = history
        self.send_messages = send
        self.add_reactions = react


class FakeCategory:
    def __init__(self, name="General", position=0):
        self.name = name
        self.position = position


class FakeGuild:
    def __init__(self, guild_id=123, channels=None):
        self.id = guild_id
        self.name = "Guild"
        self.me = object()
        self.channels = channels or []

    def get_member(self, user_id):
        return None


class FakeChannel:
    def __init__(self, name, channel_id=1, guild=None, category=None, perms=None, channel_type="text", position=0):
        self.name = name
        self.id = channel_id
        self.guild = guild
        self.category = category
        self.parent = category
        self.type = channel_type
        self.position = position
        self._perms = perms or FakePerms()
        self.sent = []
        self._history = []

    def permissions_for(self, _member):
        return self._perms

    async def send(self, content):
        self.sent.append(content)

    def history(self, limit=100, oldest_first=False, **_kwargs):
        rows = list(self._history)[:limit]
        if not oldest_first:
            rows = list(reversed(rows))
        class _History:
            def __aiter__(self_inner):
                self_inner._iter = iter(rows)
                return self_inner
            async def __anext__(self_inner):
                try:
                    return next(self_inner._iter)
                except StopIteration:
                    raise StopAsyncIteration
        return _History()


class FakeAuthor:
    def __init__(self, user_id=42, name="HellcatNZ", bot=False):
        self.id = user_id
        self.display_name = name
        self.name = name
        self.bot = bot


class FakeMessage:
    _next_id = 1000

    def __init__(self, content, channel, guild=None, author=None, message_id=None, created_at="2026-05-31T00:00:00+00:00"):
        self.content = content
        self.clean_content = content
        self.id = message_id if message_id is not None else FakeMessage._next_id
        FakeMessage._next_id += 1
        self.created_at = created_at
        self.type = "default"
        self.channel = channel
        self.guild = guild or channel.guild
        self.author = author or FakeAuthor()
        self.mentions = []
        self.raw_mentions = []
        self.reference = None
        self.replies = []

    async def reply(self, content):
        self.replies.append(content)


class ChannelAuditPassiveCaptureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.old_db = bnl01_bot.DB_FILE
        bnl01_bot.DB_FILE = self.tmp.name
        bnl01_bot.init_db()

    def tearDown(self):
        bnl01_bot.DB_FILE = self.old_db
        try:
            os.unlink(self.tmp.name)
        except FileNotFoundError:
            pass

    def make_guild_channel(self, name, channel_id=1, perms=None, channel_type="text", category_name="General"):
        category = FakeCategory(category_name)
        guild = FakeGuild(123)
        channel = FakeChannel(name, channel_id, guild=guild, category=category, perms=perms, channel_type=channel_type)
        guild.channels = [channel]
        return guild, channel

    def test_channel_policy_intentional_classifications(self):
        for name, expected in {
            "hellcat-nz": "public_selective",
            "bnl-testing": "sealed_test",
            "off-topic": "public_context",
            "sponsors": "public_context",
            "top-viewers": "public_selective",
            "rules": "reference_canon",
        }.items():
            guild, channel = self.make_guild_channel(name)
            self.assertEqual(bnl01_bot.resolve_channel_policy(channel), expected)

    def test_unknown_policy_reported_and_visible_zero_expected_flagged(self):
        guild, hellcat = self.make_guild_channel("hellcat-nz", channel_id=991)
        unknown = FakeChannel("mystery-room", 992, guild=guild, category=FakeCategory("General"), channel_type="text")
        guild.channels = [hellcat, unknown]
        rows = bnl01_bot.build_channel_audit_rows(guild)
        by_name = {r["channel_name"]: r for r in rows}
        self.assertIn("expected_capture_but_zero_rows", by_name["hellcat-nz"]["flags"])
        self.assertIn("unknown_policy", by_name["mystery-room"]["flags"])
        summary = bnl01_bot.summarize_channel_audit_rows(rows)
        self.assertEqual(summary["expected_capture_zero_channels"], 1)
        self.assertEqual(summary["unknown_policy_channels"], 1)

    def test_protected_reference_and_voice_are_special_no_capture_not_unknown_capture_problems(self):
        guild, welcome = self.make_guild_channel("welcome", 1)
        rules = FakeChannel("rules", 2, guild=guild, category=FakeCategory("Contest"), channel_type="text")
        voice = FakeChannel("voice-chat", 3, guild=guild, category=FakeCategory("Voice"), channel_type="voice")
        guild.channels = [welcome, rules, voice]
        rows = bnl01_bot.build_channel_audit_rows(guild)
        for row in rows:
            self.assertIn("special_no_capture", row["flags"])
            self.assertNotIn("expected_capture_but_zero_rows", row["flags"])
        self.assertNotIn("unknown_policy", next(r for r in rows if r["channel_name"] == "voice-chat")["flags"])

    def test_audit_counts_legacy_blank_channel_id_by_name(self):
        conn = sqlite3.connect(self.tmp.name)
        conn.execute(
            "INSERT INTO conversations (user_id,user_name,guild_id,channel_name,channel_policy,channel_id,role,content) VALUES (?,?,?,?,?,?,?,?)",
            (42, "HellcatNZ", 123, "hellcat-nz", "public_selective", 0, "user", "legacy safe activity"),
        )
        conn.commit(); conn.close()
        guild, channel = self.make_guild_channel("hellcat-nz", 991)
        row = bnl01_bot.build_channel_audit_rows(guild)[0]
        self.assertEqual(row["captured_rows"], 1)
        self.assertNotIn("expected_capture_but_zero_rows", row["flags"])

    def test_passive_eligible_message_updates_conversation_profile_habits(self):
        guild, channel = self.make_guild_channel("hellcat-nz", 991)
        message = FakeMessage("Working on a new mix lol?", channel, guild=guild, author=FakeAuthor(42, "HellcatNZ"))
        stored = bnl01_bot.record_passive_user_activity(message, message.content, "public_selective")
        self.assertTrue(stored)
        conn = sqlite3.connect(self.tmp.name)
        conv = conn.execute("SELECT user_id,user_name,channel_name,channel_policy,channel_id,content FROM conversations").fetchone()
        profile = conn.execute("SELECT display_name,last_seen FROM user_profiles WHERE user_id=42 AND guild_id=123").fetchone()
        habits = conn.execute("SELECT total_messages,question_messages,humor_messages,last_topic FROM user_habits WHERE user_id=42 AND guild_id=123").fetchone()
        rel = conn.execute("SELECT interaction_count FROM relationship_state WHERE user_id=42 AND guild_id=123").fetchone()
        conn.close()
        self.assertEqual(conv[:5], (42, "HellcatNZ", "hellcat-nz", "public_selective", 991))
        self.assertEqual(conv[5], "Working on a new mix lol?")
        self.assertEqual(profile[0], "HellcatNZ")
        self.assertIsNotNone(profile[1])
        self.assertEqual(habits[0], 1)
        self.assertEqual(habits[1], 1)
        self.assertEqual(habits[2], 1)
        self.assertIsNotNone(rel)

    def test_passive_capture_excludes_dm_protected_and_bot_messages(self):
        guild, protected = self.make_guild_channel("welcome", 1)
        self.assertFalse(bnl01_bot.record_passive_user_activity(FakeMessage("hello", protected, guild=guild), "hello", "protected_system"))
        public_guild, public = self.make_guild_channel("hellcat-nz", 2)
        self.assertFalse(bnl01_bot.record_passive_user_activity(FakeMessage("bot text", public, guild=public_guild, author=FakeAuthor(9, "Bot", bot=True)), "bot text", "public_selective"))
        dm = FakeMessage("dm text", public, guild=None, author=FakeAuthor(8, "User"))
        dm.guild = None
        self.assertFalse(bnl01_bot.record_passive_user_activity(dm, "dm text", "public_selective"))
        conn = sqlite3.connect(self.tmp.name)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0], 0)
        conn.close()

    def test_community_presence_records_when_supported(self):
        guild, channel = self.make_guild_channel("hellcat-nz", 991)
        message = FakeMessage("HellcatNZ played with Signal Witch", channel, guild=guild, author=FakeAuthor(42, "HellcatNZ"))
        with mock.patch.dict(os.environ, {"BNL_COMMUNITY_SCOUTING_ENABLED": "true"}, clear=False):
            bnl01_bot.maybe_record_live_community_presence(message, message.content, "public_selective", False)
        conn = sqlite3.connect(self.tmp.name)
        count = conn.execute("SELECT COUNT(*) FROM community_presence").fetchone()[0]
        conn.close()
        self.assertGreaterEqual(count, 1)


    def test_backfill_parse_defaults_to_dry_run_and_mentions(self):
        matched, options, error = bnl01_bot.parse_backfill_options("!bnl backfill channel #hellcat-nz | limit=200")
        self.assertTrue(matched)
        self.assertFalse(error)
        self.assertTrue(options["dry_run"])
        self.assertEqual(options["channel"], "#hellcat-nz")
        self.assertEqual(options["limit"], 200)
        matched, options, error = bnl01_bot.parse_backfill_options("!bnl backfill approved | limit_per_channel=100 | dry_run=true")
        self.assertTrue(matched)
        self.assertEqual(options["mode"], "approved")

    def test_backfill_channel_lookup_and_policy_exclusions(self):
        guild, channel = self.make_guild_channel("hellcat-nz", 991)
        rules = FakeChannel("rules", 992, guild=guild, category=FakeCategory("Info"), channel_type="text")
        internal = FakeChannel("research-and-development", 993, guild=guild, category=FakeCategory("Ops"), channel_type="text")
        guild.channels = [channel, rules, internal]
        self.assertIs(bnl01_bot.find_guild_channel_for_backfill(guild, "#hellcat-nz"), channel)
        self.assertIs(bnl01_bot.find_guild_channel_for_backfill(guild, "<#991>"), channel)
        self.assertTrue(bnl01_bot.backfill_channel_eligible(channel)[0])
        self.assertFalse(bnl01_bot.backfill_channel_eligible(rules)[0])
        self.assertFalse(bnl01_bot.backfill_channel_eligible(internal)[0])
        self.assertEqual([c.name for c in bnl01_bot._approved_backfill_channels(guild)], ["hellcat-nz"])

    def test_backfill_dry_run_is_safe_and_real_run_dedupes_updates_state(self):
        guild, channel = self.make_guild_channel("hellcat-nz", 991)
        msg1 = FakeMessage("Working on a new mix lol?", channel, guild=guild, author=FakeAuthor(42, "HellcatNZ"), message_id=7001)
        msg2 = FakeMessage("bot text", channel, guild=guild, author=FakeAuthor(9, "Bot", bot=True), message_id=7002)
        channel._history = [msg1, msg2]
        dry = asyncio.run(bnl01_bot.run_channel_backfill(channel, limit=50, dry_run=True))
        self.assertEqual(dry["messagesScanned"], 2)
        self.assertEqual(dry["messagesEligible"], 1)
        self.assertEqual(dry["skipReasons"].get("bot_author"), 1)
        self.assertIn("conversations", dry["tablesWouldUpdate"])
        conn = sqlite3.connect(self.tmp.name)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0], 0)
        conn.close()

        with mock.patch.dict(os.environ, {"BNL_COMMUNITY_SCOUTING_ENABLED": "true"}, clear=False):
            real = asyncio.run(bnl01_bot.run_channel_backfill(channel, limit=50, dry_run=False))
        self.assertEqual(real["messagesInserted"], 1)
        conn = sqlite3.connect(self.tmp.name)
        conv = conn.execute("SELECT user_id,user_name,channel_name,channel_policy,channel_id,message_id,content FROM conversations").fetchone()
        profile = conn.execute("SELECT display_name,last_seen FROM user_profiles WHERE user_id=42 AND guild_id=123").fetchone()
        habits = conn.execute("SELECT total_messages FROM user_habits WHERE user_id=42 AND guild_id=123").fetchone()
        rel = conn.execute("SELECT interaction_count FROM relationship_state WHERE user_id=42 AND guild_id=123").fetchone()
        presence = conn.execute("SELECT COUNT(*) FROM community_presence").fetchone()[0]
        conn.close()
        self.assertEqual(conv[:6], (42, "HellcatNZ", "hellcat-nz", "public_selective", 991, 7001))
        self.assertEqual(conv[6], "Working on a new mix lol?")
        self.assertEqual(profile[0], "HellcatNZ")
        self.assertEqual(habits[0], 1)
        self.assertEqual(rel[0], 1)
        self.assertGreaterEqual(presence, 1)
        again = asyncio.run(bnl01_bot.run_channel_backfill(channel, limit=50, dry_run=False))
        self.assertEqual(again["messagesInserted"], 0)
        self.assertGreaterEqual(again["rowsAlreadyExist"], 1)
        diag = bnl01_bot.build_dossier_recommendation_diagnostics(123)
        self.assertTrue(diag["backfill_available"])
        self.assertEqual(diag["backfill_last_channel"], "hellcat-nz")

    def test_backfill_command_operator_only_and_no_raw_transcripts(self):
        guild, channel = self.make_guild_channel("hellcat-nz", 991)
        channel._history = [FakeMessage("SECRET RAW TRANSCRIPT", channel, guild=guild, author=FakeAuthor(42, "HellcatNZ"), message_id=8001)]
        message = FakeMessage("!bnl backfill channel hellcat-nz | limit=20", channel, guild=guild)
        with mock.patch.object(bnl01_bot, "can_send_dossier_recommendation", return_value=True):
            handled = asyncio.run(bnl01_bot.maybe_handle_backfill_command(message, message.content))
        self.assertTrue(handled)
        reply = message.replies[-1]
        self.assertIn("channel found", reply)
        self.assertIn("dry_run: `yes`", reply)
        self.assertNotIn("SECRET RAW TRANSCRIPT", reply)

        denied = FakeMessage("!bnl backfill channel hellcat-nz | dry_run=false", channel, guild=guild)
        with mock.patch.object(bnl01_bot, "can_send_dossier_recommendation", return_value=False):
            handled = asyncio.run(bnl01_bot.maybe_handle_backfill_command(denied, denied.content))
        self.assertTrue(handled)
        self.assertIn("operator-only", denied.replies[-1])

    def test_audit_report_is_safe_and_command_routing_operator_only(self):
        guild, channel = self.make_guild_channel("hellcat-nz", 991)
        message = FakeMessage("!bnl audit channels", channel, guild=guild)
        with mock.patch.object(bnl01_bot, "can_send_dossier_recommendation", return_value=True):
            handled = asyncio.run(bnl01_bot.maybe_handle_channel_audit_command(message, message.content))
        self.assertTrue(handled)
        report = "\n".join(message.replies + channel.sent)
        self.assertIn("hellcat-nz", report)
        self.assertNotIn("DISCORD_BOT_TOKEN", report)
        self.assertNotIn("legacy safe activity", report)

        denied = FakeMessage("!bnl channels audit", channel, guild=guild)
        with mock.patch.object(bnl01_bot, "can_send_dossier_recommendation", return_value=False):
            handled = asyncio.run(bnl01_bot.maybe_handle_channel_audit_command(denied, denied.content))
        self.assertTrue(handled)
        self.assertIn("operator-only", denied.replies[-1])


if __name__ == "__main__":
    unittest.main()
