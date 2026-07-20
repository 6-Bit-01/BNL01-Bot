import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl01_bot


class FakeAuthor:
    def __init__(self, user_id=99):
        self.id = user_id
        self.bot = False


class FakeGuild:
    def __init__(self, owner_id=99):
        self.id = 1
        self.owner_id = owner_id

    def get_member(self, user_id):
        return None


class FakeChannel:
    def __init__(self, name="research-and-development", guild=None):
        self.name = name
        self.id = 123
        self.guild = guild or FakeGuild()
        self.parent = None
        self.sent = []

    async def send(self, text, **kwargs):
        self.sent.append(text)


class FakeMessage:
    def __init__(self, content="!bnl entity evidence refresh Crow", channel_name="research-and-development", author_id=99, owner_id=99):
        self.content = content
        self.author = FakeAuthor(author_id)
        self.guild = FakeGuild(owner_id)
        self.channel = FakeChannel(channel_name, self.guild)
        self.replies = []

    async def reply(self, text, **kwargs):
        self.replies.append(text)


class FailingReplyMessage(FakeMessage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_first_reply = True

    async def reply(self, text, **kwargs):
        if self.fail_first_reply:
            self.fail_first_reply = False
            raise RuntimeError("simulated discord send failure")
        self.replies.append(text)


class DiscordSafeChunkTests(unittest.TestCase):
    def test_discord_safe_chunks_never_exceed_limit_or_emit_empty_chunks(self):
        text = "A" * 250 + "\n\n" + "B" * 250 + "\n- " + ("C" * 250)
        chunks = bnl01_bot.discord_safe_chunks(text, limit=100)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunks))
        self.assertTrue(all(len(chunk) <= 100 for chunk in chunks))

    def test_discord_safe_chunks_prefers_section_and_bullet_boundaries(self):
        section_text = "Overview\n\n" + "A" * 35 + " " + "B" * 35
        section_chunks = bnl01_bot.discord_safe_chunks(section_text, limit=50)
        self.assertEqual(section_chunks[0], "Overview")

        bullet_text = "Intro line\n- First bullet has enough text\n- Second bullet has enough text"
        bullet_chunks = bnl01_bot.discord_safe_chunks(bullet_text, limit=45)
        self.assertEqual(bullet_chunks[0], "Intro line\n- First bullet has enough text")
        self.assertTrue(bullet_chunks[1].startswith("- Second bullet"))

    def test_short_responses_still_send_as_one_reply(self):
        message = FakeMessage()
        asyncio.run(bnl01_bot.reply_with_discord_safe_chunks(message, "short response"))
        self.assertEqual(message.replies, ["short response"])
        self.assertEqual(message.channel.sent, [])


class OperatorCommandSafeReplyTests(unittest.TestCase):
    def setUp(self):
        self.owner_patch = mock.patch.object(bnl01_bot, "BNL_OWNER_USER_ID", 99)
        self.owner_patch.start()

    def tearDown(self):
        self.owner_patch.stop()

    def test_entity_evidence_refresh_long_response_replies_then_channel_sends(self):
        message = FakeMessage("!bnl entity evidence refresh Crow")
        long_summary = {
            "subjectName": "Crow",
            "rawProvenance": {"rawFragments": []},
            "missingInfo": [],
            "activitySignals": [f"Signal sentence {idx}. " + ("x" * 120) for idx in range(30)],
        }
        with mock.patch.object(bnl01_bot, "refresh_entity_evidence_for_subject", return_value={
            "subjectName": "Crow",
            "createdCount": 1,
            "updatedCount": 2,
            "unchangedCount": 3,
            "sourceTypes": {"profile": 1, "broadcast_memory": 1},
            "reviewOnlyCount": 4,
            "publicSafeCandidateCount": 5,
        }), mock.patch.object(bnl01_bot, "build_entity_activity_summary", return_value=long_summary), \
             mock.patch.object(bnl01_bot, "format_entity_activity_summary_response", return_value="BNL Entity Activity Summary\n\n" + ("- evidence bullet with details.\n" * 120)):
            handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(message, message.content))

        self.assertTrue(handled)
        self.assertEqual(len(message.replies), 1)
        self.assertGreater(len(message.channel.sent), 0)
        self.assertIn("BNL Entity Evidence Refresh", message.replies[0])
        self.assertTrue(all(len(chunk) <= 1900 for chunk in message.replies + message.channel.sent))

    def test_source_enrichment_dry_run_long_response_is_split_safely(self):
        message = FakeMessage("!bnl source enrich Crow | dry_run=true")
        with mock.patch.object(bnl01_bot, "run_source_file_enrichment", return_value={"ok": True, "subject": "Crow", "dryRun": True}), \
             mock.patch.object(bnl01_bot, "format_source_enrichment_response", return_value="Source File enrichment dry run\n\n" + ("- enrichment detail.\n" * 140)):
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_enrichment_command(message, message.content))

        self.assertTrue(handled)
        self.assertEqual(len(message.replies), 1)
        self.assertGreater(len(message.channel.sent), 0)
        self.assertTrue(all(len(chunk) <= 1900 for chunk in message.replies + message.channel.sent))

    def test_source_lookup_long_response_is_split_safely(self):
        message = FakeMessage("!bnl source lookup Crow")
        with mock.patch.object(bnl01_bot, "lookup_source_file", return_value={"ok": True}), \
             mock.patch.object(bnl01_bot, "format_source_file_lookup_response", return_value="Source File lookup\n\n" + ("- lookup detail.\n" * 150)):
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_lookup_command(message, message.content))

        self.assertTrue(handled)
        self.assertEqual(len(message.replies), 1)
        self.assertGreater(len(message.channel.sent), 0)
        self.assertTrue(all(len(chunk) <= 1900 for chunk in message.replies + message.channel.sent))


    def test_source_knowledge_bridge_long_response_is_split_safely(self):
        message = FakeMessage("!bnl source bridge knowledge | dry_run=true | limit=5")
        with mock.patch.object(bnl01_bot, "run_source_knowledge_bridge", return_value={"dryRun": True, "sentCount": 0, "duplicateCount": 0, "withheldCount": 0, "failedCount": 0}), \
             mock.patch.object(bnl01_bot, "format_source_knowledge_bridge_response", return_value="Source knowledge bridge\n\n" + ("- bridge detail.\n" * 150)):
            handled = asyncio.run(bnl01_bot.maybe_handle_source_knowledge_bridge_command(message, message.content))

        self.assertTrue(handled)
        self.assertEqual(len(message.replies), 1)
        self.assertGreater(len(message.channel.sent), 0)
        self.assertTrue(all(len(chunk) <= 1900 for chunk in message.replies + message.channel.sent))

    def test_dossier_discovery_dry_run_response_uses_safe_reply_helper(self):
        message = FakeMessage("!bnl dossier discover candidates | dry_run=true")
        fake_discovery = {
            "candidates": [{"subjectName": "Signal Witch", "payload": {"subjectName": "Signal Witch"}}],
            "payloads": [{"subjectName": "Signal Witch"}],
            "withheldCount": 0,
            "duplicateCount": 0,
            "sendableCandidateCount": 1,
        }
        with mock.patch.object(bnl01_bot, "discover_candidate_recommendations", return_value=fake_discovery), \
             mock.patch.object(bnl01_bot, "reply_with_discord_safe_chunks", new=mock.AsyncMock()) as safe_reply:
            handled = asyncio.run(bnl01_bot.maybe_handle_dossier_discovery_command(message, message.content))

        self.assertTrue(handled)
        safe_reply.assert_awaited_once()

    def test_send_failure_logs_and_falls_back_without_raising(self):
        message = FailingReplyMessage("!bnl source lookup Crow")
        with mock.patch.object(bnl01_bot, "lookup_source_file", return_value={"ok": True}), \
             mock.patch.object(bnl01_bot, "format_source_file_lookup_response", return_value="Source File lookup\n\n" + ("- lookup detail.\n" * 150)), \
             self.assertLogs(level="WARNING") as logs:
            handled = asyncio.run(bnl01_bot.maybe_handle_source_file_lookup_command(message, message.content))

        self.assertTrue(handled)
        self.assertEqual(message.replies, [bnl01_bot.DISCORD_SAFE_REPLY_FALLBACK])
        self.assertTrue(any("discord_safe_reply_failed" in line for line in logs.output))

    def test_safe_replies_do_not_invoke_unrelated_behavior(self):
        message = FakeMessage("!bnl entity evidence refresh Crow")
        with mock.patch.object(bnl01_bot, "refresh_entity_evidence_for_subject", return_value={"subjectName": "Crow"}), \
             mock.patch.object(bnl01_bot, "build_entity_activity_summary", return_value={"subjectName": "Crow", "rawProvenance": {"rawFragments": []}, "missingInfo": []}), \
             mock.patch.object(bnl01_bot, "send_dossier_recommendation") as sender, \
             mock.patch.object(bnl01_bot, "update_website_status") as website, \
             mock.patch.object(bnl01_bot, "record_community_presence_event") as community:
            handled = asyncio.run(bnl01_bot.maybe_handle_entity_activity_summary_command(message, message.content))

        self.assertTrue(handled)
        sender.assert_not_called()
        website.assert_not_called()
        community.assert_not_called()
        self.assertIn("BNL Entity Evidence Refresh", message.replies[0])


if __name__ == "__main__":
    unittest.main()
