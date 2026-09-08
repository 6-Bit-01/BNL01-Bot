"""Source-backed continuation through the existing Context and show owners."""

import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from tests import test_tiktok_show_evidence_ledger as show_fixture


class FinalizedShowFollowthroughTests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {
            **show_fixture.ENABLED_QUEUE_ENV,
            "BNL_CONVERSATION_CONTEXT_V2_ENABLED": "true",
            "GEMINI_API_KEY": "test-key",
            "DISCORD_BOT_TOKEN": "test-token",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        import bnl01_bot
        self.bot = bnl01_bot
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db_file = str(Path(self.directory.name) / "show-followthrough.db")
        db_patch = mock.patch.object(self.bot, "DB_FILE", self.db_file)
        db_patch.start()
        self.addCleanup(db_patch.stop)
        fixture = show_fixture.TikTokShowEvidenceLedgerTests()
        fixture.seed_source_and_memory(self.db_file)
        self.now = datetime(2026, 9, 8, 0, 0, tzinfo=timezone.utc)
        self._sync_shows()

    def _sync_shows(self, guild_id=77):
        older = json.loads(
            json.dumps(show_fixture.archived_show())
            .replace("show-attendance-1", "show-attendance-older")
            .replace("2026-08-28", "2026-08-21")
            .replace("2026-08-29", "2026-08-22")
        )
        show_fixture.sync_tiktok_show_evidence_ledgers(
            self.db_file,
            guild_id=guild_id,
            read_model=show_fixture.authorized_read_model({
                "currentShow": None,
                "latestShow": show_fixture.archived_show(),
                "shows": [older],
            }),
            artist_identity_index=show_fixture.artist_index(),
            environ=show_fixture.ENABLED_QUEUE_ENV,
        )

    def _context(self, current, *, prior_user_id=42, exact_other_reply=False):
        with sqlite3.connect(self.db_file) as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO conversations
                (id,user_id,user_name,guild_id,channel_name,channel_policy,
                 route_mode,role,content,timestamp,channel_id,message_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    (9001, prior_user_id, "Test Member", 77, "bnl-testing",
                     "sealed_test", "normal_chat", "user",
                     "Give me a recap of the 2026-08-28 show.",
                     (self.now - timedelta(minutes=2)).isoformat(), 9010, 99001),
                    (9002, prior_user_id, "BNL-01", 77, "bnl-testing",
                     "sealed_test", "normal_chat", "model",
                     "Here is the recorded recap.",
                     (self.now - timedelta(minutes=1)).isoformat(), 9010, 99002),
                ],
            )
            conn.executemany(
                """INSERT OR REPLACE INTO conversation_discord_message_links
                (conversation_row_id,guild_id,channel_id,message_id)
                VALUES (?,?,?,?)""",
                [(9001, 77, 9010, 99001), (9002, 77, 9010, 99002)],
            )
            if exact_other_reply:
                conn.execute(
                    """INSERT INTO conversations
                    (id,user_id,user_name,guild_id,channel_name,channel_policy,
                     route_mode,role,content,timestamp,channel_id,message_id)
                    VALUES (9003,42,'BNL-01',77,'bnl-testing','sealed_test',
                            'normal_chat','model',?,?,9010,99003)""",
                    ("A checksum detects changed bytes.", self.now.isoformat()),
                )
                conn.execute(
                    """INSERT INTO conversation_discord_message_links
                    (conversation_row_id,guild_id,channel_id,message_id)
                    VALUES (9003,77,9010,99003)""",
                )
        result_out = {}
        rendered = self.bot.build_conversation_context_v2_for_prompt(
            guild_id=77, current_user_id=42, channel_id=9010,
            channel_name="bnl-testing", channel_policy="sealed_test",
            route_mode="normal_chat", current_texts=(current,),
            current_participants={42}, is_direct_target=True,
            referenced_message_ids={99003} if exact_other_reply else set(),
            now=self.now, result_out=result_out,
        )
        result = result_out["result"]
        basis = self.bot.build_conversation_prompt_source_basis(
            rendered, guild_id=77, current_user_id=42, channel_id=9010,
            channel_name="bnl-testing", channel_policy="sealed_test",
            context_result=result,
        )
        self.assertIsNotNone(basis)
        self.assertIn(9003 if exact_other_reply else 9001, basis.source_row_ids)
        return result, basis

    def _show_context(self, current, *, subject_user_id=42, guild_id=77,
                      prior_user_id=42, exact_other_reply=False):
        result, basis = self._context(
            current, prior_user_id=prior_user_id, exact_other_reply=exact_other_reply,
        )
        selection = {}
        rendered = self.bot.build_tiktok_show_evidence_context_for_turn(
            guild_id=guild_id, user_text=current,
            subject_user_id=subject_user_id, conversation_basis=basis,
            conversation_context_result=result, selection_out=selection,
        )
        return rendered, selection, result

    def test_explicit_continuation_reloads_the_human_requests_show(self):
        rendered, selection, result = self._show_context("Continue.")
        self.assertEqual(result.thread_focus_mode, "continue_or_answer")
        self.assertIn("on 2026-08-28;", rendered)
        self.assertIn("2026-08-28", selection["selection_user_text"])
        self.assertTrue(selection["source_refs"])
        self.assertTrue(selection["candidate_context"])
        self.assertNotIn("Here is the recorded recap.", rendered)

    def test_generic_request_gets_labeled_candidates_without_show_ownership(self):
        for current in (
            "Give me some quotes",
            "Please?",
            "Explain checksum detection.",
        ):
            with self.subTest(current=current):
                rendered, selection, result = self._show_context(current)
                self.assertEqual(result.thread_focus_mode, "continue_or_answer")
                self.assertEqual(result.referent_status, "not_requested")
                self.assertTrue(selection["candidate_context"])
                self.assertIn("Prior-conversation source candidate:", rendered)
                self.assertIn("does not establish that the current request concerns this show", rendered)
                self.assertIn("Source-linked authored examples:", rendered)
                self.assertIn("the green visuals during this song are wild.", rendered)
                self.assertFalse(self.bot.finalized_show_packet_owner_requested(current, rendered))

    def test_ambiguous_current_referent_does_not_inherit_a_show(self):
        rendered, selection, result = self._show_context(
            "Separate topic: briefly explain why a checksum can detect a "
            "corrupted file but cannot repair it.",
        )
        self.assertEqual(result.referent_status, "ambiguous")
        self.assertEqual(rendered, "")
        self.assertEqual(selection, {})

    def test_exact_other_reply_remains_primary_and_does_not_inherit_a_show(self):
        rendered, selection, result = self._show_context(
            "Explain that.", exact_other_reply=True,
        )
        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_reason, "discord_reply_source")
        self.assertEqual(result.thread_focus_mode, "exact_discord_reply")
        self.assertEqual(rendered, "")
        self.assertEqual(selection, {})

    def test_continuation_cannot_inherit_another_speakers_show_request(self):
        rendered, selection, _result = self._show_context(
            "Continue.", prior_user_id=43,
        )
        self.assertEqual(rendered, "")
        self.assertEqual(selection, {})

    def test_subject_must_match_the_conversation_basis_requester(self):
        rendered, selection, _result = self._show_context(
            "Continue.", subject_user_id=43,
        )
        self.assertEqual(rendered, "")
        self.assertEqual(selection, {})

    def test_guild_must_match_even_when_both_guilds_have_the_show(self):
        self._sync_shows(guild_id=78)
        explicit = self.bot.build_tiktok_show_evidence_context_for_turn(
            guild_id=78, user_text="Give me a recap of the 2026-08-28 show.",
            subject_user_id=42,
        )
        self.assertIn("on 2026-08-28;", explicit)
        rendered, selection, _result = self._show_context(
            "Continue.", guild_id=78,
        )
        self.assertEqual(rendered, "")
        self.assertEqual(selection, {})

    def test_current_date_correction_wins_over_the_prior_human_date(self):
        rendered, selection, _result = self._show_context(
            "Continue, but use 2026-08-21.",
        )
        self.assertIn("2026-08-28", selection["selection_user_text"])
        self.assertIn("2026-08-21", selection["selection_user_text"])
        self.assertIn("on 2026-08-21;", rendered)
        self.assertNotIn("on 2026-08-28;", rendered)


class FinalizedShowCandidateAssemblyTests(unittest.IsolatedAsyncioTestCase):
    async def test_generic_quote_candidates_reach_real_direct_and_batch_without_priority(self):
        from tests import test_public_network_knowledge as network_fixture

        fixture = network_fixture.PublicNetworkKnowledgeTests()
        await fixture.asyncSetUp()
        try:
            fixture._seed_finalized_show()
            bot = network_fixture.bnl01_bot
            stamp = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
            with sqlite3.connect(bot.DB_FILE) as conn:
                for channel_index, channel_id in enumerate((8810, 8811)):
                    for role_index, (role, content) in enumerate((
                        ("user", "Give me a recap of the show on 2026-08-28."),
                        ("model", "The archive contains attendee remarks."),
                    )):
                        row_id = 9001 + channel_index * 2 + role_index
                        conn.execute(
                            """INSERT INTO conversations
                            (id,user_id,user_name,guild_id,channel_name,
                             channel_policy,route_mode,role,content,timestamp)
                            VALUES (?,42,'Test Member',77,'bnl-testing',
                                    'sealed_test','normal_chat',?,?,?)""",
                            (row_id, role, content, stamp),
                        )
                        conn.execute(
                            """INSERT INTO conversation_discord_message_links
                            (conversation_row_id,guild_id,channel_id,message_id)
                            VALUES (?,77,?,?)""",
                            (row_id, channel_id, 99000 + row_id),
                        )
            request = "Give me some quotes"
            prompt, metadata = await fixture._direct_prompt_async("sealed_test", request=request)
            sources = fixture._assert_show_source(prompt, metadata["prompt_source_bases"])
            self.assertTrue(sources[0].candidate_context)
            self.assertIn("Prior-conversation source candidate:", prompt)
            self.assertNotIn("Finalized BARCODE Radio episode priority:", prompt)
            self.assertFalse(bot.finalized_show_packet_owner_requested(request, prompt))
            channel, generation, guard = await fixture._batch(
                "sealed_test", request=request,
                answer="Alex commented on the green visuals during the song.",
            )
            self.assertEqual(generation.await_count, 1)
            batch_prompt = generation.await_args.args[0]
            sources = fixture._assert_show_source(
                batch_prompt, guard.await_args.kwargs["prompt_source_bases"],
            )
            self.assertTrue(sources[0].candidate_context)
            self.assertIn("Prior-conversation source candidate:", batch_prompt)
            self.assertNotIn("Finalized BARCODE Radio episode priority:", batch_prompt)
            self.assertFalse(bot.finalized_show_packet_owner_requested(request, batch_prompt))
            self.assertTrue(channel.sent)

            unrelated = "Explain checksum detection."
            unrelated_prompt, metadata = await fixture._direct_prompt_async(
                "sealed_test", request=unrelated,
            )
            self.assertNotIn("Finalized BARCODE Radio episode priority:", unrelated_prompt)
            self.assertFalse(bot.finalized_show_packet_owner_requested(unrelated, unrelated_prompt))
        finally:
            await fixture.asyncTearDown()


if __name__ == "__main__":
    unittest.main()
