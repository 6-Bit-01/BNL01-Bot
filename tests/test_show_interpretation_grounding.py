"""Show interpretations retain original evidence through real turn assembly.

Only website/provider/Discord transport is replaced. Context, subject/frame
resolution, show readers, packets and response guards use their real owners.
"""

import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import test_public_network_knowledge as network_fixture
import test_tiktok_show_evidence_ledger as show_fixture


REAL_WEBSITE_CONTEXT = bot.maybe_build_bnl_read_model_context
QUESTION = "What is your read on the chat this evening?"
FOLLOWUP = "I feel you're skipping over some things. You didn't feel a..... Tension?"
COMMENT = "the green visuals during this song are wild."


class ShowInterpretationGroundingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = network_fixture.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.runtime._seed_finalized_show()
        self.runtime.stack.enter_context(mock.patch.object(
            bot, "BNL_PRIMARY_GUILD_ID", self.runtime.guild_id,
        ))
        self.runtime.stack.enter_context(mock.patch.object(
            bot, "maybe_build_bnl_read_model_context", new=REAL_WEBSITE_CONTEXT,
        ))
        self.fetch = self.runtime.stack.enter_context(mock.patch.object(
            bot, "fetch_bnl_read_model", return_value=show_fixture.authorized_read_model({
                "currentShow": None, "latestShow": show_fixture.archived_show(), "shows": [],
            }),
        ))

    def _packet_flags(self, channel_id):
        return mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": str(self.runtime.guild_id),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": str(self.runtime.user_id),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
        })

    def _seed_recent_exchange(self, channel_id, prior="How is the show going?"):
        now = datetime.now(timezone.utc)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.executemany(
                """INSERT INTO conversations (
                    guild_id, user_id, user_name, role, content, channel_id,
                    channel_name, channel_policy, route_mode, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, 'barcode-bot', 'public_home', 'normal_chat', ?)""",
                [(
                    self.runtime.guild_id, self.runtime.user_id, name, role, text,
                    channel_id, (now - timedelta(minutes=minutes)).isoformat(),
                ) for name, role, text, minutes in (
                    ("Test Member", "user", prior, 2),
                    ("BNL-01", "model", "A public track is receiving cheers.", 1),
                )],
            )

    def _assert_original_show_evidence(self, prompt):
        self.assertIn(COMMENT, prompt)
        self.assertIn("2026-08-28", prompt)
        self.assertNotIn("This private row must never enter", prompt)

    async def _direct_prompt(self, question):
        inputs = self.runtime._direct_prompt_inputs("public_home", question, privileged=False)
        inputs["website_read_model_context"] = bot.maybe_build_bnl_read_model_context(
            question, "public_home", conversation_context=inputs["room_context"],
            guild_id=self.runtime.guild_id, subject_user_id=self.runtime.user_id,
            channel_id=8810, channel_name="barcode-bot",
            conversation_context_result=inputs["conversation_context_result"],
        )
        prompt, *_ = await bot.build_user_aware_prompt_async(**inputs)
        return prompt, inputs["prompt_metadata"]

    async def test_direct_interpretation_keeps_show_evidence_and_valid_packet(self):
        self._seed_recent_exchange(8810)
        with self._packet_flags(8810):
            prompt, metadata = await self._direct_prompt(QUESTION)
            basis = metadata["ordinary_chat_single_packet_basis"]
            self.assertIsNotNone(basis)
            self.assertNotEqual(basis.packet.subject_resolution.status, "ambiguous")
            self.assertTrue(basis.packet.diagnostics.revalidation_status.startswith("passed"))
            assembled = bot.build_packet_owned_prompt(prompt, basis)
            self.assertTrue(assembled.ready, assembled.reason)
            self._assert_original_show_evidence(assembled.prompt)

    async def test_batch_interpretation_sends_once_without_ambiguity_repair(self):
        async def answer(*_args, **kwargs):
            if kwargs.get("attempt_counter") is not None:
                kwargs["attempt_counter"].mark_started()
            return "The recorded comments include praise for the green visuals."

        for channel_id, question, prior, route in (
            (8811, QUESTION, "How is the show going?", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE),
            (8812, FOLLOWUP, "Give us a rundown of the show chat tonight.", "get_gemini_response"),
        ):
            with self.subTest(question=question):
                self._seed_recent_exchange(channel_id, prior)
                with self._packet_flags(channel_id):
                    channel, generation, _guard = await self.runtime._batch(
                        "public_home", question, answer, privileged=False, channel_id=channel_id,
                    )
                generation.assert_awaited_once()
                self.assertEqual(generation.await_args.kwargs["route"], route)
                self._assert_original_show_evidence(generation.await_args.args[0])
                self.assertEqual(len(channel.sent), 1)
                with sqlite3.connect(bot.DB_FILE) as conn:
                    receipt = conn.execute(
                        """SELECT source_revalidation_status, corrective_call_count,
                                  response_sent FROM memory_governance_shared_brain_synthesis_runs
                           ORDER BY rowid DESC LIMIT 1""",
                    ).fetchone()
                    count = conn.execute(
                        "SELECT count(*) FROM memory_governance_shared_brain_synthesis_runs"
                    ).fetchone()[0]
                # The follow-up uses the existing website show-analysis
                # owner. It must not be forced into a competing packet route.
                self.assertEqual(count, 1)
                self.assertEqual(receipt, ("passed", 0, 1))

    async def test_interpretive_followup_reloads_original_comments(self):
        self._seed_recent_exchange(8810, "Give us a rundown of the show chat tonight.")
        with self._packet_flags(8810):
            prompt, _metadata = await self._direct_prompt(FOLLOWUP)
        self._assert_original_show_evidence(prompt)
        self.fetch.assert_called()

    async def test_source_content_and_date_do_not_require_system_vocabulary(self):
        for question in (
            "Tell me about the green visuals.",
            "How was the crowd responding on August 28, 2026?",
            "What were people saying about the green visuals?",
        ):
            with self.subTest(question=question), self._packet_flags(8810):
                prompt, metadata = await self._direct_prompt(question)
                basis = metadata["ordinary_chat_single_packet_basis"]
                self.assertNotEqual(basis.packet.subject_resolution.status, "ambiguous")
                assembled = bot.build_packet_owned_prompt(prompt, basis)
                self.assertTrue(assembled.ready, assembled.reason)
                self._assert_original_show_evidence(assembled.prompt)
        for question in ("Explain guitar string tension.", "Tell me a joke about ferns."):
            with self.subTest(question=question), self._packet_flags(8810):
                prompt, _ = await self._direct_prompt(question)
                self.assertNotIn(COMMENT, prompt)

    async def test_why_followup_reopens_sources_behind_the_paired_answer(self):
        self._seed_recent_exchange(8810, "Recap the August 28, 2026 BARCODE Radio show.")
        for question in ("What made you think that?", "Why did you say that?",
                         "What led you to conclude that?"):
            with self.subTest(question=question), self._packet_flags(8810):
                prompt, _ = await self._direct_prompt(question)
                self._assert_original_show_evidence(prompt)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("DELETE FROM conversations WHERE channel_id=8810 AND role='user'")
        # BNL's orphaned claim supplies neither a date nor factual authority.
        prompt, _ = await self._direct_prompt("What made you think that?")
        self.assertNotIn(COMMENT, prompt)

    def test_followup_cannot_open_history_from_bnl_prose_or_jump_topics(self):
        for context in (
            "",
            "BNL-01: The show chat was full of tension.",
            "User/member: Recap the show chat tonight.\n"
            "User/member: New topic: explain guitar string tension.",
        ):
            with self.subTest(context=context):
                self.assertEqual(bot.resolve_tiktok_show_analysis_request(FOLLOWUP, context), "")
        self.assertEqual(bot.resolve_tiktok_show_analysis_request(
            "New topic: what about the mood?", "User/member: Recap the show chat tonight.",
        ), "")


if __name__ == "__main__":
    unittest.main()
