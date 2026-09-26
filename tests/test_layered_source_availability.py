"""Packet ownership preserves ordinary readers and independent source lanes."""
import os
import unittest
from unittest import mock

import test_public_network_knowledge as fixture
from bnl_unified_intelligence_packet import revalidate_packet

bot = fixture.bnl01_bot


class LayeredSourceAvailabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = fixture.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.controls = self.runtime._seed_publications()
        self.runtime._seed_topic_recall_sources()
        flags = {key: "true" for key in (
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED", "BNL_MOMENT_ENGINE_SHADOW_ENABLED",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED", "BNL_RELATIONSHIP_V2_SHADOW_ENABLED",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED",
        )}
        flags.update(
            BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS=str(self.runtime.guild_id),
            BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS=str(self.runtime.user_id),
            BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS="8810",
        )
        self.runtime.stack.enter_context(mock.patch.dict(os.environ, flags))

    async def prompt(self, question):
        prompt, metadata = await self.runtime._direct_prompt_async(
            "public_home", request=question, privileged=False,
        )
        basis = metadata["ordinary_chat_single_packet_basis"]
        self.assertFalse(basis.packet.diagnostics.invalid_invariants)
        assembled = bot.build_packet_owned_prompt(prompt, basis)
        self.assertTrue(assembled.ready, assembled.reason)
        self.assertNotIn(fixture.INTERNAL_MEMORY, assembled.prompt)
        self.assertNotIn(fixture.SEALED_MEMORY, assembled.prompt)
        return assembled.prompt, basis

    async def test_topic_paraphrases_reach_both_publications_without_system_names(self):
        for question in (
            "Tell me about the Copper Kite instrumental.",
            "What do you make of how people reacted to Copper Kite?",
            "Was Copper Kite mentioned in your writing?",
            "Connect what you wrote about Copper Kite with what I told you about the paper comet.",
        ):
            with self.subTest(question=question):
                prompt, _ = await self.prompt(question)
                self.assertIn(fixture.JOURNAL_BODY, prompt)
                self.assertIn(fixture.RELAY_BODY, prompt)
                if "paper comet" in question:
                    self.assertIn("The paper comet melody has muted bells.", prompt)

    async def test_independent_relay_and_member_memory_survive_unavailable_journal(self):
        self.controls.return_value = (None, "control_snapshot_unavailable")
        prompt, basis = await self.prompt(
            "What did the Journal and Relay say about Copper Kite? Also, remind me about the paper comet."
        )
        self.assertNotIn(fixture.JOURNAL_BODY, prompt)
        self.assertIn(fixture.RELAY_BODY, prompt)
        self.assertIn("The paper comet melody has muted bells.", prompt)
        self.controls.reset_mock()
        self.assertEqual(bot._shared_brain_journal_revalidation_snapshot(basis), (None, False))
        self.controls.assert_not_called()
        # The failed Journal lookup cannot invalidate the retained Relay at
        # a later source fence. Its own eligibility remains independently checked.
        with bot.sqlite3.connect(bot.DB_FILE) as conn:
            result = revalidate_packet(
                conn, basis.packet, journal_control_snapshot=None,
                journal_control_snapshot_provided=True,
            )
        self.assertTrue(result.valid, result.status)

    async def test_unrelated_question_does_not_fetch_controls_or_add_publications(self):
        self.controls.reset_mock()
        prompt, _ = await self.prompt("Explain why a checksum detects a corrupted file.")
        self.assertNotIn(fixture.JOURNAL_BODY, prompt)
        self.assertNotIn(fixture.RELAY_BODY, prompt)
        self.controls.assert_not_called()

    async def test_recap_and_memory_are_both_answerable_in_one_request(self):
        prompt, _ = await self.prompt("Recap the Relay and remind me about my paper comet.")
        self.assertIn(fixture.RELAY_BODY, prompt)
        self.assertIn("The paper comet melody has muted bells.", prompt)
