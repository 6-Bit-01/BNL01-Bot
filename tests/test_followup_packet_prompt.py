"""Real public batch prompt assembly; Discord/provider outputs are fixtures.

These tests begin at the batch owner with explicit addressing. They verify the
Context-to-frame-to-packet handoff, not gateway routing or model answer quality.
"""
import gc
import json
import os
import sqlite3
import unittest
from contextlib import ExitStack, closing
from dataclasses import replace
from unittest import mock

import test_batch_addressed_continuation as addressing_fixture
import test_public_network_knowledge as fixtures
import test_tiktok_show_evidence_ledger as show_fixture
import bnl_unified_intelligence_packet as packet_owner

bot = fixtures.bnl01_bot
ROOT = (
    "Archive gremlin, I need a crate inspection: which of my songs have other people "
    "submitted to past shows? Give me the track, artist credit, TikTok submitter profile, "
    "and show date."
)
FEATURED = "Which of those has the featured-artist credit, and what was its show date?"
PRIOR_ANSWER = (
    "Neutral Collaboration has the featured-artist credit: 6-Bit featuring Second Artist. "
    "Its show date was September 25, 2026."
)
CURRENT = "And what is the full artist credit for that same track?"


class FollowupPacketPromptTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.runtime = fixtures.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.runtime._seed_finalized_show()
        self.channel_id = 896676
        self.message_id = 800000
        flags = {name: "true" for name in (
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED", "BNL_MOMENT_ENGINE_SHADOW_ENABLED",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED", "BNL_RELATIONSHIP_V2_SHADOW_ENABLED",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_PUBLIC_ENABLED",
        )}
        flags.update({
            "BNL_OWNER_USER_ID": "42", "BNL_PRIMARY_GUILD_ID": "77",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "77",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "42",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(self.channel_id),
        })
        self.runtime.stack.enter_context(mock.patch.dict(os.environ, flags))
        shows = []
        for index, (date, next_date, title, credit, handle) in enumerate((
            ("2026-09-11", "2026-09-12", "Neutral Signal", "6 Bit", "test.submitter"),
            ("2026-09-25", "2026-09-26", "Neutral Collaboration", "6-Bit featuring Second Artist", "second.submitter"),
        )):
            show = json.loads(json.dumps(show_fixture.archived_show()).replace(
                "2026-08-28", date).replace("2026-08-29", next_date))
            show["sessionId"] = "neutral-followup-prompt-%s" % index
            show["showDate"] = date
            show["trackRoster"] = [{
                "trackId": "neutral-track-%s" % index, "projectLabel": credit,
                "title": title, "submittedByTikTokHandle": handle,
                "outcome": "finished", "lane": "regular", "submissionEventSequence": 1,
            }]
            shows.append(show)
        result = show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=77,
            read_model=show_fixture.authorized_read_model({
                "currentShow": None, "latestShow": shows[-1], "shows": shows}),
            artist_identity_index={}, environ=show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["showsFinalized"], 2)

    async def asyncTearDown(self):
        gc.collect()
        await self.runtime.asyncTearDown()

    def save_human(self, text):
        self.message_id += 1
        bot.save_user_message(42, "Test Member", 77, text,
            channel_name="barcode-bot", channel_policy="public_context",
            channel_id=self.channel_id, message_id=self.message_id, directed_to_bnl=True)
        return replace(addressing_fixture.addressing("mention"),
            source_message_id=self.message_id, speaker_user_id=42)

    async def run_batch(self, question, answer):
        metadata = self.save_human(question)
        participants = [bot.BatchConversationTurn("Test Member", question, 42, metadata)]
        with ExitStack() as stack:
            provider = stack.enter_context(mock.patch.object(
                bot, "get_tracked_gemini_response_with_optional_typing",
                new=mock.AsyncMock(return_value=bot.TrackedGenerationResponse(answer, 1))))
            ordinary = stack.enter_context(mock.patch.object(
                bot, "maybe_generate_ordinary_chat_single_packet",
                wraps=bot.maybe_generate_ordinary_chat_single_packet))
            stack.enter_context(mock.patch.object(
                bot, "build_tiktok_show_evidence_context_for_turn",
                wraps=fixtures.REAL_SHOW_CONTEXT_FOR_TURN))
            channel, legacy, guard = await self.runtime._batch(
                "public_context", request=question, answer=answer,
                participants=participants, channel_id=self.channel_id)
            self.assertEqual(channel.sent, [answer])
            provider.assert_awaited_once()
            legacy.assert_not_awaited()
            ordinary.assert_awaited_once()
            self.assertTrue(ordinary.await_args.kwargs["scope_applied"])
            prompt = provider.await_args.args[1]
            self.assertNotIn(fixtures.INTERNAL_MEMORY, prompt)
            self.assertNotIn(fixtures.SEALED_MEMORY, prompt)
            return prompt, ordinary.await_args.kwargs["basis"], guard.await_args.kwargs["prompt_source_bases"]

    async def test_retained_featured_answer_reaches_final_packet_prompt(self):
        self.save_human(ROOT)
        # The root reply was delivered without storing model prose. Subsequent
        # addressing is explicit so this test isolates preparation, not ingress.
        bot._mark_conversation_continuation_state(
            77, self.channel_id, 42, channel_policy="public_context")
        await self.run_batch(FEATURED, PRIOR_ANSWER)
        observed_context = []
        real_context = bot.build_conversation_context_v2_for_prompt

        def capture_context(*args, **kwargs):
            rendered = real_context(*args, **kwargs)
            observed_context.append(((kwargs.get("result_out") or {}).get("result"), rendered))
            return rendered

        answer = "The full artist credit for Neutral Collaboration is 6-Bit featuring Second Artist."
        with mock.patch.object(bot, "build_conversation_context_v2_for_prompt", side_effect=capture_context):
            prompt, basis, source_bases = await self.run_batch(CURRENT, answer)

        resolved = [(result, rendered) for result, rendered in observed_context
                    if result is not None and result.referent_status == "resolved"]
        self.assertTrue(resolved)
        self.assertTrue(any(result.referent_reason == "human_request_subset_chain" for result, _ in resolved))
        for _, rendered in resolved:
            self.assertIn(rendered, prompt)
        for text in (ROOT, FEATURED, PRIOR_ANSWER, CURRENT, "Neutral Signal", "Neutral Collaboration"):
            self.assertIn(text, prompt)

        frame = basis.assessment.situation_frame
        self.assertIsNotNone(frame)
        self.assertEqual(len(frame.tasks), 1)
        self.assertEqual(frame.tasks[0].authority_scope, "packet")
        self.assertFalse(frame.subjects, "chain speaker labels are not current artist subjects")
        proved_ids = set(resolved[-1][0].referent_selected_row_ids)
        self.assertTrue(proved_ids)
        self.assertTrue(proved_ids.issubset(set(frame.exact_source_row_ids)))
        show_items = [item for item in basis.packet.items if item.lane == "show_episode"]
        self.assertEqual(len(show_items), 1)
        self.assertFalse(basis.packet.diagnostics.invalid_invariants)
        self.assertFalse(packet_owner._packet_invariants(basis.packet))
        packet_text = show_items[0].text
        for text in ("Neutral Signal", "Neutral Collaboration", "6-Bit featuring Second Artist", "2026-09-25"):
            self.assertIn(text, packet_text)
        self.assertTrue(basis.rendered_context, "selected show evidence must be rendered, not merely collected")
        self.assertIn(basis.rendered_context, prompt)
        self.assertIn("Neutral Collaboration", basis.rendered_context)
        plan = prompt[prompt.index("TURN RESPONSE PLAN:"):]
        self.assertIn("authority=packet", plan)
        self.assertIn("supportKind=packet", plan)
        self.assertRegex(plan, r'evidenceIds=\["E\d+')
        self.assertNotIn("SUPPORT REFERENCES:\n- none", plan)
        self.assertNotIn("supportKind=external_public", plan)
        self.assertNotIn('evidenceIds=["PUBLIC"]', plan)

        for field, value in (
            ("source_class", "evidence_projection"),
            ("visibility", "private"),
            ("lineage", ()),
            ("source_type", "barcode_show_dialogue_projection"),
            ("usage", "historical_topic_context"),
            ("uncertainty_status", "unrecognized_artist_credit_status"),
        ):
            with self.subTest(invalid_field=field):
                invalid_item = replace(show_items[0], **{field: value})
                invalid_packet = replace(basis.packet, items=tuple(
                    invalid_item if item is show_items[0] else item for item in basis.packet.items))
                self.assertIn("show_episode_lane_contract_violation",
                              packet_owner._packet_invariants(invalid_packet))

        native_basis = next(item for item in source_bases if isinstance(item, bot.FinalizedShowPromptSourceBasis))
        with closing(sqlite3.connect(bot.DB_FILE)) as conn, conn:
            conn.execute("UPDATE tiktok_show_evidence_ledgers SET ledger_json='{}'")
        refreshed, changed = bot.refresh_prompt_source_basis(native_basis)
        self.assertTrue(changed)
        self.assertNotIn("Neutral Collaboration", refreshed.rendered_context)
        with closing(sqlite3.connect(bot.DB_FILE)) as conn:
            packet_refresh = packet_owner.revalidate_packet(
                conn, replace(basis.packet, items=(show_items[0],), validation_items=()), environ=os.environ)
        self.assertFalse(packet_refresh.valid)
        self.assertEqual(packet_refresh.status, "source_changed")
        self.assertEqual(packet_refresh.changed_source_count, 1)

