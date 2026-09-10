"""Original TikTok quote lookup crosses real prompt and source-refresh owners.

SQLite, show selection, direct/batch assembly and delivery guards remain real.
The fixed provider replies prove plumbing and stale-draft handling, not live
model factuality. No fixture author or quoted text is a production identity.
"""

import os
import sqlite3
import unittest
from unittest import mock

import test_requested_show_date_delivery as delivery
from bnl_journal_source_store import purge_user_bound_conversation_sources_on_connection


bot = delivery.bot
QUOTE = delivery.SEPTEMBER_COMMENT
REQUEST = (
    'BNL, verify the exact TikTok comment "' + QUOTE + '" and its speaker '
    'against an original chat record from the September 4, 2026 show.'
)


class ShowOriginalQuoteDeliveryTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = delivery.RequestedShowDateDeliveryTests.asyncSetUp
    _read = delivery.RequestedShowDateDeliveryTests._read

    def _show_basis(self, bases):
        selected = [b for b in bases if isinstance(b, bot.FinalizedShowPromptSourceBasis)]
        self.assertEqual(len(selected), 1)
        return selected[0]

    def _packet_env(self, enabled, channel_id):
        return mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": str(enabled).lower(),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "77",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "42",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
        })

    def _add_bound_original(self, text, *, public=True):
        result = delivery.record_source_event(
            bot.DB_FILE, guild_id=77, source_kind="tiktok_live_chat",
            source_key="bound-original",
            occurred_at_ms=delivery.show_fixture.durable_events()[0]["occurred_at_ms"]
            + 7 * 24 * 60 * 60 * 1000,
            raw_text=text, sanitized_summary=text, channel_policy="public_context",
            subject_ref="discord_user:4242", private_display_name="Test Bound",
            public_usable=public, metadata={"eventType": "comment", "handle": "test.bound"},
        )
        self.assertTrue(result.ok)

    def _purge_bound_original(self):
        with sqlite3.connect(bot.DB_FILE) as conn:
            self.assertEqual(purge_user_bound_conversation_sources_on_connection(conn, 77, 4242), 1)

    def _sync(self):
        result = delivery.show_fixture.sync_tiktok_show_evidence_ledgers(
            bot.DB_FILE, guild_id=77, read_model=self.read_model,
            artist_identity_index=delivery.show_fixture.artist_index(),
            environ=delivery.show_fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["projectionErrors"], 0)

    async def test_original_matches_reach_direct_and_batch_normal_and_packet_prompts(self):
        answer = 'Test September wrote: "' + QUOTE + '"'

        async def provider_answer(*_args, **kwargs):
            counter = kwargs.get("attempt_counter")
            if counter is not None:
                counter.mark_started()
            return answer

        for packet_enabled in (False, True):
            # Exercise the same retained-reader path with and without a live
            # website snapshot; a website outage must not disable the lookup.
            self.fetch.return_value = {} if packet_enabled else self.read_model
            with self.subTest(packet=packet_enabled, route="direct"):
                website = delivery.REAL_READ_MODEL_CONTEXT(REQUEST, "sealed_test")
                with self._packet_env(packet_enabled, 8810):
                    prompt, metadata = await self.runtime._direct_prompt_async(
                        "sealed_test", REQUEST, website_context=website, privileged=False,
                    )
                self.assertEqual(metadata["ordinary_chat_single_packet_applied"], packet_enabled)
                self.assertNotIn("Durable TikTok show analysis context:", prompt)
                basis = self._show_basis(metadata["prompt_source_bases"])
                self.assertIn(basis.rendered_context, prompt)
                self.assertTrue(any(
                    e.event_id == "september-authored-comment"
                    and e.source_text == QUOTE
                    and e.speaker_label == "Test September (@test.september)"
                    for e in basis.authored_excerpts
                ))
                self.assertIn("Original TikTok quote lookup", basis.rendered_context)
                self.assertIn("2026-09-04", basis.rendered_context)
                self.assertEqual(bot.prompt_source_basis_failure((basis,)), "")

            channel_id = 8811 + len(self.runtime.channel_ids)
            with self.subTest(packet=packet_enabled, route="batch"):
                with self._packet_env(packet_enabled, channel_id):
                    channel, generation, guard = await self.runtime._batch(
                        "sealed_test", REQUEST, answer=provider_answer, privileged=False,
                    )
                generation.assert_awaited_once()
                self.assertEqual(
                    generation.await_args.kwargs["route"],
                    bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE
                    if packet_enabled else "get_gemini_response",
                )
                self.assertEqual(channel.sent, [answer])
                basis = self._show_basis(guard.await_args.kwargs["prompt_source_bases"])
                self.assertIn(basis.rendered_context, generation.await_args.args[0])
                self.assertNotIn("Durable TikTok show analysis context:", generation.await_args.args[0])
                self.assertIn("Original TikTok quote lookup", basis.rendered_context)
                self.assertEqual(bot.prompt_source_basis_failure((basis,)), "")

    async def test_original_changes_invalidate_even_when_finalized_ledger_is_unchanged(self):
        needle = "A copper kite crossed the window."
        self._add_bound_original(needle)
        self._sync()
        _website, _context, basis = self._read(REQUEST.replace(QUOTE, needle), website_override="")
        with sqlite3.connect(bot.DB_FILE) as conn:
            ledger_before = conn.execute(
                "SELECT ledger_json FROM tiktok_show_evidence_ledgers "
                "WHERE guild_id=77 AND show_key='show-attendance-september'",
            ).fetchone()
        for mutation in ("edit_after_purge", "ineligible_reinsertion", "delete"):
            with self.subTest(mutation=mutation):
                # Use the existing governed deletion exception and source writer.
                # Ordinary stored originals stay immutable throughout this test.
                self._purge_bound_original()
                if mutation == "edit_after_purge":
                    self._add_bound_original("The copper kite has gone.")
                elif mutation == "ineligible_reinsertion":
                    self._add_bound_original(needle, public=False)
                fresh, changed = bot.refresh_prompt_source_basis(basis)
                self.assertTrue(changed)
                self.assertEqual(fresh.show_keys, basis.show_keys)
                self.assertFalse(any(e.source_text == needle for e in fresh.authored_excerpts))
                self.assertEqual(bot.prompt_source_basis_failure((fresh,)), "")
                with sqlite3.connect(bot.DB_FILE) as conn:
                    self.assertEqual(conn.execute(
                        "SELECT ledger_json FROM tiktok_show_evidence_ledgers "
                        "WHERE guild_id=77 AND show_key='show-attendance-september'",
                    ).fetchone(), ledger_before)
                if mutation != "delete":
                    self._purge_bound_original()
                self._add_bound_original(needle)

    async def test_original_withdrawn_during_provider_await_never_sends_stale_draft(self):
        needle = "A silver kite landed at the window."
        self._add_bound_original(needle)
        self._sync()
        request = REQUEST.replace(QUOTE, needle)
        stale = 'Test Bound wrote: "' + needle + '"'
        corrected = "I cannot verify that attribution from the currently retained original records."
        calls = []

        async def provider_answer(prompt, *_args, **_kwargs):
            calls.append(prompt)
            if len(calls) == 1:
                self._purge_bound_original()
                return stale
            return corrected

        channel, generation, guard = await self.runtime._batch(
            "sealed_test", request, answer=provider_answer, privileged=False,
        )
        self.assertEqual(generation.await_count, 2)
        self.assertEqual(channel.sent, [corrected])
        self.assertNotIn(stale, channel.sent)
        self.assertIn("SOURCE LIFECYCLE UPDATE", calls[-1])
        self.assertIn("matchedOriginalRows=0", calls[-1])
        self.assertNotIn("Durable TikTok show analysis context:", calls[-1])
        # The disputed text remains in the user's request, but no current
        # source block can still pair it to the withdrawn original speaker.
        self.assertNotIn('speaker="Test Bound (@test.bound)"', calls[-1])
        self.assertTrue(guard.await_count)

    async def test_new_original_match_invalidates_a_previous_completed_no_match(self):
        needle = "The copper kites arrived after midnight."
        request = REQUEST.replace(QUOTE, needle)
        _website, _context, basis = self._read(request, website_override="")
        self.assertFalse(any(e.source_text == needle for e in basis.authored_excerpts))
        recorded = delivery.record_source_event(
            bot.DB_FILE, guild_id=77, source_kind="tiktok_live_chat",
            source_key="late-original-comment",
            occurred_at_ms=delivery.show_fixture.durable_events()[0]["occurred_at_ms"]
            + 7 * 24 * 60 * 60 * 1000,
            raw_text=needle, sanitized_summary=needle,
            channel_policy="public_context", subject_ref="tiktok_handle:test.september",
            private_display_name="Test September", public_usable=True,
            metadata={"eventType": "comment", "handle": "test.september"},
        )
        self.assertTrue(recorded.ok)
        fresh, changed = bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertTrue(any(
            e.source_text == needle and e.speaker_label == "Test September (@test.september)"
            for e in fresh.authored_excerpts
        ))

    async def test_mixed_lookup_and_current_queue_keeps_independent_operational_answer(self):
        request = REQUEST + " Also, what is playing now, and is the queue open right now?"
        website = delivery.REAL_READ_MODEL_CONTEXT(request, "sealed_test")
        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request, website_context=website, privileged=False,
        )
        self.assertIn("Present Signal", prompt)
        self.assertNotIn("Durable TikTok show analysis context:", prompt)
        basis = self._show_basis(metadata["prompt_source_bases"])
        self.assertIn("Original TikTok quote lookup", basis.rendered_context)
        self.assertTrue(any(e.source_text == QUOTE for e in basis.authored_excerpts))

    async def test_website_show_without_finalized_ledger_keeps_its_independent_context(self):
        # The website still has August, but only September has a finalized
        # lookup owner. Removing September's duplicate must preserve August.
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute(
                "DELETE FROM tiktok_show_evidence_ledgers "
                "WHERE guild_id=77 AND show_key='show-attendance-1'",
            )
        request = delivery.COMPARE_REQUEST + ' Can you verify the exact comment "' + QUOTE + '"?'
        website = delivery.REAL_READ_MODEL_CONTEXT(request, "sealed_test")
        prompt, metadata = await self.runtime._direct_prompt_async(
            "sealed_test", request, website_context=website, privileged=False,
        )
        basis = self._show_basis(metadata["prompt_source_bases"])
        self.assertEqual(basis.show_keys, ("show-attendance-september",))
        self.assertIn(delivery.AUGUST_COMMENT, prompt)
        self.assertEqual(prompt.count("Durable TikTok show analysis context:"), 1)
        self.assertIn("Original TikTok quote lookup", prompt)


if __name__ == "__main__":
    unittest.main()
