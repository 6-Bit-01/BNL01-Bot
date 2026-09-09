"""Requester consent is rechecked without gating ordinary public show evidence.

These fixtures use real SQLite source owners and prompt refresh/delivery paths.
Only the provider and external transports use the public-network test harness.
"""

import asyncio
import os
import sqlite3
import unittest
from unittest import mock

import test_public_network_knowledge as public_fixtures
from bnl_relationship_engine import observe_message, set_member_setting

bnl01_bot = public_fixtures.bnl01_bot
PERSONAL_REQUEST = "What did I ask BNL during the live?"
PUBLIC_REQUEST = "What did the chat say during the show on 2026-08-28?"
PERSONAL_EVIDENCE = "Did the Wheel put Queue Light up next, BNL?"
PUBLIC_EVIDENCE = "the green visuals during this song are wild."


class FinalizedShowConsentRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Reuse the established real-source harness without inheriting and
        # rerunning its unrelated tests in this dedicated regression module.
        self.runtime = public_fixtures.PublicNetworkKnowledgeTests()
        await self.runtime.asyncSetUp()
        self.addAsyncCleanup(self.runtime.asyncTearDown)
        self.runtime._seed_finalized_show()

    def _set_consent(self, enabled):
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            set_member_setting(
                conn, guild_id=77, user_id=42, proactive_enabled=enabled,
            )

    def _read(self, request=PERSONAL_REQUEST, subject_user_id=42):
        selection = {}
        context = public_fixtures.REAL_SHOW_CONTEXT_FOR_TURN(
            guild_id=77,
            user_text=request,
            subject_user_id=subject_user_id,
            selection_out=selection,
        )
        basis = bnl01_bot.build_finalized_show_prompt_source_basis(
            context, guild_id=77, selection=selection,
        )
        return context, basis

    async def test_unchanged_requester_consent_preserves_source_basis(self):
        context, basis = self._read()
        self.assertIn(PERSONAL_EVIDENCE, context)
        self.assertEqual(basis.subject_user_id, 42)
        fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
        self.assertFalse(changed)
        self.assertEqual(fresh, basis)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure((basis,)), "")

    async def test_initial_opt_out_removes_self_recall_not_public_show_recall(self):
        self._set_consent(False)
        context, basis = self._read()
        self.assertEqual(context, "")
        self.assertIsNone(basis)
        public, public_basis = self._read(PUBLIC_REQUEST)
        self.assertIn(PUBLIC_EVIDENCE, public)
        self.assertEqual(public_basis.subject_user_id, 0)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure((public_basis,)), "")

    async def test_mid_turn_opt_out_invalidates_personal_context_and_excerpts(self):
        context, basis = self._read()
        self.assertIn(PERSONAL_EVIDENCE, context)
        self.assertTrue(basis.authored_excerpts)
        self._set_consent(False)
        self.assertEqual(
            bnl01_bot.prompt_source_basis_failure((basis,)),
            "show_episode_source_changed",
        )
        fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertEqual(fresh.subject_user_id, 0)
        self.assertEqual(fresh.rendered_context, "")
        self.assertEqual(fresh.authored_excerpts, ())
        self.assertEqual(bnl01_bot.prompt_source_basis_failure((fresh,)), "")

    async def test_typed_opt_out_uses_the_same_existing_consent_owner(self):
        _context, basis = self._read()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            observe_message(
                conn,
                guild_id=77,
                user_id=42,
                role="user",
                content="Don't follow up with me.",
                source_row_id="test-member-consent-revocation",
                user_name="Test Member",
                channel_policy="public_home",
                channel_name="barcode-bot",
                route_mode="normal_chat",
                directed=True,
            )
        fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertEqual(fresh.rendered_context, "")
        self.assertEqual(fresh.subject_user_id, 0)

    async def test_opt_out_does_not_invalidate_independently_public_show_context(self):
        context, basis = self._read(PUBLIC_REQUEST)
        self._set_consent(False)
        fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
        self.assertFalse(changed)
        self.assertEqual(fresh.subject_user_id, 0)
        self.assertEqual(fresh.rendered_context, context)
        self.assertIn(PUBLIC_EVIDENCE, fresh.rendered_context)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure((basis,)), "")

    async def test_public_only_basis_does_not_gain_requester_scope_mid_turn(self):
        self._set_consent(False)
        context, basis = self._read(PUBLIC_REQUEST)
        self.assertEqual(basis.subject_user_id, 0)
        self._set_consent(True)
        with mock.patch.object(
            bnl01_bot, "relationship_v2_proactive_consent_decision",
            side_effect=AssertionError("public-only refresh must not seek personal scope"),
        ):
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
        self.assertFalse(changed)
        self.assertEqual(fresh.subject_user_id, 0)
        self.assertEqual(fresh.rendered_context, context)

    async def test_consent_lookup_failure_removes_only_requester_scope(self):
        _context, personal = self._read()
        public_context, public = self._read(PUBLIC_REQUEST)
        with mock.patch.object(
            bnl01_bot, "relationship_v2_proactive_consent_decision",
            side_effect=sqlite3.OperationalError("test consent read unavailable"),
        ):
            self.assertEqual(self._read()[0], "")
            self.assertEqual(self._read(PUBLIC_REQUEST)[0], public_context)
            fresh_personal, personal_changed = bnl01_bot.refresh_prompt_source_basis(personal)
            fresh_public, public_changed = bnl01_bot.refresh_prompt_source_basis(public)
        self.assertTrue(personal_changed)
        self.assertEqual(fresh_personal.rendered_context, "")
        self.assertEqual(fresh_personal.subject_user_id, 0)
        self.assertFalse(public_changed)
        self.assertEqual(fresh_public.rendered_context, public_context)
        self.assertEqual(fresh_public.subject_user_id, 0)

    async def test_missing_database_is_not_created_or_reused_as_personal_authority(self):
        _context, basis = self._read()
        missing_path = os.path.join(self.runtime.tmp, "missing-show-source.db")
        with mock.patch.object(bnl01_bot, "DB_FILE", missing_path):
            self.assertEqual(self._read()[0], "")
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
        self.assertTrue(changed)
        self.assertEqual(fresh.rendered_context, "")
        self.assertEqual(fresh.subject_user_id, 0)
        self.assertFalse(os.path.exists(missing_path))

    async def test_source_refresh_keeps_supported_independent_publications(self):
        self.runtime._seed_publications()
        request = PERSONAL_REQUEST + " Also, what did the Journal say about Copper Kite?"
        prompt, metadata = self.runtime._direct_prompt("public_home", request)
        self.assertIn(PERSONAL_EVIDENCE, prompt)
        self.assertIn(public_fixtures.JOURNAL_BODY, prompt)
        self.assertIn(public_fixtures.RELAY_BODY, prompt)
        self._set_consent(False)
        refreshed, bases, changed, failed = bnl01_bot.refresh_prompt_source_bases(
            prompt, metadata["prompt_source_bases"],
        )
        self.assertIn("show_episode", changed)
        self.assertFalse(failed)
        self.assertNotIn(PERSONAL_EVIDENCE, refreshed)
        self.assertIn(public_fixtures.JOURNAL_BODY, refreshed)
        self.assertIn(public_fixtures.RELAY_BODY, refreshed)
        self.assertEqual(bnl01_bot.prompt_source_basis_failure(bases), "")

    async def test_revocation_during_provider_await_refreshes_before_delivery(self):
        self.runtime._seed_publications()
        request = PERSONAL_REQUEST + " Also, what did the Journal say about Copper Kite?"
        corrected_answer = public_fixtures.JOURNAL_BODY
        calls = []

        async def provider(prompt, *_args, **_kwargs):
            calls.append(prompt)
            if len(calls) == 1:
                self.assertIn(PERSONAL_EVIDENCE, prompt)
                self.assertIn(public_fixtures.JOURNAL_BODY, prompt)
                await asyncio.sleep(0)
                self._set_consent(False)
                return "You asked whether the Wheel put Queue Light up next."
            self.assertNotIn(PERSONAL_EVIDENCE, prompt)
            self.assertIn(public_fixtures.JOURNAL_BODY, prompt)
            return corrected_answer

        channel, generation, _guard = await self.runtime._batch(
            "public_home", request=request, answer=provider, privileged=False,
        )
        self.assertEqual(generation.await_count, 2)
        self.assertEqual(channel.sent, [corrected_answer])
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            replies = conn.execute(
                "SELECT content FROM conversations WHERE role='model' AND id > 104",
            ).fetchall()
        self.assertEqual(replies, [(corrected_answer,)])


if __name__ == "__main__":
    unittest.main()
