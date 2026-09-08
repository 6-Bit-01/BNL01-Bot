import os
import sqlite3
import tempfile
from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_moment_engine as moments
import bnl_memory_ledger as ledger
import test_moment_episode_lifecycle_v2 as episode_fixtures
import test_public_network_knowledge as public_fixtures


CANARY_ENV = {
    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
    "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
    "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
    "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
    "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
    "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
    "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
    "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
    "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "true",
    "BNL_UNIFIED_MOMENT_CANARY_GUILD_IDS": "1",
    "BNL_UNIFIED_MOMENT_CANARY_CHANNEL_IDS": "303",
}


class UnifiedMomentSealedCanaryTests(unittest.IsolatedAsyncioTestCase):
    def assessment(self):
        evidence = (
            bnl01_bot.build_conversation_evidence_item(
                text="“Chrome Prophet” sounds like a person.",
                source_id=10,
                speaker_user_id=101,
                speaker_label="Test Member",
            ),
            bnl01_bot.build_conversation_evidence_item(
                text=(
                    "The hidden room should sound like a place, "
                    "not a character."
                ),
                source_id=11,
                speaker_user_id=202,
                speaker_label="Miss Bit",
            ),
            bnl01_bot.build_conversation_evidence_item(
                text="“Null Basilica” sounds like a place.",
                source_id=12,
                speaker_user_id=101,
                speaker_label="Test Member",
            ),
        )
        return bnl01_bot.build_unified_response_assessment(
            guild_id=1,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(101,),
            participant_user_ids=(101, 202),
            speaker_labels=("Test Member", "Miss Bit"),
            current_exchange_source_ids=(10, 11, 12),
            prompt_lanes=("current_exchange", "conversation_context"),
            current_payload_anchors=(
                "chrome prophet",
                "null basilica",
            ),
            thread_focus_mode="new_thread",
            current_text=(
                "Between Chrome Prophet and Null Basilica, which fits that "
                "requirement better, and why?"
            ),
            conversation_evidence_items=evidence,
        )

    def build_basis(self):
        with mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
            return (
                bnl01_bot
                .build_unified_moment_canary_prompt_source_basis(
                    self.assessment(),
                    guild_id=1,
                    channel_id=303,
                    channel_policy="sealed_test",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    topic_text=(
                        "Between Chrome Prophet and Null Basilica, which "
                        "fits that requirement better, and why?"
                    ),
                    participant_user_ids=(101, 202),
                )
            )

    def test_configuration_requires_exact_sealed_guild_and_channel(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            self.assertTrue(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                )
            )
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=304,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )
            configuration = (
                bnl01_bot.unified_moment_canary_configuration()
            )
            self.assertTrue(configuration["fully_scoped"])
            self.assertEqual(configuration["guild_allowlist_count"], 1)
            self.assertEqual(
                configuration["channel_allowlist_count"],
                1,
            )
        broadened = {
            **CANARY_ENV,
            "BNL_UNIFIED_MOMENT_CANARY_GUILD_IDS": "1,2",
            "BNL_UNIFIED_MOMENT_CANARY_CHANNEL_IDS": "303,304",
        }
        with mock.patch.dict(os.environ, broadened, clear=False):
            self.assertFalse(
                bnl01_bot.unified_moment_canary_configuration()[
                    "fully_scoped"
                ]
            )
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )

        disabled = {**CANARY_ENV, "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false"}
        with mock.patch.dict(os.environ, disabled, clear=False):
            self.assertFalse(
                bnl01_bot.unified_moment_canary_enabled(
                    guild_id=1,
                    channel_id=303,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                )
            )

    def test_prompt_basis_exists_only_for_the_scoped_sealed_route(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            self.assertIsNotNone(basis)
            self.assertIn(
                "SEALED UNIFIED CONVERSATION CANARY",
                basis.rendered_context,
            )
            self.assertFalse(basis.episode_context_present)
            self.assertNotIn(
                "active_episode",
                basis.assessment.prompt_lanes,
            )

            public_assessment = bnl01_bot.build_unified_response_assessment(
                guild_id=1,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home",
                conversation_surface="public",
                current_speaker_user_ids=(101,),
                prompt_lanes=("current_exchange",),
            )
            with mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                public_basis = (
                    bnl01_bot
                    .build_unified_moment_canary_prompt_source_basis(
                        public_assessment,
                        guild_id=1,
                        channel_id=303,
                        channel_policy="public_home",
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        topic_text="What do you think?",
                        participant_user_ids=(101,),
                    )
                )
            self.assertIsNone(public_basis)

    def test_direct_prompt_applies_in_sealed_channel_and_public_is_identical(self):
        conversation_basis = bnl01_bot.ConversationPromptSourceBasis(
            expected_digest="digest",
            rendered_context="bounded room context",
            guild_id=1,
            current_user_id=101,
            channel_id=303,
            channel_name="bnl-testing",
            channel_policy="sealed_test",
            source_row_ids=(10, 11, 12),
            participant_user_ids=(101, 202),
            speaker_labels=("Test Member", "Miss Bit"),
            evidence_items=self.assessment().conversation_evidence_items,
        )

        def conversation_basis_for_route(
            _rendered,
            *,
            channel_policy,
            **_kwargs,
        ):
            return bnl01_bot.replace(
                conversation_basis,
                channel_policy=channel_policy,
            )

        patches = (
            mock.patch.object(
                bnl01_bot,
                "get_user_profile",
                return_value=("Test Member", ""),
            ),
            mock.patch.object(
                bnl01_bot,
                "should_allow_greeting",
                return_value=False,
            ),
            mock.patch.object(
                bnl01_bot,
                "choose_response_style",
                return_value=("balanced", "Respond naturally."),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_user_memory_context",
                return_value="No route-safe durable memory for this mode/channel.",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_broadcast_memory_context",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "build_conversation_prompt_source_basis",
                side_effect=conversation_basis_for_route,
            ),
            mock.patch.object(
                bnl01_bot,
                "build_community_visual_basis",
                return_value=SimpleNamespace(status="not_requested"),
            ),
            mock.patch.object(
                bnl01_bot,
                "render_community_visual_basis_for_prompt",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "_active_episode_id_for_unified_assessment",
                return_value="opaque_active_episode",
            ),
            mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"),
        )
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        request = (
            "Between Chrome Prophet and Null Basilica, which fits that "
            "requirement better, and why?"
        )
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            sealed_metadata = {}
            sealed_prompt, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Test Member",
                request,
                room_context="bounded room context",
                channel_name="bnl-testing",
                channel_id=303,
                channel_policy="sealed_test",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata=sealed_metadata,
            )
            public_prompt_on, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Test Member",
                request,
                room_context="bounded room context",
                channel_name="general",
                channel_id=303,
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata={},
            )
        disabled = {**CANARY_ENV, "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false"}
        with mock.patch.dict(os.environ, disabled, clear=False):
            public_prompt_off, *_ = bnl01_bot.build_user_aware_prompt(
                101,
                1,
                "Test Member",
                request,
                room_context="bounded room context",
                channel_name="general",
                channel_id=303,
                channel_policy="public_home",
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                is_direct_interaction=True,
                prompt_metadata={},
            )

        self.assertIn("SEALED UNIFIED CONVERSATION CANARY", sealed_prompt)
        self.assertTrue(sealed_metadata["unified_moment_canary_applied"])
        self.assertNotIn(
            "SEALED UNIFIED CONVERSATION CANARY",
            public_prompt_on,
        )
        self.assertEqual(public_prompt_on, public_prompt_off)

    async def test_coherence_guard_repairs_conclusion_reason_contradiction(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            provider = mock.AsyncMock(
                return_value=(
                    "Null Basilica fits better because it reads as a place "
                    "instead of a person."
                )
            )
            with mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                provider,
            ), mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                response, diagnostics = (
                    await bnl01_bot.apply_guarded_response_regeneration(
                        (
                            "Chrome Prophet is the better fit because it "
                            "sounds like a person, while Null Basilica sounds "
                            "like a place."
                        ),
                        prompt=basis.rendered_context,
                        user_id=101,
                        guild_id=1,
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="sealed_test",
                        current_user_text=(
                            "Between Chrome Prophet and Null Basilica, which "
                            "fits that requirement better, and why?"
                        ),
                        channel=SimpleNamespace(id=303),
                        prompt_source_bases=(basis,),
                    )
                )

        self.assertIn("Null Basilica fits better", response)
        self.assertTrue(diagnostics["unified_moment_canary_applied"])
        self.assertTrue(diagnostics["unified_moment_canary_scope_valid"])
        self.assertTrue(
            diagnostics[
                "unified_moment_canary_coherence_guard_triggered"
            ]
        )
        self.assertTrue(
            diagnostics["unified_moment_canary_coherence_regenerated"]
        )
        self.assertEqual(
            diagnostics["unified_moment_canary_coherence_status"],
            "passed",
        )
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        self.assertIn(
            "CANARY COHERENCE CORRECTION REQUIRED",
            provider.await_args.args[1],
        )

    async def test_output_leak_guard_repairs_internal_narration(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            provider = mock.AsyncMock(
                return_value=(
                    "Null Basilica fits better because it reads as a place "
                    "instead of a person."
                )
            )
            with mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                provider,
            ), mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                response, diagnostics = (
                    await bnl01_bot.apply_guarded_response_regeneration(
                        (
                            "The unified response assessment says Null "
                            "Basilica fits better."
                        ),
                        prompt=basis.rendered_context,
                        user_id=101,
                        guild_id=1,
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="sealed_test",
                        current_user_text=(
                            "Between Chrome Prophet and Null Basilica, which "
                            "fits that requirement better, and why?"
                        ),
                        channel=SimpleNamespace(id=303),
                        prompt_source_bases=(basis,),
                    )
                )

        self.assertNotIn("unified response assessment", response.lower())
        self.assertTrue(
            diagnostics[
                "unified_moment_canary_output_leak_guard_triggered"
            ]
        )
        self.assertTrue(
            diagnostics["unified_moment_canary_output_leak_regenerated"]
        )
        self.assertFalse(diagnostics["suppressed"])

    async def test_guard_fails_closed_if_basis_crosses_channel_scope(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
            with mock.patch.object(bnl01_bot, "DB_FILE", ":memory:"):
                response, diagnostics = (
                    await bnl01_bot.apply_guarded_response_regeneration(
                        "Null Basilica fits the place criterion.",
                        prompt=basis.rendered_context,
                        user_id=101,
                        guild_id=1,
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="sealed_test",
                        current_user_text="Which option fits?",
                        channel=SimpleNamespace(id=304),
                        prompt_source_bases=(basis,),
                    )
                )

        self.assertEqual(response, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertFalse(
            diagnostics["unified_moment_canary_scope_valid"]
        )
        self.assertEqual(
            diagnostics["suppression_reason"],
            "unified_moment_canary_scope_invalid",
        )

    def test_kill_switch_invalidates_existing_canary_basis(self):
        with mock.patch.dict(os.environ, CANARY_ENV, clear=False):
            basis = self.build_basis()
        disabled = {**CANARY_ENV, "BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false"}
        with mock.patch.dict(os.environ, disabled, clear=False), mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            ":memory:",
        ):
            self.assertEqual(
                bnl01_bot.prompt_source_basis_failure((basis,)),
                "unified_moment_canary_source_changed",
            )


class EpisodeCanarySourceBindingTests(unittest.TestCase):
    @contextmanager
    def source_fixture(self):
        source = episode_fixtures.MomentEpisodeLifecycleV2Tests()
        source.setUp()
        self.addCleanup(source.tearDown)
        messages = (
            "Let's build the synth routing for the chorus",
            "The synth drum patch needs a bass answer",
            "Which synth layer should we test next?",
        )
        moment_id, roots = source.finalize_shared_moment(
            95, messages, users=(1, 2, 3), policy="sealed_test",
        )
        source.conn.commit()
        episode_id = source.conn.execute(
            "SELECT episode_id FROM memory_moment_episodes"
        ).fetchone()[0]
        request = "Which open loops remain in this synth routing episode?"
        settings = {**CANARY_ENV, "BNL_UNIFIED_MOMENT_CANARY_CHANNEL_IDS": "10"}
        with tempfile.NamedTemporaryFile(suffix=".db") as file:
            with sqlite3.connect(file.name) as disk:
                source.conn.backup(disk)
            with mock.patch.dict(os.environ, settings), mock.patch.object(
                bnl01_bot, "DB_FILE", file.name,
            ), mock.patch.object(
                moments, "_now", return_value=source.timestamp(minutes=14),
            ):
                assessment = bnl01_bot.build_unified_response_assessment(
                    guild_id=1, route_mode="normal_chat", channel_policy="sealed_test",
                    conversation_surface="free_speak_sealed_mirror",
                    current_speaker_user_ids=(1,), participant_user_ids=(1, 2, 3),
                    speaker_labels=("Test Member", "Second Member", "Third Member"),
                    current_text=request, active_episode_id=episode_id,
                    prompt_lanes=("current_exchange",),
                )
                basis = bnl01_bot.build_unified_moment_canary_prompt_source_basis(
                    assessment, guild_id=1, channel_id=10, channel_policy="sealed_test",
                    route_mode="normal_chat", topic_text=request,
                    participant_user_ids=(1, 2, 3),
                )
                self.assertTrue(basis.episode_context_present)
                yield file.name, basis, source, moment_id, roots, messages

    def test_current_episode_reference_is_stable_and_content_free(self):
        with self.source_fixture() as (_path, basis, _source, moment_id, _roots, _messages):
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertFalse(changed)
            self.assertEqual(fresh.episode_reference.source_moment_ids, (moment_id,))
            self.assertEqual(fresh.expected_episode_id, fresh.episode_reference.episode_id)
            self.assertEqual(fresh.assessment.active_episode_id, fresh.expected_episode_id)
            self.assertNotIn(moment_id, fresh.rendered_context)
            self.assertNotIn(fresh.expected_episode_id, fresh.rendered_context)

    def test_same_prose_replacement_episode_cannot_rebind_current_turn(self):
        with self.source_fixture() as (path, basis, _source, _moment_id, _roots, _messages):
            with sqlite3.connect(path) as conn:
                for table in (
                    "memory_moment_episodes", "memory_moment_episode_moments",
                    "memory_moment_episode_participants", "memory_moment_episode_events",
                ):
                    conn.execute(
                        "UPDATE " + table + " SET episode_id=? WHERE episode_id=?",
                        ("mep_replacement", basis.expected_episode_id),
                    )
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertFalse(fresh.episode_context_present)
            self.assertIsNone(fresh.episode_reference)
            self.assertEqual(fresh.assessment.active_episode_id, "")
            self.assertEqual(fresh.expected_episode_id, basis.expected_episode_id)
            again, changed_again = bnl01_bot.refresh_prompt_source_basis(fresh)
            self.assertFalse(changed_again)
            self.assertFalse(again.episode_context_present)
            rebuilt = bnl01_bot.build_unified_moment_canary_prompt_source_basis(
                basis.assessment, guild_id=1, channel_id=10,
                channel_policy="sealed_test", route_mode="normal_chat",
                topic_text=basis.topic_text, participant_user_ids=(1, 2, 3),
            )
            self.assertFalse(rebuilt.episode_context_present)
            self.assertEqual(rebuilt.expected_episode_id, basis.expected_episode_id)
            rebuilt_again, changed_again = bnl01_bot.refresh_prompt_source_basis(rebuilt)
            self.assertFalse(changed_again)
            self.assertFalse(rebuilt_again.episode_context_present)

    def test_same_prose_different_source_moment_is_a_source_change(self):
        with self.source_fixture() as (path, basis, source, moment_id, _roots, messages):
            replacement_moment, _ = source.finalize_shared_moment(
                195, messages, minutes=10, users=(1, 2, 3), policy="sealed_test",
            )
            source.conn.execute(
                "DELETE FROM memory_moment_episode_moments WHERE moment_id=?",
                (moment_id,),
            )
            self.assertTrue(moments._rebuild_episode_projection(
                source.conn, basis.expected_episode_id,
            ))
            source.conn.commit()
            with sqlite3.connect(path) as disk:
                source.conn.backup(disk)
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(fresh.episode_context_present)
            self.assertEqual(fresh.rendered_context, basis.rendered_context)
            self.assertEqual(fresh.episode_reference.source_moment_ids, (replacement_moment,))
            self.assertTrue(changed)

    def test_source_retraction_removes_episode_from_current_basis(self):
        with self.source_fixture() as (path, basis, _source, _moment_id, roots, _messages):
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?",
                    (roots[0].entry_id,),
                )
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertFalse(fresh.episode_context_present)
            self.assertEqual(fresh.expected_episode_id, basis.expected_episode_id)

    def test_source_correction_is_detected_before_episode_projection_sweep(self):
        with self.source_fixture() as (path, basis, _source, _moment_id, roots, _messages):
            with sqlite3.connect(path) as conn:
                ledger.insert_ledger_entry(
                    conn,
                    ledger.LedgerEntry(
                        guild_id=1, source_table="member_memory_controls",
                        source_row_id="source-correction", source_revision="1",
                        source_role="member_control", entry_type="boundary",
                        subject_key="discord_user:1", subject_display_name="Test Member",
                        predicate_key="source_correction",
                        value="The earlier source was corrected by its author.",
                        source_class=ledger.SourceClass.FIRST_PARTY_RECORD,
                        route_mode="normal_chat", channel_id=10,
                        channel_name="bnl-testing", channel_policy="sealed_test",
                        visibility=ledger.Visibility.SEALED_TEST,
                        confidence=ledger.Confidence.HIGH,
                        observed_at="2026-01-01T00:05:00+00:00",
                        source_sequence=999,
                        lineage=(("correction_of", roots[0].entry_id),),
                    ),
                )
                self.assertEqual(conn.execute(
                    "SELECT lifecycle_status FROM memory_moment_episodes WHERE episode_id=?",
                    (basis.expected_episode_id,),
                ).fetchone()[0], "active")
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertFalse(fresh.episode_context_present)

    def test_deleted_source_moment_invalidates_the_existing_basis(self):
        with self.source_fixture() as (path, basis, _source, moment_id, _roots, _messages):
            with sqlite3.connect(path) as conn:
                conn.execute("DELETE FROM memory_moment_windows WHERE moment_id=?", (moment_id,))
            fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertFalse(fresh.episode_context_present)

    def test_same_episode_reopen_keeps_identity_and_refreshes_source_membership(self):
        with self.source_fixture() as (path, basis, source, moment_id, _roots, _messages):
            moments.sweep_expired_episodes(source.conn, now=source.timestamp(hours=25))
            resumed, _ = source.finalize_shared_moment(
                295,
                ("Let's return to the synth routing and chorus",
                 "The synth drum patch still follows the bass",
                 "We should continue the synth layer test"),
                hours=48, users=(1, 2, 3), policy="sealed_test",
            )
            source.conn.commit()
            with sqlite3.connect(path) as disk:
                source.conn.backup(disk)
            with mock.patch.object(moments, "_now", return_value=source.timestamp(hours=48, minutes=4)):
                fresh, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertTrue(fresh.episode_context_present)
            self.assertEqual(fresh.episode_reference.episode_id, basis.expected_episode_id)
            self.assertEqual(set(fresh.episode_reference.source_moment_ids), {moment_id, resumed})

    def test_finalized_or_expired_episode_is_removed_from_current_basis(self):
        with self.source_fixture() as (path, basis, source, _moment_id, _roots, _messages):
            with mock.patch.object(moments, "_now", return_value=source.timestamp(hours=25)):
                stale, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertFalse(stale.episode_context_present)
            with sqlite3.connect(path) as conn:
                conn.execute("UPDATE memory_moment_episodes SET lifecycle_status='finalized'")
            finalized, changed = bnl01_bot.refresh_prompt_source_basis(basis)
            self.assertTrue(changed)
            self.assertFalse(finalized.episode_context_present)


class EpisodeOrdinaryProjectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.public = public_fixtures.PublicNetworkKnowledgeTests()
        await self.public.asyncSetUp()
        self.request = "BNL, how many human participants are in the synth routing discussion?"

    async def asyncTearDown(self):
        await self.public.asyncTearDown()

    def flags(self, channel_id):
        return {
            **CANARY_ENV,
            "BNL_UNIFIED_MOMENT_CANARY_GUILD_IDS": str(self.public.guild_id),
            "BNL_UNIFIED_MOMENT_CANARY_CHANNEL_IDS": str(channel_id),
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": str(self.public.guild_id),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": str(self.public.user_id),
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": str(channel_id),
        }

    def seed_episode(self, channel_id):
        now = bnl01_bot.datetime.now(bnl01_bot.timezone.utc)
        messages = (
            "Let's build the synth routing for the chorus",
            "The synth drum patch needs a bass answer",
            "Which synth layer should we test next?",
        )
        roots = []
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            for index, text in enumerate(messages):
                entry = ledger.shadow_conversation_row(
                    conn, row_id=900001 + index, user_id=100 + index,
                    user_name="Test Member %s" % index,
                    guild_id=self.public.guild_id, role="user", content=text,
                    channel_policy="sealed_test", channel_id=channel_id,
                    channel_name="bnl-testing", route_mode="normal_chat",
                    observed_at=(now - bnl01_bot.timedelta(minutes=5)).isoformat(),
                )
                roots.append(entry.entry_id)
                moments.observe_ledger_entry(conn, entry.entry_id)
            moments.sweep_expired_windows(conn, now=now.isoformat())
        return tuple(roots)

    def aggregate_basis(self, metadata):
        return next(
            basis for basis in metadata["prompt_source_bases"]
            if isinstance(basis, bnl01_bot.UnifiedMomentCanaryPromptSourceBasis)
        )

    async def test_direct_packet_prompt_keeps_only_validated_episode_aggregate(self):
        with mock.patch.dict(os.environ, self.flags(8810)):
            self.seed_episode(8810)
            prompt, metadata = self.public._direct_prompt("sealed_test", self.request)
            owned = bnl01_bot.build_packet_owned_prompt(
                prompt, metadata["ordinary_chat_single_packet_basis"],
            )
            basis = self.aggregate_basis(metadata)
            self.assertTrue(owned.ready)
            self.assertTrue(basis.aggregate_only)
            self.assertIsNotNone(basis.episode_reference)
            self.assertIn(basis.rendered_context, owned.prompt)
            self.assertIn("Shared human participants: 3", owned.prompt)
            self.assertNotIn("SEALED UNIFIED CONVERSATION CANARY", owned.prompt)
            self.assertNotIn(basis.episode_reference.episode_id, owned.prompt)
            for moment_id in basis.episode_reference.source_moment_ids:
                self.assertNotIn(moment_id, owned.prompt)

    async def test_default_off_wrong_channel_and_public_routes_do_not_gain_aggregate(self):
        with mock.patch.dict(os.environ, self.flags(8810)):
            self.seed_episode(8810)
            for overrides, policy in (
                ({"BNL_UNIFIED_MOMENT_CANARY_ENABLED": "false"}, "sealed_test"),
                ({"BNL_UNIFIED_MOMENT_CANARY_CHANNEL_IDS": "99999"}, "sealed_test"),
                ({}, "public_home"),
            ):
                with self.subTest(policy=policy, overrides=overrides):
                    with mock.patch.dict(os.environ, overrides):
                        prompt, metadata = self.public._direct_prompt(policy, self.request)
                    self.assertNotIn("Active same-channel episode signal", prompt)
                    self.assertFalse(any(
                        isinstance(basis, bnl01_bot.UnifiedMomentCanaryPromptSourceBasis)
                        for basis in metadata["prompt_source_bases"]
                    ))

    async def tracked_batch(self, **kwargs):
        async def tracked(_channel, prompt, user_id, guild_id, **_kwargs):
            text = await bnl01_bot.get_gemini_response(prompt, user_id, guild_id)
            return bnl01_bot.TrackedGenerationResponse(text, 1)
        with mock.patch.object(
            bnl01_bot, "get_tracked_gemini_response_with_optional_typing", side_effect=tracked,
        ):
            return await self.public._batch("sealed_test", **kwargs)

    async def test_single_speaker_batch_delivers_aggregate_without_canary_output_review(self):
        with mock.patch.dict(os.environ, self.flags(8811)):
            self.seed_episode(8811)
            with mock.patch.object(
                bnl01_bot, "assess_response_coherence",
                side_effect=AssertionError("Aggregate projection must not add canary output review"),
            ):
                channel, generation, guard = await self.tracked_batch(
                    request=self.request,
                    answer="There are three human participants in the synth routing discussion.", privileged=False,
                )
            self.assertTrue(channel.sent)
            self.assertEqual(generation.await_count, 1)
            prompt = generation.await_args.args[0]
            self.assertIn("PACKET-OWNED RESPONSE CONTRACT", prompt)
            self.assertIn("Active same-channel episode signal", prompt)
            self.assertNotIn("SEALED UNIFIED CONVERSATION CANARY", prompt)
            self.assertTrue(any(
                isinstance(basis, bnl01_bot.UnifiedMomentCanaryPromptSourceBasis)
                and basis.aggregate_only and basis.episode_reference is not None
                for basis in guard.await_args.kwargs["prompt_source_bases"]
            ))

    async def test_ordinary_source_retraction_removes_aggregate_before_delivery(self):
        with mock.patch.dict(os.environ, self.flags(8811)):
            roots = self.seed_episode(8811)
            turns = []
            def provider(prompt, *_args, **_kwargs):
                turns.append(prompt)
                if len(turns) == 1:
                    self.assertIn("Active same-channel episode signal", prompt)
                    self.assertIn(public_fixtures.PUBLIC_MEMORY, prompt)
                    with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                        conn.execute(
                            "UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?",
                            (roots[0],),
                        )
                    return "There are three human participants in the synth routing discussion."
                self.assertNotIn("Active same-channel episode signal", prompt)
                self.assertIn(public_fixtures.PUBLIC_MEMORY, prompt)
                return "I cannot verify that count from the context available."
            channel, generation, _guard = await self.tracked_batch(
                request=self.request, answer=provider, privileged=False,
            )
            self.assertEqual(generation.await_count, 2)
            self.assertEqual(len(channel.sent), 1)
            self.assertIn("I cannot verify that count", str(channel.sent[0]))


if __name__ == "__main__":
    unittest.main()
