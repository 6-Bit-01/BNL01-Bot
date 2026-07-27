import os
import sqlite3
import unittest
from dataclasses import replace
from unittest import mock

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shadow_acceptance import (
    build_v2_shadow_acceptance_snapshot,
    render_v2_shadow_acceptance_lines,
)
from bnl_shared_brain_synthesis import (
    begin_run,
    build_basis,
    build_evaluation_report,
    build_packet_owned_prompt,
    configuration,
    ensure_schema,
    evaluate_candidate,
    finalize_run,
    render_packet_context,
    revalidate_basis,
    scope_enabled,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    build_evaluation_report as build_packet_report,
    build_packet,
    mark_packet_application,
)
from bnl_unified_response_assessment import (
    build_unified_response_assessment,
)


class SharedBrainSynthesisCanaryTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_GUILD_IDS": "1",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_USER_IDS": "7",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_CHANNEL_IDS": "10",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }
        self.env = mock.patch.dict(os.environ, self.flags, clear=False)
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)
        self.conn.execute(
            """
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY,
                guild_id INTEGER,
                user_id INTEGER,
                user_name TEXT,
                role TEXT,
                content TEXT,
                channel_id INTEGER,
                channel_policy TEXT,
                timestamp TEXT
            )
            """
        )
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_policy,timestamp
            ) VALUES(900,1,7,'Crow','user',?,10,'public_context',?)
            """,
            (
                "I keep connecting modular synths to the archive project.",
                "2026-07-25T12:00:00+00:00",
            ),
        )
        result = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=901,
                source_revision="901",
                source_role="member_self_report",
                entry_type="preference",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="favorite_movie",
                value="Arrival",
                source_class=SourceClass.FIRST_PARTY_RECORD,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="public_context",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.HIGH,
                public_usable=True,
                observed_at="2026-07-25T12:00:01+00:00",
                source_sequence=901,
                lifecycle_status="active",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Crow",
                        "author",
                        0,
                    ),
                ),
            ),
        )
        self.fact_entry_id = result.entry_id
        self.packet = self._build_packet()
        self.assessment = self._build_assessment(self.packet)
        self.basis = self._build_basis(self.packet, self.assessment)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def _add_profile_fact(
        self,
        *,
        source_row_id,
        predicate_key,
        value,
        observed_at,
    ):
        result = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=source_row_id,
                source_revision=(
                    f"{source_row_id}:{predicate_key}"
                ),
                source_role="member_self_report",
                entry_type="preference",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key=predicate_key,
                value=value,
                source_class=SourceClass.FIRST_PARTY_RECORD,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="public_context",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.HIGH,
                public_usable=True,
                observed_at=observed_at,
                source_sequence=int(source_row_id),
                lifecycle_status="active",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Crow",
                        "author",
                        0,
                    ),
                ),
            ),
        )
        self.assertTrue(result.entry_id)
        return result.entry_id

    def _request(self):
        text = "BNL-01, what am I all about?"
        return IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=7,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_id=10,
            channel_name="bnl-testing",
            channel_policy="public_context",
            visibility_allowance="public_safe",
            user_text=text,
            participant_user_ids=(7,),
            direct_state="direct",
            budget_chars=5000,
            conversation_evidence=(
                PacketConversationEvidence(
                    text=(
                        "I keep connecting modular synths to the "
                        "archive project."
                    ),
                    source_id=900,
                    speaker_user_id=7,
                    speaker_label="Crow",
                ),
                PacketConversationEvidence(
                    text=text,
                    speaker_user_id=7,
                    speaker_label="Crow",
                    current_turn=True,
                ),
            ),
            now="2026-07-25T12:01:00+00:00",
        )

    def _build_packet(self):
        return build_packet(
            self.conn,
            self._request(),
            persist=True,
            environ=self.flags,
        )

    def _build_assessment(self, packet):
        profile = packet.profile_sufficiency
        return build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_context",
            conversation_surface="mention_or_reply",
            current_speaker_user_ids=(7,),
            participant_user_ids=(7,),
            speaker_labels=("Crow",),
            current_exchange_source_ids=(900,),
            governed_entry_ids=packet.governed_refs,
            canon_refs=packet.canon_refs,
            prompt_lanes=("current_exchange", "conversation_context"),
            current_text=packet.request.user_text,
            packet_selected_lanes=packet.assessment_lanes,
            packet_excluded_lanes=packet.assessment_exclusions,
            packet_conflict_reasons=packet.diagnostics.conflict_reasons,
            packet_missing_lanes=packet.assessment_missing_lanes,
            packet_revalidation_status=(
                packet.diagnostics.revalidation_status
            ),
            profile_sufficiency_status=profile.status,
            profile_sufficiency_met=profile.satisfied,
            profile_required_point_count=profile.required_point_count,
            profile_selected_point_count=profile.selected_point_count,
            profile_independent_root_count=profile.independent_root_count,
            profile_independent_occurrence_count=(
                profile.independent_occurrence_count
            ),
            profile_sufficiency_reasons=profile.reason_codes,
        )

    def _build_basis(self, packet, assessment):
        basis = build_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=packet.request.user_text,
            packet=packet,
            assessment=assessment,
            environ=self.flags,
        )
        self.assertIsNotNone(basis)
        return basis

    def test_configuration_requires_bounded_scope_and_no_global_live_gate(self):
        configured = configuration(self.flags)
        self.assertTrue(configured["effective"])
        self.assertEqual(configured["reason"], "scoped_canary")
        self.assertEqual(configured["guild_allowlist_count"], 1)
        self.assertEqual(configured["user_allowlist_count"], 1)
        self.assertEqual(configured["channel_allowlist_count"], 1)

        bounded_expansion = configuration(
            {
                **self.flags,
                "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_CHANNEL_IDS": "10,11",
            }
        )
        self.assertTrue(bounded_expansion["effective"])
        self.assertEqual(
            bounded_expansion["channel_allowlist_count"],
            2,
        )

        incomplete = configuration(
            {
                **self.flags,
                "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_USER_IDS": "",
            }
        )
        self.assertFalse(incomplete["effective"])
        self.assertEqual(incomplete["reason"], "scope_incomplete")

        global_live = configuration(
            {
                **self.flags,
                "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "true",
            }
        )
        self.assertFalse(global_live["effective"])
        self.assertEqual(
            global_live["reason"],
            "global_live_authority_detected",
        )

    def test_scope_excludes_wrong_route_surface_media_and_attribution(self):
        common = {
            "guild_id": 1,
            "user_id": 7,
            "channel_id": 10,
            "route_mode": "normal_chat",
            "channel_policy": "public_context",
            "current_direct": True,
            "user_text": "What am I all about?",
            "packet": self.packet,
            "assessment": self.assessment,
            "environ": self.flags,
        }
        self.assertTrue(scope_enabled(**common))
        for override in (
            {"guild_id": 2},
            {"user_id": 8},
            {"channel_id": 11},
            {"route_mode": "direct_payload"},
            {"channel_policy": "public_home"},
            {"current_direct": False},
            {"user_text": "Tell me a joke."},
            {
                "user_text": (
                    "What am I all about, and tell me a joke?"
                )
            },
            {"has_media": True},
            {"exact_quote_requested": True},
            {"third_party_attribution_requested": True},
        ):
            self.assertFalse(scope_enabled(**{**common, **override}))

    def test_renderer_excludes_current_intent_and_relationship_posture(self):
        (
            rendered,
            lane_counts,
            item_count,
            _source_digests,
        ) = render_packet_context(self.packet)
        self.assertGreater(item_count, 0)
        self.assertIn("modular synths", rendered)
        self.assertNotIn("what am I all about", rendered)
        self.assertNotIn("relationship posture", rendered.lower())
        self.assertNotIn("source_ref", rendered)
        self.assertNotIn("conversation:900", rendered)
        self.assertNotIn("current_intent", dict(lane_counts))
        self.assertNotIn("relationship_posture", dict(lane_counts))
        self.assertNotIn("approved canon", rendered)

    def test_renderer_allocates_concrete_examples_across_points(self):
        base = self.packet.items[0]
        first = replace(
            base,
            lane="atomic_knowledge",
            source_digest="adaptive-first",
            source_ref="atomic:first",
            text="Recurring public conversation about music.",
            point_identity="point:first",
            supporting_observations=tuple(
                "Music example %s has a distinct concrete detail." % index
                for index in range(1, 6)
            ),
        )
        second = replace(
            base,
            lane="atomic_knowledge",
            source_digest="adaptive-second",
            source_ref="atomic:second",
            text="Recurring public conversation about visual art.",
            point_identity="point:second",
            supporting_observations=tuple(
                "Art example %s has a distinct concrete detail." % index
                for index in range(1, 6)
            ),
        )
        packet = replace(self.packet, items=(first, second))

        rendered, _counts, _count, _digests = render_packet_context(packet)

        for index in range(1, 5):
            self.assertIn("Music example %s" % index, rendered)
            self.assertIn("Art example %s" % index, rendered)
        self.assertNotIn("Music example 5", rendered)
        self.assertNotIn("Art example 5", rendered)

    def test_packet_owned_prompt_replaces_only_the_competing_factual_block(self):
        legacy_context = (
            "Relationship state: stage=familiar, stance=warm.\n"
            "Observed habits: messages=100, last_topic=music.\n"
            "Derived memory summaries: one legacy summary."
        )
        basis = build_basis(
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode="normal_chat",
            channel_policy="public_context",
            current_direct=True,
            user_text=self.packet.request.user_text,
            packet=self.packet,
            assessment=self.assessment,
            competing_factual_contexts=(legacy_context,),
            environ=self.flags,
        )
        self.assertIsNotNone(basis)
        baseline_prompt = (
            "Current user request: BNL-01, what am I all about?\n"
            "BNL persona and BARCODE lore remain active.\n"
            "Recent room context: modular synths.\n"
            "Durable memory context:\n"
            + legacy_context
            + "\nPersonal-recall route contract remains active."
        )
        result = build_packet_owned_prompt(baseline_prompt, basis)
        self.assertTrue(result.ready)
        self.assertEqual(result.replaced_factual_context_count, 1)
        self.assertNotIn("Relationship state:", result.prompt)
        self.assertNotIn("Observed habits:", result.prompt)
        self.assertNotIn("Derived memory summaries:", result.prompt)
        self.assertIn("Current user request:", result.prompt)
        self.assertIn("BNL persona and BARCODE lore", result.prompt)
        self.assertIn("Recent room context:", result.prompt)
        self.assertIn("Personal-recall route contract", result.prompt)
        self.assertEqual(result.prompt.count(basis.rendered_context), 1)

    def test_nonpacket_factual_owner_records_a_fail_closed_fallback(self):
        for lane in (
            "broadcast_memory",
            "show_state",
            "source_context",
            "website_read_model",
        ):
            with self.subTest(lane=lane):
                assessment = replace(
                    self.assessment,
                    selected_lanes=(
                        *self.assessment.selected_lanes,
                        lane,
                    ),
                )
                basis = build_basis(
                    guild_id=1,
                    user_id=7,
                    channel_id=10,
                    route_mode="normal_chat",
                    channel_policy="public_context",
                    current_direct=True,
                    user_text=self.packet.request.user_text,
                    packet=self.packet,
                    assessment=assessment,
                    environ=self.flags,
                )
                self.assertIsNotNone(basis)
                self.assertEqual(
                    basis.blocking_factual_owner_lanes,
                    (lane,),
                )
                owned = build_packet_owned_prompt(
                    "Current user request: What am I all about?",
                    basis,
                )
                self.assertFalse(owned.ready)
                self.assertEqual(
                    owned.reason,
                    "nonpacket_factual_owner_selected",
                )
                run = begin_run(
                    self.conn,
                    basis,
                    baseline_response="Established path response.",
                    candidate_prompt_ready=owned.ready,
                    candidate_prompt_failure_reason=owned.reason,
                    environ=self.flags,
                )
                self.assertFalse(run.prompt_applied)
                self.assertEqual(
                    run.fallback_reason,
                    (
                        "candidate_prompt_"
                        "nonpacket_factual_owner_selected"
                    ),
                )

    def test_empty_or_canon_only_profile_cannot_enter_synthesis(self):
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='deleted'
            WHERE entry_id=?
            """,
            (self.fact_entry_id,),
        )
        packet = self._build_packet()
        self.assertEqual(packet.profile_sufficiency.status, "empty")
        self.assertFalse(packet.profile_sufficiency.satisfied)
        assessment = self._build_assessment(packet)
        self.assertIsNone(
            build_basis(
                guild_id=1,
                user_id=7,
                channel_id=10,
                route_mode="normal_chat",
                channel_policy="public_context",
                current_direct=True,
                user_text=packet.request.user_text,
                packet=packet,
                assessment=assessment,
                environ=self.flags,
            )
        )

    def test_malformed_profile_status_count_pairs_cannot_enter_synthesis(self):
        def basis_for(packet, assessment):
            return build_basis(
                guild_id=1,
                user_id=7,
                channel_id=10,
                route_mode="normal_chat",
                channel_policy="public_context",
                current_direct=True,
                user_text=packet.request.user_text,
                packet=packet,
                assessment=assessment,
                environ=self.flags,
            )

        malformed_rich_profile = replace(
            self.packet.profile_sufficiency,
            status="rich",
            satisfied=True,
            required_point_count=1,
            selected_point_count=2,
            independent_root_count=2,
            independent_occurrence_count=2,
        )
        malformed_rich_packet = replace(
            self.packet,
            profile_sufficiency=malformed_rich_profile,
        )
        malformed_rich_assessment = replace(
            self.assessment,
            profile_sufficiency_status="rich",
            profile_sufficiency_met=True,
            profile_required_point_count=1,
            profile_selected_point_count=2,
            profile_independent_root_count=2,
            profile_independent_occurrence_count=2,
        )
        malformed_sparse_profile = replace(
            self.packet.profile_sufficiency,
            status="sparse",
            satisfied=True,
            required_point_count=2,
            selected_point_count=2,
            independent_root_count=2,
            independent_occurrence_count=2,
        )
        malformed_sparse_packet = replace(
            self.packet,
            profile_sufficiency=malformed_sparse_profile,
        )
        malformed_sparse_assessment = replace(
            self.assessment,
            profile_sufficiency_status="sparse",
            profile_sufficiency_met=True,
            profile_required_point_count=2,
            profile_selected_point_count=2,
            profile_independent_root_count=2,
            profile_independent_occurrence_count=2,
        )

        self.assertIsNone(
            basis_for(
                malformed_rich_packet,
                malformed_rich_assessment,
            )
        )
        self.assertIsNone(
            basis_for(
                malformed_sparse_packet,
                malformed_sparse_assessment,
            )
        )

    def test_rich_profile_separates_grounded_opinion_from_unsupported_claims(
        self,
    ):
        second = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=902,
                source_revision="902",
                source_role="member_self_report",
                entry_type="preference",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="favorite_color",
                value="violet",
                source_class=SourceClass.FIRST_PARTY_RECORD,
                route_mode="normal_chat",
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="public_context",
                visibility=Visibility.PUBLIC,
                confidence=Confidence.HIGH,
                public_usable=True,
                observed_at="2026-07-26T12:00:01+00:00",
                source_sequence=902,
                lifecycle_status="active",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Crow",
                        "author",
                        0,
                    ),
                ),
            ),
        )
        self.assertTrue(second.entry_id)
        packet = self._build_packet()
        self.assertEqual(packet.profile_sufficiency.status, "rich")
        assessment = self._build_assessment(packet)
        basis = self._build_basis(packet, assessment)

        one_point_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        one_point = evaluate_candidate(
            self.conn,
            one_point_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response="Your favorite movie is Arrival.",
            environ=self.flags,
        )
        self.assertFalse(one_point.candidate_selected)
        self.assertEqual(
            one_point.fallback_reason,
            "candidate_member_points_insufficient",
        )

        lore_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        lore_first = evaluate_candidate(
            self.conn,
            lore_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "The liaison entity is an unfinished active intelligence. "
                "Your favorite movie is Arrival and your favorite color is "
                "violet."
            ),
            environ=self.flags,
        )
        self.assertFalse(lore_first.candidate_selected)
        self.assertFalse(lore_first.candidate_lore_dominant)
        self.assertEqual(
            lore_first.candidate_unsupported_factual_claim_count,
            1,
        )
        self.assertEqual(
            lore_first.fallback_reason,
            "candidate_claims_ungrounded",
        )

        grounded_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        grounded = evaluate_candidate(
            self.conn,
            grounded_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival, and your favorite color is "
                "violet."
            ),
            environ=self.flags,
        )
        self.assertTrue(grounded.candidate_selected)
        self.assertEqual(
            grounded.candidate_member_point_coverage_count,
            2,
        )
        self.assertGreaterEqual(
            grounded.candidate_member_root_coverage_count,
            2,
        )
        self.assertGreaterEqual(
            grounded.candidate_member_occurrence_coverage_count,
            2,
        )

        lore_after_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        lore_after = evaluate_candidate(
            self.conn,
            lore_after_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival and your favorite color is "
                "violet. The chrome signal drifts beneath midnight towers. "
                "A masked liaison tends the nocturnal broadcast. Static "
                "gathers around the cathedral antenna."
            ),
            environ=self.flags,
        )
        self.assertFalse(lore_after.candidate_selected)
        self.assertFalse(lore_after.candidate_lore_dominant)
        self.assertGreaterEqual(
            lore_after.candidate_unsupported_factual_claim_count,
            3,
        )
        self.assertEqual(
            lore_after.fallback_reason,
            "candidate_claims_ungrounded",
        )

        concise_support_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        concise_support = evaluate_candidate(
            self.conn,
            concise_support_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival and your favorite color is "
                "violet. Those preferences make a vivid pairing."
            ),
            environ=self.flags,
        )
        self.assertTrue(concise_support.candidate_selected)

        opinion_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        opinion = evaluate_candidate(
            self.conn,
            opinion_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival and your favorite color is "
                "violet. My read is that those choices give you a vivid, "
                "slightly off-center creative signal. That is the part of "
                "your frequency I recognize."
            ),
            environ=self.flags,
        )
        self.assertTrue(opinion.candidate_selected)
        self.assertEqual(opinion.candidate_opinion_claim_count, 2)
        self.assertEqual(
            opinion.candidate_unsupported_factual_claim_count,
            0,
        )

        disguised_fact_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        disguised_fact = evaluate_candidate(
            self.conn,
            disguised_fact_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival and your favorite color is "
                "violet. My read is that your favorite food is pizza."
            ),
            environ=self.flags,
        )
        self.assertFalse(disguised_fact.candidate_selected)
        self.assertEqual(
            disguised_fact.fallback_reason,
            "candidate_claims_ungrounded",
        )

        mixed_claim_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        mixed_claim = evaluate_candidate(
            self.conn,
            mixed_claim_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival, and you secretly run a "
                "lunar casino. Your favorite color is violet."
            ),
            environ=self.flags,
        )
        self.assertFalse(mixed_claim.candidate_selected)
        self.assertEqual(
            mixed_claim.fallback_reason,
            "candidate_claims_ungrounded",
        )

        subjectless_mixed_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        subjectless_mixed = evaluate_candidate(
            self.conn,
            subjectless_mixed_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival and secretly run a lunar "
                "casino. Your favorite color is violet."
            ),
            environ=self.flags,
        )
        self.assertFalse(subjectless_mixed.candidate_selected)
        self.assertEqual(
            subjectless_mixed.fallback_reason,
            "candidate_claims_ungrounded",
        )

        attached_claim_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        attached_claim = evaluate_candidate(
            self.conn,
            attached_claim_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival because you were born on "
                "Mars. Your favorite color is violet."
            ),
            environ=self.flags,
        )
        self.assertFalse(attached_claim.candidate_selected)
        self.assertEqual(
            attached_claim.fallback_reason,
            "candidate_claims_ungrounded",
        )

    def test_generic_overlap_cannot_claim_two_distinct_member_points(self):
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='deleted',
                observed_at='2026-07-25T08:00:01+00:00'
            WHERE entry_id=?
            """,
            (self.fact_entry_id,),
        )
        self._add_profile_fact(
            source_row_id=902,
            predicate_key="favorite_movie",
            value="bot code systems",
            observed_at="2026-07-25T09:00:02+00:00",
        )
        self._add_profile_fact(
            source_row_id=903,
            predicate_key="favorite_color",
            value="website code systems",
            observed_at="2026-07-25T10:00:03+00:00",
        )
        packet = self._build_packet()
        self.assertEqual(packet.profile_sufficiency.status, "rich")
        basis = self._build_basis(
            packet,
            self._build_assessment(packet),
        )

        generic_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep building connected systems.",
            environ=self.flags,
        )
        generic = evaluate_candidate(
            self.conn,
            generic_run,
            baseline_response="You keep building connected systems.",
            candidate_response="You keep returning to code systems.",
            environ=self.flags,
        )
        self.assertFalse(generic.candidate_selected)
        self.assertEqual(
            generic.fallback_reason,
            "candidate_member_points_insufficient",
        )
        self.assertLess(
            generic.candidate_member_point_coverage_count,
            2,
        )

        anchored_run = begin_run(
            self.conn,
            basis,
            baseline_response="You keep building connected systems.",
            environ=self.flags,
        )
        anchored = evaluate_candidate(
            self.conn,
            anchored_run,
            baseline_response="You keep building connected systems.",
            candidate_response=(
                "You keep returning to bot code systems and website code "
                "systems."
            ),
            environ=self.flags,
        )
        self.assertTrue(anchored.candidate_selected)
        self.assertEqual(
            anchored.candidate_member_point_coverage_count,
            2,
        )

    def test_sparse_profile_rejects_candidate_that_claims_two_points(self):
        self._add_profile_fact(
            source_row_id=901,
            predicate_key="favorite_color",
            value="violet",
            observed_at="2026-07-25T12:00:02+00:00",
        )
        packet = self._build_packet()
        self.assertEqual(packet.profile_sufficiency.status, "sparse")
        self.assertGreaterEqual(
            packet.profile_sufficiency.selected_point_count,
            2,
        )
        basis = self._build_basis(
            packet,
            self._build_assessment(packet),
        )

        broad_run = begin_run(
            self.conn,
            basis,
            baseline_response="I can support one narrow point.",
            environ=self.flags,
        )
        broad = evaluate_candidate(
            self.conn,
            broad_run,
            baseline_response="I can support one narrow point.",
            candidate_response=(
                "Your favorite movie is Arrival and your favorite color is "
                "violet."
            ),
            environ=self.flags,
        )
        self.assertFalse(broad.candidate_selected)
        self.assertEqual(
            broad.fallback_reason,
            "candidate_sparse_scope_exceeded",
        )

        narrow_run = begin_run(
            self.conn,
            basis,
            baseline_response="I can support one narrow point.",
            environ=self.flags,
        )
        narrow = evaluate_candidate(
            self.conn,
            narrow_run,
            baseline_response="I can support one narrow point.",
            candidate_response="Your favorite movie is Arrival.",
            environ=self.flags,
        )
        self.assertTrue(narrow.candidate_selected)

    def test_candidate_is_applied_only_after_revalidation_and_grounding(self):
        run = begin_run(
            self.conn,
            self.basis,
            baseline_response=(
                "You keep connecting music and project work."
            ),
            environ=self.flags,
        )
        self.assertTrue(run.prompt_applied)
        decision = evaluate_candidate(
            self.conn,
            run,
            baseline_response=(
                "You keep connecting music and project work."
            ),
            candidate_response=(
                "Your favorite movie is Arrival. You are also about modular "
                "synths, music, and building the archive project into "
                "something shared."
            ),
            candidate_generation_latency_ms=125,
            environ=self.flags,
        )
        self.assertTrue(decision.candidate_selected)
        self.assertGreater(
            decision.candidate_evidence_coverage_count,
            0,
        )
        self.assertTrue(
            finalize_run(
                self.conn,
                decision,
                final_response=decision.response,
                response_sent=True,
                candidate_live=True,
                guard_status="candidate_sent",
            )
        )

        report = build_evaluation_report(self.conn, guild_id=1)
        self.assertEqual(report["runs"], 1)
        self.assertEqual(report["promptAppliedRuns"], 1)
        self.assertEqual(report["candidateSelectedRuns"], 1)
        self.assertEqual(report["liveAppliedRuns"], 1)
        self.assertEqual(report["responseSentRuns"], 1)
        self.assertGreater(report["candidateEvidenceCoverageTotal"], 0)
        self.assertGreater(
            report["candidateMemberSupportedClaimTotal"],
            0,
        )
        self.assertEqual(
            report["candidateUnsupportedFactualClaimTotal"],
            0,
        )
        self.assertEqual(
            report["routeFamilyCounts"],
            {"broad_self_profile": 1},
        )
        self.assertEqual(
            report["candidateGenerationLatencyMs"],
            {"average": 125, "maximum": 125, "samples": 1},
        )
        self.assertEqual(report["liveInvalidRevalidationRuns"], 0)
        self.assertEqual(report["liveUngroundedRuns"], 0)
        self.assertEqual(
            report["liveUnsupportedFactualClaimRuns"],
            0,
        )
        self.assertEqual(report["relationshipPostureAppliedRuns"], 0)
        self.assertEqual(report["contentFieldsPresent"], [])

        packet_report = build_packet_report(self.conn, guild_id=1)
        self.assertEqual(packet_report["promptAppliedRuns"], 1)
        self.assertEqual(packet_report["liveAppliedRuns"], 1)

    def test_source_change_and_control_leak_fall_back_to_established_path(self):
        run = begin_run(
            self.conn,
            self.basis,
            baseline_response="You are about music and project work.",
            environ=self.flags,
        )
        self.conn.execute(
            """
            UPDATE conversations
            SET content='The source row changed before generation.'
            WHERE id=900
            """
        )
        valid, status = revalidate_basis(
            self.conn,
            self.basis,
            environ=self.flags,
        )
        self.assertFalse(valid)
        self.assertEqual(status, "source_changed")
        changed = evaluate_candidate(
            self.conn,
            run,
            baseline_response="You are about music and project work.",
            candidate_response=(
                "You are about modular synths and the archive project."
            ),
            environ=self.flags,
        )
        self.assertFalse(changed.candidate_selected)
        self.assertEqual(
            changed.fallback_reason,
            "post_generation_source_changed",
        )

        fresh_packet = self._build_packet()
        fresh_basis = self._build_basis(
            fresh_packet,
            self._build_assessment(fresh_packet),
        )
        leak_run = begin_run(
            self.conn,
            fresh_basis,
            baseline_response="You are about music and project work.",
            environ=self.flags,
        )
        leaked = evaluate_candidate(
            self.conn,
            leak_run,
            baseline_response="You are about music and project work.",
            candidate_response=(
                "The unified intelligence packet says modular synths."
            ),
            environ=self.flags,
        )
        self.assertFalse(leaked.candidate_selected)
        self.assertEqual(
            leaked.fallback_reason,
            "candidate_control_marker_leak",
        )

    def test_acceptance_reconciles_scoped_packet_application(self):
        run = begin_run(
            self.conn,
            self.basis,
            baseline_response="You are about music and project work.",
            environ=self.flags,
        )
        decision = evaluate_candidate(
            self.conn,
            run,
            baseline_response="You are about music and project work.",
            candidate_response=(
                "Your favorite movie is Arrival. You are also all about "
                "modular synths and the archive project; both keep showing "
                "up in the way you build."
            ),
            environ=self.flags,
        )
        self.assertTrue(decision.candidate_selected)
        finalize_run(
            self.conn,
            decision,
            final_response=decision.response,
            response_sent=True,
            candidate_live=True,
            guard_status="candidate_sent",
        )

        snapshot = build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=self.flags,
        )
        self.assertNotIn(
            "unified_intelligence_packet:promptAppliedRuns",
            snapshot["blockers"],
        )
        self.assertNotIn(
            "unified_intelligence_packet:liveAppliedRuns",
            snapshot["blockers"],
        )
        canary = snapshot["sharedBrainSynthesisCanary"]
        self.assertTrue(canary["requested"])
        self.assertTrue(canary["effective"])
        self.assertTrue(canary["fullyScoped"])
        self.assertEqual(
            canary["unauthorizedPacketApplications"],
            {"prompt": 0, "live": 0},
        )
        self.assertTrue(snapshot["behaviorChangesApplied"])
        rendered = "\n".join(
            render_v2_shadow_acceptance_lines(snapshot)
        )
        self.assertIn("- shared_brain_synthesis_canary:", rendered)
        self.assertIn("prompt_applied=`1`", rendered)
        self.assertIn("live_applied=`1`", rendered)
        self.assertIn("relationship_posture_applied=`0`", rendered)
        self.assertNotIn("modular synths", rendered)

    def test_acceptance_blocks_packet_application_without_canary_receipt(self):
        self.assertTrue(
            mark_packet_application(
                self.conn,
                self.packet,
                prompt_applied=True,
            )
        )
        snapshot = build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=self.flags,
        )
        self.assertIn(
            (
                "unified_intelligence_packet:"
                "promptAppliedWithoutSynthesisReceipt"
            ),
            snapshot["blockers"],
        )

    def test_acceptance_blocks_only_live_profile_or_prompt_violations(self):
        rejected_run = begin_run(
            self.conn,
            self.basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        rejected = evaluate_candidate(
            self.conn,
            rejected_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response=(
                "The liaison entity is an unfinished active intelligence. "
                "Your favorite movie is Arrival."
            ),
            environ=self.flags,
        )
        self.assertFalse(rejected.candidate_selected)
        rejected_snapshot = build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=self.flags,
        )
        self.assertNotIn(
            (
                "shared_brain_synthesis_canary:"
                "liveUnsupportedFactualClaimRuns"
            ),
            rejected_snapshot["blockers"],
        )

        accepted_run = begin_run(
            self.conn,
            self.basis,
            baseline_response="You keep connecting music and project work.",
            environ=self.flags,
        )
        accepted = evaluate_candidate(
            self.conn,
            accepted_run,
            baseline_response="You keep connecting music and project work.",
            candidate_response="Your favorite movie is Arrival.",
            environ=self.flags,
        )
        self.assertTrue(accepted.candidate_selected)
        self.assertTrue(
            finalize_run(
                self.conn,
                accepted,
                final_response=accepted.response,
                response_sent=True,
                candidate_live=True,
                guard_status="candidate_sent",
            )
        )
        self.conn.execute(
            """
            UPDATE memory_governance_shared_brain_synthesis_runs
            SET candidate_member_root_coverage_count=0,
                candidate_unsupported_factual_claim_count=1,
                competing_factual_context_count=1,
                replaced_factual_context_count=0
            WHERE run_id=?
            """,
            (accepted.run.run_id,),
        )
        report = build_evaluation_report(self.conn, guild_id=1)
        self.assertEqual(
            report["liveInsufficientMemberCoverageRuns"],
            1,
        )
        self.assertEqual(
            report["liveUnsupportedFactualClaimRuns"],
            1,
        )
        self.assertEqual(
            report["livePromptOwnershipViolationRuns"],
            1,
        )
        snapshot = build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=self.flags,
        )
        for key in (
            "liveInsufficientMemberCoverageRuns",
            "liveUnsupportedFactualClaimRuns",
            "livePromptOwnershipViolationRuns",
        ):
            self.assertIn(
                "shared_brain_synthesis_canary:%s" % key,
                snapshot["blockers"],
            )

    def test_receipt_schema_contains_no_member_or_response_content(self):
        ensure_schema(self.conn)
        columns = {
            row[1]
            for row in self.conn.execute(
                """
                PRAGMA table_info(
                  memory_governance_shared_brain_synthesis_runs
                )
                """
            )
        }
        forbidden = {
            "request_text",
            "packet_content",
            "source_text",
            "response_text",
            "baseline_response",
            "candidate_response",
            "participant_ids",
            "source_refs",
        }
        self.assertFalse(columns & forbidden)


if __name__ == "__main__":
    unittest.main()
