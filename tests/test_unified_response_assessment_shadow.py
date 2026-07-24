import json
import sqlite3
import unittest

from bnl_unified_response_assessment import (
    ASSESSMENT_VERSION,
    assess_response_coherence,
    build_conversation_evidence_item,
    build_evaluation_report,
    build_unified_response_assessment,
    ensure_schema,
    persist_shadow_run,
    shadow_configuration,
)


FOUNDATION_SHADOWS = {
    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
    "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
    "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
    "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
    "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
    "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
    "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
}


class UnifiedResponseAssessmentShadowTests(unittest.TestCase):
    def shared_choice_assessment(self):
        final_question = (
            "Between Chrome Prophet and Null Basilica, which fits that "
            "requirement better, and why?"
        )
        evidence = (
            build_conversation_evidence_item(
                text="“Chrome Prophet” sounds like a person.",
                source_id=10,
                speaker_user_id=101,
                speaker_label="Jon",
            ),
            build_conversation_evidence_item(
                text=(
                    "The hidden room should sound like a place, "
                    "not a character."
                ),
                source_id=11,
                speaker_user_id=202,
                speaker_label="Miss Bit",
            ),
            build_conversation_evidence_item(
                text="“Null Basilica” sounds like a place.",
                source_id=12,
                speaker_user_id=101,
                speaker_label="Jon",
            ),
            build_conversation_evidence_item(
                text=final_question,
                speaker_user_id=101,
                speaker_label="Jon",
                current_turn=True,
            ),
        )
        return build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(101,),
            participant_user_ids=(101, 202),
            speaker_labels=("Jon", "Miss Bit"),
            current_exchange_source_ids=(10, 11, 12),
            prompt_lanes=("current_exchange", "conversation_context"),
            current_payload_anchors=(
                "chrome prophet",
                "null basilica",
            ),
            thread_focus_mode="new_thread",
            current_text=final_question,
            conversation_evidence_items=evidence,
        )

    def test_shadow_derives_from_foundations_and_explicit_false_is_rollback(self):
        derived = shadow_configuration(FOUNDATION_SHADOWS)
        self.assertTrue(derived["requested"])
        self.assertTrue(derived["effective"])
        self.assertFalse(derived["explicitly_configured"])
        self.assertEqual(derived["reason"], "shadow_only")

        disabled = shadow_configuration(
            {
                **FOUNDATION_SHADOWS,
                "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "false",
            }
        )
        self.assertFalse(disabled["requested"])
        self.assertFalse(disabled["effective"])
        self.assertTrue(disabled["explicitly_configured"])
        self.assertEqual(disabled["reason"], "disabled")

    def test_shadow_fails_closed_when_a_foundation_is_missing_or_live(self):
        missing = shadow_configuration(
            {
                **FOUNDATION_SHADOWS,
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "false",
                "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            }
        )
        self.assertFalse(missing["effective"])
        self.assertEqual(missing["reason"], "missing_shadow_prerequisites")
        self.assertEqual(
            missing["missing_prerequisites"],
            ("BNL_MOMENT_ENGINE_SHADOW_ENABLED",),
        )

        live = shadow_configuration(
            {
                **FOUNDATION_SHADOWS,
                "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "true",
            }
        )
        self.assertFalse(live["effective"])
        self.assertEqual(live["reason"], "live_authority_detected")
        self.assertEqual(
            live["active_live_gates"],
            ("BNL_RELATIONSHIP_V2_LIVE_ENABLED",),
        )

    def test_immediate_recap_is_current_first_for_any_participant_count(self):
        participants = tuple(range(1001, 1011))
        labels = tuple("Member %s" % index for index in range(1, 11))
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="active_channel",
            current_speaker_user_ids=participants,
            participant_user_ids=participants,
            speaker_labels=labels,
            current_exchange_source_ids=tuple(range(1, 11)),
            prior_moment_ids=("moment-private-id",),
            governed_entry_ids=("ledger-private-id",),
            relationship_candidate_keys=("relationship-private-key",),
            canon_refs=("canon:one",),
            prompt_lanes=(
                "current_exchange",
                "conversation_context",
                "prior_moment",
                "governed_memory",
                "relationship",
                "canon",
            ),
            immediate_recap=True,
            continuity_required=True,
            moment_candidate_count=4,
            governed_candidate_count=3,
            relationship_v2_candidate_present=True,
            canon_relevant=True,
        )

        self.assertEqual(assessment.schema_version, ASSESSMENT_VERSION)
        self.assertEqual(assessment.participant_user_ids, participants)
        self.assertEqual(assessment.speaker_labels, labels)
        self.assertEqual(
            assessment.selected_lanes,
            ("current_exchange", "conversation_context"),
        )
        self.assertEqual(
            assessment.response_act,
            "recap_current_exchange",
        )
        self.assertIn(
            ("prior_moment", "current_exchange_precedence"),
            assessment.excluded_lanes,
        )
        self.assertIn(
            ("governed_memory", "current_exchange_precedence"),
            assessment.excluded_lanes,
        )
        self.assertIn(
            ("relationship", "current_exchange_precedence"),
            assessment.excluded_lanes,
        )
        self.assertIn("current_exchange_precedence", assessment.conflict_reasons)
        self.assertEqual(assessment.comparison_status, "prompt_overincluded")

    def test_general_turn_unifies_selected_governed_lanes_without_rendering(self):
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_context",
            conversation_surface="free_speak",
            current_speaker_user_ids=(7,),
            participant_user_ids=(7, 8),
            current_exchange_source_ids=(100, 101),
            prior_moment_ids=("moment-one",),
            governed_entry_ids=("ledger-one", "ledger-two"),
            relationship_candidate_keys=("relationship-current-member",),
            canon_refs=("canon:contract",),
            prompt_lanes=(
                "current_exchange",
                "conversation_context",
                "prior_moment",
                "governed_memory",
                "relationship",
                "canon",
            ),
            continuity_required=True,
            moment_candidate_count=2,
            governed_candidate_count=2,
            relationship_v2_candidate_present=True,
            canon_relevant=True,
        )

        self.assertEqual(
            assessment.selected_lanes,
            (
                "current_exchange",
                "conversation_context",
                "prior_moment",
                "governed_memory",
                "relationship",
                "canon",
            ),
        )
        self.assertEqual(assessment.comparison_status, "match")
        self.assertEqual(assessment.response_act, "continue_active_thread")
        self.assertEqual(assessment.prompt_extra_lanes, ())
        self.assertEqual(assessment.prompt_missing_lanes, ())

    def test_semantic_frame_resolves_shared_objective_and_attribution(self):
        assessment = self.shared_choice_assessment()

        self.assertEqual(
            assessment.current_objective,
            (
                "Between Chrome Prophet and Null Basilica, which fits that "
                "requirement better, and why?"
            ),
        )
        self.assertEqual(assessment.objective_kind, "compare_options")
        self.assertEqual(
            assessment.current_options,
            ("chrome prophet", "null basilica"),
        )
        self.assertEqual(
            tuple(
                (referent.option_key, referent.speaker_user_id)
                for referent in assessment.option_referents
            ),
            (("chrome prophet", 101), ("null basilica", 101)),
        )
        self.assertEqual(len(assessment.attributed_criteria), 1)
        criterion = assessment.attributed_criteria[0]
        self.assertEqual(criterion.speaker_user_id, 202)
        self.assertEqual(criterion.speaker_label, "Miss Bit")
        self.assertEqual(criterion.positive_terms, ("place",))
        self.assertEqual(criterion.negative_terms, ("character",))
        self.assertEqual(assessment.ambiguity_reasons, ())
        self.assertEqual(
            assessment.response_act,
            "evaluate_current_options",
        )
        self.assertEqual(
            assessment.expected_answer_shape,
            "choice_then_reason",
        )

    def test_unresolved_criterion_requires_one_clarifying_question(self):
        request = (
            "Between Chrome Prophet and Null Basilica, which fits that "
            "requirement better?"
        )
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(101,),
            current_payload_anchors=(
                "chrome prophet",
                "null basilica",
            ),
            current_text=request,
            conversation_evidence_items=(
                build_conversation_evidence_item(
                    text=request,
                    speaker_user_id=101,
                    speaker_label="Jon",
                    current_turn=True,
                ),
            ),
        )

        self.assertEqual(
            assessment.ambiguity_reasons,
            ("criterion_referent_unresolved",),
        )
        self.assertEqual(
            assessment.response_act,
            "ask_clarifying_question",
        )
        self.assertEqual(
            assessment.expected_answer_shape,
            "one_clarifying_question",
        )
        coherence = assess_response_coherence(
            assessment,
            "Which requirement should I use to compare them?",
        )
        self.assertEqual(coherence.status, "passed")
        self.assertEqual(coherence.clarification_status, "appropriate")

    def test_decisions_corrections_and_open_loops_keep_source_lineage(self):
        evidence = (
            build_conversation_evidence_item(
                text=(
                    "Actually, we decided to use Saturday instead of Friday."
                ),
                source_id=20,
                speaker_user_id=101,
                speaker_label="Jon",
            ),
            build_conversation_evidence_item(
                text="Who owns the artwork?",
                source_id=21,
                speaker_user_id=202,
                speaker_label="Miss Bit",
            ),
        )
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(202,),
            participant_user_ids=(101, 202),
            speaker_labels=("Jon", "Miss Bit"),
            current_exchange_source_ids=(20, 21),
            prompt_lanes=("current_exchange", "conversation_context"),
            current_text="Who owns the artwork?",
            conversation_evidence_items=evidence,
        )

        self.assertEqual(assessment.decision_source_ids, (20,))
        self.assertEqual(assessment.correction_source_ids, (20,))
        self.assertEqual(assessment.open_loop_source_ids, (21,))
        roles_by_source = {
            item.source_id: item.semantic_roles
            for item in assessment.conversation_evidence_items
        }
        self.assertIn("decision", roles_by_source[20])
        self.assertIn("correction", roles_by_source[20])
        self.assertIn("open_loop", roles_by_source[21])

    def test_conclusion_must_agree_with_its_own_criterion_reasoning(self):
        assessment = self.shared_choice_assessment()

        contradictory = assess_response_coherence(
            assessment,
            (
                "Chrome Prophet sounds like a person. "
                "Null Basilica sounds like a place. "
                "I choose Chrome Prophet."
            ),
        )
        self.assertEqual(contradictory.status, "failed")
        self.assertEqual(
            contradictory.conclusion_status,
            "contradictory",
        )
        self.assertIn(
            "conclusion_reason_contradiction",
            contradictory.reason_codes,
        )

        coherent = assess_response_coherence(
            assessment,
            (
                "Chrome Prophet sounds like a person. "
                "Null Basilica sounds like a place. "
                "I choose Null Basilica."
            ),
        )
        self.assertEqual(coherent.status, "passed")
        self.assertEqual(coherent.conclusion_status, "consistent")
        self.assertEqual(coherent.criterion_status, "covered")

    def test_semantic_frame_is_domain_neutral_across_conversation_shapes(self):
        cases = (
            (
                "planning",
                "We should deploy after CI passes.",
                "What should we do next?",
                "continue_or_answer",
                1,
            ),
            (
                "technical",
                "The provider retry still times out.",
                "How do we isolate the failing boundary?",
                "continue_or_answer",
                0,
            ),
            (
                "creative",
                "The chorus must feel playful, not formal.",
                "Which version fits that criterion better?",
                "continue_or_answer",
                1,
            ),
            (
                "joking",
                "The joke should stay short and weird.",
                "Give me the next line.",
                "continue_or_answer",
                1,
            ),
            (
                "resumed",
                "We left the rollout question unresolved.",
                "Go back to the rollout question from earlier.",
                "resume_thread",
                0,
            ),
            (
                "combined",
                "One thread covers tone; another covers timing.",
                "Combine both threads into one recommendation.",
                "combine_threads",
                0,
            ),
            (
                "topic_change",
                "The old topic was the server restart.",
                "Now help me name the new event.",
                "new_thread",
                0,
            ),
        )
        for name, prior_text, current_text, focus, criterion_count in cases:
            with self.subTest(name=name):
                evidence = (
                    build_conversation_evidence_item(
                        text=prior_text,
                        source_id=10,
                        speaker_user_id=101,
                        speaker_label="Jon",
                    ),
                    build_conversation_evidence_item(
                        text=current_text,
                        speaker_user_id=202,
                        speaker_label="Miss Bit",
                        current_turn=True,
                    ),
                )
                assessment = build_unified_response_assessment(
                    guild_id=1,
                    route_mode="normal_chat",
                    channel_policy="sealed_test",
                    conversation_surface="test",
                    current_speaker_user_ids=(202,),
                    participant_user_ids=(101, 202),
                    speaker_labels=("Jon", "Miss Bit"),
                    current_exchange_source_ids=(10,),
                    prompt_lanes=(
                        "current_exchange",
                        "conversation_context",
                    ),
                    continuity_required=True,
                    current_text=current_text,
                    conversation_evidence_items=evidence,
                    thread_focus_mode=focus,
                )
                self.assertEqual(
                    assessment.thread_focus_mode,
                    focus,
                )
                self.assertTrue(assessment.current_objective)
                self.assertEqual(
                    len(assessment.conversation_evidence_items),
                    2,
                )
                self.assertEqual(
                    assessment.participant_user_ids,
                    (101, 202),
                )
                self.assertEqual(
                    len(assessment.attributed_criteria),
                    criterion_count,
                )
                self.assertNotEqual(
                    assessment.response_act,
                    "ask_clarifying_question",
                )

    def test_receipt_flags_conclusion_contradiction_without_content(self):
        conn = sqlite3.connect(":memory:")
        try:
            assessment = self.shared_choice_assessment()
            persist_shadow_run(
                conn,
                assessment,
                response=(
                    "Chrome Prophet sounds like a person. "
                    "Null Basilica sounds like a place. "
                    "I choose Chrome Prophet."
                ),
            )
            conn.commit()

            row = conn.execute(
                "SELECT objective_kind, criterion_count, option_count, "
                "response_coherence_status, "
                "coherence_conclusion_status, "
                "coherence_reason_codes_json, response_alignment "
                "FROM unified_response_assessment_shadow_runs"
            ).fetchone()
            self.assertEqual(row[:5], (
                "compare_options",
                1,
                2,
                "failed",
                "contradictory",
            ))
            self.assertEqual(
                json.loads(row[5]),
                ["conclusion_reason_contradiction"],
            )
            self.assertEqual(row[6], "response_coherence_failure")

            report = build_evaluation_report(conn, guild_id=1)
            self.assertEqual(
                report["response_coherence_failure_runs"],
                1,
            )
            self.assertEqual(
                report["conclusion_contradiction_runs"],
                1,
            )
            self.assertEqual(
                report["coherence_reason_code_counts"],
                {"conclusion_reason_contradiction": 1},
            )
            encoded = json.dumps(report, sort_keys=True)
            self.assertNotIn("Chrome Prophet", encoded)
            self.assertNotIn("Null Basilica", encoded)
            self.assertNotIn("Miss Bit", encoded)
        finally:
            conn.close()

    def test_receipt_is_content_free_and_observes_visible_pause_marker(self):
        conn = sqlite3.connect(":memory:")
        try:
            assessment = build_unified_response_assessment(
                guild_id=123,
                route_mode="normal_chat",
                channel_policy="sealed_test",
                conversation_surface="test",
                current_speaker_user_ids=(987654321,),
                target_user_ids=(123456789,),
                participant_user_ids=(987654321, 123456789),
                speaker_labels=("PRIVATE ALPHA", "PRIVATE BETA"),
                current_exchange_source_ids=(444, 555),
                prior_moment_ids=("SECRET MOMENT ID",),
                governed_entry_ids=("SECRET LEDGER ID",),
                relationship_candidate_keys=("SECRET RELATIONSHIP KEY",),
                canon_refs=("SECRET CANON REF",),
                prompt_lanes=(
                    "current_exchange",
                    "conversation_context",
                    "PRIVATE PROMPT PAYLOAD",
                ),
                immediate_recap=True,
            )
            self.assertNotIn(
                "PRIVATE PROMPT PAYLOAD",
                assessment.prompt_lanes,
            )
            persist_shadow_run(
                conn,
                assessment,
                response=(
                    "[Pause: 0.2s]\n"
                    "PRIVATE ALPHA has the intro and PRIVATE BETA has the artwork."
                ),
                guard_diagnostics={
                    "contextual_followthrough_guard_triggered": True,
                    "contextual_followthrough_regenerated": True,
                },
                processing_errors=("PRIVATE ERROR PAYLOAD",),
                created_at="2026-07-24T17:00:00+00:00",
            )
            conn.commit()

            columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(unified_response_assessment_shadow_runs)"
                )
            }
            self.assertFalse(
                columns
                & {
                    "raw_text",
                    "request_text",
                    "response_text",
                    "speaker_labels",
                    "participant_ids",
                    "source_ids",
                    "source_text",
                }
            )
            row = conn.execute(
                "SELECT * FROM unified_response_assessment_shadow_runs"
            ).fetchone()
            serialized_row = json.dumps(row)
            for secret in (
                "PRIVATE ALPHA",
                "PRIVATE BETA",
                "987654321",
                "123456789",
                "SECRET MOMENT ID",
                "SECRET LEDGER ID",
                "SECRET RELATIONSHIP KEY",
                "SECRET CANON REF",
                "PRIVATE PROMPT PAYLOAD",
                "PRIVATE ERROR PAYLOAD",
            ):
                self.assertNotIn(secret, serialized_row)

            report = build_evaluation_report(conn, guild_id=123)
            self.assertEqual(report["runs"], 1)
            self.assertEqual(report["response_sent_runs"], 1)
            self.assertEqual(report["current_exchange_primary_runs"], 1)
            self.assertEqual(report["guard_triggered_runs"], 1)
            self.assertEqual(report["guard_repaired_runs"], 1)
            self.assertEqual(report["visible_control_marker_runs"], 1)
            self.assertEqual(report["processing_errors"], 1)
            self.assertEqual(report["behavior_changed_runs"], 0)
            self.assertEqual(report["new_authority_applied_runs"], 0)
            self.assertEqual(report["content_fields_present"], [])
            self.assertEqual(
                report["response_alignment_counts"],
                {"visible_control_marker": 1},
            )
        finally:
            conn.close()

    def test_receipt_detects_stale_choice_even_when_lane_comparison_matches(self):
        conn = sqlite3.connect(":memory:")
        try:
            assessment = build_unified_response_assessment(
                guild_id=321,
                route_mode="normal_chat",
                channel_policy="sealed_test",
                conversation_surface="test",
                current_speaker_user_ids=(1,),
                prompt_lanes=("current_exchange", "conversation_context"),
                current_exchange_source_ids=(10, 11),
                current_payload_anchors=("dead channel", "open circuit"),
                prior_thread_anchors=("ghost signal", "neon static"),
                thread_focus_mode="new_thread",
            )
            self.assertEqual(assessment.comparison_status, "match")

            persist_shadow_run(
                conn,
                assessment,
                response="Ghost Signal is the stronger hidden-zone title.",
                created_at="2026-07-24T18:35:11+00:00",
            )
            persist_shadow_run(
                conn,
                assessment,
                response="Dead Channel is the stronger hidden-zone title.",
                guard_diagnostics={
                    "current_payload_grounding_guard_triggered": True,
                    "current_payload_grounding_regenerated": True,
                },
                created_at="2026-07-24T18:36:11+00:00",
            )
            conn.commit()

            rows = conn.execute(
                "SELECT payload_grounding_status, response_alignment, "
                "current_payload_anchor_count, "
                "current_payload_anchor_hit_count, "
                "prior_thread_anchor_hit_count "
                "FROM unified_response_assessment_shadow_runs "
                "ORDER BY created_at"
            ).fetchall()
            self.assertEqual(
                rows[0],
                (
                    "stale_thread_substitution",
                    "payload_grounding_failure",
                    2,
                    0,
                    1,
                ),
            )
            self.assertEqual(
                rows[1],
                (
                    "grounded_current_payload",
                    "guard_repaired",
                    2,
                    1,
                    0,
                ),
            )

            report = build_evaluation_report(conn, guild_id=321)
            self.assertEqual(report["runs"], 2)
            self.assertEqual(
                report["comparison_status_counts"],
                {"match": 2},
            )
            self.assertEqual(
                report["payload_grounding_status_counts"],
                {
                    "grounded_current_payload": 1,
                    "stale_thread_substitution": 1,
                },
            )
            self.assertEqual(report["payload_grounding_applicable_runs"], 2)
            self.assertEqual(report["payload_grounding_failure_runs"], 1)
            self.assertEqual(
                report["thread_focus_mode_counts"],
                {"new_thread": 2},
            )
            self.assertEqual(report["guard_triggered_runs"], 1)
            self.assertEqual(report["guard_repaired_runs"], 1)
        finally:
            conn.close()

    def test_receipt_accepts_unambiguous_current_choice_reference(self):
        conn = sqlite3.connect(":memory:")
        try:
            assessment = build_unified_response_assessment(
                guild_id=321,
                route_mode="normal_chat",
                channel_policy="sealed_test",
                conversation_surface="test",
                current_speaker_user_ids=(1,),
                prompt_lanes=("current_exchange",),
                current_exchange_source_ids=(10,),
                current_payload_anchors=(
                    "circuit saint",
                    "null chapel",
                ),
                thread_focus_mode="new_thread",
            )
            persist_shadow_run(
                conn,
                assessment,
                response=(
                    "The latter fits better because it sounds like a place."
                ),
            )
            conn.commit()

            row = conn.execute(
                "SELECT payload_grounding_status, response_alignment, "
                "current_payload_anchor_count, "
                "current_payload_anchor_hit_count "
                "FROM unified_response_assessment_shadow_runs"
            ).fetchone()
            self.assertEqual(
                row,
                (
                    "grounded_current_payload_reference",
                    "guard_clear",
                    2,
                    0,
                ),
            )
            report = build_evaluation_report(conn, guild_id=321)
            self.assertEqual(report["payload_grounding_applicable_runs"], 1)
            self.assertEqual(report["payload_grounding_failure_runs"], 0)
            self.assertEqual(
                report["payload_grounding_status_counts"],
                {"grounded_current_payload_reference": 1},
            )
        finally:
            conn.close()

    def test_existing_v1_receipt_table_is_migrated_additively(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                """
                CREATE TABLE unified_response_assessment_shadow_runs (
                    run_id TEXT PRIMARY KEY,
                    guild_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

            ensure_schema(conn)

            columns = {
                row[1]: row
                for row in conn.execute(
                    "PRAGMA table_info("
                    "unified_response_assessment_shadow_runs)"
                )
            }
            for column in (
                "thread_focus_mode",
                "current_payload_anchor_count",
                "current_payload_anchor_hit_count",
                "prior_thread_anchor_count",
                "prior_thread_anchor_hit_count",
                "payload_grounding_status",
                "objective_kind",
                "expected_answer_shape",
                "contribution_count",
                "criterion_count",
                "option_count",
                "ambiguity_reason_count",
                "response_coherence_status",
                "coherence_conclusion_status",
                "coherence_reason_codes_json",
            ):
                self.assertIn(column, columns)
            self.assertEqual(
                columns["payload_grounding_status"][4],
                "'not_evaluated_legacy'",
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
