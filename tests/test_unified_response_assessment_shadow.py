import json
import sqlite3
import unittest

from bnl_unified_response_assessment import (
    ASSESSMENT_VERSION,
    assess_response_coherence,
    build_conversation_evidence_item,
    build_evaluation_report,
    build_situation_frame_v1,
    build_unified_response_assessment,
    ensure_schema,
    persist_shadow_run,
    render_sealed_canary_brief,
    response_exposes_canary_control_markers,
    shadow_configuration,
    with_prompt_lane_presence,
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
    def test_situation_frame_separates_addressing_from_governed_subjects(self):
        external_cases = (
            (
                "@BNL-01 Sealed Row 9 provider fixture: In one paragraph, "
                "what makes a community feel connected instead of merely "
                "active?",
                "external_public",
            ),
            (
                "Hey **@BNL-01**. When did Apollo 11 land?",
                "external_public",
            ),
            (
                "Hey *<@99>*. When did Apollo 11 land?",
                "external_public",
            ),
            (
                "Please @BNL-01, can you tell me when Apollo 11 landed?",
                "external_public",
            ),
            (
                "@BNL-01 can you explain when Apollo 11 landed?",
                "current_request",
            ),
            (
                "@BNL-01, how do you boil an egg?",
                "external_public",
            ),
            (
                "`<@99>`, when did Apollo 11 land?",
                "external_public",
            ),
            (
                "@BNL-01, what does the word 'you' mean?",
                "external_public",
            ),
            (
                "@BNL-01, what does “your birthday” mean?",
                "external_public",
            ),
            (
                "@BNL-01, what does `your birthday` mean?",
                "external_public",
            ),
            ("@BNL-01, how are you?", "current_request"),
        )
        for text, expected_authority in external_cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="free_speak_sealed_mirror",
                    channel_policy="sealed_test",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    current_speaker_labels=("Test Member",),
                    addressee_kinds=("discord_mention",),
                    addressee_user_ids=(99,),
                    explicit_mention_count=1,
                    response_act="answer",
                )

                self.assertEqual(frame.subjects, ())
                self.assertEqual(frame.subject_requirement, "not_applicable")
                self.assertEqual(
                    tuple(task.authority_scope for task in frame.tasks),
                    (expected_authority,),
                )

        governed_cases = (
            ("@BNL-01 When was BARCODE founded?", "barcode"),
            ("@BNL-01, when were you created?", "bnl_01"),
            ("@BNL-01, don't you know when you were created?", "bnl_01"),
            (
                "@BNL-01, don't you know your birthday isn't January 1?",
                "bnl_01",
            ),
            ("@BNL-01, when'd you launch?", "bnl_01"),
            ("@BNL-01, what's your origin?", "bnl_01"),
            ("@BNL-01's creator is who?", "bnl_01"),
            ("@BNL-01 's creator is who?", "bnl_01"),
            ("@BNL-01 was created when?", "bnl_01"),
            ("@BNL-01 can explain its origin?", "bnl_01"),
            ("When was **@BNL-01** created?", "bnl_01"),
        )
        for text, expected_entity_ref in governed_cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="free_speak_sealed_mirror",
                    channel_policy="sealed_test",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    current_speaker_labels=("Test Member",),
                    addressee_kinds=("discord_mention",),
                    addressee_user_ids=(99,),
                    explicit_mention_count=1,
                    response_act="answer",
                )

                self.assertEqual(frame.status, "resolved")
                self.assertEqual(frame.subject_requirement, "required")
                self.assertIn(
                    expected_entity_ref,
                    tuple(subject.entity_ref for subject in frame.subjects),
                )
                self.assertEqual(
                    tuple(task.authority_scope for task in frame.tasks),
                    ("packet",),
                )
                self.assertEqual(frame.tasks[0].subject_requirement, "required")
                self.assertTrue(frame.tasks[0].subject_indexes)

        for text in (
            "When did <@202> join?",
            "<@202>'s birthday is when?",
            "<@202> 's birthday is when?",
            "<@202> was created when?",
            "<@202> has what birthday?",
        ):
            with self.subTest(member_text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="free_speak_sealed_mirror",
                    channel_policy="sealed_test",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    current_speaker_labels=("Test Member",),
                    subject_user_ids=(202,),
                    subject_label_hints=("Second Member",),
                    response_act="answer",
                )

                self.assertEqual(frame.subjects[0].user_id, 202)
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(frame.tasks[0].subject_indexes, (0,))

    def test_situation_frame_requires_an_actual_event_referent(self):
        ordinary_text = (
            "Sealed Row 9 provider fixture: In one paragraph, what makes a "
            "community feel connected instead of merely active?"
        )
        external_cases = (
            ordinary_text,
            "When did Apollo 11 land?",
            "Who attended Woodstock?",
        )
        for moment_state in ("recent_active", "recent_finalized"):
            for text in external_cases:
                with self.subTest(moment_state=moment_state, text=text):
                    frame = build_situation_frame_v1(
                        route_allowed=True,
                        route_mode="normal_chat",
                        conversation_surface="free_speak_sealed_mirror",
                        channel_policy="sealed_test",
                        current_text=text,
                        current_speaker_user_ids=(101,),
                        current_speaker_labels=("Test Member",),
                        moment_id="moment_prior_fixture",
                        moment_situation_state=moment_state,
                        moment_topic_coherent=True,
                        moment_participant_overlap=True,
                        response_act="answer",
                    )

                    self.assertTrue(frame.event_ref)
                    self.assertEqual(frame.tasks[0].authority_scope, "external_public")

        event_cases = (
            "How many people attended?",
            "Who participated?",
            "What happened?",
            "How did the test go?",
            "Did it pass?",
            "What changed in the recent event?",
        )
        for text in event_cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="free_speak_sealed_mirror",
                    channel_policy="sealed_test",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    current_speaker_labels=("Test Member",),
                    moment_id="moment_prior_fixture",
                    moment_situation_state="recent_active",
                    moment_topic_coherent=True,
                    moment_participant_overlap=True,
                    response_act="answer",
                )

                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(frame.tasks[0].object_kind, "moment")

        no_event = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="free_speak_sealed_mirror",
            channel_policy="sealed_test",
            current_text="How many people attended?",
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            response_act="answer",
        )
        self.assertEqual(no_event.tasks[0].authority_scope, "external_public")

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
                speaker_label="Test Member",
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
                speaker_label="Test Member",
            ),
            build_conversation_evidence_item(
                text=final_question,
                speaker_user_id=101,
                speaker_label="Test Member",
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
            speaker_labels=("Test Member", "Miss Bit"),
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

    def test_profile_sufficiency_is_evaluated_without_retrieval(self):
        rich = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="active_channel_batch",
            current_speaker_user_ids=(7,),
            target_user_ids=(7,),
            current_text="What am I all about?",
            packet_selected_lanes=("governed_memory", "canon"),
            profile_sufficiency_status="rich",
            profile_sufficiency_met=True,
            profile_required_point_count=2,
            profile_selected_point_count=2,
            profile_independent_root_count=4,
            profile_independent_occurrence_count=2,
            profile_sufficiency_reasons=(
                "rich_supported_member_evidence",
            ),
        )
        empty = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="active_channel_batch",
            current_speaker_user_ids=(7,),
            target_user_ids=(7,),
            current_text="What am I all about?",
            packet_selected_lanes=("canon",),
            profile_sufficiency_status="empty",
            profile_sufficiency_met=False,
            profile_sufficiency_reasons=(
                "no_supported_member_evidence",
            ),
        )
        malformed_rich = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="active_channel_batch",
            current_speaker_user_ids=(7,),
            target_user_ids=(7,),
            current_text="What am I all about?",
            profile_sufficiency_status="rich",
            profile_sufficiency_met=True,
            profile_required_point_count=1,
            profile_selected_point_count=2,
            profile_independent_root_count=2,
            profile_independent_occurrence_count=2,
        )
        malformed_sparse = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="active_channel_batch",
            current_speaker_user_ids=(7,),
            target_user_ids=(7,),
            current_text="What am I all about?",
            profile_sufficiency_status="sparse",
            profile_sufficiency_met=True,
            profile_required_point_count=2,
            profile_selected_point_count=2,
            profile_independent_root_count=2,
            profile_independent_occurrence_count=2,
        )

        self.assertTrue(rich.profile_sufficiency_met)
        self.assertEqual(rich.profile_sufficiency_status, "rich")
        self.assertIn(
            "profile_rich_evidence_ready",
            rich.diagnostic_reasons,
        )
        self.assertFalse(empty.profile_sufficiency_met)
        self.assertEqual(empty.profile_sufficiency_status, "empty")
        self.assertIn(
            "profile_sufficiency_empty",
            empty.conflict_reasons,
        )
        self.assertIn(
            "profile_sufficiency_not_met",
            empty.diagnostic_reasons,
        )
        self.assertFalse(malformed_rich.profile_sufficiency_met)
        self.assertFalse(malformed_sparse.profile_sufficiency_met)
        self.assertIn(
            "profile_sufficiency_not_met",
            malformed_rich.diagnostic_reasons,
        )
        self.assertIn(
            "profile_sufficiency_not_met",
            malformed_sparse.diagnostic_reasons,
        )

    def test_sealed_canary_brief_preserves_attribution_without_raw_transcript(self):
        assessment = self.shared_choice_assessment()
        brief = render_sealed_canary_brief(
            assessment,
            active_episode_context=(
                "[Active same-channel episode signal; aggregate continuity "
                "only, never quotation or durable-fact authority]\n"
                "- Shared human participants: 2."
            ),
        )
        self.assertIn("SEALED UNIFIED CONVERSATION CANARY", brief)
        self.assertIn(
            "Current options: chrome prophet | null basilica",
            brief,
        )
        self.assertIn("Criterion attributed to Miss Bit", brief)
        self.assertIn("favor [place]; avoid [character]", brief)
        self.assertIn("one clear choice first", brief)
        self.assertIn("Active same-channel episode signal", brief)
        self.assertNotIn(
            "The hidden room should sound like a place, not a character.",
            brief,
        )
        self.assertTrue(
            response_exposes_canary_control_markers(
                "The unified response assessment says to choose it."
            )
        )
        self.assertFalse(
            response_exposes_canary_control_markers(
                "Null Basilica fits because it reads as a place."
            )
        )

    def test_prompt_lane_reconciliation_tracks_episode_rendering(self):
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(1,),
            current_exchange_source_ids=(10,),
            active_episode_id="opaque_episode",
            prompt_lanes=("current_exchange", "conversation_context"),
        )
        self.assertEqual(
            assessment.comparison_status,
            "prompt_underincluded",
        )
        rendered = with_prompt_lane_presence(
            assessment,
            "active_episode",
            present=True,
        )
        self.assertIn("active_episode", rendered.prompt_lanes)
        self.assertEqual(rendered.comparison_status, "match")
        removed = with_prompt_lane_presence(
            rendered,
            "active_episode",
            present=False,
        )
        self.assertNotIn("active_episode", removed.prompt_lanes)
        self.assertEqual(
            removed.comparison_status,
            "prompt_underincluded",
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

    def test_active_episode_source_moment_is_counted_without_extra_lane(self):
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="sealed_test",
            current_speaker_user_ids=(7,),
            active_episode_id="episode-one",
            active_episode_source_moment_ids=("moment-one",),
            prompt_lanes=("current_exchange", "active_episode"),
            continuity_required=True,
            current_text="What changed, and what remains open?",
        )
        self.assertEqual(
            assessment.active_episode_source_moment_ids,
            ("moment-one",),
        )
        self.assertIn("active_episode", assessment.selected_lanes)
        self.assertNotIn("prior_moment", assessment.selected_lanes)

        conn = sqlite3.connect(":memory:")
        try:
            run_id = persist_shadow_run(
                conn,
                assessment,
                response="The current phase changed; one action remains open.",
            )
            receipt = conn.execute(
                """
                SELECT active_episode_present,prior_moment_count
                FROM unified_response_assessment_shadow_runs
                WHERE run_id=?
                """,
                (run_id,),
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(receipt, (1, 1))

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
                    speaker_label="Test Member",
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

    def test_pronoun_followup_uses_selected_conversation_referent(self):
        request = "How does it work?"
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(101,),
            participant_user_ids=(101, 202),
            speaker_labels=("Test Member", "Miss Bit"),
            current_exchange_source_ids=(40,),
            prompt_lanes=("current_exchange", "conversation_context"),
            current_text=request,
            conversation_evidence_items=(
                build_conversation_evidence_item(
                    text=(
                        "The grounding fallback retries one draft before "
                        "suppressing a stale answer."
                    ),
                    source_id=40,
                    speaker_user_id=202,
                    speaker_label="Miss Bit",
                ),
                build_conversation_evidence_item(
                    text=request,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
            ),
        )

        self.assertEqual(assessment.ambiguity_reasons, ())
        self.assertEqual(assessment.response_act, "answer_current_turn")
        self.assertEqual(
            assessment.expected_answer_shape,
            "direct_answer_then_support",
        )
        coherence = assess_response_coherence(
            assessment,
            (
                "The grounding fallback retries one draft, then suppresses "
                "the answer only if that retry is still stale."
            ),
        )
        self.assertNotEqual(coherence.status, "failed")
        self.assertNotIn(
            "ambiguity_answered_without_clarification",
            coherence.reason_codes,
        )

    def test_pronoun_without_usable_context_still_requires_clarification(self):
        request = "How does it work?"
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(101,),
            current_text=request,
            conversation_evidence_items=(
                build_conversation_evidence_item(
                    text="Okay.",
                    source_id=50,
                    speaker_user_id=202,
                    speaker_label="Miss Bit",
                ),
                build_conversation_evidence_item(
                    text=request,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
            ),
        )

        self.assertEqual(
            assessment.ambiguity_reasons,
            ("current_referent_unresolved",),
        )
        self.assertEqual(
            assessment.response_act,
            "ask_clarifying_question",
        )

    def test_pronoun_can_resolve_inside_fragmented_current_turn(self):
        request = "How does it work?"
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(101,),
            current_text=request,
            conversation_evidence_items=(
                build_conversation_evidence_item(
                    text="The retry guard now has a bounded fallback.",
                    speaker_user_id=101,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
                build_conversation_evidence_item(
                    text=request,
                    speaker_user_id=101,
                    speaker_label="Test Member",
                    current_turn=True,
                ),
            ),
        )

        self.assertEqual(assessment.ambiguity_reasons, ())
        self.assertEqual(assessment.response_act, "answer_current_turn")

    def test_decisions_corrections_and_open_loops_keep_source_lineage(self):
        evidence = (
            build_conversation_evidence_item(
                text=(
                    "Actually, we decided to use Saturday instead of Friday."
                ),
                source_id=20,
                speaker_user_id=101,
                speaker_label="Test Member",
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
            speaker_labels=("Test Member", "Miss Bit"),
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

    def test_resolved_new_event_does_not_inherit_interrupted_criteria(self):
        glass_prompt = (
            "Sealed acceptance fixture: Project Glass Harbor is in "
            "rehearsal. The amber signal failed, and the open question is "
            "whether to test the relay or the decoder first. Which should "
            "we test first?"
        )
        copper_prompt = (
            "Sealed acceptance fixture: this is a separate task. Project "
            "Copper Kite has a stable blue indicator, and the antenna "
            "calibration must be completed before its notes are archived. "
            "What is the current task, and what remains open?"
        )
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=copper_prompt,
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_id="glass-moment",
            moment_situation_state="recent_active",
            moment_topic_coherent=False,
            moment_participant_overlap=True,
            response_act="answer",
        )
        evidence = (
            build_conversation_evidence_item(
                text=glass_prompt,
                source_id=1,
                speaker_user_id=101,
                speaker_label="Test Member",
            ),
            build_conversation_evidence_item(
                text=copper_prompt,
                source_id=2,
                speaker_user_id=101,
                speaker_label="Test Member",
                current_turn=True,
            ),
        )

        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="sealed_test",
            current_speaker_user_ids=(101,),
            participant_user_ids=(101,),
            speaker_labels=("Test Member",),
            current_exchange_source_ids=(2,),
            prompt_lanes=("current_exchange", "conversation_context"),
            continuity_required=True,
            current_text=copper_prompt,
            conversation_evidence_items=evidence,
            situation_frame=frame,
        )
        response = (
            "The current task is antenna calibration for Project "
            "Copper Kite. Archiving the notes remains open."
        )
        coherence = assess_response_coherence(assessment, response)

        self.assertEqual(frame.event_relation, "new_event_same_participant")
        self.assertEqual(
            tuple(
                criterion.source_id
                for criterion in assessment.attributed_criteria
            ),
            (2,),
        )
        self.assertEqual(len(assessment.conversation_evidence_items), 2)
        self.assertEqual(coherence.status, "passed")
        self.assertEqual(coherence.criterion_status, "covered")
        self.assertNotIn(
            "attributed_criterion_partial",
            coherence.reason_codes,
        )
        conn = sqlite3.connect(":memory:")
        try:
            persist_shadow_run(conn, assessment, response=response)
            self.assertEqual(
                conn.execute(
                    "SELECT response_alignment FROM "
                    "unified_response_assessment_shadow_runs"
                ).fetchone()[0],
                "guard_clear",
            )
        finally:
            conn.close()

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
                        speaker_label="Test Member",
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
                    speaker_labels=("Test Member", "Miss Bit"),
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

    def test_receipt_reports_scoped_canary_without_new_authority(self):
        conn = sqlite3.connect(":memory:")
        try:
            assessment = self.shared_choice_assessment()
            persist_shadow_run(
                conn,
                assessment,
                response=(
                    "Null Basilica fits better because it reads as a place "
                    "instead of a person."
                ),
                guard_diagnostics={
                    "unified_moment_canary_applied": True,
                    "unified_moment_canary_scope_valid": True,
                    "unified_moment_canary_episode_context": True,
                    "unified_moment_canary_coherence_guard_triggered": True,
                    "unified_moment_canary_coherence_regenerated": True,
                },
            )
            conn.commit()
            row = conn.execute(
                """
                SELECT behavior_changed,new_authority_applied,
                       scoped_canary_applied,
                       scoped_canary_scope_valid,
                       scoped_canary_episode_context,
                       scoped_canary_guard_triggered,
                       scoped_canary_guard_repaired,
                       scoped_canary_output_leak_guard
                FROM unified_response_assessment_shadow_runs
                """
            ).fetchone()
            self.assertEqual(row, (0, 0, 1, 1, 1, 1, 1, 0))
            report = build_evaluation_report(conn, guild_id=1)
            self.assertEqual(report["scoped_canary_runs"], 1)
            self.assertEqual(
                report["scoped_canary_episode_context_runs"],
                1,
            )
            self.assertEqual(
                report["scoped_canary_guard_triggered_runs"],
                1,
            )
            self.assertEqual(
                report["scoped_canary_guard_repaired_runs"],
                1,
            )
            self.assertEqual(
                report["scoped_canary_invalid_scope_runs"],
                0,
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
                "profile_sufficiency_status",
                "profile_sufficiency_met",
                "profile_required_point_count",
                "profile_selected_point_count",
                "profile_independent_root_count",
                "profile_independent_occurrence_count",
                "profile_sufficiency_reasons_json",
                "scoped_canary_applied",
                "scoped_canary_scope_valid",
                "scoped_canary_episode_context",
                "scoped_canary_guard_triggered",
                "scoped_canary_guard_repaired",
                "scoped_canary_output_leak_guard",
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
