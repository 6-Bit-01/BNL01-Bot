import asyncio
import json
import os
import sqlite3
import unittest
from dataclasses import FrozenInstanceError

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
from bnl_moment_engine import MomentSituationReference
from bnl_unified_response_assessment import (
    FRAME_SOURCE_REVALIDATION_VERSION,
    SITUATION_FRAME_VERSION,
    build_situation_frame_v1,
    build_unified_response_assessment,
    persist_shadow_run,
    render_situation_frame_receipt,
    revalidate_situation_frame,
)


def addressing(**overrides):
    values = {
        "speaker": "Test Member",
        "explicit_tag_recipients": ("@BNL-01",),
        "reply_target": "none",
        "explicitly_mentions_bnl": True,
        "reply_targets_bnl": False,
        "directly_targets_bnl": True,
        "targets_other_human": False,
        "plain_text_names_bnl": False,
        "source_message_id": 301,
    }
    values.update(overrides)
    return bnl01_bot.DiscordTurnAddressing(**values)


def moment(**overrides):
    values = {
        "moment_id": "moment_test_01",
        "lifecycle_status": "active",
        "qualification_type": "topic_activity",
        "qualification_reason": "eligible_human_roots",
        "human_entry_count": 3,
        "model_entry_count": 1,
        "participant_count": 2,
        "participant_overlap": True,
        "topic_coherent": True,
        "last_activity_at": "2026-08-09T12:00:00+00:00",
    }
    values.update(overrides)
    return MomentSituationReference(**values)


class SituationFrameV1Tests(unittest.TestCase):
    def test_frame_is_deterministic_immutable_and_complete(self):
        kwargs = {
            "route_allowed": True,
            "route_mode": "normal_chat",
            "conversation_surface": "public_home",
            "channel_policy": "public_home",
            "current_text": "Please diagnose the current memory failure and retest it.",
            "current_speaker_user_ids": (101,),
            "current_speaker_labels": ("Test Member",),
            "addressee_kinds": ("discord_mention",),
            "source_message_ids": (301,),
            "reply_message_ids": (299,),
            "exact_source_row_ids": (88,),
            "explicit_mention_count": 1,
            "subject_user_ids": (101,),
            "moment_id": "moment_test_01",
            "moment_situation_state": "recent_active",
            "moment_topic_coherent": True,
            "moment_participant_overlap": True,
            "referent_status": "resolved",
            "response_act": "answer",
            "packet_revision": "turn_01",
        }
        first = build_situation_frame_v1(**kwargs)
        second = build_situation_frame_v1(**kwargs)

        self.assertEqual(first, second)
        self.assertEqual(first.schema_version, SITUATION_FRAME_VERSION)
        self.assertEqual(first.frame_revision, second.frame_revision)
        self.assertEqual(first.input_evidence_digest, second.input_evidence_digest)
        self.assertEqual(first.status, "resolved")
        self.assertEqual(first.phase, "retest")
        self.assertEqual(first.object_kind, "memory")
        self.assertEqual(first.event_relation, "same_event_new_phase")
        self.assertEqual(first.visibility_allowance, "public_safe")
        self.assertEqual(first.required_response_act, "answer")
        self.assertEqual(first.subjects[0].binding_method, "existing_typed_target")
        with self.assertRaises(FrozenInstanceError):
            first.status = "changed"

    def test_third_party_cue_never_falls_back_to_current_speaker(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="What do you know about Jordan?",
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            response_act="answer",
        )

        self.assertEqual(frame.subjects, ())
        self.assertEqual(frame.status, "ambiguous")
        self.assertIn("third_party_subject_unresolved", frame.ambiguity_reasons)
        self.assertIn("speaker_fallback_rejected", frame.competing_frames)

    def test_self_target_precedence_binds_the_current_speaker(self):
        cases = (
            "What do you remember about me?",
            "What do you know about me?",
            "Tell me who I am.",
            "What patterns keep recurring for me?",
        )
        for text in cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="public_home",
                    channel_policy="public_home",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    current_speaker_labels=("Test Member",),
                    response_act="answer",
                )

                self.assertEqual(frame.status, "resolved")
                self.assertEqual(len(frame.subjects), 1)
                self.assertEqual(frame.subjects[0].user_id, 101)
                self.assertEqual(
                    frame.subjects[0].binding_method,
                    "current_speaker_context",
                )
                self.assertNotIn(
                    "third_party_subject_unresolved",
                    frame.ambiguity_reasons,
                )

    def test_bnl_self_questions_bind_the_existing_canon_entity(self):
        cases = (
            "Who are you?",
            "What are you?",
            "Tell me about yourself.",
            "What do you remember about yourself?",
        )
        for text in cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="public_home",
                    channel_policy="public_home",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    current_speaker_labels=("Test Member",),
                    response_act="answer",
                )

                self.assertEqual(frame.status, "resolved")
                self.assertEqual(len(frame.subjects), 1)
                self.assertEqual(frame.subjects[0].entity_ref, "bnl_01")
                self.assertEqual(
                    frame.subjects[0].binding_method,
                    "existing_typed_entity",
                )

    def test_exact_discord_reply_uses_packet_authority_without_weakening_live_holds(self):
        exact_reply = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="sealed_test",
            current_text="what test code did i give you?",
            current_speaker_user_ids=(101,),
            reply_message_ids=(700,),
            exact_source_row_ids=(77,),
            referent_status="resolved",
            response_act="answer",
        )
        live_weather = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="sealed_test",
            current_text="what is Seattle's weather right now?",
            current_speaker_user_ids=(101,),
            reply_message_ids=(700,),
            exact_source_row_ids=(77,),
            referent_status="resolved",
            response_act="answer",
        )

        self.assertEqual(exact_reply.tasks[0].authority_scope, "packet")
        self.assertEqual(exact_reply.tasks[0].required_response_act, "answer")
        self.assertEqual(live_weather.tasks[0].authority_scope, "external_current")
        self.assertEqual(live_weather.tasks[0].required_response_act, "hold")

    def test_barcode_radio_queue_is_one_packet_owned_queue_task(self):
        questions = (
            "is the Barcode Radio queue open right now?",
            "Can I submit a track right now?",
            "Is the intake open for tracks?",
        )
        for question in questions:
            with self.subTest(question=question):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="mention_or_reply",
                    channel_policy="sealed_test",
                    current_text=question,
                    current_speaker_user_ids=(101,),
                    response_act="answer",
                )

                self.assertEqual(frame.object_kind, "queue")
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].object_kind, "queue")
                self.assertEqual(frame.tasks[0].authority_scope, "packet")

    def test_multiple_explicit_mentions_bind_to_one_task_without_ambiguity(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Compare <@202> and <@303>.",
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            subject_user_ids=(202, 303),
            subject_label_hints=("First Member", "Second Member"),
            response_act="answer",
        )

        self.assertEqual(frame.status, "resolved")
        self.assertEqual(
            tuple(subject.user_id for subject in frame.subjects),
            (202, 303),
        )
        self.assertEqual(frame.tasks[0].subject_indexes, (0, 1))

    def test_unscoped_second_candidate_still_fails_closed(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Tell me about <@202>.",
            current_speaker_user_ids=(101,),
            subject_user_ids=(202, 303),
            subject_label_hints=("First Member", "Second Member"),
            response_act="answer",
        )

        self.assertEqual(frame.status, "ambiguous")
        self.assertIn("multiple_subject_candidates", frame.ambiguity_reasons)

    def test_more_than_eight_scoped_subjects_fails_closed(self):
        subject_ids = tuple(range(201, 210))
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Compare %s."
            % " and ".join("<@%s>" % user_id for user_id in subject_ids),
            current_speaker_user_ids=(101,),
            subject_user_ids=subject_ids,
            response_act="answer",
        )

        self.assertEqual(frame.status, "ambiguous")
        self.assertIn(
            "subject_candidate_limit_exceeded",
            frame.ambiguity_reasons,
        )

    def test_self_and_explicit_subjects_are_scoped_to_separate_tasks(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text=(
                "What do you remember about me, and who is <@202>?"
            ),
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            subject_user_ids=(202,),
            subject_label_hints=("Other Member",),
            response_act="answer",
        )

        self.assertEqual(frame.status, "resolved")
        self.assertEqual(
            tuple(subject.user_id for subject in frame.subjects),
            (202, 101),
        )
        self.assertEqual(
            tuple(task.subject_indexes for task in frame.tasks),
            ((1,), (0,)),
        )

    def test_publication_owner_qualifies_the_subject_of_the_question(self):
        cases = (
            "What did the Journal say about the last show?",
            "What did the Relay say about the broadcast?",
            "Summarize the Journal entry about the queue.",
        )
        for text in cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="public_home",
                    channel_policy="public_home",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    response_act="answer",
                )

                expected = "relay" if "Relay" in text else "journal"
                self.assertEqual(frame.object_kind, expected)
                self.assertEqual(frame.task_kind, "retrieve_publication")
                self.assertEqual(frame.status, "resolved")

    def test_unresolved_publication_deictic_is_not_treated_as_a_topic(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="What did that Journal entry say?",
            current_speaker_user_ids=(101,),
            subject_entity_refs=("cache_back", "call_em_bini"),
            response_act="answer",
        )

        self.assertEqual(frame.status, "ambiguous")
        self.assertIn(
            "publication_referent_unresolved",
            frame.ambiguity_reasons,
        )
        self.assertIn(
            "multiple_subject_candidates",
            frame.ambiguity_reasons,
        )

        resolved = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="sealed_test",
            current_text="What did that Journal entry say?",
            current_speaker_user_ids=(101,),
            subject_entity_refs=("cache_back", "call_em_bini"),
            reply_message_ids=(700,),
            exact_source_row_ids=(77,),
            referent_status="resolved",
            response_act="answer",
        )

        self.assertNotIn(
            "publication_referent_unresolved",
            resolved.ambiguity_reasons,
        )

    def test_mixed_request_is_split_into_ordered_authority_tasks(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text=(
                "What do you remember about me, and where is Seattle?"
            ),
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            response_act="answer",
        )

        self.assertEqual(frame.status, "resolved")
        self.assertEqual(tuple(task.task_id for task in frame.tasks), ("T1", "T2"))
        self.assertEqual(
            tuple(task.authority_scope for task in frame.tasks),
            ("packet", "external_public"),
        )
        self.assertEqual(frame.tasks[0].subject_indexes, (0,))
        self.assertEqual(frame.tasks[1].subject_indexes, ())
        self.assertEqual(frame.task_kind, "multi_task")
        self.assertEqual(frame.object_kind, "multiple")

    def test_current_external_task_is_held_without_blocking_packet_task(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text=(
                "What do you remember about me, and what is Seattle's "
                "weather today?"
            ),
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            response_act="answer",
        )

        self.assertEqual(frame.status, "resolved")
        self.assertEqual(len(frame.tasks), 2)
        self.assertEqual(frame.tasks[0].required_response_act, "answer")
        self.assertEqual(frame.tasks[1].authority_scope, "external_current")
        self.assertEqual(frame.tasks[1].required_response_act, "hold")

    def test_role_domain_event_and_visibility_matrix(self):
        cases = (
            (
                "Tell me about Test Artist as an artist and community member.",
                "recent_active",
                True,
                True,
                "same_event",
                {"music", "real_community"},
            ),
            (
                "We are resuming the queue retest.",
                "recent_reopened",
                True,
                True,
                "resume",
                {"broadcast_history"},
            ),
            (
                "This is a different website failure.",
                "recent_active",
                False,
                True,
                "new_event_same_participant",
                {"technical"},
            ),
        )
        for text, state, coherent, overlap, relation, domains in cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True,
                    route_mode="normal_chat",
                    conversation_surface="sealed_test",
                    channel_policy="sealed_test",
                    current_text=text,
                    current_speaker_user_ids=(101,),
                    subject_label_hints=("Test Artist",),
                    moment_id="moment_test_01",
                    moment_situation_state=state,
                    moment_topic_coherent=coherent,
                    moment_participant_overlap=overlap,
                    response_act="answer",
                )
                self.assertEqual(frame.event_relation, relation)
                self.assertTrue(domains.issubset(set(frame.domain_hints)))
                self.assertEqual(frame.visibility_allowance, "sealed_test")

        blocked = build_situation_frame_v1(
            route_allowed=False,
            route_mode="normal_chat",
            conversation_surface="forbidden",
            channel_policy="forbidden",
            current_text="Tell me about the Journal.",
        )
        self.assertEqual(blocked.status, "blocked")
        self.assertEqual(blocked.visibility_allowance, "blocked")

    def test_scene_transition_language_is_typed_without_guessing(self):
        base = {
            "route_allowed": True,
            "route_mode": "normal_chat",
            "conversation_surface": "public_home",
            "channel_policy": "public_home",
            "current_speaker_user_ids": (101,),
            "subject_user_ids": (101,),
            "moment_id": "moment_test_01",
            "moment_situation_state": "recent_active",
            "moment_topic_coherent": True,
            "moment_participant_overlap": True,
            "response_act": "answer",
        }
        cases = (
            (
                "This is another failure, not the same incident.",
                "new_event_same_participant",
            ),
            (
                "This is not a separate task; continue the same incident.",
                "resume",
            ),
            (
                "No new task here; keep working on the same incident.",
                "same_event",
            ),
            (
                "Is this a separate task, or should we continue?",
                "resume",
            ),
            (
                "So, is this a separate task?",
                "same_event",
            ),
            (
                "Isn't this a separate task?",
                "same_event",
            ),
            (
                "This is a separate task?",
                "same_event",
            ),
            (
                "Does this count as a separate task?",
                "same_event",
            ),
            (
                "Is the decoder work a separate task?",
                "same_event",
            ),
            (
                "This is a separate task, right?",
                "same_event",
            ),
            (
                "Maybe this is a separate task; I am not sure yet.",
                "same_event",
            ),
            (
                "It might be a separate task.",
                "same_event",
            ),
            (
                "Can you start another task?",
                "new_event_same_participant",
            ),
            (
                "Even if it looks similar, this is a separate incident.",
                "new_event_same_participant",
            ),
            (
                "This is a separate task. What remains open?",
                "new_event_same_participant",
            ),
            (
                "Meanwhile, keep the synth retest running in parallel.",
                "concurrent_activity",
            ),
            (
                "A different participant is handling the synth retest.",
                "comparison_or_participant_change",
            ),
            (
                "Correction: use the warmer synth patch instead.",
                "same_event_new_phase",
            ),
        )
        for text, expected in cases:
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    current_text=text,
                    **base,
                )
                self.assertEqual(frame.event_relation, expected)

        unresolved = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Coming back to this: continue the synth retest.",
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_situation_state="none",
            moment_topic_coherent=False,
            moment_participant_overlap=False,
            response_act="answer",
        )
        self.assertEqual(unresolved.event_relation, "resume_unresolved")
        self.assertEqual(unresolved.status, "ambiguous")
        self.assertIn("resume_target_unresolved", unresolved.ambiguity_reasons)
        self.assertIn("resume_episode_candidates", unresolved.competing_frames)

    def test_isolated_failure_is_a_diagnosis_phase_transition(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=(
                "Project Glass Harbor update: the relay test passed, so the "
                "transport route is clean. The amber failure is now isolated "
                "to the decoder input. What changed, and what is still "
                "unresolved?"
            ),
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_id="moment_glass_harbor",
            moment_situation_state="recent_active",
            moment_topic_coherent=True,
            moment_participant_overlap=True,
            response_act="answer",
        )

        self.assertEqual(frame.phase, "diagnosis")
        self.assertEqual(frame.event_relation, "same_event_new_phase")
        self.assertEqual(frame.status, "resolved")

    def test_narrow_execution_window_is_not_a_diagnosis(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=(
                "There is a narrow window to deploy the decoder fix."
            ),
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_id="moment_glass_harbor",
            moment_situation_state="recent_active",
            moment_topic_coherent=True,
            moment_participant_overlap=True,
            response_act="answer",
        )

        self.assertEqual(frame.phase, "execution")
        self.assertEqual(frame.tasks[0].task_kind, "execution")
        self.assertEqual(frame.event_relation, "same_event")

        diagnosis = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=(
                "We narrowed the amber failure to the decoder input."
            ),
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_id="moment_glass_harbor",
            moment_situation_state="recent_active",
            moment_topic_coherent=True,
            moment_participant_overlap=True,
            response_act="answer",
        )
        self.assertEqual(diagnosis.phase, "diagnosis")
        self.assertEqual(diagnosis.tasks[0].task_kind, "diagnosis")
        self.assertEqual(
            diagnosis.event_relation,
            "same_event_new_phase",
        )

        narrowing = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=(
                "We are narrowing the crash to the decoder input."
            ),
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_id="moment_glass_harbor",
            moment_situation_state="recent_active",
            moment_topic_coherent=True,
            moment_participant_overlap=True,
            response_act="answer",
        )
        self.assertEqual(narrowing.phase, "diagnosis")

        deployment_window = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="sealed_test",
            channel_policy="sealed_test",
            current_text=(
                "After the failure, we narrowed the deployment window "
                "to ten minutes."
            ),
            current_speaker_user_ids=(101,),
            subject_user_ids=(101,),
            moment_id="moment_glass_harbor",
            moment_situation_state="recent_active",
            moment_topic_coherent=True,
            moment_participant_overlap=True,
            response_act="answer",
        )
        self.assertNotEqual(deployment_window.phase, "diagnosis")

    def test_revalidation_is_separate_and_fails_closed_by_state(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Explain the current Journal entry.",
            current_speaker_user_ids=(101,),
            response_act="answer",
        )
        valid = revalidate_situation_frame(
            frame,
            current_text="Explain the current Journal entry.",
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            packet_source_snapshot_digest="packet_digest_01",
        )
        stale = revalidate_situation_frame(
            frame,
            current_text="Explain a different Journal entry.",
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
        )
        invalid = revalidate_situation_frame(
            frame,
            current_text="Explain the current Journal entry.",
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            source_status="deleted",
        )

        self.assertEqual(valid.schema_version, FRAME_SOURCE_REVALIDATION_VERSION)
        self.assertEqual(valid.status, "valid")
        self.assertEqual(stale.status, "stale")
        self.assertIn("current_text_changed", stale.reason_codes)
        self.assertEqual(invalid.status, "invalid")
        self.assertIn("source_deleted", invalid.reason_codes)
        self.assertEqual(frame.status, "resolved")

    def test_content_free_receipt_has_no_text_labels_or_account_ids(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Explain the current memory repair.",
            current_speaker_user_ids=(424242,),
            current_speaker_labels=("Test Member",),
            subject_user_ids=(424242,),
            subject_label_hints=("Test Member",),
            response_act="answer",
        )
        result = revalidate_situation_frame(
            frame,
            current_text="Explain the current memory repair.",
            route_mode="normal_chat",
            channel_policy="public_home",
        )
        receipt = render_situation_frame_receipt(frame, result)
        serialized = json.dumps(receipt, sort_keys=True)

        self.assertNotIn("Explain the current memory repair", serialized)
        self.assertNotIn("Test Member", serialized)
        self.assertNotIn("424242", serialized)
        self.assertEqual(receipt["mutationCount"], 0)
        self.assertEqual(receipt["revalidationStatus"], "valid")

    def test_live_coordinator_builds_frame_without_prompt_influence(self):
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="direct_request",
            channel_policy="public_home",
            addressings=(addressing(),),
            context_result=None,
            moment_situation=moment(),
            guild_id=7,
            channel_id=8,
            route_mode="normal_chat",
            conversation_surface="public_home",
            current_text="Please diagnose the memory failure.",
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            influence_mode="live",
            packet_revision="turn_02",
        )
        rendered = bnl01_bot.render_conversation_orchestration_prompt(decision)

        self.assertIsNotNone(decision.situation_frame)
        self.assertEqual(decision.situation_frame.current_speaker_user_ids, (101,))
        self.assertNotIn("SITUATION_FRAME", rendered)
        self.assertNotIn(decision.situation_frame.input_evidence_digest, rendered)

    def test_assessment_receipt_persists_only_content_free_frame_state(self):
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Explain the current memory repair.",
            current_speaker_user_ids=(424242,),
            current_speaker_labels=("Test Member",),
            response_act="answer",
        )
        revalidation = revalidate_situation_frame(
            frame,
            current_text="Explain the current memory repair.",
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
        )
        assessment = build_unified_response_assessment(
            guild_id=7,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="public_home",
            current_speaker_user_ids=(424242,),
            current_text="Explain the current memory repair.",
            situation_frame=frame,
            frame_revalidation=revalidation,
        )
        conn = sqlite3.connect(":memory:")
        run_id = persist_shadow_run(
            conn,
            assessment,
            response="The current repair is in shadow verification.",
        )
        row = conn.execute(
            "SELECT situation_frame_version,situation_frame_revision,"
            "situation_frame_input_digest,situation_frame_status,"
            "situation_frame_ambiguity_count,frame_revalidation_status,"
            "frame_revalidation_reason_count "
            "FROM unified_response_assessment_shadow_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        raw_row = json.dumps(row)

        self.assertEqual(row[0], SITUATION_FRAME_VERSION)
        self.assertEqual(row[1], frame.frame_revision)
        self.assertEqual(row[2], frame.input_evidence_digest)
        self.assertEqual(row[3], "resolved")
        self.assertEqual(row[5], "valid")
        self.assertNotIn("Test Member", raw_row)
        self.assertNotIn("424242", raw_row)

    def test_guard_revalidates_in_shadow_without_changing_response(self):
        text = "Explain the current memory repair."
        frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text=text,
            current_speaker_user_ids=(101,),
            response_act="answer",
        )
        response = "The memory repair is ready for the next automated check."
        validated, diagnostics = asyncio.run(
            bnl01_bot.apply_guarded_response_regeneration(
                response,
                prompt="",
                user_id=101,
                guild_id=7,
                route_mode="normal_chat",
                channel_policy="public_home",
                current_user_text=text,
                source_context_available=True,
                regeneration_allowed=False,
                situation_frame=frame,
            )
        )

        self.assertEqual(validated, response)
        self.assertFalse(diagnostics["suppressed"])
        self.assertEqual(
            diagnostics["situation_frame_revalidation_status"],
            "valid",
        )
        self.assertEqual(
            diagnostics["_situation_frame_revalidation"].frame_revision,
            frame.frame_revision,
        )


if __name__ == "__main__":
    unittest.main()
