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
    music_submission_history_query,
    music_submission_history_requested,
    persist_shadow_run,
    render_situation_frame_receipt,
    revalidate_situation_frame,
    situation_request_clauses,
    situation_task_texts,
    source_dependent_task_texts,
)


class PrefacedSituationRequestTests(unittest.TestCase):
    def test_explicit_instruction_colon_keeps_history_in_its_task(self):
        clause = "Please check: which of my songs have other people submitted to past shows"
        text = clause + "? Give me the track and artist credit."
        self.assertIn(clause, situation_request_clauses(text))
        self.assertEqual(music_submission_history_query(text), clause)
        self.assertTrue(music_submission_history_requested(text))

    def test_conversational_prefix_preserves_historical_submission_request(self):
        question = "which of my songs have other people submitted to past shows"
        for prefix in (
            "Archive gremlin, I need a crate inspection:",
            "Quick archive check:",
            "One question for the archive:",
        ):
            with self.subTest(prefix=prefix):
                text = (prefix + " " + question + "? Give me the track, artist credit, "
                        "TikTok submitter profile, and show date.")
                self.assertIn(question, "\n".join(situation_request_clauses(text)))
                self.assertIn(question, music_submission_history_query(text))
                self.assertNotIn("TikTok submitter profile", music_submission_history_query(text))
                self.assertTrue(music_submission_history_requested(text))
                frame = build_situation_frame_v1(
                    route_allowed=True, route_mode="normal_chat",
                    conversation_surface="public_context", channel_policy="public_context",
                    current_text=text, current_speaker_user_ids=(101,), response_act="answer",
                )
                self.assertEqual(tuple(subject.user_id for subject in frame.subjects), (101,))
                self.assertTrue(any(task.subject_indexes for task in frame.tasks))

    def test_prefaced_auxiliary_question_keeps_history_scope(self):
        for question in (
            "were my songs submitted to past shows",
            "can you list my tracks that appeared in past shows",
        ):
            with self.subTest(question=question):
                text = "A question for the archive: " + question + "? Give me artist credits."
                self.assertIn(question, music_submission_history_query(text))
                self.assertNotIn("Give me artist credits", music_submission_history_query(text))

    def test_prefaced_external_question_remains_an_independent_task(self):
        text = "A question for the booth: where is Seattle? Then explain how stars form."
        clauses = situation_request_clauses(text)
        self.assertEqual(len(clauses), 2)
        self.assertIn("where is Seattle", clauses[0])
        self.assertEqual(clauses[1], "explain how stars form")

    def test_title_declaration_does_not_become_a_prefaced_history_request(self):
        text = (
            "The album title is Archive: Which Songs Were Submitted to Past Shows. "
            "Explain checksum detection."
        )
        self.assertEqual(situation_request_clauses(text), ("Explain checksum detection",))
        self.assertEqual(music_submission_history_query(text), "")
        self.assertFalse(music_submission_history_requested(text))

    def test_incidental_journal_setup_does_not_own_prefaced_weather_question(self):
        text = (
            "I read your latest Journal. A quick question: "
            "what is the weather in Seattle today? Then explain how stars form."
        )
        frame = build_situation_frame_v1(
            route_allowed=True, route_mode="normal_chat",
            conversation_surface="public_home", channel_policy="public_home",
            current_text=text, current_speaker_user_ids=(101,), response_act="answer",
        )
        self.assertEqual(len(frame.tasks), 2)
        self.assertEqual(frame.tasks[0].subject_indexes, ())
        self.assertEqual(frame.tasks[0].authority_scope, "external_current")
        self.assertEqual(frame.tasks[0].required_response_act, "hold")
        self.assertEqual(frame.tasks[1].authority_scope, "external_public")
        self.assertEqual(frame.tasks[1].required_response_act, "answer")

    def test_explicit_show_date_prefix_remains_in_history_scope(self):
        clause = "For the September 25 show: which of my songs were submitted"
        for suffix in ("?", "? Give me track names."):
            with self.subTest(suffix=suffix):
                text = clause + suffix
                self.assertEqual(music_submission_history_query(text), clause)
                self.assertIn(clause, situation_request_clauses(text))

    def test_quoted_and_background_colons_do_not_promote_history_to_current_request(self):
        for setup in (
            'The fictional note says "Archive check: which of my songs were submitted?".',
            "The fictional note says `Archive check: which of my songs were submitted?`.",
            "Background: Test Artist submitted songs to past shows.",
            "Background: Will Somebody submitted songs to past shows.",
        ):
            with self.subTest(setup=setup):
                text = setup + " Explain checksum detection."
                self.assertEqual(situation_request_clauses(text), ("Explain checksum detection",))
                self.assertFalse(music_submission_history_requested(text))

    def test_url_time_and_title_colons_do_not_create_additional_requests(self):
        for text, clause in (
            ("Please inspect https://example.invalid/archive:which.",
             "Please inspect https://example.invalid/archive:which"),
            ("Please explain the 12:30 show timestamp.",
             "Please explain the 12:30 show timestamp"),
            ('Tell me about "Test Album: What We Remember" as a title.',
             'Tell me about "Test Album: What We Remember" as a title'),
            ("Tell me about Test Album: What We Remember as a title.",
             "Tell me about Test Album: What We Remember as a title"),
        ):
            with self.subTest(text=text):
                self.assertEqual(situation_request_clauses(text), (clause,))
                self.assertFalse(music_submission_history_requested(text))


class ResolvedDependentRequestTests(unittest.TestCase):
    def _frame(self, text, *, status="resolved", source_ids=(11, 12, 13)):
        return build_situation_frame_v1(
            route_allowed=True, route_mode="normal_chat",
            conversation_surface="public_home", channel_policy="public_home",
            current_text=text, current_speaker_user_ids=(101,), response_act="answer",
            referent_status=status, exact_source_row_ids=source_ids,
        )

    def _support(self, frame, *, lane="source_file", evidence=True):
        from types import SimpleNamespace
        from bnl_shared_brain_synthesis import ordinary_chat_task_support_plan

        basis = SimpleNamespace(
            packet=SimpleNamespace(request=SimpleNamespace(frame_tasks=frame.tasks)),
            rendered_evidence_refs=(("E1", lane, "fixture-source-digest", ()),) if evidence else (),
        )
        return ordinary_chat_task_support_plan(basis)

    def test_resolved_same_item_followups_use_selected_source_support(self):
        for text, lane in (
            ("And what is the full artist credit for that same track?", "show_episode"),
            ("And what is the model number for that same device?", "source_file"),
        ):
            with self.subTest(text=text):
                frame = self._frame(text)
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(frame.tasks[0].required_response_act, "answer")
                support = self._support(frame, lane=lane)[0]
                self.assertEqual((support.support_kind, support.evidence_ids), ("packet", ("E1",)))
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), (text.rstrip("?"),))

    def test_resolved_subset_followups_use_selected_source_support(self):
        for text in (
            "Which of those has the featured-artist credit?",
            "Which of those has the longer warranty?",
        ):
            with self.subTest(text=text):
                frame = self._frame(text)
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(frame.tasks[0].required_response_act, "answer")
                support = self._support(frame)[0]
                self.assertEqual((support.support_kind, support.evidence_ids), ("packet", ("E1",)))
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), (text.rstrip("?"),))

    def test_resolved_same_item_without_packet_evidence_holds(self):
        frame = self._frame("What is the model number for that same device?")
        support = self._support(frame, evidence=False)[0]
        self.assertEqual((support.support_kind, support.evidence_ids), ("hold", ()))

    def test_unbound_or_ambiguous_reference_does_not_gain_packet_authority(self):
        text = "What is the model number for that same device?"
        for status, source_ids in (
            ("not_requested", (11, 12, 13)),
            ("unresolved", (11, 12, 13)),
            ("ambiguous", (11, 12, 13)),
            ("resolved", ()),
        ):
            with self.subTest(status=status, source_ids=source_ids):
                frame = self._frame(text, status=status, source_ids=source_ids)
                self.assertNotEqual(frame.tasks[0].authority_scope, "packet")
                support = self._support(frame)[0]
                self.assertNotEqual(support.support_kind, "packet")
                self.assertNotIn("E1", support.evidence_ids)
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), ())

    def test_resolved_context_does_not_own_independent_questions_or_current_payload(self):
        for text, authority, response, support_kind, evidence_ids in (
            ("Where is Seattle?", "external_public", "answer", "external_public", ("PUBLIC",)),
            ("What is Seattle's weather today?", "external_current", "hold", "hold", ()),
            ("Please help rewrite this request: what is the model number for that same device?",
             "current_request", "answer", "current_request", ("REQUEST",)),
        ):
            with self.subTest(text=text):
                frame = self._frame(text)
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].authority_scope, authority)
                self.assertEqual(frame.tasks[0].required_response_act, response)
                support = self._support(frame)[0]
                self.assertEqual((support.support_kind, support.evidence_ids), (support_kind, evidence_ids))
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), ())

    def test_mixed_dependent_and_external_tasks_keep_separate_support(self):
        text = "What is the full artist credit for that same track? Then where is Seattle?"
        frame = self._frame(text)
        self.assertEqual(tuple(task.authority_scope for task in frame.tasks), ("packet", "external_public"))
        self.assertEqual(tuple(task.required_response_act for task in frame.tasks), ("answer", "answer"))
        support = self._support(frame, lane="show_episode")
        self.assertEqual(
            tuple((item.support_kind, item.evidence_ids) for item in support),
            (("packet", ("E1",)), ("external_public", ("PUBLIC",))),
        )
        self.assertEqual(
            source_dependent_task_texts(frame, current_text=text),
            ("What is the full artist credit for that same track",),
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

    def _task_context_frame(self, text, labels=("Test Member",)):
        return build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text=text,
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Listener",),
            subject_user_ids=tuple(range(201, 201 + len(labels))),
            subject_labels_by_user_id=dict(
                zip(range(201, 201 + len(labels)), labels)
            ),
            response_act="answer",
        )

    def test_task_lead_in_retains_main_request_and_audience(self):
        for label in ("Test Member", "Fixture Finch"):
            text = (
                "Awesome. Now introduce %s to someone new to BARCODE. "
                "What should they know, and what is your own take?" % label
            )
            with self.subTest(label=label):
                frame = self._task_context_frame(text, (label,))
                segments = situation_task_texts(frame, current_text=text)
                self.assertEqual(len(segments), 3)
                self.assertEqual(
                    segments[0],
                    "Awesome. Now introduce %s to someone new to BARCODE" % label,
                )
                self.assertEqual(segments[1], "What should they know")
                self.assertEqual(segments[2], "what is your own take")
                self.assertEqual(
                    tuple(task.subject_indexes for task in frame.tasks),
                    ((0,), (0,), (0,)),
                )
                self.assertEqual(
                    tuple(task.authority_scope for task in frame.tasks),
                    ("packet", "packet", "packet"),
                )

    def test_task_context_is_not_an_extra_request(self):
        for prefix in (
            "Awesome.",
            "Fixture Listener has just arrived.",
            "Test Listener has just arrived.",
        ):
            text = (
                "%s What would help them get acquainted with Test Member, "
                "and what impression have you formed?" % prefix
            )
            with self.subTest(prefix=prefix):
                frame = self._task_context_frame(text)
                segments = situation_task_texts(frame, current_text=text)
                self.assertEqual(len(segments), 2)
                self.assertTrue(segments[0].startswith(prefix + " "))
                self.assertEqual(
                    tuple(task.subject_indexes for task in frame.tasks),
                    ((0,), (0,)),
                )
                self.assertEqual(
                    tuple(task.authority_scope for task in frame.tasks),
                    ("packet", "packet"),
                )

    def test_task_elliptical_evaluations_keep_immediate_subject(self):
        for evaluation in (
            "what is your own take",
            "what do you think",
            "what are your thoughts",
            "what impression have you formed",
            "how do you feel about that",
        ):
            text = "Now tell me about Test Member. %s?" % evaluation
            with self.subTest(evaluation=evaluation):
                frame = self._task_context_frame(text)
                self.assertEqual(len(frame.tasks), 2)
                self.assertEqual(frame.tasks[0].subject_indexes, (0,))
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(
                    "Now tell me about Test Member",
                    situation_task_texts(frame, current_text=text)[0],
                )
                self.assertEqual(frame.tasks[1].subject_indexes, (0,))
                self.assertEqual(frame.tasks[1].authority_scope, "packet")

                explicit_text = "Tell me about Test Member, and %s?" % evaluation
                explicit_frame = self._task_context_frame(explicit_text)
                self.assertEqual(len(explicit_frame.tasks), 2)
                self.assertEqual(explicit_frame.tasks[1].subject_indexes, (0,))
                self.assertEqual(explicit_frame.tasks[1].authority_scope, "packet")

    def test_task_independent_external_question_keeps_its_authority(self):
        for followup, expected in (
            ("explain how stars form", "external_public"),
            ("where is Seattle", "external_public"),
            ("what is your own take on Neptune", "external_public"),
            ("what is your opinion of Neptune", "current_request"),
            ("what is Seattle's weather today", "external_current"),
        ):
            text = "Tell me about Test Member, and %s?" % followup
            with self.subTest(followup=followup):
                frame = self._task_context_frame(text)
                self.assertEqual(len(frame.tasks), 2)
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(frame.tasks[1].subject_indexes, ())
                self.assertEqual(frame.tasks[1].authority_scope, expected)
                if expected == "external_current":
                    self.assertEqual(frame.tasks[1].required_response_act, "hold")

    def test_task_unrecognized_imperative_does_not_absorb_external_question(self):
        for request in (
            "Introduce Test Member to someone new",
            "Walk us through Test Member's latest contribution",
            "Awesome. Now tell us about Test Member",
        ):
            text = "%s, and where is Seattle?" % request
            with self.subTest(request=request):
                frame = self._task_context_frame(text)
                self.assertEqual(
                    situation_task_texts(frame, current_text=text),
                    (request, "where is Seattle"),
                )
                self.assertEqual(frame.tasks[0].subject_indexes, (0,))
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(frame.tasks[1].subject_indexes, ())
                self.assertEqual(frame.tasks[1].authority_scope, "external_public")

    def test_task_setup_identity_does_not_lend_authority_to_external_question(self):
        for setup, question, authority, act in (
            (
                "Fixture Finch has just arrived.",
                "What is Seattle's weather today?",
                "external_current",
                "hold",
            ),
            (
                "Fixture Finch is here.",
                "Where is Seattle?",
                "external_public",
                "answer",
            ),
            (
                "Fixture Finch is making dinner.",
                "What is your own take on Neptune?",
                "external_public",
                "answer",
            ),
        ):
            text = "%s %s" % (setup, question)
            with self.subTest(text=text):
                frame = self._task_context_frame(text, ("Fixture Finch",))
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].subject_indexes, ())
                self.assertEqual(frame.tasks[0].authority_scope, authority)
                self.assertEqual(frame.tasks[0].required_response_act, act)
                self.assertEqual(
                    situation_task_texts(frame, current_text=text),
                    (text.rstrip("?"),),
                )

    def test_task_coordinated_subject_setup_preserves_pronoun_ambiguity(self):
        for setup, labels in (
            (
                "Fixture Finch and Fixture Moss are both part of BARCODE.",
                ("Fixture Finch", "Fixture Moss"),
            ),
            (
                "Fixture Finch, Fixture Moss, and Fixture Reed are part of BARCODE.",
                ("Fixture Finch", "Fixture Moss", "Fixture Reed"),
            ),
        ):
            text = setup + " What is his role in the Network?"
            with self.subTest(setup=setup):
                frame = self._task_context_frame(text, labels)
                self.assertEqual(frame.status, "ambiguous")
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertEqual(frame.tasks[0].required_response_act, "clarify")
                self.assertEqual(frame.tasks[0].subject_indexes, ())
                self.assertEqual(
                    situation_task_texts(frame, current_text=text),
                    (text.rstrip("?"),),
                )

    def test_task_request_clause_view_does_not_retarget_incidental_correction(self):
        text = (
            "Fixture Finch is a guy. He told you that. Also, did you notice "
            "any tension or chemistry between people?"
        )
        frame = self._task_context_frame(text, ("Fixture Finch",))
        self.assertEqual(
            situation_task_texts(frame, current_text=text),
            (text.rstrip("?"),),
        )
        self.assertEqual(
            situation_request_clauses(text, context_labels=("Fixture Finch",)),
            ("did you notice any tension or chemistry between people",),
        )
        # Readers without a bound label still exclude a non-task lead-in.
        self.assertEqual(
            situation_request_clauses(text),
            ("did you notice any tension or chemistry between people",),
        )

    def test_task_request_clause_view_preserves_explicit_member_recall(self):
        for text, expected in (
            (
                "Fixture Finch is a guy. What do you remember about Fixture Finch?",
                ("What do you remember about Fixture Finch",),
            ),
            (
                "Walk us through Fixture Finch's latest contribution, and where is Seattle?",
                ("Walk us through Fixture Finch's latest contribution", "where is Seattle"),
            ),
        ):
            with self.subTest(text=text):
                self.assertEqual(
                    situation_request_clauses(text, context_labels=("Fixture Finch",)),
                    expected,
                )

    def test_task_explicit_new_subject_wins_before_an_elliptical_followup(self):
        text = (
            "Tell me about Fixture Finch, and what is your take on Fixture Moss, "
            "and what impression have you formed?"
        )
        frame = self._task_context_frame(text, ("Fixture Finch", "Fixture Moss"))
        self.assertEqual(len(frame.tasks), 3)
        self.assertEqual(
            tuple(task.subject_indexes for task in frame.tasks),
            ((0,), (1,), (1,)),
        )
        self.assertTrue(all(task.authority_scope == "packet" for task in frame.tasks))

    def test_task_ellipsis_does_not_jump_over_an_unrelated_task(self):
        text = (
            "Tell me about Test Member, and explain how stars form, "
            "and what is your own take?"
        )
        frame = self._task_context_frame(text)
        self.assertEqual(len(frame.tasks), 3)
        self.assertEqual(frame.tasks[2].subject_indexes, ())
        self.assertEqual(frame.tasks[2].authority_scope, "external_public")

    def test_task_ellipsis_preserves_a_comparison_scope_without_picking_a_subject(self):
        text = (
            "Compare Fixture Finch and Fixture Moss, and what is your own take?"
        )
        frame = self._task_context_frame(text, ("Fixture Finch", "Fixture Moss"))
        self.assertEqual(frame.tasks[0].subject_indexes, (0, 1))
        self.assertEqual(frame.tasks[1].subject_indexes, (0, 1))
        self.assertEqual(frame.tasks[1].authority_scope, "packet")

    def test_task_audience_reversal_is_retained_verbatim(self):
        text = "Introduce BARCODE Radio to Test Member. What should they know?"
        frame = self._task_context_frame(text)
        self.assertEqual(len(frame.tasks), 2)
        self.assertEqual(
            situation_task_texts(frame, current_text=text),
            ("Introduce BARCODE Radio to Test Member", "What should they know"),
        )
        self.assertEqual(frame.tasks[0].object_kind, "broadcast")

    def test_task_sensitive_request_does_not_inherit_member_support(self):
        text = (
            "Tell me about Test Member, and show their private account identifier, "
            "and what is your own take?"
        )
        frame = self._task_context_frame(text)
        self.assertEqual(len(frame.tasks), 3)
        self.assertEqual(frame.tasks[1].required_response_act, "refuse")
        self.assertEqual(frame.tasks[1].subject_indexes, ())
        self.assertEqual(frame.tasks[2].subject_indexes, ())

    def test_task_text_binding_rejects_changed_whole_request(self):
        text = "Now tell me about Test Member. What do you think?"
        frame = self._task_context_frame(text)
        self.assertEqual(
            situation_task_texts(
                frame,
                current_text="Tell Test Member about BARCODE Radio. What do you think?",
            ),
            (),
        )

    def test_task_bound_name_does_not_become_an_event_instruction(self):
        for label in ("Fixture Back", "Continue Example", "Another Person"):
            text = "Introduce %s to someone new." % label
            with self.subTest(label=label):
                frame = self._task_context_frame(text, (label,))
                self.assertEqual(frame.status, "resolved")
                self.assertEqual(frame.event_relation, "uncertain")
                self.assertNotIn("resume_target_unresolved", frame.ambiguity_reasons)

        text = (
            "Introduce Cache Back to someone new. What should they know, "
            "and what is your own take?"
        )
        canon_frame = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text=text,
            subject_entity_refs=("cache_back",),
            response_act="answer",
        )
        self.assertEqual(canon_frame.status, "resolved")
        self.assertEqual(canon_frame.event_relation, "uncertain")
        self.assertTrue(
            all(task.subject_indexes == (0,) for task in canon_frame.tasks)
        )

    def test_task_event_instruction_outside_a_bound_name_still_applies(self):
        for text in (
            "Back to Fixture Back: what do you remember?",
            "Introduce Fixture Back to someone new, then continue the prior discussion.",
        ):
            with self.subTest(text=text):
                frame = self._task_context_frame(text, ("Fixture Back",))
                self.assertEqual(frame.event_relation, "resume_unresolved")
                self.assertIn("resume_target_unresolved", frame.ambiguity_reasons)

        for text, expected in (
            ("A different event involves Fixture Back.", "new_event_or_uncertain"),
            ("Meanwhile, tell me about Fixture Back.", "concurrent_activity"),
            (
                "Tell a different participant about Fixture Back.",
                "comparison_or_participant_change",
            ),
        ):
            with self.subTest(text=text):
                frame = self._task_context_frame(text, ("Fixture Back",))
                self.assertEqual(frame.event_relation, expected)

        resumed = build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_policy="public_home",
            current_text="Back to Cache Back: continue our discussion.",
            subject_entity_refs=("cache_back",),
            moment_id="moment_test_01",
            moment_situation_state="recent_active",
            moment_topic_coherent=True,
            moment_participant_overlap=True,
            response_act="answer",
        )
        self.assertEqual(resumed.event_relation, "resume")
        self.assertNotIn("resume_target_unresolved", resumed.ambiguity_reasons)

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
