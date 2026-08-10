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
