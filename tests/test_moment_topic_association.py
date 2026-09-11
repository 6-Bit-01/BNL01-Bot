"""Ordinary topic recall uses old public Moments without extending them."""

import os
import sqlite3
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shared_brain_synthesis import (
    _ordinary_rendered_evidence_refs,
    ordinary_chat_task_support_plan,
    render_packet_context,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    PacketFrameSubject,
    PacketFrameTask,
    build_packet,
    revalidate_packet,
)
from bnl_unified_response_assessment import build_situation_frame_v1


class MomentTopicAssociationTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
        }
        self.env = mock.patch.dict(os.environ, self.flags, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)
        self.conn.execute("""
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER,
                user_name TEXT, role TEXT, content TEXT, channel_id INTEGER,
                channel_policy TEXT, route_mode TEXT NOT NULL, timestamp TEXT
            )
        """)

    def add_moment(self, texts=None, *, row_start=100, day=1):
        texts = texts or (
            "I suggest simmering beans with smoked paprika for the recipe.",
            "I prefer roasted tomatoes in the beans recipe.",
            "I propose adding cumin to the beans recipe for a warmer flavor.",
        )
        base = datetime(2026, 6, day, 12, 0, tzinfo=timezone.utc)
        roots = []
        for offset, text in enumerate(texts):
            row_id = row_start + offset
            user_id = 7 + offset
            name = f"Test Member {offset + 1}"
            stamp = (base + timedelta(seconds=offset)).isoformat()
            self.conn.execute(
                "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?)",
                (row_id, 1, user_id, name, "user", text, 10,
                 "public_home", "normal_chat", stamp),
            )
            result = ledger.shadow_conversation_row(
                self.conn, row_id=row_id, user_id=user_id, user_name=name,
                guild_id=1, role="user", content=text,
                channel_policy="public_home", channel_id=10,
                channel_name="barcode-bot", route_mode="normal_chat",
                observed_at=stamp,
            )
            self.assertEqual(result.outcome, "inserted")
            roots.append(result.entry_id)
            moments.observe_ledger_entry(self.conn, result.entry_id)
        moments.sweep_expired_windows(
            self.conn, now=(base + timedelta(minutes=3)).isoformat(),
        )
        row = self.conn.execute(
            "SELECT moment_id FROM memory_moment_windows "
            "WHERE lifecycle_status='finalized' "
            "ORDER BY last_activity_at DESC,moment_id LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(row, "Fixture must qualify through the Moment owner")
        self.conn.commit()
        return str(row[0]), tuple(roots)

    def request(self, text="BNL, beans recipe ideas sound good today.", **changes):
        request = IntelligencePacketRequest(
            guild_id=1, subject_user_id=0, route_mode="normal_chat",
            conversation_surface="mention_or_reply", channel_id=10,
            channel_name="barcode-bot", channel_policy="public_home",
            visibility_allowance="public_safe", user_text=text,
            participant_user_ids=(10,), direct_state="direct",
            budget_chars=6000, now="2026-09-11T10:00:00+00:00",
            conversation_evidence=(PacketConversationEvidence(
                text=text, speaker_user_id=10,
                speaker_label="Test Member Four", current_turn=True,
            ),),
            frame_schema_version="situation_frame_v1",
            frame_revision="sf_ordinary_topic",
            frame_input_evidence_digest="a" * 64,
            frame_status="resolved", frame_subject_requirement="not_applicable",
            frame_event_relation="uncertain", frame_task_kind="conversation",
            frame_object_kind="unknown", frame_phase="conversation",
        )
        return replace(request, **changes)

    def packet(self, request=None):
        packet = build_packet(
            self.conn, request or self.request(), environ=self.flags,
        )
        self.assertEqual(packet.diagnostics.processing_errors, [])
        self.assertEqual(packet.diagnostics.invalid_invariants, [])
        return packet

    @staticmethod
    def historical(packet):
        return tuple(item for item in packet.items
                     if item.source_type == "historical_topic_gist")

    def moment_snapshot(self):
        names = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name LIKE 'memory_moment_%' ORDER BY name"
        ).fetchall()
        return tuple((name, tuple(self.conn.execute(
            f'SELECT * FROM "{name}" ORDER BY rowid'
        ).fetchall())) for (name,) in names)

    def test_new_member_gets_old_topic_without_changing_people_or_episodes(self):
        moment_id, _roots = self.add_moment()
        before = self.moment_snapshot()
        packet = self.packet()
        items = self.historical(packet)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.lane, "episode")
        self.assertEqual(item.event_ref, moment_id)
        self.assertTrue(item.subject_key.startswith("event:"))
        self.assertEqual(item.usage, "historical_topic_context")
        self.assertEqual(item.event_relation, "topic_related")
        self.assertEqual(item.uncertainty_status, "topic_association_only")
        self.assertEqual(item.participants, ())
        self.assertEqual(item.point_identity, "")
        self.assertEqual(len(item.root_identities), 3)
        self.assertEqual(self.moment_snapshot(), before)
        people = self.conn.execute(
            "SELECT DISTINCT participant_key FROM memory_moment_participants "
            "WHERE moment_id=? ORDER BY participant_key", (moment_id,),
        ).fetchall()
        self.assertEqual(people, [("discord_user:7",), ("discord_user:8",),
                                  ("discord_user:9",)])
        rendered, _counts, _number, digests = render_packet_context(packet)
        self.assertIn("historical", rendered.lower())
        self.assertIn("paraphrase", rendered.lower())
        self.assertIn("beans", item.text.lower())
        self.assertTrue(any(word in item.text.lower()
                            for word in ("simmering", "paprika", "tomatoes", "cumin")))
        self.assertNotIn("Test Member Four", item.text)
        refs = _ordinary_rendered_evidence_refs(packet, digests)
        historical_ref = next(ref for ref in refs if ref[2] == item.source_digest)
        self.assertEqual(historical_ref[3], ())

    def test_single_topic_word_with_bnl_address_can_associate(self):
        self.add_moment()
        self.assertEqual(len(self.historical(self.packet(self.request("BNL, beans!")))), 1)
        self.assertEqual(self.historical(self.packet(self.request("BNL!"))), ())

    def test_topic_association_uses_at_most_two_old_moments(self):
        for day, ingredient in enumerate(("paprika", "cumin", "rosemary"), 1):
            self.add_moment((
                f"I propose {ingredient} in the beans recipe.",
                f"I prefer extra {ingredient} in the beans recipe.",
                f"I suggest {ingredient} and fresh tomatoes in the beans recipe.",
            ), row_start=day * 100, day=day)
        packet = self.packet(self.request("BNL, beans!"))
        self.assertEqual(len(self.historical(packet)), 2)

    def test_newer_generic_summary_does_not_hide_older_substantive_detail(self):
        older_id, _roots = self.add_moment()
        newer_id, _roots = self.add_moment((
            "The beans recipe contains smoked paprika and roasted tomatoes.",
            "The beans recipe includes cumin beside the roasted tomatoes.",
            "The beans recipe has a warm flavor from the combined spices.",
        ), row_start=200, day=2)
        summaries = self.conn.execute(
            "SELECT DISTINCT summary FROM memory_moment_windows "
            "WHERE moment_id IN (?,?)", (older_id, newer_id),
        ).fetchall()
        self.assertEqual(len(summaries), 1)
        newer_details = self.conn.execute(
            "SELECT COUNT(*) FROM memory_moment_contributions WHERE moment_id=?",
            (newer_id,),
        ).fetchone()[0]
        self.assertEqual(newer_details, 0)

        items = self.historical(self.packet(self.request("BNL, beans!")))
        self.assertLessEqual(len(items), 2)
        older = next((item for item in items if item.event_ref == older_id), None)
        self.assertIsNotNone(older)
        self.assertIn("beans", older.text.lower())
        self.assertTrue(any(word in older.text.lower()
                            for word in ("simmering", "paprika", "tomatoes", "cumin")))

    def test_natural_topic_reaches_recall_through_the_real_situation_frame(self):
        self.add_moment()
        for text in ("BNL, beans are amazing.", "BNL, I love beans."):
            with self.subTest(text=text):
                frame = build_situation_frame_v1(
                    route_allowed=True, route_mode="normal_chat",
                    conversation_surface="mention_or_reply",
                    channel_policy="public_home", current_text=text,
                    current_speaker_user_ids=(10,),
                    current_speaker_labels=("Test Member Four",),
                    addressee_kinds=("discord_mention",),
                    source_message_ids=(501,), explicit_mention_count=1,
                    response_act="answer", packet_revision="natural_topic_turn",
                )
                self.assertEqual(frame.status, "resolved")
                self.assertEqual(frame.subject_requirement, "not_applicable")
                request = self.request(
                    text, frame_schema_version=frame.schema_version,
                    frame_revision=frame.frame_revision,
                    frame_input_evidence_digest=frame.input_evidence_digest,
                    frame_status=frame.status,
                    frame_subject_requirement=frame.subject_requirement,
                    frame_subjects=tuple(PacketFrameSubject(
                        user_id=subject.user_id, entity_ref=subject.entity_ref,
                        label_hint=subject.label_hint,
                        binding_method=subject.binding_method,
                        confidence=subject.confidence,
                        role_hints=subject.role_hints, domain_hints=subject.domain_hints,
                    ) for subject in frame.subjects),
                    frame_tasks=tuple(PacketFrameTask(
                        task_id=task.task_id, text_digest=task.text_digest,
                        task_kind=task.task_kind, object_kind=task.object_kind,
                        authority_scope=task.authority_scope,
                        temporal_scope=task.temporal_scope,
                        currentness=task.currentness,
                        required_response_act=task.required_response_act,
                        subject_requirement=task.subject_requirement,
                        subject_indexes=task.subject_indexes,
                    ) for task in frame.tasks),
                    frame_role_hints=frame.role_hints,
                    frame_domain_hints=frame.domain_hints,
                    frame_event_ref=frame.event_ref,
                    frame_event_relation=frame.event_relation,
                    frame_task_kind=frame.task_kind,
                    frame_object_kind=frame.object_kind,
                    frame_phase=frame.phase,
                    frame_temporal_scope=frame.temporal_scope,
                    frame_currentness=frame.currentness,
                )
                self.assertEqual(len(self.historical(self.packet(request))), 1)

    def test_same_music_family_without_actual_topic_overlap_is_not_a_hit(self):
        self.add_moment((
            "The synth patch should open the chorus.",
            "The drums can answer that synth chorus.",
            "The modular patch and drums resolve together.",
        ))
        packet = self.packet(self.request("BNL, vocal production sounds beautiful."))
        self.assertEqual(self.historical(packet), ())

    def test_new_event_reference_does_not_turn_topic_history_into_continuation(self):
        moment_id, _roots = self.add_moment()
        for relation in ("new_event_same_participant", "new_event_or_uncertain"):
            with self.subTest(relation=relation):
                packet = self.packet(self.request(
                    frame_event_ref=moment_id, frame_event_relation=relation,
                ))
                self.assertEqual(len(self.historical(packet)), 1)
                self.assertEqual(self.historical(packet)[0].event_relation, "topic_related")

    def test_explicit_resume_keeps_existing_episode_semantics(self):
        moment_id, _roots = self.add_moment()
        packet = self.packet(self.request(
            "BNL, resume that beans recipe episode.",
            frame_event_ref=moment_id, frame_event_relation="resume",
        ))
        self.assertEqual(self.historical(packet), ())
        self.assertTrue(any(item.lane == "episode" for item in packet.items))
        rejected = self.packet(self.request(
            "BNL, resume that beans recipe episode.",
            frame_event_ref=moment_id, frame_event_relation="new_event_or_uncertain",
        ))
        self.assertFalse(any(item.lane == "episode" for item in rejected.items))

    def test_recap_and_required_member_queries_do_not_receive_group_association(self):
        self.add_moment()
        requests = (
            self.request(immediate_recap=True),
            self.request(
                frame_subject_requirement="required",
                frame_subjects=(PacketFrameSubject(
                    user_id=10, label_hint="Test Member Four",
                    binding_method="current_speaker", confidence="high",
                ),),
            ),
            self.request(frame_status="ambiguous"),
            self.request(frame_revision=""),
        )
        for request in requests:
            with self.subTest(request=request):
                self.assertEqual(self.historical(self.packet(request)), ())

    def test_retired_corrected_or_restricted_source_invalidates_frozen_recall(self):
        _moment_id, roots = self.add_moment()
        packet = self.packet()
        self.assertTrue(self.historical(packet))
        mutations = (
            ("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?",),
            ("UPDATE memory_ledger_entries SET normalized_value='Correction: the recipe uses lentils.' WHERE entry_id=?",),
            ("UPDATE memory_ledger_entries SET channel_policy='sealed_test' WHERE entry_id=?",),
        )
        for (sql,) in mutations:
            with self.subTest(sql=sql):
                self.conn.execute("SAVEPOINT source_change")
                self.conn.execute(sql, (roots[0],))
                result = revalidate_packet(self.conn, packet, environ=self.flags)
                self.assertFalse(result.valid)
                self.assertEqual(result.status, "source_changed")
                self.conn.execute("ROLLBACK TO source_change")
                self.conn.execute("RELEASE source_change")

    def test_changed_contribution_gist_invalidates_its_rendered_historical_detail(self):
        moment_id, _roots = self.add_moment()
        packet = self.packet()
        self.assertTrue(self.historical(packet))
        self.conn.execute(
            "UPDATE memory_moment_contributions SET contribution_gist=? "
            "WHERE moment_id=? AND participant_key='discord_user:7'",
            ("The participant proposed a direction centered on lentils.", moment_id),
        )
        result = revalidate_packet(self.conn, packet, environ=self.flags)
        self.assertFalse(result.valid)
        self.assertEqual(result.status, "source_changed")

    def test_neutral_history_cannot_satisfy_person_scoped_task_support(self):
        self.add_moment()
        packet = self.packet()
        _text, _counts, _number, digests = render_packet_context(packet)
        refs = _ordinary_rendered_evidence_refs(packet, digests)
        task = PacketFrameTask(
            task_id="T1", text_digest="b" * 64, task_kind="retrieve_memory",
            object_kind="person", authority_scope="packet",
            temporal_scope="historical", currentness="historical",
            required_response_act="answer", subject_requirement="required",
            subject_indexes=(0,),
        )
        basis = SimpleNamespace(
            packet=replace(packet, request=replace(packet.request, frame_tasks=(task,))),
            rendered_evidence_refs=refs,
        )
        plan = ordinary_chat_task_support_plan(basis)
        self.assertEqual(plan[0].support_kind, "hold")
        self.assertEqual(plan[0].evidence_ids, ())


if __name__ == "__main__":
    unittest.main()
