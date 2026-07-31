import os
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shadow_acceptance import (
    build_v2_shadow_acceptance_snapshot,
    render_v2_shadow_acceptance_lines,
)
from bnl_shared_brain_synthesis import render_packet_context
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    _safe_atomic_supporting_observation,
    build_evaluation_report,
    build_packet,
    revalidate_packet,
    shadow_configuration,
)
from bnl_unified_response_assessment import (
    build_unified_response_assessment,
)


class UnifiedIntelligencePacketTests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
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

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def add_entry(
        self,
        row_id,
        *,
        subject_key="discord_user:7",
        entry_type="preference",
        predicate_key="favorite_movie",
        value="Alien",
        source_class=SourceClass.FIRST_PARTY_RECORD,
        visibility=Visibility.PUBLIC,
        confidence=Confidence.HIGH,
        public_usable=True,
        route_mode="normal_chat",
        channel_policy="public_home",
        lifecycle_status="active",
        participants=None,
    ):
        if participants is None:
            participants = (
                ledger.LedgerParticipant(
                    subject_key,
                    "Crow",
                    "author",
                    0,
                ),
            )
        result = ledger.insert_ledger_entry(
            self.conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=row_id,
                source_revision=str(row_id),
                source_role="member_self_report",
                entry_type=entry_type,
                subject_key=subject_key,
                subject_display_name="Crow",
                predicate_key=predicate_key,
                value=value,
                source_class=source_class,
                route_mode=route_mode,
                channel_id=10,
                channel_name="barcode-bot",
                channel_policy=channel_policy,
                visibility=visibility,
                confidence=confidence,
                public_usable=public_usable,
                observed_at=(
                    datetime(2026, 7, 25, tzinfo=timezone.utc)
                    + timedelta(seconds=int(row_id))
                ).isoformat(),
                source_sequence=int(row_id),
                lifecycle_status=lifecycle_status,
                participants=tuple(participants),
            ),
        )
        self.assertIn(result.outcome, {"inserted", "deduplicated"})
        return result.entry_id

    def add_established_atomic(
        self,
        *,
        first_row=1,
        predicate_key="favorite_instrument",
        value="modular synths",
        subject_key="discord_user:7",
        visibility=Visibility.PUBLIC,
        public_usable=True,
    ):
        roots = tuple(
            self.add_entry(
                row_id,
                subject_key=subject_key,
                entry_type="observation",
                predicate_key=predicate_key,
                value=value,
                source_class=SourceClass.PUBLIC_OBSERVATION,
                visibility=visibility,
                public_usable=public_usable,
            )
            for row_id in (first_row, first_row + 1)
        )
        result = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key=subject_key,
                subject_display_name="Crow",
                predicate_key=predicate_key,
                meaning=value,
                root_entry_ids=roots,
                participant_keys=(subject_key,),
                epistemic_status="observed",
                uncertainty_note=(
                    "Repeated observation; not a scalar identity fact."
                ),
                currentness="historical",
                contradiction_key=f"{subject_key}:{predicate_key}",
                retrieval_tags=("test_topic_motif",),
            ),
        )
        self.assertEqual(result.outcome, "created")
        canonical_id = self.conn.execute(
            """
            SELECT canonical_candidate_id
            FROM memory_ledger_knowledge_candidates
            WHERE candidate_id=?
            """,
            (result.candidate_id,),
        ).fetchone()[0]
        return roots, canonical_id or result.candidate_id

    def add_public_moment(self):
        base = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
        messages = (
            (200, 7, "Crow", "The synth patch should open the chorus."),
            (201, 8, "Moth", "The drums can answer that synth chorus."),
            (202, 7, "Crow", "The modular patch and drums resolve together."),
        )
        for row_id, user_id, name, text in messages:
            result = ledger.shadow_conversation_row(
                self.conn,
                row_id=row_id,
                user_id=user_id,
                user_name=name,
                guild_id=1,
                role="user",
                content=text,
                channel_policy="public_home",
                channel_id=10,
                channel_name="barcode-bot",
                route_mode="normal_chat",
                observed_at=(
                    base + timedelta(seconds=row_id - 200)
                ).isoformat(),
            )
            moments.observe_ledger_entry(self.conn, result.entry_id)
        moments.sweep_expired_windows(
            self.conn,
            now=(base + timedelta(minutes=3)).isoformat(),
        )
        row = self.conn.execute(
            """
            SELECT moment_id
            FROM memory_moment_windows
            WHERE guild_id=1 AND lifecycle_status='finalized'
            ORDER BY moment_id
            LIMIT 1
            """
        ).fetchone()
        self.assertIsNotNone(row)
        return row[0]

    def add_relationship_state(self):
        event_id = relationships.observe_message(
            self.conn,
            guild_id=1,
            user_id=7,
            role="user",
            content="Thanks for helping me with this project.",
            source_row_id=300,
            user_name="Crow",
            channel_policy="public_home",
            channel_name="barcode-bot",
            channel_id=10,
            route_mode="normal_chat",
            directed=True,
            observed_at="2026-07-25T12:05:00+00:00",
        )
        self.assertTrue(event_id)

    def add_conversation_context_row(self):
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_policy,timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                900,
                1,
                7,
                "Crow",
                "user",
                "I want the archive to connect my music and project work.",
                10,
                "public_home",
                "2026-07-25T12:06:00+00:00",
            ),
        )

    def add_raw_public_conversation(
        self,
        row_id,
        text,
        observed_at,
        *,
        user_id=7,
        user_name="Crow",
    ):
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_policy,timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                int(row_id),
                1,
                int(user_id),
                user_name,
                "user",
                text,
                10,
                "public_home",
                observed_at,
            ),
        )
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=int(row_id),
            user_id=int(user_id),
            user_name=user_name,
            guild_id=1,
            role="user",
            content=text,
            channel_policy="public_home",
            channel_id=10,
            channel_name="barcode-bot",
            route_mode="normal_chat",
            observed_at=observed_at,
        )
        self.assertEqual(result.outcome, "inserted")
        return result.entry_id

    def public_request(
        self,
        *,
        text="BNL, what am I all about in the BARCODE project?",
        immediate_recap=False,
        source_context_snapshot="",
    ):
        return IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=7,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy="public_home",
            visibility_allowance="public_safe",
            user_text=text,
            participant_user_ids=(7,),
            direct_state="direct",
            budget_chars=6000,
            conversation_evidence=(
                PacketConversationEvidence(
                    text=(
                        "I want the archive to connect my music and "
                        "project work."
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
            source_context_snapshot=source_context_snapshot,
            source_context_authorized=bool(source_context_snapshot),
            immediate_recap=immediate_recap,
            now="2026-07-25T12:07:00+00:00",
        )

    def add_full_evidence(self):
        self.add_conversation_context_row()
        self.add_entry(
            10,
            predicate_key="favorite_movie",
            value="Alien",
        )
        self.add_entry(
            11,
            entry_type="goal",
            predicate_key="goal",
            value="finish the source-linked archive",
            source_class=SourceClass.RUNTIME_OBSERVATION,
        )
        self.add_established_atomic()
        self.add_public_moment()
        self.add_relationship_state()

    def test_packet_combines_existing_owners_deterministically_in_shadow(self):
        self.add_full_evidence()

        first = build_packet(
            self.conn,
            self.public_request(),
            environ=self.flags,
        )
        second = build_packet(
            self.conn,
            self.public_request(),
            environ=self.flags,
        )

        self.assertIsNotNone(first)
        self.assertEqual(first.packet_id, second.packet_id)
        self.assertEqual(
            tuple(item.source_digest for item in first.items),
            tuple(item.source_digest for item in second.items),
        )
        self.assertTrue(
            {
                "current_intent",
                "conversation_context",
                "approved_fact",
                "moment",
                "atomic_knowledge",
                "open_loop",
                "canon",
                "relationship_posture",
            }.issubset(set(first.detailed_lanes))
        )
        relationship_item = next(
            item
            for item in first.items
            if item.lane == "relationship_posture"
        )
        self.assertEqual(relationship_item.visibility, "private")
        self.assertEqual(relationship_item.usage, "tone_only")
        self.assertEqual(
            first.diagnostics.selected_atomic_states,
            {"established": 1},
        )
        self.assertEqual(first.diagnostics.revalidation_status, "passed")
        self.assertFalse(first.diagnostics.processing_errors)
        self.assertFalse(first.diagnostics.invalid_invariants)
        self.assertFalse(first.diagnostics.prompt_applied)
        self.assertFalse(first.diagnostics.live_applied)

        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="mention_or_reply",
            current_speaker_user_ids=(7,),
            current_exchange_source_ids=(900,),
            prior_moment_ids=first.moment_refs,
            governed_entry_ids=first.governed_refs,
            relationship_candidate_keys=first.relationship_refs,
            canon_refs=first.canon_refs,
            prompt_lanes=("current_exchange",),
            current_text=first.request.user_text,
            packet_selected_lanes=first.assessment_lanes,
            packet_excluded_lanes=first.assessment_exclusions,
            packet_conflict_reasons=first.diagnostics.conflict_reasons,
            packet_missing_lanes=first.assessment_missing_lanes,
            packet_revalidation_status=(
                first.diagnostics.revalidation_status
            ),
        )
        self.assertIn(
            "unified_intelligence_packet_shadow",
            assessment.diagnostic_reasons,
        )
        self.assertEqual(
            assessment.comparison_status,
            "prompt_underincluded",
        )
        self.assertIn("governed_memory", assessment.prompt_missing_lanes)
        self.assertIn("prior_moment", assessment.prompt_missing_lanes)

        report = build_evaluation_report(self.conn, guild_id=1)
        self.assertEqual(report["runs"], 2)
        self.assertEqual(report["promptAppliedRuns"], 0)
        self.assertEqual(report["liveAppliedRuns"], 0)
        self.assertEqual(report["contentFieldsPresent"], [])
        self.assertEqual(report["invalidInvariants"], 0)
        acceptance = build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=self.flags,
        )
        self.assertTrue(
            acceptance["unifiedIntelligencePacketShadow"][
                "evidenceObserved"
            ]
        )
        rendered = "\n".join(
            render_v2_shadow_acceptance_lines(acceptance)
        )
        self.assertIn("- unified_intelligence_packet:", rendered)
        self.assertIn("prompt_applied=`0`", rendered)
        self.assertIn("live_applied=`0`", rendered)
        self.assertNotIn("modular synths", rendered)

    def test_profile_sufficiency_uses_distinct_points_and_occurrences(self):
        rows = (
            (
                910,
                "I keep fixing bot code and memory systems.",
                "2026-07-23T10:00:00+00:00",
            ),
            (
                911,
                "I keep developing visual art and character designs.",
                "2026-07-23T10:05:00+00:00",
            ),
            (
                912,
                "The website code still needs careful testing.",
                "2026-07-25T10:00:00+00:00",
            ),
            (
                913,
                "The character artwork needs another visual style.",
                "2026-07-25T10:05:00+00:00",
            ),
        )
        for row in rows:
            self.add_raw_public_conversation(*row)
        formed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=self.conn.execute(
                """
                SELECT entry_id FROM memory_ledger_entries
                WHERE source_table='conversations' AND source_row_id='913'
                  AND entry_type='observation'
                """
            ).fetchone()[0],
            environ=self.flags,
        )
        self.assertGreaterEqual(len(formed), 2)
        request = self.public_request()
        request = IntelligencePacketRequest(
            **{
                **request.__dict__,
                "conversation_evidence": (
                    PacketConversationEvidence(
                        text=rows[2][1],
                        source_id=912,
                        speaker_user_id=7,
                        speaker_label="Crow",
                    ),
                    request.conversation_evidence[-1],
                ),
            }
        )

        packet = build_packet(
            self.conn,
            request,
            persist=False,
            environ=self.flags,
        )

        self.assertEqual(packet.profile_sufficiency.status, "rich")
        self.assertTrue(packet.profile_sufficiency.satisfied)
        self.assertEqual(
            packet.profile_sufficiency.required_point_count,
            2,
        )
        self.assertGreaterEqual(
            packet.profile_sufficiency.selected_point_count,
            2,
        )
        self.assertGreaterEqual(
            packet.profile_sufficiency.independent_root_count,
            2,
        )
        self.assertEqual(
            packet.profile_sufficiency.independent_occurrence_count,
            2,
        )
        self.assertEqual(
            {
                item.lane
                for item in packet.items
                if item.point_identity
                and item.lane == "atomic_knowledge"
            },
            {"atomic_knowledge"},
        )
        atomic_items = tuple(
            item
            for item in packet.items
            if item.lane == "atomic_knowledge"
        )
        self.assertTrue(atomic_items)
        self.assertTrue(
            all(item.supporting_observations for item in atomic_items)
        )
        rendered, _counts, _count, _digests = render_packet_context(
            packet
        )
        self.assertIn("Source-linked public examples", rendered)
        self.assertIn("website code still needs careful testing", rendered)
        self.assertIn(
            "character artwork needs another visual style",
            rendered,
        )
        self.assertGreaterEqual(
            packet.diagnostics.root_collapse_suppression,
            1,
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "same_root_projection",
                0,
            ),
            1,
        )

    def test_supporting_observations_are_public_member_roots_only(self):
        base = {
            "source_table": "conversations",
            "source_role": "user",
            "predicate_key": "conversation",
            "channel_policy": "public_home",
            "visibility": "public",
            "public_usable": 1,
            "derived": 0,
            "projection": 0,
            "lifecycle_status": "active",
            "normalized_value": (
                "I keep composing synth tracks for the weekly show."
            ),
        }
        self.assertEqual(
            _safe_atomic_supporting_observation(base),
            base["normalized_value"],
        )
        blocked = (
            {"source_role": "model"},
            {"channel_policy": "sealed_test"},
            {"visibility": "private"},
            {"derived": 1},
            {"projection": 1},
            {"lifecycle_status": "retracted"},
            {
                "normalized_value": (
                    "See https://example.com for my synth project."
                )
            },
            {
                "normalized_value": (
                    "My home address is 100 Signal Road."
                )
            },
            {
                "normalized_value": (
                    "Correction: that synth project was not mine."
                )
            },
            {
                "normalized_value": (
                    "Pretend my character makes synth records."
                )
            },
            {
                "normalized_value": (
                    "What do you remember about my synth tracks?"
                )
            },
        )
        for override in blocked:
            with self.subTest(override=override):
                self.assertEqual(
                    _safe_atomic_supporting_observation(
                        {**base, **override}
                    ),
                    "",
                )

    def test_packet_retains_a_larger_safe_pool_for_adaptive_rendering(self):
        entry_ids = []
        for index in range(6):
            entry_ids.append(
                self.add_raw_public_conversation(
                    950 + index,
                    (
                        "I keep producing synth music mix number %s for "
                        "the radio show detail %s."
                        % (index, chr(ord("a") + index))
                    ),
                    "2026-07-%02dT10:00:00+00:00" % (20 + index),
                )
            )
        formed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=entry_ids[-1],
            environ=self.flags,
        )
        self.assertTrue(formed)

        packet = build_packet(
            self.conn,
            self.public_request(),
            persist=False,
            environ=self.flags,
        )
        music_item = next(
            item
            for item in packet.items
            if item.lane == "atomic_knowledge"
            and "music and audio production" in item.text
        )
        self.assertEqual(len(music_item.supporting_observations), 6)

        rendered, _counts, _count, _digests = render_packet_context(packet)
        self.assertIn("mix number 0", rendered)
        self.assertIn("mix number 5", rendered)

    def test_process_profile_selects_across_family_unmatched_public_pool(self):
        rows = (
            (
                970,
                "I want to compare both approaches before choosing one.",
                "2026-07-20T10:00:00+00:00",
            ),
            (
                971,
                "Let's test the smaller version and check the result first.",
                "2026-07-21T10:00:00+00:00",
            ),
            (
                972,
                "We should revise one piece at a time before deciding.",
                "2026-07-22T10:00:00+00:00",
            ),
            (
                973,
                "I prefer a bounded plan with careful verification.",
                "2026-07-23T10:00:00+00:00",
            ),
            (
                974,
                "I want custom emotes for Modem and Floppydisc.",
                "2026-07-24T10:00:00+00:00",
            ),
            (
                975,
                "The character icons need another visual design pass.",
                "2026-07-25T10:00:00+00:00",
            ),
        )
        entry_ids = tuple(
            self.add_raw_public_conversation(*row) for row in rows
        )
        self.add_raw_public_conversation(
            976,
            "Another member privately chooses a completely different plan.",
            "2026-07-25T11:00:00+00:00",
            user_id=8,
            user_name="Moth",
        )
        formation_diagnostics = {}
        ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=entry_ids[-1],
            environ=self.flags,
            diagnostics=formation_diagnostics,
        )
        self.assertGreaterEqual(
            formation_diagnostics["ledger_rows_family_unmatched"],
            4,
        )

        packet = build_packet(
            self.conn,
            self.public_request(
                text=(
                    "What have you learned about how I work and make "
                    "decisions?"
                )
            ),
            persist=False,
            environ=self.flags,
        )
        assessment_items = tuple(
            item
            for item in packet.items
            if item.lane == "assessment_observation"
        )

        self.assertEqual(
            packet.diagnostics.candidates_by_lane[
                "assessment_observation"
            ],
            6,
        )
        self.assertEqual(len(assessment_items), 4)
        self.assertGreaterEqual(
            sum(
                any(
                    marker in item.text.lower()
                    for marker in (
                        "approach",
                        "test",
                        "revise",
                        "plan",
                    )
                )
                for item in assessment_items
            ),
            3,
        )
        self.assertEqual(
            {item.subject_key for item in assessment_items},
            {"discord_user:7"},
        )
        self.assertTrue(
            all(item.usage == "assessment_only" for item in assessment_items)
        )
        rendered, counts, _count, _digests = render_packet_context(packet)
        self.assertEqual(
            dict(counts).get("assessment_observation"),
            4,
        )
        self.assertIn("selected after considering the full eligible", rendered)
        self.assertNotIn("completely different plan", rendered)

    def test_distinct_atomic_points_can_share_exact_human_roots(self):
        rows = (
            (
                920,
                (
                    "I keep fixing bot code while drawing visual art "
                    "for the project."
                ),
                "2026-07-23T10:00:00+00:00",
            ),
            (
                921,
                (
                    "The website system and artwork design both need "
                    "another pass."
                ),
                "2026-07-25T10:00:00+00:00",
            ),
        )
        entry_ids = tuple(
            self.add_raw_public_conversation(*row) for row in rows
        )
        formed = ledger.form_atomic_candidates_from_recurring_conversation(
            self.conn,
            trigger_entry_id=entry_ids[-1],
            environ=self.flags,
        )
        self.assertEqual(len(formed), 2)

        packet = build_packet(
            self.conn,
            self.public_request(),
            persist=False,
            environ=self.flags,
        )
        atomic_items = tuple(
            item for item in packet.items if item.lane == "atomic_knowledge"
        )

        self.assertEqual(len(atomic_items), 2)
        self.assertEqual(
            len({item.point_identity for item in atomic_items}),
            2,
        )
        self.assertEqual(
            len({item.root_identities for item in atomic_items}),
            1,
        )
        self.assertEqual(
            len({item.occurrence_identities for item in atomic_items}),
            1,
        )
        self.assertEqual(packet.profile_sufficiency.status, "rich")
        self.assertTrue(packet.profile_sufficiency.satisfied)
        self.assertEqual(
            packet.profile_sufficiency.selected_point_count,
            2,
        )
        self.assertEqual(
            packet.profile_sufficiency.independent_root_count,
            2,
        )
        self.assertEqual(
            packet.profile_sufficiency.independent_occurrence_count,
            2,
        )
        self.assertGreaterEqual(
            packet.diagnostics.shared_root_projection_count,
            1,
        )

    def test_moment_and_atomic_projection_share_one_human_root_set(self):
        moment_id = self.add_public_moment()
        roots = tuple(
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT ledger_entry_id
                FROM memory_moment_contribution_sources
                WHERE moment_id=? AND participant_key='discord_user:7'
                ORDER BY ledger_entry_id
                """,
                (moment_id,),
            ).fetchall()
        )
        self.assertEqual(len(roots), 2)
        formed = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="modular_synthesis_motif",
                meaning=(
                    "Recurring public conversation about modular synthesis."
                ),
                root_entry_ids=roots,
                participant_keys=("discord_user:7",),
                epistemic_status="observed",
                uncertainty_note=(
                    "Moment-derived observation; not an exact quote."
                ),
                currentness="historical",
                contradiction_key=(
                    "discord_user:7:modular_synthesis_motif"
                ),
                retrieval_tags=("music_production",),
            ),
        )
        self.assertEqual(formed.outcome, "created")

        packet = build_packet(
            self.conn,
            self.public_request(),
            persist=False,
            environ=self.flags,
        )
        selected_lanes = {
            item.lane for item in packet.items if item.root_identities
        }

        self.assertIn("atomic_knowledge", selected_lanes)
        self.assertNotIn("moment", selected_lanes)
        self.assertGreaterEqual(
            packet.diagnostics.root_collapse_suppression,
            1,
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "same_root_projection",
                0,
            ),
            1,
        )
        self.assertEqual(packet.profile_sufficiency.status, "sparse")
        self.assertTrue(packet.profile_sufficiency.satisfied)
        self.assertEqual(
            packet.profile_sufficiency.candidate_point_count,
            1,
        )

    def test_moment_superset_projection_does_not_inflate_profile(self):
        moment_id = self.add_public_moment()
        roots = tuple(
            str(row[0])
            for row in self.conn.execute(
                """
                SELECT ledger_entry_id
                FROM memory_moment_contribution_sources
                WHERE moment_id=? AND participant_key='discord_user:7'
                ORDER BY ledger_entry_id
                """,
                (moment_id,),
            ).fetchall()
        )
        self.assertEqual(len(roots), 2)
        formed = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="synth_chorus_motif",
                meaning="Recurring public conversation about synth choruses.",
                root_entry_ids=(roots[0],),
                participant_keys=("discord_user:7",),
                epistemic_status="observed",
                uncertainty_note=(
                    "One observed contribution; not an exact quote."
                ),
                currentness="historical",
                contradiction_key="discord_user:7:synth_chorus_motif",
                retrieval_tags=("music_production",),
            ),
        )
        self.assertEqual(formed.outcome, "created")

        packet = build_packet(
            self.conn,
            self.public_request(),
            persist=False,
            environ=self.flags,
        )
        member_items = tuple(
            item
            for item in packet.items
            if item.lane in {"atomic_knowledge", "moment"}
        )

        self.assertEqual(
            tuple(item.lane for item in member_items),
            ("atomic_knowledge",),
        )
        self.assertEqual(packet.profile_sufficiency.status, "sparse")
        self.assertEqual(
            packet.profile_sufficiency.candidate_point_count,
            1,
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "same_root_projection",
                0,
            ),
            1,
        )

    def test_current_intent_does_not_consume_rendered_evidence_budget(self):
        self.add_entry(
            930,
            predicate_key="favorite_movie",
            value="Arrival",
        )
        self.add_entry(
            5000,
            predicate_key="favorite_color",
            value="violet",
        )
        current_fragments = tuple(
            PacketConversationEvidence(
                text=(
                    f"current-fragment-{index} "
                    + ("temporary request wording " * 50)
                ),
                speaker_user_id=7,
                speaker_label="Crow",
                current_turn=True,
            )
            for index in range(5)
        )
        base = self.public_request()
        request = IntelligencePacketRequest(
            **{
                **base.__dict__,
                "budget_chars": 400,
                "conversation_evidence": current_fragments,
            }
        )

        packet = build_packet(
            self.conn,
            request,
            persist=False,
            environ=self.flags,
        )
        rendered, _lane_counts, _item_count, _digests = (
            render_packet_context(packet)
        )

        self.assertEqual(
            len(
                tuple(
                    item
                    for item in packet.items
                    if item.lane == "current_intent"
                )
            ),
            5,
        )
        self.assertEqual(packet.profile_sufficiency.status, "rich")
        self.assertGreaterEqual(
            len(
                tuple(
                    item
                    for item in packet.items
                    if item.lane == "approved_fact"
                )
            ),
            2,
        )
        self.assertNotIn("current-fragment-", rendered)
        self.assertLessEqual(len(rendered), 5000)

    def test_canon_cannot_satisfy_an_empty_personal_profile(self):
        packet = build_packet(
            self.conn,
            self.public_request(),
            persist=False,
            environ=self.flags,
        )

        self.assertIn("canon", packet.detailed_lanes)
        self.assertEqual(packet.profile_sufficiency.status, "empty")
        self.assertFalse(packet.profile_sufficiency.satisfied)
        self.assertEqual(
            packet.profile_sufficiency.selected_point_count,
            0,
        )
        self.assertEqual(
            packet.profile_sufficiency.independent_occurrence_count,
            0,
        )

    def test_private_cross_subject_contested_and_live_rows_fail_closed(self):
        self.add_conversation_context_row()
        _roots, selected_candidate = self.add_established_atomic()
        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET live_eligible=1
            WHERE candidate_id=?
            """,
            (selected_candidate,),
        )
        _private_roots, private_candidate = self.add_established_atomic(
            first_row=20,
            predicate_key="private_preference",
            value="a private preference",
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET visibility='private'
            WHERE candidate_id=?
            """,
            (private_candidate,),
        )
        self.add_established_atomic(
            first_row=30,
            predicate_key="other_member_fact",
            value="belongs only to member 8",
            subject_key="discord_user:8",
        )
        red = self.add_entry(
            40,
            predicate_key="favorite_color",
            value="red",
        )
        blue = self.add_entry(
            41,
            predicate_key="favorite_color",
            value="blue",
        )
        ledger.form_atomic_candidate_from_ledger_entry(self.conn, red)
        ledger.form_atomic_candidate_from_ledger_entry(self.conn, blue)

        packet = build_packet(
            self.conn,
            self.public_request(text="What do you remember about me?"),
            environ=self.flags,
        )

        self.assertNotIn(
            "belongs only to member 8",
            {item.text for item in packet.items},
        )
        self.assertNotIn(
            "a private preference",
            {item.text for item in packet.items},
        )
        self.assertNotIn(
            "modular synths",
            {item.text for item in packet.items},
        )
        self.assertIn(
            "atomic_live_eligible_selected_in_shadow",
            packet.diagnostics.invalid_invariants,
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "atomic_live_eligible_invariant",
                0,
            ),
            1,
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "atomic_visibility",
                0,
            ),
            1,
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "atomic_state",
                0,
            ),
            1,
        )

    def test_atomic_member_fact_cannot_bypass_scalar_allowlist(self):
        self.add_conversation_context_row()
        roots = tuple(
            self.add_entry(
                row_id,
                predicate_key="favorite_instrument",
                value="modular synths",
            )
            for row_id in (55, 56)
        )
        for root in roots:
            ledger.form_atomic_candidate_from_ledger_entry(self.conn, root)

        packet = build_packet(
            self.conn,
            self.public_request(text="What do you remember about me?"),
            persist=False,
            environ=self.flags,
        )

        self.assertNotIn(
            "modular synths",
            {item.text for item in packet.items},
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "atomic_member_fact_not_authorized",
                0,
            ),
            1,
        )

    def test_atomic_source_blind_root_fails_closed_if_state_is_malformed(self):
        self.add_conversation_context_row()
        roots, candidate_id = self.add_established_atomic(first_row=57)
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET source_class='legacy_source_blind',
                visibility='public',
                public_usable=1
            WHERE entry_id IN (?,?)
            """,
            roots,
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET normalized_value='modular synths',
                candidate_state='established',
                authority_class='legacy_source_blind',
                consolidated_authority_class='legacy_source_blind',
                visibility='public',
                candidate_eligible=1,
                live_eligible=0,
                invalidated_reason='',
                review_status='current',
                review_due_at=''
            WHERE candidate_id=? OR canonical_candidate_id=?
            """,
            (candidate_id, candidate_id),
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_roots
            SET root_status='eligible',
                lifecycle_status='active',
                source_class='legacy_source_blind',
                visibility='public'
            WHERE candidate_id=? AND root_entry_id IN (?,?)
            """,
            (candidate_id, *roots),
        )

        packet = build_packet(
            self.conn,
            self.public_request(text="What do you remember about me?"),
            persist=False,
            environ=self.flags,
        )

        self.assertNotIn(
            "modular synths",
            {item.text for item in packet.items},
        )
        self.assertGreaterEqual(
            packet.diagnostics.excluded_by_reason.get(
                "atomic_root_revalidation",
                0,
            ),
            1,
        )

    def test_source_mutation_breaks_revalidation(self):
        self.add_conversation_context_row()
        roots, _candidate_id = self.add_established_atomic()
        packet = build_packet(
            self.conn,
            self.public_request(text="What do you remember about me?"),
            persist=False,
            environ=self.flags,
        )
        self.assertTrue(
            any(item.revalidation_kind == "atomic" for item in packet.items)
        )

        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='superseded'
            WHERE entry_id=?
            """,
            (roots[0],),
        )
        revalidation = revalidate_packet(
            self.conn,
            packet,
            environ=self.flags,
        )

        self.assertFalse(revalidation.valid)
        self.assertEqual(revalidation.status, "source_changed")
        self.assertGreaterEqual(revalidation.changed_source_count, 1)

    def test_supporting_root_text_change_breaks_revalidation(self):
        roots, _candidate_id = self.add_established_atomic()
        packet = build_packet(
            self.conn,
            self.public_request(text="What do you remember about me?"),
            persist=False,
            environ=self.flags,
        )
        self.conn.execute(
            """
            UPDATE memory_ledger_entries
            SET normalized_value='A changed source observation.'
            WHERE entry_id=?
            """,
            (roots[0],),
        )

        revalidation = revalidate_packet(
            self.conn,
            packet,
            environ=self.flags,
        )

        self.assertFalse(revalidation.valid)
        self.assertEqual(revalidation.status, "source_changed")

    def test_provisional_is_tentative_and_due_review_is_excluded(self):
        self.add_conversation_context_row()
        root = self.add_entry(
            70,
            entry_type="observation",
            predicate_key="conversation",
            value="I spent the evening patching the music bot.",
            source_class=SourceClass.PUBLIC_OBSERVATION,
        )
        created = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="code_and_music_motif",
                meaning="Recurring public conversation about music systems.",
                root_entry_ids=(root,),
                participant_keys=("discord_user:7",),
                epistemic_status="observed",
                uncertainty_note="Tentative observation; not a scalar fact.",
                currentness="historical",
                contradiction_key=(
                    "discord_user:7:code_and_music_motif"
                ),
                retrieval_tags=("code_and_systems", "music_production"),
            ),
        )
        packet = build_packet(
            self.conn,
            self.public_request(text="What do you remember about me?"),
            environ=self.flags,
        )
        item = next(
            item
            for item in packet.items
            if item.revalidation_key == created.candidate_id
        )
        self.assertEqual(item.lifecycle, "provisional")
        self.assertEqual(item.usage, "tentative")
        self.assertEqual(
            packet.diagnostics.selected_atomic_states,
            {"provisional": 1},
        )

        self.conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET review_status='due',
                review_due_at='2026-07-24T00:00:00+00:00'
            WHERE candidate_id=?
            """,
            (created.candidate_id,),
        )
        due_packet = build_packet(
            self.conn,
            self.public_request(text="What do you remember about me?"),
            environ=self.flags,
        )
        self.assertFalse(
            any(
                item.revalidation_key == created.candidate_id
                for item in due_packet.items
            )
        )
        self.assertEqual(
            due_packet.diagnostics.excluded_by_reason.get(
                "atomic_review_due"
            ),
            1,
        )

    def test_current_turn_correction_withholds_conflicting_durable_value(self):
        self.add_conversation_context_row()
        self.add_entry(
            80,
            predicate_key="favorite_movie",
            value="Alien",
        )
        packet = build_packet(
            self.conn,
            self.public_request(
                text="Actually, my favorite movie is Arrival."
            ),
            environ=self.flags,
        )
        self.assertNotIn("Alien", {item.text for item in packet.items})
        self.assertEqual(
            packet.diagnostics.excluded_by_reason.get(
                "current_turn_correction_precedence"
            ),
            1,
        )
        self.assertTrue(
            any(
                reason.endswith("current_turn_correction_precedence")
                for reason in packet.diagnostics.conflict_reasons
            )
        )
        question = build_packet(
            self.conn,
            self.public_request(
                text="Actually, what is my favorite movie?"
            ),
            environ=self.flags,
        )
        self.assertIn("Alien", {item.text for item in question.items})

    def test_immediate_recap_excludes_all_lower_precedence_lanes(self):
        self.add_full_evidence()

        packet = build_packet(
            self.conn,
            self.public_request(
                text="What did we just decide?",
                immediate_recap=True,
            ),
            environ=self.flags,
        )

        self.assertEqual(
            set(packet.detailed_lanes),
            {"current_intent", "conversation_context"},
        )
        self.assertGreater(
            packet.diagnostics.excluded_by_reason.get(
                "current_exchange_precedence",
                0,
            ),
            0,
        )
        self.assertEqual(packet.diagnostics.revalidation_status, "passed")

    def test_source_file_snapshot_requires_existing_internal_authority(self):
        self.add_conversation_context_row()
        public = build_packet(
            self.conn,
            self.public_request(
                source_context_snapshot="SOURCE FILE PRIVATE SENTINEL"
            ),
            environ=self.flags,
        )
        self.assertNotIn("source_file", public.detailed_lanes)
        self.assertEqual(
            public.diagnostics.excluded_by_reason.get(
                "source_file_route_not_authorized"
            ),
            1,
        )

        internal_request = IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=0,
            route_mode="internal_ops",
            conversation_surface="command_only",
            channel_id=99,
            channel_policy="internal_controlled",
            visibility_allowance="internal",
            user_text="Inspect the approved source snapshot.",
            direct_state="direct",
            source_context_snapshot="SOURCE FILE PRIVATE SENTINEL",
            source_context_authorized=True,
            now="2026-07-25T12:07:00+00:00",
        )
        internal = build_packet(
            self.conn,
            internal_request,
            environ=self.flags,
        )
        self.assertIn("source_file", internal.detailed_lanes)
        self.assertEqual(
            internal.diagnostics.revalidation_status,
            "passed_with_provider_snapshot",
        )
        self.assertFalse(internal.diagnostics.prompt_applied)
        self.assertFalse(internal.diagnostics.live_applied)

    def test_gate_requires_all_shadows_and_rejects_live_authority(self):
        enabled = shadow_configuration(self.flags)
        self.assertTrue(enabled["requested"])
        self.assertTrue(enabled["effective"])
        self.assertEqual(enabled["reason"], "shadow_only")

        missing = dict(self.flags)
        missing["BNL_MOMENT_ENGINE_SHADOW_ENABLED"] = "false"
        self.assertEqual(
            shadow_configuration(missing)["reason"],
            "missing_shadow_prerequisites",
        )
        live = dict(self.flags)
        live["BNL_MEMORY_GOVERNANCE_LIVE_ENABLED"] = "true"
        self.assertEqual(
            shadow_configuration(live)["reason"],
            "live_authority_detected",
        )
        self.assertIsNone(
            build_packet(
                self.conn,
                self.public_request(),
                environ=live,
            )
        )


if __name__ == "__main__":
    unittest.main()
