import os
import sqlite3
import unittest
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
import bnl_unified_intelligence_packet as packet_module
from bnl_profile_points import material_profile_point_map
from bnl_shared_brain_synthesis import (
    SharedBrainSynthesisBasis,
    candidate_profile_coverage,
    render_packet_context,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    build_packet,
)


class OpenSignalConvergenceTests(unittest.TestCase):
    """Dependency-free convergence contract over retained public roots."""

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
                route_mode TEXT NOT NULL,
                timestamp TEXT
            )
            """
        )

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def add_public_root(
        self,
        row_id,
        text,
        observed_at,
        *,
        user_id=7,
        user_name="Test Member",
        route_mode="normal_chat",
    ):
        self.conn.execute(
            """
            INSERT INTO conversations(
                id,guild_id,user_id,user_name,role,content,channel_id,
                channel_policy,route_mode,timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(row_id),
                1,
                int(user_id),
                str(user_name),
                "user",
                str(text),
                10,
                "public_home",
                str(route_mode),
                str(observed_at),
            ),
        )
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=int(row_id),
            user_id=int(user_id),
            user_name=str(user_name),
            guild_id=1,
            role="user",
            content=str(text),
            channel_policy="public_home",
            channel_id=10,
            channel_name="barcode-bot",
            route_mode=str(route_mode),
            observed_at=str(observed_at),
        )
        self.assertEqual(result.outcome, "inserted")
        self.conn.commit()
        return result.entry_id

    def request(
        self,
        text,
        *,
        user_id=7,
        user_name="Test Member",
        context_row_id=0,
        context_text="",
    ):
        evidence = []
        if int(context_row_id or 0) > 0:
            evidence.append(
                PacketConversationEvidence(
                    text=str(context_text),
                    source_id=int(context_row_id),
                    speaker_user_id=int(user_id),
                    speaker_label=str(user_name),
                )
            )
        evidence.append(
            PacketConversationEvidence(
                text=str(text),
                speaker_user_id=int(user_id),
                speaker_label=str(user_name),
                current_turn=True,
            )
        )
        return IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=int(user_id),
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            subject_display_name=str(user_name),
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy="public_home",
            visibility_allowance="public_safe",
            user_text=str(text),
            participant_user_ids=(int(user_id),),
            direct_state="direct",
            budget_chars=6000,
            conversation_evidence=tuple(evidence),
            now="2026-07-30T12:00:00+00:00",
        )

    def packet(self, text, **kwargs):
        return build_packet(
            self.conn,
            self.request(text, **kwargs),
            persist=False,
            environ=self.flags,
        )

    @staticmethod
    def assessment_items(packet):
        return tuple(
            item
            for item in packet.items
            if item.lane == "assessment_observation"
        )

    @staticmethod
    def synthesis_basis(packet):
        rendered, lane_counts, item_count, digests = render_packet_context(
            packet
        )
        return SharedBrainSynthesisBasis(
            packet=packet,
            assessment=None,
            rendered_context=rendered,
            expected_packet_digest=packet.diagnostics.packet_digest,
            expected_context_digest="test-only",
            guild_id=packet.request.guild_id,
            user_id=packet.request.subject_user_id,
            channel_id=packet.request.channel_id,
            route_mode=packet.request.route_mode,
            channel_policy=packet.request.channel_policy,
            rendered_item_count=item_count,
            rendered_lane_counts=lane_counts,
            rendered_source_digests=digests,
            profile_sufficiency_status=packet.profile_sufficiency.status,
            profile_required_point_count=(
                packet.profile_sufficiency.required_point_count
            ),
        )

    def test_topic_scoped_question_rejects_unrelated_open_observation(self):
        relevant = "I tune ceramic antennas for unusual signal harmonics."
        unrelated = "I test pizza recipes in a stone oven."
        self.add_public_root(
            100,
            relevant,
            "2026-07-20T10:00:00+00:00",
        )
        self.add_public_root(
            101,
            unrelated,
            "2026-07-22T10:00:00+00:00",
        )

        packet = self.packet(
            "What do you know about me regarding signal work?"
        )
        selected_text = {item.text for item in self.assessment_items(packet)}

        self.assertIn(relevant, selected_text)
        self.assertNotIn(unrelated, selected_text)

    def test_process_predicate_is_consistent_for_all_supported_nouns(self):
        self.add_public_root(
            110,
            "I compare two approaches before choosing a direction.",
            "2026-07-20T10:00:00+00:00",
        )
        self.add_public_root(
            111,
            "I test a smaller version and revise after checking results.",
            "2026-07-22T10:00:00+00:00",
        )
        process_rule = (
            "This request asks how the member works and makes decisions."
        )

        for noun in ("approach", "process", "method", "workflow"):
            wording = (
                "What have you learned about me regarding my %s?" % noun
            )
            with self.subTest(noun=noun):
                self.assertTrue(
                    ledger.public_assessment_process_request(wording)
                )
                packet = self.packet(wording)
                self.assertTrue(packet.profile_sufficiency.satisfied)
                rendered, _counts, _count, _digests = render_packet_context(
                    packet
                )
                self.assertIn(process_rule, rendered)

    def test_same_root_durable_context_and_open_collapse_once(self):
        context_text = "I test a smaller release before choosing a direction."
        first_entry = self.add_public_root(
            120,
            context_text,
            "2026-07-20T10:00:00+00:00",
        )
        second_entry = self.add_public_root(
            121,
            "I compare the result and revise before choosing again.",
            "2026-07-22T10:00:00+00:00",
        )
        formed = ledger.form_atomic_knowledge_candidate(
            self.conn,
            ledger.AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key="discord_user:7",
                subject_display_name="Test Member",
                predicate_key="decision_process_motif",
                meaning="Tests a release, compares results, and revises.",
                root_entry_ids=(first_entry, second_entry),
                participant_keys=("discord_user:7",),
                epistemic_status="observed",
                uncertainty_note="Repeated public process observation.",
                currentness="historical",
                contradiction_key="discord_user:7:decision_process_motif",
                retrieval_tags=("decision_process",),
            ),
        )
        self.assertEqual(formed.outcome, "created")
        self.conn.commit()
        state = ledger.read_public_assessment_root_state(
            self.conn,
            entry_id=first_entry,
            guild_id=1,
            subject_key="discord_user:7",
        )
        self.assertIsNotNone(state)

        packet = self.packet(
            "What am I all about?",
            context_row_id=120,
            context_text=context_text,
        )
        selected_representations = tuple(
            item
            for item in packet.items
            if state.root_identity in item.root_identities
            and item.lane
            in {
                "approved_fact",
                "atomic_knowledge",
                "moment",
                "conversation_context",
                "assessment_observation",
            }
        )

        with self.subTest(invariant="one_same_root_representation"):
            self.assertEqual(
                len(selected_representations),
                1,
                tuple(item.lane for item in selected_representations),
            )
        with self.subTest(invariant="exact_safe_text_preserved"):
            self.assertTrue(
                any(
                    context_text == item.text
                    or context_text in item.supporting_observations
                    for item in selected_representations
                )
            )
        with self.subTest(invariant="validation_material_point_collapsed"):
            root_bound = tuple(
                item
                for item in packet.validation_items
                if item.lane
                in {
                    "atomic_knowledge",
                    "conversation_context",
                    "assessment_observation",
                }
                and set(item.root_identities).intersection(
                    {state.root_identity}
                )
            )
            point_map = material_profile_point_map(root_bound)
            self.assertEqual(len(set(point_map.values())), 1)

    def test_independent_open_roots_survive(self):
        self.add_public_root(
            130,
            "I tune ceramic antennas for unusual signal harmonics.",
            "2026-07-20T10:00:00+00:00",
        )
        self.add_public_root(
            131,
            "I plan interface releases around staged verification.",
            "2026-07-22T10:00:00+00:00",
        )

        packet = self.packet("What am I all about?")
        observations = self.assessment_items(packet)
        roots = {
            root
            for item in observations
            for root in item.root_identities
            if root
        }

        self.assertEqual(len(observations), 2)
        self.assertEqual(len(roots), 2)
        self.assertEqual(packet.profile_sufficiency.status, "rich")

    def test_one_ordinary_open_observation_is_sparse_not_rich(self):
        self.add_public_root(
            140,
            "I tune ceramic antennas for unusual signal harmonics.",
            "2026-07-20T10:00:00+00:00",
            user_id=14,
            user_name="Ordinary Member",
        )

        packet = self.packet(
            "What am I all about?",
            user_id=14,
            user_name="Ordinary Member",
        )

        self.assertEqual(packet.profile_sufficiency.status, "sparse")
        self.assertTrue(packet.profile_sufficiency.satisfied)
        self.assertEqual(packet.profile_sufficiency.candidate_point_count, 1)
        self.assertEqual(packet.profile_sufficiency.required_point_count, 1)
        self.assertEqual(packet.profile_sufficiency.independent_root_count, 1)
        self.assertEqual(
            packet.profile_sufficiency.independent_occurrence_count,
            1,
        )

    def test_short_paraphrases_share_one_order_independent_material_point(self):
        entry_ids = (
            self.add_public_root(
                150,
                "I test small builds.",
                "2026-07-20T10:00:00+00:00",
            ),
            self.add_public_root(
                151,
                "I test the small builds.",
                "2026-07-22T10:00:00+00:00",
            ),
        )
        states = tuple(
            ledger.read_public_assessment_root_state(
                self.conn,
                entry_id=entry_id,
                guild_id=1,
                subject_key="discord_user:7",
            )
            for entry_id in entry_ids
        )
        self.assertTrue(all(state is not None for state in states))
        self.assertEqual(
            len({state.semantics.point_identity for state in states}),
            1,
        )

        packet = self.packet("What am I all about?")
        observations = tuple(
            item
            for item in packet.validation_items
            if item.lane == "assessment_observation"
        )
        forward = material_profile_point_map(observations)
        reversed_map = material_profile_point_map(
            tuple(reversed(observations))
        )

        self.assertEqual(forward, reversed_map)
        self.assertEqual(len(set(forward.values())), 1)
        self.assertEqual(packet.profile_sufficiency.status, "sparse")
        self.assertEqual(packet.profile_sufficiency.candidate_point_count, 1)

    def add_recognized_canon_fixture(self):
        user_id = 118
        user_name = "Mac Modem"
        first_text = "I tune ceramic antennas for signal harmonics."
        context_text = "I tune the ceramic antenna for signal harmonics."
        self.add_public_root(
            160,
            first_text,
            "2026-07-20T10:00:00+00:00",
            user_id=user_id,
            user_name=user_name,
        )
        self.add_public_root(
            161,
            context_text,
            "2026-07-20T10:05:00+00:00",
            user_id=user_id,
            user_name=user_name,
        )
        wording = "What am I all about?"
        return user_id, user_name, first_text, context_text, wording

    def assert_recognized_canon(self, packet):
        self.assertTrue(
            any(
                item.lane == "canon"
                and item.source_type == "recognized_canon_fact"
                for item in packet.items
            )
        )

    def test_recognized_canon_context_renders_after_member_context(self):
        (
            user_id,
            user_name,
            _first_text,
            context_text,
            wording,
        ) = self.add_recognized_canon_fixture()

        context_packet = self.packet(
            wording,
            user_id=user_id,
            user_name=user_name,
            context_row_id=161,
            context_text=context_text,
        )
        self.assert_recognized_canon(context_packet)
        rendered_context, _counts, _count, _digests = render_packet_context(
            context_packet
        )
        self.assertLess(
            rendered_context.index("recent public context"),
            rendered_context.index("approved canon"),
        )

    def test_recognized_canon_open_renders_after_open_signal(self):
        (
            user_id,
            user_name,
            _first_text,
            _context_text,
            wording,
        ) = self.add_recognized_canon_fixture()
        open_packet = self.packet(
            wording,
            user_id=user_id,
            user_name=user_name,
        )
        self.assert_recognized_canon(open_packet)
        rendered_open, _counts, _count, _digests = render_packet_context(
            open_packet
        )
        self.assertLess(
            rendered_open.index("question-scoped public observation"),
            rendered_open.index("approved canon"),
        )

    def test_recognized_canon_combined_clause_cannot_pass_as_member_first(self):
        (
            user_id,
            user_name,
            _first_text,
            _context_text,
            wording,
        ) = self.add_recognized_canon_fixture()
        open_packet = self.packet(
            wording,
            user_id=user_id,
            user_name=user_name,
        )
        self.assert_recognized_canon(open_packet)
        basis = self.synthesis_basis(open_packet)
        member_first = candidate_profile_coverage(
            basis,
            (
                "You tune ceramic antennas for signal harmonics. "
                "Mac Modem is a founding BARCODE member and chaotic tech "
                "entity."
            ),
        )
        self.assertTrue(member_first.member_first)
        self.assertFalse(member_first.lore_dominant)

        combined_canon_first = candidate_profile_coverage(
            basis,
            (
                "Mac Modem, your approved BARCODE identity anchor, tunes "
                "ceramic antennas for signal harmonics."
            ),
        )
        self.assertFalse(combined_canon_first.member_first)
        self.assertFalse(combined_canon_first.lore_dominant)
        self.assertEqual(
            combined_canon_first.unsupported_factual_claim_count,
            1,
        )
        self.assertEqual(
            combined_canon_first.claim_classifications,
            ("unsupported_factual",),
        )


if __name__ == "__main__":
    unittest.main()
