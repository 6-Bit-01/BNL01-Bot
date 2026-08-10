import os
import sqlite3
import unittest
from dataclasses import replace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot
import bnl_canon_entity_binding as bindings
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
import bnl_unified_intelligence_packet as packet_module
from bnl_canon_source_contract import Confidence, SourceClass, Visibility
from bnl_unified_intelligence_packet import (
    IntelligencePacketDiagnostics,
    IntelligencePacketItem,
    IntelligencePacketRequest,
    PacketConversationEvidence,
    PacketFrameSubject,
    PacketSubjectResolution,
    SCHEMA_VERSION,
    UnifiedIntelligencePacket,
    build_packet,
    resolve_packet_subject,
    revalidate_packet,
)


class GovernedSubjectPacketV6Tests(unittest.TestCase):
    def setUp(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
            "BNL_OWNER_USER_ID": "99",
            "BNL_PRIMARY_GUILD_ID": "1",
            "BNL_DECLARED_CANON_AUTHORITY_SECRET": (
                "governed-subject-test-authority-secret-0001"
            ),
        }
        self.env = mock.patch.dict(
            os.environ,
            self.flags,
            clear=False,
        )
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)
        bindings.ensure_canon_entity_binding_schema(self.conn)
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
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def request(
        self,
        *,
        subjects=(),
        text="Who is the selected member?",
        requirement="required",
        frame_status="resolved",
        role_hints=(),
        domain_hints=(),
        object_kind="person",
        phase="request",
        evidence=(),
        budget_chars=2400,
    ):
        return IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=0,
            route_mode="normal_chat",
            conversation_surface="public_home",
            channel_id=10,
            channel_policy="public_home",
            visibility_allowance="public_safe",
            user_text=text,
            participant_user_ids=(111, 222),
            direct_state="direct",
            budget_chars=budget_chars,
            conversation_evidence=tuple(evidence),
            frame_schema_version="situation_frame_v1",
            frame_revision="sf_test_subject_01",
            frame_input_evidence_digest="a" * 64,
            frame_status=frame_status,
            frame_subject_requirement=requirement,
            frame_subjects=tuple(subjects),
            frame_role_hints=tuple(role_hints),
            frame_domain_hints=tuple(domain_hints),
            frame_event_relation="uncertain",
            frame_task_kind="answer",
            frame_object_kind=object_kind,
            frame_phase=phase,
            frame_temporal_scope="current",
            frame_currentness="current",
            now="2026-08-10T12:00:00+00:00",
        )

    def bind(self, account_id, entity_id, nonce):
        return bindings.bind_discord_account(
            self.conn,
            actor_user_id=99,
            authority_nonce=nonce,
            guild_id=1,
            account_id=account_id,
            entity_id=entity_id,
            reason="governed subject test binding",
        )

    def test_packet_schema_is_v6_and_alias_matrix_stays_distinct(self):
        self.assertEqual(SCHEMA_VERSION, "unified_intelligence_packet_v6")
        cases = {
            "Who is Mac Mod3m?": ("mac_modem",),
            "Tell me about DJ Floppy Disc.": ("dj_floppydisc",),
            "What do you know about Cache Back?": ("cache_back",),
            "Who is Call’em Bini?": ("call_em_bini",),
            "What is 6 Bit's role?": ("6_bit",),
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                actual = tuple(
                    key
                    for key, _label in bnl01_bot._typed_canon_subject_references(
                        text
                    )
                )
                self.assertEqual(actual, expected)
        combined = bnl01_bot._typed_canon_subject_references(
            "Compare Cache Back and Call'em Bini."
        )
        self.assertEqual(
            {key for key, _label in combined},
            {"cache_back", "call_em_bini"},
        )

    def test_typed_mention_subject_replaces_speaker_fallback(self):
        addressing = bnl01_bot.DiscordTurnAddressing(
            speaker="Requester",
            explicit_tag_recipients=("@Selected Member",),
            reply_target="none",
            explicitly_mentions_bnl=True,
            reply_targets_bnl=False,
            directly_targets_bnl=True,
            targets_other_human=True,
            plain_text_names_bnl=False,
            speaker_user_id=111,
            explicit_tag_user_ids=(222,),
            subject_user_ids=(222,),
        )
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="direct_request",
            channel_policy="public_home",
            addressings=(addressing,),
            context_result=None,
            moment_situation=None,
            guild_id=1,
            channel_id=10,
            route_mode="normal_chat",
            conversation_surface="public_home",
            current_text="Tell me about @Selected Member.",
            current_speaker_user_ids=(111,),
            current_speaker_labels=("Requester",),
            influence_mode="live",
            packet_revision="turn_subject_01",
        )

        frame = decision.situation_frame
        self.assertEqual(frame.subject_requirement, "required")
        self.assertEqual(tuple(item.user_id for item in frame.subjects), (222,))
        self.assertNotIn(111, tuple(item.user_id for item in frame.subjects))

    def test_unseen_label_and_multiple_subjects_fail_closed(self):
        unseen = self.request(
            subjects=(
                PacketFrameSubject(
                    label_hint="Unseen Member",
                    binding_method="reversible_label_hint",
                ),
            )
        )
        unseen_resolution = resolve_packet_subject(self.conn, unseen)
        self.assertEqual(unseen_resolution.status, "unresolved")
        self.assertEqual(unseen_resolution.subject_user_id, 0)
        self.assertEqual(
            unseen_resolution.reason_codes,
            ("display_label_not_identity_authority",),
        )

        multiple = self.request(
            subjects=(
                PacketFrameSubject(user_id=222),
                PacketFrameSubject(user_id=223),
            ),
            frame_status="ambiguous",
        )
        multiple_resolution = resolve_packet_subject(self.conn, multiple)
        self.assertEqual(multiple_resolution.status, "ambiguous")
        packet = build_packet(
            self.conn,
            multiple,
            persist=False,
            environ=self.flags,
        )
        self.assertEqual(packet.diagnostics.revalidation_status, "subject_ambiguous")
        self.assertTrue(
            all(item.lane == "current_intent" for item in packet.items)
        )

    def test_account_binding_and_reverse_alias_resolution_agree(self):
        self.bind(222, "mac_modem", "subject-bind-nonce-0001")
        mentioned = self.request(
            subjects=(
                PacketFrameSubject(
                    user_id=222,
                    binding_method="existing_typed_target",
                ),
            )
        )
        mention_resolution = resolve_packet_subject(self.conn, mentioned)
        self.assertEqual(mention_resolution.status, "resolved")
        self.assertEqual(mention_resolution.subject_user_id, 222)
        self.assertEqual(mention_resolution.entity_ref, "mac_modem")
        self.assertEqual(mention_resolution.binding_method, "account_binding")

        alias = self.request(
            subjects=(
                PacketFrameSubject(
                    entity_ref="mac_modem",
                    label_hint="Mac Modem",
                    binding_method="existing_typed_entity",
                ),
            ),
            text="Who is Mac Modem?",
        )
        alias_resolution = resolve_packet_subject(self.conn, alias)
        self.assertEqual(alias_resolution.status, "resolved")
        self.assertEqual(alias_resolution.subject_user_id, 222)
        self.assertEqual(alias_resolution.entity_ref, "mac_modem")
        self.assertEqual(alias_resolution.binding_method, "reverse_account_binding")

    def test_typed_alias_without_account_is_canon_only(self):
        cache = resolve_packet_subject(
            self.conn,
            self.request(
                subjects=(
                    PacketFrameSubject(
                        entity_ref="cache_back",
                        label_hint="Cache Back",
                        binding_method="existing_typed_entity",
                    ),
                ),
                text="Who is Cache Back?",
            ),
        )
        bini = resolve_packet_subject(
            self.conn,
            self.request(
                subjects=(
                    PacketFrameSubject(
                        entity_ref="call_em_bini",
                        label_hint="Call'em Bini",
                        binding_method="existing_typed_entity",
                    ),
                ),
                text="Who is Call'em Bini?",
            ),
        )
        self.assertEqual(cache.subject_key, "cache_back")
        self.assertEqual(bini.subject_key, "call_em_bini")
        self.assertNotEqual(cache.subject_key, bini.subject_key)
        self.assertEqual(cache.subject_user_id, 0)
        self.assertEqual(bini.subject_user_id, 0)

    def test_reverse_binding_collision_is_ambiguous(self):
        self.bind(222, "mac_modem", "subject-bind-nonce-0002")
        self.bind(223, "mac_modem", "subject-bind-nonce-0003")
        resolution = resolve_packet_subject(
            self.conn,
            self.request(
                subjects=(
                    PacketFrameSubject(
                        entity_ref="mac_modem",
                        binding_method="existing_typed_entity",
                    ),
                ),
            ),
        )
        self.assertEqual(resolution.status, "ambiguous")
        self.assertIn(
            "entity_has_multiple_discord_accounts",
            resolution.reason_codes,
        )

    def test_binding_retirement_invalidates_frozen_packet(self):
        bound = self.bind(222, "mac_modem", "subject-bind-nonce-0004")
        request = self.request(
            subjects=(
                PacketFrameSubject(
                    user_id=222,
                    binding_method="existing_typed_target",
                ),
            ),
            text="Who is Mac Modem?",
        )
        packet = build_packet(
            self.conn,
            request,
            persist=False,
            environ=self.flags,
        )
        self.assertEqual(packet.subject_resolution.status, "resolved")
        self.assertTrue(packet.source_snapshot_digest)
        self.assertTrue(revalidate_packet(self.conn, packet).valid)
        self.conn.commit()

        bindings.retire_discord_account_binding(
            self.conn,
            actor_user_id=99,
            authority_nonce="subject-retire-nonce-0001",
            guild_id=1,
            binding_id=bound.revision.binding_id,
            expected_revision_id=bound.revision.binding_revision_id,
            reason="test retirement between frame and packet",
        )
        result = revalidate_packet(self.conn, packet)
        self.assertFalse(result.valid)
        self.assertEqual(result.status, "subject_binding_changed")

    def test_cross_subject_items_are_removed_before_scoring(self):
        evidence = (
            PacketConversationEvidence(
                text="Tell me about the selected member.",
                speaker_user_id=111,
                speaker_label="Requester",
                current_turn=True,
            ),
            PacketConversationEvidence(
                text="The selected member works on modular performance systems.",
                speaker_user_id=222,
                speaker_label="Selected Member",
            ),
            PacketConversationEvidence(
                text="The requester works on an unrelated archive.",
                speaker_user_id=111,
                speaker_label="Requester",
            ),
        )
        packet = build_packet(
            self.conn,
            self.request(
                subjects=(
                    PacketFrameSubject(
                        user_id=222,
                        binding_method="existing_typed_target",
                    ),
                ),
                evidence=evidence,
            ),
            persist=False,
            environ=self.flags,
        )
        self.assertEqual(packet.subject_resolution.subject_user_id, 222)
        self.assertTrue(
            all(
                item.lane == "current_intent"
                or item.subject_key == "discord_user:222"
                for item in packet.items
            )
        )
        self.assertGreater(
            packet.diagnostics.excluded_by_reason.get(
                "frame_subject_mismatch",
                0,
            ),
            0,
        )

    def test_role_domain_task_and_time_filter_before_selection(self):
        request = self.request(
            subjects=(PacketFrameSubject(user_id=222),),
            role_hints=("operator",),
            domain_hints=("operational",),
            object_kind="website",
            phase="diagnosis",
        )
        resolution = PacketSubjectResolution(
            status="resolved",
            subject_user_id=222,
            subject_key="discord_user:222",
            binding_method="stable_discord_account",
            binding_digest="binding-digest",
        )
        operational = IntelligencePacketItem(
            lane="canon",
            source_class="approved_canon",
            source_type="declared_canon_claim",
            source_ref="declared:operational",
            source_digest="operational-digest",
            subject_key="discord_user:222",
            predicate_key="operator_role",
            text="Current operator role.",
            visibility="public_safe",
            confidence="approved",
            lifecycle="established",
            authority=6,
            canon_status="declared",
            canon_domain="operational",
            canon_claim_kind="role",
        )
        lore = replace(
            operational,
            source_ref="declared:lore",
            source_digest="lore-digest",
            predicate_key="lore_role",
            text="Historical lore role.",
            canon_domain="lore",
        )
        diagnostics = IntelligencePacketDiagnostics()
        exclusions = []
        kept = packet_module._filter_frame_applicable_candidates(
            request,
            resolution,
            [lore, operational],
            diagnostics,
            exclusions,
        )
        self.assertEqual(kept, [operational])
        self.assertEqual(
            diagnostics.excluded_by_reason.get("frame_domain_mismatch"),
            1,
        )

    def test_content_free_receipt_records_frame_and_subject_state(self):
        packet = build_packet(
            self.conn,
            self.request(
                subjects=(
                    PacketFrameSubject(
                        user_id=222,
                        label_hint="Private Display Hint",
                        binding_method="existing_typed_target",
                    ),
                ),
            ),
            persist=True,
            environ=self.flags,
        )
        row = self.conn.execute(
            """
            SELECT frame_revision,frame_input_digest,
                   subject_resolution_status,subject_resolution_method,
                   subject_resolution_candidate_count,
                   frame_applicability_exclusion_count,
                   source_snapshot_digest
            FROM memory_governance_intelligence_packet_runs
            WHERE run_id=?
            """,
            (packet.diagnostics.receipt_run_id,),
        ).fetchone()
        self.assertEqual(row[0], "sf_test_subject_01")
        self.assertEqual(row[1], "a" * 64)
        self.assertEqual(row[2], "resolved")
        self.assertEqual(row[3], "stable_discord_account")
        self.assertEqual(row[4], 1)
        self.assertTrue(row[6])
        self.assertNotIn("Private Display Hint", repr(row))
        columns = {
            str(column[1])
            for column in self.conn.execute(
                "PRAGMA table_info(memory_governance_intelligence_packet_runs)"
            ).fetchall()
        }
        self.assertNotIn("subject_user_id", columns)
        self.assertNotIn("subject_label", columns)


if __name__ == "__main__":
    unittest.main()
