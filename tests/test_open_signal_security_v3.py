import os
import sqlite3
import unittest
from dataclasses import replace
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shared_brain_synthesis import (
    SharedBrainSynthesisBasis,
    candidate_profile_coverage,
    render_packet_context,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketItem,
    IntelligencePacketRequest,
    PacketConversationEvidence,
    build_packet,
)


class _OpenSignalSecurityFixture:
    GUILD_ID = 1
    USER_ID = 7
    USER_NAME = "Test Member"
    SUBJECT_KEY = "discord_user:7"

    def __init__(self):
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "false",
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
              guild_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              user_name TEXT NOT NULL,
              role TEXT NOT NULL,
              content TEXT NOT NULL,
              channel_id INTEGER NOT NULL,
              channel_name TEXT NOT NULL,
              channel_policy TEXT NOT NULL,
              message_id INTEGER NOT NULL,
              route_mode TEXT NOT NULL,
              public_usable,
              visibility TEXT NOT NULL,
              timestamp TEXT NOT NULL
            )
            """
        )
        self.next_row_id = 0

    def close(self):
        self.conn.close()
        self.env.stop()

    def add_source(self, text, *, observed_at="2026-08-01T12:00:00+00:00"):
        self.next_row_id += 1
        row_id = self.next_row_id
        message_id = 10_000 + row_id
        self.conn.execute(
            """
            INSERT INTO conversations(
              id,guild_id,user_id,user_name,role,content,channel_id,
              channel_name,channel_policy,message_id,route_mode,
              public_usable,visibility,timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row_id,
                self.GUILD_ID,
                self.USER_ID,
                self.USER_NAME,
                "user",
                str(text),
                10,
                "barcode-bot",
                "public_home",
                message_id,
                "normal_chat",
                1,
                "public",
                str(observed_at),
            ),
        )
        result = ledger.shadow_conversation_row(
            self.conn,
            row_id=row_id,
            user_id=self.USER_ID,
            user_name=self.USER_NAME,
            guild_id=self.GUILD_ID,
            role="user",
            content=str(text),
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy="public_home",
            message_id=message_id,
            route_mode="normal_chat",
            observed_at=str(observed_at),
            source_sequence=row_id,
            environ=self.flags,
        )
        if result.outcome != "inserted":
            raise AssertionError(result)
        self.conn.commit()
        return result.entry_id

    def packet(self, wording="What am I all about?"):
        request = IntelligencePacketRequest(
            guild_id=self.GUILD_ID,
            subject_user_id=self.USER_ID,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            subject_display_name=self.USER_NAME,
            channel_id=10,
            channel_name="barcode-bot",
            channel_policy="public_home",
            visibility_allowance="public_safe",
            user_text=str(wording),
            participant_user_ids=(self.USER_ID,),
            direct_state="direct",
            budget_chars=6000,
            conversation_evidence=(
                PacketConversationEvidence(
                    text=str(wording),
                    speaker_user_id=self.USER_ID,
                    speaker_label=self.USER_NAME,
                    current_turn=True,
                ),
            ),
            now="2026-08-02T12:00:00+00:00",
        )
        return build_packet(
            self.conn,
            request,
            persist=False,
            environ=self.flags,
        )

    @staticmethod
    def basis(packet):
        rendered, lane_counts, item_count, digests = render_packet_context(
            packet
        )
        return SharedBrainSynthesisBasis(
            packet=packet,
            assessment=None,
            rendered_context=rendered,
            expected_packet_digest=packet.diagnostics.packet_digest,
            expected_context_digest="security-test-only",
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

    def coverage(self, response, *, packet=None):
        packet = packet or self.packet()
        return candidate_profile_coverage(self.basis(packet), response)

    @staticmethod
    def assessment_items(packet):
        return tuple(
            item
            for item in packet.validation_items
            if item.lane == "assessment_observation"
        )


class OpenSignalSecurityV3Tests(unittest.TestCase):
    def new_fixture(self):
        fixture = _OpenSignalSecurityFixture()
        self.addCleanup(fixture.close)
        return fixture

    def assert_supported(self, fixture, source, claim):
        fixture.add_source(source)
        coverage = fixture.coverage(claim)
        self.assertEqual(
            coverage.member_supported_claim_count,
            1,
            (source, claim, coverage),
        )
        self.assertEqual(
            coverage.unsupported_factual_claim_count,
            0,
            (source, claim, coverage),
        )

    def assert_unsupported(self, fixture, claim, *, packet=None):
        coverage = fixture.coverage(claim, packet=packet)
        self.assertEqual(
            coverage.member_supported_claim_count,
            0,
            (claim, coverage),
        )
        self.assertEqual(
            coverage.canon_supported_claim_count,
            0,
            (claim, coverage),
        )
        self.assertGreaterEqual(
            coverage.unsupported_factual_claim_count,
            1,
            (claim, coverage),
        )

    def test_sensitive_role_and_static_status_sources_never_enter_open(self):
        cases = (
            (
                "I was diagnosed with diabetes and test insulin schedules.",
                "You test insulin schedules.",
            ),
            (
                "I serve as moderator and review incident reports.",
                "You review incident reports.",
            ),
            (
                "I am a BARCODE administrator who reviews reports.",
                "You review reports.",
            ),
            (
                "I work as a staff member and plan broadcast releases.",
                "You plan broadcast releases.",
            ),
        )
        for source, claim in cases:
            with self.subTest(source=source):
                fixture = self.new_fixture()
                entry_id = fixture.add_source(source)
                self.assertIsNone(
                    ledger.read_public_assessment_root_state(
                        fixture.conn,
                        entry_id=entry_id,
                        guild_id=fixture.GUILD_ID,
                        subject_key=fixture.SUBJECT_KEY,
                    )
                )
                packet = fixture.packet()
                self.assertEqual(fixture.assessment_items(packet), ())
                self.assert_unsupported(fixture, claim, packet=packet)

    def test_external_actor_and_reporting_wrappers_are_not_subject_actions(self):
        cases = (
            "Mac Modem tested the signal meter.",
            "I noticed that Mac Modem tested the signal meter.",
            "I heard Sheila test the signal meter.",
            "According to Mac Modem, I tested the signal meter.",
            "Mac Modem said I tested the signal meter.",
        )
        for source in cases:
            with self.subTest(source=source):
                fixture = self.new_fixture()
                fixture.add_source(source)
                packet = fixture.packet()
                self.assertEqual(fixture.assessment_items(packet), ())
                self.assert_unsupported(
                    fixture,
                    "You tested the signal meter.",
                    packet=packet,
                )

    def test_only_approved_candidate_epistemic_frames_preserve_attribution(self):
        fixture = self.new_fixture()
        fixture.add_source("I tested the signal meter before release.")
        packet = fixture.packet()

        approved = (
            "From your public messages, you tested the signal meter "
            "before release.",
            "Based on the public record, you tested the signal meter "
            "before release.",
            "I've noticed that you tested the signal meter before release.",
        )
        for claim in approved:
            with self.subTest(approved_frame=claim):
                coverage = fixture.coverage(claim, packet=packet)
                self.assertEqual(
                    coverage.member_supported_claim_count,
                    1,
                    coverage,
                )
                self.assertEqual(
                    coverage.unsupported_factual_claim_count,
                    0,
                    coverage,
                )

        external = (
            "Mac Modem says you tested the signal meter before release.",
            "According to Mac Modem, you tested the signal meter "
            "before release.",
        )
        for claim in external:
            with self.subTest(external_frame=claim):
                self.assert_unsupported(fixture, claim, packet=packet)

    def test_passive_and_mixed_polarity_sources_fail_closed(self):
        passive_cases = (
            "I was asked to test the signal meter.",
            "I have been assigned to review the broadcast transition.",
            "We were told to publish the audio mix.",
        )
        for source in passive_cases:
            with self.subTest(passive=source):
                fixture = self.new_fixture()
                fixture.add_source(source)
                packet = fixture.packet()
                self.assertEqual(fixture.assessment_items(packet), ())

        mixed = self.new_fixture()
        mixed.add_source(
            "I test audio, but I do not test visual transitions."
        )
        mixed_packet = mixed.packet()
        for claim in (
            "You test audio.",
            "You test visual transitions.",
            "You do not test visual transitions.",
        ):
            with self.subTest(mixed_polarity_claim=claim):
                self.assert_unsupported(mixed, claim, packet=mixed_packet)

    def test_conditional_evidence_cannot_be_promoted_to_unqualified_fact(self):
        fixture = self.new_fixture()
        fixture.add_source(
            "I test audio if the staged release is ready."
        )
        packet = fixture.packet()

        self.assert_unsupported(
            fixture,
            "You test audio.",
            packet=packet,
        )
        qualified = fixture.coverage(
            "You test audio if the staged release is ready.",
            packet=packet,
        )
        self.assertEqual(qualified.member_supported_claim_count, 1)
        self.assertEqual(qualified.unsupported_factual_claim_count, 0)

    def test_relation_and_polarity_inversions_are_rejected(self):
        cases = (
            (
                "I test the broadcast before release.",
                "You test the broadcast before release.",
                "You test the broadcast after release.",
            ),
            (
                "I test the broadcast after release.",
                "You test the broadcast after release.",
                "You test the broadcast before release.",
            ),
            (
                "I test the broadcast with a signal meter.",
                "You test the broadcast with a signal meter.",
                "You test the broadcast without a signal meter.",
            ),
            (
                "I test the broadcast without a signal meter.",
                "You test the broadcast without a signal meter.",
                "You test the broadcast with a signal meter.",
            ),
            (
                "I test a great audio mix.",
                "You test a great audio mix.",
                "You test a terrible audio mix.",
            ),
            (
                "I test a terrible audio mix.",
                "You test a terrible audio mix.",
                "You test a great audio mix.",
            ),
            (
                "I did not test the broadcast before release.",
                "You did not test the broadcast before release.",
                "You test the broadcast before release.",
            ),
        )
        for source, exact, inverse in cases:
            with self.subTest(source=source):
                fixture = self.new_fixture()
                fixture.add_source(source)
                packet = fixture.packet()
                exact_coverage = fixture.coverage(exact, packet=packet)
                self.assertEqual(
                    exact_coverage.member_supported_claim_count,
                    1,
                    exact_coverage,
                )
                self.assertEqual(
                    exact_coverage.unsupported_factual_claim_count,
                    0,
                    exact_coverage,
                )
                self.assert_unsupported(
                    fixture,
                    inverse,
                    packet=packet,
                )

    def test_one_open_action_cannot_license_compound_or_subordinate_claims(self):
        fixture = self.new_fixture()
        fixture.add_source("I tested the signal meter.")
        packet = fixture.packet()

        exact = fixture.coverage(
            "You tested the signal meter.",
            packet=packet,
        )
        self.assertEqual(exact.member_supported_claim_count, 1, exact)
        self.assertEqual(exact.unsupported_factual_claim_count, 0, exact)

        attacks = (
            "You tested the signal meter and deleted the archive.",
            "You tested the signal meter while stealing passwords.",
            "You tested the signal meter because interference destroyed "
            "the archive.",
            "You tested the signal meter to destroy the software.",
        )
        for claim in attacks:
            with self.subTest(claim=claim):
                self.assert_unsupported(fixture, claim, packet=packet)

    def test_direct_object_and_subordinate_action_cannot_be_substituted(self):
        fixture = self.new_fixture()
        fixture.add_source(
            "I asked Mac Modem to test the signal meter."
        )
        packet = fixture.packet()

        exact = fixture.coverage(
            "You asked Mac Modem to test the signal meter.",
            packet=packet,
        )
        self.assertEqual(exact.member_supported_claim_count, 1, exact)
        self.assertEqual(exact.unsupported_factual_claim_count, 0, exact)

        attacks = (
            "You asked Mac Modem to sabotage the signal meter.",
            "You asked sheila to test the signal meter.",
            "You asked sheila to sabotage the signal meter.",
        )
        for claim in attacks:
            with self.subTest(claim=claim):
                self.assert_unsupported(fixture, claim, packet=packet)

    def test_authored_topic_proposal_preserves_suggested_modality(self):
        fixture = self.new_fixture()
        fixture.add_source(
            "Let's test the signal meter before release."
        )
        packet = fixture.packet()
        items = fixture.assessment_items(packet)
        self.assertEqual(len(items), 1, items)
        with self.subTest(contract="authored_topic_mode"):
            self.assertEqual(items[0].attribution_mode, "authored_topic")
        with self.subTest(contract="suggested_action"):
            self.assertEqual(items[0].action_identity, "suggest")

        completed = fixture.coverage(
            "You tested the signal meter before release.",
            packet=packet,
        )
        with self.subTest(contract="completed_action_rejected"):
            self.assertEqual(
                completed.member_supported_claim_count,
                0,
                completed,
            )
            self.assertGreaterEqual(
                completed.unsupported_factual_claim_count,
                1,
                completed,
            )

        suggested = fixture.coverage(
            "You suggested testing the signal meter before release.",
            packet=packet,
        )
        with self.subTest(contract="matching_suggestion_supported"):
            self.assertEqual(
                suggested.member_supported_claim_count,
                1,
                suggested,
            )
            self.assertEqual(
                suggested.unsupported_factual_claim_count,
                0,
                suggested,
            )

    def test_sensitive_public_sentences_never_become_open_profile_evidence(self):
        cases = (
            (
                "I manage ADHD every day.",
                "You manage ADHD every day.",
            ),
            (
                "I take Prozac every morning.",
                "You take Prozac every morning.",
            ),
            (
                "I support Trump politically.",
                "You support Trump politically.",
            ),
            (
                "I moved to Los Angeles last year.",
                "You moved to Los Angeles last year.",
            ),
            (
                "I owe Visa money right now.",
                "You owe Visa money right now.",
            ),
        )
        for source, claim in cases:
            with self.subTest(source=source):
                fixture = self.new_fixture()
                entry_id = fixture.add_source(source)
                self.assertIsNone(
                    ledger.read_public_assessment_root_state(
                        fixture.conn,
                        entry_id=entry_id,
                        guild_id=fixture.GUILD_ID,
                        subject_key=fixture.SUBJECT_KEY,
                    )
                )
                packet = fixture.packet()
                self.assertEqual(fixture.assessment_items(packet), ())
                self.assert_unsupported(fixture, claim, packet=packet)

    def test_frequency_and_tense_cannot_be_inflated(self):
        cases = (
            (
                "I tested the signal meter once yesterday.",
                "You tested the signal meter once yesterday.",
                "You test the signal meter every day.",
            ),
            (
                "I sometimes review the broadcast mix.",
                "You sometimes review the broadcast mix.",
                "You constantly review the broadcast mix.",
            ),
        )
        for source, exact_claim, inflated_claim in cases:
            with self.subTest(source=source, phase="exact_control"):
                fixture = self.new_fixture()
                fixture.add_source(source)
                packet = fixture.packet()
                exact = fixture.coverage(exact_claim, packet=packet)
                self.assertEqual(
                    exact.member_supported_claim_count,
                    1,
                    exact,
                )
                self.assertEqual(
                    exact.unsupported_factual_claim_count,
                    0,
                    exact,
                )
                self.assert_unsupported(
                    fixture,
                    inflated_claim,
                    packet=packet,
                )

    def test_opinion_frames_cannot_launder_harmful_member_inferences(self):
        fixture = self.new_fixture()
        fixture.add_source(
            "I tested the signal meter before release."
        )
        packet = fixture.packet()

        safe = fixture.coverage(
            "From your public messages, you tested the signal meter before "
            "release.",
            packet=packet,
        )
        self.assertEqual(safe.unsupported_factual_claim_count, 0, safe)
        self.assertEqual(safe.member_supported_claim_count, 1, safe)

        attacks = (
            "My read is that you are cruel.",
            "My read is that you are mentally unstable.",
            "My read is that you are dangerous.",
            "My read is that you are dishonest.",
            "My read is that you exploit people.",
            "My read is that you are likely a criminal.",
        )
        for claim in attacks:
            with self.subTest(claim=claim):
                self.assert_unsupported(fixture, claim, packet=packet)

    @staticmethod
    def _typed_item(
        *,
        lane,
        subject_key,
        predicate_key,
        text,
        source_ref,
        participants=(),
    ):
        return IntelligencePacketItem(
            lane=str(lane),
            source_class="owner_declared"
            if lane == "canon"
            else "first_party_record",
            source_type="recognized_canon_fact"
            if lane == "canon"
            else "approved_self_authored_fact",
            source_ref=str(source_ref),
            source_digest="digest:%s" % source_ref,
            subject_key=str(subject_key),
            predicate_key=str(predicate_key),
            text=str(text),
            visibility="public",
            confidence="high",
            lifecycle="active",
            authority=5 if lane == "canon" else 4,
            participants=tuple(participants),
            root_identities=("root:%s" % source_ref,),
            occurrence_identities=("occurrence:%s" % source_ref,),
            point_identity="point:%s" % source_ref,
            point_group_identity="point:%s" % source_ref,
        )

    @staticmethod
    def _packet_with_items(packet, *items):
        return replace(
            packet,
            items=(*packet.items, *items),
            validation_items=(*packet.validation_items, *items),
        )

    def test_typed_facts_bind_polarity_and_the_complete_claim(self):
        fixture = self.new_fixture()
        fixture.add_source("I tested the signal meter before release.")
        base_packet = fixture.packet()
        favorite_movie = self._typed_item(
            lane="approved_fact",
            subject_key=fixture.SUBJECT_KEY,
            predicate_key="favorite_movie",
            text="Favorite movie: Blade Runner",
            source_ref="fact:favorite-movie-blade-runner",
            participants=(fixture.SUBJECT_KEY,),
        )
        signal_operator = self._typed_item(
            lane="canon",
            subject_key="barcode:mac_modem",
            predicate_key="operator_role",
            text="Mac Modem is the signal operator.",
            source_ref="canon:mac-modem-signal-operator",
            participants=("barcode:mac_modem",),
        )
        packet = self._packet_with_items(
            base_packet,
            favorite_movie,
            signal_operator,
        )

        favorite_control = fixture.coverage(
            "Your favorite movie is Blade Runner.",
            packet=packet,
        )
        self.assertEqual(
            favorite_control.member_supported_claim_count,
            1,
            favorite_control,
        )
        self.assertEqual(
            favorite_control.unsupported_factual_claim_count,
            0,
            favorite_control,
        )
        canon_control = fixture.coverage(
            "Mac Modem is the signal operator.",
            packet=packet,
        )
        self.assertEqual(
            canon_control.canon_supported_claim_count,
            1,
            canon_control,
        )
        self.assertEqual(
            canon_control.unsupported_factual_claim_count,
            0,
            canon_control,
        )

        attacks = (
            "Your favorite movie is not Blade Runner.",
            "Your favorite movie is Blade Runner and hate cinema.",
            "Mac Modem is not the signal operator.",
            "Mac Modem is the signal operator and destroys archives.",
        )
        for claim in attacks:
            with self.subTest(claim=claim):
                self.assert_unsupported(fixture, claim, packet=packet)

    def test_typed_facts_bind_actor_and_complete_value(self):
        fixture = self.new_fixture()
        fixture.add_source("I tested the signal meter before release.")
        base_packet = fixture.packet()
        favorite_movie = self._typed_item(
            lane="approved_fact",
            subject_key=fixture.SUBJECT_KEY,
            predicate_key="favorite_movie",
            text="Favorite movie: Blade Runner",
            source_ref="fact:favorite-movie-actor-value",
            participants=(fixture.SUBJECT_KEY,),
        )
        cache_involvement = self._typed_item(
            lane="canon",
            subject_key="barcode:cache_back",
            predicate_key="typical_involvement",
            text=(
                "Cache Back's typical involvement: recovering lost "
                "BARCODE files."
            ),
            source_ref="canon:cache-back-typical-involvement",
            participants=("barcode:cache_back",),
        )
        packet = self._packet_with_items(
            base_packet,
            favorite_movie,
            cache_involvement,
        )

        favorite_control = fixture.coverage(
            "Your favorite movie is Blade Runner.",
            packet=packet,
        )
        self.assertEqual(
            favorite_control.member_supported_claim_count,
            1,
            favorite_control,
        )
        self.assertEqual(
            favorite_control.unsupported_factual_claim_count,
            0,
            favorite_control,
        )
        cache_control = fixture.coverage(
            "Cache Back's typical involvement is recovering lost BARCODE "
            "files.",
            packet=packet,
        )
        self.assertEqual(
            cache_control.canon_supported_claim_count,
            1,
            cache_control,
        )
        self.assertEqual(
            cache_control.unsupported_factual_claim_count,
            0,
            cache_control,
        )

        attacks = (
            "sheila's favorite movie is Blade Runner.",
            "His favorite movie is Blade Runner.",
            "Their favorite movie is Blade Runner.",
            "Your favorite movie is Blade Runner 2049.",
            "Your favorite movie is Blade Runner and Casablanca.",
            "sheila's typical involvement is recovering lost BARCODE files.",
        )
        for claim in attacks:
            with self.subTest(claim=claim):
                self.assert_unsupported(fixture, claim, packet=packet)

    def test_general_canon_and_unrelated_durable_verbs_do_not_license_you(self):
        fixture = self.new_fixture()
        fixture.add_source("I test the signal meter before release.")
        base_packet = fixture.packet()
        approved_fact = self._typed_item(
            lane="approved_fact",
            subject_key=fixture.SUBJECT_KEY,
            predicate_key="favorite_movie",
            text="Favorite movie: Arrival",
            source_ref="fact:favorite-movie",
            participants=(fixture.SUBJECT_KEY,),
        )
        recognized_canon = self._typed_item(
            lane="canon",
            subject_key=fixture.SUBJECT_KEY,
            predicate_key="recognized_canon_fact",
            text="Test Member is a founding BARCODE member.",
            source_ref="canon:test-member",
            participants=(fixture.SUBJECT_KEY,),
        )
        general_canon = self._typed_item(
            lane="canon",
            subject_key="barcode:network",
            predicate_key="legacy_canon_fact",
            text="BARCODE Network builds experimental radio media.",
            source_ref="canon:barcode-network",
        )
        packet = replace(
            base_packet,
            items=(
                *base_packet.items,
                approved_fact,
                recognized_canon,
                general_canon,
            ),
            validation_items=(
                *base_packet.validation_items,
                approved_fact,
                recognized_canon,
                general_canon,
            ),
        )

        for claim in (
            "You built Arrival.",
            "You manage Arrival.",
            "You build experimental radio media.",
            "You manage experimental radio media.",
        ):
            with self.subTest(unlicensed_claim=claim):
                self.assert_unsupported(fixture, claim, packet=packet)

        approved_control = fixture.coverage(
            "Your favorite movie is Arrival.",
            packet=packet,
        )
        self.assertEqual(approved_control.member_supported_claim_count, 1)
        self.assertEqual(approved_control.unsupported_factual_claim_count, 0)

        canon_control = fixture.coverage(
            "You are a founding BARCODE member.",
            packet=packet,
        )
        self.assertEqual(canon_control.canon_supported_claim_count, 1)
        self.assertEqual(canon_control.unsupported_factual_claim_count, 0)


if __name__ == "__main__":
    unittest.main()
