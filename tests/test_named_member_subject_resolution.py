"""Public member lookup feeds existing subject-scoped source readers."""

import os
import sqlite3
import tempfile
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_memory_ledger as ledger
import bnl_unified_intelligence_packet as packets
import test_governed_subject_packet_v6 as packet_fixture


class NamedMemberSubjectResolutionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = packet_fixture.GovernedSubjectPacketV6Tests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.conn = self.fixture.conn
        self.members = [self.member(222, "TestMarbles")]
        self.guild = SimpleNamespace(id=1, members=self.members)
        self.guild_patch = mock.patch.object(
            bot.client, "get_guild", return_value=self.guild,
        )
        self.guild_patch.start()
        self.addCleanup(self.guild_patch.stop)
        self.owner_patch = mock.patch.object(bot, "BNL_OWNER_USER_ID", 99)
        self.owner_patch.start()
        self.addCleanup(self.owner_patch.stop)

    @staticmethod
    def member(user_id, label, *, username=None, bot_member=False):
        return SimpleNamespace(
            id=user_id, display_name=label, global_name=label,
            name=username or label, bot=bot_member,
        )

    def frame(self, text, *, addressings=(), policy="sealed_test"):
        decision = bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer", engagement_reason="question",
            channel_policy=policy, addressings=addressings,
            context_result=None, moment_situation=None, guild_id=1,
            channel_id=10, route_mode="normal_chat",
            conversation_surface=bot.conversation_surface_for_channel_policy(policy),
            current_text=text, current_speaker_user_ids=(111,),
            current_speaker_labels=("Test Requester",), influence_mode="live",
        )
        return decision.situation_frame

    def request(self, frame, text):
        subjects = tuple(
            packets.PacketFrameSubject(
                user_id=s.user_id, entity_ref=s.entity_ref,
                label_hint=s.label_hint, binding_method=s.binding_method,
                confidence=s.confidence, role_hints=s.role_hints,
                domain_hints=s.domain_hints,
            )
            for s in frame.subjects
        )
        tasks = tuple(
            packets.PacketFrameTask(
                task_id=t.task_id, text_digest=t.text_digest,
                task_kind=t.task_kind, object_kind=t.object_kind,
                authority_scope=t.authority_scope,
                temporal_scope=t.temporal_scope, currentness=t.currentness,
                required_response_act=t.required_response_act,
                subject_requirement=t.subject_requirement,
                subject_indexes=t.subject_indexes,
            )
            for t in frame.tasks
        )
        request = self.fixture.request(
            subjects=subjects, text=text, frame_status=frame.status,
            requirement=frame.subject_requirement, tasks=tasks,
            object_kind=frame.object_kind,
        )
        return replace(
            request, now="2026-09-09T18:00:00+00:00",
            frame_temporal_scope=frame.temporal_scope,
            frame_currentness=frame.currentness,
            frame_ambiguity_reasons=frame.ambiguity_reasons,
        )

    def seed(self, row_id, user_id, text, *, policy="public_home"):
        self.conn.execute(
            "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?)",
            (row_id, 1, user_id, "TestMarbles", "user", text, 20, policy,
             "normal_chat", "2026-09-08T12:00:00+00:00"),
        )
        return ledger.shadow_conversation_row(
            self.conn, row_id=row_id, user_id=user_id,
            user_name="TestMarbles", guild_id=1, role="user", content=text,
            channel_name="test-public-stage", channel_policy=policy,
            channel_id=20, route_mode="normal_chat",
            observed_at="2026-09-08T12:00:00+00:00",
        )

    def test_natural_member_references_bind_one_stable_subject(self):
        for policy in ("public_home", "sealed_test", "public_context"):
            for text in (
                "What has testmarbles said about stealing pants?",
                "What did TestMarbles say about the stage costumes?",
                "Tell me about TestMarbles and the borrowed coats.",
                "Anything TestMarbles mentioned about the costumes?",
            ):
                with self.subTest(policy=policy, text=text):
                    frame = self.frame(text, policy=policy)
                    self.assertEqual(frame.status, "resolved")
                    self.assertEqual(frame.subject_requirement, "required")
                    self.assertEqual(tuple(s.user_id for s in frame.subjects), (222,))
                    self.assertTrue(any(t.authority_scope == "packet" for t in frame.tasks))
                    resolution = packets.resolve_packet_subject(
                        self.conn, self.request(frame, text), environ=self.fixture.flags,
                    )
                    self.assertEqual(resolution.subject_user_id, 222)
                    self.assertEqual(resolution.binding_method, "stable_discord_account")

    def test_complete_username_is_a_public_lookup_key(self):
        self.members[0].name = "test_marbles"
        frame = self.frame("What has TEST_MARBLES said about the costumes?")
        self.assertEqual(tuple(s.user_id for s in frame.subjects), (222,))
        self.assertEqual(frame.subjects[0].label_hint, "test_marbles")

    def test_longer_label_wins_only_for_the_same_text_span(self):
        self.members[:] = [self.member(222, "Test Member"), self.member(333, "Test")]
        references, unresolved = bot._named_public_member_subjects(
            self.guild, "Compare Test Member and Test.",
        )
        self.assertEqual(dict(references), {222: "Test Member", 333: "Test"})
        self.assertEqual(unresolved, ())
        references, _ = bot._named_public_member_subjects(
            self.guild, "What has Test Member said?",
        )
        self.assertEqual(dict(references), {222: "Test Member"})

    def test_topic_word_matching_another_member_is_not_an_author(self):
        self.members[:] = [self.member(222, "Test Signal"), self.member(333, "Pants")]
        text = "What has Test Signal said about pants?"
        frame = self.frame(text)
        self.assertEqual(frame.status, "resolved")
        self.assertEqual(tuple(s.user_id for s in frame.subjects), (222,))
        for text in (
            "Tell me about Test Signal.",
            "Anything Test Signal mentioned concerning pants?",
            "What did Test Signal say regarding Pants?",
        ):
            with self.subTest(text=text):
                self.assertEqual(tuple(s.user_id for s in self.frame(text).subjects), (222,))

    def test_comparisons_and_new_clauses_keep_real_multiple_subjects(self):
        self.members[:] = [self.member(222, "Test Signal"), self.member(333, "Pants")]
        for text in (
            "Compare Test Signal and Pants.",
            "Tell me about Test Signal and also about Pants.",
            "What has Test Signal said about costumes? What has Pants said?",
            "What has Test Signal said about costumes and what has Pants said about the lights?",
        ):
            with self.subTest(text=text):
                frame = self.frame(text)
                self.assertEqual(frame.status, "resolved")
                self.assertEqual({s.user_id for s in frame.subjects}, {222, 333})

    def test_missing_typed_label_cannot_shift_a_named_subject_label(self):
        addressing = bot.DiscordTurnAddressing(
            speaker="Test Requester", explicit_tag_recipients=(),
            reply_target="none", explicitly_mentions_bnl=False,
            reply_targets_bnl=False, directly_targets_bnl=False,
            targets_other_human=False, plain_text_names_bnl=False,
            speaker_user_id=111, subject_user_ids=(333,),
        )
        frame = self.frame("Compare <@333> and TestMarbles.", addressings=(addressing,))
        self.assertEqual(frame.status, "resolved")
        self.assertEqual({s.user_id: s.label_hint for s in frame.subjects}, {333: "", 222: "TestMarbles"})

    def test_typed_leading_addressee_does_not_become_a_recall_subject(self):
        self.members.append(self.member(333, "Test Receiver"))
        addressing = bot.DiscordTurnAddressing(
            speaker="Test Requester", explicit_tag_recipients=("@Test Receiver",),
            reply_target="none", explicitly_mentions_bnl=False,
            reply_targets_bnl=False, directly_targets_bnl=False,
            targets_other_human=True, plain_text_names_bnl=False,
            speaker_user_id=111, explicit_tag_user_ids=(333,),
        )
        frame = self.frame(
            "@Test Receiver, what has TestMarbles said about costumes?",
            addressings=(addressing,),
        )
        self.assertEqual(tuple(s.user_id for s in frame.subjects), (222,))
        self.assertEqual(frame.addressee_user_ids, (333,))

    def test_partial_names_and_bot_labels_do_not_bind(self):
        self.members.append(self.member(333, "TestBot", bot_member=True))
        for text in ("What has TestMarblesExtra said?", "What has Marbles said?", "What has TestBot said?"):
            with self.subTest(text=text):
                references, unresolved = bot._named_public_member_subjects(self.guild, text)
                self.assertEqual(references, ())
                self.assertEqual(unresolved, ())

    def test_duplicate_public_label_remains_unresolved(self):
        self.members.append(self.member(333, "TestMarbles"))
        text = "What has TestMarbles said about the costumes?"
        frame = self.frame(text)
        self.assertEqual(frame.status, "ambiguous")
        self.assertIn("member_label_unresolved", frame.ambiguity_reasons)
        self.assertEqual(tuple(s.user_id for s in frame.subjects), (0,))
        resolution = packets.resolve_packet_subject(
            self.conn, self.request(frame, text), environ=self.fixture.flags,
        )
        self.assertEqual(resolution.status, "ambiguous")
        self.assertEqual(resolution.subject_user_id, 0)

    def test_typed_mention_disambiguates_its_own_public_label(self):
        self.members.append(self.member(333, "TestMarbles"))
        addressing = bot.DiscordTurnAddressing(
            speaker="Test Requester", explicit_tag_recipients=("@TestMarbles",),
            reply_target="none", explicitly_mentions_bnl=False,
            reply_targets_bnl=False, directly_targets_bnl=False,
            targets_other_human=True, plain_text_names_bnl=False,
            speaker_user_id=111, explicit_tag_user_ids=(222,),
            subject_user_ids=(222,),
        )
        frame = self.frame("What has @TestMarbles said?", addressings=(addressing,))
        self.assertEqual(frame.status, "resolved")
        self.assertEqual(tuple(s.user_id for s in frame.subjects), (222,))
        self.assertEqual(bot._typed_governed_subject_user_ids(
            SimpleNamespace(content="What has <@222> said about the costumes?"),
        ), (222,))

    def test_mixed_known_and_ambiguous_names_preserve_both(self):
        self.members.extend([self.member(333, "Shared Alias"), self.member(444, "Shared Alias")])
        text = "Compare TestMarbles and Shared Alias on the costumes."
        frame = self.frame(text)
        self.assertEqual(frame.status, "ambiguous")
        self.assertEqual({s.user_id for s in frame.subjects}, {0, 222})
        resolution = packets.resolve_packet_subject(
            self.conn, self.request(frame, text), environ=self.fixture.flags,
        )
        self.assertEqual(resolution.status, "ambiguous")

    def test_owner_display_fields_are_not_inspected(self):
        class Owner:
            id = 99
            bot = False

            @property
            def display_name(self):
                raise AssertionError("Owner display field was accessed")

        self.members.append(Owner())
        frame = self.frame("What is 6 Bit's role?")
        self.assertEqual(tuple(s.entity_ref for s in frame.subjects), ("6_bit",))

    def test_no_guild_member_means_no_inferred_platform_binding(self):
        self.members.clear()
        references, unresolved = bot._named_public_member_subjects(
            self.guild, "What has TestMarbles said about the costumes?",
        )
        self.assertEqual((references, unresolved), ((), ()))

    def test_topical_recall_delivers_authored_target_discord_evidence(self):
        text = "What has testmarbles said about stealing pants?"
        authored = "I keep stealing pants from the costume rack."
        self.seed(900, 222, authored)
        self.seed(901, 111, "I am stealing pants from an unrelated set.")
        self.seed(902, 222, "My private pants note stays here.", policy="internal_controlled")
        frame = self.frame(text)
        self.conn.commit()
        with tempfile.TemporaryDirectory() as directory:
            source_path = os.path.join(directory, "conversation-sources.db")
            with sqlite3.connect(source_path) as disk_source:
                self.conn.backup(disk_source)
            with mock.patch.object(bot, "DB_FILE", source_path):
                context, basis = bot.build_named_public_conversation_context(
                    situation_frame=frame, guild_id=1,
                    route_mode="normal_chat", channel_policy="sealed_test",
                    user_text=text, channel_id=10, channel_name="bnl-testing",
                )
        self.assertIn(authored, context)
        self.assertIsNotNone(basis)
        evidence = tuple(
            packets.PacketConversationEvidence(
                text=item.text, source_id=item.source_id,
                speaker_user_id=item.speaker_user_id,
                speaker_label=item.speaker_label, current_turn=item.current_turn,
            )
            for item in basis.evidence_items
        )
        packet = packets.build_packet(
            self.conn, replace(self.request(frame, text), conversation_evidence=evidence),
            environ=self.fixture.flags, persist=False,
        )
        self.assertEqual(packet.subject_resolution.subject_user_id, 222)
        selected = [item for item in packet.items if authored in item.text]
        self.assertTrue(selected)
        self.assertTrue(all(item.subject_key == "discord_user:222" for item in selected))
        self.assertTrue(all(item.source_digest for item in selected))
        self.assertFalse(any("unrelated set" in item.text or "private pants" in item.text for item in packet.items))
        self.assertEqual(packet.diagnostics.invalid_invariants, [])
        self.assertTrue(packets.revalidate_packet(
            self.conn, packet, environ=self.fixture.flags,
        ).valid)
        self.conn.execute("DELETE FROM conversations WHERE id=900")
        self.conn.commit()
        self.assertFalse(packets.revalidate_packet(
            self.conn, packet, environ=self.fixture.flags,
        ).valid)


if __name__ == "__main__":
    unittest.main()
