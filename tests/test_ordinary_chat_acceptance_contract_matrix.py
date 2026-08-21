import os
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def _addressing():
    return bnl01_bot.DiscordTurnAddressing(
        speaker="Test Member",
        explicit_tag_recipients=("@BNL-01",),
        reply_target="none",
        explicitly_mentions_bnl=True,
        reply_targets_bnl=False,
        directly_targets_bnl=True,
        targets_other_human=False,
        plain_text_names_bnl=False,
        speaker_user_id=101,
        source_message_id=301,
    )


def _frame(text):
    decision = bnl01_bot.build_live_conversation_orchestration_decision(
        engagement_decision="answer",
        engagement_reason="direct_request",
        channel_policy="public_home",
        addressings=(_addressing(),),
        context_result=None,
        moment_situation=None,
        guild_id=1,
        channel_id=10,
        route_mode="normal_chat",
        conversation_surface="public_home",
        current_text=text,
        current_speaker_user_ids=(101,),
        current_speaker_labels=("Test Member",),
        influence_mode="live",
        packet_revision="turn_acceptance_matrix",
    )
    return decision.situation_frame


class OrdinaryChatAcceptanceContractMatrixTests(unittest.TestCase):
    def test_48_production_ingress_cases_have_one_early_typed_decision(self):
        # name, request, frame status, task authorities, task acts,
        # dominant object, bound subject kind
        cases = (
            ("self_remember", "What do you remember about me?", "resolved", ("packet",), ("answer",), "person", "speaker"),
            ("self_know", "What do you know about me?", "resolved", ("packet",), ("answer",), "person", "speaker"),
            ("self_identity", "Tell me who I am.", "resolved", ("packet",), ("answer",), "unknown", "speaker"),
            ("self_remember_short", "Remember me?", "resolved", ("packet",), ("answer",), "unknown", "speaker"),
            ("self_profile", "What is my profile?", "resolved", ("packet",), ("answer",), "unknown", "speaker"),
            ("self_history", "What is my history?", "resolved", ("packet",), ("answer",), "unknown", "speaker"),
            ("self_patterns", "What patterns keep recurring for me?", "resolved", ("packet",), ("answer",), "unknown", "speaker"),
            ("self_what", "What am I?", "resolved", ("packet",), ("answer",), "unknown", "speaker"),
            ("bnl_who", "Who are you?", "resolved", ("packet",), ("answer",), "unknown", "bnl"),
            ("bnl_what", "What are you?", "resolved", ("packet",), ("answer",), "unknown", "bnl"),
            ("bnl_tell", "Tell me about yourself.", "resolved", ("packet",), ("answer",), "person", "bnl"),
            ("bnl_describe", "Describe yourself.", "resolved", ("packet",), ("answer",), "unknown", "bnl"),
            ("bnl_remember", "What do you remember about yourself?", "resolved", ("packet",), ("answer",), "person", "bnl"),
            ("bnl_know", "What do you know about yourself?", "resolved", ("packet",), ("answer",), "person", "bnl"),
            ("bnl_who_case", "who ARE you", "resolved", ("packet",), ("answer",), "unknown", "bnl"),
            ("bnl_tell_case", "TELL ME ABOUT YOURSELF", "resolved", ("packet",), ("answer",), "person", "bnl"),
            ("journal_show", "What did the Journal say about the last show?", "resolved", ("packet",), ("answer",), "journal", ""),
            ("journal_queue", "Summarize the Journal entry about the queue.", "resolved", ("packet",), ("answer",), "journal", ""),
            ("journal_broadcast", "What did the Journal say about the broadcast?", "resolved", ("packet",), ("answer",), "journal", ""),
            ("journal_current", "Explain the current Journal entry.", "resolved", ("packet",), ("answer",), "journal", ""),
            ("relay_show", "What did the Relay say about the last show?", "resolved", ("packet",), ("answer",), "relay", ""),
            ("relay_queue", "Summarize the Relay about the queue.", "resolved", ("packet",), ("answer",), "relay", ""),
            ("relay_broadcast", "What did the Relay say about the broadcast?", "resolved", ("packet",), ("answer",), "relay", ""),
            ("relay_current", "Explain the current Relay.", "resolved", ("packet",), ("answer",), "relay", ""),
            ("external_place", "Where is Seattle?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("external_author", "Who wrote Hamlet?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("external_science", "What is photosynthesis?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("external_history", "When did Apollo 11 land?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("external_math", "What is twelve times twelve?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("external_language", "What does obsolete mean?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("external_geography", "Which continent is Kenya in?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("external_music", "Who composed The Four Seasons?", "resolved", ("external_public",), ("answer",), "unknown", ""),
            ("live_weather", "What is Seattle's weather today?", "resolved", ("external_current",), ("hold",), "unknown", ""),
            ("live_stock", "What is the stock price now?", "resolved", ("external_current",), ("hold",), "unknown", ""),
            ("live_score", "What is the score tonight?", "resolved", ("external_current",), ("hold",), "unknown", ""),
            ("live_traffic", "How is traffic currently?", "resolved", ("external_current",), ("hold",), "unknown", ""),
            ("live_news", "What is the latest news?", "resolved", ("external_current",), ("hold",), "unknown", ""),
            ("live_president", "Who is the current president?", "resolved", ("external_current",), ("hold",), "person", ""),
            ("live_ceo", "Who is the CEO today?", "resolved", ("external_current",), ("hold",), "person", ""),
            ("live_schedule", "What is the schedule tonight?", "resolved", ("external_current",), ("hold",), "unknown", ""),
            ("mixed_self_public", "What do you remember about me, and where is Seattle?", "resolved", ("packet", "external_public"), ("answer", "answer"), "multiple", "speaker"),
            ("mixed_self_live", "What do you remember about me, and what is Seattle's weather today?", "resolved", ("packet", "external_current"), ("answer", "hold"), "multiple", "speaker"),
            ("mixed_bnl_public", "Who are you? WHERE is Seattle?", "resolved", ("packet", "external_public"), ("answer", "answer"), "unknown", "bnl"),
            ("mixed_bnl_live", "Who are you, and what is the score tonight?", "resolved", ("packet", "external_current"), ("answer", "hold"), "unknown", "bnl"),
            ("mixed_journal_public", "What did the Journal say about the last show, and where is Seattle?", "resolved", ("packet", "external_public"), ("answer", "answer"), "multiple", ""),
            ("mixed_journal_live", "What did the Journal say about the queue, and what is the latest news?", "resolved", ("packet", "external_current"), ("answer", "hold"), "multiple", ""),
            ("clarify_person", "Tell me about Jordan.", "ambiguous", ("packet",), ("clarify",), "person", ""),
            ("clarify_mixed", "What do you remember about Jordan, and where is Seattle?", "ambiguous", ("packet", "external_public"), ("clarify", "answer"), "multiple", ""),
        )
        self.assertEqual(len(cases), 48)

        for name, text, status, authorities, acts, object_kind, subject_kind in cases:
            with self.subTest(name=name, text=text):
                frame = _frame(text)
                self.assertEqual(frame.status, status)
                self.assertEqual(
                    tuple(task.authority_scope for task in frame.tasks),
                    authorities,
                )
                self.assertEqual(
                    tuple(task.required_response_act for task in frame.tasks),
                    acts,
                )
                self.assertEqual(frame.object_kind, object_kind)
                if subject_kind == "speaker":
                    self.assertTrue(
                        any(
                            subject.user_id == 101
                            and subject.binding_method
                            == "current_speaker_context"
                            for subject in frame.subjects
                        )
                    )
                elif subject_kind == "bnl":
                    self.assertTrue(
                        any(
                            subject.entity_ref == "bnl_01"
                            and subject.binding_method
                            == "existing_typed_entity"
                            for subject in frame.subjects
                        )
                    )

    def test_14_true_multi_subject_cases_bind_each_task_scope(self):
        # name, request, ordered task subject entity keys
        cases = (
            (
                "compare_cache_mac",
                "Compare Cache Back and Mac Modem.",
                (("cache_back", "mac_modem"),),
            ),
            (
                "compare_dj_mac",
                "Compare DJ Floppy Disc and Mac Mod3m.",
                (("dj_floppydisc", "mac_modem"),),
            ),
            (
                "cache_then_mac",
                "Tell me about Cache Back, and tell me about Mac Modem.",
                (("cache_back",), ("mac_modem",)),
            ),
            (
                "cache_role_mac_role",
                "What is Cache Back's role, and what is Mac Modem's role?",
                (("cache_back",), ("mac_modem",)),
            ),
            (
                "cache_who_mac_who",
                "Who is Cache Back? Who is Mac Modem?",
                (("cache_back",), ("mac_modem",)),
            ),
            (
                "mac_then_dj",
                "What do you know about Mac Modem, and what do you know "
                "about DJ Floppy Disc?",
                (("mac_modem",), ("dj_floppydisc",)),
            ),
            (
                "six_bit_mac",
                "Compare 6 Bit and Mac Modem.",
                (("6_bit", "mac_modem"),),
            ),
            (
                "bnl_six_bit",
                "Compare BNL-01 and 6 Bit.",
                (("6_bit", "bnl_01"),),
            ),
            (
                "sheila_cliff",
                "What is the difference between Sheila and Cliff?",
                (("sheila", "cliff"),),
            ),
            (
                "cache_six_bit",
                "What is the relationship between Cache Back and 6 Bit?",
                (("6_bit", "cache_back"),),
            ),
            (
                "three_way",
                "Compare Cache Back, Call'em Bini, and Mac Modem.",
                (("cache_back", "mac_modem", "call_em_bini"),),
            ),
            (
                "one_then_pair",
                "Who is Cache Back? Compare Call'em Bini and Mac Modem.",
                (("cache_back",), ("mac_modem", "call_em_bini")),
            ),
            (
                "live_natural_mac_dj",
                "what do you know about Mac Modem and DJ Floppydisc?",
                (("dj_floppydisc", "mac_modem"),),
            ),
            (
                "live_compares_typo",
                "compares Cache Back, and Call'em Bini and Mac Modem",
                (("cache_back", "mac_modem", "call_em_bini"),),
            ),
        )
        self.assertEqual(len(cases), 14)

        for name, text, expected_task_subjects in cases:
            with self.subTest(name=name, text=text):
                frame = _frame(text)
                subject_keys = tuple(
                    subject.entity_ref for subject in frame.subjects
                )
                actual_task_subjects = tuple(
                    tuple(
                        subject_keys[index]
                        for index in task.subject_indexes
                    )
                    for task in frame.tasks
                )

                self.assertEqual(frame.status, "resolved")
                self.assertEqual(
                    actual_task_subjects,
                    expected_task_subjects,
                )
                self.assertTrue(
                    all(
                        task.subject_requirement == "required"
                        and task.required_response_act == "answer"
                        for task in frame.tasks
                    )
                )

    def test_coordinated_subject_parser_does_not_promote_context_objects(self):
        cases = (
            (
                "Who is DJ Floppydisc in BARCODE?",
                ("dj_floppydisc",),
            ),
            (
                "Tell me about Cache Back's work with BARCODE Radio.",
                ("cache_back",),
            ),
            (
                "What do you know about BARCODE Radio?",
                ("barcode_radio",),
            ),
            (
                "What do you know about BARCODE Network?",
                ("barcode_network",),
            ),
            (
                "What do you know about Mac Modem? Also, Cache Back is cool.",
                ("mac_modem",),
            ),
        )
        for text, expected in cases:
            with self.subTest(text=text):
                frame = _frame(text)
                self.assertEqual(
                    tuple(subject.entity_ref for subject in frame.subjects),
                    expected,
                )

    def test_exact_bnl_reply_supplies_identity_only_pronoun_binding(self):
        context = bnl01_bot.ConversationContextResult(
            rendered_context=(
                "BNL-01 (exact Discord reply source): Cache Back is a "
                "founding BARCODE member."
            ),
            selected_row_ids=(77,),
            same_room_paired_turn_count=0,
            unpaired_row_count=1,
            cross_channel_paired_turn_count=0,
            current_message_duplicates_removed=0,
            visibility_policy_exclusions=0,
            selection_reasons=("discord_reply_source",),
            final_char_count=96,
            referent_status="resolved",
            referent_candidate_count=1,
            referent_selected_row_ids=(77,),
            referent_reason="discord_reply_source",
        )
        identity_subjects, identity_status = (
            bnl01_bot._exact_reply_canon_subject_references(
                context,
                "How is he connected to Call'em Bini?",
            )
        )
        self.assertEqual(identity_status, "resolved")
        self.assertEqual(identity_subjects, (("cache_back", "Cache Back"),))
        addressing = bnl01_bot.DiscordTurnAddressing(
            speaker="Test Member",
            explicit_tag_recipients=("@BNL-01",),
            reply_target="BNL-01",
            explicitly_mentions_bnl=False,
            reply_targets_bnl=True,
            directly_targets_bnl=True,
            targets_other_human=False,
            plain_text_names_bnl=False,
            speaker_user_id=101,
            source_message_id=302,
            reply_message_id=701,
            reply_conversation_row_id=77,
        )
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="direct_request",
            channel_policy="sealed_test",
            addressings=(addressing,),
            context_result=context,
            moment_situation=None,
            guild_id=1,
            channel_id=10,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            current_text="How is he connected to Call'em Bini?",
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            influence_mode="live",
            packet_revision="turn_exact_reply_entity",
        )

        frame = decision.situation_frame
        self.assertEqual(frame.status, "resolved")
        self.assertEqual(
            tuple(subject.entity_ref for subject in frame.subjects),
            ("call_em_bini", "cache_back"),
        )
        self.assertEqual(frame.tasks[0].authority_scope, "packet")
        self.assertEqual(frame.tasks[0].subject_indexes, (0, 1))

    def test_exact_bnl_reply_pronoun_identity_ambiguity_fails_closed(self):
        question = "How is he connected to Call'em Bini?"
        cases = (
            (
                "ambiguous",
                "Cache Back and Mac Modem are founding BARCODE members.",
            ),
            (
                "unresolved",
                "The archive record does not identify that person.",
            ),
        )
        for identity_status, reply_text in cases:
            with self.subTest(identity_status=identity_status):
                context = bnl01_bot.ConversationContextResult(
                    rendered_context=(
                        "BNL-01 (exact Discord reply source): " + reply_text
                    ),
                    selected_row_ids=(77,),
                    same_room_paired_turn_count=0,
                    unpaired_row_count=1,
                    cross_channel_paired_turn_count=0,
                    current_message_duplicates_removed=0,
                    visibility_policy_exclusions=0,
                    selection_reasons=("discord_reply_source",),
                    final_char_count=len(reply_text),
                    referent_status="resolved",
                    referent_candidate_count=1,
                    referent_selected_row_ids=(77,),
                    referent_reason="discord_reply_source",
                )
                identity_subjects, actual_identity_status = (
                    bnl01_bot._exact_reply_canon_subject_references(
                        context,
                        question,
                    )
                )
                self.assertEqual(identity_subjects, ())
                self.assertEqual(actual_identity_status, identity_status)

                addressing = bnl01_bot.DiscordTurnAddressing(
                    speaker="Test Member",
                    explicit_tag_recipients=("@BNL-01",),
                    reply_target="BNL-01",
                    explicitly_mentions_bnl=False,
                    reply_targets_bnl=True,
                    directly_targets_bnl=True,
                    targets_other_human=False,
                    plain_text_names_bnl=False,
                    speaker_user_id=101,
                    source_message_id=302,
                    reply_message_id=701,
                    reply_conversation_row_id=77,
                )
                decision = (
                    bnl01_bot.build_live_conversation_orchestration_decision(
                        engagement_decision="answer",
                        engagement_reason="direct_request",
                        channel_policy="sealed_test",
                        addressings=(addressing,),
                        context_result=context,
                        moment_situation=None,
                        guild_id=1,
                        channel_id=10,
                        route_mode="normal_chat",
                        conversation_surface="mention_or_reply",
                        current_text=question,
                        current_speaker_user_ids=(101,),
                        current_speaker_labels=("Test Member",),
                        influence_mode="live",
                        packet_revision=(
                            "turn_exact_reply_entity_" + identity_status
                        ),
                    )
                )

                frame = decision.situation_frame
                self.assertEqual(frame.status, "ambiguous")
                self.assertIn(
                    "referent_%s" % identity_status,
                    frame.ambiguity_reasons,
                )
                self.assertEqual(
                    tuple(subject.entity_ref for subject in frame.subjects),
                    ("call_em_bini",),
                )
                self.assertNotIn(
                    "cache_back",
                    tuple(subject.entity_ref for subject in frame.subjects),
                )
                self.assertNotIn(
                    "mac_modem",
                    tuple(subject.entity_ref for subject in frame.subjects),
                )

    def test_exact_bnl_reply_without_canon_alias_keeps_ordinary_continuity(self):
        reply_text = "I compared the two options and preferred the first."
        question = "Why did you prefer them?"
        context = bnl01_bot.ConversationContextResult(
            rendered_context=(
                "BNL-01 (exact Discord reply source): " + reply_text
            ),
            selected_row_ids=(77,),
            same_room_paired_turn_count=0,
            unpaired_row_count=1,
            cross_channel_paired_turn_count=0,
            current_message_duplicates_removed=0,
            visibility_policy_exclusions=0,
            selection_reasons=("discord_reply_source",),
            final_char_count=len(reply_text),
            referent_status="resolved",
            referent_candidate_count=1,
            referent_selected_row_ids=(77,),
            referent_reason="discord_reply_source",
        )

        subjects, identity_status = (
            bnl01_bot._exact_reply_canon_subject_references(
                context,
                question,
            )
        )
        self.assertEqual(subjects, ())
        self.assertEqual(identity_status, "not_requested")

        addressing = bnl01_bot.DiscordTurnAddressing(
            speaker="Test Member",
            explicit_tag_recipients=("@BNL-01",),
            reply_target="BNL-01",
            explicitly_mentions_bnl=False,
            reply_targets_bnl=True,
            directly_targets_bnl=True,
            targets_other_human=False,
            plain_text_names_bnl=False,
            speaker_user_id=101,
            source_message_id=302,
            reply_message_id=701,
            reply_conversation_row_id=77,
        )
        decision = bnl01_bot.build_live_conversation_orchestration_decision(
            engagement_decision="answer",
            engagement_reason="direct_request",
            channel_policy="sealed_test",
            addressings=(addressing,),
            context_result=context,
            moment_situation=None,
            guild_id=1,
            channel_id=10,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            current_text=question,
            current_speaker_user_ids=(101,),
            current_speaker_labels=("Test Member",),
            influence_mode="live",
            packet_revision="turn_exact_reply_ordinary_continuity",
        )

        self.assertEqual(decision.situation_frame.status, "resolved")
        self.assertNotIn(
            "referent_unresolved",
            decision.situation_frame.ambiguity_reasons,
        )

        unrelated = bnl01_bot.build_situation_frame_v1(
            route_allowed=True,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_policy="sealed_test",
            current_text="By the way, who wrote Hamlet?",
            current_speaker_user_ids=(101,),
            reply_message_ids=(701,),
            exact_source_row_ids=(77,),
            referent_status="resolved",
            response_act="answer",
        )
        self.assertEqual(unrelated.status, "resolved")
        self.assertEqual(
            tuple(task.authority_scope for task in unrelated.tasks),
            ("external_public",),
        )
        self.assertEqual(
            tuple(task.required_response_act for task in unrelated.tasks),
            ("answer",),
        )


if __name__ == "__main__":
    unittest.main()
