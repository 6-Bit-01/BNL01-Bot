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


if __name__ == "__main__":
    unittest.main()
