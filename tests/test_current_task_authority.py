"""Current task ownership survives incidental source presence."""

from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timezone

from bnl_conversation_context_v2 import (
    ConversationContextRequest,
    assemble_conversation_context_v2,
)

from bnl_unified_response_assessment import (
    build_situation_frame_v1,
    build_unified_response_assessment,
    situation_request_texts,
    source_dependent_task_texts,
)


class CurrentTaskAssessmentTests(unittest.TestCase):
    def assessment(self, text, *, route="normal_chat", continuity=False,
                   recap=False, quote=False, show=True):
        frame = build_situation_frame_v1(
            route_allowed=True, route_mode=route,
            conversation_surface="sealed_test", channel_policy="sealed_test",
            current_text=text, current_speaker_user_ids=(101,),
            reply_message_ids=(1001,), exact_source_row_ids=(11,),
            referent_status="resolved", response_act="answer",
        )
        return build_unified_response_assessment(
            guild_id=1, route_mode=route, channel_policy="sealed_test",
            conversation_surface="sealed_test", current_speaker_user_ids=(101,),
            current_text=text, continuity_required=continuity,
            immediate_recap=recap, exact_quote_requested=quote,
            show_state_present=show, situation_frame=frame,
        )

    def test_incidental_show_source_cannot_choose_the_task(self):
        for text in (
            "What's up?",
            "What do you want to be when you grow up?",
            "I meant red panda.",
            "New topic: what color would you paint a paper satellite?",
        ):
            with self.subTest(text=text):
                without_show = self.assessment(text, show=False)
                with_show = self.assessment(text)
                self.assertEqual(with_show.response_act, without_show.response_act)
                self.assertEqual(with_show.response_act, "answer_current_turn")
                self.assertEqual(with_show.current_objective, without_show.current_objective)
                self.assertEqual(with_show.situation_frame, without_show.situation_frame)
                # Available context remains an input. This change does not
                # remove an existing source or create a replacement selector.
                self.assertIn("show_state", with_show.selected_lanes)

    def test_actual_show_route_retains_current_show_task(self):
        for route in ("show_status", "show_status_answer"):
            with self.subTest(route=route):
                self.assertEqual(self.assessment(
                    "What is the current show schedule?", route=route,
                    show=False,
                ).response_act, "answer_show_status")

    def test_existing_followup_recap_and_quote_owners_still_determine_the_act(self):
        for text in ("Why?", "When?"):
            with self.subTest(text=text):
                self.assertEqual(self.assessment(
                    text, continuity=True,
                ).response_act, "continue_active_thread")
        self.assertEqual(self.assessment(
            "Recap what we just discussed.", recap=True,
        ).response_act, "recap_current_exchange")
        self.assertEqual(self.assessment(
            "What exactly did the reply say?", quote=True,
        ).response_act, "verify_exact_wording")


class SourceDependentTaskBindingTests(unittest.TestCase):
    def context_and_frame(self, text, *, source_policy="public_home"):
        rows = [
            dict(id=11, guild_id=1, role="user", user_id=202,
                 user_name="Test Member", message_id=1001, channel_id=10,
                 channel_name="home", channel_policy=source_policy,
                 route_mode="normal_chat", timestamp="2026-10-03T07:00:00+00:00",
                 content="Idea A: a copper panel carries orchard music."),
            dict(id=12, guild_id=1, role="user", user_id=303,
                 user_name="Test Member B", message_id=1002, channel_id=10,
                 channel_name="home", channel_policy="public_home",
                 route_mode="normal_chat", timestamp="2026-10-04T07:00:00+00:00",
                 content="Idea B: a paper satellite follows a violet river."),
        ]
        context = assemble_conversation_context_v2(rows, ConversationContextRequest(
            guild_id=1, current_user_id=101, channel_id=10,
            channel_name="home", channel_policy="public_home",
            route_mode="normal_chat", conversation_surface="public_home",
            current_texts=(text,), current_participants=frozenset({101}),
            referenced_message_ids=frozenset({1001}), is_direct_target=True,
            now=datetime(2026, 10, 4, 7, 3, tzinfo=timezone.utc),
            route_allowed_sources=frozenset({"conversation_continuity"}),
        ))
        frame = build_situation_frame_v1(
            route_allowed=True, route_mode="normal_chat",
            conversation_surface="public_home", channel_policy="public_home",
            current_text=text, current_speaker_user_ids=(101,),
            reply_message_ids=(1001,),
            exact_source_row_ids=context.referent_selected_row_ids,
            referent_status=context.referent_status, response_act="answer",
        )
        return context, frame

    def test_same_sentence_scope_qualifier_reaches_task_authority(self):
        for text in (
            "And for that same track, what is the full artist credit?",
            "For that same track: what is the full artist credit?",
            "And for that same track: what is the full artist credit?",
        ):
            with self.subTest(text=text):
                _context, frame = self.context_and_frame(text)
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
                self.assertTrue(source_dependent_task_texts(frame, current_text=text))
        for independent in (
            "That same track was interesting. Where is Seattle?",
            "For context, I read your latest Journal, where is Seattle?",
            "For context, that same track was interesting, where is Seattle?",
        ):
            with self.subTest(independent=independent):
                _context, frame = self.context_and_frame(independent)
                self.assertEqual(frame.tasks[0].authority_scope, "external_public")
                self.assertEqual(source_dependent_task_texts(frame, current_text=independent), ())

    def test_coordinated_show_date_field_does_not_create_a_new_imperative(self):
        for text in (
            "Give me the full artist credits and show dates.",
            "Give me the full artist credits, show dates.",
        ):
            with self.subTest(text=text):
                _context, frame = self.context_and_frame(text)
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(frame.tasks[0].authority_scope, "packet")
        for text in (
            "Give me the full artist credits and show me how a clock works.",
            "Give me the full artist credits. Show dates.",
        ):
            with self.subTest(text=text):
                _context, frame = self.context_and_frame(text)
                self.assertEqual(len(frame.tasks), 2)

    def test_complete_current_request_is_independent_of_its_old_exact_source(self):
        for text in (
            "What's up?",
            "What do you want to be when you grow up?",
            "New topic: what color would you paint a paper satellite?",
            "I meant red panda. What's up?",
            "What happened during the October 2 show?",
        ):
            with self.subTest(text=text):
                context, frame = self.context_and_frame(text)
                self.assertEqual(context.referent_selected_row_ids, (11,))
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), ())
                self.assertIn("copper panel", context.rendered_context)
                self.assertNotIn("violet river", context.rendered_context)

    def test_real_transforms_and_expanded_comparisons_keep_the_exact_source_scope(self):
        for text, dependent, expanded in (
            ("Improve this idea in one sentence.",
             ("Improve this idea in one sentence",), False),
            ("Summarize that reply.", ("Summarize that reply",), False),
            ("Compare this idea with the newer idea.",
             ("Compare this idea with the newer idea",), True),
            ("Why?", ("Why",), False),
            ("I meant red panda.", ("I meant red panda",), False),
        ):
            with self.subTest(text=text):
                context, frame = self.context_and_frame(text)
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), dependent)
                self.assertEqual(context.referent_selected_row_ids, (11,))
                self.assertEqual(context.referent_scope_expanded, expanded)
                self.assertEqual("violet river" in context.rendered_context, expanded)

    def test_mixed_tasks_check_the_dependent_clause_without_retargeting_the_new_question(self):
        text = "Summarize that reply, and what do you want to be when you grow up?"
        context, frame = self.context_and_frame(text)
        self.assertEqual(len(frame.tasks), 2)
        self.assertEqual(source_dependent_task_texts(frame, current_text=text),
                         ("Summarize that reply",))
        self.assertEqual(situation_request_texts(frame, current_text=text),
                         ("Summarize that reply", "what do you want to be when you grow up"))
        self.assertEqual(context.referent_selected_row_ids, (11,))
        self.assertNotIn("violet river", context.rendered_context)

    def test_ordered_actions_exclude_incidental_setup_and_preserve_repetition(self):
        text = "I meant red panda. What's up? Summarize that reply. Summarize that reply."
        _context, frame = self.context_and_frame(text)
        self.assertEqual(situation_request_texts(frame, current_text=text),
                         ("What's up", "Summarize that reply", "Summarize that reply"))
        self.assertEqual(source_dependent_task_texts(frame, current_text=text),
                         ("Summarize that reply", "Summarize that reply"))

    def test_mixed_transform_boundaries_keep_both_request_orders_and_action_scopes(self):
        independent = "How does a vending machine dispense snacks"
        for dependent in ("improve this idea in one sentence", "rewrite this idea in one sentence"):
            for separator in ("? Also ", "? And ", ", and ", "; ", "\n"):
                for source_first in (False, True):
                    first, second = ((dependent, independent) if source_first
                                     else (independent, dependent))
                    text = first + separator + second + "."
                    with self.subTest(text=text):
                        _context, frame = self.context_and_frame(text)
                        self.assertEqual(len(frame.tasks), 2)
                        self.assertEqual(situation_request_texts(frame, current_text=text),
                                         (first, second))
                        self.assertEqual(source_dependent_task_texts(frame, current_text=text),
                                         (dependent,))

    def test_setup_words_stay_in_prompt_but_do_not_become_an_independent_request(self):
        for separator in ("; ", "\n", ". Also "):
            text = ("Someone mentioned a vending machine that trades memories for coins"
                    + separator + "improve this idea.")
            with self.subTest(text=text):
                _context, frame = self.context_and_frame(text)
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(situation_request_texts(frame, current_text=text),
                                 ("improve this idea",))
                self.assertEqual(source_dependent_task_texts(frame, current_text=text),
                                 ("improve this idea",))

    def test_quoted_or_fenced_current_payload_is_not_split_into_fake_requests(self):
        payload = "The tower wakes at midnight. Also improve the vending machine.\nHow does it glow?"
        for wrapped in ('"' + payload + '"', "'" + payload + "'",
                        "“" + payload + "”", "‘" + payload + "’",
                        "```\n" + payload + "\n```"):
            text = "Rewrite this idea: " + wrapped
            with self.subTest(wrapped=wrapped):
                _context, frame = self.context_and_frame(text)
                self.assertEqual(len(frame.tasks), 1)
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), ())
                self.assertEqual(situation_request_texts(frame, current_text=text),
                                 (" ".join(text.split()),))
                mixed = text + " Also how does a vending machine dispense snacks?"
                _context, mixed_frame = self.context_and_frame(mixed)
                self.assertEqual(len(mixed_frame.tasks), 2)
                self.assertEqual(situation_request_texts(mixed_frame, current_text=mixed),
                                 (" ".join(text.split()),
                                  "how does a vending machine dispense snacks"))
                self.assertEqual(source_dependent_task_texts(mixed_frame, current_text=mixed), ())

    def test_existing_question_leads_split_connected_elliptical_requests_without_punctuation(self):
        for question in ("why", "when"):
            for connector in (" and ", " also "):
                text = "Improve this idea" + connector + question
                with self.subTest(text=text):
                    _context, frame = self.context_and_frame(text)
                    self.assertEqual(len(frame.tasks), 2)
                    self.assertEqual(situation_request_texts(frame, current_text=text),
                                     ("Improve this idea", question))

    def test_explicit_new_source_and_self_contained_input_do_not_inherit_old_reply_authority(self):
        for text in (
            "Summarize the latest Journal, and why?",
            "Summarize the latest Relay. Explain it.",
            "Summarize this idea: plant a copper orchard.",
        ):
            with self.subTest(text=text):
                _context, frame = self.context_and_frame(text)
                self.assertEqual(source_dependent_task_texts(frame, current_text=text), ())

    def test_source_dependence_never_grants_access_to_a_sealed_contribution(self):
        text = "Summarize that reply."
        context, frame = self.context_and_frame(text, source_policy="sealed_test")
        self.assertNotEqual(context.referent_status, "resolved")
        self.assertEqual(context.referent_selected_row_ids, ())
        self.assertNotIn("copper panel", context.rendered_context)
        # Required evidence stays required, even when the source owner cannot
        # provide it. The helper must not turn this into an independent task.
        self.assertEqual(source_dependent_task_texts(frame, current_text=text),
                         ("Summarize that reply",))

    def test_unbound_changed_blocked_and_incomplete_frames_cannot_bypass_the_guard(self):
        text = "Summarize that reply."
        _context, frame = self.context_and_frame(text)
        for candidate, request in (
            (None, text),
            (frame, "What's up?"),
            (replace(frame, route_allowed=False, status="blocked"), text),
            (replace(frame, tasks=()), text),
            (replace(frame, tasks=(replace(frame.tasks[0], text_digest="changed"),)), text),
        ):
            with self.subTest(candidate=candidate, request=request):
                self.assertIsNone(source_dependent_task_texts(candidate, current_text=request))
                self.assertIsNone(situation_request_texts(candidate, current_text=request))


if __name__ == "__main__":
    unittest.main()
