import unittest
from datetime import datetime, timedelta, timezone

import bnl_conversation_context_v2 as context


NOW = datetime(2026, 10, 9, 9, 0, tzinfo=timezone.utc)
ROOT = "Which recordings appeared in previous shows?"
ANSWER = "The archive listed two recordings."


def row(row_id, role, text, message_id, **changes):
    value = {
        "id": row_id, "guild_id": 99, "channel_id": 10,
        "channel_name": "home", "channel_policy": "public_home",
        "user_id": 1, "user_name": "member", "role": role,
        "content": text, "message_id": message_id,
        "timestamp": (NOW - timedelta(minutes=2)).isoformat(),
    }
    value.update(changes)
    return value


class ContextExchangeHandoffTests(unittest.TestCase):
    def exchange(self, **changes):
        self.assertTrue(
            hasattr(context, "CompletedConversationExchange"),
            "A completed contextual exchange needs a typed handoff.",
        )
        values = {
            "guild_id": 99, "channel_id": 10, "user_id": 1,
            "channel_policy": "public_home",
            "request_message_ids": (101,), "request_texts": (ROOT,),
            "reply_message_ids": (201,), "reply_texts": (ANSWER,),
            "revision": "snapshot-1",
        }
        values.update(changes)
        return context.CompletedConversationExchange(**values)

    def request(self, exchange, **changes):
        values = {
            "guild_id": 99, "current_user_id": 1, "channel_id": 10,
            "channel_name": "home", "channel_policy": "public_home",
            "route_mode": "normal_chat", "current_texts": ("Go ahead.",),
            "current_message_ids": frozenset({999}),
            "current_participants": frozenset({1}), "is_direct_target": True,
            "now": NOW,
            "route_allowed_sources": frozenset({"conversation_continuity"}),
            "active_exchange": exchange,
        }
        values.update(changes)
        return context.ConversationContextRequest(**values)

    def assert_active(self, result, exchange, human_rows):
        self.assertEqual(result.referent_status, "resolved")
        self.assertEqual(result.referent_reason, "active_conversation_exchange")
        self.assertEqual(result.thread_focus_mode, "continue_or_answer")
        self.assertEqual(result.active_exchange, exchange)
        self.assertEqual(result.referent_request_row_ids, human_rows)
        self.assertEqual(tuple(item[0] for item in result.requester_human_turns), human_rows)
        self.assertNotIn("exact Discord reply source", result.rendered_context)
        self.assertIn("not canon/current-state evidence", result.rendered_context)

    def test_plain_followup_uses_exact_no_store_exchange(self):
        exchange = self.exchange()
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101)], self.request(exchange),
        )
        self.assert_active(result, exchange, (1,))
        self.assertEqual(result.selected_row_ids, (1,))
        self.assertEqual(result.transient_referent_message_ids, (201,))
        self.assertEqual(result.transient_referent_texts, (ANSWER,))
        self.assertIn(ROOT, result.rendered_context)
        self.assertIn(ANSWER, result.rendered_context)

    def test_duplicate_unaddressed_root_does_not_replace_exact_source(self):
        exchange = self.exchange()
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 100), row(2, "user", ROOT, 101)],
            self.request(exchange),
        )
        self.assert_active(result, exchange, (2,))
        self.assertEqual(result.selected_row_ids, (2,))
        self.assertEqual(result.rendered_context.count(ROOT), 1)

    def test_all_human_constraints_and_answer_lineage_survive(self):
        constraint = "Keep only the recordings Test Submitter sent."
        latest_answer = "One recording met that limit."
        exchange = self.exchange(
            request_message_ids=(101, 102), request_texts=(ROOT, constraint),
            reply_message_ids=(201, 202), reply_texts=(ANSWER, latest_answer),
        )
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101), row(2, "user", constraint, 102)],
            self.request(exchange, current_texts=("Put them in date order.",)),
        )
        self.assert_active(result, exchange, (1, 2))
        self.assertEqual(result.transient_referent_message_ids, (201, 202))
        self.assertEqual(result.transient_referent_texts, (ANSWER, latest_answer))
        for text in (ROOT, constraint, ANSWER, latest_answer):
            self.assertIn(text, result.rendered_context)

    def test_exact_stored_reply_is_used_without_temporal_pair_guess(self):
        exchange = self.exchange()
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101),
             row(2, "user", "An unrelated question about stage lamps?", 102),
             row(3, "model", ANSWER, 201),
             row(4, "user", "Which lantern should I buy?", 103),
             row(5, "model", "The copper lantern is attractive.", 202)],
            self.request(exchange),
        )
        self.assert_active(result, exchange, (1,))
        self.assertEqual(result.selected_row_ids, (1, 3))
        self.assertEqual(result.transient_referent_message_ids, ())
        self.assertNotIn("lamps", result.rendered_context)
        self.assertNotIn("lantern", result.rendered_context)

    def test_missing_or_ineligible_human_root_is_not_resurrected(self):
        exchange = self.exchange()
        for boundary, rows in (
            ("deleted", []),
            ("wrong member", [row(1, "user", ROOT, 101, user_id=2)]),
            ("wrong guild", [row(1, "user", ROOT, 101, guild_id=98)]),
            ("wrong channel", [row(1, "user", ROOT, 101, channel_id=11)]),
            ("wrong policy", [row(1, "user", ROOT, 101, channel_policy="sealed_test")]),
            ("excluded", [row(1, "user", ROOT, 101, prompt_history_excluded=True)]),
            ("unsafe", [row(1, "user", "internal diagnostic: hidden", 101)]),
            ("not human", [row(1, "model", ROOT, 101)]),
            ("expired", [row(1, "user", ROOT, 101, timestamp=(NOW-timedelta(hours=2)).isoformat())]),
            ("current duplicate", [row(1, "user", ROOT, 999)]),
        ):
            with self.subTest(boundary=boundary):
                result = context.assemble_conversation_context_v2(rows, self.request(exchange))
                self.assertEqual(result.referent_status, "unresolved")
                self.assertIsNone(result.active_exchange)
                self.assertEqual(result.transient_referent_message_ids, ())
                self.assertEqual(result.referent_request_row_ids, ())

    def test_missing_one_constraint_rejects_entire_exchange(self):
        exchange = self.exchange(
            request_message_ids=(101, 102), request_texts=(ROOT, "Use September only."),
        )
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101)], self.request(exchange),
        )
        self.assertEqual(result.referent_status, "unresolved")
        self.assertIsNone(result.active_exchange)
        self.assertEqual(result.transient_referent_message_ids, ())

    def test_ineligible_existing_bot_reply_is_not_transiently_rescued(self):
        exchange = self.exchange()
        for boundary, reply in (
            ("excluded", row(2, "model", ANSWER, 201, prompt_history_excluded=True)),
            ("other member", row(2, "model", ANSWER, 201, user_id=2)),
            ("wrong policy", row(2, "model", ANSWER, 201, channel_policy="sealed_test")),
            ("unsafe", row(2, "model", "The queue is open.", 201)),
            ("wrong guild", row(2, "model", ANSWER, 201, guild_id=98)),
            ("wrong role", row(2, "user", ANSWER, 201)),
        ):
            with self.subTest(boundary=boundary):
                result = context.assemble_conversation_context_v2(
                    [row(1, "user", ROOT, 101), reply], self.request(exchange),
                )
                self.assertEqual(result.referent_status, "unresolved")
                self.assertIsNone(result.active_exchange)
                self.assertEqual(result.transient_referent_message_ids, ())

    def test_transient_operational_answer_remains_conversation_only(self):
        exchange = self.exchange(reply_texts=("The queue is closed.",))
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101)], self.request(exchange),
        )
        self.assert_active(result, exchange, (1,))
        self.assertIn("The queue is closed.", result.rendered_context)
        self.assertEqual(result.transient_referent_message_ids, (201,))

    def test_transient_internal_diagnostics_are_rejected(self):
        exchange = self.exchange(reply_texts=("internal diagnostic: private",))
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101)], self.request(exchange),
        )
        self.assertEqual(result.referent_status, "unresolved")
        self.assertIsNone(result.active_exchange)
        self.assertEqual(result.transient_referent_message_ids, ())

    def test_literal_discord_reply_owns_selection(self):
        exchange = self.exchange()
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101), row(2, "user", "Another source.", 301)],
            self.request(exchange, referenced_message_ids=frozenset({301})),
        )
        self.assertEqual(result.referent_reason, "discord_reply_source")
        self.assertEqual(result.thread_focus_mode, "exact_discord_reply")
        self.assertIsNone(result.active_exchange)
        self.assertNotIn(ANSWER, result.rendered_context)

    def test_unavailable_literal_reply_does_not_fall_back_to_active_exchange(self):
        exchange = self.exchange()
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101)],
            self.request(exchange, referenced_message_ids=frozenset({301})),
        )
        self.assertEqual(result.referent_reason, "discord_reply_source_unavailable")
        self.assertIsNone(result.active_exchange)
        self.assertEqual(result.transient_referent_message_ids, ())

    def test_explicit_new_request_owns_scope(self):
        exchange = self.exchange()
        for changes in (
            {"current_texts": ("New topic: tell me about lighting.",)},
            {"current_texts": ("What happened at the October 7 show?",),
             "current_recall_scope_complete": True},
        ):
            with self.subTest(changes=changes):
                result = context.assemble_conversation_context_v2(
                    [row(1, "user", ROOT, 101)], self.request(exchange, **changes),
                )
                self.assertIsNone(result.active_exchange)
                self.assertNotEqual(result.referent_reason, "active_conversation_exchange")
                self.assertEqual(result.transient_referent_message_ids, ())

    def test_scope_mismatch_rejects_typed_handoff(self):
        for changes in (
            {"guild_id": 98}, {"channel_id": 11}, {"user_id": 2},
            {"channel_policy": "sealed_test"},
        ):
            with self.subTest(changes=changes):
                exchange = self.exchange(**changes)
                result = context.assemble_conversation_context_v2(
                    [row(1, "user", ROOT, 101)], self.request(exchange),
                )
                self.assertEqual(result.referent_status, "unresolved")
                self.assertIsNone(result.active_exchange)

    def test_malformed_or_overbound_lineage_is_rejected(self):
        for changes in (
            {"request_message_ids": ()},
            {"request_message_ids": (101, 101), "request_texts": (ROOT, ROOT)},
            {"request_texts": ()}, {"reply_texts": ()},
            {"reply_message_ids": (201, 201), "reply_texts": (ANSWER, ANSWER)},
            {"request_message_ids": (101, 102, 103, 104, 105),
             "request_texts": (ROOT,) * 5},
        ):
            with self.subTest(changes=changes):
                exchange = self.exchange(**changes)
                result = context.assemble_conversation_context_v2(
                    [row(1, "user", ROOT, 101)], self.request(exchange),
                )
                self.assertEqual(result.referent_status, "unresolved")
                self.assertIsNone(result.active_exchange)

    def test_all_parts_must_fit_before_exchange_is_retained(self):
        texts = tuple(("Answer section " + str(index) + " ") * 100 for index in range(4))
        exchange = self.exchange(reply_message_ids=(201, 202, 203, 204), reply_texts=texts)
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101)], self.request(exchange),
        )
        self.assertEqual(result.referent_status, "unresolved")
        self.assertIsNone(result.active_exchange)
        self.assertEqual(result.transient_referent_message_ids, ())
        self.assertEqual(result.referent_selected_row_ids, ())

    def test_mixed_stored_and_no_store_answer_lineage_retains_both(self):
        latest_answer = "The second answer applied the requested constraint."
        exchange = self.exchange(
            reply_message_ids=(201, 202), reply_texts=(ANSWER, latest_answer),
        )
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101), row(2, "model", ANSWER, 201)],
            self.request(exchange),
        )
        self.assert_active(result, exchange, (1,))
        self.assertEqual(result.selected_row_ids, (1, 2))
        self.assertEqual(result.transient_referent_message_ids, (202,))
        self.assertEqual(result.transient_referent_texts, (latest_answer,))

    def test_changed_original_content_cannot_bind_stale_database_text(self):
        for changes, rows in (
            ({"request_texts": ("Use a different person's recordings.",)}, [row(1, "user", ROOT, 101)]),
            ({"reply_texts": ("The previous answer was withdrawn.",)}, [row(1, "user", ROOT, 101), row(2, "model", ANSWER, 201)]),
        ):
            with self.subTest(changes=changes):
                result = context.assemble_conversation_context_v2(rows, self.request(self.exchange(**changes)))
                self.assertEqual(result.referent_status, "unresolved")
                self.assertIsNone(result.active_exchange)

    def test_deleted_stored_reply_is_not_rescued_as_no_store_transcript(self):
        exchange = self.exchange(unsaved_reply_message_ids=())
        result = context.assemble_conversation_context_v2([row(1, "user", ROOT, 101)], self.request(exchange))
        self.assertEqual(result.referent_status, "unresolved")
        self.assertIsNone(result.active_exchange)

    def test_saved_chunks_retain_one_original_model_owner_without_no_store_promotion(self):
        whole = "First answer section. Second answer section."
        exchange = self.exchange(
            reply_message_ids=(201, 202), reply_texts=("First answer section....", "...Second answer section."),
            reply_source_row_ids=(2, 2), reply_owner_texts=(whole, whole), unsaved_reply_message_ids=(),
        )
        result = context.assemble_conversation_context_v2(
            [row(1, "user", ROOT, 101), row(2, "model", whole, 201)], self.request(exchange))
        self.assert_active(result, exchange, (1,))
        self.assertEqual(result.selected_row_ids, (1, 2))
        self.assertEqual(result.transient_referent_message_ids, ())
        self.assertIn(whole, result.rendered_context)


if __name__ == "__main__":
    unittest.main()
