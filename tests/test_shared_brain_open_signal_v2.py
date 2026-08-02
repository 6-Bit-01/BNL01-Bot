import sqlite3
from pathlib import Path
import tempfile
import unittest

import bnl_memory_ledger as ledger
from bnl_memory_preview import (
    MemoryPreviewRequest,
    evaluate_memory_preview,
    prepare_memory_preview,
)


class SharedBrainOpenSignalV2Tests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "open-signal-v2.db")
        self.next_message_id = 8000
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE conversations (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER NOT NULL,
                  user_name TEXT NOT NULL,
                  guild_id INTEGER NOT NULL,
                  channel_name TEXT NOT NULL,
                  channel_policy TEXT NOT NULL,
                  channel_id INTEGER NOT NULL,
                  message_id INTEGER,
                  route_mode TEXT NOT NULL,
                  role TEXT NOT NULL,
                  content TEXT NOT NULL,
                  timestamp TEXT NOT NULL
                )
                """
            )
            ledger.ensure_memory_ledger_schema(conn)
            conn.commit()

    def tearDown(self):
        self.tempdir.cleanup()

    def add_public_message(
        self,
        content,
        observed_at,
        *,
        user_id=41,
        user_name="Test Member",
    ):
        self.next_message_id += 1
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                INSERT INTO conversations(
                  user_id,user_name,guild_id,channel_name,channel_policy,
                  channel_id,message_id,route_mode,role,content,timestamp
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(user_id),
                    str(user_name),
                    1,
                    "barcode-bot",
                    "public_home",
                    10,
                    self.next_message_id,
                    "normal_chat",
                    "user",
                    str(content),
                    str(observed_at),
                ),
            )
            row_id = int(cursor.lastrowid)
            result = ledger.shadow_conversation_row(
                conn,
                row_id=row_id,
                user_id=int(user_id),
                user_name=str(user_name),
                guild_id=1,
                role="user",
                content=str(content),
                channel_name="barcode-bot",
                channel_policy="public_home",
                channel_id=10,
                message_id=self.next_message_id,
                route_mode="normal_chat",
                observed_at=str(observed_at),
                environ={
                    ledger.CONVERSATION_MOTIF_FORMATION_ENV: "false",
                },
            )
            self.assertIn(result.outcome, {"inserted", "deduplicated"})
            conn.commit()
        return row_id

    def prepare(
        self,
        *,
        user_id=41,
        user_name="Test Member",
        wording="BNL, what am I all about?",
    ):
        baseline = (
            "Current user request: %s\n"
            "BNL persona and BARCODE lore remain active.\n"
            "Current channel: #barcode-bot\n"
            "Current channel policy: public_home\n"
            "Durable memory context:\n"
            "No stored member facts are supplied to this comparison.\n"
            "Personal-recall route contract remains active."
        ) % wording
        prepared = prepare_memory_preview(
            MemoryPreviewRequest(
                source_db_path=self.db_path,
                guild_id=1,
                subject_user_id=int(user_id),
                subject_display_name=str(user_name),
                simulated_channel_id=10,
                wording=str(wording),
                baseline_prompt=baseline,
                factual_placeholder=(
                    "No stored member facts are supplied to this comparison."
                ),
                now="2026-07-26T12:00:00+00:00",
            )
        )
        self.assertTrue(prepared.ready, prepared.packet_owned_prompt.reason)
        return prepared

    def add_model_reply(self, content, observed_at):
        self.next_message_id += 1
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO conversations(
                  user_id,user_name,guild_id,channel_name,channel_policy,
                  channel_id,message_id,route_mode,role,content,timestamp
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    41,
                    "Test Member",
                    1,
                    "barcode-bot",
                    "public_home",
                    10,
                    self.next_message_id,
                    "normal_chat",
                    "model",
                    str(content),
                    str(observed_at),
                ),
            )
            conn.commit()

    def evaluate(self, prepared, candidate):
        return evaluate_memory_preview(
            prepared,
            baseline_response="I only have one narrow grounded signal so far.",
            candidate_response=candidate,
        )

    def test_actor_action_boundary_asked_is_not_tested(self):
        self.add_public_message(
            "I asked Mac Modem to test the signal meter.",
            "2026-07-25T10:00:00+00:00",
        )
        prepared = self.prepare(
            wording="BNL, what am I all about around signal meters?",
        )
        try:
            supported = self.evaluate(
                prepared,
                "You asked Mac Modem to test the signal meter.",
            )
            self.assertTrue(
                supported.candidate_selected,
                supported.fallback_reason,
            )

            overclaimed = self.evaluate(
                prepared,
                "You tested the signal meter.",
            )
            self.assertFalse(overclaimed.candidate_selected)
            self.assertGreaterEqual(
                overclaimed.candidate_unsupported_factual_claim_count,
                1,
            )
        finally:
            prepared.close()

    def test_negative_open_signal_does_not_support_affirmative_claim(self):
        self.add_public_message(
            "I did not test the signal meter.",
            "2026-07-25T10:00:00+00:00",
        )
        prepared = self.prepare(
            wording="BNL, what am I all about around signal meters?",
        )
        try:
            negative = self.evaluate(
                prepared,
                "You did not test the signal meter.",
            )
            self.assertTrue(
                negative.candidate_selected,
                negative.fallback_reason,
            )

            affirmative = self.evaluate(
                prepared,
                "You tested the signal meter.",
            )
            self.assertFalse(affirmative.candidate_selected)
            self.assertGreaterEqual(
                affirmative.candidate_unsupported_factual_claim_count,
                1,
            )
        finally:
            prepared.close()

    def test_synonymous_paraphrases_count_as_one_sparse_point(self):
        self.add_public_message(
            "I compare the antenna before final release.",
            "2026-07-22T10:00:00+00:00",
        )
        self.add_public_message(
            "I weigh the signal before publishing.",
            "2026-07-24T10:00:00+00:00",
        )
        prepared = self.prepare(wording="What am I all about?")
        try:
            profile = prepared.packet.profile_sufficiency
            self.assertEqual(profile.status, "sparse")
            self.assertEqual(profile.required_point_count, 1)
            self.assertEqual(profile.candidate_point_count, 1)
            self.assertEqual(profile.selected_point_count, 1)
        finally:
            prepared.close()

    def test_exact_context_wins_over_same_root_assessment_projection(self):
        text = "I asked the team to review the broadcast transition."
        self.add_public_message(
            text,
            "2026-07-26T11:50:00+00:00",
        )
        self.add_model_reply(
            "I can keep that review grounded in the public thread.",
            "2026-07-26T11:51:00+00:00",
        )
        prepared = self.prepare(
            wording="BNL, what am I all about in broadcast reviews?",
        )
        try:
            contexts = tuple(
                item
                for item in prepared.packet.items
                if item.lane == "conversation_context"
            )
            self.assertEqual(len(contexts), 1)
            self.assertEqual(contexts[0].text, text)
            self.assertFalse(
                any(
                    item.lane == "assessment_observation"
                    and set(item.root_identities).intersection(
                        contexts[0].root_identities
                    )
                    for item in prepared.packet.items
                )
            )
            self.assertIn(
                "same_root_conversation_context",
                {
                    exclusion.reason
                    for exclusion in prepared.packet.exclusions
                    if exclusion.lane == "assessment_observation"
                },
            )
        finally:
            prepared.close()

    def test_durable_and_distinct_open_signal_form_one_rich_union(self):
        self.add_public_message(
            "I keep producing synth music and mixing audio tracks.",
            "2026-06-01T10:00:00+00:00",
        )
        self.add_public_message(
            "The song vocals and drum mix need another pass.",
            "2026-06-03T10:00:00+00:00",
        )
        self.add_public_message(
            "I asked the team to review the broadcast transition.",
            "2026-07-25T10:00:00+00:00",
        )
        prepared = self.prepare(wording="What am I all about?")
        try:
            profile = prepared.packet.profile_sufficiency
            self.assertEqual(profile.status, "rich")
            self.assertEqual(profile.required_point_count, 2)
            self.assertGreaterEqual(profile.selected_point_count, 2)
            member_lanes = {
                item.lane
                for item in prepared.packet.items
                if item.subject_key == "discord_user:41"
            }
            self.assertIn("atomic_knowledge", member_lanes)
            self.assertTrue(
                member_lanes.intersection(
                    {"conversation_context", "assessment_observation"}
                )
            )
        finally:
            prepared.close()

    def test_named_sparse_profile_is_member_first_then_additive_canon(self):
        self.add_public_message(
            "I compare the antenna before final release.",
            "2026-07-22T10:00:00+00:00",
            user_id=77,
            user_name="Mac Modem",
        )
        self.add_public_message(
            "I weigh the signal before publishing.",
            "2026-07-24T10:00:00+00:00",
            user_id=77,
            user_name="Mac Modem",
        )
        prepared = self.prepare(
            user_id=77,
            user_name="Mac Modem",
            wording="What am I all about?",
        )
        try:
            self.assertEqual(
                prepared.packet.profile_sufficiency.status,
                "sparse",
            )
            self.assertEqual(
                prepared.diagnostics.canon_identity_status,
                "recognized",
            )
            member_first = self.evaluate(
                prepared,
                "You compare the signal before release. In BARCODE, Mac Modem "
                "is a founding member and chaotic tech entity.",
            )
            self.assertTrue(
                member_first.candidate_selected,
                member_first.fallback_reason,
            )

            canon_first = self.evaluate(
                prepared,
                "In BARCODE, Mac Modem is a founding member and chaotic "
                "tech entity. You compare the signal before release.",
            )
            self.assertFalse(canon_first.candidate_selected)
            self.assertEqual(
                canon_first.fallback_reason,
                "candidate_canon_dominant",
            )
        finally:
            prepared.close()


if __name__ == "__main__":
    unittest.main()
