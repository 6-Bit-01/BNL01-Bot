"""Original public member statements survive normal and packet source ownership."""

import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
from bnl_unified_response_assessment import build_situation_frame_v1


QUERY = "What has TestMarbles said about stealing pants?"
STATEMENT = "I have been stealing the stage pants again."


class NamedPublicConversationRecallTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = str(Path(self.tmp.name) / "conversation.db")
        self.patches = [
            mock.patch.object(bot, "DB_FILE", self.path),
            mock.patch.object(bot, "BNL_OWNER_USER_ID", 99),
            mock.patch.dict(os.environ, {"BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "false"}),
        ]
        for patch in self.patches:
            patch.start()
            self.addCleanup(patch.stop)
        with sqlite3.connect(self.path) as conn:
            conn.executescript("""
                CREATE TABLE conversations (
                    id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER,
                    user_name TEXT, role TEXT, content TEXT, channel_id INTEGER,
                    channel_name TEXT, channel_policy TEXT, timestamp TEXT,
                    message_id INTEGER
                );
                CREATE TABLE memory_ledger_entries (
                    entry_id TEXT PRIMARY KEY, guild_id INTEGER,
                    source_table TEXT, source_row_id TEXT, subject_key TEXT,
                    lifecycle_status TEXT
                );
                CREATE TABLE memory_ledger_lineage (
                    entry_id TEXT, guild_id INTEGER, lineage_type TEXT,
                    target_entry_id TEXT, created_at TEXT
                );
            """)

    def seed(self, row_id=1, *, text=STATEMENT, user_id=222, label="TestMarbles",
             role="user", policy="public_home", guild_id=77,
             timestamp="2026-08-29T03:00:00+00:00"):
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (row_id, guild_id, user_id, label, role, text, 20,
                 "test-public-stage", policy, timestamp, row_id + 1000),
            )

    def frame(self, *, text=QUERY, policy="sealed_test", ids=(222,), labels=("TestMarbles",)):
        return build_situation_frame_v1(
            route_allowed=True, route_mode="normal_chat",
            conversation_surface=bot.conversation_surface_for_channel_policy(policy),
            channel_policy=policy, current_text=text,
            current_speaker_user_ids=(111,), current_speaker_labels=("Test Requester",),
            subject_user_ids=ids, subject_label_hints=labels,
            response_act="answer",
        )

    def read(self, *, text=QUERY, policy="sealed_test", frame=None):
        return bot.build_named_public_conversation_context(
            situation_frame=frame or self.frame(text=text, policy=policy),
            guild_id=77, route_mode="normal_chat", channel_policy=policy,
            user_text=text, channel_id=10, channel_name="test-target-room",
        )

    def control(self, *, state="active", lineage=""):
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO memory_ledger_entries VALUES (?,?,?,?,?,?)",
                ("root-one", 77, "conversations", "1", bot.subject_key_for_user(222), state),
            )
            if lineage:
                conn.execute(
                    "INSERT INTO memory_ledger_lineage VALUES (?,?,?,?,?)",
                    ("member-control", 77, lineage, "root-one", "2026-09-09"),
                )

    def test_original_old_target_message_is_available_with_packet_gate_off(self):
        self.seed()
        for policy in ("public_home", "sealed_test"):
            with self.subTest(policy=policy):
                context, basis = self.read(policy=policy)
                self.assertIn(STATEMENT, context)
                self.assertIn("TestMarbles in #test-public-stage", context)
                self.assertIn("2026-08-29T03:00:00+00:00", context)
                self.assertEqual(basis.source_row_ids, (1,))
                self.assertEqual(basis.participant_user_ids, (222,))
                self.assertEqual(basis.evidence_items[0].text, STATEMENT)
                self.assertEqual(basis.evidence_items[0].speaker_user_id, 222)
                self.assertFalse(basis.evidence_items[0].current_turn)
                self.assertFalse(bot.refresh_prompt_source_basis(basis)[1])

    def test_private_sealed_wrong_author_and_wrong_guild_rows_are_excluded(self):
        self.seed()
        self.seed(2, user_id=111, text="Requester pants statement.")
        self.seed(3, user_id=333, text="Different author pants statement.")
        for row_id, policy in enumerate(("internal_controlled", "sealed_test", "unknown", "reference_canon"), 4):
            self.seed(row_id, policy=policy, text="Excluded pants statement.")
        self.seed(8, guild_id=88, text="Different guild pants statement.")
        self.seed(9, role="model", text="Fabricated pants statement.")
        context, basis = self.read()
        self.assertEqual(basis.source_row_ids, (1,))
        self.assertNotIn("Excluded", context)
        self.assertNotIn("Fabricated", context)

    def test_public_policies_and_resolved_multiple_members_keep_authorship(self):
        self.seed(policy="public_selective")
        self.seed(2, user_id=333, label="Test Finch", policy="public_context",
                  text="I returned the stage pants to the wardrobe.")
        query = "What have TestMarbles and Test Finch said about pants?"
        frame = self.frame(text=query, ids=(222, 333), labels=("TestMarbles", "Test Finch"))
        self.assertEqual(frame.status, "resolved")
        context, basis = self.read(text=query, frame=frame)
        self.assertEqual(basis.participant_user_ids, (222, 333))
        self.assertEqual(basis.speaker_labels, ("TestMarbles", "Test Finch"))
        self.assertIn(STATEMENT, context)

    def test_only_complete_messages_fit_budget_and_rank_before_recent_noise(self):
        self.seed()
        self.seed(2, text="Pants " * 300)
        for row_id in range(3, 20):
            self.seed(row_id, text="TestMarbles said the studio looks lovely.")
        with mock.patch.object(bot, "MEMORY_PROMPT_BUDGET_PUBLIC", 220):
            context, basis = self.read()
        self.assertEqual(basis.source_row_ids, (1,))
        self.assertIn(STATEMENT, context)
        self.assertLessEqual(len(context), 220)
        self.assertNotIn("…", context)

    def test_subject_labels_and_speech_function_words_are_not_topic_evidence(self):
        self.seed(text="TestMarbles said the studio looks lovely.")
        self.assertEqual(self.read(), ("", None))
        self.assertEqual(self.read(text="What has TestMarbles said?"), ("", None))

    def test_ambiguous_unbound_and_owner_subjects_do_not_read_rows(self):
        self.seed()
        frame = self.frame()
        for candidate in (
            replace(frame, status="ambiguous"),
            replace(frame, subjects=(replace(frame.subjects[0], confidence="low"),)),
            self.frame(ids=(99,), labels=("6 Bit",)),
        ):
            with self.subTest(frame=candidate.status, subjects=candidate.subjects):
                self.assertEqual(self.read(frame=candidate), ("", None))

    def test_shared_explicit_date_filter_uses_pacific_calendar_and_invalid_date_is_empty(self):
        self.seed()
        self.seed(2, timestamp="2026-08-30T03:00:00+00:00")
        for date in ("August 28, 2026", "2026-08-28"):
            text = QUERY.rstrip("?") + " on " + date + "?"
            context, basis = self.read(text=text)
            self.assertEqual(basis.source_row_ids, (1,))
            self.assertIn(STATEMENT, context)
        self.assertEqual(self.read(text=QUERY + " February 30, 2026"), ("", None))

    def test_read_is_snapshot_only_and_does_not_create_missing_database(self):
        self.seed()
        before = hashlib.sha256(Path(self.path).read_bytes()).hexdigest()
        self.assertIsNotNone(self.read()[1])
        self.assertEqual(before, hashlib.sha256(Path(self.path).read_bytes()).hexdigest())
        absent = str(Path(self.tmp.name) / "absent.db")
        with mock.patch.object(bot, "DB_FILE", absent):
            self.assertEqual(self.read(), ("", None))
        self.assertFalse(Path(absent).exists())

    def test_selected_source_deletion_or_identity_visibility_content_change_invalidates_basis(self):
        self.seed()
        for column, value in (
            ("content", "A changed pants statement."),
            ("user_name", "Test Renamed"),
            ("user_id", 333),
            ("channel_policy", "internal_controlled"),
            ("role", "model"),
        ):
            with self.subTest(column=column):
                _context, basis = self.read()
                with sqlite3.connect(self.path) as conn:
                    original = conn.execute("SELECT " + column + " FROM conversations WHERE id=1").fetchone()[0]
                    conn.execute("UPDATE conversations SET " + column + "=? WHERE id=1", (value,))
                self.assertEqual(bot.prompt_source_basis_failure((basis,)), "conversation_source_changed")
                with sqlite3.connect(self.path) as conn:
                    conn.execute("UPDATE conversations SET " + column + "=? WHERE id=1", (original,))
        _context, basis = self.read()
        with sqlite3.connect(self.path) as conn:
            conn.execute("DELETE FROM conversations WHERE id=1")
        self.assertEqual(bot.prompt_source_basis_failure((basis,)), "conversation_source_changed")

    def test_forgotten_and_retracted_retained_raw_rows_are_never_reintroduced(self):
        self.seed()
        self.control(state="forgotten")
        self.assertEqual(self.read(), ("", None))
        self.control(state="active", lineage="retracts")
        self.assertEqual(self.read(), ("", None))

    def test_new_forget_and_correction_control_invalidates_generated_source_basis(self):
        self.seed()
        for state, lineage in (("forgotten", ""), ("active", "correction_of")):
            with self.subTest(state=state, lineage=lineage):
                with sqlite3.connect(self.path) as conn:
                    conn.execute("DELETE FROM memory_ledger_entries")
                    conn.execute("DELETE FROM memory_ledger_lineage")
                _context, basis = self.read()
                self.control(state=state, lineage=lineage)
                self.assertEqual(bot.prompt_source_basis_failure((basis,)), "conversation_source_changed")
                self.assertEqual(self.read(), ("", None))


if __name__ == "__main__":
    unittest.main()
