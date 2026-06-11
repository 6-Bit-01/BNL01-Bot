import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

from bnl_entity_evidence import (
    ENTITY_EVIDENCE_TABLE,
    backfill_subject_authored_conversation_evidence,
    ensure_entity_evidence_schema,
)
from bnl_source_file_enrichment import (
    build_subject_intelligence_brief_v1,
    build_subject_memory_packet_v1,
    collect_source_enrichment_evidence,
)


class EntityEvidenceConversationBackfillTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        self.conn = sqlite3.connect(self.db)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                role TEXT,
                user_id INTEGER,
                user_name TEXT,
                author_name TEXT,
                channel_name TEXT,
                channel_policy TEXT,
                content TEXT,
                timestamp TEXT
            )
            """
        )
        ensure_entity_evidence_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db)

    def add_conversation(self, *, user_name="antigrain", role="user", policy="public_home", content="hello", channel="general-chat", user_id=10):
        cur = self.conn.execute(
            "INSERT INTO conversations (guild_id, role, user_id, user_name, author_name, channel_name, channel_policy, content, timestamp) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)",
            (role, user_id, user_name, user_name, channel, policy, content, "2026-01-02T03:04:05Z"),
        )
        self.conn.commit()
        return cur.lastrowid

    def backfill(self, subject="Antigrain", aliases=None):
        result = backfill_subject_authored_conversation_evidence(
            self.conn,
            guild_id=1,
            subject_name=subject,
            subject_key=subject.lower(),
            confirmed_aliases=aliases,
        )
        self.conn.commit()
        return result

    def evidence_rows(self):
        return [dict(row) for row in self.conn.execute(f"SELECT * FROM {ENTITY_EVIDENCE_TABLE} ORDER BY id").fetchall()]

    def test_antigrain_authored_public_rows_backfill(self):
        self.add_conversation(content="How do we bring back Cliff? https://youtu.be/example Cream reference", channel="barcode-bot", policy="public_context")
        self.add_conversation(role="model", user_name="BNL-01", content="Antigrain mentioned Cliff", channel="barcode-bot", policy="public_context")

        result = self.backfill()
        rows = self.evidence_rows()

        self.assertEqual(result["matchedRows"], 1)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["evidence_kind"], "authored_public_conversation")
        self.assertEqual(row["relation_to_subject"], "authored")
        self.assertEqual(row["review_only"], 0)
        self.assertEqual(row["source_table"], "conversations")
        self.assertTrue(row["source_row_id"])
        self.assertIn("Subject", row["safe_summary"])
        self.assertTrue(any(term in row["safe_summary"] for term in ("YouTube", "Cliff", "Cream", "question")))
        self.assertEqual(row["authority"], "public_discord_observed")

    def test_model_row_exclusion(self):
        self.add_conversation(role="model", user_name="BNL-01", content="Antigrain asked about the Network", policy="public_home")
        result = self.backfill()
        self.assertEqual(result["matchedRows"], 0)
        self.assertFalse(self.evidence_rows())

    def test_mention_only_exclusion(self):
        self.add_conversation(user_name="Other User", content="antigrain asked about the Network", policy="public_home")
        result = self.backfill()
        self.assertEqual(result["matchedRows"], 0)
        self.assertFalse(self.evidence_rows())

    def test_internal_channel_exclusion(self):
        self.add_conversation(content="Antigrain internal note", policy="internal_controlled")
        self.add_conversation(content="Antigrain sealed note", policy="sealed_test")
        result = self.backfill()
        self.assertEqual(result["matchedRows"], 0)
        self.assertIn("non_public_channel_policy", result["skippedReasons"])
        self.assertFalse(self.evidence_rows())

    def test_alias_match_routes_to_canonical_subject(self):
        self.add_conversation(user_name="grain alias", content="Can BNL explain the BARCODE Network?", policy="public_context")
        result = self.backfill(aliases=["grain alias"])
        rows = self.evidence_rows()
        self.assertEqual(result["matchedRows"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["subject_key"], "antigrain")
        self.assertEqual(rows[0]["subject_name"], "Antigrain")

    def test_idempotency(self):
        self.add_conversation(content="BNL, do you dream of electric sheep?", policy="public_home")
        first = self.backfill()
        second = self.backfill()
        rows = self.evidence_rows()
        self.assertEqual(first["createdEvents"], 1)
        self.assertEqual(second["createdEvents"], 0)
        self.assertEqual(len(rows), 1)

    def test_source_file_refresh_integration_uses_backfilled_authored_rows(self):
        self.add_conversation(content="Why does the BARCODE Network want vibrating beds in the room?", policy="public_home")
        self.add_conversation(content="Can BNL bring back Cliff through the archive?", channel="barcode-bot", policy="public_context")

        packet = collect_source_enrichment_evidence(self.db, 1, "Antigrain")
        packet["subject"] = "Antigrain"
        memory = build_subject_memory_packet_v1(packet)
        brief = build_subject_intelligence_brief_v1(memory)

        authored = brief["subjectAuthoredEvidenceV1"]
        digest = brief["subjectOwnedEvidenceDigestV1"]
        body = str(digest).lower()
        self.assertTrue(authored["conversationBackfill"]["attempted"])
        self.assertGreaterEqual(authored["conversationBackfill"]["matchedRows"], 2)
        self.assertTrue(authored["ownedAuthoredItems"])
        self.assertIn("vibrating", body)
        self.assertTrue(digest["concreteTopics"] or digest["questionsOrChallenges"])
        self.assertNotIn("limited subject-owned evidence", brief["subjectRead"].lower())
        self.assertNotIn("limited subject-owned evidence", brief["bnlTake"].lower())


if __name__ == "__main__":
    unittest.main()
