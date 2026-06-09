import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

from aiohttp.test_utils import make_mocked_request

import bnl01_bot
from bnl_memory_snapshot import build_bnl_memory_snapshot


class MemorySnapshotTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db_path = self.tmp.name
        self.tmp.close()
        self._create_db()

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

    def _create_db(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            CREATE TABLE community_presence (
                guild_id INTEGER, subject_key TEXT, display_name TEXT,
                first_seen_at TEXT, last_seen_at TEXT, source_lanes TEXT,
                approved_channel_labels TEXT, mention_count INTEGER,
                direct_interaction_count INTEGER, operator_mention_count INTEGER,
                active_windows TEXT, connection_notes TEXT, evidence_snippets TEXT,
                category TEXT, last_error_status TEXT
            )
        """)
        c.execute("""
            CREATE TABLE entity_evidence_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT,
                subject_name TEXT, matched_user_id TEXT, source_type TEXT, source_table TEXT,
                source_label TEXT, channel_name TEXT, channel_policy TEXT, visibility TEXT,
                authority TEXT, confidence REAL, relation_to_subject TEXT, topic TEXT,
                evidence_kind TEXT, safe_summary TEXT, public_safe_candidate INTEGER,
                review_only INTEGER, music_signal INTEGER, community_signal INTEGER,
                bnl_interaction INTEGER, dossier_relevance TEXT, raw_ref_json TEXT,
                created_at TEXT, observed_at TEXT, updated_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE memory_tiers (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, guild_id INTEGER,
                tier TEXT, summary TEXT, salience REAL, mentions INTEGER, updated_at TEXT,
                source_role TEXT, source_channel_policy TEXT, source_channel_name TEXT,
                source_origin TEXT, source_trust TEXT
            )
        """)
        c.execute("""
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, user_name TEXT,
                guild_id INTEGER, channel_name TEXT, channel_policy TEXT, role TEXT,
                content TEXT, timestamp TEXT
            )
        """)
        c.execute("""
            CREATE TABLE source_file_refresh_state (
                guild_id INTEGER, subject_key TEXT, subject_name TEXT,
                last_refresh_started_at TEXT, last_refresh_completed_at TEXT,
                last_refresh_status TEXT, last_recommendation_id TEXT,
                last_evidence_hash TEXT, last_evidence_count INTEGER,
                last_failure_at TEXT, last_error TEXT, cooldown_until TEXT, updated_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE source_file_refresh_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT,
                subject_name TEXT, reason TEXT, evidence_source TEXT, evidence_count INTEGER,
                priority INTEGER, status TEXT, queued_at TEXT, updated_at TEXT,
                not_before_at TEXT, last_attempt_at TEXT, attempts INTEGER,
                last_error TEXT, refresh_mode TEXT, created_by TEXT
            )
        """)
        c.execute("""
            CREATE TABLE entity_profile_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT,
                subject_name TEXT, summary_json TEXT, source_hash TEXT, created_at TEXT, updated_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE entity_intelligence_edges (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT,
                object_key TEXT, object_label TEXT, relation_type TEXT, source_type TEXT,
                source_channel_policy TEXT, visibility TEXT, authority TEXT, confidence REAL,
                first_seen_at TEXT, last_seen_at TEXT, evidence_count INTEGER, status TEXT
            )
        """)
        c.execute("""
            CREATE TABLE entity_open_questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, subject_key TEXT,
                question TEXT, reason TEXT, source_type TEXT, visibility TEXT, priority INTEGER,
                status TEXT, created_at TEXT, updated_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE broadcast_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, episode_date TEXT,
                cleaned_summary TEXT, entry_type TEXT, importance TEXT, public_safe INTEGER,
                affects_next_show INTEGER, usage_scope TEXT, status TEXT, created_at TEXT, updated_at TEXT
            )
        """)
        c.execute("""
            CREATE TABLE ambient_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER, channel_id INTEGER,
                message TEXT, source_type TEXT, posted_at TEXT, timestamp TEXT
            )
        """)
        c.execute(
            "INSERT INTO community_presence VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "crow", "Crow", "2026-01-01", "2026-01-02", '["community_presence"]', '["public_selective"]', 5, 2, 1, '["window"]', '["private note"]', json.dumps(["Crow public-safe snippet " + "x" * 900]), "community_regular_candidate", "none"),
        )
        c.execute(
            """INSERT INTO entity_evidence_events
            (guild_id, subject_key, subject_name, matched_user_id, source_type, source_table,
             source_label, channel_name, channel_policy, visibility, authority, confidence,
             relation_to_subject, topic, evidence_kind, safe_summary, public_safe_candidate,
             review_only, music_signal, community_signal, bnl_interaction, dossier_relevance,
             raw_ref_json, created_at, observed_at, updated_at)
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (1, "crow", "Crow", "123456789012345678", "conversation", "conversations", "structured", "research-and-development", "private_internal", "review_only", "source_blind_memory", 0.7, "authored", "source-file", "summary", "Safe summary with API_KEY=supersecret and <@123456789012345678>", 0, 1, 1, 1, 1, "review", '{"raw":"full transcript DISCORD_TOKEN=abc.def.ghi"}', "2026", "2026", "2026"),
        )
        c.execute(
            "INSERT INTO entity_evidence_events (guild_id, subject_key, subject_name, safe_summary, review_only, created_at) VALUES (?,?,?,?,?,?)",
            (1, "other", "Other", "Other summary", 0, "2026"),
        )
        c.execute(
            "INSERT INTO memory_tiers (user_id, guild_id, tier, summary, updated_at, source_role, source_channel_policy, source_channel_name, source_origin, source_trust) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (42, 1, "profile", "remembered summary", "2026", "user", "private_internal", "research-and-development", "legacy", "mixed_or_legacy_consolidated"),
        )
        long_secret_text = "hello BNL_MEMORY_SNAPSHOT_TOKEN=do-not-leak 123456789012345678 " + ("x" * 1200)
        c.execute(
            "INSERT INTO conversations (user_id, user_name, guild_id, channel_name, channel_policy, role, content, timestamp) VALUES (?,?,?,?,?,?,?,?)",
            (42, "Crow", 1, "barcode-bot", "public_context", "user", long_secret_text, "2026-01-03"),
        )
        c.execute(
            "INSERT INTO source_file_refresh_state VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "crow", "Crow", "2026", "2026", "failed", "rec", "hash", 2, "2026", "Text field is too long: compact payload", "", "2026"),
        )
        c.execute(
            "INSERT INTO source_file_refresh_queue (guild_id, subject_key, subject_name, reason, evidence_source, evidence_count, priority, status, queued_at, updated_at, attempts, last_error, refresh_mode, created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "crow", "Crow", "audit", "test", 1, 50, "queued", "2026", "2026", 4, "too long", "automatic", "test"),
        )
        c.execute("INSERT INTO entity_profile_snapshots (guild_id, subject_key, subject_name, summary_json, source_hash, created_at, updated_at) VALUES (?,?,?,?,?,?,?)", (1, "crow", "Crow", json.dumps({"summary": "ok", "action_items": ["review"], "openQuestions": ["q?"]}), "hash", "2026", "2026"))
        c.execute("INSERT INTO entity_intelligence_edges (guild_id, subject_key, object_key, object_label, relation_type, source_type, source_channel_policy, visibility, authority, confidence, first_seen_at, last_seen_at, evidence_count, status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (1, "crow", "barcode", "BARCODE", "mentions", "evidence", "public_context", "review_only", "public_context", 0.8, "2026", "2026", 1, "active"))
        c.execute("INSERT INTO entity_open_questions (guild_id, subject_key, question, reason, source_type, visibility, priority, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)", (1, "crow", "What is safe?", "audit", "evidence", "review_only", 10, "open", "2026", "2026"))
        c.execute("INSERT INTO broadcast_memory (guild_id, episode_date, cleaned_summary, entry_type, importance, public_safe, affects_next_show, usage_scope, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", (1, "2026-01-09", "show summary", "show", "medium", 1, 0, "broadcast", "active", "2026", "2026"))
        c.execute("INSERT INTO ambient_log (guild_id, channel_id, message, source_type, posted_at, timestamp) VALUES (?,?,?,?,?,?)", (1, 99, "ambient note secret=hide", "ambient", "2026", "2026"))
        conn.commit()
        conn.close()

    def _table_counts(self):
        conn = sqlite3.connect(self.db_path)
        counts = {row[0]: row[1] for row in conn.execute("SELECT name, (SELECT COUNT(*) FROM sqlite_master) FROM sqlite_master WHERE type='table'")}
        per_table = {}
        for name in counts:
            per_table[name] = conn.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        conn.close()
        return per_table

    def test_valid_snapshot_contains_counts_sections_and_redacts(self):
        snapshot = build_bnl_memory_snapshot(self.db_path, limit=25)
        self.assertEqual(snapshot["snapshotVersion"], 1)
        self.assertEqual(snapshot["databaseName"], os.path.basename(self.db_path))
        self.assertGreaterEqual(snapshot["tableCounts"]["community_presence"], 1)
        self.assertEqual(snapshot["sections"]["communityPresence"][0]["subject_key"], "crow")
        evidence = snapshot["sections"]["entityEvidenceEvents"][0]
        self.assertNotIn("raw_ref_json", evidence)
        self.assertNotIn("supersecret", json.dumps(snapshot))
        self.assertNotIn("123456789012345678", json.dumps(snapshot))
        sample_text = snapshot["sections"]["memoryTierSamples"][0]["content"]
        self.assertLessEqual(len(sample_text), 500)
        self.assertIn("[redacted-secret]", sample_text)
        self.assertTrue(snapshot["sections"]["memoryTierRiskSummary"]["groups"])
        self.assertIn("oversized compact payload failure appears in refresh state", snapshot["warnings"])

    def test_subject_filter_and_limit_cap(self):
        snapshot = build_bnl_memory_snapshot(self.db_path, limit=1000, subject_key="crow")
        self.assertEqual(snapshot["rowLimits"]["default"], 100)
        keys = {row["subject_key"] for row in snapshot["sections"]["entityEvidenceEvents"]}
        self.assertEqual(keys, {"crow"})

    def test_builder_is_read_only(self):
        before = self._table_counts()
        build_bnl_memory_snapshot(self.db_path, limit=5, include_raw_refs=True)
        after = self._table_counts()
        self.assertEqual(before, after)

    def test_endpoint_rejects_missing_or_invalid_token(self):
        old_db = bnl01_bot.DB_FILE
        try:
            bnl01_bot.DB_FILE = self.db_path
            with mock.patch.dict(os.environ, {"BNL_MEMORY_SNAPSHOT_TOKEN": "snapshot-token"}):
                missing = make_mocked_request("GET", bnl01_bot.BNL_MEMORY_SNAPSHOT_PATH)
                missing_response = asyncio.run(bnl01_bot._handle_bnl_memory_snapshot(missing))
                self.assertEqual(missing_response.status, 401)

                invalid = make_mocked_request("GET", bnl01_bot.BNL_MEMORY_SNAPSHOT_PATH, headers={bnl01_bot.BNL_MEMORY_SNAPSHOT_TOKEN_HEADER: "wrong"})
                invalid_response = asyncio.run(bnl01_bot._handle_bnl_memory_snapshot(invalid))
                self.assertEqual(invalid_response.status, 401)
        finally:
            bnl01_bot.DB_FILE = old_db

    def test_endpoint_valid_token_returns_json_without_side_effects(self):
        old_db = bnl01_bot.DB_FILE
        before = self._table_counts()
        try:
            bnl01_bot.DB_FILE = self.db_path
            with mock.patch.dict(os.environ, {"BNL_MEMORY_SNAPSHOT_TOKEN": "snapshot-token"}), \
                 mock.patch.object(bnl01_bot, "process_source_file_refresh_now") as refresh_now, \
                 mock.patch.object(bnl01_bot, "send_dossier_recommendation") as send_recommendation:
                request = make_mocked_request(
                    "GET",
                    bnl01_bot.BNL_MEMORY_SNAPSHOT_PATH + "?limit=1&subject_key=crow&include_samples=false",
                    headers={bnl01_bot.BNL_MEMORY_SNAPSHOT_TOKEN_HEADER: "snapshot-token"},
                )
                response = asyncio.run(bnl01_bot._handle_bnl_memory_snapshot(request))
                self.assertEqual(response.status, 200)
                payload = json.loads(response.text)
                self.assertEqual(payload["rowLimits"]["default"], 1)
                self.assertEqual(payload["filters"]["subject_key"], "crow")
                self.assertEqual(payload["sections"]["memoryTierSamples"], [])
                self.assertIn("tableCounts", payload)
                refresh_now.assert_not_called()
                send_recommendation.assert_not_called()
        finally:
            bnl01_bot.DB_FILE = old_db
        self.assertEqual(before, self._table_counts())


if __name__ == "__main__":
    unittest.main()
