from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.journal_relay_health import inspect


class JournalRelayHealthTests(unittest.TestCase):
    def test_report_preserves_database_and_prints_only_aggregate_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "health.db"
            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE website_relay_history(guild_id INTEGER,relay_id TEXT,published_timestamp TEXT,event_type TEXT,public_message TEXT)")
                conn.execute("INSERT INTO website_relay_history VALUES(1,'test-relay','2026-09-17T01:00:00Z','fresh_discord','PrivateContentMustNotAppear')")
                conn.execute("CREATE TABLE bnl_journal_source_events(guild_id INTEGER,source_kind TEXT,source_key TEXT,occurred_at_ms INTEGER)")
                conn.execute("CREATE TABLE memory_governance_shared_brain_synthesis_runs(guild_id INTEGER,created_at TEXT,rendered_lane_counts_json TEXT,prompt_applied INTEGER,response_sent INTEGER,source_revalidation_status TEXT)")
                conn.execute("INSERT INTO memory_governance_shared_brain_synthesis_runs VALUES(1,'2026-09-17T01:00:00Z',?,1,1,'valid')", (json.dumps({"journal_publication": 1, "relay_publication": 2}),))
                conn.execute("INSERT INTO memory_governance_shared_brain_synthesis_runs VALUES(1,'2026-09-17T01:01:00Z',?,1,0,'changed')", (json.dumps({"journal_publication": 5}),))
                conn.execute("INSERT INTO memory_governance_shared_brain_synthesis_runs VALUES(2,'2026-09-17T01:01:00Z',?,1,1,'valid')", (json.dumps({"moment": 100}),))
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            report = inspect(str(path), 1, now=datetime(2026, 9, 17, 2, tzinfo=timezone.utc))
            self.assertEqual(before, hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertNotIn("PrivateContentMustNotAppear", json.dumps(report))
            self.assertNotIn("test-relay", json.dumps(report))
            self.assertEqual(1, report["acceptedRelaysMissingArchiveReceipt24h"])
            self.assertEqual(1, report["relayPublications"]["last24h"])
            self.assertFalse(report["moments"]["available"])
            self.assertEqual({"journal_publication": 1, "relay_publication": 2}, report["sharedBrainReceipts24h"]["lanesInSentPrompts"])

    def test_moment_rejection_reasons_are_time_and_guild_scoped(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "health.db"
            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE memory_moment_windows(guild_id INTEGER,last_activity_at TEXT,lifecycle_status TEXT,qualification_reason TEXT)")
                conn.executemany("INSERT INTO memory_moment_windows VALUES(?,?,?,?)", [
                    (1, "2026-09-17T01:00:00Z", "rejected", "low_signal_or_insufficient_continuity"),
                    (1, "2026-09-17T01:00:00Z", "rejected", "low_signal_or_insufficient_continuity"),
                    (1, "2026-09-17T01:00:00Z", "finalized", "qualified"),
                    (2, "2026-09-17T01:00:00Z", "rejected", "other_guild"),
                    (1, "2026-09-15T01:00:00Z", "rejected", "old_window"),
                ])
            before = path.read_bytes()
            report = inspect(str(path), 1, now=datetime(2026, 9, 17, 2, tzinfo=timezone.utc))
            self.assertEqual({"available": True, "counts": {"low_signal_or_insufficient_continuity": 2}}, report["momentRejectionReasons24h"])
            self.assertEqual(before, path.read_bytes())

    def test_missing_file_does_not_create_a_database(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "missing.db"
            with self.assertRaises(sqlite3.OperationalError):
                inspect(str(path), 1)
            self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
