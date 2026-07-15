import asyncio
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import bnl01_bot
import bnl_website_relay_state as state


class WebsiteRelayEventDrivenTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        self.db = self.tmp.name
        self.old_db = bnl01_bot.DB_FILE
        bnl01_bot.DB_FILE = self.db
        bnl01_bot._recent_relay_messages.clear()
        with sqlite3.connect(self.db) as conn:
            conn.execute("""
            CREATE TABLE conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                guild_id INTEGER NOT NULL,
                channel_name TEXT,
                channel_policy TEXT,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)
        state.ensure_schema(self.db)

    def tearDown(self):
        bnl01_bot.DB_FILE = self.old_db
        try:
            os.unlink(self.db)
        except OSError:
            pass

    def add_row(self, content, *, policy="public_home", role="user", ts=None, user="tester"):
        ts = ts or datetime.utcnow().replace(microsecond=0).isoformat(sep=" ")
        with sqlite3.connect(self.db) as conn:
            cur = conn.execute(
                "INSERT INTO conversations(user_id,user_name,guild_id,channel_name,channel_policy,role,content,timestamp) VALUES(?,?,?,?,?,?,?,?)",
                (1, user, 42, "pub", policy, role, content, ts),
            )
            return cur.lastrowid

    def run(self, coro):  # type: ignore[override]
        if asyncio.iscoroutine(coro):
            return asyncio.run(coro)
        return super().run(coro)

    def generate(self):
        return asyncio.run(bnl01_bot.generate_dynamic_website_relay(42))

    def test_bootstrap_old_rows_not_reused_and_no_model_or_post(self):
        self.add_row("old public discussion about a track and show question? " * 3)
        with mock.patch.object(bnl01_bot, "get_gemini_response", side_effect=AssertionError("model called")):
            decision = self.generate()
        self.assertFalse(decision.publish)
        self.assertEqual(decision.skipReason, "bootstrap_no_publish")
        self.assertEqual(state.get_cursor(self.db, 42), 1)

    def test_no_new_public_rows_no_model(self):
        state.bootstrap_cursor(self.db, 42, 0)
        with mock.patch.object(bnl01_bot, "get_gemini_response", side_effect=AssertionError("model called")):
            decision = self.generate()
        self.assertFalse(decision.publish)
        self.assertEqual(decision.skipReason, "no_new_public_signal")

    def test_only_eligible_public_policies_considered(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("private should not count " * 20, policy="sealed_test")
        self.add_row("bot should not count " * 20, role="assistant")
        decision = self.generate()
        self.assertFalse(decision.publish)
        self.assertEqual(decision.skipReason, "no_new_public_signal")

    def test_weak_fresh_context_does_not_publish(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("same lol")
        decision = self.generate()
        self.assertFalse(decision.publish)
        self.assertEqual(decision.skipReason, "fresh_context_below_threshold")

    def test_strong_context_publishes_once_then_cursor_advances_on_delivery(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL explain how tonight's public broadcast track submissions work? #barcode", user="a")
        self.add_row("I have a public question about the show and which track context matters?", user="b")
        with mock.patch.object(bnl01_bot, "get_gemini_response", return_value="Two fresh Discord questions compare broadcast submissions with track context for tonight's show thread.\nReview the fresh Discord questions and keep the relay tied to eligible context."):
            decision = self.generate()
        self.assertTrue(decision.publish)
        state.record_publication(self.db, 42, message=decision.message, directive=decision.directive, mode=decision.mode, relay_lane=decision.relayLane, event_type=decision.eventType, source_cursor=decision.sourceCursor)
        again = self.generate()
        self.assertFalse(again.publish)
        self.assertEqual(again.skipReason, "no_new_public_signal")

    def test_failed_delivery_does_not_advance_cursor(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL explain how tonight's public broadcast track submissions work? #barcode", user="a")
        self.add_row("I have a public question about the show and which track context matters?", user="b")
        with mock.patch.object(bnl01_bot, "get_gemini_response", return_value="Two fresh Discord questions compare broadcast submissions with track context for tonight's show thread.\nReview the fresh Discord questions and keep the relay tied to eligible context."):
            decision = self.generate()
        self.assertTrue(decision.publish)
        self.assertEqual(state.get_cursor(self.db, 42), 0)

    def test_timeout_failure_and_stock_duplicate_rejections(self):
        self.assertEqual(state.reject_reason_for_candidate(self.db, 42, "BNL remains online and observing while public signal is quiet.", "Monitor until fresh signal returns."), "stock_family_rejected")
        state.record_publication(self.db, 42, message="Public Discord is comparing show questions with track submission context.", directive="Review those fresh public questions.", mode="OBSERVATION", relay_lane="current_signal", event_type="fresh_public_discord_activity", source_cursor=5)
        self.assertIn(state.reject_reason_for_candidate(self.db, 42, "Public Discord is comparing show questions with track submission context!", "Review those fresh public questions."), {"exact_duplicate", "near_duplicate"})

    def test_history_retains_25_and_no_raw_names(self):
        for i in range(30):
            state.record_publication(self.db, 42, message=f"Public Discord raised distinct track question {i} with enough detail.", directive=f"Review public context item {i}.", mode="OBSERVATION", relay_lane="current_signal", event_type="fresh_public_discord_activity", source_cursor=i)
        hist = state.recent_history(self.db, 42, 50)
        self.assertEqual(len(hist), 25)
        joined = "\n".join(str(h) for h in hist)
        self.assertNotIn("tester", joined)

    def test_read_model_helpers_not_called_by_generation(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL explain how tonight's public broadcast track submissions work? #barcode", user="a")
        self.add_row("I have a public question about the show and which track context matters?", user="b")
        with mock.patch.object(bnl01_bot, "fetch_bnl_read_model", side_effect=AssertionError("read model called")), \
             mock.patch.object(bnl01_bot, "get_recent_signal_summary", side_effect=AssertionError("queue helper called")), \
             mock.patch.object(bnl01_bot, "get_gemini_response", return_value="Two fresh Discord questions compare broadcast submissions with track context for tonight's show thread.\nReview the fresh Discord questions and keep the relay tied to eligible context."):
            decision = self.generate()
        self.assertTrue(decision.publish)

    def test_explicit_read_model_still_callable(self):
        with mock.patch.object(bnl01_bot, "BNL_READ_MODEL_ENABLED", False):
            self.assertIsInstance(bnl01_bot.fetch_bnl_read_model(force=True), dict)


if __name__ == "__main__":
    unittest.main()
