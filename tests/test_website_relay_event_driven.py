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
        bnl01_bot._website_relay_transaction_locks_by_guild.clear()
        bnl01_bot._website_relay_generation_tasks_by_guild.clear()
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

    def test_distinct_general_public_signal_messages_are_not_family_blocked(self):
        state.record_publication(self.db, 42, message="A new public art thread gathered enough details to identify the technique being compared.", directive="Track the public technique comparison for a useful next reference.", mode="OBSERVATION", relay_lane="current_signal", event_type="fresh_public_discord_activity", source_cursor=1)
        reason = state.reject_reason_for_candidate(self.db, 42, "A collaboration planning thread shifted toward a shared zine format.", "Determine whether the collaboration has a public next step worth indexing.")
        self.assertEqual(reason, "")

    def test_repeated_fallback_directive_alone_does_not_block_new_message(self):
        repeated = "Review fresh public Discord context before the next relay update."
        state.record_publication(self.db, 42, message="A public question identified one release-credit gap for follow-up.", directive=repeated, mode="OBSERVATION", relay_lane="current_signal", event_type="fresh_public_discord_activity", source_cursor=1)
        reason = state.reject_reason_for_candidate(self.db, 42, "A separate artwork thread compared two cover treatments for a future post.", repeated)
        self.assertEqual(reason, "")

    def test_radio_release_dossier_transmission_topics_allowed_when_discord_grounded(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL compare the public BARCODE Radio release notes with the Transmission topic people mentioned? #barcode", user="a")
        self.add_row("There is also a public dossier question about credits for that music release discussion?", user="b")
        prompts = []
        async def fake_gemini(prompt, user_id=0, guild_id=0, route=""):
            prompts.append(prompt)
            return "Fresh Discord discussion connects BARCODE Radio, a release-credit question, a dossier angle, and a Transmission topic without confirming any live state.\nFollow the public thread for confirmed credits or links before treating the topic as indexed."
        with mock.patch.object(bnl01_bot, "fetch_bnl_read_model", side_effect=AssertionError("read model called")), \
             mock.patch.object(bnl01_bot, "get_recent_signal_summary", side_effect=AssertionError("queue helper called")), \
             mock.patch.object(bnl01_bot, "get_gemini_response", side_effect=fake_gemini):
            decision = self.generate()
        self.assertTrue(decision.publish)
        self.assertIn("BARCODE Radio", prompts[0])

    def test_concurrent_transactions_publish_once_and_advance_once(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL explain how tonight's public broadcast track submissions work? #barcode", user="a")
        self.add_row("I have a public question about the show and which track context matters?", user="b")
        post_count = 0
        async def fake_post(**kwargs):
            nonlocal post_count
            post_count += 1
            await asyncio.sleep(0.02)
            return True
        async def fake_gemini(prompt, user_id=0, guild_id=0, route=""):
            await asyncio.sleep(0.02)
            return "Two fresh Discord questions compare broadcast submissions with track context for tonight's show thread.\nFollow the public thread for confirmed submission details or links before indexing the answer."
        async def run_two():
            with mock.patch.object(bnl01_bot, "get_gemini_response", side_effect=fake_gemini), \
                 mock.patch.object(bnl01_bot, "update_website_status_controlled_async", side_effect=fake_post):
                return await asyncio.gather(
                    bnl01_bot._execute_website_relay_transaction(42, source="relay"),
                    bnl01_bot._execute_website_relay_transaction(42, force=True, source="forcePull", admin_note_source="forcePull"),
                )
        decisions = asyncio.run(run_two())
        self.assertEqual(sum(1 for d in decisions if d.publish), 1)
        self.assertEqual(post_count, 1)
        self.assertEqual(len(state.recent_history(self.db, 42, 10)), 1)
        self.assertEqual(state.get_cursor(self.db, 42), 2)

    def test_concurrent_manual_and_scheduled_publish_once(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL explain how tonight's public broadcast track submissions work? #barcode", user="a")
        self.add_row("I have a public question about the show and which track context matters?", user="b")
        post_count = 0
        async def fake_post(**kwargs):
            nonlocal post_count
            post_count += 1
            return True
        async def fake_gemini(prompt, user_id=0, guild_id=0, route=""):
            await asyncio.sleep(0.01)
            return "Two fresh Discord questions compare broadcast submissions with track context for tonight's show thread.\nFollow the public thread for confirmed submission details or links before indexing the answer."
        async def run_two():
            with mock.patch.object(bnl01_bot, "get_gemini_response", side_effect=fake_gemini), \
                 mock.patch.object(bnl01_bot, "update_website_status_controlled_async", side_effect=fake_post):
                manual = asyncio.create_task(bnl01_bot.request_fresh_website_relay(42, force=True))
                scheduled = asyncio.create_task(bnl01_bot._execute_website_relay_transaction(42, source="relay"))
                return await asyncio.gather(manual, scheduled)
        results = asyncio.run(run_two())
        self.assertEqual(post_count, 1)
        self.assertEqual(len(state.recent_history(self.db, 42, 10)), 1)
        self.assertEqual(state.get_cursor(self.db, 42), 2)
        self.assertTrue(results[0][0])

    def test_transaction_failed_delivery_leaves_cursor_unchanged(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL explain how tonight's public broadcast track submissions work? #barcode", user="a")
        self.add_row("I have a public question about the show and which track context matters?", user="b")
        async def fake_gemini(prompt, user_id=0, guild_id=0, route=""):
            return "Two fresh Discord questions compare broadcast submissions with track context for tonight's show thread.\nFollow the public thread for confirmed submission details or links before indexing the answer."
        async def run_one():
            with mock.patch.object(bnl01_bot, "get_gemini_response", side_effect=fake_gemini), \
                 mock.patch.object(bnl01_bot, "update_website_status_controlled_async", return_value=False):
                return await bnl01_bot._execute_website_relay_transaction(42, source="relay")
        decision = asyncio.run(run_one())
        self.assertFalse(decision.publish)
        self.assertEqual(decision.skipReason, "website_post_failed")
        self.assertEqual(state.get_cursor(self.db, 42), 0)
        self.assertEqual(state.recent_history(self.db, 42, 10), [])

    def test_timed_out_generation_is_cancelled_and_cannot_publish_late(self):
        original_timeout = bnl01_bot.BNL_WEBSITE_RELAY_GENERATION_TIMEOUT_SECONDS
        bnl01_bot.BNL_WEBSITE_RELAY_GENERATION_TIMEOUT_SECONDS = 0.01
        cancelled = asyncio.Event()
        async def slow_generation(guild_id):
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                cancelled.set()
                raise
        async def run_one():
            with mock.patch.object(bnl01_bot, "generate_dynamic_website_relay", side_effect=slow_generation), \
                 mock.patch.object(bnl01_bot, "update_website_status_controlled_async", side_effect=AssertionError("posted late")):
                return await bnl01_bot._execute_website_relay_transaction(42, source="relay")
        try:
            decision = asyncio.run(run_one())
        finally:
            bnl01_bot.BNL_WEBSITE_RELAY_GENERATION_TIMEOUT_SECONDS = original_timeout
        self.assertFalse(decision.publish)
        self.assertEqual(decision.skipReason, "relay_generation_timeout")

    def test_restart_hydration_uses_newest_eight_lanes(self):
        lanes = [f"lane_{i}" for i in range(10)]
        for i, lane in enumerate(lanes):
            state.record_publication(self.db, 42, message=f"Public Discord raised distinct track question {i} with enough detail.", directive=f"Follow public context item {i} for confirmed details.", mode="OBSERVATION", relay_lane=lane, event_type="fresh_public_discord_activity", source_cursor=i, published_timestamp=f"2026-07-15T00:00:{i:02d}Z")
        bnl01_bot._recent_relay_lanes_by_guild[42].clear()
        bnl01_bot._hydrate_recent_relay_memory(42)
        self.assertEqual(list(bnl01_bot._recent_relay_lanes_by_guild[42]), lanes[2:10])

    def test_silence_for_failure_modes_and_stock_directive(self):
        state.bootstrap_cursor(self.db, 42, 0)
        self.add_row("Can BNL explain how tonight's public broadcast track submissions work? #barcode", user="a")
        self.add_row("I have a public question about the show and which track context matters?", user="b")
        with mock.patch.object(bnl01_bot, "get_gemini_response", side_effect=RuntimeError("provider down")):
            decision = self.generate()
        self.assertFalse(decision.publish)
        self.assertEqual(decision.skipReason, "provider_failure")
        self.assertEqual(state.stock_directive_reason("Continue monitoring."), "stock_directive_rejected")


if __name__ == "__main__":
    unittest.main()
