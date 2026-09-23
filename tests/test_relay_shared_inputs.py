"""Real public readers, saved Relay retries, and changing source authority."""
import asyncio
import json
import os
import sqlite3
import tempfile
import threading
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import bnl01_bot as bot
import bnl_journal as journal
import bnl_journal_source_store as source_store
import bnl_moment_engine as moments
import bnl_tiktok_show_ledger as shows
import bnl_website_relay_state as relay
from tests import test_moment_meaning as meaning
from tests import test_tiktok_show_evidence_ledger as show_fixture
from tests import test_publication_read_adapters as publications
from tests.test_publication_read_adapters import control_snapshot
from tests.test_website_contract_v2 import Resp, accepted_body


COPY = ("An earlier exchange cast the reporters as unusually diligent journalists.\n"
        "Which part of that playful source-checking debate deserves another look?")


class RelaySharedInputsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "relay.db")
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        clock = mock.patch.object(source_store, "_now_ms", return_value=0)
        clock.start()
        self.addCleanup(clock.stop)
        self.fixture = meaning.MomentMeaningTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.mid, self.roots = self.fixture.captured_moment(started_at=self.now - timedelta(days=2))
        self.fixture.enrich()
        with sqlite3.connect(self.db) as conn:
            self.fixture.conn.backup(conn)
            conn.execute("CREATE TABLE conversations (id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT)")
        journal.ensure_schema(self.db)
        source_store.ensure_schema(self.db)
        relay.ensure_schema(self.db)
        relay.bootstrap_cursor(self.db, 1, 20)
        self.controls = control_snapshot(
            observed_at=self.now.isoformat(), fresh_until=(self.now + timedelta(seconds=120)).isoformat())
        for target, value in (("DB_FILE", self.db), ("BNL_WEBSITE_CONTRACT_VERSION", "2"),
                              ("BNL_STATUS_URL", "https://site.test"), ("BNL_API_KEY", "test-key")):
            patch = mock.patch.object(bot, target, value)
            patch.start()
            self.addCleanup(patch.stop)
        for target, value in (("get_bnl_control_flags", {"websiteRelayEnabled": True}),
                              ("_journal_publication_control_snapshot_sync", (self.controls, "valid"))):
            patch = mock.patch.object(bot, target, return_value=value)
            patch.start()
            self.addCleanup(patch.stop)
        self.reset_process()
        self.addCleanup(self.reset_process)

    def reset_process(self):
        for cache in (bot._recent_relay_messages, bot._recent_relay_topics,
                      bot._recent_relay_lanes_by_guild, bot._recent_relay_sources_by_guild,
                      bot._last_relay_lane_by_guild, bot._website_relay_transaction_locks_by_guild,
                      bot._website_relay_generation_tasks_by_guild):
            cache.clear()

    def add_journal(self, **kwargs):
        with sqlite3.connect(self.db) as conn:
            fixture = publications.PublicationReadAdapterTests()
            fixture.conn = conn
            fixture.add_journal("journal-relay-001", title="Reporters and Receivers",
                excerpt="The Journal revisited the reporters' playful verification debate.",
                body="The earlier conversation treated reporters as unusually diligent journalists.",
                published_at=(self.now - timedelta(days=1)).isoformat(), **kwargs)

    def add_show(self):
        # Keep this production-shaped archived show inside the editorial window
        # on future test dates, preserving every original event's spacing.
        shift = (self.now - timedelta(days=3)) - datetime(2026, 8, 29, tzinfo=timezone.utc)
        def move(value):
            if isinstance(value, dict):
                return {key: move(item) for key, item in value.items()}
            if isinstance(value, list):
                return [move(item) for item in value]
            if isinstance(value, str) and value.startswith(("2026-08-28", "2026-08-29")):
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if len(value) == 10:
                    return (parsed + shift).date().isoformat()
                return (parsed + shift).isoformat().replace("+00:00", "Z")
            return value
        result = shows.sync_tiktok_show_evidence_ledgers(self.db, guild_id=1,
            read_model=show_fixture.authorized_read_model({
                "currentShow": None, "latestShow": move(show_fixture.archived_show()), "shows": [],
            }), environ=show_fixture.ENABLED_QUEUE_ENV)
        self.assertEqual(result["showsWritten"], 1)

    def sources(self, topic=""):
        with sqlite3.connect(Path(self.db).as_uri() + "?mode=ro", uri=True) as conn:
            conn.execute("PRAGMA query_only=ON")
            return relay.select_shared_relay_sources_on_connection(conn, guild_id=1,
                topic_text=topic, control_snapshot=self.controls, now=self.now.isoformat(),
                source_cursor=20, highest=20)

    def only_source(self, kind):
        selected = next(item for item in self.sources() if item.source_class == kind)
        return mock.patch.object(bot, "_select_approved_quiet_relay_source", return_value=selected)

    def transaction(self, generator=None, opener=None):
        with mock.patch.object(bot, "get_gemini_response", side_effect=generator or (lambda *a, **k: COPY)), \
             mock.patch("urllib.request.urlopen", side_effect=opener or self.accept):
            return asyncio.run(bot._execute_website_relay_transaction(1))

    @staticmethod
    def accept(req, timeout=10):
        return Resp(200, accepted_body(json.loads(req.data)))

    def fail_delivery(self, req, timeout=10):
        raise urllib.error.URLError("lost response")

    def test_real_readers_supply_three_distinct_historical_sources_without_writes(self):
        self.add_journal()
        self.add_show()
        selected = self.sources()
        self.assertEqual({item.source_class for item in selected},
                         {"public_moment", "finalized_show", "published_journal"})
        moment = next(item for item in selected if item.source_class == "public_moment")
        self.assertIn("playfully", moment.context)
        self.assertIn("participant_1", moment.context)
        self.assertNotIn("discord_user:", moment.context)
        self.assertNotIn("Test Member 1", moment.context)
        self.assertEqual(moment.source_cursor, 20)
        self.assertEqual(moment.source_conversation_ids, [])
        lineage = moment.metadata["shared_source_provenance"][0]
        self.assertIn(self.roots[0], [row["ledgerEntryId"] for row in lineage["originalSourceRefs"]])
        self.assertIn("First Signal", next(item.context for item in selected if item.source_class == "finalized_show"))
        self.assertIn("publication", next(item.context for item in selected if item.source_class == "published_journal").lower())

    def test_selection_excludes_wrong_guild_sealed_future_and_unrelated_moments(self):
        self.assertEqual(self.sources("sourdough custard recipe"), ())
        for change in ("guild_id=99", "channel_policy='sealed_test'", "public_usable=0",
                       "last_activity_at='2099-01-01T00:00:00Z'"):
            with self.subTest(change=change), sqlite3.connect(self.db) as conn:
                original = conn.execute("SELECT guild_id,channel_policy,public_usable,last_activity_at FROM memory_moment_windows WHERE moment_id=?", (self.mid,)).fetchone()
                conn.execute("UPDATE memory_moment_windows SET " + change)
                conn.commit()
                self.assertFalse(self.sources())
                conn.execute("UPDATE memory_moment_windows SET guild_id=?,channel_policy=?,public_usable=?,last_activity_at=? WHERE moment_id=?", (*original, self.mid))

    def test_journal_drafts_hidden_and_reuse_excluded_entries_are_ineligible(self):
        self.add_journal(lifecycle="draft")
        self.assertFalse(any(item.source_class == "published_journal" for item in self.sources()))
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='published'")
        for field in ("public_excluded", "memory_excluded"):
            self.controls = control_snapshot(**{field: ("journal-relay-001",)},
                observed_at=self.now.isoformat(), fresh_until=(self.now + timedelta(seconds=120)).isoformat())
            self.assertFalse(any(item.source_class == "published_journal" for item in self.sources()))
            self.assertFalse(any(item.source_class == "published_journal" for item in
                self.sources("What did Journal journal-relay-001 say about reporters?")))

    def test_actual_selector_and_writer_use_moment_and_preserve_lineage_on_acceptance(self):
        prompts = []
        async def generator(prompt, **kwargs):
            prompts.append(prompt)
            return COPY
        result = self.transaction(generator)
        self.assertTrue(result.publish, result)
        self.assertEqual(result.eventType, "public_moment")
        self.assertIn("playfully", prompts[0])
        self.assertEqual(relay.get_cursor(self.db, 1), 20)
        saved = json.loads(relay.recent_history(self.db, 1)[0]["source_basis_json"])
        self.assertEqual(saved[0]["sourceId"], self.mid)

    def test_correction_during_generation_blocks_publication_and_pending_save(self):
        async def generator(*args, **kwargs):
            with sqlite3.connect(self.db) as conn:
                conn.execute("UPDATE memory_ledger_entries SET normalized_value='Corrected source.' WHERE entry_id=?", (self.roots[0],))
            return COPY
        opener = mock.Mock(side_effect=AssertionError("outdated source must not leave the bot"))
        result = self.transaction(generator, opener)
        self.assertFalse(result.publish)
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})
        self.assertEqual(relay.get_cursor(self.db, 1), 20)
        opener.assert_not_called()

    def test_restart_retries_exact_saved_envelope_without_regeneration(self):
        result = self.transaction(opener=self.fail_delivery)
        self.assertFalse(result.publish)
        pending = relay.get_pending_v2_publication(self.db, 1)
        self.assertTrue(json.loads(pending["source_basis_json"]))
        self.reset_process()
        sent = []
        def accept(req, timeout=10):
            sent.append(req.data)
            return Resp(200, accepted_body(json.loads(req.data), idempotent=True))
        result = self.transaction(mock.Mock(side_effect=AssertionError("must reuse saved draft")), accept)
        self.assertTrue(result.publish, result)
        self.assertEqual(sent, [pending["canonical_json"].encode()])
        self.assertEqual(result.metadata["accepted_relay_id"], pending["relay_id"])
        self.assertEqual(len(relay.recent_history(self.db, 1)), 1)
        self.assertEqual(relay.get_cursor(self.db, 1), 20)

    def test_retracted_original_after_restart_retires_pending_without_another_generation(self):
        self.transaction(opener=self.fail_delivery)
        self.reset_process()
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?", (self.roots[0],))
        forbidden = mock.Mock(side_effect=AssertionError("no generation or send"))
        result = self.transaction(forbidden, forbidden)
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})
        self.assertEqual(relay.recent_history(self.db, 1), [])

    def test_each_transport_retry_rechecks_the_original_source(self):
        sent = []
        def fail_and_withdraw(req, timeout=10):
            sent.append(req.data)
            with sqlite3.connect(self.db) as conn:
                conn.execute("UPDATE memory_moment_windows SET public_usable=0")
            raise urllib.error.URLError("lost response")
        result = self.transaction(opener=fail_and_withdraw)
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(len(sent), 1)
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})

    def test_historical_sources_cannot_assert_live_state_even_during_regular_show_window(self):
        with mock.patch.object(bot, "_is_relay_show_window_active", return_value=True):
            for text in ("The broadcast is live tonight, with reporters checking the signal.\n" + COPY.splitlines()[1],
                         COPY.splitlines()[0] + "\nCheck the now-playing track while the broadcast is live tonight."):
                with self.subTest(text=text):
                    result = self.transaction(lambda *a, **k: text,
                        mock.Mock(side_effect=AssertionError("historical input cannot prove live state")))
                    self.assertFalse(result.publish)
                    self.assertEqual(result.skipReason, "historical_source_current_claim")

    def test_journal_controls_are_refreshed_after_generation_and_for_pending_replay(self):
        self.add_journal()
        with self.only_source("published_journal"):
            self.transaction(opener=self.fail_delivery)
        hidden = control_snapshot(public_excluded=("journal-relay-001",),
            observed_at=self.now.isoformat(), fresh_until=(self.now + timedelta(seconds=120)).isoformat())
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", return_value=(hidden, "valid")):
            result = self.transaction(mock.Mock(side_effect=AssertionError("no rewrite")),
                mock.Mock(side_effect=AssertionError("hidden Journal must not be posted")))
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})

    def test_control_outage_holds_exact_pending_payload_for_later_revalidation(self):
        self.add_journal()
        with self.only_source("published_journal"):
            self.transaction(opener=self.fail_delivery)
        pending = relay.get_pending_v2_publication(self.db, 1)
        with mock.patch.object(bot, "_journal_publication_control_snapshot_sync", return_value=(None, "timeout")):
            result = self.transaction(opener=mock.Mock(side_effect=AssertionError("no send without authority")))
        self.assertEqual(result.skipReason, "relay_source_unavailable")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), pending)

    def test_forget_removes_pending_derivative_and_scrubs_accepted_private_lineage(self):
        self.transaction()
        with sqlite3.connect(self.db) as conn:
            history = conn.execute("SELECT * FROM website_relay_history").fetchone()
        # Reuse a real saved basis for the privacy derivative, without another model call.
        source = self.sources()[0]
        relay.save_pending_v2_publication(self.db, 1, relay_id="pending-forget", message="Public prose.",
            current_directive="Public inquiry.", source_class="public_safe_memory", trigger="scheduled",
            source_cursor=20, source_conversation_fingerprint="hash", canonical_json="{}",
            source_basis=source.metadata["shared_source_provenance"])
        with sqlite3.connect(self.db) as conn:
            journal.purge_user_journal_derivatives_on_connection(conn, 1, 1)
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})
        accepted = relay.recent_history(self.db, 1)[0]
        self.assertEqual(json.loads(accepted["source_basis_json"]), [])
        self.assertEqual(accepted["public_message"], history[2])

    def test_show_digest_change_after_restart_blocks_saved_relay(self):
        self.add_show()
        with self.only_source("finalized_show"):
            result = self.transaction(opener=self.fail_delivery)
        self.assertEqual(result.skipReason, "website_post_failed")
        self.reset_process()
        with sqlite3.connect(self.db) as conn:
            conn.execute(f"UPDATE {shows.TIKTOK_SHOW_EVIDENCE_TABLE} SET source_digest='changed'")
        result = self.transaction(opener=mock.Mock(side_effect=AssertionError("outdated show must not post")))
        self.assertEqual(result.skipReason, "relay_source_changed")

    def test_journal_hidden_during_generation_cannot_become_pending(self):
        self.add_journal()
        hidden = control_snapshot(memory_excluded=("journal-relay-001",),
            observed_at=self.now.isoformat(), fresh_until=(self.now + timedelta(seconds=120)).isoformat())
        selected = next(item for item in self.sources() if item.source_class == "published_journal")
        with mock.patch.object(bot, "_select_approved_quiet_relay_source", return_value=selected), \
             mock.patch.object(bot, "_journal_publication_control_snapshot_sync", return_value=(hidden, "valid")):
            result = self.transaction(opener=mock.Mock(side_effect=AssertionError("excluded Journal must not post")))
        self.assertEqual(result.skipReason, "relay_source_changed")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})

    def test_new_shared_pending_without_reconstructible_basis_fails_closed(self):
        self.transaction(opener=self.fail_delivery)
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE website_relay_pending_v2 SET source_basis_json='invalid json'")
        result = self.transaction(opener=mock.Mock(side_effect=AssertionError("missing basis must not post")))
        self.assertEqual(result.skipReason, "relay_source_basis_invalid")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})

    def test_startup_confirmation_preserves_lineage_without_reposting(self):
        self.transaction(opener=self.fail_delivery)
        pending = relay.get_pending_v2_publication(self.db, 1)
        envelope = json.loads(pending["canonical_json"])
        body = json.dumps({"contractVersion": 2, "persisted": True,
            "relay": {**envelope["relay"], "contractVersion": 2, "publishedAt": self.now.isoformat()}}).encode()
        requests = []
        def status(req, timeout=10):
            requests.append(req.get_method())
            return Resp(200, body)
        with mock.patch("urllib.request.urlopen", side_effect=status):
            self.assertTrue(bot.hydrate_website_relay_v2(1))
        self.assertEqual(requests, ["GET"])
        self.assertEqual(relay.recent_history(self.db, 1)[0]["source_basis_json"], pending["source_basis_json"])
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), {})

    def test_missing_stores_remain_empty_and_do_not_trigger_schema_writes(self):
        with sqlite3.connect(":memory:") as conn:
            conn.execute("PRAGMA query_only=ON")
            self.assertEqual(relay.select_shared_relay_sources_on_connection(conn, guild_id=1), ())
            self.assertEqual(conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0], 0)

    def test_transient_database_read_failure_keeps_pending_payload(self):
        self.transaction(opener=self.fail_delivery)
        pending = relay.get_pending_v2_publication(self.db, 1)
        with mock.patch.object(moments, "public_moment_source_basis", side_effect=sqlite3.OperationalError("database is locked")):
            result = self.transaction(opener=mock.Mock(side_effect=AssertionError("unverified basis must not send")))
        self.assertEqual(result.skipReason, "relay_source_unavailable")
        self.assertEqual(relay.get_pending_v2_publication(self.db, 1), pending)

    def test_shared_selection_runs_off_loop(self):
        original = bot._select_shared_relay_sources
        release = threading.Event()
        async def run():
            loop = asyncio.get_running_loop()
            started = asyncio.Event()
            def blocked(*args, **kwargs):
                loop.call_soon_threadsafe(started.set)
                if not release.wait(2):
                    raise AssertionError("selection blocked the event loop")
                return original(*args, **kwargs)
            with mock.patch.object(bot, "_select_shared_relay_sources", side_effect=blocked), \
                 mock.patch.object(bot, "get_gemini_response", return_value=COPY):
                task = asyncio.create_task(bot.generate_dynamic_website_relay(1, allow_quiet_sources=True))
                try:
                    await asyncio.wait_for(started.wait(), timeout=1)
                    self.assertFalse(task.done())
                finally:
                    release.set()
                self.assertTrue((await task).publish)
        asyncio.run(run())

    def test_health_reports_source_counts_without_source_text_or_private_lineage(self):
        from scripts.journal_relay_health import inspect
        self.transaction(opener=self.fail_delivery)
        before = Path(self.db).read_bytes()
        report = inspect(self.db, 1)
        self.assertEqual(report["relaySharedInputs"]["pending"]["sourceCounts"], {"public_moment": 1})
        for private in (self.mid, self.roots[0], "discord_user:1", COPY.splitlines()[0]):
            self.assertNotIn(private, json.dumps(report))
        self.assertEqual(Path(self.db).read_bytes(), before)
