"""Keep full show evidence while bounding repeated source work."""

import asyncio
import sqlite3
import threading
import unittest
from dataclasses import replace
from unittest import mock

import test_show_preparation_awareness as preparation
from bnl_journal_source_store import record_source_event
import bnl_tiktok_show_ledger as shows
import bnl_unified_intelligence_packet as packets
import bnl01_bot as bot
import bnl_shared_brain_synthesis as synthesis


class ShowEvidenceThroughputTests(unittest.TestCase):
    def setUp(self):
        self.fixture = preparation.ShowPreparationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def test_original_identity_checks_do_not_query_once_per_journal_copy(self):
        f = self.fixture
        f.add_discord(2001, "An original private message.", policy="sealed_test")
        f.add_discord(2002, "A private row with a malformed legacy identity.", policy="sealed_test")
        with sqlite3.connect(f.db) as conn:
            conn.execute("UPDATE conversations SET message_id='invalid-id' WHERE id=2002")
        for index in range(80):
            result = record_source_event(
                f.db, guild_id=77, source_kind="discord_message",
                source_key=str(30000 + index),
                occurred_at_ms=preparation.stamp("2026-08-28T23:45:00Z"),
                raw_text="Public retained source %s." % index,
                subject_ref="discord_user:901", private_display_name="Test Member",
                channel_policy="public_home", public_usable=True,
                metadata={"conversationRowId": 2001 if index == 0 else 40000 + index,
                          "messageId": 12001 if index in (0, 1) else 30000 + index},
            )
            self.assertTrue(result.ok)
        statements = []
        with sqlite3.connect(f.db) as conn:
            conn.set_trace_callback(statements.append)
            records, _coverage = shows._load_show_related_sources(conn, guild_id=77)
        self.assertNotIn("Public retained source 0.", [r["text"] for r in records])
        self.assertNotIn("Public retained source 1.", [r["text"] for r in records])
        self.assertIn("Public retained source 79.", [r["text"] for r in records])
        identity_queries = [s for s in statements if "FROM conversations" in s]
        self.assertLessEqual(len(identity_queries), 2, identity_queries[:5])

    def test_one_show_selection_per_validation_snapshot_and_fresh_next_time(self):
        f = self.fixture
        f.add_discord(2001, "For the August 28, 2026 BARCODE Radio show the cable check passed.")
        f.add_tiktok("prep-throughput", "The blue curtains are ready.")
        f.sync()
        with sqlite3.connect(f.db) as conn:
            request = replace(f.request(), user_text=(
                "Give me the August 28, 2026 BARCODE Radio show timeline and "
                "preparation, including TikTok and Discord chat during the show."))
            packet = packets.build_packet(conn, request, environ=preparation.PACKET_ENV)
            self.assertGreaterEqual(sum(i.lane == "show_episode" for i in packet.items), 2)
            with mock.patch.object(shows, "select_tiktok_show_episode_context_items",
                                   wraps=shows.select_tiktok_show_episode_context_items) as select:
                self.assertTrue(packets.revalidate_packet(conn, packet, environ=preparation.PACKET_ENV).valid)
                self.assertEqual(select.call_count, 1)
                conn.execute("UPDATE conversations SET content='The check is unresolved.' WHERE id=2001")
                conn.commit()
                self.assertFalse(packets.revalidate_packet(conn, packet, environ=preparation.PACKET_ENV).valid)
                self.assertEqual(select.call_count, 2)

    def test_saved_chat_backfill_joins_existing_show_without_live_fetch_or_resync(self):
        f = self.fixture
        f.sync()
        parent = f.parent()
        text = "For the August 28, 2026 BARCODE Radio show the spare mixer was reserved."
        f.add_discord(2001, text)
        f.add_tiktok("late-stored-preparation", "The blue curtains are ready.")
        with mock.patch.object(bot, "fetch_bnl_read_model", side_effect=AssertionError("live fetch forbidden")):
            context = shows.build_tiktok_show_evidence_context(
                f.db, guild_id=77, user_text=preparation.QUERY,
            )
            with sqlite3.connect(f.db) as conn:
                packet = packets.build_packet(conn, f.request(), environ=preparation.PACKET_ENV)
                rendered = synthesis.render_packet_context(packet)[0]
                self.assertTrue(packets.revalidate_packet(conn, packet, environ=preparation.PACKET_ENV).valid)
        for view in (context, rendered):
            self.assertIn(text, view)
            self.assertIn("The blue curtains are ready.", view)
        # The reader composed context from the existing show and original rows;
        # it did not create or rewrite another show record.
        self.assertEqual(f.parent(), parent)

    def test_general_show_recall_reads_unpaired_discord_and_rechecks_originals(self):
        f = self.fixture
        f.sync()
        parent = f.parent()
        query = "Recap the August 28, 2026 BARCODE Radio show."
        original = "The courtyard lanterns flickered during the chorus."
        edited = "The courtyard lanterns stayed steady during the chorus."
        f.add_discord(2001, original, at="2026-08-29T00:02:00Z")
        f.add_discord(2002, "Private rehearsal sentinel.", at="2026-08-29T00:02:00Z", policy="sealed_test")
        f.add_discord(2003, "Outside the show sentinel.", at="2026-08-29T12:00:00Z")
        with sqlite3.connect(f.db) as conn:
            request = replace(f.request(), user_text=query)
            packet = packets.build_packet(conn, request, environ=preparation.PACKET_ENV)
            rendered = synthesis.render_packet_context(packet)[0]
            full = shows.build_tiktok_show_evidence_context(f.db, guild_id=77, user_text=query)
            for view in (full, rendered):
                self.assertIn(original, view)
                self.assertNotIn("Private rehearsal sentinel", view)
                self.assertNotIn("Outside the show sentinel", view)
            line = next(line for line in full.splitlines() if original in line)
            self.assertIn('track association="Neon Fox — First Signal"', line)
            self.assertTrue(packets.revalidate_packet(conn, packet, environ=preparation.PACKET_ENV).valid)
            conn.execute("UPDATE conversations SET content=? WHERE id=2001", (edited,))
            conn.commit()
            self.assertFalse(packets.revalidate_packet(conn, packet, environ=preparation.PACKET_ENV).valid)
            refreshed = packets.build_packet(conn, request, environ=preparation.PACKET_ENV)
            self.assertIn(edited, synthesis.render_packet_context(refreshed)[0])
            self.assertNotIn(original, synthesis.render_packet_context(refreshed)[0])
            conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=2001")
            conn.commit()
            self.assertFalse(packets.revalidate_packet(conn, refreshed, environ=preparation.PACKET_ENV).valid)
            for view in (
                shows.build_tiktok_show_evidence_context(f.db, guild_id=77, user_text=query),
                synthesis.render_packet_context(packets.build_packet(conn, request, environ=preparation.PACKET_ENV))[0],
            ):
                self.assertNotIn(edited, view)
                self.assertNotIn(original, view)
        self.assertEqual(f.parent(), parent)

    def test_general_recall_keeps_discord_visible_among_many_tiktok_comments(self):
        from test_cross_source_show_recall import CrossSourceShowRecallTests
        f = CrossSourceShowRecallTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        date = f.add_show()
        for index in range(20):
            f.message(date, "The show chorus is strong, comment %s." % index)
        f.sync()
        text = "The courtyard lanterns flickered during the chorus."
        with sqlite3.connect(f.db) as conn:
            conn.execute("""INSERT INTO conversations VALUES
                (2001,901,'Test Technician',77,'user',?,'2026-08-29T00:02:30Z',
                 'public_context','show-room',9001,'normal_chat',12001)""", (text,))
        query = "Recap the last show."
        full = shows.build_tiktok_show_evidence_context(f.db, guild_id=77, user_text=query)
        with sqlite3.connect(f.db) as conn:
            compact = "\n".join(item.text for item in shows.select_tiktok_show_episode_context_items(
                conn, guild_id=77, user_text=query,
            ))
        for view in (full, compact):
            self.assertIn(text, view)
            self.assertIn("The show chorus is strong", view)


class SourceFenceResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_final_source_read_yields_to_discord_and_retains_invalidation(self):
        entered, release = threading.Event(), threading.Event()
        def read(*args, **kwargs):
            entered.set()
            release.wait(2)
            return "show_episode_source_changed"
        with mock.patch.object(bot, "prompt_source_basis_failure", side_effect=read):
            task = asyncio.create_task(bot.prompt_source_basis_failure_async(()))
            try:
                self.assertTrue(await asyncio.wait_for(asyncio.to_thread(entered.wait, 1), 1.5))
                self.assertFalse(task.done())
                release.set()
                self.assertEqual(await task, "show_episode_source_changed")
            finally:
                release.set()
