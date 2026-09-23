"""Journal consumes the existing public owners and rechecks saved source bases."""
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store
import bnl_moment_engine as moments
import bnl_tiktok_show_ledger as shows
from tests import test_moment_meaning as meaning
from tests import test_tiktok_show_evidence_ledger as show_fixture
from tests.test_bnl_journal_prepared_release import AcceptedResponse, article_json


START = "2026-08-28T01:30:00Z"
END = "2026-08-29T01:30:00Z"


class JournalSharedInputsTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "journal.db")
        clock = mock.patch.object(source_store, "_now_ms", return_value=0)
        clock.start()
        self.addCleanup(clock.stop)
        self.fixture = meaning.MomentMeaningTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.mid, self.roots = self.fixture.captured_moment(
            started_at=datetime(2026, 8, 27, 12, tzinfo=timezone.utc))
        self.fixture.enrich()
        with sqlite3.connect(self.db) as conn:
            self.fixture.conn.backup(conn)
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        source_store.ensure_schema(self.db)

    def add_show(self, show=None):
        result = shows.sync_tiktok_show_evidence_ledgers(
            self.db, guild_id=1,
            read_model=show_fixture.authorized_read_model({
                "currentShow": None, "latestShow": show or show_fixture.archived_show(), "shows": [],
            }), environ=show_fixture.ENABLED_QUEUE_ENV)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["showsWritten"], 1)

    def packet(self, conversations=None, **kwargs):
        if conversations is None:
            conversations = [{
                "refId": "conversation:42", "sourceKind": "conversation",
                "subjectRef": "discord_user:2", "displayName": "Test Member 2",
                "channelPolicy": "public_home", "conversationSurface": "discord",
                "summary": "The journalists and reporters joke returned to the broadcast discussion.",
                "observedAt": "2026-08-28T12:00:00Z",
            }]
        return journal.build_packet_from_sources(
            self.db, 1, START, END, [], conversations, prepare_schema=False, **kwargs)

    def test_relevant_moment_reaches_writer_as_dated_participant_bound_continuity(self):
        packet = self.packet()
        selected = [item for item in packet.get("reflectionBasis", [])
                    if item.get("basisKind") == "public_moment"]
        self.assertEqual(len(selected), 1)
        self.assertIn("2026-08-27", selected[0]["sourceObservedAt"])
        self.assertIn("bantered", selected[0]["summary"])
        self.assertIn("playfully", json.dumps(selected[0]["contributions"]))
        self.assertIn("public_moment", journal.build_generation_prompt(packet))
        historical = selected[0]["contributions"][0]
        current = packet["safeSources"][0]
        self.assertNotEqual(historical["participantAlias"], current["participantAlias"])
        self.assertEqual(historical["publicSpeakerName"], "Test Member 1")
        self.assertEqual(packet["aggregateCounts"]["participants"], 1)

    def test_finalized_show_operations_reach_quiet_window_without_fabricated_chatter(self):
        self.add_show()
        packet = self.packet(conversations=[])
        selected = [item for item in packet["safeSources"] if item["sourceKind"] == "finalized_show"]
        self.assertEqual(len(selected), 1)
        self.assertIn("First Signal", selected[0]["summary"])
        self.assertEqual(packet["aggregateCounts"]["eligibleConversations"], 0)
        self.assertTrue(journal.journal_source_packet_has_meaningful_activity(packet))
        self.assertFalse(packet.get("lowActivityMode", False))
        self.assertEqual(sum(segment.get("finalizedShows", 0) for segment in packet["windowSegmentActivity"]), 1)

    def test_readers_and_revalidators_are_read_only_and_do_not_initialize_empty_stores(self):
        self.add_show()
        packet = self.packet()
        with sqlite3.connect(Path(self.db).as_uri() + "?mode=ro", uri=True) as conn:
            conn.execute("PRAGMA query_only=ON")
            self.assertTrue(journal.journal_shared_source_provenance_is_current(
                conn, 1, packet["privateSharedSourceProvenance"]))
            self.assertTrue(moments.select_public_situation_moment_gists(
                conn, guild_id=1, topic_text="reporters journalists", require_topic_overlap=True,
                allowed_channel_policies=("public_home",), prepare_schema=False))
            self.assertEqual(conn.total_changes, 0)
        with sqlite3.connect(":memory:") as conn:
            conn.execute("PRAGMA query_only=ON")
            self.assertEqual(moments.select_public_situation_moment_gists(
                conn, guild_id=1, topic_text="reporters", prepare_schema=False), ())
            self.assertEqual(conn.execute("SELECT count(*) FROM sqlite_master").fetchone()[0], 0)

    def test_active_journal_keeps_moment_context_without_counting_it_as_fresh(self):
        conversation = self.packet()["privateSources"][0]
        packet = self.packet(conversations=[{**conversation, "refId": f"conversation:{i}"} for i in range(6)])
        self.assertFalse(packet.get("lowActivityMode", False))
        self.assertEqual(len(packet["safeSources"]), 6)
        self.assertEqual(len(packet["reflectionBasis"]), 1)
        self.assertIn("public_moment", journal.build_generation_prompt(packet))
        self.assertEqual(packet["aggregateCounts"]["eligibleConversations"], 6)

    def test_owner_label_uses_existing_governed_name_projection(self):
        with mock.patch.dict(os.environ, {"BNL_OWNER_USER_ID": "1"}):
            packet = self.packet()
        item = next(item for item in packet["reflectionBasis"] if item["basisKind"] == "public_moment")
        self.assertEqual(item["contributions"][0]["publicSpeakerName"], "6 Bit")
        self.assertNotIn("discord_user:", journal.build_generation_prompt(packet))

    def test_historical_moment_cannot_replace_required_current_evidence(self):
        conversation = self.packet()["privateSources"][0]
        packet = self.packet(conversations=[{**conversation, "refId": f"conversation:{i}"} for i in range(6)])
        article = self.article_with_moment(packet)
        article["sections"].append({"heading": "Earlier", "body": "Tonight the reporters debate returned."})
        article["sourceRefIds"]["Earlier"] = [packet["reflectionBasis"][0]["refId"]]
        self.assertEqual(journal.validate_article(article, packet, []), "current_activity_without_fresh_source")

    def test_new_topic_future_moment_and_sealed_moment_are_not_editorial_continuity(self):
        for update in (
            "UPDATE memory_moment_windows SET last_activity_at='2026-09-01T12:00:00Z'",
            "UPDATE memory_moment_windows SET channel_policy='sealed_test'",
        ):
            with self.subTest(update=update), sqlite3.connect(self.db) as conn:
                conn.execute("SAVEPOINT fixture")
                conn.execute(update)
                operations, selected, _ = journal._journal_shared_inputs(conn, 1, START, END, self.packet()["safeSources"])
                self.assertEqual(selected, [])
                conn.execute("ROLLBACK TO fixture")
        packet = self.packet(conversations=[{
            "refId": "conversation:99", "sourceKind": "conversation",
            "summary": "Cinnamon custard baking recipe and sourdough starter measurements.",
            "observedAt": END,
        }])
        self.assertFalse(any(item.get("basisKind") == "public_moment" for item in packet["reflectionBasis"]))

    def test_show_window_is_completion_time_not_sync_time_and_rejects_unfinalized_or_unsigned(self):
        self.add_show()
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(shows.select_finalized_show_operations(conn, guild_id=1,
                source_window_ms=(show_fixture.stamp(END), show_fixture.stamp("2026-08-30T01:30:00Z"))), ())
            for update in (
                "UPDATE tiktok_show_evidence_ledgers SET lifecycle_status='provisional'",
                "UPDATE tiktok_show_evidence_ledgers SET source_digest='invalid'",
            ):
                with self.subTest(update=update):
                    conn.execute("SAVEPOINT fixture")
                    conn.execute(update.replace("tiktok_show_evidence_ledgers", shows.TIKTOK_SHOW_EVIDENCE_TABLE))
                    self.assertEqual(shows.select_finalized_show_operations(conn, guild_id=1,
                        source_window_ms=(show_fixture.stamp(START), show_fixture.stamp(END))), ())
                    conn.execute("ROLLBACK TO fixture")

    def test_frozen_packet_revalidates_original_source_after_restart(self):
        packet = {**self.packet(), "sourceArchiveAvailable": True}
        status, run_id, epoch, _ = automation._claim_preparation(self.db, 1, "daily", START, END, force=True)
        self.assertEqual(status, "claimed")
        saved, _, reason = automation._freeze_or_load_packet(self.db, 1, run_id, epoch, lambda: packet)
        self.assertEqual(reason, "")
        self.assertTrue(saved["privateSharedSourceProvenance"])
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?", (self.roots[0],))
        saved, _, reason = automation._freeze_or_load_packet(self.db, 1, run_id, epoch,
            lambda: self.fail("A stale frozen packet must be retired before rebuilding"))
        self.assertIsNone(saved)
        self.assertEqual(reason, "privacy_source_ineligible")
        with sqlite3.connect(self.db) as conn:
            self.assertIsNone(conn.execute("SELECT frozen_packet_json FROM bnl_journal_automation_runs WHERE run_id=?", (run_id,)).fetchone()[0])

    def test_correction_during_generation_cannot_become_a_saved_draft(self):
        packet = self.packet()
        def generator(value, _prompt):
            with sqlite3.connect(self.db) as conn:
                conn.execute("UPDATE memory_ledger_entries SET normalized_value='The earlier source was corrected.' WHERE entry_id=?", (self.roots[0],))
            return article_json(value)
        result = journal.generate_and_store_packet_draft(self.db, 1, packet, generator)
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "privacy_source_ineligible")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM bnl_journal_entries").fetchone()[0], 0)

    def article_with_moment(self, packet):
        value = json.loads(article_json(packet))
        source = next(item for item in packet["reflectionBasis"] if item["basisKind"] == "public_moment")
        value["sections"][0]["sourceRefIds"].append(source["refId"])
        value["sections"][0]["body"] = "In the earlier exchange, a member playfully endorsed the reporters. " + value["sections"][0]["body"]
        return journal.parse_generated_json(json.dumps(value))

    def test_saved_moment_lineage_survives_reopen_and_retraction_blocks_manual_delivery(self):
        packet = self.packet()
        result = journal.store_validated_draft(self.db, 1, packet, self.article_with_moment(packet))
        self.assertTrue(result.ok, result)
        self.assertTrue(journal.approve_draft(self.db, 1, result.entry_id, result.content_hash).ok)
        with sqlite3.connect(self.db) as conn:
            meta = json.loads(conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0])
            basis = meta["usedSharedSourceProvenance"][0]
            self.assertIn(self.roots[0], [row["ledgerEntryId"] for row in basis["originalSourceRefs"]])
            self.assertIn("discord_user:1", basis["subjectRefs"])
            self.assertEqual(automation._prepared_invalidation_reason(conn, 1, meta, set()), "")
            conn.execute("UPDATE memory_moment_windows SET public_usable=0 WHERE moment_id=?", (self.mid,))
        opener = mock.Mock(side_effect=AssertionError("ineligible source must not be sent"))
        sent = journal.deliver_approved(self.db, 1, result.entry_id, "https://site.example", "key", opener=opener)
        self.assertFalse(sent.ok)
        self.assertEqual(sent.reason, "privacy_source_ineligible")
        opener.assert_not_called()

    def test_uncited_candidate_is_still_revalidated_and_forget_scrubs_published_private_basis(self):
        packet = self.packet()
        article = journal.parse_generated_json(article_json(packet))
        result = journal.store_validated_draft(self.db, 1, packet, article)
        self.assertTrue(result.ok, result)
        with sqlite3.connect(self.db) as conn:
            meta = json.loads(conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0])
            self.assertEqual(meta["usedSharedSourceProvenance"], [])
            self.assertTrue(meta["sharedInputSourceProvenance"])
            conn.execute("UPDATE memory_ledger_entries SET public_usable=0 WHERE entry_id=?", (self.roots[0],))
            self.assertEqual(automation._prepared_invalidation_reason(conn, 1, meta, set()), "privacy_source_ineligible")
            # Existing published-public retention policy is unchanged. Its
            # private memory/candidate metadata must not retain a forgotten member.
            conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='published'")
            conn.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='published'")
            journal.purge_user_journal_derivatives_on_connection(conn, 1, 1)
            scrubbed = json.loads(conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0])
            self.assertEqual(scrubbed["sharedInputSourceProvenance"], [])
            self.assertNotIn("discord_user:1", json.dumps(scrubbed))

    def test_scheduled_show_revision_survives_restart_and_posts_exact_bytes_once(self):
        self.add_show()
        prepared = automation.prepare_daily(self.db, 1, lambda packet, prompt: article_json(packet),
            target_day=date(2026, 8, 27), force=True)
        self.assertEqual(prepared.status, "prepared", prepared)
        with sqlite3.connect(self.db) as conn:
            canonical = bytes(conn.execute("SELECT canonical_payload_bytes FROM bnl_journal_entries").fetchone()[0])
            meta = json.loads(conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0])
            self.assertEqual(meta["usedSharedSourceProvenance"][0]["sourceKind"], "finalized_show")
        calls = []
        def opener(request, timeout=10):
            calls.append(request.data)
            return AcceptedResponse(request)
        for _ in range(2):
            result = automation.release_daily(self.db, 1, "https://site.example", "key",
                target_day=date(2026, 8, 27), force=True, opener=opener)
            self.assertTrue(result.ok, result)
        self.assertEqual(calls, [canonical])

    def test_weekly_periods_keep_show_evidence_in_its_actual_period(self):
        self.add_show()
        start, end, _ = automation._weekly_period_for_monday(date(2026, 8, 24))
        packet, _, _ = automation._weekly_packet(self.db, 1, start, end)
        show_refs = {item["refId"] for item in packet["safeSources"] if item["sourceKind"] == "finalized_show"}
        self.assertEqual(len(show_refs), 1)
        periods = packet["weeklyDailyPeriodContexts"] + [packet["weeklyFinalPeriodContext"]]
        matching = [item for item in periods if show_refs.intersection(item["sourceRefIds"])]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]["counts"]["finalizedShows"], 1)
        self.assertEqual(matching[0]["counts"]["eligibleConversations"], 0)
        self.assertTrue(start <= matching[0]["sourceWindowStart"] < end)

    def test_show_revised_after_prepare_retires_packet_and_keeps_occurrence_owed(self):
        self.add_show()
        prepared = automation.prepare_daily(self.db, 1, lambda packet, prompt: article_json(packet),
            target_day=date(2026, 8, 27), force=True)
        self.assertEqual(prepared.status, "prepared", prepared)
        revised = show_fixture.archived_show()
        revised["milestones"][2]["eventType"] = "track_skipped"
        self.add_show(revised)
        opener = mock.Mock(side_effect=AssertionError("changed show must not be sent"))
        result = automation.release_daily(self.db, 1, "https://site.example", "key",
            target_day=date(2026, 8, 27), force=True, opener=opener)
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "privacy_source_ineligible")
        opener.assert_not_called()
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT lifecycle_state,journal_entry_id,frozen_packet_json FROM bnl_journal_automation_runs").fetchone(), ("held", None, None))


if __name__ == "__main__":
    unittest.main()
