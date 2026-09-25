import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as archive
import bnl_website_relay_state as relay


START = "2026-09-24T01:30:00Z"
END = "2026-09-25T01:30:00Z"


class QuietJournalTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = str(Path(self.directory.name) / "journal.db")
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        archive.ensure_schema(self.db)

    def packet(self):
        return journal.build_source_packet_between(self.db, 1, START, END)

    def callbacks(self):
        kinds = ["finalized_show", "conversation_continuity", "published_ballad",
                 "canon", "finalized_show", "conversation_continuity", "published_journal",
                 "published_ballad", "canon", "finalized_show", "conversation_continuity",
                 "published_journal", "published_ballad", "canon"]
        for index, kind in enumerate(kinds):
            basis = [{"sourceKind":kind,"sourceId":"earlier-record","sourceVersion":"version-1"}]
            if kind == "published_journal":
                basis[0].update(sourceWindowStart="2026-09-23T01:30:00Z",
                                sourceWindowEnd=START, publishedAt="2026-09-24T02:00:00Z")
            elif kind == "published_ballad":
                basis[0]["publishedAt"] = "2026-09-19T09:27:20Z"
            elif kind == "finalized_show":
                basis[0].update(sourceWindowStart="2026-08-25T02:10:00Z",
                                sourceWindowEnd="2026-09-24T02:10:00Z",
                                observedAt="2026-09-19T07:00:00Z", showDates=["2026-09-18"])
            else:
                basis = []
            relay.record_publication(
                self.db, 1, message="An earlier public record recalls a finished musical and a broadcast.",
                directive="Consider how that older story connects to music.", mode="OBSERVATION",
                relay_lane="residual_echo", event_type=kind, source_cursor=0,
                published_timestamp=f"2026-09-24T{index+2:02}:10:00Z",
                relay_id=f"callback-{index}", source_basis=basis,
            )

    def article(self, packet, body, *, refs=None):
        return {
            "title":"The Queue in My Imagination", "excerpt":"A thought about music and anticipation.",
            "sections":[{"heading":"A Thought Between Songs","body":body}],
            "sourceRefIds":{"A Thought Between Songs":refs or [packet["reflectionBasis"][0]["refId"]]},
            "metadata":{"contextUses":[]},
        }

    def test_fourteen_callbacks_do_not_manufacture_an_active_day(self):
        self.callbacks()
        packet = self.packet()
        self.assertEqual(packet["aggregateCounts"]["eligibleRelays"], 14)
        self.assertEqual(packet["aggregateCounts"]["currentActivityRelays"], 0)
        self.assertEqual(packet["aggregateCounts"]["eligibleConversations"], 0)
        self.assertEqual(packet["safeSources"], [])
        self.assertTrue(packet["lowActivityMode"])
        self.assertTrue(packet["creativeReflectionAllowed"])
        self.assertFalse(journal.journal_source_packet_has_meaningful_activity(packet))
        self.assertEqual(sum(s["relaySources"] for s in packet["windowSegmentActivity"]), 0)
        self.assertTrue(all(s["refId"].startswith("reflection:") for s in packet["reflectionBasis"]))
        self.assertLessEqual(packet["aggregateCounts"]["reflectionRelays"], journal.MAX_REFLECTION_SOURCE_CONTEXT)

    def test_origin_dates_stay_distinct_from_relay_publication_and_show_lookback(self):
        self.callbacks()
        packet = self.packet()
        callbacks = [s for s in packet["reflectionBasis"] if s.get("relayTopicKind")]
        by_kind = {s["relayTopicKind"]:s for s in callbacks}
        original = by_kind["published_journal"]["originalSourceDates"][0]
        self.assertEqual(original["sourceWindowEnd"], START)
        self.assertNotEqual(original["publishedAt"], by_kind["published_journal"]["relayPublishedAt"])
        show = by_kind["finalized_show"]["originalSourceDates"][0]
        self.assertEqual(show["showDates"], ["2026-09-18"])
        self.assertNotIn("sourceWindowStart", show)
        self.assertNotIn("sourceId", show)
        self.assertEqual(by_kind["conversation_continuity"]["originalSourceDates"], [])
        saved = packet["privateReflectionBasisProvenance"]["historicalSourceEvents"]
        self.assertTrue(any(p.get("originalSources") for p in saved))

    def test_personal_thoughts_and_clearly_imagined_scenes_validate(self):
        packet = self.packet()
        for body in [
            "Tonight I imagine the queue as a hallway whose doors hum in different keys.",
            "I wonder whether the skip wheel dreams in green. In my head, the archive keeps a spare moon.",
            "I think anticipation is its own instrument. I feel fond of the space between songs tonight.",
            "What if the queue grew legs tonight and wandered into an imaginary record shop?",
            "The earlier broadcast remains a memory. Tonight I picture its chorus folded into a paper bird.",
        ]:
            with self.subTest(body=body):
                self.assertEqual(journal.validate_article(self.article(packet, body), packet), "")

    def test_historical_callbacks_cannot_support_current_activity(self):
        self.callbacks()
        packet = self.packet()
        callback = next(s for s in packet["reflectionBasis"] if s.get("relayTopicKind") == "published_journal")
        for body in [
            "Today Test Member submitted two songs.",
            "Fresh public inquiries arrived alongside an earlier archive entry.",
            "I wonder why Test Member released a musical today.",
            "I think Test Member is releasing an album tonight.",
            "I imagine an archive with wings. Today Test Member posted a new track.",
            "I imagine a silent stage, but today Test Member performed there.",
        ]:
            with self.subTest(body=body):
                article = self.article(packet, body, refs=[callback["refId"]])
                self.assertEqual(journal.validate_article(article, packet), "current_activity_without_fresh_source")
        article = self.article(packet, "An earlier record recalls a musical.")
        article["excerpt"] = "Today a creator submitted new music."
        self.assertEqual(journal.validate_article(article, packet), "current_activity_without_fresh_source")

    def test_real_current_conversation_still_supports_activity_in_a_reflective_entry(self):
        self.callbacks()
        archive.record_source_event(
            self.db, guild_id=1, source_kind="discord_message", source_key="current-message",
            occurred_at_ms=archive.timestamp_to_epoch_ms("2026-09-24T18:00:00Z"),
            raw_text="I submitted a new track today.", channel_policy="public_home", public_usable=True,
        )
        packet = self.packet()
        self.assertEqual(len(packet["safeSources"]), 1)
        source = packet["safeSources"][0]
        article = self.article(packet, "Today a community member submitted a new track.", refs=[source["refId"]])
        self.assertEqual(journal.validate_article(article, packet), "")

    def test_imagination_does_not_authorize_separate_factual_clauses(self):
        packet = self.packet()
        for body in [
            "I imagine a silent stage, and today a producer performed there.",
            "Today a producer performed there, and I imagine a silent stage.",
            "I imagine a stage and today a producer performed there.",
            "In my head the queue glows; tonight Test Member submitted a song.",
            "I picture an archive because fresh public inquiries arrived.",
            "I imagine a stage, and I think Test Member released another recording.",
        ]:
            with self.subTest(body=body):
                result = journal.validate_article(self.article(packet, body), packet)
                self.assertIn(result, {"current_activity_without_fresh_source", "undeclared_context_use"})
                article = self.article(packet, "I imagine a hallway of humming doors.")
                article["excerpt"] = body
                self.assertIn(journal.validate_article(article, packet),
                              {"current_activity_without_fresh_source", "undeclared_context_use"})
        for body in [
            "Tonight I imagine a producer performing on a silent stage.",
            "I imagine a stage, and in my head tonight a paper bird performs there.",
            "I think anticipation is an instrument, and I feel fond of the space between songs tonight.",
        ]:
            with self.subTest(creative_body=body):
                self.assertEqual(journal.validate_article(self.article(packet, body), packet), "")

    def test_legacy_callback_packet_is_retired_but_current_packet_and_real_activity_are_retained(self):
        old = {"safeSources":[{"refId":"fresh:1","sourceKind":"relay","eventType":"published_journal"}]}
        self.assertTrue(journal.journal_packet_needs_reflection_refresh(old))
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(automation._frozen_packet_invalidation_reason(conn, 1, old), "journal_reflection_contract_changed")
        live = copy.deepcopy(old)
        live["safeSources"][0]["eventType"] = "fresh_public_discord_activity"
        self.assertFalse(journal.journal_packet_needs_reflection_refresh(live))
        self.assertFalse(journal.journal_packet_needs_reflection_refresh(self.packet()))

    def test_saved_metadata_preserves_reflection_dates_and_stale_prepared_work_is_detected(self):
        self.callbacks()
        packet = self.packet()
        source = next(s for s in packet["reflectionBasis"] if s.get("relayTopicKind") == "published_journal")
        article = self.article(packet, "An earlier Journal stays in its own day. Tonight I imagine the archive growing wings.", refs=[source["refId"]])
        result = journal.store_validated_draft(self.db, 1, packet, article)
        self.assertTrue(result.ok, result)
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0])
            self.assertEqual(metadata["reflectionVersion"], journal.JOURNAL_REFLECTION_VERSION)
            self.assertTrue(metadata["creativeReflectionAllowed"])
            self.assertEqual(metadata["usedReflectionBasis"][0]["originalSourceDates"][0]["sourceWindowEnd"], START)
            self.assertFalse(journal.journal_metadata_needs_reflection_refresh(conn, 1, metadata))
            old = {k:v for k,v in metadata.items() if k != "reflectionVersion"}
            self.assertEqual(automation._prepared_invalidation_reason(conn, 1, old, set()), "journal_reflection_contract_changed")

    def test_callback_only_day_still_prepares_once_with_creative_output(self):
        self.callbacks()
        packet = self.packet()
        packet["coverageComplete"] = True
        calls = []
        def writer(source_packet, prompt):
            calls.append(prompt)
            article = self.article(source_packet, "Tonight I imagine the queue as a hallway of humming doors.")
            for section in article["sections"]:
                section["sourceRefIds"] = article["sourceRefIds"][section["heading"]]
            return json.dumps(article)
        with patch.object(automation, "build_source_packet_between", return_value=packet):
            first = automation._prepare_daily_window(self.db, 1, writer, START, END, "2026-09-23", force=True)
            second = automation._prepare_daily_window(self.db, 1, writer, START, END, "2026-09-23")
        self.assertEqual(first.status, "prepared", first)
        self.assertEqual(second.status, "prepared", second)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
