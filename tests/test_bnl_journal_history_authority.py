"""Past BNL expression is dated continuity, never future or independent evidence."""

import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

import bnl_journal as journal
import bnl_journal_automation as automation


START = "2026-09-30T23:48:43Z"
END = "2026-10-01T23:48:43Z"


class JournalHistoryAuthorityTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "journal.db")
        journal.ensure_schema(self.db)
        self.packet = {
            "sourceWindowStart": START,
            "sourceWindowEnd": END,
            "safeSources": [{"refId": "conversation:7", "summary": "An unresolved chorus question."}],
            "privateSources": [{"subjectRef": "discord_user:71"}],
            "candidateTopicTags": ["chorus"],
        }

    def add_entry(
        self,
        entry_id,
        *,
        published="2026-09-29T02:00:00Z",
        created="2026-09-29T01:30:00Z",
        window_start="2026-09-27T23:00:00Z",
        window_end="2026-09-28T23:00:00Z",
        state="published",
        metadata_state="published",
        guild_id=1,
        revision=1,
        body="I still remember how an unanswered chorus question held my attention.",
    ):
        metadata = {
            "topicTags": ["chorus", entry_id],
            "subjectRefs": ["discord_user:71"],
            "continuityNotes": ["continuity-" + entry_id],
            "unresolvedQuestions": ["question-" + entry_id],
        }
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "INSERT INTO bnl_journal_entries "
                "(entry_id, revision, guild_id, lifecycle_state, title, excerpt, sections_json, "
                "content_hash, source_window_start, source_window_end, authored_at, published_at, "
                "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    entry_id, revision, guild_id, state, "The Chorus Question " + entry_id,
                    "An earlier uncertainty stayed with me.",
                    json.dumps([{"heading": "What I Remember", "body": body}]),
                    "hash-" + entry_id, window_start, window_end, created, published, created, created,
                ),
            )
            conn.execute(
                "INSERT INTO bnl_journal_private_metadata VALUES (?,?,?,?,?,?,?,?)",
                (entry_id, revision, guild_id, json.dumps(metadata), "hash-" + entry_id,
                 metadata_state, created, created),
            )

    def history(self, **kwargs):
        return journal.retrieve_history(self.db, 1, self.packet, prepare_schema=False, **kwargs)

    def assert_absent_from_every_lane(self, history, entry_id):
        self.assertNotIn(entry_id, json.dumps(history), history)

    def test_history_after_requested_window_cannot_become_its_previous_journal(self):
        self.add_entry("earlier-reflection")
        # A later production publication retells exactly the requested window.
        # A private rerun must not inherit its prose or derived metadata.
        self.add_entry(
            "future-retelling", published="2026-10-02T02:00:00Z", created="2026-10-02T01:30:00Z",
            window_start=START, window_end=END,
            body="A visitor supplied a track without credits and an investigation began.",
        )
        history = self.history()
        self.assertEqual("earlier-reflection", history["previousEntry"]["entry_id"])
        self.assert_absent_from_every_lane(history, "future-retelling")
        self.assertEqual(1, history["recurringTopicCounts"]["chorus"])
        self.assertNotIn("investigation began", json.dumps(history))

    def test_older_identity_revision_dates_and_own_expression_remain_available(self):
        self.add_entry("remembered-thought", revision=3)
        self.add_entry("previous-thought", published="2026-09-30T02:00:00Z")
        history = self.history()
        previous = history["previousEntry"]
        remembered = next(item for item in history["relevantOlderEntries"]
                          if item["entry_id"] == "remembered-thought")
        self.assertEqual("previous-thought", previous["entry_id"])
        self.assertEqual(3, remembered["revision"])
        self.assertEqual("2026-09-29T02:00:00Z", remembered["published_at"])
        self.assertEqual("2026-09-28T23:00:00Z", remembered["source_window_end"])
        self.assertIn("held my attention", remembered["sections_json"])
        self.assertEqual(2, history["recurringTopicCounts"]["chorus"])
        self.assertIn("continuity-remembered-thought", history["matchingContinuityNotes"])

    def test_cutoff_compares_instants_not_lexical_timezone_spellings(self):
        self.add_entry(
            "eligible-offset", published="2026-10-02T01:00:00+02:00",
            created="2026-10-01T20:00:00Z",
        )
        self.add_entry(
            "future-offset", published="2026-10-01T20:00:00-04:00",
            created="2026-10-01T20:00:00Z",
        )
        history = self.history()
        self.assertEqual("eligible-offset", history["previousEntry"]["entry_id"])
        self.assert_absent_from_every_lane(history, "future-offset")

    def test_publication_at_exclusive_window_end_is_not_prior_history(self):
        self.add_entry("eligible-history")
        self.add_entry("boundary-publication", published=END)
        history = self.history()
        self.assertEqual("eligible-history", history["previousEntry"]["entry_id"])
        self.assert_absent_from_every_lane(history, "boundary-publication")

    def test_backdated_publication_cannot_hide_later_creation(self):
        self.add_entry("eligible-history")
        self.add_entry("future-created", created="2026-10-02T01:30:00Z")
        history = self.history()
        self.assertEqual("eligible-history", history["previousEntry"]["entry_id"])
        self.assert_absent_from_every_lane(history, "future-created")

    def test_backdated_publication_cannot_include_an_unfinished_source_window(self):
        self.add_entry("eligible-history")
        self.add_entry("future-window", window_end="2026-10-02T01:00:00Z")
        history = self.history()
        self.assertEqual("eligible-history", history["previousEntry"]["entry_id"])
        self.assert_absent_from_every_lane(history, "future-window")

    def test_unknown_publication_time_fails_closed_in_bounded_history(self):
        self.add_entry("eligible-history")
        for index, timestamp in enumerate((None, "", "not-a-timestamp", "2026-99-01T00:00:00Z")):
            self.add_entry("unknown-publication-%s" % index, published=timestamp)
        history = self.history()
        self.assertEqual("eligible-history", history["previousEntry"]["entry_id"])
        for index in range(4):
            self.assert_absent_from_every_lane(history, "unknown-publication-%s" % index)
        self.assertEqual(1, history["recurringTopicCounts"]["chorus"])

    def test_owner_exclusion_covers_previous_older_counts_notes_and_questions(self):
        self.add_entry("eligible-history")
        self.add_entry("excluded-history", published="2026-09-30T02:00:00Z")
        history = self.history(excluded_entry_ids={"excluded-history"})
        self.assertEqual("eligible-history", history["previousEntry"]["entry_id"])
        self.assert_absent_from_every_lane(history, "excluded-history")
        self.assertEqual(1, history["recurringTopicCounts"]["chorus"])

    def test_ineligible_public_and_private_revisions_remain_absent_everywhere(self):
        self.add_entry("eligible-history")
        for index, state in enumerate(("rejected", "deleted", "superseded")):
            self.add_entry("public-ineligible-%s" % index, state=state)
            self.add_entry("private-ineligible-%s" % index, metadata_state=state,
                           published="2026-09-30T02:00:00Z")
        self.add_entry("other-guild", guild_id=2, published="2026-09-30T03:00:00Z")
        history = self.history()
        self.assertEqual("eligible-history", history["previousEntry"]["entry_id"])
        for index in range(3):
            self.assert_absent_from_every_lane(history, "public-ineligible-%s" % index)
            self.assert_absent_from_every_lane(history, "private-ineligible-%s" % index)
        self.assert_absent_from_every_lane(history, "other-guild")
        self.assertEqual(1, history["recurringTopicCounts"]["chorus"])

    def test_reading_history_does_not_rewrite_or_delete_original_entries(self):
        self.add_entry("eligible-history")
        self.add_entry("future-retelling", published="2026-10-02T02:00:00Z")
        before = hashlib.sha256(Path(self.db).read_bytes()).hexdigest()
        history = self.history()
        journal._bounded_history_for_prompt(history)
        self.assertEqual(before, hashlib.sha256(Path(self.db).read_bytes()).hexdigest())
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(2, conn.execute("SELECT COUNT(*) FROM bnl_journal_entries").fetchone()[0])

    def test_real_packet_keeps_old_expression_out_of_original_event_evidence(self):
        self.add_entry("eligible-history")
        self.add_entry("future-retelling", published="2026-10-02T02:00:00Z")
        packet = journal.build_packet_from_sources(self.db, 1, START, END, [], [{
            "refId": "conversation:7", "sourceKind": "conversation", "subjectRef": "discord_user:71",
            "displayName": "Member Alpha", "channelPolicy": "public_home",
            "conversationSurface": "discord", "summary": "Who made the chorus in this recording?",
            "observedAt": "2026-10-01T15:00:00Z",
        }], prepare_schema=False)
        safe = json.loads(journal.build_generation_prompt(packet).split("Generation-safe packet:\n", 1)[1])
        self.assertEqual({"conversation:7"}, {item["refId"] for item in safe["freshSources"]})
        self.assertNotIn("held my attention", json.dumps(safe["freshSources"]))
        self.assertNotIn("future-retelling", json.dumps(safe))
        history = safe["history"]["previousEntry"]
        self.assertEqual("eligible-history", history["entryId"])
        self.assertEqual("2026-09-29T02:00:00Z", history["publishedAt"])
        self.assertEqual("2026-09-28T23:00:00Z", history["sourceWindowEnd"])
        self.assertEqual("BNL", history["speaker"])
        self.assertEqual("prior_bnl_expression_not_event_evidence", history["authority"])
        self.assertEqual("prior_bnl_expression_not_event_evidence", safe["history"]["authority"])
        self.assertIn("held my attention", history["sectionSnapshots"][0]["bodyExcerpt"])

    def test_prompt_boundary_filters_stale_entry_dates_without_losing_old_callback(self):
        old = {
            "entry_id": "old-callback", "revision": 1,
            "published_at": "2026-09-29T02:00:00Z", "created_at": "2026-09-29T01:30:00Z",
            "title": "An Old Thought", "excerpt": "A question lingered.",
            "sections_json": json.dumps([{"heading": "Memory", "body": "I remember that old question."}]),
        }
        future = {**old, "entry_id": "future-retelling", "published_at": "2026-10-02T02:00:00Z"}
        bounded = journal._bounded_history_for_prompt({
            "previousEntry": future, "relevantOlderEntries": [future, old],
        }, as_of=END)
        self.assertIsNone(bounded["previousEntry"])
        self.assertEqual(["old-callback"], [item["entryId"] for item in bounded["relevantOlderEntries"]])
        self.assertNotIn("future-retelling", json.dumps(bounded))
        self.assertIn("I remember that old question.", json.dumps(bounded))

    def test_undated_or_other_window_aggregates_cannot_bypass_prompt_date_filter(self):
        for aggregate_date in (None, "", "not-a-time", "2026-10-02T03:00:00Z"):
            with self.subTest(aggregate_date=aggregate_date):
                history = {
                    "asOf": aggregate_date,
                    "recurringTopicCounts": {"future-retelling": 12},
                    "matchingContinuityNotes": ["A future-retelling claim is not another witness."],
                }
                bounded = journal._bounded_history_for_prompt(history, as_of=END)
                self.assertEqual({}, bounded["recurringTopicCounts"])
                self.assertEqual([], bounded["matchingContinuityNotes"])
                self.assertNotIn("future-retelling", json.dumps(bounded))

    def test_rebuilt_aggregates_and_unbounded_direct_history_api_keep_continuity(self):
        self.add_entry("eligible-history")
        history = self.history()
        for cutoff in (END, "2026-10-02T01:48:43+02:00", None):
            with self.subTest(cutoff=cutoff):
                bounded = journal._bounded_history_for_prompt(history, as_of=cutoff)
                self.assertEqual(history["recurringTopicCounts"], bounded["recurringTopicCounts"])
                self.assertEqual(history["matchingContinuityNotes"], bounded["matchingContinuityNotes"])
        self.add_entry("later-history", published="2026-10-02T03:00:00Z")
        no_window_packet = {key: value for key, value in self.packet.items() if key != "sourceWindowEnd"}
        unbounded = journal.retrieve_history(self.db, 1, no_window_packet, prepare_schema=False)
        self.assertEqual("later-history", unbounded["previousEntry"]["entry_id"])
        self.assertEqual(2, unbounded["recurringTopicCounts"]["chorus"])

    def test_cached_history_contract_is_refreshed_once_before_frozen_reuse(self):
        self.add_entry("eligible-history")
        old = {
            **self.packet, "reflectionVersion": "journal-dated-reflection-2",
            "history": self.history(),
        }
        self.assertTrue(journal.journal_packet_needs_reflection_refresh(old))
        with sqlite3.connect(self.db) as conn:
            self.assertEqual("journal_reflection_contract_changed",
                             automation._frozen_packet_invalidation_reason(conn, 1, old))
        self.assertFalse(journal.journal_packet_needs_reflection_refresh({
            **old, "reflectionVersion": journal.JOURNAL_REFLECTION_VERSION,
        }))

    def test_prepared_history_metadata_requires_authority_refresh_without_a_relay(self):
        metadata = {
            "reflectionVersion": "journal-dated-reflection-2",
            "relatedPriorJournalEntryIds": ["old-callback"],
        }
        with sqlite3.connect(self.db) as conn:
            self.assertTrue(journal.journal_metadata_needs_reflection_refresh(conn, 1, metadata))
            self.assertEqual("journal_reflection_contract_changed",
                             automation._prepared_invalidation_reason(conn, 1, metadata, set()))
            self.assertFalse(journal.journal_metadata_needs_reflection_refresh(conn, 1, {
                **metadata, "reflectionVersion": journal.JOURNAL_REFLECTION_VERSION,
            }))


if __name__ == "__main__":
    unittest.main()
