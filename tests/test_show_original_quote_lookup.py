"""Literal show checks use fresh eligible original rows, never cached prose."""

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import test_tiktok_show_evidence_ledger as fixture
import bnl_tiktok_show_ledger as reader
from bnl_journal_source_store import (
    record_source_event,
    purge_user_bound_conversation_sources_on_connection,
)


class OriginalShowQuoteLookupTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = str(Path(self.directory.name) / "show.db")
        fixture.TikTokShowEvidenceLedgerTests().seed_source_and_memory(self.db)
        self.show = fixture.archived_show()
        self.archive = {"currentShow": None, "latestShow": self.show, "shows": []}
        self.sync()

    def sync(self):
        result = reader.sync_tiktok_show_evidence_ledgers(
            self.db, guild_id=77, read_model=fixture.authorized_read_model(self.archive),
            artist_identity_index=fixture.artist_index(), environ=fixture.ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["projectionErrors"], 0)

    def add(self, key, text, speaker="Test Viewer", *, offset=0, public=True, subject=None):
        result = record_source_event(
            self.db, guild_id=77, source_kind="tiktok_live_chat", source_key=key,
            occurred_at_ms=fixture.stamp("2026-08-29T00:03:30Z") + offset,
            raw_text=text, sanitized_summary=text, channel_policy="public_context",
            subject_ref=subject or f"tiktok_handle:{key}", private_display_name=speaker,
            public_usable=public, metadata={"eventType": "comment", "handle": key},
        )
        self.assertTrue(result.ok)

    def read(self, literal="No source has this exact wording.", *, request=None, **kwargs):
        selection = {}
        context = reader.build_tiktok_show_evidence_context(
            self.db, guild_id=77,
            user_text=request or f'Verify TikTok chat during August 28, 2026: "{literal}"',
            selection_out=selection, **kwargs,
        )
        return context, selection

    def result(self, selection):
        return selection["original_quote_lookup"][0]

    def test_authentic_literal_beyond_ranked_examples_uses_actual_author(self):
        for index in range(20):
            self.add(f"decoy{index}", "BNL, check Test Claim August queue: violet or tiny?", offset=index)
        self.add("actual", "tiny violet", "Test Actual", offset=100)
        self.sync()
        request = 'Check TikTok chat during August 28, 2026: Did Test Claim say "tiny violet"?'
        baseline, _ = self.read(request=request.replace('"', ""), message_limit=16)
        self.assertNotIn('"tiny violet"', baseline)
        context, selection = self.read(request=request, message_limit=16)
        query = self.result(selection)["queries"][0]
        self.assertEqual(query["match_count"], 1)
        self.assertEqual(query["matches"][0]["speakerLabel"], "Test Actual (@actual)")
        self.assertIn('speaker="Test Actual (@actual)"; text="tiny violet"', context)
        self.assertTrue(any(item[2] == "actual" and item[5] == "tiny violet"
                            for item in selection["authored_excerpts"]))

    def test_complete_zero_is_only_for_named_current_eligible_window(self):
        self.add("excluded", "No source has this exact wording.", public=False)
        context, selection = self.read()
        result = self.result(selection)
        self.assertEqual((result["status"], result["eligible_rows_checked"]), ("complete", 4))
        self.assertEqual(result["queries"][0]["match_count"], 0)
        self.assertIn("windowUTC=2026-08-29T00:00:00+00:00", context)
        self.assertIn("No author-absence search", context)
        self.assertIn("Count covers the checked retained eligible original window only", context)

    def test_case_punctuation_whitespace_and_curly_delimiters_are_literal(self):
        self.add("spacing", "Tiny  Violet!", "Test Spacing")
        self.sync()
        for literal, count in (("Tiny  Violet!", 1), ("tiny  Violet!", 0),
                               ("Tiny Violet!", 0), ("Tiny  Violet.", 0)):
            with self.subTest(literal=literal):
                _context, selection = self.read(request=f"Verify TikTok show August 28, 2026: “{literal}”")
                self.assertEqual(self.result(selection)["queries"][0]["match_count"], count)
        self.add("edgewhite", "  Exact edge whitespace.  ")
        _context, selection = self.read("  Exact edge whitespace.  ")
        matched = self.result(selection)["queries"][0]["matches"][0]
        self.assertEqual(matched["text"], "  Exact edge whitespace.  ")
        self.assertTrue(any(item[5] == "  Exact edge whitespace.  " for item in selection["authored_excerpts"]))

    def test_duplicate_rows_count_all_authors_and_selected_show_roots(self):
        literal = "Same retained words."
        for index in range(10):
            self.add(f"copy{index}", literal, f"Test Copy {index}", offset=index)
        later = json.loads(json.dumps(self.show).replace("show-attendance-1", "later-show")
                           .replace("2026-08-28", "2026-09-04").replace("2026-08-29", "2026-09-05"))
        self.archive = {"currentShow": None, "latestShow": later, "shows": [self.show]}
        self.add("latercopy", literal, "Test Later", offset=7 * 86400 * 1000)
        self.sync()
        _context, selection = self.read(request=f'Compare TikTok shows August 28, 2026 and September 4, 2026 for "{literal}"')
        results = {item["show_date"]: item for item in selection["original_quote_lookup"]}
        self.assertEqual(set(results), {"2026-08-28", "2026-09-04"})
        self.assertEqual(results["2026-08-28"]["queries"][0]["match_count"], 10)
        self.assertEqual(results["2026-08-28"]["queries"][0]["shown_match_count"], 8)
        self.assertEqual(results["2026-09-04"]["queries"][0]["matches"][0]["speakerLabel"], "Test Later (@latercopy)")

    def test_formatting_candidates_preserve_originals_without_changing_literal_counts(self):
        original = "  the amber lamp is blinking  "
        self.add("formatting", original, "Test Original")
        self.sync()
        for literal in ("The amber lamp is blinking.", "the amber\nlamp is blinking", "the amber lamp is blinking!"):
            with self.subTest(literal=literal):
                context, selection = self.read(literal)
                query = self.result(selection)["queries"][0]
                self.assertEqual(query["match_count"], 0)
                self.assertEqual(query["format_candidate_count"], 1)
                self.assertEqual(query["shown_format_candidate_count"], 1)
                row = query["format_candidates"][0]
                self.assertEqual((row["eventId"], row["text"], row["speakerLabel"]),
                                 ("formatting", original, "Test Original (@formatting)"))
                self.assertIn("not verbatim matches or proof of equivalent meaning", context)
                self.assertIn("formatCandidateRows=1", context)
                self.assertTrue(any(item[2] == "formatting" and item[5] == original
                                    for item in selection["authored_excerpts"]))
                self.assertFalse(any(item[5] == literal for item in selection["authored_excerpts"]))
        _context, selection = self.read(original)
        query = self.result(selection)["queries"][0]
        self.assertEqual((query["match_count"], query["format_candidate_count"]), (1, 0))

    def test_formatting_candidates_require_contiguous_whole_words_and_current_eligibility(self):
        self.add("changed", "the amber lamp is not blinking")
        self.add("fragment", "the amber lamp is blinkingly bright")
        self.add("reordered", "blinking is the amber lamp")
        self.add("private", "the amber lamp is blinking", public=False)
        self.add("other-window", "the amber lamp is blinking", offset=7 * 86400 * 1000)
        self.sync()
        context, selection = self.read("The amber lamp is blinking.")
        query = self.result(selection)["queries"][0]
        self.assertEqual((query["match_count"], query["format_candidate_count"]), (0, 0))
        self.assertFalse(query["format_candidates"])
        self.assertIn("No candidate does not rule out other wording", context)

    def test_all_exact_results_take_display_priority_over_formatting_candidates(self):
        for index in range(10):
            self.add(f"variant{index}", "the amber lamp is blinking", f"Test Variant {index}", offset=index)
        self.add("later-exact", "Keep this exact original.", "Test Exact", offset=20)
        self.sync()
        context, selection = self.read(request=(
            'Verify TikTok chat during August 28, 2026: '
            '"The amber lamp is blinking." "Keep this exact original."'
        ))
        queries = self.result(selection)["queries"]
        self.assertEqual(queries[0]["format_candidate_count"], 10)
        self.assertEqual(queries[0]["shown_format_candidate_count"], 7)
        self.assertEqual(queries[1]["shown_match_count"], 1)
        self.assertIn('speaker="Test Exact"; text="Keep this exact original."', context)
        self.assertEqual(sum(q["shown_match_count"] + q["shown_format_candidate_count"] for q in queries), 8)
        self.assertEqual(len({r["speakerLabel"] for r in queries[0]["format_candidates"]}), 7)

    def test_withdrawal_does_not_reintroduce_cached_quote_or_participant(self):
        self.add("withdrawn", "The copper lantern is dim.", "Test Withdrawn", subject="discord_user:4242")
        self.sync()
        before, _ = self.read("The copper lantern is dim.")
        self.assertIn("Test Withdrawn", before)
        with sqlite3.connect(self.db) as conn:
            purge_user_bound_conversation_sources_on_connection(conn, 77, 4242)
        after, selection = self.read("The copper lantern is dim.")
        self.assertEqual(self.result(selection)["queries"][0]["match_count"], 0)
        self.assertEqual(selection["stale_projection_show_keys"], ("show-attendance-1",))
        self.assertNotIn("Test Withdrawn", after)
        self.assertNotIn("Source-linked authored examples:", after)
        self.assertFalse(selection["authored_excerpts"])
        self.assertNotEqual(before, after)

    def test_packet_literal_lookup_keeps_existing_two_show_raw_read_bound(self):
        shows = [self.show]
        for index, day in enumerate(("2026-09-04", "2026-09-11"), 1):
            following_day = "2026-09-05" if index == 1 else "2026-09-12"
            shows.append(json.loads(
                json.dumps(self.show).replace("show-attendance-1", f"later-{index}")
                .replace("2026-08-28", day).replace("2026-08-29", following_day)
            ))
            self.add(f"later{index}", "A bounded original.", offset=index * 7 * 86400 * 1000)
        self.archive = {"currentShow": None, "latestShow": shows[-1], "shows": shows[:-1]}
        self.sync()
        request = ('Compare TikTok shows August 28, 2026, September 4, 2026 and '
                   'September 11, 2026 for "A bounded original."')
        with sqlite3.connect(self.db) as conn, mock.patch.object(
            reader, "_load_show_source_events", wraps=reader._load_show_source_events,
        ) as raw_reader:
            reader.select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=request)
        self.assertEqual(raw_reader.call_count, 2)

    def test_original_update_changes_lookup_without_ledger_sync(self):
        self.add("edited", "Earlier exact line.", "Test Earlier", subject="discord_user:4242")
        self.sync()
        with sqlite3.connect(self.db) as conn:
            purge_user_bound_conversation_sources_on_connection(conn, 77, 4242)
        # Reinsert after the explicit governed deletion; ordinary stored rows
        # remain immutable throughout this test.
        self.add("edited", "Current exact line.", "Test Current", subject="discord_user:4242")
        context, selection = self.read("Current exact line.")
        self.assertIn('speaker="Test Current (@edited)"; text="Current exact line."', context)
        self.assertNotIn("Earlier exact line.", context)
        self.assertNotIn("Test Earlier", context)
        self.assertEqual(selection["stale_projection_show_keys"], ("show-attendance-1",))

    def test_packet_revalidation_removes_stale_human_projection_but_keeps_operations(self):
        self.add("boundviewer", "A copper lantern quote.", "Test Bound", subject="discord_user:4242")
        self.sync()
        request = 'Verify "A copper lantern quote." during the August 28, 2026 TikTok show and explain the wheel timeline.'
        with sqlite3.connect(self.db) as conn:
            before = reader.select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=request)
        self.assertTrue(any(item.kind == "dialogue" for item in before))
        with sqlite3.connect(self.db) as conn:
            purge_user_bound_conversation_sources_on_connection(conn, 77, 4242)
        with sqlite3.connect(self.db) as conn:
            after = reader.select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=request)
            for item in before:
                version = reader.tiktok_show_episode_context_item_version(
                    conn, guild_id=77, user_text=request, subject_user_id=0, source_ref=item.source_ref,
                )
                if item.kind in {"community", "dialogue"}:
                    self.assertEqual(version, "")
                else:
                    self.assertEqual(version, item.source_digest)
        self.assertTrue(after)
        self.assertEqual({item.kind for item in after}, {"operations"})
        context, _selection = self.read(request=request)
        self.assertIn("Independent recorded queue/broadcast chronology", context)
        self.assertNotIn("Test Bound", context)

    def test_new_original_in_unsynced_ledger_is_looked_up(self):
        before, _selection = self.read("Added after synchronization.")
        self.add("fresh", "Added after synchronization.", "Test Fresh")
        after, selection = self.read("Added after synchronization.")
        self.assertNotEqual(before, after)
        self.assertEqual(self.result(selection)["queries"][0]["match_count"], 1)
        self.assertIn('speaker="Test Fresh (@fresh)"', after)
        self.assertEqual(selection["stale_projection_show_keys"], ("show-attendance-1",))

    def test_literal_request_limit_and_total_match_display_limit_are_explicit(self):
        for index in range(10):
            self.add(f"many{index}", "shared first second", offset=index)
        self.sync()
        literals = ["shared", "first", "second", *[f"literal {index}" for index in range(7)]]
        request = "Verify TikTok show August 28, 2026 " + " ".join(json.dumps(text) for text in literals)
        context, selection = self.read(request=request)
        result = self.result(selection)
        self.assertEqual(result["unsearched_literal_count"], 2)
        self.assertEqual(len(result["queries"]), 8)
        self.assertEqual(sum(query["shown_match_count"] for query in result["queries"]), 8)
        self.assertEqual(result["queries"][1]["match_count"], 10)
        self.assertIn("2 additional quoted strings were not searched", context)

    def test_missing_source_table_and_read_failure_are_not_completed_zero(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("DROP TABLE bnl_journal_source_events")
        context, selection = self.read()
        self.assertEqual(self.result(selection)["status"], "unavailable")
        self.assertEqual(self.result(selection)["queries"], ())
        self.assertNotIn("matchedOriginalRows=0", context)
        with mock.patch.object(reader.sqlite3, "connect", side_effect=sqlite3.OperationalError("unavailable")):
            diagnostics = {}
            self.assertIsNone(reader.load_tiktok_show_source_events(self.db, guild_id=77, show=self.show,
                                                                   diagnostics_out=diagnostics))
            self.assertEqual(diagnostics["status"], "unavailable")

    def test_source_cap_and_truncated_original_have_incomplete_coverage(self):
        with mock.patch.object(reader, "TIKTOK_SHOW_EVIDENCE_MAX_SOURCE_EVENTS", 2):
            context, selection = self.read()
        result = self.result(selection)
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["eligible_rows_checked"], 0)
        self.assertEqual(result["queries"], ())
        self.assertNotIn("matchedOriginalRows=0", context)
        self.add("longsource", "x" * 1001 + "Needle at the end.")
        context, selection = self.read("Needle at the end.")
        result = self.result(selection)
        self.assertEqual((result["status"], result["skipped_rows"]), ("partial", 1))
        self.assertEqual(result["eligible_rows_checked"], 4)
        self.assertIn("Coverage is incomplete", context)

    def test_owner_label_uses_existing_public_normalization(self):
        self.add("ownerfixture", "A verified owner comment.", "Test Owner Label", subject="discord_user:4242")
        self.sync()
        with mock.patch.dict(os.environ, {"BNL_OWNER_USER_ID": "4242"}):
            context, selection = self.read("A verified owner comment.")
        self.assertEqual(self.result(selection)["queries"][0]["matches"][0]["speakerLabel"], "6 Bit")
        self.assertNotIn("Test Owner Label", context)

    def test_current_request_only_and_pinned_show_refresh(self):
        context, selection = self.read(request="What stood out in TikTok chat during August 28, 2026?",
                                       selection_user_text='Earlier request about TikTok show: "some old quote"')
        self.assertNotIn("original_quote_lookup", selection)
        self.assertNotIn("Original TikTok quote lookup:", context)
        context, selection = self.read()
        refreshed, fresh_selection = self.read(pinned_show_keys=("show-attendance-1",))
        self.assertEqual(refreshed, context)
        self.assertEqual(fresh_selection["original_quote_lookup"], selection["original_quote_lookup"])

    def test_optional_diagnostics_do_not_change_normal_source_event_shape(self):
        normal = reader.load_tiktok_show_source_events(self.db, guild_id=77, show=self.show)
        diagnostics = {}
        looked_up = reader.load_tiktok_show_source_events(self.db, guild_id=77, show=self.show,
                                                         diagnostics_out=diagnostics)
        self.assertEqual(looked_up, normal)
        self.assertEqual(diagnostics["status"], "complete")
        diagnostics = {}
        self.assertIsNone(reader.load_tiktok_show_source_events(self.db + ".missing", guild_id=77,
                                                              show=self.show, diagnostics_out=diagnostics))
        self.assertEqual(diagnostics["status"], "unavailable")
