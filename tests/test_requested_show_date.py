"""Existing show readers preserve the requested calendar dates."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from bnl_journal_source_store import ensure_schema, record_source_event
from bnl_tiktok_live_context import (
    explicit_show_date,
    explicit_show_dates,
    has_explicit_show_date,
    is_live_show_reaction_query,
    is_tiktok_show_analysis_query,
    requested_show_date,
    requested_show_dates,
    select_show_for_tiktok_analysis,
)
from bnl_tiktok_show_ledger import (
    build_tiktok_show_evidence_context,
    select_tiktok_show_episode_context_items,
    sync_tiktok_show_evidence_ledgers,
)
from tests.test_tiktok_show_evidence_ledger import (
    ENABLED_QUEUE_ENV,
    archived_show,
    authorized_read_model,
    durable_events,
)


ACCEPTANCE_QUERY = (
    "BNL, what stood out in the TikTok chat during the August 28, 2026 show? "
    "Give me a couple of actual comments and who said them."
)


def two_show_archive():
    older = archived_show()
    newer = json.loads(
        json.dumps(older)
        .replace("show-attendance-1", "show-attendance-2")
        .replace("2026-08-28", "2026-09-04")
        .replace("2026-08-29", "2026-09-05")
    )
    return {"currentShow": None, "latestShow": newer, "shows": [newer, older]}


class RequestedShowDateTests(unittest.TestCase):
    def test_month_names_ordinals_and_iso_resolve_to_same_calendar_date(self):
        for date_text in (
            "2026-08-28", "2026-8-28", "August 28, 2026", "August 28 2026",
            "August 28th, 2026", "Aug 28, 2026", "Aug. 28, 2026",
            "AUGUST 28, 2026", "28 August 2026", "28th Aug. 2026",
            "August28,2026", "28August2026",
        ):
            with self.subTest(date_text=date_text):
                query = ACCEPTANCE_QUERY.replace("August 28, 2026", date_text)
                self.assertTrue(has_explicit_show_date(query))
                self.assertEqual(explicit_show_date(query), "2026-08-28")
                self.assertEqual(requested_show_date(query), "2026-08-28")
        self.assertEqual(explicit_show_date("September 4, 2026"), "2026-09-04")
        self.assertEqual(explicit_show_date("Sept. 4, 2026"), "2026-09-04")
        self.assertEqual(explicit_show_date("February 29, 2024"), "2024-02-29")

    def test_invalid_calendar_date_is_recognized_without_guessing(self):
        for date_text in ("2026-13-28", "2026-02-29", "February 30, 2026", "31 April 2026"):
            with self.subTest(date_text=date_text):
                query = "Show chat for " + date_text
                self.assertTrue(has_explicit_show_date(query))
                self.assertEqual(explicit_show_date(query), "")
                self.assertEqual(requested_show_date(query), "")
        self.assertFalse(has_explicit_show_date("Give me the show recap."))
        self.assertEqual(requested_show_date("Give me the show recap."), "")

    def test_relative_dates_use_pacific_calendar_including_utc_day_boundary(self):
        now = "2026-08-29T03:30:00Z"  # August 28 evening in Pacific time.
        for cue in ("today", "tonight", "this evening"):
            self.assertEqual(requested_show_date("TikTok show " + cue, now=now), "2026-08-28")
        for cue in ("yesterday", "last night"):
            self.assertEqual(requested_show_date("TikTok show " + cue, now=now), "2026-08-27")
        self.assertEqual(
            requested_show_date("Yesterday's show", now="2026-11-02T07:30:00Z"),
            "2026-10-31",
        )
        self.assertEqual(requested_show_date("What did I do yesterday?", now=now), "")

    def test_multiple_dates_keep_order_without_duplicate_or_invalid_entries(self):
        query = "Compare the September 4, 2026, 2026-08-28 and August 28, 2026 shows."
        self.assertEqual(explicit_show_dates(query), ("2026-09-04", "2026-08-28"))
        self.assertEqual(requested_show_dates(query), explicit_show_dates(query))
        self.assertEqual(explicit_show_date(query), "2026-09-04")
        self.assertEqual(
            requested_show_dates("Compare the February 30, 2026 and August 28, 2026 shows."),
            ("2026-08-28",),
        )
        self.assertEqual(requested_show_dates("Show on February 30, 2026"), ())
        self.assertEqual(requested_show_dates(
            "Yesterday's show", now="2026-09-05T17:00:00Z",
        ), ("2026-09-04",))

    def test_live_date_matches_the_authoritative_show_and_not_the_calendar(self):
        for day, expected in (("September 4, 2026", True), ("September 5, 2026", False)):
            self.assertEqual(is_live_show_reaction_query(
                "What's TikTok chat saying in the " + day + " show right now?",
                now="2026-09-05T07:30:00Z", current_show_date="2026-09-04",
            ), expected)
        for day in ("February 30, 2026", "September 4, 2026 and September 5, 2026"):
            self.assertFalse(is_live_show_reaction_query(
                "What's TikTok chat saying in the " + day + " shows?",
                now="2026-09-05T07:30:00Z", current_show_date="2026-09-04",
            ))

    def test_historical_chat_request_is_not_current_live_reaction(self):
        now = "2026-09-09T17:00:00Z"
        self.assertTrue(is_tiktok_show_analysis_query(ACCEPTANCE_QUERY))
        for query in (
            ACCEPTANCE_QUERY,
            ACCEPTANCE_QUERY.replace("August 28, 2026", "2026-08-28"),
            "What did TikTok chat say during yesterday's show?",
            "Give me the TikTok chat from last night.",
            "What stood out in TikTok chat during the previous show?",
            "What did TikTok chat say in the February 30, 2026 show?",
        ):
            with self.subTest(query=query):
                self.assertFalse(is_live_show_reaction_query(query, now=now))
        for query in (
            "What's TikTok chat saying?", "What's TikTok chat saying right now?",
            "What's TikTok chat saying during today's show?",
            "What's TikTok chat saying tonight?",
            "What's TikTok chat saying on September 9, 2026?",
        ):
            with self.subTest(query=query):
                self.assertTrue(is_live_show_reaction_query(query, now=now))

    def test_archive_selection_matches_named_and_iso_dates_without_latest_substitution(self):
        archive = two_show_archive()
        for query in (ACCEPTANCE_QUERY, ACCEPTANCE_QUERY.replace("August 28, 2026", "2026-08-28")):
            show, _ = select_show_for_tiktok_analysis(archive, query)
            self.assertEqual(show["showDate"], "2026-08-28")
        for date_text in ("August 21, 2026", "2026-08-21", "February 30, 2026"):
            self.assertEqual(
                select_show_for_tiktok_analysis(archive, "Show chat for " + date_text),
                ({}, "none"),
            )
        show, source = select_show_for_tiktok_analysis(
            archive, "What stood out in TikTok chat during yesterday's show?",
            now="2026-09-05T17:00:00Z",
        )
        self.assertEqual((show["showDate"], source), ("2026-09-04", "latestShow"))
        archive["currentShow"] = {"sessionId": "current", "showDate": "2026-09-09", "milestones": [{}]}
        for query in ("What's TikTok chat saying right now?", "What's TikTok chat saying tonight?"):
            show, source = select_show_for_tiktok_analysis(archive, query, now="2026-09-09T17:00:00Z")
            self.assertEqual((show["sessionId"], source), ("current", "currentShow"))

    def test_current_show_retains_website_date_across_pacific_midnight(self):
        archive = two_show_archive()
        archive["currentShow"] = {
            "sessionId": "friday-live", "showDate": "2026-09-04", "milestones": [{}],
        }
        now = "2026-09-05T07:30:00Z"  # Saturday 00:30 Pacific; Friday show still live.
        for cue in ("right now", "today", "tonight", "this evening"):
            query = "What's TikTok chat saying " + cue + "?"
            self.assertTrue(is_live_show_reaction_query(query, now=now))
            show, source = select_show_for_tiktok_analysis(archive, query, now=now)
            self.assertEqual((show["sessionId"], source), ("friday-live", "currentShow"))
            self.assertEqual(requested_show_date(
                query, now=now, include_current_relative=False,
            ), "")


class RequestedShowEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db_file = str(Path(self.directory.name) / "show-date.db")
        ensure_schema(self.db_file)
        # Both dates have equal public evidence so incidental topic overlap
        # cannot hide a wrong-date selection behind a higher relevance score.
        for offset, suffix in ((0, ""), (7 * 24 * 3600 * 1000, "-newer")):
            for event in durable_events():
                record_source_event(
                    self.db_file, guild_id=77, source_kind="tiktok_live_chat",
                    source_key=event["event_id"] + suffix,
                    occurred_at_ms=event["occurred_at_ms"] + offset,
                    raw_text=event["raw_text"], sanitized_summary=event["raw_text"],
                    channel_policy="public_context", subject_ref=event["subject_ref"],
                    private_display_name=event["private_display_name"], public_usable=True,
                    metadata=event["metadata"],
                )
        result = sync_tiktok_show_evidence_ledgers(
            self.db_file, guild_id=77, read_model=authorized_read_model(two_show_archive()),
            environ=ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["showsFinalized"], 2)
        self.assertEqual(result["sourceEvents"], 8)

    def packet_items(self, query):
        with sqlite3.connect(self.db_file) as conn:
            return select_tiktok_show_episode_context_items(
                conn, guild_id=77, user_text=query, subject_user_id=0,
            )

    def test_exact_acceptance_wording_gets_same_original_evidence_as_iso_date(self):
        contexts = []
        for query in (ACCEPTANCE_QUERY, ACCEPTANCE_QUERY.replace("August 28, 2026", "2026-08-28")):
            selected = {}
            contexts.append(build_tiktok_show_evidence_context(
                self.db_file, guild_id=77, user_text=query, selection_out=selected,
            ))
            self.assertEqual([key for key, _ in selected["source_refs"]], ["show-attendance-1"])
            self.assertIn('"Alex (@alex.signal)"', contexts[-1])
            self.assertIn(durable_events()[0]["raw_text"], contexts[-1])
            items = self.packet_items(query)
            self.assertTrue(items)
            self.assertTrue(all(item.show_dates == ("2026-08-28",) for item in items))
        self.assertEqual(contexts[0], contexts[1])

    def test_both_explicit_dates_reach_ledger_and_packet_selection(self):
        for dates in (
            "August 28, 2026 and September 4, 2026",
            "2026-08-28 and 2026-09-04",
            "September 4, 2026 and 2026-08-28",
        ):
            with self.subTest(dates=dates):
                query = "Compare TikTok chat across the " + dates + " shows."
                selected = {}
                rendered = build_tiktok_show_evidence_context(
                    self.db_file, guild_id=77, user_text=query, selection_out=selected,
                )
                self.assertEqual(
                    {key for key, _ in selected["source_refs"]},
                    {"show-attendance-1", "show-attendance-2"},
                )
                self.assertIn("on 2026-08-28;", rendered)
                self.assertIn("on 2026-09-04;", rendered)
                self.assertEqual(
                    {day for item in self.packet_items(query) for day in item.show_dates},
                    {"2026-08-28", "2026-09-04"},
                )

    def test_same_date_sessions_do_not_crowd_out_another_requested_date(self):
        archive = two_show_archive()
        second_september = json.loads(json.dumps(archive["latestShow"]))
        second_september["sessionId"] = "show-attendance-3"
        archive["shows"].insert(0, second_september)
        result = sync_tiktok_show_evidence_ledgers(
            self.db_file, guild_id=77, read_model=authorized_read_model(archive),
            environ=ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["showsFinalized"], 3)
        for dates in (
            "September 4, 2026 and August 28, 2026",
            "August 28, 2026 and September 4, 2026",
        ):
            with self.subTest(dates=dates):
                query = "Compare TikTok chat across the " + dates + " shows."
                rendered = build_tiktok_show_evidence_context(
                    self.db_file, guild_id=77, user_text=query, show_limit=2,
                )
                self.assertIn("on 2026-08-28;", rendered)
                self.assertIn("on 2026-09-04;", rendered)
                with sqlite3.connect(self.db_file) as conn:
                    items = select_tiktok_show_episode_context_items(
                        conn, guild_id=77, user_text=query, max_shows=2,
                    )
                self.assertEqual({day for item in items for day in item.show_dates},
                                 {"2026-08-28", "2026-09-04"})

    def test_inherited_two_date_scope_survives_pinned_refresh_without_plural_cue(self):
        prior = "TikTok chat from the August 28, 2026 show and September 4, 2026 show."
        selected = {}
        rendered = build_tiktok_show_evidence_context(
            self.db_file, guild_id=77, user_text="Give me some quotes",
            selection_user_text=prior, candidate_context=True, selection_out=selected,
        )
        self.assertEqual(len(selected["source_refs"]), 2)
        refreshed = build_tiktok_show_evidence_context(
            self.db_file, guild_id=77, user_text="Give me some quotes",
            selection_user_text=prior, candidate_context=True,
            pinned_show_keys=tuple(key for key, _ in selected["source_refs"]),
        )
        self.assertEqual(refreshed, rendered)

    def test_missing_or_invalid_requested_date_returns_no_other_episode(self):
        for date_text in ("August 21, 2026", "2026-08-21", "February 30, 2026", "2026-13-28"):
            with self.subTest(date_text=date_text):
                query = ACCEPTANCE_QUERY.replace("August 28, 2026", date_text)
                self.assertEqual(build_tiktok_show_evidence_context(
                    self.db_file, guild_id=77, user_text=query,
                ), "")
                self.assertEqual(self.packet_items(query), ())

    def test_missing_or_invalid_date_keeps_the_other_requested_show_only(self):
        for dates in (
            "August 14, 2026 and August 28, 2026",
            "August 28, 2026 and August 14, 2026",
            "February 30, 2026 and August 28, 2026",
        ):
            with self.subTest(dates=dates):
                query = "Compare TikTok chat across the " + dates + " shows."
                selected = {}
                rendered = build_tiktok_show_evidence_context(
                    self.db_file, guild_id=77, user_text=query, selection_out=selected,
                )
                self.assertEqual([key for key, _ in selected["source_refs"]], ["show-attendance-1"])
                self.assertIn("on 2026-08-28;", rendered)
                self.assertNotIn("on 2026-09-04;", rendered)
                self.assertEqual(
                    {day for item in self.packet_items(query) for day in item.show_dates},
                    {"2026-08-28"},
                )

    def test_current_date_correction_wins_prior_cue_without_expanding_pinned_roots(self):
        previous = ACCEPTANCE_QUERY.replace("August 28, 2026", "September 4, 2026")
        context = build_tiktok_show_evidence_context(
            self.db_file, guild_id=77, user_text=ACCEPTANCE_QUERY,
            selection_user_text=previous,
        )
        self.assertIn("on 2026-08-28;", context)
        self.assertNotIn("on 2026-09-04;", context)
        self.assertEqual(build_tiktok_show_evidence_context(
            self.db_file, guild_id=77, user_text=ACCEPTANCE_QUERY,
            selection_user_text=previous, pinned_show_keys=("show-attendance-2",),
        ), "")


if __name__ == "__main__":
    unittest.main()
