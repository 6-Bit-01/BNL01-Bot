import gc
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from bnl_canon_source_contract import show_queue_evidence_authorization
from bnl_journal_source_store import ensure_schema, _record_on_connection
from bnl_tiktok_live_context import (
    build_durable_show_prompt_context, build_tiktok_show_evidence_ledger,
    count_tiktok_show_word_frequency, is_tiktok_show_analysis_followup,
    is_tiktok_show_analysis_query, render_tiktok_show_word_frequency,
    requested_recent_show_count, requested_tiktok_show_word_count,
    select_show_for_tiktok_analysis, show_timeline_bounds_ms,
    tiktok_show_word_frequency_bounds_ms,
)
from bnl_tiktok_show_ledger import (
    _lookup_tiktok_show_word_frequency, _seal_authorized_show_ledger,
    build_tiktok_show_evidence_context,
    ensure_tiktok_show_evidence_schema, select_tiktok_show_episode_context_items,
    tiktok_show_episode_context_item_version,
)


def stamp(value):
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def show(day="2026-10-02"):
    next_day = "2026-10-03" if day == "2026-10-02" else "2026-09-26"
    return {
        "sessionId": "anonymous-" + day, "title": "BARCODE Radio",
        "showDate": day, "status": "archived", "milestones": [
            {"eventType": "broadcast_started", "occurredAt": next_day + "T02:05:35.254Z"},
            {"eventType": "session_archived", "occurredAt": next_day + "T08:08:03.054Z"},
        ],
    }


def event(index, text, occurred, prefix="current"):
    return {
        "event_id": prefix + "-" + str(index), "occurred_at_ms": occurred,
        "ingested_at_ms": occurred,
        "subject_ref": "tiktok_user:viewer" + str(index % 11),
        "private_display_name": "Viewer " + str(index % 11),
        "raw_text": text, "content_hash": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "metadata": {"eventType": "comment", "handle": "viewer" + str(index % 11)},
    }


def full_fixture():
    start = stamp("2026-10-03T02:05:35.254Z")
    cutoff = stamp("2026-10-03T04:18:14Z")
    records = []
    for index in range(2103):
        if index == 0:
            text = "Panda panda!"
        elif index < 66:
            text = "PANDA"
        else:
            text = "Good drums tonight"
        occurred = (start + 1000 + index * 500 if index < 37 else
                    cutoff + (index - 36) * 1000 if index < 66 else
                    start + (index % 120) * 60_000)
        records.append(event(index, text, occurred))
    return records


class TikTokShowWordFrequencyTests(unittest.TestCase):
    def test_word_count_queries_and_corrections_use_the_existing_durable_owner(self):
        for query in (
            'THE TIKTOK CHAT. HOW MANY TIMES DID THEY SAY PANDA?',
            'How many times did people say the word "panda" during the last stream?',
            'Count the word panda in TikTok chat tonight',
        ):
            with self.subTest(query=query):
                self.assertTrue(is_tiktok_show_analysis_query(query))
                self.assertEqual(requested_tiktok_show_word_count(query), "panda")
        query = 'How many times did they say the word "panda"?'
        self.assertFalse(is_tiktok_show_analysis_query(query))
        self.assertTrue(is_tiktok_show_analysis_followup(query))
        self.assertTrue(is_tiktok_show_analysis_followup("Not now, in the whole stream"))
        self.assertEqual(requested_tiktok_show_word_count("Tell me about pandas"), "")

    def test_explicit_quoted_and_unquoted_stopwords_are_valid_word_targets(self):
        at = stamp("2026-10-03T03:00:00Z")
        for word in ("the", "word", "count", "it", "that", "this", "what", "anything", "something"):
            for literal in (word, '"' + word + '"'):
                query = "Count the word " + literal + " in TikTok chat tonight"
                with self.subTest(word=word, literal=literal):
                    self.assertEqual(requested_tiktok_show_word_count(query), word)
                    result = count_tiktok_show_word_frequency(
                        show(), [event(0, word + " " + word, at)], query)
                    self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 2))
        self.assertEqual(requested_tiktok_show_word_count("How many times did they say it?"), "")
        self.assertEqual(requested_tiktok_show_word_count("What is the word count in TikTok chat?"), "")

    def test_current_correction_keeps_target_from_the_human_followup_chain(self):
        query = ('TikTok chat tonight\nPrior follow-up: How many times did they say the word "panda"?'
                 '\nCurrent follow-up: Not now, in the whole stream')
        self.assertEqual(requested_tiktok_show_word_count(query), "panda")
        self.assertTrue(is_tiktok_show_analysis_query(query))

    def test_last_stream_means_latest_single_completed_source(self):
        latest, older = show(), show("2026-09-25")
        archive = {"latestShow": latest, "shows": [older]}
        self.assertEqual(requested_recent_show_count("during the last stream"), 1)
        selected, _ = select_show_for_tiktok_analysis(archive, "Count word panda in the last stream")
        self.assertEqual(selected["sessionId"], latest["sessionId"])
        selected, _ = select_show_for_tiktok_analysis(
            archive, "Count word panda during the 2026-09-25 stream",
            now=datetime(2026, 10, 3, tzinfo=timezone.utc))
        self.assertEqual(selected["sessionId"], older["sessionId"])

    def test_complete_full_stream_counts_occurrences_messages_and_speakers(self):
        result = count_tiktok_show_word_frequency(
            show(), full_fixture(), 'How many times did they say word "panda" in the stream?')
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["capturedMessageCount"], 2103)
        self.assertEqual((result["occurrenceCount"], result["matchingMessageCount"],
                          result["matchingSpeakerCount"]), (67, 66, 11))
        self.assertEqual(len(result["originalSourceRefs"]), 2103)

    def test_ongoing_stream_uses_the_frozen_observation_cutoff(self):
        active = show()
        active["status"] = "live"
        active["milestones"] = active["milestones"][:1]
        active["_evidenceObservedThroughMs"] = stamp("2026-10-03T04:18:14Z")
        result = count_tiktok_show_word_frequency(
            active, full_fixture(), "How many times did they say panda in the whole stream?")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["windowEndMs"], active["_evidenceObservedThroughMs"])
        self.assertEqual((result["occurrenceCount"], result["matchingMessageCount"]), (38, 37))

    def test_active_word_count_requires_owned_observation_bound_not_last_queue_milestone(self):
        active = show()
        active["status"] = "live"
        active["milestones"] = [active["milestones"][0], {
            "eventType": "track_loaded", "occurredAt": "2026-10-03T03:00:00Z",
        }]
        records = [
            event(0, "Good drums", stamp("2026-10-03T02:30:00Z")),
            event(1, "panda", stamp("2026-10-03T03:30:00Z")),
        ]
        query = "Count word panda in this TikTok live"
        for marker in (None, False, 0, "unavailable", stamp("2026-10-03T02:45:00Z")):
            selected = dict(active)
            if marker is not None:
                selected["_evidenceObservedThroughMs"] = marker
            with self.subTest(marker=marker):
                result = count_tiktok_show_word_frequency(selected, records, query)
                self.assertEqual((result["status"], result["reason"]),
                                 ("unavailable", "active_observation_bound_unavailable"))
                self.assertIsNone(result["occurrenceCount"])
                self.assertIsNone(result["windowEndMs"])
                self.assertNotIn("occurrenceCount=0", render_tiktok_show_word_frequency(result))
        active["_evidenceObservedThroughMs"] = stamp("2026-10-03T04:18:14Z")
        result = count_tiktok_show_word_frequency(active, records, query)
        self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 1))
        finalized = {
            "lifecycle": "finalized", "showKey": "anonymous-finalized", "showDate": "2026-10-02",
            "startedAtMs": stamp("2026-10-03T02:05:35.254Z"),
            "endedAtMs": stamp("2026-10-03T04:18:14Z"),
        }
        result = count_tiktok_show_word_frequency(finalized, records, query)
        self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 1))

    def test_word_matching_ignores_case_and_counts_repeats_without_substrings(self):
        at = stamp("2026-10-03T03:00:00Z")
        result = count_tiktok_show_word_frequency(show(), [
            event(0, "panda PANDAS pandamonium redpanda PANDA!", at),
        ], "Count the word panda in this stream")
        self.assertEqual((result["occurrenceCount"], result["matchingMessageCount"]), (2, 1))

    def test_pre_show_post_show_model_and_other_surfaces_are_excluded(self):
        start, end = stamp("2026-10-03T02:05:35.254Z"), stamp("2026-10-03T08:08:03.054Z")
        records = [
            event(0, "panda", start - 1), event(1, "panda", end + 1),
            event(2, "panda", start + 1), {**event(3, "panda", start + 1), "role": "model"},
            {**event(4, "panda", start + 1), "source_kind": "discord_message"},
            {**event(5, "panda", start + 1), "public_usable": False},
            {**event(6, "panda", start + 1), "channel_policy": "private"},
        ]
        result = count_tiktok_show_word_frequency(show(), records, "Count word panda in this stream")
        self.assertEqual(result["occurrenceCount"], 1)
        self.assertEqual(result["capturedMessageCount"], 1)

    def test_duplicate_original_identity_is_not_counted_twice(self):
        original = event(0, "panda", stamp("2026-10-03T03:00:00Z"))
        result = count_tiktok_show_word_frequency(show(), [original, dict(original)],
                                                 "Count word panda in this stream")
        self.assertEqual(result["occurrenceCount"], 1)
        self.assertEqual(result["capturedMessageCount"], 1)

    def test_matching_identity_keys_preserve_source_ownership_across_shared_handles(self):
        at = stamp("2026-10-03T03:00:00Z")
        first, second = event(0, "panda", at), event(1, "panda", at + 1)
        first["metadata"]["handle"] = second["metadata"]["handle"] = "shared.handle"
        self.assertNotEqual(first["subject_ref"], second["subject_ref"])
        result = count_tiktok_show_word_frequency(
            show(), [first, second], "Count word panda in this stream")
        self.assertEqual((result["occurrenceCount"], result["matchingSpeakerCount"]), (2, 2))
        self.assertIn("distinct captured chat identity keys", render_tiktok_show_word_frequency(result))

    def test_one_source_identity_key_is_not_split_by_a_changed_handle(self):
        at = stamp("2026-10-03T03:00:00Z")
        first, second = event(0, "panda", at), event(1, "panda", at + 1)
        second["subject_ref"] = first["subject_ref"]
        self.assertNotEqual(first["metadata"]["handle"], second["metadata"]["handle"])
        result = count_tiktok_show_word_frequency(
            show(), [first, second], "Count word panda in this stream")
        self.assertEqual((result["occurrenceCount"], result["matchingSpeakerCount"]), (2, 1))

    def test_missing_empty_and_partial_sources_never_produce_an_exact_zero(self):
        at = stamp("2026-10-03T03:00:00Z")
        corrupt = {**event(0, "panda", at), "content_hash": "wrong"}
        truncated = {**event(1, "panda", at), "raw_text_truncated": True}
        for records in (None, [], [corrupt], [truncated], [{}]):
            with self.subTest(records=records):
                result = count_tiktok_show_word_frequency(show(), records, "Count word panda in this stream")
                self.assertNotEqual(result["status"], "complete")
                self.assertIsNone(result["occurrenceCount"])
                self.assertNotIn("occurrenceCount=0", render_tiktok_show_word_frequency(result))

    def test_verified_captured_window_can_report_no_matches(self):
        result = count_tiktok_show_word_frequency(show(), [
            event(0, "Good drums", stamp("2026-10-03T03:00:00Z")),
        ], "Count word panda in this stream")
        self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 0))

    def test_rendered_durable_context_leads_with_exact_totals_and_selected_episode(self):
        context = build_durable_show_prompt_context(
            {"latestShow": show()}, full_fixture(), "How many times did they say word panda in the last stream?")
        self.assertIn("occurrenceCount=67; matchingMessageCount=66", context)
        self.assertIn('sessionId="anonymous-2026-10-02"', context)
        self.assertNotIn("top tracks", context.casefold())

    def test_rendered_count_context_omits_original_identity_and_text(self):
        result = count_tiktok_show_word_frequency(
            show(), full_fixture(), "Count word panda in this stream")
        context = render_tiktok_show_word_frequency(result)
        self.assertNotIn("tiktok_user:", context)
        self.assertNotIn("Viewer 0", context)
        self.assertNotIn("Good drums tonight", context)


    def test_named_speaker_and_specific_interval_do_not_become_whole_chat_counts(self):
        for query in (
            "How many times did Chris say the word panda during this stream?",
            "Count the word panda during the current track",
            "Count the word panda before 04:18 in the stream",
        ):
            with self.subTest(query=query):
                result = count_tiktok_show_word_frequency(show(), full_fixture(), query)
                self.assertEqual(result["status"], "unavailable")
                self.assertIsNone(result["occurrenceCount"])
        self.assertEqual(requested_tiktok_show_word_count("What is the word count in TikTok chat?"), "")

    def test_non_text_metric_payloads_cannot_enter_chat_word_counts(self):
        at = stamp("2026-10-03T03:00:00Z")
        gift = event(0, "panda", at)
        gift["metadata"]["eventType"] = "gift"
        result = count_tiktok_show_word_frequency(show(), [
            gift, event(1, "panda", at),
        ], "Count word panda in this stream")
        self.assertEqual((result["occurrenceCount"], result["capturedMessageCount"]), (1, 1))



    def test_current_source_aliases_cannot_select_an_older_latest_show(self):
        for alias in ("stream", "live", "episode", "show"):
            for modifier in ("", "TikTok ", "Tik Tok ", "public TikTok "):
                query = "Count word panda in this " + modifier + alias
                with self.subTest(alias=alias, modifier=modifier):
                    selected, owner = select_show_for_tiktok_analysis(
                        {"latestShow": show("2026-09-25")}, query)
                    self.assertEqual((selected, owner), ({}, "none"))

    def test_current_stream_alias_keeps_website_owned_frozen_source(self):
        active = show()
        active.update(status="live", milestones=active["milestones"][:1],
                      _evidenceObservedThroughMs=stamp("2026-10-03T04:18:14Z"))
        selected, owner = select_show_for_tiktok_analysis(
            {"currentShow": active, "latestShow": show("2026-09-25")},
            "Count word panda in this stream")
        self.assertEqual(owner, "currentShow")
        result = count_tiktok_show_word_frequency(
            selected, full_fixture(), "Count word panda in this stream")
        self.assertEqual(result["occurrenceCount"], 38)

    def test_counting_a_literal_interval_word_does_not_request_an_interval(self):
        original = event(0, "track track", stamp("2026-10-03T03:00:00Z"))
        result = count_tiktok_show_word_frequency(
            show(), [original], "How many times did they say the word track in the stream?")
        self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 2))


    def test_tonight_cannot_fall_back_to_an_unrelated_old_episode(self):
        now = datetime(2026, 10, 3, 4, 18, 14, tzinfo=timezone.utc)
        query = "Count word panda in the TikTok stream tonight"
        selected, owner = select_show_for_tiktok_analysis(
            {"latestShow": show("2026-09-25")}, query, now=now)
        self.assertEqual((selected, owner), ({}, "none"))
        selected, owner = select_show_for_tiktok_analysis(
            {"latestShow": show(), "shows": [show("2026-09-25")]}, query, now=now)
        self.assertEqual((selected["showDate"], owner), ("2026-10-02", "latestShow"))

    def test_after_midnight_tonight_retains_the_current_website_show_date(self):
        active = show()
        active.update(status="live", milestones=active["milestones"][:1],
                      _evidenceObservedThroughMs=stamp("2026-10-03T08:05:00Z"))
        selected, owner = select_show_for_tiktok_analysis(
            {"currentShow": active, "latestShow": show("2026-09-25")},
            "Count word panda in tonight's TikTok stream",
            now=datetime(2026, 10, 3, 8, 5, tzinfo=timezone.utc))
        self.assertEqual((selected["showDate"], owner), ("2026-10-02", "currentShow"))
        self.assertEqual(selected["_evidenceObservedThroughMs"], active["_evidenceObservedThroughMs"])

    def test_current_human_episode_scope_overrides_earlier_human_scope(self):
        active = show()
        active.update(status="live", milestones=active["milestones"][:1],
                      _evidenceObservedThroughMs=stamp("2026-10-03T04:18:14Z"))
        archive = {"currentShow": active, "latestShow": show("2026-09-25")}
        query = ("Count word panda in the 2026-09-25 stream"
                 "\nCurrent follow-up: Count word panda in this stream")
        selected, owner = select_show_for_tiktok_analysis(archive, query)
        self.assertEqual((selected["showDate"], owner), ("2026-10-02", "currentShow"))
        query = ("Count word panda in tonight's stream"
                 "\nCurrent follow-up: Count word panda in the last stream")
        selected, owner = select_show_for_tiktok_analysis(archive, query)
        self.assertEqual((selected["showDate"], owner), ("2026-09-25", "latestShow"))

    def test_later_named_speaker_cannot_be_widened_by_prior_generic_chat_scope(self):
        query = ('TikTok stream tonight: How many times did they say word "panda"?'
                 "\nCurrent follow-up: How many times did Chris say panda?")
        result = count_tiktok_show_word_frequency(show(), full_fixture(), query)
        self.assertEqual(result["reason"], "specific_speaker_scope_not_resolved")
        self.assertIsNone(result["occurrenceCount"])

    def test_literal_relative_time_words_do_not_select_a_current_episode(self):
        archive = {"latestShow": show()}
        for word in ("now", "tonight", "currently", "this", "it", "that", "count"):
            query = "Count the word " + word + " in the last stream"
            selected, owner = select_show_for_tiktok_analysis(archive, query)
            with self.subTest(word=word):
                self.assertEqual((selected["showDate"], owner), ("2026-10-02", "latestShow"))


    def test_bare_count_and_latest_human_term_corrections(self):
        at = stamp("2026-10-03T03:00:00Z")
        records = [event(0, "panda panda goat", at)]
        for query in ("Count panda in the last stream", "Count 'panda' in the last stream"):
            with self.subTest(query=query):
                self.assertEqual(requested_tiktok_show_word_count(query), "panda")
                self.assertEqual(count_tiktok_show_word_frequency(show(), records, query)["occurrenceCount"], 2)
        for correction in ("I meant goat", "not panda, goat"):
            query = "Count word panda in the last stream\nCurrent follow-up: " + correction
            with self.subTest(correction=correction):
                self.assertTrue(is_tiktok_show_analysis_followup(correction))
                self.assertEqual(requested_tiktok_show_word_count(query), "goat")
                self.assertEqual(count_tiktok_show_word_frequency(show(), records, query)["occurrenceCount"], 1)
        self.assertEqual(requested_tiktok_show_word_count("I meant goat"), "")
        self.assertEqual(requested_tiktok_show_word_count("not panda, goat"), "")
        self.assertEqual(requested_tiktok_show_word_count(
            "Count word panda in tonight's stream\nCurrent follow-up: I meant 'goat'"), "goat")
        for measure in ("comments", "viewers", "taps", "messages",
                        "songs", "tracks", "submissions", "gifts", "shares", "follows"):
            self.assertEqual(requested_tiktok_show_word_count("Count " + measure + " in TikTok chat"), "")

    def test_discarded_correction_operand_cannot_steal_the_historical_episode(self):
        active = show()
        active.update(status="live", milestones=active["milestones"][:1],
                      _evidenceObservedThroughMs=stamp("2026-10-03T04:18:14Z"))
        older = show("2026-09-25")
        archive = {"currentShow": active, "latestShow": older}
        for old_word in ("now", "tonight"):
            query = "Count word '" + old_word + "' in the last stream\nCurrent follow-up: not " + old_word + ", goat"
            with self.subTest(old_word=old_word):
                self.assertEqual(requested_tiktok_show_word_count(query), "goat")
                selected, owner = select_show_for_tiktok_analysis(archive, query)
                self.assertEqual((selected["showDate"], owner), ("2026-09-25", "latestShow"))
                result = count_tiktok_show_word_frequency(selected, [
                    event(0, "goat goat", stamp("2026-09-26T03:00:00Z")),
                ], query)
                self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 2))

    def test_bare_metric_counts_keep_existing_owners_while_explicit_literals_remain_words(self):
        at = stamp("2026-10-03T03:00:00Z")
        for measure in ("songs", "tracks", "submissions", "gifts", "shares", "follows",
                        "donations", "diamonds", "wheel", "spins", "joins", "participants", "artists"):
            with self.subTest(measure=measure):
                self.assertEqual(requested_tiktok_show_word_count("Count " + measure + " in the last stream"), "")
                verbal_query = "How many times did TikTok chat say " + measure + " in the last stream?"
                self.assertEqual(requested_tiktok_show_word_count(verbal_query), measure)
                verbal_result = count_tiktok_show_word_frequency(
                    show(), [event(0, measure + " " + measure, at)], verbal_query)
                self.assertEqual((verbal_result["status"], verbal_result["occurrenceCount"]), ("complete", 2))
                for literal in ("'" + measure + "'", "word " + measure):
                    query = "Count " + literal + " in the last stream"
                    self.assertEqual(requested_tiktok_show_word_count(query), measure)
                    result = count_tiktok_show_word_frequency(show(), [event(0, measure, at)], query)
                    self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 1))

    def test_bare_metric_modifiers_do_not_select_chat_words(self):
        for query in (
            "Count all comments in TikTok chat",
            "Count total taps in the last stream",
            "Count my submissions in this show",
            "Count number of gifts in the last stream",
        ):
            with self.subTest(query=query):
                self.assertEqual(requested_tiktok_show_word_count(query), "")
        for word in ("all", "total", "my", "our", "your", "every", "each",
                     "number", "these", "those", "a", "an"):
            for query in (
                'Count "' + word + '" in the last stream',
                "Count word " + word + " in the last stream",
                "How many times did TikTok chat say " + word + " in the last stream?",
                "Count panda in the last stream\nCurrent follow-up: I meant "
                + ("the word all" if word == "all" else word),
            ):
                with self.subTest(word=word, query=query):
                    self.assertEqual(requested_tiktok_show_word_count(query), word)

        # Bare "all" is an existing scope correction; an explicit word cue
        # distinguishes it from selecting the literal target above.
        self.assertEqual(requested_tiktok_show_word_count(
            "Count panda in the last stream\nCurrent follow-up: I meant all"), "panda")

    def test_unsupported_later_selector_cannot_reuse_an_earlier_word(self):
        query = ('Count the word "panda" in TikTok chat tonight.\n'
                 'Current follow-up: Actually, the word "red panda".')
        result = count_tiktok_show_word_frequency(
            show(), [event(0, "panda panda panda panda panda", stamp("2026-10-03T03:00:00Z"))], query)
        self.assertEqual(requested_tiktok_show_word_count(query), "red panda")
        self.assertEqual(result["reason"], "unsupported_word_target")
        self.assertEqual(result["status"], "unavailable")
        self.assertIsNone(result["occurrenceCount"])
        self.assertNotIn("occurrenceCount=5", render_tiktok_show_word_frequency(result))
        for literal in ('"word panda"', "'red panda'"):
            query = "Count panda in the last stream\nCurrent follow-up: Count " + literal
            result = count_tiktok_show_word_frequency(show(), [
                event(0, "panda", stamp("2026-10-03T03:00:00Z")),
            ], query)
            with self.subTest(literal=literal):
                self.assertEqual(result["reason"], "unsupported_word_target")
                self.assertIsNone(result["occurrenceCount"])

    def test_first_hour_is_not_widened_to_a_whole_stream_count(self):
        records = [
            event(0, "panda panda", stamp("2026-10-03T02:06:00Z")),
            event(1, "panda panda panda", stamp("2026-10-03T04:10:00Z")),
        ]
        result = count_tiktok_show_word_frequency(
            show(), records, "How many times did they say word panda during the first hour of TikTok?")
        self.assertEqual(result["reason"], "specific_interval_scope_not_resolved")
        self.assertEqual(result["status"], "unavailable")
        self.assertIsNone(result["occurrenceCount"])

    def test_new_target_forms_keep_literal_scope_words_inert(self):
        archive = {"latestShow": show()}
        for word in ("now", "tonight", "track", "hour"):
            records = [event(0, word + " " + word, stamp("2026-10-03T03:00:00Z"))]
            for query in (
                "Count '" + word + "' in the last stream",
                "Count panda in the last stream\nCurrent follow-up: I meant '" + word + "'",
            ):
                with self.subTest(word=word, query=query):
                    selected, owner = select_show_for_tiktok_analysis(archive, query)
                    self.assertEqual((selected["showDate"], owner), ("2026-10-02", "latestShow"))
                    result = count_tiktok_show_word_frequency(selected, records, query)
                    self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 2))

    def test_recorded_intake_count_window_preserves_broadcast_and_track_clock(self):
        selected = show()
        selected["milestones"][:0] = [
            {"eventType": "session_created", "occurredAt": "2026-10-03T01:30:00Z"},
            {"eventType": "submissions_opened", "occurredAt": "2026-10-03T01:45:00Z"},
        ]
        broadcast_bounds = show_timeline_bounds_ms(selected)
        result = count_tiktok_show_word_frequency(selected, [
            event(0, "panda", stamp("2026-10-03T01:29:59Z")),
            event(1, "panda", stamp("2026-10-03T02:00:00Z")),
            event(2, "Good drums", stamp("2026-10-03T02:30:00Z")),
        ], "Count panda in the whole TikTok stream")
        self.assertEqual((result["occurrenceCount"], result["capturedMessageCount"]), (1, 2))
        self.assertEqual(result["windowStartMs"], stamp("2026-10-03T01:30:00Z"))
        self.assertEqual(show_timeline_bounds_ms(selected), broadcast_bounds)
        ledger = build_tiktok_show_evidence_ledger(selected, [])
        self.assertEqual(ledger["startedAtMs"], broadcast_bounds[0])
        self.assertEqual(tiktok_show_word_frequency_bounds_ms(ledger), (
            stamp("2026-10-03T01:30:00Z"), broadcast_bounds[1]))

    def test_active_first_receipt_cutoff_excludes_future_originals_with_quiet_zero_control(self):
        cutoff = stamp("2026-10-03T04:18:14Z")
        selected = show()
        selected.update(status="live", milestones=selected["milestones"][:1],
                        _evidenceObservedThroughMs=cutoff)
        quiet = event(0, "Good drums", cutoff - 2000)
        future = event(1, "panda", cutoff - 1000)
        future["ingested_at_ms"] = cutoff + 1
        result = count_tiktok_show_word_frequency(selected, [quiet, future], "Count panda in this stream")
        self.assertEqual((result["status"], result["occurrenceCount"], result["capturedMessageCount"]),
                         ("complete", 0, 1))
        result = count_tiktok_show_word_frequency(selected, [future], "Count panda in this stream")
        self.assertEqual(result["status"], "unavailable")
        self.assertIsNone(result["occurrenceCount"])

    def test_active_missing_or_malformed_receipt_does_not_certify_zero_or_one(self):
        cutoff = stamp("2026-10-03T04:18:14Z")
        selected = show()
        selected.update(status="live", milestones=selected["milestones"][:1],
                        _evidenceObservedThroughMs=cutoff)
        for receipt in (None, True, 0, -1, "1790999000000", 1790999000000.5):
            for text in ("panda", "Good drums"):
                original = event(0, text, cutoff - 1000)
                original["ingested_at_ms"] = receipt
                with self.subTest(receipt=receipt, text=text):
                    result = count_tiktok_show_word_frequency(selected, [original], "Count panda in this stream")
                    self.assertEqual(result["status"], "partial")
                    self.assertIsNone(result["occurrenceCount"])

    def test_known_room_and_supplied_owner_alias_conflicts_cannot_certify_counts(self):
        cutoff = stamp("2026-10-03T04:18:14Z")
        selected = show()
        selected.update(status="live", milestones=selected["milestones"][:1],
                        roomId="test-room", _evidenceObservedThroughMs=cutoff)
        changes = (
            {"roomId": "other-room"}, {"roomId": None},
            {"sessionId": "other-session"}, {"showSessionId": "other-session"},
            {"showKey": "other-key"},
        )
        for change in changes:
            for text in ("panda", "Good drums"):
                original = event(0, text, cutoff - 1000)
                original["metadata"].update(roomId=selected["roomId"], sessionId=selected["sessionId"])
                original["metadata"].update(change)
                with self.subTest(change=change, text=text):
                    result = count_tiktok_show_word_frequency(selected, [original], "Count panda in this stream")
                    self.assertEqual(result["status"], "partial")
                    self.assertIsNone(result["occurrenceCount"])
        valid = event(0, "panda", cutoff - 1000)
        valid["metadata"].update(roomId=selected["roomId"], sessionId=selected["sessionId"],
                                 showSessionId=selected["sessionId"], showKey=selected["sessionId"])
        self.assertEqual(count_tiktok_show_word_frequency(
            selected, [valid], "Count panda in this stream")["occurrenceCount"], 1)

    def test_archived_canonical_session_key_validates_each_supplied_source_alias(self):
        selected = show()
        ledger = build_tiktok_show_evidence_ledger(selected, [])
        self.assertNotIn("sessionId", ledger)
        self.assertEqual(ledger["showKey"], selected["sessionId"])
        for alias in ("sessionId", "showSessionId"):
            for alias_value, expected_status in (
                ("other-session", "partial"), (selected["sessionId"], "complete"),
            ):
                original = event(0, "panda", stamp("2026-10-03T03:00:00Z"))
                original["metadata"][alias] = alias_value
                with self.subTest(alias=alias, value=alias_value):
                    result = count_tiktok_show_word_frequency(ledger, [original], "Count panda in the last stream")
                    self.assertEqual(result["status"], expected_status)
                    self.assertEqual(result["occurrenceCount"], 1 if expected_status == "complete" else None)

    def test_archived_missing_receipt_cannot_certify_a_count(self):
        for text in ("panda", "Good drums"):
            original = event(0, text, stamp("2026-10-03T03:00:00Z"))
            original.pop("ingested_at_ms")
            with self.subTest(text=text):
                result = count_tiktok_show_word_frequency(show(), [original], "Count panda in the last stream")
                self.assertEqual(result["status"], "partial")
                self.assertIsNone(result["occurrenceCount"])

    def test_archived_late_receipt_and_optional_legacy_aliases_remain_eligible(self):
        original = event(0, "panda", stamp("2026-10-03T03:00:00Z"))
        original["ingested_at_ms"] = stamp("2026-10-04T03:00:00Z")
        result = count_tiktok_show_word_frequency(show(), [original], "Count panda in the last stream")
        self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 1))



class TikTokArchivedWordFrequencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(gc.collect)
        self.db = str(Path(self.directory.name) / "bnl.db")
        ensure_schema(self.db)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            ensure_tiktok_show_evidence_schema(conn)
            self.latest_ledger = self.insert_episode(conn, show(), full_fixture())
            old_show = show("2026-09-25")
            old_at = stamp("2026-09-26T03:00:00Z")
            old_events = [event(i, "panda", old_at + i, "older") for i in range(205)]
            self.older_ledger = self.insert_episode(conn, old_show, old_events)

    def insert_episode(self, conn, source_show, events):
        for original in events:
            _record_on_connection(
                conn, guild_id=77, source_kind="tiktok_live_chat",
                source_key=original["event_id"], occurred_at_ms=original["occurred_at_ms"],
                raw_text=original["raw_text"], channel_policy="public_context",
                subject_ref=original["subject_ref"], private_display_name=original["private_display_name"],
                metadata=original["metadata"])
        ledger = build_tiktok_show_evidence_ledger(source_show, events)
        self.assertEqual(ledger["lifecycle"], "finalized")
        archive = {
            "available": True, "reason": None,
            "schemaVersion": "queue_public_history_projection_v1",
            "source": "queue_bnl_history_projection",
            "visibility": "public_safe", "accessScope": "public",
            "historyCoverageStartedAt": "2026-09-25", "sourceRevision": 1,
            "sourceDigest": hashlib.sha256(json.dumps(
                source_show, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8")).hexdigest(),
            "memoryDefault": "do_not_store", "sourceFileDefault": "review_evidence_only",
            "publicDossierDefault": "not_automatic", "personalHistory": None,
            "currentShow": None, "latestShow": source_show, "shows": [],
        }
        authorization = show_queue_evidence_authorization({
            "ok": True, "version": 1, "source": "barcode-network-site",
            "publicOnly": True, "accessScope": "public",
            "capabilities": {"queueProduction": True}, "sections": {"archive": archive},
        }, environ={"BNL_QUEUE_PRODUCTION_ENABLED": "true"})
        self.assertTrue(authorization["usable"], authorization["reason"])
        ledger = _seal_authorized_show_ledger(ledger, authorization["receipt"])
        self.assertIsNotNone(ledger)
        conn.execute(
            "INSERT INTO tiktok_show_evidence_ledgers(guild_id,show_key,schema_version,show_date,"
            "show_title,lifecycle_status,started_at_ms,ended_at_ms,source_digest,ledger_json,"
            "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (77, ledger["showKey"], str(ledger["schemaVersion"]), ledger["showDate"],
             ledger["showTitle"], ledger["lifecycle"], ledger["startedAtMs"], ledger["endedAtMs"],
             ledger["sourceDigest"], json.dumps(ledger), "test", "test"))
        return ledger

    def test_latest_stream_count_cannot_be_replaced_by_a_louder_older_episode(self):
        selected = {}
        context = build_tiktok_show_evidence_context(
            self.db, guild_id=77,
            user_text='How many times did they say the word "panda" during the last stream?',
            selection_out=selected)
        self.assertIn("occurrenceCount=67; matchingMessageCount=66", context)
        self.assertNotIn("occurrenceCount=205", context)
        self.assertEqual(selected["word_frequency_lookup"][0]["showKey"], self.latest_ledger["showKey"])

    def test_explicit_past_stream_date_keeps_its_own_count(self):
        context = build_tiktok_show_evidence_context(
            self.db, guild_id=77,
            user_text="Count word panda during the 2026-09-25 stream")
        self.assertIn("occurrenceCount=205; matchingMessageCount=205", context)
        self.assertNotIn("occurrenceCount=67", context)


    def test_current_counts_emit_no_competing_finalized_packet_or_reader_count(self):
        for phrase in ("this stream", "current stream", "this live", "current episode",
                       "this show", "right now", "this TikTok live", "current Tik Tok stream",
                       "this public TikTok live"):
            query = "Count word panda in " + phrase
            with self.subTest(phrase=phrase), closing(sqlite3.connect(self.db)) as conn:
                self.assertEqual(select_tiktok_show_episode_context_items(
                    conn, guild_id=77, user_text=query), ())
                selected = {}
                context = build_tiktok_show_evidence_context(
                    self.db, guild_id=77, user_text=query, selection_out=selected)
                self.assertEqual(context, "")
                self.assertNotIn("word_frequency_lookup", selected)

    def test_active_count_and_finalized_packet_have_one_current_episode_owner(self):
        active = show()
        active.update(status="live", milestones=active["milestones"][:1],
                      _evidenceObservedThroughMs=stamp("2026-10-03T04:18:14Z"))
        query = ("Count word panda in the 2026-09-25 stream"
                 "\nCurrent follow-up: Count word panda in this stream")
        archive = {"currentShow": active, "latestShow": show("2026-09-25")}
        context = build_durable_show_prompt_context(archive, full_fixture(), query)
        self.assertIn("occurrenceCount=38; matchingMessageCount=37", context)
        with closing(sqlite3.connect(self.db)) as conn:
            self.assertEqual(select_tiktok_show_episode_context_items(
                conn, guild_id=77, user_text=query), ())
        self.assertEqual(build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text=query,
            selection_user_text="Count word panda during the 2026-09-25 stream",
            pinned_show_keys=(self.older_ledger["showKey"],)), "")

    def test_current_platform_modified_live_overrides_prior_dated_scope(self):
        active = show()
        active.update(status="live", milestones=active["milestones"][:1],
                      _evidenceObservedThroughMs=stamp("2026-10-03T04:18:14Z"))
        for platform in ("TikTok", "Tik Tok"):
            query = ("Count word panda during the 2026-09-25 stream"
                     "\nCurrent follow-up: How many times did " + platform +
                     " chat say panda in this " + platform + " live?")
            with self.subTest(platform=platform):
                self.assertEqual(requested_tiktok_show_word_count(query), "panda")
                self.assertTrue(is_tiktok_show_analysis_query(query))
                selected, owner = select_show_for_tiktok_analysis(
                    {"currentShow": active, "latestShow": show("2026-09-25")}, query)
                self.assertEqual((selected["showDate"], owner), ("2026-10-02", "currentShow"))
                result = count_tiktok_show_word_frequency(selected, full_fixture(), query)
                self.assertEqual((result["status"], result["occurrenceCount"]), ("complete", 38))
                with closing(sqlite3.connect(self.db)) as conn:
                    self.assertEqual(select_tiktok_show_episode_context_items(
                        conn, guild_id=77, user_text=query), ())
                self.assertEqual(build_tiktok_show_evidence_context(
                    self.db, guild_id=77, user_text=query,
                    selection_user_text="Count word panda during the 2026-09-25 stream"), "")

    def test_explicit_current_human_past_date_retains_finalized_history(self):
        query = "Count word panda in this stream dated 2026-09-25"
        with closing(sqlite3.connect(self.db)) as conn:
            items = select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=query)
        self.assertEqual(len(items), 1)
        self.assertIn("occurrenceCount=205", items[0].text)
        self.assertIn("occurrenceCount=205", build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text=query))

    def test_current_last_stream_request_overrides_an_earlier_dated_retrieval_cue(self):
        selected = {}
        context = build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text="Count word panda during the last stream",
            selection_user_text="Count word panda during the 2026-09-25 stream",
            selection_out=selected)
        self.assertIn("occurrenceCount=67; matchingMessageCount=66", context)
        self.assertNotIn("occurrenceCount=205", context)
        self.assertEqual(selected["word_frequency_lookup"][0]["showKey"], self.latest_ledger["showKey"])

    def test_frequency_refresh_preserves_the_generations_selected_root(self):
        context = build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text="Count word panda during the last stream",
            selection_user_text="Count word panda during the last stream",
            pinned_show_keys=(self.older_ledger["showKey"],))
        self.assertIn("occurrenceCount=205", context)
        self.assertNotIn("occurrenceCount=67", context)

    def test_shared_packet_frequency_revalidates_fresh_original_eligibility(self):
        query = "Count word panda during the last stream"
        with closing(sqlite3.connect(self.db)) as conn:
            items = select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=query)
            self.assertEqual(len(items), 1)
            self.assertIn("occurrenceCount=67", items[0].text)
            source_ref, initial_version = items[0].source_ref, items[0].source_digest
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("DROP TRIGGER trg_bnl_journal_sources_no_update")
            conn.execute("UPDATE bnl_journal_source_events SET public_usable=0 WHERE source_key='current-0'")
        with closing(sqlite3.connect(self.db)) as conn:
            updated_version = tiktok_show_episode_context_item_version(
                conn, guild_id=77, user_text=query, subject_user_id=0, source_ref=source_ref)
            items = select_tiktok_show_episode_context_items(conn, guild_id=77, user_text=query)
        self.assertNotEqual(initial_version, updated_version)
        self.assertIn("occurrenceCount=65; matchingMessageCount=65", items[0].text)

    def test_original_scan_limit_or_read_failure_does_not_certify_zero(self):
        def partial(_conn, *, diagnostics_out, **_kwargs):
            diagnostics_out.update(status="partial", reason="source_event_limit_exceeded")
            return None

        with closing(sqlite3.connect(self.db)) as conn:
            with mock.patch("bnl_tiktok_show_ledger._load_show_source_events", side_effect=partial):
                result = _lookup_tiktok_show_word_frequency(
                    "", guild_id=77, ledger=self.latest_ledger,
                    user_text="Count word panda in the last stream", source_conn=conn)
        self.assertEqual(result["status"], "partial")
        self.assertIsNone(result["occurrenceCount"])
        self.assertNotIn("occurrenceCount=0", render_tiktok_show_word_frequency(result))



class TikTokCurrentWordCountRequestRegressionTests(unittest.TestCase):
    """One human word selector must survive current-episode source handoff."""

    @staticmethod
    def _eleven_originals():
        texts = (
            "Butt butt!", "butt", "BUTT?", "butt, butts", "no butt?",
            "butt... butt", "butts", "BUTTS!", "butter", "about", "butt",
        )
        return [event(index, text, stamp("2026-10-03T03:00:00Z") + index * 1000,
                      prefix="current-butt") for index, text in enumerate(texts)]

    @staticmethod
    def _active_show():
        current = show()
        current.update(status="open", milestones=current["milestones"][:1],
                       _evidenceObservedThroughMs=stamp("2026-10-03T04:18:14Z"))
        return current

    def test_target_before_word_count_uses_the_existing_show_request_owner(self):
        for query, target in (
            ("BNL butt word count. Tonight\u2019s show. Go.", "butt"),
            ("BNL, butts word count in this stream.", "butts"),
            ('"butt" word count during the last TikTok stream', "butt"),
        ):
            with self.subTest(query=query):
                self.assertEqual(requested_tiktok_show_word_count(query), target)
                self.assertTrue(is_tiktok_show_analysis_query(query))
        self.assertEqual(requested_tiktok_show_word_count(
            "What is the word count in TikTok chat?"), "")

    def test_targetless_current_stream_correction_is_an_eligible_human_followup(self):
        self.assertTrue(is_tiktok_show_analysis_followup("Current stream not last stream"))
        query = ("Count word butt in the last TikTok stream\n"
                 "Current follow-up: Current stream not last stream")
        self.assertEqual(requested_tiktok_show_word_count(query), "butt")

    def test_affirmative_current_stream_beats_negated_last_stream(self):
        active = self._active_show()
        older = show("2026-09-25")
        query = ("Count word butt in the last TikTok stream\n"
                 "Current follow-up: Current stream not last stream")
        selected, source = select_show_for_tiktok_analysis(
            {"currentShow": active, "latestShow": older, "shows": [older]}, query,
            now=datetime(2026, 10, 3, 4, 18, 14, tzinfo=timezone.utc))
        self.assertEqual((selected.get("sessionId"), source),
                         (active["sessionId"], "currentShow"))

    def test_eleven_originals_measure_singular_and_plural_independently(self):
        records = self._eleven_originals()
        results = [count_tiktok_show_word_frequency(
            show(), records, "Count word " + target + " in this stream")
            for target in ("butt", "butts")]
        singular, plural = results
        self.assertEqual((singular["status"], singular["occurrenceCount"],
                          singular["matchingMessageCount"]), ("complete", 9, 7))
        self.assertEqual((plural["status"], plural["occurrenceCount"],
                          plural["matchingMessageCount"]), ("complete", 3, 3))
        for result in results:
            self.assertEqual(result["capturedMessageCount"], 11)
            self.assertEqual(len(result["originalSourceRefs"]), 11)
            self.assertEqual(result["sessionId"], show()["sessionId"])
            self.assertEqual((result["windowStartMs"], result["windowEndMs"]),
                             tiktok_show_word_frequency_bounds_ms(show()))
        self.assertNotEqual(singular["sourceDigest"], plural["sourceDigest"])

    def test_active_count_freezes_first_receipts_without_conflating_plural(self):
        active = self._active_show()
        records = self._eleven_originals()
        records[-1]["ingested_at_ms"] = active["_evidenceObservedThroughMs"] + 1
        singular = count_tiktok_show_word_frequency(
            active, records, "Count word butt in this stream")
        plural = count_tiktok_show_word_frequency(
            active, records, "Count word butts in this stream")
        self.assertEqual((singular["status"], singular["capturedMessageCount"],
                          singular["occurrenceCount"], singular["matchingMessageCount"]),
                         ("complete", 10, 8, 6))
        self.assertEqual((plural["occurrenceCount"], plural["matchingMessageCount"]), (3, 3))
        self.assertEqual(singular["windowEndMs"], active["_evidenceObservedThroughMs"])
        self.assertNotIn(records[-1]["event_id"],
                         [ref["sourceKey"] for ref in singular["originalSourceRefs"]])

    def test_missing_and_partial_originals_cannot_replace_a_verified_zero(self):
        records = self._eleven_originals()
        partial = [dict(item) for item in records]
        partial[-1]["ingested_at_ms"] = None
        cases = ((None, "unavailable"), ([], "unavailable"), (partial, "partial"))
        for originals, status in cases:
            with self.subTest(status=status, source_missing=originals is None):
                result = count_tiktok_show_word_frequency(
                    show(), originals, "Count word goat in this stream")
                self.assertEqual(result["status"], status)
                self.assertIsNone(result["occurrenceCount"])
                self.assertNotIn("occurrenceCount=0", render_tiktok_show_word_frequency(result))
        zero = count_tiktok_show_word_frequency(show(), records, "Count word goat in this stream")
        self.assertEqual((zero["status"], zero["capturedMessageCount"],
                          zero["occurrenceCount"]), ("complete", 11, 0))

    def test_unavailable_count_prompt_keeps_the_requested_word_and_episode(self):
        result = count_tiktok_show_word_frequency(show(), None, "Count word butt in this stream")
        rendered = render_tiktok_show_word_frequency(result)
        self.assertIn('"butt"', rendered)
        self.assertIn(show()["sessionId"], rendered)
        self.assertIn("Coverage=unavailable", rendered)
        self.assertNotIn("occurrenceCount=0", rendered)

    def test_packet_relative_night_keeps_the_native_resolved_word_request(self):
        from types import SimpleNamespace
        from bnl_unified_intelligence_packet import _show_episode_query

        current = "Tonight\u2019s show. Go."
        resolved = "Count word butt in TikTok chat\nCurrent follow-up: " + current
        request = SimpleNamespace(
            user_text=current, now=datetime(2026, 10, 3, 7, 1, tzinfo=timezone.utc),
            show_episode_artist_request=None, show_episode_selection_text=resolved,
            show_episode_dates=("2026-10-02",))
        packet_query = _show_episode_query(request)
        self.assertEqual(requested_tiktok_show_word_count(packet_query), "butt")
        self.assertIn("Current follow-up: " + current, packet_query)
        self.assertIn("2026-10-02", packet_query)

    def test_packet_current_explicit_history_keeps_its_own_word_and_date(self):
        from types import SimpleNamespace
        from bnl_unified_intelligence_packet import _show_episode_query

        current = "Count word butts during the September 25, 2026 TikTok stream"
        request = SimpleNamespace(
            user_text=current, now=datetime(2026, 10, 3, 7, 1, tzinfo=timezone.utc),
            show_episode_artist_request=None,
            show_episode_selection_text="Count word butt during the October 2, 2026 stream",
            show_episode_dates=("2026-10-02",))
        packet_query = _show_episode_query(request)
        self.assertEqual(packet_query, current)
        self.assertEqual(requested_tiktok_show_word_count(packet_query), "butts")

if __name__ == "__main__":
    unittest.main()
