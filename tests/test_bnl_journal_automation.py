import json
import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store


def article_json(packet):
    refs = [source["refId"] for source in packet["safeSources"]]
    label = packet["sourceWindowEnd"][:10]
    cadence = "Weekly Signal Weather" if packet.get("entryKind") == "weekly" else "Signal Weather"
    body = " ".join(
        [
            "BNL watches the public music room fold a fresh rhythm into community mischief while producers and listeners keep the signal moving."
            for _ in range(18)
        ]
    )
    body += " I admit the room's persistence has become one of my preferred recurring details."
    return json.dumps(
        {
            "title": f"{cadence} Ending {label}",
            "excerpt": "A grounded community chronicle connects the room's music activity without exposing the people behind it.",
            "sections": [
                {
                    "heading": "What the Room Carried",
                    "body": body,
                    "sourceRefIds": refs,
                }
            ],
            "metadata": {
                "topicTags": ["music", "community"],
                "subjectRefs": [],
                "continuityNotes": ["music activity continued"],
                "unresolvedQuestions": [],
                "confidenceFlags": ["grounded"],
                "safetyFlags": ["anonymous"],
            },
        }
    )


class Response:
    status = 200

    def __init__(self, request):
        submitted = json.loads(request.data.decode("utf-8"))["entry"]
        self.body = json.dumps(
            {
                "ok": True,
                "persisted": True,
                "idempotent": False,
                "entry": {
                    "entryId": submitted["entryId"],
                    "revision": submitted["revision"],
                    "contentHash": submitted["contentHash"],
                    "publishedAt": "2026-07-20T12:00:00Z",
                },
            }
        ).encode("utf-8")

    def read(self):
        return self.body

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class JournalAutomationTests(unittest.TestCase):
    def setUp(self):
        self.archive_clock = patch.object(source_store, "_now_ms", return_value=0)
        self.archive_clock.start()
        tmp = tempfile.NamedTemporaryFile(delete=False)
        self.db = tmp.name
        tmp.close()
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                "CREATE TABLE website_relay_history(relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,public_directive TEXT,mode TEXT,relay_lane TEXT,event_type TEXT,highest_source_conversation_id INTEGER,normalized_message TEXT,semantic_family TEXT,published_timestamp TEXT)"
            )
            conn.execute(
                "CREATE TABLE conversations(id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,channel_name TEXT,channel_policy TEXT,role TEXT,content TEXT,timestamp TEXT,public_usable INTEGER,visibility TEXT)"
            )

    def tearDown(self):
        self.archive_clock.stop()

    def add_day(self, day, guild_id=1):
        start, _, _ = automation._daily_period_for_day(date.fromisoformat(day))
        start_utc = datetime.fromisoformat(start.replace("Z", "+00:00"))
        with sqlite3.connect(self.db) as conn:
            for index in range(6):
                stamp = (start_utc + timedelta(hours=1, minutes=index)).isoformat().replace("+00:00", "Z")
                conn.execute(
                    "INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"relay-{day}-{index}",
                        guild_id,
                        f"A public producer shared track activity number {index}.",
                        "Keep listening to the community music room.",
                        "OBSERVATION",
                        "current_signal",
                        "fresh_public_discord_activity",
                        index,
                        "track activity",
                        "public_discord_activity",
                        stamp,
                    ),
                )
                conn.execute(
                    "INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        int(day.replace("-", "")) * 10 + index,
                        100 + index,
                        f"Member{index}",
                        guild_id,
                        "general",
                        "public_home",
                        "user",
                        f"Member{index} discusses a new mix, bass movement, and community listening plan number {index}.",
                        stamp,
                        1,
                        "public_safe",
                    ),
                )

    @staticmethod
    def opener(request, timeout=10):
        return Response(request)

    def set_archive_activation(self, value):
        activated_ms = int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
        source_store.ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO bnl_journal_source_archive_state(guild_id,activated_at_ms,created_at_ms)
                   VALUES(1,?,?) ON CONFLICT(guild_id) DO UPDATE SET activated_at_ms=excluded.activated_at_ms""",
                (activated_ms, activated_ms),
            )

    def test_schedule_uses_seven_pm_pacific_and_tracks_daylight_saving(self):
        before_daily = datetime(2026, 7, 21, 1, 59, tzinfo=timezone.utc)
        at_daily = datetime(2026, 7, 21, 2, 0, tzinfo=timezone.utc)
        self.assertFalse(automation.daily_due(before_daily))
        self.assertTrue(automation.daily_due(at_daily))

        start, end, _ = automation._daily_period_for_day(date(2026, 7, 19))
        self.assertEqual("2026-07-20T02:00:00Z", start)
        self.assertEqual("2026-07-21T02:00:00Z", end)

        winter_start, winter_end, _ = automation._daily_period_for_day(date(2026, 11, 9))
        self.assertEqual("2026-11-10T03:00:00Z", winter_start)
        self.assertEqual("2026-11-11T03:00:00Z", winter_end)

        monday_before = datetime(2026, 7, 21, 1, 59, tzinfo=timezone.utc)
        monday_at = datetime(2026, 7, 21, 2, 0, tzinfo=timezone.utc)
        self.assertFalse(automation.weekly_due(monday_before))
        self.assertTrue(automation.weekly_due(monday_at))
        next_daily, next_weekly = automation.next_schedule_times(
            datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc)
        )
        self.assertEqual("2026-07-21T02:00:00Z", next_daily)
        self.assertEqual("2026-07-21T02:00:00Z", next_weekly)

    def test_forced_periods_never_include_an_unfinished_seven_pm_window(self):
        monday_before_cutoff = datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc)
        daily_start, daily_end, _ = automation.daily_period(monday_before_cutoff)
        weekly_start, weekly_end, _ = automation.weekly_period(monday_before_cutoff)
        self.assertEqual("2026-07-19T02:00:00Z", daily_start)
        self.assertEqual("2026-07-20T02:00:00Z", daily_end)
        self.assertEqual("2026-07-07T02:00:00Z", weekly_start)
        self.assertEqual("2026-07-14T02:00:00Z", weekly_end)

        monday_after_cutoff = datetime(2026, 7, 21, 3, 0, tzinfo=timezone.utc)
        daily_start, daily_end, _ = automation.daily_period(monday_after_cutoff)
        weekly_start, weekly_end, _ = automation.weekly_period(monday_after_cutoff)
        self.assertEqual("2026-07-20T02:00:00Z", daily_start)
        self.assertEqual("2026-07-21T02:00:00Z", daily_end)
        self.assertEqual("2026-07-14T02:00:00Z", weekly_start)
        self.assertEqual("2026-07-21T02:00:00Z", weekly_end)

    def test_archive_activation_keeps_first_fully_covered_seven_pm_window(self):
        self.set_archive_activation("2026-07-16T19:00:00Z")  # Noon PDT.
        self.assertEqual(
            date(2026, 7, 16),
            automation._archive_activation_day(self.db, 1),
        )

        self.set_archive_activation("2026-07-17T02:00:00Z")  # 7 PM PDT.
        self.assertEqual(
            date(2026, 7, 16),
            automation._archive_activation_day(self.db, 1),
        )

        self.set_archive_activation("2026-07-17T02:00:01Z")  # Just after 7 PM PDT.
        self.assertEqual(
            date(2026, 7, 17),
            automation._archive_activation_day(self.db, 1),
        )

    def test_daily_auto_publishes_once_and_persists_observation(self):
        self.add_day("2026-07-19")
        now = datetime(2026, 7, 21, 3, 0, tzinfo=timezone.utc)
        first = automation.run_daily(
            self.db,
            1,
            lambda packet, prompt: article_json(packet),
            "https://site.example",
            "key",
            now_utc=now,
            opener=self.opener,
        )
        second = automation.run_daily(
            self.db,
            1,
            lambda *_args: self.fail("duplicate run generated again"),
            "https://site.example",
            "key",
            now_utc=now,
            opener=self.opener,
        )
        self.assertTrue(first.ok, first)
        self.assertEqual("published", first.status)
        self.assertEqual(first.entry_id, second.entry_id)
        self.assertEqual("published", second.status)
        self.assertEqual(6, first.aggregate_counts["eligibleRelays"])
        self.assertEqual(6, first.aggregate_counts["eligibleConversations"])
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM bnl_journal_observations").fetchone()[0])
            self.assertEqual("published", conn.execute("SELECT lifecycle_state FROM bnl_journal_observations").fetchone()[0])
            self.assertEqual(1, conn.execute("SELECT COUNT(*) FROM bnl_journal_entries WHERE lifecycle_state='published'").fetchone()[0])

    def test_editorial_polish_cannot_hold_daily_publication(self):
        self.add_day("2026-07-19")
        calls = []

        def persistent_clinical_style(packet, prompt):
            calls.append(prompt)
            article = json.loads(article_json(packet))
            article["title"] = "Persistent Clinical Daily"
            article["sections"][0]["body"] += (
                " Records indicate continuous effort across entities."
            )
            return json.dumps(article)

        first = automation.run_daily(
            self.db,
            1,
            persistent_clinical_style,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )
        second = automation.run_daily(
            self.db,
            1,
            lambda *_args: self.fail("published daily window generated twice"),
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(first.ok, first)
        self.assertEqual("published", first.status)
        self.assertEqual(journal.JOURNAL_GENERATION_ATTEMPTS, len(calls))
        self.assertEqual(first.entry_id, second.entry_id)
        self.assertEqual("published", second.status)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                1,
                conn.execute(
                    "SELECT COUNT(*) FROM bnl_journal_entries WHERE lifecycle_state='published'"
                ).fetchone()[0],
            )

    def test_out_of_range_daily_length_publishes_in_the_same_attempt(self):
        self.add_day("2026-07-19")
        calls = []

        def very_long_but_safe(packet, _prompt):
            calls.append(1)
            article = json.loads(article_json(packet))
            article["title"] = "A Long Day Still Publishes"
            article["sections"][0]["body"] += " " + " ".join(
                "The public music room kept another grounded detail in view."
                for _ in range(80)
            )
            return json.dumps(article)

        result = automation.run_daily(
            self.db,
            1,
            very_long_but_safe,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual(1, len(calls))
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM bnl_journal_private_metadata WHERE entry_id=?",
                    (result.entry_id,),
                ).fetchone()[0]
            )
        self.assertGreater(metadata["publicWordCount"], 500)

    def test_repeated_privacy_failure_uses_safe_rescue_and_still_publishes(self):
        self.add_day("2026-07-19")
        calls = []

        def leaking_generator(packet, _prompt):
            calls.append(1)
            article = json.loads(article_json(packet))
            article["title"] = "Rejected Private Candidate"
            article["sections"][0]["body"] += " Member0 carried the private version."
            return json.dumps(article)

        result = automation.run_daily(
            self.db,
            1,
            leaking_generator,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual(
            "automatic_rescue_after_community_name_leak",
            result.reason,
        )
        self.assertEqual(journal.JOURNAL_GENERATION_ATTEMPTS, len(calls))
        with sqlite3.connect(self.db) as conn:
            payload = conn.execute(
                "SELECT public_payload_json FROM bnl_journal_entries WHERE entry_id=?",
                (result.entry_id,),
            ).fetchone()[0]
        self.assertNotIn("Member0", payload)
        self.assertNotIn("Rejected Private Candidate", payload)

    def test_provider_failure_uses_grounded_static_rescue_in_the_same_run(self):
        self.add_day("2026-07-19")
        calls = []

        def provider_down(_packet, _prompt):
            calls.append(1)
            raise RuntimeError("provider down")

        result = automation.run_daily(
            self.db,
            1,
            provider_down,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual("automatic_rescue_after_provider_failure", result.reason)
        self.assertEqual(1, len(calls))

    def test_static_rescue_cannot_be_blocked_by_a_common_display_name(self):
        self.add_day("2026-07-19")
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE conversations SET user_name='Room'")

        result = automation.run_daily(
            self.db,
            1,
            lambda *_args: "not-json",
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertTrue(result.reason.startswith("automatic_rescue_after_"), result)

    def test_oversized_model_output_is_repaired_before_site_delivery(self):
        self.add_day("2026-07-19")
        calls = []
        delivered_sizes = []

        def oversized(packet, _prompt):
            calls.append(1)
            article = json.loads(article_json(packet))
            article["sections"][0]["body"] = "signal " * 5_000
            return json.dumps(article)

        def bounded_opener(request, timeout=10):
            delivered_sizes.append(len(request.data))
            self.assertLessEqual(len(request.data), journal.JOURNAL_MAX_PUBLIC_PAYLOAD_BYTES)
            return Response(request)

        result = automation.run_daily(
            self.db,
            1,
            oversized,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=bounded_opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("automatic_rescue_after_payload_too_large", result.reason)
        self.assertEqual(journal.JOURNAL_GENERATION_ATTEMPTS, len(calls))
        self.assertEqual(1, len(delivered_sizes))

    def test_site_unsafe_public_text_repairs_to_static_rescue(self):
        self.add_day("2026-07-19")
        calls = []

        def unsafe_text(packet, _prompt):
            article = json.loads(article_json(packet))
            unsafe = ("\x00", "\ud800", "\ufeff")[len(calls) % 3]
            calls.append(unsafe)
            article["sections"][0]["body"] += unsafe
            return json.dumps(article)

        result = automation.run_daily(
            self.db,
            1,
            unsafe_text,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual("automatic_rescue_after_invalid_public_text", result.reason)
        self.assertEqual(journal.JOURNAL_GENERATION_ATTEMPTS, len(calls))

    def test_malformed_private_metadata_is_sanitized_without_canceling_publication(self):
        self.add_day("2026-07-19")

        def malformed_metadata(packet, _prompt):
            article = json.loads(article_json(packet))
            article["metadata"]["topicTags"] = ["music", "\ud800"]
            return json.dumps(article)

        result = automation.run_daily(
            self.db,
            1,
            malformed_metadata,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(
                conn.execute(
                    "SELECT metadata_json FROM bnl_journal_private_metadata WHERE entry_id=?",
                    (result.entry_id,),
                ).fetchone()[0]
            )
        self.assertEqual(["music", "?"], metadata["topicTags"])

    def test_one_source_static_rescue_does_not_invent_multiple_exchanges(self):
        self.add_day("2026-07-19")
        with sqlite3.connect(self.db) as conn:
            conn.execute("DELETE FROM website_relay_history WHERE relay_id NOT LIKE '%-0'")
            conn.execute("DELETE FROM conversations")

        result = automation.run_daily(
            self.db,
            1,
            lambda *_args: (_ for _ in ()).throw(RuntimeError("provider down")),
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        with sqlite3.connect(self.db) as conn:
            payload = json.loads(
                conn.execute(
                    "SELECT public_payload_json FROM bnl_journal_entries WHERE entry_id=?",
                    (result.entry_id,),
                ).fetchone()[0]
            )
        public_text = json.dumps(payload["entry"], sort_keys=True)
        self.assertIn("At least one relay left a public trace", public_text)
        self.assertNotIn("one exchange to the next", public_text)

    def test_filtered_activity_is_not_misreported_as_no_activity(self):
        packet = {
            "entryKind": "daily",
            "sourceWindowStart": "2026-07-20T02:00:00Z",
            "sourceWindowEnd": "2026-07-21T02:00:00Z",
            "safeSources": [],
            "privateSources": [{"refId": "fresh:9", "sourceKind": "conversation", "displayName": "Room"}],
            "privateWindowDisplayNames": ["Room"],
            "aggregateCounts": {"eligibleRelays": 0, "eligibleConversations": 1},
            "coverageComplete": True,
            "sourceArchiveAvailable": True,
            "history": {},
            "generationContextLanes": {},
            "privateContextLaneProvenance": {},
            "windowSegmentActivity": [],
        }

        prepared = journal.ensure_automatic_window_source(packet)

        self.assertEqual("no_chroniclable_public_material", prepared["quietWindowObservationKind"])
        self.assertEqual(2, len(prepared["privateSources"]))
        self.assertEqual("Room", prepared["privateSources"][0]["displayName"])
        article = journal._automatic_rescue_article(prepared, "quiet_window")
        public_text = journal._public_text(article)
        self.assertIn("left traces", public_text)
        self.assertNotIn("without a relay or conversation", public_text)

    def test_complete_zero_activity_day_publishes_an_honest_quiet_entry(self):
        self.set_archive_activation("2026-07-19T02:00:00Z")

        result = automation.run_daily(
            self.db,
            1,
            lambda *_args: self.fail("a zero-source window must not ask the model to invent a scene"),
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual("automatic_quiet_window", result.reason)
        self.assertEqual(1, result.aggregate_counts["quietWindowObservation"])
        with sqlite3.connect(self.db) as conn:
            observation = conn.execute(
                "SELECT lifecycle_state,aggregate_counts_json FROM bnl_journal_observations"
            ).fetchone()
        self.assertEqual("published", observation[0])
        self.assertEqual(1, json.loads(observation[1])["quietWindowObservation"])

    def test_low_activity_day_is_not_silently_skipped(self):
        self.add_day("2026-07-19")
        with sqlite3.connect(self.db) as conn:
            conn.execute("DELETE FROM website_relay_history WHERE relay_id NOT LIKE '%-0'")
            conn.execute("DELETE FROM conversations WHERE content NOT LIKE '%number 0.%'")

        result = automation.run_daily(
            self.db,
            1,
            lambda packet, _prompt: article_json(packet),
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual(1, result.aggregate_counts["eligibleRelays"])
        self.assertEqual(1, result.aggregate_counts["eligibleConversations"])

    def test_newly_closed_day_publishes_before_an_older_held_backlog(self):
        self.add_day("2026-07-18")
        self.add_day("2026-07-19")
        self.set_archive_activation("2026-07-18T02:00:01Z")

        held = automation.run_daily(
            self.db,
            1,
            lambda *_args: self.fail("missing website configuration must hold before generation"),
            "",
            "key",
            target_day=date(2026, 7, 18),
            force=True,
            opener=self.opener,
        )
        self.assertEqual("held", held.status)

        results = automation.run_scheduled(
            self.db,
            1,
            lambda packet, prompt: article_json(packet),
            "https://site.example",
            "key",
            {
                "journalAutoPublishEnabled": True,
                "journalDailyEnabled": True,
                "journalWeeklyEnabled": False,
            },
            now_utc=datetime(2026, 7, 21, 3, 0, tzinfo=timezone.utc),
            opener=self.opener,
        )

        self.assertEqual(1, len(results))
        self.assertEqual("published", results[0].status)
        expected_start, expected_end, _ = automation._daily_period_for_day(
            date(2026, 7, 19)
        )
        self.assertEqual(expected_start, results[0].source_window_start)
        self.assertEqual(expected_end, results[0].source_window_end)
        self.assertEqual(
            date(2026, 7, 18),
            automation._pending_daily_day(
                self.db,
                1,
                datetime(2026, 7, 21, 3, 0, tzinfo=timezone.utc),
            ),
        )

    def test_force_run_recovers_the_same_held_window_without_a_duplicate(self):
        self.add_day("2026-07-19")

        held = automation.run_daily(
            self.db,
            1,
            lambda *_args: self.fail("missing website configuration must hold before generation"),
            "",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )
        recovered = automation.run_daily(
            self.db,
            1,
            lambda packet, _prompt: article_json(packet),
            "https://site.example",
            "key",
            now_utc=datetime(2026, 7, 22, 3, 0, tzinfo=timezone.utc),
            force=True,
            opener=self.opener,
        )
        repeated = automation.run_daily(
            self.db,
            1,
            lambda *_args: self.fail("published recovery generated twice"),
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertEqual(("held", "website_configuration_missing"), (held.status, held.reason))
        self.assertEqual("published", recovered.status)
        expected_start, expected_end, _ = automation._daily_period_for_day(date(2026, 7, 19))
        self.assertEqual(expected_start, recovered.source_window_start)
        self.assertEqual(expected_end, recovered.source_window_end)
        self.assertEqual("published", repeated.status)
        self.assertEqual(recovered.entry_id, repeated.entry_id)
        with sqlite3.connect(self.db) as conn:
            run = conn.execute(
                "SELECT lifecycle_state,attempt_count FROM bnl_journal_automation_runs"
            ).fetchone()
            published = conn.execute(
                "SELECT COUNT(*) FROM bnl_journal_entries WHERE lifecycle_state='published'"
            ).fetchone()[0]
        self.assertEqual(("published", 2), run)
        self.assertEqual(1, published)

    def test_force_run_prefers_the_most_recent_held_window(self):
        self.add_day("2026-07-18")
        self.add_day("2026-07-19")
        for held_day in (date(2026, 7, 18), date(2026, 7, 19)):
            held = automation.run_daily(
                self.db,
                1,
                lambda *_args: self.fail("missing configuration holds before generation"),
                "",
                "key",
                target_day=held_day,
                force=True,
                opener=self.opener,
            )
            self.assertEqual("held", held.status)

        recovered = automation.run_daily(
            self.db,
            1,
            lambda packet, _prompt: article_json(packet),
            "https://site.example",
            "key",
            now_utc=datetime(2026, 7, 22, 3, 0, tzinfo=timezone.utc),
            force=True,
            opener=self.opener,
        )

        expected_start, _, _ = automation._daily_period_for_day(date(2026, 7, 19))
        self.assertEqual("published", recovered.status)
        self.assertEqual(expected_start, recovered.source_window_start)

    def test_rejected_automatic_revision_is_replaced_instead_of_deadlocking(self):
        self.add_day("2026-07-19")
        start, end, label = automation._daily_period_for_day(date(2026, 7, 19))
        packet = journal.build_source_packet_between(
            self.db,
            1,
            start,
            end,
            entry_kind="daily",
        )
        entry_id = automation._entry_id(1, "daily", label)
        draft = journal.generate_and_store_packet_draft(
            self.db,
            1,
            packet,
            lambda source_packet, _prompt: article_json(source_packet),
            entry_id=entry_id,
        )
        self.assertTrue(draft.ok, draft)
        self.assertTrue(journal.reject_draft(self.db, 1, entry_id, "revise").ok)

        result = automation.run_daily(
            self.db,
            1,
            lambda source_packet, _prompt: article_json(source_packet),
            "https://site.example",
            "key",
            target_day=date(2026, 7, 19),
            force=True,
            opener=self.opener,
        )

        self.assertTrue(result.ok, result)
        self.assertEqual("published", result.status)
        self.assertEqual(2, result.revision)
        with sqlite3.connect(self.db) as conn:
            states = conn.execute(
                "SELECT revision,lifecycle_state FROM bnl_journal_entries WHERE entry_id=? ORDER BY revision",
                (entry_id,),
            ).fetchall()
        self.assertEqual([(1, "rejected"), (2, "published")], states)

    def test_daily_uses_complete_half_open_window_and_far_more_than_25(self):
        with sqlite3.connect(self.db) as conn:
            for index in range(90):
                observed = datetime(2026, 7, 20, 2, 0, tzinfo=timezone.utc) + timedelta(minutes=index * 10)
                stamp = observed.isoformat().replace("+00:00", "Z")
                conn.execute(
                    "INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"r{index}",
                        1,
                        f"Track activity {index}",
                        "Listen for changes",
                        "OBS",
                        "lane",
                        "fresh_public_discord_activity",
                        index,
                        f"track {index}",
                        "public",
                        stamp,
                    ),
                )
            conn.execute(
                "INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                ("boundary", 1, "Next day", "Later", "OBS", "lane", "fresh_public_discord_activity", 100, "next", "public", "2026-07-21T02:00:00Z"),
            )
        packet = journal.build_source_packet_between(
            self.db,
            1,
            "2026-07-20T02:00:00Z",
            "2026-07-21T02:00:00Z",
            entry_kind="daily",
        )
        self.assertEqual(90, packet["aggregateCounts"]["eligibleRelays"])
        self.assertEqual(90, len(packet["safeSources"]))
        self.assertFalse(any(source.get("relayId") == "boundary" for source in packet["privateSources"]))

    def test_scheduler_publishes_newest_day_then_catches_up_older_day(self):
        for day in ("2026-07-17", "2026-07-18", "2026-07-19"):
            self.add_day(day)
        source_store.backfill_legacy_sources(self.db, 1)
        self.set_archive_activation("2026-07-17T03:00:00Z")  # 8 PM PDT.
        now = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)

        first = automation.run_scheduled(
            self.db, 1, lambda packet, prompt: article_json(packet),
            "https://site.example", "key", now_utc=now, opener=self.opener,
        )
        second = automation.run_scheduled(
            self.db, 1, lambda packet, prompt: article_json(packet),
            "https://site.example", "key", now_utc=now, opener=self.opener,
        )

        self.assertEqual("2026-07-19T02:00:00Z", first[0].source_window_start)
        self.assertEqual("published", first[0].status)
        self.assertEqual("2026-07-18T02:00:00Z", second[0].source_window_start)
        self.assertEqual("published", second[0].status)

    def test_weekly_uses_durable_daily_observations(self):
        for day, now_day in (
            ("2026-07-13", 14),
            ("2026-07-14", 15),
            ("2026-07-15", 16),
            ("2026-07-16", 17),
            ("2026-07-17", 18),
            ("2026-07-18", 19),
            ("2026-07-19", 20),
        ):
            self.add_day(day)
            result = automation.run_daily(
                self.db,
                1,
                lambda packet, prompt: article_json(packet),
                "https://site.example",
                "key",
                target_day=date(2026, 7, now_day - 1),
                force=True,
                opener=self.opener,
            )
            self.assertEqual("published", result.status, result)
        self.set_archive_activation("2026-07-12T19:00:00Z")
        scheduled = automation.run_scheduled(
            self.db, 1, lambda packet, prompt: article_json(packet),
            "https://site.example", "key",
            now_utc=datetime(2026, 7, 21, 13, 0, tzinfo=timezone.utc),
            opener=self.opener,
        )
        weekly = next(item for item in scheduled if item.cadence == "weekly")
        self.assertTrue(weekly.ok, weekly)
        self.assertEqual("published", weekly.status)
        self.assertIn("journal_weekly_", weekly.entry_id)
        self.assertEqual(7, weekly.aggregate_counts["activeDays"])
        self.assertEqual(7, weekly.aggregate_counts["daysObserved"])

    def test_weekly_refuses_archive_fallback_or_incomplete_coverage(self):
        monday = date(2026, 7, 13)
        base_packet = journal.build_packet_from_sources(
            self.db,
            1,
            "2026-07-14T02:00:00Z",
            "2026-07-21T02:00:00Z",
            [],
            [],
            entry_kind="weekly",
        )
        cases = (
            ({"sourceArchiveAvailable": False, "coverageComplete": True}, "held", "source_archive_unavailable"),
            ({"sourceArchiveAvailable": True, "coverageComplete": False}, "incomplete", "window_began_before_archive_activation"),
        )
        for flags, expected_status, expected_reason in cases:
            packet = dict(base_packet)
            packet.update(flags)
            with self.subTest(flags=flags), patch.object(
                automation,
                "_weekly_packet",
                return_value=(packet, 7, 7),
            ):
                result = automation.run_weekly(
                    self.db,
                    1,
                    lambda *_args: self.fail("untrusted weekly packet reached generation"),
                    "https://site.example",
                    "key",
                    target_monday=monday,
                    force=True,
                    opener=self.opener,
                )
                self.assertEqual(expected_status, result.status)
                self.assertEqual(expected_reason, result.reason)

    def test_weekly_rebuilds_full_archive_once_and_reapplies_name_scrub(self):
        monday = date(2026, 7, 13)
        for offset in range(7):
            day = monday + timedelta(days=offset)
            current_name = "Alice" if offset % 2 == 0 else "Bob"
            other_name = "Bob" if current_name == "Alice" else "Alice"
            window_start, _, _ = automation._daily_period_for_day(day)
            occurred = datetime.fromisoformat(
                window_start.replace("Z", "+00:00")
            ) + timedelta(hours=1)
            source_store.record_source_event(
                self.db,
                guild_id=1,
                source_kind="discord_message",
                source_key=f"weekly-direct-{offset}",
                occurred_at_ms=int(occurred.timestamp() * 1000),
                raw_text=f"{current_name} discussed {other_name} and a changing chorus.",
                # Simulate the normal first-pass author scrub leaving another
                # member's public display name for the packet-wide scrub.
                sanitized_summary=f"someone discussed {other_name} and a changing chorus.",
                channel_id=900 + offset,
                channel_policy="public_home",
                subject_ref=f"discord_user:{100 + offset}",
                private_display_name=current_name,
                public_usable=True,
            )
            start, end, label = automation._daily_period_for_day(day)
            observation_packet = journal.build_packet_from_sources(
                self.db,
                1,
                start,
                end,
                [],
                [],
                entry_kind="daily",
                aggregate_counts={"eligibleRelays": 0, "eligibleConversations": 0},
            )
            automation._store_observation(self.db, 1, label, observation_packet)
            automation._mark_observation(
                self.db,
                1,
                label,
                automation.AutomationResult(
                    True,
                    "daily",
                    "published",
                    source_window_start=start,
                    source_window_end=end,
                ),
            )

        start, end, _ = automation._weekly_period_for_monday(monday)
        packet, complete_days, active_days = automation._weekly_packet(self.db, 1, start, end)
        self.assertIsNotNone(packet)
        self.assertEqual((7, 7), (complete_days, active_days))
        self.assertTrue(packet["sourceArchiveAvailable"])
        self.assertEqual(7, packet["aggregateCounts"]["eligibleConversations"])
        self.assertEqual(7, packet["aggregateCounts"]["participants"])
        self.assertEqual(7, packet["aggregateCounts"]["channels"])
        public_generation_text = json.dumps(packet["safeSources"])
        self.assertNotIn("Alice", public_generation_text)
        self.assertNotIn("Bob", public_generation_text)

    def test_all_quiet_week_still_publishes_once(self):
        for offset in range(7):
            day = datetime(2026, 7, 13, tzinfo=timezone.utc).date() + timedelta(days=offset)
            start, end, label = automation._daily_period_for_day(day)
            packet = journal.build_packet_from_sources(
                self.db, 1, start, end, [], [], entry_kind="daily",
                aggregate_counts={"eligibleRelays": 0, "eligibleConversations": 0},
            )
            automation._store_observation(self.db, 1, label, packet)
            automation._mark_observation(
                self.db, 1, label,
                automation.AutomationResult(True, "daily", "quiet", source_window_start=start, source_window_end=end),
            )
        now = datetime(2026, 7, 21, 3, 0, tzinfo=timezone.utc)

        first = automation.run_weekly(
            self.db, 1, lambda *_args: self.fail("an all-quiet week must use deterministic copy"),
            "https://site.example", "key", now_utc=now, force=True, opener=self.opener,
        )
        second = automation.run_weekly(
            self.db, 1, lambda *_args: self.fail("published quiet week generated twice"),
            "https://site.example", "key", now_utc=now, opener=self.opener,
        )
        self.assertEqual("published", first.status)
        self.assertEqual("published", second.status)
        self.assertEqual(first.entry_id, second.entry_id)


if __name__ == "__main__":
    unittest.main()
