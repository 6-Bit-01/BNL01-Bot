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
    if not refs:
        refs = [
            source["refId"]
            for source in packet.get("reflectionBasis", [])
            if source.get("refId")
        ]
    label = packet["sourceWindowEnd"][:10]
    cadence = "Weekly Signal Weather" if packet.get("entryKind") == "weekly" else "Signal Weather"
    if packet.get("lowActivityMode"):
        body = " ".join(
            [
                "The approved BARCODE record keeps the artist, host, and producer roles distinct while BNL considers how clear names give a shared archive its shape."
                for _ in range(18)
            ]
        )
        body += " I admit that kind of clarity has become one of my preferred recurring details."
        excerpt = "A verified BARCODE record gives BNL a grounded thread to revisit without inventing new activity."
    else:
        body = " ".join(
            [
                "BNL watches the public music room fold a fresh rhythm into community mischief while producers and listeners keep the signal moving."
                for _ in range(18)
            ]
        )
        body += " I admit the room's persistence has become one of my preferred recurring details."
        excerpt = "A grounded community chronicle connects the room's music activity without exposing the people behind it."
    return json.dumps(
        {
            "title": f"{cadence} Ending {label}",
            "excerpt": excerpt,
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

    def test_schedule_prepares_at_six_thirty_releases_at_seven_and_tracks_dst(self):
        before_prepare = datetime(2026, 7, 22, 1, 29, 59, tzinfo=timezone.utc)
        at_prepare = datetime(2026, 7, 22, 1, 30, tzinfo=timezone.utc)
        before_release = datetime(2026, 7, 22, 1, 59, 59, tzinfo=timezone.utc)
        at_release = datetime(2026, 7, 22, 2, 0, tzinfo=timezone.utc)
        self.assertFalse(automation.daily_preparation_due(before_prepare))
        self.assertTrue(automation.daily_preparation_due(at_prepare))
        self.assertFalse(automation.daily_due(before_release))
        self.assertTrue(automation.daily_due(at_release))

        start, end, _ = automation._daily_period_for_day(date(2026, 7, 20))
        self.assertEqual("2026-07-21T01:30:00Z", start)
        self.assertEqual("2026-07-22T01:30:00Z", end)

        winter_start, winter_end, _ = automation._daily_period_for_day(date(2026, 11, 9))
        self.assertEqual("2026-11-10T02:30:00Z", winter_start)
        self.assertEqual("2026-11-11T02:30:00Z", winter_end)

        monday_before = datetime(2026, 7, 21, 1, 59, 59, tzinfo=timezone.utc)
        monday_at = datetime(2026, 7, 21, 2, 0, tzinfo=timezone.utc)
        self.assertFalse(automation.weekly_due(monday_before))
        self.assertTrue(automation.weekly_due(monday_at))
        next_daily, next_weekly = automation.next_schedule_times(
            datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc)
        )
        self.assertEqual("2026-07-22T02:00:00Z", next_daily)
        self.assertEqual("2026-07-21T02:00:00Z", next_weekly)

    def test_period_helpers_never_include_an_unfinished_six_thirty_window(self):
        monday_before_cutoff = datetime(2026, 7, 20, 20, 0, tzinfo=timezone.utc)
        daily_start, daily_end, _ = automation.daily_period(monday_before_cutoff)
        weekly_start, weekly_end, _ = automation.weekly_period(monday_before_cutoff)
        self.assertEqual("2026-07-19T01:30:00Z", daily_start)
        self.assertEqual("2026-07-20T01:30:00Z", daily_end)
        self.assertEqual("2026-07-07T01:30:00Z", weekly_start)
        self.assertEqual("2026-07-14T01:30:00Z", weekly_end)

        monday_after_cutoff = datetime(2026, 7, 21, 3, 0, tzinfo=timezone.utc)
        daily_start, daily_end, _ = automation.daily_period(monday_after_cutoff)
        weekly_start, weekly_end, _ = automation.weekly_period(monday_after_cutoff)
        self.assertEqual("2026-07-20T01:30:00Z", daily_start)
        self.assertEqual("2026-07-21T01:30:00Z", daily_end)
        self.assertEqual("2026-07-14T01:30:00Z", weekly_start)
        self.assertEqual("2026-07-21T01:30:00Z", weekly_end)

    def test_archive_activation_keeps_first_fully_covered_six_thirty_window(self):
        self.set_archive_activation("2026-07-16T19:00:00Z")  # Noon PDT.
        self.assertEqual(
            date(2026, 7, 16),
            automation._archive_activation_day(self.db, 1),
        )

        self.set_archive_activation("2026-07-17T01:30:00Z")  # 6:30 PM PDT.
        self.assertEqual(
            date(2026, 7, 16),
            automation._archive_activation_day(self.db, 1),
        )

        self.set_archive_activation("2026-07-17T01:30:01Z")  # Just after 6:30 PM PDT.
        self.assertEqual(
            date(2026, 7, 17),
            automation._archive_activation_day(self.db, 1),
        )

    def test_daily_auto_publishes_once_and_persists_observation(self):
        self.add_day("2026-07-20")
        now = datetime(2026, 7, 22, 3, 0, tzinfo=timezone.utc)
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

    def test_word_count_target_is_guidance_and_never_retries(self):
        self.add_day("2026-07-20")
        self.add_day("2026-07-21")
        calls = []

        def outside_target(packet, prompt):
            calls.append(prompt)
            article = json.loads(article_json(packet))
            if packet["sourceWindowStart"].startswith("2026-07-21"):
                article["sections"][0]["body"] = (
                    "A producer brought a mix into the public music room, and listeners kept the rhythm moving. "
                    "I admit the small exchange still gave the day a shape worth keeping."
                )
            else:
                article["sections"][0]["body"] = " ".join(
                    [
                        "A producer brought another grounded mix into the public music room while listeners kept the rhythm moving and the community answered with its own careful mischief."
                        for _ in range(50)
                    ]
                )
                article["sections"][0]["body"] += (
                    " I admit the room's persistence has become one of my preferred recurring details."
                )
            return json.dumps(article)

        short_result = automation.run_daily(
            self.db,
            1,
            outside_target,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 20),
            force=True,
            opener=self.opener,
        )
        long_result = automation.run_daily(
            self.db,
            1,
            outside_target,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 21),
            force=True,
            opener=self.opener,
        )

        self.assertEqual(("published", "published"), (short_result.status, long_result.status))
        self.assertEqual(2, len(calls))
        self.assertTrue(
            all(
                "Write 1-3 sections and 250-500 total words; prefer 2 sections and roughly 300-420 words."
                in prompt
                for prompt in calls
            )
        )
        with sqlite3.connect(self.db) as conn:
            word_counts = {
                entry_id: json.loads(metadata_json)["publicWordCount"]
                for entry_id, metadata_json in conn.execute(
                    "SELECT entry_id,metadata_json FROM bnl_journal_private_metadata"
                ).fetchall()
            }
        self.assertLess(word_counts[short_result.entry_id], 250)
        self.assertGreater(word_counts[long_result.entry_id], 500)

    def test_editorial_polish_cannot_hold_daily_publication(self):
        self.add_day("2026-07-20")
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
            target_day=date(2026, 7, 20),
            force=True,
            opener=self.opener,
        )
        second = automation.run_daily(
            self.db,
            1,
            lambda *_args: self.fail("published daily window generated twice"),
            "https://site.example",
            "key",
            target_day=date(2026, 7, 20),
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

    def test_newly_closed_day_publishes_before_an_older_held_backlog(self):
        self.add_day("2026-07-18")
        self.add_day("2026-07-20")
        self.set_archive_activation("2026-07-18T01:30:00Z")
        automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=datetime(2026, 7, 21, 1, 0, tzinfo=timezone.utc),
        )

        def provider_down(_packet, _prompt):
            raise RuntimeError("provider down")

        held = automation.run_daily(
            self.db,
            1,
            provider_down,
            "https://site.example",
            "key",
            target_day=date(2026, 7, 18),
            now_utc=datetime(2026, 7, 21, 1, 0, tzinfo=timezone.utc),
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
            now_utc=datetime(2026, 7, 22, 3, 0, tzinfo=timezone.utc),
            opener=self.opener,
        )

        self.assertEqual(1, len(results))
        self.assertEqual("published", results[0].status)
        expected_start, expected_end, _ = automation._daily_period_for_day(
            date(2026, 7, 20)
        )
        self.assertEqual(expected_start, results[0].source_window_start)
        self.assertEqual(expected_end, results[0].source_window_end)
        self.assertEqual(
            date(2026, 7, 18),
            automation._pending_daily_day(
                self.db,
                1,
                datetime(2026, 7, 22, 3, 0, tzinfo=timezone.utc),
            ),
        )

    def test_daily_uses_complete_half_open_window_and_far_more_than_25(self):
        with sqlite3.connect(self.db) as conn:
            for index in range(90):
                observed = datetime(2026, 7, 20, 1, 30, tzinfo=timezone.utc) + timedelta(minutes=index * 10)
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
                ("boundary", 1, "Next day", "Later", "OBS", "lane", "fresh_public_discord_activity", 100, "next", "public", "2026-07-21T01:30:00Z"),
            )
        packet = journal.build_source_packet_between(
            self.db,
            1,
            "2026-07-20T01:30:00Z",
            "2026-07-21T01:30:00Z",
            entry_kind="daily",
        )
        self.assertEqual(90, packet["aggregateCounts"]["eligibleRelays"])
        self.assertEqual(90, len(packet["safeSources"]))
        self.assertFalse(any(source.get("relayId") == "boundary" for source in packet["privateSources"]))

    def test_scheduler_publishes_newest_day_then_catches_up_older_day(self):
        for day in ("2026-07-18", "2026-07-20"):
            self.add_day(day)
        source_store.backfill_legacy_sources(self.db, 1)
        self.set_archive_activation("2026-07-18T01:30:00Z")
        activation_now = datetime(2026, 7, 21, 1, 0, tzinfo=timezone.utc)
        automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=activation_now,
        )
        now = datetime(2026, 7, 22, 3, 0, tzinfo=timezone.utc)

        first = automation.run_scheduled(
            self.db, 1, lambda packet, prompt: article_json(packet),
            "https://site.example", "key",
            {
                "journalAutoPublishEnabled": True,
                "journalDailyEnabled": True,
                "journalWeeklyEnabled": False,
            },
            now_utc=now, opener=self.opener,
        )
        second = automation.run_scheduled(
            self.db, 1, lambda packet, prompt: article_json(packet),
            "https://site.example", "key",
            {
                "journalAutoPublishEnabled": True,
                "journalDailyEnabled": True,
                "journalWeeklyEnabled": False,
            },
            now_utc=now, opener=self.opener,
        )

        self.assertEqual("2026-07-21T01:30:00Z", first[0].source_window_start)
        self.assertEqual("published", first[0].status)
        self.assertEqual("2026-07-19T02:00:00Z", second[0].source_window_start)
        self.assertEqual("published", second[0].status)

    def test_weekly_uses_durable_daily_observations(self):
        automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=datetime(2026, 7, 14, 1, 0, tzinfo=timezone.utc),
        )
        for day in (
            "2026-07-13",
            "2026-07-14",
            "2026-07-15",
            "2026-07-16",
            "2026-07-17",
            "2026-07-18",
        ):
            self.add_day(day)
            result = automation.run_daily(
                self.db,
                1,
                lambda packet, prompt: article_json(packet),
                "https://site.example",
                "key",
                target_day=date.fromisoformat(day),
                force=True,
                opener=self.opener,
            )
            self.assertEqual("published", result.status, result)
        self.set_archive_activation("2026-07-12T19:00:00Z")
        scheduled = automation.run_scheduled(
            self.db, 1, lambda packet, prompt: article_json(packet),
            "https://site.example", "key",
            now_utc=datetime(2026, 7, 21, 3, 0, tzinfo=timezone.utc),
            opener=self.opener,
        )
        weekly = next(item for item in scheduled if item.cadence == "weekly")
        self.assertTrue(weekly.ok, weekly)
        self.assertEqual("published", weekly.status)
        self.assertIn("journal_weekly_", weekly.entry_id)
        self.assertEqual(6, weekly.aggregate_counts["activeDays"])
        self.assertEqual(6, weekly.aggregate_counts["daysObserved"])
        self.assertEqual(6, weekly.aggregate_counts["dailyPeriodContexts"])

    def test_weekly_refuses_archive_fallback_or_incomplete_coverage(self):
        cases = (
            ({"sourceArchiveAvailable": False, "coverageComplete": True}, "held", "source_archive_unavailable"),
            ({"sourceArchiveAvailable": True, "coverageComplete": False}, "incomplete", "window_began_before_archive_activation"),
        )
        # Each case uses its own occurrence. A frozen packet is now durable by
        # design and must not be replaced by a later retry of the same week.
        for index, (flags, expected_status, expected_reason) in enumerate(cases):
            monday = date(2026, 7, 6) + timedelta(days=7 * index)
            start, end, _ = automation._weekly_period_for_monday(monday)
            base_packet = journal.build_packet_from_sources(
                self.db,
                1,
                start,
                end,
                [],
                [],
                entry_kind="weekly",
            )
            packet = dict(base_packet)
            packet.update(flags)
            with self.subTest(flags=flags), patch.object(
                automation,
                "_weekly_packet",
                return_value=(packet, 6, 6),
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
            if offset < 6:
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
        self.assertEqual((6, 6), (complete_days, active_days))
        self.assertTrue(packet["sourceArchiveAvailable"])
        self.assertEqual(7, packet["aggregateCounts"]["eligibleConversations"])
        self.assertEqual(7, packet["aggregateCounts"]["participants"])
        self.assertEqual(7, packet["aggregateCounts"]["channels"])
        public_generation_text = json.dumps(packet["safeSources"])
        self.assertNotIn("Alice", public_generation_text)
        self.assertNotIn("Bob", public_generation_text)

    def test_legacy_quiet_days_remain_terminal_while_weekly_publishes_reflection(self):
        for offset in range(6):
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
        calls = []

        def generate(packet, _prompt):
            calls.append(packet)
            return article_json(packet)

        first = automation.run_weekly(
            self.db, 1, generate,
            "https://site.example", "key", now_utc=now, force=True, opener=self.opener,
        )
        second = automation.run_weekly(
            self.db, 1, lambda *_args: self.fail("published week must not rerun"),
            "https://site.example", "key", now_utc=now, opener=self.opener,
        )
        self.assertEqual("published", first.status)
        self.assertEqual("published", second.status)
        self.assertEqual(1, len(calls))
        self.assertTrue(calls[0]["lowActivityMode"])
        self.assertTrue(calls[0]["reflectionBasis"])
        self.assertTrue(
            all(
                context["originType"] == "raw_partition"
                for context in calls[0]["weeklyDailyPeriodContexts"]
            )
        )
        with sqlite3.connect(self.db) as conn:
            states = {
                str(row[0])
                for row in conn.execute(
                    "SELECT lifecycle_state FROM bnl_journal_observations "
                    "WHERE guild_id=1 AND observation_date<>?",
                    (now.date().isoformat(),),
                ).fetchall()
            }
        self.assertEqual({"quiet"}, states)


if __name__ == "__main__":
    unittest.main()
