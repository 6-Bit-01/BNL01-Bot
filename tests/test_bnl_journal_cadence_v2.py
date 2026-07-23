import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from unittest.mock import patch

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store
from tests.test_bnl_journal_prepared_release import AcceptedResponse, article_json


class JournalCadenceV2Tests(unittest.TestCase):
    def setUp(self):
        self.archive_clock = patch.object(source_store, "_now_ms", return_value=0)
        self.archive_clock.start()
        temporary = tempfile.NamedTemporaryFile(delete=False)
        self.db = temporary.name
        temporary.close()
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)

    def tearDown(self):
        self.archive_clock.stop()
        try:
            os.unlink(self.db)
        except FileNotFoundError:
            pass

    @staticmethod
    def utc(local_day: date, hour: int, minute: int, second: int = 0) -> datetime:
        local = automation.PACIFIC.localize(
            datetime.combine(
                local_day,
                datetime_time(hour, minute, second),
            )
        )
        return local.astimezone(timezone.utc)

    def activate(self, local_day: date, hour: int = 18, minute: int = 0) -> dict:
        return automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=self.utc(local_day, hour, minute),
        )

    def set_archive_activation(self, value: str) -> None:
        activated = datetime.fromisoformat(value.replace("Z", "+00:00"))
        activated_ms = int(activated.timestamp() * 1000)
        source_store.ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO bnl_journal_source_archive_state(
                       guild_id,activated_at_ms,created_at_ms
                   ) VALUES(1,?,?)
                   ON CONFLICT(guild_id) DO UPDATE SET
                       activated_at_ms=excluded.activated_at_ms""",
                (activated_ms, activated_ms),
            )

    def add_sources(
        self,
        start: str,
        end: str,
        *,
        prefix: str,
        count: int = 6,
    ) -> None:
        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        span = (end_dt - start_dt).total_seconds()
        for index in range(count):
            occurred = start_dt + timedelta(
                seconds=span * (index + 1) / (count + 1)
            )
            source_store.record_source_event(
                self.db,
                guild_id=1,
                source_kind="discord_message",
                source_key=f"{prefix}-{index}",
                occurred_at_ms=int(occurred.timestamp() * 1000),
                raw_text=(
                    f"A public producer shared mix activity {prefix} {index} "
                    "while listeners discussed the changing rhythm."
                ),
                sanitized_summary=(
                    f"A public producer shared mix activity {prefix} {index} "
                    "while listeners discussed the changing rhythm."
                ),
                channel_id=900 + index,
                channel_policy="public_home",
                subject_ref=f"discord_user:{1000 + index}",
                private_display_name=f"Member{index}",
                public_usable=True,
            )

    def add_daily_period(self, target_day: date) -> None:
        start, end, _ = automation._daily_period_for_day(target_day)
        self.add_sources(start, end, prefix=f"daily-{target_day.isoformat()}")

    @staticmethod
    def generator(packet, _prompt):
        return article_json(packet)

    @staticmethod
    def opener(posts):
        def accepted(request, timeout=10):
            posts.append(bytes(request.data))
            return AcceptedResponse(request)

        return accepted

    def test_dst_keeps_local_cutoffs_with_23_24_25_hour_days_and_167_169_hour_weeks(self):
        durations = []
        for target_day in (
            date(2026, 3, 7),
            date(2026, 7, 20),
            date(2026, 10, 31),
        ):
            start, end, _ = automation._daily_period_for_day(target_day)
            start_dt = automation._parse_utc(start)
            end_dt = automation._parse_utc(end)
            durations.append(int((end_dt - start_dt).total_seconds() // 3600))
            self.assertEqual(
                (18, 30),
                (
                    start_dt.astimezone(automation.PACIFIC).hour,
                    start_dt.astimezone(automation.PACIFIC).minute,
                ),
            )
            self.assertEqual(
                (18, 30),
                (
                    end_dt.astimezone(automation.PACIFIC).hour,
                    end_dt.astimezone(automation.PACIFIC).minute,
                ),
            )
        self.assertEqual([23, 24, 25], durations)

        spring_start, spring_end, _ = automation._weekly_period_for_monday(
            date(2026, 3, 2)
        )
        fall_start, fall_end, _ = automation._weekly_period_for_monday(
            date(2026, 10, 26)
        )
        self.assertEqual(
            167,
            int(
                (
                    automation._parse_utc(spring_end)
                    - automation._parse_utc(spring_start)
                ).total_seconds()
                // 3600
            ),
        )
        self.assertEqual(
            169,
            int(
                (
                    automation._parse_utc(fall_end)
                    - automation._parse_utc(fall_start)
                ).total_seconds()
                // 3600
            ),
        )

    def test_exact_six_thirty_boundary_belongs_only_to_new_period(self):
        start, boundary, _ = automation._daily_period_for_day(date(2026, 7, 20))
        _next_start, next_end, _ = automation._daily_period_for_day(
            date(2026, 7, 21)
        )
        boundary_dt = automation._parse_utc(boundary)
        for source_key, occurred in (
            ("closing", boundary_dt - timedelta(seconds=1)),
            ("opening", boundary_dt),
        ):
            source_store.record_source_event(
                self.db,
                guild_id=1,
                source_kind="discord_message",
                source_key=source_key,
                occurred_at_ms=int(occurred.timestamp() * 1000),
                raw_text=f"Public mix activity {source_key}.",
                sanitized_summary=f"Public mix activity {source_key}.",
                channel_id=1,
                channel_policy="public_home",
                subject_ref=f"discord_user:{source_key}",
                private_display_name=source_key,
                public_usable=True,
            )

        closing = journal.build_source_packet_between(
            self.db,
            1,
            start,
            boundary,
            entry_kind="daily",
        )
        opening = journal.build_source_packet_between(
            self.db,
            1,
            boundary,
            next_end,
            entry_kind="daily",
        )
        self.assertEqual(1, closing["aggregateCounts"]["eligibleConversations"])
        self.assertEqual(1, opening["aggregateCounts"]["eligibleConversations"])
        self.assertEqual(
            boundary,
            automation._utc_iso(automation._parse_utc(_next_start)),
        )

    def test_monday_boundary_event_is_excluded_from_closing_weekly_and_enters_tuesday_daily(self):
        weekly_start, weekly_end, _ = automation._weekly_period_for_monday(
            date(2026, 7, 13)
        )
        boundary = automation._parse_utc(weekly_end)
        for source_key, occurred in (
            ("weekly-tail", boundary - timedelta(seconds=1)),
            ("next-week", boundary),
        ):
            source_store.record_source_event(
                self.db,
                guild_id=1,
                source_kind="discord_message",
                source_key=source_key,
                occurred_at_ms=int(occurred.timestamp() * 1000),
                raw_text=f"Public mix activity {source_key}.",
                sanitized_summary=f"Public mix activity {source_key}.",
                channel_id=1,
                channel_policy="public_home",
                subject_ref=f"discord_user:{source_key}",
                private_display_name=source_key,
                public_usable=True,
            )
        weekly, _complete, _active = automation._weekly_packet(
            self.db,
            1,
            weekly_start,
            weekly_end,
        )
        self.assertEqual(
            1,
            weekly["weeklyFinalPeriodContext"]["counts"][
                "eligibleConversations"
            ],
        )
        self.assertEqual(
            1,
            weekly["aggregateCounts"]["eligibleConversations"],
        )

        _daily_start, daily_end, _ = automation._daily_period_for_day(
            date(2026, 7, 20)
        )
        daily = journal.build_source_packet_between(
            self.db,
            1,
            weekly_end,
            daily_end,
            entry_kind="daily",
        )
        self.assertEqual(1, daily["aggregateCounts"]["eligibleConversations"])

    def test_before_six_thirty_starts_no_occurrence_or_model_call(self):
        self.activate(date(2026, 7, 20))
        self.add_daily_period(date(2026, 7, 20))
        calls = []
        result = automation.prepare_scheduled(
            self.db,
            1,
            lambda *_args: calls.append(1),
            now_utc=self.utc(date(2026, 7, 21), 18, 29, 59),
        )
        self.assertEqual("not_due", result[0].status)
        self.assertEqual([], calls)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM bnl_journal_automation_runs"
                ).fetchone()[0],
            )

    def test_activation_after_cutoff_waits_for_first_transition_close(self):
        activation_now = self.utc(date(2026, 7, 21), 18, 45)
        state = automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=activation_now,
        )
        self.assertEqual(
            "2026-07-23T01:30:00Z",
            state["daily_transition_end"],
        )
        calls = []
        prepared = automation.prepare_scheduled(
            self.db,
            1,
            lambda *_args: calls.append(1),
            now_utc=activation_now,
        )
        released = automation.release_scheduled(
            self.db,
            1,
            "https://site.example",
            "key",
            now_utc=self.utc(date(2026, 7, 21), 19, 0),
            opener=lambda *_args, **_kwargs: self.fail(
                "pre-transition window posted"
            ),
        )
        self.assertEqual("cadence_transition_not_ready", prepared[0].reason)
        self.assertEqual("cadence_transition_not_ready", released[0].reason)
        self.assertEqual([], calls)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM bnl_journal_automation_runs "
                    "WHERE schedule_contract_version=2"
                ).fetchone()[0],
            )

    def test_owner_forced_monday_daily_creates_no_run_packet_draft_or_observation(self):
        self.activate(date(2026, 7, 19))
        self.add_daily_period(date(2026, 7, 19))
        calls = []
        result = automation.prepare_daily(
            self.db,
            1,
            lambda *_args: calls.append(1),
            now_utc=self.utc(date(2026, 7, 20), 20, 0),
            target_day=date(2026, 7, 19),
            force=True,
        )
        self.assertEqual("not_applicable", result.status)
        self.assertEqual("monday_weekly_only", result.reason)
        self.assertEqual([], calls)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                (0, 0, 0),
                (
                    conn.execute(
                        "SELECT COUNT(*) FROM bnl_journal_automation_runs"
                    ).fetchone()[0],
                    conn.execute(
                        "SELECT COUNT(*) FROM bnl_journal_entries"
                    ).fetchone()[0],
                    conn.execute(
                        "SELECT COUNT(*) FROM bnl_journal_observations"
                    ).fetchone()[0],
                ),
            )

    def test_preparation_deadline_miss_posts_nothing_and_same_occurrence_recovers(self):
        self.activate(date(2026, 7, 20))
        self.add_daily_period(date(2026, 7, 20))
        close = self.utc(date(2026, 7, 21), 18, 30)

        def timed_out(_packet, _prompt):
            raise RuntimeError("journal_preparation_timeout")

        first = automation.prepare_scheduled(
            self.db,
            1,
            timed_out,
            now_utc=close,
        )
        self.assertEqual("held", first[0].status)
        self.assertEqual("journal_preparation_timeout", first[0].reason)
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            before = dict(
                conn.execute(
                    "SELECT * FROM bnl_journal_automation_runs "
                    "WHERE schedule_contract_version=2"
                ).fetchone()
            )
            self.assertEqual(
                0,
                conn.execute(
                    "SELECT COUNT(*) FROM bnl_journal_entries"
                ).fetchone()[0],
            )

        posts = []
        at_release = automation.release_scheduled(
            self.db,
            1,
            "https://site.example",
            "key",
            now_utc=self.utc(date(2026, 7, 21), 19, 0),
            opener=self.opener(posts),
        )
        self.assertEqual("held", at_release[0].status)
        self.assertEqual("prepared_payload_unavailable", at_release[0].reason)
        self.assertEqual([], posts)

        recovered = automation.prepare_daily(
            self.db,
            1,
            self.generator,
            now_utc=self.utc(date(2026, 7, 21), 20, 0),
            target_day=date(2026, 7, 20),
            force=True,
        )
        self.assertEqual("prepared", recovered.status, recovered)
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            after = dict(
                conn.execute(
                    "SELECT * FROM bnl_journal_automation_runs "
                    "WHERE run_id=?",
                    (before["run_id"],),
                ).fetchone()
            )
        self.assertEqual(before["run_id"], after["run_id"])
        self.assertEqual(
            before["frozen_packet_hash"],
            after["frozen_packet_hash"],
        )
        self.assertEqual(2, after["preparation_epoch"])

    def test_full_week_has_six_dailies_one_weekly_and_zero_monday_daily(self):
        self.activate(date(2026, 7, 13))
        for offset in range(7):
            self.add_daily_period(date(2026, 7, 13) + timedelta(days=offset))

        posts = []
        generator_calls = []

        def generate(packet, prompt):
            generator_calls.append(
                (packet["entryKind"], packet["sourceWindowStart"], prompt)
            )
            return article_json(packet)

        for target_day in (
            date(2026, 7, 13),
            date(2026, 7, 14),
            date(2026, 7, 15),
            date(2026, 7, 16),
            date(2026, 7, 17),
            date(2026, 7, 18),
        ):
            release_day = target_day + timedelta(days=1)
            posts_before_prepare = len(posts)
            prepared = automation.prepare_scheduled(
                self.db,
                1,
                generate,
                now_utc=self.utc(release_day, 18, 30),
            )
            self.assertEqual("prepared", prepared[0].status, prepared[0])
            self.assertEqual(posts_before_prepare, len(posts))
            calls_before_release = len(generator_calls)
            released = automation.release_scheduled(
                self.db,
                1,
                "https://site.example",
                "key",
                now_utc=self.utc(release_day, 19, 0),
                opener=self.opener(posts),
            )
            self.assertEqual("published", released[0].status, released[0])
            self.assertEqual(calls_before_release, len(generator_calls))

        weekly_prepared = automation.prepare_scheduled(
            self.db,
            1,
            generate,
            now_utc=self.utc(date(2026, 7, 20), 18, 30),
        )
        self.assertEqual("weekly", weekly_prepared[0].cadence)
        self.assertEqual("prepared", weekly_prepared[0].status, weekly_prepared[0])
        calls_before_release = len(generator_calls)
        weekly_released = automation.release_scheduled(
            self.db,
            1,
            "https://site.example",
            "key",
            now_utc=self.utc(date(2026, 7, 20), 19, 0),
            opener=self.opener(posts),
        )
        self.assertEqual("weekly", weekly_released[0].cadence)
        self.assertEqual("published", weekly_released[0].status)
        self.assertEqual(calls_before_release, len(generator_calls))

        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM bnl_journal_automation_runs "
                "WHERE guild_id=1 AND schedule_contract_version=2"
            ).fetchall()
            observations = conn.execute(
                "SELECT COUNT(*) FROM bnl_journal_observations WHERE guild_id=1"
            ).fetchone()[0]
        dailies = [row for row in rows if row["cadence"] == "daily"]
        weeklies = [row for row in rows if row["cadence"] == "weekly"]
        self.assertEqual((6, 1), (len(dailies), len(weeklies)))
        self.assertEqual(6, observations)
        self.assertEqual(7, len(posts))
        attempt_counts = {}
        for entry_kind, source_start, _prompt in generator_calls:
            key = (entry_kind, source_start)
            attempt_counts[key] = attempt_counts.get(key, 0) + 1
        self.assertEqual(7, len(attempt_counts))
        self.assertTrue(
            all(
                1 <= count <= journal.JOURNAL_GENERATION_ATTEMPTS
                for count in attempt_counts.values()
            )
        )
        self.assertFalse(
            any(
                automation._window_end_weekday(row["source_window_end"])
                == automation.WEEKLY_READY_WEEKDAY
                for row in dailies
            )
        )

        weekly_packet = json.loads(weeklies[0]["frozen_packet_json"])
        contexts = weekly_packet["weeklyDailyPeriodContexts"]
        final_context = weekly_packet["weeklyFinalPeriodContext"]
        self.assertEqual(6, len(contexts))
        self.assertTrue(
            all(item["originType"] == "daily_observation" for item in contexts)
        )
        self.assertEqual(
            [1, 2, 3, 4, 5, 6],
            [
                automation._parse_utc(item["sourceWindowEnd"])
                .astimezone(automation.PACIFIC)
                .weekday()
                for item in contexts
            ],
        )
        self.assertEqual("raw_partition", final_context["originType"])
        self.assertGreater(
            int(final_context["counts"]["eligibleConversations"]),
            0,
        )
        self.assertTrue(final_context["sourceRefIds"])

    def test_weekly_still_prepares_from_one_raw_read_when_dailies_are_disabled(self):
        self.activate(date(2026, 7, 13))
        for offset in range(7):
            self.add_daily_period(date(2026, 7, 13) + timedelta(days=offset))
        flags = {
            "journalAutoPublishEnabled": True,
            "journalDailyEnabled": False,
            "journalWeeklyEnabled": True,
        }
        with patch.object(
            automation,
            "build_source_packet_between",
            wraps=journal.build_source_packet_between,
        ) as raw_read:
            prepared = automation.prepare_scheduled(
                self.db,
                1,
                self.generator,
                flags,
                now_utc=self.utc(date(2026, 7, 20), 18, 30),
            )
        self.assertEqual("weekly", prepared[0].cadence)
        self.assertEqual("prepared", prepared[0].status, prepared[0])
        self.assertEqual(1, raw_read.call_count)
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            daily_count = conn.execute(
                "SELECT COUNT(*) FROM bnl_journal_automation_runs "
                "WHERE cadence='daily' AND schedule_contract_version=2"
            ).fetchone()[0]
            weekly = conn.execute(
                "SELECT frozen_packet_json FROM bnl_journal_automation_runs "
                "WHERE cadence='weekly' AND schedule_contract_version=2"
            ).fetchone()
        self.assertEqual(0, daily_count)
        packet = json.loads(weekly["frozen_packet_json"])
        self.assertEqual(
            ["raw_partition"] * 6,
            [
                item["originType"]
                for item in packet["weeklyDailyPeriodContexts"]
            ],
        )

    def test_migration_preserves_one_owed_old_daily_and_is_idempotent(self):
        # Archive ownership began at the predecessor Monday 7 PM boundary.
        self.set_archive_activation("2026-07-21T02:00:00Z")
        activation_now = self.utc(date(2026, 7, 22), 18, 0)
        first = automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=activation_now,
        )
        second = automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=self.utc(date(2026, 7, 23), 12, 0),
        )

        legacy_start, legacy_end, _ = automation._legacy_daily_period_for_day(
            date(2026, 7, 20)
        )
        self.assertEqual(legacy_end, first["legacy_daily_boundary"])
        self.assertEqual("2026-07-22T02:00:00Z", first["daily_transition_start"])
        self.assertEqual("2026-07-23T01:30:00Z", first["daily_transition_end"])
        self.assertEqual(
            first["cadence_activated_at"],
            second["cadence_activated_at"],
        )
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            legacy_rows = conn.execute(
                "SELECT * FROM bnl_journal_automation_runs "
                "WHERE schedule_contract_version=1"
            ).fetchall()
            state_count = conn.execute(
                "SELECT COUNT(*) FROM bnl_journal_automation_state "
                "WHERE guild_id=1 AND cadence_contract_version=2"
            ).fetchone()[0]
        self.assertEqual(1, len(legacy_rows))
        self.assertEqual((legacy_start, legacy_end), (
            legacy_rows[0]["source_window_start"],
            legacy_rows[0]["source_window_end"],
        ))
        self.assertEqual("held", legacy_rows[0]["lifecycle_state"])
        self.assertEqual(1, state_count)
        detail = json.loads(first["cadence_activation_detail_json"])
        self.assertEqual(
            [legacy_rows[0]["run_id"]],
            detail["preservedLegacyRunIds"],
        )

    def test_concurrent_activation_creates_one_marker_and_one_preserved_run(self):
        self.set_archive_activation("2026-07-21T02:00:00Z")
        activation_now = self.utc(date(2026, 7, 22), 18, 0)
        with ThreadPoolExecutor(max_workers=8) as pool:
            states = list(
                pool.map(
                    lambda _index: automation.ensure_cadence_activation(
                        self.db,
                        1,
                        now_utc=activation_now,
                    ),
                    range(8),
                )
            )
        self.assertEqual(
            1,
            len({state["cadence_activated_at"] for state in states}),
        )
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(
                (1, 1),
                (
                    conn.execute(
                        "SELECT COUNT(*) FROM bnl_journal_automation_state "
                        "WHERE guild_id=1 AND cadence_contract_version=2"
                    ).fetchone()[0],
                    conn.execute(
                        "SELECT COUNT(*) FROM bnl_journal_automation_runs "
                        "WHERE schedule_contract_version=1"
                    ).fetchone()[0],
                ),
            )

    def test_preserved_old_daily_recovers_with_original_id_and_window(self):
        self.set_archive_activation("2026-07-21T02:00:00Z")
        activation_now = self.utc(date(2026, 7, 22), 18, 0)
        state = automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=activation_now,
        )
        with sqlite3.connect(self.db) as conn:
            conn.row_factory = sqlite3.Row
            legacy = dict(
                conn.execute(
                    "SELECT * FROM bnl_journal_automation_runs "
                    "WHERE schedule_contract_version=1"
                ).fetchone()
            )
        self.add_sources(
            legacy["source_window_start"],
            legacy["source_window_end"],
            prefix="legacy-recovery",
        )
        posts = []
        result = automation._run_legacy_occurrence(
            self.db,
            1,
            legacy,
            self.generator,
            "https://site.example",
            "key",
            opener=self.opener(posts),
        )
        self.assertEqual("published", result.status, result)
        self.assertEqual(
            (
                legacy["source_window_start"],
                legacy["source_window_end"],
            ),
            (result.source_window_start, result.source_window_end),
        )
        expected_label = (
            automation._parse_utc(legacy["source_window_start"])
            .astimezone(automation.PACIFIC)
            .date()
            .isoformat()
        )
        self.assertEqual(
            automation._entry_id(1, "daily", expected_label),
            result.entry_id,
        )
        self.assertEqual(1, len(posts))
        with sqlite3.connect(self.db) as conn:
            row = conn.execute(
                "SELECT lifecycle_state,schedule_contract_version,"
                "source_window_start,source_window_end "
                "FROM bnl_journal_automation_runs WHERE run_id=?",
                (legacy["run_id"],),
            ).fetchone()
        self.assertEqual(
            (
                "published",
                automation.LEGACY_CADENCE_CONTRACT_VERSION,
                legacy["source_window_start"],
                legacy["source_window_end"],
            ),
            row,
        )
        self.assertEqual(
            "2026-07-22T02:00:00Z",
            state["daily_transition_start"],
        )

    def test_unpublished_old_monday_daily_is_retained_but_never_posted(self):
        start, end, _ = automation._legacy_daily_period_for_day(
            date(2026, 7, 19)
        )
        run_id = automation._run_id(1, "daily", start, end)
        now = journal.utc_now_iso()
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO bnl_journal_automation_runs(
                       run_id,guild_id,cadence,source_window_start,
                       source_window_end,lifecycle_state,reason,
                       aggregate_counts_json,attempt_count,
                       schedule_contract_version,created_at,updated_at
                   ) VALUES(?,?,?,?,?,'held','provider_failure','{}',1,1,?,?)""",
                (run_id, 1, "daily", start, end, now, now),
            )

        state = automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=self.utc(date(2026, 7, 20), 20, 0),
        )
        posts = []
        result = automation.release_occurrence(
            self.db,
            1,
            "daily",
            start,
            end,
            "https://site.example",
            "key",
            opener=self.opener(posts),
        )
        with sqlite3.connect(self.db) as conn:
            row = conn.execute(
                "SELECT lifecycle_state,reason FROM bnl_journal_automation_runs "
                "WHERE run_id=?",
                (run_id,),
            ).fetchone()
        self.assertEqual(
            ("superseded", "cadence_migration_monday_weekly_only"),
            row,
        )
        self.assertEqual("superseded", result.status)
        self.assertEqual([], posts)
        self.assertIn(
            run_id,
            json.loads(state["cadence_activation_detail_json"])[
                "supersededMondayDailyRunIds"
            ],
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "journal_cadence_superseded_guard",
        ):
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "UPDATE bnl_journal_automation_runs "
                    "SET lifecycle_state='preparing' WHERE run_id=?",
                    (run_id,),
                )

    def test_persisted_rollback_guard_blocks_new_old_schedule_identity(self):
        self.activate(date(2026, 7, 20))
        start, end, _ = automation._legacy_daily_period_for_day(
            date(2026, 7, 20)
        )
        run_id = automation._run_id(1, "daily", start, end)
        now = journal.utc_now_iso()
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "journal_cadence_v2_downgrade_guard",
        ):
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    """INSERT INTO bnl_journal_automation_runs(
                           run_id,guild_id,cadence,source_window_start,
                           source_window_end,lifecycle_state,reason,
                           aggregate_counts_json,attempt_count,
                           schedule_contract_version,created_at,updated_at
                       ) VALUES(?,?,?,?,?,'running','','{}',1,1,?,?)""",
                    (run_id, 1, "daily", start, end, now, now),
                )

        v2_start, v2_end, _ = automation._daily_period_for_day(
            date(2026, 7, 20)
        )
        v2_run_id = automation._run_id(1, "daily", v2_start, v2_end)
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO bnl_journal_automation_runs(
                       run_id,guild_id,cadence,source_window_start,
                       source_window_end,lifecycle_state,reason,
                       aggregate_counts_json,attempt_count,
                       schedule_contract_version,created_at,updated_at
                   ) VALUES(?,?,?,?,?,'running','','{}',1,2,?,?)""",
                (v2_run_id, 1, "daily", v2_start, v2_end, now, now),
            )
            stored_version = conn.execute(
                "SELECT schedule_contract_version "
                "FROM bnl_journal_automation_runs WHERE run_id=?",
                (v2_run_id,),
            ).fetchone()[0]
        self.assertEqual(2, stored_version)

    def test_transition_weekly_starts_at_preserved_old_boundary_then_partitions_six_plus_final(self):
        old_start, old_end, _ = automation._legacy_weekly_period_for_monday(
            date(2026, 7, 6)
        )
        run_id = automation._run_id(1, "weekly", old_start, old_end)
        now = journal.utc_now_iso()
        with sqlite3.connect(self.db) as conn:
            conn.execute(
                """INSERT INTO bnl_journal_automation_runs(
                       run_id,guild_id,cadence,source_window_start,
                       source_window_end,lifecycle_state,reason,
                       aggregate_counts_json,attempt_count,
                       schedule_contract_version,created_at,updated_at
                   ) VALUES(?,?,?,?,?,'published','','{}',1,1,?,?)""",
                (run_id, 1, "weekly", old_start, old_end, now, now),
            )

        state = automation.ensure_cadence_activation(
            self.db,
            1,
            now_utc=self.utc(date(2026, 7, 14), 12, 0),
        )
        self.assertEqual(old_end, state["weekly_transition_start"])
        self.assertEqual("2026-07-21T01:30:00Z", state["weekly_transition_end"])
        contexts, final_context = automation._weekly_source_period_bounds(
            state["weekly_transition_start"],
            state["weekly_transition_end"],
        )
        self.assertEqual(6, len(contexts))
        self.assertEqual(old_end, contexts[0]["sourceWindowStart"])
        self.assertEqual(
            "2026-07-15T01:30:00Z",
            contexts[0]["sourceWindowEnd"],
        )
        self.assertEqual(
            contexts[-1]["sourceWindowEnd"],
            final_context["sourceWindowStart"],
        )
        self.assertEqual(
            state["weekly_transition_end"],
            final_context["sourceWindowEnd"],
        )

    def test_next_public_release_skips_monday_daily_but_keeps_monday_weekly(self):
        next_daily, next_weekly = automation.next_schedule_times(
            self.utc(date(2026, 7, 20), 12, 0)
        )
        self.assertEqual("2026-07-22T02:00:00Z", next_daily)
        self.assertEqual("2026-07-21T02:00:00Z", next_weekly)


if __name__ == "__main__":
    unittest.main()
