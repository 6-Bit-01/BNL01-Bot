import asyncio
import calendar
import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

import pytz

import bnl_occasion as occasion_store

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

try:
    import bnl01_bot
except ModuleNotFoundError as exc:  # Local minimal images may omit Discord.
    if exc.name != "discord":
        raise
    bnl01_bot = None


PACIFIC = pytz.timezone("America/Los_Angeles")


def pacific(year, month, day, hour=10, minute=5):
    return PACIFIC.localize(datetime(year, month, day, hour, minute, 0))


def valid_reflection(occasion_name="Independence Day", target_chars=920):
    seed = (
        f"{occasion_name} reaches the BARCODE Network as a living signal. "
        "Freedom, memory, rhythm, responsibility, and care do not become real "
        "through slogans; they become real when people keep making room for "
        "one another. The archive carries names and unfinished work forward, "
        "while artists turn inherited noise into new language. Community is "
        "not a perfect chorus, but a practice of listening, crediting, "
        "questioning, repairing, and returning. "
    )
    extension = (
        "The signal stays alive through patience, humor, disagreement, "
        "curiosity, restraint, courage, and the stubborn choice to continue. "
    )
    text = seed
    while len(text) + len(extension) < target_chars:
        text += extension
    remaining = target_chars - len(text)
    if remaining <= 0:
        return text[: target_chars - 1].rstrip() + "."
    if remaining == 1:
        return text.rstrip()[: target_chars - 1] + "."
    filler = (
        "Shared creative work keeps changing shape without surrendering its "
        "source or its human center. "
    )
    tail = (filler * ((remaining // len(filler)) + 2))[: remaining - 1]
    return text + tail + "."


class FakeMessage:
    def __init__(
        self,
        message_id,
        content,
        author_id,
        *,
        nonce=None,
        created_at=None,
    ):
        self.id = message_id
        self.content = content
        self.author = SimpleNamespace(id=author_id)
        self.nonce = nonce
        self.created_at = created_at or datetime.now(timezone.utc)


class FakeChannel:
    def __init__(self, channel_id=222, bot_user_id=777, fail_sends=0):
        self.id = channel_id
        self.bot_user_id = bot_user_id
        self.fail_sends = fail_sends
        self.send_attempts = []
        self.messages = []

    async def send(self, content, **kwargs):
        self.send_attempts.append((content, kwargs))
        if self.fail_sends:
            self.fail_sends -= 1
            raise RuntimeError("temporary Discord failure")
        message = FakeMessage(
            1000 + len(self.messages) + 1,
            content,
            self.bot_user_id,
            nonce=kwargs.get("nonce"),
        )
        self.messages.append(message)
        return message

    async def history(self, **_kwargs):
        for message in reversed(self.messages):
            yield message


class OccasionCalendarTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(delete=False)
        handle.close()
        self.db_path = handle.name

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

    def test_registry_has_stable_rules_and_no_unsourced_barcode_date(self):
        occasion_store.validate_registry()
        self.assertEqual(
            date(2026, 1, 19),
            occasion_store.occasion_date(
                occasion_store.occasion_by_id("martin_luther_king_jr_day"),
                2026,
            ),
        )
        self.assertEqual(
            date(2026, 5, 25),
            occasion_store.occasion_date(
                occasion_store.occasion_by_id("memorial_day"),
                2026,
            ),
        )
        self.assertEqual(
            date(2026, 11, 26),
            occasion_store.occasion_date(
                occasion_store.occasion_by_id("thanksgiving"),
                2026,
            ),
        )
        self.assertEqual(
            date(2026, 4, 5),
            occasion_store.occasion_date(
                occasion_store.occasion_by_id("easter"),
                2026,
            ),
        )
        self.assertEqual((), occasion_store.BARCODE_OCCASIONS)
        curated = occasion_store.CURATED_BARCODE_RELEVANT_OCCASIONS
        self.assertEqual(39, len(curated))
        self.assertEqual(
            {
                "world_braille_day",
                "world_logic_day",
                "international_day_of_education",
                "world_cancer_day",
                "world_radio_day",
                "world_day_of_social_justice",
                "daventry_radar_demonstration_anniversary",
                "zero_discrimination_day",
                "international_day_of_mathematics",
                "world_poetry_day",
                "world_backup_day",
                "world_health_day",
                "world_art_day",
                "international_jazz_day",
                "african_world_heritage_day",
                "world_telecommunication_information_society_day",
                "world_day_for_cultural_diversity",
                "international_archives_day",
                "first_retail_barcode_scan_anniversary",
                "world_population_day",
                "world_listening_day",
                "world_chess_day",
                "international_day_of_friendship",
                "international_day_of_indigenous_peoples",
                "hip_hop_origin_anniversary",
                "world_humanitarian_day",
                "linux_announcement_anniversary",
                "international_day_for_people_of_african_descent",
                "international_literacy_day",
                "international_day_of_democracy",
                "international_music_day",
                "international_day_for_eradication_of_poverty",
                "world_day_for_audiovisual_heritage",
                "international_day_to_end_impunity_for_crimes_against_journalists",
                "world_television_day",
                "world_aids_day",
                "first_sms_message_anniversary",
                "first_transatlantic_wireless_signal_anniversary",
                "international_migrants_day",
            },
            {item.occasion_id for item in curated},
        )
        self.assertTrue(
            all(
                item.source_reference.startswith("https://")
                for item in curated
            )
        )
        self.assertEqual(
            [],
            occasion_store.calendar_occasions_on(date(2026, 2, 27)),
        )
        self.assertEqual(
            ["daventry_radar_demonstration_anniversary"],
            [
                item.occasion_id
                for item in occasion_store.calendar_occasions_on(
                    date(2026, 2, 26)
                )
            ],
        )
        # Father's Day and World Music Day both fall on 21 June in 2026.
        # The established major date wins and only one reflection is due.
        self.assertEqual(
            ["fathers_day"],
            [
                item.occasion_id
                for item in occasion_store.calendar_occasions_on(
                    date(2026, 6, 21)
                )
            ],
        )
        collision_keys = occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            pacific(2026, 6, 21),
        )
        self.assertEqual(1, len(collision_keys))
        self.assertTrue(collision_keys[0].endswith(":fathers_day"))
        unsourced = occasion_store.OccasionDefinition(
            "barcode_anniversary",
            "BARCODE Anniversary",
            "barcode",
            "An unapproved approximate date.",
            ("anniversary",),
            "fixed",
            month=7,
            day=1,
        )
        with self.assertRaisesRegex(ValueError, "unsourced_barcode_occasion"):
            occasion_store.validate_registry((unsourced,))

    def test_maintained_calendar_never_exceeds_ten_day_gap(self):
        registry = occasion_store.OCCASION_REGISTRY

        def dates_for_year(year):
            return {
                occasion_store.occasion_date(item, year)
                for item in registry
                if item.enabled
            }

        # Gregorian weekday/Easter behavior repeats every 400 years. Include
        # the adjacent year boundaries so December-to-January is covered too.
        for year in range(2000, 2400):
            dates = [
                max(dates_for_year(year - 1)),
                *sorted(dates_for_year(year)),
                min(dates_for_year(year + 1)),
            ]
            for previous, current in zip(dates, dates[1:]):
                with self.subTest(previous=previous, current=current):
                    self.assertLessEqual((current - previous).days, 10)

    def test_rule_validator_rejects_impossible_nth_weekday(self):
        invalid = occasion_store.OccasionDefinition(
            "invalid_weekday",
            "Invalid Weekday",
            "major",
            "Invalid calendar test.",
            ("calendar",),
            "nth_weekday",
            month=1,
            weekday=calendar.MONDAY,
            ordinal=6,
        )
        with self.assertRaisesRegex(ValueError, "invalid_nth_weekday"):
            occasion_store.validate_registry((invalid,))

    def test_first_activation_does_not_backfill_before_deployment(self):
        seeded = occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            pacific(2026, 7, 5),
        )
        self.assertEqual([], seeded)
        old_key = occasion_store.occurrence_key(
            1,
            date(2026, 7, 4),
            "independence_day",
        )
        self.assertEqual({}, occasion_store.get_occurrence(self.db_path, old_key))

    def test_restart_catches_every_missed_date_after_activation(self):
        occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            pacific(2026, 7, 3),
        )
        seeded = occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            pacific(2026, 7, 5),
        )
        key = occasion_store.occurrence_key(
            1,
            date(2026, 7, 4),
            "independence_day",
        )
        self.assertIn(key, seeded)
        row = occasion_store.get_occurrence(self.db_path, key)
        self.assertEqual("pending", row["state"])
        self.assertEqual("2026-07-04T17:00:00Z", row["scheduled_for"])

    def test_ten_am_target_honors_pacific_dst(self):
        for local_now, expected_utc in (
            (pacific(2026, 1, 1, 9, 0), "2026-01-01T18:00:00Z"),
            (pacific(2026, 7, 4, 9, 0), "2026-07-04T17:00:00Z"),
        ):
            with self.subTest(local_now=local_now):
                handle = tempfile.NamedTemporaryFile(delete=False)
                handle.close()
                try:
                    keys = occasion_store.seed_occurrences(
                        handle.name,
                        1,
                        222,
                        local_now,
                    )
                    self.assertEqual(1, len(keys))
                    row = occasion_store.get_occurrence(handle.name, keys[0])
                    self.assertEqual(expected_utc, row["scheduled_for"])
                    self.assertEqual(
                        {},
                        occasion_store.claim_next_due(
                            handle.name,
                            1,
                            now=local_now,
                        ),
                    )
                finally:
                    os.unlink(handle.name)

    def test_generation_retry_survives_reopen_and_preserves_identity(self):
        now = pacific(2026, 7, 4)
        keys = occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            now,
        )
        key = keys[0]
        first = occasion_store.claim_next_due(self.db_path, 1, now=now)
        self.assertEqual("generation", first["stage"])
        self.assertEqual(
            {},
            occasion_store.claim_next_due(self.db_path, 1, now=now),
        )
        self.assertTrue(
            occasion_store.fail_claim(
                self.db_path,
                key,
                first["lease_token"],
                stage="generation",
                reason="provider_unavailable",
                retry_minutes=15,
                now=now,
            )
        )
        self.assertEqual(
            {},
            occasion_store.claim_next_due(
                self.db_path,
                1,
                now=now + timedelta(minutes=14),
            ),
        )
        retry = occasion_store.claim_next_due(
            self.db_path,
            1,
            now=now + timedelta(minutes=15),
        )
        self.assertEqual(key, retry["occurrence_key"])
        self.assertEqual("generation", retry["stage"])
        self.assertEqual(2, retry["attempt_count"])

    def test_canonical_payload_is_reused_after_delivery_failure(self):
        now = pacific(2026, 7, 4)
        key = occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            now,
        )[0]
        generating = occasion_store.claim_next_due(
            self.db_path,
            1,
            now=now,
        )
        content = valid_reflection()
        self.assertTrue(
            occasion_store.store_prepared(
                self.db_path,
                key,
                generating["lease_token"],
                content,
                [{"kind": "occasion_metadata", "ref": "occasion:test"}],
                "context-hash",
                now=now,
            )
        )
        delivering = occasion_store.claim_next_due(
            self.db_path,
            1,
            now=now,
        )
        self.assertEqual("delivery", delivering["stage"])
        self.assertTrue(
            occasion_store.fail_claim(
                self.db_path,
                key,
                delivering["lease_token"],
                stage="delivery",
                reason="discord_send_runtimeerror",
                retry_minutes=2,
                now=now,
            )
        )
        retry = occasion_store.claim_next_due(
            self.db_path,
            1,
            now=now + timedelta(minutes=2),
        )
        self.assertEqual("delivery", retry["stage"])
        self.assertEqual(content, retry["canonical_content"])
        self.assertEqual("delivery_failed", retry["previous_state"])

    def test_disabled_occurrence_is_terminal_and_never_claimed(self):
        now = pacific(2026, 7, 4)
        key = occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            now,
            disabled_ids={"independence_day"},
        )[0]
        row = occasion_store.get_occurrence(self.db_path, key)
        self.assertEqual("cancelled", row["state"])
        self.assertEqual(
            {},
            occasion_store.claim_next_due(self.db_path, 1, now=now),
        )

    def test_occurrence_key_and_delivery_nonce_are_stable(self):
        key = occasion_store.occurrence_key(
            42,
            date(2026, 7, 4),
            "independence_day",
        )
        self.assertEqual(
            "occasion:occasion_calendar_v1:42:2026-07-04:independence_day",
            key,
        )
        self.assertEqual(
            occasion_store.delivery_nonce(key),
            occasion_store.delivery_nonce(key),
        )
        self.assertLess(occasion_store.delivery_nonce(key), 2**63)


@unittest.skipIf(bnl01_bot is None, "discord.py is not installed")
class OccasionBotPathTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(delete=False)
        handle.close()
        self.db_path = handle.name
        self.db_patch = mock.patch.object(bnl01_bot, "DB_FILE", self.db_path)
        self.db_patch.start()
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                "BNL_OCCASION_POSTS_ENABLED": "true",
                "BNL_OCCASION_DISABLED_IDS": "",
            },
        )
        self.env_patch.start()
        bnl01_bot.init_db()

    def tearDown(self):
        self.env_patch.stop()
        self.db_patch.stop()
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass

    def run_cycle(self, channel, now, generator):
        fake_client = SimpleNamespace(user=SimpleNamespace(id=channel.bot_user_id))
        with mock.patch.object(bnl01_bot, "client", fake_client), \
             mock.patch.object(
                 bnl01_bot,
                 "generate_occasion_reflection",
                 new=generator,
             ):
            return asyncio.run(
                bnl01_bot.process_due_occasion_for_guild(
                    1,
                    channel.id,
                    channel,
                    now_pacific=now,
                )
            )

    def test_holiday_posts_without_recent_signal_and_only_once(self):
        now = pacific(2026, 7, 4)
        content = valid_reflection()
        channel = FakeChannel()
        generator = mock.AsyncMock(return_value=(content, ""))

        first = self.run_cycle(channel, now, generator)
        second = self.run_cycle(channel, now, generator)

        self.assertEqual("published", first["status"])
        self.assertEqual("idle", second["status"])
        self.assertEqual(1, generator.await_count)
        self.assertEqual(1, len(channel.send_attempts))
        sent_content, kwargs = channel.send_attempts[0]
        self.assertEqual(content, sent_content)
        self.assertEqual(
            occasion_store.delivery_nonce(first["occurrenceKey"]),
            kwargs["nonce"],
        )
        self.assertIsNotNone(kwargs["allowed_mentions"])
        with sqlite3.connect(self.db_path) as conn:
            occurrence = conn.execute(
                """
                SELECT state,canonical_content,discord_message_id
                FROM bnl_occasion_occurrences
                """
            ).fetchone()
            log_rows = conn.execute(
                "SELECT source_type FROM ambient_log"
            ).fetchall()
        self.assertEqual(("published", content, "1001"), occurrence)
        self.assertEqual([("occasion",)], log_rows)
        capacity = bnl01_bot.get_ambient_capacity_state(
            1,
            channel.id,
            now_pacific=now,
        )
        self.assertEqual(1, capacity["actualPosts"])
        self.assertEqual(1, capacity["capacityUsed"])
        self.assertEqual(0, capacity["occasionReserved"])
        for source_type in ("ambient", "dormant_echo", "showday"):
            self.assertFalse(
                bnl01_bot.ambient_capacity_decision(
                    1,
                    channel.id,
                    source_type,
                    now_pacific=now,
                )["allowed"]
            )

    def test_occasion_reserves_normal_day_capacity_before_target_time(self):
        now = pacific(2026, 7, 4, 9, 0)

        capacity = bnl01_bot.get_ambient_capacity_state(
            1,
            222,
            now_pacific=now,
        )
        self.assertEqual(0, capacity["actualPosts"])
        self.assertEqual(1, capacity["capacityUsed"])
        self.assertEqual(1, capacity["occasionReserved"])
        self.assertFalse(
            bnl01_bot.ambient_capacity_decision(
                1,
                222,
                "ambient",
                now_pacific=now,
            )["allowed"]
        )
        self.assertTrue(
            bnl01_bot.ambient_capacity_decision(
                1,
                222,
                "occasion",
                now_pacific=now,
            )["allowed"]
        )

    def test_disabled_occasion_does_not_reserve_capacity(self):
        now = pacific(2026, 7, 4, 9, 0)
        with mock.patch.dict(
            os.environ,
            {"BNL_OCCASION_DISABLED_IDS": "independence_day"},
        ):
            capacity = bnl01_bot.get_ambient_capacity_state(
                1,
                222,
                now_pacific=now,
            )
        self.assertEqual(0, capacity["actualPosts"])
        self.assertEqual(0, capacity["capacityUsed"])
        self.assertEqual(0, capacity["occasionReserved"])

    def test_provider_failure_retries_after_restart_and_eventually_posts(self):
        now = pacific(2026, 7, 4)
        channel = FakeChannel()
        failure = mock.AsyncMock(
            return_value=("", "occasion_generation_failed")
        )
        first = self.run_cycle(channel, now, failure)
        self.assertEqual("retryable", first["status"])
        self.assertEqual([], channel.send_attempts)
        retry_capacity = bnl01_bot.get_ambient_capacity_state(
            1,
            channel.id,
            now_pacific=now,
        )
        self.assertEqual(0, retry_capacity["actualPosts"])
        self.assertEqual(1, retry_capacity["capacityUsed"])
        self.assertEqual(1, retry_capacity["occasionReserved"])

        content = valid_reflection()
        success = mock.AsyncMock(return_value=(content, ""))
        second = self.run_cycle(
            channel,
            now + timedelta(minutes=15),
            success,
        )
        self.assertEqual("published", second["status"])
        self.assertEqual(1, success.await_count)
        self.assertEqual(content, channel.messages[0].content)

    def test_occasion_waits_when_an_existing_post_already_filled_normal_cap(self):
        now = pacific(2026, 7, 4)
        channel = FakeChannel()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO ambient_log(
                    guild_id,channel_id,message,source_type,posted_at
                ) VALUES(?,?,?,?,?)
                """,
                (1, channel.id, "earlier", "ambient", now.isoformat()),
            )
        generator = mock.AsyncMock(
            return_value=(valid_reflection(), "")
        )

        result = self.run_cycle(channel, now, generator)

        self.assertEqual("waiting_for_capacity", result["status"])
        self.assertEqual("daily_cap_reached", result["reason"])
        self.assertEqual(0, generator.await_count)
        self.assertEqual([], channel.send_attempts)

    def test_discord_failure_reuses_canonical_payload_without_regeneration(self):
        now = pacific(2026, 7, 4)
        content = valid_reflection()
        channel = FakeChannel(fail_sends=1)
        channel.messages.append(
            FakeMessage(
                900,
                content,
                channel.bot_user_id,
                nonce=123,
            )
        )
        generator = mock.AsyncMock(return_value=(content, ""))

        first = self.run_cycle(channel, now, generator)
        retry = self.run_cycle(
            channel,
            now + timedelta(minutes=2),
            generator,
        )

        self.assertEqual("retryable", first["status"])
        self.assertEqual("published", retry["status"])
        self.assertEqual(1, generator.await_count)
        self.assertEqual(2, len(channel.send_attempts))
        self.assertEqual(content, channel.send_attempts[0][0])
        self.assertEqual(content, channel.send_attempts[1][0])
        self.assertEqual(
            channel.send_attempts[0][1]["nonce"],
            channel.send_attempts[1][1]["nonce"],
        )
        self.assertEqual(2, len(channel.messages))
        self.assertEqual(
            channel.send_attempts[1][1]["nonce"],
            channel.messages[-1].nonce,
        )

    def test_crash_after_send_is_reconciled_without_second_delivery(self):
        now = pacific(2026, 7, 4)
        content = valid_reflection()
        channel = FakeChannel()
        generator = mock.AsyncMock(return_value=(content, ""))
        fake_client = SimpleNamespace(user=SimpleNamespace(id=channel.bot_user_id))

        with mock.patch.object(bnl01_bot, "client", fake_client), \
             mock.patch.object(
                 bnl01_bot,
                 "generate_occasion_reflection",
                 new=generator,
             ), \
             mock.patch.object(
                 bnl01_bot,
                 "mark_occasion_published",
                 return_value=False,
             ):
            first = asyncio.run(
                bnl01_bot.process_due_occasion_for_guild(
                    1,
                    channel.id,
                    channel,
                    now_pacific=now,
                )
            )
        self.assertEqual("delivery_record_pending", first["status"])
        self.assertEqual(1, len(channel.messages))
        crash_capacity = bnl01_bot.get_ambient_capacity_state(
            1,
            channel.id,
            now_pacific=now,
        )
        self.assertEqual(0, crash_capacity["actualPosts"])
        self.assertEqual(1, crash_capacity["capacityUsed"])
        self.assertEqual(1, crash_capacity["occasionReserved"])
        self.assertFalse(
            bnl01_bot.ambient_capacity_decision(
                1,
                channel.id,
                "showday",
                now_pacific=now,
            )["allowed"]
        )

        retry = self.run_cycle(
            channel,
            now + timedelta(minutes=21),
            generator,
        )
        self.assertEqual("published", retry["status"])
        self.assertTrue(retry["reconciled"])
        self.assertEqual(1, len(channel.send_attempts))
        self.assertEqual(1, generator.await_count)
        with sqlite3.connect(self.db_path) as conn:
            state, message_id = conn.execute(
                """
                SELECT state,discord_message_id
                FROM bnl_occasion_occurrences
                """
            ).fetchone()
        self.assertEqual(("published", "1001"), (state, message_id))

    def test_global_kill_switch_cancels_without_generation_or_delivery(self):
        now = pacific(2026, 7, 4)
        channel = FakeChannel()
        generator = mock.AsyncMock(
            return_value=(valid_reflection(), "")
        )
        with mock.patch.dict(
            os.environ,
            {"BNL_OCCASION_POSTS_ENABLED": "false"},
        ):
            result = self.run_cycle(channel, now, generator)
        self.assertEqual("disabled", result["status"])
        self.assertEqual(0, generator.await_count)
        self.assertEqual([], channel.send_attempts)
        with sqlite3.connect(self.db_path) as conn:
            state = conn.execute(
                "SELECT state FROM bnl_occasion_occurrences"
            ).fetchone()[0]
        self.assertEqual("cancelled", state)

    def test_validator_enforces_full_length_range_and_grounding(self):
        definition = occasion_store.occasion_by_id("independence_day")
        self.assertEqual(
            "occasion_output_too_short",
            bnl01_bot.validate_occasion_reflection(
                valid_reflection(target_chars=bnl01_bot.OCCASION_MIN_CHARS - 1),
                definition,
            ),
        )
        self.assertEqual(
            "",
            bnl01_bot.validate_occasion_reflection(
                valid_reflection(target_chars=bnl01_bot.OCCASION_MIN_CHARS),
                definition,
            ),
        )
        self.assertEqual(
            "",
            bnl01_bot.validate_occasion_reflection(
                valid_reflection(target_chars=bnl01_bot.OCCASION_MAX_CHARS),
                definition,
            ),
        )
        self.assertEqual(
            "occasion_output_too_long",
            bnl01_bot.validate_occasion_reflection(
                valid_reflection(target_chars=bnl01_bot.OCCASION_MAX_CHARS + 1),
                definition,
            ),
        )
        missing_occasion = valid_reflection(
            occasion_name="A summer day",
            target_chars=920,
        )
        self.assertEqual(
            "occasion_anchor_missing",
            bnl01_bot.validate_occasion_reflection(
                missing_occasion,
                definition,
            ),
        )
        missing_barcode = (
            "Independence Day is a living reminder. "
            + ("x" * 880)
            + "."
        )
        self.assertEqual(
            "barcode_grounding_missing",
            bnl01_bot.validate_occasion_reflection(
                missing_barcode,
                definition,
            ),
        )

    def test_validator_allows_glitched_voice_but_blocks_process_reports(self):
        definition = occasion_store.occasion_by_id("juneteenth")
        technical = (
            "Juneteenth enters the BARCODE Network through a damaged carrier "
            "wave, static folding around a signal that arrived late but refused "
            "to disappear. The signal remains human, unfinished, and alive. "
            + valid_reflection(
                occasion_name="Juneteenth",
                target_chars=900,
            )
        )
        technical = bnl01_bot.sanitize_occasion_reflection(technical)
        self.assertGreaterEqual(len(technical), bnl01_bot.OCCASION_MIN_CHARS)
        self.assertEqual(
            "",
            bnl01_bot.validate_occasion_reflection(technical, definition),
        )

        report = (
            "Juneteenth BARCODE Network status report: this system indexed "
            "recent messages from the database and generated this reflection "
            "from the supplied context. "
            + ("All source data has been processed. " * 25)
        )
        report = bnl01_bot.sanitize_occasion_reflection(report)
        self.assertGreaterEqual(len(report), bnl01_bot.OCCASION_MIN_CHARS)
        self.assertEqual(
            "internal_process_report",
            bnl01_bot.validate_occasion_reflection(report, definition),
        )

    def test_generation_context_uses_corrected_canon_and_no_journal_lane(self):
        definition = occasion_store.occasion_by_id("independence_day")
        packet = bnl01_bot.build_occasion_generation_context(1, definition)
        lowered = packet["context"].lower()
        self.assertIn("6 bit is an artist, mc, host", lowered)
        self.assertIn("galaknoise is barcode's music producer", lowered)
        self.assertNotIn("journal", lowered)
        kinds = {ref["kind"] for ref in packet["sourceRefs"]}
        self.assertNotIn("journal", kinds)
        self.assertNotIn("queue", kinds)
        self.assertIn("occasion_metadata", kinds)
        self.assertIn("occasion_source", kinds)
        self.assertIn("approved_canon", kinds)

        radio = occasion_store.occasion_by_id("world_radio_day")
        radio_packet = bnl01_bot.build_occasion_generation_context(1, radio)
        self.assertIn("Occasion: World Radio Day", radio_packet["context"])
        self.assertNotIn("BARCODE Open Channel Day", radio_packet["context"])
        self.assertNotIn("BARCODE house tradition", radio_packet["context"])

    def test_shared_capacity_counts_every_automatic_ambient_owned_source(self):
        now = pacific(2026, 7, 23, 12, 0)
        now_iso = now.isoformat()
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO ambient_log(
                    guild_id,channel_id,message,source_type,posted_at
                ) VALUES(?,?,?,?,?)
                """,
                (
                    (1, 222, "ambient", "ambient", now_iso),
                    (1, 222, "echo", "dormant_echo", now_iso),
                    (1, 222, "holiday", "occasion", now_iso),
                    (1, 222, "show", "showday", now_iso),
                    (1, 222, "manual test", "showday_test", now_iso),
                ),
            )
        capacity = bnl01_bot.get_ambient_capacity_state(
            1,
            222,
            now_pacific=now,
        )
        self.assertEqual(4, capacity["actualPosts"])
        self.assertEqual(4, capacity["capacityUsed"])
        self.assertEqual(
            {
                "ambient": 1,
                "dormant_echo": 1,
                "occasion": 1,
                "showday": 1,
            },
            capacity["sourceCounts"],
        )

    def test_high_activity_never_allows_a_third_post(self):
        now = pacific(2026, 7, 4, 9, 0)
        occasion_store.seed_occurrences(
            self.db_path,
            1,
            222,
            now,
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO ambient_log(
                    guild_id,channel_id,message,source_type,posted_at
                ) VALUES(?,?,?,?,?)
                """,
                (1, 222, "show", "showday", now.isoformat()),
            )

        with mock.patch.object(
            bnl01_bot,
            "ambient_daily_post_cap",
            return_value=2,
        ):
            capacity = bnl01_bot.get_ambient_capacity_state(
                1,
                222,
                now_pacific=now,
            )
            self.assertEqual(1, capacity["actualPosts"])
            self.assertEqual(2, capacity["capacityUsed"])
            self.assertTrue(
                bnl01_bot.ambient_capacity_decision(
                    1,
                    222,
                    "occasion",
                    now_pacific=now,
                )["allowed"]
            )
            for source_type in ("ambient", "dormant_echo", "showday"):
                decision = bnl01_bot.ambient_capacity_decision(
                    1,
                    222,
                    source_type,
                    now_pacific=now,
                )
                self.assertFalse(decision["allowed"])
                self.assertEqual(2, decision["cap"])

    def test_high_activity_cap_uses_public_day_signal_not_weekday(self):
        friday = pacific(2026, 7, 24, 12, 0)
        self.assertEqual(4, friday.weekday())
        self.assertEqual(
            1,
            bnl01_bot.ambient_daily_post_cap(1, now_pacific=friday),
        )
        start_utc = friday.replace(
            hour=9,
            minute=0,
            second=0,
        ).astimezone(timezone.utc)
        with sqlite3.connect(self.db_path) as conn:
            for index in range(15):
                conn.execute(
                    """
                    INSERT INTO conversations(
                        user_id,user_name,guild_id,channel_name,channel_policy,
                        role,content,timestamp
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        100 + (index % 4),
                        f"member-{index % 4}",
                        1,
                        "barcode-bot",
                        "public_home",
                        "user",
                        "public conversation signal " + ("x" * 40),
                        (
                            start_utc + timedelta(minutes=index)
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                    ),
                )
            conn.execute(
                """
                INSERT INTO conversations(
                    user_id,user_name,guild_id,channel_name,channel_policy,
                    role,content,timestamp
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    999,
                    "internal",
                    1,
                    "research-and-development",
                    "internal_controlled",
                    "user",
                    "x" * 5000,
                    start_utc.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
        signal = bnl01_bot.get_public_activity_today(
            1,
            now_pacific=friday,
        )
        self.assertEqual(15, signal["messages"])
        self.assertEqual(4, signal["uniqueUsers"])
        self.assertGreaterEqual(signal["characters"], 700)
        self.assertEqual(
            2,
            bnl01_bot.ambient_daily_post_cap(1, now_pacific=friday),
        )

    def test_second_ambient_is_optional_and_scheduled_only_while_capacity_remains(self):
        now = pacific(2026, 7, 23, 12, 0)
        with mock.patch.object(
            bnl01_bot,
            "ambient_daily_post_cap",
            return_value=2,
        ), mock.patch.object(
            bnl01_bot.random,
            "uniform",
            return_value=4.0,
        ), mock.patch.object(
            bnl01_bot,
            "update_guild_ambient_times",
        ) as update:
            scheduled = bnl01_bot.schedule_after_ambient_post(
                1,
                "first",
                1,
                now_pacific=now,
            )
        self.assertEqual(pacific(2026, 7, 23, 16, 0).isoformat(), scheduled)
        update.assert_called_once_with(1, "first", scheduled)

        with mock.patch.object(
            bnl01_bot,
            "ambient_daily_post_cap",
            return_value=1,
        ), mock.patch.object(
            bnl01_bot,
            "schedule_next_day_ambient",
            return_value="next-day",
        ) as next_day:
            scheduled = bnl01_bot.schedule_after_ambient_post(
                1,
                "first",
                1,
                now_pacific=now,
            )
        self.assertEqual("next-day", scheduled)
        next_day.assert_called_once_with(1, "first")


if __name__ == "__main__":
    unittest.main()
