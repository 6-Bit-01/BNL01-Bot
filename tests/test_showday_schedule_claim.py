import asyncio
import concurrent.futures
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock


os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class ShowdayScheduleClaimTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "showday.sqlite")
        self.original_db = bnl01_bot.DB_FILE
        bnl01_bot.DB_FILE = self.db_path
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE friday_show_updates (
                    guild_id INTEGER NOT NULL,
                    show_date TEXT NOT NULL,
                    phase_key TEXT NOT NULL,
                    discord_message TEXT,
                    website_message TEXT,
                    fired_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, show_date, phase_key)
                );
                CREATE TABLE friday_show_update_claims (
                    guild_id INTEGER NOT NULL,
                    show_date TEXT NOT NULL,
                    phase_key TEXT NOT NULL,
                    claimed_at TEXT NOT NULL,
                    claim_token TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (guild_id, show_date, phase_key)
                );
                """
            )

    def tearDown(self):
        bnl01_bot.DB_FILE = self.original_db
        self.tempdir.cleanup()

    def test_concurrent_workers_create_one_pre_generation_claim(self):
        barrier = threading.Barrier(4)

        def claim():
            barrier.wait(timeout=5)
            return bnl01_bot.claim_show_update_period(
                42,
                "2026-08-21",
                "show_live",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _index: claim(), range(4)))

        self.assertEqual(sum(bool(result) for result in results), 1)
        self.assertEqual(results.count(None), 3)

    def test_fresh_claim_keeps_duplicate_worker_out(self):
        claim_token = bnl01_bot.claim_show_update_period(
            42,
            "2026-08-21",
            "show_live",
        )
        self.assertTrue(claim_token)
        self.assertIsNone(
            bnl01_bot.claim_show_update_period(
                42,
                "2026-08-21",
                "show_live",
            )
        )

    def test_stale_claim_is_recovered_inside_showday_window(self):
        self.assertGreater(
            bnl01_bot.SHOWDAY_UPDATE_CLAIM_LEASE_SECONDS,
            240,
            "the claim must outlive the maximum provider timeout",
        )
        self.assertLess(
            bnl01_bot.SHOWDAY_UPDATE_CLAIM_LEASE_SECONDS,
            bnl01_bot.SHOWDAY_WINDOW_MINUTES * 60,
            "the claim must expire while the scheduled retry window is open",
        )
        stale_at = datetime.now(timezone.utc) - timedelta(
            seconds=bnl01_bot.SHOWDAY_UPDATE_CLAIM_LEASE_SECONDS + 1
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO friday_show_update_claims
                (guild_id, show_date, phase_key, claimed_at, claim_token)
                VALUES (?, ?, ?, ?, ?)
                """,
                (42, "2026-08-21", "show_live", stale_at.isoformat(), "old-worker"),
            )

        claim_token = bnl01_bot.claim_show_update_period(
            42,
            "2026-08-21",
            "show_live",
        )
        self.assertTrue(claim_token)
        self.assertNotEqual(claim_token, "old-worker")
        with sqlite3.connect(self.db_path) as conn:
            claimed_at, persisted_token = conn.execute(
                "SELECT claimed_at, claim_token FROM friday_show_update_claims"
            ).fetchone()
        self.assertGreater(
            datetime.fromisoformat(claimed_at),
            stale_at,
        )
        self.assertEqual(persisted_token, claim_token)

    def test_malformed_or_materially_future_claim_is_recovered(self):
        for index, claimed_at in enumerate(
            (
                "not-a-timestamp",
                (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat(),
            )
        ):
            phase_key = f"phase-{index}"
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO friday_show_update_claims
                    (guild_id, show_date, phase_key, claimed_at, claim_token)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (42, "2026-08-21", phase_key, claimed_at, "old-worker"),
                )
            self.assertTrue(
                bnl01_bot.claim_show_update_period(
                    42,
                    "2026-08-21",
                    phase_key,
                )
            )

    def test_completed_phase_cannot_be_claimed_again(self):
        claim_token = bnl01_bot.claim_show_update_period(
            42,
            "2026-08-21",
            "show_live",
        )
        self.assertTrue(claim_token)
        self.assertTrue(bnl01_bot.mark_show_update_fired(
            42,
            "2026-08-21",
            "show_live",
            claim_token,
            "discord",
            "website",
        ))

        self.assertIsNone(
            bnl01_bot.claim_show_update_period(
                42,
                "2026-08-21",
                "show_live",
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            claims = conn.execute(
                "SELECT COUNT(*) FROM friday_show_update_claims"
            ).fetchone()[0]
        self.assertEqual(claims, 0)

    def test_superseded_worker_cannot_release_or_complete_new_claim(self):
        old_token = bnl01_bot.claim_show_update_period(
            42,
            "2026-08-21",
            "show_live",
        )
        self.assertTrue(old_token)
        stale_at = datetime.now(timezone.utc) - timedelta(
            seconds=bnl01_bot.SHOWDAY_UPDATE_CLAIM_LEASE_SECONDS + 1
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE friday_show_update_claims SET claimed_at=?",
                (stale_at.isoformat(),),
            )

        new_token = bnl01_bot.claim_show_update_period(
            42,
            "2026-08-21",
            "show_live",
        )
        self.assertTrue(new_token)
        self.assertNotEqual(new_token, old_token)
        self.assertFalse(
            bnl01_bot.release_show_update_claim(
                42,
                "2026-08-21",
                "show_live",
                old_token,
            )
        )
        self.assertFalse(
            bnl01_bot.mark_show_update_fired(
                42,
                "2026-08-21",
                "show_live",
                old_token,
                "old discord",
                "old website",
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            persisted_token = conn.execute(
                "SELECT claim_token FROM friday_show_update_claims"
            ).fetchone()[0]
            fired_count = conn.execute(
                "SELECT COUNT(*) FROM friday_show_updates"
            ).fetchone()[0]
        self.assertEqual(persisted_token, new_token)
        self.assertEqual(fired_count, 0)
        self.assertTrue(
            bnl01_bot.mark_show_update_fired(
                42,
                "2026-08-21",
                "show_live",
                new_token,
                "new discord",
                "new website",
            )
        )

    def test_current_worker_can_renew_but_superseded_token_cannot(self):
        claim_token = bnl01_bot.claim_show_update_period(
            42,
            "2026-08-21",
            "show_live",
        )
        self.assertTrue(claim_token)
        with sqlite3.connect(self.db_path) as conn:
            original = conn.execute(
                "SELECT claimed_at FROM friday_show_update_claims"
            ).fetchone()[0]
            conn.execute(
                "UPDATE friday_show_update_claims SET claimed_at=?",
                ((datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat(),),
            )

        self.assertFalse(
            bnl01_bot.renew_show_update_claim(
                42,
                "2026-08-21",
                "show_live",
                "not-the-owner",
            )
        )
        self.assertTrue(
            bnl01_bot.renew_show_update_claim(
                42,
                "2026-08-21",
                "show_live",
                claim_token,
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            renewed = conn.execute(
                "SELECT claimed_at FROM friday_show_update_claims"
            ).fetchone()[0]
        self.assertGreaterEqual(
            datetime.fromisoformat(renewed),
            datetime.fromisoformat(original),
        )
        self.assertLess(
            bnl01_bot.SHOWDAY_UPDATE_CLAIM_HEARTBEAT_SECONDS,
            bnl01_bot.SHOWDAY_UPDATE_CLAIM_LEASE_SECONDS,
        )

    def test_active_worker_heartbeat_renews_claim_through_provider_work(self):
        claim_token = bnl01_bot.claim_show_update_period(
            42,
            "2026-08-21",
            "show_live",
        )
        self.assertTrue(claim_token)
        older = datetime.now(timezone.utc) - timedelta(minutes=5)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE friday_show_update_claims SET claimed_at=?",
                (older.isoformat(),),
            )

        async def exercise_heartbeat():
            with mock.patch.object(
                bnl01_bot,
                "SHOWDAY_UPDATE_CLAIM_HEARTBEAT_SECONDS",
                0.01,
            ):
                task = asyncio.create_task(
                    bnl01_bot.maintain_show_update_claim(
                        42,
                        "2026-08-21",
                        "show_live",
                        claim_token,
                    )
                )
                await asyncio.sleep(0.04)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        asyncio.run(exercise_heartbeat())
        with sqlite3.connect(self.db_path) as conn:
            renewed_at, persisted_token = conn.execute(
                "SELECT claimed_at, claim_token FROM friday_show_update_claims"
            ).fetchone()
        self.assertGreater(datetime.fromisoformat(renewed_at), older)
        self.assertEqual(persisted_token, claim_token)

    def test_heartbeat_retries_transient_renewal_failure(self):
        attempts = 0

        def renew_after_transient_failure(*_args):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise sqlite3.OperationalError("database is temporarily locked")
            return True

        async def exercise_heartbeat():
            ownership_lost = asyncio.Event()
            with (
                mock.patch.object(
                    bnl01_bot,
                    "SHOWDAY_UPDATE_CLAIM_HEARTBEAT_SECONDS",
                    0.01,
                ),
                mock.patch.object(
                    bnl01_bot,
                    "renew_show_update_claim",
                    side_effect=renew_after_transient_failure,
                ),
            ):
                task = asyncio.create_task(
                    bnl01_bot.maintain_show_update_claim(
                        42,
                        "2026-08-21",
                        "show_live",
                        "current-worker",
                        ownership_lost,
                    )
                )
                await asyncio.sleep(0.04)
                self.assertFalse(task.done())
                self.assertFalse(ownership_lost.is_set())
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        asyncio.run(exercise_heartbeat())
        self.assertGreaterEqual(attempts, 2)

    def test_heartbeat_signals_definitive_ownership_loss(self):
        async def exercise_heartbeat():
            ownership_lost = asyncio.Event()
            with (
                mock.patch.object(
                    bnl01_bot,
                    "SHOWDAY_UPDATE_CLAIM_HEARTBEAT_SECONDS",
                    0.01,
                ),
                mock.patch.object(
                    bnl01_bot,
                    "renew_show_update_claim",
                    return_value=False,
                ),
            ):
                await asyncio.wait_for(
                    bnl01_bot.maintain_show_update_claim(
                        42,
                        "2026-08-21",
                        "show_live",
                        "superseded-worker",
                        ownership_lost,
                    ),
                    timeout=0.1,
                )
            self.assertTrue(ownership_lost.is_set())
            self.assertFalse(
                await bnl01_bot.revalidate_show_update_claim(
                    42,
                    "2026-08-21",
                    "show_live",
                    "superseded-worker",
                    ownership_lost,
                )
            )

        asyncio.run(exercise_heartbeat())


if __name__ == "__main__":
    unittest.main()
