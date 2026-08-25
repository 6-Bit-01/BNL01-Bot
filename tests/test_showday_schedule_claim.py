import concurrent.futures
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest


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

        self.assertEqual(results.count(True), 1)
        self.assertEqual(results.count(False), 3)

    def test_fresh_claim_keeps_duplicate_worker_out(self):
        self.assertTrue(
            bnl01_bot.claim_show_update_period(
                42,
                "2026-08-21",
                "show_live",
            )
        )
        self.assertFalse(
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
                "INSERT INTO friday_show_update_claims VALUES (?, ?, ?, ?)",
                (42, "2026-08-21", "show_live", stale_at.isoformat()),
            )

        self.assertTrue(
            bnl01_bot.claim_show_update_period(
                42,
                "2026-08-21",
                "show_live",
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            claimed_at = conn.execute(
                "SELECT claimed_at FROM friday_show_update_claims"
            ).fetchone()[0]
        self.assertGreater(
            datetime.fromisoformat(claimed_at),
            stale_at,
        )

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
                    "INSERT INTO friday_show_update_claims VALUES (?, ?, ?, ?)",
                    (42, "2026-08-21", phase_key, claimed_at),
                )
            self.assertTrue(
                bnl01_bot.claim_show_update_period(
                    42,
                    "2026-08-21",
                    phase_key,
                )
            )

    def test_completed_phase_cannot_be_claimed_again(self):
        self.assertTrue(
            bnl01_bot.claim_show_update_period(
                42,
                "2026-08-21",
                "show_live",
            )
        )
        bnl01_bot.mark_show_update_fired(
            42,
            "2026-08-21",
            "show_live",
            "discord",
            "website",
        )

        self.assertFalse(
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


if __name__ == "__main__":
    unittest.main()
