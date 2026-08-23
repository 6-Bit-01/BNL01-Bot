import concurrent.futures
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
