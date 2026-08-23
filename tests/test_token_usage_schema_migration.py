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


class TokenUsageSchemaMigrationTests(unittest.TestCase):
    def test_simultaneous_workers_can_apply_additive_cost_columns(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path = str(Path(tempdir) / "legacy.sqlite")
            with sqlite3.connect(path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE token_usage_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        usage_date TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        route TEXT NOT NULL,
                        model TEXT NOT NULL,
                        prompt_tokens INTEGER NOT NULL DEFAULT 0,
                        candidate_tokens INTEGER NOT NULL DEFAULT 0,
                        thought_tokens INTEGER NOT NULL DEFAULT 0,
                        cached_tokens INTEGER NOT NULL DEFAULT 0,
                        total_tokens INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE model_generation_attempts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        usage_date TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        route TEXT NOT NULL,
                        model TEXT NOT NULL,
                        outcome TEXT NOT NULL,
                        error_category TEXT NOT NULL DEFAULT '',
                        provider_status_code INTEGER NOT NULL DEFAULT 0,
                        prompt_tokens INTEGER NOT NULL DEFAULT 0,
                        candidate_tokens INTEGER NOT NULL DEFAULT 0,
                        thought_tokens INTEGER NOT NULL DEFAULT 0,
                        cached_tokens INTEGER NOT NULL DEFAULT 0,
                        total_tokens INTEGER NOT NULL DEFAULT 0
                    );
                    """
                )
            barrier = threading.Barrier(4)

            def migrate(_index):
                with sqlite3.connect(path, timeout=30) as conn:
                    barrier.wait(timeout=5)
                    bnl01_bot._ensure_token_usage_schema(conn.cursor())
                    conn.commit()

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(migrate, range(4)))

            with sqlite3.connect(path) as conn:
                event_columns = {
                    row[1]
                    for row in conn.execute(
                        "PRAGMA table_info(token_usage_events)"
                    )
                }
                attempt_columns = {
                    row[1]
                    for row in conn.execute(
                        "PRAGMA table_info(model_generation_attempts)"
                    )
                }
            self.assertTrue(
                {"estimated_cost_nanos", "cost_priced", "pricing_version"}
                <= event_columns
            )
            self.assertTrue(
                {
                    "attempt_number",
                    "is_retry",
                    "is_fallback",
                    "estimated_cost_nanos",
                    "cost_priced",
                    "pricing_version",
                }
                <= attempt_columns
            )


if __name__ == "__main__":
    unittest.main()
