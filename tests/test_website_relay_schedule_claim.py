import concurrent.futures
import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import bnl_website_relay_state as state


class WebsiteRelayScheduleClaimTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        handle.close()
        self.db_path = handle.name
        state.ensure_schema(self.db_path)

    def tearDown(self):
        os.unlink(self.db_path)

    def test_two_workers_claim_same_period_once_and_next_period_succeeds(self):
        barrier = threading.Barrier(2)

        def claim_from_worker():
            barrier.wait(timeout=5)
            return state.claim_scheduled_relay_period(
                self.db_path,
                42,
                "2026-08-22T17:40-07:00",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _worker: claim_from_worker(), range(2)))

        self.assertEqual(sorted(results), [False, True])
        self.assertTrue(
            state.claim_scheduled_relay_period(
                self.db_path,
                42,
                "2026-08-22T18:00-07:00",
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT period_key FROM website_relay_schedule_claims "
                "WHERE guild_id=? ORDER BY period_key",
                (42,),
            ).fetchall()
        self.assertEqual(
            rows,
            [
                ("2026-08-22T17:40-07:00",),
                ("2026-08-22T18:00-07:00",),
            ],
        )

    def test_claim_retention_prunes_oldest_period_for_each_guild(self):
        with mock.patch.object(state, "MAX_SCHEDULE_CLAIMS_PER_GUILD", 2):
            for period_key, claimed_at in (
                ("period-1", "2026-08-22T00:00:00Z"),
                ("period-2", "2026-08-22T00:20:00Z"),
                ("period-3", "2026-08-22T00:40:00Z"),
            ):
                self.assertTrue(
                    state.claim_scheduled_relay_period(
                        self.db_path,
                        42,
                        period_key,
                        claimed_at=claimed_at,
                    )
                )

        with sqlite3.connect(self.db_path) as conn:
            retained = conn.execute(
                "SELECT period_key FROM website_relay_schedule_claims "
                "WHERE guild_id=? ORDER BY claimed_at",
                (42,),
            ).fetchall()
        self.assertEqual(retained, [("period-2",), ("period-3",)])


if __name__ == "__main__":
    unittest.main()
