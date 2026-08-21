from datetime import datetime, timedelta
import os
import sqlite3
import tempfile
import unittest
from unittest import mock


os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


GUILD_ID = 7101


def _insert_memory(
    database,
    *,
    episode_date,
    summary,
    entry_type="notable_moment",
    status="active",
    public_safe=1,
    usage_scope="ambient,direct",
    superseded_by_id=None,
):
    timestamp = f"{episode_date}T20:00:00+00:00"
    with sqlite3.connect(database) as conn:
        cursor = conn.execute(
            """
            INSERT INTO broadcast_memory (
                guild_id, episode_date, submitted_by_user_id,
                submitted_by_name, raw_note, cleaned_summary, entry_type,
                importance, public_safe, affects_next_show, usage_scope,
                target_show_date, valid_until, override_span_count,
                needs_clarification, status, created_at, updated_at,
                superseded_by_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                GUILD_ID,
                episode_date,
                61,
                "6 Bit",
                f"Raw source for {summary}",
                summary,
                entry_type,
                "medium",
                public_safe,
                1 if entry_type == "show_state_override" else 0,
                usage_scope,
                episode_date if entry_type == "show_state_override" else None,
                None,
                1,
                0,
                status,
                timestamp,
                timestamp,
                superseded_by_id,
            ),
        )
        return int(cursor.lastrowid)


class BroadcastMemoryFreshnessTests(unittest.TestCase):
    RECENCY_QUERIES = (
        "What happened on the last show?",
        "What happened on the latest show?",
        "What happened on the most recent show?",
        "What happened on the last episode?",
        "What happened on the latest episode?",
        "What happened on the most recent episode?",
        "What happened on the last broadcast?",
        "What happened on the latest broadcast?",
        "What happened on the most recent broadcast?",
    )

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database = os.path.join(
            self.temporary_directory.name,
            "broadcast-memory.sqlite3",
        )
        self.database_patch = mock.patch.object(
            bnl01_bot,
            "DB_FILE",
            self.database,
        )
        self.database_patch.start()
        bnl01_bot.init_db()

    def tearDown(self):
        self.database_patch.stop()
        self.temporary_directory.cleanup()

    def test_recency_variants_select_one_newest_past_episode_despite_id_order(
        self,
    ):
        today = datetime.now(bnl01_bot.PACIFIC_TZ).date()
        newest_date = (today - timedelta(days=2)).isoformat()
        older_date = (today - timedelta(days=9)).isoformat()

        newest_first_id = _insert_memory(
            self.database,
            episode_date=newest_date,
            summary="NEWEST_EPISODE_PRIMARY",
        )
        _insert_memory(
            self.database,
            episode_date=newest_date,
            summary="NEWEST_EPISODE_SECONDARY",
            entry_type="technical_issue",
        )
        older_later_id = _insert_memory(
            self.database,
            episode_date=older_date,
            summary="OLDER_HIGHER_ID_EPISODE",
        )
        self.assertGreater(older_later_id, newest_first_id)

        selected = bnl01_bot.get_latest_eligible_broadcast_episode_entries(
            GUILD_ID,
            public_only=True,
        )
        self.assertEqual(
            {entry["episode_date"] for entry in selected},
            {newest_date},
        )

        for query in self.RECENCY_QUERIES:
            with self.subTest(query=query):
                self.assertTrue(
                    bnl01_bot._broadcast_historical_recency_query(query)
                )
                context = bnl01_bot.build_broadcast_memory_context(
                    GUILD_ID,
                    query,
                    "public_home",
                )
                self.assertIn(
                    f"newest completed episode ({newest_date})",
                    context,
                )
                self.assertIn("NEWEST_EPISODE_PRIMARY", context)
                self.assertIn("NEWEST_EPISODE_SECONDARY", context)
                self.assertNotIn(older_date, context)
                self.assertNotIn("OLDER_HIGHER_ID_EPISODE", context)

    def test_latest_episode_excludes_future_override_inactive_and_superseded_rows(
        self,
    ):
        today = datetime.now(bnl01_bot.PACIFIC_TZ).date()
        newest_date = (today - timedelta(days=2)).isoformat()
        future_date = (today + timedelta(days=5)).isoformat()

        _insert_memory(
            self.database,
            episode_date=newest_date,
            summary="ELIGIBLE_NEWEST_EPISODE",
        )
        _insert_memory(
            self.database,
            episode_date=newest_date,
            summary="INACTIVE_ROW_MUST_NOT_APPEAR",
            status="inactive",
        )
        _insert_memory(
            self.database,
            episode_date=newest_date,
            summary="SUPERSEDED_ROW_MUST_NOT_APPEAR",
            superseded_by_id=999,
        )
        _insert_memory(
            self.database,
            episode_date=future_date,
            summary="FUTURE_OVERRIDE_MUST_NOT_APPEAR",
            entry_type="show_state_override",
        )

        context = bnl01_bot.build_broadcast_memory_context(
            GUILD_ID,
            "What was the latest Barcode Radio show?",
            "public_home",
        )

        self.assertIn(f"newest completed episode ({newest_date})", context)
        self.assertIn("ELIGIBLE_NEWEST_EPISODE", context)
        self.assertNotIn("INACTIVE_ROW_MUST_NOT_APPEAR", context)
        self.assertNotIn("SUPERSEDED_ROW_MUST_NOT_APPEAR", context)
        self.assertNotIn("FUTURE_OVERRIDE_MUST_NOT_APPEAR", context)
        self.assertNotIn(future_date, context)

    def test_ineligible_latest_past_episode_holds_instead_of_falling_back(
        self,
    ):
        today = datetime.now(bnl01_bot.PACIFIC_TZ).date()
        latest_past_date = (today - timedelta(days=2)).isoformat()
        older_date = (today - timedelta(days=9)).isoformat()
        future_date = (today + timedelta(days=5)).isoformat()

        _insert_memory(
            self.database,
            episode_date=older_date,
            summary="OLDER_ELIGIBLE_EVIDENCE_MUST_NOT_BE_SUBSTITUTED",
        )
        _insert_memory(
            self.database,
            episode_date=latest_past_date,
            summary="LATEST_EPISODE_INACTIVE",
            status="inactive",
        )
        _insert_memory(
            self.database,
            episode_date=latest_past_date,
            summary="LATEST_EPISODE_SUPERSEDED",
            superseded_by_id=999,
        )
        _insert_memory(
            self.database,
            episode_date=future_date,
            summary="FUTURE_SHOW_STATE_OVERRIDE",
            entry_type="show_state_override",
        )

        selected = bnl01_bot.get_latest_eligible_broadcast_episode_entries(
            GUILD_ID,
            public_only=True,
        )
        context = bnl01_bot.build_broadcast_memory_context(
            GUILD_ID,
            "What happened on the most recent broadcast?",
            "public_home",
        )

        self.assertEqual(selected, [])
        self.assertEqual(context, "")
        self.assertNotIn(
            "OLDER_ELIGIBLE_EVIDENCE_MUST_NOT_BE_SUBSTITUTED",
            context,
        )
