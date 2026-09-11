"""Routine transcript bounds must preserve eligible, source-backed experiences."""

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


class MomentRetentionPruningTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db_path = str(Path(self.directory.name) / "conversations.db")
        self.db_patch = mock.patch.object(bot, "DB_FILE", self.db_path)
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)
        self.env_patch = mock.patch.dict(
            os.environ,
            {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "1",
            },
        )
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        bot.init_db()
        self.observed_at = datetime.now(timezone.utc) - timedelta(days=90)

    def rows(self, sql, parameters=()):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql, parameters).fetchall()

    def add_source(
        self, user_id, text, *, guild=1, channel=10,
        policy="public_home", role="user", targets=(), observe=True,
    ):
        timestamp = self.observed_at.isoformat()
        with sqlite3.connect(self.db_path) as conn:
            row_id = conn.execute(
                """
                INSERT INTO conversations (
                    user_id,user_name,guild_id,channel_id,channel_name,
                    channel_policy,route_mode,role,content,timestamp
                ) VALUES (?,?,?,?,?,?,'normal_chat',?,?,?)
                """,
                (
                    user_id, "Test Member %s" % user_id, guild, channel,
                    "test-room", policy, role, text, timestamp,
                ),
            ).lastrowid
            if targets:
                conn.executemany(
                    """
                    INSERT INTO conversation_response_participants (
                        conversation_row_id,guild_id,user_id
                    ) VALUES (?,?,?)
                    """,
                    [(row_id, guild, target) for target in targets],
                )
            result = ledger.shadow_conversation_row(
                conn, row_id=row_id, user_id=user_id,
                user_name="Test Member %s" % user_id, guild_id=guild,
                role=role, content=text, channel_name="test-room",
                channel_policy=policy, channel_id=channel,
                route_mode="normal_chat", observed_at=timestamp,
                conversation_target_user_ids=targets,
            )
            self.assertEqual(result.outcome, "inserted")
            if observe:
                moments.observe_ledger_entry(conn, result.entry_id)
        return row_id, result.entry_id

    def add_moment(self, *, guild=1, channel=10, policy="public_home", model=False):
        sources = [self.add_source(
            41, "The bean recipe needs smoky seasoning", guild=guild,
            channel=channel, policy=policy,
        )]
        if model:
            sources.append(self.add_source(
                0, "The bean recipe can compare the smoky seasoning options",
                guild=guild, channel=channel, policy=policy,
                role="model", targets=(41, 42),
            ))
        sources.extend((
            self.add_source(
                42, "The bean recipe works with smoked paprika", guild=guild,
                channel=channel, policy=policy,
            ),
            self.add_source(
                41, "The bean recipe should keep the smoky seasoning balanced",
                guild=guild, channel=channel, policy=policy,
            ),
        ))
        with sqlite3.connect(self.db_path) as conn:
            moments.sweep_expired_windows(
                conn, guild_id=guild,
                now=(self.observed_at + timedelta(minutes=3)).isoformat(),
            )
            moment = conn.execute(
                """
                SELECT moment_id,lifecycle_status FROM memory_moment_windows
                WHERE guild_id=? AND channel_id=?
                """, (guild, channel),
            ).fetchone()
        self.assertIsNotNone(moment)
        self.assertEqual(moment[1], "finalized")
        return moment[0], sources

    def recall(self, *, guild=1, participant=None):
        # Reopen the database for every read: recall cannot depend on a process cache.
        with sqlite3.connect(self.db_path) as conn:
            kwargs = dict(
                guild_id=guild, topic_text="bean recipe smoky seasoning",
                allowed_channel_policies=("public_home", "public_context"),
            )
            if participant is None:
                return moments.select_public_situation_moment_gists(conn, **kwargs)
            return moments.select_public_participant_moment_gists(
                conn, participant_key="discord_user:%s" % participant, **kwargs,
            )

    def test_pruning_preserves_old_public_experience_and_exact_recent_cutoff(self):
        moment_id, sources = self.add_moment()
        ordinary = self.add_source(41, "An unrelated ordinary note", channel=20, observe=False)
        recent = self.add_source(41, "A newer ordinary note", channel=20, observe=False)
        self.assertEqual([item.moment_id for item in self.recall()], [moment_id])
        original_people = self.rows(
            "SELECT participant_key,participant_role FROM memory_moment_participants "
            "WHERE moment_id=? ORDER BY participant_key,participant_role", (moment_id,),
        )

        bot.prune_conversation_history(41, 1, max_rows=1)
        bot.prune_conversation_history(42, 1, max_rows=0)

        self.assertEqual(
            {row[0] for row in self.rows("SELECT id FROM conversations")},
            {row_id for row_id, _ in sources} | {recent[0]},
        )
        self.assertEqual(self.rows(
            "SELECT entry_id FROM memory_ledger_entries WHERE entry_id=?", (ordinary[1],),
        ), [])
        self.assertEqual([item.moment_id for item in self.recall()], [moment_id])
        self.assertEqual([item.moment_id for item in self.recall(participant=41)], [moment_id])
        self.assertEqual(self.recall(participant=44), ())
        self.assertEqual(self.rows(
            "SELECT participant_key,participant_role FROM memory_moment_participants "
            "WHERE moment_id=? ORDER BY participant_key,participant_role", (moment_id,),
        ), original_people)

    def test_group_reply_roots_survive_flags_off_but_explicit_member_clear_still_scrubs(self):
        moment_id, sources = self.add_moment(model=True)
        model_row, model_entry = sources[1]
        with mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "0",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "0",
        }):
            for user_id in (0, 41, 42):
                bot.prune_conversation_history(user_id, 1, max_rows=0)

        self.assertEqual(
            {row[0] for row in self.rows("SELECT id FROM conversations")},
            {row_id for row_id, _ in sources},
        )
        self.assertEqual(self.rows(
            "SELECT user_id FROM conversation_response_participants "
            "WHERE conversation_row_id=? ORDER BY user_id", (model_row,),
        ), [(41,), (42,)])
        self.assertEqual([item.moment_id for item in self.recall()], [moment_id])

        bot.clear_user_history(41, 1)

        self.assertEqual(self.recall(), ())
        self.assertEqual(self.rows(
            "SELECT id FROM conversations WHERE id=?", (model_row,),
        ), [])
        self.assertEqual(self.rows(
            "SELECT entry_id FROM memory_ledger_entries WHERE entry_id=?", (model_entry,),
        ), [])
        self.assertEqual(self.rows(
            "SELECT user_id FROM conversation_response_participants "
            "WHERE conversation_row_id=?", (model_row,),
        ), [])
        self.assertEqual(self.rows(
            "SELECT lifecycle_status,summary,public_usable FROM memory_moment_windows "
            "WHERE moment_id=?", (moment_id,),
        ), [("retracted", "", 0)])

    def test_ineligible_or_invalid_sources_do_not_pin_transcript(self):
        variants = ("private", "retracted", "corrected", "dangling_ledger", "dangling_transcript")
        for offset, variant in enumerate(variants):
            with self.subTest(variant=variant):
                guild = 100 + offset
                moment_id, sources = self.add_moment(
                    guild=guild, channel=100 + offset,
                    policy="sealed_test" if variant == "private" else "public_home",
                )
                with sqlite3.connect(self.db_path) as conn:
                    if variant == "retracted":
                        conn.execute(
                            "UPDATE memory_moment_windows SET lifecycle_status='retracted' "
                            "WHERE moment_id=?", (moment_id,),
                        )
                    elif variant == "corrected":
                        conn.execute(
                            "UPDATE memory_ledger_entries SET lifecycle_status='corrected' "
                            "WHERE entry_id=?", (sources[0][1],),
                        )
                    elif variant == "dangling_ledger":
                        conn.execute(
                            "DELETE FROM memory_ledger_entries WHERE entry_id=?", (sources[0][1],),
                        )
                    elif variant == "dangling_transcript":
                        conn.execute("DELETE FROM conversations WHERE id=?", (sources[0][0],))
                    self.assertEqual(moments.retained_moment_conversation_sources(
                        conn, guild_id=guild,
                        source_row_ids=[row_id for row_id, _ in sources],
                    ), set())
                bot.prune_conversation_history(41, guild, max_rows=0)
                bot.prune_conversation_history(42, guild, max_rows=0)
                self.assertEqual(self.rows(
                    "SELECT id FROM conversations WHERE guild_id=?", (guild,),
                ), [])

    def test_guild_clear_preserves_other_guild_retained_experience(self):
        first_moment, first_sources = self.add_moment(guild=1)
        second_moment, second_sources = self.add_moment(guild=2)
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(moments.retained_moment_conversation_sources(
                conn, guild_id=1,
                source_row_ids=[row_id for row_id, _ in first_sources + second_sources],
            ), {row_id for row_id, _ in first_sources})
        for guild in (1, 2):
            for user_id in (41, 42):
                bot.prune_conversation_history(user_id, guild, max_rows=0)

        self.assertEqual([item.moment_id for item in self.recall(guild=1)], [first_moment])
        bot.clear_guild_history(1)

        self.assertEqual(self.recall(guild=1), ())
        self.assertEqual([item.moment_id for item in self.recall(guild=2)], [second_moment])
        self.assertEqual(
            {row[0] for row in self.rows("SELECT id FROM conversations")},
            {row_id for row_id, _ in second_sources},
        )

    def test_overflow_lookup_does_not_scan_unrelated_guild_moment_history(self):
        moment_id, sources = self.add_moment()
        with sqlite3.connect(self.db_path) as conn:
            # Matching public metadata elsewhere in this guild must not make
            # pruning three source rows inspect the entire Moment history.
            conn.executemany(
                """
                INSERT INTO memory_moment_windows (
                    moment_id,guild_id,channel_id,channel_policy,route_mode,
                    topic_key,window_started_at,last_activity_at,
                    lifecycle_status,visibility,public_usable,created_at,updated_at
                )
                SELECT ?,guild_id,channel_id,channel_policy,route_mode,
                    topic_key,window_started_at,last_activity_at,
                    lifecycle_status,visibility,public_usable,created_at,updated_at
                FROM memory_moment_windows WHERE moment_id=?
                """,
                [("unrelated-history-%s" % index, moment_id) for index in range(2000)],
            )
            vm_steps = 0
            step_budget = 10_000

            def enforce_budget():
                nonlocal vm_steps
                vm_steps += 100
                return int(vm_steps > step_budget)

            conn.set_progress_handler(enforce_budget, 100)
            try:
                retained = moments.retained_moment_conversation_sources(
                    conn, guild_id=1,
                    source_row_ids=[row_id for row_id, _ in sources],
                )
            except sqlite3.OperationalError:
                if vm_steps > step_budget:
                    self.fail("Retention lookup exceeded its bounded SQLite work budget")
                raise
            finally:
                conn.set_progress_handler(None, 0)
            self.assertEqual(retained, {row_id for row_id, _ in sources})

    def test_concurrent_finalization_defers_prune_without_losing_moment_sources(self):
        moment_id, sources = self.add_moment()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                "UPDATE memory_moment_windows SET lifecycle_status='open' WHERE moment_id=?",
                (moment_id,),
            )
        read_retained_sources = moments.retained_moment_conversation_sources

        def finalize_after_retention_read(conn, **kwargs):
            retained = read_retained_sources(conn, **kwargs)
            self.assertEqual(retained, set())
            # WAL permits the other connection to finish its write while
            # pruning still holds the preceding snapshot. That old snapshot
            # must never delete sources of the newly finalized experience.
            with sqlite3.connect(self.db_path) as writer:
                moments.finalize_moment(writer, moment_id)
                self.assertEqual(writer.execute(
                    "SELECT lifecycle_status FROM memory_moment_windows WHERE moment_id=?",
                    (moment_id,),
                ).fetchone(), ("finalized",))
            return retained

        with mock.patch.object(
            bot, "retained_moment_conversation_sources",
            side_effect=finalize_after_retention_read,
        ) as retention_read:
            with self.assertLogs(level="WARNING") as captured:
                bot.prune_conversation_history(41, 1, max_rows=0)

        retention_read.assert_called_once()
        self.assertTrue(any(
            "conversation_prune_deferred_for_memory_lifecycle" in line
            for line in captured.output
        ))
        self.assertEqual(
            {row[0] for row in self.rows("SELECT id FROM conversations")},
            {row_id for row_id, _ in sources},
        )
        self.assertEqual([item.moment_id for item in self.recall()], [moment_id])


if __name__ == "__main__":
    unittest.main()
