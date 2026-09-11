"""Existing memory tiers retain exact transcript roots until their own retirement."""

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
from bnl_memory_governance import (
    correct_member_memory,
    forget_member_memory,
    view_member_memory,
)


class TierRetentionPruningTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db_path = str(Path(directory.name) / "conversations.db")
        db_patch = mock.patch.object(bot, "DB_FILE", self.db_path)
        db_patch.start()
        self.addCleanup(db_patch.stop)
        # Tier persistence and source protection must work with ledger and
        # Moment capture disabled, as they did before either projection existed.
        env_patch = mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "0",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "0",
        })
        env_patch.start()
        self.addCleanup(env_patch.stop)
        bot.init_db()

    def rows(self, sql, parameters=()):
        # Every assertion opens the database again; no in-process source map
        # may be required to preserve a memory after restart.
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql, parameters).fetchall()

    def save(self, text, *, user=42, guild=1, directed=False):
        decision = bot.save_user_message(
            user, "Test Member", guild, text,
            channel_name="test-room", channel_policy="public_home",
            channel_id=10, route_mode="normal_chat", directed_to_bnl=directed,
        )
        self.assertTrue(decision.save_conversation)
        row = self.rows(
            "SELECT id FROM conversations WHERE guild_id=? AND user_id=? "
            "ORDER BY id DESC LIMIT 1", (guild, user),
        )
        return row[0][0] if row else None

    def source_ids(self, *, guild=1, user=42, tier=None):
        sql = (
            "SELECT DISTINCT s.conversation_row_id "
            "FROM memory_tier_conversation_sources s JOIN memory_tiers t "
            "ON t.guild_id=s.guild_id AND t.id=s.tier_row_id "
            "WHERE t.guild_id=? AND t.user_id=?"
        )
        params = [guild, user]
        if tier is not None:
            sql += " AND t.tier=?"
            params.append(tier)
        return {row[0] for row in self.rows(sql, params)}

    def advance(self, tier, *, guild=1, user=42):
        limits = {
            "short": 0,
            "medium": 10 if tier == "medium" else 0,
            "long": 10,
        }
        return bot._consolidate_memory_tiers(user, guild, limits=limits)

    def assert_source_present(self, source_id):
        self.assertEqual(self.rows(
            "SELECT id FROM conversations WHERE id=?", (source_id,),
        ), [(source_id,)])

    def test_real_memory_preserves_raw_source_through_all_three_tiers(self):
        source_id = self.save(
            "Remember this bean recipe uses smoked paprika and lemon zest."
        )
        recent_id = self.save("ok")
        self.assertEqual(bot.get_memory_tier_counts(42, 1), {
            "short": 1, "medium": 0, "long": 0,
        })

        for tier in ("short", "medium", "long"):
            with self.subTest(tier=tier):
                if tier != "short":
                    self.advance(tier)
                bot.prune_conversation_history(42, 1, max_rows=1)
                self.assertEqual(self.source_ids(tier=tier), {source_id})
                self.assertEqual(self.rows(
                    "SELECT id FROM conversations ORDER BY id"
                ), [(source_id,), (recent_id,)])
                memories = bot.get_memory_tiers(42, 1)
                self.assertEqual([row[0] for row in memories], [tier])
                self.assertIn("smoked paprika", memories[0][1])

        self.assertEqual(self.rows(
            "SELECT entry_id FROM memory_ledger_entries "
            "WHERE source_table IN ('conversations','memory_tiers')"
        ), [])

    def test_source_capture_precedes_prune_in_actual_message_write(self):
        limits = bot.calculate_adaptive_memory_limits(42, 1)
        limits["conversation_rows"] = 0
        with mock.patch.object(
            bot, "calculate_adaptive_memory_limits", return_value=limits,
        ):
            source_id = self.save(
                "Remember this bean recipe uses smoked paprika and lemon zest."
            )
        self.assertIsNotNone(source_id)
        self.assertEqual(self.source_ids(tier="short"), {source_id})
        self.assert_source_present(source_id)

    def test_source_attachment_failure_rolls_back_message_and_tier_together(self):
        with mock.patch.object(
            bot, "attach_memory_tier_conversation_sources",
            side_effect=sqlite3.OperationalError("test source attachment failed"),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                self.save("Remember this bean recipe uses smoked paprika.")
        self.assertEqual(self.rows("SELECT id FROM conversations"), [])
        self.assertEqual(self.rows("SELECT id FROM memory_tiers"), [])
        self.assertEqual(self.rows(
            "SELECT tier_row_id FROM memory_tier_conversation_sources"
        ), [])

    def test_prune_cannot_see_message_before_its_tier_source_capture_commits(self):
        attach_sources = ledger.attach_memory_tier_conversation_sources

        def inspect_uncommitted_capture(conn, **kwargs):
            attach_sources(conn, **kwargs)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM conversations"
            ).fetchone(), (1,))
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM memory_tiers"
            ).fetchone(), (1,))
            self.assertEqual(self.rows("SELECT id FROM conversations"), [])
            self.assertEqual(self.rows("SELECT id FROM memory_tiers"), [])
            bot.prune_conversation_history(42, 1, max_rows=0)

        with mock.patch.object(
            bot, "attach_memory_tier_conversation_sources",
            side_effect=inspect_uncommitted_capture,
        ) as capture:
            source_id = self.save("Remember this bean recipe uses smoked paprika.")
        capture.assert_called_once()
        self.assert_source_present(source_id)
        self.assertEqual(self.source_ids(), {source_id})

    def test_merging_into_existing_mid_and_long_target_preserves_both_roots(self):
        for guild, tier in ((1, "medium"), (2, "long")):
            with self.subTest(tier=tier):
                first = self.save(
                    "Remember this bean recipe uses smoked paprika for seasoning.",
                    guild=guild,
                )
                self.advance(tier, guild=guild)
                target_id = self.rows(
                    "SELECT id FROM memory_tiers WHERE guild_id=? AND tier=?",
                    (guild, tier),
                )[0][0]
                second = self.save(
                    "Remember this bean recipe uses lemon zest for brightness.",
                    guild=guild,
                )
                self.advance(tier, guild=guild)
                self.assertEqual(self.rows(
                    "SELECT id FROM memory_tiers WHERE guild_id=? AND tier=?",
                    (guild, tier),
                ), [(target_id,)])
                self.assertEqual(self.source_ids(guild=guild, tier=tier), {
                    first, second,
                })
                bot.prune_conversation_history(42, guild, max_rows=0)
                self.assert_source_present(first)
                self.assert_source_present(second)
                summary = bot.get_memory_tiers(42, guild)[0][1]
                self.assertIn("smoked paprika", summary)
                self.assertIn("lemon zest", summary)

    def test_retiring_one_tier_releases_only_its_own_source_protection(self):
        source_id = self.save(
            "Remember this bean recipe uses smoked paprika and lemon zest."
        )
        self.advance("long")
        # Another retained memory can depend on the same original record.
        with sqlite3.connect(self.db_path) as conn:
            bot._insert_memory_tier(
                conn.cursor(), 42, 1, "short",
                "Remember this bean recipe uses smoked paprika and lemon zest.",
                0.7, source_role="user", source_channel_policy="public_home",
                source_origin="conversations", source_trust="source_safe_public",
                topic_key="memory", source_conversation_row_ids=(source_id,),
            )
        bot._consolidate_memory_tiers(42, 1, limits={
            "short": 10, "medium": 10, "long": 0,
        })
        self.assertEqual(self.source_ids(tier="long"), set())
        self.assertEqual(self.source_ids(tier="short"), {source_id})
        bot.prune_conversation_history(42, 1, max_rows=0)
        self.assert_source_present(source_id)

        bot._consolidate_memory_tiers(42, 1, limits={
            "short": 0, "medium": 0, "long": 0,
        })
        self.assertEqual(self.rows("SELECT id FROM memory_tiers"), [])
        self.assertEqual(self.rows(
            "SELECT tier_row_id FROM memory_tier_conversation_sources"
        ), [])
        bot.prune_conversation_history(42, 1, max_rows=0)
        self.assertEqual(self.rows("SELECT id FROM conversations"), [])

    def test_legacy_unlinked_memory_holds_cleanup_without_guessing_source(self):
        text = "The bean recipe uses smoked paprika and lemon zest."
        with sqlite3.connect(self.db_path) as conn:
            source_id = conn.execute(
                "INSERT INTO conversations (user_id,user_name,guild_id,role,"
                "content,channel_id,channel_policy,route_mode) "
                "VALUES (42,'Test Member',1,'user',?,10,'public_home','normal_chat')",
                (text,),
            ).lastrowid
            legacy_id = bot._insert_memory_tier(
                conn.cursor(), 42, 1, "long", text, 0.8,
                source_role="user", source_channel_policy="public_home",
                source_origin="conversations", source_trust="source_safe_public",
            )
        # Reinitialization may install indexes, but must not fabricate lineage
        # for records that predate exact source IDs.
        bot.init_db()
        self.assertEqual(self.source_ids(), set())
        other_member = self.save("ok", user=43)
        other_guild = self.save("ok", guild=2)
        bot.prune_conversation_history(42, 1, max_rows=0)
        bot.prune_conversation_history(43, 1, max_rows=0)
        bot.prune_conversation_history(42, 2, max_rows=0)
        self.assert_source_present(source_id)
        self.assertEqual(self.rows(
            "SELECT id FROM conversations WHERE id IN (?,?)",
            (other_member, other_guild),
        ), [])
        self.assertEqual(self.rows(
            "SELECT id,summary FROM memory_tiers WHERE id=?", (legacy_id,),
        ), [(legacy_id, text)])
        self.assertEqual(self.rows(
            "SELECT source_lineage_complete FROM memory_tiers WHERE id=?",
            (legacy_id,),
        ), [(0,)])
        bot._consolidate_memory_tiers(42, 1, limits={
            "short": 10, "medium": 10, "long": 0,
        })
        bot.prune_conversation_history(42, 1, max_rows=0)
        self.assertEqual(self.rows("SELECT id FROM conversations"), [])

    def test_merging_legacy_and_linked_memory_does_not_claim_complete_lineage(self):
        legacy_text = "Remember this bean recipe uses smoked paprika."
        with sqlite3.connect(self.db_path) as conn:
            legacy_source_id = conn.execute(
                "INSERT INTO conversations (user_id,user_name,guild_id,role,"
                "content,channel_id,channel_policy,route_mode) "
                "VALUES (42,'Test Member',1,'user',?,10,'public_home','normal_chat')",
                (legacy_text,),
            ).lastrowid
            legacy_tier_id = bot._insert_memory_tier(
                conn.cursor(), 42, 1, "medium", legacy_text, 0.8,
                source_role="user", source_channel_policy="public_home",
                source_origin="conversations", source_trust="source_safe_public",
                topic_key="memory",
            )
        new_source_id = self.save("Remember this bean recipe uses lemon zest.")
        self.advance("medium")
        self.assertEqual(self.rows(
            "SELECT id,source_lineage_complete FROM memory_tiers"
        ), [(legacy_tier_id, 0)])
        self.assertEqual(self.source_ids(), {new_source_id})
        bot.prune_conversation_history(42, 1, max_rows=0)
        self.assert_source_present(legacy_source_id)
        self.assert_source_present(new_source_id)
        self.advance("long")
        self.assertEqual(self.rows(
            "SELECT source_lineage_complete FROM memory_tiers"
        ), [(0,)])
        self.assertEqual(self.source_ids(tier="long"), {new_source_id})
        bot.prune_conversation_history(42, 1, max_rows=0)
        self.assert_source_present(legacy_source_id)

    def test_real_capture_keeps_raw_ledger_lineage_after_each_tier_promotion(self):
        with mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
        }):
            source_id = self.save(
                "Remember this bean recipe uses smoked paprika and lemon zest."
            )
            raw_entry = self.rows(
                "SELECT entry_id FROM memory_ledger_entries "
                "WHERE source_table='conversations' AND source_row_id=? "
                "AND predicate_key='conversation'", (str(source_id),),
            )[0][0]
            for tier in ("short", "medium", "long"):
                with self.subTest(tier=tier):
                    if tier != "short":
                        self.advance(tier)
                    projections = self.rows(
                        "SELECT e.entry_id,e.visibility,e.public_usable,"
                        "e.lifecycle_status FROM memory_ledger_entries e "
                        "JOIN memory_tiers t ON t.guild_id=e.guild_id "
                        "AND CAST(t.id AS TEXT)=e.source_row_id "
                        "WHERE e.source_table='memory_tiers' AND t.tier=?",
                        (tier,),
                    )
                    self.assertTrue(projections)
                    for entry_id, visibility, public_usable, lifecycle in projections:
                        self.assertEqual(
                            (visibility, public_usable, lifecycle),
                            ("private", 0, "review_only"),
                        )
                        self.assertIn((raw_entry,), self.rows(
                            "SELECT target_entry_id FROM memory_ledger_lineage "
                            "WHERE entry_id=? AND lineage_type='derived_from'",
                            (entry_id,),
                        ))
                    bot.prune_conversation_history(42, 1, max_rows=0)
                    self.assert_source_present(source_id)

    def test_retired_tiers_leave_independent_moment_source_protection_intact(self):
        with mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "1",
        }):
            sources = {
                self.save("The bean recipe needs smoky seasoning for the shared meal."),
                self.save(
                    "The bean recipe works with smoked paprika for that meal.",
                    user=43,
                ),
                self.save("The bean recipe should keep the smoky seasoning balanced."),
            }
            with sqlite3.connect(self.db_path) as conn:
                moments.sweep_expired_windows(
                    conn, guild_id=1,
                    now=(datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat(),
                )
                moment_ids = conn.execute(
                    "SELECT moment_id FROM memory_moment_windows "
                    "WHERE guild_id=1 AND lifecycle_status='finalized'"
                ).fetchall()
            self.assertEqual(len(moment_ids), 1)

        # All tier summaries retire locally; the separate retained experience
        # still owns all three original messages with its original people.
        for user in (42, 43):
            bot._consolidate_memory_tiers(user, 1, limits={
                "short": 0, "medium": 0, "long": 0,
            })
        self.assertEqual(self.rows("SELECT id FROM memory_tiers"), [])
        self.assertEqual(self.rows(
            "SELECT tier_row_id FROM memory_tier_conversation_sources"
        ), [])
        for user in (42, 43):
            bot.prune_conversation_history(user, 1, max_rows=0)
        self.assertEqual({row[0] for row in self.rows(
            "SELECT id FROM conversations"
        )}, sources)
        with sqlite3.connect(self.db_path) as conn:
            recalled = moments.select_public_situation_moment_gists(
                conn, guild_id=1, topic_text="bean recipe smoky seasoning",
                allowed_channel_policies=("public_home", "public_context"),
            )
        self.assertEqual([item.moment_id for item in recalled], [moment_ids[0][0]])

    def test_explicit_user_and_guild_clear_remove_mapped_memory_with_shadow_off(self):
        first = self.save("Remember this bean recipe uses smoked paprika.")
        other_user = self.save(
            "Remember this bean recipe uses lemon zest.", user=43,
        )
        other_guild = self.save(
            "Remember this bean recipe uses fresh herbs.", guild=2,
        )
        self.advance("long")
        self.advance("long", user=43)
        self.advance("long", guild=2)
        bot.prune_conversation_history(42, 1, max_rows=0)
        self.assert_source_present(first)

        self.assertEqual(bot.clear_user_history(42, 1), 1)
        self.assertEqual(bot.get_memory_tiers(42, 1), [])
        self.assertEqual(self.source_ids(), set())
        self.assert_source_present(other_user)
        self.assert_source_present(other_guild)

        self.assertEqual(bot.clear_guild_history(1), 1)
        self.assertEqual(bot.get_memory_tiers(43, 1), [])
        self.assertEqual(self.rows(
            "SELECT tier_row_id FROM memory_tier_conversation_sources WHERE guild_id=1"
        ), [])
        self.assertEqual(self.source_ids(guild=2), {other_guild})
        self.assert_source_present(other_guild)

    def test_complete_member_delete_removes_tiers_and_maps_without_touching_other_member(self):
        self.save("Remember this bean recipe uses smoked paprika.")
        other_user = self.save(
            "Remember this bean recipe uses lemon zest.", user=43,
        )
        self.advance("long")
        result = bot._complete_delete_member_data_sync(
            1, 42, "DELETE MY BNL DATA 1",
        )
        self.assertTrue(result["ok"])
        self.assertEqual(bot.get_memory_tiers(42, 1), [])
        self.assertEqual(self.source_ids(), set())
        self.assertEqual(self.rows(
            "SELECT id FROM conversations WHERE guild_id=1 AND user_id=42"
        ), [])
        self.assertEqual(self.source_ids(user=43), {other_user})
        self.assert_source_present(other_user)

    def test_member_correction_and_forget_invalidate_retained_source_summaries(self):
        for user, operation in ((42, "correct"), (43, "forget")):
            with self.subTest(operation=operation):
                with mock.patch.dict(os.environ, {
                    "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
                }):
                    source_id = self.save(
                        "My favorite movie is Hackers.", user=user, directed=True,
                    )
                self.advance("medium", user=user)
                self.assertEqual(self.source_ids(user=user), {source_id})
                with sqlite3.connect(self.db_path) as conn:
                    facts = view_member_memory(conn, guild_id=1, user_id=user)
                    fact_ref = next(
                        item["ref"] for item in facts
                        if item["kind"] == "member-authored fact"
                        and item["summary"].lower() == "hackers"
                    )
                    if operation == "correct":
                        result = correct_member_memory(
                            conn, guild_id=1, user_id=user, safe_ref=fact_ref,
                            corrected_text="Sneakers",
                        )
                    else:
                        result = forget_member_memory(
                            conn, guild_id=1, user_id=user, safe_ref=fact_ref,
                        )
                self.assertTrue(result["ok"])
                self.assertEqual(bot.get_memory_tiers(user, 1), [])
                self.assertEqual(self.source_ids(user=user), set())
                context = bot.build_user_memory_context(
                    user, 1, channel_policy="public_home", user_text="favorite movie",
                )
                self.assertNotIn("Hackers", context)
                evidence = bot.get_approved_member_fact_evidence(user, 1)
                if operation == "correct":
                    self.assertTrue(any(item.value == "Sneakers" for item in evidence))
                else:
                    self.assertFalse(any(item.value == "Hackers" for item in evidence))

    def test_concurrent_new_tier_defers_prune_and_preserves_its_source(self):
        text = "Remember this bean recipe uses smoked paprika and lemon zest."
        source_id = self.save(text)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("DELETE FROM memory_tiers")
        read_retained_sources = ledger.retained_tier_conversation_sources

        def retain_after_prune_snapshot(conn, **kwargs):
            retained = read_retained_sources(conn, **kwargs)
            self.assertEqual(retained, set())
            with sqlite3.connect(self.db_path) as writer:
                bot._insert_memory_tier(
                    writer.cursor(), 42, 1, "short", text, 0.7,
                    source_role="user", source_channel_policy="public_home",
                    source_origin="conversations", source_trust="source_safe_public",
                    source_conversation_row_ids=(source_id,),
                )
            return retained

        with mock.patch.object(
            bot, "retained_tier_conversation_sources",
            side_effect=retain_after_prune_snapshot,
        ) as retention_read:
            with self.assertLogs(level="WARNING") as captured:
                bot.prune_conversation_history(42, 1, max_rows=0)
        retention_read.assert_called_once()
        self.assertTrue(any(
            "conversation_prune_deferred_for_memory_lifecycle" in line
            for line in captured.output
        ))
        self.assert_source_present(source_id)
        self.assertEqual(self.source_ids(tier="short"), {source_id})


if __name__ == "__main__":
    unittest.main()
