"""Trusted Discord reply references reach the existing Moment writer."""

import os
import sqlite3
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_moment_engine as moments
import test_conversation_batching as batch_fixtures


REQUEST = (
    "BNL, you attributed an observatory comment to Test Member in the recap. "
    "Can you verify that exact quote and speaker against an original record?"
)
CORRECTION = (
    "That processing explanation isn’t established by the evidence. "
    "What can you actually verify about your earlier attribution, "
    "and what remains unknown?"
)


class MomentReplyWriterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db_path = os.path.join(temporary.name, "moments.sqlite3")
        patches = (
            mock.patch.object(bot, "DB_FILE", self.db_path),
            mock.patch.dict(os.environ, {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "1",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "1",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "0",
            }),
        )
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        bot.init_db()

    def rows(self, sql, parameters=()):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql, parameters).fetchall()

    def save_user(self, message_id, text, **kwargs):
        return bot.save_user_message(
            101, "Test Member", 7700, text,
            channel_id=8300, channel_name="bnl-testing",
            channel_policy="sealed_test", message_id=message_id,
            directed_to_bnl=True, **kwargs,
        )

    def test_retained_reply_binding_keeps_correction_in_one_qualifying_moment(self):
        self.save_user(10001, REQUEST)
        bot.save_model_message(
            101, 7700, "I cannot verify the attributed comment.",
            channel_id=8300, channel_name="bnl-testing",
            channel_policy="sealed_test", discord_message_ids=(10002,),
        )
        target_row = self.rows(
            "SELECT id FROM conversations WHERE message_id=10002"
        )[0][0]

        self.save_user(
            10003, CORRECTION,
            reply_to_conversation_row_id=target_row,
        )

        self.assertEqual(self.rows(
            """SELECT current.source_message_id, target.source_message_id
               FROM memory_ledger_lineage edge
               JOIN memory_ledger_entries current
                 ON current.entry_id=edge.entry_id
               JOIN memory_ledger_entries target
                 ON target.entry_id=edge.target_entry_id
               WHERE edge.lineage_type='reply_to'"""
        ), [(10003, 10002)])
        self.assertEqual(self.rows(
            """SELECT COUNT(DISTINCT member.moment_id)
               FROM memory_moment_members member
               JOIN memory_ledger_entries source
                 ON source.entry_id=member.ledger_entry_id
               WHERE source.source_message_id IN (10001,10002,10003)"""
        ), [(1,)])
        with sqlite3.connect(self.db_path) as conn:
            moments.sweep_expired_windows(
                conn,
                now=(datetime.now(timezone.utc) + timedelta(minutes=3)).isoformat(),
            )
        self.assertEqual(self.rows(
            """SELECT lifecycle_status, human_entry_count, model_entry_count,
                      qualification_reason
               FROM memory_moment_windows"""
        ), [("finalized", 2, 1, "one_human_bnl_continuity")])
        self.assertEqual(self.rows(
            "SELECT COUNT(*) FROM memory_moment_episode_moments"
        ), [(1,)])
        self.assertEqual(self.rows(
            """SELECT COUNT(*) FROM memory_ledger_lineage
               WHERE lineage_type IN ('correction_of','supersedes','retracts')"""
        ), [(0,)])

    def test_unretained_target_does_not_create_reply_edge(self):
        self.save_user(
            10004, CORRECTION,
            reply_to_conversation_row_id=987654,
        )
        self.assertEqual(self.rows(
            "SELECT COUNT(*) FROM memory_ledger_lineage WHERE lineage_type='reply_to'"
        ), [(0,)])
        self.assertEqual(self.rows(
            "SELECT content FROM conversations WHERE message_id=10004"
        ), [(CORRECTION,)])


class _SavedIngress(Exception):
    """Stop after the real ingress reaches the persistence boundary."""


class MomentReplyIngressTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = batch_fixtures.ConversationBatchCoordinatorTests.asyncSetUp
    asyncTearDown = batch_fixtures.ConversationBatchCoordinatorTests.asyncTearDown
    _channel = batch_fixtures.ConversationBatchCoordinatorTests._channel
    _on_message_runtime = batch_fixtures.ConversationBatchCoordinatorTests._on_message_runtime

    async def test_each_direct_ingress_passes_only_retained_bnl_reply_reference(self):
        for active_mode in ("active", "other", "unset"):
            for target_kind in ("retained_bnl", "other_human", "transient_bnl"):
                with self.subTest(active_mode=active_mode, target_kind=target_kind):
                    channel = self._channel(8350 + len(self.channel_ids))
                    fake_bot = SimpleNamespace(id=999, display_name="BNL-01")
                    target_author = (
                        batch_fixtures.FakeAuthor(102, "Another Member")
                        if target_kind == "other_human" else fake_bot
                    )
                    message = batch_fixtures.FakeMessage(
                        channel, "BNL, can you clarify that point?",
                        mentions=[fake_bot],
                    )
                    message.reference = SimpleNamespace(
                        message_id=12001,
                        resolved=SimpleNamespace(
                            id=12001, author=target_author, channel=channel,
                            content="This visible reply mentions source row 987654.",
                        ),
                    )
                    retained_row = 0 if target_kind == "transient_bnl" else 765
                    stored_role = "user" if target_kind == "other_human" else "model"
                    active_channel_id = {
                        "active": channel.id,
                        "other": channel.id + 100,
                        "unset": None,
                    }[active_mode]
                    with self._on_message_runtime(channel.id, followup_candidate=False):
                        with ExitStack() as stack:
                            for patcher in (
                                mock.patch.object(bot, "get_guild_config", return_value=active_channel_id),
                                mock.patch.object(bot, "resolve_channel_policy", return_value=(
                                    "sealed_test" if active_mode == "active" else "public_context"
                                )),
                                mock.patch.object(bot, "is_direct_bnl_target", return_value=True),
                                mock.patch.object(bot, "_conversation_row_for_discord_message", return_value=(
                                    retained_row, stored_role if retained_row else "", "Test Member"
                                )),
                                mock.patch.object(bot, "get_sealed_test_recall_guard_response", return_value=None),
                                mock.patch.object(bot, "get_restricted_channel_recall_guard_response", return_value=None),
                                mock.patch.object(bot, "build_current_turn_addressing_context", return_value=(
                                    "Rendered reply context claims row 987654."
                                )),
                            ):
                                stack.enter_context(patcher)
                            saved = stack.enter_context(mock.patch.object(
                                bot, "save_user_message", side_effect=_SavedIngress,
                            ))
                            with self.assertRaises(_SavedIngress):
                                await bot.on_message(message)
                    saved.assert_called_once()
                    self.assertEqual(
                        saved.call_args.kwargs.get("reply_to_conversation_row_id", 0),
                        765 if target_kind == "retained_bnl" else 0,
                    )


if __name__ == "__main__":
    unittest.main()
