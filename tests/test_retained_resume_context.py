"""Retained Moment references recover original context without changing memory."""
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import bnl_memory_ledger as ledger
import bnl_moment_engine as moments


QUERY = "Let's return to our playback correction discussion. What remains unresolved?"
SOURCES = (
    "Let's discuss playback correction: acknowledging an error and changing a saved record are different claims.",
    "The playback correction needs evidence of a saved record change, separate from acknowledgment of the error.",
    "Playback is still unconfirmed. Keep the saved-record change unresolved until there is evidence.",
)


class RetainedResumeContextTests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_CONVERSATION_CONTEXT_V2_ENABLED": "true",
            "GEMINI_API_KEY": "test-key", "DISCORD_BOT_TOKEN": "test-token",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        import bnl01_bot
        self.bot = bnl01_bot
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_file = str(Path(self.tmp.name) / "resume.db")
        db_patch = mock.patch.object(self.bot, "DB_FILE", self.db_file)
        db_patch.start()
        self.addCleanup(db_patch.stop)
        self.now = datetime.now(timezone.utc)
        self.conn = sqlite3.connect(self.db_file)
        self.addCleanup(self.conn.close)
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        self.conn.execute("""CREATE TABLE conversations (
            id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER,
            user_name TEXT, role TEXT, content TEXT, channel_id INTEGER,
            channel_name TEXT, channel_policy TEXT, route_mode TEXT, timestamp TEXT,
            message_id INTEGER)""")
        self.mid, self.roots = self.seed(100, self.now - timedelta(days=2))
        moments.sweep_expired_episodes(self.conn, now=(self.now - timedelta(days=1)).isoformat())
        self.conn.commit()

    def seed(self, start, stamp):
        roots = []
        for i, text in enumerate(SOURCES):
            row_id, uid = start + i, (7, 8, 7)[i]
            ts = (stamp + timedelta(seconds=i * 10)).isoformat()
            self.conn.execute("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (row_id, 1, uid, 'Test Member %s' % uid, 'user', text, 10,
                 'bnl-testing', 'sealed_test', 'normal_chat', ts, row_id + 10000))
            result = ledger.shadow_conversation_row(
                self.conn, row_id=row_id, user_id=uid, user_name='Test Member %s' % uid,
                guild_id=1, role='user', content=text, channel_id=10,
                channel_name='bnl-testing', channel_policy='sealed_test',
                route_mode='normal_chat', observed_at=ts,
            )
            roots.append(result.entry_id)
            moments.observe_ledger_entry(self.conn, result.entry_id)
        moments.sweep_expired_windows(self.conn, now=(stamp + timedelta(minutes=3)).isoformat())
        mid = self.conn.execute("SELECT moment_id FROM memory_moment_members WHERE ledger_entry_id=?",
                                (roots[0],)).fetchone()[0]
        return mid, roots

    def context(self, **changes):
        args = dict(guild_id=1, current_user_id=7, channel_id=10,
                    channel_name='bnl-testing', channel_policy='sealed_test',
                    route_mode='normal_chat', current_texts=(QUERY,),
                    current_participants={7}, now=self.now)
        args.update(changes)
        out = {}
        text = self.bot.build_conversation_context_v2_for_prompt(**args, result_out=out)
        return text, out['result']

    def test_expired_episode_recovers_original_rows_and_revalidates_without_reopening(self):
        before = self.conn.total_changes
        text, result = self.context()
        self.assertIn('Playback is still unconfirmed', text)
        self.assertIn('separate from acknowledgment', text)
        self.assertEqual(result.retained_moment_ids, (self.mid,))
        self.assertFalse(result.referent_reason == 'discord_reply_source')
        self.assertEqual(self.conn.total_changes, before)
        self.assertEqual(self.conn.execute('SELECT SUM(reopen_count) FROM memory_moment_episodes').fetchone()[0], 0)
        basis = self.bot.build_conversation_prompt_source_basis(
            text, guild_id=1, current_user_id=7, channel_id=10,
            channel_name='bnl-testing', channel_policy='sealed_test', context_result=result,
        )
        self.assertIsNotNone(basis)
        self.assertTrue(basis.source_row_ids)
        self.assertFalse(self.bot.refresh_prompt_source_basis(basis)[1])
        self.conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?", (self.roots[0],))
        self.conn.commit()
        self.assertTrue(self.bot.refresh_prompt_source_basis(basis)[1])
        self.assertEqual(self.context()[0], '')

    def test_scope_ambiguity_and_source_deletion_do_not_return_old_transcripts(self):
        for changes in (
            {'guild_id': 2}, {'channel_id': 11}, {'channel_policy': 'public_home'},
            {'current_user_id': 99}, {'current_texts': ('Tell me about studio lights.',)},
            {'current_texts': ('Return to our cooking discussion.',)},
            {'current_texts': (QUERY + ' From 2025-01-01.',)},
        ):
            with self.subTest(changes=changes):
                self.assertEqual(self.context(**changes)[0], '')
        self.conn.execute('DELETE FROM conversations WHERE id=100')
        self.conn.commit()
        self.assertEqual(self.context()[0], '')

    def test_distinct_retained_occurrences_remain_ambiguous(self):
        self.seed(200, self.now - timedelta(hours=3))
        self.conn.commit()
        text, result = self.context()
        self.assertEqual(text, '')
        self.assertEqual(result.retained_moment_ids, ())

    def test_explicit_date_selects_the_matching_retained_occurrence(self):
        self.seed(200, self.now - timedelta(minutes=15))
        self.conn.commit()
        earlier = self.now - timedelta(days=2)
        for day in (earlier.strftime('%Y-%m-%d'), earlier.strftime('%B %d')):
            with self.subTest(day=day):
                text, result = self.context(current_texts=(QUERY + ' From ' + day + '.',))
                self.assertIn('Playback is still unconfirmed', text)
                self.assertEqual(result.retained_moment_ids, (self.mid,))
                self.assertTrue(set(result.selected_row_ids).issubset({100, 101, 102}))
