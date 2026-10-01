"""Real SQLite contention around an already-rendered Ambient artwork receipt."""

from contextlib import ExitStack, closing
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import bnl_ambient_art as art


class AmbientArtPersistenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        folder = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.path = str(Path(folder) / "art.db")
        self.stack.enter_context(mock.patch.object(bot, "DB_FILE", self.path))
        self.stack.enter_context(mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", 42))
        self.stack.enter_context(mock.patch.object(bot, "_pacific_now", return_value=
            bot.PACIFIC_TZ.localize(datetime(2026, 9, 30, 19))))
        self.stack.enter_context(mock.patch.dict(os.environ, {"BNL_OWN_ART_ENABLED": "true"}))
        self.connect = sqlite3.connect
        self.connections = []
        self.stack.callback(self._close_connections)
        self.art_id = art.claim(bot, 42)

    def _close_connections(self):
        for conn in self.connections:
            conn.close()

    def _short_connection(self, *args, **kwargs):
        kwargs["timeout"] = 0.02
        kwargs["check_same_thread"] = False
        conn = self.connect(*args, **kwargs)
        self.connections.append(conn)
        return conn

    def _reader(self):
        conn = self.connect(self.path, check_same_thread=False)
        self.stack.callback(conn.close)
        conn.execute("BEGIN")
        conn.execute("SELECT * FROM bnl_own_art_delivery").fetchall()
        return conn

    def _row(self):
        with closing(self.connect(self.path)) as conn:
            rows = conn.execute("SELECT status,metadata_json,discord_message_id,website_status "
                                "FROM bnl_own_art_delivery").fetchall()
        self.assertEqual(len(rows), 1)
        return rows[0]

    def _assert_attempts_closed(self):
        self.assertTrue(self.connections)
        for conn in self.connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_reader_commit_contention_retries_complete_receipt_atomically(self):
        reader = self._reader()
        metadata = {"privateCreativeContinuity": {"source": "private fixture"}}
        with mock.patch.object(art.sqlite3, "connect", self._short_connection), \
             mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: reader.rollback()) as backoff:
            art.record(bot, self.art_id, "draft_ready", metadata=metadata,
                       message_id=9001, website_status="unconfirmed")
        backoff.assert_called_once()
        self.assertEqual(self._row(), ("draft_ready", json.dumps(metadata), "9001", "unconfirmed"))
        self._assert_attempts_closed()
        self.assertIsNone(art.claim(bot, 42))

    def test_exclusive_writer_contention_retries_without_replacing_other_fields(self):
        art.record(bot, self.art_id, "draft_ready", metadata={"fixture": 1})
        with closing(self.connect(self.path)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            with mock.patch.object(art.sqlite3, "connect", self._short_connection), \
                 mock.patch.object(bot.time, "sleep", side_effect=lambda _delay: writer.rollback()) as backoff:
                art.record(bot, self.art_id, "discord_confirmed", message_id=9001)
        backoff.assert_called_once()
        self.assertEqual(self._row(), ("discord_confirmed", '{"fixture": 1}', "9001", ""))
        self._assert_attempts_closed()

    def test_exhausted_contention_rolls_back_every_field_and_releases_connections(self):
        reader = self._reader()
        with mock.patch.object(art.sqlite3, "connect", self._short_connection), \
             mock.patch.object(bot.time, "sleep") as backoff:
            with self.assertRaises(sqlite3.OperationalError) as retained:
                art.record(bot, self.art_id, "draft_ready", metadata={"fixture": "private"}, message_id=9001)
        self.assertTrue(bot._sqlite_busy(retained.exception))
        self.assertEqual(backoff.call_count, 2)
        self.assertEqual(len(self.connections), 3)
        self._assert_attempts_closed()
        self.assertEqual(self._row(), ("claimed", "{}", "", ""))
        reader.rollback()
        art.record(bot, self.art_id, "draft_ready", metadata={"fixture": "private"})
        self.assertEqual(self._row()[0], "draft_ready")

    def test_schema_error_is_not_retried_or_misreported_as_transient_contention(self):
        with closing(self.connect(self.path)) as conn, conn:
            conn.execute("DROP TABLE bnl_own_art_delivery")
            conn.execute("CREATE TABLE bnl_own_art_delivery (art_id TEXT PRIMARY KEY,status TEXT)")
            conn.execute("INSERT INTO bnl_own_art_delivery VALUES (?, 'claimed')", (self.art_id,))
        with mock.patch.object(art.sqlite3, "connect", self._short_connection), \
             mock.patch.object(bot.time, "sleep") as backoff:
            with self.assertRaises(sqlite3.OperationalError) as retained:
                art.record(bot, self.art_id, "draft_ready", metadata={"fixture": 1})
        self.assertFalse(bot._sqlite_busy(retained.exception))
        backoff.assert_not_called()
        self._assert_attempts_closed()

    def test_lazy_schema_failure_closes_connection_even_while_traceback_is_retained(self):
        with closing(self.connect(self.path)) as writer:
            writer.execute("BEGIN EXCLUSIVE")
            with mock.patch.object(art.sqlite3, "connect", self._short_connection):
                with self.assertRaises(sqlite3.OperationalError) as retained:
                    art._db(bot)
            self.assertTrue(bot._sqlite_busy(retained.exception))
            self._assert_attempts_closed()
            writer.rollback()
        self.assertEqual(self._row()[0], "claimed")

    async def _prepare_under_contention(self, *, release_lock):
        # A new day lets the real prepare() consume its own daily claim.
        self.stack.enter_context(mock.patch.object(bot, "_pacific_now", return_value=
            bot.PACIFIC_TZ.localize(datetime(2026, 10, 1, 19))))
        concept = {"action": "create", "title": "Test Signal", "meaning": "An imagined musical room.",
                   "imagePrompt": "An invented room made of rhythm.", "inspirationRefs": []}
        private = {"fixture": "PRIVATE CONTINUITY MUST STAY LOCAL"}
        image = b"isolated provider image fixture"
        blockers = []

        def render(*_args):
            blockers.append(self._reader())
            return image, {"sha256": hashlib.sha256(image).hexdigest(), "mimeType": "image/png"}

        with mock.patch.object(art, "develop_art_concept", return_value=concept) as develop, \
             mock.patch.object(art, "saved_creative_continuity", return_value=private), \
             mock.patch.object(art, "generate_private_image", side_effect=render) as provider, \
             mock.patch.object(bot, "revalidate_ambient_sources", new=mock.AsyncMock(return_value=True)) as validate, \
             mock.patch.object(art.sqlite3, "connect", self._short_connection), \
             mock.patch.object(bot.time, "sleep", side_effect=(
                 (lambda _delay: blockers[0].rollback()) if release_lock else None)) as backoff:
            with self.assertLogs(level="WARNING") as logs:
                result = await art.prepare(bot, 42, {"art": concept, "art_context": {"fixture": True}})
            blockers[0].rollback()
            self.assertIsNone(await art.prepare(bot, 42, {"art": concept}))
        provider.assert_called_once()
        develop.assert_called_once()
        self.assertEqual([c.kwargs["stage"] for c in validate.await_args_list], ["before_image", "after_image"])
        self._assert_attempts_closed()
        saved = json.loads((Path(self.path).parent / "bnl-own-art" / "bnl-art-2026-10-01" / "receipt.json").read_text())
        self.assertEqual(saved["metadata"]["privateCreativeContinuity"], private)
        self.assertEqual((Path(self.path).parent / "bnl-own-art" / "bnl-art-2026-10-01" / "image.png").read_bytes(), image)
        self.assertFalse(art.available(bot, 42))
        return result, backoff, logs.output

    async def test_prepared_image_survives_transient_receipt_contention_with_one_render(self):
        result, backoff, logs = await self._prepare_under_contention(release_lock=True)
        self.assertIsNotNone(result)
        self.assertNotIn("privateCreativeContinuity", result["metadata"])
        backoff.assert_called_once()
        self.assertFalse(any("ambient_art_unavailable" in line for line in logs))
        with closing(self.connect(self.path)) as conn:
            self.assertEqual(conn.execute("SELECT status FROM bnl_own_art_delivery WHERE art_id='bnl-art-2026-10-01'").fetchone()[0], "draft_ready")

    async def test_exhausted_receipt_contention_keeps_private_file_evidence_and_consumed_claim(self):
        result, backoff, logs = await self._prepare_under_contention(release_lock=False)
        self.assertIsNone(result)
        self.assertEqual(backoff.call_count, 4)  # Two bounded local receipt transactions.
        self.assertTrue(any("stage=draft_receipt" in line and "category=sqlite_busy" in line for line in logs))
        self.assertFalse(any("PRIVATE CONTINUITY" in line for line in logs))
        with closing(self.connect(self.path)) as conn:
            self.assertEqual(conn.execute("SELECT status,metadata_json FROM bnl_own_art_delivery WHERE art_id='bnl-art-2026-10-01'").fetchone(), ("claimed", "{}"))
