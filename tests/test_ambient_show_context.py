from contextlib import ExitStack
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
from bnl_tiktok_live_chat import LiveChatAdapter, LiveChatBuffer
from bnl_tiktok_live_context import LiveContextSnapshotWriter


class Response:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


class AmbientShowContextTests(unittest.IsolatedAsyncioTestCase):
    """Use real public readers and Ambient assembly with isolated source data."""

    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        directory = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.snapshot_path = str(Path(directory) / "live-context.json")
        self.now = bot.PACIFIC_TZ.localize(datetime(2026, 9, 11, 18, 45))
        self.stack.enter_context(mock.patch.dict(os.environ, {"BNL_QUEUE_PRODUCTION_ENABLED": "true"}))
        for name, value in {
            "DB_FILE": str(Path(directory) / "conversations.db"),
            "BNL_PRIMARY_GUILD_ID": 42,
            "BNL_READ_MODEL_ENABLED": True,
            "BNL_READ_MODEL_URL": "https://example.test/api/bnl/read-model",
            "BNL_API_KEY": "test-service-key",
            "BNL_READ_MODEL_TTL_SECONDS": 20,
            "_bnl_read_model_cache": None,
            "_bnl_read_model_cached_at": None,
            "_bnl_read_model_cache_scope": None,
            "_bnl_read_model_request_serial": 0,
            "_bnl_read_model_applied_serial": 0,
            "BNL_TIKTOK_LIVE_CONTEXT_ENABLED": True,
            "BNL_TIKTOK_LIVE_CONTEXT_PATH": self.snapshot_path,
            "BNL_TIKTOK_LIVE_CONTEXT_MAX_AGE_SECONDS": 20,
            "_ambient_runtime_state": {},
        }.items():
            self.stack.enter_context(mock.patch.object(bot, name, value))
        bot.init_db()
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute(
                "INSERT INTO conversations(user_id,user_name,guild_id,channel_id,channel_name,channel_policy,role,content) "
                "VALUES(7,'Test Member',42,100,'barcode-bot','public_home','user','A new rhythm is forming in the room.')"
            )
        clock = mock.Mock(wraps=datetime)
        clock.now.side_effect = lambda _tz: self.now
        self.stack.enter_context(mock.patch.object(bot, "datetime", clock))
        self.stack.enter_context(mock.patch("bnl_tiktok_live_context.time.time", side_effect=lambda: self.now.timestamp()))
        self.http = self.stack.enter_context(mock.patch.object(bot.urllib.request, "urlopen"))
        self.http.return_value = Response(self.read_model())
        self.provider = self.stack.enter_context(mock.patch.object(
            bot, "get_gemini_response", new=mock.AsyncMock(
                return_value="The room carries an unexpected rhythm through the evening.",
            ),
        ))

    def read_model(self, *, private=False, status="open", phase="pre_show"):
        return {
            "ok": True,
            "version": 1,
            "publicOnly": not private,
            "accessScope": "private" if private else "public",
            "capabilities": {"queueProduction": True},
            "sections": {"queue": {
                "session": {"title": "PRIVATE OR UNUSED TITLE", "showDate": "2026-09-11", "status": status, "queueOpen": True, "broadcastPhase": phase},
                "nowPlaying": {"artistName": "Test Artist", "title": "Loaded Does Not Mean Playing"},
                "payment": "PRIVATE PAYMENT VALUE",
                "operator": "PRIVATE OPERATOR VALUE",
            }},
        }

    def write_snapshot(self, event_type="connected", *, age=0):
        at = self.now.timestamp() - age
        clock = lambda: at
        adapter = LiveChatAdapter(LiveChatBuffer(100, 600, clock), clock)
        for lifecycle in ("connected", event_type):
            adapter.ingest_line(json.dumps({
                "schema_version": 1,
                "event_type": lifecycle,
                "event_id": lifecycle + "-1",
                "room_id": "PRIVATE ROOM IDENTIFIER",
                "observed_at": at,
            }))
        adapter.ingest_line(json.dumps({
            "schema_version": 1,
            "event_type": "comment",
            "event_id": "comment-1",
            "room_id": "PRIVATE ROOM IDENTIFIER",
            "observed_at": at,
            "source_at": at,
            "unique_id": "test.viewer",
            "display_name": "Test Viewer",
            "comment_text": "UNSELECTED COMMENT TEXT",
        }))
        LiveContextSnapshotWriter(self.snapshot_path, time_fn=clock).publish(adapter, force=True)

    async def capture(self, guild_id=42):
        self.provider.reset_mock()
        result = await bot.generate_dynamic_ambient(guild_id, 100)
        self.assertEqual(result, "The room carries an unexpected rhythm through the evening.")
        self.provider.assert_awaited_once()
        self.assertEqual(self.provider.call_args.kwargs["route"], "ambient_generation")
        prompt = self.provider.call_args.args[0]
        self.assertNotIn("Current show phase:", prompt)
        self.assertNotIn("If show_phase is live_now", prompt)
        self.assertIn("weekday and clock describe the calendar only", prompt)
        self.assertNotIn("PRIVATE ROOM IDENTIFIER", prompt)
        self.assertNotIn("UNSELECTED COMMENT TEXT", prompt)
        self.assertNotIn(self.snapshot_path, prompt)
        return prompt

    async def test_friday_intake_window_does_not_become_observed_live_show(self):
        prompt = await self.capture()
        self.assertIn("Website production session: showDate=2026-09-11; status=open; queueOpen=True; phase=pre_show", prompt)
        self.assertIn("Current TikTok webcast observation: unavailable; live state is unknown", prompt)
        self.assertNotIn("state=connected", prompt)
        self.assertNotIn("Loaded Does Not Mean Playing", prompt)
        self.assertNotIn("PRIVATE OR UNUSED TITLE", prompt)
        self.assertNotIn("PRIVATE PAYMENT VALUE", prompt)
        self.assertNotIn("PRIVATE OPERATOR VALUE", prompt)
        self.assertIn("A new rhythm is forming in the room.", prompt)

    async def test_fresh_live_observation_survives_late_friday_and_after_midnight(self):
        for at in (datetime(2026, 9, 11, 22, 0), datetime(2026, 9, 12, 0, 30)):
            with self.subTest(at=at):
                self.now = bot.PACIFIC_TZ.localize(at)
                self.write_snapshot()
                prompt = await self.capture()
                self.assertIn("TikTok webcast observation: state=connected; observedAt=", prompt)
                self.assertNotIn("post_show", prompt)
                self.assertNotIn("off_cycle", prompt)

    async def test_expired_and_future_snapshots_are_not_current_observations(self):
        for age in (21, -11):
            with self.subTest(age=age):
                self.write_snapshot(age=age)
                prompt = await self.capture()
                self.assertIn("live state is unknown", prompt)
                self.assertNotIn("state=connected", prompt)

    async def test_disconnect_and_reconnect_remain_distinct_from_observed_end(self):
        for event_type, state in (("disconnected", "disconnected"), ("reconnecting", "reconnecting"), ("live_ended", "ended")):
            with self.subTest(event_type=event_type):
                self.write_snapshot(event_type)
                prompt = await self.capture()
                self.assertIn(f"TikTok webcast observation: state={state}; observedAt=", prompt)
                self.assertIn("Reconnecting, disconnected, stopped, error or unavailable observations do not establish either current live transmission or an ended broadcast", prompt)

    async def test_private_queue_does_not_hide_independent_public_webcast_state(self):
        self.http.return_value = Response(self.read_model(private=True, phase="private_rehearsal"))
        self.write_snapshot()
        prompt = await self.capture()
        self.assertIn("Public production session observations: unavailable", prompt)
        self.assertNotIn("private_rehearsal", prompt)
        self.assertIn("TikTok webcast observation: state=connected", prompt)
        self.assertNotIn("PRIVATE PAYMENT VALUE", prompt)
        self.assertNotIn("PRIVATE OPERATOR VALUE", prompt)

    async def test_website_unavailable_does_not_hide_fresh_public_webcast_state(self):
        self.write_snapshot()
        for failure in (TimeoutError("test timeout"), ValueError("test malformed transport")):
            with self.subTest(failure=type(failure).__name__):
                self.http.side_effect = failure
                prompt = await self.capture()
                self.assertIn("Public production session observations: unavailable", prompt)
                self.assertIn("TikTok webcast observation: state=connected", prompt)

    async def test_existing_tiktok_gate_does_not_suppress_ambient_generation(self):
        self.write_snapshot()
        with mock.patch.object(bot, "BNL_TIKTOK_LIVE_CONTEXT_ENABLED", False):
            prompt = await self.capture()
        self.assertIn("status=open", prompt)
        self.assertIn("live state is unknown", prompt)

    async def test_unbound_and_other_guild_do_not_receive_global_show_observations(self):
        self.write_snapshot()
        for primary, guild in ((42, 43), (0, 42)):
            with self.subTest(primary=primary, guild=guild):
                self.http.reset_mock()
                with mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", primary):
                    prompt = await self.capture(guild)
                self.assertIn("current broadcast state is unknown", prompt)
                self.assertNotIn("state=connected", prompt)
                self.http.assert_not_called()

    async def test_read_failure_uses_existing_cache_ttl_then_continues_without_show_claim(self):
        await self.capture()
        original_cached_at = bot._bnl_read_model_cached_at
        self.now += timedelta(seconds=10)
        self.http.side_effect = TimeoutError("test source unavailable")
        prompt = await self.capture()
        self.assertIn("status=open", prompt)
        self.assertEqual(bot._bnl_read_model_cached_at, original_cached_at)
        self.now += timedelta(seconds=10)
        prompt = await self.capture()
        self.assertIn("live state is unknown", prompt)
        self.assertNotIn("Website production session:", prompt)
        self.assertNotIn("test source unavailable", prompt)

    async def test_new_source_read_runs_off_the_discord_event_loop(self):
        caller_thread = threading.get_ident()
        read_threads = []

        def read(*_args, **_kwargs):
            read_threads.append(threading.get_ident())
            return Response(self.read_model())

        self.http.side_effect = read
        await self.capture()
        self.assertEqual(len(read_threads), 1)
        self.assertNotEqual(read_threads[0], caller_thread)


if __name__ == "__main__":
    unittest.main()
