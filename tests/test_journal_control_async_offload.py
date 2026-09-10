"""Journal control SQLite waits must leave the Discord event loop responsive."""

import asyncio
import os
import sqlite3
import tempfile
import threading
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

import bnl_journal as journal
import bnl_journal_automation as automation

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

try:
    import bnl01_bot as bot
except ModuleNotFoundError as exc:
    if exc.name != "discord":
        raise
    bot = None


@unittest.skipIf(bot is None, "discord.py is not installed in this local test image")
class JournalControlAsyncOffloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = os.path.join(self.temp.name, "journal.db")
        journal.ensure_schema(self.db_path)
        automation.store_journal_memory_exclusions(self.db_path, 1, {"journal-before"})
        bot._journal_automation_locks_by_guild.clear()
        bot._journal_automation_runtime_by_guild.clear()
        self.base_flags = {
            "journalAutoPublishEnabled": False,
            "journalDailyEnabled": True,
            "journalWeeklyEnabled": True,
        }
        self.live_control = {
            "config": {
                "journalAutoPublishEnabled": False,
                "journalDailyEnabled": False,
                "journalWeeklyEnabled": True,
            },
            "runRequests": [],
            "memoryExcludedEntryIds": ["journal-after"],
        }

    def run_contended(self, invoke, *, live=True):
        """An independent SQLite writer is released by a live-loop callback.

        A synchronous call on that loop prevents the callback from running,
        exhausting the short test-only SQLite timeout. A responsive caller
        lets it release the lock and finish the real exclusion read/write.
        """
        real_connect = sqlite3.connect
        real_resolver = bot._journal_control_flags_for_guild
        sql_wait_started = threading.Event()
        resolver_finished = threading.Event()
        resolved_flags = []
        loop_progress = []
        blocker = real_connect(self.db_path)
        blocker.execute("BEGIN IMMEDIATE")

        def connect(*args, **kwargs):
            kwargs["timeout"] = 0.15
            conn = real_connect(*args, **kwargs)

            def trace(statement):
                if statement.strip().upper() == "BEGIN IMMEDIATE":
                    sql_wait_started.set()

            conn.set_trace_callback(trace)
            return conn

        def resolve(*args, **kwargs):
            try:
                flags = real_resolver(*args, **kwargs)
                resolved_flags.append(flags)
                return flags
            finally:
                resolver_finished.set()

        async def scenario():
            pending = asyncio.create_task(invoke())

            async def heartbeat_and_unlock():
                while not sql_wait_started.is_set() and not pending.done():
                    await asyncio.sleep(0.001)
                loop_progress.append(
                    sql_wait_started.is_set() and not resolver_finished.is_set()
                )
                blocker.rollback()

            heartbeat = asyncio.create_task(heartbeat_and_unlock())
            try:
                result, _ = await asyncio.wait_for(
                    asyncio.gather(pending, heartbeat), timeout=2
                )
                return result
            finally:
                blocker.rollback()

        try:
            with ExitStack() as stack:
                patches = {
                    "DB_FILE": self.db_path,
                    "BNL_JOURNAL_AUTOMATION_ENABLED": True,
                    "BNL_API_KEY": "test-key",
                }
                for name, value in patches.items():
                    stack.enter_context(mock.patch.object(bot, name, value))
                for name, value in {
                    "resolve_network_guild_id": 1,
                    "get_bnl_control_flags": self.base_flags,
                    "_journal_control_request_sync": (
                        self.live_control if live else None,
                        "" if live else "control_plane_timeout",
                    ),
                    "_journal_heartbeat_sync": (None, ""),
                    "_journal_report_run_sync": (None, ""),
                    "journal_automation_status": {},
                    "_journal_website_base_url": "https://site.example",
                }.items():
                    stack.enter_context(mock.patch.object(bot, name, return_value=value))
                stack.enter_context(mock.patch.object(bot, "_journal_control_flags_for_guild", side_effect=resolve))
                stack.enter_context(mock.patch.object(automation.sqlite3, "connect", side_effect=connect))
                errors = stack.enter_context(mock.patch.object(bot.logging, "exception"))
                result = asyncio.run(scenario())
            self.assertEqual([True], loop_progress, "SQLite wait blocked the event-loop heartbeat")
            errors.assert_not_called()
            self.assertEqual(1, len(resolved_flags))
            self.assertTrue(resolved_flags[0]["journalMemoryExclusionsConfirmed"])
            self.assertFalse(resolved_flags[0]["journalAutoPublishEnabled"])
            expected = {"journal-after"} if live else {"journal-before"}
            self.assertEqual(expected, set(resolved_flags[0]["journalMemoryExcludedEntryIds"]))
            self.assertEqual(
                (expected, True),
                automation.load_journal_memory_exclusions(self.db_path, 1),
                "The exclusion snapshot must survive after the lock is released",
            )
            return result
        finally:
            blocker.close()

    def test_control_cycle_keeps_heartbeat_and_persists_live_exclusions(self):
        results = self.run_contended(
            lambda: bot.run_journal_automation_control_cycle(1, phase="release")
        )
        self.assertEqual("auto_publish_paused", results[0]["reason"])

    def test_control_outage_keeps_heartbeat_and_durable_pause(self):
        results = self.run_contended(
            lambda: bot.run_journal_automation_control_cycle(1, phase="release"),
            live=False,
        )
        self.assertEqual("auto_publish_paused", results[0]["reason"])

    def test_direct_automation_fallback_keeps_heartbeat_and_durable_exclusions(self):
        results = self.run_contended(
            lambda: bot.run_journal_automation_once(1), live=False
        )
        self.assertEqual("auto_publish_paused", results[0]["reason"])

    def test_manual_journal_paths_keep_heartbeat_and_forward_exclusions(self):
        for action in ("status", "run-daily", "create", "regenerate", "retry"):
            with self.subTest(action=action):
                message = SimpleNamespace(
                    guild=SimpleNamespace(id=1, get_member=lambda _id: None),
                    author=SimpleNamespace(id=2),
                    channel=SimpleNamespace(name="bnl-testing"),
                    reply=mock.AsyncMock(),
                )
                held = SimpleNamespace(ok=False, reason="test_held", status="held", http_status=0)
                with ExitStack() as stack:
                    for name, value in {
                        "journal_command_permission_denial": "",
                        "resolve_channel_policy": "sealed_test",
                        "is_public_prompt_context": False,
                        "_parse_journal_command": (True, {"action": action, "entry_id": "journal-test"}, ""),
                    }.items():
                        stack.enter_context(mock.patch.object(bot, name, return_value=value))
                    targets = {}
                    for name in (
                        "generate_and_store_journal_draft",
                        "regenerate_journal_draft",
                        "release_prepared_journal_entry",
                    ):
                        targets[name] = stack.enter_context(mock.patch.object(bot, name, return_value=held))
                    handled = self.run_contended(
                        lambda: bot.maybe_handle_journal_command(message, "!bnl journal " + action)
                    )
                self.assertTrue(handled)
                message.reply.assert_awaited_once()
                selected = {
                    "create": ("generate_and_store_journal_draft", "excluded_history_entry_ids"),
                    "regenerate": ("regenerate_journal_draft", "excluded_history_entry_ids"),
                    "retry": ("release_prepared_journal_entry", "memory_excluded_entry_ids"),
                }.get(action)
                if selected:
                    target, argument = selected
                    targets[target].assert_called_once()
                    self.assertEqual({"journal-after"}, targets[target].call_args.kwargs[argument])


if __name__ == "__main__":
    unittest.main()
