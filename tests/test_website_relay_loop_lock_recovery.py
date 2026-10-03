"""Keep the real Discord Relay loop alive through transient SQLite contention.

Only the relevant bot definitions are compiled, avoiding bot import side effects.
All control flags, schedule claims, and transactions are isolated test doubles.
"""

import ast
import asyncio
from datetime import datetime, timezone
import logging
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest import mock

from discord.ext import tasks


def load_relay_namespace():
    bot_path = Path(__file__).resolve().parents[1] / "bnl01_bot.py"
    wanted = {
        "_sqlite_busy",
        "_website_relay_task_once",
        "website_relay_task",
    }
    parsed = ast.parse(bot_path.read_text(encoding="utf-8"))
    definitions = [
        node for node in parsed.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in wanted
    ]
    found = {node.name for node in definitions}
    if found != wanted:
        raise AssertionError("Missing Relay definitions: %s" % (wanted - found))
    namespace = {
        "asyncio": asyncio,
        "datetime": datetime,
        "logging": logging,
        "sqlite3": sqlite3,
        "tasks": tasks,
        "PACIFIC_TZ": timezone.utc,
        "BNL_WEBSITE_RELAY_ENABLED": True,
        "DB_FILE": "isolated-test-db-not-opened",
        "get_bnl_control_flags": mock.Mock(
            return_value={"websiteRelayEnabled": True}
        ),
        "_scheduled_relay_due": mock.Mock(return_value=True),
        "_scheduled_quiet_relay_due": mock.Mock(return_value=False),
        "iter_managed_guilds": mock.Mock(return_value=[]),
        "relay_claim_scheduled_period": mock.Mock(return_value=True),
        "get_guild_config": mock.Mock(return_value=0),
        "_execute_website_relay_transaction": mock.AsyncMock(),
    }
    module = ast.Module(body=definitions, type_ignores=[])
    exec(compile(module, str(bot_path), "exec"), namespace)
    return namespace


class WebsiteRelayLoopLockRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.namespace = load_relay_namespace()
        self.loop = self.namespace["website_relay_task"]
        self.loop.change_interval(seconds=0.001)

    async def run_loop(self):
        await asyncio.wait_for(self.loop.start(), timeout=2)

    async def assert_next_tick_survives(self, error):
        attempts = []

        async def isolated_once():
            attempts.append(len(attempts) + 1)
            if len(attempts) == 1:
                raise error
            self.loop.stop()

        self.namespace["_website_relay_task_once"] = isolated_once
        with self.assertLogs(level="WARNING") as logs:
            await self.run_loop()

        self.assertEqual(attempts, [1, 2])
        self.assertFalse(self.loop.failed())
        self.assertIn(
            "website_relay_retry_next_tick reason=database_locked",
            "\n".join(logs.output),
        )

    async def assert_loop_failure_preserved(self, error):
        once = mock.AsyncMock(side_effect=error)
        self.namespace["_website_relay_task_once"] = once
        reported = []

        async def report_error(exc):
            reported.append(exc)

        self.loop.error(report_error)
        with self.assertRaises(type(error)) as raised:
            await self.run_loop()

        self.assertIs(raised.exception, error)
        self.assertTrue(self.loop.failed())
        once.assert_awaited_once()
        self.assertEqual(reported, [error])

    async def test_real_discord_loop_runs_next_iteration_after_database_lock(self):
        await self.assert_next_tick_survives(
            sqlite3.OperationalError("database is locked")
        )

    async def test_native_sqlite_busy_code_keeps_loop_alive(self):
        error = sqlite3.OperationalError("busy from native SQLite")
        error.sqlite_errorcode = 5  # SQLITE_BUSY
        await self.assert_next_tick_survives(error)

    async def test_extended_sqlite_locked_code_keeps_loop_alive(self):
        error = sqlite3.OperationalError("locked from native SQLite")
        error.sqlite_errorcode = 262  # SQLITE_LOCKED_SHAREDCACHE
        await self.assert_next_tick_survives(error)

    async def test_non_lock_operational_error_still_fails_loop(self):
        await self.assert_loop_failure_preserved(
            sqlite3.OperationalError("no such table: test_missing_table")
        )

    async def test_unexpected_failure_still_fails_loop(self):
        await self.assert_loop_failure_preserved(
            RuntimeError("unexpected Relay failure")
        )

    async def test_flags_read_lock_recovers_on_next_real_loop_tick(self):
        self.namespace["get_bnl_control_flags"].side_effect = [
            sqlite3.OperationalError("database schema is locked"),
            {"websiteRelayEnabled": True},
        ]

        def finish_next_tick(now_pt):
            self.loop.stop()
            return False

        self.namespace["_scheduled_relay_due"].side_effect = finish_next_tick
        with self.assertLogs(level="WARNING"):
            await self.run_loop()

        self.assertFalse(self.loop.failed())
        self.assertEqual(
            self.namespace["get_bnl_control_flags"].call_count, 2
        )
        self.namespace["relay_claim_scheduled_period"].assert_not_called()
        self.namespace["_execute_website_relay_transaction"].assert_not_awaited()

    async def test_lock_after_claim_does_not_bypass_existing_delivery_authority(self):
        guild = SimpleNamespace(id=42)
        self.namespace["iter_managed_guilds"].return_value = [guild]
        claim = self.namespace["relay_claim_scheduled_period"]
        claim.side_effect = [True, False]
        execute = self.namespace["_execute_website_relay_transaction"]
        execute.side_effect = sqlite3.OperationalError("database is locked")
        fixed_now = datetime(2026, 10, 2, 19, 0, tzinfo=timezone.utc)
        self.namespace["datetime"] = SimpleNamespace(now=lambda _tz: fixed_now)

        with self.assertLogs(level="WARNING"):
            await self.loop()
        await self.loop()

        self.assertEqual(claim.call_count, 2)
        self.assertEqual(claim.call_args_list[0], claim.call_args_list[1])
        execute.assert_awaited_once_with(
            42,
            force=False,
            source="relay",
            admin_note_source="relay",
            allow_quiet_sources=False,
        )


if __name__ == "__main__":
    unittest.main()
