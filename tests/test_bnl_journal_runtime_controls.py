import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store
from bnl_journal_automation import AutomationResult

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

try:
    import bnl01_bot
except ModuleNotFoundError as exc:  # Local minimal test images may omit the Discord runtime dependency.
    if exc.name != "discord":
        raise
    bnl01_bot = None


class Response:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode("utf-8")

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


@unittest.skipIf(bnl01_bot is None, "discord.py is not installed in this local test image")
class JournalRuntimeControlTests(unittest.TestCase):
    def setUp(self):
        bnl01_bot._journal_automation_locks_by_guild.clear()
        bnl01_bot._journal_automation_runtime_by_guild.clear()

    def test_new_operator_commands_parse(self):
        for command, action in (
            ("!bnl journal status", "status"),
            ("!bnl journal run-daily", "run-daily"),
            ("!bnl journal run-weekly", "run-weekly"),
            ("!bnl journal rehydrate", "rehydrate"),
        ):
            matched, options, error = bnl01_bot._parse_journal_command(command)
            self.assertTrue(matched)
            self.assertEqual(action, options["action"])
            self.assertEqual("", error)

    def test_daily_worker_is_pinned_to_seven_pm_pacific(self):
        scheduled_times = bnl01_bot.journal_daily_schedule_task.time
        self.assertEqual(1, len(scheduled_times))
        scheduled = scheduled_times[0]
        self.assertEqual((19, 0), (scheduled.hour, scheduled.minute))
        self.assertEqual("America/Los_Angeles", getattr(scheduled.tzinfo, "key", ""))

    def test_control_get_uses_api_key_and_expected_endpoint(self):
        captured = []

        def opener(request, timeout=10):
            captured.append((request, timeout))
            return Response({"contractVersion": 1, "runRequests": []})

        with mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://site.example/api/bnl/status"), \
             mock.patch.object(bnl01_bot, "BNL_API_KEY", "secret"), \
             mock.patch("urllib.request.urlopen", side_effect=opener):
            payload, reason = bnl01_bot._journal_control_request_sync("GET")

        self.assertEqual("", reason)
        self.assertEqual(1, payload["contractVersion"])
        request, timeout = captured[0]
        self.assertEqual("https://site.example/api/bnl/journal/control", request.full_url)
        self.assertEqual("secret", request.get_header("X-api-key"))
        self.assertEqual(10, timeout)

    def test_claim_and_run_reports_match_site_contract(self):
        posts = []

        def post(method, payload=None):
            posts.append((method, payload))
            if payload["action"] == "claimRunRequest":
                return {
                    "ok": True,
                    "request": {
                        "requestId": "request-1",
                        "cadence": "daily",
                        "status": "running",
                    },
                }, ""
            return {"ok": True}, ""

        control = {
            "runRequests": [
                {"requestId": "request-1", "cadence": "daily", "status": "queued"}
            ]
        }
        with mock.patch.object(bnl01_bot, "_journal_control_request_sync", side_effect=post):
            claimed, reason = bnl01_bot._journal_control_claim_sync(control)
            self.assertEqual("", reason)
            self.assertEqual("request-1", claimed["requestId"])
            bnl01_bot._journal_report_run_sync(
                {
                    "cadence": "daily",
                    "status": "quiet",
                    "reason": "insufficient_meaningful_activity",
                    "entry_id": "",
                    "source_window_start": "2026-07-17T07:00:00Z",
                    "source_window_end": "2026-07-18T07:00:00Z",
                    "aggregate_counts": {"eligibleRelays": 75, "eligibleConversations": 8},
                },
                guild_id=1,
                request_id="request-1",
            )

        claim = posts[0][1]
        report = posts[1][1]["run"]
        self.assertEqual("claimRunRequest", claim["action"])
        self.assertEqual("request-1", claim["requestId"])
        self.assertRegex(claim["claimToken"], r"^worker-[a-f0-9]{32}$")
        self.assertEqual("reportRun", posts[1][1]["action"])
        self.assertEqual("request-1", report["runId"])
        self.assertEqual("request-1", report["requestId"])
        self.assertEqual("skipped", report["state"])
        self.assertEqual(83, report["sourceCount"])

    def test_daily_pause_flag_blocks_generator_runner(self):
        flags = {
            "journalAutoPublishEnabled": True,
            "journalDailyEnabled": False,
            "journalWeeklyEnabled": True,
        }
        with mock.patch.object(bnl01_bot, "BNL_JOURNAL_AUTOMATION_ENABLED", True), \
             mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 0), \
             mock.patch.object(bnl01_bot, "run_daily_journal_automation") as daily:
            results = asyncio.run(
                bnl01_bot.run_journal_automation_once(
                    1, cadence="daily", force=True, flags=flags
                )
            )
        daily.assert_not_called()
        self.assertEqual("paused", results[0]["status"])
        self.assertEqual("daily_automation_paused", results[0]["reason"])

    def test_entry_memory_exclusions_are_sanitized_and_forwarded(self):
        flags = bnl01_bot._journal_control_flags(
            {
                "config": {},
                "memoryExcludedEntryIds": [
                    " journal_valid-1 ",
                    "journal_valid-1",
                    "../invalid",
                    "",
                    123,
                ],
            },
            {
                "journalAutoPublishEnabled": True,
                "journalDailyEnabled": True,
                "journalWeeklyEnabled": True,
            },
        )
        self.assertEqual(["journal_valid-1"], flags["journalMemoryExcludedEntryIds"])

        result = AutomationResult(True, "daily", "quiet", aggregate_counts={})
        with mock.patch.object(bnl01_bot, "BNL_JOURNAL_AUTOMATION_ENABLED", True), \
             mock.patch.object(bnl01_bot, "BNL_PRIMARY_GUILD_ID", 0), \
             mock.patch.object(bnl01_bot, "run_daily_journal_automation", return_value=result) as daily:
            asyncio.run(bnl01_bot.run_journal_automation_once(1, cadence="daily", force=True, flags=flags))

        self.assertEqual(
            {"journal_valid-1"},
            daily.call_args.kwargs["memory_excluded_entry_ids"],
        )

    def test_memory_exclusion_snapshot_survives_outage_and_confirmed_empty(self):
        temp = tempfile.NamedTemporaryFile(delete=False)
        db_path = temp.name
        temp.close()
        try:
            base = {
                "journalAutoPublishEnabled": True,
                "journalDailyEnabled": True,
                "journalWeeklyEnabled": True,
            }
            with mock.patch.object(bnl01_bot, "DB_FILE", db_path):
                live = bnl01_bot._journal_control_flags_for_guild(
                    1,
                    {
                        "config": {},
                        "memoryExcludedEntryIds": ["journal-z", "journal-a"],
                    },
                    base,
                )
                self.assertTrue(live["journalMemoryExclusionsConfirmed"])
                self.assertEqual(
                    ["journal-z", "journal-a"],
                    live["journalMemoryExcludedEntryIds"],
                )

                after_restart_outage = bnl01_bot._journal_control_flags_for_guild(
                    1,
                    None,
                    base,
                )
                self.assertTrue(
                    after_restart_outage["journalMemoryExclusionsConfirmed"]
                )
                self.assertEqual(
                    ["journal-a", "journal-z"],
                    after_restart_outage["journalMemoryExcludedEntryIds"],
                )

                confirmed_empty = bnl01_bot._journal_control_flags_for_guild(
                    1,
                    {"config": {}, "memoryExcludedEntryIds": []},
                    base,
                )
                self.assertTrue(confirmed_empty["journalMemoryExclusionsConfirmed"])
                self.assertEqual([], confirmed_empty["journalMemoryExcludedEntryIds"])
                stored, confirmed = bnl01_bot.load_journal_memory_exclusions(db_path, 1)
                self.assertTrue(confirmed)
                self.assertEqual(set(), stored)

            second = tempfile.NamedTemporaryFile(delete=False)
            second_path = second.name
            second.close()
            try:
                with mock.patch.object(bnl01_bot, "DB_FILE", second_path):
                    cold = bnl01_bot._journal_control_flags_for_guild(1, None, base)
                self.assertFalse(cold["journalMemoryExclusionsConfirmed"])
            finally:
                os.unlink(second_path)
        finally:
            os.unlink(db_path)

    def test_control_flag_fetch_failure_retains_confirmed_pause(self):
        paused = {
            "websiteRelayEnabled": True,
            "heartbeatEnabled": True,
            "showdayDiscordPostsEnabled": False,
            "journalAutoPublishEnabled": False,
            "journalDailyEnabled": False,
            "journalWeeklyEnabled": True,
        }
        with mock.patch.object(bnl01_bot, "_bnl_control_flags_cache", dict(paused)), \
             mock.patch.object(bnl01_bot, "_bnl_control_flags_cached_at", None), \
             mock.patch.object(bnl01_bot, "_bnl_control_flags_has_remote_snapshot", True), \
             mock.patch.object(bnl01_bot, "_build_bnl_control_flag_urls", return_value=["https://site.example/api/bnl/control-flags"]), \
             mock.patch("urllib.request.urlopen", side_effect=OSError("temporary network failure")):
            flags = bnl01_bot.get_bnl_control_flags(force_refresh=True)

        self.assertFalse(flags["journalAutoPublishEnabled"])
        self.assertFalse(flags["journalDailyEnabled"])

    def test_control_flag_initial_local_failure_still_uses_startup_defaults(self):
        with mock.patch.object(bnl01_bot, "_bnl_control_flags_cache", None), \
             mock.patch.object(bnl01_bot, "_bnl_control_flags_cached_at", None), \
             mock.patch.object(bnl01_bot, "_bnl_control_flags_has_remote_snapshot", False), \
             mock.patch.object(bnl01_bot, "_build_bnl_control_flag_urls", return_value=[]):
            flags = bnl01_bot.get_bnl_control_flags(force_refresh=True)

        self.assertTrue(flags["journalAutoPublishEnabled"])
        self.assertTrue(flags["journalDailyEnabled"])
        self.assertTrue(flags["journalWeeklyEnabled"])

    def test_control_get_failure_runs_local_schedule_with_cached_flags(self):
        cached_flags = {
            "journalAutoPublishEnabled": True,
            "journalDailyEnabled": True,
            "journalWeeklyEnabled": True,
        }
        runner = mock.AsyncMock(return_value=[{
            "cadence": "all",
            "status": "not_due",
            "reason": "no_schedule_due",
        }])
        with mock.patch.object(bnl01_bot, "resolve_network_guild_id", return_value=1), \
             mock.patch.object(bnl01_bot, "get_bnl_control_flags", return_value=cached_flags), \
             mock.patch.object(bnl01_bot, "_journal_control_request_sync", return_value=(None, "control_plane_timeout")), \
             mock.patch.object(bnl01_bot, "load_journal_memory_exclusions", return_value=(set(), False)), \
             mock.patch.object(bnl01_bot, "_journal_heartbeat_sync", return_value=(None, "")), \
             mock.patch.object(bnl01_bot, "journal_automation_status", return_value={}), \
             mock.patch.object(bnl01_bot, "run_journal_automation_once", new=runner):
            results = asyncio.run(bnl01_bot.run_journal_automation_control_cycle(1))

        runner.assert_awaited_once_with(
            1,
            cadence="scheduled",
            force=False,
            flags={**cached_flags, "journalMemoryExclusionsConfirmed": False},
        )
        self.assertEqual("not_due", results[0]["status"])
        self.assertEqual("no_schedule_due", results[0]["reason"])
        self.assertEqual(
            "control_plane_unavailable:control_plane_timeout",
            bnl01_bot._journal_automation_runtime_by_guild[1]["lastError"],
        )

    def test_control_get_failure_still_honors_cached_publish_pause(self):
        cached_flags = {
            "journalAutoPublishEnabled": False,
            "journalDailyEnabled": False,
            "journalWeeklyEnabled": True,
        }
        with mock.patch.object(bnl01_bot, "resolve_network_guild_id", return_value=1), \
             mock.patch.object(bnl01_bot, "BNL_JOURNAL_AUTOMATION_ENABLED", True), \
             mock.patch.object(bnl01_bot, "get_bnl_control_flags", return_value=cached_flags), \
             mock.patch.object(bnl01_bot, "_journal_control_request_sync", return_value=(None, "control_plane_timeout")), \
             mock.patch.object(bnl01_bot, "load_journal_memory_exclusions", return_value=(set(), False)), \
             mock.patch.object(bnl01_bot, "_journal_heartbeat_sync", return_value=(None, "")), \
             mock.patch.object(bnl01_bot, "journal_automation_status", return_value={}), \
             mock.patch.object(bnl01_bot, "run_scheduled_journal_automation") as scheduled:
            results = asyncio.run(bnl01_bot.run_journal_automation_control_cycle(1))

        scheduled.assert_not_called()
        self.assertEqual("paused", results[0]["status"])
        self.assertEqual("auto_publish_paused", results[0]["reason"])

    def test_overlapping_schedule_waits_instead_of_returning_busy(self):
        async def scenario():
            lock = bnl01_bot._journal_automation_lock(1)
            await lock.acquire()
            try:
                pending = asyncio.create_task(
                    bnl01_bot.run_journal_automation_once(
                        1,
                        cadence="daily",
                        flags={
                            "journalAutoPublishEnabled": False,
                            "journalDailyEnabled": True,
                            "journalWeeklyEnabled": True,
                        },
                    )
                )
                await asyncio.sleep(0)
                self.assertFalse(pending.done())
            finally:
                lock.release()
            return await pending

        with mock.patch.object(bnl01_bot, "resolve_network_guild_id", return_value=1):
            results = asyncio.run(scenario())

        self.assertEqual("paused", results[0]["status"])
        self.assertEqual("auto_publish_paused", results[0]["reason"])

    def test_clear_history_also_purges_durable_journal_discord_sources(self):
        temp = tempfile.NamedTemporaryFile(delete=False)
        db_path = temp.name
        temp.close()
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "CREATE TABLE conversations(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER,guild_id INTEGER)"
                )
                conn.executemany(
                    "INSERT INTO conversations(user_id,guild_id) VALUES(?,?)",
                    [(7, 1), (8, 1), (7, 2)],
                )
            for key, guild_id, source_kind, subject_ref in (
                ("u7-g1", 1, "discord_message", "discord_user:7"),
                ("u8-g1", 1, "discord_message", "discord_user:8"),
                ("u7-g2", 2, "discord_message", "discord_user:7"),
                ("relay-g1", 1, "website_relay", "bnl_01"),
            ):
                source_store.record_source_event(
                    db_path,
                    guild_id=guild_id,
                    source_kind=source_kind,
                    source_key=key,
                    occurred_at_ms=1_000,
                    raw_text="durable source text",
                    sanitized_summary="durable source text",
                    subject_ref=subject_ref,
                    channel_policy="public_home" if source_kind == "discord_message" else "public_relay",
                )

            journal.ensure_schema(db_path)
            automation.ensure_schema(db_path)
            source_event = next(
                event
                for event in source_store.query_source_events(db_path, 1, 0, 2_000).events
                if event["source_key"] == "u7-g1"
            )
            claim, run_id, epoch, _ = automation._claim_preparation(
                db_path,
                1,
                "daily",
                "2026-07-20T02:00:00Z",
                "2026-07-21T02:00:00Z",
                force=True,
            )
            self.assertEqual("claimed", claim)
            frozen_packet = {
                "entryKind": "daily",
                "sourceArchiveAvailable": True,
                "coverageComplete": True,
                "sourceWindowStart": "2026-07-20T02:00:00Z",
                "sourceWindowEnd": "2026-07-21T02:00:00Z",
                "aggregateCounts": {"eligibleConversations": 1},
                "privateSources": [
                    {
                        "refId": f"fresh:{source_event['event_seq']}",
                        "sourceKind": "conversation",
                        "subjectRef": "discord_user:7",
                        "summary": "durable source text",
                    }
                ],
                "safeSources": [],
                "privateContextLaneProvenance": {},
            }
            frozen, packet_hash, reason = automation._freeze_or_load_packet(
                db_path,
                1,
                run_id,
                epoch,
                lambda: frozen_packet,
            )
            self.assertEqual(frozen_packet, frozen)
            self.assertTrue(packet_hash)
            self.assertEqual("", reason)
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    "UPDATE bnl_journal_automation_runs "
                    "SET lifecycle_state='prepared',lease_expires_at=NULL WHERE run_id=?",
                    (run_id,),
                )

            with mock.patch.object(bnl01_bot, "DB_FILE", db_path):
                self.assertEqual(1, bnl01_bot.clear_user_history(7, 1))
                self.assertEqual(
                    {"u8-g1", "relay-g1"},
                    {event["source_key"] for event in source_store.query_source_events(db_path, 1, 0, 2_000).events},
                )
                with sqlite3.connect(db_path) as conn:
                    lifecycle, frozen_json, prepared_hash = conn.execute(
                        "SELECT lifecycle_state,frozen_packet_json,prepared_payload_hash "
                        "FROM bnl_journal_automation_runs WHERE run_id=?",
                        (run_id,),
                    ).fetchone()
                self.assertEqual("held", lifecycle)
                self.assertIsNone(frozen_json)
                self.assertIsNone(prepared_hash)
                self.assertEqual(1, bnl01_bot.clear_guild_history(1))

            self.assertEqual(
                {"relay-g1"},
                {event["source_key"] for event in source_store.query_source_events(db_path, 1, 0, 2_000).events},
            )
            self.assertEqual(
                {"u7-g2"},
                {event["source_key"] for event in source_store.query_source_events(db_path, 2, 0, 2_000).events},
            )
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
