from datetime import datetime
import os
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


def gemini_response(text: str):
    return SimpleNamespace(
        candidates=[
            SimpleNamespace(
                content=SimpleNamespace(
                    parts=[SimpleNamespace(text=text)]
                )
            )
        ],
        usage_metadata=SimpleNamespace(total_token_count=10),
    )


class CostControlSchedulerTests(unittest.IsolatedAsyncioTestCase):
    def test_background_decorator_routes_keep_zero_retry_policy(self):
        for child in (
            "glitch_rewrite",
            "cross_universe_bleed",
            "media_response_grounding_repair",
            "conversation_grounding_regeneration",
        ):
            route = bnl01_bot._generation_child_route(
                "ambient_generation",
                child,
            )
            with self.subTest(route=route):
                policy = bnl01_bot.policy_for_route(route)
                self.assertEqual(policy.lane, "background")
                self.assertEqual(policy.provider_retries, 0)
                self.assertFalse(policy.allow_fallback)
        self.assertEqual(
            bnl01_bot._generation_child_route("normal_chat", "glitch_rewrite"),
            "glitch_rewrite",
        )

    def test_quiet_cadence_reduces_snapshot_periodic_slots_by_two_thirds(self):
        with mock.patch.object(
            bnl01_bot,
            "BNL_WEBSITE_QUIET_RELAY_INTERVAL_MINUTES",
            60,
        ):
            scheduled = [
                bnl01_bot.PACIFIC_TZ.localize(
                    datetime(2026, 8, 22, minute // 60, minute % 60)
                )
                for minute in range(0, 17 * 60 + 41, 20)
            ]
            quiet_eligible = [
                at
                for at in scheduled
                if bnl01_bot._scheduled_quiet_relay_due(at)
            ]
        self.assertEqual(len(scheduled), 54)
        self.assertEqual(len(quiet_eligible), 18)

    async def run_relay_tick(self, at, *, claimed=True):
        guild = SimpleNamespace(id=42, get_channel=lambda _channel_id: None)
        decision = bnl01_bot.WebsiteRelayDecision(
            False,
            skipReason="no_new_public_signal",
        )
        execute = mock.AsyncMock(return_value=decision)
        datetime_mock = mock.Mock(wraps=datetime)
        datetime_mock.now.return_value = at
        with (
            mock.patch.object(bnl01_bot, "datetime", datetime_mock),
            mock.patch.object(bnl01_bot, "BNL_WEBSITE_RELAY_ENABLED", True),
            mock.patch.object(
                bnl01_bot,
                "get_bnl_control_flags",
                return_value={"websiteRelayEnabled": True},
            ),
            mock.patch.object(
                bnl01_bot,
                "relay_claim_scheduled_period",
                return_value=claimed,
            ) as claim,
            mock.patch.object(
                bnl01_bot,
                "iter_managed_guilds",
                return_value=[guild],
            ),
            mock.patch.object(bnl01_bot, "get_guild_config", return_value=0),
            mock.patch.object(
                bnl01_bot,
                "_execute_website_relay_transaction",
                execute,
            ),
            mock.patch.object(
                bnl01_bot,
                "BNL_WEBSITE_RELAY_INTERVAL_MINUTES",
                20,
            ),
            mock.patch.object(
                bnl01_bot,
                "BNL_WEBSITE_QUIET_RELAY_INTERVAL_MINUTES",
                60,
            ),
        ):
            await bnl01_bot.website_relay_task.coro()
        return claim, execute

    async def test_regular_tick_checks_fresh_signal_without_quiet_generation(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 22, 17, 40))
        claim, execute = await self.run_relay_tick(at)
        claim.assert_called_once_with(
            bnl01_bot.DB_FILE,
            42,
            "2026-08-22T17:40-07:00",
        )
        execute.assert_awaited_once()
        self.assertFalse(execute.await_args.kwargs["allow_quiet_sources"])

    async def test_hourly_tick_retains_scheduled_quiet_relay_behavior(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 22, 18, 0))
        _claim, execute = await self.run_relay_tick(at)
        execute.assert_awaited_once()
        self.assertTrue(execute.await_args.kwargs["allow_quiet_sources"])

    async def test_duplicate_worker_claim_makes_no_generation_call(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 22, 18, 20))
        _claim, execute = await self.run_relay_tick(at, claimed=False)
        execute.assert_not_awaited()

    async def test_disabled_website_relay_exits_before_claim_or_generation(self):
        claim = mock.Mock()
        execute = mock.AsyncMock()
        with (
            mock.patch.object(bnl01_bot, "BNL_WEBSITE_RELAY_ENABLED", True),
            mock.patch.object(
                bnl01_bot,
                "get_bnl_control_flags",
                return_value={"websiteRelayEnabled": False},
            ),
            mock.patch.object(
                bnl01_bot,
                "relay_claim_scheduled_period",
                claim,
            ),
            mock.patch.object(
                bnl01_bot,
                "_execute_website_relay_transaction",
                execute,
            ),
        ):
            await bnl01_bot.website_relay_task.coro()
        claim.assert_not_called()
        execute.assert_not_awaited()

    async def test_disabled_showday_outputs_make_zero_provider_calls(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 21, 18, 40))
        datetime_mock = mock.Mock(wraps=datetime)
        datetime_mock.now.return_value = at
        generate = mock.AsyncMock()
        mark = mock.Mock()
        with (
            mock.patch.object(bnl01_bot, "datetime", datetime_mock),
            mock.patch.object(
                bnl01_bot,
                "FRIDAY_SHOW_PHASES",
                [{"key": "submissions_open", "hour": 18, "minute": 40, "window_min": 5}],
            ),
            mock.patch.object(
                bnl01_bot,
                "iter_managed_guilds",
                return_value=[SimpleNamespace(id=42)],
            ),
            mock.patch.object(
                bnl01_bot,
                "claim_show_update_period",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_active_show_state_override",
                return_value=None,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_bnl_control_flags",
                return_value={
                    "showdayDiscordPostsEnabled": False,
                    "websiteRelayEnabled": False,
                },
            ),
            mock.patch.object(
                bnl01_bot,
                "generate_showday_messages",
                generate,
            ),
            mock.patch.object(
                bnl01_bot,
                "mark_show_update_fired",
                mark,
            ),
        ):
            await bnl01_bot.barcode_radio_queue_task.coro()
        generate.assert_not_awaited()
        mark.assert_called_once()

    async def test_v2_showday_with_discord_disabled_has_no_delivery_or_provider_call(self):
        at = bnl01_bot.PACIFIC_TZ.localize(
            datetime(2026, 8, 21, 18, 40)
        )
        datetime_mock = mock.Mock(wraps=datetime)
        datetime_mock.now.return_value = at
        generate = mock.AsyncMock()
        with (
            mock.patch.object(bnl01_bot, "datetime", datetime_mock),
            mock.patch.object(
                bnl01_bot,
                "FRIDAY_SHOW_PHASES",
                [{"key": "submissions_open", "hour": 18, "minute": 40, "window_min": 5}],
            ),
            mock.patch.object(
                bnl01_bot,
                "iter_managed_guilds",
                return_value=[SimpleNamespace(id=42)],
            ),
            mock.patch.object(
                bnl01_bot,
                "claim_show_update_period",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_active_show_state_override",
                return_value=None,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_bnl_control_flags",
                return_value={
                    "showdayDiscordPostsEnabled": False,
                    "websiteRelayEnabled": True,
                },
            ),
            mock.patch.object(bnl01_bot, "BNL_WEBSITE_CONTRACT_VERSION", "2"),
            mock.patch.object(bnl01_bot, "BNL_API_KEY", "configured"),
            mock.patch.object(bnl01_bot, "BNL_STATUS_URL", "https://example.test"),
            mock.patch.object(
                bnl01_bot,
                "generate_showday_messages",
                generate,
            ),
            mock.patch.object(bnl01_bot, "mark_show_update_fired"),
        ):
            await bnl01_bot.barcode_radio_queue_task.coro()
        generate.assert_not_awaited()

    async def test_showday_generation_is_labeled_as_background(self):
        seen = []

        async def generate(_prompt, **kwargs):
            seen.append(kwargs)
            return "Discord line\nWebsite line with enough complete text to use safely."

        with (
            mock.patch.object(bnl01_bot, "get_recent_signal_summary", return_value=""),
            mock.patch.object(bnl01_bot, "get_gemini_response", new=generate),
        ):
            await bnl01_bot.generate_showday_messages(42, "show_live")
        self.assertEqual(seen[0]["route"], "showday_generation")
        self.assertEqual(
            bnl01_bot.policy_for_route(seen[0]["route"]).lane,
            "background",
        )

    async def test_ambient_generation_is_labeled_as_background(self):
        seen = []

        async def generate(_prompt, **kwargs):
            seen.append(kwargs)
            return "The room signal holds a precise and unfamiliar angle."

        with (
            mock.patch.object(
                bnl01_bot,
                "get_recent_guild_user_messages",
                return_value=[("member", "public signal")],
            ),
            mock.patch.object(bnl01_bot, "get_recent_ambient", return_value=[]),
            mock.patch.object(
                bnl01_bot,
                "build_dynamic_curiosity_payload",
                return_value=("light_probe", "- public signal"),
            ),
            mock.patch.object(
                bnl01_bot,
                "get_temporal_context",
                return_value={
                    "now_str": "Saturday 5:00 PM Pacific",
                    "weekday": "Saturday",
                    "show_phase": "off_cycle",
                },
            ),
            mock.patch.object(
                bnl01_bot,
                "build_scoped_broadcast_memory_context",
                return_value="",
            ),
            mock.patch.object(bnl01_bot, "_too_similar", return_value=False),
            mock.patch.object(bnl01_bot, "get_gemini_response", new=generate),
        ):
            result = await bnl01_bot.generate_dynamic_ambient(42, 100)
        self.assertTrue(result)
        self.assertEqual(seen[0]["route"], "ambient_generation")
        self.assertEqual(
            bnl01_bot.policy_for_route(seen[0]["route"]).lane,
            "background",
        )

    async def test_ambient_provider_failure_does_not_trigger_validator_retry(self):
        failure = bnl01_bot.BackgroundGenerationUnavailable(
            bnl01_bot.GenerationResult(
                False,
                error_category=bnl01_bot.GENERATION_ERROR_PROVIDER_SERVER,
            )
        )
        generate = mock.AsyncMock(side_effect=failure)
        with (
            mock.patch.object(
                bnl01_bot,
                "get_recent_guild_user_messages",
                return_value=[("member", "public signal")],
            ),
            mock.patch.object(bnl01_bot, "get_recent_ambient", return_value=[]),
            mock.patch.object(
                bnl01_bot,
                "build_dynamic_curiosity_payload",
                return_value=("light_probe", "- public signal"),
            ),
            mock.patch.object(
                bnl01_bot,
                "get_temporal_context",
                return_value={
                    "now_str": "Saturday 5:00 PM Pacific",
                    "weekday": "Saturday",
                    "show_phase": "off_cycle",
                },
            ),
            mock.patch.object(
                bnl01_bot,
                "build_scoped_broadcast_memory_context",
                return_value="",
            ),
            mock.patch.object(bnl01_bot, "get_gemini_response", new=generate),
        ):
            result = await bnl01_bot.generate_dynamic_ambient(42, 100)
        self.assertEqual(result, "")
        generate.assert_awaited_once()

    async def test_ambient_failed_grounding_repair_stops_outer_retry(self):
        provider = mock.AsyncMock(
            side_effect=[
                gemini_response(
                    "The Network archives yielded no results for this signal."
                ),
                RuntimeError("503 service unavailable"),
            ]
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "get_recent_guild_user_messages",
                return_value=[("member", "public signal")],
            ),
            mock.patch.object(bnl01_bot, "get_recent_ambient", return_value=[]),
            mock.patch.object(
                bnl01_bot,
                "build_dynamic_curiosity_payload",
                return_value=("light_probe", "- public signal"),
            ),
            mock.patch.object(
                bnl01_bot,
                "get_temporal_context",
                return_value={
                    "now_str": "Saturday 5:00 PM Pacific",
                    "weekday": "Saturday",
                    "show_phase": "off_cycle",
                },
            ),
            mock.patch.object(
                bnl01_bot,
                "build_scoped_broadcast_memory_context",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "check_quota_availability",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "_generate_gemini_content_with_fallback_async",
                new=provider,
            ),
            mock.patch.object(bnl01_bot.random, "random", return_value=1.0),
        ):
            result = await bnl01_bot.generate_dynamic_ambient(42, 100)

        self.assertEqual(result, "")
        self.assertEqual(provider.await_count, 2)
        self.assertEqual(
            [call.args[1] for call in provider.await_args_list],
            [
                "ambient_generation",
                "ambient_generation.conversation_grounding_regeneration",
            ],
        )

    async def test_dormant_echo_provider_failure_does_not_retry(self):
        failure = bnl01_bot.BackgroundGenerationUnavailable(
            bnl01_bot.GenerationResult(
                False,
                error_category=bnl01_bot.GENERATION_ERROR_PROVIDER_SERVER,
            )
        )
        generate = mock.AsyncMock(side_effect=failure)
        with (
            mock.patch.object(
                bnl01_bot,
                "dormant_echo_room_has_signal",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_recent_ambient_owned_messages",
                return_value=[],
            ),
            mock.patch.object(
                bnl01_bot,
                "build_dormant_echo_prompt",
                return_value="prompt",
            ),
            mock.patch.object(bnl01_bot, "get_gemini_response", new=generate),
        ):
            result, reason = await bnl01_bot.generate_dormant_echo(
                42,
                100,
                {},
                recent_context=[("member", "public signal")],
            )
        self.assertEqual(result, "")
        self.assertEqual(reason, bnl01_bot.GENERATION_ERROR_PROVIDER_SERVER)
        generate.assert_awaited_once()

    async def test_occasion_provider_failure_does_not_retry(self):
        failure = bnl01_bot.BackgroundGenerationUnavailable(
            bnl01_bot.GenerationResult(
                False,
                error_category=bnl01_bot.GENERATION_ERROR_PROVIDER_SERVER,
            )
        )
        generate = mock.AsyncMock(side_effect=failure)
        with (
            mock.patch.object(
                bnl01_bot,
                "build_occasion_prompt",
                return_value="prompt",
            ),
            mock.patch.object(bnl01_bot, "get_gemini_response", new=generate),
        ):
            result, reason = await bnl01_bot.generate_occasion_reflection(
                42,
                SimpleNamespace(),
                {},
            )
        self.assertEqual(result, "")
        self.assertEqual(reason, bnl01_bot.GENERATION_ERROR_PROVIDER_SERVER)
        generate.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
