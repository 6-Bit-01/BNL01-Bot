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

    def test_hourly_relay_and_quiet_cascade_share_the_ten_minute_offset(self):
        with mock.patch.multiple(
            bnl01_bot,
            BNL_WEBSITE_RELAY_INTERVAL_MINUTES=60,
            BNL_WEBSITE_QUIET_RELAY_INTERVAL_MINUTES=60,
            BNL_WEBSITE_RELAY_MINUTE_OFFSET=10,
        ):
            at_ten = bnl01_bot.PACIFIC_TZ.localize(
                datetime(2026, 8, 22, 17, 10)
            )
            at_hour = bnl01_bot.PACIFIC_TZ.localize(
                datetime(2026, 8, 22, 18, 0)
            )
            self.assertTrue(bnl01_bot._scheduled_relay_due(at_ten))
            self.assertTrue(bnl01_bot._scheduled_quiet_relay_due(at_ten))
            self.assertFalse(bnl01_bot._scheduled_relay_due(at_hour))
            self.assertFalse(bnl01_bot._scheduled_quiet_relay_due(at_hour))

    def test_next_hourly_relay_is_reported_at_ten_past(self):
        with mock.patch.multiple(
            bnl01_bot,
            BNL_WEBSITE_RELAY_INTERVAL_MINUTES=60,
            BNL_WEBSITE_RELAY_MINUTE_OFFSET=10,
        ):
            now = bnl01_bot.PACIFIC_TZ.localize(
                datetime(2026, 8, 22, 17, 40, 30)
            )
            self.assertEqual(
                bnl01_bot._next_scheduled_relay_at(now),
                bnl01_bot.PACIFIC_TZ.localize(
                    datetime(2026, 8, 22, 18, 10)
                ),
            )

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
                60,
            ),
            mock.patch.object(
                bnl01_bot,
                "BNL_WEBSITE_RELAY_MINUTE_OFFSET",
                10,
            ),
            mock.patch.object(
                bnl01_bot,
                "BNL_WEBSITE_QUIET_RELAY_INTERVAL_MINUTES",
                60,
            ),
        ):
            await bnl01_bot.website_relay_task.coro()
        return claim, execute

    async def test_hourly_tick_checks_fresh_signal_and_quiet_cascade(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 22, 17, 10))
        claim, execute = await self.run_relay_tick(at)
        claim.assert_called_once_with(
            bnl01_bot.DB_FILE,
            42,
            "2026-08-22T17:10-07:00",
        )
        execute.assert_awaited_once()
        self.assertTrue(execute.await_args.kwargs["allow_quiet_sources"])

    async def test_top_of_hour_is_not_a_relay_tick(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 22, 18, 0))
        claim, execute = await self.run_relay_tick(at)
        claim.assert_not_called()
        execute.assert_not_awaited()

    async def test_duplicate_worker_claim_makes_no_generation_call(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 22, 18, 10))
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

    async def test_showday_revalidates_claim_inside_discord_delivery_lock(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 21, 18, 40))
        datetime_mock = mock.Mock(wraps=datetime)
        datetime_mock.now.return_value = at
        inside_lock = {"value": False}

        class DeliveryLock:
            async def __aenter__(self):
                inside_lock["value"] = True
                return self

            async def __aexit__(self, *_args):
                inside_lock["value"] = False
                return False

        revalidation_count = 0

        async def revalidate(*_args):
            nonlocal revalidation_count
            revalidation_count += 1
            if revalidation_count == 1:
                self.assertFalse(inside_lock["value"])
                return True
            self.assertTrue(inside_lock["value"])
            return False

        channel = SimpleNamespace(id=99, send=mock.AsyncMock())
        guild = SimpleNamespace(
            id=42,
            get_channel=lambda _channel_id: channel,
        )
        generate = mock.AsyncMock(return_value=("discord copy", "website copy"))
        mark = mock.Mock()
        with (
            mock.patch.object(bnl01_bot, "datetime", datetime_mock),
            mock.patch.object(
                bnl01_bot,
                "FRIDAY_SHOW_PHASES",
                [{"key": "submissions_open", "hour": 18, "minute": 40, "window_min": 5}],
            ),
            mock.patch.object(bnl01_bot, "iter_managed_guilds", return_value=[guild]),
            mock.patch.object(bnl01_bot, "claim_show_update_period", return_value="claim-token"),
            mock.patch.object(bnl01_bot, "get_active_show_state_override", return_value=None),
            mock.patch.object(
                bnl01_bot,
                "get_bnl_control_flags",
                return_value={
                    "showdayDiscordPostsEnabled": True,
                    "websiteRelayEnabled": False,
                },
            ),
            mock.patch.object(bnl01_bot, "get_guild_config", return_value=99),
            mock.patch.object(bnl01_bot, "get_recent_ambient", return_value=[]),
            mock.patch.object(bnl01_bot, "get_showday_discord_post_count", return_value=0),
            mock.patch.object(bnl01_bot, "had_recent_showday_discord_post", return_value=False),
            mock.patch.object(bnl01_bot, "generate_showday_messages", generate),
            mock.patch.object(bnl01_bot, "maintain_show_update_claim", mock.AsyncMock()),
            mock.patch.object(bnl01_bot, "revalidate_show_update_claim", side_effect=revalidate),
            mock.patch.object(bnl01_bot, "_ambient_post_lock_for", return_value=DeliveryLock()),
            mock.patch.object(
                bnl01_bot,
                "ambient_capacity_decision",
                return_value={"allowed": True, "capacityUsed": 0, "cap": 2},
            ),
            mock.patch.object(bnl01_bot, "log_ambient") as log_ambient,
            mock.patch.object(bnl01_bot, "mark_show_update_fired", mark),
        ):
            await bnl01_bot.barcode_radio_queue_task.coro()

        self.assertEqual(revalidation_count, 2)
        channel.send.assert_not_awaited()
        log_ambient.assert_not_called()
        mark.assert_not_called()

    async def test_showday_publication_is_fenced_before_discord_send(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 21, 18, 40))
        datetime_mock = mock.Mock(wraps=datetime)
        datetime_mock.now.return_value = at
        inside_lock = {"value": False}
        fence_committed = {"value": False}

        class DeliveryLock:
            async def __aenter__(self):
                inside_lock["value"] = True
                return self

            async def __aexit__(self, *_args):
                inside_lock["value"] = False
                return False

        async def send_after_fence(*_args, **_kwargs):
            self.assertTrue(fence_committed["value"])

        channel = SimpleNamespace(
            id=99,
            send=mock.AsyncMock(side_effect=send_after_fence),
        )
        guild = SimpleNamespace(
            id=42,
            get_channel=lambda _channel_id: channel,
        )
        revalidate = mock.AsyncMock(return_value=True)

        async def commit_fence(*_args):
            self.assertTrue(inside_lock["value"])
            channel.send.assert_not_awaited()
            fence_committed["value"] = True
            return True

        fence = mock.AsyncMock(side_effect=commit_fence)
        website_delivery = mock.AsyncMock()
        generate = mock.AsyncMock(return_value=("discord copy", "website copy"))
        log_ambient = mock.Mock()
        mark = mock.Mock()
        with mock.patch.multiple(
            bnl01_bot,
            datetime=datetime_mock,
            FRIDAY_SHOW_PHASES=[
                {
                    "key": "submissions_open",
                    "hour": 18,
                    "minute": 40,
                    "window_min": 5,
                }
            ],
            iter_managed_guilds=mock.Mock(return_value=[guild]),
            claim_show_update_period=mock.Mock(return_value="claim-token"),
            get_active_show_state_override=mock.Mock(return_value=None),
            get_bnl_control_flags=mock.Mock(
                return_value={
                    "showdayDiscordPostsEnabled": True,
                    "websiteRelayEnabled": True,
                }
            ),
            BNL_WEBSITE_CONTRACT_VERSION="1",
            BNL_API_KEY="configured",
            BNL_STATUS_URL="https://example.test",
            get_guild_config=mock.Mock(return_value=99),
            get_recent_ambient=mock.Mock(return_value=[]),
            get_showday_discord_post_count=mock.Mock(return_value=0),
            had_recent_showday_discord_post=mock.Mock(return_value=False),
            generate_showday_messages=generate,
            maintain_show_update_claim=mock.AsyncMock(),
            revalidate_show_update_claim=revalidate,
            _ambient_post_lock_for=mock.Mock(return_value=DeliveryLock()),
            ambient_capacity_decision=mock.Mock(
                return_value={"allowed": True, "capacityUsed": 0, "cap": 2}
            ),
            log_ambient=log_ambient,
            commit_show_update_publication_fence=fence,
            update_website_status_controlled_async=website_delivery,
            mark_show_update_fired=mark,
        ):
            await bnl01_bot.barcode_radio_queue_task.coro()

        channel.send.assert_awaited_once()
        self.assertEqual(revalidate.await_count, 2)
        fence.assert_awaited_once()
        self.assertEqual(
            fence.await_args.args[:6],
            (
                42,
                "2026-08-21",
                "submissions_open",
                "claim-token",
                "discord copy",
                "website copy",
            ),
        )
        self.assertTrue(hasattr(fence.await_args.args[6], "is_set"))
        website_delivery.assert_awaited_once()
        log_ambient.assert_called_once()
        mark.assert_not_called()

    async def test_showday_website_only_is_fenced_before_publish(self):
        at = bnl01_bot.PACIFIC_TZ.localize(datetime(2026, 8, 21, 18, 40))
        datetime_mock = mock.Mock(wraps=datetime)
        datetime_mock.now.return_value = at
        website_delivery = mock.AsyncMock()

        async def commit_fence(*_args):
            website_delivery.assert_not_awaited()
            return True

        fence = mock.AsyncMock(side_effect=commit_fence)
        revalidate = mock.AsyncMock(return_value=True)
        guild = SimpleNamespace(id=42, get_channel=lambda _channel_id: None)
        mark = mock.Mock()
        with mock.patch.multiple(
            bnl01_bot,
            datetime=datetime_mock,
            FRIDAY_SHOW_PHASES=[
                {
                    "key": "submissions_open",
                    "hour": 18,
                    "minute": 40,
                    "window_min": 5,
                }
            ],
            iter_managed_guilds=mock.Mock(return_value=[guild]),
            claim_show_update_period=mock.Mock(return_value="claim-token"),
            get_active_show_state_override=mock.Mock(return_value=None),
            get_bnl_control_flags=mock.Mock(
                return_value={
                    "showdayDiscordPostsEnabled": False,
                    "websiteRelayEnabled": True,
                }
            ),
            BNL_WEBSITE_CONTRACT_VERSION="1",
            BNL_API_KEY="configured",
            BNL_STATUS_URL="https://example.test",
            get_guild_config=mock.Mock(return_value=None),
            get_recent_ambient=mock.Mock(return_value=[]),
            get_showday_discord_post_count=mock.Mock(return_value=0),
            had_recent_showday_discord_post=mock.Mock(return_value=False),
            generate_showday_messages=mock.AsyncMock(
                return_value=("discord copy", "website copy")
            ),
            maintain_show_update_claim=mock.AsyncMock(),
            revalidate_show_update_claim=revalidate,
            commit_show_update_publication_fence=fence,
            update_website_status_controlled_async=website_delivery,
            mark_show_update_fired=mark,
        ):
            await bnl01_bot.barcode_radio_queue_task.coro()

        self.assertEqual(revalidate.await_count, 2)
        fence.assert_awaited_once()
        self.assertEqual(
            fence.await_args.args[:6],
            (
                42,
                "2026-08-21",
                "submissions_open",
                "claim-token",
                "",
                "website copy",
            ),
        )
        website_delivery.assert_awaited_once()
        mark.assert_not_called()

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
        self.assertTrue(
            bnl01_bot.policy_for_route(
                seen[0]["route"]
            ).showday_protected
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
