"""A failed packet call recovers an answer without extra decoration calls.

The direct and batched delivery fixtures exercise their real generation owners.
Packet selection/receipt adapters, provider transport and Discord are isolated;
the generation result, attempt accounting and fallback dispatch are real.
"""

import os
import unittest
from contextlib import ExitStack, contextmanager
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import test_conversation_batching as batch_fixtures


REAL_GENERATE = bot.get_gemini_response
REAL_PACKET = bot.maybe_generate_ordinary_chat_single_packet
ANSWER = "A checksum detects a mismatch but does not contain the original file."
REQUEST = "Briefly explain why a checksum detects corruption but cannot repair it."


def provider_response(text):
    return SimpleNamespace(
        candidates=[SimpleNamespace(content=SimpleNamespace(
            parts=[SimpleNamespace(text=text)]
        ))],
        usage_metadata=SimpleNamespace(total_token_count=12),
    )


class PacketRecoveryStyleTests(unittest.IsolatedAsyncioTestCase):
    # Reuse only the isolated runtime helpers, not the other fixture's tests.
    asyncSetUp = batch_fixtures.ConversationBatchCoordinatorTests.asyncSetUp
    asyncTearDown = batch_fixtures.ConversationBatchCoordinatorTests.asyncTearDown
    _channel = batch_fixtures.ConversationBatchCoordinatorTests._channel
    _prime_flush = batch_fixtures.ConversationBatchCoordinatorTests._prime_flush
    _flush_runtime = batch_fixtures.ConversationBatchCoordinatorTests._flush_runtime
    _on_message_runtime = batch_fixtures.ConversationBatchCoordinatorTests._on_message_runtime

    @contextmanager
    def _generation_runtime(self, *, packet_mode="failed"):
        self.provider_routes = []
        self.packet_metadata = []
        basis = SimpleNamespace(packet=SimpleNamespace(
            source_snapshot_digest="test-source-digest"
        ))
        run = SimpleNamespace(prompt_applied=packet_mode != "preflight", fallback_reason="")
        decision = SimpleNamespace(candidate_selected=False, fallback_reason="generation_failed")

        async def provider(contents, route, *, attempt_counter=None):
            self.provider_routes.append(route)
            if attempt_counter is not None:
                attempt_counter.mark_started()
            if route == bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE:
                raise RuntimeError("503 Service Unavailable")
            return provider_response(ANSWER)

        async def packet(**kwargs):
            kwargs.update(scope_applied=packet_mode != "disabled", basis=basis)
            if packet_mode == "unavailable":
                kwargs["basis"] = None
            kwargs["preflight_reason"] = ""
            result = await REAL_PACKET(**kwargs)
            self.packet_metadata.append(dict(kwargs["generation_metadata_out"]))
            return result

        with ExitStack() as stack:
            patches = (
                mock.patch.object(
                    bot, "check_quota_availability",
                    side_effect=lambda route: not (
                        packet_mode == "budget"
                        and route == bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE
                    ),
                ),
                mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True),
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", side_effect=provider),
                mock.patch.object(bot, "maybe_generate_ordinary_chat_single_packet", side_effect=packet),
                mock.patch.object(bot, "build_packet_owned_prompt", return_value=SimpleNamespace(
                    ready=True, prompt="Answer the checksum question.", reason=""
                )),
                mock.patch.object(bot, "_begin_ordinary_chat_single_packet_receipt", return_value=run),
                mock.patch.object(bot, "_evaluate_ordinary_chat_single_packet_receipt", return_value=decision),
                mock.patch.object(bot.random, "random", return_value=0.0),
                mock.patch.object(bot, "CROSS_UNIVERSE_BLEED_CHANCE", 0.05),
            )
            for patcher in patches:
                stack.enter_context(patcher)
            yield

    def _assert_failed_packet_recovery(self, normal_route):
        self.assertEqual(self.provider_routes, [bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE, normal_route])
        self.assertEqual(len(self.packet_metadata), 1)
        metadata = self.packet_metadata[0]
        self.assertEqual(metadata["provider_call_count"], 1)
        self.assertGreaterEqual(metadata["generation_latency_ms"], 0)
        self.assertTrue(metadata["error_category"])

    async def test_batch_recovers_after_failed_packet_without_either_optional_rewrite(self):
        channel = self._channel(8950)
        self._prime_flush(channel, REQUEST)
        with self._flush_runtime(channel.id, REAL_GENERATE):
            with self._generation_runtime():
                await bot._flush_channel_buffer(channel)
        self.assertEqual(channel.sent, [ANSWER])
        self._assert_failed_packet_recovery("get_gemini_response")

    async def _assert_direct_recovery(self, active_mode):
        channel = self._channel(8951 + len(self.channel_ids))
        message = batch_fixtures.FakeMessage(channel, REQUEST)
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
                    mock.patch.object(bot, "BNL_ACTIVE_BATCHING_ENABLED", False),
                    mock.patch.object(bot, "get_sealed_test_recall_guard_response", return_value=None),
                    mock.patch.object(bot, "get_restricted_channel_recall_guard_response", return_value=None),
                    mock.patch.object(bot, "try_self_reflection_response", return_value=None),
                    mock.patch.object(bot, "resolve_recent_media_followup", return_value=None),
                    mock.patch.object(bot, "try_memory_recall_response", return_value=None),
                    mock.patch.object(bot, "build_show_state_override_context", return_value={}),
                    mock.patch.object(bot, "_get_recent_show_state_topic_context", return_value={}),
                    mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
                    mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
                    mock.patch.object(bot, "build_broadcast_memory_context", return_value=""),
                    mock.patch.object(bot, "build_tiktok_show_evidence_context_for_turn", return_value=""),
                    mock.patch.object(bot, "should_allow_greeting", return_value=False),
                    mock.patch.object(bot, "choose_response_style", return_value=("balanced", "Answer naturally.")),
                    mock.patch.object(bot, "log_response_style"),
                    mock.patch.object(bot, "is_privileged_member", return_value=False),
                    mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()),
                    mock.patch.object(bot, "save_model_message"),
                ):
                    stack.enter_context(patcher)
                with self._generation_runtime():
                    await bot.on_message(message)
        self.assertEqual(message.replies + channel.sent, [ANSWER])
        self._assert_failed_packet_recovery("get_gemini_response")

    async def test_all_direct_callers_recover_without_either_optional_rewrite(self):
        for active_mode in ("active", "other", "unset"):
            with self.subTest(active_mode=active_mode):
                await self._assert_direct_recovery(active_mode)

    async def test_unused_packet_keeps_healthy_normal_style(self):
        for mode in ("disabled", "unavailable", "preflight", "budget"):
            with self.subTest(mode=mode):
                channel = self._channel(8952 + len(self.channel_ids))
                self._prime_flush(channel, REQUEST)
                with self._flush_runtime(channel.id, REAL_GENERATE):
                    with self._generation_runtime(packet_mode=mode):
                        await bot._flush_channel_buffer(channel)
                self.assertEqual(channel.sent, [ANSWER])
                self.assertEqual(self.provider_routes, ["get_gemini_response", "glitch_rewrite", "cross_universe_bleed"])
                if mode == "budget":
                    self.assertEqual(self.packet_metadata[0]["provider_call_count"], 0)
                    self.assertEqual(self.packet_metadata[0]["error_category"], bot.GENERATION_ERROR_LOCAL_MODEL_BUDGET)
                else:
                    self.assertEqual(self.packet_metadata, [{}])


if __name__ == "__main__":
    unittest.main()
