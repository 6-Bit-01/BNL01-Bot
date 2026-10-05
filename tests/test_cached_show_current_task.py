"""Real ingress/cache/coordinator contract with isolated source/transport inputs."""

from __future__ import annotations

import os
import sys
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
import test_conversation_batching as fixtures
from bnl_conversation_context_v2 import (
    ConversationContextRequest,
    assemble_conversation_context_v2,
)
from bnl_unified_response_assessment import build_unified_response_assessment


class CachedShowCurrentTaskTests(unittest.IsolatedAsyncioTestCase):
    # Reuse ingress isolation only; do not inherit unrelated fixture tests or
    # create a database. The actual cache, addressing, plan and coordinator run.
    _on_message_runtime = fixtures.ConversationBatchCoordinatorTests._on_message_runtime

    async def asyncSetUp(self):
        self.cache = {}
        self.cache_patch = mock.patch.object(bot, "_show_state_topic_context", self.cache)
        self.cache_patch.start()
        self.addCleanup(self.cache_patch.stop)
        self.channel_ids = set()

    async def asyncTearDown(self):
        for state in (bot._direct_conversation_ingress_revision,
                      bot._inflight_direct_repair_generations):
            for key in list(state):
                if len(key) >= 2 and key[1] in self.channel_ids:
                    state.pop(key, None)

    def seed_cache(self, channel, *, expired=False):
        key = bot._show_state_topic_key(channel.guild.id, channel.id)
        self.cache[key] = {
            "target_show_date": "2026-10-02",
            "cleaned_summary": "The copper orchard broadcast was postponed.",
            "created_at": datetime.now(timezone.utc) - timedelta(
                seconds=bot.SHOW_STATE_TOPIC_TTL_SECONDS + 1 if expired else 1),
            "last_user_id": 404,
            "last_bot_answer_type": "show_state",
        }
        return key

    async def run_ingress(self, text, mode, *, reply=False, expired=False,
                          generated_text="Fixture answer to the current task."):
        channel = fixtures.FakeChannel(8970 + len(self.channel_ids), name="home")
        self.channel_ids.add(channel.id)
        key = self.seed_cache(channel, expired=expired)
        fake_bot = SimpleNamespace(id=999, display_name="BNL-01", bot=True)
        message = fixtures.FakeMessage(channel, "<@999> " + text, mentions=[fake_bot])
        if reply:
            message.reference = SimpleNamespace(message_id=1001, resolved=SimpleNamespace(
                id=1001, author=fake_bot, channel=channel,
                content="The copper orchard broadcast was postponed.",
            ))
        active_id = {"active": channel.id, "other": channel.id + 100, "unset": None}[mode]
        policy = "public_home" if mode == "active" else "public_context"
        built = []
        cache_sites = []
        generation = mock.AsyncMock(return_value=generated_text)
        send = mock.AsyncMock()

        async def room_context(*args, **kwargs):
            rows = [dict(id=11, guild_id=channel.guild.id, role="model", user_id=100,
                         user_name="BNL-01", message_id=1001, channel_id=channel.id,
                         channel_name=channel.name, channel_policy=policy,
                         route_mode="normal_chat", timestamp=datetime.now(timezone.utc).isoformat(),
                         content="The copper orchard broadcast was postponed.")]
            result = assemble_conversation_context_v2(rows, ConversationContextRequest(
                guild_id=channel.guild.id, current_user_id=message.author.id,
                channel_id=channel.id, channel_name=channel.name, channel_policy=policy,
                route_mode=kwargs["route_mode"], conversation_surface=kwargs["conversation_surface"],
                current_texts=(kwargs["current_text"],),
                current_participants=frozenset({message.author.id}),
                referenced_message_ids=frozenset(kwargs.get("referenced_message_ids") or ()),
                is_direct_target=True, is_reply_to_bnl=reply, now=datetime.now(timezone.utc),
                route_allowed_sources=frozenset({"conversation_continuity"}),
            ))
            kwargs["context_result_out"]["result"] = result
            return result.rendered_context

        async def prompt_input(user_id, guild_id, name, current_text, **kwargs):
            decision = kwargs["conversation_orchestration"]
            assessment = build_unified_response_assessment(
                guild_id=guild_id, route_mode=kwargs["route_mode"], channel_policy=policy,
                conversation_surface=decision.situation_frame.conversation_surface,
                current_speaker_user_ids=(user_id,), current_text=current_text,
                continuity_required=decision.continuity_required,
                show_state_present=bool(kwargs["show_state_context"]),
                situation_frame=decision.situation_frame,
            )
            built.append((current_text, kwargs, decision, assessment))
            kwargs["prompt_metadata"]["unified_response_assessment_shadow"] = assessment
            return ("Current user request: " + current_text + "\n"
                    + kwargs["show_state_context"] + "\n" + kwargs["room_context"], False, "balanced")

        def observe_cache_call(frame, event, _arg):
            if event == "call" and frame.f_code is bot._get_recent_show_state_topic_context.__code__:
                cache_sites.append(frame.f_back.f_lineno)

        with self._on_message_runtime(channel.id, followup_candidate=reply), ExitStack() as stack:
            for patcher in (
                mock.patch.object(bot, "get_guild_config", return_value=active_id),
                mock.patch.object(bot, "resolve_channel_policy", return_value=policy),
                mock.patch.object(bot, "BNL_ACTIVE_BATCHING_ENABLED", False),
                mock.patch.object(bot, "conversation_orchestration_influence_mode", return_value="live"),
                mock.patch.object(bot, "_load_bnl_self_name_records", return_value=()),
                mock.patch.object(bot, "_conversation_row_for_discord_message", side_effect=lambda **kw:
                                  (11, "model", "BNL-01") if kw["message_id"] == 1001 else (0, "", "")),
                mock.patch.object(bot, "get_sealed_test_recall_guard_response", return_value=None),
                mock.patch.object(bot, "get_restricted_channel_recall_guard_response", return_value=None),
                mock.patch.object(bot, "try_self_reflection_response", return_value=None),
                mock.patch.object(bot, "resolve_recent_media_followup", return_value=None),
                mock.patch.object(bot, "try_memory_recall_response", return_value=None),
                mock.patch.object(bot, "get_active_show_state_override", return_value=(
                    1, "fixture", "The current show is postponed.", "", "", "2026-10-09", "")),
                mock.patch.object(bot, "build_room_first_direct_context_async", side_effect=room_context),
                mock.patch.object(bot, "_recent_moment_situation_for_turn_async", new=mock.AsyncMock(return_value=None)),
                mock.patch.object(bot, "conversation_supporting_owner_states", new=mock.AsyncMock(return_value={})),
                mock.patch.object(bot, "maybe_build_bnl_read_model_context", return_value=""),
                mock.patch.object(bot, "maybe_build_source_context_for_direct_message", new=mock.AsyncMock(return_value="")),
                mock.patch.object(bot, "resolve_live_exact_quote_authority", new=mock.AsyncMock(return_value=None)),
                mock.patch.object(bot, "build_user_aware_prompt_async", side_effect=prompt_input),
                mock.patch.object(bot, "maybe_generate_ordinary_chat_single_packet", new=mock.AsyncMock(return_value=None)),
                mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=generation),
                mock.patch.object(bot, "suppress_stale_media_fallback", side_effect=lambda response, **kw: response),
                mock.patch.object(bot, "send_planned_conversation_response", new=send),
                mock.patch.object(bot, "is_privileged_member", return_value=False),
                mock.patch.object(bot, "maybe_update_broadcast_status_from_text"),
                mock.patch.object(bot, "maybe_update_restricted_status_from_text"),
                mock.patch.object(bot, "log_response_style"),
            ):
                stack.enter_context(patcher)
            previous_profile = sys.getprofile()
            sys.setprofile(observe_cache_call)
            try:
                await bot.on_message(message)
            finally:
                sys.setprofile(previous_profile)
        self.assertEqual(len(built), 1)
        generation.assert_awaited_once()
        expected_route = (
            "show_state_direct" if built[0][1]["route_mode"] == bot.ROUTE_MODE_SHOW_STATUS
            else "get_gemini_response"
        )
        self.assertEqual(generation.await_args.kwargs["route"], expected_route)
        if generated_text:
            send.assert_awaited_once()
            self.assertEqual(send.await_args.args[1], generated_text)
        else:
            send.assert_not_awaited()
        return built[0], send.await_args, cache_sites, key

    async def test_cached_show_does_not_promote_any_of_the_three_ingresses(self):
        observed_sites = set()
        for mode in ("active", "other", "unset"):
            for text in ("What's up?", "What do you want to be when you grow up?", "I meant red panda."):
                with self.subTest(mode=mode, text=text):
                    (current_text, kwargs, decision, assessment), sent, sites, _key = await self.run_ingress(text, mode)
                    self.assertEqual(current_text, text)
                    self.assertEqual(kwargs["route_mode"], bot.ROUTE_MODE_NORMAL_CHAT)
                    self.assertEqual(sent.args[2].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
                    self.assertEqual(decision.response_act, "answer")
                    self.assertEqual(decision.situation_frame.route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
                    self.assertEqual(assessment.response_act, "answer_current_turn")
                    self.assertIn("copper orchard", kwargs["show_state_context"])
                    self.assertIsNone(sent.kwargs["show_state_context_on_commit"])
                    self.assertEqual(len(sites), 1)
                    observed_sites.update(sites)
        self.assertEqual(len(observed_sites), 3)

    async def test_true_followup_keeps_context_and_existing_continuity_owner(self):
        for mode in ("active", "other", "unset"):
            with self.subTest(mode=mode):
                (_text, kwargs, decision, assessment), sent, _sites, _key = await self.run_ingress("Why?", mode, reply=True)
                self.assertEqual(kwargs["route_mode"], bot.ROUTE_MODE_NORMAL_CHAT)
                self.assertEqual(sent.args[2].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
                self.assertTrue(decision.continuity_required)
                self.assertEqual(decision.referent_status, "resolved")
                self.assertEqual(assessment.response_act, "continue_active_thread")
                self.assertIn("copper orchard", kwargs["show_state_context"])
                self.assertIsNone(sent.kwargs["show_state_context_on_commit"])

    async def test_explicit_show_request_uses_fresh_override_and_special_route(self):
        for mode in ("active", "other", "unset"):
            with self.subTest(mode=mode):
                (_text, kwargs, decision, assessment), sent, sites, _key = await self.run_ingress("Is Friday's show cancelled?", mode)
                self.assertEqual(kwargs["route_mode"], bot.ROUTE_MODE_SHOW_STATUS)
                self.assertEqual(sent.args[2].route_mode, bot.ROUTE_MODE_SHOW_STATUS)
                self.assertEqual(decision.situation_frame.route_mode, bot.ROUTE_MODE_SHOW_STATUS)
                self.assertEqual(assessment.response_act, "answer_show_status")
                self.assertIn("2026-10-09", kwargs["show_state_context"])
                self.assertEqual(sent.kwargs["show_state_context_on_commit"]["target_show_date"],
                                 "2026-10-09")
                self.assertNotIn("copper orchard", kwargs["show_state_context"])
                self.assertEqual(sites, [])

    async def test_expired_cache_is_removed_without_changing_the_new_task(self):
        (_text, kwargs, _decision, assessment), sent, sites, key = await self.run_ingress(
            "What do you want to be when you grow up?", "active", expired=True)
        self.assertNotIn(key, self.cache)
        self.assertEqual(kwargs["show_state_context"], "")
        self.assertEqual(assessment.response_act, "answer_current_turn")
        self.assertEqual(sent.args[2].route_mode, bot.ROUTE_MODE_NORMAL_CHAT)
        self.assertEqual(len(sites), 1)

    async def test_cached_evidence_does_not_fabricate_a_show_answer_after_generation_failure(self):
        for mode in ("active", "other", "unset"):
            with self.subTest(mode=mode):
                (_text, kwargs, _decision, assessment), sent, sites, _key = await self.run_ingress(
                    "What's up?", mode, generated_text="")
                self.assertEqual(kwargs["route_mode"], bot.ROUTE_MODE_NORMAL_CHAT)
                self.assertEqual(assessment.response_act, "answer_current_turn")
                self.assertIsNone(sent)
                self.assertEqual(len(sites), 1)

    def test_disallowed_route_empty_text_and_wrong_cache_type_supply_no_context(self):
        channel = fixtures.FakeChannel(8969)
        key = self.seed_cache(channel)
        self.assertEqual(bot._get_recent_show_state_topic_context(
            channel.guild.id, channel.id, 101, False, "What's up?"), {})
        self.assertEqual(bot._get_recent_show_state_topic_context(
            channel.guild.id, channel.id, 101, True, ""), {})
        self.cache[key]["last_bot_answer_type"] = "conversation"
        self.assertEqual(bot._get_recent_show_state_topic_context(
            channel.guild.id, channel.id, 101, True, "Why?"), {})


if __name__ == "__main__":
    unittest.main()
