"""Recovery keeps the active generator and independently valid source evidence."""
import os
import unittest
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


class ResponseLifecycleRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_recovery_preserves_effective_generator_without_optional_style_calls(self):
        for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
            with self.subTest(route=route):
                result = bot.TrackedGenerationResponse(
                    text="The Journal covered the queue's first public test.",
                    provider_call_count=2,
                )
                with mock.patch.object(
                    bot, "get_tracked_gemini_response_with_optional_typing",
                    new=mock.AsyncMock(return_value=result),
                ) as generate:
                    recovered = await bot.resolve_guarded_response_obligation(
                        "", baseline_response="", prompt="Current user request: What did the Journal say?",
                        current_user_text="What did the Journal say?",
                        diagnostics={"suppressed": True, "suppression_reason": "candidate_rejected",
                                     "response_review_requires_rewrite": True},
                        route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="public_home",
                        user_id=101, guild_id=1, channel=None, source_context_available=True,
                        generation_route=route,
                    )
                self.assertEqual(recovered[0], result.text)
                self.assertEqual(recovered[3], 2)
                self.assertEqual(generate.await_args.kwargs["route"], route)
                self.assertFalse(generate.await_args.kwargs["allow_style_rewrite"])

    async def test_corrective_normal_generation_skips_both_optional_rewrites(self):
        async def provider(contents, route, *, attempt_counter=None):
            if attempt_counter is not None:
                attempt_counter.mark_started()
            return bot.GenerationResult(True, "The queue is closed.", route=route)

        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_result_async", side_effect=provider) as generate, \
             mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as optional, \
             mock.patch.object(bot.random, "random", return_value=0.0):
            result = await bot.get_tracked_gemini_response_with_optional_typing(
                None, "Current user request: Is the queue open?", 101, 1,
                route="get_gemini_response", source_context_available=True,
                allow_style_rewrite=False,
            )
        self.assertEqual(result.text, "The queue is closed.")
        self.assertEqual(result.provider_call_count, 1)
        generate.assert_awaited_once()
        optional.assert_not_awaited()

    async def test_nested_grounding_repair_counts_both_physical_calls(self):
        texts = iter((
            "Archival records indicate this is about his weekly broadcast deployments.",
            "Not everybody knows how to do everything. The room can respect that.",
        ))
        async def provider(contents, route, *, attempt_counter=None):
            if attempt_counter is not None:
                attempt_counter.mark_started()
            return SimpleNamespace(
                candidates=[SimpleNamespace(content=SimpleNamespace(
                    parts=[SimpleNamespace(text=next(texts))]))],
                usage_metadata=SimpleNamespace(total_token_count=7),
            )
        prompt = (
            "Current channel policy: sealed_test\n"
            "Current user request: [Current message media context:\n"
            "- gif embed (title=not everybody knows how to do everything)\n]"
        )
        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", side_effect=provider):
            result = await bot.get_tracked_gemini_response_with_optional_typing(
                None, prompt, 101, 1, route="free_speak_media_generation",
                allow_style_rewrite=False,
            )
        self.assertEqual(result.provider_call_count, 2)
        self.assertEqual(result.text, "Not everybody knows how to do everything. The room can respect that.")

    def test_stale_packet_source_never_reuses_the_rejected_baseline(self):
        diagnostics = {"suppressed": True, "suppression_reason": "shared_brain_synthesis_source_changed"}
        result = bot.recover_guarded_response_obligation(
            "", baseline_response="The queue is open.",
            prompt="Current user request: Is the queue open?",
            current_user_text="Is the queue open?", diagnostics=diagnostics,
            route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="public_home",
            source_context_available=True,
        )
        self.assertEqual(result, "")
        self.assertTrue(diagnostics["suppressed"])
        self.assertEqual(diagnostics["response_obligation_recovery_kind"], "model_rewrite_required")

    def test_source_change_preserves_independently_valid_publication_and_reverse(self):
        journal = SimpleNamespace(rendered_context="Published Journal: The queue's first public test.")
        queue = SimpleNamespace(rendered_context="Current queue snapshot: open.")
        request = "What did the Journal say, and is the queue open now?"
        prompt = "Current user request: " + request + "\n" + journal.rendered_context + "\n" + queue.rendered_context
        for invalid, valid in ((queue, journal), (journal, queue)):
            with self.subTest(invalid=invalid.rendered_context), mock.patch.object(
                bot, "refresh_prompt_source_basis", side_effect=lambda b: (b, b is invalid),
            ):
                rewritten, bases, neutral = bot.build_ordinary_chat_response_repair_prompt(
                    prompt, reason="source_revalidation_snapshot_changed",
                    prompt_source_bases=(journal, queue), current_user_text=request,
                )
            self.assertEqual(bases, (valid,))
            self.assertFalse(neutral)
            self.assertIn(valid.rendered_context, rewritten)
            self.assertNotIn(invalid.rendered_context, rewritten)
            self.assertIn(request, rewritten)

    def test_revalidation_failure_drops_affected_private_memory_only(self):
        memory = SimpleNamespace(rendered_context="Retired member fact that must no longer be used.")
        journal = SimpleNamespace(rendered_context="Public Journal: Community artists shared new tracks.")
        def refresh(basis):
            if basis is memory:
                raise RuntimeError("unavailable source")
            return basis, False
        with mock.patch.object(bot, "refresh_prompt_source_basis", side_effect=refresh):
            prompt, bases, neutral = bot.build_ordinary_chat_response_repair_prompt(
                "Current user request: What have you learned?", reason="memory_source_changed",
                prompt_source_bases=(memory, journal),
            )
        self.assertEqual(bases, (journal,))
        self.assertFalse(neutral)
        self.assertNotIn(memory.rendered_context, prompt)
        self.assertIn(journal.rendered_context, prompt)

    async def test_mixed_journal_clock_and_show_context_pass_without_show_only_veto(self):
        prompt = """Current user request: What did the Journal say, and what opened the show?
Published Journal: New entries release at 7:00 PM; Cliff discussed the queue's first test.
Durable BARCODE Radio show episode memory:
- t+1.2m [track play started] Neon Fox — First Signal
Finalized BARCODE Radio episode priority:
- Use this for finalized-show claims only.
"""
        answer = "The Journal releases at 7:00 PM and covered Cliff's queue test. First Signal opened the show."
        with mock.patch.object(bot, "refresh_community_visual_basis_before_send", return_value=(None, False)), \
             mock.patch.object(bot, "get_gemini_response", new=mock.AsyncMock()) as generate, \
             mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=mock.AsyncMock()) as typed:
            result, diagnostics = await bot.apply_guarded_response_regeneration(
                answer, prompt=prompt, current_user_text="What did the Journal say, and what opened the show?",
                user_id=101, guild_id=1, route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_home", source_context_available=True,
            )
        self.assertEqual(result, answer)
        self.assertFalse(diagnostics.get("suppressed"))
        generate.assert_not_awaited()
        typed.assert_not_awaited()
