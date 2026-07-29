import os
from contextlib import ExitStack
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class FakeChannel:
    def __init__(self):
        self.id = 10
        self.name = "bnl-testing"
        self.sent = []

    async def send(self, content, **_kwargs):
        self.sent.append(content)


class FakeMessage:
    def __init__(self):
        self.author = SimpleNamespace(id=7, display_name="Crow")
        self.guild = SimpleNamespace(id=1)
        self.channel = FakeChannel()
        self.content = "BNL-01, what am I all about?"
        self.replies = []

    async def reply(self, content, **_kwargs):
        self.replies.append(content)


class SharedBrainSynthesisBotPathTests(
    unittest.IsolatedAsyncioTestCase
):
    def synthesis_basis(self, legacy_context):
        return bnl01_bot.SharedBrainSynthesisBasis(
            packet=SimpleNamespace(),
            assessment=SimpleNamespace(),
            rendered_context=(
                "Grounded response evidence:\n"
                "[E1 | approved direct fact] favorite movie: Arrival"
            ),
            expected_packet_digest="packet-digest",
            expected_context_digest="context-digest",
            guild_id=1,
            user_id=7,
            channel_id=10,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="public_context",
            rendered_item_count=1,
            rendered_lane_counts=(("approved_fact", 1),),
            rendered_source_digests=("source-digest",),
            competing_factual_contexts=(legacy_context,),
            competing_factual_context_digests=("legacy-digest",),
            profile_sufficiency_status="sparse",
            profile_required_point_count=1,
        )

    def memory_source_basis(self, legacy_context):
        return bnl01_bot.MemoryPromptSourceBasis(
            expected_digest="legacy-digest",
            rendered_context=legacy_context,
            user_id=7,
            guild_id=1,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            channel_policy="public_context",
            user_text="BNL-01, what am I all about?",
            is_owner_or_mod=False,
            current_direct=True,
            governance_allowed=False,
            channel_id=10,
            moment_attribution_target_user_id=0,
        )

    def plan(self):
        return bnl01_bot.plan_conversation_response(
            "BNL-01, what am I all about?",
            "public_context",
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            real_direct_target=True,
            batching_enabled=False,
            conversation_surface=(
                bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY
            ),
        )

    def execution(self, *, candidate_active=True):
        decision = SimpleNamespace(
            run=SimpleNamespace(run_id="run-1"),
            candidate_selected=candidate_active,
            fallback_reason="",
        )
        return bnl01_bot.SharedBrainSynthesisExecution(
            decision=decision,
            response=(
                "Packet-grounded candidate."
                if candidate_active
                else "Established baseline."
            ),
            prompt=(
                "baseline prompt\n\nprivate evidence"
                if candidate_active
                else "baseline prompt"
            ),
            prompt_source_bases=(
                ("existing-basis", "synthesis-basis")
                if candidate_active
                else ("existing-basis",)
            ),
            candidate_active=candidate_active,
        )

    def common_patches(self):
        return (
            mock.patch.object(
                bnl01_bot,
                "_apply_direct_response_pacing",
                new=mock.AsyncMock(),
            ),
            mock.patch.object(
                bnl01_bot,
                "build_message_media_context",
                return_value={"present": False, "included": False, "count": 0},
            ),
            mock.patch.object(bnl01_bot, "update_last_route_debug"),
            mock.patch.object(
                bnl01_bot,
                "exact_quote_presend_failure",
                new=mock.AsyncMock(return_value=""),
            ),
            mock.patch.object(
                bnl01_bot,
                "prompt_source_basis_failure",
                return_value="",
            ),
            mock.patch.object(
                bnl01_bot,
                "_mark_conversation_continuation_state",
            ),
            mock.patch.object(
                bnl01_bot,
                "record_unified_response_assessment_shadow_after_send",
                new=mock.AsyncMock(return_value=""),
            ),
            mock.patch.object(
                bnl01_bot,
                "is_generic_non_answer_response",
                return_value=False,
            ),
        )

    async def test_candidate_call_uses_packet_as_only_factual_owner(self):
        legacy_context = (
            "Relationship state: stage=familiar, stance=warm.\n"
            "Observed habits: messages=100, last_topic=music.\n"
            "Derived memory summaries: one legacy summary."
        )
        prompt = (
            "Current user request: BNL-01, what am I all about?\n"
            "BNL persona and BARCODE lore remain active.\n"
            "Recent room context: modular synths.\n"
            "Durable memory context:\n"
            + legacy_context
            + "\nPersonal-recall route contract remains active."
        )
        basis = self.synthesis_basis(legacy_context)
        memory_basis = self.memory_source_basis(legacy_context)
        conversation_basis = SimpleNamespace(kind="conversation")
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="unchanged",
        )
        decision = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        generation = mock.AsyncMock(
            return_value="Your favorite movie is Arrival."
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "_begin_shared_brain_synthesis_receipt",
                return_value=run,
            ) as begin,
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                new=generation,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_shared_brain_synthesis_receipt",
                return_value=decision,
            ),
            mock.patch.object(
                bnl01_bot,
                "is_generic_non_answer_response",
                return_value=False,
            ),
        ):
            execution = (
                await bnl01_bot
                .maybe_generate_shared_brain_synthesis_canary(
                    channel=FakeChannel(),
                    baseline_response="Established baseline.",
                    prompt=prompt,
                    prompt_source_bases=(
                        conversation_basis,
                        memory_basis,
                    ),
                    basis=basis,
                    user_id=7,
                    guild_id=1,
                    user_display_name="Crow",
                    source_context_available=True,
                )
            )

        self.assertIsNotNone(execution)
        self.assertTrue(execution.candidate_active)
        candidate_prompt = generation.await_args.args[1]
        self.assertNotIn("Relationship state:", candidate_prompt)
        self.assertNotIn("Observed habits:", candidate_prompt)
        self.assertNotIn("Derived memory summaries:", candidate_prompt)
        self.assertIn("BNL persona and BARCODE lore", candidate_prompt)
        self.assertIn("Recent room context:", candidate_prompt)
        self.assertIn(
            "Personal-recall route contract",
            candidate_prompt,
        )
        self.assertEqual(
            candidate_prompt.count(basis.rendered_context),
            1,
        )
        self.assertEqual(
            execution.prompt_source_bases,
            (conversation_basis, basis),
        )
        self.assertIn(legacy_context, prompt)
        self.assertTrue(
            begin.call_args.kwargs["candidate_prompt_ready"]
        )
        self.assertEqual(
            begin.call_args.kwargs[
                "replaced_factual_context_count"
            ],
            1,
        )

    async def test_category_only_candidate_gets_one_grounded_repair(self):
        legacy_context = "Legacy factual profile block."
        basis = replace(
            self.synthesis_basis(legacy_context),
            profile_sufficiency_status="rich",
            profile_required_point_count=2,
            profile_required_detail_count=2,
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="unchanged",
        )
        rejected = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="candidate_member_details_insufficient",
        )
        accepted = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        generation = mock.AsyncMock(
            side_effect=(
                "Software and music are recurring themes.",
                (
                    "You keep fixing the bot code and memory system, "
                    "while composing synth songs and refining their mixes."
                ),
            )
        )
        evaluator = mock.Mock(side_effect=(rejected, accepted))
        prompt = (
            "Current user request: BNL-01, what am I all about?\n"
            "Durable memory context:\n"
            + legacy_context
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "_begin_shared_brain_synthesis_receipt",
                return_value=run,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                new=generation,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_shared_brain_synthesis_receipt",
                new=evaluator,
            ),
            mock.patch.object(
                bnl01_bot,
                "is_generic_non_answer_response",
                return_value=False,
            ),
        ):
            execution = (
                await bnl01_bot
                .maybe_generate_shared_brain_synthesis_canary(
                    channel=FakeChannel(),
                    baseline_response="Established baseline.",
                    prompt=prompt,
                    prompt_source_bases=(
                        self.memory_source_basis(legacy_context),
                    ),
                    basis=basis,
                    user_id=7,
                    guild_id=1,
                    user_display_name="Crow",
                    source_context_available=True,
                )
            )

        self.assertTrue(execution.candidate_active)
        self.assertIn("bot code", execution.response)
        self.assertEqual(generation.await_count, 2)
        self.assertEqual(
            generation.await_args_list[0].kwargs["route"],
            "shared_brain_synthesis_canary",
        )
        self.assertEqual(
            generation.await_args_list[1].kwargs["route"],
            "shared_brain_synthesis_canary_repair",
        )
        self.assertIn(
            "Grounded rewrite requirements",
            generation.await_args_list[1].args[1],
        )
        self.assertEqual(evaluator.call_count, 2)

    async def test_ungrounded_repair_gets_one_final_cleanup(self):
        legacy_context = "Legacy factual profile block."
        basis = replace(
            self.synthesis_basis(legacy_context),
            profile_sufficiency_status="rich",
            profile_required_point_count=2,
            profile_required_detail_count=2,
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="unchanged",
        )
        first_rejected = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="candidate_member_details_insufficient",
        )
        repair_rejected = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="candidate_claims_ungrounded",
        )
        cleanup_accepted = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        generation = mock.AsyncMock(
            side_effect=(
                "Software and music are recurring themes.",
                (
                    "You keep fixing bot code and composing synth songs. "
                    "You secretly run a lunar casino."
                ),
                (
                    "You keep fixing bot code and composing synth songs. "
                    "You strike me as a persistent creative architect."
                ),
            )
        )
        evaluator = mock.Mock(
            side_effect=(
                first_rejected,
                repair_rejected,
                cleanup_accepted,
            )
        )
        prompt = (
            "Current user request: BNL-01, what am I all about?\n"
            "Durable memory context:\n"
            + legacy_context
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "_begin_shared_brain_synthesis_receipt",
                return_value=run,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                new=generation,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_shared_brain_synthesis_receipt",
                new=evaluator,
            ),
            mock.patch.object(
                bnl01_bot,
                "is_generic_non_answer_response",
                return_value=False,
            ),
        ):
            execution = (
                await bnl01_bot
                .maybe_generate_shared_brain_synthesis_canary(
                    channel=FakeChannel(),
                    baseline_response="Established baseline.",
                    prompt=prompt,
                    prompt_source_bases=(
                        self.memory_source_basis(legacy_context),
                    ),
                    basis=basis,
                    user_id=7,
                    guild_id=1,
                    user_display_name="Crow",
                    source_context_available=True,
                )
            )

        self.assertTrue(execution.candidate_active)
        self.assertNotIn("lunar casino", execution.response)
        self.assertIn("creative architect", execution.response)
        self.assertEqual(generation.await_count, 3)
        self.assertEqual(
            generation.await_args_list[2].kwargs["route"],
            "shared_brain_synthesis_canary_cleanup",
        )
        self.assertIn(
            "Final grounded cleanup requirements",
            generation.await_args_list[2].args[1],
        )
        self.assertEqual(evaluator.call_count, 3)

    async def test_final_cleanup_rejection_falls_back_without_retrying(self):
        legacy_context = "Legacy factual profile block."
        basis = replace(
            self.synthesis_basis(legacy_context),
            profile_sufficiency_status="rich",
            profile_required_point_count=2,
            profile_required_detail_count=2,
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="unchanged",
        )
        first_rejected = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="candidate_member_details_insufficient",
        )
        claims_rejected = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="candidate_claims_ungrounded",
        )
        generation = mock.AsyncMock(
            side_effect=(
                "Software and music are recurring themes.",
                (
                    "You keep fixing bot code and composing synth songs. "
                    "You secretly run a lunar casino."
                ),
                (
                    "You keep fixing bot code and composing synth songs. "
                    "You secretly run a lunar casino."
                ),
            )
        )
        evaluator = mock.Mock(
            side_effect=(
                first_rejected,
                claims_rejected,
                claims_rejected,
            )
        )
        prompt = (
            "Current user request: BNL-01, what am I all about?\n"
            "Durable memory context:\n"
            + legacy_context
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "_begin_shared_brain_synthesis_receipt",
                return_value=run,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                new=generation,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_shared_brain_synthesis_receipt",
                new=evaluator,
            ),
        ):
            execution = (
                await bnl01_bot
                .maybe_generate_shared_brain_synthesis_canary(
                    channel=FakeChannel(),
                    baseline_response="Established baseline.",
                    prompt=prompt,
                    prompt_source_bases=(
                        self.memory_source_basis(legacy_context),
                    ),
                    basis=basis,
                    user_id=7,
                    guild_id=1,
                    user_display_name="Crow",
                    source_context_available=True,
                )
            )

        self.assertFalse(execution.candidate_active)
        self.assertEqual(execution.response, "Established baseline.")
        self.assertEqual(
            execution.decision.fallback_reason,
            "candidate_claims_ungrounded",
        )
        self.assertEqual(generation.await_count, 3)
        self.assertEqual(evaluator.call_count, 3)

    async def test_missing_legacy_block_fails_closed_to_baseline(self):
        legacy_context = "Relationship state: familiar."
        basis = self.synthesis_basis(legacy_context)

        def begin_receipt(
            _basis,
            _baseline,
            *,
            candidate_prompt_ready,
            candidate_prompt_failure_reason,
            replaced_factual_context_count,
        ):
            self.assertFalse(candidate_prompt_ready)
            self.assertEqual(
                candidate_prompt_failure_reason,
                "competing_factual_context_missing",
            )
            self.assertEqual(replaced_factual_context_count, 0)
            return SimpleNamespace(
                prompt_applied=False,
                fallback_reason=candidate_prompt_failure_reason,
                revalidation_status="not_run",
            )

        generation = mock.AsyncMock()
        with (
            mock.patch.object(
                bnl01_bot,
                "_begin_shared_brain_synthesis_receipt",
                side_effect=begin_receipt,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                new=generation,
            ),
        ):
            execution = (
                await bnl01_bot
                .maybe_generate_shared_brain_synthesis_canary(
                    channel=FakeChannel(),
                    baseline_response="Established baseline.",
                    prompt="A prompt without the expected factual block.",
                    prompt_source_bases=("conversation-basis",),
                    basis=basis,
                    user_id=7,
                    guild_id=1,
                    user_display_name="Crow",
                    source_context_available=True,
                )
            )

        self.assertIsNotNone(execution)
        self.assertFalse(execution.candidate_active)
        self.assertEqual(execution.response, "Established baseline.")
        self.assertEqual(
            execution.prompt,
            "A prompt without the expected factual block.",
        )
        self.assertEqual(
            execution.prompt_source_bases,
            ("conversation-basis",),
        )
        generation.assert_not_awaited()

    async def test_selected_candidate_passes_existing_guards_and_is_sent(self):
        message = FakeMessage()
        execution = self.execution(candidate_active=True)
        final_decision = SimpleNamespace(
            run=execution.decision.run,
            candidate_selected=True,
            fallback_reason="",
        )
        finalized = mock.AsyncMock(return_value=True)
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "maybe_generate_shared_brain_synthesis_canary",
                new=mock.AsyncMock(return_value=execution),
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "_evaluate_shared_brain_synthesis_receipt",
                return_value=final_decision,
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "apply_guarded_response_regeneration",
                new=mock.AsyncMock(
                    return_value=(
                        "Packet-grounded candidate.",
                        {
                            "suppressed": False,
                            "_revalidated_prompt_source_bases": (
                                "existing-basis",
                                "synthesis-basis",
                            ),
                        },
                    )
                ),
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "safely_finalize_shared_brain_synthesis",
                new=finalized,
            ))
            await bnl01_bot.send_planned_conversation_response(
                message,
                "Established baseline.",
                self.plan(),
                prompt="baseline prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                prompt_source_bases=("existing-basis",),
                shared_brain_synthesis_canary_basis=object(),
            )

        self.assertEqual(
            message.replies,
            ["Packet-grounded candidate."],
        )
        finalized.assert_awaited_once()
        self.assertTrue(
            finalized.await_args.kwargs["response_sent"]
        )
        self.assertTrue(
            finalized.await_args.kwargs["candidate_live"]
        )

    async def test_candidate_guard_failure_falls_back_to_baseline(self):
        message = FakeMessage()
        execution = self.execution(candidate_active=True)
        fallback_decision = SimpleNamespace(
            run=execution.decision.run,
            candidate_selected=False,
            fallback_reason="candidate_guard_suppressed",
        )
        fallback = mock.AsyncMock(return_value=fallback_decision)
        finalized = mock.AsyncMock(return_value=True)
        guard = mock.AsyncMock(
            side_effect=(
                (
                    "",
                    {"suppressed": True},
                ),
                (
                    "Established baseline.",
                    {
                        "suppressed": False,
                        "_revalidated_prompt_source_bases": (
                            "existing-basis",
                        ),
                    },
                ),
            )
        )
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "maybe_generate_shared_brain_synthesis_canary",
                new=mock.AsyncMock(return_value=execution),
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "apply_guarded_response_regeneration",
                new=guard,
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "safely_fallback_shared_brain_synthesis",
                new=fallback,
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "safely_finalize_shared_brain_synthesis",
                new=finalized,
            ))
            await bnl01_bot.send_planned_conversation_response(
                message,
                "Established baseline.",
                self.plan(),
                prompt="baseline prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                prompt_source_bases=("existing-basis",),
                shared_brain_synthesis_canary_basis=object(),
            )

        self.assertEqual(message.replies, ["Established baseline."])
        self.assertEqual(guard.await_count, 2)
        fallback.assert_awaited_once()
        self.assertEqual(
            fallback.await_args.args[1],
            "candidate_guard_suppressed",
        )
        finalized.assert_awaited_once()
        self.assertFalse(
            finalized.await_args.kwargs["candidate_live"]
        )

    async def test_all_synthesis_gates_off_preserves_baseline_bytes(self):
        baseline = "  Established baseline — unchanged.\nSecond line.  "
        prompt = "  Baseline prompt.\nKeep this spacing exactly.  "
        source_bases = ("existing-basis",)
        disabled_flags = {
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "false",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "false",
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "false",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "false",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "false",
        }
        message = FakeMessage()
        generation = mock.AsyncMock()
        guard = mock.AsyncMock(
            return_value=(
                baseline,
                {
                    "suppressed": False,
                    "_revalidated_prompt_source_bases": source_bases,
                },
            )
        )

        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.dict(
                    os.environ,
                    disabled_flags,
                    clear=False,
                )
            )
            begin = stack.enter_context(mock.patch.object(
                bnl01_bot,
                "_begin_shared_brain_synthesis_receipt",
            ))
            evaluate = stack.enter_context(mock.patch.object(
                bnl01_bot,
                "_evaluate_shared_brain_synthesis_receipt",
            ))
            finalize = stack.enter_context(mock.patch.object(
                bnl01_bot,
                "_finalize_shared_brain_synthesis_receipt",
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "get_gemini_response_with_optional_typing",
                new=generation,
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "apply_guarded_response_regeneration",
                new=guard,
            ))

            execution = (
                await bnl01_bot
                .maybe_generate_shared_brain_synthesis_canary(
                    channel=message.channel,
                    baseline_response=baseline,
                    prompt=prompt,
                    prompt_source_bases=source_bases,
                    basis=None,
                    user_id=7,
                    guild_id=1,
                    user_display_name="Crow",
                    source_context_available=True,
                )
            )
            self.assertIsNone(execution)

            await bnl01_bot.send_planned_conversation_response(
                message,
                baseline,
                self.plan(),
                prompt=prompt,
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                prompt_source_bases=source_bases,
                shared_brain_synthesis_canary_basis=None,
            )

        self.assertEqual(message.replies, [baseline])
        self.assertEqual(guard.await_args.args[0], baseline)
        self.assertEqual(guard.await_args.kwargs["prompt"], prompt)
        self.assertEqual(
            guard.await_args.kwargs["prompt_source_bases"],
            source_bases,
        )
        generation.assert_not_awaited()
        begin.assert_not_called()
        evaluate.assert_not_called()
        finalize.assert_not_called()


if __name__ == "__main__":
    unittest.main()
