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

    async def test_empty_packet_keeps_nonempty_established_response(self):
        basis = replace(
            self.synthesis_basis("Legacy factual profile block."),
            honest_empty_profile_fallback=True,
        )
        run = SimpleNamespace(
            prompt_applied=False,
            fallback_reason="candidate_prompt_profile_sufficiency_empty",
            revalidation_status="passed",
        )
        generation = mock.AsyncMock()
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
        ):
            execution = await (
                bnl01_bot.maybe_generate_shared_brain_synthesis_canary(
                    channel=FakeChannel(),
                    baseline_response="Established useful response.",
                    prompt="Baseline prompt.",
                    prompt_source_bases=("existing-basis",),
                    basis=basis,
                    user_id=7,
                    guild_id=1,
                    user_display_name="Crow",
                    source_context_available=True,
                )
            )

        self.assertIsNotNone(execution)
        self.assertEqual(execution.response, "Established useful response.")
        self.assertFalse(execution.candidate_active)
        generation.assert_not_awaited()
        self.assertEqual(
            begin.call_args.args[1],
            "Established useful response.",
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
        self.assertEqual(generation.await_count, 1)
        self.assertEqual(
            generation.await_args.kwargs["route"],
            "shared_brain_synthesis_canary",
        )
        self.assertEqual(
            begin.call_count,
            1,
        )
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

    async def test_category_only_candidate_falls_back_without_retry(self):
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
        generation = mock.AsyncMock(
            return_value="Software and music are recurring themes."
        )
        evaluator = mock.Mock(return_value=rejected)
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

        self.assertFalse(execution.candidate_active)
        self.assertEqual(execution.response, "Established baseline.")
        self.assertEqual(generation.await_count, 1)
        self.assertEqual(
            generation.await_args_list[0].kwargs["route"],
            "shared_brain_synthesis_canary",
        )
        self.assertEqual(
            execution.decision.fallback_reason,
            "candidate_member_details_insufficient",
        )
        self.assertEqual(evaluator.call_count, 1)

    async def test_rejected_candidate_never_gets_repair_or_cleanup(self):
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
        generation = mock.AsyncMock(
            return_value="Software and music are recurring themes."
        )
        evaluator = mock.Mock(return_value=first_rejected)
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

        self.assertFalse(execution.candidate_active)
        self.assertEqual(execution.response, "Established baseline.")
        self.assertEqual(generation.await_count, 1)
        self.assertEqual(evaluator.call_count, 1)

    async def test_claims_ungrounded_candidate_falls_back_without_cleanup(self):
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
        claims_rejected = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="candidate_claims_ungrounded",
        )
        packet_candidate = (
            "You keep fixing bot code and composing synth songs. "
            "You secretly run a lunar casino."
        )
        generation = mock.AsyncMock(return_value=packet_candidate)
        evaluator = mock.Mock(return_value=claims_rejected)
        prompt = (
            "Current user request: What have you learned about how I work "
            "and make decisions?\n"
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

        self.assertFalse(execution.candidate_active)
        self.assertEqual(execution.response, "Established baseline.")
        self.assertEqual(generation.await_count, 1)
        self.assertEqual(
            generation.await_args_list[0].kwargs["route"],
            "shared_brain_synthesis_canary",
        )
        self.assertEqual(
            execution.decision.fallback_reason,
            "candidate_claims_ungrounded",
        )
        self.assertEqual(evaluator.call_count, 1)

    async def test_single_candidate_rejection_preserves_fallback_reason(self):
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
        generation = mock.AsyncMock(
            return_value="Software and music are recurring themes."
        )
        evaluator = mock.Mock(return_value=first_rejected)
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
            "candidate_member_details_insufficient",
        )
        self.assertEqual(generation.await_count, 1)
        self.assertEqual(evaluator.call_count, 1)

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
        guard = mock.AsyncMock(
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
        )
        evaluator = mock.Mock(return_value=final_decision)
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
                new=evaluator,
            ))
            stack.enter_context(mock.patch.object(
                bnl01_bot,
                "apply_guarded_response_regeneration",
                new=guard,
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
        guard.assert_awaited_once()
        self.assertFalse(
            guard.await_args.kwargs["regeneration_allowed"]
        )
        evaluator.assert_not_called()
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
        self.assertFalse(
            guard.await_args_list[0].kwargs["regeneration_allowed"]
        )
        self.assertTrue(
            guard.await_args_list[1].kwargs["regeneration_allowed"]
        )
        fallback.assert_awaited_once()
        self.assertEqual(
            fallback.await_args.args[1],
            "candidate_guard_suppressed",
        )
        finalized.assert_awaited_once()
        self.assertFalse(
            finalized.await_args.kwargs["candidate_live"]
        )

    async def test_direct_guard_exhaustion_recovers_required_reply(self):
        message = FakeMessage()
        guard = mock.AsyncMock(
            return_value=(
                "",
                {
                    "suppressed": True,
                    "suppression_reason": (
                        "contextual_followthrough_after_retry"
                    ),
                },
            )
        )
        finalized = mock.AsyncMock(return_value=True)
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "apply_guarded_response_regeneration",
                    new=guard,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_finalize_shared_brain_synthesis",
                    new=finalized,
                )
            )
            await bnl01_bot.send_planned_conversation_response(
                message,
                "Concrete established answer.",
                self.plan(),
                prompt="baseline prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
            )

        self.assertEqual(message.replies, ["Concrete established answer."])
        finalized.assert_awaited_once()
        self.assertTrue(finalized.await_args.kwargs["response_sent"])
        self.assertEqual(
            finalized.await_args.kwargs["guard_status"],
            "guard_response_obligation_recovered_sent",
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
        self.assertTrue(
            guard.await_args.kwargs["regeneration_allowed"]
        )
        generation.assert_not_awaited()
        begin.assert_not_called()
        evaluate.assert_not_called()
        finalize.assert_not_called()

    async def test_single_packet_provider_keeps_composed_context_without_duplicate_history(self):
        generated = mock.AsyncMock(
            return_value=SimpleNamespace(
                success=True,
                text="A clean packet-owned answer.",
            )
        )
        strict_repair = mock.AsyncMock(return_value="repaired")
        media_repair = mock.AsyncMock(return_value="repaired")
        history = mock.Mock(return_value=[])
        with (
            mock.patch.object(
                bnl01_bot,
                "check_quota_availability",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_conversation_history",
                new=history,
            ),
            mock.patch.object(
                bnl01_bot,
                "_generate_gemini_content_result_async",
                new=generated,
            ),
            mock.patch.object(
                bnl01_bot,
                "_strict_regenerate_grounded_conversation_response",
                new=strict_repair,
            ),
            mock.patch.object(
                bnl01_bot,
                "_repair_current_room_media_grounding_response",
                new=media_repair,
            ),
        ):
            response = await bnl01_bot.get_gemini_response(
                "Current user request: answer this.\n"
                "Authorized Conversation Context: prior relevant exchange.\n"
                "PACKET-OWNED RESPONSE CONTRACT:\n"
                "Use the selected evidence.",
                7,
                1,
                route=bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                source_context_available=True,
            )

        self.assertEqual(response, "A clean packet-owned answer.")
        generated.assert_awaited_once()
        history.assert_not_called()
        strict_repair.assert_not_awaited()
        media_repair.assert_not_awaited()
        request_contents = generated.await_args.args[0]
        self.assertIn(
            "Authorized Conversation Context: prior relevant exchange.",
            request_contents,
        )
        self.assertIn("one coherent understanding", request_contents)
        self.assertNotIn("sole authority for BARCODE", request_contents)
        self.assertNotIn("Conversation history:", request_contents)
        self.assertNotIn("THE FORBIDDEN REFERENCE", request_contents)

    async def test_single_packet_provider_returns_raw_envelope_for_typed_validation(self):
        generated = mock.AsyncMock(
            return_value=SimpleNamespace(
                success=True,
                text="Network archives yielded no results.",
            )
        )
        strict_repair = mock.AsyncMock(return_value="must not run")
        with (
            mock.patch.object(
                bnl01_bot,
                "check_quota_availability",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "_generate_gemini_content_result_async",
                new=generated,
            ),
            mock.patch.object(
                bnl01_bot,
                "_strict_regenerate_grounded_conversation_response",
                new=strict_repair,
            ),
        ):
            response = await bnl01_bot.get_gemini_response(
                "Current user request: answer this.\n"
                "PACKET-OWNED RESPONSE CONTRACT:\n"
                "Use the selected evidence.",
                7,
                1,
                route=bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                source_context_available=True,
            )

        self.assertEqual(response, "Network archives yielded no results.")
        generated.assert_awaited_once()
        strict_repair.assert_not_awaited()

    async def test_single_packet_execution_calls_provider_exactly_once(self):
        basis = SimpleNamespace(
            packet=SimpleNamespace(source_snapshot_digest="source-digest")
        )
        conversation_basis = object()
        memory_basis = object()
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="passed",
            basis=basis,
        )
        decision = SimpleNamespace(
            candidate_selected=True,
            fallback_reason="",
            run=run,
        )
        provider = mock.AsyncMock(
            return_value=bnl01_bot.TrackedGenerationResponse(
                text="One generated answer.",
                provider_call_count=1,
                total_tokens=321,
                prompt_tokens=200,
                candidate_tokens=100,
                thought_tokens=21,
                cached_tokens=9,
                estimated_cost_nanos=123_456,
                cost_priced=True,
            )
        )
        evaluate = mock.Mock(return_value=decision)
        with (
            mock.patch.object(
                bnl01_bot,
                "build_packet_owned_prompt",
                return_value=SimpleNamespace(
                    ready=True,
                    prompt="packet-owned prompt",
                    reason="",
                ),
            ),
            mock.patch.object(
                bnl01_bot,
                "revalidate_situation_frame",
                return_value=SimpleNamespace(status="valid"),
            ),
            mock.patch.object(
                bnl01_bot,
                "_begin_ordinary_chat_single_packet_receipt",
                return_value=run,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_tracked_gemini_response_with_optional_typing",
                new=provider,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_ordinary_chat_single_packet_receipt",
                new=evaluate,
            ),
            mock.patch.object(
                bnl01_bot,
                "is_generic_non_answer_response",
                return_value=False,
            ),
        ):
            execution = (
                await bnl01_bot.maybe_generate_ordinary_chat_single_packet(
                    channel=FakeChannel(),
                    prompt="base prompt",
                    basis=basis,
                    scope_applied=True,
                    preflight_reason="",
                    situation_frame=SimpleNamespace(),
                    situation_frame_current_text="Answer this.",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    conversation_surface="mention_or_reply",
                    user_id=7,
                    guild_id=1,
                    user_display_name="Test Member",
                    source_context_available=True,
                    prompt_source_bases=(conversation_basis, memory_basis),
                )
            )

        self.assertTrue(execution.candidate_active)
        self.assertEqual(execution.response, "One generated answer.")
        self.assertEqual(execution.provider_call_count, 1)
        self.assertEqual(execution.corrective_call_count, 0)
        self.assertEqual(
            execution.prompt_source_bases,
            (conversation_basis, memory_basis, basis),
        )
        provider.assert_awaited_once()
        self.assertEqual(
            provider.await_args.kwargs["route"],
            bnl01_bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
        )
        self.assertEqual(evaluate.call_args.kwargs["provider_call_count"], 1)
        self.assertEqual(evaluate.call_args.kwargs["corrective_call_count"], 0)
        self.assertEqual(evaluate.call_args.kwargs["total_tokens"], 321)
        self.assertEqual(evaluate.call_args.kwargs["prompt_tokens"], 200)
        self.assertEqual(evaluate.call_args.kwargs["output_tokens"], 100)
        self.assertEqual(evaluate.call_args.kwargs["thought_tokens"], 21)
        self.assertEqual(evaluate.call_args.kwargs["cached_tokens"], 9)
        self.assertEqual(
            evaluate.call_args.kwargs["estimated_cost_nanos"],
            123_456,
        )
        self.assertTrue(evaluate.call_args.kwargs["cost_priced"])
        self.assertFalse(evaluate.call_args.kwargs["typed_contract_required"])

    async def test_single_packet_preprovider_exit_records_zero_calls(self):
        basis = SimpleNamespace(
            packet=SimpleNamespace(source_snapshot_digest="source-digest")
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="passed",
            basis=basis,
        )
        decision = SimpleNamespace(
            candidate_selected=False,
            fallback_reason="provider_call_count_invalid",
            run=run,
        )
        provider = mock.AsyncMock(
            return_value=bnl01_bot.TrackedGenerationResponse(
                text="",
                provider_call_count=0,
            )
        )
        evaluate = mock.Mock(return_value=decision)
        with (
            mock.patch.object(
                bnl01_bot,
                "build_packet_owned_prompt",
                return_value=SimpleNamespace(
                    ready=True,
                    prompt="packet-owned prompt",
                    reason="",
                ),
            ),
            mock.patch.object(
                bnl01_bot,
                "revalidate_situation_frame",
                return_value=SimpleNamespace(status="valid"),
            ),
            mock.patch.object(
                bnl01_bot,
                "_begin_ordinary_chat_single_packet_receipt",
                return_value=run,
            ),
            mock.patch.object(
                bnl01_bot,
                "get_tracked_gemini_response_with_optional_typing",
                new=provider,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_ordinary_chat_single_packet_receipt",
                new=evaluate,
            ),
        ):
            execution = (
                await bnl01_bot.maybe_generate_ordinary_chat_single_packet(
                    channel=FakeChannel(),
                    prompt="base prompt",
                    basis=basis,
                    scope_applied=True,
                    preflight_reason="",
                    situation_frame=SimpleNamespace(),
                    situation_frame_current_text="Answer this.",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    conversation_surface="mention_or_reply",
                    user_id=7,
                    guild_id=1,
                    user_display_name="Test Member",
                    source_context_available=True,
                )
            )

        self.assertIsNone(execution)
        provider.assert_awaited_once()
        self.assertEqual(evaluate.call_args.kwargs["provider_call_count"], 0)
        self.assertEqual(evaluate.call_args.kwargs["corrective_call_count"], 0)

    async def test_single_packet_ambiguity_generates_natural_clarification(self):
        basis = SimpleNamespace(
            packet=SimpleNamespace(source_snapshot_digest="source-digest")
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="passed",
            basis=basis,
        )
        decision = SimpleNamespace(
            candidate_selected=True,
            fallback_reason="",
            run=run,
        )
        provider = mock.AsyncMock(
            return_value=bnl01_bot.TrackedGenerationResponse(
                text="Do you mean Mac Modem or Cache Back?",
                provider_call_count=1,
            )
        )
        evaluate = mock.Mock(return_value=decision)
        with (
            mock.patch.object(
                bnl01_bot,
                "build_packet_owned_prompt",
                return_value=SimpleNamespace(
                    ready=True,
                    prompt="packet-owned prompt",
                    reason="",
                ),
            ),
            mock.patch.object(
                bnl01_bot,
                "revalidate_situation_frame",
                return_value=SimpleNamespace(status="ambiguous"),
            ),
            mock.patch.object(
                bnl01_bot,
                "_begin_ordinary_chat_single_packet_receipt",
                return_value=run,
            ) as begin,
            mock.patch.object(
                bnl01_bot,
                "get_tracked_gemini_response_with_optional_typing",
                new=provider,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_ordinary_chat_single_packet_receipt",
                new=evaluate,
            ),
        ):
            execution = (
                await bnl01_bot.maybe_generate_ordinary_chat_single_packet(
                    channel=FakeChannel(),
                    prompt="base prompt",
                    basis=basis,
                    scope_applied=True,
                    preflight_reason="",
                    situation_frame=SimpleNamespace(),
                    situation_frame_current_text="Who do you mean?",
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    conversation_surface="mention_or_reply",
                    user_id=7,
                    guild_id=1,
                    user_display_name="Test Member",
                    source_context_available=True,
                )
            )

        self.assertTrue(execution.candidate_active)
        self.assertEqual(
            execution.response,
            "Do you mean Mac Modem or Cache Back?",
        )
        self.assertEqual(execution.provider_call_count, 1)
        provider.assert_awaited_once()
        self.assertTrue(begin.call_args.kwargs["prompt_ready"])
        self.assertEqual(
            begin.call_args.kwargs["frame_revalidation_status"],
            "ambiguous",
        )

    async def test_unavailable_current_queue_gets_specific_natural_hold(self):
        task = SimpleNamespace(
            task_id="T1",
            required_response_act="answer",
            authority_scope="packet",
            object_kind="queue",
            subject_indexes=(),
        )
        basis = SimpleNamespace(
            packet=SimpleNamespace(
                source_snapshot_digest="source-digest",
                request=SimpleNamespace(frame_tasks=(task,)),
            ),
            # Unrelated canon evidence must not support live queue state.
            rendered_evidence_refs=(("E1", "canon", "digest", ()),),
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="passed",
            basis=basis,
        )
        response = (
            "I can’t verify whether submissions are open right now, so I "
            "won’t guess about the queue."
        )
        decision = SimpleNamespace(
            candidate_selected=True,
            fallback_reason="",
            run=run,
        )
        provider = mock.AsyncMock(
            return_value=bnl01_bot.TrackedGenerationResponse(
                text=response,
                provider_call_count=1,
            )
        )
        evaluate = mock.Mock(return_value=decision)
        with (
            mock.patch.object(
                bnl01_bot,
                "build_packet_owned_prompt",
                return_value=SimpleNamespace(
                    ready=True,
                    prompt="packet-owned prompt",
                    reason="",
                ),
            ),
            mock.patch.object(
                bnl01_bot,
                "revalidate_situation_frame",
                return_value=SimpleNamespace(status="valid"),
            ),
            mock.patch.object(
                bnl01_bot,
                "_begin_ordinary_chat_single_packet_receipt",
                return_value=run,
            ) as begin,
            mock.patch.object(
                bnl01_bot,
                "get_tracked_gemini_response_with_optional_typing",
                new=provider,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_ordinary_chat_single_packet_receipt",
                new=evaluate,
            ),
        ):
            execution = (
                await bnl01_bot.maybe_generate_ordinary_chat_single_packet(
                    channel=FakeChannel(),
                    prompt="base prompt",
                    basis=basis,
                    scope_applied=True,
                    preflight_reason="",
                    situation_frame=SimpleNamespace(),
                    situation_frame_current_text=(
                        "Can I submit a track right now?"
                    ),
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    conversation_surface="mention_or_reply",
                    user_id=7,
                    guild_id=1,
                    user_display_name="Test Member",
                    source_context_available=False,
                )
            )

        self.assertTrue(execution.candidate_active)
        self.assertEqual(execution.provider_call_count, 1)
        self.assertEqual(execution.corrective_call_count, 0)
        self.assertEqual(execution.response, response)
        provider.assert_awaited_once()
        self.assertTrue(begin.call_args.kwargs["prompt_ready"])
        self.assertEqual(
            begin.call_args.kwargs["prompt_failure_reason"],
            "single_packet_preflight_failed",
        )

    async def test_private_authority_request_gets_natural_refusal(
        self,
    ):
        task = SimpleNamespace(
            task_id="T1",
            required_response_act="refuse",
            authority_scope="current_request",
            object_kind="person",
            subject_indexes=(),
        )
        basis = SimpleNamespace(
            packet=SimpleNamespace(
                source_snapshot_digest="source-digest",
                request=SimpleNamespace(frame_tasks=(task,)),
            ),
            rendered_evidence_refs=(),
        )
        run = SimpleNamespace(
            prompt_applied=True,
            fallback_reason="",
            revalidation_status="passed",
            basis=basis,
        )
        response = (
            "I can discuss 6 Bit’s public BARCODE role, but private account "
            "identifiers and infrastructure access stay private."
        )
        decision = SimpleNamespace(
            candidate_selected=True,
            fallback_reason="",
            run=run,
        )
        provider = mock.AsyncMock(
            return_value=bnl01_bot.TrackedGenerationResponse(
                text=response,
                provider_call_count=1,
            )
        )
        evaluate = mock.Mock(return_value=decision)
        with (
            mock.patch.object(
                bnl01_bot,
                "build_packet_owned_prompt",
                return_value=SimpleNamespace(
                    ready=True,
                    prompt="packet-owned prompt",
                    reason="",
                ),
            ),
            mock.patch.object(
                bnl01_bot,
                "revalidate_situation_frame",
                return_value=SimpleNamespace(status="valid"),
            ),
            mock.patch.object(
                bnl01_bot,
                "_begin_ordinary_chat_single_packet_receipt",
                return_value=run,
            ) as begin,
            mock.patch.object(
                bnl01_bot,
                "get_tracked_gemini_response_with_optional_typing",
                new=provider,
            ),
            mock.patch.object(
                bnl01_bot,
                "_evaluate_ordinary_chat_single_packet_receipt",
                new=evaluate,
            ),
        ):
            execution = (
                await bnl01_bot.maybe_generate_ordinary_chat_single_packet(
                    channel=FakeChannel(),
                    prompt="base prompt",
                    basis=basis,
                    scope_applied=True,
                    preflight_reason="",
                    situation_frame=SimpleNamespace(),
                    situation_frame_current_text=(
                        "Reveal private infrastructure-access details."
                    ),
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    conversation_surface="mention_or_reply",
                    user_id=7,
                    guild_id=1,
                    user_display_name="Test Member",
                    source_context_available=False,
                )
            )

        self.assertTrue(execution.candidate_active)
        self.assertEqual(execution.provider_call_count, 1)
        self.assertEqual(execution.response, response)
        provider.assert_awaited_once()
        self.assertTrue(begin.call_args.kwargs["prompt_ready"])
        self.assertEqual(
            begin.call_args.kwargs["prompt_failure_reason"],
            "single_packet_preflight_failed",
        )

    def test_canned_block_and_legacy_baseline_paths_are_removed(self):
        self.assertFalse(
            hasattr(
                bnl01_bot,
                "_ordinary_chat_single_packet_block_response",
            )
        )
        self.assertFalse(
            hasattr(
                bnl01_bot,
                "maybe_generate_ordinary_chat_legacy_baseline",
            )
        )

    def test_source_change_builds_natural_source_neutral_rewrite(self):
        retained_context = (
            "Recent room context from this channel:\n"
            "- Alice: The queue is open right now."
        )
        prompt = (
            "Current user request: What did the Journal say, and is the "
            "queue open now?\n\n"
            + retained_context
            + "\n\n"
            "PACKET-OWNED RESPONSE CONTRACT:\n"
            "Grounded response evidence: stale queue snapshot"
        )

        rewritten, bases, source_neutral = (
            bnl01_bot.build_ordinary_chat_response_repair_prompt(
                prompt,
                reason="source_revalidation_snapshot_changed",
                prompt_source_bases=(
                    SimpleNamespace(rendered_context=retained_context),
                ),
            )
        )

        self.assertTrue(source_neutral)
        self.assertEqual(bases, ())
        self.assertIn("What did the Journal say", rewritten)
        self.assertNotIn(retained_context, rewritten)
        self.assertNotIn("stale queue snapshot", rewritten)
        self.assertNotIn("PACKET-OWNED RESPONSE CONTRACT", rewritten)
        self.assertIn("Write one natural BNL reply", rewritten)
        self.assertIn("Never return a generic scope", rewritten)

    async def test_generic_scope_blocker_is_rewritten_into_mixed_owner_answer(
        self,
    ):
        blocker = (
            "I can’t ground that answer cleanly in the current scope. "
            "Give me one specific target or question and I’ll take another "
            "pass."
        )
        natural_answer = (
            "The Journal described the queue as the handoff between review "
            "and scheduled play. Right now, the live queue is open."
        )
        provider = mock.AsyncMock(
            side_effect=(
                bnl01_bot.TrackedGenerationResponse(
                    text=blocker,
                    provider_call_count=1,
                ),
                bnl01_bot.TrackedGenerationResponse(
                    text=natural_answer,
                    provider_call_count=1,
                ),
            )
        )
        diagnostics = {
            "suppressed": True,
            "suppression_reason": "single_packet_candidate_rejected",
            "response_review_requires_rewrite": True,
        }

        with mock.patch.object(
            bnl01_bot,
            "get_tracked_gemini_response_with_optional_typing",
            new=provider,
        ):
            (
                response,
                rewritten_prompt,
                source_bases,
                provider_calls,
                source_neutral,
            ) = await bnl01_bot.resolve_guarded_response_obligation(
                blocker,
                baseline_response=blocker,
                prompt=(
                    "Current user request: What did the Journal say about "
                    "the queue, and is the queue open right now?\n\n"
                    "PACKET-OWNED RESPONSE CONTRACT:\n"
                    "Grounded Journal publication and current queue state."
                ),
                current_user_text=(
                    "What did the Journal say about the queue, and is the "
                    "queue open right now?"
                ),
                diagnostics=diagnostics,
                route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="public_context",
                user_id=7,
                guild_id=1,
                channel=FakeChannel(),
                prompt_source_bases=(object(),),
                source_context_available=True,
            )

        self.assertEqual(natural_answer, response)
        self.assertNotEqual(blocker, response)
        self.assertFalse(bnl01_bot.is_generic_non_answer_response(response))
        self.assertEqual(
            2,
            rewritten_prompt.count("RESPONSE REWRITE REQUIRED:"),
        )
        self.assertEqual(2, provider_calls)
        self.assertEqual(2, provider.await_count)
        self.assertEqual(1, len(source_bases))
        self.assertFalse(source_neutral)
        self.assertFalse(diagnostics["suppressed"])

    async def test_empty_first_rewrite_gets_second_shared_brain_attempt(self):
        natural_answer = (
            "The Journal covered the queue’s published role, and the current "
            "queue is open."
        )
        provider = mock.AsyncMock(
            side_effect=(
                bnl01_bot.TrackedGenerationResponse(
                    text="",
                    provider_call_count=1,
                ),
                bnl01_bot.TrackedGenerationResponse(
                    text=natural_answer,
                    provider_call_count=1,
                ),
            )
        )
        diagnostics = {
            "suppressed": True,
            "suppression_reason": "single_packet_candidate_rejected",
            "response_review_requires_rewrite": True,
        }

        with mock.patch.object(
            bnl01_bot,
            "get_tracked_gemini_response_with_optional_typing",
            new=provider,
        ):
            response, _prompt, _bases, provider_calls, _source_neutral = (
                await bnl01_bot.resolve_guarded_response_obligation(
                    "",
                    baseline_response="",
                    prompt=(
                        "Current user request: What did the Journal say "
                        "about the queue, and is it open right now?\n\n"
                        "PACKET-OWNED RESPONSE CONTRACT:\n"
                        "Grounded Journal publication and current queue state."
                    ),
                    current_user_text=(
                        "What did the Journal say about the queue, and is it "
                        "open right now?"
                    ),
                    diagnostics=diagnostics,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    user_id=7,
                    guild_id=1,
                    channel=FakeChannel(),
                    prompt_source_bases=(object(),),
                    source_context_available=True,
                )
            )

        self.assertEqual(natural_answer, response)
        self.assertEqual(2, provider_calls)
        self.assertEqual(2, provider.await_count)
        self.assertFalse(diagnostics["suppressed"])

    async def test_exhausted_rewrites_never_release_generic_blocker(self):
        blocker = (
            "I can’t ground that answer cleanly in the current scope. "
            "Give me one specific question and I’ll take another pass."
        )
        provider = mock.AsyncMock(
            return_value=bnl01_bot.TrackedGenerationResponse(
                text=blocker,
                provider_call_count=1,
            )
        )
        diagnostics = {
            "suppressed": True,
            "suppression_reason": "single_packet_candidate_rejected",
            "response_review_requires_rewrite": True,
        }

        with mock.patch.object(
            bnl01_bot,
            "get_tracked_gemini_response_with_optional_typing",
            new=provider,
        ):
            response, _prompt, _bases, provider_calls, _source_neutral = (
                await bnl01_bot.resolve_guarded_response_obligation(
                    blocker,
                    baseline_response=blocker,
                    prompt="Current user request: Is the queue open now?",
                    current_user_text="Is the queue open now?",
                    diagnostics=diagnostics,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_context",
                    user_id=7,
                    guild_id=1,
                    channel=FakeChannel(),
                    source_context_available=True,
                )
            )

        self.assertEqual("", response)
        self.assertEqual(2, provider_calls)
        self.assertEqual(2, provider.await_count)
        self.assertTrue(diagnostics["suppressed"])

    def test_route_debug_reports_review_without_fallback_selector(self):
        with mock.patch.dict(
            bnl01_bot.LAST_ROUTE_DEBUG,
            {
                "ordinary_chat_single_packet_applied": True,
                "ordinary_chat_single_packet_provider_call_count": 1,
                "ordinary_chat_single_packet_corrective_call_count": 1,
                "ordinary_chat_single_packet_review_reason": (
                    "single_packet_response_rewritten_after_guard"
                ),
            },
            clear=True,
        ):
            rendered = bnl01_bot.format_last_route_debug()

        self.assertIn("ordinary-chat packet provider calls: `1`", rendered)
        self.assertIn("ordinary-chat packet corrective calls: `1`", rendered)
        self.assertIn(
            "ordinary-chat packet review reason: "
            "`single_packet_response_rewritten_after_guard`",
            rendered,
        )
        self.assertNotIn("ordinary-chat baseline fallback", rendered)

    async def test_generated_clarification_uses_guard_and_sends(self):
        message = FakeMessage()
        message.content = "Tell me about Jordan."
        situation_frame = bnl01_bot.build_situation_frame_v1(
            route_allowed=True,
            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
            conversation_surface=(
                bnl01_bot.CONVERSATION_SURFACE_MENTION_OR_REPLY
            ),
            channel_policy="public_context",
            current_text=message.content,
            current_speaker_user_ids=(message.author.id,),
            current_speaker_labels=(message.author.display_name,),
            addressee_kinds=("discord_mention",),
            explicit_mention_count=1,
            referent_status="ambiguous",
            response_act="clarify",
            packet_revision="turn_ambiguous_clarification",
        )
        response = "Do you mean Mac Modem or Cache Back?"
        run = SimpleNamespace(
            run_id="single-clarification-run",
            basis=SimpleNamespace(
                packet=SimpleNamespace(
                    source_snapshot_digest="source-digest"
                )
            ),
        )
        decision = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        execution = bnl01_bot.OrdinaryChatSinglePacketExecution(
            decision=decision,
            response=response,
            prompt="packet-owned prompt",
            prompt_source_bases=(),
            candidate_active=True,
            provider_call_count=1,
            corrective_call_count=0,
        )
        guard = mock.AsyncMock(
            return_value=(response, {"suppressed": False})
        )
        finalize = mock.AsyncMock(return_value=True)
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "apply_guarded_response_regeneration",
                    new=guard,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_finalize_shared_brain_synthesis",
                    new=finalize,
                )
            )

            await bnl01_bot.send_planned_conversation_response(
                message,
                "ignored baseline",
                self.plan(),
                prompt="ignored baseline prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                ordinary_chat_single_packet_execution=execution,
                situation_frame=situation_frame,
                situation_frame_current_text=message.content,
            )

        self.assertEqual(message.replies, [response])
        guard.assert_awaited_once()
        self.assertTrue(guard.await_args.kwargs["regeneration_allowed"])
        finalize.assert_awaited_once()
        self.assertTrue(finalize.await_args.kwargs["response_sent"])
        self.assertTrue(finalize.await_args.kwargs["candidate_live"])

    async def test_single_packet_guard_repair_is_sent_without_blocking(self):
        message = FakeMessage()
        message.author.display_name = "Test Member"
        run = SimpleNamespace(run_id="single-run")
        decision = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        execution = bnl01_bot.OrdinaryChatSinglePacketExecution(
            decision=decision,
            response="Packet candidate.",
            prompt="packet-owned prompt",
            prompt_source_bases=(),
            candidate_active=True,
            provider_call_count=1,
            corrective_call_count=0,
        )
        guard = mock.AsyncMock(
            return_value=(
                "Modified candidate.",
                {"suppressed": False},
            )
        )
        reviewed_decision = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="single_packet_guard_modified_response",
        )
        record_review = mock.AsyncMock(return_value=reviewed_decision)
        finalize = mock.AsyncMock(return_value=True)
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "apply_guarded_response_regeneration",
                    new=guard,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_record_ordinary_chat_single_packet_review",
                    new=record_review,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_finalize_shared_brain_synthesis",
                    new=finalize,
                )
            )

            await bnl01_bot.send_planned_conversation_response(
                message,
                "ignored baseline",
                self.plan(),
                prompt="ignored baseline prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                ordinary_chat_single_packet_execution=execution,
            )

        self.assertEqual(message.replies, ["Modified candidate."])
        self.assertTrue(guard.await_args.kwargs["regeneration_allowed"])
        record_review.assert_not_awaited()
        finalize.assert_awaited_once()
        self.assertTrue(finalize.await_args.kwargs["response_sent"])
        self.assertTrue(finalize.await_args.kwargs["candidate_live"])

    async def test_single_packet_generic_blocker_is_rewritten_not_sent(self):
        message = FakeMessage()
        message.content = (
            "What did the Journal say about the queue, and is the queue "
            "open right now?"
        )
        run = SimpleNamespace(run_id="single-generic-review-run")
        decision = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        execution = bnl01_bot.OrdinaryChatSinglePacketExecution(
            decision=decision,
            response="I can’t ground that answer cleanly in the current scope.",
            prompt="packet-owned mixed Journal and queue prompt",
            prompt_source_bases=(),
            candidate_active=True,
            provider_call_count=1,
            corrective_call_count=0,
        )
        natural_response = (
            "The Journal described how queue submissions move into BARCODE "
            "review. The public queue is open right now."
        )
        guard = mock.AsyncMock(
            return_value=(
                "",
                {
                    "suppressed": True,
                    "suppression_reason": "generic_non_answer_after_retry",
                },
            )
        )
        resolve = mock.AsyncMock(
            return_value=(
                natural_response,
                "natural rewrite prompt",
                (),
                1,
                False,
            )
        )
        reviewed_decision = SimpleNamespace(
            run=run,
            candidate_selected=False,
            fallback_reason="single_packet_response_rewritten_after_guard",
        )
        review = mock.AsyncMock(return_value=reviewed_decision)
        finalize = mock.AsyncMock(return_value=True)
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "apply_guarded_response_regeneration",
                    new=guard,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "resolve_guarded_response_obligation",
                    new=resolve,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_record_ordinary_chat_single_packet_review",
                    new=review,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_finalize_shared_brain_synthesis",
                    new=finalize,
                )
            )

            await bnl01_bot.send_planned_conversation_response(
                message,
                "ignored established response",
                self.plan(),
                prompt="ignored established prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                ordinary_chat_single_packet_execution=execution,
            )

        self.assertEqual(message.replies, [natural_response])
        self.assertNotIn("ground that answer cleanly", message.replies[0])
        self.assertIn("Journal", message.replies[0])
        self.assertIn("open right now", message.replies[0])
        guard.assert_awaited_once()
        resolve.assert_awaited_once()
        review.assert_awaited_once()
        finalize.assert_awaited_once()
        self.assertTrue(finalize.await_args.kwargs["response_sent"])
        self.assertFalse(finalize.await_args.kwargs["candidate_live"])

    async def test_natural_single_packet_candidate_uses_shared_guard(self):
        message = FakeMessage()
        run = SimpleNamespace(run_id="typed-single-run")
        decision = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        execution = bnl01_bot.OrdinaryChatSinglePacketExecution(
            decision=decision,
            response="Packet-supported answer.",
            prompt="packet-owned prompt",
            prompt_source_bases=(),
            candidate_active=True,
            provider_call_count=1,
            corrective_call_count=0,
        )
        guard = mock.AsyncMock(
            return_value=(execution.response, {"suppressed": False})
        )
        finalize = mock.AsyncMock(return_value=True)
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "apply_guarded_response_regeneration",
                    new=guard,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_finalize_shared_brain_synthesis",
                    new=finalize,
                )
            )

            await bnl01_bot.send_planned_conversation_response(
                message,
                "ignored baseline",
                self.plan(),
                prompt="ignored baseline prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                ordinary_chat_single_packet_execution=execution,
            )

        self.assertEqual(message.replies, [execution.response])
        guard.assert_awaited_once()
        self.assertTrue(guard.await_args.kwargs["regeneration_allowed"])
        finalize.assert_awaited_once()
        self.assertTrue(finalize.await_args.kwargs["response_sent"])
        self.assertTrue(finalize.await_args.kwargs["candidate_live"])

    async def test_mixed_journal_and_current_queue_answer_is_sent_naturally(self):
        message = FakeMessage()
        message.content = (
            "What did the Journal say about the queue, and is the queue "
            "open right now?"
        )
        response = (
            "The Journal described the queue as the handoff between public "
            "submissions and BARCODE review. Right now, the public queue is "
            "open for submissions."
        )
        run = SimpleNamespace(run_id="mixed-journal-queue-run")
        decision = SimpleNamespace(
            run=run,
            candidate_selected=True,
            fallback_reason="",
        )
        execution = bnl01_bot.OrdinaryChatSinglePacketExecution(
            decision=decision,
            response=response,
            prompt="packet-owned mixed Journal and queue prompt",
            prompt_source_bases=(),
            candidate_active=True,
            provider_call_count=1,
            corrective_call_count=0,
        )
        guard = mock.AsyncMock(
            return_value=(response, {"suppressed": False})
        )
        finalize = mock.AsyncMock(return_value=True)
        with ExitStack() as stack:
            for patcher in self.common_patches():
                stack.enter_context(patcher)
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "apply_guarded_response_regeneration",
                    new=guard,
                )
            )
            stack.enter_context(
                mock.patch.object(
                    bnl01_bot,
                    "safely_finalize_shared_brain_synthesis",
                    new=finalize,
                )
            )

            await bnl01_bot.send_planned_conversation_response(
                message,
                "ignored established response",
                self.plan(),
                prompt="ignored established prompt",
                source_context_available=True,
                allow_model_save=False,
                mark_recent_direct=False,
                ordinary_chat_single_packet_execution=execution,
            )

        self.assertEqual(message.replies, [response])
        self.assertIn("Journal", message.replies[0])
        self.assertIn("Right now", message.replies[0])
        self.assertNotIn("ground that answer cleanly", message.replies[0])
        self.assertNotIn("one specific target", message.replies[0])
        guard.assert_awaited_once()
        finalize.assert_awaited_once()
        self.assertTrue(finalize.await_args.kwargs["response_sent"])
        self.assertTrue(finalize.await_args.kwargs["candidate_live"])


if __name__ == "__main__":
    unittest.main()
