import os
from contextlib import ExitStack
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


if __name__ == "__main__":
    unittest.main()
