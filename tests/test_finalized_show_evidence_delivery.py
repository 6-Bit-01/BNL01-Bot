"""Authored show evidence informs Gemini without classifying its prose.

These local tests use supported fixture answers. Passing delivery is not a
semantic verdict about arbitrary provider output or a live factuality receipt.
Source revision, scope and consent remain the existing readers' responsibility.
"""

import os
import unittest
from dataclasses import replace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


REQUEST = "Give me some quotes."
SHOW_CONTEXT = (
    "Durable BARCODE Radio show episode memory:\n"
    "Source-linked authored examples:\n"
    '- [tiktok] t+2.0m "Test Member (@test.member)": '
    '"The green lights changed."\n'
    '- [tiktok] t+3.0m "Test Guest (@test.guest)": '
    '"The green lights are bright."\n'
)
PROMPT = "Current user request: " + REQUEST + "\n\n" + SHOW_CONTEXT


def excerpt(
    *,
    event_id="test-event-a",
    subject_ref="tiktok_handle:test.member",
    speaker_label="Test Member (@test.member)",
    source_text="The green lights changed.",
):
    return bot.FinalizedShowAuthoredExcerpt(
        show_key="test-show",
        source_digest="a" * 64,
        event_id=event_id,
        subject_ref=subject_ref,
        speaker_label=speaker_label,
        source_text=source_text,
        surface="tiktok",
    )


def basis(*authored_excerpts, candidate_context=True):
    return bot.FinalizedShowPromptSourceBasis(
        expected_digest="b" * 64,
        rendered_context=SHOW_CONTEXT,
        guild_id=77,
        user_text=REQUEST,
        selection_user_text="Give me a recap of the test show.\n" + REQUEST,
        subject_user_id=42,
        show_keys=("test-show",),
        candidate_context=candidate_context,
        authored_excerpts=tuple(authored_excerpts),
    )


class FinalizedShowTypedEvidenceTests(unittest.TestCase):
    @staticmethod
    def selection(source_text="The green lights changed.", speaker_label="Test Member (@test.member)"):
        return {
            "source_refs": (("test-show", "a" * 64),),
            "user_text": REQUEST,
            "subject_user_id": 42,
            "authored_excerpts": (
                ("test-show", "a" * 64, "test-event-a", "tiktok_handle:test.member",
                 speaker_label, source_text, "tiktok"),
                ("different-show", "d" * 64, "test-event-other", "tiktok_handle:test.guest",
                 "Test Guest (@test.guest)", "A different show's source.", "tiktok"),
            ),
        }

    def test_typed_excerpt_keeps_original_speaker_event_and_source_revision(self):
        selected = bot.build_finalized_show_prompt_source_basis(
            SHOW_CONTEXT, guild_id=77, selection=self.selection(),
        )
        self.assertEqual(selected.authored_excerpts, (excerpt(),))
        self.assertEqual(selected.show_keys, ("test-show",))
        self.assertEqual(selected.rendered_context, SHOW_CONTEXT)

    def test_typed_digest_changes_when_source_text_or_speaker_changes(self):
        original = bot.build_finalized_show_prompt_source_basis(
            SHOW_CONTEXT, guild_id=77, selection=self.selection(),
        )
        for selection in (
            self.selection(source_text="The green lights are bright."),
            self.selection(speaker_label="Test Guest (@test.guest)"),
        ):
            with self.subTest(selection=selection):
                changed = bot.build_finalized_show_prompt_source_basis(
                    SHOW_CONTEXT, guild_id=77, selection=selection,
                )
                self.assertNotEqual(original.expected_digest, changed.expected_digest)


class FinalizedShowEvidenceDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.bases = (
            basis(
                excerpt(),
                excerpt(
                    event_id="test-event-b",
                    subject_ref="tiktok_handle:test.guest",
                    speaker_label="Test Guest (@test.guest)",
                    source_text="The green lights are bright.",
                ),
            ),
        )

    async def assert_delivered_without_rewrite(
        self, answer, *, request=REQUEST, prompt=PROMPT,
        generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
    ):
        provider = mock.AsyncMock()
        with (
            mock.patch.object(
                bot, "refresh_prompt_source_bases",
                return_value=(prompt, self.bases, (), False),
            ) as refresh,
            mock.patch.object(bot, "prompt_source_basis_failure", return_value=""),
            mock.patch.object(
                bot, "get_gemini_response_with_optional_typing", new=provider,
            ),
        ):
            response, diagnostics = await bot.apply_guarded_response_regeneration(
                answer,
                prompt=prompt,
                user_id=42,
                guild_id=77,
                route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="sealed_test",
                current_user_text=request,
                generation_route=generation_route,
                source_context_available=True,
                prompt_source_bases=self.bases,
            )
        self.assertEqual(response, answer)
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_not_awaited()
        refresh.assert_called_once_with(prompt, self.bases)

    async def test_natural_paraphrases_do_not_require_labels_or_lexical_overlap(self):
        for answer in (
            "@test.member said the green lights have changed.",
            "@test.member described a change in the green lighting.",
            "The green lighting shifted, according to @test.member.",
            "Test Member noticed the lights changing; Test Guest found them bright.",
        ):
            with self.subTest(answer=answer):
                await self.assert_delivered_without_rewrite(answer)

    async def test_natural_exact_quote_formatting_does_not_trigger_a_second_call(self):
        for answer in (
            'Test Member wrote, "The green lights changed."',
            'From @test.member: "The green lights changed."',
            '"The green lights changed." — Test Member (@test.member).',
            '@test.member said "The green lights changed." '
            'and @test.guest said "The green lights are bright."',
        ):
            with self.subTest(answer=answer):
                await self.assert_delivered_without_rewrite(answer)

    async def test_continue_keeps_inherited_source_context_without_quote_retry(self):
        prompt = (
            "Earlier human request: Give me some quotes.\n"
            "Current user request: Continue.\n\n" + SHOW_CONTEXT
        )
        await self.assert_delivered_without_rewrite(
            "Test Guest also commented on how bright the green lights were.",
            request="Continue.",
            prompt=prompt,
        )

    async def test_mixed_answer_does_not_assign_every_quote_to_the_show(self):
        prompt = (
            PROMPT + '\nCurrent publication title: "Copper Kite Connections".\n'
            "Current publication summary: The instrumental brought the room together."
        )
        await self.assert_delivered_without_rewrite(
            'The Journal title is "Copper Kite Connections". '
            'In the show chat, @test.member wrote "The green lights changed."',
            request="Give me some quotes from the show and the Journal title.",
            prompt=prompt,
        )

    async def test_same_evidence_is_not_a_wording_gate_on_normal_route(self):
        await self.assert_delivered_without_rewrite(
            "@test.member described a change in the green lighting.",
            generation_route="get_gemini_response",
        )

    async def test_model_authored_specific_uncertainty_is_not_a_canned_fallback(self):
        self.bases = (basis(),)
        await self.assert_delivered_without_rewrite(
            "The episode chronology is available, but these records do not "
            "include an audience comment I can quote exactly."
        )

    async def test_existing_response_repair_does_not_reclassify_natural_show_prose(self):
        answer = "@test.member said the green lights have changed."
        provider = mock.AsyncMock(
            return_value=bot.TrackedGenerationResponse(answer, 1)
        )
        diagnostics = {
            "suppressed": True,
            "suppression_reason": "generic_non_answer_after_retry",
            "response_review_requires_rewrite": True,
        }
        with mock.patch.object(
            bot, "get_tracked_gemini_response_with_optional_typing", new=provider,
        ):
            response, prompt, retained, calls, source_neutral = (
                await bot.resolve_guarded_response_obligation(
                    "", baseline_response="What do you need?",
                    prompt=PROMPT, current_user_text=REQUEST,
                    diagnostics=diagnostics,
                    route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test", user_id=42, guild_id=77,
                    channel=None, prompt_source_bases=self.bases,
                    source_context_available=True,
                    generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                )
            )
        self.assertEqual(response, answer)
        self.assertEqual(retained, self.bases)
        self.assertFalse(source_neutral)
        self.assertEqual(calls, 1)
        provider.assert_awaited_once()
        self.assertIn(SHOW_CONTEXT, prompt)
        self.assertIn(SHOW_CONTEXT, provider.await_args.args[1])
        self.assertEqual(
            provider.await_args.kwargs["route"], bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
        )
        self.assertFalse(provider.await_args.kwargs["allow_style_rewrite"])

    async def test_source_revision_still_rebuilds_context_before_response_delivery(self):
        old_answer = '@test.member wrote "The green lights changed."'
        refreshed = replace(
            self.bases[0], rendered_context="", authored_excerpts=(),
            expected_digest="c" * 64,
        )
        prompt = "Current user request: " + REQUEST
        answer = "The selected show record is no longer available for quoting."
        provider = mock.AsyncMock(return_value=answer)
        with (
            mock.patch.object(
                bot, "refresh_prompt_source_bases",
                return_value=(prompt, (refreshed,), ("show_episode",), False),
            ),
            mock.patch.object(bot, "prompt_source_basis_failure", return_value=""),
            mock.patch.object(
                bot, "get_gemini_response_with_optional_typing", new=provider,
            ),
        ):
            response, diagnostics = await bot.apply_guarded_response_regeneration(
                old_answer, prompt=PROMPT, user_id=42, guild_id=77,
                route_mode=bot.ROUTE_MODE_NORMAL_CHAT, channel_policy="sealed_test",
                current_user_text=REQUEST,
                generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                source_context_available=True, prompt_source_bases=self.bases,
            )
        self.assertEqual(response, answer)
        self.assertTrue(diagnostics["prompt_source_basis_changed"])
        self.assertTrue(diagnostics["prompt_source_basis_regenerated"])
        self.assertFalse(diagnostics["suppressed"])
        provider.assert_awaited_once()
        self.assertIn("SOURCE LIFECYCLE UPDATE", provider.await_args.args[1])
        self.assertNotIn("The green lights changed.", provider.await_args.args[1])


class FinalizedShowDirectDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_direct_assembly_and_send_keep_supported_natural_wording(self):
        from tests import test_public_network_knowledge as network_fixture
        from tests.test_conversation_batching import FakeAuthor, FakeChannel, FakeGuild, FakeMessage

        fixture = network_fixture.PublicNetworkKnowledgeTests()
        await fixture.asyncSetUp()
        try:
            fixture._seed_finalized_show()
            request = "Give me some quotes from the show on 2026-08-28."
            answer = (
                "Alex said the green visuals during the song were wild, "
                "addressing BNL in the show chat."
            )
            for policy in ("sealed_test", "public_home"):
                with self.subTest(policy=policy):
                    prompt, metadata = await fixture._direct_prompt_async(policy, request=request)
                    fixture._assert_show_source(prompt, metadata["prompt_source_bases"])
                    channel = FakeChannel(
                        8890, name="bnl-testing" if policy == "sealed_test" else "barcode-bot",
                        guild=FakeGuild(fixture.guild_id),
                    )
                    message = FakeMessage(
                        channel, request, author=FakeAuthor(user_id=fixture.user_id),
                    )
                    plan = bot.plan_conversation_response(
                        request, policy, route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                        real_direct_target=True, batching_enabled=False,
                        conversation_surface=bot.CONVERSATION_SURFACE_MENTION_OR_REPLY,
                    )
                    provider = mock.AsyncMock()
                    with (
                        mock.patch.object(bot, "_apply_direct_response_pacing", new=mock.AsyncMock()),
                        mock.patch.object(bot, "get_gemini_response_with_optional_typing", new=provider),
                        mock.patch.object(
                            bot, "get_tracked_gemini_response_with_optional_typing", new=provider,
                        ),
                    ):
                        decision = await bot.send_planned_conversation_response(
                            message, answer, plan, prompt=prompt,
                            source_context_available=metadata["source_context_available"],
                            prompt_source_bases=metadata["prompt_source_bases"],
                            mark_recent_direct=False,
                        )
                    self.assertEqual(message.replies, [answer])
                    self.assertTrue(decision.save_conversation)
                    provider.assert_not_awaited()
        finally:
            await fixture.asyncTearDown()


if __name__ == "__main__":
    unittest.main()
