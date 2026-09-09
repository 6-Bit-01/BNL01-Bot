"""Deterministic show speaker/wording checks over typed authored events."""

import os
import unittest
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot


REQUEST = "Give me some quotes."
PROMPT = (
    "Current user request: Give me some quotes.\n\n"
    "Durable BARCODE Radio show episode memory:\n"
    "Source-linked authored examples:\n"
    '- [tiktok] t+2.0m "Test Member (@test.member)": '
    '"The green lights changed."\n'
    '- [tiktok] t+3.0m "Test Guest (@test.guest)": '
    '"The green lights are bright."\n'
)


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
        rendered_context=PROMPT,
        guild_id=77,
        user_text=REQUEST,
        selection_user_text=(
            "Give me a recap of the test show.\n" + REQUEST
        ),
        subject_user_id=42,
        show_keys=("test-show",),
        candidate_context=candidate_context,
        authored_excerpts=tuple(authored_excerpts),
    )


class FinalizedShowAuthoredResponseValidatorTests(unittest.TestCase):
    def setUp(self):
        self.member = excerpt()
        self.guest = excerpt(
            event_id="test-event-b",
            subject_ref="tiktok_handle:test.guest",
            speaker_label="Test Guest (@test.guest)",
            source_text="The green lights are bright.",
        )
        self.bases = (basis(self.member, self.guest),)

    def failure(self, response, request=REQUEST, bases=None):
        return bot.finalized_show_authored_response_failure(
            response,
            current_user_text=request,
            prompt_source_bases=self.bases if bases is None else bases,
        )

    def test_rejects_fabricated_words_and_participant(self):
        self.assertEqual(
            self.failure(
                'TestBeacon: "The paper lantern blinked twice."'
            ),
            "show_authored_quote_not_in_supplied_event",
        )
        self.assertEqual(
            self.failure("TestBeacon said the lights were blue."),
            "show_authored_participant_not_in_supplied_events",
        )

    def test_rejects_real_words_with_wrong_or_missing_speaker(self):
        self.assertEqual(
            self.failure(
                'Test Guest (@test.guest): "The green lights changed."'
            ),
            "show_authored_quote_speaker_mismatch",
        )
        self.assertEqual(
            self.failure('The exact comment was "The green lights changed."'),
            "show_authored_quote_missing_speaker",
        )

    def test_accepts_only_the_same_source_event_pair(self):
        self.assertEqual(
            self.failure(
                '@test.member: "The green lights changed."\n'
                '"The green lights are bright." — @test.guest.'
            ),
            "",
        )
        self.assertEqual(
            self.failure(
                "@test.member commented that the green lights changed."
            ),
            "show_authored_wording_requires_quote_or_labeled_gist",
        )
        self.assertEqual(
            self.failure(
                "As a paraphrase, @test.member commented that the green "
                "lights changed."
            ),
            "",
        )

    def test_known_speaker_cannot_receive_an_ungrounded_unquoted_claim(self):
        self.assertEqual(
            self.failure(
                "As a paraphrase, @test.member said the paper lantern "
                "blinked twice."
            ),
            "show_authored_attribution_not_supported",
        )

    def test_same_display_name_requires_the_source_handle_for_exact_words(self):
        one = excerpt(
            speaker_label="Test Member (@signal.one)",
            source_text="Signal one stayed green.",
        )
        two = excerpt(
            event_id="test-event-two",
            subject_ref="tiktok_handle:signal.two",
            speaker_label="Test Member (@signal.two)",
            source_text="Signal two changed blue.",
        )
        same_name_bases = (basis(one, two),)
        self.assertEqual(
            self.failure(
                'Test Member: "Signal one stayed green."',
                bases=same_name_bases,
            ),
            "show_authored_quote_speaker_mismatch",
        )
        self.assertEqual(
            self.failure(
                '"Signal one stayed green." — @signal.two.',
                bases=same_name_bases,
            ),
            "show_authored_quote_speaker_mismatch",
        )
        self.assertEqual(
            self.failure(
                '"Signal one stayed green." — @signal.one.',
                bases=same_name_bases,
            ),
            "",
        )

    def test_empty_bounded_events_allow_uncertainty_but_not_an_invented_quote(self):
        empty_bases = (basis(),)
        self.assertEqual(
            self.failure(
                "I can't verify an exact audience quote from the bounded "
                "events, but the episode chronology is still available.",
                bases=empty_bases,
            ),
            "",
        )
        self.assertEqual(
            self.failure(
                'TestBeacon: "The paper lantern blinked twice."',
                bases=empty_bases,
            ),
            "show_authored_quote_not_in_supplied_event",
        )

    def test_candidate_show_does_not_capture_an_unrelated_current_request(self):
        self.assertEqual(
            self.failure(
                'A checksum may label a state "valid" after comparison.',
                request="Explain checksum detection.",
            ),
            "",
        )
        self.assertEqual(
            self.failure(
                'TestBeacon: "The paper lantern blinked twice."',
                request="Continue.",
            ),
            "show_authored_quote_not_in_supplied_event",
        )


class FinalizedShowAuthoredResponseLifecycleTests(
    unittest.IsolatedAsyncioTestCase
):
    def setUp(self):
        self.basis = basis(excerpt())
        self.bases = (self.basis,)

    async def test_single_packet_guard_regenerates_the_reported_failure_class(self):
        supported = '@test.member: "The green lights changed."'
        regenerate = mock.AsyncMock(return_value=supported)
        with (
            mock.patch.object(
                bot,
                "refresh_prompt_source_bases",
                return_value=(PROMPT, self.bases, (), False),
            ),
            mock.patch.object(bot, "prompt_source_basis_failure", return_value=""),
            mock.patch.object(
                bot,
                "get_gemini_response_with_optional_typing",
                new=regenerate,
            ),
        ):
            response, diagnostics = await bot.apply_guarded_response_regeneration(
                'TestBeacon: "The paper lantern blinked twice."',
                prompt=PROMPT,
                user_id=42,
                guild_id=77,
                route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                channel_policy="sealed_test",
                current_user_text=REQUEST,
                generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                source_context_available=True,
                prompt_source_bases=self.bases,
            )
        self.assertEqual(response, supported)
        self.assertTrue(
            diagnostics["show_authored_evidence_guard_triggered"]
        )
        self.assertTrue(diagnostics["show_authored_evidence_regenerated"])
        self.assertEqual(diagnostics["show_authored_evidence_guard_reason"], "")
        regenerate.assert_awaited_once()
        correction_prompt = regenerate.await_args.args[1]
        self.assertIn("SHOW-AUTHORED EVIDENCE CORRECTION REQUIRED", correction_prompt)
        self.assertIn("same event's speaker", correction_prompt)
        self.assertFalse(regenerate.await_args.kwargs["allow_style_rewrite"])

    async def test_obligation_rewrite_never_releases_repeated_fabrication(self):
        fabricated = 'TestBeacon: "The paper lantern blinked twice."'
        regenerate = mock.AsyncMock(
            return_value=bot.TrackedGenerationResponse(fabricated, 1)
        )
        diagnostics = {
            "suppressed": True,
            "suppression_reason": "show_authored_quote_not_in_supplied_event",
            "response_review_requires_rewrite": True,
        }
        with mock.patch.object(
            bot,
            "get_tracked_gemini_response_with_optional_typing",
            new=regenerate,
        ):
            response, _prompt, retained, calls, source_neutral = (
                await bot.resolve_guarded_response_obligation(
                    fabricated,
                    baseline_response=fabricated,
                    prompt=PROMPT,
                    current_user_text=REQUEST,
                    diagnostics=diagnostics,
                    route_mode=bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="sealed_test",
                    user_id=42,
                    guild_id=77,
                    channel=None,
                    prompt_source_bases=self.bases,
                    source_context_available=True,
                    generation_route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                )
            )
        self.assertEqual(response, "")
        self.assertEqual(retained, self.bases)
        self.assertEqual(calls, 2)
        self.assertFalse(source_neutral)
        self.assertEqual(regenerate.await_count, 2)


if __name__ == "__main__":
    unittest.main()
