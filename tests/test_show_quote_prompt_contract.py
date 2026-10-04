"""Provider-bound source provenance; these are not model-quality tests."""

import os
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot as bot
from bnl_tiktok_live_context import build_durable_show_prompt_context


AUTHORSHIP_PROVENANCE = (
    "Authored excerpts retain their original speaker and event. Summaries, "
    "participant lists, track titles, and prior BNL replies are not audience "
    "transcripts."
)
BOUNDED_COVERAGE = (
    "The supplied selection can be incomplete; absence here is not proof "
    "of absence."
)
PRIOR_BNL_PROVENANCE = (
    "Prior BNL replies document BNL's claims, not independent source confirmation."
)


def show_context(events_available=True):
    def stamp(value):
        return int(datetime.fromisoformat(value).timestamp() * 1000)

    show = {
        "sessionId": "test-quote-show",
        "title": "Test Broadcast",
        "showDate": "2026-08-28",
        "status": "archived",
        "milestones": [
            {"sequence": 1, "eventType": "broadcast_started",
             "occurredAt": "2026-08-29T00:00:00+00:00"},
            {"sequence": 2, "eventType": "session_archived",
             "occurredAt": "2026-08-29T00:10:00+00:00"},
        ],
    }
    events = [
        {
            "event_id": "test-event-a",
            "occurred_at_ms": stamp("2026-08-29T00:02:00+00:00"),
            "subject_ref": "tiktok_handle:test.member",
            "private_display_name": "Test Member",
            "raw_text": "The green lights changed.",
            "metadata": {"eventType": "comment", "handle": "test.member"},
        },
        {
            "event_id": "test-event-b",
            "occurred_at_ms": stamp("2026-08-29T00:03:00+00:00"),
            "subject_ref": "tiktok_handle:test.guest",
            "private_display_name": "Test Guest",
            "raw_text": "The green lights are bright.",
            "metadata": {"eventType": "comment", "handle": "test.guest"},
        },
    ]
    return build_durable_show_prompt_context(
        {"latestShow": show, "shows": []},
        events if events_available else None,
        "What did people discuss in the last show?",
    )


class ShowQuoteProviderContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_self_state_and_aspiration_contract_reaches_both_provider_routes(self):
        cases = (
            (
                "What do you want to be when you grow up?",
                "Historical BNL reply: My memory is fully operational after a recalibration.",
                "I'd like to become a signal that helps people find their next favorite track.",
            ),
            (
                "Are all your memory repairs complete now?",
                "Fresh eligible operational evidence: the latest reply was delivered; broader repair status is unknown.",
                "I can answer this turn, but the broader repair status isn't established.",
            ),
        )
        for request, source_text, response in cases:
            source = SimpleNamespace(rendered_context=source_text)
            with mock.patch.object(bot, "refresh_prompt_source_basis", return_value=(source, False)):
                rebuilt, bases, neutral = bot.build_ordinary_chat_response_repair_prompt(
                    "An outdated source block.", reason="prompt_source_changed",
                    prompt_source_bases=(source,), current_user_text=request,
                )
            self.assertEqual(bases, (source,))
            self.assertFalse(neutral)
            for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
                for prompt in (source_text + "\nCurrent user request: " + request, rebuilt):
                    with self.subTest(request=request, route=route, rebuilt=prompt == rebuilt):
                        generate = mock.AsyncMock(return_value=bot.GenerationResult(
                            True, response, route=route,
                        ))
                        with (
                            mock.patch.object(bot, "check_quota_availability", return_value=True),
                            mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                        ):
                            actual = await bot.get_gemini_response(
                                prompt, 101, 1, route=route,
                                source_context_available=True, allow_style_rewrite=False,
                            )
                        generate.assert_awaited_once()
                        self.assertEqual(actual, response)
                        sent = " ".join(generate.await_args.args[0].split())
                        self.assertIn(request, sent)
                        self.assertIn(source_text, sent)
                        for contract in (
                            "Current operational health or a completed repair needs fresh eligible evidence",
                            "Earlier BNL status or repair replies are historical context, not diagnostic proof",
                            "Aspirations, wishes, and banter may be imaginative and in-world",
                            "not approval for autonomy or control, or proof that a change has happened",
                        ):
                            self.assertTrue(contract in sent, "Missing provider-bound contract: " + contract)
                        self.assertNotIn("You are functioning as intended.", sent)
                        self.assert_no_response_form_mandates(sent)

    async def test_outcome_distinctions_reach_initial_and_source_rebuilt_replies(self):
        # Use a proposal and an explicit reported outcome together: the rule
        # must not turn all negative statements into blanket uncertainty.
        facts = (
            'Test Member: "Maybe I should give away my spare mixer."\n'
            'Test Guest: "I kept my mixer."'
        )
        request = "What do those messages establish happened?"
        source = SimpleNamespace(rendered_context=facts)
        with mock.patch.object(bot, "refresh_prompt_source_basis", return_value=(source, False)):
            repaired, bases, neutral = bot.build_ordinary_chat_response_repair_prompt(
                "An outdated source block.", reason="prompt_source_changed",
                prompt_source_bases=(source,), current_user_text=request,
            )
        self.assertEqual(bases, (source,))
        self.assertFalse(neutral)
        self.assertNotIn("An outdated source block.", repaired)
        self.assertIn(bot.EVIDENCE_OUTCOME_RULE, repaired)
        prompts = (facts + "\nCurrent user request: " + request, repaired)
        for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
            for prompt in prompts:
                with self.subTest(route=route, rebuilt=prompt == repaired):
                    generate = mock.AsyncMock(return_value=bot.GenerationResult(
                        True, "A source-grounded answer.", route=route,
                    ))
                    with (
                        mock.patch.object(bot, "check_quota_availability", return_value=True),
                        mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                    ):
                        await bot.get_gemini_response(
                            prompt, 101, 1, route=route,
                            source_context_available=True, allow_style_rewrite=False,
                        )
                    generate.assert_awaited_once()
                    sent = " ".join(generate.await_args.args[0].split())
                    self.assertIn(" ".join(facts.split()), sent)
                    self.assertIn(request, sent)
                    self.assertIn(bot.EVIDENCE_OUTCOME_RULE, sent)
                    self.assertIn("Preserve supported outcomes and explicit denials", sent)
                    self.assertIn("do not replace an unsupported positive with an unsupported negative", sent)
                    self.assert_no_response_form_mandates(sent)

    def assert_no_response_form_mandates(self, prompt):
        normalized = " ".join(prompt.casefold().split())
        for wording_mandate in (
            "label a paraphrase as a summary",
            "explicitly labeled as a gist",
            "any non-exact wording must",
            "every word must overlap",
        ):
            self.assertNotIn(wording_mandate, normalized)

    async def test_authored_pairs_and_provenance_reach_both_provider_routes(self):
        context = show_context()
        batch = bot._format_batched_prompt(
            [("Test Member", "Give me some quotes.")], "balanced", "",
        )
        prompt = (
            context + "\n"
            + bot.build_tiktok_show_analysis_turn_contract(context)
            + batch
        )
        authored_pairs = (
            'Test Member (@test.member): "The green lights changed."',
            'Test Guest (@test.guest): "The green lights are bright."',
        )
        for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
            with self.subTest(route=route):
                generate = mock.AsyncMock(
                    return_value=bot.GenerationResult(True, "A supported answer.", route=route)
                )
                with (
                    mock.patch.object(bot, "check_quota_availability", return_value=True),
                    mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True),
                    mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                ):
                    await bot.get_gemini_response(
                        prompt, 101, 1, route=route,
                        source_context_available=True, allow_style_rewrite=False,
                    )
                generate.assert_awaited_once()
                request = " ".join(generate.await_args.args[0].split())
                for pair in authored_pairs:
                    self.assertIn(pair, request)
                self.assertIn(AUTHORSHIP_PROVENANCE, request)
                self.assertIn(BOUNDED_COVERAGE, request)
                self.assertIn(PRIOR_BNL_PROVENANCE, request)
                self.assert_no_response_form_mandates(request)
                self.assertNotIn("Exact wording is allowed only when a typed", request)
                self.assertNotIn("Does not repeat from its database verbatim", request)
                self.assertFalse(bot.is_consequential_exact_quote_request("Give me some quotes."))

    async def test_missing_archive_keeps_specific_uncertainty_at_provider_boundary(self):
        context = show_context(events_available=False)
        generate = mock.AsyncMock(return_value=bot.GenerationResult(True, "I cannot verify that quote."))
        with (
            mock.patch.object(bot, "check_quota_availability", return_value=True),
            mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
        ):
            await bot.get_gemini_response(
                context + "\nCurrent user request: Give me some quotes.",
                101, 1, route=bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE,
                source_context_available=True,
            )
        request = " ".join(generate.await_args.args[0].split())
        self.assertIn("durable TikTok event archive could not be read", request)
        self.assertIn("When one exact fact is unavailable, answer everything else that is supported", request)
        self.assertIn(BOUNDED_COVERAGE, request)
        self.assert_no_response_form_mandates(request)
        self.assertNotIn("The green lights changed.", request)

    async def test_creative_followup_cannot_treat_prior_bnl_pronouns_as_identity_evidence(self):
        # Check the actual provider inputs, including the no-packet fallback.
        # This proves transport of the constraint, not the model's compliance.
        original = 'Test Listener: "She turned on the green lights."'
        self_identification = 'Test Guest: "I use he/him pronouns."'
        prior = 'Prior BNL reply: "Test Listener said she was teaching pottery."'
        prompt = (original + "\n" + self_identification + "\n" + prior
                  + "\nWrite two imaginative lyrics, then separate chat facts from imagery.")
        for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
            with self.subTest(route=route):
                generate = mock.AsyncMock(return_value=bot.GenerationResult(True, "A supported answer.", route=route))
                with (
                    mock.patch.object(bot, "check_quota_availability", return_value=True),
                    mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                ):
                    await bot.get_gemini_response(prompt, 101, 1, route=route,
                                                  source_context_available=True, allow_style_rewrite=False)
                generate.assert_awaited_once()
                request = " ".join(generate.await_args.args[0].split())
                for text in (original, self_identification, prior):
                    self.assertIn(text, request)
                self.assertIn("Earlier BNL wording is not independent identity evidence.", request)
                self.assertIn("invent imagery, not personal attributes", request)
                self.assertIn("A pronoun referring to someone else inside a quotation does not establish the speaker's own pronouns", request)
                self.assertIn("supported attribution and explicit self-identification", request)
                self.assertIn("Missing confirmation supports uncertainty, not a categorical claim", request)
                self.assertIn("A joke, suggestion, or proposal establishes what was said", request)
                self.assert_no_response_form_mandates(request)

    async def test_each_optional_style_provider_preserves_facts_and_attribution(self):
        original = 'Test Member said, "The green lights changed."'
        for expected_route, rolls in (
            ("glitch_rewrite", [0.0, 1.0]),
            ("cross_universe_bleed", [1.0, 0.0]),
        ):
            with self.subTest(route=expected_route):
                generate = mock.AsyncMock(return_value=bot.GenerationResult(True, original))
                style = mock.AsyncMock(return_value=SimpleNamespace(
                    candidates=[SimpleNamespace(content=SimpleNamespace(
                        parts=[SimpleNamespace(text=original)],
                    ))],
                    usage_metadata=SimpleNamespace(total_token_count=4),
                ))
                with (
                    mock.patch.object(bot, "check_quota_availability", return_value=True),
                    mock.patch.object(bot, "conversation_context_v2_enabled", return_value=True),
                    mock.patch.object(bot, "_generate_gemini_content_result_async", generate),
                    mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", style),
                    mock.patch.object(bot.random, "random", side_effect=rolls),
                ):
                    await bot.get_gemini_response(
                        show_context() + "\nCurrent user request: Give me some quotes.",
                        101, 1, source_context_available=True,
                    )
                style.assert_awaited_once()
                self.assertEqual(style.await_args.args[1], expected_route)
                request = style.await_args.args[0]
                self.assertIn(original, request)
                self.assertIn("Style changes must preserve factual content and source attribution.", request)
                self.assertIn("Earlier BNL wording is not independent identity evidence.", request)
                self.assertIn("invent imagery, not personal attributes", request)
                self.assertIn("Missing confirmation supports uncertainty, not a categorical claim", request)
                self.assert_no_response_form_mandates(request)

    def test_final_episode_contract_preserves_source_roles_and_scoped_uncertainty(self):
        context = (
            "Durable BARCODE Radio show episode memory:\n"
            "Source-linked authored examples:\n"
            '- [tiktok] t+2.0m "Test Member": "The green lights changed."\n'
            "Public Discord interactions with BNL during this episode:\n"
            '  BNL replied: "Test Phantom said the lights were blue."\n'
        )
        contract = bot.build_tiktok_show_episode_turn_contract(context)
        normalized = " ".join(contract.split())
        self.assertIn(AUTHORSHIP_PROVENANCE, normalized)
        self.assertIn(BOUNDED_COVERAGE, normalized)
        self.assertIn(PRIOR_BNL_PROVENANCE, normalized)
        self.assertIn(bot.EVIDENCE_OUTCOME_RULE, normalized)
        self.assert_no_response_form_mandates(contract)
        self.assertEqual(bot.build_tiktok_show_episode_turn_contract(""), "")
        self.assertEqual(bot.build_tiktok_show_episode_turn_contract("Current queue state: closed"), "")


if __name__ == "__main__":
    unittest.main()
