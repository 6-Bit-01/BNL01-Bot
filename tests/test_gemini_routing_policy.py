import unittest
from types import SimpleNamespace
from unittest import mock

import bnl_gemini_routing as routing


class GeminiRoutingPolicyTests(unittest.TestCase):
    def test_defaults_use_stable_full_flash_models(self):
        self.assertEqual(routing.DEFAULT_PRIMARY_MODEL, "gemini-3.6-flash")
        self.assertEqual(routing.DEFAULT_FALLBACK_MODEL, "gemini-3.5-flash")

    def test_journal_gets_largest_output_and_no_automatic_fallback(self):
        journal = routing.policy_for_route("bnl_journal_generation")
        memory = routing.policy_for_route("bnl_memory_preview_candidate")
        chat = routing.policy_for_route("normal_chat")
        self.assertEqual(journal.lane, "journal")
        self.assertGreater(journal.max_output_tokens, memory.max_output_tokens)
        self.assertGreater(memory.max_output_tokens, chat.max_output_tokens)
        self.assertFalse(journal.allow_fallback)
        self.assertFalse(memory.allow_fallback)
        self.assertTrue(chat.allow_fallback)
        self.assertTrue(journal.journal_protected)

    def test_non_journal_routes_preserve_daily_journal_capacity(self):
        with mock.patch.dict(
            "os.environ",
            {"BNL_GEMINI_JOURNAL_PROTECTED_TOKENS": "250000"},
            clear=False,
        ):
            self.assertEqual(
                routing.budget_ceiling_for_route(1_350_000, "normal_chat"),
                1_100_000,
            )
            self.assertEqual(
                routing.budget_ceiling_for_route(
                    1_350_000,
                    "bnl_journal_generation",
                ),
                1_350_000,
            )

    def test_reservation_covers_retries_and_fallback(self):
        chat = routing.policy_for_route("normal_chat")
        single = routing.single_attempt_reservation("abc", chat)
        self.assertEqual(
            routing.estimated_generation_reservation("abc", chat),
            single * 2 * (1 + chat.provider_retries),
        )
        journal = routing.policy_for_route("bnl_journal_generation")
        self.assertEqual(
            routing.estimated_generation_reservation("abc", journal),
            routing.single_attempt_reservation("abc", journal)
            * (1 + journal.provider_retries),
        )

    def test_provider_failures_are_typed(self):
        self.assertEqual(
            routing.provider_failure_kind(SimpleNamespace(status_code=429)),
            routing.ProviderFailureKind.RATE_LIMITED,
        )
        self.assertEqual(
            routing.provider_failure_kind(Exception("503 service unavailable")),
            routing.ProviderFailureKind.SERVER,
        )
        self.assertEqual(
            routing.provider_failure_kind(
                Exception("404 model is no longer available")
            ),
            routing.ProviderFailureKind.MODEL_UNAVAILABLE,
        )
        self.assertEqual(
            routing.provider_failure_kind(Exception("400 INVALID_ARGUMENT")),
            routing.ProviderFailureKind.INVALID_REQUEST,
        )


if __name__ == "__main__":
    unittest.main()
