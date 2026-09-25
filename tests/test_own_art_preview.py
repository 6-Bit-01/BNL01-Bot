"""Self-directed art is private, optional, accounted, and never a commission."""
import base64
from datetime import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from zoneinfo import ZoneInfo

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")
import bnl01_bot as bot
import bnl_own_art as art
from bnl_gemini_cost import estimate_gemini_cost
from bnl_gemini_routing import OWN_ART_CONCEPT_ROUTE, OWN_ART_IMAGE_MODEL, OWN_ART_IMAGE_ROUTE, policy_for_route

PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC")
CONCEPT = {"action": "create", "title": "A quiet room", "meaning": "A place I imagined.",
           "imagePrompt": "One empty room floating above an imagined ocean.", "inspirationRefs": []}


def provider_payload():
    return {"status": "completed", "steps": [{"type": "model_output", "content": [
        {"type": "image", "mime_type": "image/png", "data": base64.b64encode(PNG).decode("ascii")}
    ]}], "usage": {"total_input_tokens": 50, "total_output_tokens": 1120,
                   "total_thought_tokens": 10, "total_cached_tokens": 0, "total_tokens": 1180}}


class OwnArtPreviewTests(unittest.TestCase):
    def test_brief_uses_public_projection_and_allows_imagination_and_silence(self):
        prompt, refs = art.build_own_art_brief({
            "safeSources": [{"refId": "fresh:1", "summary": "A public track note.", "observedAt": "2026-09-25T00:00:00Z"}],
            "privateSources": [{"summary": "PRIVATE_FIXTURE_DO_NOT_USE"}],
            "raw_text": "PRIVATE_FIXTURE_DO_NOT_USE",
        })
        self.assertNotIn("PRIVATE_FIXTURE_DO_NOT_USE", prompt)
        self.assertEqual(refs, {"fresh:1"})
        self.assertEqual(art.parse_own_art_concept(json.dumps(CONCEPT), refs)["action"], "create")
        self.assertEqual(art.parse_own_art_concept('{"action":"skip","reason":"Nothing I want to make yet."}', refs)["action"], "skip")

    def test_no_required_style_words_and_no_invented_source_refs(self):
        value = {**CONCEPT, "imagePrompt": "An orange circle."}
        self.assertEqual(art.parse_own_art_concept(json.dumps(value), set())["imagePrompt"], "An orange circle.")
        with self.assertRaisesRegex(ValueError, "source_refs"):
            art.parse_own_art_concept(json.dumps({**value, "inspirationRefs": ["made-up"]}), set())

    def test_image_request_is_stateless_and_has_no_tools_or_community_inputs(self):
        request = art.own_art_image_request(CONCEPT["imagePrompt"])
        self.assertFalse(request["store"])
        self.assertNotIn("tools", request)
        self.assertEqual(request["model"], OWN_ART_IMAGE_MODEL)
        for route in (OWN_ART_CONCEPT_ROUTE, OWN_ART_IMAGE_ROUTE):
            self.assertEqual(policy_for_route(route).provider_retries, 0)
            self.assertFalse(policy_for_route(route).allow_fallback)
            self.assertEqual(policy_for_route(route).lane, "background")

    def test_decode_requires_one_completed_inline_png(self):
        self.assertEqual(art.extract_generated_png(provider_payload()), PNG)
        for altered in ({"status": "requires_action"}, {"status": "completed", "steps": []}):
            with self.assertRaises(ValueError):
                art.extract_generated_png(altered)
        payload = provider_payload()
        payload["steps"][0]["content"][0] = {"type": "image", "mime_type": "image/png", "uri": "https://example.invalid/image"}
        with self.assertRaises(ValueError):
            art.extract_generated_png(payload)

    def test_usage_is_required_and_output_price_is_an_explicit_upper_bound(self):
        with self.assertRaises(ValueError):
            art.image_usage_response({})
        estimate = estimate_gemini_cost(OWN_ART_IMAGE_MODEL, prompt_tokens=50, candidate_tokens=1120,
                                       thought_tokens=10, total_tokens=1180, at=datetime(2026, 9, 25))
        self.assertEqual(estimate.estimated_cost_usd, Decimal("0.067825"))
        self.assertIn("upper_bound", estimate.price.source)
        with mock.patch.object(bot, "_pacific_now", return_value=datetime(2026, 9, 25, tzinfo=ZoneInfo("America/Los_Angeles"))):
            self.assertGreater(bot._estimated_request_cost_nanos("art", OWN_ART_IMAGE_ROUTE),
                               bot._estimated_request_cost_nanos("art", OWN_ART_CONCEPT_ROUTE))

    def fake_bot(self, db_file):
        fake = SimpleNamespace(
            DB_FILE=db_file, BNL_PRIMARY_GUILD_ID=123, GEMINI_API_KEY="fixture-key",
            BNL01_PACKET_OWNED_SYSTEM_PROMPT="BNL shared character context",
            ProviderAttemptCounter=bot.ProviderAttemptCounter,
            reserve_local_model_budget=mock.Mock(return_value=SimpleNamespace(cost_reservation_id="fixture")),
            release_local_model_budget=mock.Mock(), record_generation_token_usage=mock.Mock(),
            record_failed_generation_attempt=mock.Mock(),
        )
        fake._generate_gemini_content_with_fallback = mock.Mock()
        fake._extract_text_and_tokens = mock.Mock(return_value=(json.dumps(CONCEPT), 5))
        return fake

    def test_default_preparation_has_zero_provider_calls(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "existing.sqlite"
            db.touch()
            fake = self.fake_bot(str(db))
            with mock.patch.object(art, "build_source_packet", return_value={"safeSources": []}):
                receipt = art.prepare_private_preview(fake, str(Path(folder) / "preview"))
            self.assertFalse(receipt["published"])
            self.assertEqual(receipt["status"], "prepared_only")
            fake._generate_gemini_content_with_fallback.assert_not_called()
            fake.reserve_local_model_budget.assert_not_called()

    def test_bnl_can_decline_without_image_request(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "existing.sqlite"
            db.touch()
            fake = self.fake_bot(str(db))
            def concept_call(*args, **kwargs):
                kwargs["attempt_counter"].mark_started()
            fake._generate_gemini_content_with_fallback.side_effect = concept_call
            fake._extract_text_and_tokens.return_value = ('{"action":"skip","reason":"No image today."}', 10)
            with mock.patch.object(art, "build_source_packet", return_value={}), mock.patch.object(art, "generate_private_image") as image:
                receipt = art.prepare_private_preview(fake, str(Path(folder) / "preview"), generate=True)
            image.assert_not_called()
            self.assertEqual(receipt["status"], "bnl_chose_not_to_create")
            self.assertEqual(receipt["conceptCalls"], 1)
            self.assertEqual(receipt["imageCalls"], 0)

    def test_actual_provider_boundary_reserves_and_accounts_once(self):
        fake = self.fake_bot("unused")
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(provider_payload()).encode()
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
            data, receipt = art.generate_private_image(fake, "A quiet orange room.")
        self.assertEqual(data, PNG)
        self.assertEqual(receipt["providerCalls"], 1)
        fake.reserve_local_model_budget.assert_called_once_with("A quiet orange room.", OWN_ART_IMAGE_ROUTE)
        fake.record_generation_token_usage.assert_called_once()
        fake.release_local_model_budget.assert_called_once_with(fake.reserve_local_model_budget.return_value, retain_cost_reservation=False)
        self.assertEqual(opener.open.call_count, 1)

    def test_budget_denial_makes_no_provider_call(self):
        fake = self.fake_bot("unused")
        fake.reserve_local_model_budget.side_effect = bot.LocalModelBudgetExhausted("fixture_denied")
        with mock.patch.object(art.urllib.request, "build_opener") as opener:
            with self.assertRaises(bot.LocalModelBudgetExhausted):
                art.generate_private_image(fake, "An orange room.")
        opener.assert_not_called()

    def test_failure_does_not_retry_and_keeps_unresolved_cost_reserved(self):
        fake = self.fake_bot("unused")
        opener = mock.Mock()
        opener.open.side_effect = TimeoutError("PRIVATE_TRANSPORT_DETAILS")
        counter = bot.ProviderAttemptCounter()
        with mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
            with self.assertRaisesRegex(RuntimeError, "^art_image_provider_request_failed$"):
                art.generate_private_image(fake, "A quiet room.", attempt_counter=counter)
        self.assertEqual(counter.count, 1)
        self.assertEqual(opener.open.call_count, 1)
        fake.release_local_model_budget.assert_called_once_with(fake.reserve_local_model_budget.return_value, retain_cost_reservation=True)

    def test_complete_preview_is_private_file_only_and_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "existing.sqlite"
            db.touch()
            fake = self.fake_bot(str(db))
            fake._generate_gemini_content_with_fallback.side_effect = lambda *a, **kw: kw["attempt_counter"].mark_started()
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps(provider_payload()).encode()
            opener = mock.Mock()
            opener.open.return_value = response
            target = Path(folder) / "preview"
            with mock.patch.object(art, "build_source_packet", return_value={}), mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
                receipt = art.prepare_private_preview(fake, str(target), generate=True)
            self.assertEqual(receipt["status"], "private_draft_ready")
            self.assertEqual((receipt["conceptCalls"], receipt["imageCalls"]), (1, 1))
            self.assertFalse(receipt["published"])
            self.assertEqual((target / "bnl-own-art.png").stat().st_mode & 0o777, 0o600)
            self.assertEqual(target.stat().st_mode & 0o777, 0o700)
            self.assertEqual((target / "bnl-own-art.png").read_bytes(), PNG)
            with self.assertRaises(FileExistsError):
                art.prepare_private_preview(fake, str(target))
