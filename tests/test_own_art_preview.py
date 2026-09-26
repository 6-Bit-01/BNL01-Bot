"""Self-directed art is private, optional, accounted, and never a commission."""
import base64
import io
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
JPEG = base64.b64decode(json.loads((Path(__file__).parent / "fixtures/bnl-art-jpeg.json").read_text())["jpegBase64"])


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

    def test_request_matches_google_published_image_format_schema(self):
        fixture = Path(__file__).parent / "fixtures/google_interactions_image_format_2026_09_25.json"
        schema = json.loads(fixture.read_text())["schema"]
        request = art.own_art_image_request(CONCEPT["imagePrompt"])
        output = request["response_format"]
        self.assertNotIn("mime_type", output)
        self.assertLessEqual(set(schema["required"]), set(output))
        self.assertLessEqual(set(output), set(schema["properties"]))
        for key, value in output.items():
            field = schema["properties"][key]
            if "enum" in field:
                self.assertIn(value, field["enum"], key)
            if "const" in field:
                self.assertEqual(value, field["const"], key)
        self.assertNotIn("delivery", output)
        self.assertNotIn("image/png", schema["properties"]["mime_type"]["enum"])

    def test_usage_allows_omitted_zero_counters_but_requires_accounting_totals(self):
        payload = {"usage": {"total_tokens": 1130, "total_input_tokens": 10, "total_output_tokens": 1120}}
        usage = art.image_usage_response(payload).usage_metadata
        self.assertEqual(usage.thoughts_token_count, 0)
        self.assertEqual(usage.cached_content_token_count, 0)
        for key in ("total_tokens", "total_input_tokens", "total_output_tokens"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                art.image_usage_response({"usage": {k: v for k, v in payload["usage"].items() if k != key}})
        with self.assertRaises(ValueError):
            art.image_usage_response({"usage": {**payload["usage"], "total_thought_tokens": 10}})

    def test_decode_requires_one_completed_inline_png(self):
        self.assertEqual(art.extract_generated_image(provider_payload())[0], PNG)
        for altered in ({"status": "requires_action"}, {"status": "completed", "steps": []}):
            with self.assertRaises(ValueError):
                art.extract_generated_image(altered)
        payload = provider_payload()
        payload["steps"][0]["content"][0] = {"type": "image", "mime_type": "image/png", "uri": "https://example.invalid/image"}
        with self.assertRaises(ValueError):
            art.extract_generated_image(payload)

    def test_decode_preserves_jpeg_and_png_bytes_and_checks_declared_type(self):
        for data, mime, dimensions in ((PNG, "image/png", (1, 1)), (JPEG, "image/jpeg", (2, 3))):
            with self.subTest(mime=mime):
                payload = provider_payload()
                part = payload["steps"][0]["content"][0]
                part.update(data=base64.b64encode(data).decode(), mime_type=mime)
                decoded, info = art.extract_generated_image(payload)
                self.assertEqual(decoded, data)
                self.assertEqual(info, {"mimeType": mime, "width": dimensions[0], "height": dimensions[1]})
                part["mime_type"] = "image/jpeg" if mime == "image/png" else "image/png"
                with self.assertRaisesRegex(ValueError, "mime_mismatch"):
                    art.extract_generated_image(payload)
        for data in (JPEG[:30], JPEG[:-2], b'<svg>' + b'x' * 40):
            with self.assertRaises(ValueError):
                art.image_info(data)

    def test_jpeg_private_preview_uses_jpg_filename_and_original_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "existing.sqlite"
            db.touch()
            fake = self.fake_bot(str(db))
            fake._generate_gemini_content_with_fallback.side_effect = lambda *a, **kw: kw["attempt_counter"].mark_started()
            payload = provider_payload()
            payload["steps"][0]["content"][0].update(mime_type="image/jpeg", data=base64.b64encode(JPEG).decode())
            response = mock.MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps(payload).encode()
            opener = mock.Mock()
            opener.open.return_value = response
            target = Path(folder) / "preview"
            with mock.patch.object(art, "build_source_packet", return_value={}), mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
                receipt = art.prepare_private_preview(fake, str(target), generate=True)
            self.assertEqual(receipt["image"]["mimeType"], "image/jpeg")
            self.assertEqual(receipt["image"]["fileName"], "bnl-own-art.jpg")
            self.assertEqual((target / "bnl-own-art.jpg").read_bytes(), JPEG)
            self.assertFalse((target / "bnl-own-art.png").exists())

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

        def provider_response(request, **kwargs):
            # Reproduce the live 400: schema-valid delivery overrides fail.
            body = json.loads(request.data)
            if "delivery" in body.get("response_format", {}):
                raw = b'{"error":{"message":"Image delivery mode is not supported.","code":"invalid_request"}}'
                raise art.urllib.error.HTTPError(art.IMAGE_ENDPOINT, 400, "bad", {}, io.BytesIO(raw))
            return response

        opener.open.side_effect = provider_response
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

    def test_explicit_invalid_request_releases_estimate_and_preserves_safe_diagnostic(self):
        fake = self.fake_bot("unused")
        prompt = "A private fixture concept."
        body = {"error": {"code": 400, "status": "INVALID_ARGUMENT", "message":
                "Unsupported mime_type image/png; fixture-key; " + prompt,
                "private": "PRIVATE_FIELD_MUST_NOT_APPEAR"}}
        error = art.urllib.error.HTTPError(art.IMAGE_ENDPOINT, 400, "bad", {}, io.BytesIO(json.dumps(body).encode()))
        opener = mock.Mock()
        opener.open.side_effect = error
        counter = bot.ProviderAttemptCounter()
        with mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
            with self.assertRaises(RuntimeError) as caught:
                art.generate_private_image(fake, prompt, attempt_counter=counter)
        self.assertEqual(counter.count, 1)
        self.assertEqual(opener.open.call_count, 1)
        self.assertEqual(caught.exception.status_code, 400)
        detail = caught.exception.provider_diagnostics
        self.assertEqual(detail["status"], "INVALID_ARGUMENT")
        self.assertIn("Unsupported mime_type image/png", detail["message"])
        for secret in ("fixture-key", prompt, "PRIVATE_FIELD_MUST_NOT_APPEAR"):
            self.assertNotIn(secret, json.dumps(detail))
        fake.release_local_model_budget.assert_called_once_with(fake.reserve_local_model_budget.return_value, retain_cost_reservation=False)
        fake.record_failed_generation_attempt.assert_called_once()

    def test_ambiguous_http_errors_keep_reservation_and_bound_error_reads(self):
        for code, raw in ((503, b'{"error":{"status":"UNAVAILABLE","message":"Busy"}}'),
                          (400, b'not JSON'), (400, b'x' * (art.MAX_ERROR_BYTES + 2))):
            with self.subTest(code=code, length=len(raw)):
                fake = self.fake_bot("unused")
                stream = io.BytesIO(raw)
                error = art.urllib.error.HTTPError(art.IMAGE_ENDPOINT, code, "bad", {}, stream)
                opener = mock.Mock()
                opener.open.side_effect = error
                with mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
                    with self.assertRaises(RuntimeError):
                        art.generate_private_image(fake, "A quiet room.")
                self.assertLessEqual(stream.tell(), art.MAX_ERROR_BYTES + 1)
                fake.release_local_model_budget.assert_called_once_with(fake.reserve_local_model_budget.return_value, retain_cost_reservation=True)

    def test_request_payload_in_provider_error_is_omitted(self):
        raw = b'{"error":{"status":"INVALID_ARGUMENT","message":"Invalid prompt: PRIVATE_FIXTURE"}}'
        error = art.urllib.error.HTTPError(art.IMAGE_ENDPOINT, 400, "bad", {}, io.BytesIO(raw))
        detail = art._image_provider_error(error, secrets=()).provider_diagnostics
        self.assertEqual(detail["message_omitted"], "request_payload_marker")
        self.assertNotIn("PRIVATE_FIXTURE", json.dumps(detail))

    def test_failed_preview_receipt_preserves_status_and_safe_provider_explanation(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "existing.sqlite"
            db.touch()
            fake = self.fake_bot(str(db))
            fake._generate_gemini_content_with_fallback.side_effect = lambda *a, **kw: kw["attempt_counter"].mark_started()
            raw = b'{"error":{"status":"INVALID_ARGUMENT","message":"Unsupported image output option."}}'
            opener = mock.Mock()
            opener.open.side_effect = art.urllib.error.HTTPError(art.IMAGE_ENDPOINT, 400, "bad", {}, io.BytesIO(raw))
            target = Path(folder) / "preview"
            with mock.patch.object(art, "build_source_packet", return_value={}), mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
                with self.assertRaises(RuntimeError):
                    art.prepare_private_preview(fake, str(target), generate=True)
            receipt = json.loads((target / "receipt.json").read_text())
            self.assertEqual(receipt["status"], "preview_failed")
            self.assertEqual(receipt["providerStatus"], 400)
            self.assertEqual(receipt["providerDiagnostics"]["message"], "Unsupported image output option.")
            self.assertEqual((receipt["conceptCalls"], receipt["imageCalls"]), (1, 1))
            self.assertFalse(receipt["published"])
            self.assertFalse((target / "bnl-own-art.png").exists())

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
