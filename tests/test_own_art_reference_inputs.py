"""Approved own-art references reach pixels, budget guards, and revocation checks."""
import base64
import hashlib
import io
import json
import os
import struct
from types import SimpleNamespace
import unittest
from unittest import mock
import zlib

os.environ.setdefault("GEMINI_API_KEY", "test-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-token")

import bnl_own_art as art
from bnl_gemini_routing import (
    GeminiImageRequest,
    OWN_ART_IMAGE_ROUTE,
    estimate_gemini_prompt_tokens,
)


PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
)
PROMPT = "A quiet orange room."
SNAPSHOT = {"guildId": 123, "selectionId": "approved-fixture", "assets": [
    {"sha256": hashlib.sha256(PNG).hexdigest(), "bytes": len(PNG), "mimeType": "image/png"}]}


def reference(data=PNG, mime_type="image/png"):
    return {"mimeType": mime_type, "data": data}


def padded_png(size):
    """A valid ancillary PNG chunk makes large fixtures without disk artifacts."""
    payload = b"Comment\x00" + b"x" * (size - len(PNG) - 20)
    chunk = b"tEXt" + payload
    return PNG[:-12] + struct.pack(">I", len(payload)) + chunk + struct.pack(
        ">I", zlib.crc32(chunk) & 0xFFFFFFFF
    ) + PNG[-12:]


def provider_payload():
    return {
        "status": "completed",
        "steps": [{"type": "model_output", "content": [{
            "type": "image", "mime_type": "image/png",
            "data": base64.b64encode(PNG).decode("ascii"),
        }]}],
        "usage": {
            "total_input_tokens": 4146, "total_output_tokens": 1120,
            "total_thought_tokens": 10, "total_cached_tokens": 0,
            "total_tokens": 5276,
        },
    }


class OwnArtReferenceInputsTests(unittest.TestCase):
    def fake_bot(self):
        return SimpleNamespace(
            DB_FILE="unused-fixture.sqlite", GEMINI_API_KEY="fixture-key",
            reserve_local_model_budget=mock.Mock(
                return_value=SimpleNamespace(cost_reservation_id="fixture")
            ),
            release_local_model_budget=mock.Mock(),
            record_generation_token_usage=mock.Mock(),
            record_failed_generation_attempt=mock.Mock(),
        )

    def opener(self):
        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps(
            provider_payload()
        ).encode("utf-8")
        opener = mock.Mock()
        opener.open.return_value = response
        return opener

    def test_no_reference_keeps_plain_text_stateless_request(self):
        body = art.own_art_image_request(PROMPT, reference_inputs=())
        self.assertEqual(body["input"], PROMPT)
        self.assertFalse(body["store"])
        self.assertNotIn("tools", body)
        self.assertEqual(body["response_format"], {
            "type": "image", "aspect_ratio": "1:1", "image_size": "1K",
        })

    def test_four_references_are_inline_pixels_in_interactions_input(self):
        body = art.own_art_image_request(
            PROMPT, reference_inputs=tuple(reference() for _ in range(4))
        )
        self.assertEqual(body["input"], [
            {"type": "text", "text": PROMPT},
            *({"type": "image", "mime_type": "image/png",
               "data": base64.b64encode(PNG).decode("ascii")} for _ in range(4)),
        ])
        self.assertFalse(body["store"])
        self.assertNotIn("delivery", body["response_format"])
        self.assertNotIn("mime_type", body["response_format"])

    def test_invalid_reference_count_type_and_png_are_rejected(self):
        cases = (
            tuple(reference() for _ in range(5)),
            (reference(mime_type="image/jpeg"),),
            (reference(data=b"not a png"),),
            (reference(data=PNG[:24]),),
            (reference(data="not bytes"),),
            (reference(data=b""),),
        )
        for refs in cases:
            with self.subTest(case=cases.index(refs)), self.assertRaises(ValueError):
                art.own_art_image_request(PROMPT, reference_inputs=refs)

    def test_raw_image_and_total_byte_limits_reject_before_provider_access(self):
        cases = (
            (reference(padded_png(8 * 1024 * 1024 + 1)),),
            tuple(reference(padded_png(6 * 1024 * 1024)) for _ in range(3)),
        )
        for refs in cases:
            fake = self.fake_bot()
            with mock.patch.object(art.urllib.request, "build_opener") as opener:
                with self.assertRaises(ValueError):
                    art.generate_private_image(
                        fake, PROMPT, reference_inputs=refs,
                        reference_snapshot=SNAPSHOT,
                    )
            opener.assert_not_called()
            fake.reserve_local_model_budget.assert_not_called()

    def test_serialized_request_cap_includes_base64_expansion(self):
        # Original bytes fit 300 bytes; the complete Interactions JSON does not.
        self.assertLess(len(PNG), 300)
        with mock.patch.object(art, "MAX_IMAGE_REQUEST_BYTES", 300, create=True):
            with self.assertRaises(ValueError):
                art.own_art_image_request(PROMPT, reference_inputs=(reference(),))

    def test_reference_pixels_are_reserved_and_actual_usage_settles_once(self):
        fake, opener = self.fake_bot(), self.opener()
        with mock.patch.object(art, "visual_reference_snapshot_current", return_value=True, create=True) as current, \
                mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
            data, receipt = art.generate_private_image(
                fake, PROMPT, reference_inputs=(reference(),),
                reference_snapshot=SNAPSHOT,
            )
        reserved, route = fake.reserve_local_model_budget.call_args.args
        self.assertIsInstance(reserved, GeminiImageRequest)
        self.assertEqual(route, OWN_ART_IMAGE_ROUTE)
        self.assertEqual(reserved.text, PROMPT)
        self.assertEqual([(p.mime_type, p.data) for p in reserved.images], [("image/png", PNG)])
        self.assertGreater(estimate_gemini_prompt_tokens(reserved), estimate_gemini_prompt_tokens(PROMPT))
        self.assertNotIn(base64.b64encode(PNG).decode("ascii"), str(reserved))
        self.assertEqual(data, PNG)
        self.assertEqual(receipt["providerCalls"], 1)
        self.assertEqual(opener.open.call_count, 1)
        fake.record_generation_token_usage.assert_called_once()
        usage = fake.record_generation_token_usage.call_args.args[0].usage_metadata
        self.assertEqual(usage.prompt_token_count, 4146)
        fake.release_local_model_budget.assert_called_once_with(
            fake.reserve_local_model_budget.return_value, retain_cost_reservation=False,
        )
        self.assertGreaterEqual(current.call_count, 2)
        for call in current.call_args_list:
            self.assertEqual(call.args, (fake.DB_FILE, 123, SNAPSHOT))

    def test_revoked_snapshot_prevents_provider_call(self):
        fake = self.fake_bot()
        with mock.patch.object(art, "visual_reference_snapshot_current", return_value=False, create=True), \
                mock.patch.object(art.urllib.request, "build_opener") as opener:
            with self.assertRaises(ValueError):
                art.generate_private_image(
                    fake, PROMPT, reference_inputs=(reference(),),
                    reference_snapshot=SNAPSHOT,
                )
        opener.assert_not_called()
        fake.reserve_local_model_budget.assert_not_called()

    def test_pixels_must_match_the_approved_snapshot_before_reservation(self):
        fake = self.fake_bot()
        wrong = {**SNAPSHOT, "assets": [{**SNAPSHOT["assets"][0], "sha256": "0" * 64}]}
        with mock.patch.object(art, "visual_reference_snapshot_current", return_value=True), \
                mock.patch.object(art.urllib.request, "build_opener", return_value=self.opener()) as opener:
            with self.assertRaises(ValueError):
                art.generate_private_image(fake, PROMPT, reference_inputs=(reference(),),
                                           reference_snapshot=wrong)
        opener.assert_not_called()
        fake.reserve_local_model_budget.assert_not_called()

    def test_snapshot_revoked_during_generation_cannot_return_image(self):
        fake, opener = self.fake_bot(), self.opener()
        with mock.patch.object(art, "visual_reference_snapshot_current", side_effect=(True, False), create=True), \
                mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
            with self.assertRaises(ValueError):
                art.generate_private_image(
                    fake, PROMPT, reference_inputs=(reference(),),
                    reference_snapshot=SNAPSHOT,
                )
        self.assertEqual(opener.open.call_count, 1)
        fake.record_generation_token_usage.assert_called_once()
        fake.release_local_model_budget.assert_called_once_with(
            fake.reserve_local_model_budget.return_value, retain_cost_reservation=False,
        )

    def test_uncertain_reference_call_retains_reservation_and_does_not_retry(self):
        fake, opener = self.fake_bot(), self.opener()
        opener.open.side_effect = TimeoutError("fixture uncertainty")
        with mock.patch.object(art, "visual_reference_snapshot_current", return_value=True, create=True), \
                mock.patch.object(art.urllib.request, "build_opener", return_value=opener):
            with self.assertRaises(RuntimeError):
                art.generate_private_image(
                    fake, PROMPT, reference_inputs=(reference(),),
                    reference_snapshot=SNAPSHOT,
                )
        self.assertEqual(opener.open.call_count, 1)
        fake.release_local_model_budget.assert_called_once_with(
            fake.reserve_local_model_budget.return_value, retain_cost_reservation=True,
        )

    def test_reference_provider_errors_omit_echoed_pixels_but_keep_status(self):
        encoded = base64.b64encode(PNG).decode("ascii")
        marker = "PRIVATE_IMAGE_ECHO_CANARY"
        for code, status, retain in ((400, "INVALID_ARGUMENT", False),
                                     (503, "UNAVAILABLE", True)):
            with self.subTest(status=status):
                fake, opener = self.fake_bot(), self.opener()
                payload = {"error": {
                    "status": status,
                    "message": "Invalid input: " + marker + json.dumps([
                        {"type": "image", "data": encoded, "mime_type": "image/png"}
                    ]),
                    "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                                 "reason": marker, "domain": "fixture.invalid"}],
                }}
                opener.open.side_effect = art.urllib.error.HTTPError(
                    art.IMAGE_ENDPOINT, code, "fixture", {},
                    io.BytesIO(json.dumps(payload).encode("utf-8")),
                )
                with mock.patch.object(art, "visual_reference_snapshot_current", return_value=True), \
                        mock.patch.object(art.urllib.request, "build_opener", return_value=opener), \
                        self.assertLogs(level="WARNING") as logs:
                    with self.assertRaises(RuntimeError) as caught:
                        art.generate_private_image(
                            fake, PROMPT, reference_inputs=(reference(),),
                            reference_snapshot=SNAPSHOT,
                        )
                diagnostics = caught.exception.provider_diagnostics
                self.assertEqual(diagnostics["status"], status)
                self.assertNotIn("message", diagnostics)
                for text in (json.dumps(diagnostics), "\n".join(logs.output)):
                    self.assertNotIn(marker, text)
                    self.assertNotIn(encoded, text)
                self.assertEqual(opener.open.call_count, 1)
                recorded = fake.record_failed_generation_attempt.call_args.args[0]
                self.assertIs(recorded, caught.exception)
                fake.release_local_model_budget.assert_called_once_with(
                    fake.reserve_local_model_budget.return_value, retain_cost_reservation=retain,
                )


if __name__ == "__main__":
    unittest.main()
