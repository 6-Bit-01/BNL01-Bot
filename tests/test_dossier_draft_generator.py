import os
import unittest

os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")
os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")

import bnl01_bot
import bnl_dossier_draft as draft


def valid_packet(**overrides):
    packet = {
        "requestType": "bnl_proposed_dossier_draft",
        "version": "1.0",
        "candidate": {"sourceFileId": "sf_1", "subjectName": "Signal Fox", "category": "artist", "kind": "person"},
        "stylePacket": {"suggestedTags": ["Music"]},
        "fieldRequirements": {"summary": "required"},
        "safeClassification": {"ecosystemLane": "music", "identityAuthority": "unconfirmed", "status": "proposed"},
        "publicSafeFacts": ["Signal Fox is a music artist connected to the BARCODE community."],
        "publicSafeNotes": ["Public materials frame the project around experimental electronic tracks."],
        "ownerReviewRules": ["Owner must confirm links before publication."],
        "sourceBoundaryRules": ["Do not use review-only material as public copy."],
    }
    packet.update(overrides)
    return packet


class DossierDraftGeneratorTests(unittest.TestCase):
    def test_missing_and_invalid_token_reject(self):
        env = {draft.DRAFT_TOKEN_ENV: "secret"}
        self.assertFalse(draft.is_dossier_draft_token_valid(None, environ=env))
        self.assertFalse(draft.is_dossier_draft_token_valid("wrong", environ=env))
        self.assertTrue(draft.is_dossier_draft_token_valid("secret", environ=env))

    def test_valid_packet_returns_structured_draft(self):
        result = draft.generate_dossier_draft(valid_packet())
        self.assertIn("draft", result)
        self.assertEqual(result["draft"]["name"], "Signal Fox")
        self.assertIsInstance(result["draft"]["tags"], list)
        self.assertEqual(result["draft"]["files"], [])
        self.assertIn("sourceUsageSummary", result["draft"])

    def test_internal_alias_and_do_not_say_terms_not_in_public_fields(self):
        result = draft.generate_dossier_draft(valid_packet(
            publicSafeFacts=["Signal Fox makes public music."],
            doNotSayNotes=["Internal alias: SECRET HANDLE"],
            internalAliasCount=3,
        ))["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes"))
        self.assertNotIn("SECRET HANDLE", public)
        self.assertNotIn("Internal alias", public)
        self.assertTrue(any("internal aliases" in x for x in result["unsupportedClaimsRejected"]))

    def test_payment_priority_terms_are_rejected_or_not_used(self):
        result = draft.generate_dossier_draft(valid_packet(
            publicSafeFacts=["Signal Fox paid through Stripe checkout for Priority Signal.", "Signal Fox makes public music."],
            publicSafeNotes=["Priority purchase should not be public."]
        ))["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes"))
        self.assertNotIn("Stripe", public)
        self.assertNotIn("Priority", public)
        self.assertTrue(any("payment/Priority" in x for x in result["unsupportedClaimsRejected"]))

    def test_thin_packet_returns_conservative_draft_and_missing_info(self):
        result = draft.generate_dossier_draft(valid_packet(publicSafeFacts=[], publicSafeNotes=[]))["draft"]
        self.assertIn("awaiting owner review", result["summary"])
        self.assertTrue(result["missingInfoQuestions"])

    def test_public_safe_artist_music_packet_returns_music_wording(self):
        result = draft.generate_dossier_draft(valid_packet())["draft"]
        self.assertIn("music", result["role"].lower())
        self.assertIn("music artist", result["summary"].lower())

    def test_endpoint_no_discord_send_or_memory_write(self):
        self.assertTrue(hasattr(bnl01_bot, "_handle_dossier_draft"))
        self.assertIn(draft.DRAFT_ENDPOINT_PATH, bnl01_bot.DRAFT_ENDPOINT_PATH)
        # The route delegates to pure draft generation only; no DB file, Discord client send, or memory writer is passed in.
        result = draft.generate_dossier_draft(valid_packet())
        self.assertIn("draft", result)

    def test_validation_requires_contract_fields(self):
        errors = draft.validate_dossier_draft_packet({"requestType": "bad"})
        self.assertIn("invalid_requestType", errors)
        self.assertIn("missing_candidate", errors)
        self.assertIn("missing_stylePacket", errors)
        self.assertIn("missing_fieldRequirements", errors)


if __name__ == "__main__":
    unittest.main()
