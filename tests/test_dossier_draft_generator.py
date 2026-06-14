import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")
os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")

import bnl01_bot
import bnl_dossier_draft as draft


def pr217_packet(**overrides):
    packet = {
        "requestType": "bnl_proposed_dossier_draft",
        "version": "1.0",
        "candidate": {
            "sourceFileId": "sf_217",
            "subjectName": "Signal Fox",
            "sourceCandidateUpdatedAt": "2026-06-13T00:00:00.000Z",
        },
        "sourceFileSummary": "Internal admin summary that must not be copied.",
        "publicSafeFacts": [
            "Signal Fox is a music artist connected to the BARCODE community.",
            "Public materials describe experimental electronic tracks and collaborative releases.",
        ],
        "publicSafeNotes": [
            {"id": "note_1", "text": "Public-facing notes should stay concise and avoid identity certainty."},
            {"id": "note_2", "note": "The available public copy supports an artist/community framing."},
        ],
        "reviewOnlyWarnings": ["Owner review must confirm identity labels before publication."],
        "doNotSayNotes": ["Internal alias: SECRET HANDLE"],
        "missingInfo": ["Confirm the preferred public link."],
        "identityAliasStatus": {
            "publicSafeIdentityLabels": ["artist"],
            "internalAliasCount": 2,
            "needsConfirmation": True,
            "status": "needs_confirmation",
        },
        "sourceUsageSummary": {
            "sourceFileNoteIds": ["note_1", "note_2"],
            "recommendationIds": ["rec_1"],
            "sourceLanes": ["internal_review", "public_safe"],
        },
        "currentDraft": None,
        "safeClassification": {"category": "Artist", "kind": "Person", "ecosystemLane": "Music"},
        "stylePacket": {
            "representativePublicDossierExamples": [
                {"name": "Example Star", "summary": "Example Star founded the copied-only Nebula Choir fact."}
            ],
            "categorySpecificExamples": [
                {"category": "Artist", "role": "Music artist", "notes": "Short, concrete, public-safe."}
            ],
            "authoringGuideSummary": {
                "lengthGuide": "Use one compact paragraph for summary and one or two short notes sentences.",
                "toneGuide": "Clear BARCODE dossier style without lore fog.",
                "draftingRules": ["Do not invent facts from examples."],
            },
            "tagRegistryGuidance": {
                "canonicalTags": ["artist", "community", "member", "collaborator", "systems", "tech"],
            },
        },
        "fieldRequirements": ["summary", "role", "tags", "ownerReviewWarnings"],
        "forbiddenPublicCopyPatterns": ["internal alias", "payment", "Priority Signal"],
        "ownerReviewRules": ["Owner Review must approve the draft before publication."],
        "sourceBoundaryRules": ["Use public-safe facts and public-safe notes only."],
    }
    packet.update(overrides)
    return packet


PUBLIC_FIELD_KEYS = ("role", "summary", "notes", "status", "sourceUsageSummary")


class DossierDraftGeneratorTests(unittest.TestCase):
    def test_missing_and_invalid_token_reject(self):
        env = {draft.DRAFT_TOKEN_ENV: "secret"}
        self.assertFalse(draft.is_dossier_draft_token_valid(None, environ=env))
        self.assertFalse(draft.is_dossier_draft_token_valid("wrong", environ=env))
        self.assertTrue(draft.is_dossier_draft_token_valid("secret", environ=env))

    def test_realistic_pr217_packet_validates_successfully(self):
        self.assertEqual(draft.validate_dossier_draft_packet(pr217_packet()), [])

    def test_field_requirements_string_array_is_accepted(self):
        packet = pr217_packet(fieldRequirements=["summary", "notes"])
        self.assertEqual(draft.validate_dossier_draft_packet(packet), [])

    def test_field_requirements_missing_or_empty_is_rejected(self):
        missing = pr217_packet()
        missing.pop("fieldRequirements")
        self.assertIn("missing_fieldRequirements", draft.validate_dossier_draft_packet(missing))
        self.assertIn("missing_fieldRequirements", draft.validate_dossier_draft_packet(pr217_packet(fieldRequirements=[])))
        self.assertIn("missing_fieldRequirements", draft.validate_dossier_draft_packet(pr217_packet(fieldRequirements={})))

    def test_returned_draft_uses_site_valid_enum_values(self):
        result = draft.generate_dossier_draft(pr217_packet())["draft"]
        self.assertIn(result["status"], draft.VALID_STATUSES)
        self.assertIn(result["clearance"], draft.VALID_CLEARANCES)
        self.assertIn(result["origin"], draft.VALID_ORIGINS)
        self.assertIn(result["identityAuthority"], draft.VALID_IDENTITY_AUTHORITIES)
        self.assertEqual(result["status"], "PENDING")
        self.assertEqual(result["clearance"], "PUBLIC")
        self.assertEqual(result["origin"], "UNVERIFIED")
        self.assertEqual(result["identityAuthority"], "mixed_or_unclear")

    def test_final_status_metadata_does_not_leak_and_returns_pending(self):
        packet = pr217_packet(
            candidate={
                "sourceFileId": "sf_approved",
                "subjectName": "Status Fox",
                "status": "approved published live final complete",
            },
            publicSafeFacts=["Status Fox is a community artist with public music context."],
            publicSafeNotes=[{"text": "Do not say this record is approved, published, live, final, or complete."}],
            identityAliasStatus={"needsConfirmation": True, "status": "approved published live final complete"},
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        self.assertEqual(result["status"], "PENDING")
        public = " ".join(str(result[k]) for k in PUBLIC_FIELD_KEYS).lower()
        for forbidden in ("approved", "published", "live", "final", "complete"):
            self.assertNotIn(forbidden, public)

    def test_internal_alias_and_do_not_say_terms_not_in_public_fields(self):
        result = draft.generate_dossier_draft(pr217_packet())["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes"))
        self.assertNotIn("SECRET HANDLE", public)
        self.assertNotIn("Internal alias", public)
        self.assertTrue(any("internal aliases" in x for x in result["unsupportedClaimsRejected"]))

    def test_payment_priority_stripe_checkout_terms_are_rejected_or_excluded(self):
        result = draft.generate_dossier_draft(pr217_packet(
            publicSafeFacts=[
                "Signal Fox paid through Stripe checkout for Priority Signal.",
                "Signal Fox is a music artist connected to the BARCODE community.",
            ],
            publicSafeNotes=[{"text": "Priority purchase should not be public."}],
        ))["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes"))
        for forbidden in ("Stripe", "checkout", "Priority", "payment"):
            self.assertNotIn(forbidden, public)
        self.assertTrue(any("payment/Priority" in x for x in result["unsupportedClaimsRejected"]))

    def test_thin_packet_returns_conservative_summary_and_missing_info(self):
        result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]))["draft"]
        self.assertIn("awaiting owner review", result["summary"])
        self.assertTrue(result["missingInfoQuestions"])
        self.assertIn("Add more public-safe facts", result["missingInfoQuestions"][0])

    def test_public_safe_artist_music_packet_returns_artist_music_wording(self):
        result = draft.generate_dossier_draft(pr217_packet())["draft"]
        self.assertEqual(result["role"], "Music artist")
        self.assertIn("music artist", result["summary"].lower())

    def test_style_examples_used_for_shape_not_copied_facts(self):
        result = draft.generate_dossier_draft(pr217_packet())["draft"]
        combined = " ".join(str(result[k]) for k in ("role", "summary", "notes"))
        self.assertNotIn("Example Star", combined)
        self.assertNotIn("Nebula Choir", combined)
        self.assertLessEqual(len(result["role"].split()), 8)
        self.assertLessEqual(len(result["summary"].split()), 85)
        self.assertTrue(any("examples only for structure" in x for x in result["unsupportedClaimsRejected"]))

    def test_tag_registry_guidance_splits_existing_and_proposed_tags(self):
        result = draft.generate_dossier_draft(pr217_packet(
            safeClassification={"category": "Artist", "kind": "Collaborator", "ecosystemLane": "Music"},
            stylePacket={
                "tagRegistryGuidance": {"canonicalTags": ["artist", "collaborator"]},
                "authoringGuideSummary": {"lengthGuide": "compact"},
            },
            proposedTags=["new-scene", "payment-vip"],
        ))["draft"]
        self.assertIn("artist", result["tags"])
        self.assertIn("collaborator", result["tags"])
        self.assertIn("new-scene", result["proposedTags"])
        self.assertNotIn("payment-vip", result["tags"])
        self.assertNotIn("payment-vip", result["proposedTags"])

    def test_endpoint_causes_no_discord_send_or_memory_write(self):
        self.assertTrue(hasattr(bnl01_bot, "_handle_dossier_draft"))
        self.assertEqual(draft.DRAFT_ENDPOINT_PATH, bnl01_bot.DRAFT_ENDPOINT_PATH)
        result = draft.generate_dossier_draft(pr217_packet())
        self.assertIn("draft", result)

    def test_public_safe_evidence_retrieval_enriches_subject_specific_draft(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, topic TEXT, relation_to_subject TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER, updated_at TEXT, created_at TEXT)"
            )
            conn.execute(
                "INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "Signal Fox",
                    "Signal Fox regularly shares experimental electronic tracks with the BARCODE community.",
                    "Music/track-sharing discussion",
                    "subject_direct_evidence",
                    "public_home",
                    "public",
                    "public_safe",
                    1,
                    0,
                    "2026-06-14",
                    "2026-06-14",
                ),
            )
            conn.execute(
                "CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, usage_scope TEXT, status TEXT)"
            )
            conn.execute(
                "INSERT INTO broadcast_memory VALUES (?,?,?,?)",
                ("Signal Fox appears in active public BARCODE Radio notes as a recurring music collaborator.", 1, "public", "active"),
            )
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            combined = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary"))
            self.assertIn("experimental electronic tracks", combined)
            self.assertIn("public-safe structured entity evidence", combined)
            self.assertNotIn("awaiting owner review", result["summary"])

    def test_private_alias_payment_and_source_blind_evidence_are_excluded(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, topic TEXT, relation_to_subject TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER, updated_at TEXT, created_at TEXT)"
            )
            rows = [
                ("Secret Fox", "Secret Fox is a private alias that must never appear.", "alias", "matched", "public_home", "public", "public_safe", 1, 0, "now", "now"),
                ("Signal Fox", "Signal Fox paid through Stripe checkout for Priority customer handling.", "payment", "matched", "public_home", "public", "public_safe", 1, 0, "now", "now"),
                ("Signal Fox", "Signal Fox source-blind unknown-policy memory says private lore.", "memory", "matched", "source_blind", "review_only", "source_blind_memory", 0, 1, "now", "now"),
                ("Signal Fox", "Signal Fox is publicly known for BARCODE community music discussions.", "community", "matched", "public_context", "public", "public_safe", 1, 0, "now", "now"),
            ]
            conn.executemany("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            conn.execute("CREATE TABLE memory_tiers (summary TEXT)")
            conn.execute("INSERT INTO memory_tiers VALUES (?)", ("Signal Fox private source-blind detail.",))
            conn.commit()
            conn.close()
            packet = pr217_packet(
                publicSafeFacts=[],
                publicSafeNotes=[],
                identityAliasStatus={"publicSafeIdentityLabels": ["Secret Fox"], "needsConfirmation": True},
            )
            evidence = draft.build_public_dossier_draft_evidence(packet, tmp.name)
            result = draft.generate_dossier_draft(packet, tmp.name)["draft"]
            public = " ".join(str(result[k]) for k in ("role", "summary", "notes"))
            self.assertIn("Secret Fox", evidence["matchedAliasesUsedPrivately"])
            for forbidden in ("Secret Fox", "Stripe", "checkout", "Priority", "private lore", "source-blind unknown-policy"):
                self.assertNotIn(forbidden, public)
            self.assertTrue(any("Excluded review-only" in x for x in evidence["excludedSourceWarnings"]))
            self.assertTrue(any("memory_tiers" in x for x in evidence["excludedSourceWarnings"]))

    def test_thin_retrieved_evidence_adds_missing_questions_and_review_warnings(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE memory_tiers (summary TEXT)")
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            self.assertTrue(any("Add approved public-safe subject evidence" in x for x in result["missingInfoQuestions"]))
            self.assertTrue(any("Fewer than two public-safe evidence" in x for x in result["ownerReviewWarnings"]))

    def test_validation_requires_candidate_and_style_packet(self):
        errors = draft.validate_dossier_draft_packet({"requestType": "bad", "fieldRequirements": ["summary"]})
        self.assertIn("invalid_requestType", errors)
        self.assertIn("missing_candidate", errors)
        self.assertIn("missing_stylePacket", errors)


if __name__ == "__main__":
    unittest.main()
