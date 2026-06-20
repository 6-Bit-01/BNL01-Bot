import os
import json
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

    def test_draft_uses_public_dossier_style_context_and_filters_private_examples(self):
        packet = pr217_packet(
            stylePacket={
                "publicDossierStyleContext": [
                    {"status": "PUBLIC", "type": "artist dossier", "toneNotes": ["tight BARCODE public voice"], "structureNotes": ["role then compact summary"]},
                    {"status": "OWNER_DRAFT", "type": "private draft", "toneNotes": ["secret owner style"]},
                ],
                "tagRegistryGuidance": {"canonicalTags": ["artist", "community"]},
            }
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        combined = json.dumps(result)
        self.assertIn("closest house pattern", result["sourceUsageSummary"])
        self.assertIn("artist/music public dossier pattern", combined)
        self.assertNotIn("secret owner style", combined)
        self.assertNotIn("Example Star founded the copied-only Nebula Choir fact", combined)

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

    def test_source_file_classification_internal_and_rejected_tags_not_public_tags(self):
        packet = pr217_packet(
            publicSafeFacts=["Signal Fox is a public-safe music project."],
            proposedTags=["moderator", "queue", "artist"],
            sourceFileClassificationV1={
                "version": "1.0",
                "publicSafeTagCandidates": ["artist", "music"],
                "internalTags": ["moderator", "queue"],
                "rejectedTagCandidates": ["contest"],
                "doNotPubliclyTagAs": ["moderator", "queue", "contest"],
                "blockedPublicTags": [{"tag": "moderator", "reason": "internal only"}],
            },
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public_tags = set(result["tags"]) | set(result["proposedTags"])
        self.assertNotIn("moderator", public_tags)
        self.assertNotIn("queue", public_tags)
        self.assertNotIn("contest", public_tags)
        self.assertIn("artist", public_tags)

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

    def test_public_draft_does_not_emit_review_helper_fields_or_source_blind_terms(self):
        packet = pr217_packet(
            publicSafeFacts=["Signal Fox is a BARCODE Network community member."],
            publicSafeNotes=[],
            reviewableClaims=[{
                "claimText": "Some subject memory lacked public-safe provenance",
                "displayTitle": "Needs public source",
                "displayDecision": "What public-safe source or owner-approved wording supports this claim?",
                "verificationPacketQuestion": "What public-safe source supports this claim?",
                "suggestedInternalNote": "BNL found source-blind context for Signal Fox.",
                "suggestedMissingInfoQuestion": "What public-safe source or owner-approved wording supports this source-blind context?",
                "recommendedAction": "needs_public_source",
                "actionability": "source_blind_warning",
            }],
            withheldEvidenceAudit={"source_blind": {"count": 1}},
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public = json.dumps(result).lower()
        for forbidden in ("reviewableclaims", "displaytitle", "displaydecision", "verificationpacketquestion", "suggestedinternalnote", "suggestedmissinginfoquestion", "recommendedaction", "actionability", "withheldevidenceaudit", "source-blind", "source_blind"):
            self.assertNotIn(forbidden, public)

    def test_thin_packet_returns_conservative_summary_and_missing_info(self):
        result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]))["draft"]
        self.assertIn("limited public-safe source detail", result["summary"])
        self.assertNotIn("owner review", result["summary"].lower())
        self.assertTrue(result["missingInfoQuestions"])
        self.assertIn("Add more public-safe facts", result["missingInfoQuestions"][0])

    def test_public_safe_artist_music_packet_returns_artist_music_wording(self):
        result = draft.generate_dossier_draft(pr217_packet())["draft"]
        self.assertEqual(result["role"], "Music artist")
        self.assertIn("music artist", result["summary"].lower())


    def test_public_notes_filter_review_admin_missing_info_warning_text(self):
        packet = pr217_packet(
            publicSafeFacts=["Signal Fox is connected to public BARCODE community context."],
            publicSafeNotes=[
                {"text": "Owner Review: needs review before claiming preferred display name / public link / role confirmation / owner approval."},
                {"text": "Admin-only source-blind missing info must not be copied into public text."},
                {"text": "Dossier Blueprint readiness: Boundaries / what not to claim."},
                {"text": "Signal Fox appears in public community context with concise source-backed wording."},
            ],
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary", "tags", "proposedTags")).lower()
        for forbidden in (
            "owner review",
            "admin-only",
            "review-only",
            "source-blind",
            "missing info",
            "needs review before claiming",
            "dossier blueprint readiness",
            "boundaries",
            "what not to claim",
            "must not be copied into public text",
        ):
            self.assertNotIn(forbidden, public)
        self.assertIn("public community context", result["notes"])
        metadata = " ".join(result["ownerReviewWarnings"] + result["publicSafetyWarnings"] + result["missingInfoQuestions"] + result["unsupportedClaimsRejected"]).lower()
        self.assertIn("review/admin/missing-info note text", metadata)

    def test_source_usage_summary_uses_clean_plural_approved_wording(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, topic TEXT, relation_to_subject TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER, updated_at TEXT, created_at TEXT)")
            conn.executemany(
                "INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [
                    ("Signal Fox", "Signal Fox has one public-safe community evidence summary.", "community", "direct", "public_home", "public", "public_safe", 1, 0, "now", "now"),
                    ("Signal Fox", "Signal Fox has another public-safe community evidence summary.", "community", "direct", "public_home", "public", "public_safe", 1, 0, "now", "now"),
                ],
            )
            conn.execute("CREATE TABLE entity_intelligence_facts (subject_key TEXT, subject_name TEXT, fact_label TEXT, fact_value TEXT, visibility TEXT, authority TEXT, public_safe INTEGER, review_only INTEGER, status TEXT, last_seen_at TEXT)")
            conn.execute("INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?,?)", ("signal_fox", "Signal Fox", "role", "Signal Fox has one public-safe entity intelligence fact.", "public_safe", "owner_confirmed", 1, 0, "active", "now"))
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.executemany("INSERT INTO broadcast_memory VALUES (?,?,?)", [("Signal Fox has active public-safe broadcast memory one.", 1, "active"), ("Signal Fox has active public-safe broadcast memory two.", 1, "active")])
            conn.commit(); conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
        usage = result["sourceUsageSummary"]
        self.assertIn("Used 2 public-safe structured entity evidence summaries.", usage)
        self.assertIn("Used 1 public-safe entity intelligence fact.", usage)
        self.assertIn("Used 2 active public-safe broadcast memory summaries.", usage)
        self.assertNotIn("summarie(s)", usage)
        self.assertNotIn("fact(s)", usage)
        for forbidden in ("private", "source-blind", "review-only", "internal", "admin-only", "payment", "priority", "stripe", "checkout", "customer"):
            self.assertNotIn(forbidden, usage.lower())

    def test_music_role_from_public_safe_entity_has_approved_source_phrase(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, topic TEXT, relation_to_subject TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER, updated_at TEXT, created_at TEXT)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?)", ("Signal Fox", "Signal Fox shares public electronic music tracks with the BARCODE community.", "music", "direct", "public_home", "public", "public_safe", 1, 0, "now", "now"))
            conn.commit(); conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[], safeClassification={"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"}), tmp.name)["draft"]
        self.assertEqual(result["role"], "Music artist")
        self.assertIn("Used 1 public-safe structured entity evidence summary.", result["sourceUsageSummary"])

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

    def test_matching_official_public_dossier_context_enriches_draft(self):
        read_model = {
            "sections": {
                "dossiers": {
                    "items": [
                        {
                            "name": "Signal Fox",
                            "role": "Experimental electronic artist",
                            "summary": "Signal Fox is an official public BARCODE dossier subject known for glitchy electronic collaborations.",
                            "publicFacts": ["Signal Fox appears in the public dossier registry as a music collaborator."],
                        },
                        {
                            "name": "Unrelated Star",
                            "summary": "Unrelated Star founded the Nebula Choir.",
                        },
                    ]
                }
            }
        }
        result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), public_read_model=read_model)["draft"]
        combined = " ".join(str(result[k]) for k in ("summary", "notes", "sourceUsageSummary"))
        self.assertIn("public dossier registry", combined)
        self.assertIn("official public dossier authority for Signal Fox", combined)
        self.assertIn("Signal Fox", result["sourceUsageSummary"])
        self.assertNotIn("Nebula Choir", combined)

    def test_public_read_model_alias_match_redacts_alias_from_public_fields(self):
        read_model = {
            "sections": {
                "dossiers": {
                    "items": [
                        {
                            "name": "Secret Fox",
                            "role": "Secret Fox listening-room host",
                            "summary": "Secret Fox appears in official public dossier context for BARCODE listening sessions.",
                            "publicFacts": ["Secret Fox is connected to public BARCODE listening-room sessions."],
                        }
                    ]
                }
            }
        }
        packet = pr217_packet(publicSafeFacts=[], publicSafeNotes=[], identityAliasStatus={"publicSafeIdentityLabels": ["Secret Fox"], "needsConfirmation": True})
        result = draft.generate_dossier_draft(packet, public_read_model=read_model)["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary", "ownerReviewWarnings", "publicSafetyWarnings", "unsupportedClaimsRejected", "tags", "proposedTags"))
        self.assertIn("Signal Fox", public)
        self.assertIn("BARCODE listening", public)
        self.assertIn("official public dossier authority for Signal Fox", result["sourceUsageSummary"])
        self.assertNotIn("Secret Fox", public)
        self.assertNotIn("Secret Fox", result["sourceUsageSummary"])
        for forbidden in ("private", "source-blind", "review-only", "internal", "admin-only", "payment", "priority", "stripe", "checkout", "customer"):
            self.assertNotIn(forbidden, result["sourceUsageSummary"].lower())


    def test_music_role_from_matching_public_dossier_has_subject_authority_source_usage(self):
        read_model = {
            "sections": {
                "dossiers": {
                    "items": [
                        {
                            "name": "Signal Fox",
                            "role": "Music artist",
                            "summary": "Signal Fox is listed in public dossier context as an electronic music artist for BARCODE collaborations.",
                            "publicFacts": ["Signal Fox appears in public dossier context as a music artist."],
                        }
                    ]
                }
            }
        }
        result = draft.generate_dossier_draft(
            pr217_packet(publicSafeFacts=[], publicSafeNotes=[], safeClassification={"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"}),
            public_read_model=read_model,
        )["draft"]
        self.assertEqual(result["role"], "Music artist")
        self.assertIn("Used matching current public dossier context as official public dossier authority for Signal Fox.", result["sourceUsageSummary"])
        for forbidden in ("private", "source-blind", "review-only", "internal", "admin-only", "payment", "priority", "stripe", "checkout", "customer"):
            self.assertNotIn(forbidden, result["sourceUsageSummary"].lower())

    def test_no_public_read_model_context_warns_without_failing(self):
        result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]))["draft"]
        self.assertTrue(any("No matching current public dossier/read-model facts" in x for x in result["ownerReviewWarnings"]))
        self.assertNotIn("style examples were used only", result["sourceUsageSummary"])
        self.assertIn("style examples were used only", " ".join(result["ownerReviewWarnings"]))

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

    def test_role_identity_label_is_not_used_as_subject_alias(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, topic TEXT, relation_to_subject TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER, updated_at TEXT, created_at TEXT)"
            )
            conn.execute(
                "INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("Other Person", "Other Person is a public artist with unrelated BARCODE activity.", "artist", "matched", "public_home", "public", "public_safe", 1, 0, "2026-06-14", "2026-06-14"),
            )
            conn.commit()
            conn.close()
            packet = pr217_packet(publicSafeFacts=[], publicSafeNotes=[], identityAliasStatus={"publicSafeIdentityLabels": ["artist"], "needsConfirmation": True})
            result = draft.generate_dossier_draft(packet, tmp.name)["draft"]
            public = " ".join(str(result[k]) for k in ("role", "summary", "notes"))
            self.assertNotIn("Other Person", public)
            self.assertNotIn("unrelated BARCODE activity", public)

    def test_role_identity_labels_do_not_match_broadcast_or_entity_intelligence_text(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_intelligence_facts (subject_key TEXT, subject_name TEXT, fact_label TEXT, fact_value TEXT, visibility TEXT, authority TEXT, public_safe INTEGER, review_only INTEGER, status TEXT, last_seen_at TEXT)"
            )
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute(
                "INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("other_artist", "Other Artist", "artist", "Other Artist is a public community collaborator.", "public_safe", "owner_confirmed", 1, 0, "active", "now"),
            )
            conn.execute(
                "INSERT INTO broadcast_memory VALUES (?,?,?)",
                ("Other Artist appears in public radio broadcast memory as a community collaborator.", 1, "active"),
            )
            conn.commit()
            conn.close()
            packet = pr217_packet(
                publicSafeFacts=[],
                publicSafeNotes=[],
                identityAliasStatus={"publicSafeIdentityLabels": ["artist", "community", "collaborator", "radio"], "needsConfirmation": True},
            )
            result = draft.generate_dossier_draft(packet, tmp.name)["draft"]
            public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary"))
            self.assertNotIn("Other Artist", public)
            self.assertNotIn("public community collaborator", public)

    def test_common_role_labels_are_all_excluded_from_alias_terms(self):
        labels = ["artist", "member", "community", "collaborator", "mod", "system", "radio", "producer", "ai", "human", "active", "pending"]
        for label in labels:
            with self.subTest(label=label):
                self.assertFalse(draft._is_probable_subject_alias(label))
        self.assertTrue(draft._is_probable_subject_alias("Signal Fox Alt"))

    def test_p1_review_thread_alias_rules_are_covered_directly(self):
        packet = pr217_packet(
            publicSafeFacts=[],
            publicSafeNotes=[],
            identityAliasStatus={"publicSafeIdentityLabels": ["artist", "Signal Fox Alt"], "needsConfirmation": True},
        )
        subject_name, aliases = draft._subject_terms(packet)
        self.assertEqual(subject_name, "Signal Fox")
        self.assertNotIn("artist", aliases)
        self.assertIn("Signal Fox Alt", aliases)
        redacted = draft._redact_private_match_terms(
            "Signal Fox Alt appears in public BARCODE music context.",
            aliases,
            subject_name,
        )
        self.assertEqual(redacted, "Signal Fox appears in public BARCODE music context.")

    def test_name_like_alias_matches_and_is_redacted_from_public_fields(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, topic TEXT, relation_to_subject TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER, updated_at TEXT, created_at TEXT)"
            )
            conn.execute(
                "INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("Secret Fox", "Secret Fox regularly hosts public BARCODE listening-room sessions.", "community", "matched", "public_home", "public", "public_safe", 1, 0, "2026-06-14", "2026-06-14"),
            )
            conn.commit()
            conn.close()
            packet = pr217_packet(publicSafeFacts=[], publicSafeNotes=[], identityAliasStatus={"publicSafeIdentityLabels": ["Secret Fox"], "needsConfirmation": True})
            result = draft.generate_dossier_draft(packet, tmp.name)["draft"]
            combined = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary", "ownerReviewWarnings", "publicSafetyWarnings", "unsupportedClaimsRejected", "tags", "proposedTags"))
            self.assertIn("Signal Fox regularly hosts public BARCODE listening-room sessions", combined)
            self.assertNotIn("Secret Fox", combined)

    def test_alias_redaction_applies_to_broadcast_and_entity_intelligence_evidence(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_intelligence_facts (subject_key TEXT, subject_name TEXT, fact_label TEXT, fact_value TEXT, visibility TEXT, authority TEXT, public_safe INTEGER, review_only INTEGER, status TEXT, last_seen_at TEXT)"
            )
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute(
                "INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("secret_fox", "Secret Fox", "role", "Secret Fox hosts public BARCODE collaboration reviews.", "public_safe", "owner_confirmed", 1, 0, "active", "now"),
            )
            conn.execute(
                "INSERT INTO broadcast_memory VALUES (?,?,?)",
                ("Secret Fox appears in public-safe BARCODE Radio notes.", 1, "active"),
            )
            conn.commit()
            conn.close()
            packet = pr217_packet(publicSafeFacts=[], publicSafeNotes=[], identityAliasStatus={"publicSafeIdentityLabels": ["Secret Fox"], "needsConfirmation": True})
            evidence = draft.build_public_dossier_draft_evidence(packet, tmp.name)
            result = draft.generate_dossier_draft(packet, tmp.name)["draft"]
            public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary", "ownerReviewWarnings", "publicSafetyWarnings", "unsupportedClaimsRejected", "tags", "proposedTags"))
            evidence_text = " ".join(evidence["publicFacts"] + evidence["notablePublicSignals"])
            self.assertIn("Signal Fox hosts public BARCODE collaboration reviews", public)
            self.assertIn("Signal Fox appears in public-safe BARCODE Radio notes", public)
            self.assertNotIn("Secret Fox", public)
            self.assertNotIn("Secret Fox", evidence_text)

    def test_entity_intelligence_public_safe_active_facts_enrich_and_private_excluded(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_intelligence_facts (subject_key TEXT, subject_name TEXT, fact_label TEXT, fact_value TEXT, visibility TEXT, authority TEXT, public_safe INTEGER, review_only INTEGER, status TEXT, last_seen_at TEXT)"
            )
            rows = [
                ("signal_fox", "Signal Fox", "role", "Signal Fox runs public BARCODE listening-room collaborations.", "public_safe", "owner_confirmed", 1, 0, "active", "now"),
                ("signal_fox", "Signal Fox", "private", "Signal Fox source_blind_memory private detail.", "review_only", "source_blind_memory", 0, 1, "active", "now"),
                ("signal_fox", "Signal Fox", "payment", "Signal Fox Stripe checkout customer note.", "public_safe", "owner_confirmed", 1, 0, "active", "now"),
            ]
            conn.executemany("INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary"))
            self.assertIn("listening-room collaborations", public)
            self.assertIn("public-safe entity intelligence fact", public)
            self.assertNotIn("source_blind_memory", public)
            self.assertNotIn("Stripe", public)

    def test_subject_filtering_happens_before_entity_evidence_row_limit(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_evidence_events (subject_key TEXT, subject_name TEXT, safe_summary TEXT, topic TEXT, relation_to_subject TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER, updated_at TEXT, created_at TEXT)"
            )
            conn.execute(
                "INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("signal_fox", "Signal Fox", "Signal Fox has older public-safe dossier-worthy source evidence.", "community", "matched", "public_home", "public", "public_safe", 1, 0, "2026-01-01", "2026-01-01"),
            )
            for idx in range(300):
                conn.execute(
                    "INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"other_{idx}", f"Other {idx}", f"Other {idx} public artist note.", "artist", "matched", "public_home", "public", "public_safe", 1, 0, f"2026-06-{idx % 28 + 1:02d}", f"2026-06-{idx % 28 + 1:02d}"),
                )
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            self.assertIn("older public-safe dossier-worthy", " ".join(str(result[k]) for k in ("summary", "sourceUsageSummary")))

    def test_subject_filtering_happens_before_entity_intelligence_row_limit(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute(
                "CREATE TABLE entity_intelligence_facts (subject_key TEXT, subject_name TEXT, fact_label TEXT, fact_value TEXT, visibility TEXT, authority TEXT, public_safe INTEGER, review_only INTEGER, status TEXT, last_seen_at TEXT)"
            )
            conn.execute(
                "INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("signal_fox", "Signal Fox", "older", "Signal Fox has older public-safe entity intelligence evidence.", "public_safe", "owner_confirmed", 1, 0, "active", "2026-01-01"),
            )
            for idx in range(300):
                conn.execute(
                    "INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (f"other_{idx}", f"Other {idx}", "newer", f"Other {idx} public artist intelligence.", "public_safe", "owner_confirmed", 1, 0, "active", f"2026-06-{idx % 28 + 1:02d}"),
                )
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            self.assertIn("older public-safe entity intelligence", " ".join(str(result[k]) for k in ("summary", "sourceUsageSummary")))

    def test_broadcast_memory_active_public_safe_without_scope_columns_is_usable(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute(
                "INSERT INTO broadcast_memory VALUES (?,?,?)",
                ("Signal Fox is mentioned in active public-safe BARCODE Radio broadcast memory.", 1, "active"),
            )
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            self.assertIn("BARCODE Radio broadcast memory", " ".join(str(result[k]) for k in ("summary", "sourceUsageSummary")))

    def test_subject_filtering_happens_before_broadcast_memory_row_limit(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute(
                "INSERT INTO broadcast_memory VALUES (?,?,?)",
                ("Signal Fox has older active public-safe broadcast memory.", 1, "active"),
            )
            for idx in range(300):
                conn.execute(
                    "INSERT INTO broadcast_memory VALUES (?,?,?)",
                    (f"Other {idx} has newer active public-safe broadcast memory.", 1, "active"),
                )
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            self.assertIn("older active public-safe broadcast memory", " ".join(str(result[k]) for k in ("summary", "sourceUsageSummary")))

    def test_thin_retrieved_evidence_adds_missing_questions_and_review_warnings(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE memory_tiers (summary TEXT)")
            conn.commit()
            conn.close()
            result = draft.generate_dossier_draft(pr217_packet(publicSafeFacts=[], publicSafeNotes=[]), tmp.name)["draft"]
            self.assertTrue(any("Add approved public-safe subject evidence" in x for x in result["missingInfoQuestions"]))
            self.assertTrue(any("Fewer than two public-safe evidence" in x for x in result["ownerReviewWarnings"]))

    def test_crow_like_memory_uses_analyst_metadata_and_conservative_public_copy(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            for idx in range(14):
                conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", f"Crow has recurring public BARCODE community interaction pattern {idx}.", "public_home", "public", "public_safe", 1, 0))
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow may be a music artist with queue and public link relationship needing confirmation.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind music link clue without provenance.", 0, "active"))
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe Priority customer raw Discord ID 123456 note.", "public_safe", 1))
            conn.commit(); conn.close()
            packet = pr217_packet(candidate={"sourceFileId":"sf_crow", "subjectName":"Crow"}, publicSafeFacts=[], publicSafeNotes=[], safeClassification={"category":"Unknown", "kind":"Person", "ecosystemLane":"Unknown"}, identityAliasStatus={"needsConfirmation": True})
            result = draft.generate_dossier_draft(packet, tmp.name)["draft"]
            self.assertIn(result["role"], {"Community participant", "Community member"})
            public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "sourceUsageSummary")).lower()
            for forbidden in ("owner", "admin", "review-only", "source-blind", "private", "internal", "stripe", "priority", "customer", "discord id"):
                self.assertNotIn(forbidden, public)
            self.assertIn("reduced them to", result["sourceUsageSummary"])
            self.assertTrue(any("BNL internal subject read" in x and "recurring BARCODE-facing community subject" in x for x in result["ownerReviewWarnings"]))
            self.assertTrue(any("display name" in x.lower() for x in result["missingInfoQuestions"]))
            self.assertTrue(any("public links" in x.lower() for x in result["missingInfoQuestions"]))
            self.assertTrue(any("role" in x.lower() or "music" in x.lower() or "link" in x.lower() for x in result["unsupportedClaimsRejected"]))

    def test_analyst_music_role_requires_clean_public_music_evidence(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow is a confirmed public-safe music artist connected to BARCODE Radio.", "public_home", "public", "public_safe", 1, 0))
            conn.commit(); conn.close()
            packet = pr217_packet(candidate={"sourceFileId":"sf_crow", "subjectName":"Crow"}, publicSafeFacts=[], publicSafeNotes=[], safeClassification={"category":"Unknown", "kind":"Person", "ecosystemLane":"Unknown"})
            result = draft.generate_dossier_draft(packet, tmp.name)["draft"]
            self.assertEqual(result["role"], "Music artist")
            self.assertIn("music artist", result["summary"].lower())

    def test_review_actionability_keeps_optional_internal_details_out_of_public_draft(self):
        packet = pr217_packet(
            candidate={"sourceFileId": "sf_crow", "subjectName": "Crow"},
            publicSafeFacts=["Crow is connected to BARCODE public community context."],
            publicSafeNotes=[],
            safeClassification={"category": "Community", "kind": "Person", "ecosystemLane": "BARCODE"},
        )
        packet["reviewActionabilityV1"] = {
            "version": "1",
            "items": [
                {"id": "internal:queue", "claimText": "Crow queue submission history may exist.", "actionability": "keep_internal", "publicUse": "internal_only"},
                {"id": "omit:link", "claimText": "Crow unapproved private music link.", "actionability": "omit_from_public", "publicUse": "omit"},
                {"id": "diagnostic:route", "claimText": "route classification tag community_candidate", "actionability": "diagnostic_only", "publicUse": "not_public"},
            ],
        }
        result = draft.generate_dossier_draft(packet)["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "tags", "proposedTags")).lower()
        self.assertNotIn("queue", public)
        self.assertNotIn("private music link", public)
        self.assertNotIn("classification", public)
        rejected = " ".join(result["unsupportedClaimsRejected"]).lower()
        self.assertIn("queue submission", rejected)
        self.assertIn("private music link", rejected)

    def test_validation_requires_candidate_and_style_packet(self):
        errors = draft.validate_dossier_draft_packet({"requestType": "bad", "fieldRequirements": ["summary"]})
        self.assertIn("invalid_requestType", errors)
        self.assertIn("missing_candidate", errors)
        self.assertIn("missing_stylePacket", errors)


if __name__ == "__main__":
    unittest.main()

class DossierDraftReadinessMemoryTests(unittest.TestCase):
    def test_readiness_memory_is_top_level_not_public_draft(self):
        packet = pr217_packet(subjectAnalystReadV1={
            "dossierReadinessQuestions": [{"id": "q1", "audience": "subject", "question": "Which links are Signal Fox’s and approved for BNL to mention publicly?", "whyItMatters": "Needed for links.", "dossierSection": "links", "priority": "high", "relatedReviewClaimIds": [], "sourceSafety": "needs_confirmation"}],
            "dossierReadinessSummary": "One blocker remains.",
            "dossierBlockedBy": ["links"],
            "readyForDraft": False,
            "draftReadinessReason": "Resolve link ownership first.",
        })
        result = draft.generate_dossier_draft(packet)
        self.assertIn("draftReadiness", result)
        self.assertEqual(result["draftReadiness"]["dossierBlockedBy"], ["links"])
        public = json.dumps(result["draft"]).lower()
        for forbidden in ("dossierreadinessquestions", "draftreadinessreason", "readyfordraft", "which links are signal fox"):
            self.assertNotIn(forbidden, public)

class DossierDraftMemoryFirstTests(unittest.TestCase):
    def test_draft_uses_public_safe_claims_not_internal_judgment(self):
        packet = pr217_packet(
            publicSafeFacts=["Signal Fox is connected to BARCODE public community context."],
            publicSafeNotes=[],
            subjectAnalystReadV1={
                "readyForDraft": True,
                "dossierWorthiness": "possible_fit",
                "internalRead": "Strong Discord queue engagement and Orion lore make this internally interesting.",
                "publicSafeClaims": ["Signal Fox is connected to BARCODE public community context."],
                "draftIngredients": ["Signal Fox is connected to BARCODE public community context."],
                "missingInfoQuestions": ["Confirm public links", "Confirm Orion lore use"],
                "dossierBlockedBy": [],
                "draftReadinessReason": "Draft cautiously and omit unresolved links/lore.",
            },
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public = " ".join(str(result[k]) for k in PUBLIC_FIELD_KEYS)
        self.assertIn("BARCODE", public)
        self.assertNotIn("Orion", public)
        self.assertNotIn("Discord queue", public)
        self.assertNotIn("Confirm Orion", json.dumps(result["missingInfoQuestions"]))

class DossierDraftSourceFilePagePlanTests(unittest.TestCase):
    def test_draft_consumes_source_file_page_plan_safely(self):
        packet = pr217_packet(
            publicSafeFacts=[],
            sourceFilePagePlanV1={
                "draftOrUpdatePlan": {
                    "canDraft": False,
                    "useTheseMaterials": ["Signal Fox has public BARCODE music context."],
                    "omitTheseMaterials": ["Private queue note"],
                    "ownerReviewWarnings": ["Drafting is not recommended yet."],
                },
                "internalOmitHold": {"keepInternal": [{"material": "Internal/source-blind material withheld"}], "omitFromPublicDraft": []},
                "dossierWorthItDecision": {"decision": "not_worth_it_yet", "draftReadiness": "not_ready"},
            },
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public = " ".join(str(result[k]) for k in PUBLIC_FIELD_KEYS)
        self.assertIn("BARCODE", public)
        self.assertNotIn("Private queue note", public)
        self.assertIn("Source File page plan says drafting is not recommended yet.", result["missingInfoQuestions"])
        self.assertTrue(any("page plan warning" in x.lower() for x in result["ownerReviewWarnings"]))
        self.assertTrue(any("Private queue note" in x for x in result["unsupportedClaimsRejected"]))

    def test_page_plan_material_informs_role_and_thin_check_before_missing_info(self):
        packet = pr217_packet(
            publicSafeFacts=[],
            publicSafeNotes=[],
            missingInfo=[],
            safeClassification={"category": "Unknown", "kind": "Unknown", "ecosystemLane": "Unknown"},
            sourceFilePagePlanV1={
                "publicSafeMaterial": [
                    {"material": "Signal Fox is publicly described as a community moderator for BARCODE events.", "confidence": "high", "needsApproval": True},
                ],
                "draftOrUpdatePlan": {
                    "canDraft": True,
                    "useTheseMaterials": ["Signal Fox has public BARCODE community moderator context."],
                    "omitTheseMaterials": ["Private DJ set note"],
                    "ownerReviewWarnings": ["Owner Review must approve the page-plan wording."],
                },
                "internalOmitHold": {
                    "keepInternal": [{"material": "Internal source-blind music artist diagnostic"}],
                    "omitFromPublicDraft": [{"material": "Private DJ set note"}],
                },
                "diagnosticsSummary": {
                    "safetyNotes": ["Diagnostics say music artist but must not become public copy."],
                },
                "dossierWorthItDecision": {"decision": "worth_drafting_now", "draftReadiness": "ready"},
            },
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public = " ".join(str(result[k]) for k in PUBLIC_FIELD_KEYS)
        self.assertEqual(result["role"], "Community moderator")
        self.assertNotIn("Review pending", result["role"])
        self.assertFalse(any("more public-safe facts" in q.lower() for q in result["missingInfoQuestions"]))
        self.assertNotIn("DJ", public)
        self.assertNotIn("source-blind music artist", public.lower())
        self.assertNotIn("diagnostics say music artist", public.lower())
        self.assertTrue(any("page-plan wording" in x for x in result["ownerReviewWarnings"]))
        self.assertTrue(any("Private DJ set note" in x for x in result["unsupportedClaimsRejected"]))

class DossierDraftPagePlanGroundingTests(unittest.TestCase):
    def test_topic_only_page_plan_does_not_become_public_copy(self):
        topic_line = "Recurring named topic: Bit’s may inform public-safe wording only after owner and admin review."
        packet = pr217_packet(
            publicSafeFacts=[],
            publicSafeNotes=[],
            proposedTags=["artist", "community", "member", "mod"],
            safeClassification={"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"},
            sourceFileClassificationV1={"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"},
            sourceFilePagePlanV1={
                "publicSafeMaterial": [{"material": topic_line, "confidence": "low"}],
                "draftOrUpdatePlan": {
                    "canDraft": False,
                    "useTheseMaterials": [topic_line, "clean public summary needed", "clean role needed"],
                    "omitTheseMaterials": ["admin-only evidence exists"],
                    "ownerReviewWarnings": ["needs review before claiming"],
                },
                "dossierWorthItDecision": {"decision": "not_worth_it_yet", "draftReadiness": "not_ready"},
            },
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "tags", "proposedTags"))
        self.assertNotIn("Recurring named topic", public)
        self.assertNotIn("may inform public-safe wording", public)
        self.assertNotEqual(result["role"], "Music artist")
        self.assertEqual(result["role"], "Dossier subject")
        self.assertFalse({"artist", "community", "member", "mod"} & (set(result["tags"]) | set(result["proposedTags"])))
        review_text = " ".join(result["ownerReviewWarnings"] + result["unsupportedClaimsRejected"])
        self.assertIn("topic-only or review-only material", review_text)

    def test_page_plan_public_safe_material_drives_draft(self):
        packet = pr217_packet(
            publicSafeFacts=[],
            publicSafeNotes=[],
            safeClassification={"category": "Community", "kind": "Person", "ecosystemLane": "BARCODE"},
            sourceFileClassificationV1={"category": "Community", "kind": "Person", "ecosystemLane": "BARCODE"},
            sourceFilePagePlanV1={
                "publicSafeMaterial": [
                    {"material": "Crow is publicly documented as a BARCODE community moderator for event discussions.", "confidence": "high"}
                ],
                "draftOrUpdatePlan": {
                    "canDraft": True,
                    "useTheseMaterials": ["Crow helps moderate public BARCODE community event discussions."],
                    "ownerReviewWarnings": ["Owner Review must approve public moderator wording."],
                },
                "dossierWorthItDecision": {"decision": "worth_drafting_now", "draftReadiness": "ready"},
            },
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        public = " ".join(str(result[k]) for k in ("role", "summary", "notes", "tags", "proposedTags"))
        self.assertEqual(result["role"], "Community moderator")
        self.assertIn("BARCODE", public)
        self.assertIn("moderator", public.lower())
        self.assertTrue(any("moderator wording" in x for x in result["ownerReviewWarnings"]))

    def test_invalid_and_conflicting_classification_is_conservative(self):
        packet = pr217_packet(
            publicSafeFacts=[],
            publicSafeNotes=[],
            safeClassification={"category": "Artist", "kind": "internal_only", "ecosystemLane": "Music"},
            sourceFileClassificationV1={"category": "Unknown", "kind": "kind", "ecosystemLane": "ecosystemLane"},
            sourceFilePagePlanV1={
                "publicSafeMaterial": [],
                "draftOrUpdatePlan": {"canDraft": False, "useTheseMaterials": []},
                "dossierWorthItDecision": {"decision": "not_worth_it_yet", "draftReadiness": "not_ready"},
            },
        )
        result = draft.generate_dossier_draft(packet)["draft"]
        self.assertEqual(result["category"], "Unknown")
        self.assertEqual(result["kind"], "Unknown")
        self.assertEqual(result["ecosystemLane"], "Unknown")
        self.assertEqual(result["role"], "Dossier subject")
        emitted = json.dumps(result)
        self.assertNotIn("internal_only", emitted)
        self.assertNotIn('"kind"', result["kind"])
        self.assertTrue(any("classification" in x.lower() for x in result["ownerReviewWarnings"]))
