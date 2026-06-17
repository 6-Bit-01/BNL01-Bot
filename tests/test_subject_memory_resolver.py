import sqlite3
import tempfile
import unittest

import bnl_dossier_draft as draft
from bnl_subject_memory_resolver import build_subject_analyst_read, build_subject_memory_diagnostic, resolve_subject_memory


class SubjectMemoryResolverTests(unittest.TestCase):
    def test_resolver_finds_and_classifies_crow_memory_across_tables(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE entity_intelligence_facts (subject_key TEXT, subject_name TEXT, fact_label TEXT, fact_value TEXT, visibility TEXT, authority TEXT, public_safe INTEGER, review_only INTEGER, status TEXT)")
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow is publicly listed as a BARCODE community music artist.", "public_home", "public", "public_safe", 1, 0))
            conn.execute("INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?)", ("crow", "Crow", "queue", "Crow queue submission may connect to Raven.", "review_only", "", 0, 1, "active"))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow unknown-policy source-blind music mention.", 0, "active"))
            conn.commit(); conn.close()
            result = resolve_subject_memory("Crow", tmp.name, aliases=["artist", "Crow Alt"])
            self.assertEqual(result["evidenceCounts"]["publicSafe"], 1)
            self.assertEqual(result["evidenceCounts"]["reviewOnly"], 1)
            self.assertEqual(result["evidenceCounts"]["sourceBlind"], 1)
            self.assertIn("Crow is publicly listed", " ".join(result["publicSafeFacts"]))
            self.assertNotIn("artist", result["matchedAliasesUsedPrivately"])
            diagnostic = build_subject_memory_diagnostic("Crow", tmp.name)
            self.assertEqual(diagnostic["candidateMemoriesFound"], 3)


    def test_subject_analyst_synthesizes_crow_like_memory_without_public_overclaim(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            for idx in range(18):
                conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", f"Crow has recurring public BARCODE community interaction pattern {idx}.", "public_home", "public", "public_safe", 1, 0))
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow may be a music artist with queue history needing confirmation.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind artist link mention with no provenance.", 0, "active"))
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe Priority customer raw Discord ID 123456 note.", "public_safe", 1))
            conn.commit(); conn.close()
            resolved = resolve_subject_memory("Crow", tmp.name)
            analyst = build_subject_analyst_read("Crow", resolved)
            self.assertIn("internalRead", analyst)
            self.assertEqual(analyst["likelySubjectType"], "community_participant")
            self.assertLessEqual(len(analyst["draftIngredients"]), 9)
            self.assertTrue(any("role" in q.lower() for q in analyst["missingInfoQuestions"]))
            public_blob = " ".join(analyst["publicSafeClaims"] + analyst["draftIngredients"]).lower()
            for forbidden in ("stripe", "priority", "customer", "discord id", "source-blind", "queue history needing confirmation"):
                self.assertNotIn(forbidden, public_blob)
            self.assertTrue(any("role" in x.lower() or "queue" in x.lower() for x in analyst["reviewNeededClaims"] + analyst["doNotSayPublicly"]))


    def test_analyst_outputs_specific_claims_orion_and_safe_withheld_audit(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow may be a recurring BARCODE community participant who references Orion as an AI/message relay context.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow may have public Suno/music links and queue submission history needing confirmation.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe customer cus_123 paid Priority signal with raw Discord ID 123456789012345678.", "public_safe", 1))
            conn.commit(); conn.close()
            analyst = build_subject_analyst_read("Crow", resolve_subject_memory("Crow", tmp.name))
            review_blob = "\n".join(analyst["reviewNeededClaims"])
            self.assertIn("Possible relationship/context claim", review_blob)
            self.assertIn("Orion", review_blob)
            self.assertIn("Possible queue/submission claim", review_blob)
            self.assertNotIn("Possible role, queue/submission, link, or relationship claims require", review_blob)
            self.assertTrue(analyst["reviewableClaims"])
            self.assertTrue(any(c["claimType"] in {"relationship", "queue_submission", "music_link"} for c in analyst["reviewableClaims"]))
            self.assertTrue(any("Confirm Orion context" in q for q in analyst["missingInfoQuestions"]))
            audit = analyst["withheldEvidenceAudit"]
            self.assertGreaterEqual(audit["totalWithheld"], 1)
            self.assertGreaterEqual(audit["categories"]["private_internal"]["count"], 1)
            audit_blob = str(audit).lower()
            self.assertNotIn("cus_123", audit_blob)
            self.assertNotIn("123456789012345678", audit_blob)
            public_blob = " ".join(analyst["publicSafeClaims"] + analyst["draftIngredients"]).lower()
            self.assertNotIn("orion", public_blob)
            self.assertNotIn("queue", public_blob)
            self.assertNotIn("stripe", public_blob)


    def test_review_and_source_blind_contact_details_are_redacted(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow relationship context: Orion relay details at crow.private@example.com and phone 555-123-4567 need confirmation.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind Orion context mentions private contact crow.blind@example.com and Discord ID 123456789012345678.", 0, "active"))
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe customer cus_123 paid Priority signal contact crow.pay@example.com raw Discord ID 123456789012345678.", "public_safe", 1))
            conn.commit(); conn.close()
            resolved = resolve_subject_memory("Crow", tmp.name)
            analyst = build_subject_analyst_read("Crow", resolved)
            combined = " ".join([
                str(resolved["sourceSafetyWarnings"]),
                str(analyst["reviewNeededClaims"]),
                str(analyst["reviewableClaims"]),
                str(analyst["sourceBlindInsights"]),
                str(analyst["withheldEvidenceAudit"]),
            ]).lower()
            for leaked in ("crow.private@example.com", "crow.blind@example.com", "crow.pay@example.com", "555-123-4567", "123456789012345678", "cus_123"):
                self.assertNotIn(leaked, combined)
            self.assertTrue(any("Orion" in claim for claim in analyst["reviewNeededClaims"]))
            self.assertTrue(any("withheld" in c.get("safeEvidenceSummary", "").lower() for c in analyst["reviewableClaims"]))
            audit = analyst["withheldEvidenceAudit"]
            self.assertGreaterEqual(audit["categories"]["payment_customer_priority"]["count"], 1)
            public_blob = " ".join(analyst["publicSafeClaims"] + analyst["draftIngredients"]).lower()
            self.assertNotIn("example.com", public_blob)
            self.assertNotIn("orion", public_blob)

    def test_reviewable_claim_suggestions_cover_public_internal_source_blind_artifacts_and_weak_labels(self):
        public_claim = build_subject_analyst_read("Crow", {
            "subjectName": "Crow",
            "matchedAliasesUsedPrivately": [],
            "publicSafeFacts": ["Crow is a public-safe recurring BARCODE community member."],
            "publicSafeNotes": [],
            "publicCommunitySignals": [],
            "publicCreativeMusicSignals": [],
            "publicRoleSignals": [],
            "publicLinkSignals": [],
            "reviewOnlyEvidence": [
                {"summary": "Crow may have public Suno/music links and repeated music link context needing confirmation."},
                {"summary": "Crow may reference Orion as AI/message relay context needing confirmation."},
                {"summary": "subject-owned/keyed local evidence exists"},
                {"summary": "contest organizer"},
            ],
            "queueOrSubmissionSignals": ["Crow queue submission history needs confirmation."],
            "relationshipOrContextSignals": [],
            "sourceSafetyWarnings": ["Some subject memory lacked public-safe provenance"],
            "privateOrInternalEvidence": [],
            "evidenceCounts": {"publicSafe": 1, "reviewOnly": 4, "privateOrInternal": 0, "sourceBlind": 1, "totalScanned": 6},
        })
        claims = public_claim["reviewableClaims"]
        self.assertTrue(any(c.get("suggestedPublicWording") == "Crow is a BARCODE Network community member." for c in claims))
        music = next(c for c in claims if c["claimType"] == "music_link")
        self.assertIn("repeated music/link context", music["suggestedInternalNote"])
        self.assertIn("Which Crow links are owned", music["suggestedMissingInfoQuestion"])
        self.assertNotIn("suggestedPublicWording", music)
        orion = next(c for c in claims if c["claimType"] == "relationship")
        self.assertIn("Orion-related context", orion["suggestedInternalNote"])
        self.assertIn("Can BNL mention Orion", orion["suggestedMissingInfoQuestion"])
        blind = next(c for c in claims if c.get("actionability") == "source_blind_warning")
        self.assertEqual(blind["recommendedAction"], "needs_public_source")
        self.assertIn("Source-blind memory cannot become public copy", blind["cannotSuggestPublicReason"])
        artifact = next(c for c in claims if c.get("actionability") == "non_actionable_artifact")
        self.assertEqual(artifact["recommendedAction"], "reject")
        self.assertIn("technical evidence artifact", artifact["suggestedRejectionReason"])
        self.assertNotIn("suggestedPublicWording", artifact)
        weak = next(c for c in claims if c.get("actionability") == "weak_label")
        self.assertIn(weak["recommendedAction"], {"keep_internal", "reject"})
        self.assertIn('weak pattern label: "contest organizer"', weak["suggestedInternalNote"])
        self.assertTrue(any("Can Crow's queue/submission history" in c.get("suggestedMissingInfoQuestion", "") for c in claims))


    def test_source_blind_warnings_are_category_only_for_sensitive_fragments(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind Orion context email crow.orion@example.com Discord ID 123456789012345678.", 0, "active"))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind music link context at https://private.example/song and phone 555-222-3333.", 0, "active"))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind queue submission context for user ID 987654321098765432.", 0, "active"))
            conn.commit(); conn.close()
            resolved = resolve_subject_memory("Crow", tmp.name)
            warnings = "\n".join(resolved["sourceSafetyWarnings"])
            warning_blob = warnings.lower()
            for leaked in ("crow.orion@example.com", "https://private.example/song", "555-222-3333", "123456789012345678", "987654321098765432"):
                self.assertNotIn(leaked, warning_blob)
            self.assertIn("Source-blind Orion/context evidence exists for Crow, but raw details were withheld.", warnings)
            self.assertIn("Source-blind music/link context exists for Crow, but raw details were withheld.", warnings)
            self.assertIn("Source-blind queue/submission context exists for Crow, but raw details were withheld.", warnings)

    def test_private_payment_rejected_and_missing_tables_do_not_crash(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe checkout Priority customer note.", "public_safe", 1))
            conn.commit(); conn.close()
            result = resolve_subject_memory("Crow", tmp.name)
            self.assertEqual(result["evidenceCounts"]["privateOrInternal"], 1)
            self.assertEqual(result["publicSafeFacts"], [])

    def test_draft_uses_public_safe_resolver_memory_not_review_only_music(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow is an unconfirmed music artist from a review-only queue note.", "", "review_only", "", 0, 1))
            conn.commit(); conn.close()
            packet = draft.validate_dossier_draft_packet
            p = {
                "requestType": "bnl_proposed_dossier_draft", "version": "1.0",
                "candidate": {"sourceFileId": "sf_crow", "subjectName": "Crow"},
                "publicSafeFacts": [], "publicSafeNotes": [], "stylePacket": {}, "fieldRequirements": ["summary"],
                "safeClassification": {"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"},
            }
            result = draft.generate_dossier_draft(p, tmp.name)["draft"]
            self.assertNotEqual(result["role"], "Music artist")
            self.assertIn("limited public-safe source detail", result["summary"])
            self.assertTrue(any("resolver scanned" in x for x in result["ownerReviewWarnings"]))

    def test_draft_music_role_requires_public_safe_music(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow is a public-safe music artist connected to BARCODE Radio.", "public_home", "public", "public_safe", 1, 0))
            conn.commit(); conn.close()
            p = {"requestType": "bnl_proposed_dossier_draft", "version": "1.0", "candidate": {"sourceFileId": "sf_crow", "subjectName": "Crow"}, "publicSafeFacts": [], "publicSafeNotes": [], "stylePacket": {}, "fieldRequirements": ["summary"], "safeClassification": {"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"}}
            result = draft.generate_dossier_draft(p, tmp.name)["draft"]
            self.assertEqual(result["role"], "Music artist")
            self.assertIn("subject memory resolver", result["sourceUsageSummary"])

    def test_contest_context_lanes_do_not_infer_organizer_and_humanize_labels(self):
        analyst = build_subject_analyst_read("Crow", {
            "subjectName": "Crow", "matchedAliasesUsedPrivately": [],
            "publicSafeFacts": [], "publicSafeNotes": [], "publicCommunitySignals": [], "publicCreativeMusicSignals": [], "publicRoleSignals": [], "publicLinkSignals": [],
            "reviewOnlyEvidence": [{"summary": "Crow Suno contest name/rules/channel/dates/public link; lore/anomaly public_safe status; official_public_dossier source not connected yet; public artist links / public_music_role"}],
            "queueOrSubmissionSignals": [], "relationshipOrContextSignals": [], "sourceSafetyWarnings": [], "privateOrInternalEvidence": [],
            "evidenceCounts": {"publicSafe": 0, "reviewOnly": 1, "privateOrInternal": 0, "sourceBlind": 0, "totalScanned": 1},
        })
        claim = next(c for c in analyst["reviewableClaims"] if c["claimType"] == "contest_music_context")
        self.assertIn("Possible public music/link context", claim["claimText"])
        self.assertNotIn("organizer", claim["claimType"])
        self.assertEqual(claim["recommendedAction"], "needs_more_info")
        self.assertIn("which links are Crow-owned", claim["claimText"])
        self.assertIn("official public dossier", str(claim))
        self.assertNotIn("official_public_dossier", str(claim))
        self.assertNotIn("public_music_role", str(claim))
        self.assertTrue(claim["eventEvidenceHints"]["rulesMentioned"])
        self.assertTrue(any("relationship to this contest context" in q for q in analyst["missingInfoQuestions"]))

    def test_contest_strong_host_submitter_rules_and_admin_rejection(self):
        host = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Crow hosts the contest with public-safe confirmation."}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":1,"privateOrInternal":0,"sourceBlind":0,"totalScanned":1}})
        self.assertTrue(any(c["claimType"] == "contest_host" for c in host["reviewableClaims"]))
        submitted = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Crow submitted to the contest as a contest entry."},{"summary":"Crow posted rules for the contest."}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":2,"privateOrInternal":0,"sourceBlind":0,"totalScanned":2}})
        types = {c["claimType"] for c in submitted["reviewableClaims"]}
        self.assertIn("contest_submitter", types)
        self.assertIn("contest_rules_poster", types)
        self.assertNotIn("contest_organizer", types)
        rejected = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Admin note: Crow is not a contest organizer."},{"summary":"Crow contest organizer"}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":2,"privateOrInternal":0,"sourceBlind":0,"totalScanned":2}})
        correction = next(c for c in rejected["reviewableClaims"] if c.get("adminCorrectionApplied"))
        self.assertEqual(correction["recommendedAction"], "reject")
        self.assertEqual(correction["rejectedLabel"], "contest organizer")
        self.assertIn("Do not infer contest organizer", correction["blockedInference"])

    def test_admin_corrections_block_roles_reuse_facts_and_resolve_missing_info(self):
        packet = {
            "adminDecisions": [
                {"type": "rejected weak label", "label": "moderator", "note": "Admin rejected moderator for Crow."},
                {"type": "approved_public_fact", "text": "Crow is a BARCODE Network community member."},
                {"type": "missing_info_answer", "question": "What was Crow’s relationship to this contest context?", "answer": "Crow was mentioned around a contest link, but was not the organizer."},
            ]
        }
        analyst = build_subject_analyst_read("Crow", {
            "subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],
            "reviewOnlyEvidence":[{"summary":"Crow may be a moderator."},{"summary":"Crow hosts the contest link roundup."}],
            "queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],
            "evidenceCounts":{"publicSafe":0,"reviewOnly":2,"privateOrInternal":0,"sourceBlind":0,"totalScanned":2},
        }, packet)
        self.assertIn("Crow is a BARCODE Network community member.", analyst["publicReadyClaims"])
        self.assertFalse(any("relationship to this contest context" in q for q in analyst["missingInfoQuestions"]))
        blocked = [c for c in analyst["reviewableClaims"] if c.get("adminCorrectionApplied")]
        self.assertTrue(any(c.get("rejectedLabel") == "moderator" and c.get("recommendedAction") == "reject" for c in blocked))
        self.assertTrue(any(c.get("rejectedLabel") == "contest organizer" for c in blocked))

    def test_admin_conflict_review_card_and_public_boundary(self):
        packet = {"adminCorrectionNotes": [{"kind": "rejected_label", "text": "Admin rejected artist for Crow."}]}
        analyst = build_subject_analyst_read("Crow", {
            "subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],
            "reviewOnlyEvidence":[{"summary":"New public-looking evidence says Crow is an artist with music context."}],
            "queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":["Crow source-blind payment customer contact crow@example.com raw Discord ID 123456789012345678"],"privateOrInternalEvidence":[],
            "evidenceCounts":{"publicSafe":0,"reviewOnly":1,"privateOrInternal":0,"sourceBlind":1,"totalScanned":2},
        }, packet)
        conflict = next(c for c in analyst["reviewableClaims"] if c.get("reviewLane") == "admin_correction_conflict")
        self.assertEqual(conflict["recommendedAction"], "needs_more_info")
        self.assertIn("Admin previously rejected this label", conflict["suggestedMissingInfoQuestion"])
        public_blob = " ".join(analyst["publicReadyClaims"] + analyst["draftIngredients"]).lower()
        for forbidden in ("admincorrections", "admincorrectionapplied", "rejectedlabel", "blockedinference", "conflictingevidencesummary", "source-blind", "payment", "crow@example.com", "123456789012345678"):
            self.assertNotIn(forbidden.lower(), public_blob)


if __name__ == '__main__':
    unittest.main()
