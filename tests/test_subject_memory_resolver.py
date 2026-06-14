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


if __name__ == '__main__':
    unittest.main()
