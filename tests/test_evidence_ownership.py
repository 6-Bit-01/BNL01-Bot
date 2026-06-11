import unittest

from bnl_evidence_ownership import classify_evidence_ownership, subject_owned_text_fragments


class EvidenceOwnershipStrictGateTests(unittest.TestCase):
    def classify(self, item, subject="Antigrain"):
        return classify_evidence_ownership(item, subject, subject.lower(), [])

    def test_source_file_marker_without_subject_fields_is_not_site_authoritative(self):
        classified = self.classify({"summary": "Source File note says Orion is nearby in public context.", "visibility": "public_context"})
        self.assertNotEqual(classified["ownership"], "site_authoritative")
        self.assertIn(classified["ownership"], {"shared_topic", "co_mention", "unknown", "generated_or_taxonomy"})

    def test_identity_or_public_profile_marker_without_subject_fields_is_not_site_authoritative(self):
        for text in ("Identity check found a public profile near Orion.", "Public profile dossier mentions Orion in broad context."):
            with self.subTest(text=text):
                classified = self.classify({"summary": text, "visibility": "public_context"})
                self.assertNotEqual(classified["ownership"], "site_authoritative")
                self.assertIn(classified["ownership"], {"shared_topic", "co_mention", "unknown", "generated_or_taxonomy"})

    def test_derived_relationship_lane_without_subject_metadata_is_review_only(self):
        classified = self.classify({"summary": "Orion appears near Antigrain in relationship context.", "sourceLane": "relationshipSignals", "visibility": "public_context"})
        self.assertNotIn(classified["ownership"], {"owned", "site_authoritative", "queue_authoritative", "broadcast_memory_authoritative"})
        self.assertIn(classified["ownership"], {"co_mention", "shared_topic", "generated_or_taxonomy"})

    def test_authority_requires_explicit_subject_match(self):
        missing_subject_items = [
            {"summary": "Queue submission missing status for a nearby public profile.", "visibility": "public_context"},
            {"summary": "Site authoritative source file note mentions Orion broadly.", "visibility": "public_context"},
            {"summary": "Broadcast memory says BARCODE Radio played on a public episode.", "visibility": "public_context"},
        ]
        for item in missing_subject_items:
            with self.subTest(item=item):
                self.assertNotIn(self.classify(item)["ownership"], {"queue_authoritative", "site_authoritative", "broadcast_memory_authoritative"})

        # Explicit subject fields are still subject-owned evidence and therefore allowed
        # through the owned gate before generic authority checks run.
        owned = self.classify({"summary": "Source note attached to Antigrain.", "subjectName": "Antigrain", "sourceFileNote": True, "visibility": "public_context"})
        self.assertIn(owned["ownership"], {"owned", "site_authoritative"})

    def test_subject_fragments_include_overpromotion_diagnostics(self):
        memory = {"sourceIntelligenceContext": {"relationshipSignals": ["Orion appears near Antigrain in broad relationship context only."]}}
        fragments = subject_owned_text_fragments(memory, "Antigrain", "antigrain")
        diagnostics = fragments["summary"]["ownershipRoutingDiagnostics"]
        self.assertTrue(any(item.get("term") == "Orion" for item in diagnostics["overpromotedCandidatesBlocked"]))


if __name__ == "__main__":
    unittest.main()
