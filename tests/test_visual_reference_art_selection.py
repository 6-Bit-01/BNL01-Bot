"""Reference availability must not become a compulsory cast or a second owner."""
import json
from types import SimpleNamespace
import unittest
from unittest import mock

import bnl_own_art as art


CONCEPT = {"action": "create", "title": "Quiet orbit", "meaning": "An imagined room.",
           "imagePrompt": "An empty room above an ocean.", "inspirationRefs": []}
SNAPSHOT = {"contractVersion": 1, "guildId": 4242, "subjectId": "test_character",
            "label": "Test Character", "revisionId": "neutral-revision", "assets": []}


class VisualReferenceArtSelectionTests(unittest.TestCase):
    def test_explicit_depiction_survives_concept_parsing(self):
        result = art.parse_own_art_concept(json.dumps({**CONCEPT,
            "depictedSubjects": ["test_character"]}), set())
        self.assertEqual(result["depictedSubjects"], ["test_character"])

    def test_canonical_subject_keys_may_begin_with_a_digit(self):
        result = art.parse_own_art_concept(json.dumps({**CONCEPT,
            "depictedSubjects": ["7_test_character"]}), set())
        self.assertEqual(result["depictedSubjects"], ["7_test_character"])

    def test_missing_depiction_stays_empty_and_invalid_fields_are_rejected(self):
        self.assertEqual(art.parse_own_art_concept(json.dumps(CONCEPT), set()).get("depictedSubjects", []), [])
        for value in ("test_character", ["../private"], [1], ["Test Character"]):
            with self.subTest(value=value), self.assertRaises(ValueError):
                art.parse_own_art_concept(json.dumps({**CONCEPT, "depictedSubjects": value}), set())

    def test_unrelated_art_does_not_load_reference_pixels_or_add_lineage(self):
        context = {"sourceBases": [], "visualReferenceSnapshot": SNAPSHOT}
        with mock.patch.object(art, "load_visual_reference_inputs", create=True) as loader:
            inputs, snapshot = art.bind_art_visual_references(SimpleNamespace(DB_FILE="unused"),
                                                             4242, CONCEPT, context)
        self.assertEqual((inputs, snapshot), ((), None))
        self.assertEqual(context["sourceBases"], [])
        loader.assert_not_called()

    def test_selected_pixels_retain_existing_source_lineage(self):
        context = {"sourceBases": [], "visualReferenceSnapshot": SNAPSHOT}
        inputs = ({"mimeType": "image/png", "data": b"neutral pixels"},)
        with mock.patch.object(art, "load_visual_reference_inputs", create=True,
                               return_value=(SNAPSHOT, inputs)) as loader:
            actual = art.bind_art_visual_references(SimpleNamespace(DB_FILE="unused"), 4242,
                {**CONCEPT, "depictedSubjects": ["test_character"]}, context)
        self.assertEqual(actual, (inputs, SNAPSHOT))
        self.assertEqual(context["sourceBases"], [{"visualReferences": SNAPSHOT}])
        self.assertEqual(loader.call_args.kwargs["snapshot"], SNAPSHOT)

    def test_retired_reference_invalidates_saved_art_lineage(self):
        with mock.patch.object(art, "visual_reference_snapshot_current", create=True,
                               return_value=False):
            self.assertFalse(art.art_sources_current(SimpleNamespace(DB_FILE="unused"), 4242,
                                                    [{"visualReferences": SNAPSHOT}]))

    def test_available_reference_is_optional_and_only_safe_identity_is_prompted(self):
        prompt = art.build_own_art_creative_context(reference_subjects=(
            {"subjectId": "test_character", "label": "Test Character"},))
        self.assertIn("depictedSubjects", prompt)
        self.assertIn("Test Character", prompt)
        self.assertIn("not a reason to include", prompt)
        self.assertNotIn("No visual reference images or established appearances are supplied", prompt)


if __name__ == "__main__":
    unittest.main()
