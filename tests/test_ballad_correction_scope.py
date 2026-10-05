"""Correction review retains the original song as inert comparison data."""
import json
import unittest

from bnl_broadcast_ballads import attribution_review_prompt


class BalladCorrectionScopeTests(unittest.TestCase):
    def test_second_review_receives_original_composition_and_exact_findings(self):
        original = {"title": "Last Light", "style": "chamber soul",
                    "lyrics": "[Verse]\nLeave a light", "palette": {}, "linerNotes": {}}
        corrected = {**original, "title": "Metal Birds", "style": "industrial techno",
                     "lyrics": "[Bridge]\nMetal birds assemble"}
        findings = {"verdict": "unsupported",
                    "issues": ["An addressee was mistaken for the speaker"]}
        prompt = attribution_review_prompt("Authorized original sources", corrected,
            correction_original=original, correction_findings=findings)
        for label, expected in (("ORIGINAL_DRAFT_JSON: ", original),
                                ("CORRECTION_FEEDBACK_JSON: ", findings),
                                ("DRAFT_JSON: ", corrected)):
            value = next(line[len(label):] for line in prompt.splitlines()
                         if line.startswith(label))
            self.assertEqual(json.loads(value), expected)
        self.assertIn("Use unsupported if unrelated changes replace", prompt)
        self.assertIn("not factual authorities", prompt)
        self.assertIn("Allow changes required by the original sources", prompt)
        self.assertIn("Do not score taste, artistry or word overlap", prompt)

    def test_initial_review_has_no_unavailable_original_comparison(self):
        prompt = attribution_review_prompt("Original sources", {"lyrics": "A song"})
        self.assertNotIn("ORIGINAL_DRAFT_JSON", prompt)
        self.assertNotIn("CORRECTION_FEEDBACK_JSON", prompt)
