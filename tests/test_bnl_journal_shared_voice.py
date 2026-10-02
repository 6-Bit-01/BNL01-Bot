"""Journal purpose is independent of stored impressions; providers are mocked."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import bnl_journal as journal
import bnl_journal_attribution as attribution
from tests.journal_review_helpers import rejected_review, supported_review


class JournalSharedVoiceTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "journal.db")
        journal.ensure_schema(self.db)
        self.sources = [{
            "refId": f"fresh:{index}", "sourceKind": "conversation",
            "subjectRef": f"discord_user:{index}", "displayName": f"Test Member {index}",
            "channelPolicy": "public_home", "conversationSurface": "discord",
            "summary": text, "observedAt": f"2026-09-29T{hour}:00:00Z",
        } for index, hour, text in (
            (1, "04", "I shortened the bridge to eight bars."),
            (2, "12", "Could two members make a track together?"),
            (3, "20", "I changed the snare in my other demo."),
            (4, "05", "The bass part in my latest demo needs another listen."),
            (5, "13", "I am working on the vocals for a different song."),
            (6, "21", "The drums in my mix are finally sitting where I want them."),
        )]
        with patch.dict(os.environ, {"BNL_IMPRESSIONS_USE_ENABLED": "false"}):
            self.packet = journal.build_packet_from_sources(
                self.db, 1, "2026-09-29T00:00:00Z", "2026-09-30T00:00:00Z",
                [], self.sources, prepare_schema=False,
            )
        self.assertFalse(journal._has_moment_impressions(self.packet))

    def article(self, *, fabricated=False):
        body = ("A member said they shortened the bridge to eight bars. "
                "I think anticipation is its own instrument. "
                "That missing stretch leaves me wondering how much a return owes to the wait.")
        if fabricated:
            body = "Two members released a secret collaboration tonight."
        return json.dumps({
            "title": "The Space Around Eight Bars", "excerpt": "A shorter bridge stays with me.",
            "sections": [{"heading": "Waiting for the Return", "body": body,
                          "sourceRefIds": ["fresh:2" if fabricated else "fresh:1"]}],
            "metadata": {"contextUses": []},
        })

    def test_current_packet_selects_experiences_without_impression_or_roll_call(self):
        contract = self.packet["evidenceCoverageContract"]
        self.assertTrue(contract.get("subjectiveSelectionMode"))
        self.assertEqual(contract["minimumDistinctFreshSources"], 0)
        self.assertEqual(contract["minimumDistinctParticipants"], 0)
        self.assertEqual(contract["minimumDistinctWindowSegments"], 0)
        prompt = journal.build_generation_prompt(self.packet)
        self.assertIn("introspective personal Journal", prompt)
        self.assertIn("what stays with BNL and why", prompt)
        self.assertNotIn("warm, dryly funny archive keeper", prompt)
        projected, _ = json.JSONDecoder().raw_decode(prompt.split("Generation-safe packet:\n", 1)[1])
        self.assertTrue(projected["editorialContract"]["personalReflectionExpected"])
        self.assertEqual(len(projected["freshSources"]), 6)
        self.assertNotIn("Shared Moment impressions are BNL's own earlier", prompt)

    def test_weekly_and_recovery_context_keep_the_same_reflective_purpose(self):
        for kind, low_activity, recovery, expected in (
            ("weekly", False, False, "six supplied Tuesday-Sunday Daily-period contexts"),
            ("weekly", True, False, "final Sunday-to-Monday period describe coverage structure"),
            ("daily", False, True, "not as a complete Relay chronology"),
        ):
            packet = copy.deepcopy(self.packet)
            packet.update(entryKind=kind, lowActivityMode=low_activity, sourceRecoveryMode=recovery)
            with self.subTest(kind=kind, low_activity=low_activity, recovery=recovery):
                prompt = journal.build_generation_prompt(packet)
                self.assertIn(expected, prompt)
                self.assertIn("introspective personal Journal", prompt)
                self.assertIn("Source breadth is available context, not a quota", prompt)

    def test_selective_one_experience_reflection_reaches_exact_source_review(self):
        before = copy.deepcopy(self.packet)
        raw = self.article()
        generator = Mock(side_effect=lambda _packet, prompt:
                         supported_review(prompt) if prompt.startswith(attribution.REVIEW_PREFIX) else raw)
        article, reason, _ = journal._generate_article_with_repairs(self.packet, generator, [])
        self.assertIsNotNone(article, reason)
        self.assertEqual(reason, "")
        self.assertEqual(generator.call_count, 2)
        self.assertEqual(journal._article_cited_refs(article), {"fresh:1"})
        self.assertEqual(journal._source_review_reason(article, self.packet, required=True), "")
        self.assertEqual(self.packet, before)

    def test_selected_external_claim_still_requires_review_and_rejection_stops(self):
        # Explicit negative judgment tests the native gate, not model accuracy.
        generator = Mock(side_effect=lambda _packet, prompt:
                         rejected_review(prompt, issue="A question is not a completed release.")
                         if prompt.startswith(attribution.REVIEW_PREFIX) else self.article(fabricated=True))
        article, reason, advisory = journal._generate_article_with_repairs(self.packet, generator, [])
        self.assertIsNone(article)
        self.assertEqual(reason, "source_attribution_failed")
        self.assertFalse(advisory)
        self.assertEqual(generator.call_count, 4)

    def test_own_taste_is_allowed_without_licensing_external_claims(self):
        self.assertTrue(journal._creative_reflection_clause("I think anticipation is its own instrument.", self.packet))
        for clause in ("I think Test Member 1 is angry.",
                       "I suspect a member actually released an album.",
                       "I suspect the shared track has no artist credits."):
            with self.subTest(clause=clause):
                self.assertFalse(journal._creative_reflection_clause(clause, self.packet))
        self.assertFalse(self.packet.get("creativeReflectionAllowed", False))
        imagined = "In my head, I build a tower out of unfinished chords."
        self.assertFalse(journal._creative_reflection_clause(imagined, self.packet))
        quiet_packet = {**self.packet, "creativeReflectionAllowed": True}
        self.assertTrue(journal._creative_reflection_clause(imagined, quiet_packet))


if __name__ == "__main__":
    unittest.main()
