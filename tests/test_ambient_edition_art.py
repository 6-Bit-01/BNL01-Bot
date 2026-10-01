import copy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

import bnl_own_art as art


class AmbientEditionArtTests(unittest.TestCase):
    def setUp(self):
        self.anchor = {"ref": "fresh:1", "kind": "conversation",
                       "observedAt": "2026-01-01T12:00:00Z", "summary": "A public discussion about a rhythm."}
        self.packet = {
            "sourceWindowStart": "2026-01-01T11:54:00+00:00",
            "sourceWindowEnd": "2026-01-01T12:06:00+00:00",
            "safeSources": [
                {"refId": "fresh:1", "sourceKind": "conversation", "summary": self.anchor["summary"],
                 "observedAt": self.anchor["observedAt"]},
                {"refId": "fresh:2", "sourceKind": "conversation", "summary": "WITHDRAWN ORIGINAL TEXT",
                 "observedAt": "2026-01-01T12:01:00Z"},
            ],
        }
        self.context = {"sources": [self.anchor], "sourceBases": [], "continuity": []}
        self.proposal = {"action": "create", "title": "Shared rhythm", "meaning": "A public idea about music.",
                         "imagePrompt": "A playful clay rhythm machine.", "inspirationRefs": ["fresh:1"]}
        self.provider = mock.Mock(return_value=json.dumps(self.proposal))
        self.bot = SimpleNamespace(
            DB_FILE="unused-test-database", BNL01_PACKET_OWNED_SYSTEM_PROMPT="Public Network intelligence.",
            _generate_gemini_content_with_fallback=self.provider,
            _extract_text_and_tokens=lambda response: (response, 1),
            revalidate_ambient_local_sources=mock.Mock(return_value=True),
        )
        reader_patch = mock.patch.object(art, "build_source_packet_between", side_effect=lambda *a, **kw: copy.deepcopy(self.packet))
        self.reader = reader_patch.start()
        self.addCleanup(reader_patch.stop)

    def public_expansion(self, packet, start, end):
        self.assertLess(start, end)
        filtered = {**packet, "safeSources": [item for item in packet["safeSources"] if item["refId"] != "fresh:2"]}
        self.context["sourceBases"].append({"ambient": {
            "guild_id": 42, "rows": {"conversations": {1: "current-original-digest"}},
        }})
        return filtered

    def test_filtered_expansion_cannot_reenter_concept_prompt_or_selected_refs(self):
        filter_packet = mock.Mock(side_effect=self.public_expansion)
        self.context["packet_filter"] = filter_packet
        result = art.develop_art_concept(self.bot, 42, self.proposal, self.context)
        filter_packet.assert_called_once()
        self.provider.assert_called_once()
        prompt = self.provider.call_args.args[0]
        self.assertNotIn("WITHDRAWN ORIGINAL TEXT", prompt)
        self.assertNotIn("fresh:2", prompt)
        self.assertIn(self.anchor["summary"], prompt)
        self.assertEqual(result["inspirationRefs"], ["fresh:1"])
        self.bot.revalidate_ambient_local_sources.assert_called_once_with(
            42, {"guild_id": 42, "rows": {"conversations": {1: "current-original-digest"}}, "tier_sources": {}})

    def test_callback_original_root_withdrawal_blocks_concept_provider(self):
        self.context["packet_filter"] = self.public_expansion
        # The callback stores the exact original-source basis. Existing art
        # revalidation must see its failure before making a provider call.
        self.bot.revalidate_ambient_local_sources.return_value = False
        with self.assertRaisesRegex(ValueError, "art_sources_changed"):
            art.develop_art_concept(self.bot, 42, self.proposal, self.context)
        self.bot.revalidate_ambient_local_sources.assert_called_once()
        self.provider.assert_not_called()

    def test_existing_art_context_without_filter_preserves_its_behavior(self):
        art.develop_art_concept(self.bot, 42, self.proposal, self.context)
        self.assertIn("WITHDRAWN ORIGINAL TEXT", self.provider.call_args.args[0])
        self.bot.revalidate_ambient_local_sources.assert_not_called()


if __name__ == "__main__":
    unittest.main()
