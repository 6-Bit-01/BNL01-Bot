import copy
import json
import os
from types import SimpleNamespace
import unittest
from unittest import mock

import bnl_ambient_edition as edition


class EditionExpressionTests(unittest.TestCase):
    def setUp(self):
        self.context = {
            "window_start": "2026-09-29T03:00:00Z", "window_end": "2026-09-30T03:00:00Z",
            "_private_provenance": "NEVER_RENDER_PRIVATE_ROOTS",
            "items": [
                {"ref": "conversation:1", "kind": "conversation", "label": "Test Member",
                 "text": "Test Member described a clay radio during the show.", "url": "",
                 "occurred_at": "2026-09-29T04:00:00Z", "published_at": "",
                 "subject_refs": ["discord_user:123"], "subject_labels": {"discord_user:123": "Test Member"}},
                {"ref": "journal:entry", "kind": "journal", "label": "Clay Radio Notes",
                 "text": "A newly published Journal reflects on an older show.",
                 "url": "https://example.test/journal/entry", "subject_refs": [],
                 "occurred_at": "", "published_at": "2026-09-29T23:00:00Z",
                 "reported_window_start": "2026-09-25T00:00:00Z", "reported_window_end": "2026-09-26T00:00:00Z"},
            ],
        }
        self.draft = {"action": "post", "headline": "A radio made of clay",
                      "stories": [{"text": "[[person:discord_user:123]] imagined a clay radio. That deserves a second look.",
                                   "sourceRefs": ["conversation:1", "journal:entry"],
                                   "subjectRefs": ["discord_user:123"]}], "art": None}

    def parse(self):
        return edition.parse_response(json.dumps(self.draft), self.context)

    def test_source_owned_link_labels_and_attribution_survive(self):
        result = self.parse()
        self.assertIn("Test Member imagined a clay radio", result["description"])
        self.assertIn("[Clay Radio Notes](<https://example.test/journal/entry>)", result["description"])
        self.assertEqual(result["subject_refs"], ("discord_user:123",))
        self.assertNotIn("<@", result["description"])

    def test_deduplicate_links_across_independent_stories(self):
        self.draft["stories"].append({"text": "The Journal also considers the texture of sound.",
                                      "sourceRefs": ["journal:entry"], "subjectRefs": []})
        body = self.parse()["description"]
        self.assertEqual(body.count("https://example.test/journal/entry"), 1)
        self.assertIn("\n\nThe Journal", body)

    def test_unknown_source_and_person_cannot_become_authority(self):
        for key, value in (("sourceRefs", ["journal:invented"]), ("subjectRefs", ["discord_user:456"])):
            with self.subTest(key=key):
                original = self.draft["stories"][0][key]
                self.draft["stories"][0][key] = value
                with self.assertRaises(ValueError):
                    self.parse()
                self.draft["stories"][0][key] = original

    def test_a_person_in_another_story_does_not_authorize_this_story(self):
        self.draft["stories"][0]["sourceRefs"] = ["journal:entry"]
        with self.assertRaisesRegex(ValueError, "unbound_subject"):
            self.parse()

    def test_notifications_and_invented_urls_are_never_model_authored(self):
        for suffix in (" <@123>", " <@&456>", " @everyone", " @here", " https://evil.test", " www.evil.test"):
            with self.subTest(suffix=suffix):
                original = self.draft["stories"][0]["text"]
                self.draft["stories"][0]["text"] += suffix
                with self.assertRaisesRegex(ValueError, "unowned_link_or_mention"):
                    self.parse()
                self.draft["stories"][0]["text"] = original

    def test_long_complete_article_is_not_trimmed_to_legacy_280_chars(self):
        self.draft["stories"][0]["text"] += " The interesting part was the shared musical idea." * 18
        result = self.parse()
        self.assertGreater(len(result["description"]), 800)
        self.assertTrue(result["description"].endswith("journal/entry>)"))

    def test_platform_limit_includes_source_links_and_utf16(self):
        self.draft["stories"][0]["text"] += "🎵" * 1900
        with self.assertRaisesRegex(ValueError, "too_long"):
            self.parse()

    def test_skip_and_one_story_are_valid_without_required_categories(self):
        self.assertEqual(edition.parse_response('{"action":"skip"}', self.context), {"action": "skip"})
        self.assertEqual(self.parse()["action"], "post")

    def test_missing_evidence_cannot_become_a_fake_quiet_day_story(self):
        with self.assertRaisesRegex(ValueError, "missing_or_unknown_source"):
            edition.parse_response(json.dumps(self.draft), {**self.context, "items": []})

    def test_prompt_preserves_dates_and_excludes_private_receipts(self):
        prompt = edition.build_prompt(self.context, current_time="September 29", show_context="unknown",
                                      recent_editions=[], art_available=False)
        self.assertIn("2026-09-25T00:00:00Z", prompt)
        self.assertIn("occurred_at", prompt)
        self.assertIn("published_at", prompt)
        self.assertNotIn("NEVER_RENDER_PRIVATE_ROOTS", prompt)
        self.assertIn("Return art:null", prompt)
        self.assertIn("quieter people", prompt)

    def test_disabled_until_exact_activation_and_only_primary_guild(self):
        bot = SimpleNamespace(BNL_PRIMARY_GUILD_ID=42)
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(edition.enabled(bot, 42))
            os.environ["BNL_AMBIENT_COMMUNITY_EDITION_ENABLED"] = "true"
            self.assertTrue(edition.enabled(bot, 42))
            self.assertFalse(edition.enabled(bot, 43))

    def test_link_text_cannot_smuggle_a_ping_and_urls_require_https(self):
        self.context["items"][1]["label"] = "<@456> @everyone [bad](link)"
        result = self.parse()
        self.assertNotIn("@", result["description"])
        for url in ("javascript:alert(1)", "https://user:secret@example.test/path", "https://example.test/\n@everyone"):
            self.assertEqual(edition.source_url(url), "")


if __name__ == "__main__":
    unittest.main()
