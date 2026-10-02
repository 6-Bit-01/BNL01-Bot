from tests.journal_review_helpers import reviewed_article
"""The Journal reads shared Moment opinions, retaining original evidence and fences."""
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as source_store
import bnl_moment_engine as moments
from tests import test_moment_meaning as meaning
from tests.test_bnl_journal_prepared_release import article_json


START = "2026-08-28T01:30:00Z"
END = "2026-08-29T01:30:00Z"


class JournalMomentImpressionTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "journal.db")
        self.fixture = meaning.MomentMeaningTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        flags = mock.patch.dict(os.environ, {
            "BNL_IMPRESSIONS_FORMATION_ENABLED": "true", "BNL_IMPRESSIONS_USE_ENABLED": "true",
            "BNL_IMPRESSIONS_GUILD_IDS": "1",
        })
        flags.start()
        self.addCleanup(flags.stop)
        clock = mock.patch.object(source_store, "_now_ms", return_value=0)
        clock.start()
        self.addCleanup(clock.stop)
        self.mid, self.roots = self.fixture.captured_moment(
            started_at=datetime(2026, 8, 27, 12, tzinfo=timezone.utc))
        self.fixture.enrich({**meaning.REPORTER_MEANING, "impression": {
            "impression": "I find the playful authority of this exchange oddly endearing.",
            "reason": "The contrast between earnest approval and dry skepticism held my attention.",
            "sourceRefs": ["turn_1", "turn_3"],
        }})
        with sqlite3.connect(self.db) as conn:
            self.fixture.conn.backup(conn)
        journal.ensure_schema(self.db)
        automation.ensure_schema(self.db)
        source_store.ensure_schema(self.db)

    def packet(self):
        return journal.build_packet_from_sources(self.db, 1, START, END, [], [{
            "refId": "conversation:42", "sourceKind": "conversation", "subjectRef": "discord_user:2",
            "displayName": "Test Member 2", "channelPolicy": "public_home", "conversationSurface": "discord",
            "summary": "The journalists and reporters joke returned to the broadcast discussion.",
            "observedAt": "2026-08-28T12:00:00Z",
        }], prepare_schema=False)

    def impression(self, packet):
        return next(item for item in packet["reflectionBasis"] if item["basisKind"] == "moment_impression")

    def reflective_article(self, packet):
        source = self.impression(packet)
        return journal.parse_generated_json(json.dumps({
            "title": "The Weight of a Joke", "excerpt": "An old exchange leaves an impression.",
            "sections": [{"heading": "What stayed with me", "sourceRefIds": [source["refId"]],
                          "body": "I keep thinking about the old reporter joke. I think dry humor matters to me more than formal authority. Looking back, that tension still amuses me."}],
            "metadata": {"contextUses": []},
        }))

    def test_real_shared_selector_provides_subjectivity_and_separate_originals(self):
        packet = self.packet()
        item = self.impression(packet)
        self.assertIn("endearing", item["impression"])
        self.assertEqual(item["authority"], "bnl_subjective_perspective_not_event_evidence")
        self.assertEqual(len(item["evidence"]), len(meaning.REPORTERS))
        self.assertIn("old school reporters", item["evidence"][2]["summary"])
        self.assertEqual(item["evidence"][1]["authority"], "speech_only")
        self.assertEqual(item["sourceObservedAt"], "2026-08-27T12:00:50+00:00")
        prompt = journal.build_generation_prompt(packet)
        self.assertIn("revisable perspective", prompt)
        self.assertNotIn("discord_user:", prompt)
        self.assertNotIn(self.roots[0], prompt)
        self.assertEqual(packet["aggregateCounts"]["eligibleConversations"], 1)

    def test_use_off_is_identical_to_no_shared_impressions(self):
        with mock.patch.dict(os.environ, {"BNL_IMPRESSIONS_USE_ENABLED": "false"}):
            off = self.packet()
        with mock.patch.object(moments, "select_moment_impressions", return_value=[]):
            baseline = self.packet()
        self.assertEqual(off, baseline)
        self.assertEqual(journal.build_generation_prompt(off), journal.build_generation_prompt(baseline))
        self.assertFalse(journal._has_moment_impressions(off))
        self.assertNotIn("subjectiveSelectionMode", off["evidenceCoverageContract"])

    def test_reflective_section_needs_no_new_event_or_participant_roll_call(self):
        packet = self.packet()
        self.assertTrue(packet["evidenceCoverageContract"]["subjectiveSelectionMode"])
        self.assertEqual(journal.validate_article(self.reflective_article(packet), packet), "")

    def test_retained_impression_preserves_previous_journal_body_as_continuity(self):
        body = "I remember the reporters joke: earnest approval met dry skepticism, and I preferred that playful authority."
        history = {"previousEntry": {
            "entry_id": "old-journal", "revision": 1, "published_at": "2026-08-20T12:00:00Z",
            "title": "An Earlier Thought", "excerpt": "An earlier perspective worth remembering.",
            "sections_json": json.dumps([{"heading": "A callback", "body": body}]),
        }}
        with mock.patch.object(journal, "retrieve_history", return_value=history):
            packet = self.packet()
        prompt = journal.build_generation_prompt(packet)
        safe = json.loads(prompt.split("Generation-safe packet:\n", 1)[1])
        self.assertEqual(body,
                         safe["history"]["previousEntry"]["sectionSnapshots"][0]["bodyExcerpt"])
        self.assertEqual("prior_bnl_expression_for_continuity_not_evidence_or_style_template",
                         safe["editorialContract"]["historyRole"])
        self.assertIn("not independent proof or a writing template", prompt)

    def test_retained_impression_keeps_unrelated_previous_journal_as_dated_card(self):
        history = {"previousEntry": {
            "entry_id": "old-journal", "revision": 1, "published_at": "2026-08-20T12:00:00Z",
            "title": "An Earlier Thought", "excerpt": "An earlier perspective worth remembering.",
            "sections_json": json.dumps([{"heading": "A callback", "body": "The room once left me with a different question."}]),
        }}
        with mock.patch.object(journal, "retrieve_history", return_value=history):
            packet = self.packet()
        safe = journal._journal_prompt_projection(packet)
        card = safe["history"]["previousEntry"]
        self.assertEqual("old-journal", card["entryId"])
        self.assertEqual("2026-08-20T12:00:00Z", card["publishedAt"])
        self.assertEqual("An earlier perspective worth remembering.", card["excerpt"])
        self.assertEqual("", card["sectionSnapshots"][0]["bodyExcerpt"])

    def test_saved_subjective_thought_may_be_quoted_without_becoming_current_activity(self):
        packet = self.packet()
        article = self.reflective_article(packet)
        article["sections"][0]["body"] = 'Looking back, my earlier thought was "' + self.impression(packet)["impression"] + '" I still find that tension amusing.'
        self.assertEqual(journal.validate_article(article, packet), "")

    def test_current_action_cannot_be_supported_by_an_impression(self):
        packet = self.packet()
        article = self.reflective_article(packet)
        article["sections"][0]["body"] += " Tonight a member released a new track."
        self.assertEqual(journal.validate_article(article, packet), "current_activity_without_fresh_source")

    def test_nested_original_refs_and_private_aliases_cannot_enter_any_public_field(self):
        packet = self.packet()
        evidence = self.impression(packet)["evidence"][0]
        for token, reason in ((evidence["refId"], "source_ref_leak"),
                              (evidence["participantAlias"], "public_leak_pattern")):
            for field in ("title", "excerpt", "heading", "body"):
                with self.subTest(token_kind=reason, field=field):
                    article = self.reflective_article(packet)
                    target = article if field in {"title", "excerpt"} else article["sections"][0]
                    target[field] += " " + token
                    self.assertEqual(journal._article_privacy_reason(article, packet), reason)

    def test_uncited_impression_survives_freeze_and_blocks_after_use_gate_changes(self):
        # build_packet_from_sources is the lower-level fixture path. Scheduled
        # preparation additionally requires the archive-ready envelope normally
        # supplied by build_source_packet_between; keep that production fence.
        packet = {**self.packet(), "sourceArchiveAvailable": True}
        status, run, epoch, _ = automation._claim_preparation(self.db, 1, "daily", START, END, force=True)
        self.assertEqual(status, "claimed")
        frozen, digest, reason = automation._freeze_or_load_packet(self.db, 1, run, epoch, lambda: packet)
        self.assertEqual(reason, "")
        self.assertEqual(self.impression(frozen), self.impression(packet))
        self.assertTrue(digest)
        with mock.patch.dict(os.environ, {"BNL_IMPRESSIONS_USE_ENABLED": "false"}):
            frozen, _, reason = automation._freeze_or_load_packet(
                self.db, 1, run, epoch, lambda: self.fail("Stale input cannot be silently replaced"))
        self.assertIsNone(frozen)
        self.assertEqual(reason, "privacy_source_ineligible")

    def test_retracted_original_stops_before_generation(self):
        packet = self.packet()
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE memory_ledger_entries SET lifecycle_status='retracted' WHERE entry_id=?", (self.roots[0],))
        generator = mock.Mock(side_effect=AssertionError("No provider call for a revoked source"))
        result = journal.generate_and_store_packet_draft(self.db, 1, packet, generator)
        self.assertEqual(result.reason, "privacy_source_ineligible")
        generator.assert_not_called()

    def test_impression_revision_during_generation_is_not_saved(self):
        packet = self.packet()
        def generate(value, _prompt):
            with sqlite3.connect(self.db) as conn:
                conn.execute("UPDATE memory_moment_windows SET impression_payload='' WHERE moment_id=?", (self.mid,))
            return article_json(value)
        result = journal.generate_and_store_packet_draft(self.db, 1, packet, generate)
        self.assertEqual(result.reason, "privacy_source_ineligible")
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM bnl_journal_entries").fetchone()[0], 0)

    def test_provider_failure_cannot_resurrect_an_advisory_after_source_revocation(self):
        packet = self.packet()
        calls = []
        def generate(value, _prompt):
            calls.append(1)
            if len(calls) == 1:
                article = json.loads(article_json(value))
                article["sections"][0]["body"] = "Records indicate a recurring theme. " + article["sections"][0]["body"]
                return json.dumps(article)
            with sqlite3.connect(self.db) as conn:
                conn.execute("UPDATE memory_moment_windows SET impression_payload='' WHERE moment_id=?", (self.mid,))
            raise RuntimeError("provider unavailable")
        article, reason, advisory = journal._generate_article_with_repairs(
            packet, generate, [], generation_guard=journal._journal_generation_guard(self.db, 1, packet))
        self.assertEqual(len(calls), 2)
        self.assertIsNone(article)
        self.assertEqual(reason, "privacy_source_ineligible")
        self.assertFalse(advisory)

    def test_real_bot_writer_keeps_legacy_off_path_and_shared_personality_when_on(self):
        import bnl01_bot as bot
        for packet, enabled in (({}, False), (self.packet(), True)):
            with self.subTest(enabled=enabled), mock.patch.object(bot, "check_quota_availability", return_value=True), \
                    mock.patch.object(bot, "_generate_gemini_content_with_fallback", return_value=object()) as provider, \
                    mock.patch.object(bot, "_extract_text_and_tokens", return_value=("{}", 0)):
                self.assertEqual(bot._generate_journal_json_sync(packet, "JOURNAL PAYLOAD"), "{}")
                provider.assert_called_once()
                prompt, route = provider.call_args.args
                self.assertEqual(route, bot.JOURNAL_ROUTE)
                if not enabled:
                    self.assertEqual(prompt, bot.BNL01_SYSTEM_PROMPT + "\n\nJOURNAL PAYLOAD")
                else:
                    self.assertIn(bot.BNL01_PUBLIC_PERSONALITY_PROMPT, prompt)
                    self.assertIn("Your conversational brevity does not limit this reflection", prompt)
                    self.assertNotIn(bot.BNL01_SYSTEM_PROMPT, prompt)
                    self.assertNotIn("only when the user is explicitly asking for recall", prompt)
                    self.assertTrue(prompt.endswith("\n\nJOURNAL PAYLOAD"))

    def test_uncited_impression_is_fenced_before_approval_and_delivery(self):
        packet = self.packet()
        article = journal.parse_generated_json(article_json(packet))
        result = journal.store_validated_draft(self.db, 1, packet, reviewed_article(article, packet))
        self.assertTrue(result.ok, result)
        with sqlite3.connect(self.db) as conn:
            metadata = json.loads(conn.execute("SELECT metadata_json FROM bnl_journal_private_metadata").fetchone()[0])
            self.assertFalse(any(item["sourceKind"] == "moment_impression" for item in metadata["usedSharedSourceProvenance"]))
            self.assertTrue(any(item["sourceKind"] == "moment_impression" for item in metadata["sharedInputSourceProvenance"]))
        with mock.patch.dict(os.environ, {"BNL_IMPRESSIONS_USE_ENABLED": "false"}):
            self.assertEqual(journal.approve_draft(self.db, 1, result.entry_id, result.content_hash).reason,
                             "privacy_source_ineligible")
        self.assertTrue(journal.approve_draft(self.db, 1, result.entry_id, result.content_hash).ok)
        with mock.patch.dict(os.environ, {"BNL_IMPRESSIONS_USE_ENABLED": "false"}):
            opener = mock.Mock(side_effect=AssertionError("No revoked opinion can be published"))
            delivered = journal.deliver_approved(self.db, 1, result.entry_id, "https://site.example", "key", opener=opener)
            self.assertEqual(delivered.reason, "privacy_source_ineligible")
            opener.assert_not_called()


if __name__ == "__main__":
    unittest.main()
