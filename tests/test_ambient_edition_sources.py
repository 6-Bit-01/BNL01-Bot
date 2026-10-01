"""The edition reads existing governed owners without a new archive or model."""
import copy
from datetime import datetime, timezone
import json
import sqlite3
from types import SimpleNamespace
import unittest
from unittest import mock

import bnl_ambient_edition_sources as edition
from bnl_journal import JOURNAL_REFLECTION_SCOPE


NOW = datetime(2026, 9, 30, 3, tzinfo=timezone.utc)
STAMP = "2026-09-29T20:00:00Z"


def activity(ref="fresh:1", subject="discord_user:123", surface="discord", speaker="Test Member", timestamp=STAMP):
    safe = {"refId": ref, "sourceKind": "conversation", "summary": "A strange bass sound sparked a lively discussion.",
            "observedAt": timestamp, "publicSpeakerName": speaker, "conversationSurface": surface,
            "participantAlias": "participant-" + ref, "channelPolicy": "public_home"}
    return safe, {**safe, "subjectRef": subject, "messageId": 7654}


class EditionSourcesTests(unittest.TestCase):
    def setUp(self):
        source, original = activity()
        self.packet = {"safeSources": [source], "privateSources": [original],
                       "privateSharedSourceProvenance": [], "reflectionBasis": [],
                       "sourceArchiveAvailable": True, "coverageComplete": True,
                       "aggregateCounts": {"eligibleConversations": 1}}
        self.reader = mock.patch.object(edition, "build_source_packet_between", side_effect=lambda *a, **k: copy.deepcopy(self.packet)).start()
        self.original_fence = mock.patch.object(edition, "_fence_discord_originals", side_effect=lambda bot, guild, packet, *a: (packet, {"guild_id": guild})).start()
        self.addCleanup(mock.patch.stopall)
        self.bot = SimpleNamespace(DB_FILE="not-a-real-database", _pacific_now=lambda: NOW,
                                   _journal_website_base_url=lambda: "https://site.test",
                                   _build_publication_prompt_source_basis=mock.Mock(return_value=None),
                                   _refresh_publication_prompt_source_basis=mock.Mock(side_effect=lambda basis: (basis, False)),
                                   revalidate_ambient_local_sources=mock.Mock(return_value=True))

    def context(self):
        self.basis = {"guild_id": 7}
        return edition.build_context(self.bot, 7, 9, basis=self.basis, now=NOW)

    def test_exact_window_existing_reader_and_no_art_dependency(self):
        context = self.context()
        self.reader.assert_called_once_with("not-a-real-database", 7, "2026-09-29T03:00:00Z", "2026-09-30T03:00:00Z",
                                            entry_kind="daily", prepare_schema=False)
        self.assertIs(self.basis["edition_context"], context)
        self.assertEqual(context["items"][0]["subject_refs"], ["discord_user:123"])
        self.assertEqual(context["items"][0]["subject_labels"], {"discord_user:123": "Test Member"})
        self.assertEqual(context["items"][0]["guild_id"], 7)
        self.assertEqual(context["items"][0]["evidence_role"], "original_contribution")
        self.assertEqual(context["coverage"]["eligible_conversations"], 1)
        self.assertTrue(edition.revalidate(self.bot, 7, context))

    def test_tiktok_correlated_discord_subject_is_never_tag_authority(self):
        safe, original = activity(surface="tiktok_live_chat")
        self.packet.update(safeSources=[safe], privateSources=[original])
        item = self.context()["items"][0]
        self.assertEqual(item["subject_refs"], [])
        self.assertEqual(item["subject_labels"], {})

    def test_private_names_and_provenance_never_enter_model_items(self):
        self.packet["privateSources"][0]["displayName"] = "PRIVATE_ACCOUNT_LABEL"
        self.packet["privateSources"][0]["rawSummary"] = "PRIVATE_ARCHIVE_TEXT"
        self.packet["privatePublicPeople"] = [{"privateAlias": "PRIVATE_ACCOUNT_LABEL"}]
        context = self.context()
        self.assertNotIn("PRIVATE_", json.dumps(context["items"]))
        self.assertNotIn("PRIVATE_", json.dumps(context["_root_digests"]))

    def test_unnamed_speaker_gets_no_guess_from_subject_or_text(self):
        self.packet["safeSources"][0]["publicSpeakerName"] = ""
        self.packet["safeSources"][0]["summary"] = "Test Member asked someone a question."
        self.assertEqual(self.context()["items"][0]["subject_refs"], [])

    def test_source_claimed_rooms_and_reply_links_are_not_projected(self):
        self.packet["safeSources"][0].update(room_ref="claimed-room", reply_to_ref="fresh:0")
        self.packet["privateSources"][0]["room_ref"] = "another-claim"
        item = self.context()["items"][0]
        self.assertNotIn("room_ref", item)
        self.assertNotIn("reply_to_ref", item)
        self.assertEqual(item["occurred_at"], STAMP)

    def test_window_excludes_future_and_old_activity_but_keeps_start(self):
        values = [activity(ref=str(index), timestamp=timestamp) for index, timestamp in enumerate((
            "2026-09-29T02:59:59Z", "2026-09-29T03:00:00Z", "2026-09-30T03:00:00Z", "invalid"))]
        self.packet.update(safeSources=[v[0] for v in values], privateSources=[v[1] for v in values])
        self.assertEqual([item["ref"] for item in self.context()["items"]], ["1"])

    def test_publication_time_never_becomes_event_time_and_links_are_real_route(self):
        publication = SimpleNamespace(entry_id="journal_daily_2026-09-28_abcd", title="A New Perspective", excerpt="An older discussion revisited.",
                                      sections_json='[{"heading":"Music","body":"Test Member discussed a sound."}]',
                                      published_at=STAMP, created_at=STAMP, source_window_start="2026-09-27T01:00:00Z",
                                      source_window_end="2026-09-28T01:00:00Z", revision=1)
        basis = SimpleNamespace(publications=(publication,))
        self.bot._build_publication_prompt_source_basis.side_effect = lambda **kw: basis if kw["source_kind"] == "journal" else None
        context = self.context()
        item = context["items"][0]
        self.assertEqual(item["kind"], "published_journal")
        self.assertEqual(item["evidence_role"], "bnl_expression")
        self.assertEqual(item["occurred_at"], "")
        self.assertEqual(item["reported_window_end"], "2026-09-28T01:00:00Z")
        self.assertEqual(item["url"], "https://site.test/journal/journal_daily_2026-09-28_abcd")
        self.assertEqual(item["subject_refs"], [])
        self.assertEqual(item["publication_card"], {"title": "A New Perspective",
            "excerpt": "An older discussion revisited.", "section_headings": ["Music"]})
        self.assertTrue(edition.revalidate(self.bot, 7, context))
        self.bot._refresh_publication_prompt_source_basis.return_value = (SimpleNamespace(publications=()), True)
        self.bot._refresh_publication_prompt_source_basis.side_effect = None
        self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_journal_card_has_only_bounded_owner_fields_and_is_tamper_pinned(self):
        publication = SimpleNamespace(entry_id="journal_card_fixture", title="T" * 300,
            excerpt="E" * 1000, sections_json=json.dumps([
                {"heading": "H" * 200, "body": "Narrative body excluded from the card."} for _ in range(12)]),
            private_note="PRIVATE_METADATA", published_at=STAMP, created_at=STAMP,
            source_window_start="2026-09-28T01:00:00Z", source_window_end="2026-09-29T01:00:00Z", revision=1)
        basis = SimpleNamespace(publications=(publication,))
        self.bot._build_publication_prompt_source_basis.side_effect = lambda **kw: basis if kw["source_kind"] == "journal" else None
        context = self.context()
        card = context["items"][0]["publication_card"]
        self.assertEqual(set(card), {"title", "excerpt", "section_headings"})
        self.assertEqual(len(card["title"]), 240)
        self.assertEqual(len(card["excerpt"]), 800)
        self.assertEqual([len(heading) for heading in card["section_headings"]], [140] * 8)
        self.assertNotIn("Narrative body", json.dumps(card))
        self.assertNotIn("PRIVATE_METADATA", json.dumps(card))
        self.assertTrue(edition.revalidate(self.bot, 7, context))
        card["excerpt"] = "A forged publication description."
        self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_journal_card_never_recovers_headings_from_malformed_sections(self):
        for sections in ("invalid", '{"body":"Do not invent a heading"}', None):
            with self.subTest(sections=sections):
                card = edition._journal_publication_card(SimpleNamespace(
                    title="Owner title", excerpt="Owner excerpt", sections_json=sections))
                self.assertEqual(card["section_headings"], [])

    def test_old_publication_is_not_announced_as_a_new_release(self):
        self.bot._build_publication_prompt_source_basis.return_value = SimpleNamespace(publications=(SimpleNamespace(published_at="2026-09-28T01:00:00Z"),))
        self.assertEqual(len(self.context()["items"]), 1)

    def test_relay_has_no_invented_release_card(self):
        publication = SimpleNamespace(relay_id="relay_fixture", public_message="An existing Relay observation.",
                                      published_timestamp=STAMP)
        self.bot._build_publication_prompt_source_basis.side_effect = lambda **kw: (
            SimpleNamespace(publications=(publication,)) if kw["source_kind"] == "relay" else None)
        self.assertNotIn("publication_card", self.context()["items"][0])

    def test_ballad_release_uses_owner_metadata_url_and_historical_label(self):
        self.packet["reflectionBasis"] = [{"refId": "reflection:ballad:1", "basisKind": "published_ballad",
            "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
            "summary": "A creative release about an older show; mentions Test Member.",
            "showLink": "https://site.test/radio/archive?view=shows&show=show-1#broadcast-ballad",
            "sourceObservedAt": "2026-09-27T12:00:00Z", "sourceVersion": "version-1"}]
        context = self.context()
        item = context["items"][-1]
        self.assertEqual(item["scope"], "historical_context")
        self.assertEqual(item["evidence_role"], "bnl_expression")
        self.assertEqual(item["occurred_at"], "")
        self.assertEqual(item["subject_refs"], [])
        self.assertNotIn("publication_card", item)
        self.assertEqual(item["label"], "Broadcast Ballad")
        self.assertTrue(item["url"].endswith("show-1#broadcast-ballad"))
        self.packet["reflectionBasis"][0]["sourceVersion"] = "version-2"
        self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_ballad_card_uses_sanitized_structure_without_parsing_summary(self):
        from bnl_broadcast_ballads import PUBLICATION_CARD_LIMITS
        card = {"title": "Fictional Frequency", "show_date": "2026-09-29", "show_title": "Test Broadcast",
                "style": "Retro funk", "about": "A creative tribute", "mentions": "Test Member",
                "inspired_by": "A public joke", "private_note": "PRIVATE_METADATA"}
        self.packet["reflectionBasis"] = [{"refId": "reflection:ballad:card", "basisKind": "published_ballad",
            "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
            "summary": 'Creative archive text {title: A different title, private_note: Do not parse me}',
            "publication_card": card, "sourceObservedAt": STAMP, "sourceVersion": "v1"}]
        context = self.context()
        item = context["items"][-1]
        self.assertEqual(item["label"], "Fictional Frequency")
        self.assertEqual(set(item["publication_card"]), set(PUBLICATION_CARD_LIMITS))
        self.assertNotIn("PRIVATE_METADATA", json.dumps(item["publication_card"]))
        self.assertNotIn("A different title", json.dumps(item["publication_card"]))
        self.assertIn("Creative archive text", item["text"])
        self.assertTrue(edition.revalidate(self.bot, 7, context))
        card["about"] = "A changed release description."
        self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_ballad_card_bounds_and_invalid_title_do_not_override_label(self):
        from bnl_broadcast_ballads import PUBLICATION_CARD_LIMITS
        card = {key: "x" * (limit + 50) for key, limit in PUBLICATION_CARD_LIMITS.items()}
        self.assertEqual({key: len(value) for key, value in edition._ballad_publication_card(card).items()},
                         PUBLICATION_CARD_LIMITS)
        self.packet["reflectionBasis"] = [{"refId": "reflection:ballad:invalid", "basisKind": "published_ballad",
            "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
            "summary": "A creative release", "publication_card": {"title": {"private": "not text"}, "style": "Funk"},
            "sourceObservedAt": STAMP, "sourceVersion": "v1"}]
        item = self.context()["items"][-1]
        self.assertEqual(item["label"], "Broadcast Ballad")
        self.assertEqual(item["publication_card"], {"style": "Funk"})

    def test_invalid_or_other_site_link_never_becomes_public_link(self):
        self.assertEqual(edition._owner_url(self.bot, "https://site.test.evil.test/steal"), "")
        self.assertEqual(edition._owner_url(self.bot, "https://other.test/radio/show-1"), "")
        self.assertEqual(edition._owner_url(self.bot, "javascript:alert(1)"), "")

    def test_episode_link_requires_current_public_exact_session_and_withdrawal_blocks_send(self):
        self.packet["safeSources"] = [{"refId": "show:1", "sourceKind": "finalized_show",
                                      "summary": "A recorded show ended.", "observedAt": STAMP}]
        self.packet["privateSharedSourceProvenance"] = [{"refId": "show:1", "sourceKind": "finalized_show",
                                                        "sourceId": "session-1", "sourceVersion": "v1"}]
        self.bot.BNL_PRIMARY_GUILD_ID = 7
        self.bot.fetch_bnl_read_model = mock.Mock(return_value={"shows": [
            {"sessionId": "session-1", "showDate": "2026-09-29", "status": "archived"}]})
        self.bot.public_show_evidence_archive = mock.Mock(side_effect=lambda model: model)
        context = self.context()
        self.assertEqual(context["items"][0]["url"], "https://site.test/radio/archive?view=shows&show=session-1")
        self.assertTrue(edition.revalidate(self.bot, 7, context))
        self.bot.public_show_evidence_archive.side_effect = lambda model: {}
        self.assertFalse(edition.revalidate(self.bot, 7, context))
        self.assertEqual(self.context()["items"][0]["url"], "")

    def test_episode_link_never_matches_another_show_on_the_same_date(self):
        self.packet["safeSources"] = [{"refId": "show:1", "sourceKind": "finalized_show",
                                      "summary": "A recorded show ended.", "observedAt": STAMP}]
        self.packet["privateSharedSourceProvenance"] = [{"refId": "show:1", "sourceKind": "finalized_show",
                                                        "sourceId": "session-1", "sourceVersion": "v1"}]
        self.bot.BNL_PRIMARY_GUILD_ID = 7
        self.bot.fetch_bnl_read_model = mock.Mock(return_value={"shows": [
            {"sessionId": "session-2", "showDate": "2026-09-29", "status": "archived"}]})
        self.bot.public_show_evidence_archive = lambda model: model
        self.assertEqual(self.context()["items"][0]["url"], "")

    def test_correction_deletion_original_subject_change_and_forgery_fail_closed(self):
        context = self.context()
        self.packet["privateSources"][0]["subjectRef"] = "discord_user:456"
        self.assertFalse(edition.revalidate(self.bot, 7, context))
        self.packet["privateSources"][0]["subjectRef"] = "discord_user:123"
        self.packet["safeSources"][0]["summary"] = "Corrected contribution."
        self.assertFalse(edition.revalidate(self.bot, 7, context))
        self.packet["safeSources"] = []
        self.assertFalse(edition.revalidate(self.bot, 7, context))
        context["items"][0]["url"] = "https://other.test/fiction"
        self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_invisible_root_version_change_invalidates_same_rendered_summary(self):
        self.packet["privateSharedSourceProvenance"] = [{"refId": "fresh:1", "sourceVersion": "v1"}]
        context = self.context()
        self.packet["privateSharedSourceProvenance"][0]["sourceVersion"] = "v2"
        self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_bounded_selection_leaves_room_for_quiet_people_and_shows(self):
        values = [activity(ref="fresh:" + str(n), subject="discord_user:123") for n in range(100)]
        values.append(activity(ref="quiet", subject="discord_user:456", speaker="Quiet Contributor"))
        self.packet.update(safeSources=[v[0] for v in values], privateSources=[v[1] for v in values])
        self.packet["safeSources"].append({"refId": "show:1", "sourceKind": "finalized_show", "summary": "The recorded show ended after 43 tracks.", "observedAt": STAMP})
        context = self.context()
        self.assertEqual(len(context["items"]), edition.MAX_ACTIVITY_ITEMS)
        self.assertIn("quiet", [s["ref"] for s in context["items"]])
        self.assertIn("show:1", [s["ref"] for s in context["items"]])

    def test_busy_distinct_speakers_do_not_starve_later_show_or_late_contribution(self):
        values = [activity(ref="busy:" + str(n), subject="discord_user:" + str(1000 + n),
                           speaker="Test Member " + str(n), timestamp="2026-09-29T08:00:00Z") for n in range(80)]
        values.append(activity(ref="late-question", subject="discord_user:2000", speaker="Late Contributor",
                               timestamp="2026-09-30T02:40:00Z"))
        self.packet.update(safeSources=[v[0] for v in values], privateSources=[v[1] for v in values])
        self.packet["safeSources"].append({"refId": "completed-show", "sourceKind": "finalized_show",
            "summary": "Recorded show operations include 43 played tracks.", "observedAt": "2026-09-30T02:50:00Z"})
        context = self.context()
        refs = {s["ref"] for s in context["items"]}
        self.assertEqual(len(refs), edition.MAX_ACTIVITY_ITEMS)
        self.assertIn("completed-show", refs)
        self.assertIn("late-question", refs)
        self.assertTrue(edition.revalidate(self.bot, 7, context))

    def test_timeless_approved_canon_is_available_without_inventing_an_event_date(self):
        self.packet["safeSources"] = []
        self.packet["privateSources"] = []
        self.packet["reflectionBasis"] = [{"refId": "reflection:canon:fixture", "basisKind": "approved_canon",
            "sourceType": "approved_canon", "sourceVersion": "canon-v1", "scope": JOURNAL_REFLECTION_SCOPE,
            "publicSafe": True, "reuseEligible": True, "summary": "A fictional station archivist collects obsolete radio dials."}]
        context = self.context()
        self.assertEqual(len(context["items"]), 1)
        item = context["items"][0]
        self.assertEqual(item["evidence_role"], "established_context")
        self.assertEqual(item["scope"], "historical_context")
        self.assertEqual(item["occurred_at"], "")
        self.assertEqual(item["published_at"], "")
        self.assertTrue(edition.revalidate(self.bot, 7, context))

    def test_many_recent_retellings_do_not_hide_governed_moment_or_approved_context(self):
        def reflection(ref, kind, timestamp="2026-09-29T20:00:00Z"):
            return {"refId": "reflection:" + ref, "basisKind": kind, "sourceObservedAt": timestamp,
                    "sourceVersion": "v1", "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True,
                    "reuseEligible": True, "summary": "Fictional community context " + ref}
        self.packet["reflectionBasis"] = [reflection("retelling-" + str(n), "accepted_relay_continuity") for n in range(8)]
        self.packet["reflectionBasis"].extend([
            reflection("moment", "public_moment", "2026-09-28T18:00:00Z"),
            reflection("release", "published_ballad"),
            reflection("canon", "approved_canon", ""),
        ])
        context = self.context()
        refs = {s["ref"] for s in context["items"]}
        self.assertIn("reflection:release", refs)
        self.assertIn("reflection:moment", refs)
        self.assertIn("reflection:canon", refs)
        self.assertEqual(len(refs), 1 + edition.MAX_REFLECTION_ITEMS)
        self.assertTrue(edition.revalidate(self.bot, 7, context))

    def test_busy_mixed_window_retains_fresh_journal_ballads_and_show_candidates(self):
        values = [activity(ref="mixed:" + str(n), subject="discord_user:" + str(3000 + n % 12),
                           speaker="Test Contributor " + str(n % 12)) for n in range(160)]
        self.packet.update(safeSources=[v[0] for v in values], privateSources=[v[1] for v in values])
        self.packet["safeSources"].append({"refId": "show:mixed", "sourceKind": "finalized_show",
            "summary": "Recorded operations establish the show ended after 43 tracks.", "observedAt": STAMP})
        self.packet["reflectionBasis"] = [
            {"refId": "reflection:ballad:" + str(n), "basisKind": "published_ballad",
             "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
             "summary": "A newly released fictional Broadcast Ballad " + str(n),
             "showLink": "https://site.test/radio/archive?view=shows&show=fixture-" + str(n) + "#broadcast-ballad",
             "sourceObservedAt": STAMP, "sourceVersion": "v1"} for n in range(2)]
        publication = SimpleNamespace(entry_id="journal_mixed_fixture", title="Fictional Community Edition",
            excerpt="A reflection on the day's contributions.", sections_json='[{"heading":"Contributions","body":"An eligible public reflection."}]',
            published_at=STAMP, created_at=STAMP, source_window_start="2026-09-28T03:00:00Z",
            source_window_end="2026-09-29T03:00:00Z", revision=1)
        self.bot._build_publication_prompt_source_basis.side_effect = lambda **kw: (
            SimpleNamespace(publications=(publication,)) if kw["source_kind"] == "journal" else None)
        context = self.context()
        by_ref = {item["ref"]: item for item in context["items"]}
        self.assertEqual(len(context["items"]), edition.MAX_ACTIVITY_ITEMS + 3)
        self.assertIn("show:mixed", by_ref)
        self.assertEqual(by_ref["journal:journal_mixed_fixture"]["scope"], "window_publication")
        for n in range(2):
            self.assertEqual(by_ref["reflection:ballad:" + str(n)]["scope"], "window_publication")
        self.assertTrue(edition.revalidate(self.bot, 7, context))

    def test_single_kind_uses_available_slots_and_empty_window_adds_no_forced_categories(self):
        values = [activity(ref="single:" + str(n)) for n in range(100)]
        self.packet.update(safeSources=[v[0] for v in values], privateSources=[v[1] for v in values])
        context = self.context()
        self.assertEqual(len(context["items"]), edition.MAX_ACTIVITY_ITEMS)
        self.assertEqual({item["kind"] for item in context["items"]}, {"conversation"})
        self.packet.update(safeSources=[], privateSources=[], reflectionBasis=[])
        self.assertEqual(self.context()["items"], [])

    def test_wrong_guild_cannot_read_or_revalidate(self):
        with self.assertRaises(ValueError):
            edition.build_context(self.bot, 7, 9, basis={"guild_id": 8}, now=NOW)
        self.reader.assert_not_called()
        self.assertFalse(edition.revalidate(self.bot, 8, self.context()))

    def test_original_ambient_governance_failure_blocks_archive_reprojection(self):
        context = self.context()
        self.bot.revalidate_ambient_local_sources.return_value = False
        self.assertFalse(edition.revalidate(self.bot, 7, context))
        self.bot.revalidate_ambient_local_sources.assert_called_once_with(7, {"guild_id": 7})

    def test_activity_projection_separates_member_speech_show_operations_and_relay(self):
        self.packet["safeSources"].extend([
            {"refId": "show:1", "sourceKind": "finalized_show", "observedAt": STAMP,
             "summary": "The recorded show included 43 tracks."},
            {"refId": "relay:1", "sourceKind": "relay", "observedAt": STAMP,
             "summary": "BNL's published interpretation of the room."},
        ])
        context = self.context()
        by_ref = {item["ref"]: item for item in context["items"]}
        self.assertEqual(by_ref["fresh:1"]["evidence_role"], "original_contribution")
        self.assertEqual(by_ref["show:1"]["evidence_role"], "recorded_event")
        # A current publication timestamp does not grant independent authority.
        self.assertEqual(by_ref["relay:1"]["scope"], "window_activity")
        self.assertEqual(by_ref["relay:1"]["evidence_role"], "bnl_expression")
        self.assertEqual(by_ref["relay:1"]["text"], "BNL's published interpretation of the room.")
        self.assertTrue(edition.revalidate(self.bot, 7, context))

    def test_reflection_projection_preserves_original_history_and_context_roles(self):
        definitions = (
            ("public_source_history", "discord_message", "original_contribution"),
            ("public_source_history", "tiktok_live_chat", "original_contribution"),
            ("public_source_history", "", "governed_interpretation"),
            ("public_moment", "", "governed_interpretation"),
            ("approved_canon", "approved_canon", "established_context"),
            ("established_broadcast_memory", "broadcast_memory", "established_context"),
            ("accepted_relay_continuity", "website_relay", "bnl_expression"),
        )
        self.packet["reflectionBasis"] = [
            {"refId": "reflection:" + str(index), "basisKind": kind, "sourceType": source_type,
             "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
             "summary": "Existing source text " + str(index), "sourceObservedAt": "2026-09-27T20:00:00Z",
             "sourceVersion": "v1", "contributions": [{"publicSpeakerName": "Test Member", "summary": "An attributed contribution."}]}
            for index, (kind, source_type, _role) in enumerate(definitions)
        ]
        context = self.context()
        by_ref = {item["ref"]: item for item in context["items"]}
        for index, (_kind, source_type, role) in enumerate(definitions):
            item = by_ref["reflection:" + str(index)]
            self.assertEqual(item["evidence_role"], role)
            self.assertEqual(item["source_type"], source_type)
            self.assertEqual(item["text"], "Existing source text " + str(index))
            self.assertEqual(item["scope"], "historical_context")
        self.assertEqual(by_ref["reflection:3"]["contributions"], [
            {"speaker": "Test Member", "summary": "An attributed contribution."}])
        self.assertTrue(edition.revalidate(self.bot, 7, context))
        self.packet["reflectionBasis"][0]["sourceVersion"] = "withdrawn-root-version"
        self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_role_metadata_cannot_upgrade_bnl_output_or_unknown_context(self):
        for kind in ("journal", "published_journal", "relay", "published_relay", "website_relay",
                     "published_ballad", "accepted_relay_continuity"):
            with self.subTest(kind=kind):
                self.assertEqual(edition.evidence_role({"kind": kind, "scope": "window_activity",
                    "source_type": "discord_message", "evidence_role": "original_contribution"}), "bnl_expression")
        for item in ({"kind": "unknown"}, {"kind": "public_source_history"},
                     {"kind": "public_source_history", "source_type": "website_relay"}):
            self.assertEqual(edition.evidence_role({**item, "evidence_role": "recorded_event"}), "governed_interpretation")

    def test_projected_role_is_pinned_in_the_same_item_hash(self):
        context = self.context()
        self.assertTrue(edition.revalidate(self.bot, 7, context))
        context["items"][0]["evidence_role"] = "recorded_event"
        self.assertFalse(edition.revalidate(self.bot, 7, context))


class OriginalDiscordFenceTests(unittest.TestCase):
    def setUp(self):
        safe, original = activity()
        self.packet = {"safeSources": [safe], "privateSources": [original],
                       "sourceArchiveAvailable": True, "aggregateCounts": {"eligibleConversations": 1}}
        self.row = {"id": 42, "user_id": 123, "content": "Original content", "channel_policy": "public_home", "channel_id": 901}
        self.event = {"event_seq": 1, "source_kind": "discord_message", "source_key": "7654",
                      "raw_text": "Original content", "metadata": {"conversationRowId": 42}}
        self.events = [self.event]
        self.bot = SimpleNamespace(DB_FILE="memory-fixture", _ambient_source_rows=mock.Mock(return_value=[self.row]),
                                   _journal_website_base_url=lambda: "https://site.test",
                                   _remember_ambient_sources=lambda basis, table, rows: basis.setdefault("rows", {}).update(
                                       {table: {r["id"]: edition._digest(r) for r in rows}}))

    def fence(self):
        conn = sqlite3.connect(":memory:")
        with mock.patch.object(edition.sqlite3, "connect", return_value=conn), mock.patch.object(
                edition, "query_source_events", return_value=SimpleNamespace(events=tuple(self.events))):
            return edition._fence_discord_originals(self.bot, 7, self.packet, "2026-09-29T03:00:00Z", "2026-09-30T03:00:00Z")

    def test_original_owner_receives_exact_row_and_saved_hash(self):
        packet, basis = self.fence()
        self.assertEqual(packet["safeSources"], self.packet["safeSources"])
        self.assertEqual(basis["rows"]["conversations"], {42: edition._digest(self.row)})
        self.assertEqual(self.bot._ambient_source_rows.call_args.kwargs, {"row_ids": {42}})

    def test_room_comes_only_from_current_original_and_exposes_no_channel_id(self):
        self.packet["safeSources"][0]["room_ref"] = "claimed-room"
        self.packet["_ambient_original_context"] = {"fresh:1": {"room_ref": "forged-private-room"}}
        self.event["channel_id"] = 902
        packet, _ = self.fence()
        item = edition._packet_items(self.bot, packet, NOW.replace(day=29), NOW, 7)[0]
        self.assertEqual(item["room_ref"], "discord-room:" + edition._digest([7, 901])[:24])
        self.assertEqual(item["occurred_at"], STAMP)
        self.assertNotIn("channel_id", item)
        self.assertNotIn("reply_to_ref", item)
        self.assertNotIn("forged-private-room", json.dumps(item))
        self.assertEqual(self.packet["_ambient_original_context"]["fresh:1"]["room_ref"], "forged-private-room")

    def test_separate_original_rooms_or_threads_keep_distinct_opaque_refs(self):
        safe, original = activity(ref="fresh:2", timestamp="2026-09-29T21:00:00Z")
        self.packet["safeSources"].append(safe)
        self.packet["privateSources"].append(original)
        self.events.append({**self.event, "event_seq": 2, "metadata": {"conversationRowId": 43}})
        other_row = {**self.row, "id": 43, "channel_id": 902}
        self.bot._ambient_source_rows.return_value.append(other_row)
        packet, _ = self.fence()
        items = edition._packet_items(self.bot, packet, NOW.replace(day=29), NOW, 7)
        self.assertNotEqual(items[0]["room_ref"], items[1]["room_ref"])
        self.assertEqual([item["occurred_at"] for item in items], [STAMP, "2026-09-29T21:00:00Z"])
        other_row["channel_id"] = 901
        packet, _ = self.fence()
        items = edition._packet_items(self.bot, packet, NOW.replace(day=29), NOW, 7)
        self.assertEqual(items[0]["room_ref"], items[1]["room_ref"])

    def test_unknown_or_withdrawn_room_never_inherits_claimed_metadata(self):
        self.row["channel_id"] = 0
        self.packet["safeSources"][0]["room_ref"] = "claimed-room"
        packet, _ = self.fence()
        self.assertEqual(packet["_ambient_original_context"], {})
        self.assertNotIn("room_ref", edition._packet_items(self.bot, packet, NOW.replace(day=29), NOW, 7)[0])
        self.row["channel_id"] = 901
        self.bot._ambient_source_rows.return_value = []
        packet, _ = self.fence()
        self.assertEqual(packet["_ambient_original_context"], {})
        self.assertEqual(packet["safeSources"], [])

    def test_changed_original_room_invalidates_reprojected_context(self):
        self.bot._pacific_now = lambda: NOW
        self.bot._build_publication_prompt_source_basis = mock.Mock(return_value=None)
        self.bot._merge_ambient_source_hashes = lambda basis, table, rows: basis.setdefault("rows", {}).update({table: rows})
        self.bot.revalidate_ambient_local_sources = mock.Mock(return_value=True)
        real_connect = sqlite3.connect
        with mock.patch.object(edition.sqlite3, "connect", side_effect=lambda *a, **kw: real_connect(":memory:")), mock.patch.object(
                edition, "query_source_events", return_value=SimpleNamespace(events=tuple(self.events))), mock.patch.object(
                edition, "build_source_packet_between", side_effect=lambda *a, **kw: copy.deepcopy(self.packet)):
            context = edition.build_context(self.bot, 7, 9, basis={"guild_id": 7}, now=NOW)
            self.assertTrue(edition.revalidate(self.bot, 7, context))
            original_digest = context["_discord_basis"]["rows"]["conversations"][42]
            self.row["channel_id"] = 902
            self.assertNotEqual(original_digest, edition._digest(self.row))
            self.assertFalse(edition.revalidate(self.bot, 7, context))
            self.row["channel_id"] = 901
            context["items"][0]["room_ref"] = "forged-room"
            self.assertFalse(edition.revalidate(self.bot, 7, context))

    def test_live_privacy_forget_or_deletion_rejects_still_public_archive_copy(self):
        self.bot._ambient_source_rows.return_value = []
        packet, basis = self.fence()
        self.assertEqual(packet["safeSources"], [])
        self.assertEqual(packet["privateSources"], [])
        self.assertEqual(basis["rows"]["conversations"], {})
        self.assertEqual(packet["aggregateCounts"]["eligibleConversations"], 0)
        self.assertEqual(packet["_ambient_original_context"], {})

    def test_withdrawn_original_is_absent_from_edition_and_optional_art_input(self):
        from bnl_own_art import art_source_records
        self.bot._ambient_source_rows.return_value = []
        self.bot._pacific_now = lambda: NOW
        self.bot._journal_website_base_url = lambda: "https://site.test"
        self.bot._build_publication_prompt_source_basis = mock.Mock(return_value=None)
        self.bot._merge_ambient_source_hashes = lambda basis, table, rows: basis.setdefault("rows", {}).update({table: rows})
        conn = sqlite3.connect(":memory:")
        with mock.patch.object(edition.sqlite3, "connect", return_value=conn), mock.patch.object(
                edition, "query_source_events", return_value=SimpleNamespace(events=(self.event,))), mock.patch.object(
                edition, "build_source_packet_between", return_value=self.packet):
            context = edition.build_context(self.bot, 7, 9, basis={"guild_id": 7}, now=NOW)
        self.assertEqual(context["items"], [])
        self.assertEqual(art_source_records(context["_packet"]), [])

    def test_corrected_original_does_not_authorize_old_archived_text(self):
        self.row["content"] = "Corrected original content"
        self.assertEqual(self.fence()[0]["safeSources"], [])

    def test_historical_archive_reflection_uses_same_original_fence_and_art_filter(self):
        from bnl_own_art import art_source_records
        self.packet["safeSources"] = []
        self.packet["privateSources"] = []
        self.packet["reflectionBasis"] = [{"refId": "reflection:event:1", "basisKind": "public_source_history",
            "sourceType": "discord_message", "scope": JOURNAL_REFLECTION_SCOPE,
            "publicSafe": True, "reuseEligible": True, "summary": "A historical public observation.",
            "sourceVersion": "archive-v1", "sourceObservedAt": "2026-09-27T20:00:00Z"}]
        self.packet["privateReflectionBasisProvenance"] = {"historicalSourceEvents": [
            {"refId": "reflection:event:1", "sourceKind": "discord_message", "eventSeq": 1,
             "subjectRef": "discord_user:123", "occurredAtMs": 1790539200000}]}
        packet, basis = self.fence()
        self.assertEqual(len(packet["reflectionBasis"]), 1)
        self.assertEqual(basis["rows"]["conversations"], {42: edition._digest(self.row)})
        item = edition._packet_items(self.bot, packet, NOW.replace(day=29), NOW, 7)[0]
        self.assertEqual(item["room_ref"], "discord-room:" + edition._digest([7, 901])[:24])
        self.assertEqual(item["occurred_at"], "2026-09-27T20:00:00Z")
        self.bot._ambient_source_rows.return_value = []
        packet, basis = self.fence()
        self.assertEqual(packet["reflectionBasis"], [])
        self.assertEqual(art_source_records(packet), [])
        self.assertEqual(packet["privateReflectionBasisProvenance"]["historicalSourceEvents"], [])
        self.assertEqual(basis["rows"]["conversations"], {})

    def test_historical_reflection_without_original_provenance_is_not_reused(self):
        self.packet["safeSources"] = []
        self.packet["privateSources"] = []
        self.packet["reflectionBasis"] = [{"refId": "reflection:event:1", "basisKind": "public_source_history",
            "sourceType": "discord_message", "scope": JOURNAL_REFLECTION_SCOPE,
            "publicSafe": True, "reuseEligible": True, "summary": "Missing its original proof.",
            "sourceVersion": "archive-v1", "sourceObservedAt": "2026-09-27T20:00:00Z"}]
        self.assertEqual(self.fence()[0]["reflectionBasis"], [])

    def test_missing_original_binding_is_not_guessed_from_message_id(self):
        self.event["metadata"] = {}
        self.assertEqual(self.fence()[0]["safeSources"], [])

    def test_wrong_subject_cannot_borrow_a_public_original(self):
        self.row["user_id"] = 999
        self.assertEqual(self.fence()[0]["safeSources"], [])


if __name__ == "__main__":
    unittest.main()
