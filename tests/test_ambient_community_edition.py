"""Community editions use the real Ambient owner with isolated source data."""
import asyncio
import copy
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
import json
import os
import sqlite3
from types import SimpleNamespace
import unittest
from unittest import mock

import test_ambient_show_context as fixtures
import bnl_ambient_art as art
import bnl_ambient_edition as edition
import bnl_ambient_edition_sources as sources
from bnl_journal_source_store import backfill_legacy_sources
from bnl_journal import JOURNAL_REFLECTION_SCOPE


bot = fixtures.bot
REAL_GET = bot.get_gemini_response
LEGACY_TEXT = "The room carries an unexpected rhythm through the evening."


class CommunityEditionIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = fixtures.AmbientShowContextTests()
        self.fixture.setUp()
        self.stack = self.fixture.stack
        self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.dict(os.environ, {
            "BNL_AMBIENT_COMMUNITY_EDITION_ENABLED": "true",
            "BNL_OWN_ART_ENABLED": "false",
            "BNL_GEMINI_BACKGROUND_MAX_OUTPUT_TOKENS": "",
        }))
        self.stack.enter_context(mock.patch.object(bot, "_ambient_post_locks", {}, create=True))
        self.stack.enter_context(mock.patch.object(bot, "_journal_website_base_url", return_value="https://site.test"))
        self.publications = self.stack.enter_context(mock.patch.object(
            bot, "_build_publication_prompt_source_basis", return_value=None))
        self.stack.enter_context(mock.patch.object(
            bot, "_refresh_publication_prompt_source_basis", side_effect=lambda basis: (basis, False)))
        self.execute("UPDATE conversations SET timestamp=?", (self.stamp(minutes=5),))
        self.archive()
        self.provider = self.fixture.provider
        self.provider.side_effect = self.response

    def execute(self, sql, args=()):
        with sqlite3.connect(bot.DB_FILE) as conn:
            return conn.execute(sql, args).fetchall()

    def stamp(self, **delta):
        return (self.fixture.now - timedelta(**delta)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def archive(self):
        backfill_legacy_sources(bot.DB_FILE, 42)

    def raw_material(self, prompt):
        encoded = prompt.split("Eligible material:\n", 1)[1].split("\nRecent Ambient editions", 1)[0]
        return json.loads(encoded)

    def material(self, prompt):
        # Fixture writers select evidence by role; normalize the room-separated
        # provider view without changing the actual production prompt or refs.
        raw = self.raw_material(prompt)
        originals = [item for room in raw["room_excerpts"] for item in room["remarks"]]
        originals.extend(raw["unassociated_originals"])
        return {"original_contributions": originals,
                **{key: value for key, value in raw.items()
                   if key not in {"room_excerpts", "unassociated_originals"}}}

    def assert_edition_voice(self, contents):
        self.assertTrue(contents.startswith(bot.BNL01_AMBIENT_EDITION_SYSTEM_PROMPT))
        self.assertEqual(contents.count(bot.BNL01_PUBLIC_PERSONALITY_PROMPT), 1)
        for trait in bot._BNL01_PUBLIC_PERSONALITY_LINES:
            self.assertIn(trait, bot.BNL01_SYSTEM_PROMPT)
            self.assertEqual(contents.count(trait), 1)
        self.assertNotIn(bot.BNL01_PACKET_OWNED_SYSTEM_PROMPT, contents)
        self.assertNotIn("## RESTRICTED TOPICS", contents)
        self.assertNotIn("- Nickname Policy:", contents)

    def response(self, prompt, *args, **kwargs):
        items = [item for group in self.material(prompt).values() for item in group]
        selected = next((item for item in items if item["kind"] == "conversation"), None)
        if selected is None:
            return '{"action":"skip"}'
        subject = next(iter(selected.get("subject_refs", ())), None)
        person = "[[person:" + subject + "]]" if subject else "A community member"
        paragraph = (person + " noticed an unexpected rhythm taking shape. That is worth listening to: "
                 "the conversation makes room for an unusual sound without demanding a polished release. "
                 "I like that kind of opening. A small observation can give the next conversation "
                 "somewhere interesting to go, and this one deserves more than a passing glance.")
        paragraphs = [{"text": paragraph, "sourceRefs": [selected["ref"]],
                    "publicationRefs": [], "contextRefs": [],
                    "subjectRefs": [subject] if subject else []}]
        journal = next((item for item in items if item["kind"] == "published_journal"), None)
        if journal:
            paragraphs.append({"text": "A newly published Journal revisits an earlier discussion about ceramic receivers.",
                               "sourceRefs": [], "publicationRefs": [journal["ref"]],
                               "contextRefs": [], "subjectRefs": []})
        return json.dumps({"action": "post", "headline": "A rhythm worth following",
                           "paragraphs": paragraphs, "art": None})

    def add_message(self, text, when, *, policy="public_home", user_id=8, label="Another Member"):
        self.execute("INSERT INTO conversations(user_id,user_name,guild_id,channel_id,channel_name,channel_policy,role,content,timestamp) "
                     "VALUES(?,?,42,100,'barcode-bot',?,'user',?,?)",
                     (user_id, label, policy, text, when))

    def representative_owner_inputs(self, *, show=False, ballad=False, moment=False):
        """Replace published-owner data, retaining real Discord/source assembly.

        These fixtures exercise evidence routing and delivery. Mocked writing
        cannot establish the quality of an actual generated community edition.
        """
        real_packet = sources.build_source_packet_between
        extras = {"safeSources": [], "privateSharedSourceProvenance": [], "reflectionBasis": []}
        if show:
            extras["safeSources"].append({
                "refId": "show:fixture-1", "sourceKind": "finalized_show",
                "observedAt": self.stamp(hours=2),
                "summary": "The recorded show included 43 tracks and 180500 taps.",
            })
            extras["privateSharedSourceProvenance"].append({
                "refId": "show:fixture-1", "sourceKind": "finalized_show",
                "sourceId": "fixture-show-1", "sourceVersion": "show-v1",
            })
            self.stack.enter_context(mock.patch.object(bot, "public_show_evidence_archive", return_value={
                "shows": [{"sessionId": "fixture-show-1", "showDate": "2026-09-11", "status": "archived"}],
            }))
        if ballad:
            extras["reflectionBasis"].append({
                "refId": "reflection:ballad:fixture-1", "basisKind": "published_ballad",
                "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
                "summary": "An invented choir of 999 moons sings about the September 9 show and Test Listener.",
                "publication_card": {"title": "The Moon Choir", "show_date": "2026-09-09",
                                     "about": "An imagined choir inspired by an earlier show."},
                "showLink": "https://site.test/radio/archive?view=shows&show=fixture-older-show#broadcast-ballad",
                "sourceObservedAt": self.stamp(minutes=15), "sourceVersion": "ballad-v1",
            })
        if moment:
            extras["reflectionBasis"].append({
                "refId": "reflection:moment:fixture-1", "basisKind": "public_moment",
                "scope": JOURNAL_REFLECTION_SCOPE, "publicSafe": True, "reuseEligible": True,
                "summary": "An earlier exchange explored leaving space in a rhythm.",
                "sourceObservedAt": self.stamp(days=2), "sourceVersion": "moment-v1",
                "contributions": [{"publicSpeakerName": "Earlier Member", "summary": "Proposed leaving space."}],
            })
        def packet(*args, **kwargs):
            result = real_packet(*args, **kwargs)
            for key, values in extras.items():
                result[key] = list(result.get(key, ())) + copy.deepcopy(values)
            return result
        self.stack.enter_context(mock.patch.object(sources, "build_source_packet_between", side_effect=packet))
        return extras

    async def deliver_representative(self, response):
        captured = []
        async def boundary(contents, route, **kwargs):
            material = self.material(contents)
            self.assertEqual(bot._generation_config_for_model(bot.GEMINI_MODEL, route).max_output_tokens, 8192)
            captured.append((contents, route, material))
            value = response(material)
            return SimpleNamespace(success=True, text=json.dumps(value))
        channel, guild, _ = self.scheduler(fetch_effect=lambda user_id: SimpleNamespace(
            id=user_id, bot=False, guild=SimpleNamespace(id=42)))
        with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
            await bot.ambient_message_task.coro()
        rewrite.assert_not_awaited()
        return channel, guild, provider, captured

    def add_publication(self, *, body="Ceramic receivers caught a strange signal."):
        publication = SimpleNamespace(
            entry_id="journal_daily_2026-09-10_fixture", revision=1,
            title="Ceramic Receivers", excerpt="An earlier discussion revisited.",
            sections_json=json.dumps([{"heading": "Music", "body": body}]),
            published_at=self.stamp(minutes=10), created_at=self.stamp(minutes=20),
            source_window_start=self.stamp(days=2), source_window_end=self.stamp(days=1))
        basis = SimpleNamespace(publications=(publication,))
        self.publications.side_effect = lambda **kwargs: basis if kwargs["source_kind"] == "journal" else None
        return basis

    async def test_default_off_uses_the_legacy_owner_without_edition_source_reads(self):
        with mock.patch.dict(os.environ, {"BNL_AMBIENT_COMMUNITY_EDITION_ENABLED": ""}), \
                mock.patch.object(sources, "build_context") as reader:
            self.provider.side_effect = None
            self.provider.return_value = LEGACY_TEXT
            self.assertEqual(await bot.generate_dynamic_ambient(42, 100), LEGACY_TEXT)
        reader.assert_not_called()
        self.assertIn("at most 280 characters", self.provider.call_args.args[0])

    async def test_other_guild_cannot_enable_the_primary_edition(self):
        with mock.patch.object(edition, "generate", new=mock.AsyncMock()) as generate:
            self.provider.side_effect = None
            self.provider.return_value = LEGACY_TEXT
            self.assertEqual(await bot.generate_dynamic_ambient(43, 100), LEGACY_TEXT)
        generate.assert_not_awaited()

    async def test_preparation_freezes_source_window_before_thread_and_art_expansion(self):
        self.fixture.now = self.fixture.now.replace(hour=19, minute=29)
        cutoff = self.fixture.now
        actual_to_thread = asyncio.to_thread
        async def delayed_read(function, *args, **kwargs):
            if function.__name__ == "read":
                self.fixture.now = self.fixture.now.replace(hour=19, minute=45)
            return await actual_to_thread(function, *args, **kwargs)
        art_context = {"sources": [], "sourceBases": [], "continuity": []}
        self.provider.side_effect = None
        self.provider.return_value = '{"action":"skip"}'
        with mock.patch.object(bot, "ambient_source_window_end", side_effect=lambda: self.fixture.now, create=True) as end, \
                mock.patch.object(edition.asyncio, "to_thread", side_effect=delayed_read), \
                mock.patch.object(sources, "build_context", wraps=sources.build_context) as reader, \
                mock.patch.object(art, "available", return_value=True), \
                mock.patch.object(art, "journal_context", return_value=None), \
                mock.patch.object(art, "build_art_context", return_value=art_context), \
                mock.patch.object(bot, "revalidate_ambient_sources", new=mock.AsyncMock(return_value=True)):
            await bot.generate_dynamic_ambient(42, 100)
        end.assert_called_once()
        self.assertEqual(reader.call_args.kwargs["now"], cutoff)
        self.assertEqual(datetime.fromisoformat(art_context["ambient_source_window_end"].replace("Z", "+00:00")), cutoff)

    async def test_real_provider_wrapper_preserves_rich_envelope_without_voice_rewrite(self):
        self.add_message("WITHHELD_EDITION_MARKER", self.stamp(minutes=4), policy="sealed_test")
        publication = self.add_publication(body="UNSUPPORTED_JOURNAL_NARRATIVE: Everyone heard an imaginary instrument.")
        publication.rendered_context = "UNSUPPORTED_JOURNAL_NARRATIVE: Everyone heard an imaginary instrument."
        publication.journal_control_snapshot = None
        self.archive()
        async def boundary(contents, route, **kwargs):
            return SimpleNamespace(success=True, text=self.response(contents))
        for art_available in (False, True):
            with self.subTest(art_available=art_available), \
                    mock.patch.object(art, "available", return_value=art_available), \
                    mock.patch("bnl_own_art.journal_art_basis", return_value={"ambient": {"guild_id": 42, "rows": {}}}), \
                    mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                    mock.patch.object(bot, "check_quota_availability", return_value=True), \
                    mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                    mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
                basis = {}
                result = await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis)
                self.assertGreater(len(result), 280)
                self.assertEqual(basis["edition"]["headline"], "A rhythm worth following")
                self.assertEqual(provider.await_count, 2)
                contents, route = provider.call_args.args
                self.assertEqual([call.args[1] for call in provider.await_args_list], [
                    "ambient_generation.community_edition", "ambient_generation.community_edition_repair"])
                self.assertEqual(route, "ambient_generation.community_edition_repair")
                self.assertEqual(self.raw_material(provider.await_args_list[0].args[0]),
                                 self.raw_material(contents))
                self.assertEqual(bot._generation_config_for_model(bot.GEMINI_MODEL, route).max_output_tokens, 8192)
                self.assert_edition_voice(contents)
                raw = self.raw_material(contents)
                self.assertNotIn("original_contributions", raw)
                room = next(room for room in raw["room_excerpts"]
                            if any(item["text"] == "A new rhythm is forming in the room."
                                   for item in room["remarks"]))
                self.assertTrue(room["room_ref"].startswith("discord-room:"))
                self.assertEqual(room["conversation_surface"], "discord")
                self.assertTrue(all(item["evidence_role"] == "original_contribution"
                                    for item in room["remarks"]))
                article_context = contents.split("Eligible material:\n", 1)[0]
                self.assertIn(edition.render_prompt_canon_block(), article_context)
                self.assertIn(edition.render_ecosystem_lore_block(include_restricted=False), article_context)
                self.assertEqual(contents.count(edition.render_prompt_canon_block()), 1)
                self.assertEqual(contents.count(edition.render_ecosystem_lore_block(include_restricted=False)), 1)
                self.assertNotIn("UNSUPPORTED_JOURNAL_NARRATIVE", contents)
                self.assertIn("An earlier discussion revisited.", contents)
                self.assertNotIn("9 Bit", article_context)
                self.assertIn("A new rhythm is forming in the room.", contents)
                for private in ("WITHHELD_EDITION_MARKER", "PRIVATE PAYMENT VALUE", "PRIVATE OPERATOR VALUE",
                                "privateSharedSourceProvenance", "_discord_basis"):
                    self.assertNotIn(private, contents)
                self.assertEqual("art_context" in basis, art_available)
                if art_available:
                    self.assertTrue(any("UNSUPPORTED_JOURNAL_NARRATIVE" in source.get("summary", "")
                                        for source in basis["art_context"]["sources"]))
                    self.assertEqual(basis["art_context"]["basis"]["entryKind"], "daily")
                    self.assertTrue(any(ref.startswith("reflection:canon:")
                                        for ref in basis["art_context"]["basis"]["sources"]))
                rewrite.assert_not_awaited()

    async def test_public_personality_is_scoped_to_edition_calls_without_changing_other_packet_routes(self):
        exact_prompt = 'AUTHORIZED_SOURCE_PAYLOAD: {"speaker":"Test Member","text":"An unfinished rhythm."}'
        raw_response = ' {"action":"skip"} \n'
        edition_routes = {"ambient_generation.community_edition", "ambient_generation.community_edition_repair"}
        routes = [bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE, "ambient_generation",
                  "ambient_generation.conversation_grounding_regeneration", *sorted(edition_routes)]
        with mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(
                    return_value=SimpleNamespace(success=True, text=raw_response))) as provider, \
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
            for route in routes:
                with self.subTest(route=route):
                    before = provider.await_count
                    result = await REAL_GET(exact_prompt, 0, 42, route=route,
                                            source_context_available=True,
                                            ambient_envelope=route != bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE)
                    self.assertEqual(result, raw_response)
                    self.assertEqual(provider.await_count, before + 1)
                    contents, actual_route = provider.call_args.args
                    self.assertEqual(actual_route, route)
                    self.assertEqual(contents.count(exact_prompt), 1)
                    if route in edition_routes:
                        self.assert_edition_voice(contents)
                    else:
                        self.assertTrue(contents.startswith(bot.BNL01_PACKET_OWNED_SYSTEM_PROMPT))
                        self.assertEqual(contents.count(bot.BNL01_PACKET_OWNED_SYSTEM_PROMPT), 1)
                        self.assertNotIn(bot.BNL01_PUBLIC_PERSONALITY_PROMPT, contents)
                        self.assertNotIn(bot.BNL01_AMBIENT_EDITION_SYSTEM_PROMPT, contents)
            rewrite.assert_not_awaited()

    async def test_real_provider_failure_never_becomes_an_edition_or_voice_retry(self):
        for art_available in (False, True):
            with self.subTest(art_available=art_available), \
                    mock.patch.object(art, "available", return_value=art_available), \
                    mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                    mock.patch.object(bot, "check_quota_availability", return_value=True), \
                    mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(
                        return_value=SimpleNamespace(success=False, error_category="fixture_unavailable"))) as provider, \
                    mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
                basis = {}
                self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), "")
                self.assertEqual(basis, {})
                provider.assert_awaited_once()
                rewrite.assert_not_awaited()

    async def test_real_day_source_adapter_excludes_old_future_and_private_independent_of_art(self):
        self.add_message("OLD_DAY_MARKER", self.stamp(days=2))
        self.add_message("FUTURE_MARKER", self.stamp(minutes=-5))
        self.add_message("SEALED_MARKER", self.stamp(minutes=4), policy="sealed_test")
        self.add_message("ELIGIBLE_DAY_MARKER", self.stamp(hours=20))
        self.archive()
        basis = {}
        result = await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis)
        self.assertTrue(result)
        prompt = self.provider.call_args.args[0]
        self.assertIn("ELIGIBLE_DAY_MARKER", prompt)
        for excluded in ("FUTURE_MARKER", "SEALED_MARKER"):
            self.assertNotIn(excluded, prompt)
        for item in basis["edition_context"]["items"]:
            if "OLD_DAY_MARKER" in item["text"]:
                self.assertEqual(item["scope"], "historical_context")
        self.assertFalse(any("OLD_DAY_MARKER" in item["text"]
                             for item in basis["edition_context"]["items"]
                             if item["scope"] == "window_activity"))
        self.assertIn("edition_context", basis)
        self.assertNotIn("art_context", basis)

    async def test_publication_keeps_actual_link_and_separate_reported_window(self):
        publication_basis = self.add_publication()
        basis = {}
        result = await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis)
        self.assertIn("https://site.test/journal/journal_daily_2026-09-10_fixture", result)
        item = next(item for item in basis["edition_context"]["items"] if item["kind"] == "published_journal")
        self.assertEqual(item["occurred_at"], "")
        self.assertEqual(item["reported_window_end"], self.stamp(days=1))
        self.assertIn(publication_basis, basis["edition_context"]["_publication_bases"])
        with mock.patch.object(bot, "_refresh_publication_prompt_source_basis", return_value=(SimpleNamespace(publications=()), True)):
            self.assertFalse(await bot.revalidate_ambient_sources(42, basis, stage="before_send"))

    async def test_real_wrapper_keeps_journal_expression_separate_and_repairs_wrong_role_once(self):
        original_text = ("My new project is an electronic collaboration about an optimistic future. "
                         "I have not shared any audio here.")
        journal_text = "The member's booming acoustic rhythm filled the room while everyone cheered."
        self.add_message(original_text, self.stamp(minutes=3))
        self.archive()
        self.add_publication(body=journal_text)
        captured = []
        accepted = {}

        async def boundary(contents, route, **kwargs):
            self.assert_edition_voice(contents)
            material = self.material(contents)
            original = next(item for item in material["original_contributions"] if item.get("text") == original_text)
            journal = next(item for item in (material["new_publications"] + material["earlier_publications"]) if item["kind"] == "published_journal")
            captured.append((material, route))
            if len(captured) == 1:
                # A publication ref in the original-fact lane must be rejected
                # before any transport, regardless of its plausible prose.
                value = {"action": "post", "paragraphs": [{
                    "text": journal_text, "sourceRefs": [journal["ref"]],
                    "publicationRefs": [], "contextRefs": [], "subjectRefs": [],
                }], "art": None}
            else:
                subject = original["subject_refs"][0]
                marker = "[[person:" + subject + "]]"
                text = ("That is an optimistic future I can get behind. " + marker +
                        " described an electronic collaboration; the audio itself has not been shared here. "
                        "My Journal took a different imaginative angle, and it is linked below.")
                accepted["text"] = text.replace(marker, original["subject_labels"][subject])
                value = {"action": "post", "paragraphs": [{
                    "text": text, "sourceRefs": [original["ref"]],
                    "publicationRefs": [journal["ref"]], "contextRefs": [], "subjectRefs": [subject],
                }], "art": None}
            return SimpleNamespace(success=True, text=json.dumps(value))

        channel, _guild, _ = self.scheduler(fetch_effect=lambda user_id: SimpleNamespace(
            id=user_id, bot=False, guild=SimpleNamespace(id=42)))
        with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
            await bot.ambient_message_task.coro()

        self.assertEqual(provider.await_count, 2)
        self.assertEqual([route for _, route in captured], [
            "ambient_generation.community_edition", "ambient_generation.community_edition_repair"])
        for material, _ in captured:
            self.assertEqual(next(iter(material)), "original_contributions")
            original = next(item for item in material["original_contributions"] if item.get("text") == original_text)
            journal = next(item for item in (material["new_publications"] + material["earlier_publications"]) if item["kind"] == "published_journal")
            self.assertEqual(original["evidence_role"], "original_contribution")
            self.assertEqual(journal["evidence_role"], "bnl_expression")
            self.assertNotIn("text", journal)
            self.assertNotIn("expression_text", journal)
            self.assertEqual(journal["publication_card"]["excerpt"], "An earlier discussion revisited.")
            self.assertNotIn(journal_text, json.dumps(material))
            self.assertFalse(any(item["ref"] == journal["ref"] for item in material["recorded_events"]))
        channel.send.assert_awaited_once()
        embed = channel.send.call_args.kwargs["embed"]
        self.assertNotIn("title", embed.to_dict())
        self.assertEqual(embed.description, accepted["text"] +
                         "\n[Ceramic Receivers](<https://site.test/journal/journal_daily_2026-09-10_fixture>)")
        self.assertNotIn(journal_text, embed.description)
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [8], "parse": []})
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 1)
        rewrite.assert_not_awaited()

    async def test_quiet_day_can_deliver_one_original_contribution_without_invented_sections(self):
        selected = {}
        def response(material):
            original = next(item for item in material["original_contributions"]
                            if item["scope"] == "window_activity")
            selected.update(original)
            subject = original["subject_refs"][0]
            return {"action": "post", "paragraphs": [{
                "text": "[[person:" + subject + "]] left a rhythm idea for the room to explore.",
                "sourceRefs": [original["ref"]], "subjectRefs": [subject],
            }], "art": None}
        channel, guild, provider, captured = await self.deliver_representative(response)
        self.assertEqual(provider.await_count, 2)
        channel.send.assert_awaited_once()
        embed = channel.send.call_args.kwargs["embed"]
        self.assertNotIn("title", embed.to_dict())
        self.assertNotIn("\n\n", embed.description)
        self.assertIn(selected["subject_labels"][selected["subject_refs"][0]], embed.description)
        self.assertEqual(captured[0][2]["recorded_events"], [])
        self.assertEqual(captured[0][2]["new_publications"], [])
        self.assertEqual(captured[0][2]["earlier_publications"], [])
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [7], "parse": []})
        guild.fetch_member.assert_awaited_once_with(7)

    async def test_busy_show_day_keeps_quiet_contributor_show_authority_and_exact_episode_link(self):
        for index in range(75):
            self.add_message("Another beat observation " + str(index), self.stamp(minutes=80 - index),
                             user_id=20, label="Frequent Member")
        self.add_message("A rare glass-harmonica rhythm could leave the middle open.", self.stamp(minutes=7),
                         user_id=8, label="Quiet Contributor")
        self.archive()
        self.representative_owner_inputs(show=True)
        expected = {}
        def response(material):
            contributor = next(item for item in material["original_contributions"]
                               if "discord_user:8" in item.get("subject_refs", ()))
            show = next(item for item in material["recorded_events"] if item["kind"] == "finalized_show")
            expected.update(contributor=contributor, show=show)
            return {"action": "post", "paragraphs": [{
                "text": "The recorded show carried 43 tracks and 180500 taps; [[person:discord_user:8]] "
                        "also proposed a glass-harmonica rhythm. That leaves an interesting direction to explore.",
                "sourceRefs": [show["ref"], contributor["ref"]], "subjectRefs": ["discord_user:8"],
            }], "art": None}
        channel, guild, provider, captured = await self.deliver_representative(response)
        self.assertEqual(provider.await_count, 2)
        channel.send.assert_awaited_once()
        self.assertEqual(expected["show"]["evidence_role"], "recorded_event")
        self.assertEqual(expected["contributor"]["evidence_role"], "original_contribution")
        self.assertGreater(len(captured[0][2]["original_contributions"]), 2)
        self.assertIn("https://site.test/radio/archive?view=shows&show=fixture-show-1",
                      channel.send.call_args.kwargs["embed"].description)
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [8], "parse": []})
        guild.fetch_member.assert_awaited_once_with(8)

    async def test_new_journal_and_ballad_can_announce_older_reporting_without_new_event_claims(self):
        self.execute("UPDATE conversations SET channel_policy='sealed_test'")
        self.add_publication()
        self.representative_owner_inputs(ballad=True)
        expected = {}
        def response(material):
            journal = next(item for item in (material["new_publications"] + material["earlier_publications"]) if item["kind"] == "published_journal")
            ballad = next(item for item in (material["new_publications"] + material["earlier_publications"]) if item["kind"] == "published_ballad")
            expected.update(journal=journal, ballad=ballad)
            return {"action": "post", "paragraphs": [{
                "text": "Two new publications revisit earlier material: the Journal follows ceramic receivers, "
                        "while my Ballad gives the September 9 show an imaginary choir. Both are available below.",
                "publicationRefs": [journal["ref"], ballad["ref"]],
            }], "art": None}
        channel, guild, provider, captured = await self.deliver_representative(response)
        self.assertEqual(provider.await_count, 2)
        channel.send.assert_awaited_once()
        self.assertEqual(captured[0][2]["recorded_events"], [])
        for item in expected.values():
            self.assertEqual(item["evidence_role"], "bnl_expression")
            self.assertNotIn("occurred_at", item)
            self.assertEqual(item["scope"], "window_publication")
            self.assertNotIn("text", item)
        self.assertLess(sources._utc(expected["journal"]["reported_window_end"]),
                        sources._utc(expected["journal"]["published_at"]))
        description = channel.send.call_args.kwargs["embed"].description
        self.assertIn("https://site.test/journal/journal_daily_2026-09-10_fixture", description)
        self.assertIn("https://site.test/radio/archive?view=shows&show=fixture-older-show#broadcast-ballad", description)
        self.assertNotIn("999", description)
        self.assertIsNone(channel.send.call_args.args[0])
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [], "parse": []})
        guild.fetch_member.assert_not_awaited()

    async def test_mixed_day_delivers_connected_prose_with_all_roles_and_only_featured_verified_person(self):
        self.add_message("A new arrangement leaves space around the percussion.", self.stamp(minutes=3),
                         user_id=8, label="Test Arranger")
        self.add_message("WITHHELD_MIXED_DAY", self.stamp(minutes=2), policy="sealed_test", user_id=9)
        self.archive()
        self.add_publication()
        self.representative_owner_inputs(show=True, ballad=True, moment=True)
        expected = {}
        def response(material):
            original = next(item for item in material["original_contributions"]
                            if "discord_user:8" in item.get("subject_refs", ()))
            show = next(item for item in material["recorded_events"] if item["kind"] == "finalized_show")
            journal = next(item for item in (material["new_publications"] + material["earlier_publications"]) if item["kind"] == "published_journal")
            ballad = next(item for item in (material["new_publications"] + material["earlier_publications"]) if item["kind"] == "published_ballad")
            moment = next(item for item in material["governed_interpretations"] if item["kind"] == "public_moment")
            expected["refs"] = [item["ref"] for item in (original, show, journal, ballad, moment)]
            return {"action": "post", "paragraphs": [{
                "text": "[[person:discord_user:8]] proposed leaving space around percussion after a show with "
                        "43 recorded tracks. An earlier exchange explored that idea too. It gives me a fresh "
                        "way to revisit my newly published Journal and Ballad without treating either as another witness.",
                "sourceRefs": [original["ref"], show["ref"]],
                "publicationRefs": [journal["ref"], ballad["ref"]], "contextRefs": [moment["ref"]],
                "subjectRefs": ["discord_user:8"],
            }], "art": None}
        channel, guild, provider, captured = await self.deliver_representative(response)
        self.assertEqual(provider.await_count, 2)
        channel.send.assert_awaited_once()
        self.assertEqual(len(expected["refs"]), 5)
        self.assertNotIn("WITHHELD_MIXED_DAY", captured[0][0])
        material = captured[0][2]
        self.assertTrue(all(material[key] for key in (
            "original_contributions", "recorded_events", "new_publications", "governed_interpretations")))
        description = channel.send.call_args.kwargs["embed"].description
        self.assertEqual(description.count("https://site.test/"), 3)
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [8], "parse": []})
        self.assertEqual(channel.send.call_args.args[0], "Featuring: <@8>")
        guild.fetch_member.assert_awaited_once_with(8)

    async def test_real_wrapper_repairs_actual_unbound_draft_with_paragraph_and_source_binding_feedback(self):
        self.add_message("My new arrangement keeps room around the percussion.", self.stamp(minutes=3),
                         user_id=8, label="Test Arranger")
        self.archive()
        drafts = []
        support = {}
        def response(material):
            first = next(item for item in material["original_contributions"]
                         if "discord_user:7" in item.get("subject_refs", ()))
            arranger = next(item for item in material["original_contributions"]
                            if "discord_user:8" in item.get("subject_refs", ()))
            support.update(first=first, arranger=arranger)
            value = {"action": "post", "paragraphs": [{
                "text": "[[person:discord_user:8]] described an arrangement with space around the percussion.",
                # The first draft attaches the wrong person's source to its
                # attribution. The second keeps the prose and corrects proof.
                "sourceRefs": [first["ref"] if not drafts else arranger["ref"]],
                "subjectRefs": ["discord_user:8"],
            }], "art": None}
            drafts.append(json.dumps(value))
            return value
        channel, guild, provider, captured = await self.deliver_representative(response)
        self.assertEqual(provider.await_count, 2)
        self.assertEqual([item[1] for item in captured], [
            "ambient_generation.community_edition", "ambient_generation.community_edition_repair"])
        repair_prompt = captured[1][0]
        raw_rejected = repair_prompt.split("Rejected draft:\n", 1)[1].split("\nValidation feedback:\n", 1)[0]
        self.assertEqual(json.loads(raw_rejected), drafts[0])
        feedback_text = repair_prompt.split("Validation feedback:\n", 1)[1]
        feedback, _ = json.JSONDecoder().raw_decode(feedback_text)
        self.assertEqual(feedback["reason"], "edition_unbound_subject")
        self.assertEqual(feedback["paragraph"], 1)
        self.assertFalse(feedback["draftTruncated"])
        self.assertTrue(feedback["instruction"])
        self.assertEqual(feedback["details"]["tokenSubjects"], ["discord_user:8"])
        self.assertEqual(feedback["details"]["declaredSubjects"], ["discord_user:8"])
        self.assertEqual(feedback["details"]["supportedByParagraph"], ["discord_user:7"])
        self.assertEqual(feedback["details"]["eligibleBindings"]["discord_user:8"]["sourceRefs"],
                         [support["arranger"]["ref"]])
        self.assertIn("original_contribution", feedback["referenceFields"]["sourceRefs"])
        self.assertEqual(feedback["referenceFields"]["publicationRefs"], ["bnl_expression"])
        self.assertEqual(captured[0][2], captured[1][2])
        channel.send.assert_awaited_once()
        guild.fetch_member.assert_awaited_once_with(8)
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [8], "parse": []})
        self.assertNotIn("Validation feedback", channel.send.call_args.kwargs["embed"].description)
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 1)

    async def test_valid_first_draft_is_revised_before_delivery_with_original_sources_and_owned_links(self):
        # Mocked prose proves the two-stage delivery contract, not that the
        # model can independently discover every chronology or attribution error.
        earlier = "Do not let a bucket of code boss your music around."
        later = "I have now shared two newly finished tracks."
        self.add_message(earlier, self.stamp(hours=2), user_id=8, label="Test Creator")
        self.add_message(later, self.stamp(minutes=3), user_id=8, label="Test Creator")
        self.archive()
        self.add_publication()
        drafts, calls = [], []
        revised_text = ("[[person:discord_user:8]] warned against code bossing music around earlier, "
                        "then later shared two finished tracks. I like the priorities. "
                        "My new Journal revisits an older discussion about receivers.")

        async def boundary(contents, route, **kwargs):
            self.assert_edition_voice(contents)
            channel.send.assert_not_awaited()
            prepare.assert_not_awaited()
            material = self.material(contents)
            originals = [item for item in material["original_contributions"]
                         if item.get("text") in (earlier, later)]
            journal = material["new_publications"][0]
            calls.append((contents, route, material))
            text = ("[[person:discord_user:8]] shared two finished tracks, prompting the earlier "
                    "warning about code bossing music around.") if len(calls) == 1 else revised_text
            value = {"action": "post", "paragraphs": [{
                "text": text, "sourceRefs": [item["ref"] for item in originals],
                "publicationRefs": [journal["ref"]], "subjectRefs": ["discord_user:8"],
            }], "art": None}
            drafts.append(json.dumps(value))
            return SimpleNamespace(success=True, text=drafts[-1])

        channel, guild, _ = self.scheduler(fetch_effect=lambda user_id: SimpleNamespace(
            id=user_id, bot=False, guild=SimpleNamespace(id=42)))
        authority_guard = bot.should_reject_unsupported_source_authority
        with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite, \
                mock.patch.object(bot, "should_reject_unsupported_source_authority", wraps=authority_guard) as guard, \
                mock.patch.object(art, "prepare", new=mock.AsyncMock(return_value=None)) as prepare:
            await bot.ambient_message_task.coro()

        self.assertEqual(provider.await_count, 2)
        self.assertEqual([item[1] for item in calls], [
            "ambient_generation.community_edition", "ambient_generation.community_edition_repair"])
        self.assertEqual(calls[0][2], calls[1][2])
        review = calls[1][0]
        encoded = review.split("Draft for editorial review:\n", 1)[1].split("\nValidation feedback:\n", 1)[0]
        self.assertEqual(json.loads(encoded), drafts[0])
        feedback, _ = json.JSONDecoder().raw_decode(review.split("Validation feedback:\n", 1)[1])
        self.assertEqual(feedback["reason"], "edition_editorial_review")
        self.assertEqual(guard.call_count, 2)
        self.assertEqual(guard.call_args_list[0].args[1], guard.call_args_list[1].args[1])
        self.assertNotIn("Draft for editorial review", guard.call_args_list[1].args[1])
        self.assertNotIn("prompting the earlier", guard.call_args_list[1].args[1])
        channel.send.assert_awaited_once()
        self.assertEqual(channel.send.call_args.kwargs["embed"].description,
                         revised_text.replace("[[person:discord_user:8]]", "Test Creator") +
                         "\n[Ceramic Receivers](<https://site.test/journal/journal_daily_2026-09-10_fixture>)")
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [8], "parse": []})
        guild.fetch_member.assert_awaited_once_with(8)
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 1)
        rewrite.assert_not_awaited()

    async def test_unsuccessful_editorial_call_never_falls_back_to_valid_first_draft_or_generates_art(self):
        for outcome in ("skip", "invalid", "empty", "failure", "budget_denied"):
            with self.subTest(outcome=outcome):
                self.execute("DELETE FROM guild_configs")
                self.execute("DELETE FROM ambient_log")
                calls = []
                async def boundary(contents, route, **kwargs):
                    channel.send.assert_not_awaited()
                    prepare.assert_not_awaited()
                    calls.append(route)
                    if len(calls) == 1:
                        return SimpleNamespace(success=True, text=self.response(contents))
                    if outcome == "failure":
                        return SimpleNamespace(success=False, error_category="fixture_unavailable")
                    text = ('{"action":"skip"}' if outcome == "skip" else
                            "" if outcome == "empty" else '{"action":"post","paragraphs":[]}')
                    return SimpleNamespace(success=True, text=text)

                channel, guild, _ = self.scheduler()
                quota = [True, False] if outcome == "budget_denied" else [True, True]
                with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                        mock.patch.object(bot, "check_quota_availability", side_effect=quota) as allowance, \
                        mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                        mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite, \
                        mock.patch.object(art, "prepare", new=mock.AsyncMock()) as prepare:
                    await bot.ambient_message_task.coro()
                self.assertEqual(provider.await_count, 1 if outcome == "budget_denied" else 2)
                self.assertEqual(allowance.call_count, 2)
                channel.send.assert_not_awaited()
                guild.fetch_member.assert_not_awaited()
                prepare.assert_not_awaited()
                rewrite.assert_not_awaited()
                self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_source_withdrawal_during_editorial_call_discards_both_drafts(self):
        calls = []
        async def boundary(contents, route, **kwargs):
            calls.append(route)
            answer = self.response(contents)
            if len(calls) == 2:
                self.execute("UPDATE conversations SET channel_policy='sealed_test'")
            return SimpleNamespace(success=True, text=answer)

        channel, guild, _ = self.scheduler()
        with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=boundary)) as provider, \
                mock.patch.object(art, "prepare", new=mock.AsyncMock()) as prepare:
            await bot.ambient_message_task.coro()
        self.assertEqual(provider.await_count, 2)
        channel.send.assert_not_awaited()
        guild.fetch_member.assert_not_awaited()
        prepare.assert_not_awaited()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_withdrawal_during_generation_discards_without_repair(self):
        def withdraw(prompt, *args, **kwargs):
            answer = self.response(prompt)
            self.execute("UPDATE conversations SET channel_policy='sealed_test'")
            return answer
        self.provider.side_effect = withdraw
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100), "")
        self.provider.assert_awaited_once()

    async def test_art_enabled_real_provider_discards_a_withdrawn_original_without_repair(self):
        async def withdraw(contents, route, **kwargs):
            response = self.response(contents)
            self.execute("UPDATE conversations SET channel_policy='sealed_test'")
            return SimpleNamespace(success=True, text=response)
        with mock.patch.object(art, "available", return_value=True), \
                mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=True), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(side_effect=withdraw)) as provider, \
                mock.patch.object(bot, "_generate_gemini_content_with_fallback_async", new=mock.AsyncMock()) as rewrite:
            basis = {}
            self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), "")
            self.assertEqual(basis, {})
            provider.assert_awaited_once()
            rewrite.assert_not_awaited()

    async def test_silence_is_one_decision_and_not_an_error(self):
        self.provider.side_effect = None
        self.provider.return_value = '{"action":"skip"}'
        basis = {}
        self.assertEqual(await bot.generate_dynamic_ambient(42, 100, source_basis_out=basis), "")
        self.assertTrue(basis["declined"])
        self.provider.assert_awaited_once()

    def scheduler(self, *, send_effect=None, fetch_effect=None):
        self.execute("INSERT INTO guild_configs(guild_id,active_channel_id,next_ambient_message_at) VALUES(42,100,?)",
                     ((self.fixture.now - timedelta(minutes=1)).isoformat(),))
        channel = mock.Mock(id=100)
        channel.send = mock.AsyncMock(side_effect=send_effect, return_value=SimpleNamespace(id=7654))
        guild = SimpleNamespace(id=42)
        member = SimpleNamespace(id=7, bot=False, guild=guild)
        guild.fetch_member = mock.AsyncMock(side_effect=fetch_effect, return_value=member)
        stack = ExitStack()
        stack.enter_context(mock.patch.object(bot.client, "get_channel", return_value=channel))
        stack.enter_context(mock.patch.object(bot.client, "get_guild", return_value=guild))
        stack.enter_context(mock.patch.object(bot, "resolve_channel_policy", return_value="public_home"))
        stack.enter_context(mock.patch.object(bot, "is_community_image_channel", return_value=False))
        stack.enter_context(mock.patch.object(bot, "process_due_occasion_for_guild", new=mock.AsyncMock(return_value={"status": "idle"})))
        stack.enter_context(mock.patch.object(bot, "is_high_activity_day", return_value=True))
        dormant = stack.enter_context(mock.patch.object(bot, "prepare_dormant_echo_canary", new=mock.AsyncMock(return_value={"status": "idle"})))
        self.addCleanup(stack.close)
        return channel, guild, dormant

    async def test_scheduler_delivers_one_rich_embed_and_exact_verified_mention_allowlist(self):
        def send(content, **kwargs):
            scheduled = self.execute("SELECT next_ambient_message_at FROM guild_configs WHERE guild_id=42")[0][0]
            self.assertGreater(datetime.fromisoformat(scheduled), self.fixture.now)
            return SimpleNamespace(id=7654)
        channel, guild, dormant = self.scheduler(send_effect=send)
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.assertEqual(channel.send.call_args.args[0], "Featuring: <@7>")
        kwargs = channel.send.call_args.kwargs
        self.assertGreater(len(kwargs["embed"].description), 280)
        self.assertEqual(kwargs["embed"].title, "A rhythm worth following")
        self.assertEqual(kwargs["allowed_mentions"].to_dict(), {"users": [7], "parse": []})
        self.assertNotIn("file", kwargs)
        guild.fetch_member.assert_awaited_once_with(7)
        dormant.assert_not_awaited()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 1)
        await bot.ambient_message_task.coro()
        self.assertEqual(channel.send.await_count, 1)
        self.assertEqual(self.provider.await_count, 2)

    async def test_delivery_preserves_a_personal_interpretation_before_the_factual_update(self):
        opening = "That unfinished rhythm has somewhere to go. I am leaving the door open. "
        def reflect(prompt, *args, **kwargs):
            value = json.loads(self.response(prompt))
            value["paragraphs"][0]["text"] = opening + value["paragraphs"][0]["text"]
            return json.dumps(value)
        self.provider.side_effect = reflect
        channel, _guild, _ = self.scheduler()
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        description = channel.send.call_args.kwargs["embed"].description
        self.assertTrue(description.startswith(opening + "Test Member noticed"))
        self.assertEqual(channel.send.call_args.kwargs["allowed_mentions"].to_dict(), {"users": [7], "parse": []})

    async def test_membership_lookup_withdrawal_is_caught_by_final_source_fence(self):
        def withdraw(_user_id):
            self.execute("UPDATE conversations SET channel_policy='sealed_test'")
            return SimpleNamespace(id=7, bot=False, guild=SimpleNamespace(id=42))
        channel, guild, _ = self.scheduler(fetch_effect=withdraw)
        await bot.ambient_message_task.coro()
        guild.fetch_member.assert_awaited_once()
        channel.send.assert_not_awaited()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_budget_block_never_reaches_provider_or_discord(self):
        channel, _guild, _ = self.scheduler()
        with mock.patch.object(bot, "get_gemini_response", new=REAL_GET), \
                mock.patch.object(bot, "check_quota_availability", return_value=False), \
                mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock()) as provider:
            await bot.ambient_message_task.coro()
        provider.assert_not_awaited()
        channel.send.assert_not_awaited()

    async def test_unavailable_source_database_never_generates_or_sends(self):
        channel, _guild, _ = self.scheduler()
        with mock.patch.object(sources, "build_context", side_effect=sqlite3.OperationalError("fixture source unavailable")):
            await bot.ambient_message_task.coro()
        self.provider.assert_not_awaited()
        channel.send.assert_not_awaited()

    async def test_uncertain_transport_is_not_retried_on_the_next_tick(self):
        channel, _guild, _ = self.scheduler(send_effect=TimeoutError("fixture uncertain transport"))
        await bot.ambient_message_task.coro()
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.assertEqual(self.provider.await_count, 2)
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_process_cancellation_after_acceptance_keeps_durable_next_day_reservation(self):
        channel, _guild, _ = self.scheduler(send_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await bot.ambient_message_task.coro()
        channel.send.side_effect = None
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.assertEqual(self.provider.await_count, 2)
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)

    async def test_high_activity_cap_does_not_authorize_a_second_edition(self):
        channel, _guild, _ = self.scheduler()
        await bot.ambient_message_task.coro()
        decision = bot.ambient_capacity_decision(42, 100, "ambient", now_pacific=self.fixture.now)
        self.assertEqual(decision["cap"], 2)
        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["reason"], "community_edition_already_posted_today")
        self.execute("UPDATE guild_configs SET next_ambient_message_at=? WHERE guild_id=42",
                     ((self.fixture.now - timedelta(minutes=1)).isoformat(),))
        self.fixture.now += timedelta(hours=5)
        await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        self.assertEqual(self.provider.await_count, 2)

    async def test_old_day_art_never_remains_in_a_prebuilt_edition_attachment(self):
        channel, _guild, _ = self.scheduler()
        old_art = {"metadata": {"artId": "bnl-art-2026-09-10"}}
        with mock.patch.object(art, "prepare", new=mock.AsyncMock(return_value=old_art)), \
                mock.patch.object(art, "record"), mock.patch.object(art, "discord_file") as file:
            await bot.ambient_message_task.coro()
        channel.send.assert_awaited_once()
        file.assert_not_called()
        self.assertNotIn("file", channel.send.call_args.kwargs)
        self.assertIsNone(channel.send.call_args.kwargs["embed"].image.url)

    async def test_midnight_candidate_is_withheld_before_generation_or_member_confirmation(self):
        self.fixture.now = self.fixture.now.replace(hour=23, minute=59, second=58)
        def after_midnight(_user_id):
            self.fixture.now += timedelta(seconds=5)
            return SimpleNamespace(id=7, bot=False, guild=SimpleNamespace(id=42))
        channel, guild, _ = self.scheduler(fetch_effect=after_midnight)
        image = {"metadata": {"artId": "bnl-art-2026-09-11"}}
        attachment = SimpleNamespace(filename="fixture.png", close=mock.Mock())
        with mock.patch.object(art, "prepare", new=mock.AsyncMock(return_value=image)), \
                mock.patch.object(art, "record"), \
                mock.patch.object(art, "discord_file", return_value=attachment), \
                mock.patch.object(art, "publish_website") as website:
            await bot.ambient_message_task.coro()
        self.provider.assert_not_awaited()
        guild.fetch_member.assert_not_awaited()
        channel.send.assert_not_awaited()
        self.assertEqual(self.execute("SELECT COUNT(*) FROM ambient_log")[0][0], 0)
        website.assert_not_called()


if __name__ == "__main__":
    unittest.main()
