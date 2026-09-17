"""Rehearsal facts survive real Context/direct/batch prompt composition.

Only transport/provider and the website read boundary are substituted. These
tests prove source delivery and scope, not live Gemini creative acceptance.
"""

import sqlite3
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest import mock

from tests import test_public_network_knowledge as network
from tests.test_rehearsal_read_model import REQUEST, rehearsal_model

bot = network.bnl01_bot
REAL_WEBSITE_CONTEXT = bot.maybe_build_bnl_read_model_context
SONG = (
    "Write the end-of-show song for Suno using the verified rehearsal facts "
    "from our last exchange. Use the normal song defaults. "
    "Do not invent other artists or events."
)
FEEDBACK = "Make the chorus less repetitive and change the genres. Keep the same rehearsal facts."
OVERRIDE = "Rewrite that as an eight-line hip hop chorus set in 2020. No Style section."
STANDALONE_SONG = (
    "Write an end-of-show song about the private BARCODE Radio [09-15-2026] "
    "rehearsal, using the available records for the whole session."
)


class RehearsalSongFollowthroughTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = network.PublicNetworkKnowledgeTests()
        await self.fixture.asyncSetUp()
        self.fixture._seed_finalized_show()
        self.model = rehearsal_model()
        self.model["sections"]["archive"]["latestShow"] = network.show_fixtures.archived_show()
        self.fixture.stack.enter_context(mock.patch.object(
            bot, "maybe_build_bnl_read_model_context", side_effect=REAL_WEBSITE_CONTEXT,
        ))
        self.fetch = self.fixture.stack.enter_context(mock.patch.object(
            bot, "fetch_bnl_read_model", side_effect=lambda **kw: self.model,
        ))

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    def seed(self, requests=(REQUEST,), *, user_id=None, policy="sealed_test"):
        now = datetime.now(timezone.utc)
        batch_channel = 8811 + len(self.fixture.channel_ids)
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("DELETE FROM conversations WHERE guild_id=?", (self.fixture.guild_id,))
            for channel_id in (8810, batch_channel):
                for i, request in enumerate(requests):
                    for role, text in (("user", request), ("model", "Earlier draft with unverified claims.")):
                        conn.execute(
                            """INSERT INTO conversations
                            (user_id,user_name,guild_id,channel_id,channel_name,
                             channel_policy,route_mode,role,content,timestamp)
                            VALUES (?,?,?,?,'bnl-testing',?,'normal_chat',?,?,?)""",
                            (self.fixture.user_id if user_id is None else user_id,
                             "Test Member", self.fixture.guild_id, channel_id, policy,
                             role, text, (now - timedelta(seconds=60-i)).isoformat()),
                        )

    def website(self, inputs):
        return REAL_WEBSITE_CONTEXT(
            inputs["clean_content"], inputs["channel_policy"],
            conversation_context=inputs["room_context"],
            guild_id=inputs["guild_id"], subject_user_id=inputs["user_id"],
            channel_id=inputs["channel_id"], channel_name=inputs["channel_name"],
            conversation_context_result=inputs["conversation_context_result"],
        )

    async def direct(self, request, policy="sealed_test"):
        inputs = self.fixture._direct_prompt_inputs(policy, request=request)
        website = self.website(inputs)
        inputs["website_read_model_context"] = website
        prompt, *_ = await bot.build_user_aware_prompt_async(**inputs)
        return prompt, website

    def assert_fresh_facts(self, prompt):
        self.assertIn("Test Artist B — B2 Complete", prompt)
        self.assertIn("Test Artist B — B3 Partial Priority", prompt)
        self.assertIn("actualPlayback=confirmed", prompt)
        self.assertIn("earlyCutoff=True", prompt)
        self.assertIn("track_resumed=1", prompt)
        self.assertIn("Do not treat this as durable memory", prompt)
        self.assertNotIn("Wrong Uploader", prompt)
        self.assertNotIn("Wrong Filename", prompt)
        self.assertNotIn("the green visuals during this song are wild", prompt)

    async def test_ballad_control_delivery_retry_does_not_generate_again(self):
        command = dict(id="control-draft-1", showId="show-attendance-1", showDate="2026-08-28",
                       kind="generate", baseVersion=None, options={})
        control = {"contractVersion": 1, "commands": [command], "catalogVersions": {}}
        result = bot.GenerationResult(True, '{"title":"Last Light","lyrics":"[Chorus]\\nLeave a light","style":"1977 chamber soul","palette":{}}')
        with mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", self.fixture.guild_id), \
             mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(return_value=result)) as generate, \
             mock.patch.object(bot, "_ballad_control_request_sync", side_effect=[control, OSError("delivery unavailable"), control, {"ok": True}]) as transport:
            await bot._run_ballad_control_cycle()
            await bot._run_ballad_control_cycle()
        generate.assert_awaited_once()
        first_receipt = transport.call_args_list[1].args[1]
        second_receipt = transport.call_args_list[3].args[1]
        self.assertEqual(first_receipt, second_receipt)
        self.assertEqual(first_receipt["outcome"], "complete")
        self.assertEqual(first_receipt["version"]["title"], "Last Light")

    async def test_ballad_budget_refusal_keeps_reason_and_replays_without_retry(self):
        command = dict(id="budget-draft-1", showId="show-attendance-1", showDate="2026-08-28",
                       kind="generate", baseVersion=None, options={})
        control = {"contractVersion": 1, "commands": [command], "catalogVersions": {}}
        result = bot.GenerationResult(
            False, error_category=bot.GENERATION_ERROR_LOCAL_MODEL_BUDGET,
            provider_error_code="monthly_target_pace",
            provider_error_message_safe="Provider detail must not enter the receipt",
        )
        with mock.patch.object(bot, "BNL_PRIMARY_GUILD_ID", self.fixture.guild_id), \
             mock.patch.object(bot, "check_quota_availability", return_value=True), \
             mock.patch.object(bot, "_generate_gemini_content_result_async", new=mock.AsyncMock(return_value=result)) as generate, \
             mock.patch.object(bot, "_ballad_control_request_sync", side_effect=[control, {"ok": True}, control, {"ok": True}]) as transport:
            await bot._run_ballad_control_cycle()
            await bot._run_ballad_control_cycle()
        generate.assert_awaited_once()
        receipt = transport.call_args_list[1].args[1]
        self.assertEqual(receipt, transport.call_args_list[3].args[1])
        self.assertEqual(receipt["outcome"], "failed")
        self.assertEqual(receipt["error"], "budget_restricted:monthly_target_pace")
        self.assertNotIn("version", receipt)

    async def test_revision_intent_does_not_promote_casual_phrases(self):
        self.assertTrue(bot._detect_request_intent(FEEDBACK)[0])
        self.assertTrue(bot._detect_request_intent(OVERRIDE)[0])
        for text in ("keep it real", "make it home safely", "change the subject eventually"):
            self.assertFalse(bot._detect_request_intent(text)[0])

    async def test_ballad_reader_uses_existing_finalized_public_show_owner(self):
        text, digest = bot.build_broadcast_ballad_evidence(
            bot.DB_FILE, self.fixture.guild_id, "show-attendance-1",
        )
        self.assertIn("First Signal", text)
        self.assertEqual(len(digest), 64)
        self.assertEqual(bot.build_broadcast_ballad_evidence(
            bot.DB_FILE, self.fixture.guild_id, "private-rehearsal",
        ), ("", ""))

    async def test_unaddressed_revision_after_standalone_song_reaches_provider(self):
        previous = []
        for request in (STANDALONE_SONG, FEEDBACK, OVERRIDE):
            with self.subTest(request=request):
                self.seed(previous)
                prompt, _website = await self.direct(request)
                self.assert_fresh_facts(prompt)
                channel, generation, _guard = await self.fixture._batch(
                    "sealed_test", request=request,
                    answer="[Chorus]\nA new refrain with room to breathe.",
                )
                generation.assert_awaited_once()
                self.assert_fresh_facts(generation.await_args.args[0])
                self.assertTrue(channel.sent)
            previous.append(request)

    async def test_lookup_song_feedback_override_reach_direct_and_batch_with_fresh_facts(self):
        previous = [REQUEST]
        for request in (SONG, FEEDBACK, OVERRIDE):
            with self.subTest(request=request):
                self.seed(previous)
                prompt, website = await self.direct(request)
                self.assert_fresh_facts(prompt)
                self.assertIn("Prior-conversation queue source candidate", website)
                self.assertIn(request, prompt)
                channel, generation, _guard = await self.fixture._batch(
                    "sealed_test", request="BNL, " + request, answer="[Chorus]\nThe station lights are coming home.",
                )
                generation.assert_awaited_once()
                self.assert_fresh_facts(generation.await_args.args[0])
                self.assertTrue(channel.sent)
                previous.append(request)
        self.assertTrue(all(call.kwargs.get("force") for call in self.fetch.call_args_list))

    async def test_song_and_revision_receive_other_tracks_in_the_same_session(self):
        queue = self.model["sections"]["queue"]
        queue["completed"][0]["submittedArtistName"] = "Test Artist A"
        queue["queue"][0]["submittedArtistName"] = "Test Artist C"
        previous = [REQUEST]
        for request in (SONG, FEEDBACK):
            with self.subTest(request=request):
                self.seed(previous)
                direct_prompt, _website = await self.direct(request)
                _channel, generation, _guard = await self.fixture._batch(
                    "sealed_test", request="BNL, " + request,
                    answer="[Chorus]\nAn original refrain.",
                )
                generation.assert_awaited_once()
                for prompt in (direct_prompt, generation.await_args.args[0]):
                    self.assert_fresh_facts(prompt)
                    for credit, state in (
                        ("Test Artist A — A1 Loaded Only", "stage=completed"),
                        ("Test Artist C — Waiting Song", "stage=queued"),
                        ("Test Artist B — Removed Song", "stage=removed"),
                    ):
                        self.assertIn(credit, prompt)
                        row = next(line for line in prompt.splitlines() if credit in line)
                        self.assertIn(state, row)
                        self.assertIn("actualPlayback=not_evidenced", row)
                    self.assertIn("not proof of the complete show history", prompt)
                previous.append(request)
        self.assertTrue(all(call.kwargs.get("force") for call in self.fetch.call_args_list))

    async def test_no_store_replies_and_checkins_keep_the_named_rehearsal_for_override(self):
        # This is the live no-store sequence, not idealized user/model pairs.
        # A later public show exists in the fixture and must not take over.
        self.seed((REQUEST, SONG, FEEDBACK, "yo!", "You get that BNL?"))
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("DELETE FROM conversations WHERE role='model'")
        direct_prompt, website = await self.direct(OVERRIDE)
        channel, generation, _guard = await self.fixture._batch(
            "sealed_test", request="BNL, " + OVERRIDE,
            answer="[Chorus]\nTest Artist B brought B2 Complete.",
        )
        generation.assert_awaited_once()
        self.assertTrue(channel.sent)
        for prompt in (direct_prompt, generation.await_args.args[0]):
            self.assert_fresh_facts(prompt)
            self.assertIn("BARCODE Radio [09-15-2026]", prompt)
        self.assertIn("Prior-conversation queue source candidate", website)
        # The bounded human request identifies the session; stale source facts
        # cannot be substituted if the private feed has moved to another one.
        self.model["sections"]["queue"]["session"]["showDate"] = "2026-09-16"
        _prompt, changed = await self.direct(OVERRIDE)
        self.assertIn("earlier session is unavailable", changed)
        self.assertNotIn("actualPlayback=confirmed", changed)
        _prompt, public = await self.direct(OVERRIDE, "public_home")
        self.assertNotIn("B2 Complete", public)

    async def test_style_ceiling_reaches_delivery_without_regeneration_or_lyric_changes(self):
        self.seed()
        lyrics = "1. Lyrics\n[Chorus]\nTest Artist B brings B2 Complete.\n"
        style = "1983: psychedelic soul + breakbeat. " + "Muted bass under a dry lead vocal. " * 40
        answer = lyrics + "\n2. Style\n" + style
        channel, generation, guard = await self.fixture._batch(
            "sealed_test", request="BNL, " + SONG, answer=answer,
        )
        generation.assert_awaited_once()
        guard.assert_awaited_once()
        self.assertEqual(len(channel.sent), 1)
        delivered = channel.sent[0]
        self.assertTrue(delivered.startswith(lyrics))
        self.assertLessEqual(len(delivered.split("2. Style\n", 1)[1]), 500)
        self.assertIn("psychedelic soul + breakbeat", delivered)
        # Typed packet generation still returns its raw envelope; the common
        # visible-response boundary applies the same formatting after parsing.
        prompt, _website = await self.direct(SONG)
        for route in ("get_gemini_response", bot.ORDINARY_CHAT_SINGLE_PACKET_ROUTE):
            checked, diagnostics = await bot.apply_guarded_response_regeneration(
                answer, prompt=prompt, current_user_text=SONG,
                user_id=self.fixture.user_id, guild_id=self.fixture.guild_id,
                route_mode="normal_chat", channel_policy="sealed_test",
                generation_route=route, source_context_available=True,
                regeneration_allowed=False,
            )
            self.assertEqual(checked, delivered)
            self.assertFalse(diagnostics["suppressed"])

    async def test_explicit_public_show_correction_keeps_its_own_sources(self):
        self.seed((REQUEST, SONG))
        prompt, website = await self.direct("Instead, recap the public show on 2026-08-28.")
        self.assertNotIn("Prior-conversation queue source candidate", website)
        self.assertNotIn("actualPlayback=confirmed", website)
        self.assertIn("the green visuals during this song are wild", prompt)
        prompt, website = await self.direct("Instead, recap the latest public show.")
        self.assertNotIn("Prior-conversation queue source candidate", website)
        self.assertIn("the green visuals during this song are wild", prompt)

    async def test_older_public_topic_cannot_replace_the_newer_rehearsal_candidate(self):
        self.seed(("Give me a recap of the public show on 2026-08-28.", REQUEST))
        prompt, _website = await self.direct(SONG)
        self.assert_fresh_facts(prompt)

    async def test_new_named_person_recall_keeps_independent_public_history(self):
        from tests.test_cross_source_show_recall import CrossSourceShowRecallTests, QUERY

        history = CrossSourceShowRecallTests()
        history.db = bot.DB_FILE
        history.shows = []
        history.sequence = 10000
        day = history.add_show()
        history.message(day, "I brought amber lanterns for the courtyard.")
        day = history.add_show("2026-09-04", "2026-09-05")
        history.message(day, "The amber lanterns are beside the doorway.")
        history.sync()
        baseline, _website = await self.direct(QUERY)
        self.assertIn("I brought amber lanterns for the courtyard.", baseline)
        self.assertIn("The amber lanterns are beside the doorway.", baseline)
        self.seed((REQUEST, SONG))
        prompt, _website = await self.direct(QUERY)
        self.assertIn("I brought amber lanterns for the courtyard.", prompt)
        self.assertIn("The amber lanterns are beside the doorway.", prompt)

    def test_website_title_dates_use_calendar_validation_without_guessing_numeric_prose(self):
        self.assertEqual(bot.requested_show_dates("BARCODE Radio [09-15-2026]"), ("2026-09-15",))
        self.assertTrue(bot.has_explicit_show_date("BARCODE Radio [02-30-2026]"))
        self.assertEqual(bot.requested_show_dates("BARCODE Radio [02-30-2026]"), ())
        self.assertFalse(bot.has_explicit_show_date("01-02-2026 could use either locale"))

    async def test_public_channel_cannot_inherit_private_rehearsal(self):
        self.seed()
        prompt, website = await self.direct(SONG, "public_home")
        self.assertNotIn("Test Artist B", prompt)
        self.assertNotIn("private-rehearsal", website)
        self.assertNotIn("B2 Complete", website)

    async def test_explicit_live_request_does_not_inherit_an_older_queue_scope(self):
        self.seed((REQUEST, SONG))
        request = "What is TikTok chat reacting to right now?"
        self.assertTrue(bot.is_live_show_reaction_query(request, check_show_date=False))
        _prompt, website = await self.direct(request)
        self.assertNotIn("Prior-conversation queue source candidate", website)

    async def test_other_speaker_and_ambiguous_referent_cannot_select_lookup(self):
        self.seed(user_id=99991)
        _prompt, website = await self.direct(SONG)
        self.assertNotIn("Prior-conversation queue source candidate", website)
        self.seed()
        inputs = self.fixture._direct_prompt_inputs("sealed_test", request=SONG)
        inputs["conversation_context_result"] = replace(
            inputs["conversation_context_result"], referent_status="ambiguous",
        )
        self.assertNotIn("Prior-conversation queue source candidate", self.website(inputs))

    async def test_new_session_or_revoked_queue_access_does_not_reuse_old_facts(self):
        self.seed()
        self.model["sections"]["queue"]["session"]["showDate"] = "2026-09-16"
        _prompt, website = await self.direct(SONG)
        self.assertIn("earlier session is unavailable", website)
        self.assertNotIn("actualPlayback=confirmed", website)
        self.model = rehearsal_model()
        self.model["capabilities"]["queueProduction"] = False
        _prompt, website = await self.direct(SONG)
        self.assertNotIn("actualPlayback=confirmed", website)
        self.assertNotIn("sessionId=private-rehearsal", website)

    async def test_prior_model_claim_alone_cannot_supply_lookup(self):
        self.seed(("Can you help me write something?",))
        with sqlite3.connect(bot.DB_FILE) as conn:
            conn.execute("UPDATE conversations SET content=? WHERE role='model'", (REQUEST,))
        _prompt, website = await self.direct(SONG)
        self.assertNotIn("Prior-conversation queue source candidate", website)
        self.assertNotIn("actualPlayback=confirmed", website)


if __name__ == "__main__":
    unittest.main()
