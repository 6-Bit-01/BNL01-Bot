import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

os.environ.setdefault("GEMINI_API_KEY", "test-gemini-key")
os.environ.setdefault("DISCORD_BOT_TOKEN", "test-discord-token")

import bnl01_bot


class FakeLiveQuoteChannel:
    def __init__(
        self,
        content,
        *,
        message_id=8801,
        author_id=202,
        channel_id=99,
        guild_id=1,
    ):
        self.id = channel_id
        self.guild = SimpleNamespace(id=guild_id)
        self.content = content
        self.message_id = message_id
        self.author_id = author_id
        self.deleted = False
        self.fetch_count = 0

    async def fetch_message(self, message_id):
        self.fetch_count += 1
        if self.deleted or int(message_id) != self.message_id:
            raise LookupError("message unavailable")
        return SimpleNamespace(
            id=self.message_id,
            content=self.content,
            author=SimpleNamespace(id=self.author_id, bot=False),
            channel=self,
            guild=self.guild,
        )


class ExactQuoteAuthorityTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.old_db = bnl01_bot.DB_FILE
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.close()
        bnl01_bot.DB_FILE = self.tmp.name
        bnl01_bot.init_db()

    def tearDown(self):
        bnl01_bot.DB_FILE = self.old_db
        try:
            os.unlink(self.tmp.name)
        except OSError:
            pass

    def insert_source(
        self,
        content,
        *,
        user_id=202,
        user_name="Crow",
        guild_id=1,
        channel_id=99,
        channel_policy="public_home",
        message_id=8801,
        observed_at=None,
    ):
        timestamp = observed_at or datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
            cursor = conn.execute(
                """
                INSERT INTO conversations
                  (user_id,user_name,guild_id,channel_name,channel_policy,
                   channel_id,message_id,role,content,timestamp)
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    user_id,
                    user_name,
                    guild_id,
                    "barcode-bot",
                    channel_policy,
                    channel_id,
                    message_id,
                    "user",
                    content,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def authority(self, request=None):
        request_text = request or (
            "what exactly did @Crow say about the payment in this dispute?"
        )
        return bnl01_bot.build_current_room_quote_authority(
            guild_id=1,
            requester_user_id=101,
            channel_id=99,
            channel_policy="public_home",
            request_text=request_text,
            target_user_id=202,
            current_direct=True,
        )

    def live_channel(self, content=None, **kwargs):
        return FakeLiveQuoteChannel(
            content
            or (
                "I agreed to send the payment after the final mix "
                "was approved."
            ),
            **kwargs,
        )

    def direct_batch_addressing(self):
        return bnl01_bot.DiscordTurnAddressing(
            speaker="Asker",
            explicit_tag_recipients=("BNL-01", "@Crow"),
            reply_target="none",
            explicitly_mentions_bnl=True,
            reply_targets_bnl=False,
            directly_targets_bnl=True,
            targets_other_human=True,
            plain_text_names_bnl=False,
        )

    def batch_turn(
        self,
        *,
        user_id,
        content,
        target_user_id=0,
        direct=True,
        name="Asker",
    ):
        addressing = bnl01_bot.DiscordTurnAddressing(
            speaker=name,
            explicit_tag_recipients=(),
            reply_target="none",
            explicitly_mentions_bnl=direct,
            reply_targets_bnl=False,
            directly_targets_bnl=direct,
            targets_other_human=bool(target_user_id),
            plain_text_names_bnl=False,
        )
        return bnl01_bot.BatchConversationTurn(
            name,
            content,
            user_id,
            addressing,
            (target_user_id,) if target_user_id else (),
        )

    def test_typed_target_survives_cleaning_and_ignores_bnl_mention(self):
        message = type(
            "Message",
            (),
            {
                "content": (
                    "<@999> what exactly did <@202> say about payment "
                    "in this dispute?"
                )
            },
        )()
        self.assertEqual(
            bnl01_bot.typed_moment_attribution_target_user_id(message),
            202,
        )
        ordinary = type(
            "Message",
            (),
            {
                "content": (
                    "<@999> what did <@202> mean about the poster layout?"
                )
            },
        )()
        self.assertEqual(
            bnl01_bot.typed_moment_attribution_target_user_id(ordinary),
            202,
        )

    def test_batch_attribution_belongs_to_actual_asker_not_first_speaker(self):
        contract = bnl01_bot.build_batch_attribution_contract(
            (
                self.batch_turn(
                    user_id=77,
                    content="The poster layout is nearly done.",
                    direct=False,
                    name="Aster",
                ),
                self.batch_turn(
                    user_id=101,
                    content="what did @Crow mean about the poster layout?",
                    target_user_id=202,
                ),
            ),
            guild_id=1,
            channel_id=99,
            channel_policy="public_home",
        )
        self.assertTrue(contract.third_party_attribution_requested)
        self.assertFalse(contract.exact_quote_requested)
        self.assertEqual(contract.requester_user_id, 101)
        self.assertEqual(contract.target_user_id, 202)
        self.assertIn("summarize the named member's meaning", contract.prompt_block)

    async def test_batch_high_stakes_quote_uses_live_typed_authority(self):
        self.insert_source(
            "I agreed to send the payment after the final mix was approved."
        )
        contract = bnl01_bot.build_batch_attribution_contract(
            (
                self.batch_turn(
                    user_id=101,
                    content=(
                        "what exactly did @Crow say about the payment "
                        "in this dispute?"
                    ),
                    target_user_id=202,
                ),
            ),
            guild_id=1,
            channel_id=99,
            channel_policy="public_home",
        )
        self.assertTrue(contract.exact_quote_requested)
        self.assertIsNone(contract.exact_quote_authority)

        source_message = type(
            "SourceMessage",
            (),
            {
                "id": 8801,
                "content": (
                    "I agreed to send the payment after the final mix "
                    "was approved."
                ),
                "author": type(
                    "Author",
                    (),
                    {"id": 202, "bot": False},
                )(),
                "guild": type("Guild", (), {"id": 1})(),
            },
        )()
        channel = type(
            "Channel",
            (),
            {
                "id": 99,
                "fetch_message": mock.AsyncMock(return_value=source_message),
            },
        )()
        source_message.channel = channel

        verified = await bnl01_bot.live_verify_batch_attribution_contract(
            contract,
            guild_id=1,
            channel=channel,
            channel_policy="public_home",
        )

        self.assertIsNotNone(verified.exact_quote_authority)
        self.assertEqual(
            verified.exact_quote_authority.target_user_id,
            202,
        )
        self.assertIn("Exact-quote authority", verified.prompt_block)
        channel.fetch_message.assert_awaited_once_with(8801)

    def test_ambiguous_batch_targets_fail_closed_to_gist_only(self):
        contract = bnl01_bot.build_batch_attribution_contract(
            (
                self.batch_turn(
                    user_id=101,
                    content="what did @Crow mean about the poster?",
                    target_user_id=202,
                ),
                self.batch_turn(
                    user_id=101,
                    content="what did @Aster mean about the poster?",
                    target_user_id=303,
                ),
            ),
            guild_id=1,
            channel_id=99,
            channel_policy="public_home",
        )
        self.assertTrue(contract.third_party_attribution_requested)
        self.assertEqual(contract.target_user_id, 0)
        self.assertIsNone(contract.exact_quote_authority)

    def test_prompt_marks_typed_ordinary_attribution_as_gist_only(self):
        metadata = {}
        prompt, _allow_greeting, _style = bnl01_bot.build_user_aware_prompt(
            101,
            1,
            "Asker",
            "what did @Crow mean about the poster layout?",
            channel_name="barcode-bot",
            channel_id=99,
            channel_policy="public_home",
            is_direct_interaction=True,
            prompt_metadata=metadata,
            moment_attribution_target_user_id=202,
        )
        self.assertTrue(metadata["third_party_attribution_requested"])
        self.assertFalse(metadata["exact_quote_requested"])
        self.assertIsNone(metadata["exact_quote_authority"])
        self.assertIn("Third-party attribution mode", prompt)
        self.assertIn("summarize the named member's meaning", prompt)

    def test_plain_name_attribution_gets_same_gist_only_guard(self):
        metadata = {}
        prompt, _allow_greeting, _style = bnl01_bot.build_user_aware_prompt(
            101,
            1,
            "Asker",
            "what did Crow mean about the poster layout?",
            channel_name="barcode-bot",
            channel_id=99,
            channel_policy="public_home",
            is_direct_interaction=True,
            prompt_metadata=metadata,
            moment_attribution_target_user_id=0,
        )
        self.assertTrue(metadata["third_party_attribution_requested"])
        self.assertFalse(metadata["exact_quote_requested"])
        self.assertIsNone(metadata["exact_quote_authority"])
        self.assertIn("Third-party attribution mode", prompt)
        for request in (
            "what did I say about the poster?",
            "what did you mean about the poster?",
            "what did BNL-01 say about the poster?",
        ):
            with self.subTest(request=request):
                self.assertFalse(
                    bnl01_bot.is_supported_third_party_attribution_request(
                        request,
                        excluded_labels=("Asker",),
                    )
                )
        self.assertFalse(
            bnl01_bot.is_supported_third_party_attribution_request(
                "what did Asker say about the poster?",
                excluded_labels=("Asker",),
            )
        )

    async def test_batch_wires_one_typed_target_and_live_quote_authority(self):
        source = (
            "I agreed to send the payment after the final mix was approved."
        )
        self.insert_source(source)
        turn = bnl01_bot.BatchConversationTurn(
            "Asker",
            "what exactly did @Crow say about payment in this dispute?",
            101,
            self.direct_batch_addressing(),
            (202,),
        )
        contract = bnl01_bot.build_batch_attribution_contract(
            [turn],
            guild_id=1,
            channel_id=99,
            channel_policy="public_home",
        )
        self.assertTrue(contract.exact_quote_requested)
        self.assertEqual(contract.target_user_id, 202)
        self.assertIsNone(contract.exact_quote_authority)
        self.assertNotIn(source, contract.prompt_block)

        channel = self.live_channel(source)
        verified = (
            await bnl01_bot.live_verify_batch_attribution_contract(
                contract,
                guild_id=1,
                channel=channel,
                channel_policy="public_home",
            )
        )
        self.assertIsNotNone(verified.exact_quote_authority)
        self.assertIn(source, verified.prompt_block)
        self.assertEqual(channel.fetch_count, 1)

    async def test_batch_with_multiple_typed_targets_fails_closed(self):
        turns = [
            bnl01_bot.BatchConversationTurn(
                "Asker",
                "what exactly did @Crow say about payment in this dispute?",
                101,
                self.direct_batch_addressing(),
                (202,),
            ),
            bnl01_bot.BatchConversationTurn(
                "Asker",
                "and what exactly did @Rook say about payment in this dispute?",
                101,
                self.direct_batch_addressing(),
                (303,),
            ),
        ]
        contract = bnl01_bot.build_batch_attribution_contract(
            turns,
            guild_id=1,
            channel_id=99,
            channel_policy="public_home",
        )
        self.assertEqual(contract.target_user_id, 0)
        self.assertTrue(contract.exact_quote_requested)
        channel = self.live_channel()
        verified = (
            await bnl01_bot.live_verify_batch_attribution_contract(
                contract,
                guild_id=1,
                channel=channel,
                channel_policy="public_home",
            )
        )
        self.assertIsNone(verified.exact_quote_authority)
        self.assertEqual(channel.fetch_count, 0)
        self.assertIn("unavailable", verified.prompt_block)

    def test_multi_user_batch_moment_context_is_target_scoped_only(self):
        contract = bnl01_bot.BatchAttributionContract(
            target_user_id=202,
            requester_user_id=101,
            request_text="what did @Crow mean about the poster layout?",
            current_direct=True,
            third_party_attribution_requested=True,
            exact_quote_requested=False,
        )
        with (
            mock.patch.object(
                bnl01_bot,
                "moment_gist_canary_enabled",
                return_value=True,
            ),
            mock.patch.object(
                bnl01_bot,
                "render_shadow_moment_context",
                return_value="Crow favored the cobalt framing.",
            ) as render,
        ):
            context = bnl01_bot.build_batch_moment_attribution_context(
                contract,
                guild_id=1,
                channel_id=99,
                channel_policy="public_home",
            )
        self.assertIn("paraphrase only", context)
        self.assertIn("cobalt framing", context)
        self.assertEqual(
            render.call_args.kwargs["participant_key"],
            bnl01_bot.subject_key_for_user(101),
        )
        self.assertEqual(
            render.call_args.kwargs["attribution_target_key"],
            bnl01_bot.subject_key_for_user(202),
        )

    def test_current_same_room_public_raw_row_becomes_typed_authority(self):
        row_id = self.insert_source(
            "I agreed to send the payment after the final mix was approved."
        )
        authority = self.authority()
        self.assertIsNotNone(authority)
        self.assertEqual(authority.source_row_id, row_id)
        self.assertEqual(authority.source_message_id, 8801)
        self.assertEqual(authority.target_user_id, 202)
        self.assertEqual(authority.channel_id, 99)
        self.assertEqual(authority.channel_policy, "public_home")
        self.assertIn("payment after the final mix", authority.source_text)

    async def test_live_discord_message_must_match_before_prompt_authority(self):
        source = (
            "I agreed to send the payment after the final mix was approved."
        )
        self.insert_source(source)
        channel = self.live_channel(source)
        authority = await bnl01_bot.resolve_live_exact_quote_authority(
            guild_id=1,
            requester_user_id=101,
            channel=channel,
            channel_policy="public_home",
            request_text=(
                "what exactly did @Crow say about the payment "
                "in this dispute?"
            ),
            target_user_id=202,
            current_direct=True,
        )
        self.assertIsNotNone(authority)
        self.assertEqual(channel.fetch_count, 1)

        metadata = {}
        prompt, _allow, _style = bnl01_bot.build_user_aware_prompt(
            101,
            1,
            "Asker",
            "what exactly did @Crow say about the payment in this dispute?",
            channel_name="barcode-bot",
            channel_id=99,
            channel_policy="public_home",
            is_direct_interaction=True,
            prompt_metadata=metadata,
            moment_attribution_target_user_id=202,
            verified_exact_quote_authority=authority,
        )
        self.assertIn(source, prompt)
        self.assertIs(metadata["exact_quote_authority"], authority)

    def test_unverified_db_candidate_never_enters_prompt(self):
        source = (
            "I agreed to send the payment after the final mix was approved."
        )
        self.insert_source(source)
        self.assertIsNotNone(self.authority())
        metadata = {}
        prompt, _allow, _style = bnl01_bot.build_user_aware_prompt(
            101,
            1,
            "Asker",
            "what exactly did @Crow say about the payment in this dispute?",
            channel_name="barcode-bot",
            channel_id=99,
            channel_policy="public_home",
            is_direct_interaction=True,
            prompt_metadata=metadata,
            moment_attribution_target_user_id=202,
        )
        self.assertNotIn(source, prompt)
        self.assertIn("Exact-quote authority: unavailable", prompt)
        self.assertIsNone(metadata["exact_quote_authority"])

    async def test_live_edit_or_delete_never_becomes_prompt_authority(self):
        source = (
            "I agreed to send the payment after the final mix was approved."
        )
        self.insert_source(source)
        for mode in ("edited", "deleted"):
            with self.subTest(mode=mode):
                channel = self.live_channel(source)
                if mode == "edited":
                    channel.content = "I did not agree to that payment."
                else:
                    channel.deleted = True
                authority = await bnl01_bot.resolve_live_exact_quote_authority(
                    guild_id=1,
                    requester_user_id=101,
                    channel=channel,
                    channel_policy="public_home",
                    request_text=(
                        "what exactly did @Crow say about the payment "
                        "in this dispute?"
                    ),
                    target_user_id=202,
                    current_direct=True,
                )
                self.assertIsNone(authority)
                self.assertEqual(channel.fetch_count, 1)

    async def test_ordinary_gist_attribution_never_fetches_discord(self):
        channel = self.live_channel("The poster should use cobalt.")
        authority = await bnl01_bot.resolve_live_exact_quote_authority(
            guild_id=1,
            requester_user_id=101,
            channel=channel,
            channel_policy="public_home",
            request_text="what did @Crow mean about the poster?",
            target_user_id=202,
            current_direct=True,
        )
        self.assertIsNone(authority)
        self.assertEqual(channel.fetch_count, 0)

    def test_high_stakes_and_dispute_are_both_required(self):
        self.assertFalse(
            bnl01_bot.is_consequential_exact_quote_request(
                "what exactly did Crow say about payment?"
            )
        )
        self.assertFalse(
            bnl01_bot.is_consequential_exact_quote_request(
                "what exactly did Crow say in this disagreement?"
            )
        )
        self.assertTrue(
            bnl01_bot.is_consequential_exact_quote_request(
                "what exactly did Crow say about payment in this dispute?"
            )
        )
        self.assertTrue(
            bnl01_bot.is_consequential_exact_quote_request(
                "verify Crow's exact wording for the harassment complaint"
            )
        )
        self.assertTrue(
            bnl01_bot.is_consequential_exact_quote_request(
                "what were Crow's exact words for the ban appeal?"
            )
        )

    def test_unrelated_latest_row_cannot_replace_topic_matching_source(self):
        relevant_id = self.insert_source(
            "I agreed to send the payment after the final mix was approved.",
            message_id=8801,
        )
        self.insert_source(
            "The purple synth patch needs a slower attack.",
            message_id=8802,
        )
        authority = self.authority()
        self.assertIsNotNone(authority)
        self.assertEqual(authority.source_row_id, relevant_id)
        self.assertNotIn("purple synth", authority.source_text)

    def test_equally_relevant_rows_fail_closed_without_a_replied_to_message(self):
        self.insert_source(
            "I agreed to send the payment after the final mix was approved.",
            message_id=8801,
        )
        self.insert_source(
            "I later said the payment should wait until Friday.",
            message_id=8802,
        )
        self.assertIsNone(self.authority())

    def test_unique_highest_topic_match_remains_eligible(self):
        self.insert_source(
            "I agreed to send the payment after the final mix was approved.",
            message_id=8801,
        )
        strongest_id = self.insert_source(
            "The payment deadline is Friday after the final mix review.",
            message_id=8802,
        )
        authority = self.authority(
            "what exactly did @Crow say about the payment deadline "
            "in this dispute?"
        )
        self.assertIsNotNone(authority)
        self.assertEqual(authority.source_row_id, strongest_id)

    def test_mixed_attribution_clauses_fail_before_topic_scoring(self):
        self.insert_source(
            "The poster layout should use stronger cobalt framing.",
            message_id=8801,
        )
        self.insert_source(
            "I agreed to send the payment Friday.",
            message_id=8802,
        )
        authority = self.authority(
            "what did @Crow mean about the poster layout, and what exactly "
            "did @Crow say about the payment in this dispute?"
        )
        self.assertIsNone(authority)

    def test_zero_topic_overlap_yields_no_quote_authority(self):
        self.insert_source(
            "The purple synth patch needs a slower attack.",
            message_id=8802,
        )
        self.assertIsNone(self.authority())

    def test_authority_fails_closed_outside_narrow_source_contract(self):
        now = datetime.now(timezone.utc)
        cases = (
            {
                "content": "I agreed to send the payment.",
                "channel_id": 77,
                "message_id": 1,
            },
            {
                "content": "I agreed to send the payment.",
                "channel_policy": "public_selective",
                "message_id": 2,
            },
            {
                "content": "I agreed to send the payment.",
                "message_id": None,
            },
            {
                "content": "I agreed to send the payment.",
                "message_id": 4,
                "observed_at": (
                    now
                    - timedelta(
                        minutes=bnl01_bot.EXACT_QUOTE_SOURCE_WINDOW_MINUTES + 1
                    )
                ).isoformat(),
            },
            {
                "content": "I agreed to send the @payment.",
                "message_id": 5,
            },
        )
        for index, case in enumerate(cases):
            with self.subTest(index=index):
                with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                    conn.execute("DELETE FROM conversations")
                self.insert_source(**case)
                self.assertIsNone(self.authority())
        self.insert_source(
            "I agreed to send the payment.",
            message_id=6,
        )
        self.assertIsNone(
            bnl01_bot.build_current_room_quote_authority(
                guild_id=1,
                requester_user_id=101,
                channel_id=99,
                channel_policy="public_home",
                request_text="what exactly did @Crow say about payment?",
                target_user_id=202,
                current_direct=True,
            )
        )
        self.assertIsNone(
            bnl01_bot.build_current_room_quote_authority(
                guild_id=1,
                requester_user_id=101,
                channel_id=99,
                channel_policy="public_home",
                request_text="what exactly did @Crow say about the final mix?",
                target_user_id=202,
                current_direct=True,
            )
        )
        self.assertIsNone(
            bnl01_bot.build_current_room_quote_authority(
                guild_id=1,
                requester_user_id=101,
                channel_id=99,
                channel_policy="public_home",
                request_text=(
                    "what exactly did @Crow say about payment in this dispute?"
                ),
                target_user_id=202,
                current_direct=False,
            )
        )

    def test_only_minimal_substrings_of_typed_raw_source_are_allowed(self):
        self.insert_source(
            "I agreed to send the payment after the final mix was approved."
        )
        authority = self.authority()
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                'Crow said: “send the payment after the final mix was approved.”',
                requested=True,
                authority=authority,
            ),
            "",
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                'Crow said: “I never agreed to the payment.”',
                requested=True,
                authority=authority,
            ),
            "quote_not_in_current_raw_source",
        )

    def test_moment_tier_or_relationship_text_never_authorizes_a_quote(self):
        response = (
            'A Moment gist and relationship note say Crow’s exact words were '
            '“the payment was already approved.”'
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                response,
                requested=True,
                authority=None,
            ),
            "quote_authority_unavailable",
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                "I cannot verify exact wording from a derived gist.",
                requested=True,
                authority=None,
            ),
            "",
        )

    def test_ordinary_attribution_rejects_quote_but_allows_paraphrase(self):
        invented_quote = 'Crow said “make the border red.”'
        paraphrase = (
            "Crow's point was that the border should carry more visual weight."
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                invented_quote,
                requested=True,
                authority=None,
            ),
            "quote_authority_unavailable",
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                paraphrase,
                requested=True,
                authority=None,
            ),
            "",
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                "Crow preferred cobalt over amber.",
                requested=True,
                authority=None,
                exact_requested=False,
            ),
            "",
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                invented_quote,
                requested=False,
                authority=None,
            ),
            "",
        )

    def test_unquoted_wording_claims_fail_but_labeled_gist_is_allowed(self):
        for response in (
            "Crow literally said I agreed to send the payment.",
            "Crow said I agreed to send the payment.",
            "Crow wrote that the payment was approved.",
        ):
            with self.subTest(response=response):
                self.assertEqual(
                    bnl01_bot.exact_quote_response_failure(
                        response,
                        requested=True,
                        authority=None,
                    ),
                    "quote_authority_unavailable",
                )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                "In other words, Crow planned to pay after approval.",
                requested=True,
                authority=None,
            ),
            "",
        )

    def test_ordinary_gist_label_does_not_cover_a_separate_attribution_clause(self):
        for response in (
            (
                "The gist is that Crow planned to pay after approval. "
                "Vale wrote that the payment had cleared."
            ),
            (
                "In other words, Crow planned to wait; "
                "Vale posted that the transfer was complete."
            ),
            (
                "Roughly, Crow expected another review, but "
                "Vale said the decision was final."
            ),
        ):
            with self.subTest(response=response):
                self.assertEqual(
                    bnl01_bot.exact_quote_response_failure(
                        response,
                        requested=True,
                        authority=None,
                        exact_requested=False,
                    ),
                    "quote_authority_unavailable",
                )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                "The gist is that Crow said the payment could wait.",
                requested=True,
                authority=None,
                exact_requested=False,
            ),
            "",
        )

    def test_exact_request_mixed_output_requires_each_attribution_to_be_labeled(self):
        response = (
            "I can't verify the exact wording. "
            "The gist is that Crow planned to pay after approval. "
            "Crow said the transfer was approved."
        )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                response,
                requested=True,
                authority=None,
                exact_requested=True,
            ),
            "quote_authority_unavailable",
        )

    def test_exact_request_without_authority_requires_refusal_or_labeled_gist(self):
        for response in (
            "Crow agreed to send the payment.",
            "Crow's position was that the payment could wait.",
        ):
            with self.subTest(response=response):
                self.assertEqual(
                    bnl01_bot.exact_quote_response_failure(
                        response,
                        requested=True,
                        authority=None,
                        exact_requested=True,
                    ),
                    "exact_request_requires_refusal_or_labeled_gist",
                )
        self.assertEqual(
            bnl01_bot.exact_quote_response_failure(
                (
                    "I can't verify the exact wording, but Crow said "
                    "I agreed to send the payment."
                ),
                requested=True,
                authority=None,
                exact_requested=True,
            ),
            "quote_authority_unavailable",
        )
        for response in (
            "The gist is that Crow planned to pay after approval.",
            "I can't verify the exact wording from a live message.",
        ):
            with self.subTest(response=response):
                self.assertEqual(
                    bnl01_bot.exact_quote_response_failure(
                        response,
                        requested=True,
                        authority=None,
                        exact_requested=True,
                    ),
                    "",
                )

    async def test_guard_regenerates_unsupported_quote_to_authorized_excerpt(self):
        self.insert_source(
            "I agreed to send the payment after the final mix was approved."
        )
        authority = self.authority()
        channel = self.live_channel()
        repaired = (
            'Crow said: “I agreed to send the payment after the final mix '
            'was approved.”'
        )
        provider = mock.AsyncMock(return_value=repaired)
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    'Crow said: “I never agreed to the payment.”',
                    prompt="Current user request: exact payment dispute",
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text=(
                        "what exactly did Crow say about payment in this dispute?"
                    ),
                    channel=channel,
                    exact_quote_requested=True,
                    exact_quote_authority=authority,
                )
            )
        self.assertEqual(response, repaired)
        self.assertTrue(diagnostics["exact_quote_guard_triggered"])
        self.assertTrue(diagnostics["exact_quote_regenerated"])
        provider.assert_awaited_once()

    async def test_ordinary_attribution_guard_regenerates_quote_to_gist(self):
        provider = mock.AsyncMock(
            return_value=(
                "Crow's point was that the border should carry more visual weight."
            )
        )
        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            provider,
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    'Crow said “make the border red.”',
                    prompt="Current user request: what did Crow mean?",
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text="what did Crow mean about the poster?",
                    third_party_attribution_requested=True,
                )
            )
        self.assertEqual(
            response,
            "Crow's point was that the border should carry more visual weight.",
        )
        self.assertTrue(diagnostics["exact_quote_guard_triggered"])
        self.assertTrue(diagnostics["exact_quote_regenerated"])
        provider.assert_awaited_once()

    async def test_guard_suppresses_when_source_disappears_during_retry(self):
        row_id = self.insert_source(
            "I agreed to send the payment after the final mix was approved."
        )
        authority = self.authority()
        channel = self.live_channel()
        repaired = (
            'Crow said: “I agreed to send the payment after the final mix '
            'was approved.”'
        )

        async def delete_then_return(*_args, **_kwargs):
            with sqlite3.connect(bnl01_bot.DB_FILE) as conn:
                conn.execute(
                    "DELETE FROM conversations WHERE id=?",
                    (row_id,),
                )
            return repaired

        with mock.patch.object(
            bnl01_bot,
            "get_gemini_response_with_optional_typing",
            mock.AsyncMock(side_effect=delete_then_return),
        ):
            response, diagnostics = (
                await bnl01_bot.apply_guarded_response_regeneration(
                    'Crow said: “I never agreed to the payment.”',
                    prompt="Current user request: exact payment dispute",
                    user_id=101,
                    guild_id=1,
                    route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                    channel_policy="public_home",
                    current_user_text=(
                        "what exactly did Crow say about payment in this dispute?"
                    ),
                    channel=channel,
                    exact_quote_requested=True,
                    exact_quote_authority=authority,
                )
            )
        self.assertEqual(response, "")
        self.assertTrue(diagnostics["exact_quote_basis_stale"])
        self.assertEqual(
            diagnostics["suppression_reason"],
            "exact_quote_basis_changed_before_send",
        )

    async def test_stale_live_authority_suppresses_even_labeled_gist(self):
        self.insert_source(
            "I agreed to send the payment after the final mix was approved."
        )
        authority = self.authority()
        for mode in ("edited", "deleted"):
            with self.subTest(mode=mode):
                channel = self.live_channel()
                if mode == "edited":
                    channel.content = "I withdrew that payment statement."
                else:
                    channel.deleted = True
                response, diagnostics = (
                    await bnl01_bot.apply_guarded_response_regeneration(
                        "The gist is that Crow planned to pay after approval.",
                        prompt="Prompt already contained exact authority.",
                        user_id=101,
                        guild_id=1,
                        route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                        channel_policy="public_home",
                        current_user_text=(
                            "what exactly did Crow say about payment "
                            "in this dispute?"
                        ),
                        channel=channel,
                        exact_quote_requested=True,
                        exact_quote_authority=authority,
                    )
                )
                self.assertEqual(response, "")
                self.assertTrue(diagnostics["exact_quote_basis_stale"])
                self.assertEqual(
                    diagnostics["suppression_reason"],
                    "exact_quote_basis_stale_before_guard",
                )

    async def test_live_edit_or_delete_during_retry_suppresses_labeled_gist(self):
        self.insert_source(
            "I agreed to send the payment after the final mix was approved."
        )
        authority = self.authority()
        for mode in ("edited", "deleted"):
            with self.subTest(mode=mode):
                channel = self.live_channel()

                async def mutate_then_paraphrase(*_args, **_kwargs):
                    if mode == "edited":
                        channel.content = "I withdrew that payment statement."
                    else:
                        channel.deleted = True
                    return (
                        "The gist is that Crow planned to pay after approval."
                    )

                with mock.patch.object(
                    bnl01_bot,
                    "get_gemini_response_with_optional_typing",
                    mock.AsyncMock(side_effect=mutate_then_paraphrase),
                ):
                    response, diagnostics = (
                        await bnl01_bot.apply_guarded_response_regeneration(
                            'Crow said: “I never agreed to the payment.”',
                            prompt="Prompt contained exact authority.",
                            user_id=101,
                            guild_id=1,
                            route_mode=bnl01_bot.ROUTE_MODE_NORMAL_CHAT,
                            channel_policy="public_home",
                            current_user_text=(
                                "what exactly did Crow say about payment "
                                "in this dispute?"
                            ),
                            channel=channel,
                            exact_quote_requested=True,
                            exact_quote_authority=authority,
                        )
                    )
                self.assertEqual(response, "")
                self.assertTrue(diagnostics["exact_quote_basis_stale"])
                self.assertEqual(
                    diagnostics["suppression_reason"],
                    "exact_quote_basis_changed_before_send",
                )


if __name__ == "__main__":
    unittest.main()
