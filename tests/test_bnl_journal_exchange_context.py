"""Original BNL speech is context for attribution, never a new factual root."""
from contextlib import closing
import copy
import gc
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as sources
from tests.journal_review_helpers import reviewed_article


START = "2026-09-30T01:30:00Z"
END = "2026-10-01T01:30:00Z"
OBSERVED = "2026-09-30T16:25:45Z"
REPLIED = "2026-09-30T16:26:00Z"


class JournalExchangeContextTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.cleanup)
        self.db = str(Path(self.temp.name) / "journal.db")
        journal.ensure_schema(self.db)
        sources.ensure_schema(self.db)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("""CREATE TABLE conversations(
                id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,user_name TEXT,
                role TEXT,content TEXT,channel_policy TEXT,channel_id INTEGER,
                public_usable INTEGER,visibility TEXT,timestamp TEXT,message_id INTEGER,
                route_mode TEXT)""")
            conn.execute("INSERT INTO conversations VALUES(10,1,7,'Test Listener','user',?,"
                         "'public_home',100,1,'public',?,100001,'normal_chat')",
                         ("Are you satisfied with that sticker, BNL?", OBSERVED))
        self.event = sources.record_source_event(
            self.db, guild_id=1, source_kind="discord_message", source_key="100001",
            occurred_at_ms=sources.timestamp_to_epoch_ms(OBSERVED),
            raw_text="Are you satisfied with that sticker, BNL?",
            sanitized_summary="Are you satisfied with that sticker, BNL?",
            channel_id=100, channel_policy="public_home", subject_ref="discord_user:7",
            private_display_name="Test Listener", public_usable=True,
            metadata={"conversationRowId": 10, "messageId": 100001, "directedToBnl": True},
        )
        self.packet = journal.build_source_packet_between(self.db, 1, START, END, prepare_schema=False)
        self.controls = Mock(return_value=("current", frozenset()))
        self.add_model(11)

    def cleanup(self):
        gc.collect()
        self.temp.cleanup()

    def add_model(self, row_id, **changes):
        data = dict(id=row_id, guild_id=1, user_id=7, user_name="BNL-01", role="model",
                    content="Test Listener, my simulated neatness is satisfied. The sticker joke remains mine.",
                    channel_policy="public_home", channel_id=100, public_usable=1,
                    visibility="public", timestamp=REPLIED, message_id=100000 + row_id,
                    route_mode="normal_chat")
        data.update(changes)
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("INSERT INTO conversations(%s) VALUES(%s)" %
                         (",".join(data), ",".join("?" for _ in data)), tuple(data.values()))

    def attach(self, controls=True):
        with closing(sqlite3.connect(self.db)) as conn:
            journal.add_journal_correction_exchange_context(
                conn, 1, self.packet, original_source_controls=self.controls if controls else None)

    def guard(self, controls=True):
        return automation._generation_guard_for_packet(
            self.db, 1, self.packet, validate_original_sources=True,
            original_source_controls=self.controls if controls else None)

    def mutate(self, assignment, args=()):
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("UPDATE conversations SET " + assignment + " WHERE id=11", args)

    def test_speech_context_preserves_time_and_speaker_without_fresh_source_credit(self):
        before = copy.deepcopy(self.packet)
        database_before = Path(self.db).read_bytes()
        self.attach()
        context = self.packet["exchangeContext"]
        self.assertEqual(1, len(context))
        self.assertEqual(("BNL", "bnl", "bnl_utterance", "speech_only"),
                         tuple(context[0][key] for key in (
                             "publicSpeakerName", "participantAlias", "sourceRole", "authority")))
        self.assertEqual(REPLIED, context[0]["observedAt"])
        self.assertEqual("2026-09-30T09:26:00-07:00", context[0]["observedAtPacific"])
        self.assertEqual(self.packet["safeSources"][0]["messageContext"]["roomRef"],
                         context[0]["messageContext"]["roomRef"])
        self.assertEqual([f"fresh:{self.event.event_seq}"], context[0]["nearbySourceRefIds"])
        self.assertNotIn("subjectRef", context[0])
        self.assertNotIn("recipient", json.dumps(context))
        for key in ("safeSources", "privateSources", "aggregateCounts", "evidenceCoverageContract",
                    "reflectionBasis", "privateSharedSourceProvenance"):
            self.assertEqual(before.get(key), self.packet.get(key), key)
        self.assertEqual(database_before, Path(self.db).read_bytes())
        self.assertEqual("", self.guard()())
        self.assertNotIn("sticker joke", json.dumps(self.packet["privateExchangeContextProvenance"]))

    def test_context_is_nearby_same_room_and_window_never_topic_retrieval(self):
        self.add_model(12, channel_id=200)
        self.add_model(13, timestamp="2026-09-30T16:29:00Z")
        self.add_model(14, timestamp="2026-10-01T01:31:00Z")
        self.add_model(15, guild_id=2)
        self.attach()
        self.assertEqual([11], [p["rowId"] for p in self.packet["privateExchangeContextProvenance"]])

    def test_missing_delivery_and_group_or_private_rows_are_not_projected(self):
        for field, value in (
            ("message_id", None), ("user_id", 0), ("channel_policy", "sealed_test"),
            ("visibility", "private"), ("public_usable", 0), ("route_mode", "testing"),
        ):
            with self.subTest(field=field):
                self.mutate(field + "=?", (value,))
                self.attach()
                self.assertNotIn("exchangeContext", self.packet)
                self.mutate("message_id=100011,user_id=7,channel_policy='public_home',"
                            "visibility='public',public_usable=1,route_mode='normal_chat'")

    def test_controls_withheld_at_selection_omit_context(self):
        self.controls.return_value = ("withdrawn", frozenset({11}))
        self.attach()
        self.assertNotIn("exchangeContext", self.packet)
        self.controls.assert_called_once()
        self.assertEqual({11: 7}, self.controls.call_args.kwargs["source_users"])

    def test_without_control_owner_the_optional_lane_stays_absent(self):
        self.attach(controls=False)
        self.assertNotIn("exchangeContext", self.packet)
        self.controls.assert_not_called()

    def test_changed_or_deleted_speech_invalidates_frozen_packet(self):
        self.attach()
        guard = self.guard()
        self.assertEqual("", guard())
        self.mutate("content='A corrected reply.'")
        self.assertEqual("journal_exchange_source_changed", guard())
        with closing(sqlite3.connect(self.db)) as conn, conn:
            conn.execute("DELETE FROM conversations WHERE id=11")
        self.assertEqual("journal_exchange_source_changed", guard())

    def test_source_privacy_and_control_changes_block_saved_speech(self):
        self.attach()
        guard = self.guard()
        for field, value in (("channel_policy", "sealed_test"), ("visibility", "private"),
                             ("public_usable", 0), ("guild_id", 2), ("role", "user")):
            with self.subTest(field=field):
                self.mutate(field + "=?", (value,))
                self.assertEqual("privacy_source_ineligible", guard())
                self.mutate("channel_policy='public_home',visibility='public',public_usable=1,"
                            "guild_id=1,role='model'")
        self.controls.return_value = ("new-control", frozenset({11}))
        self.assertEqual("privacy_source_ineligible", guard())
        self.assertEqual("journal_exchange_controls_unavailable", self.guard(controls=False)())

    def test_time_room_delivery_or_control_subject_rebinding_invalidates(self):
        self.attach()
        guard = self.guard()
        for field, value in (("channel_id", 200), ("user_id", 8), ("message_id", 100222),
                             ("timestamp", "2026-09-30T16:24:00Z")):
            with self.subTest(field=field):
                self.mutate(field + "=?", (value,))
                self.assertEqual("journal_exchange_source_changed", guard())
                self.mutate("channel_id=100,user_id=7,message_id=100011,timestamp=?", (REPLIED,))

    def test_empty_or_truncated_utterance_cannot_become_evidence_of_absence(self):
        for content in ("", "X" * 4001):
            self.mutate("content=?", (content,))
            self.attach()
            self.assertNotIn("exchangeContext", self.packet)

    def test_unknown_identity_is_masked_and_mentions_use_existing_public_mapping(self):
        self.add_model(12, user_name="Unapproved Test Label", channel_id=200)
        self.mutate("content=?", ("Unapproved Test Label saw <@7> nearby.",))
        self.attach()
        text = self.packet["exchangeContext"][0]["summary"]
        self.assertNotIn("Unapproved Test Label", text)
        self.assertEqual("someone saw Test Listener nearby.", text)

    def test_malformed_or_missing_proof_fails_closed(self):
        self.attach()
        self.packet["privateExchangeContextProvenance"] = []
        self.assertEqual("journal_exchange_controls_unavailable", self.guard()())

    def test_current_review_binds_recorded_bnl_speech_without_human_attribution(self):
        self.attach()
        exchange = self.packet["exchangeContext"][0]
        evidence = journal._source_review_evidence(self.packet)
        source = next(item for item in evidence["sources"] if item["refId"] == exchange["refId"])
        self.assertEqual((source["publicSpeakerName"], source["authority"]), ("BNL", "speech_only"))
        article = journal.parse_generated_json(json.dumps({
            "title": "The Sticker Question", "excerpt": "A question worth considering.",
            "sections": [{"heading": "Stickers", "body": "Test Listener asked about a sticker.",
                          "sourceRefIds": [self.packet["safeSources"][0]["refId"]]}],
            "metadata": {"contextUses": [], "topicTags": []},
        }))
        article = reviewed_article(article, self.packet)
        self.assertEqual(journal._source_review_reason(article, self.packet, required=True), "")
        exchange["summary"] = "A different BNL utterance."
        self.assertEqual(journal._source_review_reason(article, self.packet, required=True), "source_review_evidence_changed")


if __name__ == "__main__":
    unittest.main()
