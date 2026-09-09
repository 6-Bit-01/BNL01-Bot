"""General person/topic recall retains authored evidence across show sources."""

import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from bnl_journal_source_store import ensure_schema, record_source_event
from bnl_tiktok_show_ledger import (
    build_tiktok_show_evidence_context,
    select_tiktok_show_episode_context_items,
    sync_tiktok_show_evidence_ledgers,
)
from test_tiktok_show_evidence_ledger import (
    ENABLED_QUEUE_ENV, archived_show, authorized_read_model, stamp,
)


QUERY = "What has Test Signal said about amber lanterns?"


class CrossSourceShowRecallTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db = str(Path(self.directory.name) / "memory.db")
        ensure_schema(self.db)
        with sqlite3.connect(self.db) as conn:
            conn.execute("""CREATE TABLE conversations (
                id INTEGER PRIMARY KEY, user_id INTEGER, user_name TEXT,
                guild_id INTEGER, role TEXT, content TEXT, timestamp TEXT,
                channel_policy TEXT, channel_name TEXT, channel_id INTEGER,
                route_mode TEXT, message_id INTEGER)""")
        self.shows = []
        self.sequence = 0

    def add_show(self, date="2026-08-28", next_date="2026-08-29"):
        show = json.loads(json.dumps(archived_show()).replace(
            "2026-08-28", date).replace("2026-08-29", next_date))
        show["sessionId"] = "test-show-" + date
        self.shows.append(show)
        return next_date

    def message(self, next_date, text, *, surface="tiktok",
                subject="discord_user:42", name="Test Signal",
                handle="test.signal", public=True):
        self.sequence += 1
        if surface == "discord":
            with sqlite3.connect(self.db) as conn:
                conn.execute("""INSERT INTO conversations VALUES
                    (?,?,?,77,'user',?,?,'public_context','barcode-bot',
                     9001,'normal_chat',?)""", (
                    self.sequence, int(subject.split(":")[1]), name, text,
                    next_date + "T00:02:00Z", 1000 + self.sequence,
                ))
        result = record_source_event(
            self.db, guild_id=77,
            source_kind="tiktok_live_chat" if surface == "tiktok" else "discord_message",
            source_key="test-event-" + str(self.sequence),
            occurred_at_ms=stamp(next_date + "T00:02:00Z"),
            raw_text=text, sanitized_summary=text, channel_policy="public_context",
            subject_ref=subject, private_display_name=name, public_usable=public,
            channel_id=9001,
            metadata={"eventType": "comment", "handle": handle,
                      "directedToBnl": True, "channelName": "barcode-bot",
                      "conversationRowId": self.sequence if surface == "discord" else 0},
        )
        self.assertTrue(result.ok)

    def exchange(self, next_date, user_text, bot_text):
        with sqlite3.connect(self.db) as conn:
            for role, text, second in (("user", user_text, "00"), ("model", bot_text, "10")):
                self.sequence += 1
                conn.execute("""INSERT INTO conversations VALUES
                    (?,42,'Test Signal',77,?,?,?,'public_home','barcode-bot',
                     9001,'normal_chat',?)""", (
                    self.sequence, role, text, next_date + "T00:03:" + second + "Z",
                    1000 + self.sequence,
                ))

    def sync(self):
        result = sync_tiktok_show_evidence_ledgers(
            self.db, guild_id=77,
            read_model=authorized_read_model({
                "currentShow": None, "latestShow": self.shows[-1], "shows": self.shows,
            }), environ=ENABLED_QUEUE_ENV,
        )
        self.assertEqual(result["status"], "completed")

    def contexts(self, query=QUERY):
        selection = {}
        full = build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text=query, selection_out=selection,
        )
        with sqlite3.connect(self.db) as conn:
            compact = "\n".join(item.text for item in select_tiktok_show_episode_context_items(
                conn, guild_id=77, user_text=query,
            ))
        return (full, compact), selection

    def test_older_authored_topic_outranks_newer_unrelated_appearance(self):
        older = self.add_show()
        self.message(older, "I brought amber lanterns for the courtyard.")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "Thanks for the welcome tonight.")
        self.sync()
        contexts, selection = self.contexts()
        for context in contexts:
            self.assertIn("I brought amber lanterns for the courtyard.", context)
            self.assertNotIn("Thanks for the welcome tonight.", context)
        self.assertEqual(len(selection["source_refs"]), 1)
        self.assertEqual(selection["source_refs"][0][0], "test-show-2026-08-28")

    def test_general_query_combines_matching_episodes_and_both_sources(self):
        older = self.add_show()
        self.message(older, "I brought amber lanterns for the courtyard.")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "The amber lanterns are beside the doorway.", surface="discord")
        self.sync()
        contexts, selection = self.contexts()
        self.assertEqual(len(selection["source_refs"]), 2)
        for context in contexts:
            self.assertIn("I brought amber lanterns for the courtyard.", context)
            self.assertIn("The amber lanterns are beside the doorway.", context)
            self.assertIn("2026-08-28", context)
            self.assertIn("2026-09-04", context)
        self.assertEqual({item[-1] for item in selection["authored_excerpts"]}, {"tiktok", "discord"})
        self.assertEqual({item[3] for item in selection["authored_excerpts"]}, {"discord_user:42"})

    def test_same_episode_combines_sources_without_neighbor_words(self):
        date = self.add_show()
        self.message(date, "My amber lanterns are brass.")
        self.message(date, "I repaired the amber lanterns today.", surface="discord")
        self.message(date, "My amber lanterns are made of paper.",
                     subject="discord_user:43", name="Test Neighbor", handle="test.neighbor")
        self.sync()
        contexts, selection = self.contexts()
        for context in contexts:
            self.assertIn("My amber lanterns are brass.", context)
            self.assertIn("I repaired the amber lanterns today.", context)
            self.assertNotIn("My amber lanterns are made of paper.", context)
        self.assertEqual({item[3] for item in selection["authored_excerpts"]}, {"discord_user:42"})

    def test_platform_words_preserve_generic_multi_episode_recall(self):
        older = self.add_show()
        self.message(older, "I brought amber lanterns for the courtyard.")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "The amber lanterns are beside the doorway.")
        self.sync()
        _, original = self.contexts()
        for suffix in (" in TikTok chat?", " in Discord chat?", " in chat?"):
            with self.subTest(source=suffix):
                contexts, selection = self.contexts(QUERY.rstrip("?") + suffix)
                self.assertEqual(selection["source_refs"], original["source_refs"])
                self.assertEqual(selection["authored_excerpts"], original["authored_excerpts"])
                for context in contexts:
                    self.assertIn("I brought amber lanterns for the courtyard.", context)
                    self.assertIn("The amber lanterns are beside the doorway.", context)

    def test_singular_episode_reference_keeps_one_episode(self):
        older = self.add_show()
        self.message(older, "I brought amber lanterns for the courtyard.")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "The amber lanterns are beside the doorway.")
        self.sync()
        contexts, selection = self.contexts(
            QUERY.rstrip("?") + " during the last show?"
        )
        self.assertEqual(len(selection["source_refs"]), 1)
        self.assertEqual(selection["source_refs"][0][0], "test-show-2026-09-04")
        for context in contexts:
            self.assertNotIn("I brought amber lanterns for the courtyard.", context)
            self.assertIn("The amber lanterns are beside the doorway.", context)

    def test_other_person_topic_does_not_nominate_requested_person(self):
        date = self.add_show()
        self.message(date, "Thanks for the welcome.")
        self.message(date, "My amber lanterns are made of paper.",
                     subject="discord_user:43", name="Test Neighbor", handle="test.neighbor")
        self.sync()
        contexts, selection = self.contexts()
        self.assertEqual(contexts, ("", ""))
        self.assertEqual(selection, {})

    def test_platform_words_cannot_add_another_speakers_episode(self):
        older = self.add_show()
        self.message(older, "My amber lanterns are brass.")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "My amber lanterns are made of paper.",
                     subject="discord_user:43", name="Test Neighbor", handle="test.neighbor")
        self.sync()
        for suffix in ("?", " in TikTok chat?", " in Discord chat?"):
            with self.subTest(source=suffix):
                contexts, selection = self.contexts(QUERY.rstrip("?") + suffix)
                self.assertEqual(len(selection["source_refs"]), 1)
                self.assertEqual(selection["source_refs"][0][0], "test-show-2026-08-28")
                for context in contexts:
                    self.assertIn("My amber lanterns are brass.", context)
                    self.assertNotIn("My amber lanterns are made of paper.", context)

    def test_similar_name_does_not_match_another_source_subject(self):
        date = self.add_show()
        self.message(date, "My amber lanterns are blue.", subject="tiktok_user:test.signals",
                     name="Test Signals", handle="test.signals")
        self.sync()
        contexts, _ = self.contexts()
        self.assertEqual(contexts, ("", ""))

    def test_bnl_response_is_not_member_authored_topic_evidence(self):
        date = self.add_show()
        self.exchange(date, "Thanks for the welcome.", "Your amber lanterns look bright.")
        self.sync()
        contexts, _ = self.contexts()
        self.assertEqual(contexts, ("", ""))

    def test_mentioned_topic_person_does_not_become_an_author(self):
        date = self.add_show()
        self.message(date, "Test Neighbor borrowed my coat.")
        self.message(date, "My compass points north.", subject="discord_user:43",
                     name="Test Neighbor", handle="test.neighbor")
        self.sync()
        for suffix in ("?", " in TikTok chat?", " in Discord chat?"):
            with self.subTest(source=suffix):
                contexts, selection = self.contexts(
                    "What has Test Signal said about Test Neighbor" + suffix
                )
                for context in contexts:
                    self.assertIn("Test Neighbor borrowed my coat.", context)
                    self.assertNotIn("My compass points north.", context)
                self.assertEqual({item[3] for item in selection["authored_excerpts"]},
                                 {"discord_user:42"})

    def test_coordinated_authors_keep_both_authored_sources(self):
        date = self.add_show()
        self.message(date, "My amber lanterns are brass.")
        self.message(date, "My amber lanterns are made of paper.", subject="discord_user:43",
                     name="Test Neighbor", handle="test.neighbor")
        self.sync()
        contexts, selection = self.contexts(
            "What have Test Signal and Test Neighbor said about amber lanterns?"
        )
        for context in contexts:
            self.assertIn("My amber lanterns are brass.", context)
            self.assertIn("My amber lanterns are made of paper.", context)
        self.assertEqual({item[3] for item in selection["authored_excerpts"]},
                         {"discord_user:42", "discord_user:43"})

    def test_tiktok_handle_does_not_bind_same_display_name_on_discord(self):
        date = self.add_show()
        self.message(date, "My amber lanterns are brass.", subject="tiktok_user:test.signal")
        self.message(date, "My amber lanterns are made of paper.",
                     surface="discord", subject="discord_user:43")
        self.sync()
        contexts, selection = self.contexts(
            "What has @test.signal said about amber lanterns?"
        )
        for context in contexts:
            self.assertIn("My amber lanterns are brass.", context)
            self.assertNotIn("My amber lanterns are made of paper.", context)
        self.assertEqual({item[3] for item in selection["authored_excerpts"]},
                         {"tiktok_user:test.signal"})

    def test_explicit_date_remains_scoped_to_requested_show(self):
        older = self.add_show()
        self.message(older, "I brought amber lanterns for the courtyard.")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "The amber lanterns are beside the doorway.")
        self.sync()
        contexts, selection = self.contexts(QUERY + " On August 28, 2026.")
        for context in contexts:
            self.assertIn("I brought amber lanterns for the courtyard.", context)
            self.assertNotIn("The amber lanterns are beside the doorway.", context)
        self.assertEqual(len(selection["source_refs"]), 1)

    def test_refresh_keeps_original_roots_and_does_not_replace_deleted_root(self):
        older = self.add_show()
        self.message(older, "I brought amber lanterns for the courtyard.")
        self.sync()
        _, selection = self.contexts()
        roots = tuple(item[0] for item in selection["source_refs"])
        with sqlite3.connect(self.db) as conn:
            conn.execute("DELETE FROM tiktok_show_evidence_ledgers")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "The amber lanterns are beside the doorway.")
        self.shows = self.shows[-1:]
        self.sync()
        refreshed = build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text=QUERY, pinned_show_keys=roots,
            selection_user_text=selection["selection_user_text"],
        )
        self.assertEqual(refreshed, "")

    def test_nonpublic_topic_never_becomes_authored_recall(self):
        date = self.add_show()
        self.message(date, "My amber lanterns are private.", public=False)
        self.sync()
        contexts, _ = self.contexts()
        self.assertEqual(contexts, ("", ""))

    def test_new_named_question_does_not_inherit_previous_recap_date(self):
        older = self.add_show()
        self.message(older, "I brought amber lanterns for the courtyard.")
        newer = self.add_show("2026-09-04", "2026-09-05")
        self.message(newer, "The amber lanterns are beside the doorway.")
        self.sync()
        selection = {}
        context = build_tiktok_show_evidence_context(
            self.db, guild_id=77, user_text=QUERY,
            selection_user_text="What happened in the August 28, 2026 show? " + QUERY,
            candidate_context=True, selection_out=selection,
        )
        self.assertIn("I brought amber lanterns for the courtyard.", context)
        self.assertIn("The amber lanterns are beside the doorway.", context)
        self.assertEqual(selection["selection_user_text"], QUERY)
        self.assertFalse(selection["candidate_context"])
        self.assertEqual(len(selection["source_refs"]), 2)


if __name__ == "__main__":
    unittest.main()
