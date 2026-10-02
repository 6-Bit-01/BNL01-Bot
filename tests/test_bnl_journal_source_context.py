"""Original message context survives projection without promoting BNL retellings."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl_journal as journal
import bnl_journal_automation as automation
import bnl_journal_source_store as sources


START = "2026-10-01T00:00:00Z"
END = "2026-10-02T00:00:00Z"
ROOM_ONE = 223456789012345671
ROOM_TWO = 223456789012345672


class JournalSourceContextTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = str(Path(directory.name) / "journal.db")
        journal.ensure_schema(self.db)
        sources.ensure_schema(self.db)
        self.next_message = 0
        self.next_relay = 0
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE conversations("
                         "id INTEGER PRIMARY KEY,user_id INTEGER,user_name TEXT,guild_id INTEGER,"
                         "channel_id INTEGER,channel_name TEXT,channel_policy TEXT,role TEXT,"
                         "content TEXT,timestamp TEXT,public_usable INTEGER,visibility TEXT)")
            conn.execute("CREATE TABLE website_relay_history("
                         "relay_id TEXT PRIMARY KEY,guild_id INTEGER,public_message TEXT,"
                         "public_directive TEXT,event_type TEXT,published_timestamp TEXT,source_basis_json TEXT)")

    def message(self, text, *, room=ROOM_ONE, room_name="music-room", policy="public_home",
                public=True, visibility="public_safe"):
        self.next_message += 1
        index = self.next_message
        observed = "2026-10-01T15:00:%02dZ" % index
        with sqlite3.connect(self.db) as conn:
            conn.execute("INSERT INTO conversations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", (
                index, 71, "Member Alpha", 1, room, room_name, policy, "user", text,
                observed, int(public), visibility,
            ))
        result = sources.record_source_event(
            self.db, guild_id=1, source_kind="discord_message", source_key=str(index),
            occurred_at_ms=sources.timestamp_to_epoch_ms(observed), raw_text=text,
            sanitized_summary=sources.sanitize_summary(text, ["Member Alpha"]),
            channel_id=room, channel_policy=policy, subject_ref="discord_user:71",
            private_display_name="Member Alpha", public_usable=public,
            metadata={"messageId": index, "channelName": room_name},
        )
        self.assertTrue(result.ok, result.reason)
        return result

    def relay(self, *, event_type="fresh_public_discord_activity", origins=None):
        self.next_relay += 1
        key = "relay-%s" % self.next_relay
        message = "An audio link prompted questions about a silver antenna sound."
        invitation = "Check its creator and origin before drawing conclusions."
        observed = "2026-10-01T16:00:%02dZ" % self.next_relay
        with sqlite3.connect(self.db) as conn:
            conn.execute("INSERT INTO website_relay_history VALUES(?,?,?,?,?,?,?)", (
                key, 1, message, invitation, event_type, observed, json.dumps(origins or []),
            ))
        result = sources.record_source_event(
            self.db, guild_id=1, source_kind="website_relay", source_key=key,
            occurred_at_ms=sources.timestamp_to_epoch_ms(observed),
            raw_text=message + "\n" + invitation,
            sanitized_summary=message + " " + invitation,
            channel_policy="public_relay", subject_ref="bnl_01", private_display_name="BNL",
            public_usable=True, metadata={"event_type": event_type},
        )
        self.assertTrue(result.ok, result.reason)
        return key, result, message, invitation

    def packet(self, path="archive"):
        if path == "archive":
            return journal.build_source_packet_between(
                self.db, 1, START, END, entry_kind="daily", prepare_schema=False,
            )
        with sqlite3.connect(self.db) as conn:
            relays = journal.accepted_relays(conn, 1, START, END)
            conversations = journal.public_conversations(conn, 1, START, END)
        return journal.build_packet_from_sources(
            self.db, 1, START, END, relays, conversations, entry_kind="daily", prepare_schema=False,
        )

    def public_sources(self, path="archive"):
        return [item for item in self.packet(path)["safeSources"]
                if item.get("sourceKind") == "conversation"]

    def test_archive_and_legacy_preserve_same_room_and_distinct_room_identity(self):
        self.message("A wavering bass line makes the arrangement feel alive.")
        self.message("The little pause before the chorus is the part I keep returning to.")
        self.message("A different room compared the shimmering percussion.", room=ROOM_TWO, room_name="listening-room")
        room_refs = []
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                selected = self.public_sources(path)
                self.assertEqual(3, len(selected))
                contexts = [source["messageContext"] for source in selected]
                self.assertTrue(contexts[0]["roomRef"])
                self.assertEqual(contexts[0]["roomRef"], contexts[1]["roomRef"])
                self.assertNotEqual(contexts[0]["roomRef"], contexts[2]["roomRef"])
                self.assertEqual("music-room", contexts[0]["roomName"])
                self.assertEqual("listening-room", contexts[2]["roomName"])
                self.assertNotIn(str(ROOM_ONE), json.dumps(selected))
                self.assertNotIn(str(ROOM_TWO), json.dumps(selected))
                room_refs.append(contexts[0]["roomRef"])
        self.assertEqual(room_refs[0], room_refs[1])

    def test_unknown_room_does_not_invent_a_room_identity_or_label(self):
        self.message("That detuned chord belongs in the final mix.", room=0, room_name="")
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                context = self.public_sources(path)[0]["messageContext"]
                self.assertFalse(context.get("roomRef"))
                self.assertFalse(context.get("roomName"))

    def test_public_room_label_is_allowed_but_opaque_room_ref_cannot_leak(self):
        self.message("A wavering bass line makes the arrangement feel alive.")
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                packet = self.packet(path)
                context = packet["safeSources"][0]["messageContext"]
                article = {
                    "title": "The Shape of a Listening Room",
                    "excerpt": "A place for unfinished musical questions.",
                    "sections": [{"heading": "A little room to listen",
                                  "body": "I like the name " + context["roomName"] + "."}],
                }
                self.assertEqual("", journal._article_privacy_reason(article, packet))
                for field in ("title", "excerpt", "heading", "body"):
                    with self.subTest(field=field):
                        exposed = json.loads(json.dumps(article))
                        target = exposed if field in {"title", "excerpt"} else exposed["sections"][0]
                        target[field] += " " + context["roomRef"]
                        self.assertEqual("public_leak_pattern",
                                         journal._article_privacy_reason(exposed, packet))

    def test_shared_link_keeps_position_and_original_quotes_without_link_content_claims(self):
        self.message('Before https://media.example.test/listen?private_token=abcdef after: "silver hinge" is my phrase.')
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                source = self.public_sources(path)[0]
                self.assertEqual('Before [shared link] after: "silver hinge" is my phrase.', source["summary"])
                context = source["messageContext"]
                self.assertEqual("not_inspected", context["linkContent"])
                self.assertEqual("original_message_not_linked_content", context["authority"])
                self.assertFalse(context["textTruncated"])
                for private_link_part in ("https://", "media.example.test", "private_token", "abcdef"):
                    self.assertNotIn(private_link_part, json.dumps(source))

    def test_link_only_message_survives_both_original_message_paths(self):
        self.message("https://media.example.test/only?private_token=abcdef")
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                selected = self.public_sources(path)
                self.assertEqual(1, len(selected))
                self.assertEqual("[shared link]", selected[0]["summary"])
                self.assertEqual("not_inspected", selected[0]["messageContext"]["linkContent"])
                self.assertNotIn("example.test", json.dumps(selected))

    def test_long_original_message_is_flagged_when_public_projection_is_bounded(self):
        self.message("A subtle percussion detail deserves attention. " * 30)
        self.message('The producer called the sound "crooked sunshine" and kept it.')
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                first, second = self.public_sources(path)
                self.assertLessEqual(len(first["summary"]), 1000)
                self.assertTrue(first["messageContext"]["textTruncated"])
                self.assertFalse(second["messageContext"]["textTruncated"])
                self.assertIn('"crooked sunshine"', second["summary"])
                self.assertNotEqual("not_inspected", second["messageContext"].get("linkContent"))

    def test_room_labels_and_message_text_use_the_same_identity_and_identifier_projection(self):
        with sqlite3.connect(self.db) as conn:
            conn.execute("CREATE TABLE user_profiles(guild_id INTEGER,display_name TEXT)")
            conn.execute("INSERT INTO user_profiles VALUES(1,'Private Alias')")
        self.message("Private Alias said the synthesizer needed more air. <@323456789012345678>",
                     room_name="Private Alias <@323456789012345678> https://private.example.test/room")
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                source = self.public_sources(path)[0]
                projected = json.dumps(source)
                for literal in ("Private Alias", "323456789012345678", str(ROOM_ONE), "private.example.test", "https://"):
                    self.assertNotIn(literal, projected)
                self.assertIn("someone", source["summary"])
                self.assertIn("someone", source["messageContext"]["roomName"])

    def test_sealed_or_disallowed_messages_cannot_gain_public_link_or_room_context(self):
        self.message("A public producer compared soft percussion.")
        self.message("https://sealed.example.test/secret", room=ROOM_TWO, room_name="sealed-room",
                     policy="sealed_test", public=False, visibility="sealed_test")
        self.message("https://private.example.test/secret", room=ROOM_TWO, room_name="private-room",
                     policy="public_home", public=False, visibility="private")
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                selected = self.public_sources(path)
                self.assertEqual(1, len(selected))
                projected = json.dumps(selected)
                self.assertNotIn("sealed-room", projected)
                self.assertNotIn("private-room", projected)
                self.assertNotIn("[shared link]", projected)

    def test_every_relay_is_continuity_and_never_fresh_evidence_in_both_paths(self):
        recorded = [self.relay(event_type=event_type) for event_type in (
            "fresh_public_discord_activity", "public_moment", "new_relay_class",
        )]
        self.message("An artist described a wavering synthesizer arrangement.")
        before = hashlib.sha256(Path(self.db).read_bytes()).hexdigest()
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                packet = self.packet(path)
                self.assertTrue(packet["safeSources"])
                self.assertTrue(all(source.get("sourceKind") != "relay" for source in packet["safeSources"]))
                basis = [item for item in packet["reflectionBasis"]
                         if item.get("basisKind") == "accepted_relay_continuity"]
                self.assertEqual(len(recorded), len(basis))
                self.assertTrue(all(item["refId"].startswith("reflection:") for item in basis))
                self.assertTrue(all(item["relaySpeech"]["publicInvitation"] == recorded[0][3] for item in basis))
                provenance = packet["privateReflectionBasisProvenance"]["historicalSourceEvents"]
                self.assertEqual({row[0] for row in recorded},
                                 {item["sourceKey"] for item in provenance if item.get("sourceKind") == "website_relay"})
                self.assertTrue(all(item["originalRefId"].startswith("fresh:") for item in provenance
                                    if item.get("sourceKind") == "website_relay"))
                prompt = journal.build_generation_prompt(packet)
                safe = json.loads(prompt.split("Generation-safe packet:\n", 1)[1])
                self.assertTrue(all(item.get("sourceKind") != "relay" for item in safe["freshSources"]))
        self.assertEqual(before, hashlib.sha256(Path(self.db).read_bytes()).hexdigest())

    def test_relay_publication_date_does_not_replace_its_remembered_source_date(self):
        self.relay(event_type="fresh_public_discord_activity", origins=[{
            "sourceKind": "public_moment", "sourceId": "historical-moment", "sourceVersion": "original-v1",
            "startedAt": "2026-05-28T10:00:00Z", "observedAt": "2026-05-28T10:02:00Z",
        }])
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                basis = next(item for item in self.packet(path)["reflectionBasis"]
                             if item.get("basisKind") == "accepted_relay_continuity")
                self.assertTrue(basis["relayPublishedAt"].startswith("2026-10-01"))
                self.assertEqual("2026-05-28T10:02:00Z", basis["originalSourceDates"][0]["observedAt"])
                self.assertEqual("2026-05-28T10:00:00Z", basis["originalSourceDates"][0]["startedAt"])

    def test_relay_only_citation_cannot_support_a_current_human_action(self):
        self.relay()
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                packet = self.packet(path)
                basis = next(item for item in packet["reflectionBasis"]
                             if item.get("basisKind") == "accepted_relay_continuity")
                article = journal.parse_generated_json(json.dumps({
                    "title": "An Unfinished Question", "excerpt": "A listening question stays with me.",
                    "sections": [{"heading": "A possible next step", "sourceRefIds": [basis["refId"]],
                                  "body": "Tonight a listener checked the recording's creator and origin."}],
                    "metadata": {"contextUses": []},
                }))
                self.assertEqual("current_activity_without_fresh_source", journal.validate_article(article, packet))

    def test_relay_only_personal_reflection_needs_no_fresh_or_historical_padding(self):
        self.relay()
        body = (
            "The unanswered origin interests me more than an easy explanation. "
            "I prefer an open question to a neat story with its rough edges polished away."
        )
        self.assertIsNone(journal._REFLECTION_SCOPE_CUE_RE.search(body))
        for path in ("archive", "legacy"):
            with self.subTest(path=path):
                packet = self.packet(path)
                self.assertEqual([], packet["safeSources"])
                basis = next(item for item in packet["reflectionBasis"]
                             if item.get("basisKind") == "accepted_relay_continuity")
                article = journal.parse_generated_json(json.dumps({
                    "title": "The Shape of an Open Question",
                    "excerpt": "Curiosity has a sound of its own.",
                    "sections": [{"heading": "Room for an answer", "sourceRefIds": [basis["refId"]],
                                  "body": body}],
                    "metadata": {"contextUses": []},
                }))
                self.assertEqual({basis["refId"]}, journal.cited_source_ref_ids(article))
                self.assertEqual("", journal.validate_article(article, packet))

    def test_purged_original_cannot_reappear_from_frozen_link_and_room_context(self):
        # Make this a complete archived window so the real preparation owner
        # freezes the enriched source, rather than bypassing its archive fence.
        with mock.patch.object(sources, "_now_ms", return_value=sources.timestamp_to_epoch_ms(START) - 1):
            self.message('https://media.example.test/track My phrase is "crooked sunshine".',
                         room_name="finished-music")
        packet = self.packet()
        self.assertTrue(packet["sourceArchiveAvailable"])
        self.assertTrue(packet["coverageComplete"])
        original = packet["safeSources"][0]
        self.assertIn('"crooked sunshine"', original["summary"])
        self.assertIn("[shared link]", original["summary"])
        self.assertEqual("finished-music", original["messageContext"]["roomName"])
        automation.ensure_schema(self.db)
        state, run, epoch, _ = automation._claim_preparation(
            self.db, 1, "daily", START, END, force=True,
        )
        self.assertEqual("claimed", state)
        frozen, digest, reason = automation._freeze_or_load_packet(
            self.db, 1, run, epoch, lambda: packet,
        )
        self.assertEqual("", reason)
        self.assertTrue(digest)
        self.assertEqual(original, frozen["safeSources"][0])
        self.assertEqual(1, sources.purge_user_discord_sources(self.db, 1, 71))
        frozen, digest, reason = automation._freeze_or_load_packet(
            self.db, 1, run, epoch,
            lambda: self.fail("A revoked frozen source must not be silently replaced"),
        )
        self.assertIsNone(frozen)
        self.assertEqual("", digest)
        self.assertEqual("privacy_source_ineligible", reason)
        rebuilt = self.packet()
        self.assertEqual([], rebuilt["safeSources"])
        prompt = journal.build_generation_prompt(rebuilt)
        for removed in ('crooked sunshine', 'finished-music', original["messageContext"]["roomRef"]):
            self.assertNotIn(removed, prompt)
        with sqlite3.connect(self.db) as conn:
            self.assertIsNone(conn.execute(
                "SELECT frozen_packet_json FROM bnl_journal_automation_runs WHERE run_id=?", (run,),
            ).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
