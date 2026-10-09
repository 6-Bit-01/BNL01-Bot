import os
import sqlite3
from pathlib import Path
import stat
import unittest
from unittest import mock
import uuid

import bnl_source_file_enrichment as enrich


class SourceFileIdentityBindingTests(unittest.TestCase):
    def setUp(self):
        root = Path(os.environ.get("TMPDIR") or os.environ.get("TEMP") or "")
        if not root.is_absolute() or not root.is_dir():
            raise RuntimeError("An explicit writable task scratch directory is required")
        for part in (root, *root.parents):
            info = part.lstat()
            if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
                raise RuntimeError("Task scratch must not contain reparse links")
        if os.name == "nt" and root.drive.upper() != "D:":
            raise RuntimeError("Local identity fixtures require D: scratch")
        directory = root / ("identity-binding-" + uuid.uuid4().hex)
        directory.mkdir()
        self.db = str(directory / "evidence.sqlite")
        self.conn = sqlite3.connect(self.db)
        self.conn.executescript(
            "CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER, "
            "display_name TEXT, preferred_name TEXT);"
            "CREATE TABLE conversations (id INTEGER PRIMARY KEY, user_id INTEGER, "
            "user_name TEXT, guild_id INTEGER, channel_name TEXT, channel_policy TEXT, "
            "role TEXT, content TEXT, timestamp TEXT);"
            "CREATE TABLE community_presence (guild_id INTEGER, display_name TEXT, "
            "subject_key TEXT, connection_notes TEXT, source_lanes TEXT, user_id INTEGER, "
            "mention_count INTEGER DEFAULT 0, direct_interaction_count INTEGER DEFAULT 0, "
            "operator_mention_count INTEGER DEFAULT 0);"
        )

    def tearDown(self):
        self.conn.close()

    def _lookup(self, name="Target Artist", **fields):
        return {
            "ok": True,
            "found": True,
            "matchKind": "exact",
            "data": {"sourceFile": {"id": "sf_target", "name": name, "status": "active", **fields}},
        }

    def _profiles(self, rows):
        self.conn.executemany("INSERT INTO user_profiles VALUES (?,1,?,?)", rows)
        self.conn.commit()

    def _conversation(self, user_id, name, content):
        self.conn.execute(
            "INSERT INTO conversations VALUES (NULL,?,?,1,'general','public_home','user',?,'2026-10-08T12:00:00+00:00')",
            (user_id, name, content),
        )
        self.conn.commit()

    def test_shared_word_does_not_bind_another_member(self):
        self._profiles([(10, "Test Member", "Test Member"), (20, "Other Member", "Other Member")])

        identity = enrich.resolve_enrichment_subject_identity("Test Member", self._lookup("Test Member"), self.db, 1)

        self.assertEqual(identity["_matchedUserIds"], {10})
        self.assertEqual(identity["matchedUserProfileCount"], 1)

    def test_profile_names_do_not_create_transitive_bindings_in_either_order(self):
        profiles = [(10, "Target Artist", "Bridge Alias"), (20, "Bridge Alias", "Other Member")]
        for rows in (profiles, list(reversed(profiles))):
            with self.subTest(rows=rows):
                self.conn.execute("DELETE FROM user_profiles")
                self._profiles(rows)

                identity = enrich.resolve_enrichment_subject_identity("Target Artist", self._lookup(), self.db, 1)

                self.assertEqual(identity["_matchedUserIds"], {10})

    def test_unconfirmed_alias_and_connection_fields_do_not_bind_profiles(self):
        self._profiles([(10, "Target Artist", "Target Artist"), (20, "Other Member", "Other Member")])
        for field in ("proposedAliases", "possibleAliases", "possibleConnections", "identityLinks", "identity_links", "aliases", "alias", "matchedAlias"):
            with self.subTest(field=field):
                identity = enrich.resolve_enrichment_subject_identity(
                    "Target Artist", self._lookup(**{field: ["Other Member"]}), self.db, 1
                )

                self.assertEqual(identity["_matchedUserIds"], {10})
                self.assertNotIn("Other Member", identity["aliasLabels"])

    def test_unconfirmed_alias_lookup_does_not_bind_matched_alias(self):
        self._profiles([(10, "Target Artist", "Target Artist"), (20, "Other Member", "Other Member")])
        lookup = self._lookup()
        lookup["matchKind"] = "unconfirmed_alias"
        lookup["data"]["matchedAlias"] = "Other Member"

        identity = enrich.resolve_enrichment_subject_identity("Target Artist", lookup, self.db, 1)

        self.assertEqual(identity["_matchedUserIds"], {10})

    def test_confirmed_alias_maps_bind_only_the_confirmed_names(self):
        self._profiles([(10, "Target Persona", "Target Persona"), (11, "Known Performer", "Known Performer"), (20, "Other Member", "Other Member")])
        for field in ("aliases", "identityLinks", "identity_links"):
            with self.subTest(field=field):
                identity = enrich.resolve_enrichment_subject_identity(
                    "Target Persona",
                    self._lookup("Target Persona", **{field: {"confirmed": ["Known Performer"], "proposed": ["Other Member"]}}),
                    self.db,
                    1,
                )

                self.assertEqual(identity["_matchedUserIds"], {10, 11})
                self.assertNotIn("Other Member", identity["aliasLabels"])

    def test_canonical_identity_links_bind_only_confirmed_enabled_labels(self):
        self._profiles([(10, "Target Persona", "Target Persona"), (11, "Known Performer", "Known Performer"),
                        (20, "Other Member", "Other Member")])
        links = [
            {"id": "private-link-id", "label": "Known Performer", "normalizedLabel": "known performer",
             "status": "confirmed", "useForMatching": True, "useInPublicDossier": False},
            {"label": "Other Member", "status": "confirmed", "useForMatching": False},
            {"label": "Other Member", "status": "proposed", "useForMatching": True},
            {"label": "Other Member", "status": "rejected", "useForMatching": True},
            "Other Member",
        ]
        identity = enrich.resolve_enrichment_subject_identity(
            "Target Persona", self._lookup("Target Persona", identityLinks=links), self.db, 1,
        )
        self.assertEqual(identity["_matchedUserIds"], {10, 11})
        self.assertIn("Known Performer", identity["aliasLabels"])
        self.assertNotIn("Other Member", identity["aliasLabels"])
        self.assertNotIn("private-link-id", identity["aliasLabels"])

    def test_canonical_identity_link_preserves_alias_authored_delivery(self):
        self._profiles([(10, "Target Persona", "Target Persona"), (11, "Known Performer", "Known Performer")])
        self._conversation(11, "Known Performer", "My finished track is CONFIRMED_LINK_TRACK_CEDAR.")
        lookup = self._lookup("Target Persona", identityLinks=[{
            "label": "Known Performer", "normalizedLabel": "known performer", "status": "confirmed",
            "useForMatching": True, "useInPublicDossier": False,
        }])
        archives = []
        result = enrich.run_source_file_enrichment(
            self.db, 1, "Target Persona", force=True, lookup_func=lambda query: lookup,
            archive_sender=lambda payload: archives.append(payload) or {"ok": True, "archiveId": "arc_fixture"},
            sender=lambda payload: {"ok": True, "recommendationId": "rec_fixture"},
            environ={"BNL_SOURCE_FILE_ARCHIVE_TOKEN": "offline-fixture-token"},
        )
        self.assertTrue(result["sent"], result.get("status"))
        self.assertEqual(result["sourceCounts"].get("conversations_by_author"), 1)
        self.assertEqual(len(archives), 1)
        self.assertEqual(archives[0]["evidenceReceiptSummary"]["sourceCounts"].get("conversations_by_author"), 1)

    def test_whole_normalized_name_and_confirmed_alias_routes_remain_valid(self):
        self._profiles([(10, "Hellcat NZ", "HellcatNZ"), (11, "Known Artist", "Known Artist")])
        lookup = self._lookup("HellcatNZ")
        lookup["matchKind"] = "confirmed alias"
        lookup["data"]["matchedAlias"] = "Known Artist"
        lookup["data"]["confirmedAliases"] = ["Hellcat NZ"]

        identity = enrich.resolve_enrichment_subject_identity("HellcatNZ", lookup, self.db, 1)

        self.assertEqual(identity["_matchedUserIds"], {10, 11})

    def test_presence_connection_note_does_not_bind_a_member(self):
        self.conn.execute("INSERT INTO community_presence (guild_id,display_name,subject_key,connection_notes,source_lanes,user_id) VALUES (1,'Other Member','other_member','Target Artist','discord',20)")
        self.conn.commit()

        identity = enrich.resolve_enrichment_subject_identity("Target Artist", self._lookup(), self.db, 1)

        self.assertEqual(identity["_matchedUserIds"], set())

    def test_presence_names_do_not_create_transitive_bindings(self):
        self.conn.executemany(
            "INSERT INTO community_presence (guild_id,display_name,subject_key,connection_notes,source_lanes,user_id) VALUES (1,?,?,?,'discord',?)",
            [("Bridge Alias", "target_artist", "", 10), ("Other Member", "bridge_alias", "", 20)],
        )
        self.conn.commit()

        identity = enrich.resolve_enrichment_subject_identity("Target Artist", self._lookup(), self.db, 1)

        self.assertEqual(identity["_matchedUserIds"], {10})

    def test_author_name_shared_word_is_not_subject_authorship(self):
        self._conversation(20, "Other Member", "A neutral discussion without self naming.")

        evidence = enrich.collect_source_enrichment_evidence(self.db, 1, "Test Member", lookup_result=self._lookup("Test Member"))

        self.assertEqual(evidence["sourceCounts"].get("conversations_by_author", 0), 0)
        self.assertFalse(evidence["diagnostics"]["channelEvidenceFound"])

    def test_profile_preferred_name_cannot_bind_another_author(self):
        self._profiles([(10, "Target Artist", "Bridge Alias")])
        self._conversation(20, "Bridge Alias", "A neutral discussion without self naming.")

        evidence = enrich.collect_source_enrichment_evidence(self.db, 1, "Target Artist", lookup_result=self._lookup())

        self.assertEqual(evidence["sourceCounts"].get("conversations_by_author", 0), 0)
        self.assertNotIn("Bridge Alias", evidence["subjectIdentity"]["aliasLabels"])

    def test_bound_user_id_preserves_authorship_without_subject_text(self):
        self._profiles([(10, "Target Artist", "Target Artist")])
        self._conversation(10, "Changed Display", "A neutral discussion without self naming.")

        evidence = enrich.collect_source_enrichment_evidence(self.db, 1, "Target Artist", lookup_result=self._lookup())

        self.assertEqual(evidence["sourceCounts"].get("conversations_by_author", 0), 1)
        self.assertTrue(evidence["diagnostics"]["channelEvidenceFound"])

    def test_fuzzy_words_do_not_enter_operational_subject_evidence(self):
        self._conversation(20, "Other Artist", "Member arrived for the discussion.")

        evidence = enrich.collect_source_enrichment_evidence(self.db, 1, "Test Member", lookup_result=self._lookup("Test Member"))

        self.assertEqual(evidence["sourceCounts"].get("conversations", 0), 0)
        self.assertEqual(evidence["sourceCounts"].get("conversations_by_author", 0), 0)

    def test_named_mentions_remain_review_context_without_authorship(self):
        self._conversation(20, "Other Artist", "Test Member arrived for the discussion.")
        evidence = enrich.collect_source_enrichment_evidence(self.db, 1, "Test Member", lookup_result=self._lookup("Test Member"))
        self.assertEqual(evidence["sourceCounts"].get("conversations", 0), 1)
        self.assertEqual(evidence["sourceCounts"].get("conversations_by_author", 0), 0)

    def test_unconfirmed_alias_lookup_never_confirms_a_binding(self):
        for kind in ("unconfirmed_alias", "alias_unconfirmed", "possible_alias"):
            with self.subTest(kind=kind):
                self.assertFalse(enrich._is_confirmed_alias_lookup({"matchKind": kind}))
        self.assertTrue(enrich._is_confirmed_alias_lookup({"matchKind": "confirmed_alias"}))

    def test_unconfirmed_alias_route_does_not_collect_or_send(self):
        for kind in ("unconfirmed_alias", "alias_unconfirmed", "possible_alias", "proposed_alias"):
            with self.subTest(kind=kind):
                lookup = self._lookup()
                lookup["matchKind"] = kind
                sender = mock.Mock()
                archive = mock.Mock()
                with mock.patch.object(enrich, "collect_source_enrichment_evidence") as collector:
                    result = enrich.run_source_file_enrichment(
                        self.db, 1, "Other Member", lookup_key="alias", lookup_value="Other Member",
                        lookup_func=lambda query: lookup, sender=sender, archive_sender=archive,
                    )
                self.assertEqual(result["status"], "possible_match_review")
                collector.assert_not_called()
                sender.assert_not_called()
                archive.assert_not_called()

    def test_presence_connection_note_does_not_become_subject_activity(self):
        self.conn.execute("INSERT INTO community_presence (guild_id,display_name,subject_key,connection_notes,source_lanes,user_id) VALUES (1,'Other Member','other_member','Target Artist','discord',20)")
        self.conn.commit()

        evidence = enrich.collect_source_enrichment_evidence(self.db, 1, "Target Artist", lookup_result=self._lookup())

        self.assertEqual(evidence["sourceCounts"].get("community_presence", 0), 0)

    def test_public_dossier_target_preserves_whole_name_binding(self):
        self._profiles([(10, "Target Artist", "Target Artist"), (20, "Other Artist", "Other Artist")])
        lookup = self._lookup(type="public_dossier", targetDossierId="dossier_target")
        lookup["data"]["sourceFile"].pop("status")
        lookup["data"]["existingDossierMatch"] = {"targetDossierId": "dossier_target", "title": "Target Artist"}

        identity = enrich.resolve_enrichment_subject_identity("Target Artist", lookup, self.db, 1)

        self.assertEqual(identity["_matchedUserIds"], {10})
        self.assertTrue(identity["existingDossierUpdateLane"])


if __name__ == "__main__":
    unittest.main()
