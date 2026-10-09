import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import bnl_entity_evidence as evidence
import bnl_entity_activity_summary as activity
import bnl_entity_intelligence as intelligence
import bnl_subject_memory_resolver as memory_resolver


class EntityEvidenceSourceLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """CREATE TABLE conversations (
                id INTEGER PRIMARY KEY, guild_id INTEGER, user_id INTEGER,
                user_name TEXT, channel_id INTEGER, channel_name TEXT,
                channel_policy TEXT, role TEXT, content TEXT, timestamp TEXT
            )"""
        )
        evidence.ensure_entity_evidence_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def add_source(self, *, policy="public_home", author="Test Member", content="I shared a new track."):
        self.conn.execute(
            "INSERT INTO conversations VALUES (1,1,42,?,70,'general-chat',?,'user',?,'2026-10-08T12:00:00Z')",
            (author, policy, content),
        )
        self.derive()

    def derive(self):
        row = self.conn.execute("SELECT * FROM conversations WHERE id=1").fetchone()
        return evidence.derive_entity_evidence_from_conversation_row(
            self.conn, row, "Test Member", guild_id=1
        )

    def selected(self):
        return evidence.get_ranked_entity_evidence_for_subject(self.conn, "Test Member", guild_id=1)

    def set_generic_claim(self):
        row = self.conn.execute("SELECT raw_ref_json FROM entity_evidence_events").fetchone()
        fingerprint = json.loads(row[0])["source_fingerprint"]
        self.conn.execute(
            "UPDATE entity_evidence_events SET safe_summary='Test Member hosts a community radio show.',topic='radio host role',raw_ref_json=?",
            (json.dumps({"source_fingerprint": fingerprint}),),
        )

    def fixture_db_path(self):
        scratch = os.environ.get("TMPDIR")
        if not scratch:
            if os.name == "nt":
                raise RuntimeError("An explicit TMPDIR is required for local SQLite fixtures.")
            scratch = tempfile.gettempdir()
        task_dir = os.path.join(scratch, "entity-source-lifecycle-fixtures")
        os.makedirs(task_dir, exist_ok=True)
        db_path = os.path.join(task_dir, self._testMethodName + ".sqlite")
        target = sqlite3.connect(db_path)
        try:
            self.conn.commit()
            self.conn.backup(target)
        finally:
            target.close()
        return db_path

    def summary(self, *, output_mode="admin_internal", source_read_snapshot=None):
        return activity.build_entity_activity_summary(self.fixture_db_path(), "Test Member", 1, output_mode=output_mode, source_read_snapshot=source_read_snapshot)

    def test_public_to_internal_policy_drops_former_public_event(self):
        self.add_source()
        self.conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=1")
        self.derive()

        rows = self.selected()

        self.assertEqual([row["evidence_kind"] for row in rows], ["authored_review_only_conversation"])
        self.assertFalse(rows[0]["public_safe_candidate"])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM entity_evidence_events").fetchone()[0], 2)

    def test_deleted_original_is_not_selected_without_rewriting_evidence(self):
        self.add_source()
        self.conn.execute("DELETE FROM conversations WHERE id=1")

        self.assertEqual(self.selected(), [])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM entity_evidence_events").fetchone()[0], 1)

    def test_original_reassigned_to_other_guild_is_not_selected(self):
        self.add_source()
        self.conn.execute("UPDATE conversations SET guild_id=2 WHERE id=1")

        self.assertEqual(self.selected(), [])

    def test_original_author_changed_is_not_selected(self):
        self.add_source()
        self.conn.execute("UPDATE conversations SET user_id=77,user_name='Other Member' WHERE id=1")

        self.assertEqual(self.selected(), [])

    def test_original_content_edited_after_truncated_snippet_is_not_selected(self):
        original = "I discussed a track. " + "a" * 200 + " old ending"
        self.add_source(content=original)
        self.conn.execute("UPDATE conversations SET content=? WHERE id=1", (original[:-10] + "new ending",))

        self.assertEqual(self.selected(), [])

    def test_sealed_and_protected_sources_are_never_derived(self):
        for policy in ("sealed_test", "protected_system"):
            with self.subTest(policy=policy):
                self.conn.execute("DELETE FROM conversations")
                self.conn.execute("DELETE FROM entity_evidence_events")
                self.add_source(policy=policy, content="Test Member prohibited fixture context.")

                self.assertEqual(self.selected(), [])
                self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM entity_evidence_events").fetchone()[0], 0)

    def test_former_public_source_becoming_sealed_is_not_selected(self):
        self.add_source()
        self.conn.execute("UPDATE conversations SET channel_policy='sealed_test' WHERE id=1")

        self.assertEqual(self.selected(), [])

    def test_internal_source_keeps_admin_provenance(self):
        self.add_source(policy="internal_controlled", content="Test Member internal review context.")

        rows = self.selected()

        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["review_only"])
        self.assertIn("internal review context", json.loads(rows[0]["raw_ref_json"])["snippet"])

    def test_mentioned_original_no_longer_mentioning_subject_is_not_selected(self):
        self.add_source(author="Other Member", content="Test Member shared a track.")
        self.conn.execute("UPDATE conversations SET content='Another Member shared a track.' WHERE id=1")

        self.assertEqual(self.selected(), [])

    def test_mentioned_confirmed_alias_is_validated_by_current_identity_labels(self):
        self.conn.execute(
            "INSERT INTO conversations VALUES (1,1,77,'Other Member',70,'general-chat','public_home','user','Stage Alias shared a track.','2026-10-08T12:00:00Z')"
        )
        row = self.conn.execute("SELECT * FROM conversations WHERE id=1").fetchone()
        evidence.derive_entity_evidence_from_conversation_row(
            self.conn, row, "Test Member", guild_id=1, aliases=["Stage Alias"]
        )
        event = self.conn.execute("SELECT * FROM entity_evidence_events").fetchone()

        self.assertTrue(evidence.validate_entity_evidence_source(
            self.conn, event, "Test Member", 1, aliases=["Stage Alias"]
        )["eligible"])
        self.assertFalse(evidence.validate_entity_evidence_source(
            self.conn, event, "Test Member", 1, aliases=[]
        )["eligible"])

    def test_legacy_complete_snapshot_keeps_valid_internal_review_source(self):
        self.add_source(policy="internal_controlled", content="Test Member internal review context with fuller detail.")
        self.conn.execute(
            "UPDATE entity_evidence_events SET raw_ref_json=?",
            (json.dumps({"table": "conversations", "row_id": 1, "snippet": "Test Member internal review context with fuller detail."}),),
        )

        self.assertEqual(len(self.selected()), 1)

    def test_legacy_event_without_content_lineage_is_not_selected(self):
        self.add_source()
        self.conn.execute("UPDATE entity_evidence_events SET raw_ref_json='{}'")

        self.assertEqual(self.selected(), [])

    def test_source_metadata_changed_before_validation_is_not_selected(self):
        self.add_source()
        self.conn.execute("UPDATE conversations SET timestamp='2026-10-08T13:00:00Z' WHERE id=1")

        self.assertEqual(self.selected(), [])

    def test_sealed_and_protected_sources_are_absent_from_intelligence_corpus(self):
        for policy in ("sealed_test", "protected_system"):
            with self.subTest(policy=policy):
                self.conn.execute("DELETE FROM conversations")
                self.add_source(policy=policy, content="Test Member prohibited corpus fixture.")

                self.assertEqual(activity.collect_subject_intelligence_rows(self.conn, "Test Member", 1), [])

    def test_invalid_structured_source_cannot_rehydrate_current_private_content(self):
        self.add_source()
        event = self.conn.execute("SELECT * FROM entity_evidence_events").fetchone()
        self.conn.execute("UPDATE conversations SET channel_policy='sealed_test',content='Test Member prohibited raw fixture.' WHERE id=1")

        self.assertEqual(activity.extract_full_text_from_source_row(self.conn, "entity_evidence_events", event), "")

    def test_public_only_intelligence_preserves_public_and_excludes_internal_originals(self):
        self.add_source()
        self.conn.execute(
            "INSERT INTO conversations VALUES (2,1,42,'Test Member',71,'internal-review','internal_controlled','user','Test Member private corpus fixture.','2026-10-08T13:00:00Z')"
        )

        rows = activity.collect_subject_intelligence_rows(self.conn, "Test Member", 1, public_only=True)

        self.assertEqual([row["text"] for row in rows], ["I shared a new track."])

    def test_admin_selected_source_snapshot_rejects_later_mutation(self):
        self.add_source()

        snapshot = []
        self.summary(source_read_snapshot=snapshot)

        self.assertEqual(len(snapshot), 1)
        self.assertTrue(evidence.validate_entity_evidence_source(self.conn, snapshot[0], "Test Member", 1)["eligible"])
        self.assertNotIn("snippet", json.dumps(snapshot))
        self.conn.execute("UPDATE conversations SET content='I corrected my track statement.' WHERE id=1")
        self.assertFalse(evidence.validate_entity_evidence_source(self.conn, snapshot[0], "Test Member", 1)["eligible"])

    def test_public_read_has_no_selected_source_snapshot(self):
        self.add_source()
        snapshot = []

        summary = self.summary(output_mode="public", source_read_snapshot=snapshot)

        self.assertNotIn("_sourceReadSnapshot", summary)
        self.assertEqual(snapshot, [])
        self.assertNotIn("rawRefJson", json.dumps(summary["rawProvenance"]))
        self.assertTrue(summary["conversationHighlights"])

    def test_admin_output_keeps_selected_snapshot_only_in_outparam(self):
        self.add_source()
        snapshot = []

        summary = self.summary(source_read_snapshot=snapshot)

        self.assertTrue(snapshot)
        self.assertNotIn("_sourceReadSnapshot", summary)
        normal_output = {key: value for key, value in summary.items() if key != "rawProvenance"}
        self.assertNotIn("source_fingerprint", json.dumps(normal_output))

    def test_activity_snapshot_accepts_existing_json_reference(self):
        self.add_source()
        event = dict(self.conn.execute("SELECT * FROM entity_evidence_events").fetchone())
        fingerprint = json.loads(event["raw_ref_json"])["source_fingerprint"]
        reference = evidence._source_read_reference(event, fingerprint)
        reference["raw_ref_json"] = json.dumps(reference["raw_ref_json"])
        snapshot = [reference]

        summary = self.summary(source_read_snapshot=snapshot)

        self.assertEqual(len(snapshot), 1)
        self.assertTrue(summary["conversationHighlights"])

    def test_public_read_excludes_private_broadcast_fallback(self):
        self.conn.execute(
            "CREATE TABLE broadcast_memory (id INTEGER PRIMARY KEY,guild_id INTEGER,cleaned_summary TEXT,entry_type TEXT,public_safe INTEGER,status TEXT)"
        )
        self.conn.execute("INSERT INTO broadcast_memory VALUES (1,1,'Test Member PRIVATE_BROADCAST_CEDAR','show_note',0,'draft')")

        summary = self.summary(output_mode="public")

        self.assertEqual(summary["rawProvenance"]["rawFragments"], [])

    def test_intelligence_reader_rejects_stale_generic_claims(self):
        for policy in (None, "private", "sealed_test", "protected_system"):
            with self.subTest(policy=policy):
                self.conn.execute("DELETE FROM conversations")
                self.conn.execute("DELETE FROM entity_evidence_events")
                self.add_source()
                self.set_generic_claim()
                if policy is None:
                    self.conn.execute("DELETE FROM conversations")
                else:
                    self.conn.execute("UPDATE conversations SET channel_policy=?", (policy,))

                rows = intelligence._collect_rows(self.conn, "Test Member", 1, 20)

                self.assertEqual([row for row in rows if row["source"] == "entity_evidence_events"], [])

    def test_subject_memory_reader_rejects_stale_generic_claims(self):
        for policy in (None, "private", "sealed_test", "protected_system"):
            with self.subTest(policy=policy):
                self.conn.execute("DELETE FROM conversations")
                self.conn.execute("DELETE FROM entity_evidence_events")
                self.add_source()
                self.set_generic_claim()
                if policy is None:
                    self.conn.execute("DELETE FROM conversations")
                else:
                    self.conn.execute("UPDATE conversations SET channel_policy=?", (policy,))

                resolved = memory_resolver.resolve_subject_memory("Test Member", self.fixture_db_path())

                self.assertEqual(resolved["evidenceCounts"]["publicSafe"], 0)
                self.assertNotIn("hosts a community radio show", json.dumps(resolved))

    def test_current_conversation_events_remain_available_in_other_readers(self):
        self.add_source()
        self.set_generic_claim()

        rows = intelligence._collect_rows(self.conn, "Test Member", 1, 20)
        resolved = memory_resolver.resolve_subject_memory("Test Member", self.fixture_db_path())

        self.assertEqual(len([row for row in rows if row["source"] == "entity_evidence_events"]), 1)
        self.assertEqual(resolved["evidenceCounts"]["publicSafe"], 1)

    def test_profile_prefix_is_not_subject_identity_or_discovered_alias(self):
        self.conn.execute("CREATE TABLE user_profiles (user_id INTEGER,guild_id INTEGER,display_name TEXT,preferred_name TEXT)")
        for order in ((42, 77), (77, 42)):
            with self.subTest(order=order):
                self.conn.execute("DELETE FROM user_profiles")
                self.conn.execute("DELETE FROM conversations")
                self.conn.execute("DELETE FROM entity_evidence_events")
                for user_id in order:
                    name = "Target Artist" if user_id == 42 else "Target Artist Junior"
                    self.conn.execute("INSERT INTO user_profiles VALUES (?,1,?,?)", (user_id, name, name))
                self.conn.execute("INSERT INTO conversations VALUES (1,1,77,'Target Artist Junior',70,'general-chat','public_home','user','I shared a track.','2026-10-08T12:00:00Z')")
                db_path = self.fixture_db_path()

                evidence.derive_entity_evidence_for_subject(db_path, "Target Artist", 1)

                with sqlite3.connect(db_path) as read:
                    selected = read.execute("SELECT source_table,matched_user_id FROM entity_evidence_events").fetchall()
                read.close()
                self.assertNotIn(("user_profiles", 77), selected)
                self.assertNotIn(("conversations", 77), selected)

    def test_whole_profile_name_and_confirmed_alias_preserve_authored_identity(self):
        self.conn.execute("CREATE TABLE user_profiles (user_id INTEGER,guild_id INTEGER,display_name TEXT,preferred_name TEXT)")
        for label, aliases in (("TargetArtist", []), ("Stage Alias", ["Stage Alias"])):
            with self.subTest(label=label):
                self.conn.execute("DELETE FROM user_profiles")
                self.conn.execute("DELETE FROM conversations")
                self.conn.execute("DELETE FROM entity_evidence_events")
                self.conn.execute("INSERT INTO user_profiles VALUES (42,1,?,?)", (label, label))
                self.conn.execute("INSERT INTO conversations VALUES (1,1,42,?,70,'general-chat','public_home','user','I shared a track.','2026-10-08T12:00:00Z')", (label,))
                db_path = self.fixture_db_path()

                evidence.derive_entity_evidence_for_subject(db_path, "Target Artist", 1, confirmed_aliases=aliases)

                read = sqlite3.connect(db_path)
                read.row_factory = sqlite3.Row
                try:
                    selected = evidence.get_ranked_entity_evidence_for_subject(read, "Target Artist", 1, aliases=aliases)
                    self.assertTrue(any(row["relation_to_subject"] == "authored" and row["matched_user_id"] == 42 for row in selected))
                finally:
                    read.close()

    def test_legacy_prefix_cannot_confirm_changed_source_tail(self):
        original = "I discussed a track. " + "a" * 200 + " old ending"
        self.add_source(content=original)
        self.conn.execute(
            "UPDATE entity_evidence_events SET raw_ref_json=?",
            (json.dumps({"table": "conversations", "row_id": 1, "snippet": evidence.safe_text(original)}),),
        )
        self.conn.execute("UPDATE conversations SET content=?", (original[:-10] + "new ending",))

        self.assertEqual(self.selected(), [])
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM entity_evidence_events").fetchone()[0], 1)

    def test_stored_matched_id_does_not_confirm_old_prefix_binding(self):
        self.conn.execute("CREATE TABLE user_profiles (user_id INTEGER,guild_id INTEGER,display_name TEXT,preferred_name TEXT)")
        self.conn.execute("INSERT INTO user_profiles VALUES (77,1,'Target Artist Junior','Target Artist Junior')")
        self.conn.execute("INSERT INTO conversations VALUES (1,1,77,'Target Artist Junior',70,'general-chat','public_home','user','I shared a track.','2026-10-08T12:00:00Z')")
        original = self.conn.execute("SELECT * FROM conversations").fetchone()
        evidence.upsert_entity_evidence_event(
            self.conn, guild_id=1, subject_name="Target Artist", source_type="conversation",
            source_table="conversations", source_row_id="1", matched_user_id=77,
            relation_to_subject="authored", channel_policy="public_home", public_safe_candidate=True,
            review_only=False, evidence_kind="authored_public_conversation", safe_summary="Subject authored public-side track context.",
            raw_ref_json={"table": "conversations", "row_id": 1, "source_fingerprint": evidence.conversation_source_fingerprint(original)},
        )
        event = self.conn.execute("SELECT * FROM entity_evidence_events").fetchone()

        self.assertFalse(evidence.validate_entity_evidence_source(self.conn, event, "Target Artist", 1)["eligible"])

    def test_current_exact_profile_corroborates_stable_author_id(self):
        self.conn.execute("CREATE TABLE user_profiles (user_id INTEGER,guild_id INTEGER,display_name TEXT,preferred_name TEXT)")
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Test Member','Test Member')")
        self.add_source(author="Account Label")
        original = self.conn.execute("SELECT * FROM conversations").fetchone()
        evidence.upsert_entity_evidence_event(
            self.conn, guild_id=1, subject_name="Test Member", source_type="conversation",
            source_table="conversations", source_row_id="1", matched_user_id=42,
            relation_to_subject="authored", channel_policy="public_home", public_safe_candidate=True,
            review_only=False, evidence_kind="authored_public_conversation", safe_summary="Subject authored public-side track context.",
            raw_ref_json={"table": "conversations", "row_id": 1, "source_fingerprint": evidence.conversation_source_fingerprint(original)},
        )

        self.assertEqual(len(self.selected()), 1)

    def test_activity_matching_does_not_promote_discovered_identity_label_to_alias(self):
        self.conn.execute("CREATE TABLE user_profiles (user_id INTEGER,guild_id INTEGER,display_name TEXT,preferred_name TEXT)")
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Unconfirmed Stage','Test Member')")
        self.conn.execute("INSERT INTO conversations VALUES (1,1,77,'Other Member',70,'general-chat','public_home','user','Unconfirmed Stage hosts a show.','2026-10-08T12:00:00Z')")
        original = self.conn.execute("SELECT * FROM conversations").fetchone()
        evidence.derive_entity_evidence_from_conversation_row(self.conn, original, "Test Member", guild_id=1, aliases=["Unconfirmed Stage"])

        summary = self.summary()

        self.assertEqual(summary["conversationHighlights"], [])

    def test_intelligence_selected_sources_support_ephemeral_delivery_revalidation(self):
        self.add_source()
        snapshot = []
        db_path = self.fixture_db_path()

        profile = intelligence.build_entity_intelligence_profile(db_path, 1, "Test Member", source_read_snapshot=snapshot)

        self.assertGreaterEqual(len(snapshot), 2)
        self.assertTrue(all(evidence.validate_entity_evidence_source(self.conn, ref, "Test Member", 1)["eligible"] for ref in snapshot))
        self.assertNotIn("_sourceReadSnapshot", profile)
        read = sqlite3.connect(db_path)
        try:
            saved = read.execute("SELECT summary_json FROM entity_profile_snapshots").fetchone()[0]
            self.assertNotIn("source_fingerprint", saved)
        finally:
            read.close()
        self.conn.execute("UPDATE conversations SET channel_policy='sealed_test'")
        self.assertTrue(all(not evidence.validate_entity_evidence_source(self.conn, ref, "Test Member", 1)["eligible"] for ref in snapshot))

    def test_subject_memory_selected_sources_support_ephemeral_delivery_revalidation(self):
        self.add_source()
        snapshot = []

        resolved = memory_resolver.resolve_subject_memory("Test Member", self.fixture_db_path(), source_read_snapshot=snapshot)

        self.assertEqual(len(snapshot), 1)
        self.assertTrue(evidence.validate_entity_evidence_source(self.conn, snapshot[0], "Test Member", 1)["eligible"])
        self.assertNotIn("_sourceReadSnapshot", resolved)
        self.conn.execute("DELETE FROM conversations")
        self.assertFalse(evidence.validate_entity_evidence_source(self.conn, snapshot[0], "Test Member", 1)["eligible"])

    def test_subject_memory_withholds_engine_facts_without_original_lineage(self):
        intelligence.ensure_entity_intelligence_schema(self.conn)
        intelligence._upsert_fact(
            self.conn, 1, "test-member", "Test Member", "role",
            {"label": "radio host", "value": "Test Member hosts a community radio show.",
             "publicSafe": True, "reviewOnly": False, "visibility": "public_safe_candidate", "authority": "public_discord_observed"},
        )

        resolved = memory_resolver.resolve_subject_memory("Test Member", self.fixture_db_path())

        self.assertEqual(resolved["evidenceCounts"]["publicSafe"], 0)
        self.assertNotIn("hosts a community radio show", json.dumps(resolved))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM entity_intelligence_facts WHERE status='active'").fetchone()[0], 1)

    def test_subject_memory_keeps_non_engine_confirmed_legacy_facts(self):
        intelligence.ensure_entity_intelligence_schema(self.conn)
        intelligence._upsert_fact(
            self.conn, 1, "test-member", "Test Member", "role",
            {"label": "radio host", "value": "Test Member hosts a community radio show.",
             "sourceType": "first_party_record", "publicSafe": True, "reviewOnly": False,
             "visibility": "public_safe_candidate", "authority": "official_public"},
        )

        resolved = memory_resolver.resolve_subject_memory("Test Member", self.fixture_db_path())

        self.assertEqual(resolved["evidenceCounts"]["publicSafe"], 1)

    def test_subject_memory_expected_guild_excludes_other_guild_same_subject(self):
        self.add_source()
        self.conn.execute("UPDATE conversations SET guild_id=2")
        original = self.conn.execute("SELECT * FROM conversations").fetchone()
        fingerprint = evidence.conversation_source_fingerprint(original)
        self.conn.execute(
            "UPDATE entity_evidence_events SET guild_id=2,safe_summary='Test Member hosts a community radio show.',raw_ref_json=?",
            (json.dumps({"source_fingerprint": fingerprint}),),
        )
        snapshot = []

        resolved = memory_resolver.resolve_subject_memory("Test Member", self.fixture_db_path(), guild_id=1, source_read_snapshot=snapshot)

        self.assertEqual(resolved["evidenceCounts"]["publicSafe"], 0)
        self.assertEqual(snapshot, [])

    def test_public_output_excludes_unconfirmed_identity_diagnostics(self):
        self.add_source()
        identity = {"matchedIdentityLabels": ["Unconfirmed Stage"], "aliasLabels": []}
        with patch.object(activity, "_resolve_existing_enrichment_identity", return_value=identity):
            public = self.summary(output_mode="public")
            admin = self.summary()

        self.assertNotIn("Unconfirmed Stage", json.dumps(public))
        self.assertIn("Unconfirmed Stage", admin["matchedNames"])
        self.assertTrue(public["conversationHighlights"])

    def test_intelligence_rowid_only_original_keeps_source_reference(self):
        self.conn.execute("DROP TABLE conversations")
        self.conn.execute(
            "CREATE TABLE conversations (guild_id INTEGER,user_name TEXT,channel_policy TEXT,role TEXT,content TEXT)"
        )
        self.conn.execute("INSERT INTO conversations VALUES (1,'Test Member','public_home','user','I shared a new track.')")
        snapshot = []

        rows = intelligence._collect_rows(self.conn, "Test Member", 1, 20, source_read_snapshot=snapshot)

        self.assertEqual(len(rows), 1)
        self.assertEqual(snapshot[0]["source_row_id"], "1")
        self.assertTrue(evidence.validate_entity_evidence_source(self.conn, snapshot[0], "Test Member", 1)["eligible"])

    def test_raw_activity_rejects_longer_author_name_without_current_binding(self):
        self.conn.execute("INSERT INTO conversations VALUES (1,1,77,'Target Artist Junior',70,'general-chat','public_home','user','I shared a new track.','2026-10-08T12:00:00Z')")

        rows = activity.collect_subject_intelligence_rows(self.conn, "Target Artist", 1)
        snapshot = []
        summary = activity.build_entity_activity_summary(self.fixture_db_path(), "Target Artist", 1, source_read_snapshot=snapshot)

        self.assertEqual(rows, [])
        self.assertEqual(summary["conversationHighlights"], [])
        self.assertEqual(snapshot, [])

    def test_raw_activity_rejects_model_author_and_channel_only_mention(self):
        for role, author, channel in (("assistant", "Test Member", "general-chat"), ("user", "Other Member", "Test Member")):
            with self.subTest(role=role, author=author):
                self.conn.execute("DELETE FROM conversations")
                self.conn.execute("INSERT INTO conversations VALUES (1,1,77,?,70,?,'public_home',?,'I shared a new track.','2026-10-08T12:00:00Z')", (author, channel, role))

                rows = activity.collect_subject_intelligence_rows(self.conn, "Test Member", 1)
                snapshot = []
                summary = self.summary(source_read_snapshot=snapshot)

                self.assertEqual(rows, [])
                self.assertEqual(summary["conversationHighlights"], [])
                self.assertEqual(snapshot, [])

    def test_raw_activity_preserves_current_bound_id_and_confirmed_alias(self):
        self.conn.execute("CREATE TABLE user_profiles (user_id INTEGER,guild_id INTEGER,display_name TEXT,preferred_name TEXT)")
        self.conn.execute("INSERT INTO user_profiles VALUES (42,1,'Test Member','Test Member')")
        for label, aliases in (("Account Label", []), ("Stage Alias", ["Stage Alias"])):
            with self.subTest(label=label):
                self.conn.execute("DELETE FROM conversations")
                self.conn.execute("INSERT INTO conversations VALUES (1,1,42,?,70,'general-chat','internal_controlled','user','I shared a new track.','2026-10-08T12:00:00Z')", (label,))
                identity = {"_matchedUserIds": [42], "aliasLabels": aliases}
                snapshot = []
                with patch.object(activity, "_resolve_existing_enrichment_identity", return_value=identity):
                    summary = self.summary(source_read_snapshot=snapshot)

                self.assertTrue(summary["conversationHighlights"])
                self.assertTrue(snapshot)
                self.assertTrue(all(evidence.validate_entity_evidence_source(self.conn, ref, "Test Member", 1, aliases=aliases)["eligible"] for ref in snapshot))

    def test_raw_activity_selected_roots_reject_later_revision(self):
        self.conn.execute("INSERT INTO conversations VALUES (1,1,42,'Test Member',70,'general-chat','public_home','user','I shared a new track.','2026-10-08T12:00:00Z')")

        snapshot = []
        self.summary(source_read_snapshot=snapshot)

        self.assertTrue(snapshot)
        self.assertTrue(all(evidence.validate_entity_evidence_source(self.conn, ref, "Test Member", 1)["eligible"] for ref in snapshot))
        self.conn.execute("UPDATE conversations SET content='I corrected my track statement.'")
        self.assertTrue(all(not evidence.validate_entity_evidence_source(self.conn, ref, "Test Member", 1)["eligible"] for ref in snapshot))

    def test_activity_keeps_materialized_authored_event_with_current_confirmed_alias(self):
        self.conn.execute("INSERT INTO conversations VALUES (1,1,11,'Known Performer',70,'finished-tracks','public_home','user','I shared my finished track.','2026-10-08T12:00:00Z')")
        original = self.conn.execute("SELECT * FROM conversations").fetchone()
        evidence.derive_entity_evidence_from_conversation_row(self.conn, original, "Test Member", guild_id=1, aliases=["Known Performer"])
        snapshot = []

        summary = activity.build_entity_activity_summary(
            self.fixture_db_path(), "Test Member", 1,
            confirmed_aliases=["Known Performer"], source_read_snapshot=snapshot,
        )

        fragments = summary["rawProvenance"]["rawFragments"]
        self.assertTrue(any(fragment.get("evidenceKind") == "authored_public_conversation" and fragment.get("rowId") == "1" for fragment in fragments))
        self.assertEqual(summary["rawProvenance"]["sourceCounts"].get("conversations"), 1)
        self.assertTrue(summary["conversationHighlights"])
        self.assertEqual(len(snapshot), 1)
        self.assertTrue(evidence.validate_entity_evidence_source(self.conn, snapshot[0], "Test Member", 1, aliases=["Known Performer"])["eligible"])
        self.assertEqual(self.summary()["conversationHighlights"], [])


if __name__ == "__main__":
    unittest.main()
