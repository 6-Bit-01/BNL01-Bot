"""Offline SourceFile evidence acceptance; retained fixtures, no live effects."""

from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import stat
import unittest
from unittest import mock
import uuid

import bnl_source_file_enrichment as enrichment


class SourceFileEvidenceAcceptanceTests(unittest.TestCase):
    OLD = "OLD_MIX_CEDAR"
    NEW = "NEW_MIX_WILLOW"
    PRIVATE = "PRIVATE_NOTE_LARCH"
    OTHER = "OTHER_MEMBER_MAPLE"
    SEALED = "SEALED_NOTE_ASPEN"
    PROTECTED = "PROTECTED_NOTE_ELM"
    UNKNOWN = "UNKNOWN_NOTE_HEMLOCK"

    def setUp(self):
        raw = next((os.environ[key] for key in ("BNL_SOURCE_ACCEPTANCE_SCRATCH", "TMPDIR", "TEMP", "TMP")
                    if os.environ.get(key)), "")
        root = Path(raw)
        if not raw or not root.is_absolute() or not root.is_dir():
            raise RuntimeError("Supply an explicit existing absolute acceptance scratch directory")
        for component in (root, *root.parents):
            attrs = component.lstat()
            if stat.S_ISLNK(attrs.st_mode) or getattr(attrs, "st_file_attributes", 0) & 0x400:
                raise RuntimeError("Acceptance scratch has a reparse-linked component")
        root = root.resolve()
        if os.name == "nt":
            allowed = Path(r"D:\CodexScratch").resolve()
            if allowed not in root.parents or root.name != "scratch":
                raise RuntimeError("Acceptance scratch must be D:/CodexScratch/<task>/scratch")
        self.db = str(root / ("source-file-evidence-" + uuid.uuid4().hex + ".sqlite"))
        self.conn = sqlite3.connect(self.db)
        self.addCleanup(self.conn.close)
        self.conn.executescript("""
            CREATE TABLE user_profiles (user_id INTEGER, guild_id INTEGER,
                display_name TEXT, preferred_name TEXT, last_seen TEXT, last_greeting_at TEXT);
            CREATE TABLE user_memory_facts (id INTEGER PRIMARY KEY, user_id INTEGER,
                guild_id INTEGER, fact_key TEXT, fact_value TEXT, confidence REAL,
                is_core INTEGER, updated_at TEXT);
            CREATE TABLE conversations (id INTEGER PRIMARY KEY, user_id INTEGER,
                user_name TEXT, guild_id INTEGER, channel_name TEXT, channel_policy TEXT,
                role TEXT, content TEXT, timestamp TEXT);
            CREATE TABLE broadcast_memory (id INTEGER PRIMARY KEY, guild_id INTEGER,
                episode_date TEXT, cleaned_summary TEXT, entry_type TEXT, public_safe INTEGER,
                usage_scope TEXT, status TEXT, created_at TEXT);
        """)
        self.now = datetime.now(timezone.utc).isoformat()
        self.conn.executemany("INSERT INTO user_profiles VALUES (?,?,?, ?,NULL,NULL)", [
            (10, 1, "Test Member", "Test Member"),
            (20, 1, "Neighbor Artist", "Neighbor Artist"),
        ])
        self.conn.execute("INSERT INTO user_memory_facts VALUES (1,10,1,'role',?,0.9,1,?)",
                          ("Test Member is a recurring artist candidate.", self.now))
        self.conn.execute("INSERT INTO broadcast_memory VALUES (1,1,?,?, 'show_note',1,'ambient,direct','active',?)",
                          (self.now[:10], "Test Member participated in a public music discussion.", self.now))
        self._conversation(1, 10, "Test Member", "public_home", "My current mix is " + self.OLD + ".")
        self._conversation(2, 10, "Test Member", "internal_controlled", "My private note is " + self.PRIVATE + ".")
        self._conversation(3, 20, "Neighbor Artist", "public_home", "My current mix is " + self.OTHER + ".")
        self.conn.commit()
        self.archives = []
        self.recommendations = []
        guards = ExitStack()
        self.addCleanup(guards.close)
        for target in ("socket.create_connection", "socket.socket.connect", "socket.socket.connect_ex",
                       "socket.socket.sendto", "socket.getaddrinfo",
                       "urllib.request.urlopen", "urllib.request.OpenerDirector.open"):
            guards.enter_context(mock.patch(target, side_effect=AssertionError("Offline acceptance forbids network")))
        guards.enter_context(mock.patch.dict(os.environ, {
            "BNL_DB_PATH": self.db,
            "BNL_SOURCE_FILE_ARCHIVE_TOKEN": "",
            "BNL_DOSSIER_INGEST_TOKEN": "",
        }))

    def _conversation(self, row_id, user_id, name, policy, content):
        self.conn.execute("INSERT INTO conversations VALUES (?,?,?,?,?,?,?,?,?)",
                          (row_id, user_id, name, 1, "general", policy, "user", content, self.now))

    def _lookup(self, query):
        return {"ok": True, "found": True, "matchKind": "exact", "data": {
            "sourceFile": {"candidateId": "cand_test_member", "name": "Test Member", "status": "active"}}}

    def _archive(self, payload):
        self.archives.append(payload)
        return {"ok": True, "archiveId": "arc_fixture", "status": 200}

    def _recommendation(self, payload):
        self.recommendations.append(payload)
        return {"ok": True, "recommendationId": "rec_fixture", "status": 200}

    def _run(self, *, dry_run=False, archive_sender=None):
        return enrichment.run_source_file_enrichment(
            self.db, 1, "Test Member", lookup_func=self._lookup,
            dry_run=dry_run, sender=self._recommendation,
            archive_sender=archive_sender or self._archive,
            environ={"BNL_DB_PATH": self.db, "BNL_SOURCE_FILE_ARCHIVE_TOKEN": "offline-fixture-token"},
        )

    @staticmethod
    def _text(value):
        return json.dumps(value, sort_keys=True, ensure_ascii=True)

    def _assert_sent(self, result):
        self.assertTrue(result["sent"], result.get("status"))
        self.assertTrue(bool(self.archives), "Expected archive delivery")
        self.assertTrue(bool(self.recommendations), "Expected recommendation delivery")
        self.assertEqual(self.archives[-1]["candidateId"], "cand_test_member")
        self.assertEqual(self.recommendations[-1]["targetCandidateId"], "cand_test_member")

    def _assert_absent_from_output(self, marker, result):
        for label, value in self._outputs(result):
            with self.subTest(output=label):
                self.assertFalse(marker in self._text(value), f"{marker} present in {label}")

    def _outputs(self, result):
        return (("packet", result), ("archive", self.archives),
                ("recommendation", self.recommendations))

    @classmethod
    def _readable(cls, value):
        if isinstance(value, dict):
            return {key: cls._readable(item) for key, item in value.items()
                    if key not in {"rawProvenance", "rawRefJson"}}
        if isinstance(value, list):
            return [cls._readable(item) for item in value]
        return value

    def _assert_not_readable(self, marker, result):
        for label, value in self._outputs(result):
            with self.subTest(output=label):
                self.assertFalse(marker in self._text(self._readable(value)),
                                 f"{marker} present outside raw provenance in {label}")

    def _assert_no_public_authority(self, marker, result):
        def check(value):
            if isinstance(value, list):
                for item in value:
                    check(item)
            elif isinstance(value, dict):
                if any(marker in str(item) for item in value.values()
                       if isinstance(item, (str, int, float, bool))):
                    for key in ("publicSafe", "public_safe", "publicSafeCandidate", "public_safe_candidate"):
                        self.assertFalse(value.get(key), f"{marker} retained public authority in {key}")
                    for key in ("channelPolicy", "channel_policy"):
                        self.assertFalse(value.get(key) in {"public_home", "public_context", "public_selective"},
                                         f"{marker} retained a public source policy")
                for key, item in value.items():
                    if key == "rawRefJson" and isinstance(item, str):
                        try:
                            item = json.loads(item)
                        except ValueError:
                            pass
                    check(item)

        for label, value in self._outputs(result):
            with self.subTest(output=label):
                check(value)

    def test_real_collection_packet_archive_and_compact_payload_preserve_current_evidence(self):
        result = self._run()
        self._assert_sent(result)
        self.assertTrue("conversations_by_author" in result["sourceTypes"], "Authored conversation source missing")
        self.assertTrue(self.OLD in self._text(self.archives[-1]), f"{self.OLD} absent from baseline archive")
        self.assertTrue("sourcePackage" in self.archives[-1], "Archive sourcePackage missing")
        self.assertTrue("subjectMemoryPacketV1" in self.archives[-1], "Archive subjectMemoryPacketV1 missing")
        self.assertTrue("sourceFileCaseReportV1" in self.archives[-1], "Archive sourceFileCaseReportV1 missing")
        self.assertTrue("review-only" in self.recommendations[-1]["reason"].lower(), "Recommendation review-only label missing")
        self._assert_not_readable(self.PRIVATE, result)
        self._assert_no_public_authority(self.PRIVATE, result)
        self._assert_absent_from_output(self.OTHER, result)

    def test_sealed_and_protected_sources_never_enter_operational_archive(self):
        self._conversation(4, 10, "Test Member", "sealed_test", "My sealed note is " + self.SEALED + ".")
        self._conversation(5, 10, "Test Member", "protected_system", "My protected note is " + self.PROTECTED + ".")
        self.conn.commit()
        result = self._run()
        self._assert_sent(result)
        self._assert_absent_from_output(self.SEALED, result)
        self._assert_absent_from_output(self.PROTECTED, result)

    def test_ineligible_bot_authors_do_not_block_current_member_delivery(self):
        for role, name in (("assistant", "Test Member"), ("bot", "Test Member"),
                           ("system", "Test Member"), ("model", "Test Member"), ("user", "BNL-01")):
            with self.subTest(role=role, name=name):
                self.conn.execute("DELETE FROM conversations WHERE id=4")
                self._conversation(4, 10, name, "public_home", "INELIGIBLE_BOT_SOURCE_CYPRESS discusses a track.")
                self.conn.execute("UPDATE conversations SET role=? WHERE id=4", (role,))
                self.conn.commit()
                self.archives.clear()
                self.recommendations.clear()
                result = self._run()
                self._assert_sent(result)
                self._assert_absent_from_output("INELIGIBLE_BOT_SOURCE_CYPRESS", result)

    def test_unknown_policy_never_supplies_readable_or_public_claims(self):
        self._conversation(4, 10, "Test Member", "unknown", "My unknown note is " + self.UNKNOWN + ".")
        self.conn.commit()
        result = self._run()
        self._assert_sent(result)
        self._assert_not_readable(self.UNKNOWN, result)
        self._assert_no_public_authority(self.UNKNOWN, result)

    def test_fresh_correction_removes_obsolete_evidence_after_prior_materialization(self):
        self._assert_sent(self._run())
        self.archives.clear()
        self.recommendations.clear()
        self.conn.execute("UPDATE conversations SET content=? WHERE id=1", ("My current mix is " + self.NEW + ".",))
        self.conn.commit()
        result = self._run()
        self._assert_sent(result)
        self._assert_absent_from_output(self.OLD, result)
        self.assertTrue(self.NEW in self._text(self.archives[-1]), f"{self.NEW} absent from corrected archive")

    def test_fresh_privacy_withdrawal_removes_previously_materialized_evidence(self):
        self._assert_sent(self._run())
        self.archives.clear()
        self.recommendations.clear()
        self.conn.execute("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=1")
        self.conn.commit()
        result = self._run()
        self._assert_not_readable(self.OLD, result)
        self._assert_no_public_authority(self.OLD, result)

    def test_fresh_sealed_or_protected_withdrawal_removes_prior_operational_evidence(self):
        for policy in ("sealed_test", "protected_system"):
            with self.subTest(policy=policy):
                self.conn.execute("UPDATE conversations SET channel_policy='public_home' WHERE id=1")
                self.conn.commit()
                self._assert_sent(self._run())
                self.archives.clear()
                self.recommendations.clear()
                self.conn.execute("UPDATE conversations SET channel_policy=? WHERE id=1", (policy,))
                self.conn.commit()
                result = self._run()
                self._assert_absent_from_output(self.OLD, result)
                self.archives.clear()
                self.recommendations.clear()

    def test_fresh_source_deletion_removes_previously_materialized_evidence(self):
        self._assert_sent(self._run())
        self.archives.clear()
        self.recommendations.clear()
        self.conn.execute("DELETE FROM conversations WHERE id=1")
        self.conn.commit()
        result = self._run()
        self._assert_absent_from_output(self.OLD, result)

    def test_unrelated_member_originals_never_become_subject_evidence(self):
        self.conn.execute("UPDATE user_profiles SET display_name='Other Member',preferred_name='Other Member' WHERE user_id=20")
        self.conn.execute("UPDATE conversations SET user_name='Other Member' WHERE user_id=20")
        self.conn.commit()
        result = self._run()
        self._assert_sent(result)
        self._assert_absent_from_output(self.OTHER, result)

    def test_question_joke_and_speculation_are_not_public_factual_claims(self):
        for row_id, text in (
            (4, "Could I perform QUESTION_PLAN_BIRCH next week?"),
            (5, "Just joking: I won JOKE_AWARD_SPRUCE."),
            (6, "Maybe I will collaborate on SPECULATION_FIR; nothing is agreed."),
        ):
            self._conversation(row_id, 10, "Test Member", "public_home", text)
        self.conn.commit()
        result = self._run()
        self._assert_sent(result)
        archive = self.archives[-1]
        public_claims = {
            "possibilities": archive.get("publicSafePossibilities"),
            "caseClaims": archive["sourceFileCaseReportV1"].get("publicSafeClaims"),
            "analystClaims": archive["subjectAnalystReadV1"].get("publicSafeClaims"),
            "draftIngredients": archive["subjectAnalystReadV1"].get("draftIngredients"),
            "publicUseNow": archive["dossierCompletionReadV1"].get("publicSafeToUseNow"),
        }
        for marker in ("QUESTION_PLAN_BIRCH", "JOKE_AWARD_SPRUCE", "SPECULATION_FIR"):
            with self.subTest(marker=marker):
                self.assertFalse(marker in self._text(public_claims), f"{marker} present in public claim fields")

    def _late_change(self, sql, parameters=(), *, after_archive=False, unrelated=False):
        original_builder = enrichment.build_source_file_archive_payload
        changed = []

        def mutate():
            self.conn.execute(sql, parameters)
            self.conn.commit()
            changed.append(True)

        def build_after_change(packet, **kwargs):
            self.assertTrue(self.OLD in self._text(packet), "Fixture must reach the stale-packet boundary")
            mutate()
            return original_builder(packet, **kwargs)

        def archive_then_change(payload):
            self.assertTrue(self.OLD in self._text(payload), "Fixture must archive before its original changes")
            receipt = self._archive(payload)
            mutate()
            return receipt

        if after_archive:
            result = self._run(archive_sender=archive_then_change)
        else:
            with mock.patch.object(enrichment, "build_source_file_archive_payload", side_effect=build_after_change):
                result = self._run()
        self.assertEqual(changed, [True], "The requested delivery boundary must be exercised once")
        if unrelated:
            self._assert_sent(result)
            self.assertTrue(self.OLD in self._text(self.archives[-1]), "Unrelated change lost current subject evidence")
            return result
        if after_archive:
            self.assertEqual(len(self.archives), 1, "The successful pre-change archive must be retained")
            self.assertFalse(bool(self.recommendations), "Stale recommendation effect after archive")
            self.assertFalse(result["sent"], "Changed-source delivery cannot report full success")
            return result
        # The original changed before either route could deliver this materialization.
        for label, payloads in (("archive", self.archives), ("recommendation", self.recommendations)):
            with self.subTest(output=label):
                self.assertFalse(self.OLD in self._text(payloads), f"{self.OLD} present in late-change {label}")
        return result

    def test_correction_between_collection_and_archive_send_rejects_stale_payload(self):
        self._late_change("UPDATE conversations SET content=? WHERE id=1", ("My current mix is " + self.NEW + ".",))

    def test_privacy_change_between_collection_and_archive_send_rejects_stale_payload(self):
        self._late_change("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=1")

    def test_deletion_between_collection_and_archive_send_rejects_stale_payload(self):
        self._late_change("DELETE FROM conversations WHERE id=1")

    def test_sealed_or_protected_change_before_archive_rejects_stale_payload(self):
        for policy in ("sealed_test", "protected_system"):
            with self.subTest(policy=policy):
                self.conn.execute("UPDATE conversations SET channel_policy='public_home' WHERE id=1")
                self.conn.commit()
                self.archives.clear()
                self.recommendations.clear()
                self._late_change("UPDATE conversations SET channel_policy=? WHERE id=1", (policy,))

    def test_correction_after_archive_success_stops_recommendation(self):
        self._late_change("UPDATE conversations SET content=? WHERE id=1",
                          ("My current mix is " + self.NEW + ".",), after_archive=True)

    def test_privacy_change_after_archive_success_stops_recommendation(self):
        self._late_change("UPDATE conversations SET channel_policy='internal_controlled' WHERE id=1", after_archive=True)

    def test_deletion_after_archive_success_stops_recommendation(self):
        self._late_change("DELETE FROM conversations WHERE id=1", after_archive=True)

    def test_sealed_or_protected_change_after_archive_success_stops_recommendation(self):
        for policy in ("sealed_test", "protected_system"):
            with self.subTest(policy=policy):
                self.conn.execute("UPDATE conversations SET channel_policy='public_home' WHERE id=1")
                self.conn.commit()
                self.archives.clear()
                self.recommendations.clear()
                self._late_change("UPDATE conversations SET channel_policy=? WHERE id=1", (policy,), after_archive=True)

    def test_unrelated_member_changes_do_not_block_subject_delivery(self):
        for after_archive in (False, True):
            with self.subTest(after_archive=after_archive):
                self.archives.clear()
                self.recommendations.clear()
                self._late_change("UPDATE conversations SET content=? WHERE id=3",
                                  ("My unrelated revision is NEIGHBOR_UPDATE_OAK.",),
                                  after_archive=after_archive, unrelated=True)

    def _change_source_outside_recent_window(self, *, after_archive):
        self._run(dry_run=True)
        self.conn.executemany(
            "INSERT INTO conversations VALUES (?,?,?,1,'general','public_home','user',?,?)",
            [(1000 + idx, 20, "Neighbor Artist", "Unrelated recent discussion.", "2030-01-01T00:00:00+00:00")
             for idx in range(801)],
        )
        self.conn.executemany(
            "INSERT INTO conversations VALUES (?,10,'Test Member',1,'general','public_home','user',?,?)",
            [(2000 + idx, "My newer mix is PUBLIC_TRACK_SLOT_" + str(idx), "2029-01-01T00:00:00+00:00")
             for idx in range(70)],
        )
        self.conn.commit()
        original_builder = enrichment.build_source_file_archive_payload
        changed = []

        def mutate():
            self.conn.execute("UPDATE conversations SET content=? WHERE id=1", ("My corrected mix is " + self.NEW,))
            self.conn.commit()
            changed.append(True)

        def build_then_change(packet, **kwargs):
            payload = original_builder(packet, **kwargs)
            mutate()
            return payload

        def archive_then_change(payload):
            receipt = self._archive(payload)
            mutate()
            return receipt

        if after_archive:
            result = self._run(archive_sender=archive_then_change)
        else:
            with mock.patch.object(enrichment, "build_source_file_archive_payload", side_effect=build_then_change):
                result = self._run()
        self.assertEqual(changed, [True])
        self.assertEqual(len(self.archives), int(after_archive), "Old selected source must be checked at the requested boundary")
        self.assertFalse(bool(self.recommendations), "Old selected source bypassed recommendation validation")
        self.assertFalse(result["sent"])
        self.assertEqual(result["status"], "source_changed_before_" + ("recommendation" if after_archive else "archive"))

    def test_old_selected_source_change_before_archive_stops_delivery(self):
        self._change_source_outside_recent_window(after_archive=False)

    def test_old_selected_source_change_after_archive_stops_recommendation(self):
        self._change_source_outside_recent_window(after_archive=True)

    def _other_reader_late_change(self, *, after_archive):
        original_collector = enrichment.collect_source_enrichment_evidence

        def collect_without_snapshot(*args, **kwargs):
            evidence = original_collector(*args, **kwargs)
            # Isolate the originals selected by the other real readers.
            evidence["_sourceReadSnapshot"] = []
            return evidence

        with mock.patch.object(enrichment, "collect_source_enrichment_evidence", side_effect=collect_without_snapshot):
            result = self._late_change("DELETE FROM conversations WHERE id=1", after_archive=after_archive)
        self.assertFalse(result["sent"])
        self.assertEqual(result["status"], "source_changed_before_" + ("recommendation" if after_archive else "archive"))

    def test_other_reader_originals_are_checked_before_archive(self):
        self._other_reader_late_change(after_archive=False)

    def test_other_reader_originals_are_checked_before_recommendation(self):
        self._other_reader_late_change(after_archive=True)

    def test_dry_run_retains_review_packet_without_external_delivery(self):
        result = self._run(dry_run=True)
        self.assertFalse(result["sent"], "Dry run must not report delivery")
        self.assertFalse(bool(self.archives), "Dry run called the archive sender")
        self.assertFalse(bool(self.recommendations), "Dry run called the recommendation sender")
        self.assertTrue(self.OLD in self._text(result), "Dry run lost current subject evidence")
        self._assert_not_readable(self.PRIVATE, result)
        self._assert_no_public_authority(self.PRIVATE, result)
        self._assert_absent_from_output(self.OTHER, result)


if __name__ == "__main__":
    unittest.main()
