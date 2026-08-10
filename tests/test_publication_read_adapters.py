import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import bnl_journal as journal
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
import bnl_website_relay_state as relay
from bnl_shared_brain_synthesis import render_packet_context
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    build_evaluation_report,
    build_packet,
    revalidate_packet,
)


NOW = "2026-08-10T10:01:00Z"


def control_snapshot(
    *,
    public_excluded=(),
    memory_excluded=(),
    digest="a" * 64,
    observed_at="2026-08-10T10:00:00Z",
    fresh_until="2026-08-10T10:02:00Z",
):
    return journal.JournalControlSnapshot(
        snapshot_version=1,
        revision="2026-08-10T09:59:00Z",
        digest=digest,
        observed_at=observed_at,
        fresh_until=fresh_until,
        fresh_for_seconds=120,
        public_excluded_entry_ids=tuple(sorted(public_excluded)),
        memory_excluded_entry_ids=tuple(sorted(memory_excluded)),
    )


class PublicationReadAdapterTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        self.db_path = handle.name
        handle.close()
        journal.ensure_schema(self.db_path)
        relay.ensure_schema(self.db_path)
        self.conn = sqlite3.connect(self.db_path)

    def tearDown(self):
        self.conn.close()
        os.unlink(self.db_path)

    def add_journal(
        self,
        entry_id,
        *,
        revision=1,
        title="Copper Antennas",
        excerpt="The room returned to ceramic receivers.",
        body="A ceramic receiver found a clean carrier.",
        published_at="2026-08-02T01:00:00Z",
        lifecycle="published",
    ):
        section_values = [{"heading": "Carrier Notes", "body": body}]
        sections = json.dumps(
            section_values,
            sort_keys=True,
            separators=(",", ":"),
        )
        content_hash = hashlib.sha256(
            "|".join((title, excerpt, sections)).encode()
        ).hexdigest()
        authored_at = "2026-08-02T00:00:00Z"
        source_window_start = "2026-08-01T00:00:00Z"
        source_window_end = "2026-08-02T00:00:00Z"
        public_payload = {
            "contractVersion": 1,
            "kind": "journal_entry",
            "entry": {
                "entryId": entry_id,
                "revision": revision,
                "entryKind": "daily",
                "title": title,
                "excerpt": excerpt,
                "sections": section_values,
                "authoredAt": authored_at,
                "sourceWindowStart": source_window_start,
                "sourceWindowEnd": source_window_end,
                "contentHash": content_hash,
            },
        }
        canonical_payload = json.dumps(
            public_payload,
            sort_keys=True,
            separators=(",", ":"),
        )
        self.conn.execute(
            """
            INSERT INTO bnl_journal_entries(
              entry_id,revision,guild_id,lifecycle_state,title,excerpt,
              sections_json,public_payload_json,canonical_payload_bytes,
              content_hash,source_window_start,source_window_end,authored_at,
              approved_at,published_at,review_reason,delivery_status,
              delivery_http_status,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                entry_id,
                revision,
                1,
                lifecycle,
                title,
                excerpt,
                sections,
                canonical_payload,
                canonical_payload.encode(),
                content_hash,
                source_window_start,
                source_window_end,
                authored_at,
                "2026-08-02T00:30:00Z",
                published_at if lifecycle == "published" else None,
                None,
                "published" if lifecycle == "published" else None,
                200 if lifecycle == "published" else 0,
                "2026-08-02T00:00:00Z",
                "2026-08-02T01:00:00Z",
            ),
        )
        return content_hash

    def add_relay(
        self,
        relay_id,
        *,
        message="A ceramic carrier cleared the public relay.",
        directive="Tune the receiver toward the next clean signal.",
        lane="current_signal",
        event_type="public_discord_activity",
        published_at="2026-08-03T01:00:00Z",
    ):
        self.conn.execute(
            """
            INSERT INTO website_relay_history(
              relay_id,guild_id,public_message,public_directive,mode,
              relay_lane,event_type,highest_source_conversation_id,
              normalized_message,semantic_family,published_timestamp
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                relay_id,
                1,
                message,
                directive,
                "OBSERVATION",
                lane,
                event_type,
                20,
                relay.normalize_text(message),
                relay.semantic_family(message),
                published_at,
            ),
        )

    def test_control_snapshot_requires_persisted_fresh_complete_contract(self):
        payload = {
            "contractVersion": 1,
            "persisted": True,
            "controlSnapshotVersion": 1,
            "controlRevision": "2026-08-10T09:59:00Z",
            "controlDigest": "b" * 64,
            "controlObservedAt": "2026-08-10T10:00:00Z",
            "controlFreshUntil": "2026-08-10T10:02:00Z",
            "controlFreshForSeconds": 120,
            "publicExcludedEntryIds": ["journal_hidden"],
            "memoryExcludedEntryIds": ["journal_reuse_off"],
        }
        snapshot, reason = journal.parse_journal_control_snapshot(
            payload,
            now=NOW,
        )
        self.assertEqual("", reason)
        self.assertEqual(("journal_hidden",), snapshot.public_excluded_entry_ids)
        stale, reason = journal.parse_journal_control_snapshot(
            payload,
            now="2026-08-10T10:03:00Z",
        )
        self.assertIsNone(stale)
        self.assertEqual("control_snapshot_stale", reason)
        snapshot, reason = journal.parse_journal_control_snapshot(
            {**payload, "persisted": False},
            now=NOW,
        )
        self.assertIsNone(snapshot)
        self.assertEqual("control_snapshot_unpersisted", reason)

    def test_journal_exact_identity_title_date_and_latest_published_revision(self):
        entry_id = "journal_revision_case"
        self.add_journal(entry_id, revision=1, title="Original Carrier")
        self.add_journal(
            entry_id,
            revision=2,
            title="Revised Carrier",
            published_at="2026-08-04T01:00:00Z",
        )
        self.add_journal(
            entry_id,
            revision=3,
            title="Unpublished Rewrite",
            lifecycle="draft",
        )
        snapshot = control_snapshot()
        by_id = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text=f"show Journal entry {entry_id}",
            control_snapshot=snapshot,
            now=NOW,
        )
        self.assertEqual("eligible", by_id.status)
        self.assertEqual(2, by_id.publications[0].revision)
        by_title = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text='show the Journal titled "Revised Carrier"',
            control_snapshot=snapshot,
            now=NOW,
        )
        self.assertEqual("exact_title", by_title.query_mode)
        self.assertEqual(entry_id, by_title.publications[0].entry_id)
        by_date = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text="what did the Journal publish on 2026-08-04?",
            control_snapshot=snapshot,
            now=NOW,
        )
        self.assertEqual("exact_date", by_date.query_mode)
        self.assertEqual(entry_id, by_date.publications[0].entry_id)

    def test_journal_public_visibility_and_reuse_are_independent(self):
        entry_id = "journal_visibility_case"
        self.add_journal(entry_id, title="Public Ceramic Log")
        exact_snapshot = control_snapshot(memory_excluded=(entry_id,))
        exact = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text='show the Journal titled "Public Ceramic Log"',
            control_snapshot=exact_snapshot,
            now=NOW,
        )
        self.assertEqual("eligible", exact.status)
        topic = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text="what has the Journal said about receivers?",
            control_snapshot=exact_snapshot,
            now=NOW,
        )
        self.assertEqual("memory_ineligible", topic.status)
        hidden = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text=f"show Journal entry {entry_id}",
            control_snapshot=control_snapshot(public_excluded=(entry_id,)),
            now=NOW,
        )
        self.assertEqual("public_hidden", hidden.status)
        unavailable = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text=f"show Journal entry {entry_id}",
            control_snapshot=None,
            now=NOW,
        )
        self.assertEqual("control_snapshot_unavailable", unavailable.status)
        self.conn.execute(
            """
            UPDATE bnl_journal_entries
            SET canonical_payload_bytes=?
            WHERE entry_id=?
            """,
            (b"{}", entry_id),
        )
        contradictory = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text=f"show Journal entry {entry_id}",
            control_snapshot=control_snapshot(),
            now=NOW,
        )
        self.assertEqual("source_invalid", contradictory.status)

    def test_relay_permanent_history_and_site_manual_provenance(self):
        for index in range(30):
            self.add_relay(
                f"bnl-history-{index:02d}",
                message=f"Permanent carrier record {index}.",
                published_at=f"2026-07-{index + 1:02d}T01:00:00Z",
            )
        oldest = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text="show relay bnl-history-00",
        )
        self.assertEqual("eligible", oldest.status)
        self.assertEqual("bnl-history-00", oldest.publications[0].relay_id)
        by_date = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text="what Relay was published on 2026-07-01?",
        )
        self.assertEqual("exact_date", by_date.query_mode)
        self.assertEqual("bnl-history-00", by_date.publications[0].relay_id)
        by_topic = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text="show recent Relay history about carrier records",
        )
        self.assertEqual("topic", by_topic.query_mode)
        self.assertTrue(by_topic.publications)

        manual_id = "bnl-manual-001"
        self.add_relay(
            manual_id,
            message="Manual projection text.",
            lane="hydrated",
            event_type="approved_canon",
            published_at="2026-08-06T01:00:00Z",
        )
        missing = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text=f"show relay {manual_id}",
        )
        self.assertEqual("provenance_unapproved", missing.status)
        self.conn.execute(
            """
            INSERT INTO website_relay_attempts(
              attempt_id,guild_id,trigger,source_class,started_at,
              completed_at,outcome,reason,aggregate_source_counts,cursor,
              highest_eligible_conversation_id,accepted_relay_id,
              prepared_relay_id,website_published_at,idempotent
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "attempt-manual-001",
                1,
                "scheduled",
                "approved_canon",
                "2026-08-06T00:59:00Z",
                "2026-08-06T01:00:01Z",
                "published",
                "",
                "{}",
                0,
                0,
                manual_id,
                manual_id,
                "2026-08-06T01:00:00Z",
                0,
            ),
        )
        misleading = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text=f"show relay {manual_id}",
        )
        self.assertEqual("provenance_unapproved", misleading.status)
        self.conn.execute(
            """
            UPDATE website_relay_attempts
            SET trigger='owner_manual'
            WHERE attempt_id='attempt-manual-001'
            """
        )
        approved = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text=f"show relay {manual_id}",
        )
        self.assertEqual("eligible", approved.status)
        self.assertEqual(
            "site_manual_owner_receipt",
            approved.publications[0].provenance_kind,
        )

    def test_relay_attempts_and_presence_are_not_accepted_speech(self):
        self.conn.execute(
            """
            INSERT INTO website_relay_attempts(
              attempt_id,guild_id,trigger,source_class,started_at,
              completed_at,outcome,reason,aggregate_source_counts,cursor,
              highest_eligible_conversation_id,accepted_relay_id,
              prepared_relay_id,website_published_at,idempotent
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "attempt-failed-only",
                1,
                "scheduled",
                "approved_canon",
                "2026-08-06T00:00:00Z",
                "2026-08-06T00:00:01Z",
                "failed",
                "provider_failed",
                "{}",
                0,
                0,
                "",
                "bnl-failed-only",
                "",
                0,
            ),
        )
        failed = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text="show relay bnl-failed-only",
        )
        self.assertEqual("not_found", failed.status)
        self.add_relay(
            "bnl-presence-001",
            message="BNL-01 is online.",
            lane="presence",
            event_type="online",
        )
        presence = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text="show relay bnl-presence-001",
        )
        self.assertEqual("presence_only", presence.status)


class PublicationPacketIntegrationTests(PublicationReadAdapterTests):
    def setUp(self):
        super().setUp()
        ledger.ensure_memory_ledger_schema(self.conn)
        moments.ensure_moment_schema(self.conn)
        relationships.ensure_relationship_v2_schema(self.conn)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversations (
              id INTEGER PRIMARY KEY,guild_id INTEGER,user_id INTEGER,
              user_name TEXT,role TEXT,content TEXT,channel_id INTEGER,
              channel_policy TEXT,route_mode TEXT NOT NULL,timestamp TEXT
            )
            """
        )
        self.flags = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }
        self.env = mock.patch.dict(os.environ, self.flags, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        super().tearDown()

    def request(self, text, *, snapshot=None, control_status="not_requested"):
        return IntelligencePacketRequest(
            guild_id=1,
            subject_user_id=7,
            route_mode="normal_chat",
            conversation_surface="mention_or_reply",
            channel_id=10,
            channel_policy="public_home",
            visibility_allowance="public_safe",
            user_text=text,
            direct_state="direct",
            now=NOW,
            journal_control_snapshot=snapshot,
            journal_control_status=control_status,
        )

    def test_packet_adapters_are_publication_only_and_revalidate_mutation(self):
        journal_id = "journal_packet_001"
        relay_id = "bnl-packet-001"
        self.add_journal(journal_id, title="Packet Carrier")
        self.add_relay(relay_id)
        snapshot = control_snapshot()
        journal_packet = build_packet(
            self.conn,
            self.request(
                f"show Journal entry {journal_id}",
                snapshot=snapshot,
                control_status="valid",
            ),
            persist=True,
            environ=self.flags,
        )
        self.assertIsNotNone(journal_packet)
        item = next(
            item
            for item in journal_packet.items
            if item.lane == "journal_publication"
        )
        self.assertEqual("publication_projection", item.usage)
        self.assertEqual((), item.root_identities)
        self.assertEqual((), item.occurrence_identities)
        self.assertEqual("", item.canon_status)
        self.assertFalse(journal_packet.diagnostics.invalid_invariants)
        rendered, lanes, count, _digests = render_packet_context(journal_packet)
        self.assertGreaterEqual(count, 1)
        self.assertIn(("journal_publication", 1), lanes)
        self.assertIn("zero independent fact or recurrence weight", rendered)
        report = build_evaluation_report(self.conn, guild_id=1)
        self.assertEqual({"eligible": 1}, report["journalQueryStatusCounts"])
        self.assertEqual({"valid": 1}, report["journalControlStatusCounts"])
        self.assertEqual(1, report["publicationProjectionTotal"])

        same_authority = control_snapshot(
            digest=snapshot.digest,
            observed_at="2026-08-10T10:00:30Z",
            fresh_until="2026-08-10T10:02:30Z",
        )
        valid = revalidate_packet(
            self.conn,
            journal_packet,
            environ=self.flags,
            journal_control_snapshot=same_authority,
            journal_control_snapshot_provided=True,
        )
        self.assertTrue(valid.valid)
        hidden = control_snapshot(
            public_excluded=(journal_id,),
            digest="c" * 64,
        )
        changed = revalidate_packet(
            self.conn,
            journal_packet,
            environ=self.flags,
            journal_control_snapshot=hidden,
            journal_control_snapshot_provided=True,
        )
        self.assertFalse(changed.valid)
        self.assertEqual("source_changed", changed.status)

        relay_packet = build_packet(
            self.conn,
            self.request(f"show relay {relay_id}"),
            persist=False,
            environ=self.flags,
        )
        relay_item = next(
            item
            for item in relay_packet.items
            if item.lane == "relay_publication"
        )
        self.assertEqual((), relay_item.root_identities)
        self.conn.execute(
            "UPDATE website_relay_history SET public_message=? WHERE relay_id=?",
            ("Mutated after selection.", relay_id),
        )
        changed = revalidate_packet(
            self.conn,
            relay_packet,
            environ=self.flags,
        )
        self.assertFalse(changed.valid)
        self.assertEqual("source_changed", changed.status)

    def test_missing_hidden_and_unapproved_publications_fail_packet_closed(self):
        missing = build_packet(
            self.conn,
            self.request(
                "show Journal entry journal_missing_001",
                snapshot=control_snapshot(),
                control_status="valid",
            ),
            persist=False,
            environ=self.flags,
        )
        self.assertIn(
            "journal_publication_query_failed_closed",
            missing.diagnostics.invalid_invariants,
        )
        manual_id = "bnl-packet-manual-001"
        self.add_relay(
            manual_id,
            lane="hydrated",
            event_type="approved_canon",
        )
        unapproved = build_packet(
            self.conn,
            self.request(f"show relay {manual_id}"),
            persist=False,
            environ=self.flags,
        )
        self.assertIn(
            "relay_publication_query_failed_closed",
            unapproved.diagnostics.invalid_invariants,
        )


if __name__ == "__main__":
    unittest.main()
