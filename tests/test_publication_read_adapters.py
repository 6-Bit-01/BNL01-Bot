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
from bnl_shared_brain_synthesis import (
    audit_ordinary_chat_candidate_claims,
    begin_single_packet_run,
    build_ordinary_chat_basis,
    evaluate_single_packet_response,
    ordinary_chat_task_support_plan,
    parse_ordinary_chat_response_contract,
    render_packet_context,
    validate_ordinary_chat_response_contract,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    PacketFrameSubject,
    PacketFrameTask,
    build_evaluation_report,
    build_packet,
    revalidate_packet,
)
from bnl_unified_response_assessment import (
    build_situation_frame_v1,
    build_unified_response_assessment,
    revalidate_situation_frame,
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

    def test_explicit_latest_journal_prefers_publication_time_over_topic_density(self):
        self.add_journal(
            "journal_older_dense",
            title="BARCODE Radio Broadcast Queue Audience",
            excerpt="BARCODE Radio broadcast queue audience report.",
            body="The broadcast queue and audience filled the report.",
            published_at="2026-08-04T01:00:00Z",
        )
        self.add_journal(
            "journal_newer_light",
            title="New BARCODE Radio Note",
            excerpt="A newer BARCODE Radio note was published.",
            body="The new note reached the public Journal.",
            published_at="2026-08-05T01:00:00Z",
        )
        selected = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text=(
                "what did the latest Journal say about BARCODE Radio "
                "broadcast queue audience?"
            ),
            control_snapshot=control_snapshot(),
            now=NOW,
        )

        self.assertEqual(selected.status, "eligible")
        self.assertEqual(selected.query_mode, "latest")
        self.assertEqual(len(selected.publications), 1)
        self.assertEqual(
            selected.publications[0].entry_id,
            "journal_newer_light",
        )

    def test_natural_journal_topic_preserves_recent_publications_for_reasoning(self):
        self.add_journal(
            "journal_broadcast_older",
            title="August 21 Broadcast Notes",
            excerpt="The broadcast ran long and the video feed stuttered.",
            body="Several BARCODE Vol. 1 contributors visited the session.",
            published_at="2026-08-22T01:00:00Z",
        )
        self.add_journal(
            "journal_broadcast_newer",
            title="August 28 Broadcast Notes",
            excerpt="The broadcast logged 48 rostered tracks.",
            body="Operational and community activity were recorded together.",
            published_at="2026-08-29T01:00:00Z",
        )

        selected = journal.select_published_journal_entries_on_connection(
            self.conn,
            guild_id=1,
            user_text="What did the Journal say about the last show?",
            control_snapshot=control_snapshot(),
            now=NOW,
        )

        self.assertEqual("eligible", selected.status)
        self.assertEqual("topic", selected.query_mode)
        self.assertEqual(
            (
                "journal_broadcast_newer",
                "journal_broadcast_older",
            ),
            tuple(item.entry_id for item in selected.publications),
        )

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

    def test_explicit_latest_relay_prefers_publication_time_over_topic_density(self):
        self.add_relay(
            "bnl-relay-older-dense",
            message=(
                "BARCODE Radio broadcast queue audience report covered the "
                "broadcast queue and audience."
            ),
            published_at="2026-08-04T01:00:00Z",
        )
        self.add_relay(
            "bnl-relay-newer-light",
            message="A newer BARCODE Radio signal reached the Relay.",
            published_at="2026-08-05T01:00:00Z",
        )

        selected = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text=(
                "what did the latest Relay say about BARCODE Radio "
                "broadcast queue audience?"
            ),
        )

        self.assertEqual("eligible", selected.status)
        self.assertEqual("latest", selected.query_mode)
        self.assertEqual(1, len(selected.publications))
        self.assertEqual(
            "bnl-relay-newer-light",
            selected.publications[0].relay_id,
        )

    def test_natural_relay_topic_preserves_recent_publications_for_reasoning(self):
        self.add_relay(
            "bnl-broadcast-older",
            message=(
                "The August 21 broadcast ran long and the video feed "
                "stuttered."
            ),
            directive="Retain the preceding session as public context.",
            published_at="2026-08-22T01:00:00Z",
        )
        self.add_relay(
            "bnl-broadcast-newer",
            message="The August 28 broadcast logged 48 rostered tracks.",
            directive="Use the newest accepted session record first.",
            published_at="2026-08-29T01:00:00Z",
        )

        selected = relay.select_accepted_relay_publications_on_connection(
            self.conn,
            guild_id=1,
            user_text="What did the Relay say about the last show?",
        )

        self.assertEqual("eligible", selected.status)
        self.assertEqual("topic", selected.query_mode)
        self.assertEqual(
            ("bnl-broadcast-newer", "bnl-broadcast-older"),
            tuple(item.relay_id for item in selected.publications),
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

    def test_mixed_journal_queue_full_chain_survives_live_budget(self):
        text = (
            "What did the Journal say about the queue, and is the queue "
            "open right now?"
        )
        queue_snapshot = (
            "Current BARCODE queue snapshot:\n"
            "- accessScope=public; readOnly=true\n"
            "- queue open: false"
        )
        changed_queue_snapshot = queue_snapshot.replace("false", "true")
        snapshot = control_snapshot()
        self.add_journal(
            "journal_queue_handoff",
            title="Queue Rehearsal Notes",
            excerpt=(
                "The Journal recorded the queue rehearsal as read-only."
            ),
            body=(
                "The published rehearsal kept historical commentary "
                "separate from current queue state."
            ),
        )
        self.conn.commit()
        flags = {
            **self.flags,
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "false",
            "BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED": "false",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED": "true",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_GUILD_IDS": "1",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_USER_IDS": "7",
            "BNL_ORDINARY_CHAT_SINGLE_PACKET_CHANNEL_IDS": "10",
        }
        with mock.patch.dict(os.environ, flags, clear=False):
            frame = build_situation_frame_v1(
                route_allowed=True,
                route_mode="normal_chat",
                conversation_surface="mention_or_reply",
                channel_policy="public_home",
                current_text=text,
                current_speaker_user_ids=(7,),
                current_speaker_labels=("Test Member",),
                addressee_kinds=("discord_mention",),
                source_message_ids=(301,),
                explicit_mention_count=1,
                referent_status="not_applicable",
                response_act="answer",
                packet_revision="mixed_journal_queue_full_chain",
            )
            self.assertEqual(frame.status, "resolved")
            self.assertEqual(
                tuple(task.object_kind for task in frame.tasks),
                ("journal", "queue"),
            )
            request = IntelligencePacketRequest(
                guild_id=1,
                subject_user_id=0,
                route_mode="normal_chat",
                conversation_surface="mention_or_reply",
                channel_id=10,
                channel_name="bnl-testing",
                channel_policy="public_home",
                visibility_allowance="public_safe",
                user_text=text,
                participant_user_ids=(7,),
                direct_state="direct",
                budget_chars=2400,
                conversation_evidence=(
                    PacketConversationEvidence(
                        text=text,
                        speaker_user_id=7,
                        speaker_label="Test Member",
                        current_turn=True,
                    ),
                ),
                operational_context_snapshot=queue_snapshot,
                operational_context_kind="website_read_model",
                operational_context_authorized=True,
                now=NOW,
                frame_schema_version=frame.schema_version,
                frame_revision=frame.frame_revision,
                frame_input_evidence_digest=frame.input_evidence_digest,
                frame_status=frame.status,
                frame_subject_requirement=frame.subject_requirement,
                frame_subjects=tuple(
                    PacketFrameSubject(
                        user_id=subject.user_id,
                        entity_ref=subject.entity_ref,
                        label_hint=subject.label_hint,
                        binding_method=subject.binding_method,
                        confidence=subject.confidence,
                        role_hints=subject.role_hints,
                        domain_hints=subject.domain_hints,
                    )
                    for subject in frame.subjects
                ),
                frame_tasks=tuple(
                    PacketFrameTask(
                        task_id=task.task_id,
                        text_digest=task.text_digest,
                        task_kind=task.task_kind,
                        object_kind=task.object_kind,
                        authority_scope=task.authority_scope,
                        temporal_scope=task.temporal_scope,
                        currentness=task.currentness,
                        required_response_act=(
                            task.required_response_act
                        ),
                        subject_requirement=task.subject_requirement,
                        subject_indexes=task.subject_indexes,
                    )
                    for task in frame.tasks
                ),
                frame_role_hints=frame.role_hints,
                frame_domain_hints=frame.domain_hints,
                frame_event_ref=frame.event_ref,
                frame_event_relation=frame.event_relation,
                frame_task_kind=frame.task_kind,
                frame_object_kind=frame.object_kind,
                frame_phase=frame.phase,
                frame_temporal_scope=frame.temporal_scope,
                frame_currentness=frame.currentness,
                journal_control_snapshot=snapshot,
                journal_control_status="valid",
            )
            packet = build_packet(
                self.conn,
                request,
                persist=True,
                environ=flags,
            )
            packet_lanes = {item.lane for item in packet.items}
            self.assertIn("journal_publication", packet_lanes)
            self.assertIn("website_read_model", packet_lanes)
            self.assertFalse(packet.diagnostics.invalid_invariants)

            frame_revalidation = revalidate_situation_frame(
                frame,
                current_text=text,
                route_mode="normal_chat",
                conversation_surface="mention_or_reply",
                channel_policy="public_home",
                packet_source_snapshot_digest=(
                    packet.source_snapshot_digest
                ),
            )
            self.assertEqual(frame_revalidation.status, "valid")
            profile = packet.profile_sufficiency
            assessment = build_unified_response_assessment(
                guild_id=1,
                route_mode="normal_chat",
                channel_policy="public_home",
                conversation_surface="mention_or_reply",
                current_speaker_user_ids=(7,),
                participant_user_ids=(7,),
                speaker_labels=("Test Member",),
                prompt_lanes=packet.assessment_lanes,
                website_read_model_present=True,
                current_text=text,
                packet_selected_lanes=packet.assessment_lanes,
                packet_excluded_lanes=packet.assessment_exclusions,
                packet_conflict_reasons=(
                    packet.diagnostics.conflict_reasons
                ),
                packet_missing_lanes=packet.assessment_missing_lanes,
                packet_revalidation_status=(
                    packet.diagnostics.revalidation_status
                ),
                profile_sufficiency_status=profile.status,
                profile_sufficiency_met=profile.satisfied,
                profile_required_point_count=profile.required_point_count,
                profile_selected_point_count=profile.selected_point_count,
                profile_independent_root_count=(
                    profile.independent_root_count
                ),
                profile_independent_occurrence_count=(
                    profile.independent_occurrence_count
                ),
                profile_sufficiency_reasons=profile.reason_codes,
                situation_frame=frame,
                frame_revalidation=frame_revalidation,
            )
            self.assertEqual(assessment.comparison_status, "match")
            basis = build_ordinary_chat_basis(
                guild_id=1,
                user_id=7,
                channel_id=10,
                route_mode="normal_chat",
                channel_policy="public_home",
                current_direct=True,
                user_text=text,
                packet=packet,
                assessment=assessment,
                environ=flags,
            )
            self.assertIsNotNone(basis)
            plans = ordinary_chat_task_support_plan(basis)
            self.assertEqual(len(plans), 2)
            evidence_lanes = {
                evidence_id: lane
                for evidence_id, lane, _digest, _subjects in (
                    basis.rendered_evidence_refs
                )
            }
            self.assertEqual(
                {evidence_lanes[item] for item in plans[0].evidence_ids},
                {"journal_publication"},
            )
            self.assertEqual(
                {evidence_lanes[item] for item in plans[1].evidence_ids},
                {"website_read_model"},
            )
            contract = parse_ordinary_chat_response_contract(
                json.dumps(
                    {
                        "tasks": [
                            {
                                "taskId": plans[0].task_id,
                                "text": (
                                    "The Journal said the queue rehearsal "
                                    "stayed read-only."
                                ),
                                "supportKind": plans[0].support_kind,
                                "evidenceIds": list(
                                    plans[0].evidence_ids
                                ),
                            },
                            {
                                "taskId": plans[1].task_id,
                                "text": "The queue is closed right now.",
                                "supportKind": plans[1].support_kind,
                                "evidenceIds": list(
                                    plans[1].evidence_ids
                                ),
                            },
                        ]
                    }
                )
            )
            validation = validate_ordinary_chat_response_contract(
                basis,
                contract,
            )
            self.assertTrue(validation.valid)
            self.assertEqual(validation.covered_task_count, 2)
            natural_response = (
                "The Journal said the queue rehearsal stayed read-only. "
                "The queue is closed right now."
            )
            natural_run = begin_single_packet_run(
                self.conn,
                basis,
                prompt_ready=True,
                frame_revalidation_status=frame_revalidation.status,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            natural = evaluate_single_packet_response(
                self.conn,
                natural_run,
                response=natural_response,
                typed_contract_required=False,
                provider_call_count=1,
                corrective_call_count=0,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            self.assertTrue(natural.candidate_selected, natural)
            self.assertEqual(
                natural.candidate_claim_classifications,
                (
                    "authorized_evidence_supported",
                    "authorized_evidence_supported",
                ),
            )
            contracted, unsupported = audit_ordinary_chat_candidate_claims(
                basis,
                "The queue isn't open right now.",
            )
            self.assertEqual(unsupported, 0)
            self.assertEqual(
                contracted,
                ("authorized_evidence_supported",),
            )
            combined, unsupported = audit_ordinary_chat_candidate_claims(
                basis,
                (
                    "The Journal said the queue rehearsal stayed read-only "
                    "and the queue is closed right now."
                ),
            )
            self.assertEqual(unsupported, 0)
            self.assertEqual(
                combined,
                ("authorized_evidence_supported",),
            )
            contradictory_run = begin_single_packet_run(
                self.conn,
                basis,
                prompt_ready=True,
                frame_revalidation_status=frame_revalidation.status,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            contradictory = evaluate_single_packet_response(
                self.conn,
                contradictory_run,
                response=(
                    "The Journal said the queue rehearsal stayed read-only. "
                    "The queue is open right now."
                ),
                typed_contract_required=False,
                provider_call_count=1,
                corrective_call_count=0,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            self.assertFalse(
                contradictory.candidate_selected,
                contradictory,
            )
            self.assertEqual(
                contradictory.fallback_reason,
                "unsupported_packet_domain_claim",
            )
            accepted_run = begin_single_packet_run(
                self.conn,
                basis,
                prompt_ready=True,
                frame_revalidation_status=frame_revalidation.status,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            accepted = evaluate_single_packet_response(
                self.conn,
                accepted_run,
                response=contract.response,
                response_contract=contract,
                typed_contract_required=True,
                provider_call_count=1,
                corrective_call_count=0,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            self.assertTrue(accepted.candidate_selected)
            self.assertEqual(accepted.typed_task_coverage_count, 2)

            run = begin_single_packet_run(
                self.conn,
                basis,
                prompt_ready=True,
                frame_revalidation_status=frame_revalidation.status,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            self.assertTrue(run.prompt_applied)
            decision = evaluate_single_packet_response(
                self.conn,
                run,
                response=contract.response,
                response_contract=contract,
                typed_contract_required=True,
                provider_call_count=1,
                corrective_call_count=0,
                environ=flags,
                journal_control_snapshot=snapshot,
                journal_control_snapshot_provided=True,
                operational_context_snapshot=changed_queue_snapshot,
                operational_context_snapshot_provided=True,
            )
            self.assertFalse(decision.candidate_selected)
            self.assertEqual(
                decision.fallback_reason,
                "post_generation_source_changed",
            )
            self.assertEqual(decision.typed_contract_status, "valid")
            self.assertEqual(decision.typed_task_coverage_count, 2)

    def test_natural_relay_question_reaches_the_publication_packet(self):
        self.add_relay(
            "bnl-live-acceptance-older",
            message=(
                "The August 21 broadcast ran long and the video feed "
                "stuttered."
            ),
            published_at="2026-08-22T01:00:00Z",
        )
        self.add_relay(
            "bnl-live-acceptance-newer",
            message="The August 28 broadcast logged 48 rostered tracks.",
            published_at="2026-08-29T01:00:00Z",
        )

        packet = build_packet(
            self.conn,
            self.request("What did the Relay say about the last show?"),
            persist=False,
            environ=self.flags,
        )

        self.assertEqual("eligible", packet.diagnostics.relay_query_status)
        self.assertFalse(packet.diagnostics.invalid_invariants)
        self.assertEqual(2, packet.diagnostics.publication_projection_count)
        self.assertEqual(
            {
                "relay:bnl-live-acceptance-newer",
                "relay:bnl-live-acceptance-older",
            },
            {
                item.source_ref
                for item in packet.items
                if item.lane == "relay_publication"
            },
        )

    def test_latest_journal_revalidation_rejects_newer_matching_publication(self):
        snapshot = control_snapshot()
        self.add_journal(
            "journal_latest_old",
            title="BARCODE Radio Old Signal",
            excerpt="An older BARCODE Radio signal reached the Journal.",
            body="The older radio signal was published first.",
            published_at="2026-08-04T01:00:00Z",
        )
        packet = build_packet(
            self.conn,
            self.request(
                "what did the latest Journal say about BARCODE Radio?",
                snapshot=snapshot,
                control_status="valid",
            ),
            persist=False,
            environ=self.flags,
        )
        selected = next(
            item
            for item in packet.items
            if item.lane == "journal_publication"
        )
        self.assertEqual("journal:journal_latest_old:1", selected.source_ref)

        self.add_journal(
            "journal_latest_new",
            title="BARCODE Radio New Signal",
            excerpt="A newer BARCODE Radio signal reached the Journal.",
            body="The newer radio signal supersedes the latest selection.",
            published_at="2026-08-05T01:00:00Z",
        )
        self.conn.commit()

        changed = revalidate_packet(
            self.conn,
            packet,
            environ=self.flags,
            journal_control_snapshot=snapshot,
            journal_control_snapshot_provided=True,
        )
        self.assertFalse(changed.valid)
        self.assertEqual("source_changed", changed.status)
        self.assertGreaterEqual(changed.changed_source_count, 1)

    def test_latest_relay_revalidation_rejects_newer_matching_publication(self):
        self.add_relay(
            "bnl-latest-old",
            message="An older BARCODE Radio signal reached the Relay.",
            published_at="2026-08-04T01:00:00Z",
        )
        packet = build_packet(
            self.conn,
            self.request(
                "what did the latest Relay say about BARCODE Radio?"
            ),
            persist=False,
            environ=self.flags,
        )
        selected = next(
            item
            for item in packet.items
            if item.lane == "relay_publication"
        )
        self.assertEqual("relay:bnl-latest-old", selected.source_ref)

        self.add_relay(
            "bnl-latest-new",
            message="A newer BARCODE Radio signal reached the Relay.",
            published_at="2026-08-05T01:00:00Z",
        )
        self.conn.commit()

        changed = revalidate_packet(
            self.conn,
            packet,
            environ=self.flags,
        )
        self.assertFalse(changed.valid)
        self.assertEqual("source_changed", changed.status)
        self.assertGreaterEqual(changed.changed_source_count, 1)

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
