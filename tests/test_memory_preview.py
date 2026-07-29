import hashlib
from pathlib import Path
import sqlite3
import tempfile
import unittest

from bnl_canon_source_contract import (
    Confidence,
    SourceClass,
    Visibility,
)
import bnl_memory_ledger as ledger
from bnl_memory_preview import (
    MemoryPreviewRequest,
    PREVIEW_FACTUAL_PLACEHOLDER,
    evaluate_memory_preview,
    finalize_memory_preview,
    prepare_memory_preview,
    render_content_free_diagnostics,
    snapshots_equivalent,
)


class MemoryPreviewTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(
            Path(self.tempdir.name) / "production-memory.db"
        )
        with sqlite3.connect(self.db_path) as conn:
            ledger.ensure_memory_ledger_schema(conn)
            self._insert_public_conversation(
                conn,
                row_id=1,
                text=(
                    "I keep fixing the bot code and memory system "
                    "carefully."
                ),
                observed_at="2026-07-24T20:00:00+00:00",
            )
            self._insert_public_conversation(
                conn,
                row_id=2,
                text=(
                    "The website code needs another careful "
                    "troubleshooting pass."
                ),
                observed_at="2026-07-25T20:00:00+00:00",
            )
            conn.commit()

    def tearDown(self):
        self.tempdir.cleanup()

    def _insert_public_conversation(
        self,
        conn,
        *,
        row_id,
        text,
        observed_at,
        channel_policy="public_home",
        visibility=Visibility.PUBLIC,
        public_usable=True,
    ):
        result = ledger.insert_ledger_entry(
            conn,
            ledger.LedgerEntry(
                guild_id=1,
                source_table="conversations",
                source_row_id=row_id,
                source_revision=str(row_id),
                source_role="user",
                entry_type="observation",
                subject_key="discord_user:7",
                subject_display_name="Crow",
                predicate_key="conversation",
                value=text,
                source_class=SourceClass.PUBLIC_OBSERVATION,
                route_mode="normal_chat",
                channel_id=10,
                channel_name=(
                    "barcode-bot"
                    if channel_policy == "public_home"
                    else "bnl-testing"
                ),
                channel_policy=channel_policy,
                visibility=visibility,
                confidence=Confidence.MEDIUM,
                public_usable=public_usable,
                observed_at=observed_at,
                source_sequence=row_id,
                lifecycle_status="active",
                participants=(
                    ledger.LedgerParticipant(
                        "discord_user:7",
                        "Crow",
                        "author",
                        0,
                    ),
                ),
            ),
        )
        self.assertIn(result.outcome, {"inserted", "deduplicated"})

    def _request(self, wording="BNL-01, what am I all about?"):
        baseline_prompt = (
            "Current user request: %s\n"
            "BNL persona and BARCODE lore remain active.\n"
            "Current channel: #barcode-bot\n"
            "Current channel policy: public_home\n"
            "Durable memory context:\n%s\n"
            "Personal-recall route contract remains active."
            % (wording, PREVIEW_FACTUAL_PLACEHOLDER)
        )
        return MemoryPreviewRequest(
            source_db_path=self.db_path,
            guild_id=1,
            subject_user_id=7,
            subject_display_name="Crow",
            simulated_channel_id=10,
            wording=wording,
            baseline_prompt=baseline_prompt,
            now="2026-07-26T12:00:00+00:00",
        )

    def _source_hash(self):
        return hashlib.sha256(
            Path(self.db_path).read_bytes()
        ).hexdigest()

    def _ensure_conversation_context_schema(self, conn):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conversations (
              id INTEGER PRIMARY KEY,
              user_id INTEGER NOT NULL,
              user_name TEXT NOT NULL,
              guild_id INTEGER NOT NULL,
              role TEXT NOT NULL,
              content TEXT NOT NULL,
              timestamp TEXT NOT NULL,
              channel_name TEXT NOT NULL,
              channel_policy TEXT NOT NULL,
              channel_id INTEGER NOT NULL,
              message_id INTEGER
            )
            """
        )

    def _insert_conversation_context_row(
        self,
        conn,
        *,
        row_id,
        role,
        content,
        observed_at,
        channel_id=10,
        channel_name="barcode-bot",
        channel_policy="public_home",
        user_id=7,
        user_name="Crow",
    ):
        conn.execute(
            """
            INSERT INTO conversations(
              id,user_id,user_name,guild_id,role,content,timestamp,
              channel_name,channel_policy,channel_id,message_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(row_id),
                int(user_id),
                str(user_name),
                1,
                str(role),
                str(content),
                str(observed_at),
                str(channel_name),
                str(channel_policy),
                int(channel_id),
                int(row_id) + 1000,
            ),
        )

    def test_preview_mutates_only_the_memory_clone(self):
        source_hash_before = self._source_hash()
        prepared = prepare_memory_preview(self._request())
        try:
            self.assertTrue(prepared.ready)
            self.assertEqual(
                prepared.diagnostics.profile_status,
                "sparse",
            )
            self.assertEqual(
                dict(prepared.diagnostics.formation_outcomes),
                {"created": 1},
            )
            clone_candidate_count = prepared.connection.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_candidates
                """
            ).fetchone()[0]
            clone_packet_receipts = prepared.connection.execute(
                """
                SELECT COUNT(*)
                FROM memory_governance_intelligence_packet_runs
                """
            ).fetchone()[0]
            self.assertEqual(clone_candidate_count, 1)
            self.assertEqual(clone_packet_receipts, 1)

            evaluation = evaluate_memory_preview(
                prepared,
                baseline_response=(
                    "I only have a narrow grounded view so far."
                ),
                candidate_response=(
                    "You keep returning to software and technical "
                    "systems."
                ),
            )
            self.assertTrue(evaluation.candidate_selected)
            self.assertEqual(
                evaluation.candidate_member_point_count,
                1,
            )
            self.assertGreaterEqual(
                evaluation.candidate_member_root_count,
                2,
            )
            self.assertGreaterEqual(
                evaluation.candidate_member_occurrence_count,
                2,
            )
            self.assertTrue(
                finalize_memory_preview(
                    prepared,
                    evaluation,
                    final_response=evaluation.response,
                    guard_status="preview_guard_passed",
                )
            )
        finally:
            prepared.close()

        self.assertEqual(source_hash_before, self._source_hash())
        with sqlite3.connect(self.db_path) as source:
            self.assertEqual(
                source.execute(
                    """
                    SELECT COUNT(*)
                    FROM memory_ledger_knowledge_candidates
                    """
                ).fetchone()[0],
                0,
            )
            self.assertFalse(
                source.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='table'
                      AND name='memory_governance_intelligence_packet_runs'
                    """
                ).fetchone()
            )

    def test_fresh_snapshot_revalidates_and_detects_source_change(self):
        first = prepare_memory_preview(self._request())
        second = prepare_memory_preview(self._request())
        try:
            self.assertEqual(
                snapshots_equivalent(first, second),
                (True, "preview_source_unchanged"),
            )
        finally:
            second.close()

        with sqlite3.connect(self.db_path) as source:
            self._insert_public_conversation(
                source,
                row_id=3,
                text=(
                    "I am still testing code and fixing another "
                    "system issue."
                ),
                observed_at="2026-07-26T11:00:00+00:00",
            )
            source.commit()
        changed = prepare_memory_preview(self._request())
        try:
            equivalent, reason = snapshots_equivalent(first, changed)
            self.assertFalse(equivalent)
            self.assertEqual(reason, "preview_source_changed")
        finally:
            first.close()
            changed.close()

    def test_ambiguous_wording_requests_clarification_without_opening_db(self):
        prepared = prepare_memory_preview(
            self._request("BNL, what do you remember?")
        )
        self.assertFalse(prepared.ready)
        self.assertIsNone(prepared.connection)
        self.assertEqual(
            prepared.diagnostics.route_status,
            "needs_context",
        )
        self.assertEqual(
            prepared.diagnostics.route_reason,
            "recall_target_ambiguous",
        )

    def test_sealed_test_rows_never_participate_in_public_home_preview(self):
        with sqlite3.connect(self.db_path) as source:
            self._insert_public_conversation(
                source,
                row_id=3,
                text="I keep building synth music and radio tracks.",
                observed_at="2026-07-22T10:00:00+00:00",
                channel_policy="sealed_test",
                visibility=Visibility.SEALED_TEST,
                public_usable=False,
            )
            self._insert_public_conversation(
                source,
                row_id=4,
                text="The music mix and synth track need another pass.",
                observed_at="2026-07-23T10:00:00+00:00",
                channel_policy="sealed_test",
                visibility=Visibility.SEALED_TEST,
                public_usable=False,
            )
            source.commit()

        prepared = prepare_memory_preview(self._request())
        try:
            atomic_text = " ".join(
                item.text
                for item in prepared.packet.items
                if item.lane == "atomic_knowledge"
            ).lower()
        finally:
            prepared.close()

        self.assertIn("software and technical systems", atomic_text)
        self.assertNotIn("music and audio production", atomic_text)

    def test_context_v2_uses_real_public_roots_and_revalidates_rendered_pair(self):
        public_user = "Could we revisit the blue umbrella detail?"
        public_model = "Yes, we can revisit that blue umbrella detail."
        sealed_user = "The sealed chrysalis phrase belongs only in testing."
        sealed_model = "The sealed chrysalis phrase remains private."
        with sqlite3.connect(self.db_path) as source:
            self._ensure_conversation_context_schema(source)
            self._insert_conversation_context_row(
                source,
                row_id=101,
                role="user",
                content=public_user,
                observed_at="2026-07-26T11:50:00+00:00",
            )
            self._insert_conversation_context_row(
                source,
                row_id=102,
                role="model",
                content=public_model,
                observed_at="2026-07-26T11:51:00+00:00",
            )
            self._insert_conversation_context_row(
                source,
                row_id=103,
                role="user",
                content=sealed_user,
                observed_at="2026-07-26T11:52:00+00:00",
                channel_id=20,
                channel_name="bnl-testing",
                channel_policy="sealed_test",
            )
            self._insert_conversation_context_row(
                source,
                row_id=104,
                role="model",
                content=sealed_model,
                observed_at="2026-07-26T11:53:00+00:00",
                channel_id=20,
                channel_name="bnl-testing",
                channel_policy="sealed_test",
            )
            self._insert_public_conversation(
                source,
                row_id=101,
                text=public_user,
                observed_at="2026-07-26T11:50:00+00:00",
            )
            source.commit()

        source_hash = self._source_hash()
        first = prepare_memory_preview(self._request())
        try:
            self.assertTrue(first.ready)
            self.assertTrue(first.conversation_context_digest)
            self.assertIn(public_user, first.request.baseline_prompt)
            self.assertIn(public_model, first.request.baseline_prompt)
            self.assertNotIn(sealed_user, first.request.baseline_prompt)
            self.assertNotIn(sealed_model, first.request.baseline_prompt)
            self.assertIn(
                public_user,
                first.packet_owned_prompt.prompt,
            )
            context_items = tuple(
                item
                for item in first.packet.items
                if item.lane == "conversation_context"
            )
            self.assertEqual(
                tuple(item.source_ref for item in context_items),
                ("conversation:101",),
            )
            self.assertEqual(
                context_items[0].source_type,
                "conversation_row",
            )
            self.assertTrue(context_items[0].root_identities)
            self.assertTrue(context_items[0].occurrence_identities)
            self.assertTrue(
                any(
                    item.lane == "current_intent"
                    for item in first.packet.items
                )
            )
            self.assertTrue(
                any(
                    item.lane == "atomic_knowledge"
                    for item in first.packet.items
                )
            )
            diagnostics = "\n".join(
                render_content_free_diagnostics(first)
            )
            self.assertNotIn(public_user, diagnostics)
            self.assertNotIn(public_model, diagnostics)
            self.assertNotIn(sealed_user, diagnostics)
            self.assertEqual(source_hash, self._source_hash())

            with sqlite3.connect(self.db_path) as source:
                source.execute(
                    """
                    UPDATE conversations
                    SET content=?
                    WHERE id=102
                    """,
                    (
                        "Updated public reply about the blue umbrella.",
                    ),
                )
                source.commit()
            changed = prepare_memory_preview(self._request())
            try:
                self.assertEqual(
                    snapshots_equivalent(first, changed),
                    (False, "preview_source_changed"),
                )
            finally:
                changed.close()
        finally:
            first.close()

    def test_context_v2_legacy_schema_fails_closed_without_breaking_preview(self):
        with sqlite3.connect(self.db_path) as source:
            source.execute(
                """
                CREATE TABLE conversations (
                  id INTEGER PRIMARY KEY,
                  guild_id INTEGER,
                  user_id INTEGER,
                  role TEXT,
                  content TEXT
                )
                """
            )
            source.execute(
                """
                INSERT INTO conversations(id,guild_id,user_id,role,content)
                VALUES(1,1,7,'user','legacy private-shaped context')
                """
            )
            source.commit()

        prepared = prepare_memory_preview(self._request())
        try:
            self.assertTrue(prepared.ready)
            self.assertFalse(prepared.conversation_context_digest)
            self.assertNotIn(
                "legacy private-shaped context",
                prepared.request.baseline_prompt,
            )
            self.assertFalse(
                any(
                    item.lane == "conversation_context"
                    for item in prepared.packet.items
                )
            )
        finally:
            prepared.close()

    def test_context_v2_excludes_live_path_bare_media_fallback_pair(self):
        user_text = "What was in that recent GIF?"
        fallback_text = (
            "I saw your recent GIF, but I don't have a detailed "
            "visual description."
        )
        with sqlite3.connect(self.db_path) as source:
            self._ensure_conversation_context_schema(source)
            self._insert_conversation_context_row(
                source,
                row_id=201,
                role="user",
                content=user_text,
                observed_at="2026-07-26T11:50:00+00:00",
            )
            self._insert_conversation_context_row(
                source,
                row_id=202,
                role="model",
                content=fallback_text,
                observed_at="2026-07-26T11:51:00+00:00",
            )
            source.commit()

        prepared = prepare_memory_preview(self._request())
        try:
            self.assertTrue(prepared.ready)
            self.assertFalse(prepared.conversation_context_digest)
            self.assertNotIn(user_text, prepared.request.baseline_prompt)
            self.assertNotIn(
                fallback_text,
                prepared.request.baseline_prompt,
            )
            self.assertFalse(
                any(
                    item.lane == "conversation_context"
                    for item in prepared.packet.items
                )
            )
        finally:
            prepared.close()

    def test_context_v2_carries_selected_participants_into_assessment(self):
        with sqlite3.connect(self.db_path) as source:
            self._ensure_conversation_context_schema(source)
            self._insert_conversation_context_row(
                source,
                row_id=301,
                role="user",
                content="Could we compare the copper lantern?",
                observed_at="2026-07-26T11:50:00+00:00",
                user_id=8,
                user_name="Helpful Neighbor",
            )
            self._insert_conversation_context_row(
                source,
                row_id=302,
                role="model",
                content="The copper lantern was the warmer option.",
                observed_at="2026-07-26T11:51:00+00:00",
                user_id=8,
                user_name="Helpful Neighbor",
            )
            source.commit()

        prepared = prepare_memory_preview(self._request())
        try:
            self.assertTrue(prepared.ready)
            self.assertTrue(prepared.conversation_context_digest)
            self.assertEqual(
                prepared.assessment.participant_user_ids,
                (7, 8),
            )
            self.assertEqual(
                prepared.assessment.speaker_labels,
                ("Crow", "Helpful Neighbor"),
            )
            self.assertEqual(
                prepared.assessment.current_exchange_source_ids,
                (301, 302),
            )
            self.assertIn(
                "conversation_context",
                prepared.assessment.prompt_lanes,
            )
        finally:
            prepared.close()

    def test_retained_public_history_reaches_multiple_profile_points(self):
        raw_markers = (
            "NEON MIX SOURCE",
            "GLASS ART SOURCE",
            "PRIVATE SOURCE SENTINEL",
        )
        with sqlite3.connect(self.db_path) as source:
            self._ensure_conversation_context_schema(source)
            rows = (
                (
                    401,
                    "user",
                    (
                        "NEON MIX SOURCE: I am revising the music track "
                        "with <@123456789012345678> at "
                        "https://example.com/one."
                    ),
                    "2026-07-20T10:00:00+00:00",
                    20,
                    "artist-room",
                    "public_selective",
                    7,
                    "Crow",
                ),
                (
                    402,
                    "user",
                    (
                        "The album vocals and music mix need another "
                        "production pass."
                    ),
                    "2026-07-21T10:00:00+00:00",
                    21,
                    "music-chat",
                    "public_context",
                    7,
                    "Crow",
                ),
                (
                    403,
                    "user",
                    (
                        "GLASS ART SOURCE: I am comparing artwork and "
                        "visual design for the cover."
                    ),
                    "2026-07-22T10:00:00+00:00",
                    10,
                    "barcode-bot",
                    "public_home",
                    7,
                    "Crow",
                ),
                (
                    404,
                    "user",
                    (
                        "The animation and artwork need a stronger visual "
                        "style."
                    ),
                    "2026-07-23T10:00:00+00:00",
                    20,
                    "artist-room",
                    "public_selective",
                    7,
                    "Crow",
                ),
                (
                    405,
                    "user",
                    "The queue rehearsal website code needs another test.",
                    "2026-07-24T10:00:00+00:00",
                    10,
                    "barcode-bot",
                    "public_home",
                    7,
                    "Crow",
                ),
                (
                    406,
                    "user",
                    "PRIVATE SOURCE SENTINEL about visual design.",
                    "2026-07-25T10:00:00+00:00",
                    30,
                    "operations",
                    "internal_controlled",
                    7,
                    "Crow",
                ),
                (
                    407,
                    "user",
                    "Another member is mixing an unrelated music track.",
                    "2026-07-25T11:00:00+00:00",
                    10,
                    "barcode-bot",
                    "public_home",
                    8,
                    "Other Member",
                ),
            )
            for (
                row_id,
                role,
                content,
                observed_at,
                channel_id,
                channel_name,
                channel_policy,
                user_id,
                user_name,
            ) in rows:
                self._insert_conversation_context_row(
                    source,
                    row_id=row_id,
                    role=role,
                    content=content,
                    observed_at=observed_at,
                    channel_id=channel_id,
                    channel_name=channel_name,
                    channel_policy=channel_policy,
                    user_id=user_id,
                    user_name=user_name,
                )
            source.commit()
        source_hash = self._source_hash()

        prepared = prepare_memory_preview(self._request())
        try:
            self.assertTrue(prepared.ready)
            self.assertEqual(prepared.diagnostics.profile_status, "rich")
            self.assertGreaterEqual(
                prepared.diagnostics.profile_selected_point_count,
                2,
            )
            self.assertGreaterEqual(
                dict(prepared.diagnostics.packet_lane_counts).get(
                    "atomic_knowledge",
                    0,
                ),
                2,
            )
            funnel = dict(prepared.diagnostics.source_funnel_counts)
            self.assertEqual(funnel["retained_rows_total"], 6)
            self.assertEqual(funnel["retained_rows_public_safe"], 5)
            self.assertEqual(funnel["retained_rows_policy_excluded"], 1)
            self.assertEqual(funnel["ledger_projection_inserted"], 5)
            self.assertEqual(
                funnel["ledger_rows_operational_excluded"],
                1,
            )
            self.assertGreaterEqual(funnel["motif_candidates_returned"], 2)
            diagnostics = "\n".join(
                render_content_free_diagnostics(prepared)
            )
            self.assertIn("source_funnel", diagnostics)
            for marker in raw_markers:
                self.assertNotIn(marker, diagnostics)
            self.assertNotIn("Other Member", diagnostics)
        finally:
            prepared.close()

        self.assertEqual(source_hash, self._source_hash())
        with sqlite3.connect(self.db_path) as source:
            self.assertEqual(
                source.execute(
                    """
                    SELECT COUNT(*)
                    FROM memory_ledger_entries
                    WHERE source_row_id IN ('401','402','403','404','405')
                    """
                ).fetchone()[0],
                0,
            )

    def test_diagnostics_are_content_free(self):
        prepared = prepare_memory_preview(self._request())
        try:
            lines = "\n".join(
                render_content_free_diagnostics(prepared)
            )
        finally:
            prepared.close()
        self.assertNotIn("fixing the bot code", lines)
        self.assertNotIn("website code", lines)
        self.assertNotIn("Crow", lines)
        self.assertNotIn("discord_user:7", lines)
        self.assertNotIn("source_row_id", lines)
        self.assertIn("source_db_read_only=true", lines)
        self.assertIn("invocation_saved=false", lines)
        self.assertIn("response_saved=false", lines)


if __name__ == "__main__":
    unittest.main()
