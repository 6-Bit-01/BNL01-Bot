import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import bnl_declared_canon as declared
import bnl_memory_ledger as ledger


def claim_fields(**overrides):
    values = {
        "guild_id": 7,
        "subject_type": "project",
        "subject_id": "barcode_radio",
        "predicate": "current_role",
        "value": {"role": "broadcast continuity"},
        "raw_declaration": "6 Bit declares the broadcast continuity role.",
        "cleaned_summary": "BARCODE Radio has a broadcast continuity role.",
        "domain": "broadcast_history",
        "claim_kind": "role",
        "visibility": "internal",
        "eligible_routes": ("declared_canon_review",),
        "now": "2026-08-01T00:00:00+00:00",
    }
    values.update(overrides)
    return values


class DeclaredCanonProjectionTests(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(
            os.environ,
            {
                "BNL_OWNER_USER_ID": "61",
                "BNL_PRIMARY_GUILD_ID": "7",
            },
        )
        self.env.start()
        self.conn = sqlite3.connect(":memory:")
        ledger.ensure_memory_ledger_schema(self.conn)
        declared.ensure_declared_canon_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.env.stop()

    def add_claim(self, nonce="projection-add1", **overrides):
        return declared.add_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce=nonce,
            **claim_fields(**overrides),
        ).primary

    def project(self, revision, nonce, root_entry_ids=()):
        return ledger.shadow_declared_canon_projection(
            self.conn,
            guild_id=7,
            declaration_id=revision.declaration_id,
            revision_id=revision.revision_id,
            actor_user_id=61,
            authority_nonce=nonce,
            expected_source_fingerprint=revision.source_fingerprint,
            expected_lifecycle_status=revision.lifecycle_status,
            root_entry_ids=root_entry_ids,
        )

    @staticmethod
    def create_broadcast_table(conn):
        conn.execute(
            """
            CREATE TABLE broadcast_memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                episode_date TEXT NOT NULL,
                submitted_by_user_id INTEGER,
                submitted_by_name TEXT,
                raw_note TEXT,
                cleaned_summary TEXT NOT NULL,
                entry_type TEXT NOT NULL,
                importance TEXT DEFAULT 'medium',
                public_safe INTEGER DEFAULT 0,
                affects_next_show INTEGER DEFAULT 0,
                usage_scope TEXT DEFAULT 'internal',
                target_show_date TEXT,
                valid_until TEXT,
                override_span_count INTEGER DEFAULT 1,
                needs_clarification INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                corrected_by_user_id INTEGER,
                corrected_by_name TEXT,
                correction_reason TEXT,
                supersedes_id INTEGER,
                superseded_by_id INTEGER
            )
            """
        )
        conn.commit()

    @staticmethod
    def insert_broadcast(conn, cleaned="Authoritative Broadcast summary."):
        cursor = conn.execute(
            """
            INSERT INTO broadcast_memory(
                guild_id,episode_date,submitted_by_user_id,submitted_by_name,
                raw_note,cleaned_summary,entry_type,importance,public_safe,
                affects_next_show,usage_scope,target_show_date,valid_until,
                override_span_count,needs_clarification,status,created_at,
                updated_at,corrected_by_user_id,corrected_by_name,
                correction_reason,supersedes_id,superseded_by_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                7,
                "2026-08-01",
                61,
                "6 Bit",
                "Owner Broadcast note.",
                cleaned,
                "notable_moment",
                "medium",
                1,
                0,
                "ambient,direct",
                None,
                "",
                1,
                0,
                "active",
                "2026-08-01T00:00:00+00:00",
                "2026-08-01T00:00:00+00:00",
                None,
                None,
                None,
                None,
                None,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)

    @staticmethod
    def broadcast_source(conn, row_id):
        cursor = conn.execute(
            "SELECT * FROM broadcast_memory WHERE id=?",
            (int(row_id),),
        )
        row = cursor.fetchone()
        return dict(zip((item[0] for item in cursor.description), row))

    def classify_broadcast(self, conn, row_id, nonce="projection-classify1"):
        fingerprint = declared.broadcast_source_fingerprint(
            self.broadcast_source(conn, row_id)
        )
        revision = declared.classify_broadcast_memory(
            conn,
            actor_user_id=61,
            authority_nonce=nonce,
            guild_id=7,
            broadcast_row_id=row_id,
            expected_source_fingerprint=fingerprint,
            subject_type="broadcast",
            subject_id="barcode_radio",
            visibility="internal",
            eligible_routes=("broadcast_memory",),
            now="2026-08-01T01:00:00+00:00",
        ).primary
        return revision

    def test_projection_is_derived_internal_review_only_and_nonpublic(self):
        revision = self.add_claim()
        projected = self.project(revision, "projection-run1")

        row = self.conn.execute(
            """
            SELECT source_table,source_revision,source_role,route_mode,
                   channel_policy,visibility,public_usable,derived,projection,
                   lifecycle_status
            FROM memory_ledger_entries
            WHERE entry_id=?
            """,
            (projected.entry_id,),
        ).fetchone()
        self.assertEqual(
            row,
            (
                "declared_canon_projection",
                revision.revision_id,
                "declared_canon_projection",
                "declared_canon_review",
                "declared_canon_review",
                "internal",
                0,
                1,
                1,
                "review_only",
            ),
        )
        lineage = self.conn.execute(
            """
            SELECT lineage_type,target_entry_id
            FROM memory_ledger_lineage
            WHERE entry_id=?
            ORDER BY lineage_type,target_entry_id
            """,
            (projected.entry_id,),
        ).fetchall()
        self.assertEqual(lineage, [])

    def test_general_declaration_rejects_arbitrary_ledger_roots(self):
        revision = self.add_claim("projection-no-root1")
        arbitrary_root = ledger.shadow_broadcast_memory_row(
            self.conn,
            row_id=11,
            guild_id=7,
            cleaned_summary="An unrelated Ledger root.",
            entry_type="notable_moment",
            public_safe=True,
            status="active",
            usage_scope="ambient,direct",
            updated_at="2026-08-01T00:00:00+00:00",
        )
        self.conn.commit()

        projected = self.project(
            revision,
            "projection-no-root2",
            root_entry_ids=(arbitrary_root.entry_id,),
        )

        self.assertEqual(projected.outcome, "skipped")
        self.assertEqual(
            projected.reason_code,
            "declared_projection_general_roots_forbidden",
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_entries
                WHERE source_table='declared_canon_projection'
                """
            ).fetchone()[0],
            0,
        )

    def test_broadcast_projection_requires_one_exact_broadcast_root(self):
        self.create_broadcast_table(self.conn)
        row_id = self.insert_broadcast(self.conn)
        revision = self.classify_broadcast(self.conn, row_id)
        root = ledger.shadow_broadcast_memory_row(
            self.conn,
            row_id=row_id,
            guild_id=7,
            cleaned_summary="Authoritative Broadcast summary.",
            entry_type="notable_moment",
            public_safe=True,
            status="active",
            usage_scope="ambient,direct",
            created_at="2026-08-01T00:00:00+00:00",
            updated_at="2026-08-01T00:00:00+00:00",
        )
        self.conn.commit()

        missing = self.project(revision, "projection-root-missing1")
        duplicate = self.project(
            revision,
            "projection-root-duplicate1",
            root_entry_ids=(root.entry_id, root.entry_id),
        )
        projected = self.project(
            revision,
            "projection-root-exact1",
            root_entry_ids=(root.entry_id,),
        )

        self.assertEqual(missing.outcome, "skipped")
        self.assertEqual(duplicate.outcome, "skipped")
        self.assertEqual(projected.outcome, "inserted")
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage
                WHERE entry_id=?
                """,
                (projected.entry_id,),
            ).fetchall(),
            [("derived_from", root.entry_id)],
        )

    def test_broadcast_projection_rejects_stale_prior_root_revision(self):
        self.create_broadcast_table(self.conn)
        row_id = self.insert_broadcast(
            self.conn,
            cleaned="The earlier Broadcast value.",
        )
        stale_root = ledger.shadow_broadcast_memory_row(
            self.conn,
            row_id=row_id,
            guild_id=7,
            cleaned_summary="The earlier Broadcast value.",
            entry_type="notable_moment",
            public_safe=True,
            status="active",
            usage_scope="ambient,direct",
            created_at="2026-08-01T00:00:00+00:00",
            updated_at="2026-08-01T00:00:00+00:00",
        )
        self.conn.execute(
            """
            UPDATE broadcast_memory
            SET cleaned_summary=?,updated_at=?
            WHERE guild_id=? AND id=?
            """,
            (
                "The current Broadcast value.",
                "2026-08-01T00:01:00+00:00",
                7,
                row_id,
            ),
        )
        self.conn.commit()
        revision = self.classify_broadcast(
            self.conn,
            row_id,
            nonce="projection-stale-classify1",
        )

        projected = self.project(
            revision,
            "projection-stale-root1",
            root_entry_ids=(stale_root.entry_id,),
        )

        self.assertEqual(projected.outcome, "skipped")
        self.assertEqual(
            projected.reason_code,
            "declared_projection_broadcast_root_stale",
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*) FROM memory_ledger_entries
                WHERE source_table='declared_canon_projection'
                """
            ).fetchone()[0],
            0,
        )

    def test_terminal_broadcast_projection_retracts_active_declared_projection(self):
        self.create_broadcast_table(self.conn)
        row_id = self.insert_broadcast(self.conn)
        active_revision = self.classify_broadcast(
            self.conn,
            row_id,
            nonce="projection-terminal-classify1",
        )
        root = ledger.shadow_broadcast_memory_row(
            self.conn,
            row_id=row_id,
            guild_id=7,
            cleaned_summary="Authoritative Broadcast summary.",
            entry_type="notable_moment",
            public_safe=True,
            status="active",
            usage_scope="ambient,direct",
            created_at="2026-08-01T00:00:00+00:00",
            updated_at="2026-08-01T00:00:00+00:00",
        )
        active_projection = self.project(
            active_revision,
            "projection-terminal-run1",
            root_entry_ids=(root.entry_id,),
        )
        self.assertEqual(active_projection.outcome, "inserted")

        self.conn.execute(
            """
            UPDATE broadcast_memory
            SET status='resolved',updated_at=?,corrected_by_user_id=?,
                corrected_by_name=?,correction_reason=?
            WHERE guild_id=? AND id=?
            """,
            (
                "2026-08-01T02:00:00+00:00",
                61,
                "6 Bit",
                "Owner resolved the Broadcast memory.",
                7,
                row_id,
            ),
        )
        self.conn.commit()
        source = self.broadcast_source(self.conn, row_id)
        terminal_revision = declared.classify_broadcast_memory(
            self.conn,
            actor_user_id=61,
            authority_nonce="projection-terminal-classify2",
            guild_id=7,
            broadcast_row_id=row_id,
            expected_source_fingerprint=declared.broadcast_source_fingerprint(source),
            expected_revision_id=active_revision.revision_id,
            subject_type="broadcast",
            subject_id="barcode_radio",
            visibility="internal",
            eligible_routes=("broadcast_memory",),
            now="2026-08-01T02:00:00+00:00",
        ).primary

        rejected = self.project(
            terminal_revision,
            "projection-terminal-root-rejected1",
            root_entry_ids=(root.entry_id,),
        )
        terminal_projection = self.project(
            terminal_revision,
            "projection-terminal-run2",
        )

        self.assertEqual(rejected.outcome, "skipped")
        self.assertEqual(
            rejected.reason_code,
            "declared_projection_broadcast_terminal_roots_forbidden",
        )
        self.assertEqual(terminal_projection.outcome, "inserted")
        self.assertEqual(
            self.conn.execute(
                """
                SELECT normalized_value,lifecycle_status,visibility,
                       public_usable,derived,projection
                FROM memory_ledger_entries WHERE entry_id=?
                """,
                (terminal_projection.entry_id,),
            ).fetchone(),
            ("resolved", "resolved", "internal", 0, 1, 1),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage WHERE entry_id=?
                """,
                (terminal_projection.entry_id,),
            ).fetchall(),
            [("retracts", active_projection.entry_id)],
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_entries AS projection_row
                WHERE projection_row.entry_id=?
                  AND NOT EXISTS (
                      SELECT 1 FROM memory_ledger_lineage AS edge
                      WHERE edge.guild_id=projection_row.guild_id
                        AND edge.target_entry_id=projection_row.entry_id
                        AND edge.lineage_type IN ('supersedes','retracts')
                  )
                """,
                (active_projection.entry_id,),
            ).fetchone()[0],
            0,
        )

    def test_new_revision_supersedes_prior_projection_without_becoming_live(self):
        first_revision = self.add_claim("projection-add2")
        first = self.project(first_revision, "projection-run2")
        self.conn.commit()
        corrected_revision = declared.correct_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="projection-fix2",
            declaration_id=first_revision.declaration_id,
            expected_revision_id=first_revision.revision_id,
            reason="Correct the role wording.",
            **claim_fields(
                value={"role": "broadcast memory continuity"},
                raw_declaration="6 Bit corrects the broadcast continuity role.",
                cleaned_summary="BARCODE Radio has a broadcast memory continuity role.",
                now="2026-08-01T00:01:00+00:00",
            ),
        ).primary
        second = self.project(corrected_revision, "projection-run3")

        lineage = self.conn.execute(
            """
            SELECT lineage_type,target_entry_id
            FROM memory_ledger_lineage
            WHERE entry_id=?
            """,
            (second.entry_id,),
        ).fetchall()
        self.assertEqual(lineage, [("supersedes", first.entry_id)])
        live_rows = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM memory_ledger_entries
            WHERE source_table='declared_canon_projection'
              AND (public_usable=1 OR lifecycle_status!='review_only'
                   OR visibility!='internal')
            """
        ).fetchone()[0]
        self.assertEqual(live_rows, 0)

    def test_typed_subject_keys_do_not_collapse_person_and_project(self):
        person = self.add_claim(
            "projection-person1",
            subject_type="person",
            subject_id="shared_identity",
        )
        project = self.add_claim(
            "projection-project1",
            subject_type="project",
            subject_id="shared_identity",
        )

        person_projection = self.project(person, "projection-person2")
        project_projection = self.project(project, "projection-project2")

        self.assertNotEqual(person_projection.entry_id, project_projection.entry_id)
        rows = self.conn.execute(
            """
            SELECT subject_key
            FROM memory_ledger_entries
            WHERE entry_id IN (?,?)
            ORDER BY subject_key
            """,
            (person_projection.entry_id, project_projection.entry_id),
        ).fetchall()
        self.assertEqual(
            rows,
            [("person:shared_identity",), ("project:shared_identity",)],
        )

    def test_relationship_projection_preserves_both_typed_endpoints(self):
        relation = self.add_claim(
            "projection-relation1",
            subject_type="person",
            subject_id="six_bit",
            object_subject_type="project",
            object_subject_id="barcode_radio",
            predicate="hosts",
            claim_kind="relationship",
            value={"relationship": "host"},
            raw_declaration="6 Bit declares that 6 Bit hosts BARCODE Radio.",
            cleaned_summary="6 Bit hosts BARCODE Radio.",
        )

        projected = self.project(relation, "projection-relation2")

        self.assertEqual(projected.outcome, "inserted")
        self.assertEqual(
            self.conn.execute(
                """
                SELECT subject_key,predicate_key
                FROM memory_ledger_entries WHERE entry_id=?
                """,
                (projected.entry_id,),
            ).fetchone(),
            ("person:six_bit", "hosts"),
        )
        self.assertEqual(
            self.conn.execute(
                """
                SELECT participant_key,participant_role
                FROM memory_ledger_participants
                WHERE entry_id=?
                ORDER BY order_index
                """,
                (projected.entry_id,),
            ).fetchall(),
            [
                ("person:six_bit", "subject"),
                ("project:barcode_radio", "relationship_object"),
            ],
        )

    def test_cross_declaration_supersession_preserves_exact_lineage(self):
        old_revision = self.add_claim(
            "projection-old-add1",
            subject_id="old_broadcast_role",
            cleaned_summary="The old broadcast role is established.",
        )
        replacement_revision = self.add_claim(
            "projection-new-add1",
            subject_id="replacement_broadcast_role",
            cleaned_summary="The replacement broadcast role is established.",
        )
        old_projection = self.project(old_revision, "projection-old-run1")
        replacement_projection = self.project(
            replacement_revision,
            "projection-new-run1",
        )

        supersession = declared.supersede_declared_canon(
            self.conn,
            actor_user_id=61,
            authority_nonce="projection-supersede1",
            guild_id=7,
            declaration_id=old_revision.declaration_id,
            expected_revision_id=old_revision.revision_id,
            replacement_declaration_id=replacement_revision.declaration_id,
            expected_replacement_revision_id=replacement_revision.revision_id,
            reason="Replace the old role declaration.",
            now="2026-08-01T00:02:00+00:00",
        )
        by_declaration = {
            revision.declaration_id: revision
            for revision in supersession.revisions
        }
        old_terminal = by_declaration[old_revision.declaration_id]
        replacement_current = by_declaration[replacement_revision.declaration_id]

        replacement_current_projection = self.project(
            replacement_current,
            "projection-new-run2",
        )
        old_terminal_projection = self.project(
            old_terminal,
            "projection-old-run2",
        )

        replacement_edges = set(
            self.conn.execute(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage WHERE entry_id=?
                """,
                (replacement_current_projection.entry_id,),
            ).fetchall()
        )
        old_terminal_edges = set(
            self.conn.execute(
                """
                SELECT lineage_type,target_entry_id
                FROM memory_ledger_lineage WHERE entry_id=?
                """,
                (old_terminal_projection.entry_id,),
            ).fetchall()
        )
        self.assertEqual(
            replacement_edges,
            {
                ("supersedes", replacement_projection.entry_id),
                ("supersedes", old_projection.entry_id),
            },
        )
        self.assertEqual(
            old_terminal_edges,
            {("retracts", old_projection.entry_id)},
        )

    def test_projection_holds_source_snapshot_until_insert(self):
        handle = tempfile.NamedTemporaryFile(delete=False)
        path = handle.name
        handle.close()
        writer = None
        contender = None
        thread = None
        release_validation = threading.Event()
        validation_complete = threading.Event()
        outcome = {}
        try:
            writer = sqlite3.connect(
                path,
                timeout=1,
                check_same_thread=False,
            )
            contender = sqlite3.connect(path, timeout=0.05)
            contender.execute("PRAGMA busy_timeout=50")
            ledger.ensure_memory_ledger_schema(writer)
            declared.ensure_declared_canon_schema(writer)
            self.create_broadcast_table(writer)
            row_id = self.insert_broadcast(
                writer,
                cleaned="The source value held by the atomic snapshot.",
            )
            revision = self.classify_broadcast(
                writer,
                row_id,
                nonce="projection-lock-classify1",
            )
            root = ledger.shadow_broadcast_memory_row(
                writer,
                row_id=row_id,
                guild_id=7,
                cleaned_summary="The source value held by the atomic snapshot.",
                entry_type="notable_moment",
                public_safe=True,
                status="active",
                usage_scope="ambient,direct",
                created_at="2026-08-01T00:00:00+00:00",
                updated_at="2026-08-01T00:00:00+00:00",
            )
            writer.commit()

            real_validator = declared.validate_current_declared_canon_revision

            def paused_validator(*args, **kwargs):
                validated = real_validator(*args, **kwargs)
                validation_complete.set()
                if not release_validation.wait(5):
                    raise RuntimeError("projection_test_release_timeout")
                return validated

            def run_projection():
                try:
                    outcome["result"] = ledger.shadow_declared_canon_projection(
                        writer,
                        guild_id=7,
                        declaration_id=revision.declaration_id,
                        revision_id=revision.revision_id,
                        actor_user_id=61,
                        authority_nonce="projection-lock-run1",
                        expected_source_fingerprint=revision.source_fingerprint,
                        expected_lifecycle_status=revision.lifecycle_status,
                        root_entry_ids=(root.entry_id,),
                    )
                except Exception as exc:  # pragma: no cover - assertion below
                    outcome["error"] = exc

            with mock.patch.object(
                declared,
                "validate_current_declared_canon_revision",
                side_effect=paused_validator,
            ):
                thread = threading.Thread(target=run_projection)
                thread.start()
                self.assertTrue(validation_complete.wait(5))
                with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                    contender.execute(
                        """
                        UPDATE broadcast_memory
                        SET cleaned_summary=?,updated_at=?
                        WHERE guild_id=? AND id=?
                        """,
                        (
                            "A racing source mutation.",
                            "2026-08-01T00:00:01+00:00",
                            7,
                            row_id,
                        ),
                    )
                contender.rollback()
                release_validation.set()
                thread.join(5)

            self.assertFalse(thread.is_alive())
            self.assertNotIn("error", outcome)
            self.assertEqual(outcome["result"].outcome, "inserted")
            self.assertEqual(
                writer.execute(
                    """
                    SELECT normalized_value
                    FROM memory_ledger_entries WHERE entry_id=?
                    """,
                    (outcome["result"].entry_id,),
                ).fetchone()[0],
                "The source value held by the atomic snapshot.",
            )
        finally:
            release_validation.set()
            if thread is not None and thread.is_alive():
                thread.join(5)
            if contender is not None:
                contender.close()
            if writer is not None:
                writer.close()
            try:
                os.unlink(path)
            except OSError:
                pass


if __name__ == "__main__":
    unittest.main()
