import json
import os
import sqlite3
import unittest
from unittest import mock

from bnl_memory_governance import (
    GovernanceDiagnostics,
    GovernanceRequest,
    GovernanceResult,
    ensure_governance_schema,
    persist_shadow_diagnostics,
)
from bnl_memory_ledger import (
    ensure_memory_ledger_schema,
    record_shadow_receipt,
    shadow_conversation_row,
)
from bnl_moment_engine import ensure_moment_schema
from bnl_relationship_engine import (
    build_evaluation_report as build_relationship_evaluation_report,
    ensure_relationship_v2_schema,
    observe_message,
    plan_engagement,
)
from bnl_shadow_acceptance import (
    SHADOW_EVALUATION_ORDER,
    build_gate_snapshot,
    build_v2_shadow_acceptance_snapshot,
    render_v2_shadow_acceptance_lines,
)
from bnl_unified_response_assessment import (
    build_unified_response_assessment,
    persist_shadow_run as persist_unified_assessment_shadow_run,
)


class V2ShadowAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.environment = mock.patch.dict(
            os.environ,
            {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "",
                "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "",
                "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "",
                "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "",
                "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "",
            },
            clear=False,
        )
        self.environment.start()
        self.conn = sqlite3.connect(":memory:")
        ensure_memory_ledger_schema(self.conn)
        ensure_moment_schema(self.conn)
        ensure_governance_schema(self.conn)
        ensure_relationship_v2_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        self.environment.stop()

    def snapshot(self, environ=None, context=None):
        return build_v2_shadow_acceptance_snapshot(
            self.conn,
            guild_id=1,
            environ=environ or {},
            conversation_context_diagnostics=context,
        )

    def test_defaults_are_reporting_only_and_all_shadow_stages_are_disabled(self):
        before_changes = self.conn.total_changes
        before_schema = self.conn.execute(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
        ).fetchall()

        snapshot = self.snapshot()

        self.assertEqual(snapshot["evaluationOrder"], list(SHADOW_EVALUATION_ORDER))
        self.assertEqual(snapshot["status"], "collecting_shadow_evidence")
        self.assertTrue(snapshot["gates"]["all_live_gates_clear"])
        self.assertTrue(snapshot["ownerReviewRequired"])
        self.assertFalse(snapshot["automaticCutoverAllowed"])
        self.assertFalse(snapshot["behaviorChangesApplied"])
        self.assertFalse(
            snapshot["unifiedResponseAssessmentShadow"]["requested"]
        )
        self.assertTrue(
            all(stage["status"] == "disabled" for stage in snapshot["stages"].values())
        )
        self.assertEqual(self.conn.total_changes, before_changes)
        self.assertEqual(
            self.conn.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall(),
            before_schema,
        )

    def test_unified_assessment_overlay_is_content_free_and_not_a_fifth_stage(self):
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="active_channel",
            current_speaker_user_ids=(99,),
            participant_user_ids=(99, 100),
            speaker_labels=("PRIVATE ONE", "PRIVATE TWO"),
            current_exchange_source_ids=(500, 501),
            governed_entry_ids=("PRIVATE LEDGER REF",),
            prompt_lanes=("current_exchange", "conversation_context"),
            immediate_recap=True,
        )
        persist_unified_assessment_shadow_run(
            self.conn,
            assessment,
            response=(
                "[Pause: 0.2s]\n"
                "PRIVATE ONE has the intro; PRIVATE TWO has the artwork."
            ),
        )
        self.conn.commit()

        environ = {
            "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
            "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
            "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
            "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
        }
        snapshot = self.snapshot(environ)
        overlay = snapshot["unifiedResponseAssessmentShadow"]
        report = snapshot["reports"]["unifiedResponseAssessment"]

        self.assertEqual(
            snapshot["evaluationOrder"],
            list(SHADOW_EVALUATION_ORDER),
        )
        self.assertNotIn("unified_response_assessment", snapshot["stages"])
        self.assertTrue(overlay["requested"])
        self.assertTrue(overlay["effective"])
        self.assertEqual(overlay["reason"], "shadow_only")
        self.assertTrue(overlay["evidenceObserved"])
        self.assertEqual(overlay["blockers"], [])
        self.assertEqual(report["runs"], 1)
        self.assertEqual(report["current_exchange_primary_runs"], 1)
        self.assertEqual(report["visible_control_marker_runs"], 1)
        self.assertEqual(report["behavior_changed_runs"], 0)
        self.assertEqual(report["new_authority_applied_runs"], 0)
        self.assertEqual(report["content_fields_present"], [])
        self.assertIn(
            "unified_assessment_visible_control_marker_review",
            snapshot["warnings"],
        )

        encoded = json.dumps(snapshot, sort_keys=True)
        self.assertNotIn("PRIVATE ONE", encoded)
        self.assertNotIn("PRIVATE TWO", encoded)
        self.assertNotIn("PRIVATE LEDGER REF", encoded)
        rendered = "\n".join(render_v2_shadow_acceptance_lines(snapshot))
        self.assertIn("unified_assessment:", rendered)
        self.assertIn("visible_control_markers=`1`", rendered)

    def test_unified_assessment_new_authority_or_behavior_is_a_hard_blocker(self):
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="public_home",
            conversation_surface="active_channel",
            current_speaker_user_ids=(99,),
        )
        persist_unified_assessment_shadow_run(
            self.conn,
            assessment,
            response="Safe response.",
        )
        self.conn.execute(
            "UPDATE unified_response_assessment_shadow_runs "
            "SET behavior_changed=1, new_authority_applied=1"
        )
        self.conn.commit()

        snapshot = self.snapshot(
            {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            }
        )
        self.assertEqual(
            snapshot["status"],
            "blocked_shadow_invariant_failure",
        )
        self.assertIn(
            "unified_response_assessment:behavior_changed_runs",
            snapshot["blockers"],
        )
        self.assertIn(
            "unified_response_assessment:new_authority_applied_runs",
            snapshot["blockers"],
        )

    def test_unrepaired_payload_grounding_failure_is_a_hard_blocker(self):
        assessment = build_unified_response_assessment(
            guild_id=1,
            route_mode="normal_chat",
            channel_policy="sealed_test",
            conversation_surface="test",
            current_speaker_user_ids=(99,),
            prompt_lanes=("current_exchange", "conversation_context"),
            current_exchange_source_ids=(1, 2),
            current_payload_anchors=("dead channel", "open circuit"),
            prior_thread_anchors=("ghost signal", "neon static"),
            thread_focus_mode="new_thread",
        )
        persist_unified_assessment_shadow_run(
            self.conn,
            assessment,
            response="Ghost Signal is the stronger title.",
        )
        self.conn.commit()

        snapshot = self.snapshot(
            {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            }
        )
        report = snapshot["reports"]["unifiedResponseAssessment"]
        self.assertEqual(report["payload_grounding_failure_runs"], 1)
        self.assertEqual(
            report["payload_grounding_status_counts"],
            {"stale_thread_substitution": 1},
        )
        self.assertIn(
            "unified_response_assessment:"
            "payload_grounding_failure_runs",
            snapshot["blockers"],
        )
        rendered = "\n".join(
            render_v2_shadow_acceptance_lines(snapshot)
        )
        self.assertIn("grounding_failures=`1`", rendered)

    def test_missing_schemas_are_reported_without_creating_tables(self):
        empty = sqlite3.connect(":memory:")
        try:
            snapshot = build_v2_shadow_acceptance_snapshot(
                empty,
                guild_id=1,
                environ={},
            )
            self.assertEqual(snapshot["status"], "collecting_shadow_evidence")
            self.assertEqual(
                empty.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()[0],
                0,
            )
            self.assertEqual(empty.total_changes, 0)
        finally:
            empty.close()

    def test_requested_live_gate_is_detected_even_when_not_effective(self):
        cases = (
            ("BNL_MEMORY_GOVERNANCE_LIVE_ENABLED", "memory_governance_live_requested"),
            ("BNL_RELATIONSHIP_V2_LIVE_ENABLED", "relationship_v2_live_requested"),
            ("BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED", "active_engagement_v2_live_requested"),
        )
        for environment_name, snapshot_name in cases:
            with self.subTest(environment_name=environment_name):
                gates = build_gate_snapshot({environment_name: "true"})
                self.assertTrue(gates[snapshot_name])
                self.assertFalse(gates["all_live_gates_clear"])
                if environment_name == "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED":
                    self.assertFalse(gates["memory_governance_live_effective"])

                snapshot = self.snapshot({environment_name: "true"})
                self.assertEqual(
                    snapshot["status"], "blocked_live_authority_detected"
                )
                self.assertIn(
                    "live_gate_enabled:%s" % environment_name,
                    snapshot["blockers"],
                )
                self.assertFalse(snapshot["automaticCutoverAllowed"])

    def test_relationship_shadow_is_blocked_when_earlier_stages_are_not_accepted(self):
        snapshot = self.snapshot({"BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true"})
        stage = snapshot["stages"]["relationship_v2"]
        self.assertEqual(stage["status"], "blocked_by_prerequisite")
        self.assertEqual(
            stage["prerequisites"],
            ["memory_ledger", "moment_engine", "memory_governance"],
        )

    def test_governance_persists_only_aggregate_acceptance_diagnostics(self):
        diagnostics = GovernanceDiagnostics(
            selected_by_source={"first_party_record": 2},
            invalid_invariants=["cross_guild_selected", "cross_guild_selected"],
            fallback_reason="controlled_fallback",
            visibility_exclusions=3,
            token_budget_exclusions=4,
            duplicate_suppression=5,
            contradiction_resolutions=["opaque-a", "opaque-b"],
            moment_candidate_count=6,
            moment_needs_review_excluded=7,
        )
        request = GovernanceRequest(
            guild_id=1,
            subject_user_id=99,
            route_mode="normal_chat",
            conversation_surface="test",
            channel_policy="public_home",
        )
        persist_shadow_diagnostics(
            self.conn,
            request,
            GovernanceResult("", (), (), diagnostics),
            "legacy private content",
        )

        snapshot = self.snapshot(
            {"BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true"}
        )
        report = snapshot["reports"]["memoryGovernance"]
        self.assertTrue(report["aggregateDiagnosticsPresent"])
        self.assertEqual(report["aggregateDiagnosticRuns"], 1)
        self.assertEqual(report["selected_by_source"], {"first_party_record": 2})
        self.assertEqual(report["invalid_invariant_count"], 2)
        self.assertEqual(report["invalid_invariant_runs"], 1)
        self.assertEqual(report["visibility_exclusions"], 3)
        self.assertEqual(report["token_budget_exclusions"], 4)
        self.assertEqual(report["duplicate_suppression"], 5)
        self.assertEqual(report["contradiction_resolution_count"], 2)
        self.assertEqual(report["moment_candidate_count"], 6)
        self.assertEqual(report["moment_needs_review_excluded"], 7)

        encoded = json.dumps(snapshot, sort_keys=True)
        self.assertNotIn("legacy private content", encoded)
        self.assertNotIn("discord_user:99", encoded)
        subject_hash = self.conn.execute(
            "SELECT subject_hash FROM memory_governance_shadow_runs"
        ).fetchone()[0]
        self.assertNotIn(subject_hash, encoded)

    def test_route_policy_label_is_reclassified_once_and_not_a_hard_blocker(self):
        request = GovernanceRequest(
            guild_id=1,
            subject_user_id=99,
            route_mode="normal_chat",
            conversation_surface="test",
            channel_policy="public_home",
        )
        persist_shadow_diagnostics(
            self.conn,
            request,
            GovernanceResult(
                "",
                (),
                (),
                GovernanceDiagnostics(
                    invalid_invariants=["invalid_route_channel_policy_selected"],
                    excluded_by_reason={"invalid_route_channel_policy": 1},
                ),
            ),
            "legacy",
        )
        before_row = self.conn.execute(
            "SELECT excluded_json, diagnostics_json "
            "FROM memory_governance_shadow_runs"
        ).fetchone()
        before_changes = self.conn.total_changes

        snapshot = self.snapshot(
            {"BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true"}
        )
        report = snapshot["reports"]["memoryGovernance"]

        self.assertEqual(report["invalid_invariants"], {})
        self.assertEqual(report["invalid_invariant_count"], 0)
        self.assertEqual(report["invalid_invariant_runs"], 0)
        self.assertEqual(
            report["legacy_reclassified_exclusions"],
            {"invalid_route_channel_policy_selected": 1},
        )
        self.assertEqual(report["legacy_reclassified_exclusion_count"], 1)
        self.assertEqual(report["legacy_reclassified_exclusion_runs"], 1)
        self.assertNotIn(
            "memory_governance:invalid_invariants", snapshot["blockers"]
        )
        self.assertEqual(
            snapshot["stages"]["memory_governance"]["status"],
            "blocked_by_prerequisite",
        )
        self.assertIn(
            "governance_legacy_safe_exclusions_reclassified",
            snapshot["warnings"],
        )
        self.assertEqual(snapshot["status"], "collecting_shadow_evidence")
        self.assertEqual(self.conn.total_changes, before_changes)
        self.assertEqual(
            self.conn.execute(
                "SELECT excluded_json, diagnostics_json "
                "FROM memory_governance_shadow_runs"
            ).fetchone(),
            before_row,
        )

        rendered = "\n".join(render_v2_shadow_acceptance_lines(snapshot))
        self.assertIn("governance_invalid_invariants: `{}`", rendered)
        self.assertIn(
            'governance_legacy_reclassified_exclusions: `{"invalid_route_channel_policy_selected": 1}`',
            rendered,
        )

    def test_persisted_unmatched_route_policy_label_remains_a_hard_blocker(self):
        persist_shadow_diagnostics(
            self.conn,
            GovernanceRequest(
                guild_id=1,
                subject_user_id=99,
                route_mode="normal_chat",
                conversation_surface="test",
                channel_policy="public_home",
            ),
            GovernanceResult(
                "",
                (),
                (),
                GovernanceDiagnostics(
                    invalid_invariants=[
                        "invalid_route_channel_policy_selected",
                        "invalid_route_channel_policy_selected",
                    ],
                    excluded_by_reason={"invalid_route_channel_policy": 1},
                ),
            ),
            "legacy",
        )
        stored = self.conn.execute(
            "SELECT excluded_json, diagnostics_json "
            "FROM memory_governance_shadow_runs"
        ).fetchone()
        self.assertEqual(
            json.loads(stored[0]),
            {"invalid_route_channel_policy": 1},
        )
        self.assertEqual(
            json.loads(stored[1])["invalid_invariant_counts"],
            {"invalid_route_channel_policy_selected": 2},
        )

        snapshot = self.snapshot(
            {"BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true"}
        )
        report = snapshot["reports"]["memoryGovernance"]
        self.assertEqual(
            report["legacy_reclassified_exclusions"],
            {"invalid_route_channel_policy_selected": 1},
        )
        self.assertEqual(
            report["invalid_invariants"],
            {"invalid_route_channel_policy_selected": 1},
        )
        self.assertEqual(report["invalid_invariant_count"], 1)
        self.assertIn("memory_governance:invalid_invariants", snapshot["blockers"])
        self.assertEqual(snapshot["status"], "blocked_shadow_invariant_failure")

    def test_true_governance_invariant_remains_a_hard_blocker(self):
        persist_shadow_diagnostics(
            self.conn,
            GovernanceRequest(
                guild_id=1,
                subject_user_id=99,
                route_mode="normal_chat",
                conversation_surface="test",
                channel_policy="public_home",
            ),
            GovernanceResult(
                "",
                (),
                (),
                GovernanceDiagnostics(
                    invalid_invariants=[
                        "cross_guild_selected",
                        "future_unknown_selected",
                    ]
                ),
            ),
            "legacy",
        )

        snapshot = self.snapshot(
            {"BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true"}
        )
        report = snapshot["reports"]["memoryGovernance"]
        self.assertEqual(
            report["invalid_invariants"],
            {"cross_guild_selected": 1, "future_unknown_selected": 1},
        )
        self.assertEqual(report["invalid_invariant_count"], 2)
        self.assertIn("memory_governance:invalid_invariants", snapshot["blockers"])
        self.assertEqual(snapshot["status"], "blocked_shadow_invariant_failure")

    def test_governance_schema_migration_preserves_old_rows_and_accepts_new_aggregates(self):
        migrated = sqlite3.connect(":memory:")
        try:
            migrated.execute(
                "CREATE TABLE memory_governance_shadow_runs ("
                "run_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, subject_hash TEXT NOT NULL, "
                "route_mode TEXT, channel_policy TEXT, created_at TEXT NOT NULL, rendered_hash TEXT, "
                "rendered_size INTEGER, legacy_hash TEXT, legacy_size INTEGER, selected_count INTEGER, "
                "excluded_json TEXT, errors_json TEXT)"
            )
            migrated.execute(
                "INSERT INTO memory_governance_shadow_runs VALUES "
                "('old',1,'opaque','normal_chat','public_home','2026-01-01','a',1,'b',1,0,'{}','[]')"
            )
            migrated.commit()

            ensure_governance_schema(migrated)
            columns = {
                row[1]
                for row in migrated.execute(
                    "PRAGMA table_info(memory_governance_shadow_runs)"
                )
            }
            self.assertIn("diagnostics_json", columns)
            self.assertEqual(
                migrated.execute(
                    "SELECT run_id, diagnostics_json FROM memory_governance_shadow_runs"
                ).fetchall(),
                [("old", "{}")],
            )

            persist_shadow_diagnostics(
                migrated,
                GovernanceRequest(
                    guild_id=1,
                    subject_user_id=2,
                    route_mode="normal_chat",
                    conversation_surface="test",
                    channel_policy="public_home",
                ),
                GovernanceResult("", (), (), GovernanceDiagnostics()),
                "legacy",
            )
            self.assertEqual(
                migrated.execute(
                    "SELECT COUNT(*) FROM memory_governance_shadow_runs"
                ).fetchone()[0],
                2,
            )
        finally:
            migrated.close()

    def test_malformed_governance_aggregates_fail_closed_without_echoing_payload(self):
        persist_shadow_diagnostics(
            self.conn,
            GovernanceRequest(
                guild_id=1,
                subject_user_id=2,
                route_mode="normal_chat",
                conversation_surface="test",
                channel_policy="public_home",
            ),
            GovernanceResult("", (), (), GovernanceDiagnostics()),
            "legacy",
        )
        for column, malformed in (
            ("excluded_json", '{"corrupt":-1}'),
            ("diagnostics_json", '{"visibility_exclusions":"PRIVATE MALFORMED VALUE"}'),
        ):
            with self.subTest(column=column):
                self.conn.execute(
                    "UPDATE memory_governance_shadow_runs "
                    "SET excluded_json='{}', diagnostics_json='{}'"
                )
                self.conn.execute(
                    "UPDATE memory_governance_shadow_runs SET %s=?" % column,
                    (malformed,),
                )
                self.conn.commit()

                snapshot = self.snapshot(
                    {"BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true"}
                )
                report = snapshot["reports"]["memoryGovernance"]
                self.assertEqual(report["reportError"], "ValueError")
                self.assertIn(
                    "memory_governance:report_error:ValueError",
                    snapshot["blockers"],
                )
                self.assertNotIn(malformed, json.dumps(snapshot))

    def test_relationship_report_flattens_reasons_and_zero_emissions_are_visible(self):
        observe_message(
            self.conn,
            guild_id=1,
            user_id=2,
            role="user",
            content="thank you for helping",
            source_row_id="1",
            channel_policy="public_home",
            route_mode="normal_chat",
            directed=True,
            observed_at="2026-07-19T00:00:00+00:00",
        )
        plan_engagement(
            self.conn,
            guild_id=1,
            user_id=2,
            candidate_type="recognition",
            route_mode="relay",
            channel_policy="sealed_test",
            current_direct=True,
            now="2026-07-19T00:01:00+00:00",
        )

        direct = build_relationship_evaluation_report(
            self.conn,
            guild_id=1,
            prepare_schema=False,
        )
        self.assertEqual(direct["shadow_runs"], 1)
        self.assertEqual(direct["would_select"], 0)
        self.assertEqual(direct["live_emission_allowed"], 0)
        self.assertEqual(direct["actual_live_emissions"], 0)
        self.assertEqual(direct["withheld_reasons"]["channel_policy_ineligible"], 1)
        self.assertEqual(direct["withheld_reasons"]["route_ineligible"], 1)

        snapshot = self.snapshot({"BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true"})
        report = snapshot["reports"]["relationshipV2"]
        self.assertEqual(report["engagement_shadow_runs"], 1)
        self.assertEqual(report["engagement_would_select"], 0)
        self.assertEqual(report["engagement_live_emission_allowed"], 0)
        self.assertEqual(report["actual_live_emissions"], 0)

    def test_relationship_subject_or_link_scope_mismatch_is_a_hard_stop(self):
        observe_message(
            self.conn,
            guild_id=1,
            user_id=2,
            role="user",
            content="thank you",
            source_row_id="scope-test",
            channel_policy="public_home",
            route_mode="normal_chat",
            directed=True,
            observed_at="2026-07-19T00:00:00+00:00",
        )
        self.conn.execute(
            "UPDATE relationship_events_v2 SET subject_key='discord_user:999' "
            "WHERE guild_id=1"
        )
        self.conn.commit()

        snapshot = self.snapshot({"BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true"})
        report = snapshot["reports"]["relationshipV2"]
        self.assertGreater(report["cross_guild_member_violations"], 0)
        self.assertIn(
            "relationship_v2:cross_guild_member_violations",
            snapshot["blockers"],
        )

    def test_relationship_target_links_and_privacy_counts_remain_guild_scoped(self):
        event_id = observe_message(
            self.conn,
            guild_id=1,
            user_id=2,
            role="user",
            content="thank you",
            source_row_id="target-scope",
            channel_policy="public_home",
            route_mode="normal_chat",
            directed=True,
            observed_at="2026-07-19T00:00:00+00:00",
        )
        self.conn.execute(
            "UPDATE relationship_event_ledger_links_v2 SET ledger_entry_id='missing-ledger' "
            "WHERE guild_id=1 AND event_id=?",
            (event_id,),
        )
        self.conn.execute(
            "INSERT INTO memory_moment_windows ("
            "moment_id,guild_id,channel_id,topic_key,window_started_at,last_activity_at,"
            "lifecycle_status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                "wrong-member-moment",
                1,
                100,
                "scope-test",
                "2026-07-19T00:00:00+00:00",
                "2026-07-19T00:01:00+00:00",
                "finalized",
                "2026-07-19T00:00:00+00:00",
                "2026-07-19T00:01:00+00:00",
            ),
        )
        self.conn.execute(
            "INSERT INTO memory_moment_participants ("
            "moment_id,participant_key,participant_role,created_at,updated_at) "
            "VALUES ('wrong-member-moment','discord_user:9','user',?,?)",
            (
                "2026-07-19T00:00:00+00:00",
                "2026-07-19T00:00:00+00:00",
            ),
        )
        self.conn.execute(
            "INSERT INTO relationship_event_moment_links_v2 VALUES "
            "(?, 'wrong-member-moment', 1, 2, 'active', ?, ?)",
            (
                event_id,
                "2026-07-19T00:00:00+00:00",
                "2026-07-19T00:00:00+00:00",
            ),
        )
        missing_target_event = observe_message(
            self.conn,
            guild_id=1,
            user_id=3,
            role="user",
            content="thank you too",
            source_row_id="missing-moment-target",
            channel_policy="public_home",
            route_mode="normal_chat",
            directed=True,
            observed_at="2026-07-19T00:00:30+00:00",
        )
        self.conn.execute(
            "INSERT INTO relationship_event_moment_links_v2 VALUES "
            "(?, 'missing-moment', 1, 3, 'active', ?, ?)",
            (
                missing_target_event,
                "2026-07-19T00:00:30+00:00",
                "2026-07-19T00:00:30+00:00",
            ),
        )
        plan_engagement(
            self.conn,
            guild_id=2,
            user_id=9,
            candidate_type="recognition",
            route_mode="relay",
            channel_policy="sealed_test",
            current_direct=True,
            now="2026-07-19T00:01:00+00:00",
        )
        self.conn.commit()

        report = build_relationship_evaluation_report(
            self.conn,
            guild_id=1,
            prepare_schema=False,
        )
        self.assertEqual(report["ledger_link_integrity_violations"], 1)
        self.assertEqual(report["moment_link_integrity_violations"], 2)
        self.assertEqual(report["cross_guild_member_violations"], 0)
        self.assertEqual(report["privacy_rejections"], 0)

        snapshot = self.snapshot({"BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true"})
        self.assertIn(
            "relationship_v2:ledger_link_integrity_violations",
            snapshot["blockers"],
        )
        self.assertIn(
            "relationship_v2:moment_link_integrity_violations",
            snapshot["blockers"],
        )

    def test_live_emission_authorization_or_actual_emission_is_a_hard_stop(self):
        observe_message(
            self.conn,
            guild_id=1,
            user_id=2,
            role="user",
            content="thank you",
            source_row_id="2",
            channel_policy="public_home",
            route_mode="normal_chat",
            directed=True,
            observed_at="2026-07-19T00:00:00+00:00",
        )
        plan_engagement(
            self.conn,
            guild_id=1,
            user_id=2,
            candidate_type="recognition",
            route_mode="normal_chat",
            channel_policy="public_home",
            current_direct=True,
            now="2026-07-19T00:01:00+00:00",
        )
        for column, blocker in (
            ("live_emission_allowed", "engagement_live_emission_allowed"),
            ("actual_emitted", "actual_live_emissions"),
        ):
            with self.subTest(column=column):
                self.conn.execute(
                    "UPDATE relationship_engagement_shadow_runs "
                    "SET live_emission_allowed=0, actual_emitted=0"
                )
                self.conn.execute(
                    "UPDATE relationship_engagement_shadow_runs SET %s=1" % column
                )
                self.conn.commit()

                snapshot = self.snapshot(
                    {"BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true"}
                )
                self.assertIn(
                    "relationship_v2:%s" % blocker,
                    snapshot["blockers"],
                )
                self.assertEqual(
                    snapshot["status"], "blocked_shadow_invariant_failure"
                )

    def test_renderer_does_not_echo_context_selection_details(self):
        snapshot = self.snapshot(
            context={
                "same_room_paired_turn_count": 1,
                "selection_reasons": ["PRIVATE MEMBER DETAIL SHOULD NOT APPEAR"],
            }
        )
        rendered = "\n".join(render_v2_shadow_acceptance_lines(snapshot))
        self.assertNotIn("PRIVATE MEMBER DETAIL", rendered)
        self.assertIn("automatic_cutover: `forbidden`", rendered)
        self.assertIn("no gates or behavior changed", rendered)
        self.assertIn("relationship_legacy_v2_comparison: `not_collected`", rendered)

    def test_context_cross_channel_pair_blocks_acceptance(self):
        snapshot = self.snapshot(
            context={
                "same_room_paired_turn_count": 1,
                "cross_channel_paired_turn_count": 1,
            }
        )
        self.assertEqual(snapshot["status"], "blocked_shadow_invariant_failure")
        self.assertEqual(
            snapshot["conversationContextPreflight"]["status"],
            "failed_cross_channel_pair_detected",
        )
        self.assertIn("conversation_context_cross_channel_pair", snapshot["blockers"])

    def test_all_four_stages_can_reach_owner_review_without_authorizing_cutover(self):
        ledger_result = shadow_conversation_row(
            self.conn,
            row_id=10,
            user_id=2,
            user_name="Test Member",
            guild_id=1,
            role="user",
            content="thank you for helping with the synth patch",
            channel_policy="public_home",
            channel_id=100,
            channel_name="general",
            route_mode="normal_chat",
            observed_at="2026-07-19T00:00:00+00:00",
        )
        for outcome in ("inserted", "deduplicated"):
            record_shadow_receipt(
                self.conn,
                guild_id=1,
                writer="controlled_test",
                source_table="conversations",
                source_row_id=10,
                outcome=outcome,
                reason_code="ok" if outcome == "inserted" else "exact_source_duplicate",
                entry_id=ledger_result.entry_id,
            )
        self.conn.execute(
            "INSERT INTO memory_moment_windows ("
            "moment_id,guild_id,channel_id,channel_name,channel_policy,route_mode,topic_key,"
            "window_started_at,last_activity_at,finalized_at,qualification_type,qualification_reason,"
            "lifecycle_status,visibility,public_usable,salience,human_entry_count,model_entry_count,"
            "participant_count,summary,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "controlled-moment", 1, 100, "general", "public_home", "normal_chat",
                "synth-patch", "2026-07-19T00:00:00+00:00", "2026-07-19T00:01:00+00:00",
                "2026-07-19T00:10:00+00:00", "conversational", "controlled_test",
                "finalized", "public_safe", 1, 0.8, 1, 0, 1, "aggregate test moment",
                "2026-07-19T00:00:00+00:00", "2026-07-19T00:10:00+00:00",
            ),
        )
        self.conn.execute(
            "INSERT INTO memory_moment_diagnostics "
            "(guild_id,moment_id,event_type,reason_code,ledger_entry_id,created_at) "
            "VALUES (1,'controlled-moment','eligible_ledger_entry_observed','ok',?,?)",
            (ledger_result.entry_id, "2026-07-19T00:00:00+00:00"),
        )
        persist_shadow_diagnostics(
            self.conn,
            GovernanceRequest(
                guild_id=1,
                subject_user_id=2,
                route_mode="normal_chat",
                conversation_surface="test",
                channel_policy="public_home",
            ),
            GovernanceResult("", (), (), GovernanceDiagnostics()),
            "legacy",
        )
        observe_message(
            self.conn,
            guild_id=1,
            user_id=2,
            role="user",
            content="thank you",
            source_row_id="acceptance-event",
            channel_policy="public_home",
            route_mode="normal_chat",
            directed=True,
            observed_at="2026-07-19T00:11:00+00:00",
        )
        plan_engagement(
            self.conn,
            guild_id=1,
            user_id=2,
            candidate_type="recognition",
            route_mode="normal_chat",
            channel_policy="public_home",
            current_direct=True,
            now="2026-07-19T00:12:00+00:00",
        )
        persist_unified_assessment_shadow_run(
            self.conn,
            build_unified_response_assessment(
                guild_id=1,
                route_mode="normal_chat",
                channel_policy="public_home",
                conversation_surface="test",
                current_speaker_user_ids=(2,),
                prompt_lanes=("current_exchange",),
            ),
            response="Acknowledged.",
        )
        self.conn.commit()

        snapshot = self.snapshot(
            {
                "BNL_MEMORY_LEDGER_SHADOW_ENABLED": "true",
                "BNL_MOMENT_ENGINE_SHADOW_ENABLED": "true",
                "BNL_MEMORY_GOVERNANCE_SHADOW_ENABLED": "true",
                "BNL_RELATIONSHIP_V2_SHADOW_ENABLED": "true",
            },
            context={"same_room_paired_turn_count": 1},
        )
        self.assertEqual(
            snapshot["status"],
            "ready_for_owner_review_not_live_cutover",
        )
        self.assertTrue(
            all(
                stage["status"] == "candidate_pass_owner_review_required"
                for stage in snapshot["stages"].values()
            )
        )
        self.assertTrue(snapshot["gates"]["all_live_gates_clear"])
        self.assertFalse(snapshot["automaticCutoverAllowed"])


if __name__ == "__main__":
    unittest.main()
