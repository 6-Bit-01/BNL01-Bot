from contextlib import ExitStack
import inspect
import unittest
from unittest import mock

import bnl_source_file_enrichment as enrichment


class SourceFileRefreshDeliveryGuardTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        self.receipts = {}
        self.evidence = {
            "sections": {"Known Facts": ["Test Member has a reviewed source note."]},
            "sourceCounts": {"conversations": 1},
            "warningCounts": {},
            "sourceTypes": ["conversations"],
            "warnings": [],
            "diagnostics": {},
            "classification": {},
        }
        patches = ExitStack()
        self.addCleanup(patches.close)
        patches.enter_context(mock.patch.object(
            enrichment, "collect_source_enrichment_evidence", return_value=self.evidence,
        ))
        patches.enter_context(mock.patch.object(
            enrichment, "build_entity_intelligence_profile", return_value={"diagnostics": {}},
        ))
        patches.enter_context(mock.patch.object(
            enrichment, "resolve_entity_context_for_surface", return_value={},
        ))
        patches.enter_context(mock.patch.object(
            enrichment, "resolve_subject_memory", return_value={},
        ))
        patches.enter_context(mock.patch.object(
            enrichment, "build_source_file_subject_analyst_read_v1", return_value={},
        ))
        patches.enter_context(mock.patch.object(
            enrichment, "build_enrichment_recommendation_payload",
            side_effect=lambda *args, **kwargs: {
                "targetCandidateId": "cand_test_member",
                "subjectName": "Test Member",
                "ingestKey": "bnl:test:source-file-delivery-guard",
            },
        ))
        patches.enter_context(mock.patch.object(
            enrichment, "build_source_file_archive_payload",
            side_effect=lambda *args, **kwargs: {
                "candidateId": "cand_test_member",
                "sourcePackage": {"subjectName": "Test Member"},
            },
        ))
        patches.enter_context(mock.patch.object(
            enrichment, "sanitize_compact_recommendation_payload",
            side_effect=lambda payload, **kwargs: dict(payload),
        ))

    def _lookup(self, query):
        return {
            "ok": True,
            "found": True,
            "sourceFile": {"candidateId": "cand_test_member", "name": "Test Member"},
            "matchKind": "active_source_file",
        }

    def _observe(self, stage, phase, result=None):
        self.events.append((stage, phase))
        if phase == "after":
            self.receipts[stage] = result

    def _archive_sender(self, payload):
        self.assertEqual(payload["candidateId"], "cand_test_member")
        self.events.append(("archive", "send"))
        return {"ok": True, "archiveId": "arc_test_member", "status": 200}

    def _recommendation_sender(self, payload):
        self.assertEqual(payload["targetCandidateId"], "cand_test_member")
        self.events.append(("recommendation", "send"))
        return {"ok": True, "recommendationId": "rec_test_member", "status": 200}

    def _run(self, **overrides):
        self.assertIn("effect_observer", inspect.signature(enrichment.run_source_file_enrichment).parameters)
        options = {
            "lookup_func": self._lookup,
            "sender": self._recommendation_sender,
            "archive_sender": self._archive_sender,
            "environ": {"BNL_SOURCE_FILE_ARCHIVE_TOKEN": "test-archive-token"},
            "effect_observer": self._observe,
        }
        options.update(overrides)
        return enrichment.run_source_file_enrichment(":memory:", 1, "Test Member", **options)

    def _failing_observer(self, failed_stage, failed_phase):
        def observe(stage, phase, result=None):
            self._observe(stage, phase, result)
            if (stage, phase) == (failed_stage, failed_phase):
                raise RuntimeError("delivery guard persistence failed")
        return observe

    def _public_dossier_lookup(self, query):
        if query["lookupKey"] == "candidateId":
            return {
                "ok": True,
                "found": True,
                "matchKind": "candidate_id",
                "data": {
                    "workflowLane": "existing_dossier_update",
                    "sourceFile": {
                        "candidateId": "cand_test_member",
                        "id": "cand_test_member",
                        "name": "Test Member",
                        "status": "existing_dossier_update",
                    },
                },
            }
        return {
            "ok": True,
            "found": True,
            "matchKind": "public_dossier_only",
            "data": {
                "matchKind": "public_dossier_only",
                "workflowLane": "public_dossier_update_target",
                "recommendedAction": "create_existing_dossier_update",
                "targetDossierId": "EN-TEST",
                "publicDossierName": "Test Member",
                "sourceFile": None,
            },
        }

    def _workspace_creator(self, lookup, subject, environ):
        self.assertEqual(subject, "Test Member")
        self.events.append(("workspace", "create"))
        return {"ok": True, "candidateId": "cand_test_member", "targetDossierId": "EN-TEST"}

    def test_delivery_intent_precedes_each_send_and_receipt_follows_it(self):
        result = self._run()

        self.assertEqual(self.events, [
            ("archive", "before"),
            ("archive", "send"),
            ("archive", "after"),
            ("recommendation", "before"),
            ("recommendation", "send"),
            ("recommendation", "after"),
        ])
        self.assertEqual(self.receipts["archive"]["archiveId"], "arc_test_member")
        self.assertEqual(self.receipts["recommendation"]["recommendationId"], "rec_test_member")
        self.assertTrue(result["sent"])

    def test_dry_run_never_observes_or_sends_effects(self):
        result = self._run(dry_run=True)

        self.assertEqual(self.events, [])
        self.assertEqual(result["status"], "dry_run")
        self.assertFalse(result["sent"])

    def test_suppressed_enrichment_never_observes_or_sends_effects(self):
        self.evidence["sections"] = {}
        self.evidence["sourceCounts"] = {}

        result = self._run()

        self.assertEqual(self.events, [])
        self.assertEqual(result["status"], "suppressed_too_thin")
        self.assertFalse(result["sent"])

    def test_failed_archive_intent_prevents_both_deliveries(self):
        with self.assertRaisesRegex(RuntimeError, "delivery guard persistence failed"):
            self._run(effect_observer=self._failing_observer("archive", "before"))

        self.assertEqual(self.events, [("archive", "before")])

    def test_failed_archive_receipt_prevents_recommendation_delivery(self):
        with self.assertRaisesRegex(RuntimeError, "delivery guard persistence failed"):
            self._run(effect_observer=self._failing_observer("archive", "after"))

        self.assertEqual(self.events, [
            ("archive", "before"), ("archive", "send"), ("archive", "after"),
        ])

    def test_failed_recommendation_intent_prevents_its_delivery(self):
        with self.assertRaisesRegex(RuntimeError, "delivery guard persistence failed"):
            self._run(effect_observer=self._failing_observer("recommendation", "before"))

        self.assertEqual(self.events, [
            ("archive", "before"), ("archive", "send"), ("archive", "after"),
            ("recommendation", "before"),
        ])

    def test_workspace_guard_prevents_internal_target_creation(self):
        with self.assertRaisesRegex(RuntimeError, "delivery guard persistence failed"):
            self._run(
                lookup_func=self._public_dossier_lookup,
                workspace_creator=self._workspace_creator,
                effect_observer=self._failing_observer("workspace", "before"),
            )

        self.assertEqual(self.events, [("workspace", "before")])

    def test_workspace_intent_and_receipt_surround_creation_before_delivery(self):
        result = self._run(
            lookup_func=self._public_dossier_lookup,
            workspace_creator=self._workspace_creator,
        )

        self.assertEqual(self.events, [
            ("workspace", "before"), ("workspace", "create"), ("workspace", "after"),
            ("archive", "before"), ("archive", "send"), ("archive", "after"),
            ("recommendation", "before"), ("recommendation", "send"), ("recommendation", "after"),
        ])
        self.assertEqual(self.receipts["workspace"]["candidateId"], "cand_test_member")
        self.assertTrue(result["sent"])


if __name__ == "__main__":
    unittest.main()
