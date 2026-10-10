"""The R&D summary must not override current, governed capability evidence."""

import ast
from pathlib import Path
import unittest
from unittest import mock


SOURCE = Path(__file__).resolve().parents[1] / "bnl01_bot.py"


class OperationsCapabilityContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Follow the source-definition harness used by other bot tests. This
        # executes the real helper and prompt expression without bot startup.
        tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
        helper = next(node for node in tree.body
                      if isinstance(node, ast.FunctionDef)
                      and node.name == "build_operations_brief_context")
        cls.helper_code = compile(ast.Module(body=[helper], type_ignores=[]), str(SOURCE), "exec")
        on_message = next(node for node in tree.body
                          if isinstance(node, ast.AsyncFunctionDef) and node.name == "on_message")
        assignments = [node for node in ast.walk(on_message)
                       if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == "ops_prompt"
                               for target in node.targets)]
        assert len(assignments) == 1, "Expected one generic R&D prompt owner"
        cls.prompt_code = compile(ast.Expression(body=assignments[0].value), str(SOURCE), "eval")

    def setUp(self):
        self.memory = mock.Mock(return_value=[])
        self.namespace = {
            "get_active_show_state_override": mock.Mock(return_value=None),
            "get_recent_broadcast_memory": self.memory,
            "get_bnl_control_flags": mock.Mock(return_value={"websiteRelayEnabled": False}),
            "BNL_STATUS_URL": "", "BNL_API_KEY": "",
            "is_internal_operations_request": mock.Mock(return_value=False),
            "_safe_truncate_summary": lambda text, limit: text[:limit],
        }
        exec(self.helper_code, self.namespace)

    def brief(self):
        return self.namespace["build_operations_brief_context"](123, "What is the current status?")

    def prompt(self, evidence=""):
        return eval(self.prompt_code, {
            "ops_context": self.brief(), "rd_read_model_context": evidence,
            "rd_intent": "operations_brief", "clean_content": "What is the current status?",
        })

    def assert_no_static_capability_claims(self, text):
        for claim in (
            "website dossiers not connected yet", "queue runtime not connected yet",
            "payment event state not connected yet", "public chatter layer not implemented yet",
        ):
            self.assertNotIn(claim, text)

    def test_operations_summary_does_not_assert_static_capability_gaps(self):
        self.assert_no_static_capability_claims(self.brief())

    def test_missing_evidence_is_unknown_rather_than_connected_or_disconnected(self):
        prompt = self.prompt()
        self.assertIn("Without fresh eligible evidence, current state is unknown", prompt)
        self.assertIn("do not infer connected, disconnected, or unimplemented", prompt)
        self.assertIn("Do not imply website dossier, queue, or payment integrations are live unless explicitly stated in context", prompt)

    def test_fresh_queue_and_tiktok_evidence_is_not_contradicted(self):
        evidence = (
            "Current eligible read-model evidence: queueProduction=true; accessScope=private.\n"
            "TikTok: enabled; snapshot valid; age=1s; status=reconnecting; buffered text=12."
        )
        prompt = self.prompt(evidence)
        self.assertIn(evidence, prompt)
        self.assert_no_static_capability_claims(prompt)
        self.assertIn("Use only fresh, eligible source context supplied for this request", prompt)

    def test_configured_bridge_does_not_establish_live_health(self):
        self.namespace.update(BNL_STATUS_URL="https://example.invalid/status", BNL_API_KEY="fixture-secret")
        self.namespace["get_bnl_control_flags"].return_value = {"websiteRelayEnabled": True}
        brief = self.brief()
        self.assertIn("Website bridge configured: yes", brief)
        self.assertIn("Website relay flag: enabled", brief)
        self.assertIn("Configuration and enabled flags do not establish current runtime health", brief)
        self.assertNotIn("fixture-secret", brief)
        self.assertNotIn("example.invalid", brief)

    def test_restricted_override_and_public_memory_boundary_are_preserved(self):
        self.namespace["get_active_show_state_override"].return_value = (
            1, "2026-10-09", "restricted fixture detail", False, None, "2026-10-09",
        )
        brief = self.brief()
        self.assertIn("summary is restricted", brief)
        self.assertNotIn("restricted fixture detail", brief)
        self.memory.assert_called_once_with(123, public_only=True, limit=5)

    def test_prompt_keeps_privacy_and_non_mutating_boundaries(self):
        prompt = self.prompt()
        self.assertIn("Do not expose raw database rows, raw notes, or restricted internal details", prompt)
        self.assertIn("Do not convert research-and-development operations discussion into canon", prompt)
        self.assertIn("Do not publish, relay, or write broadcast memory automatically", prompt)
        self.assertIn("Existing privacy, channel-access, and production gates still apply", prompt)


if __name__ == "__main__":
    unittest.main()
