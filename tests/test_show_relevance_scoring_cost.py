"""Relevance must not build attributed output records just to score text."""

import ast
import copy
import inspect
import textwrap
import unittest
from collections import UserDict
from unittest import mock

import bnl_tiktok_show_ledger as shows


def legacy_relevance():
    """Use the unchanged owner with its pre-optimization scoring expression.

    Only this expression is frozen: the oracle still exercises the whole
    subject/date/topic owner, not a separately invented ranking formula.
    """
    owner = ast.parse(textwrap.dedent(inspect.getsource(shows._document_relevance)))
    legacy = ast.parse("""
authored_overlap = max((
    len(topic_terms.intersection(_query_terms(str(message.get("text") or ""))))
    for message in _authored_show_messages(ledger)
    if not authored_subject_refs or str(message.get("subjectRef") or "") in authored_subject_refs
), default=0)
""").body[0]
    replaced = 0
    for index, node in enumerate(owner.body[0].body):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "authored_overlap"
            for target in node.targets
        ):
            owner.body[0].body[index] = legacy
            replaced += 1
    if replaced != 1:
        raise AssertionError("Expected one owned authored-overlap computation")
    scope = dict(vars(shows))
    exec(compile(ast.fix_missing_locations(owner), "<legacy relevance oracle>", "exec"), scope)
    return scope["_document_relevance"]


class ShowRelevanceScoringCostTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.legacy = staticmethod(legacy_relevance())

    def fixture(self):
        return {
            "showDate": "2026-10-02",
            "participants": [
                {"subjectRef": "tiktok_user:sparrow", "speakerLabel": "Test Sparrow", "handle": "test_sparrow"},
                {"subjectRef": "tiktok_user:comet", "speakerLabel": "Test Comet"},
            ],
            "discordParticipants": [
                {"subjectRef": "discord_user:77", "speakerLabel": "Test Lantern"},
            ],
            "messages": [
                {"subjectRef": "tiktok_user:sparrow", "speakerLabel": "Test Sparrow", "text": "Copper lantern quartz nebula drumming"},
                {"subjectRef": "tiktok_user:comet", "speakerLabel": "Test Comet", "text": "Café ÉCHO snowflake Δέλτα bass-line 123"},
                {"subjectRef": "tiktok_user:sparrow", "speakerLabel": "Other label", "text": "copper copper copper"},
                None, "not a message", 123, {}, {"text": None},
                UserDict({"subjectRef": "tiktok_user:sparrow", "text": "quartz NEBULA"}),
            ],
            "discordInteractions": [
                {
                    "subjectRef": "discord_user:77", "speakerLabel": "Test Lantern",
                    "userMessages": [
                        # The exchange owns attribution, not this leaf's label/ref.
                        {"subjectRef": "tiktok_user:comet", "text": "copper quartz nebula drumming lantern", "conversationRowId": 1},
                        {}, None, "bad", {"text": 0}, {"text": ["copper", "lantern"]},
                    ],
                    "bnlResponse": {"text": "hidden unique model phrase"},
                },
                None, "bad exchange", {}, {"userMessages": None},
                UserDict({"subjectRef": "discord_user:88", "userMessages": [{"text": "snowflake bass-line"}]}),
            ],
            "trackMoments": [{"trackLabel": "Test Melody"}],
            "trackRoster": [{"title": "Quartz Rhythm", "projectLabel": "Test Album"}],
            "showTopics": [{"term": "nebula"}],
            "operationalEvents": [{"eventType": "broadcast_started", "detail": "copper lantern"}],
        }

    def assert_parity(self, ledger, **overrides):
        options = dict(user_text="copper lantern", subject_ref="", recency_rank=0)
        options.update(overrides)
        before = copy.deepcopy(ledger)
        expected = self.legacy(ledger, **options)
        actual = shows._document_relevance(ledger, **options)
        self.assertEqual(actual, expected)
        self.assertEqual(ledger, before, "Relevance must not edit original sources")
        return actual

    def test_complete_score_and_participant_parity(self):
        ledger = self.fixture()
        cases = [
            {},
            {"user_text": ""},
            {"user_text": "hello"},
            {"user_text": "copper copper"},
            {"user_text": "copper lantern quartz nebula drumming"},
            {"user_text": "CAFÉ écho snowflake ΔΈΛΤΑ bass-line 123"},
            {"user_text": "hidden unique model phrase"},
            {"user_text": "Test Sparrow copper lantern"},
            {"user_text": "Test Comet copper lantern quartz"},
            {"user_text": "Test Lantern copper quartz nebula"},
            {"user_text": "What did Test Sparrow say during the show?"},
            {"user_text": "What did Unknown Person say about copper lantern?"},
            {"user_text": "What did I say about copper lantern?", "subject_ref": "discord_user:77", "allow_direct_subject": True},
            {"user_text": "What did I say about copper lantern?", "subject_ref": "discord_user:999", "allow_direct_subject": True},
            {"user_text": "copper lantern", "requested_dates": ("2026-10-02",)},
            {"user_text": "copper lantern", "requested_dates": ("2026-10-01",)},
            {"user_text": "copper lantern", "named_subject_refs": {"tiktok_user:comet"}},
            {"user_text": "copper lantern", "named_subject_refs": {"discord_user:999"}},
            {"user_text": "Test Melody Quartz Rhythm show broadcast started"},
            {"user_text": "copper lantern", "recency_rank": 99},
        ]
        for case in cases:
            with self.subTest(case=case):
                self.assert_parity(ledger, **case)
        for empty in ({}, {"messages": None, "discordInteractions": None},
                      {"messages": [None, 0, "bad", {}], "discordInteractions": [None, 0, "bad", {}]}):
            with self.subTest(empty=empty):
                self.assert_parity(empty)

    def test_overlap_threshold_and_cap_preserve_full_owner_result(self):
        for text, expected_score in (("copper", 0), ("copper lantern", 180),
                                     ("copper lantern quartz", 260),
                                     ("copper lantern quartz nebula drumming", 260)):
            ledger = {"messages": [{"text": text}]}
            with self.subTest(text=text):
                self.assertEqual(self.assert_parity(
                    ledger, user_text="copper lantern quartz nebula drumming",
                ), (expected_score, []))

    def test_malformed_container_errors_remain_visible(self):
        for ledger in ({"messages": 17}, {"discordInteractions": 17},
                       {"discordInteractions": [{"userMessages": 17}]},
                       {"messages": [{"text": "copper lantern quartz"}],
                        "discordInteractions": [{"userMessages": 17}]}):
            options = dict(user_text="copper lantern quartz", subject_ref="", recency_rank=0)
            with self.subTest(ledger=ledger):
                with self.assertRaises(TypeError):
                    self.legacy(ledger, **options)
                with self.assertRaises(TypeError):
                    shows._document_relevance(ledger, **options)

    def test_large_scoring_read_never_materializes_or_labels_messages(self):
        ledger = {
            "messages": [{"text": "copper lantern", "subjectRef": "tiktok_user:sparrow", "speakerLabel": "Test Sparrow"} for _ in range(6000)],
            "discordInteractions": [{"subjectRef": "discord_user:77", "speakerLabel": "Test Lantern",
                "userMessages": [{"text": "quartz nebula", "conversationRowId": index} for index in range(6000)]}],
        }
        options = dict(user_text="copper lantern quartz nebula", subject_ref="", recency_rank=0)
        expected = self.legacy(ledger, **options)
        with mock.patch.object(shows, "_authored_show_messages", side_effect=AssertionError("unneeded attributed message copies")), \
             mock.patch.object(shows, "_public_show_speaker_label", side_effect=AssertionError("unneeded per-message labels")):
            self.assertEqual(shows._document_relevance(ledger, **options), expected)


if __name__ == "__main__":
    unittest.main()
