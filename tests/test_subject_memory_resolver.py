import sqlite3
import tempfile
import unittest
import json

import bnl_dossier_draft as draft
from bnl_subject_memory_resolver import build_subject_analyst_read, build_subject_memory_diagnostic, resolve_subject_memory, _review_guidance, _make_reviewable_claim, _cluster_clarification_needs, _build_dossier_readiness, _build_dossier_readiness_from_clarifications, _public_role_candidate_for_subject, _build_review_context, _compose_source_file_review_card


class SubjectMemoryResolverTests(unittest.TestCase):

    def test_clarification_clustering_duplicates_related_source_blind_and_audit(self):
        duplicate_a = _make_reviewable_claim("Crow", "theory/anomaly-heavy", "relationship", "needs_confirmation", "Review-only theory anomaly signal.", False, "low", "theory/anomaly-heavy")
        duplicate_b = _make_reviewable_claim("Crow", "theory/anomaly-heavy", "relationship", "needs_confirmation", "Review-only theory anomaly signal.", False, "low", "theory/anomaly-heavy")
        normal = _make_reviewable_claim("Crow", "Crow is publicly listed as a BARCODE participant.", "role", "public_home", "public-safe fact", True, "high", "public BARCODE participant")
        clustered, needs, audit = _cluster_clarification_needs("Crow", [duplicate_a, duplicate_b, normal])
        self.assertEqual(len(needs), 1)
        self.assertEqual(needs[0]["relationshipType"], "duplicate")
        self.assertEqual(needs[0]["sourceCount"], 2)
        self.assertEqual(needs[0]["relatedReviewClaimIds"], ["reviewableClaims[0]", "reviewableClaims[1]"])
        self.assertTrue(any(c.get("claimType") == "role" and c.get("decisionState") == "decidable" for c in clustered))
        self.assertEqual(len([c for c in clustered if c.get("decisionState") == "needs_clarification"]), 1)
        self.assertFalse(audit)

        theory = _make_reviewable_claim("Crow", "theory/anomaly-heavy", "relationship", "needs_confirmation", "Review-only theory anomaly signal.", False, "low", "theory/anomaly-heavy")
        strange = _make_reviewable_claim("Crow", "strange/anomaly archive evidence", "relationship", "needs_confirmation", "Review-only strange archive evidence signal.", False, "low", "strange/anomaly archive evidence")
        _, related_needs, _ = _cluster_clarification_needs("Crow", [theory, strange])
        need = related_needs[0]
        self.assertEqual(need["relationshipType"], "related")
        self.assertIn("same thing", need["question"])
        self.assertIn("If not", need["question"])
        diffs = " ".join(s["differenceFromOtherSignals"] for s in need["contributingSignals"])
        self.assertIn("theory/anomaly-heavy", diffs)
        self.assertIn("strange/anomaly archive evidence", diffs)
        self.assertIn("theory, interpretation, or pattern", need["contributingSignals"][0]["whatBnlKnows"])
        self.assertIn("archive/evidence", need["contributingSignals"][1]["whatBnlKnows"])

        rules_a = _make_reviewable_claim("Crow", "unclear rules and instructions near Crow", "rules_instructions_context", "needs_confirmation", "rules signal", False, "low", "unclear rules and instructions near Crow")
        rules_b = _make_reviewable_claim("Crow", "unclear rules and instructions near Crow", "rules_instructions_context", "needs_confirmation", "rules signal", False, "low", "unclear rules and instructions near Crow")
        _, rules_needs, _ = _cluster_clarification_needs("Crow", [rules_a, rules_b])
        self.assertEqual(rules_needs[0]["relationshipType"], "duplicate")

        contest_rules = _make_reviewable_claim("Crow", "mysterious contest rules mention Crow", "rules_instructions_context", "needs_confirmation", "rules signal", False, "low", "mysterious contest rules mention Crow")
        admin_rules = _make_reviewable_claim("Crow", "mysterious Discord admin instructions mention Crow", "rules_instructions_context", "needs_confirmation", "rules signal", False, "low", "mysterious Discord admin instructions mention Crow")
        contest_rules["decisionState"] = admin_rules["decisionState"] = "needs_clarification"
        contest_rules["actionability"] = admin_rules["actionability"] = "needs_clarification"
        _, rules_related, _ = _cluster_clarification_needs("Crow", [contest_rules, admin_rules])
        self.assertIn(rules_related[0]["relationshipType"], {"related", "duplicate"})
        self.assertIn("contest rules", rules_related[0]["contributingSignals"][0]["whatBnlKnows"])
        if len(rules_related[0]["contributingSignals"]) > 1:
            self.assertIn("Discord/admin", rules_related[0]["contributingSignals"][1]["whatBnlKnows"])

        blind = _make_reviewable_claim("Crow", "source-blind lore theory anomaly private evidence says secret room", "relationship", "source_blind", "source blind", False, "low", "source-blind private raw evidence says secret room")
        _, blind_needs, _ = _cluster_clarification_needs("Crow", [theory, blind])
        self.assertEqual({n["sourceSafety"] for n in blind_needs}, {"internal_safe", "source_blind_blocked"})
        self.assertNotIn("secret room", json.dumps(blind_needs))

        vague = _make_reviewable_claim("Crow", "unclassified possible claim", "missing_confirmation", "needs_confirmation", "vague", False, "low", "possible signal only")
        _, vague_needs, vague_audit = _cluster_clarification_needs("Crow", [vague])
        self.assertFalse(vague_needs)
        self.assertTrue(any("not enough context" in a for a in vague_audit))

    def test_readiness_uses_consolidated_clarification_questions(self):
        theory = _make_reviewable_claim("Crow", "theory/anomaly-heavy", "relationship", "needs_confirmation", "Review-only theory anomaly signal.", False, "low", "theory/anomaly-heavy")
        strange = _make_reviewable_claim("Crow", "strange/anomaly archive evidence", "relationship", "needs_confirmation", "Review-only strange archive evidence signal.", False, "low", "strange/anomaly archive evidence")
        clustered, needs, _ = _cluster_clarification_needs("Crow", [theory, strange])
        readiness = _build_dossier_readiness("Crow", clustered, [], [])
        readiness = _build_dossier_readiness_from_clarifications(readiness, needs)
        questions = " ".join(q["question"] for q in readiness["questions"])
        self.assertIn("Clarify anomaly/theory context for Crow", questions)
        self.assertIn("Used by 2 related signals", questions)
        self.assertNotEqual(questions.count("What theory or anomaly is this referring"), 2)

    def test_review_cards_include_human_display_copy_and_clean_questions(self):
        packet = {
            "publicSafeFacts": ["Crow is a BARCODE Network community member."],
            "publicCreativeMusicSignals": [],
            "publicRoleSignals": [],
            "publicLinkSignals": [],
            "reviewOnlyEvidence": [
                {"summary": "Crow may have public Suno/music links and repeated music link context needing confirmation."},
                {"summary": "Crow has contest-related music/link context and contest rules links but the relationship is unclear."},
                {"summary": "Crow may be an artist role/title but needs confirmation."},
                {"summary": "subject-owned/keyed local evidence exists"},
            ],
            "queueOrSubmissionSignals": ["Crow queue submission history needs confirmation."],
            "relationshipOrContextSignals": [{"summary": "Crow may reference Orion as AI/message relay context needing confirmation."}],
            "sourceSafetyWarnings": ["Some subject memory lacked public-safe provenance"],
            "privateOrInternalEvidence": [],
            "evidenceCounts": {"publicSafe": 1, "reviewOnly": 5, "privateOrInternal": 0, "sourceBlind": 1, "totalScanned": 7},
        }
        analyst = build_subject_analyst_read("Crow", packet)
        claims = analyst["reviewableClaims"]
        music = next(c for c in claims if c["claimType"] == "music_link")
        self.assertEqual(music["displayTitle"], "Confirm approved public links")
        self.assertEqual(music["displayDecision"], "Which links, if any, may BNL publicly associate with Crow?")
        self.assertEqual(music["confirmationTarget"], "link_ownership")
        self.assertEqual(music["primaryActionLabel"], "Keep as internal note")
        self.assertIn("Which Crow links, music links, or social links", music["verificationPacketQuestion"])
        self.assertFalse(any(c["claimType"] == "contest_music_context" for c in claims))
        self.assertTrue(any("unclear contest/rules wording" in q for q in analyst["missingInfoQuestions"]))
        blind = next(c for c in claims if c.get("actionability") == "source_blind_warning")
        self.assertIn("Need approved public wording", blind["displayTitle"])
        self.assertEqual(blind["confirmationTarget"], "public_source")
        self.assertEqual(blind["primaryActionLabel"], "Request public source")
        self.assertFalse(any(c.get("actionability") == "non_actionable_artifact" for c in claims))
        queue = next(c for c in claims if c["claimType"] == "queue_submission")
        self.assertEqual(queue["displayTitle"], "Confirm queue/submission history")
        self.assertEqual(queue["confirmationTarget"], "admin")
        role = next(c for c in claims if c["claimType"] == "role")
        self.assertEqual(role["confirmationTarget"], "subject")
        cards = analyst["recommendedAdminActionCards"]
        self.assertNotIn("sourceFileReviewClaims", " ".join(c["displayBNLRecommendation"] for c in cards))
        self.assertTrue(any("public name" in c["displayBNLRecommendation"] or "public role" in c["displayBNLRecommendation"] for c in cards))
        display_keys = ("displayTitle", "displayDecision", "displayWhatIsBeingChecked", "displayWhyBNLFlaggedIt", "displayBNLRecommendation", "displayEvidenceSummary", "displaySafeDefault", "displayApprovalInstruction", "primaryActionLabel", "secondaryActionLabels", "verificationPacketQuestion")
        human_blob = json.dumps([{k: c.get(k) for k in display_keys} for c in [*claims, *cards]])
        for forbidden in ("sourceFileReviewClaims", "official_public_dossier", "public_safe", "source_blind", "review_only", "public_music_role", "queue_submission"):
            self.assertNotIn(forbidden, human_blob)


    def test_lane_specific_review_guidance_copy_and_questions(self):
        cases = {
            "link": _review_guidance("Crow", "Crow shared Suno/music links", "music_link", "owner_approval", False),
            "contest": _review_guidance("Crow", "Crow appeared near contest rules", "contest_rules_poster", "owner_approval", False),
            "ai": _review_guidance("Crow", "Crow appears near AI persona project context", "relationship", "owner_approval", False),
            "role": _review_guidance("Crow", "Crow may be a producer role", "role", "owner_approval", False),
            "orion": _review_guidance("Crow", "Crow appears near Orion lore context", "relationship", "owner_approval", False),
            "queue": _review_guidance("Crow", "Crow queue submission history needs confirmation", "queue_submission", "admin", False),
        }
        self.assertEqual(cases["link"]["displayTitle"], "Confirm approved public links")
        self.assertEqual(cases["contest"]["displayTitle"], "Clarify Crow's possible contest/event connection")
        self.assertEqual(cases["ai"]["displayTitle"], "Needs clarification before review")
        self.assertEqual(cases["role"]["displayTitle"], "Confirm public role")
        self.assertEqual(cases["orion"]["displayTitle"], "Confirm Orion/context use")
        self.assertEqual(cases["queue"]["displayTitle"], "Confirm queue/submission history")
        summaries = {name: item["displayEvidenceSummary"] for name, item in cases.items()}
        safe_defaults = {name: item["displaySafeDefault"] for name, item in cases.items()}
        questions = {name: item["verificationPacketQuestion"] for name, item in cases.items()}
        self.assertGreaterEqual(len(set(summaries.values())), 5)
        self.assertGreaterEqual(len(set(safe_defaults.values())), 3)
        self.assertIn("Which Crow links, music links, or social links", questions["link"])
        self.assertIn("contest or event", questions["contest"])
        self.assertIn("AI/persona/project connection", questions["ai"])
        self.assertIn("public role/title", questions["role"])
        self.assertIn("Can BNL mention Orion", questions["orion"])
        generic = "What public-safe source or owner-approved wording supports this claim?"
        self.assertFalse(any(generic in item.get("verificationPacketQuestion", "") for item in cases.values()))
        fallback = _review_guidance("Crow", "unclassified context", "unknown", "needs_confirmation", False)
        self.assertEqual(fallback["verificationPacketQuestion"], "What proof or approved wording should BNL use before saying this publicly?")


    def test_evidence_anchor_answerability_and_review_context_hydration(self):
        contest = _make_reviewable_claim("Crow", "Crow appeared near contest rules and a public link", "contest_rules_poster", "needs_confirmation", "contest", False, "low", "Crow appeared near contest rules and a public link")
        show = _make_reviewable_claim("Crow", "Crow appeared near show-operations planning", "role", "needs_confirmation", "show", False, "low", "Crow appeared near show-operations planning")
        collab = _make_reviewable_claim("Crow", "Crow is interested in collaborating on a song", "relationship", "needs_confirmation", "collab", False, "low", "Crow is interested in collaborating on a song")
        for card, phrase in ((contest, "contest/rules/link context"), (show, "show-operations wording"), (collab, "collaboration-interest wording")):
            blob = json.dumps(card)
            self.assertNotIn("Confirm public role", blob)
            self.assertNotIn("Clarify role/title context", blob)
            self.assertNotIn("What public role/title may BNL use", blob)
            anchor = card.get("evidenceAnchor") or card.get("reviewContext", {}).get("evidenceAnchor", {})
            self.assertIn(phrase, anchor["safeAnchorText"])
            self.assertIn(card.get("answerability") or card.get("reviewContext", {}).get("answerability"), {"partially_answerable", "answerable"})
            self.assertNotIn("BNL found a Source File signal that may matter", blob)
            self.assertNotIn("BNL found dossier-relevant context", blob)

        raw = _make_reviewable_claim("Crow", "contest/event activity", "role", "needs_confirmation", "activity", False, "low", "contest/event activity")
        self.assertEqual("not_answerable", raw["answerability"])
        self.assertEqual("internal_audit", raw["decisionState"])
        self.assertFalse(raw["evidenceAnchor"]["hasAnchor"])

        ctx = _build_review_context("Crow", "Crow appeared near contest rules", "contest_rules_poster", "needs_confirmation", False, "contest_event_activity", "classify_context", "Crow appeared near contest rules", {"publicSafeFacts": ["Crow is a public community member."], "reviewOnlyEvidence": [{"summary": "Crow appeared near contest rules."}], "queueOrSubmissionSignals": ["Crow queue mention"], "relationshipOrContextSignals": []}, {"knownFacts": ["Known Source File fact"], "internalNotes": ["Existing admin note"], "dossierReadiness": {"questions": [{"question": "Which contest was this?"}]}})
        self.assertIn("Known Source File fact", ctx["knownSourceFileFacts"])
        self.assertIn("Crow is a public community member.", ctx["existingPublicFacts"])
        self.assertTrue(ctx["existingInternalNotes"])
        self.assertIn("Which contest was this?", ctx["dossierReadinessNeeds"])


    def test_public_safe_anchored_claims_and_generic_dimensions_keep_approval_actions(self):
        contest = _make_reviewable_claim("Crow", "Crow publicly submitted to the Moon Jam contest", "contest_submitter", "public_home", "public contest submission", True, "high", "Crow publicly submitted to the Moon Jam contest")
        contest["suggestedPublicWording"] = "Crow submitted to the Moon Jam contest."
        contest.update(_review_guidance("Crow", contest["claimText"], contest["claimType"], contest["reviewLane"], True, contest["safeEvidenceSummary"]))
        self.assertEqual("decidable", contest["decisionState"])
        self.assertEqual("approve_public", contest["recommendedAction"])
        self.assertEqual("answerable", contest["answerability"])
        self.assertIn("approve_public_wording", {a["action"] for a in contest["allowedReviewActions"]})

        stale_claim = {
            "publicSafe": True,
            "decisionState": "needs_clarification",
            "recommendedAction": "needs_more_info",
            "allowedReviewActions": [{"action": "ask_follow_up", "label": "Ask follow-up"}],
            "suggestedPublicWording": "Crow submitted to the Moon Jam contest.",
        }
        stale_context = _build_review_context("Crow", "Crow publicly submitted to the Moon Jam contest", "contest_submitter", "public_home", False, "contest_event_activity", "approve_public_fact", "Crow publicly submitted to the Moon Jam contest")
        restored = _compose_source_file_review_card("Crow", stale_claim, "contest_event_activity", "approve_public_fact", stale_context)
        self.assertEqual("decidable", restored["decisionState"])
        self.assertEqual("approve_public", restored["recommendedAction"])
        self.assertEqual("answerable", restored["answerability"])
        self.assertIn("approve_public_wording", {a["action"] for a in restored["allowedReviewActions"]})

        collab = _make_reviewable_claim("Crow", "Owner-confirmed public-safe collaborator credited on a released project", "role", "public_home", "confirmed collaborator", True, "high", "Owner-confirmed public-safe collaborator credited on a released project")
        self.assertEqual("collaboration_status", collab["dossierDimension"])
        self.assertEqual("decidable", collab["decisionState"])
        self.assertEqual("approve_public", collab["recommendedAction"])
        self.assertIn("approve_public_wording", {a["action"] for a in collab["allowedReviewActions"]})

        stale_collab = {
            "publicSafe": True,
            "decisionState": "needs_clarification",
            "recommendedAction": "needs_more_info",
            "allowedReviewActions": [{"action": "ask_follow_up", "label": "Ask follow-up"}],
            "suggestedPublicWording": "Crow is credited as a collaborator on the released project.",
        }
        stale_collab_context = _build_review_context("Crow", "Owner-confirmed public-safe collaborator credited on a released project", "role", "public_home", False, "collaboration_status", "approve_public_fact", "Owner-confirmed public-safe collaborator credited on a released project")
        restored_collab = _compose_source_file_review_card("Crow", stale_collab, "collaboration_status", "approve_public_fact", stale_collab_context)
        self.assertEqual("decidable", restored_collab["decisionState"])
        self.assertEqual("approve_public", restored_collab["recommendedAction"])
        self.assertIn("approve_public_wording", {a["action"] for a in restored_collab["allowedReviewActions"]})

        generic_cases = [
            ("Crow public profile link https://example.com/crow", "public_link", "links_socials"),
            ("Crow public music artist profile", "music_link", "music_artist_context"),
            ("Crow is a public producer role", "role", "public_role_title"),
            ("Crow display name is public", "identity", "public_identity"),
        ]
        for text, claim_type, dimension in generic_cases:
            card = _make_reviewable_claim("Crow", text, claim_type, "public_home", "public-safe", True, "high", text)
            self.assertEqual(dimension, card["dossierDimension"], text)
            self.assertIn("approve_public_wording", {a["action"] for a in card["allowedReviewActions"]}, text)

        raw = _make_reviewable_claim("Crow", "contest/event activity", "role", "needs_confirmation", "activity", False, "low", "contest/event activity")
        self.assertNotIn("approve_public_wording", {a["action"] for a in raw["allowedReviewActions"]})
        protected = _make_reviewable_claim("Crow", "source-blind private Orion detail", "relationship", "source_blind", "protected", False, "low", "source-blind private protected note")
        self.assertNotIn("approve_public_wording", {a["action"] for a in protected["allowedReviewActions"]})

    def test_packet_internal_notes_are_redacted_before_review_context_hydration(self):
        ctx = _build_review_context(
            "Crow",
            "Crow appeared near contest rules",
            "contest_rules_poster",
            "needs_confirmation",
            False,
            "contest_event_activity",
            "classify_context",
            "Crow appeared near contest rules",
            {},
            {"internalNotes": ["source-blind private token secret room detail", "Safe admin note about contest context."], "notes": ["protected DM note with raw id 123456"], "bnlNotes": []},
        )
        notes = ctx["existingInternalNotes"]
        self.assertIn("Protected internal note exists, but details are withheld from this card.", notes)
        self.assertIn("Safe admin note about contest context.", notes)
        blob = json.dumps(notes).lower()
        for forbidden in ("secret room", "token", "raw id", "123456", "dm note"):
            self.assertNotIn(forbidden, blob)

    def test_resolver_finds_and_classifies_crow_memory_across_tables(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE entity_intelligence_facts (subject_key TEXT, subject_name TEXT, fact_label TEXT, fact_value TEXT, visibility TEXT, authority TEXT, public_safe INTEGER, review_only INTEGER, status TEXT)")
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow is publicly listed as a BARCODE community music artist.", "public_home", "public", "public_safe", 1, 0))
            conn.execute("INSERT INTO entity_intelligence_facts VALUES (?,?,?,?,?,?,?,?,?)", ("crow", "Crow", "queue", "Crow queue submission may connect to Raven.", "review_only", "", 0, 1, "active"))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow unknown-policy source-blind music mention.", 0, "active"))
            conn.commit(); conn.close()
            result = resolve_subject_memory("Crow", tmp.name, aliases=["artist", "Crow Alt"])
            self.assertEqual(result["evidenceCounts"]["publicSafe"], 1)
            self.assertEqual(result["evidenceCounts"]["reviewOnly"], 1)
            self.assertEqual(result["evidenceCounts"]["sourceBlind"], 1)
            self.assertIn("Crow is publicly listed", " ".join(result["publicSafeFacts"]))
            self.assertNotIn("artist", result["matchedAliasesUsedPrivately"])
            diagnostic = build_subject_memory_diagnostic("Crow", tmp.name)
            self.assertEqual(diagnostic["candidateMemoriesFound"], 3)


    def test_subject_analyst_synthesizes_crow_like_memory_without_public_overclaim(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            for idx in range(18):
                conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", f"Crow has recurring public BARCODE community interaction pattern {idx}.", "public_home", "public", "public_safe", 1, 0))
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow may be a music artist with queue history needing confirmation.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind artist link mention with no provenance.", 0, "active"))
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe Priority customer raw Discord ID 123456 note.", "public_safe", 1))
            conn.commit(); conn.close()
            resolved = resolve_subject_memory("Crow", tmp.name)
            analyst = build_subject_analyst_read("Crow", resolved)
            self.assertIn("internalRead", analyst)
            self.assertEqual(analyst["likelySubjectType"], "community_participant")
            self.assertLessEqual(len(analyst["draftIngredients"]), 9)
            self.assertTrue(any("role" in q.lower() for q in analyst["missingInfoQuestions"]))
            public_blob = " ".join(analyst["publicSafeClaims"] + analyst["draftIngredients"]).lower()
            for forbidden in ("stripe", "priority", "customer", "discord id", "source-blind", "queue history needing confirmation"):
                self.assertNotIn(forbidden, public_blob)
            self.assertTrue(any("role" in x.lower() or "queue" in x.lower() for x in analyst["reviewNeededClaims"] + analyst["doNotSayPublicly"]))


    def test_analyst_outputs_specific_claims_orion_and_safe_withheld_audit(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow may be a recurring BARCODE community participant who references Orion as an AI/message relay context.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow may have public Suno/music links and queue submission history needing confirmation.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe customer cus_123 paid Priority signal with raw Discord ID 123456789012345678.", "public_safe", 1))
            conn.commit(); conn.close()
            analyst = build_subject_analyst_read("Crow", resolve_subject_memory("Crow", tmp.name))
            review_blob = "\n".join(analyst["reviewNeededClaims"])
            self.assertIn("Possible relationship/context claim", review_blob)
            self.assertIn("Orion", review_blob)
            self.assertIn("Possible queue/submission claim", review_blob)
            self.assertNotIn("Possible role, queue/submission, link, or relationship claims require", review_blob)
            self.assertTrue(analyst["reviewableClaims"])
            self.assertTrue(any(c["claimType"] in {"relationship", "queue_submission", "music_link"} for c in analyst["reviewableClaims"]))
            self.assertTrue(any("Confirm Orion context" in q for q in analyst["missingInfoQuestions"]))
            audit = analyst["withheldEvidenceAudit"]
            self.assertGreaterEqual(audit["totalWithheld"], 1)
            self.assertGreaterEqual(audit["categories"]["private_internal"]["count"], 1)
            audit_blob = str(audit).lower()
            self.assertNotIn("cus_123", audit_blob)
            self.assertNotIn("123456789012345678", audit_blob)
            public_blob = " ".join(analyst["publicSafeClaims"] + analyst["draftIngredients"]).lower()
            self.assertNotIn("orion", public_blob)
            self.assertNotIn("queue", public_blob)
            self.assertNotIn("stripe", public_blob)


    def test_review_and_source_blind_contact_details_are_redacted(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow relationship context: Orion relay details at crow.private@example.com and phone 555-123-4567 need confirmation.", "", "review_only", "", 0, 1))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind Orion context mentions private contact crow.blind@example.com and Discord ID 123456789012345678.", 0, "active"))
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe customer cus_123 paid Priority signal contact crow.pay@example.com raw Discord ID 123456789012345678.", "public_safe", 1))
            conn.commit(); conn.close()
            resolved = resolve_subject_memory("Crow", tmp.name)
            analyst = build_subject_analyst_read("Crow", resolved)
            combined = " ".join([
                str(resolved["sourceSafetyWarnings"]),
                str(analyst["reviewNeededClaims"]),
                str(analyst["reviewableClaims"]),
                str(analyst["sourceBlindInsights"]),
                str(analyst["withheldEvidenceAudit"]),
            ]).lower()
            for leaked in ("crow.private@example.com", "crow.blind@example.com", "crow.pay@example.com", "555-123-4567", "123456789012345678", "cus_123"):
                self.assertNotIn(leaked, combined)
            self.assertTrue(any("Orion" in claim for claim in analyst["reviewNeededClaims"]))
            self.assertTrue(any("withheld" in c.get("safeEvidenceSummary", "").lower() for c in analyst["reviewableClaims"]))
            audit = analyst["withheldEvidenceAudit"]
            self.assertGreaterEqual(audit["categories"]["payment_customer_priority"]["count"], 1)
            public_blob = " ".join(analyst["publicSafeClaims"] + analyst["draftIngredients"]).lower()
            self.assertNotIn("example.com", public_blob)
            self.assertNotIn("orion", public_blob)

    def test_reviewable_claim_suggestions_cover_public_internal_source_blind_artifacts_and_weak_labels(self):
        public_claim = build_subject_analyst_read("Crow", {
            "subjectName": "Crow",
            "matchedAliasesUsedPrivately": [],
            "publicSafeFacts": ["Crow is a public-safe recurring BARCODE community member."],
            "publicSafeNotes": [],
            "publicCommunitySignals": [],
            "publicCreativeMusicSignals": [],
            "publicRoleSignals": [],
            "publicLinkSignals": [],
            "reviewOnlyEvidence": [
                {"summary": "Crow may have public Suno/music links and repeated music link context needing confirmation."},
                {"summary": "Crow may reference Orion as AI/message relay context needing confirmation."},
                {"summary": "subject-owned/keyed local evidence exists"},
                {"summary": "contest organizer"},
            ],
            "queueOrSubmissionSignals": ["Crow queue submission history needs confirmation."],
            "relationshipOrContextSignals": [],
            "sourceSafetyWarnings": ["Some subject memory lacked public-safe provenance"],
            "privateOrInternalEvidence": [],
            "evidenceCounts": {"publicSafe": 1, "reviewOnly": 4, "privateOrInternal": 0, "sourceBlind": 1, "totalScanned": 6},
        })
        claims = public_claim["reviewableClaims"]
        self.assertTrue(any(c.get("suggestedPublicWording") == "Crow is a BARCODE Network community member." for c in claims))
        music = next(c for c in claims if c["claimType"] == "music_link")
        self.assertIn("repeated music/link context", music["suggestedInternalNote"])
        self.assertIn("Which Crow links are owned", music["suggestedMissingInfoQuestion"])
        self.assertNotIn("suggestedPublicWording", music)
        orion = next(c for c in claims if c["claimType"] == "relationship")
        self.assertIn("Orion-related context", orion["suggestedInternalNote"])
        self.assertIn("Can BNL mention Orion", orion["suggestedMissingInfoQuestion"])
        blind = next(c for c in claims if c.get("actionability") == "source_blind_warning")
        self.assertEqual(blind["recommendedAction"], "needs_public_source")
        self.assertIn("Source-blind memory cannot become public copy", blind["cannotSuggestPublicReason"])
        self.assertFalse(any(c.get("actionability") == "non_actionable_artifact" for c in claims))
        weak = next(c for c in claims if c.get("actionability") == "weak_label")
        self.assertIn(weak["recommendedAction"], {"keep_internal", "reject"})
        self.assertIn('weak pattern label: "contest organizer"', weak["suggestedInternalNote"])
        self.assertTrue(any("Can Crow's queue/submission history" in c.get("suggestedMissingInfoQuestion", "") for c in claims))


    def test_source_blind_warnings_are_category_only_for_sensitive_fragments(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE broadcast_memory (cleaned_summary TEXT, public_safe INTEGER, status TEXT)")
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind Orion context email crow.orion@example.com Discord ID 123456789012345678.", 0, "active"))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind music link context at https://private.example/song and phone 555-222-3333.", 0, "active"))
            conn.execute("INSERT INTO broadcast_memory VALUES (?,?,?)", ("Crow source-blind queue submission context for user ID 987654321098765432.", 0, "active"))
            conn.commit(); conn.close()
            resolved = resolve_subject_memory("Crow", tmp.name)
            warnings = "\n".join(resolved["sourceSafetyWarnings"])
            warning_blob = warnings.lower()
            for leaked in ("crow.orion@example.com", "https://private.example/song", "555-222-3333", "123456789012345678", "987654321098765432"):
                self.assertNotIn(leaked, warning_blob)
            self.assertIn("Source-blind Orion/context evidence exists for Crow, but raw details were withheld.", warnings)
            self.assertIn("Source-blind music/link context exists for Crow, but raw details were withheld.", warnings)
            self.assertIn("Source-blind queue/submission context exists for Crow, but raw details were withheld.", warnings)

    def test_private_payment_rejected_and_missing_tables_do_not_crash(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE message_memory (subject_name TEXT, summary TEXT, visibility TEXT, public_safe INTEGER)")
            conn.execute("INSERT INTO message_memory VALUES (?,?,?,?)", ("Crow", "Crow Stripe checkout Priority customer note.", "public_safe", 1))
            conn.commit(); conn.close()
            result = resolve_subject_memory("Crow", tmp.name)
            self.assertEqual(result["evidenceCounts"]["privateOrInternal"], 1)
            self.assertEqual(result["publicSafeFacts"], [])

    def test_draft_uses_public_safe_resolver_memory_not_review_only_music(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow is an unconfirmed music artist from a review-only queue note.", "", "review_only", "", 0, 1))
            conn.commit(); conn.close()
            packet = draft.validate_dossier_draft_packet
            p = {
                "requestType": "bnl_proposed_dossier_draft", "version": "1.0",
                "candidate": {"sourceFileId": "sf_crow", "subjectName": "Crow"},
                "publicSafeFacts": [], "publicSafeNotes": [], "stylePacket": {}, "fieldRequirements": ["summary"],
                "safeClassification": {"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"},
            }
            result = draft.generate_dossier_draft(p, tmp.name)["draft"]
            self.assertNotEqual(result["role"], "Music artist")
            self.assertIn("limited public-safe source detail", result["summary"])
            self.assertTrue(any("resolver scanned" in x for x in result["ownerReviewWarnings"]))

    def test_draft_music_role_requires_public_safe_music(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE entity_evidence_events (subject_name TEXT, safe_summary TEXT, channel_policy TEXT, visibility TEXT, authority TEXT, public_safe_candidate INTEGER, review_only INTEGER)")
            conn.execute("INSERT INTO entity_evidence_events VALUES (?,?,?,?,?,?,?)", ("Crow", "Crow is a public-safe music artist connected to BARCODE Radio.", "public_home", "public", "public_safe", 1, 0))
            conn.commit(); conn.close()
            p = {"requestType": "bnl_proposed_dossier_draft", "version": "1.0", "candidate": {"sourceFileId": "sf_crow", "subjectName": "Crow"}, "publicSafeFacts": [], "publicSafeNotes": [], "stylePacket": {}, "fieldRequirements": ["summary"], "safeClassification": {"category": "Unknown", "kind": "Person", "ecosystemLane": "Unknown"}}
            result = draft.generate_dossier_draft(p, tmp.name)["draft"]
            self.assertEqual(result["role"], "Music artist")
            self.assertIn("subject memory resolver", result["sourceUsageSummary"])

    def test_contest_context_lanes_do_not_infer_organizer_and_humanize_labels(self):
        analyst = build_subject_analyst_read("Crow", {
            "subjectName": "Crow", "matchedAliasesUsedPrivately": [],
            "publicSafeFacts": [], "publicSafeNotes": [], "publicCommunitySignals": [], "publicCreativeMusicSignals": [], "publicRoleSignals": [], "publicLinkSignals": [],
            "reviewOnlyEvidence": [{"summary": "Crow Suno contest name/rules/channel/dates/public link; lore/anomaly public_safe status; official_public_dossier source not connected yet; public artist links / public_music_role"}],
            "queueOrSubmissionSignals": [], "relationshipOrContextSignals": [], "sourceSafetyWarnings": [], "privateOrInternalEvidence": [],
            "evidenceCounts": {"publicSafe": 0, "reviewOnly": 1, "privateOrInternal": 0, "sourceBlind": 0, "totalScanned": 1},
        })
        claim = next(c for c in analyst["reviewableClaims"] if c.get("dossierDimension") == "contest_event_activity")
        self.assertNotIn("organizer", claim["claimType"])
        self.assertEqual(claim["recommendedAction"], "needs_more_info")
        self.assertIn("contest/rules/link context", claim["evidenceAnchor"]["safeAnchorText"])
        self.assertIn("official public dossier", str(claim))
        self.assertNotIn("official_public_dossier", str(claim))
        self.assertNotIn("public_music_role", str(claim))
        self.assertTrue(claim["eventEvidenceHints"]["rulesMentioned"])
        self.assertTrue(any("contest" in q.lower() for q in analyst["missingInfoQuestions"]))

    def test_contest_strong_host_submitter_rules_and_admin_rejection(self):
        host = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Crow hosts the contest with public-safe confirmation."}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":1,"privateOrInternal":0,"sourceBlind":0,"totalScanned":1}})
        self.assertTrue(any(c.get("dossierDimension") == "contest_event_activity" for c in host["reviewableClaims"]))
        submitted = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Crow submitted to the contest as a contest entry."},{"summary":"Crow posted rules for the contest."}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":2,"privateOrInternal":0,"sourceBlind":0,"totalScanned":2}})
        contest_cards = [c for c in submitted["reviewableClaims"] if c.get("dossierDimension") == "contest_event_activity"]
        self.assertTrue(contest_cards)
        self.assertFalse(any(c.get("displayTitle") in {"Confirm public role", "Clarify role/title context"} for c in contest_cards))
        rejected = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Admin note: Crow is not a contest organizer."},{"summary":"Crow contest organizer"}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":2,"privateOrInternal":0,"sourceBlind":0,"totalScanned":2}})
        correction = next(c for c in rejected["reviewableClaims"] if c.get("adminCorrectionApplied"))
        self.assertEqual(correction["recommendedAction"], "reject")
        self.assertEqual(correction["rejectedLabel"], "contest organizer")
        self.assertIn("Do not infer contest organizer", correction["blockedInference"])

    def test_evidence_scoped_contest_hosts_and_vague_suppression(self):
        vague = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Crow was chatting when someone said contest rules are confusing."}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":1,"privateOrInternal":0,"sourceBlind":0,"totalScanned":1}})
        self.assertFalse(any(c["claimType"].startswith("contest_") for c in vague["reviewableClaims"]))
        self.assertTrue(any("What contest or event is this referring to" in q for q in vague["missingInfoQuestions"]))

        hellcat = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Hellcat hosted the Moon Jam contest on Discord and Crow shared the submission link."}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":1,"privateOrInternal":0,"sourceBlind":0,"totalScanned":1}})
        hclaim = next(c for c in hellcat["reviewableClaims"] if c.get("dossierDimension") == "contest_event_activity")
        self.assertIn("contest", hclaim["verificationPacketQuestion"])
        self.assertNotIn("public role/title", hclaim["verificationPacketQuestion"])

        other = build_subject_analyst_read("Crow", {"subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],"reviewOnlyEvidence":[{"summary":"Raven hosted the Signal Battle competition on Twitch; Crow was mentioned nearby."}],"queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],"evidenceCounts":{"publicSafe":0,"reviewOnly":1,"privateOrInternal":0,"sourceBlind":0,"totalScanned":1}})
        oclaim = next(c for c in other["reviewableClaims"] if c.get("dossierDimension") == "contest_event_activity")
        self.assertNotIn("Crow-hosted", json.dumps(oclaim))
        self.assertNotIn("What public role/title may BNL use", json.dumps(oclaim))

    def test_rules_lore_theory_scoped_questions(self):
        rules = _review_guidance("Crow", "Crow saw instructions for queue submitters", "rules_instructions_context", "needs_confirmation", False)
        self.assertEqual(rules["displayTitle"], "Clarify rules/instructions context")
        self.assertIn("who were they for", rules["verificationPacketQuestion"])
        self.assertNotIn("rules/instructions poster", json.dumps(rules).lower())
        lore = _review_guidance("Crow", "Orion lore context near Crow", "relationship", "needs_confirmation", False)
        self.assertIn("Orion", lore["verificationPacketQuestion"])
        unknown_lore = _review_guidance("Crow", "lore-heavy context only", "relationship", "needs_confirmation", False)
        self.assertIn("BARCODE Network lore", unknown_lore["verificationPacketQuestion"])
        theory = _review_guidance("Crow", "theory/anomaly-heavy context", "relationship", "needs_confirmation", False)
        self.assertEqual(theory["displayTitle"], "Needs clarification before review")
        self.assertIn("What theory or anomaly", theory["verificationPacketQuestion"])

    def test_admin_corrections_block_roles_reuse_facts_and_resolve_missing_info(self):
        packet = {
            "adminDecisions": [
                {"type": "rejected weak label", "label": "moderator", "note": "Admin rejected moderator for Crow."},
                {"type": "approved_public_fact", "text": "Crow is a BARCODE Network community member."},
                {"type": "missing_info_answer", "question": "What was Crow’s unclear contest/rules wording?", "answer": "Crow was mentioned around a contest link, but was not the organizer."},
            ]
        }
        analyst = build_subject_analyst_read("Crow", {
            "subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],
            "reviewOnlyEvidence":[{"summary":"Crow may be a moderator."},{"summary":"Crow hosts the contest link roundup."}],
            "queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":[],"privateOrInternalEvidence":[],
            "evidenceCounts":{"publicSafe":0,"reviewOnly":2,"privateOrInternal":0,"sourceBlind":0,"totalScanned":2},
        }, packet)
        self.assertIn("Crow is a BARCODE Network community member.", analyst["publicReadyClaims"])
        self.assertFalse(any("unclear contest/rules wording" in q for q in analyst["missingInfoQuestions"]))
        blocked = [c for c in analyst["reviewableClaims"] if c.get("adminCorrectionApplied")]
        self.assertTrue(any(c.get("rejectedLabel") == "moderator" and c.get("recommendedAction") == "reject" for c in blocked))
        self.assertTrue(any(c.get("rejectedLabel") == "contest organizer" for c in blocked))

    def test_admin_conflict_review_card_and_public_boundary(self):
        packet = {"adminCorrectionNotes": [{"kind": "rejected_label", "text": "Admin rejected artist for Crow."}]}
        analyst = build_subject_analyst_read("Crow", {
            "subjectName":"Crow","matchedAliasesUsedPrivately":[],"publicSafeFacts":[],"publicSafeNotes":[],"publicCommunitySignals":[],"publicCreativeMusicSignals":[],"publicRoleSignals":[],"publicLinkSignals":[],
            "reviewOnlyEvidence":[{"summary":"New public-looking evidence says Crow is an artist with music context."}],
            "queueOrSubmissionSignals":[],"relationshipOrContextSignals":[],"sourceSafetyWarnings":["Crow source-blind payment customer contact crow@example.com raw Discord ID 123456789012345678"],"privateOrInternalEvidence":[],
            "evidenceCounts":{"publicSafe":0,"reviewOnly":1,"privateOrInternal":0,"sourceBlind":1,"totalScanned":2},
        }, packet)
        conflict = next(c for c in analyst["reviewableClaims"] if c.get("reviewLane") == "admin_correction_conflict")
        self.assertEqual(conflict["recommendedAction"], "needs_more_info")
        self.assertIn("Admin previously rejected this label", conflict["suggestedMissingInfoQuestion"])
        public_blob = " ".join(analyst["publicReadyClaims"] + analyst["draftIngredients"]).lower()
        for forbidden in ("admincorrections", "admincorrectionapplied", "rejectedlabel", "blockedinference", "conflictingevidencesummary", "source-blind", "payment", "crow@example.com", "123456789012345678"):
            self.assertNotIn(forbidden.lower(), public_blob)


class RoleVsActivitySemanticsTests(unittest.TestCase):
    def test_activity_labels_are_not_public_roles(self):
        cases = [
            ("contest/event activity near Crow", "contest_event_activity"),
            ("Crow has queue submission history", "queue_submission_activity"),
            ("Crow has lore/context around Orion", "lore_context"),
            ("Crow has theory/anomaly context", "theory_anomaly_context"),
            ("Crow discussed rules/instructions", "rules_instruction_context"),
            ("Crow talked about systems and network behavior", "systems_context"),
        ]
        for text, activity_type in cases:
            role = _public_role_candidate_for_subject("Crow", text, {})
            self.assertFalse(role["isPublicRole"], text)
            self.assertNotEqual("contest/event activity", role["roleLabel"])
            self.assertEqual(activity_type, role["activityContextType"])

    def test_collaboration_interest_is_not_collaborator_role_without_confirmation(self):
        interest = _public_role_candidate_for_subject("Crow", "Crow wants to collaborate and may work together on a song", {})
        self.assertFalse(interest["isPublicRole"])
        self.assertEqual("collaboration_interest", interest["roleEvidenceType"])
        self.assertEqual("collaboration_interest", interest["activityContextType"])

        confirmed = _public_role_candidate_for_subject("Crow", "Owner-confirmed public-safe collaborator credited on a released project", {})
        self.assertTrue(confirmed["isPublicRole"])
        self.assertEqual("collaborator", confirmed["roleLabel"])

    def test_activity_only_role_card_uses_not_confirmed_copy(self):
        card = _review_guidance("Crow", "contest/event activity near Crow", "role", "needs_confirmation", False)
        self.assertEqual("contest_event_activity", card["dossierDimension"])
        self.assertEqual("Clarify Crow's possible contest/event connection", card["displayTitle"])
        self.assertEqual("Was Crow involved in this contest or event, or was he only mentioned nearby?", card["displayDecision"])
        blob = json.dumps(card).lower()
        self.assertNotIn("public role: contest/event activity", blob)
        self.assertIn("not a general role/title", blob)

    def test_universal_follow_up_separates_collaboration_interest(self):
        card = _review_guidance("Crow", "Crow wants to collaborate on a song", "collaboration_interest", "needs_confirmation", False)
        self.assertEqual("collaboration_interest", card["followUpCategory"])
        self.assertIn("only show interest", card["recommendedFollowUpQuestion"])
        self.assertIn("actual public collaboration", card["recommendedFollowUpQuestion"])
        self.assertIn("interest-only vs actual collaboration", card["suggestedConfirmationAnswerType"])

    def test_dossier_readiness_separates_role_from_activity_context(self):
        role_card = _make_reviewable_claim("Crow", "contest/event activity near Crow", "role", "needs_confirmation", "activity only", False, "low", "contest/event activity")
        contest_card = _make_reviewable_claim("Crow", "Crow appeared near contest rules", "contest_rules_poster", "needs_confirmation", "contest context", False, "low", "contest rules")
        collab_card = _make_reviewable_claim("Crow", "Crow wants to collaborate on a song", "collaboration_interest", "needs_confirmation", "collaboration interest", False, "low", "wants to collaborate")
        readiness = _build_dossier_readiness("Crow", [role_card, contest_card, collab_card], [], [])
        joined = " ".join(q["question"] for q in readiness["questions"])
        self.assertNotIn("What public role/title may BNL use for Crow, if any?", joined)
        self.assertIn("contest or event", joined)
        self.assertIn("collaborat", joined.lower())
        self.assertNotIn("role is contest/event activity", joined.lower())

    def test_dimension_decision_cards_for_source_file_review(self):
        contest = _make_reviewable_claim("Crow", "contest/event activity near Crow", "role", "needs_confirmation", "activity", False, "low", "contest/event activity")
        self.assertEqual("contest_event_activity", contest["dossierDimension"])
        self.assertNotEqual("public_role_title", contest["dossierDimension"])
        self.assertEqual("Internal audit only — BNL needs a concrete safe evidence anchor before review.", contest["displayDecision"])
        self.assertFalse(contest.get("suggestedPublicWording"))

        show = _make_reviewable_claim("Crow", "show operations", "role", "needs_confirmation", "show ops", False, "low", "show operations")
        self.assertEqual("show_operations", show["dossierDimension"])
        self.assertEqual("confirm_yes_no", show["cardIntent"])
        self.assertEqual("Internal audit only — BNL needs a concrete safe evidence anchor before review.", show["displayDecision"])
        self.assertEqual({"ask_follow_up", "keep_internal", "reject"}, {a["action"] for a in show["allowedReviewActions"]})
        self.assertFalse(show.get("suggestedPublicWording"))

        interest = _make_reviewable_claim("Crow", "Crow is interested in collaborating with BARCODE", "relationship", "needs_confirmation", "collab", False, "low", "interested in collaborating")
        self.assertEqual("collaboration_interest", interest["dossierDimension"])
        self.assertIn("only show interest", interest["displayDecision"])
        self.assertIn("BARCODE or another project", interest["recommendedFollowUpQuestion"])

        wants = _make_reviewable_claim("Crow", "Crow wants to collaborate on a song", "relationship", "needs_confirmation", "collab", False, "low", "wants to collaborate")
        self.assertEqual("collaboration_interest", wants["dossierDimension"])
        confirmed = _make_reviewable_claim("Crow", "Owner-confirmed public-safe collaborator credited on a released project", "role", "needs_confirmation", "collab", False, "low", "confirmed collaborator")
        self.assertEqual("collaboration_status", confirmed["dossierDimension"])

        protected = _make_reviewable_claim("Crow", "source-blind private Orion detail", "relationship", "source_blind", "protected", False, "low", "SECRET PRIVATE SOURCE-BLIND DETAIL")
        self.assertEqual("source_protected_context", protected["dossierDimension"])
        self.assertIn("Need approved public wording", protected["displayTitle"])
        self.assertNotIn("SECRET", json.dumps(protected))
        self.assertEqual(["keep_internal", "add_approved_public_wording", "attach_public_source", "ask_owner_admin", "reject"], [a["action"] for a in protected["allowedReviewActions"]])

    def test_non_role_dimensions_have_specific_copy(self):
        cases = [
            ("Crow has queue submission history", "queue_submission", "queue_submission_activity"),
            ("Crow shared a social link", "public_link", "links_socials"),
            ("Crow has lore/context around Orion", "relationship", "lore_context"),
            ("Crow has theory/anomaly context", "relationship", "theory_anomaly_context"),
            ("Crow discussed rules/instructions", "rules_instructions_context", "rules_instruction_context"),
            ("Crow talked about systems and network behavior", "relationship", "systems_context"),
        ]
        titles = set()
        for text, claim_type, dimension in cases:
            card = _make_reviewable_claim("Crow", text, claim_type, "needs_confirmation", "why", False, "low", text)
            self.assertEqual(dimension, card["dossierDimension"], text)
            self.assertNotEqual("Public role/title not confirmed", card["displayTitle"])
            titles.add(card["displayTitle"])
        self.assertGreaterEqual(len(titles), 5)

    def test_role_and_identity_link_only_do_not_create_role_blockers(self):
        link = _make_reviewable_claim("Crow", "https://example.com/crow", "public_link", "needs_confirmation", "link", False, "low", "link")
        ident = _make_reviewable_claim("Crow", "Crow display name", "identity", "needs_confirmation", "identity", False, "low", "identity")
        role = _make_reviewable_claim("Crow", "Crow is a producer", "role", "needs_confirmation", "role", False, "low", "Crow is a producer")
        self.assertNotEqual("public_role_title", link["dossierDimension"])
        self.assertNotEqual("public_role_title", ident["dossierDimension"])
        self.assertEqual("public_role_title", role["dossierDimension"])

    def test_evidence_grounded_card_composer_replaces_template_notes(self):
        contest = _make_reviewable_claim("Crow", "contest/event activity near Crow", "role", "needs_confirmation", "activity", False, "low", "contest/music/link context near Crow")
        self.assertNotEqual("BNL found contest event activity for Crow; keep internal until reviewed.", contest["suggestedInternalNote"])
        self.assertIn("available context does not confirm", contest["suggestedInternalNote"])
        self.assertIn("participated", contest["suggestedInternalNote"])
        self.assertNotIn(contest["displayTopic"], {"contest/event activity", "contests/events"})
        self.assertFalse(contest.get("suggestedPublicWording"))
        self.assertIn(contest["specificityLevel"], {"specific", "partially_specific"})
        self.assertEqual("contest_event_activity", contest["dossierDimension"])
        self.assertEqual("classify_context", contest["cardIntent"])

        show = _make_reviewable_claim("Crow", "show operations", "role", "needs_confirmation", "show ops", False, "low", "show operations context near Crow")
        self.assertNotEqual("BNL found show operations for Crow; keep internal until reviewed.", show["suggestedInternalNote"])
        self.assertIn("actually helps with show operations", show["suggestedInternalNote"])
        self.assertEqual("Crow's possible show-operations involvement", show["displayTopic"])

        collab = _make_reviewable_claim("Crow", "Crow wants to collaborate on a song", "relationship", "needs_confirmation", "collab", False, "low", "collaboration interest around a song")
        self.assertIn("distinguish interest in collaborating from an actual approved collaboration", collab["suggestedInternalNote"])

        protected = _make_reviewable_claim("Crow", "source-blind private Orion detail", "relationship", "source_blind", "protected", False, "low", "SECRET PRIVATE SOURCE-BLIND DETAIL")
        self.assertIn("Protected context exists for Crow", protected["suggestedInternalNote"])
        self.assertEqual("Protected Crow context that needs public-safe wording", protected["displayTopic"])
        self.assertNotIn("SECRET", json.dumps(protected))

    def test_raw_label_only_card_is_clarification_not_normal_review(self):
        card = _make_reviewable_claim("Crow", "contest/event activity", "role", "needs_confirmation", "activity", False, "low", "contest/event activity")
        self.assertEqual("generic", card["specificityLevel"])
        self.assertEqual("internal_audit", card["decisionState"])
        self.assertEqual("keep_internal", card["recommendedAction"])
        self.assertEqual("not_answerable", card["answerability"])
        self.assertNotEqual("contest/event activity", card["evidenceSummary"])


if __name__ == '__main__':
    unittest.main()

class DossierReadinessPacketTests(unittest.TestCase):
    def test_dossier_readiness_packet_collapses_generic_review_questions(self):
        packet = {
            "reviewOnlyEvidence": [
                {"summary": "Crow has Suno music link context needing public-safe provenance."},
                {"summary": "Crow shared another music link that needs owner-approved wording."},
                {"summary": "Crow appears near contest rules and submission link context."},
                {"summary": "Crow may have artist role/title context needing confirmation."},
                {"summary": "Crow has Orion relay context needing confirmation."},
                {"summary": "Crow has lore-heavy context without a specific public fact."},
                {"summary": "subject-owned/keyed local evidence exists"},
            ],
            "relationshipOrContextSignals": [{"summary": "Crow may reference Orion as AI/message relay context needing confirmation."}],
            "sourceSafetyWarnings": ["Some subject memory lacked public-safe provenance"],
            "evidenceCounts": {"publicSafe": 0, "reviewOnly": 7, "privateOrInternal": 0, "sourceBlind": 1, "totalScanned": 8},
        }
        analyst = build_subject_analyst_read("Crow", packet)
        questions = analyst["dossierReadinessQuestions"]
        self.assertLessEqual(len(questions), 7)
        joined = " ".join(q["question"] for q in questions)
        self.assertIn("Which links are Crow", joined)
        self.assertIn("contest or event", joined)
        self.assertNotIn("What public role/title may BNL use", joined)
        self.assertIn("Orion", joined)
        self.assertIn("Crow's lore, BARCODE lore, Orion lore, Null TV lore, or a recurring topic", joined)
        self.assertNotIn("What public-safe source or owner-approved wording supports this claim", joined)
        self.assertIn("readyForDraft", analyst)
        self.assertIn("draftReadinessReason", analyst)
        self.assertFalse(analyst["readyForDraft"])
        self.assertNotIn("sourceFileReviewClaims", " ".join(analyst["recommendedAdminActions"]))

    def test_lore_heavy_card_uses_decision_copy_not_public_source(self):
        card = _review_guidance("Crow", "Crow lore-heavy context only", "relationship", "needs_confirmation", False)
        self.assertEqual(card["displayTitle"], "Confirm Crow's lore use")
        self.assertIn("Crow's lore", card["verificationPacketQuestion"])
        self.assertNotIn("public-safe source", card["verificationPacketQuestion"].lower())

    def test_scoped_follow_up_populates_current_and_legacy_fields(self):
        generic = "What proof or approved wording should BNL use before saying this publicly?"
        cases = [
            ("ai", _review_guidance("Crow", "Crow appears near AI persona project context", "relationship", "needs_confirmation", False), "AI/persona/project connection", "clarification of the AI/persona/project"),
            ("lore", _review_guidance("Crow", "lore-heavy context only", "relationship", "needs_confirmation", False), "BARCODE Network lore", "clarification of the lore/context"),
            ("theory", _review_guidance("Crow", "unknown theory anomaly near Crow", "relationship", "needs_confirmation", False), "What theory or anomaly", "clarification of the theory/anomaly"),
            ("rules", _review_guidance("Crow", "unclear rules and instructions near Crow", "rules_instructions_context", "needs_confirmation", False), "What rules or instructions", "clarification of the instruction source"),
            ("contest", _review_guidance("Crow", "Crow appeared near contest rules", "contest_rules_poster", "needs_confirmation", False), "contest or event", "event name, subject relationship"),
        ]
        for _name, card, expected, answer_type in cases:
            question = card["recommendedFollowUpQuestion"]
            self.assertIn(expected, question)
            self.assertNotEqual(generic, question)
            self.assertEqual(card["suggestedConfirmationQuestion"], question)
            self.assertEqual(card["suggestedMissingInfoQuestion"], question)
            self.assertEqual(card["verificationPacketQuestions"], [question])
            self.assertEqual(card["verificationPacketQuestion"], question)
            self.assertEqual(card["suggestedConfirmationAudience"], card["recommendedFollowUpAudience"])
            self.assertIn(answer_type, card["suggestedConfirmationAnswerType"])
            self.assertNotEqual("BNL found context that needs a human decision before it can be used publicly.", card["suggestedConfirmationReason"])
            self.assertEqual(card["suggestedConfirmationReason"], card["recommendedFollowUpReason"])

        blind = _review_guidance("Crow", "source-blind lore theory anomaly private evidence says secret room", "relationship", "source_blind", False)
        self.assertEqual(blind["suggestedConfirmationQuestion"], "Should this protected context stay internal, or is there approved public wording BNL can use?")
        self.assertIn("approved public wording", blind["suggestedConfirmationAnswerType"])
        self.assertNotIn("secret room", blind["suggestedConfirmationQuestion"].lower())

        for _name, card, _expected, _answer_type in cases:
            self.assertEqual(card["decisionState"], "needs_clarification")
            self.assertFalse(card["publicSafe"])
            self.assertFalse(card["hasSafePublicSuggestion"])
            self.assertEqual(card["primaryActionLabel"], "Add clarification question")
            self.assertNotIn("Confirm exact public-safe wording before use", json.dumps(card))
            self.assertNotIn("approve_public", json.dumps(card))

        fallback = _review_guidance("Crow", "unclassified possible claim", "missing_confirmation", "needs_confirmation", False)
        self.assertEqual(fallback["suggestedMissingInfoQuestion"], generic)
