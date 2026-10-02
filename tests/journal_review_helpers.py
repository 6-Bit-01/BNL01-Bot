"""Explicit mocked editor responses for Journal flow tests, not an entailment oracle.

These helpers bind the supplied unit IDs and exact source anchors. Their chosen
verdict is fixture input; a passing flow test does not prove semantic accuracy.
Production code must never import this module.
"""
from functools import wraps
import json

from bnl_journal_attribution import ASSESSMENT_CHECKS, REVIEW_PREFIX, source_authority


def is_source_review(prompt):
    return prompt.startswith(REVIEW_PREFIX)


def review_inputs(prompt):
    if not is_source_review(prompt):
        raise AssertionError("Expected the source-only Journal editor prompt")
    decoder = json.JSONDecoder()
    units, _ = decoder.raw_decode(prompt.split("CANDIDATE_UNITS_JSON: ", 1)[1])
    evidence, _ = decoder.raw_decode(prompt.split("ORIGINAL_EVIDENCE_JSON: ", 1)[1])
    return units, evidence


def fixture_grounding(text, *, factual=True, source_meaning="Controlled source meaning."):
    """Explicit protocol fixture, never a semantic judgment about test prose."""
    return {
        "externalPremises": [{
            "claim": text, "claimType": "reported_speech", "sourceStance": "assertion",
            "evidenceIndexes": [0], "sourceMeaning": source_meaning, "support": "entails",
            "assumptions": [], "evidenceScope": "recorded_content",
        }] if factual else [],
        "nonFactualReason": "Controlled fixture has only personal or imagined expression." if not factual else "",
    }


def supported_review(prompt):
    """Supply a test-approved verdict with structurally genuine source anchors."""
    units, evidence = review_inputs(prompt)
    sources = evidence["sources"]
    anchor = None
    for source in sources:
        for contribution in source.get("contributions") or [source]:
            if contribution.get("summary"):
                anchor = {"refId": source["refId"], "quote": contribution["summary"],
                          "speaker": str(contribution.get("participantAlias") or ""),
                          "use": "speech" if source_authority(source) in {"original", "speech_only"} else "context"}
                break
        if anchor:
            break
    if not anchor:
        raise AssertionError("Mock review needs an actual supplied original source")
    return json.dumps({"assessments": [
        {"check": check, "unitIds": [unit["unitId"] for unit in units],
         "sourceRefIds": [anchor["refId"]],
         "explanation": "This controlled fixture supplies a passing " + check + " verdict; not model-quality evidence.",
         "issues": [], "verdict": "supported"} for check in ASSESSMENT_CHECKS
    ], "units": [
        {"unitId": unit["unitId"], "spans": [
            {"text": unit["text"], "kind": "factual", "evidence": [anchor],
             "grounding": fixture_grounding(unit["text"], source_meaning=anchor["quote"]),
             "issues": [], "verdict": "supported"}]} for unit in units
    ], "verdict": "supported"})


def rejected_review(prompt, *, issue="The candidate reverses the original attribution."):
    response = json.loads(supported_review(prompt))
    response["verdict"] = "unsupported"
    target = next(unit for unit in response["units"] if ".body:" in unit["unitId"])
    target["spans"][0].update(verdict="unsupported", evidence=[], issues=[issue])
    target["spans"][0]["grounding"]["externalPremises"][0].update(
        evidenceIndexes=[], support="unknown", assumptions=[issue])
    return json.dumps(response)


def supported_review_with_anchor(prompt, *, unit_id, source_ref):
    """Choose one exact evidence dependency, not a semantic support judgment."""
    response = json.loads(supported_review(prompt))
    _, evidence = review_inputs(prompt)
    source = next(source for source in evidence["sources"] if source["refId"] == source_ref)
    contribution = next(item for item in source.get("contributions") or [source] if item.get("summary"))
    unit = next(unit for unit in response["units"] if unit["unitId"] == unit_id)
    unit["spans"][0]["evidence"] = [{
        "refId": source_ref, "quote": contribution["summary"],
        "speaker": str(contribution.get("participantAlias") or ""),
        "use": "speech" if source_authority(source) in {"original", "speech_only"} else "context",
    }]
    return json.dumps(response)


def with_supported_review(writer):
    """Keep an existing mock writer while explicitly mocking source review."""
    @wraps(writer)
    def generate(packet, prompt):
        return supported_review(prompt) if is_source_review(prompt) else writer(packet, prompt)
    return generate


def reviewed_article(article, packet):
    """Attach a controlled protocol receipt for storage/fence tests only."""
    import copy
    import bnl_journal as journal
    import bnl_journal_attribution as attribution
    result = copy.deepcopy(article)
    evidence = journal._source_review_evidence(packet)
    prompt = attribution.review_prompt(result, evidence)
    receipt, reason, _ = attribution.accept_review(
        supported_review(prompt), result, evidence["sources"],
        context_contract=journal._source_review_context_contract(packet),
    )
    if reason:
        raise AssertionError("Controlled review fixture failed: " + reason)
    result.setdefault("metadata", {})["sourceReview"] = receipt
    return result
