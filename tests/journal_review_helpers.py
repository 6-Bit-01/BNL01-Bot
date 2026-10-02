"""Explicit mocked editor responses for Journal flow tests, not an entailment oracle.

These helpers bind the supplied unit IDs and exact source anchors. Their chosen
verdict is fixture input; a passing flow test does not prove semantic accuracy.
Production code must never import this module.
"""
from functools import wraps
import json

from bnl_journal_attribution import REVIEW_PREFIX


def is_source_review(prompt):
    return prompt.startswith(REVIEW_PREFIX)


def review_inputs(prompt):
    if not is_source_review(prompt):
        raise AssertionError("Expected the source-only Journal editor prompt")
    decoder = json.JSONDecoder()
    units, _ = decoder.raw_decode(prompt.split("CANDIDATE_UNITS_JSON: ", 1)[1])
    evidence, _ = decoder.raw_decode(prompt.split("ORIGINAL_EVIDENCE_JSON: ", 1)[1])
    # Rehydrate the compact prompt catalog for fixture authors. The provider
    # sees each source-owned context once and only chooses the stable fragment ID.
    fragments = []
    for item in evidence["fragments"]:
        context = evidence["contexts"][item["contextRef"]]
        identity = {key: context[key] for key in ("refId", "speaker", "authority", "publicSpeakerName", "evidenceKind") if key in context}
        metadata = {key: value for key, value in context.items() if key not in identity}
        fragments.append({key: value for key, value in item.items() if key != "contextRef"}
                         | identity | {"context": metadata})
    evidence["fragments"] = fragments
    return units, evidence


def fixture_issue(unit_ids, fragments, *, source_meaning, added_premise,
                  repair="Preserve the expression but correct the unsupported premise."):
    """Explicit negative semantic judgment supplied by a test, never an oracle."""
    return {"unitIds": list(unit_ids), "fragmentIds": [item["fragmentId"] for item in fragments],
            "sourceMeaning": source_meaning, "addedPremise": added_premise, "repair": repair}


def supported_review(prompt):
    """Supply a test-chosen judgment covering the complete immutable candidate."""
    units, _evidence = review_inputs(prompt)
    return json.dumps({"reviewedUnitIds": [unit["unitId"] for unit in units],
                       "issues": [], "verdict": "supported"})


def rejected_review(prompt, *, issue="The candidate reverses the original attribution."):
    response = json.loads(supported_review(prompt))
    units, evidence = review_inputs(prompt)
    target = next(unit for unit in units if ".body:" in unit["unitId"])
    fragment = next(item for item in evidence["fragments"] if item["field"] == "summary")
    response.update(verdict="unsupported", issues=[fixture_issue(
        [target["unitId"]], [fragment], source_meaning=fragment["text"], added_premise=issue)])
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
