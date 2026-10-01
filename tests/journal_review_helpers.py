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
    return units, evidence


def supported_review(prompt):
    """Supply a test-approved verdict with structurally genuine source anchors."""
    units, evidence = review_inputs(prompt)
    sources = evidence["sources"]
    anchor = None
    for source in sources:
        for contribution in source.get("contributions") or [source]:
            if contribution.get("summary"):
                anchor = {"refId": source["refId"], "quote": contribution["summary"],
                          "speaker": str(contribution.get("participantAlias") or ""), "use": "speech"}
                break
        if anchor:
            break
    if not anchor:
        raise AssertionError("Mock review needs an actual supplied original source")
    return json.dumps({"verdict": "supported", "units": [
        {"unitId": unit["unitId"], "kind": "factual", "verdict": "supported",
         "evidence": [anchor], "issues": []} for unit in units
    ]})


def rejected_review(prompt, *, issue="The candidate reverses the original attribution."):
    response = json.loads(supported_review(prompt))
    response["verdict"] = "unsupported"
    target = next(unit for unit in response["units"] if ".body:" in unit["unitId"])
    target.update(verdict="unsupported", evidence=[], issues=[issue])
    return json.dumps(response)


def with_supported_review(writer):
    """Keep an existing mock writer while explicitly mocking source review."""
    @wraps(writer)
    def generate(packet, prompt):
        return supported_review(prompt) if is_source_review(prompt) else writer(packet, prompt)
    return generate
