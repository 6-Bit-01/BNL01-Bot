"""Explicit mocked editor responses for Journal flow tests, not an entailment oracle.

These helpers bind the supplied unit IDs and exact source anchors. Their chosen
verdict is fixture input; a passing flow test does not prove semantic accuracy.
Production code must never import this module.
"""
from functools import wraps
import json

from bnl_journal_attribution import ASSESSMENT_CHECKS, REVIEW_PREFIX


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
        identity = {key: context[key] for key in ("refId", "speaker", "authority", "publicSpeakerName") if key in context}
        metadata = {key: value for key, value in context.items() if key not in identity}
        fragments.append({key: value for key, value in item.items() if key != "contextRef"}
                         | identity | {"context": metadata})
    evidence["fragments"] = fragments
    return units, evidence


def fixture_claim(text, fragment, *, source_meaning=None, **changes):
    """Explicit protocol fixture, never a semantic judgment about test prose."""
    claim = {
        "claim": text, "claimType": "reported_speech", "sourceStance": "assertion",
        "evidence": [{"fragmentId": fragment["fragmentId"],
                      "use": "speech" if fragment["authority"] in {"original", "speech_only"} else "context"}],
        "sourceMeaning": source_meaning or fragment["text"], "support": "entails",
        "assumptions": [], "evidenceScope": "recorded_content",
    }
    claim.update(changes)
    return claim


def supported_review(prompt):
    """Supply a test-approved verdict with structurally genuine source anchors."""
    units, evidence = review_inputs(prompt)
    fragment = next((item for item in evidence["fragments"] if item["field"] == "summary"), None)
    if fragment is None:
        raise AssertionError("Mock review needs an actual supplied original source")
    return json.dumps({"assessments": [
        {"check": check, "unitIds": [unit["unitId"] for unit in units],
         "sourceRefIds": [fragment["refId"]],
         "explanation": "This controlled fixture supplies a passing " + check + " verdict; not model-quality evidence.",
         "issues": [], "verdict": "supported"} for check in ASSESSMENT_CHECKS
    ], "units": [
        {"unitId": unit["unitId"], "claims": [fixture_claim(unit["text"], fragment)],
         "nonFactualReason": ""} for unit in units
    ], "verdict": "supported"})


def rejected_review(prompt, *, issue="The candidate reverses the original attribution."):
    response = json.loads(supported_review(prompt))
    response["verdict"] = "unsupported"
    target = next(unit for unit in response["units"] if ".body:" in unit["unitId"])
    target["claims"][0].update(evidence=[], support="unknown", assumptions=[issue])
    return json.dumps(response)


def supported_review_with_anchor(prompt, *, unit_id, source_ref):
    """Choose one exact evidence dependency, not a semantic support judgment."""
    response = json.loads(supported_review(prompt))
    _, evidence = review_inputs(prompt)
    fragment = next(item for item in evidence["fragments"]
                    if item["refId"] == source_ref and item["field"] == "summary")
    unit = next(unit for unit in response["units"] if unit["unitId"] == unit_id)
    unit["claims"][0] = fixture_claim(unit["claims"][0]["claim"], fragment)
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
