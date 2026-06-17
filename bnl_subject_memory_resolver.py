"""Subject memory evidence resolver for BNL dossier/source-file drafting.

The resolver is read-only and conservative: it scans known memory/evidence tables
when they exist, classifies candidate subject memories by public-readiness, and
only returns sanitized text in public-safe sections.
"""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from typing import Any

LOG = logging.getLogger(__name__)

_PAYMENT_RE = re.compile(r"\b(stripe|checkout|payment|paid|purchase|purchased|customer|priority\s*signal|priority)\b", re.I)
_INTERNAL_RE = re.compile(r"\b(admin[-\s]*only|internal|private\s+alias|raw\s*id|discord\s*id|user\s*id|rowid|debug|diagnostic|database|table|source\s*row|memory_tiers|redis|dm|direct\s+message)\b", re.I)
_SOURCE_BLIND_RE = re.compile(r"\b(source[-_\s]*blind|unknown[-_\s]*policy|no\s+provenance|unverified\s+source)\b", re.I)
_REVIEW_RE = re.compile(r"\b(review[-\s]*only|needs?\s+confirmation|unconfirmed|uncertain|inferred|ambiguous|maybe|appears\s+to|queue|submission|relationship)\b", re.I)
_PUBLIC_RE = re.compile(r"\b(public[-_\s]*safe|dossier[-_\s]*safe|public\s+context|public\s+home|public\s+discord|owner[-_\s]*confirmed|official\s+public|broadcast\s+memory)\b", re.I)
_LINK_RE = re.compile(r"https?://\S+", re.I)
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_PHONE_RE = re.compile(r"(?<!\w)(?:\+?1[\s.-]?)?(?:\(?\d{3}\)?[\s.-]?)\d{3}[\s.-]?\d{4}(?!\w)")
_RAW_ID_RE = re.compile(r"\b\d{15,22}\b|\b(?:discord|user|channel|message|guild|member)[-_\s]*(?:id)?\s*[:=#]?\s*\d+\b", re.I)
_SECRET_RE = re.compile(r"\b(?:api[_-]?key|token|secret|authorization|bearer|password)\b\s*[:=]?\s*[A-Za-z0-9_./+\-=]{8,}|\b[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{20,}\b", re.I)
_CONTACT_RE = re.compile(r"\b(?:email|e-mail|phone|telephone|contact me|contact info|address|sms|text me)\b", re.I)
_PRIVATE_ALIAS_RE = re.compile(r"\b(?:private alias|alias privately|known privately as|legal name|real name)\b", re.I)
_DB_ROW_RE = re.compile(r"\b(?:rowid|source row|database row|raw json|raw payload|table row|memory_tiers|entity_evidence_events)\b", re.I)


_CONTEST_HOST_RE = re.compile(r"\b(?:hosts? the contest|organizes? the contest|runs? the contest|created the contest|contest organizer|owner of the contest|official host)\b", re.I)
_CONTEST_JUDGE_RE = re.compile(r"\b(?:judg(?:e|es|ing) the contest|contest judge|judge for the contest)\b", re.I)
_CONTEST_SUBMIT_RE = re.compile(r"\b(?:submitted? (?:to|an? entry|for)|contest entr(?:y|ant)|submission link|submitter)\b", re.I)
_CONTEST_RULES_RE = re.compile(r"\b(?:rules? (?:posted|mention(?:ed)?|for)|posted rules|contest rules|rules)\b", re.I)
_CONTEST_DATE_RE = re.compile(r"\b(?:date|dates|deadline|starts?|ends?|schedule)\b", re.I)
_CONTEST_CHANNEL_RE = re.compile(r"\b(?:channel|where contest happened)\b", re.I)
_CONTEST_LINK_RE = re.compile(r"\b(?:public link|submission link|link|url|https?://)\b", re.I)
_CONTEST_MUSIC_RE = re.compile(r"\b(?:suno|music|song|track|artist)\b", re.I)
_CONTEST_REJECTION_RE = re.compile(r"\b(?:not a contest organizer|is not a contest organizer|reject(?:ed)? contest organizer|contest organizer rejected|rejected weak label ['\"]?contest organizer|do not infer contest organizer)\b", re.I)

def humanize_internal_label(value: str) -> str:
    text = _text(value, 300)
    replacements = {"official_public_dossier": "official public dossier", "public_safe": "public-safe", "source_blind": "source-blind", "review_only": "review-only", "public_music_role": "public music role"}
    for raw, friendly in replacements.items():
        text = re.sub(re.escape(raw), friendly, text, flags=re.I)
    return re.sub(r"\b([a-z]+(?:_[a-z]+)+)\b", lambda m: m.group(1).replace("_", " "), text)

def classify_contest_event_context(text: str) -> tuple[str, dict[str, bool]]:
    value = _text(text, 1000)
    has_contest = bool(re.search(r"\b(contest|challenge|tournament|battle|competition)\b", value, re.I))
    hints = {
        "contestName": has_contest and bool(re.search(r"\b(?:contest name|suno contest|[A-Z][A-Za-z0-9 ]+ contest)\b", value)),
        "rulesMentioned": bool(_CONTEST_RULES_RE.search(value)),
        "channelMentioned": has_contest and bool(_CONTEST_CHANNEL_RE.search(value)),
        "dateMentioned": has_contest and bool(_CONTEST_DATE_RE.search(value)),
        "publicLinkMentioned": has_contest and bool(_CONTEST_LINK_RE.search(value)),
        "submissionMentioned": has_contest and bool(_CONTEST_SUBMIT_RE.search(value)),
        "participantMentioned": has_contest and bool(re.search(r"\b(?:participant|entered|entry|submitted|submitter)\b", value, re.I)),
        "hostMentioned": bool(_CONTEST_HOST_RE.search(value)),
        "organizerMentioned": bool(re.search(r"\b(?:organizes? the contest|contest organizer|owner of the contest|created the contest|runs? the contest)\b", value, re.I)),
        "judgeMentioned": bool(_CONTEST_JUDGE_RE.search(value)),
        "musicContextMentioned": has_contest and bool(_CONTEST_MUSIC_RE.search(value)),
        "publicSourceMissing": bool(re.search(r"\b(?:source[-_ ]blind|official_public_dossier source not connected|source not connected|no provenance)\b", value, re.I)),
        "loreOrAnomalyContext": bool(re.search(r"\b(?:lore|anomaly|orion|relay)\b", value, re.I)),
    }
    if not has_contest: return "contest_unknown", hints
    if hints["hostMentioned"]: return "contest_host", hints
    if hints["organizerMentioned"]: return "contest_organizer", hints
    if hints["judgeMentioned"]: return "contest_judge", hints
    if hints["submissionMentioned"]: return "contest_submitter", hints
    if hints["participantMentioned"]: return "contest_participant", hints
    if hints["musicContextMentioned"]: return "contest_music_context", hints
    if hints["rulesMentioned"]: return "contest_rules_poster", hints
    if hints["publicLinkMentioned"]: return "contest_public_link_context", hints
    if hints["dateMentioned"]: return "contest_date_context", hints
    if hints["channelMentioned"]: return "contest_channel_context", hints
    return "contest_mention_only", hints

_BROAD_SCOPE_DEFAULT = {
    "hasSignal": False, "scopeKnown": False, "scopeLabel": "", "scopeType": "",
    "subjectRoleHint": "unknown", "objectName": "", "host": "", "platform": "",
    "targetAudience": "", "confidence": "low", "shouldEmitReviewCard": False,
    "shouldEmitReadinessQuestion": False, "reason": "",
}

def _named_actor_before_verb(text: str, verbs: str) -> str:
    m = re.search(rf"\b([A-Z][A-Za-z0-9_-]{{2,40}})\s+(?:is\s+|was\s+)?(?:the\s+)?(?:{verbs})\b", text)
    return m.group(1) if m else ""

def _contest_scope_for_text(subject: str, text: str) -> dict[str, Any]:
    value = _text(text, 1000)
    low = value.lower()
    scope = dict(_BROAD_SCOPE_DEFAULT)
    has_signal = bool(re.search(r"\b(contest|challenge|tournament|battle|competition)\b", value, re.I))
    scope.update({"hasSignal": has_signal, "scopeType": "unknown_contest_context"})
    if not has_signal:
        return scope
    obj = ""
    m = re.search(r"\b(?:the\s+)?([A-Z][A-Za-z0-9' -]{2,70}?\s+(?:contest|challenge|tournament|battle|competition))\b", value)
    if m and not m.group(1).lower().startswith(subject.lower() + " "):
        obj = _text(m.group(1), 90)
    host = _named_actor_before_verb(value, r"host(?:ed|s|ing)?|organize(?:d|s|r|rs|ing)?|run(?:s|ning)?|ran|created")
    if not host:
        hm = re.search(r"\b([A-Z][A-Za-z0-9_-]{2,40})[- ]hosted\s+(?:contest|challenge|tournament|battle|competition)\b", value)
        if hm:
            host = hm.group(1)
    if not host:
        hm = re.search(r"\binvolving\s+([A-Z][A-Za-z0-9_-]{2,40})[- ]hosted\s+(?:contest|challenge|tournament|battle|competition)\b", value)
        if hm:
            host = hm.group(1)
    if re.search(rf"\b{re.escape(subject)}\s+(?:host(?:ed|s|ing)?|organize(?:d|s|ing)?|runs?|ran|created)\b.*\b(contest|challenge|tournament|battle|competition)\b", value, re.I):
        scope["subjectRoleHint"] = "host_or_organizer"
        if not host:
            host = subject
    elif re.search(rf"\b{re.escape(subject)}\b.*\b(submitted|submitter|entry|entered|participant)\b|\b(submitted|entered)\b.*\b{re.escape(subject)}\b", value, re.I):
        scope["subjectRoleHint"] = "submitter_or_participant"
    elif re.search(rf"\b{re.escape(subject)}\b.*\b(posted|shared|linked|reposted)\b.*\b(rules|link|url)\b", value, re.I):
        scope["subjectRoleHint"] = "rules_or_link_source"
    elif re.search(rf"\b{re.escape(subject)}\b", value, re.I):
        scope["subjectRoleHint"] = "mentioned_near_context"
    platform = ""
    pm = re.search(r"\b(?:on|in|via|at)\s+([A-Z][A-Za-z0-9 _-]{2,40}|Discord|Suno|YouTube|Twitch|Null TV|BARCODE Radio)\b", value)
    if pm:
        platform = _text(pm.group(1), 60)
    role_clue = scope["subjectRoleHint"] != "unknown" and scope["subjectRoleHint"] != "mentioned_near_context"
    known = bool(obj or host or platform or role_clue or re.search(r"\b(contest name|submission|entry|deadline|dates?|channel|public link)\b", value, re.I))
    scope.update({
        "scopeKnown": known,
        "scopeLabel": (f"{host}-hosted contest" if host else (obj or (f"{platform} contest context" if platform else "contest/rules context"))),
        "objectName": obj,
        "host": host,
        "platform": platform,
        "confidence": "high" if (obj and role_clue) or (host and role_clue) else ("medium" if known else "low"),
        "shouldEmitReviewCard": bool(known),
        "shouldEmitReadinessQuestion": True,
        "reason": ("Contest object identified — subject appears as " + scope["subjectRoleHint"]) if (known and role_clue) else ("Contest context identified — subject relationship unclear" if known else "Unscoped contest wording — no host, event, platform, source, or role identified"),
    })
    if host:
        scope["scopeType"] = "contest_with_evidence_host"
    elif obj:
        scope["scopeType"] = "named_contest"
    elif platform:
        scope["scopeType"] = "platform_contest_context"
    return scope

def _rules_instructions_scope_for_text(subject: str, text: str) -> dict[str, Any]:
    value = _text(text, 1000)
    low = value.lower()
    scope = dict(_BROAD_SCOPE_DEFAULT)
    has_signal = bool(re.search(r"\b(rules?|instructions?|guidelines?)\b", value, re.I))
    if not has_signal:
        return scope
    if "contest" in low or "challenge" in low:
        st = "contest_rules"
    elif "queue" in low or "submission" in low:
        st = "queue_submission_instructions"
    elif "discord" in low or "admin" in low or "mod" in low:
        st = "discord_admin_instructions"
    elif "broadcast" in low or "show" in low or "radio" in low:
        st = "broadcast_show_instructions"
    elif "site" in low or "workflow" in low or "source file" in low:
        st = "site_workflow_instructions"
    elif "prompt" in low or "system instruction" in low:
        st = "prompt_or_system_instructions"
    else:
        st = "unknown_instructions"
    target = ""
    tm = re.search(r"\b(?:for|to)\s+([A-Z][A-Za-z0-9 _-]{2,50}|artists|submitters|admins|mods|viewers|participants)\b", value)
    if tm:
        target = _text(tm.group(1), 70)
    scope.update({"hasSignal": True, "scopeKnown": st != "unknown_instructions", "scopeType": st, "scopeLabel": st.replace("_", " "), "targetAudience": target, "confidence": "medium" if st != "unknown_instructions" else "low", "shouldEmitReviewCard": True, "shouldEmitReadinessQuestion": True, "reason": "Scoped rules/instructions context identified." if st != "unknown_instructions" else "Unclear rules/instructions wording needs classification before any role claim."})
    return scope

def _lore_scope_for_text(subject: str, text: str) -> dict[str, Any]:
    value = _text(text, 1000); low = value.lower(); scope = dict(_BROAD_SCOPE_DEFAULT)
    if not re.search(r"\b(lore|canon|context|recurring (?:bit|topic))\b", value, re.I):
        return scope
    pairs = [("orion_lore", "Orion-related lore", "orion"), ("null_tv_lore", "Null TV lore", "null tv"), ("barcode_network_lore", "BARCODE Network lore", "barcode"), ("six_bit_bnl_lore", "Six Bit/BNL lore", "bnl"), ("discord_community_lore", "Discord community lore", "discord"), ("broadcast_radio_lore", "broadcast/radio lore", "broadcast radio show"), ("recurring_bit_or_topic", "recurring stream topic", "recurring bit topic")]
    st, label = "unknown_lore_context", "unknown lore/context"
    for a, b, keys in pairs:
        if any(k in low for k in keys.split()):
            st, label = a, b; break
    else:
        if re.search(rf"\b{re.escape(subject)}(?:'s|’s)?\s+lore\b|\blore\b.*\b{re.escape(subject)}\b", value, re.I):
            st, label = "subject_lore", f"{subject}'s lore"
    scope.update({"hasSignal": True, "scopeKnown": st != "unknown_lore_context", "scopeType": st, "scopeLabel": label, "confidence": "medium" if st != "unknown_lore_context" else "low", "shouldEmitReviewCard": True, "shouldEmitReadinessQuestion": True, "reason": "Lore/context scope identified." if st != "unknown_lore_context" else "Lore/context wording is unscoped and needs classification."})
    return scope

def _theory_anomaly_scope_for_text(subject: str, text: str) -> dict[str, Any]:
    value = _text(text, 1000); low = value.lower(); scope = dict(_BROAD_SCOPE_DEFAULT)
    if not re.search(r"\b(theory|theories|anomal(?:y|ies)|glitch|technical issue)\b", value, re.I):
        return scope
    if re.search(rf"\b{re.escape(subject)}\b.*\b(theory|anomal)", value, re.I):
        st, label = "subject_theory", f"{subject}-related theory/anomaly"
    elif "orion" in low:
        st, label = "orion_anomaly", "Orion anomaly"
    elif "barcode" in low:
        st, label = "barcode_theory", "BARCODE theory"
    elif "bnl" in low or "system" in low:
        st, label = "bnl_system_anomaly", "BNL/system anomaly"
    elif "broadcast" in low or "radio" in low:
        st, label = "broadcast_anomaly", "broadcast anomaly"
    elif "recurring" in low:
        st, label = "recurring_topic", "recurring topic"
    elif "technical" in low or "glitch" in low:
        st, label = "technical_issue", "technical issue"
    else:
        st, label = "unknown_theory_anomaly", "unknown theory/anomaly"
    scope.update({"hasSignal": True, "scopeKnown": st != "unknown_theory_anomaly", "scopeType": st, "scopeLabel": label, "confidence": "medium" if st != "unknown_theory_anomaly" else "low", "shouldEmitReviewCard": True, "shouldEmitReadinessQuestion": True, "reason": "Theory/anomaly scope identified." if st != "unknown_theory_anomaly" else "Theory/anomaly wording is unscoped and needs classification."})
    return scope

def _ai_persona_project_scope_for_text(subject: str, text: str) -> dict[str, Any]:
    value = _text(text, 1000)
    scope = dict(_BROAD_SCOPE_DEFAULT)
    if not re.search(r"\b(ai|persona|project|bot|model|character)\b", value, re.I):
        return scope
    label = ""
    m = re.search(r"\b(?:AI|persona|project|bot|model|character)\s+(?:named|called|with)\s+([A-Z][A-Za-z0-9 _-]{2,60})\b", value)
    if m:
        label = _text(m.group(1), 80)
    scope.update({
        "hasSignal": True,
        "scopeKnown": bool(label),
        "scopeType": "named_ai_persona_project" if label else "unknown_ai_persona_project",
        "scopeLabel": label or "AI/persona/project interaction",
        "objectName": label,
        "confidence": "medium" if label else "low",
        "shouldEmitReviewCard": True,
        "shouldEmitReadinessQuestion": True,
        "reason": "AI/persona/project object identified." if label else "AI/persona/project wording needs interaction scope before any public claim.",
    })
    return scope

GENERIC_FOLLOW_UP_FALLBACK = "What proof or approved wording should BNL use before saying this publicly?"

_FOLLOW_UP_CATEGORY_PATTERNS: list[tuple[str, str]] = [
    ("source_blind", r"\b(source[-_\s]*blind|private|withheld|internal[-\s]*only|no provenance|unknown[-_\s]*policy)\b"),
    ("orion_context", r"\b(orion|relay)\b"),
    ("collaboration_interest", r"\b(collaboration interest|interested in collaborating|wants? to collab|wants? to collaborate|could collab|can collab|work together)\b"),
    ("ai_persona_project", r"\b(ai|persona|project|character|bot|agent|universe|crossover|interaction|model)\b"),
    ("theory_anomaly", r"\b(theory|theories|anomal(?:y|ies)|glitch|technical issue)\b"),
    ("lore_context", r"\b(lore|canon|recurring (?:bit|topic)|barcode|null tv)\b"),
    ("public_boundary", r"\b(boundar(?:y|ies)|do[-\s]*not[-\s]*say|avoid saying|should not mention|keep private|public boundary)\b"),
    ("identity_name", r"\b(identity|display name|public name|alias|aka|known as|legal name|real name)\b"),
    ("links_music_socials", r"\b(link|links|url|social|suno|music|song|track|artist page|spotify|youtube|soundcloud|bandcamp)\b"),
    ("queue_submission", r"\b(queue|submission|submitted|submitter|entry)\b"),
    ("contest_event", r"\b(contest|challenge|tournament|battle|competition|event)\b"),
    ("rules_instructions", r"\b(rules?|instructions?|guidelines?)\b"),
    ("role_title", r"\b(role|title|artist|producer|moderator|mod|host|organizer|participant|member|staff|core team)\b"),
    ("relationship_affiliation", r"\b(relationship|affiliation|connected to|connection|associated with|collaborat(?:e|ion|or)|partner|member of|team)\b"),
    ("public_summary", r"\b(summary|bio|dossier|public copy|public wording|public-ready|public ready)\b"),
]

def _scope_label_from_text(subject: str, text: str, category: str, scope: dict[str, Any] | None = None) -> tuple[str, bool]:
    if scope and _text(scope.get("scopeLabel"), 100):
        return _text(scope.get("scopeLabel"), 100), bool(scope.get("scopeKnown"))
    value = _text(text, 500)
    if category == "ai_persona_project":
        s = _ai_persona_project_scope_for_text(subject, value)
        return _text(s.get("scopeLabel"), 100) or "AI/persona/project interaction", bool(s.get("scopeKnown"))
    if category == "contest_event":
        s = _contest_scope_for_text(subject, value)
        return _text(s.get("scopeLabel"), 100) or "contest/event context", bool(s.get("scopeKnown"))
    if category == "rules_instructions":
        s = _rules_instructions_scope_for_text(subject, value)
        return _text(s.get("scopeLabel"), 100) or "rules/instructions context", bool(s.get("scopeKnown"))
    if category == "lore_context":
        s = _lore_scope_for_text(subject, value)
        return _text(s.get("scopeLabel"), 100) or "lore/context", bool(s.get("scopeKnown"))
    if category == "theory_anomaly":
        s = _theory_anomaly_scope_for_text(subject, value)
        return _text(s.get("scopeLabel"), 100) or "theory/anomaly", bool(s.get("scopeKnown"))
    m = re.search(r"\b(?:about|for|with|to|mention(?:ing)?)\s+([A-Z][A-Za-z0-9' _/-]{2,70})", value)
    if m:
        return _text(m.group(1), 90), True
    labels = {
        "role_title": "role/title", "identity_name": "identity/name", "links_music_socials": "links/music/socials",
        "relationship_affiliation": "relationship/affiliation", "collaboration_interest": "collaboration interest", "public_boundary": "public boundary", "queue_submission": "queue/submission",
        "orion_context": "Orion/context", "public_summary": "public summary", "source_blind": "source-blind/private context",
    }
    return labels.get(category, ""), category in labels

def _infer_follow_up_category(subject: str, claim_text: str, claim_type: str, lane: str, actionability: str) -> str:
    low = f"{claim_type} {lane} {actionability} {claim_text}".lower()
    if actionability == "source_blind_warning" or lane == "source_blind":
        return "source_blind"
    type_map = {
        "music_link": "links_music_socials", "public_link": "links_music_socials", "queue_submission": "queue_submission",
        "identity": "identity_name", "role": "role_title",
        "admin_task": "admin_action", "source_blind_context": "source_blind",
    }
    if claim_type in type_map:
        return type_map[claim_type]
    if claim_type.startswith("contest_"):
        return "contest_event"
    if claim_type == "rules_instructions_context":
        return "rules_instructions"
    for category, pattern in _FOLLOW_UP_CATEGORY_PATTERNS:
        if re.search(pattern, low, re.I):
            return category
    if claim_type == "collaboration_interest":
        return "collaboration_interest"
    if claim_type == "relationship":
        return "relationship_affiliation"
    if claim_type == "unknown" and re.search(r"\b(unclear|ambiguous|something|item)\b", low, re.I):
        return "unknown_context"
    return "fallback"

def _universal_follow_up_for_claim(subject: str, claim_text: str, claim_type: str, lane: str, actionability: str, scope: dict[str, Any] | None = None, display_copy: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return the canonical BNL-authored follow-up question metadata for any actionable review card."""
    category = _infer_follow_up_category(subject, claim_text, claim_type, lane, actionability)
    scope_label, scope_known = _scope_label_from_text(subject, claim_text, category, scope)
    fallback = False
    not_enough = ""
    audience = "admin"
    confidence = "medium" if scope_known else "low"
    answer_type = "public/internal/ignore decision plus exact approved wording where public use is allowed"
    reason = "BNL needs a scoped human decision before this can shape public dossier wording; do not infer facts from proximity, labels, or private/source-blind evidence."

    if category == "source_blind":
        question = "Can an owner/admin provide public-safe replacement wording for this context, or should BNL keep it internal?"
        audience = "public_source"; answer_type = "public-safe replacement wording or confirmation to keep internal"; confidence = "low"; scope_known = False; scope_label = "source-blind/private context"
        reason = "BNL cannot expose source-blind/private evidence or approve it directly for public copy; it needs separate public-safe replacement wording or an internal-only decision."
    elif category == "ai_persona_project":
        s = scope or _ai_persona_project_scope_for_text(subject, claim_text)
        obj = _text(s.get("objectName") or (scope_label if scope_known and scope_label != "AI/persona/project interaction" else ""), 90)
        if obj:
            question = f"Can BNL mention the subject’s connection to {obj} publicly? If yes, what exact wording should it use?"
            scope_label = obj; scope_known = True; confidence = "medium"
        else:
            question = "What AI/persona/project connection is this referring to? What AI, persona, project, character, bot, or universe is this referring to, and what was the subject’s actual connection to it? Should BNL mention it publicly, keep it internal, or ignore it?"
        answer_type = "clarification of the AI/persona/project object, interaction, and subject role" if not obj else "plain-language explanation of the interaction, AI/persona/project object, public/internal/ignore decision, and exact approved wording if public"
        reason = "BNL found AI/persona/project wording but must not infer the object, interaction, or public permission from nearby context."
    elif category == "role_title":
        question = f"What exact public role or title may BNL use for {subject}, if any? Should BNL avoid giving {subject} a public role/title for now?"
        audience = "subject"; answer_type = "exact public role/title or confirmation not to state one"; scope_label = "role/title"; scope_known = True
        reason = "BNL needs exact approved role/title wording and must not infer a role from weak labels or context."
    elif category == "collaboration_interest":
        question = f"Is this only collaboration interest, or is there an actual public collaboration BNL should reference? If public, what project/song/context should it reference?"
        audience = "admin"; answer_type = "collaboration status, project/context, and whether it can be public"; scope_label = "collaboration interest"; scope_known = True
        reason = "BNL found possible collaboration interest, but interest is not the same as an approved public collaborator role."
    elif category == "relationship_affiliation":
        question = f"What public relationship, affiliation, or connection may BNL state for {subject}, if any?"
        audience = "subject"; answer_type = "approved relationship/affiliation wording or confirmation none should be stated"; scope_label = scope_label or "relationship/affiliation"; scope_known = True
        reason = "BNL found possible relationship context but must not state a connection until the approved public relationship is confirmed."
    elif category == "links_music_socials":
        question = f"Which {subject} links are owned or approved for public reference? Which {subject} links, music links, or social links are owned by {subject} and approved for BNL to mention publicly? Which links, music links, social links, or artist pages are owned by {subject} and approved for BNL to mention publicly? Should BNL avoid public links for {subject} for now?"
        audience = "subject"; answer_type = "owned approved links or confirmation to avoid public links"; scope_label = "links/music/socials"; scope_known = True
        reason = "BNL needs ownership and public-use approval before associating links, music, socials, or artist pages with the subject."
    elif category == "identity_name":
        question = "What exact public display name should BNL use for this subject? Which aliases, if any, are approved for public use, and which should stay internal?"
        audience = "subject"; answer_type = "public display name and alias boundary"; scope_label = "identity/name"; scope_known = True
        reason = "BNL needs an approved public name and alias boundary; it must not expose private aliases or infer identity wording."
    elif category == "public_boundary":
        question = f"What should BNL avoid saying publicly about {subject}?" if not scope_label or scope_label == "public boundary" else f"Should BNL avoid mentioning {scope_label} in {subject}’s public dossier?"
        audience = "subject"; answer_type = "public boundary or do-not-say instruction"; scope_label = scope_label or "public boundary"; scope_known = True
        reason = "BNL needs explicit public boundaries so it does not overstate private, internal, unwanted, or source-blind context."
    elif category == "queue_submission":
        question = f"Can {subject}'s queue/submission history be referenced publicly, or should it stay internal? Should BNL mention {subject}'s queue or submission context publicly, keep it internal, or ignore it? If public, what exact wording is approved?"
        answer_type = "queue/submission public/internal/ignore decision and approved wording"; scope_label = "queue/submission"; scope_known = True
        reason = "BNL must not turn queue/submission context into public dossier wording without admin/owner approval."
    elif category == "contest_event":
        question = f"What contest or event is this referring to? BNL found {scope_label or 'contest/event'} context near {subject}. Was {subject} a participant, submitter, link source, rules source, organizer, host, or just mentioned nearby? Should BNL mention it publicly, keep it internal, or ignore it?"
        answer_type = "clarification of the contest/event object, subject role, and whether it belongs in the dossier" if not scope_known else "contest/event name plus subject role and public/internal/ignore decision"
        reason = "BNL cannot infer contest/event role, host, organizer, or participation from proximity."
    elif category == "rules_instructions":
        question = f"What rules or instructions is this referring to, who were they for, and what was {subject}'s actual role with them? Should BNL mention this publicly, keep it internal, or ignore it?"
        answer_type = "clarification of the instruction source, audience, purpose, and subject role" if not scope_known else "what the instructions were, who they were for, subject role, and public/internal/ignore decision"
        reason = "BNL needs the instruction source, audience, purpose, and subject role before treating rules/instructions as dossier context."
    elif category == "theory_anomaly":
        question = f"What theory or anomaly is this referring to, and is it about {subject}, BARCODE lore, Orion, a recurring topic, a technical issue, or something unrelated? Should BNL use it publicly, keep it internal, or ignore it?"
        answer_type = "clarification of the theory/anomaly scope and whether it belongs in the dossier" if not scope_known else "scope of the theory/anomaly plus public/internal/ignore decision"
        reason = "BNL needs the theory/anomaly scope before deciding whether it belongs in a public dossier."
    elif category == "orion_context":
        question = f"Can BNL mention Orion in public {subject} context, or should Orion stay internal? If public, what exact wording is approved?"
        answer_type = "Orion/context public/internal decision and approved wording"; scope_label = "Orion/context"; scope_known = True
        reason = "BNL needs an explicit public/internal boundary for Orion/context references."
    elif category == "lore_context":
        question = f"Is this {subject}'s own lore, BARCODE Network lore, Orion lore, Null TV lore, broadcast lore, recurring community context, or something else? Should BNL mention {subject}'s lore publicly, keep it internal, or ignore it?"
        answer_type = "clarification of the lore/context scope and whether it belongs in the dossier" if not scope_known else "classification of the lore/context plus public/internal/ignore decision"
        reason = "BNL needs to classify lore/context before it can decide whether it belongs in the subject's public dossier."
    elif category == "public_summary":
        question = f"What short public summary is approved for {subject}, and what should BNL avoid inferring beyond that wording?"
        audience = "subject"; answer_type = "approved public summary plus do-not-infer boundary"; scope_label = "public summary"; scope_known = True
        reason = "BNL needs owner/admin-approved summary direction and must not infer unsupported public copy."
    elif category == "unknown_context":
        question = "What is this item referring to, and should BNL use it publicly, keep it internal, or ignore it?"
        answer_type = "clarification of what this item refers to and whether it belongs in the dossier"
        reason = "BNL can tell the item is unclear context, so it needs classification before asking for proof or public wording."
        scope_label = "unclear context"; scope_known = False
    elif category == "admin_action":
        question = f"Should BNL mention {scope_label or 'this context'} in {subject}'s public dossier, keep it internal, or ignore it?"
        answer_type = "admin public/internal/ignore decision and approved wording if public"
        reason = "Actionable admin follow-up must resolve the same dossier question rather than become a vague workflow reminder."
    else:
        fallback = True
        not_enough = "BNL could not identify whether this is a role, link, lore, contest, relationship, queue, identity, AI/persona/project, source-blind, or public-boundary issue, so it needs proof or approved wording before using it publicly."
        question = GENERIC_FOLLOW_UP_FALLBACK
        answer_type = "clarification of what this item refers to and whether it belongs in the dossier"
        reason = not_enough
        scope_label = ""; scope_known = False; confidence = "low"
    return {
        "question": question, "audience": audience, "reason": reason, "answerType": answer_type,
        "confidence": confidence, "scopeKnown": bool(scope_known), "scopeLabel": scope_label,
        "category": category, "fallbackUsed": fallback, "notEnoughContextReason": not_enough,
    }


def _is_clarification_follow_up(follow_up: dict[str, Any] | None, actionability: str = "") -> bool:
    if not follow_up:
        return False
    if follow_up.get("category") == "source_blind" or actionability == "source_blind_warning":
        return False
    question = str(follow_up.get("question") or "").lower()
    answer_type = str(follow_up.get("answerType") or "").lower()
    asks_scope = bool(re.search(r"\b(what|which|who|is this about|referring to|belongs? in|actual role|what .* role|scope)\b", question, re.I))
    return (not bool(follow_up.get("scopeKnown"))) and ("clarification" in answer_type or asks_scope or bool(follow_up.get("fallbackUsed")))

def _clarification_copy(subject: str, follow_up: dict[str, Any], fallback_question: str = "") -> dict[str, Any]:
    question = str(follow_up.get("question") or fallback_question or "What is this item referring to?")
    category = str(follow_up.get("category") or "")
    reason = str(follow_up.get("reason") or follow_up.get("notEnoughContextReason") or "BNL cannot decide whether this belongs in the dossier until the missing context is provided.")
    answer_type = str(follow_up.get("answerType") or "clarification of the item scope and whether it belongs in the dossier")
    if "clarification" not in answer_type.lower():
        if category == "rules_instructions":
            answer_type = "clarification of the instruction source, audience, purpose, and subject role"
            reason = "BNL cannot decide whether this belongs in the dossier until it knows what instructions are being referenced."
        elif category == "theory_anomaly":
            answer_type = "clarification of the theory/anomaly scope and whether it belongs in the dossier"
            reason = "BNL cannot decide whether this is public, internal, or irrelevant until the theory/anomaly is identified."
        elif category == "lore_context":
            answer_type = "clarification of the lore/context scope and whether it belongs in the dossier"
        elif category == "ai_persona_project":
            answer_type = "clarification of the AI/persona/project object, interaction, and subject role"
        elif category == "contest_event":
            answer_type = "clarification of the contest/event object, subject role, and whether it belongs in the dossier"
        else:
            answer_type = "clarification of what this item refers to and whether it belongs in the dossier"
    return {
        "decisionState": "needs_clarification",
        "actionability": "needs_clarification",
        "suggestedDecision": "needs_clarification",
        "publicSafe": False,
        "hasSafePublicSuggestion": False,
        "suggestedPublicWording": "",
        "recommendedAction": "needs_clarification",
        "recommendedActionReason": "BNL must clarify the item scope before any public/internal/reject Source File decision.",
        "cannotSuggestPublicReason": "BNL does not know what this item refers to yet.",
        "displayTitle": "Needs clarification before review",
        "displayDecision": "What is this item referring to?",
        "displayWhatIsBeingChecked": "You are not approving this as a fact yet. BNL needs the missing context before this can become a Source File decision.",
        "displayWhyItExists": "BNL found a possible signal, but it does not know enough about the source, subject role, audience, or context to make a public/internal/reject decision.",
        "displayWhyBNLFlaggedIt": "BNL found a possible signal, but it does not know enough about the source, subject role, audience, or context to make a public/internal/reject decision.",
        "displayBNLRecommendation": "Ask for clarification or dismiss it as unusable. Do not approve public wording yet.",
        "displayEvidenceSummary": "Possible signal only; the referenced source, subject role, audience, or context is not clear enough to review as a fact.",
        "displaySafetyDefault": "Do not use this in public wording until the missing context is clear.",
        "displaySafeDefault": "Do not use this in public wording until the missing context is clear.",
        "displayApprovalInstruction": "Do not approve public wording from this card. First clarify what this refers to.",
        "confirmationTarget": "admin",
        "primaryActionLabel": "Add clarification question",
        "secondaryActionLabels": ["Keep as internal note", "Reject / not useful"],
        "verificationPacketQuestion": question,
        "verificationPacketQuestions": [question],
        "verificationPacketAudience": str(follow_up.get("audience") or "admin"),
        "recommendedFollowUpQuestion": question,
        "recommendedFollowUpAudience": str(follow_up.get("audience") or "admin"),
        "recommendedFollowUpReason": reason,
        "bestGuessConfidence": str(follow_up.get("confidence") or "low"),
        "suggestedConfirmationQuestion": question,
        "suggestedConfirmationAudience": str(follow_up.get("audience") or "admin"),
        "suggestedConfirmationReason": reason,
        "suggestedConfirmationAnswerType": answer_type,
        "suggestedMissingInfoQuestion": question,
        "followUpCategory": category,
        "fallbackUsed": bool(follow_up.get("fallbackUsed")),
        "notEnoughContextReason": str(follow_up.get("notEnoughContextReason") or reason),
        "followUpScopeKnown": False,
        "followUpScopeLabel": str(follow_up.get("scopeLabel") or ""),
    }

def _scoped_follow_up_for_claim(subject: str, claim_text: str, claim_type: str, lane: str, actionability: str, scope: dict[str, Any] | None = None) -> dict[str, Any]:
    return _universal_follow_up_for_claim(subject, claim_text, claim_type, lane, actionability, scope)

def _has_admin_contest_organizer_rejection(resolved_memory: dict[str, Any], packet: dict[str, Any] | None = None) -> bool:
    blob = json.dumps(packet or {}, default=str) + " " + " ".join(json.dumps(resolved_memory.get(k) or [], default=str) for k in ("reviewOnlyEvidence", "privateOrInternalEvidence", "sourceSafetyWarnings", "missingInfoQuestions"))
    return bool(_CONTEST_REJECTION_RE.search(blob))

_BLOCKED_LABEL_ALIASES = {
    "artist": {"artist", "music artist"},
    "producer": {"producer", "music producer"},
    "moderator": {"moderator", "mod"},
    "core team": {"core team", "staff", "team member"},
    "barcode team": {"barcode team", "barcode_team", "barcodeteam"},
    "contest organizer": {"contest organizer", "contest_organizer", "organizer", "contest host", "contest_host", "host"},
    "contest host": {"contest host", "contest_host", "host", "contest organizer", "contest_organizer"},
    "judge": {"judge", "contest judge", "contest_judge"},
    "participant": {"participant", "contest participant", "contest_participant"},
    "owner": {"owner"},
    "public link owner": {"public link owner", "link owner", "owned link", "owns link", "public_link_owner"},
    "alias": {"alias", "aka", "known as"},
    "relationship": {"relationship", "connected to", "associated with"},
    "queue submitter": {"queue submitter", "submitter", "queue_submission", "submission history"},
}
_ADMIN_APPROVED_RE = re.compile(r"\b(?:approved public fact|public fact approved|admin approved|approved_public_fact|approve_public|admin approved public|owner[-_ ]confirmed|public-safe approved)\b", re.I)
_ADMIN_INTERNAL_RE = re.compile(r"\b(?:confirmed internal note|internal correction|admin internal note|saved internal note|confirmed_internal_note)\b", re.I)
_ADMIN_REJECT_RE = re.compile(r"\b(?:rejected claim|rejected weak label|rejected label|admin rejected|reject(?:ed)?|not an? |is not an? |do not infer|blocked decision|source-blind blocked|source_blind_blocked)\b", re.I)
_ADMIN_MISSING_RE = re.compile(r"\b(?:missing[-_ ]info answer|answered missing info|admin answer|missing_info_answer|question answered)\b", re.I)

def _label_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()

def _detect_blocked_label(text: str) -> str:
    low = _label_key(text)
    for canonical, aliases in _BLOCKED_LABEL_ALIASES.items():
        if any(_label_key(alias) and _label_key(alias) in low for alias in aliases):
            return canonical
    return ""

def _admin_text_items(value: Any, path: str = "") -> list[tuple[str, str, Any]]:
    items: list[tuple[str, str, Any]] = []
    if isinstance(value, dict):
        keys = " ".join(str(k) for k in value.keys())
        text_bits = []
        for k, v in value.items():
            if isinstance(v, (str, int, float, bool)) or v is None:
                text_bits.append(f"{k}: {v}")
            elif isinstance(v, (dict, list)):
                items.extend(_admin_text_items(v, f"{path}.{k}" if path else str(k)))
        combined = " ".join(text_bits)
        if combined:
            items.append((combined, f"{path} {keys}", value))
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            items.extend(_admin_text_items(item, f"{path}[{idx}]"))
    elif value is not None:
        items.append((str(value), path, value))
    return items

def extract_admin_corrections(subject: str, resolved_memory: dict[str, Any] | None = None, packet: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Normalize admin decisions/corrections from Source File packet and resolver context.

    The returned structure is internal review metadata only; public dossier code must
    use only approved public facts from these rows.
    """
    sources: list[Any] = [packet or {}]
    if resolved_memory:
        for key in ("reviewOnlyEvidence", "privateOrInternalEvidence", "sourceSafetyWarnings", "missingInfoQuestions", "publicSafeFacts", "publicSafeNotes"):
            sources.append({key: resolved_memory.get(key)})
    corrections: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for source in sources:
        for text, path, raw in _admin_text_items(source):
            hay = f"{path} {text}"
            label = _detect_blocked_label(hay)
            kind = ""
            blocks = False
            public_safe = False
            if _ADMIN_APPROVED_RE.search(hay):
                kind = "approved_public_fact"; public_safe = True
            elif _ADMIN_INTERNAL_RE.search(hay):
                kind = "confirmed_internal_note"; blocks = bool(label)
            elif _ADMIN_MISSING_RE.search(hay):
                kind = "missing_info_answer"; blocks = bool(label) or bool(re.search(r"not (?:the )?(?:organizer|host|artist|moderator|owner|submitter)", hay, re.I))
                if not label: label = _detect_blocked_label(hay)
            elif _ADMIN_REJECT_RE.search(hay) and label:
                kind = "rejected_label"; blocks = True
            if not kind:
                continue
            if kind == "approved_public_fact" and isinstance(raw, dict):
                clean = _text(raw.get("text") or raw.get("fact") or raw.get("value") or raw.get("wording") or text, 260)
            elif kind in {"confirmed_internal_note", "missing_info_answer"} and isinstance(raw, dict):
                clean = _text(raw.get("answer") or raw.get("note") or raw.get("text") or raw.get("value") or text, 260)
            else:
                clean = _text(text, 260)
            if not label and kind in {"approved_public_fact", "confirmed_internal_note", "missing_info_answer"}:
                label = _detect_blocked_label(clean)
            norm = clean
            if blocks and label:
                norm = f"{subject} is not a {label}." if label[0] not in "aeiou" else f"{subject} is not an {label}."
            rec = {"subject": subject, "kind": kind, "label": label, "normalizedMeaning": norm, "authority": "admin", "publicSafe": public_safe, "blocksInference": bool(blocks), "sourcePath": path[:120]}
            key = (rec["kind"], rec["label"], rec["normalizedMeaning"].lower())
            if key not in seen:
                seen.add(key); corrections.append(rec)
    return corrections[:24]

def _correction_blocks_claim(corrections: list[dict[str, Any]], claim_text: str, claim_type: str) -> dict[str, Any] | None:
    hay = f"{claim_type} {claim_text}"
    for corr in corrections:
        if not corr.get("blocksInference"):
            continue
        label = str(corr.get("label") or "")
        if not label:
            continue
        aliases = _BLOCKED_LABEL_ALIASES.get(label, {label})
        if any(_label_key(alias) and _label_key(alias) in _label_key(hay) for alias in aliases):
            return corr
    return None

def _approved_public_correction_facts(corrections: list[dict[str, Any]], subject: str) -> list[str]:
    facts = []
    for corr in corrections:
        if corr.get("kind") == "approved_public_fact" and corr.get("publicSafe"):
            clean = _sanitize_public(str(corr.get("normalizedMeaning") or ""), subject, [])
            if clean and clean not in facts:
                facts.append(clean)
    return facts

_ROLE_OR_TAXONOMY_LABELS = {
    "artist", "member", "community", "collaborator", "mod", "moderator", "personnel", "staff", "sponsor", "system", "interface", "production", "radio", "producer", "ai", "human", "hybrid", "unknown", "public", "internal", "restricted", "active", "pending", "person", "music", "tech", "systems", "broadcast", "participant", "dossier", "source", "source file",
}
_PUBLIC_POLICIES = {"public_home", "public_context", "public_selective", "broadcast_memory", "public"}
_PUBLIC_VISIBILITIES = {"public", "public_safe", "dossier_safe", "public_candidate", "public_use"}
_PUBLIC_AUTHORITIES = {"public", "public_safe", "dossier_safe", "broadcast_memory", "public_conversation", "owner_confirmed", "official_public_dossier", "public_discord_observed", "queue_submission_confirmed"}


def _text(value: Any, limit: int = 320) -> str:
    if value is None or isinstance(value, (dict, list)):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()[:limit].rstrip()


def _subject_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").lower()).strip("_")


def _is_probable_subject_alias(label: str) -> bool:
    clean = _text(label, 100)
    low = clean.lower().strip()
    if not clean or low in _ROLE_OR_TAXONOMY_LABELS or len(clean) < 3 or len(clean) > 80:
        return False
    words = re.findall(r"[A-Za-z0-9]+", clean)
    return bool(words) and not (len(words) == 1 and clean.islower() and not any(ch.isdigit() for ch in clean))


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    try:
        return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
    except sqlite3.Error:
        return False


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _matches(text: str, terms: list[str]) -> bool:
    low = (text or "").lower()
    keys = {_subject_key(text)}
    return any(term and (term.lower() in low or _subject_key(term) in keys or _subject_key(term) in _subject_key(text)) for term in terms)


def _sanitize_public(text: str, subject: str, aliases: list[str]) -> str:
    clean = _text(text, 260)
    for alias in sorted([a for a in aliases if a.lower() != subject.lower()], key=len, reverse=True):
        clean = re.sub(re.escape(alias), subject, clean, flags=re.I)
    if (_PAYMENT_RE.search(clean) or _INTERNAL_RE.search(clean) or _EMAIL_RE.search(clean) or _PHONE_RE.search(clean) or _RAW_ID_RE.search(clean) or _SECRET_RE.search(clean) or _CONTACT_RE.search(clean) or _PRIVATE_ALIAS_RE.search(clean) or _DB_ROW_RE.search(clean)):
        return ""
    return clean



def _redact_review_evidence_summary(text: str, subject: str) -> str:
    """Return a privacy-safe review/source-blind evidence summary.

    Unlike _sanitize_public, this is for non-public metadata: it may preserve
    coarse claim category (Orion/context, queue/submission, music/link) but must
    not preserve raw contact details, private aliases, IDs, URLs, tokens, payment
    details, or database-row wording.
    """
    original = _text(text, 360)
    low = original.lower()
    if not original:
        return f"Review-needed evidence exists for {subject}, but the raw detail was withheld for privacy."
    payment = bool(_PAYMENT_RE.search(original))
    unsafe = bool(
        payment
        or _EMAIL_RE.search(original)
        or _PHONE_RE.search(original)
        or _RAW_ID_RE.search(original)
        or _SECRET_RE.search(original)
        or _CONTACT_RE.search(original)
        or _INTERNAL_RE.search(original)
        or _PRIVATE_ALIAS_RE.search(original)
        or _DB_ROW_RE.search(original)
        or _LINK_RE.search(original)
    )
    if unsafe:
        if payment:
            return "Private/payment-related evidence was counted but withheld."
        if "source-blind" in low or "source blind" in low or "unknown-policy" in low or "no provenance" in low:
            if "orion" in low:
                return f"Source-blind Orion/context evidence exists for {subject}, but raw details were withheld."
            return "Source-blind context exists, but raw details were withheld."
        if "orion" in low:
            return f"{subject} has recurring Orion-related context. Raw supporting detail withheld for privacy/source safety."
        if "queue" in low or "submission" in low:
            return f"{subject} has possible queue/submission context. Raw supporting detail withheld for privacy/source safety."
        if "suno" in low or "music" in low or "link" in low or "url" in low:
            return f"{subject} has possible music/link context. Raw supporting detail withheld for privacy/source safety."
        if "relationship" in low or "relay" in low or "context" in low:
            return f"{subject} has possible relationship/context evidence. Raw supporting detail withheld for privacy/source safety."
        return "Review-needed evidence exists, but the raw detail was withheld for privacy."
    clean = original
    clean = _EMAIL_RE.sub("[redacted email]", clean)
    clean = _PHONE_RE.sub("[redacted phone]", clean)
    clean = _RAW_ID_RE.sub("[redacted id]", clean)
    clean = _SECRET_RE.sub("[redacted secret]", clean)
    clean = _LINK_RE.sub("[redacted link]", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    if _EMAIL_RE.search(clean) or _PHONE_RE.search(clean) or _RAW_ID_RE.search(clean) or _SECRET_RE.search(clean) or _LINK_RE.search(clean):
        return "Review-needed evidence exists, but the raw detail was withheld for privacy."
    return clean[:240].rstrip()


def _source_blind_warning_from_summary(summary: str, subject: str) -> str:
    """Return only category-level safe wording for source-blind warnings."""
    low = _text(summary, 360).lower()
    if "orion" in low or "relay" in low:
        return f"Source-blind Orion/context evidence exists for {subject}, but raw details were withheld."
    if "queue" in low or "submission" in low:
        return f"Source-blind queue/submission context exists for {subject}, but raw details were withheld."
    if "suno" in low or "music" in low or "song" in low or "track" in low or "link" in low or "url" in low or _LINK_RE.search(summary or ""):
        return f"Source-blind music/link context exists for {subject}, but raw details were withheld."
    if "relationship" in low or "context" in low:
        return f"Source-blind Orion/context evidence exists for {subject}, but raw details were withheld."
    if "barcode" in low or "bnl" in low or "community" in low:
        return f"Source-blind community context exists for {subject}, but raw details were withheld."
    return "Some subject memory lacked public-safe provenance and was not used as public copy."

def _blank_result(subject: str, aliases: list[str]) -> dict[str, Any]:
    return {
        "subjectName": subject,
        "matchedAliasesUsedPrivately": aliases[:8],
        "publicSafeFacts": [], "publicSafeNotes": [], "reviewOnlyEvidence": [], "privateOrInternalEvidence": [],
        "publicRoleSignals": [], "publicCreativeMusicSignals": [], "publicCommunitySignals": [], "publicLinkSignals": [],
        "queueOrSubmissionSignals": [], "relationshipOrContextSignals": [], "sourceSafetyWarnings": [], "missingInfoQuestions": [],
        "confidence": "low",
        "evidenceCounts": {"publicSafe": 0, "reviewOnly": 0, "privateOrInternal": 0, "sourceBlind": 0, "totalScanned": 0},
        "diagnostic": {"tablesScanned": [], "tablesContributed": {}, "classificationReasons": {}},
    }


def _public_ready(data: dict[str, Any], text: str) -> bool:
    blob = " ".join(str(data.get(k) or "") for k in data)
    return bool(data.get("public_safe") or data.get("public_safe_candidate") or _PUBLIC_RE.search(blob) or str(data.get("channel_policy") or "").lower() in _PUBLIC_POLICIES or str(data.get("visibility") or "").lower() in _PUBLIC_VISIBILITIES or str(data.get("authority") or "").lower() in _PUBLIC_AUTHORITIES) and not (_REVIEW_RE.search(blob) or _SOURCE_BLIND_RE.search(blob) or _PAYMENT_RE.search(text) or _INTERNAL_RE.search(text))


def _classify(data: dict[str, Any], text: str) -> tuple[str, str]:
    blob = " ".join(str(data.get(k) or "") for k in data)
    if not text:
        return "unusable", "empty_text"
    if _PAYMENT_RE.search(blob):
        return "private_internal", "private_or_internal_terms"
    if _SOURCE_BLIND_RE.search(blob):
        return "source_blind", "missing_public_provenance"
    if _INTERNAL_RE.search(blob):
        return "private_internal", "private_or_internal_terms"
    if _REVIEW_RE.search(blob) or bool(data.get("review_only")):
        return "review_only", "needs_review_or_confirmation"
    if _public_ready(data, text):
        return "public_safe", "explicit_public_safe_status"
    return "source_blind", "no_public_safe_status"


def _add_public_sections(result: dict[str, Any], text: str) -> None:
    low = text.lower()
    for key in ("publicSafeFacts", "publicSafeNotes"):
        if text not in result[key] and len(result[key]) < 10:
            result[key].append(text)
    if any(w in low for w in ("artist", "member", "moderator", "producer", "dj", "role", "known for")):
        result["publicRoleSignals"].append(text)
    if any(w in low for w in ("music", "track", "song", "artist", "producer", "dj", "radio", "album")):
        result["publicCreativeMusicSignals"].append(text)
    if any(w in low for w in ("barcode", "bnl", "community", "source file", "dossier")):
        result["publicCommunitySignals"].append(text)
    if _LINK_RE.search(text):
        result["publicLinkSignals"].append(text)
    if any(w in low for w in ("relationship", "context", "barcode", "bnl", "community")):
        result["relationshipOrContextSignals"].append(text)


def _scan_table(conn: sqlite3.Connection, table: str, subject: str, aliases: list[str]) -> list[dict[str, Any]]:
    cols = _table_columns(conn, table)
    if not cols:
        return []
    text_cols = [c for c in ("safe_summary", "cleaned_summary", "summary", "fact_value", "fact_label", "note", "body", "message", "content", "text", "topic", "description", "public_summary") if c in cols]
    terms = [subject] + aliases
    key_terms = [_subject_key(t) for t in terms]
    clauses, params = [], []
    for col in ("subject_name", "entity_name", "name", "display_name"):
        if col in cols:
            clauses.extend([f"LOWER({col})=?" for _ in terms]); params.extend(t.lower() for t in terms)
    for col in ("subject_key", "entity_key", "normalized_subject"):
        if col in cols:
            clauses.extend([f"LOWER({col})=?" for _ in key_terms]); params.extend(key_terms)
    for col in text_cols:
        for term in terms:
            clauses.append(f"{col} LIKE ?"); params.append(f"%{term}%")
    where = " OR ".join(f"({c})" for c in clauses) or "1=1"
    try:
        rows = conn.execute(f"SELECT * FROM {table} WHERE {where} ORDER BY rowid DESC LIMIT 300", params).fetchall()
    except sqlite3.Error:
        return []
    out = []
    for row in rows:
        data = dict(row)
        text = next((_text(data.get(c)) for c in text_cols if _text(data.get(c))), "")
        hay = " ".join(_text(data.get(c), 120) for c in cols)
        if text and _matches(hay, terms):
            data["_resolver_text"] = text
            out.append(data)
    return out


def resolve_subject_memory(subject_name: str, db_path: str, aliases: list[str] | None = None) -> dict[str, Any]:
    subject = _text(subject_name, 120) or "Unnamed subject"
    clean_aliases = []
    for alias in aliases or []:
        if _is_probable_subject_alias(alias) and alias.lower() != subject.lower() and alias not in clean_aliases:
            clean_aliases.append(alias)
    result = _blank_result(subject, clean_aliases)
    if not db_path:
        result["sourceSafetyWarnings"].append("No local BNL memory database was available for subject memory resolution.")
        return result
    tables = ["entity_evidence_events", "entity_intelligence_facts", "broadcast_memory", "source_file_enrichments", "bnl_source_file_enrichment", "dossier_candidates", "bnl_dossier_recommendations", "dossier_recommendations", "population_recommendations", "conversation_memory", "message_memory", "messages", "memory_tiers", "user_memory_facts", "relationship_journal", "member_activity_events"]
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True); conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        result["sourceSafetyWarnings"].append("Local BNL memory database could not be opened read-only.")
        return result
    try:
        for table in tables:
            if not _table_exists(conn, table):
                continue
            result["diagnostic"]["tablesScanned"].append(table)
            rows = _scan_table(conn, table, subject, clean_aliases)
            if rows:
                result["diagnostic"]["tablesContributed"][table] = len(rows)
            for data in rows:
                raw = data.get("_resolver_text") or ""
                klass, reason = _classify(data, raw)
                result["evidenceCounts"]["totalScanned"] += 1
                result["diagnostic"]["classificationReasons"][reason] = result["diagnostic"]["classificationReasons"].get(reason, 0) + 1
                if klass == "public_safe":
                    safe = _sanitize_public(raw, subject, clean_aliases)
                    if safe:
                        _add_public_sections(result, safe); result["evidenceCounts"]["publicSafe"] += 1
                elif klass == "review_only":
                    safe_review = _redact_review_evidence_summary(raw, subject) or "Subject memory needs owner/admin confirmation before public use."
                    result["reviewOnlyEvidence"].append({"table": table, "summary": safe_review, "reason": reason})
                    result["evidenceCounts"]["reviewOnly"] += 1
                    if re.search(r"queue|submission", str(raw), re.I): result["queueOrSubmissionSignals"].append(safe_review if safe_review else "Queue or submission context needs admin confirmation before public use.")
                    if re.search(r"relationship|context|orion|relay|references|through", str(raw), re.I) and safe_review:
                        result["relationshipOrContextSignals"].append(safe_review)
                elif klass == "private_internal":
                    result["privateOrInternalEvidence"].append({"table": table, "summary": "Private/internal memory was excluded from public copy.", "reason": reason})
                    result["evidenceCounts"]["privateOrInternal"] += 1
                elif klass == "source_blind":
                    warning = _source_blind_warning_from_summary(raw, subject)
                    if re.search(r"relationship|context|orion|relay|references|through", str(raw), re.I):
                        result["relationshipOrContextSignals"].append(warning)
                    result["sourceSafetyWarnings"].append(warning)
                    result["evidenceCounts"]["sourceBlind"] += 1
        ps = result["evidenceCounts"]["publicSafe"]
        result["confidence"] = "high" if ps >= 5 else ("medium" if ps >= 2 else "low")
        if ps == 0:
            result["missingInfoQuestions"].append("Confirm at least one public-safe subject fact before drafting rich public dossier copy.")
        if result["evidenceCounts"]["reviewOnly"] or result["evidenceCounts"]["sourceBlind"]:
            result["missingInfoQuestions"].append("Review BNL subject memory and promote confirmed public-safe facts with provenance.")
    finally:
        conn.close()
    LOG.debug("subject_memory_resolver subject=%s tables=%s publicSafe=%s reviewOnly=%s rejected=%s confidence=%s", subject, result["diagnostic"]["tablesScanned"], result["evidenceCounts"]["publicSafe"], result["evidenceCounts"]["reviewOnly"], result["evidenceCounts"]["privateOrInternal"] + result["evidenceCounts"]["sourceBlind"], result["confidence"])
    return result



def _dedupe_by_pattern(items: list[str], subject: str, limit: int = 9) -> list[str]:
    """Collapse repetitive subject evidence into a small public-safe ingredient set."""
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        clean = _sanitize_public(item, subject, [])
        if not clean:
            continue
        key = re.sub(re.escape(subject), "subject", clean.lower(), flags=re.I)
        key = re.sub(r"\b\d+\b", "#", key)
        key = re.sub(r"\b(public[-_\s]*safe|public|barcode|bnl|community|subject|memory|context|recurring|regularly|appears|mentioned)\b", "", key)
        words = sorted(set(re.findall(r"[a-z]{4,}", key)))[:8]
        pattern = " ".join(words) or clean.lower()[:80]
        if pattern in seen:
            continue
        seen.add(pattern)
        out.append(clean)
        if len(out) >= limit:
            break
    return out



def _claim_type_from_text(text: str) -> str:
    low = (text or "").lower()
    if any(w in low for w in ("do not", "never state", "must not")):
        return "do_not_say"
    if "contest" in low and any(w in low for w in ("music", "suno", "song", "track", "artist", "link", "url")):
        return "contest_music_context"
    if "contest" in low and any(w in low for w in ("submit", "submission", "entry")):
        return "contest_submitter"
    if re.search(r"\b(rules?|instructions?|guidelines?)\b", low):
        return "rules_instructions_context"
    if any(w in low for w in ("queue", "submission")):
        return "queue_submission"
    if any(w in low for w in ("suno", "music link", "social link", "http", "link")):
        return "music_link" if any(w in low for w in ("suno", "music", "song", "track")) else "public_link"
    if any(w in low for w in ("orion", "relationship", "relay", "references", "through", "context")):
        return "relationship"
    if any(w in low for w in ("artist", "participant", "member", "role", "collaborator", "moderator", "producer", "dj")):
        return "role"
    if any(w in low for w in ("barcode", "bnl", "community")):
        return "community_context"
    return "missing_confirmation"





_ACTIVITY_CONTEXT_TYPES = {
    "community_context", "systems_context", "show_operations", "collaboration_interest", "collaboration_status", "contest_event_activity",
    "queue_submission_activity", "music_discussion", "link_reference", "lore_context",
    "theory_anomaly_context", "rules_instruction_context", "source_blind_context", "unknown_context",
}
_NON_ROLE_LABEL_RE = re.compile(r"\b(contest/event activity|collaboration interest|queue/submission history|link ownership|lore/context|theory/anomaly|rules/instructions|evidence signal|source-blind context|activity signal|conversation topic)\b", re.I)
_CONFIRMED_COLLAB_RE = re.compile(r"\b(?:approved|confirmed|official|public[- ]safe|released|published|credited)\b.{0,60}\b(?:collaborator|collaboration|worked with|featured)\b|\b(?:collaborator|collaboration|worked with|featured)\b.{0,60}\b(?:approved|confirmed|official|public[- ]safe|released|published|credited)\b", re.I)
_COLLAB_INTEREST_RE = re.compile(r"\b(?:wants? to|interested in|asked to|hopes? to|would like to|can|could|may)\b.{0,50}\b(?:collab\w*|collaborat\w*|work together)\b|\b(?:collaboration interest|interested in collaborating)\b", re.I)

def _activity_context_type_for_text(claim_text: str, claim_type: str = "", lane: str = "") -> str:
    low = f"{claim_type} {lane} {claim_text}".lower()
    if "source_blind" in low or "source-blind" in low:
        return "source_blind_context"
    if _CONFIRMED_COLLAB_RE.search(low):
        return "collaboration_status"
    if _COLLAB_INTEREST_RE.search(low) or ("collaboration" in low and not _CONFIRMED_COLLAB_RE.search(low)):
        return "collaboration_interest"
    if re.search(r"\b(show operations?|show[- ]ops|show[- ]operations?|broadcast operations?|run of show|show workflow|show production)\b", low):
        return "show_operations"
    if re.search(r"\b(contest|challenge|tournament|battle|competition|event)\b", low):
        return "contest_event_activity"
    if re.search(r"\b(queue|submission|submitted|submitter|entry)\b", low):
        return "queue_submission_activity"
    if re.search(r"\b(system|systems|technical|operator|network)\b", low):
        return "systems_context"
    if re.search(r"\b(community|barcode radio|viewer|participant|member)\b", low):
        return "community_context"
    if re.search(r"\b(music|song|track|demo|artist page|soundcloud|spotify|suno)\b", low):
        return "music_discussion"
    if re.search(r"https?://|\b(link|url|social)\b", low):
        return "link_reference"
    if re.search(r"\b(lore|canon|recurring bit|recurring topic)\b", low):
        return "lore_context"
    if re.search(r"\b(theory|theories|anomal|glitch)\b", low):
        return "theory_anomaly_context"
    if re.search(r"\b(rules?|instructions?|guidelines?)\b", low):
        return "rules_instruction_context"
    return "unknown_context"

def _public_role_candidate_for_subject(subject: str, evidence: str, signals: Any = None) -> dict[str, Any]:
    """Classify whether evidence supports a public role/title versus activity/context only."""
    text = " ".join([_text(evidence, 1000), json.dumps(signals or {}, default=str)[:1000]]).strip()
    low = text.lower()
    activity_type = _activity_context_type_for_text(text)
    base = {
        "roleCandidate": False, "roleLabel": "", "roleConfidence": "none", "roleEvidenceType": "none",
        "isPublicRole": False, "needsClarification": True, "reason": "No public role/title evidence was found.",
        "notRoleReason": "No public role/title should be stated from this evidence.", "activityContextType": activity_type,
    }
    if not low:
        base.update({"roleEvidenceType": "insufficient_context", "notRoleReason": "Insufficient context to identify a public role/title."}); return base
    if _COLLAB_INTEREST_RE.search(low):
        base.update({"roleEvidenceType": "collaboration_interest", "reason": "BNL found collaboration interest, not confirmed collaborator status.", "notRoleReason": "Interest in collaborating is not the same as an approved public collaborator role."}); return base
    if _NON_ROLE_LABEL_RE.search(low) or activity_type in {"contest_event_activity", "queue_submission_activity", "show_operations", "collaboration_interest", "lore_context", "theory_anomaly_context", "rules_instruction_context", "source_blind_context"}:
        base.update({"roleEvidenceType": "activity_only", "reason": f"BNL found {activity_type.replace('_', ' ')}, not a public role/title.", "notRoleReason": "Activity/context signals must not be turned into public role/title wording."}); return base
    if _CONFIRMED_COLLAB_RE.search(low):
        base.update({"roleCandidate": True, "roleLabel": "collaborator", "roleConfidence": "high", "roleEvidenceType": "collaboration_confirmed", "isPublicRole": True, "needsClarification": False, "reason": "Evidence indicates confirmed/approved collaborator status.", "notRoleReason": ""}); return base
    explicit = [(r"\b(?:is|as|public role|title)\b.{0,40}\b(artist|producer|moderator|host|sponsor|guest|technical operator|team member|network operator|community support)\b", "explicit_role"), (r"\b(artist|producer|moderator|host|sponsor|guest|technical operator|team member|network operator|community support)\b", "explicit_role")]
    for pat, etype in explicit:
        m = re.search(pat, low)
        if m:
            label = next((g for g in m.groups() if g), m.group(0))
            base.update({"roleCandidate": True, "roleLabel": label, "roleConfidence": "medium", "roleEvidenceType": etype, "isPublicRole": True, "needsClarification": True, "reason": "Role-like wording exists and needs exact public wording confirmation.", "notRoleReason": ""}); return base
    if re.search(r"\b(artist|producer|moderator|host|sponsor|guest|technical operator|team member|network operator|community support)\s+(?:role|title)\b", low):
        label = re.search(r"\b(artist|producer|moderator|host|sponsor|guest|technical operator|team member|network operator|community support)\s+(?:role|title)\b", low).group(1)
        base.update({"roleCandidate": True, "roleLabel": label, "roleConfidence": "medium", "roleEvidenceType": "explicit_role", "isPublicRole": True, "needsClarification": True, "reason": "Role/title wording exists and needs exact public wording confirmation.", "notRoleReason": ""}); return base
    if activity_type == "community_context":
        base.update({"roleCandidate": True, "roleLabel": "community member / BARCODE Radio viewer / community participant", "roleConfidence": "low", "roleEvidenceType": "community_pattern", "isPublicRole": False, "needsClarification": True, "reason": "Repeated community conversation may support a role clarification question, but not an assertion.", "notRoleReason": "Community activity needs admin confirmation before becoming a public role/title."}); return base
    if activity_type == "systems_context":
        base.update({"roleEvidenceType": "activity_only", "reason": "Systems-related conversation is context, not a systems role.", "notRoleReason": "Do not create a systems role unless a public role is confirmed."}); return base
    base.update({"roleEvidenceType": "insufficient_context", "activityContextType": activity_type, "notRoleReason": "Evidence is too thin or contextual to state a public role/title."})
    return base


def _display_clean(text: str) -> str:
    text = _text(text, 500)
    replacements = {
        "sourceFileReviewClaims": "BNL claims",
        "official_public_dossier": "public dossier",
        "public_safe": "approved for public use",
        "source_blind": "without a public source",
        "source-blind": "without a public source",
        "review_only": "for review only",
        "review-only": "for review only",
        "public_music_role": "public music role",
        "queue_submission": "queue or submission",
    }
    for old, new in replacements.items():
        text = re.sub(re.escape(old), new, text, flags=re.I)
    text = re.sub(r"\b[A-Za-z]+(?:[A-Z][a-z0-9]+)+\b", lambda m: "BNL item" if m.group(0) != "BNL" else m.group(0), text)
    return text


def _review_display_copy(subject: str, claim_text: str, claim_type: str, lane: str, public_safe: bool, actionability: str) -> dict[str, Any]:
    low = (claim_text or "").lower()
    is_ai_project = bool(re.search(r"\b(ai|persona|project|bot|model|character|lore)\b", low)) and claim_type in {"relationship", "community_context"}
    is_orion = "orion" in low and claim_type in {"relationship", "community_context"}
    title = "Review BNL claim"
    decision = "Is this claim approved for public use, internal use only, or should it be rejected?"
    checked = "You are checking whether this claim is true and approved for public use."
    why = "BNL found context that needs a human decision before it can be used publicly."
    rec = "BNL recommends asking for confirmation before using this in public copy."
    evidence_summary = "No public approval has been provided yet." if not public_safe else "BNL found approved public wording for this item."
    safe = "Keep this out of public copy until approved."
    approval = "Approve only the exact wording that can be safely used publicly."
    target = "admin"
    audience = "admin"
    primary = "Add to admin follow-up"
    secondary = ["Reject / not needed"]
    question = "What proof or approved wording should BNL use before saying this publicly?"

    if actionability == "non_actionable_artifact":
        title = "Internal audit item"
        decision = "Keep this internal or dismiss it."
        checked = "BNL found a weak internal signal, but it is not specific enough to become a Source File fact."
        why = "BNL flagged this because it is audit context, not a fact about the subject."
        rec = "Keep it only if it helps explain why BNL flagged the subject. Otherwise dismiss it."
        evidence_summary = "Internal signal only. No public fact is ready."
        safe = "Do not use this in public wording."
        approval = ""
        target = "none"; audience = "none"; question = ""
        primary = "Keep as internal note"; secondary = ["Dismiss artifact"]
    elif actionability == "source_blind_warning" or lane == "source_blind":
        title = "Needs public source"
        decision = "What public-safe source or owner-approved replacement wording supports this item?"
        checked = "BNL found context, but it cannot be used publicly until it has a public source or owner-approved replacement."
        why = "BNL flagged this because the available context is not enough public proof by itself."
        rec = "BNL recommends requesting a public source or owner-approved replacement wording before approval."
        evidence_summary = "Source-blind context exists, but it cannot be used publicly by itself."
        safe = "Use only a public source or owner-approved replacement wording."
        approval = "Only approve a public-source-backed replacement, not the source-blind note itself."
        target = "public_source"; audience = "public_source"
        primary = "Request public source"; secondary = ["Ask for owner-approved wording", "Reject / not needed"]
        question = "What proof or approved wording should BNL use before saying this publicly?"
    elif (not is_orion) and re.search(r"\blore[-\s]*heavy|\blore\b", low):
        scope = _lore_scope_for_text(subject, claim_text)
        title = "Clarify lore/context scope" if not scope["scopeKnown"] else f"Confirm {scope['scopeLabel']} use"
        decision = (f"Should BNL mention this {scope['scopeLabel']} in {subject}'s public dossier, or keep it internal?"
                    if scope["scopeKnown"] else f"Is this {subject}'s own lore, BARCODE Network lore, Orion lore, Null TV lore, or a recurring stream topic? Should it be public or internal?")
        checked = "You are deciding whether this context belongs in a concrete public dossier or should stay internal."
        why = f"BNL found lore/context wording, but it needs to know whether this is {subject}'s lore, BARCODE lore, Orion lore, Null TV lore, recurring show context, or something that should stay internal."
        rec = "BNL recommends keeping it internal unless an admin confirms it belongs in the public dossier."
        evidence_summary = "Lore-heavy context exists without a specific public factual claim."
        safe = "Keep as internal lore/context unless approved for public use."
        approval = "Approve only public lore/context wording, keep internal, or reject as not useful."
        target = "admin"; audience = "admin"
        primary = "Decide public/internal/reject"; secondary = ["Keep internal", "Reject / not needed"]
        question = decision
    elif re.search(r"\b(theory|theories|anomal(?:y|ies)|glitch)\b", low):
        scope = _theory_anomaly_scope_for_text(subject, claim_text)
        title = "Clarify theory/anomaly context"
        decision = (f"Should BNL mention this {scope['scopeLabel']} in {subject}'s public dossier, or keep it internal?"
                    if scope["scopeKnown"] else "What theory or anomaly is this referring to, and does it belong in the public dossier?")
        checked = "You are identifying the theory/anomaly scope before any public dossier decision."
        why = "BNL found theory/anomaly wording, but it needs to know what the theory/anomaly refers to before it can decide whether it belongs in the dossier."
        rec = "BNL recommends asking what the theory/anomaly is about before requesting public wording."
        evidence_summary = "Theory/anomaly context exists without enough scoped dossier meaning."
        safe = "Keep as internal context unless scope and public use are approved."
        approval = "Classify the theory/anomaly first; only then approve public/internal use."
        target = "admin"; audience = "admin"; primary = "Clarify scope"; secondary = ["Keep internal", "Reject / not needed"]
        question = decision if not scope["scopeKnown"] else f"BNL found {scope['scopeLabel']} context near {subject}. Does it belong in the public dossier?"
    elif claim_type == "rules_instructions_context":
        scope = _rules_instructions_scope_for_text(subject, claim_text)
        title = "Clarify rules/instructions context"
        decision = f"What rules or instructions is this referring to, who were they for, and what was {subject}'s actual role with them?"
        checked = "You are identifying which instructions are meant before treating them as a role or dossier fact."
        why = "BNL found rules/instructions wording, but it cannot tell whether these were contest rules, queue instructions, Discord/admin instructions, broadcast instructions, site workflow instructions, or unrelated text."
        rec = "BNL recommends clarifying the instruction source, audience, purpose, and subject role before any public claim."
        evidence_summary = f"Rules/instructions signal: {scope['scopeLabel'] or 'unknown instructions'}."
        safe = "Do not create a public role/title claim from rules/instructions wording alone."
        approval = "Clarify what the instructions were and the subject's role before approving wording."
        target = "admin"; audience = "admin"; primary = "Clarify scope"; secondary = ["Keep internal", "Reject / not needed"]
        question = decision
    elif actionability == "weak_label":
        title = "Check weak label"
        label = "contest organizer" if "contest organizer" in low else _text(claim_text, 80)
        decision = f"Is the label \"{label}\" accurate and useful for {subject}, or should it be rejected?"
        checked = "You are checking whether a thin label has enough support to keep internally or approve later."
        why = "BNL flagged this because the label was too vague to approve as public copy."
        rec = "BNL recommends keeping this internal only if it helps review, otherwise reject it."
        evidence_summary = "BNL saw a weak internal signal, but no Source File fact is ready."
        safe = "Do not use this in public wording."
        approval = "Only keep this as an internal note if the label is useful and accurate."
        target = "admin"; audience = "admin"; primary = "Save BNL internal note"; secondary = ["Reject weak label"]
        question = decision
    elif claim_type in {"music_link", "public_link"}:
        title = "Confirm approved public links"
        decision = f"Which links, if any, may BNL publicly associate with {subject}?"
        checked = f"You are checking whether these links belong to {subject} and whether BNL may mention them publicly."
        why = "BNL found repeated link or music context, but it does not yet know which links are approved for public reference."
        rec = "BNL recommends asking who owns the links and which ones are approved for public use."
        evidence_summary = f"BNL saw music/link context connected to {subject}, but no owner-approved public link list has been confirmed."
        safe = "Keep the links out of public copy until approved."
        approval = f"Only approve links that are confirmed as {subject}'s and approved for public reference."
        target = "link_ownership"; audience = "link_ownership"; primary = "Keep as internal note"; secondary = ["Ask who owns these links", "Reject / not needed"]
        question = f"Which {subject} links, music links, or social links are yours and approved for BNL to mention publicly?"
    elif claim_type.startswith("contest_"):
        scope = _contest_scope_for_text(subject, claim_text)
        title = "Confirm contest relationship"
        decision = (f"BNL found contest context involving {scope['scopeLabel']} near {subject}. Was {subject} a participant, submitter, rules/link source, organizer, host, or just mentioned nearby?"
                    if scope["scopeKnown"] else f"BNL found unclear contest/rules wording near {subject}. What contest or event is this referring to, if any, and was {subject} actually involved?")
        checked = f"You are checking whether {subject} was a participant, submitter, rules poster, link source, host, organizer, or only mentioned near the contest."
        why = f"BNL found contest-related wording, but it cannot tell whether {subject} participated, submitted, reposted rules, shared a link, hosted anything, organized anything, or was only mentioned nearby. The host/organizer must come from direct evidence, not proximity."
        rec = "BNL recommends asking for the exact relationship before using this publicly."
        evidence_summary = "BNL saw contest/rules context, but the subject's role is not confirmed."
        safe = f"Do not call {subject} a host or organizer unless that role is confirmed."
        approval = "Only approve exact wording that matches the confirmed relationship."
        target = "subject"; audience = "subject"; primary = "Keep as internal note"; secondary = ["Ask about contest relationship", "Reject / not needed"]
        question = decision
    elif is_orion:
        title = "Confirm Orion/context use"
        decision = f"Can BNL mention Orion in public {subject} context, or should it stay internal?"
        checked = "You are checking whether this is public-safe story/context, internal continuity, or a private/source-blind reference."
        why = "BNL found Orion/context references, but public use needs confirmation."
        rec = "BNL recommends keeping this internal unless public wording is approved."
        evidence_summary = "Orion/context reference exists, but public-safe wording is not confirmed."
        safe = "Keep Orion/context internal until approved."
        approval = "Only approve public wording if Orion/context is allowed for this subject."
        target = "subject"; audience = "subject"; primary = "Keep as internal note"; secondary = ["Ask about Orion/context use", "Reject / not needed"]
        question = f"Can BNL mention Orion in public {subject} context, or should that stay internal?"
    elif is_ai_project:
        title = "Confirm project relationship"
        decision = f"How may BNL describe {subject}'s connection to this AI/persona/project context?"
        checked = "You are checking whether this is a real public relationship, an internal/lore reference, or just nearby context that should not be used publicly."
        why = f"BNL found {subject} near AI/persona/project context, but the public-safe relationship is not confirmed."
        rec = "BNL recommends asking what wording is approved before using this in public copy."
        evidence_summary = "BNL found AI/persona/project context, but no approved public relationship wording yet."
        safe = "Keep this internal until the relationship and wording are confirmed."
        approval = "Only approve the exact public wording that the subject or admin confirms."
        target = "owner_approved_wording"; audience = "subject"; primary = "Keep as internal note"; secondary = ["Ask for approved wording", "Reject / not needed"]
        question = "Can BNL mention this AI/persona/project connection publicly? If yes, what exact wording should it use?"
    elif claim_type in {"role", "community_context"}:
        role_read = _public_role_candidate_for_subject(subject, claim_text, {"lane": lane, "publicSafe": public_safe})
        if not role_read.get("isPublicRole"):
            title = "Public role/title not confirmed"
            decision = "Should BNL state a public role for this subject, or avoid role/title wording for now?"
            checked = "BNL found activity/context signals, but those do not prove a public role or title."
            why = "This card exists because a public dossier may need a short role/title, but BNL cannot infer one from activity alone."
            rec = "BNL recommends not stating a public role unless an admin confirms one."
            evidence_summary = f"BNL found {role_read.get('activityContextType', 'activity/context').replace('_', ' ')}; this is not a confirmed public role/title."
            safe = "Do not turn activity signals into public role/title wording."
            approval = "Approve only an exact public role/title, or mark that no public role should be stated yet."
            target = "subject"; audience = "subject"; primary = "Mark no public role yet"; secondary = ["Approve exact role/title", "Keep as internal context"]
            question = f"BNL found activity/context near {subject}, but this does not prove a public role. Should BNL describe {subject} with an approved public role/title, leave role/title blank, or use some other approved wording?"
        else:
            title = "Confirm public role"
            decision = f"What public role/title may BNL use for {subject}, if any?"
            checked = "You are checking whether the role is true and approved for public use."
            why = "BNL found role-like context, but role wording needs owner/admin approval before public use."
            rec = "BNL recommends confirming the exact public role/title before using it."
            evidence_summary = "BNL saw role-like context, but exact public title is not approved."
            safe = "Do not use a role/title publicly until it is confirmed."
            approval = "Only approve the exact role/title that is confirmed for public use."
            target = "subject"; audience = "subject"; primary = "Keep as internal note"; secondary = ["Ask for public role/title", "Reject / not needed"]
            question = "What public role/title should BNL use for you, if any?"
    elif claim_type == "queue_submission":
        title = "Confirm queue/submission history"
        decision = "May BNL mention this queue or submission history publicly?"
        checked = "You are checking whether this history is public-safe, useful, and approved for mention."
        why = "BNL found queue/submission context, but it needs confirmation before public use."
        rec = "BNL recommends keeping it internal unless the subject/admin approves public wording."
        evidence_summary = "Queue/submission context exists, but public-safe wording is not confirmed."
        safe = "Keep queue/submission history internal unless approved."
        approval = "Only approve exact public wording that does not expose private queue or admin context."
        target = "admin"; audience = "admin"; primary = "Keep as internal note"; secondary = ["Ask admin about queue/submission history", "Reject / not needed"]
        question = "May BNL mention this queue or submission history publicly, and what exact wording is approved?"
    follow_up = None
    if actionability != "non_actionable_artifact" and lane != "admin_correction_conflict":
        follow_up = _scoped_follow_up_for_claim(subject, claim_text, claim_type, lane, actionability)
        if actionability != "weak_label" or follow_up.get("question") != "What proof or approved wording should BNL use before saying this publicly?":
            question = str(follow_up.get("question") or question)
            audience = str(follow_up.get("audience") or audience)
            why = str(follow_up.get("reason") or why)
    if _is_clarification_follow_up(follow_up, actionability):
        return {k: _display_clean(v) if isinstance(v, str) else v for k, v in _clarification_copy(subject, follow_up, question).items()}
    answer_type = str((follow_up or {}).get("answerType") or "plain-language approval, correction, or public-safe wording")
    confidence = str((follow_up or {}).get("confidence") or ("medium" if public_safe else ("low" if actionability in {"source_blind_warning", "weak_label"} else "medium")))
    base_display = {
        "displayTitle": title,
        "displayDecision": decision,
        "displayWhatIsBeingChecked": checked,
        "displayWhyBNLFlaggedIt": why,
        "displayBNLRecommendation": rec,
        "displayEvidenceSummary": evidence_summary,
        "displaySafeDefault": safe,
        "displayApprovalInstruction": approval,
        "confirmationTarget": target,
        "primaryActionLabel": primary,
        "secondaryActionLabels": secondary,
        "verificationPacketQuestion": question,
        "verificationPacketQuestions": [question] if question else [],
        "verificationPacketAudience": audience,
        "recommendedFollowUpQuestion": question,
        "recommendedFollowUpAudience": audience if audience in {"subject", "admin", "owner", "public_source"} else ("subject" if audience == "link_ownership" else ("admin" if audience == "none" else audience)),
        "recommendedFollowUpReason": why,
        "bestGuessConfidence": confidence,
        "suggestedConfirmationAudience": audience if audience in {"subject", "admin", "owner", "public_source"} else ("subject" if audience == "link_ownership" else "admin"),
        "suggestedConfirmationQuestion": question,
        "suggestedConfirmationReason": why,
        "suggestedConfirmationAnswerType": answer_type,
        "suggestedMissingInfoQuestion": question,
        "followUpCategory": (follow_up or {}).get("category", ""),
        "fallbackUsed": bool((follow_up or {}).get("fallbackUsed")),
        "notEnoughContextReason": (follow_up or {}).get("notEnoughContextReason", ""),
        "followUpScopeKnown": bool((follow_up or {}).get("scopeKnown")),
        "followUpScopeLabel": (follow_up or {}).get("scopeLabel", ""),
    }
    return {k: _display_clean(v) if isinstance(v, str) else v for k, v in base_display.items()}


def _dossier_question_id(section: str, audience: str, index: int) -> str:
    return f"dossier-readiness-{section}-{audience}-{index}"


def _dossier_section_for_claim_type(claim_type: str, text: str) -> str:
    low = (text or "").lower()
    if "orion" in low or "relay" in low:
        return "orion"
    if claim_type == "collaboration_interest" or "collaboration interest" in low:
        return "collaboration_interest"
    if claim_type == "collaboration_status" or "collaboration status" in low:
        return "collaboration_status"
    if claim_type == "show_operations" or "show operations" in low:
        return "show_operations"
    if "lore" in low or "context" in low and "contest" not in low:
        return "lore"
    if claim_type.startswith("contest_") or claim_type == "contest_event_activity" or "contest" in low or "event" in low:
        return "contest"
    if claim_type == "queue_submission_activity" or "queue/submission" in low:
        return "queue_submission"
    if claim_type in {"music_link", "public_link", "links_socials", "music_artist_context"} or re.search(r"\b(suno|music|song|track|link|social|url)\b", low):
        return "links" if "music" not in low and "suno" not in low else "music"
    if claim_type == "rules_instructions_context" or re.search(r"\b(rules?|instructions?)\b", low):
        return "rules"
    if re.search(r"\b(theory|theories|anomal(?:y|ies)|glitch|technical issue)\b", low):
        return "theory_anomaly"
    if claim_type in {"role", "community_context"} or re.search(r"\b(role|title|artist|collaborator|participant|moderator)\b", low):
        return "role"
    if claim_type == "relationship" or "relationship" in low:
        return "relationships"
    if "display name" in low or "identity" in low:
        return "identity"
    return "unknown"


def _readiness_question_for_section(subject: str, section: str) -> tuple[str, str, str, str, str]:
    if section == "identity":
        return ("subject", f"What exact public name may BNL use for {subject}?", "A public dossier needs an approved identity label before BNL writes summary copy.", "high", "needs_confirmation")
    if section == "role":
        return ("subject", f"What public role/title may BNL use for {subject}, if any?", "A public dossier needs a precise approved role instead of inferred labels.", "high", "needs_confirmation")
    if section in {"links", "music"}:
        return ("subject", f"Which links are {subject}’s and approved for BNL to mention publicly?", "BNL found possible link/music context but does not know ownership or approval.", "high", "needs_confirmation")
    if section == "show_operations":
        return ("admin", f"Is {subject} involved in show operations? If yes, what does he help with, and can BNL mention it publicly?", "BNL must confirm show-operations involvement instead of inferring a role from context.", "high", "needs_confirmation")
    if section == "collaboration_interest":
        return ("admin", f"Did {subject} only show interest in collaborating, or is there an actual public collaboration BNL can mention publicly?", "BNL must separate interest in collaborating from an approved public collaborator role or project reference.", "high", "needs_confirmation")
    if section == "collaboration_status":
        return ("admin", f"Can BNL mention this collaboration publicly? If yes, what exact project, song, show, or workflow should it reference?", "BNL needs exact approved collaboration context before public use.", "high", "needs_confirmation")
    if section == "contest":
        return ("subject", f"Was {subject} involved in this contest or event, or was he only mentioned nearby?", "BNL found contest/rules context but cannot safely infer the subject’s role, host, organizer, or event from proximity.", "high", "needs_confirmation")
    if section == "rules":
        return ("admin", f"BNL found unclear rules/instructions context near {subject}. Were these contest rules, queue instructions, Discord/admin instructions, broadcast instructions, or something else? What was {subject}'s role?", "BNL must know which instructions, from who, to who, and about what before creating any role or public claim.", "high", "needs_confirmation")
    if section == "orion":
        return ("admin", f"Can BNL mention Orion in {subject}’s public dossier, or should that stay internal?", "Orion/context references require an explicit public/internal boundary decision.", "high", "needs_confirmation")
    if section == "lore":
        return ("admin", f"Is this {subject}'s lore, BARCODE lore, Orion lore, Null TV lore, or a recurring topic? Should BNL use it publicly or keep it internal?", "BNL found lore-heavy context and needs a scoped dossier decision, not a generic source request.", "medium", "internal_only")
    if section == "theory_anomaly":
        return ("admin", f"BNL found theory/anomaly context near {subject}. Is this about {subject}, BARCODE lore, Orion, a recurring topic, or something unrelated?", "BNL needs to know what the theory/anomaly refers to before it can decide whether it belongs in the dossier.", "medium", "internal_only")
    if section == "queue_submission":
        return ("admin", f"May BNL mention {subject}'s queue or submission activity publicly, or should it stay internal?", "Queue/submission context needs a public boundary before dossier use.", "high", "needs_confirmation")
    if section == "boundaries":
        return ("subject", f"What should BNL absolutely avoid saying publicly about {subject}?", "A concrete public dossier needs explicit boundaries so BNL does not overstate private, internal, or unwanted context.", "medium", "needs_confirmation")
    if section == "summary":
        return ("subject", f"What short public summary would feel accurate for {subject}?", "BNL needs a human-approved summary direction before drafting beyond confirmed facts.", "medium", "needs_confirmation")
    if section == "relationships":
        return ("subject", f"What public relationship to BARCODE/BNL, if any, is approved for BNL to state for {subject}?", "A public dossier needs approved relationship wording and public boundaries.", "high", "needs_confirmation")
    return ("admin", f"What is still needed before BNL can write a complete public summary for {subject}?", "BNL has unresolved dossier-readiness context that does not fit a more specific section.", "low", "needs_confirmation")


def _build_dossier_readiness(subject: str, reviewable_claims: list[dict[str, Any]], missing: list[dict[str, Any]], blind_insights: list[str]) -> dict[str, Any]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    def add(section: str, claim_id: str = "", clarification_question: str = "") -> None:
        audience, question, why, priority, safety = _readiness_question_for_section(subject, section)
        if clarification_question:
            question = clarification_question
            why = "Clarification needed before BNL can draft this part of the dossier."
            safety = "needs_clarification"
            priority = "high"
        key = (section, audience)
        item = buckets.setdefault(key, {"audience": audience, "question": question, "whyItMatters": why, "dossierSection": section, "priority": priority, "relatedReviewClaimIds": [], "sourceSafety": safety})
        if claim_id and claim_id not in item["relatedReviewClaimIds"]:
            item["relatedReviewClaimIds"].append(claim_id)
    for m in missing:
        add(_dossier_section_for_claim_type(str(m.get("relatedClaimType") or ""), str(m.get("question") or "")))
    for idx, c in enumerate(reviewable_claims):
        section = _dossier_section_for_claim_type(str(c.get("dossierDimension") or c.get("claimType") or ""), " ".join(str(c.get(k) or "") for k in ("claimText", "displayTitle", "displayDecision")))
        if c.get("reviewLane") == "source_blind" and section == "unknown":
            continue
        clar_q = ""
        if c.get("decisionState") == "needs_clarification":
            clar_q = str(c.get("recommendedFollowUpQuestion") or c.get("verificationPacketQuestion") or "Clarification needed before BNL can draft this part of the dossier")
        add(section, f"reviewableClaims[{idx}]", clar_q)
        # Do not add a public-role readiness blocker for activity/context cards.
        # Role/title readiness belongs only to explicit public_role_title cards with
        # evidence-anchor support for role/title wording.
        anchor = c.get("evidenceAnchor") if isinstance(c.get("evidenceAnchor"), dict) else {}
        if (
            str(c.get("dossierDimension") or "") == "public_role_title"
            and anchor.get("anchorType") == "public_role_title"
            and anchor.get("hasAnchor")
        ):
            add("role", f"reviewableClaims[{idx}]")
    for txt in blind_insights:
        if "orion" in txt.lower() or "relay" in txt.lower():
            add("orion")
    if len(buckets) < 3:
        add("boundaries")
    if len(buckets) < 3:
        add("summary")
    priority_rank = {"high": 0, "medium": 1, "low": 2}
    questions = sorted(buckets.values(), key=lambda x: (priority_rank.get(x["priority"], 9), x["dossierSection"]) )[:7]
    for i, q in enumerate(questions, 1):
        q["id"] = _dossier_question_id(q["dossierSection"], q["audience"], i)
    blocked = [q["dossierSection"] for q in questions if q["priority"] == "high"]
    ready = not blocked
    return {"questions": questions, "summary": f"BNL has {len(questions)} prioritized dossier-readiness question(s); {len(blocked)} block a complete public draft.", "blockedBy": blocked, "readyForDraft": ready, "reason": "Ready for draft with current public-safe facts." if ready else "Resolve high-priority identity, role, link/music, relationship, contest, or boundary questions before generating a concrete public dossier."}


def _clarification_source_safety(claim: dict[str, Any]) -> str:
    if claim.get("reviewLane") == "source_blind" or claim.get("decisionState") == "source_blind_blocked" or claim.get("actionability") == "source_blind_warning":
        return "source_blind_blocked"
    if claim.get("publicSafe") is True:
        return "public_safe"
    return "internal_safe"


def _clarification_intent(question: str, category: str) -> str:
    q = re.sub(r"\b(?:crow|subject|bnl|publicly|public|internal|ignore|keep|mention|use|dossier|source file)\b", " ", (question or "").lower())
    if category in {"theory_anomaly", "rules_instructions", "contest_event", "lore_context", "source_blind"}:
        return category
    if "link" in q or "music" in q:
        return "links_music_socials"
    if "role" in q or "title" in q:
        return "role_title"
    return re.sub(r"[^a-z0-9]+", " ", q).strip()[:80] or category or "clarification"


def _is_clarification_claim(claim: dict[str, Any]) -> bool:
    return claim.get("decisionState") == "needs_clarification" or claim.get("actionability") == "needs_clarification" or claim.get("decisionState") == "source_blind_blocked" or claim.get("actionability") == "source_blind_warning"


def _meaningful_signal_summary(claim: dict[str, Any]) -> str:
    summary = _text(claim.get("safeEvidenceSummary") or claim.get("claimText") or claim.get("displayEvidenceSummary"), 240)
    if not summary:
        return ""
    vague = re.sub(r"\s+", " ", summary).strip().lower()
    if vague in {"possible signal only", "possible signal only.", "what is this referring to?", "unclear context"}:
        return ""
    if re.fullmatch(r"(possible\s+)?(signal|item|context|claim)(\s+only)?\.?", vague):
        return ""
    return summary


def _clarification_label(claim: dict[str, Any]) -> str:
    label = _text(claim.get("followUpScopeLabel") or claim.get("displayTitle") or claim.get("claimType") or "Unresolved signal", 90)
    text = _text(claim.get("claimText"), 120)
    if text and text.lower() not in label.lower() and len(text) <= 80:
        label = text
    return humanize_internal_label(label).strip() or "Unresolved signal"


def _signal_kind(label: str, summary: str, category: str) -> str:
    low = f"{label} {summary}".lower()
    if category == "theory_anomaly":
        if "archive" in low or "evidence" in low:
            return "archive/evidence wording suggesting something unusual"
        if "theory" in low or "pattern" in low or "interpret" in low:
            return "theory, interpretation, or pattern wording"
        return "theory/anomaly wording"
    if category == "rules_instructions":
        if "contest" in low:
            return "contest rules wording"
        if "queue" in low or "submission" in low:
            return "queue/submission instruction wording"
        if "admin" in low or "discord" in low or "mod" in low:
            return "Discord/admin instruction wording"
        return "rules/instructions wording"
    return f"{category.replace('_', '/')} wording" if category else "unclear wording"


def _clarification_signal(claim: dict[str, Any], category: str, safety: str) -> dict[str, Any]:
    label = _clarification_label(claim)
    summary = _meaningful_signal_summary(claim)
    if safety == "source_blind_blocked":
        label = "Source-blind/internal context withheld"
        summary = "Source-blind context exists, but raw details were withheld."
    kind = _signal_kind(label, summary, category)
    signal = {
        "label": label,
        "claimType": _text(claim.get("claimType") or "missing_confirmation", 80),
        "summary": summary,
        "whatBnlKnows": f"BNL saw {kind}, but has not identified enough context to turn it into a dossier fact.",
        "whatBnlDoesNotKnow": _text(claim.get("recommendedFollowUpReason") or claim.get("cannotSuggestPublicReason") or "BNL does not know the exact scope, subject role, or whether it belongs in the dossier.", 240),
        "whyThisMayBeRelated": f"It asks the same broad {category.replace('_', '/')} clarification question." if category else "It asks the same broad clarification question.",
        "differenceFromOtherSignals": f"This signal is labeled/summarized as {label}: {kind}.",
    }
    if safety != "source_blind_blocked":
        excerpt = _safe_evidence_example(summary or str(claim.get("claimText") or ""), "")
        if excerpt and not re.search(r"\b(private|source[-_\s]*blind|internal[-\s]*only|dm|raw id|token)\b", excerpt, re.I):
            signal["safeExcerpt"] = _text(excerpt, 220)
    return signal


def _cluster_clarification_needs(subject: str, reviewable_claims: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    groups: dict[tuple[Any, ...], list[tuple[int, dict[str, Any]]]] = {}
    audit: list[str] = []
    passthrough: list[dict[str, Any]] = []
    for idx, claim in enumerate(reviewable_claims):
        if not _is_clarification_claim(claim):
            passthrough.append(claim)
            continue
        category = str(claim.get("followUpCategory") or _infer_follow_up_category(subject, str(claim.get("claimText") or ""), str(claim.get("claimType") or ""), str(claim.get("reviewLane") or ""), str(claim.get("actionability") or "")))
        question = str(claim.get("recommendedFollowUpQuestion") or claim.get("suggestedConfirmationQuestion") or claim.get("verificationPacketQuestion") or "")
        summary = _meaningful_signal_summary(claim)
        if not question or not summary or (claim.get("fallbackUsed") and question == GENERIC_FOLLOW_UP_FALLBACK):
            audit.append(f"BNL found vague {category.replace('_', '/')} wording, but there is not enough context to review or use it.")
            continue
        safety = _clarification_source_safety(claim)
        section = _dossier_section_for_claim_type(str(claim.get("claimType") or ""), " ".join(str(claim.get(k) or "") for k in ("claimText", "displayTitle", "displayDecision", "recommendedFollowUpQuestion")))
        if safety == "source_blind_blocked":
            section = "source_blind"
        if category == "theory_anomaly":
            section = "theory_anomaly"
        elif category == "rules_instructions":
            section = "rules"
        key = (
            category,
            section,
            str(claim.get("suggestedConfirmationAudience") or claim.get("recommendedFollowUpAudience") or "admin"),
            str(claim.get("suggestedConfirmationAnswerType") or ""),
            _clarification_intent(question, category),
            safety,
            bool(claim.get("followUpScopeKnown")),
        )
        groups.setdefault(key, []).append((idx, claim))

    needs: list[dict[str, Any]] = []
    for n, (key, entries) in enumerate(groups.items(), 1):
        category, section, audience, answer_type, _intent, safety, _scope_known = key
        labels = [_clarification_label(c) for _, c in entries]
        summaries = [_meaningful_signal_summary(c).lower() for _, c in entries]
        label_norms = {re.sub(r"[^a-z0-9]+", " ", x.lower()).strip() for x in labels}
        summary_norms = {re.sub(r"[^a-z0-9]+", " ", x).strip() for x in summaries}
        relationship = "duplicate" if len(label_norms) <= 1 and len(summary_norms) <= 1 else ("distinct" if len(entries) == 1 else "related")
        title_topic = category.replace("_", "/")
        if category == "theory_anomaly":
            title = f"Clarify anomaly/theory context for {subject}"
        elif category == "rules_instructions":
            title = f"Clarify rules/instructions context for {subject}"
        elif safety == "source_blind_blocked":
            title = f"Clarify source-blind replacement wording for {subject}"
        else:
            title = f"Clarify {title_topic} context for {subject}"
        base_question = str(entries[0][1].get("recommendedFollowUpQuestion") or entries[0][1].get("suggestedConfirmationQuestion") or "")
        if relationship == "related":
            joined = " and ".join(labels[:2]) if len(labels) == 2 else ", ".join(labels[:3])
            question = f"Are these signals referring to the same thing? If yes, what {title_topic} context are they about? If not, what does each signal refer to, and should either belong in {subject}'s public dossier? Signals: {joined}."
        elif relationship == "duplicate":
            question = base_question
        else:
            question = base_question
        reason = (f"BNL found {len(entries)} {relationship} {title_topic} signal(s). "
                  f"{'They may refer to the same unresolved context, but labels or summaries differ, so BNL needs clarification before treating them as one dossier fact.' if relationship == 'related' else 'BNL cannot decide whether this belongs in the dossier until the missing context is identified.'}")
        signals = [_clarification_signal(c, str(category), str(safety)) for _, c in entries]
        needs.append({
            "id": f"dossier-clarification-{section}-{n}",
            "title": title,
            "question": question,
            "audience": audience,
            "reason": reason,
            "answerType": answer_type or "clarification of what this signal refers to and whether it belongs in the dossier",
            "dossierSection": section,
            "priority": "medium" if safety == "source_blind_blocked" else "high",
            "sourceCount": len(entries),
            "relatedReviewClaimIds": [f"reviewableClaims[{idx}]" for idx, _ in entries],
            "relationshipType": relationship,
            "contributingSignals": signals,
            "sourceSafety": safety,
            "canBecomeReviewCardAfterAnswer": True,
        })
        card = dict(entries[0][1])
        if safety == "source_blind_blocked" and len(entries) == 1:
            passthrough.append(card)
            continue
        card.update({
            "claimText": title,
            "displayTitle": title,
            "displayDecision": "Clarification question only — not a public claim approval",
            "displayWhatIsBeingChecked": "This is a question BNL needs answered before it can make a dossier fact or Source File review item.",
            "displayBNLRecommendation": "Answering may create later reviewable Source File facts; rejecting means BNL should ignore or suppress the vague signal.",
            "recommendedFollowUpQuestion": question,
            "suggestedConfirmationQuestion": question,
            "verificationPacketQuestion": question,
            "verificationPacketQuestions": [question],
            "clarificationNeedId": needs[-1]["id"],
            "clarificationRelationshipType": relationship,
            "relatedReviewClaimIds": needs[-1]["relatedReviewClaimIds"],
        })
        passthrough.append(card)
    return passthrough[:14], needs, audit


def _build_dossier_readiness_from_clarifications(readiness: dict[str, Any], needs: list[dict[str, Any]]) -> dict[str, Any]:
    existing = [q for q in readiness.get("questions", []) if isinstance(q, dict)]
    clar_questions: list[dict[str, Any]] = []
    for need in needs[:7]:
        used = f" Used by {need.get('sourceCount', 1)} {need.get('relationshipType', 'distinct')} signal{'s' if int(need.get('sourceCount') or 1) != 1 else ''}."
        clar_questions.append({
            "id": f"readiness-{need.get('id')}",
            "audience": need.get("audience") or "admin",
            "question": f"{need.get('title')}: {need.get('question')}{used}",
            "whyItMatters": need.get("reason") or "Clarification needed before BNL can draft this part of the dossier.",
            "dossierSection": need.get("dossierSection") or "unknown",
            "priority": need.get("priority") or "high",
            "relatedReviewClaimIds": need.get("relatedReviewClaimIds") or [],
            "sourceSafety": need.get("sourceSafety") or "needs_clarification",
            "relationshipType": need.get("relationshipType") or "distinct",
            "sourceCount": need.get("sourceCount") or 1,
        })
    seen_sections = {q.get("dossierSection") for q in clar_questions}
    merged = clar_questions + [q for q in existing if q.get("dossierSection") not in seen_sections]
    priority_rank = {"high": 0, "medium": 1, "low": 2}
    merged = sorted(merged, key=lambda x: (priority_rank.get(x.get("priority"), 9), str(x.get("dossierSection") or "")))[:7]
    blocked = [q["dossierSection"] for q in merged if q.get("priority") == "high"]
    out = dict(readiness)
    out.update({
        "questions": merged,
        "blockedBy": blocked,
        "readyForDraft": not blocked,
        "summary": f"BNL has {len(merged)} prioritized dossier-readiness question(s), including {len(clar_questions)} consolidated clarification need(s); {len(blocked)} block a complete public draft.",
        "reason": "Ready for draft with current public-safe facts." if not blocked else "Resolve high-priority consolidated clarification, identity, role, link/music, relationship, contest, or boundary questions before generating a concrete public dossier.",
    })
    return out


def _admin_action_card(action: str) -> dict[str, Any]:
    raw = str(action or "")
    title = "Admin follow-up task"
    display = _display_clean(raw)
    explanation = "This is an admin workflow task, not a public claim approval."
    if "sourceFileReviewClaims" in raw:
        display = "Review each BNL claim separately."
        explanation = "Do not approve a whole group of claims at once. Each claim needs its own approve, edit, reject, or confirmation decision."
    elif "Promote only confirmed" in raw:
        display = "Only say what we know is true."
        explanation = "BNL should not use guesses in public text. Approve the fact first, then BNL can use it."
    elif "Attach owner-approved provenance" in raw:
        display = "Get proof or approval first."
        explanation = "If the claim involves someone’s role, links, music, queue history, Orion/context, or relationships, BNL needs a public source or owner-approved wording before using it publicly."
    follow_up = _universal_follow_up_for_claim("the subject", raw, "admin_task", "admin_follow_up", "actionable_claim")
    return {
        "claimText": raw,
        "claimType": "admin_task",
        "reviewLane": "admin_follow_up",
        "displayTitle": title,
        "displayDecision": "What follow-up should happen before this Source File is used for public copy?",
        "displayWhatIsBeingChecked": "This is an admin workflow task, not a public claim approval.",
        "displayWhyBNLFlaggedIt": explanation,
        "displayBNLRecommendation": display,
        "displayEvidenceSummary": explanation,
        "displaySafeDefault": "Do not use this task as public copy.",
        "displayApprovalInstruction": "Complete this follow-up before approving related public wording.",
        "confirmationTarget": "admin",
        "primaryActionLabel": "Add to admin follow-up",
        "secondaryActionLabels": ["Reject / not needed"],
        "verificationPacketQuestion": follow_up["question"],
        "verificationPacketQuestions": [follow_up["question"]],
        "verificationPacketAudience": follow_up["audience"],
        "recommendedFollowUpQuestion": follow_up["question"],
        "recommendedFollowUpAudience": follow_up["audience"],
        "recommendedFollowUpReason": follow_up["reason"],
        "bestGuessConfidence": follow_up["confidence"],
        "suggestedConfirmationQuestion": follow_up["question"],
        "suggestedConfirmationAudience": follow_up["audience"],
        "suggestedConfirmationReason": follow_up["reason"],
        "suggestedConfirmationAnswerType": follow_up["answerType"],
        "suggestedMissingInfoQuestion": follow_up["question"],
        "followUpCategory": follow_up["category"],
        "fallbackUsed": follow_up["fallbackUsed"],
        "notEnoughContextReason": follow_up["notEnoughContextReason"],
        "followUpScopeKnown": follow_up["scopeKnown"],
        "followUpScopeLabel": follow_up["scopeLabel"],
    }


def _claim_prefix(claim_type: str) -> str:
    return {
        "role": "Possible role claim",
        "relationship": "Possible relationship/context claim",
        "music_link": "Possible music/link claim",
        "contest_music_context": "Possible public music/link context",
        "contest_submitter": "Possible contest submitter context",
        "contest_rules_poster": "Possible contest rules context",
        "contest_host": "Possible contest host claim",
        "contest_organizer": "Possible contest organizer claim",
        "rules_instructions_context": "Possible rules/instructions context",
        "public_link": "Possible public-link claim",
        "queue_submission": "Possible queue/submission claim",
        "community_context": "Possible community/context claim",
        "source_blind_context": "Source-blind context claim",
        "collaboration_interest": "Possible collaboration-interest context",
        "contest_event_activity": "Possible contest/event activity context",
        "queue_submission_activity": "Possible queue/submission activity context",
        "lore_context": "Possible lore/context",
        "theory_anomaly_context": "Possible theory/anomaly context",
        "rules_instruction_context": "Possible rules/instructions context",
        "systems_context": "Possible systems context",
    }.get(claim_type, "Possible review claim")


def _claim_actionability(claim_text: str, claim_type: str, lane: str, public_safe: bool) -> str:
    low = (claim_text or "").lower()
    if "subject-owned/keyed local evidence exists" in low or "local/keyed evidence" in low:
        return "non_actionable_artifact"
    if "lacked public-safe provenance" in low or "source-blind" in low or lane == "source_blind":
        return "source_blind_warning"
    if claim_text.strip().lower() in {"contest organizer", "organizer", "participant", "artist", "member"} or "contest organizer" in low:
        return "weak_label"
    if claim_type in {"contest_music_context", "contest_submitter", "contest_rules_poster", "contest_public_link_context", "contest_channel_context", "contest_date_context", "contest_mention_only", "rules_instructions_context"}:
        return "actionable_claim"
    if "?" in claim_text or "confirm" in low or "can bnl" in low:
        return "missing_confirmation"
    if public_safe:
        return "actionable_claim"
    if claim_type in {"relationship", "music_link", "public_link", "queue_submission", "role", "community_context"}:
        return "actionable_claim"
    return "evidence_signal"


def _suggested_public_wording(subject: str, claim_text: str, claim_type: str, public_safe: bool) -> str:
    if not public_safe:
        return ""
    text = _sanitize_public(claim_text, subject, [])
    low = text.lower()
    if not text:
        return ""
    if claim_type == "identity":
        return subject
    if claim_type in {"role", "community_context"}:
        if "radio" in low:
            return f"{subject} is a BARCODE Radio viewer/community participant."
        if re.search(r"\b(member|community|barcode|bnl|participant)\b", low):
            return f"{subject} is a BARCODE Network community member."
    if claim_type in {"music_link", "public_link"} and _LINK_RE.search(text):
        return f"{subject} has an approved public music link: {_LINK_RE.search(text).group(0)}."
    if claim_type == "relationship" and "orion" in low:
        return f"{subject} has recurring Orion-related BARCODE lore/context."
    if claim_type == "queue_submission":
        return f"{subject} has submitted music to BARCODE Radio."
    return text if not re.search(r"\b(may|possible|unconfirmed|needs confirmation|source-blind|internal)\b", low) else ""



_RAW_PUBLIC_WORDING_LABELS = {"contest/event activity", "show operations", "collaboration", "collaboration interest", "queue/submission history", "link ownership", "lore/context", "theory/anomaly", "rules/instructions"}

def _clean_suggested_public_wording(value: str) -> str:
    text = _text(value, 500).strip()
    if not text or text.lower() in _RAW_PUBLIC_WORDING_LABELS:
        return ""
    if not re.search(r"[.!?]$", text) and len(text.split()) <= 4:
        return ""
    return text

def _dossier_dimension_for_claim(subject: str, claim_text: str, claim_type: str, lane: str, public_safe: bool, role_read: dict[str, Any] | None = None) -> str:
    low = f"{claim_type} {lane} {claim_text}".lower()
    role_read = role_read or _public_role_candidate_for_subject(subject, claim_text, {"claimType": claim_type, "lane": lane})
    activity = str(role_read.get("activityContextType") or _activity_context_type_for_text(claim_text, claim_type, lane))
    if lane == "source_blind" or "source_blind" in low or "source-blind" in low or "lacked public-safe provenance" in low:
        return "source_protected_context"
    if re.search(r"\b(show operations?|show[- ]ops|show[- ]operations?|broadcast operations?|run of show|show workflow|show production)\b", low) or activity == "show_operations":
        return "show_operations"
    if _CONFIRMED_COLLAB_RE.search(low) or activity == "collaboration_status":
        return "collaboration_status"
    if _COLLAB_INTEREST_RE.search(low) or activity == "collaboration_interest":
        return "collaboration_interest"
    if claim_type.startswith("contest_") or activity == "contest_event_activity":
        return "contest_event_activity"
    if activity == "rules_instruction_context" or claim_type == "rules_instructions_context":
        return "rules_instruction_context"
    if claim_type == "queue_submission" or activity == "queue_submission_activity":
        return "queue_submission_activity"
    if claim_type in {"music_link"} or activity == "music_discussion":
        return "music_artist_context"
    if claim_type == "public_link" or activity == "link_reference":
        return "links_socials"
    if claim_type == "identity" or re.search(r"\b(display name|public name|identity|alias)\b", low):
        return "public_identity"
    if activity == "systems_context":
        return "systems_context"
    if activity == "lore_context" or "lore" in low:
        return "lore_context"
    if activity == "theory_anomaly_context" or re.search(r"\b(theory|anomal|glitch)\b", low):
        return "theory_anomaly_context"
    if claim_type == "relationship" and not re.search(r"\b(orion|lore|theory|anomal|system)\b", low):
        return "relationship_affiliation"
    if claim_type == "do_not_say" or re.search(r"\b(do not say|never state|boundary|avoid saying)\b", low):
        return "public_boundary"
    if role_read.get("isPublicRole") or role_read.get("roleCandidate") or claim_type == "role":
        return "public_role_title"
    if activity == "community_context" or claim_type == "community_context":
        return "community_presence"
    return "unknown_context"

def _card_intent_for_dimension(dimension: str, public_safe: bool) -> str:
    if dimension == "source_protected_context": return "attach_public_source"
    if dimension == "show_operations": return "confirm_yes_no"
    if dimension in {"collaboration_interest", "contest_event_activity", "rules_instruction_context", "lore_context", "theory_anomaly_context", "systems_context", "unknown_context"}: return "classify_context" if dimension != "unknown_context" else "clarify_missing_context"
    if dimension in {"public_role_title", "public_identity", "links_socials", "music_artist_context", "collaboration_status"}: return "choose_public_wording" if not public_safe else "approve_public_fact"
    if dimension == "public_boundary": return "set_public_boundary"
    return "keep_internal_or_public"

def _actions(*pairs: tuple[str, str]) -> list[dict[str, str]]:
    return [{"action": a, "label": b} for a, b in pairs]

def _public_approval_actions() -> list[dict[str, str]]:
    return _actions(("approve_public_wording", "Approve public wording"), ("keep_internal", "Keep internal"), ("ask_follow_up", "Ask follow-up"), ("reject", "Reject / not useful"))

def _dimension_template(subject: str, dimension: str) -> dict[str, Any]:
    common = {
        "recommendedFollowUpAudience": "admin", "recommendedFollowUpReason": "BNL needs a human decision before using this in public copy.",
        "exampleApprovedText": "", "suggestedPublicWording": "", "suggestedInternalNote": f"BNL found {dimension.replace('_',' ')} for {subject}; keep internal until reviewed.",
    }
    templates = {
        "show_operations": dict(displayTitle="Confirm show-operations involvement", displayDecision=f"Is {subject} involved in show operations?", displayWhatIsBeingChecked=f"BNL found show-operations context, but it does not know whether {subject} is actually involved.", displayWhyItExists=f"Show-operations involvement could matter for {subject}’s Source File or dossier, but BNL should not infer it from weak context.", displayBNLRecommendation="BNL recommends asking a direct yes/no question before using this publicly.", displaySafetyDefault="Do not mention show-operations involvement publicly unless confirmed.", displayApprovalInstruction=f"Confirm involvement only if {subject} actually helps with show operations and the wording is safe to use.", displayWhatYouAreApproving="A show-operations involvement fact, not a general role/title.", recommendedFollowUpQuestion=f"Is {subject} involved in show operations? If yes, what does he help with, and can BNL mention it publicly?", suggestedConfirmationAnswerType="yes/no involvement, what he helps with, and whether it can be public", allowedReviewActions=_actions(("confirm_show_operations","Yes — involved in show operations"),("not_involved","No — not involved"),("keep_internal","Keep internal"),("ask_follow_up","Ask follow-up"),("reject","Reject / not useful"))),
        "contest_event_activity": dict(displayTitle="Clarify contest/event activity", displayDecision=f"Was {subject} involved in this contest or event?", displayWhatIsBeingChecked=f"BNL found contest/event activity near {subject}, but this does not prove a public role or title.", displayWhyItExists="Contest/event context can matter, but proximity is not proof of participation, hosting, organizing, or public role.", displayBNLRecommendation="BNL recommends clarifying the exact relationship before using this publicly.", displaySafetyDefault="Do not turn contest/event activity into a role/title unless confirmed.", displayApprovalInstruction="Approve only the exact event relationship and public-safe wording.", displayWhatYouAreApproving="Contest/event involvement or mention-only status, not a general role/title.", recommendedFollowUpQuestion=f"Was {subject} involved in this contest or event? If yes, was he a participant, submitter, host, organizer, rules poster, link source, or just mentioned nearby?", suggestedConfirmationAnswerType="event name, subject relationship to the event, and whether it can be public", allowedReviewActions=_actions(("confirm_event_involvement","Confirm event involvement"),("mentioned_only","Mentioned only"),("keep_internal","Keep internal"),("ask_follow_up","Ask follow-up"),("reject","Reject / not useful"))),
        "collaboration_interest": dict(displayTitle="Clarify collaboration interest", displayDecision=f"Did {subject} only show interest in collaborating, or is there an actual public collaboration BNL can mention?", displayWhatIsBeingChecked="BNL found possible collaboration interest, but interest is not the same as an approved public collaboration.", displayWhyItExists="Collaboration interest should not become collaborator status without confirmation.", displayBNLRecommendation="BNL recommends separating interest-only context from confirmed public collaboration.", displaySafetyDefault="Do not call this an actual collaboration unless confirmed and approved for public use.", displayApprovalInstruction="Classify interest-only vs actual collaboration before approving wording.", displayWhatYouAreApproving="A collaboration-interest classification or confirmed collaboration path, not a role/title.", recommendedFollowUpQuestion=f"Did {subject} only show interest in collaborating, or is there an actual public collaboration BNL can mention publicly? If public, is it with BARCODE or another project, song, show, or context?", suggestedConfirmationAnswerType="interest-only vs actual collaboration, project/context, and public permission", allowedReviewActions=_actions(("interest_only","Interest only"),("confirm_actual_collaboration","Confirm actual collaboration"),("keep_internal","Keep internal"),("ask_subject","Ask subject"),("reject","Reject / not useful"))),
        "collaboration_status": dict(displayTitle="Confirm public collaboration", displayDecision="Can BNL mention this collaboration publicly?", displayWhatIsBeingChecked="BNL found possible collaboration status and needs exact public-safe project/context wording.", displayWhyItExists="Actual collaboration can be dossier-relevant only when public permission and context are clear.", displayBNLRecommendation="BNL recommends confirming the exact project/context before public use.", displaySafetyDefault="Do not mention collaboration publicly without approved wording.", displayApprovalInstruction="Approve only the confirmed public collaboration wording.", displayWhatYouAreApproving="A public collaboration fact with exact project/context.", recommendedFollowUpQuestion="Can BNL mention this collaboration publicly? If yes, what exact project, song, show, or workflow should it reference?", suggestedConfirmationAnswerType="confirmed collaboration, exact project/context, and public wording", allowedReviewActions=_actions(("confirm_public_collaboration","Confirm public collaboration"),("needs_project_context","Ask for exact project/context"),("keep_internal","Keep internal"),("reject","Reject / not useful"))),
        "source_protected_context": dict(displayTitle=f"Need approved public wording for {subject}", displayDecision="Should this stay internal, or is there approved public wording BNL can use?", displayWhatIsBeingChecked="BNL has protected context it cannot show or quote here. It needs a separate public source or owner-approved wording before anything can be used publicly.", displayWhyItExists="BNL may know something internally, but protected context cannot become public dossier copy by itself.", displayBNLRecommendation="Keep this internal unless an owner/admin provides public-safe wording or a public source.", displaySafetyDefault="Keep internal. Do not use protected context publicly.", displayApprovalInstruction="Only approve separately confirmed wording. Do not approve the protected context itself.", displayWhatYouAreApproving="A separately approved public wording or public source, not the protected context.", evidenceSummary="Protected context exists, but details are withheld from this card.", suggestedInternalNote=f"BNL has protected context for {subject}; keep internal unless approved public wording or a public source is provided.", recommendedFollowUpQuestion="Should this protected context stay internal, or is there approved public wording BNL can use?", suggestedConfirmationAnswerType="keep internal, approved public wording, or public source", allowedReviewActions=_actions(("keep_internal","Keep internal"),("add_approved_public_wording","Add approved public wording"),("attach_public_source","Attach public source"),("ask_owner_admin","Ask owner/admin"),("reject","Reject / not useful"))),
    }
    generic = {
        "public_identity": ("Confirm public identity", f"What exact public name may BNL use for {subject}?", "approved public identity/name"),
        "public_role_title": ("Confirm public role", f"What public role/title may BNL use for {subject}, if any?", "approved public role/title"),
        "community_presence": ("Clarify community presence", f"How should BNL describe {subject}'s community presence, if at all?", "community presence wording"),
        "queue_submission_activity": ("Confirm queue/submission activity", f"May BNL mention {subject}'s queue or submission activity publicly?", "queue/submission public boundary"),
        "music_artist_context": ("Confirm music/artist context", f"What music or artist context may BNL mention for {subject}?", "music/artist public wording"),
        "links_socials": ("Confirm approved links/socials", f"Which links or socials may BNL publicly associate with {subject}?", "approved links/socials"),
        "relationship_affiliation": ("Clarify relationship/affiliation", f"What relationship or affiliation may BNL state for {subject}, if any?", "relationship/affiliation wording"),
        "systems_context": ("Clarify systems context", f"What systems context is this, and does it involve {subject} publicly?", "systems-context classification"),
        "lore_context": ("Clarify lore/context scope", f"Is this {subject}'s lore, BARCODE lore, Orion lore, Null TV lore, or a recurring topic? Should BNL use it publicly or keep it internal?", "lore/context classification"),
        "theory_anomaly_context": ("Clarify theory/anomaly context", f"What theory or anomaly is this referring to, and does it belong in {subject}'s dossier?", "theory/anomaly classification"),
        "rules_instruction_context": ("Clarify rules/instructions context", f"What rules or instructions is this referring to, who were they for, and what was {subject}'s actual role with them?", "instruction source, audience, subject role, and public boundary"),
        "public_boundary": ("Set public boundary", f"What should BNL avoid saying publicly about {subject}?", "public boundary decision"),
        "unknown_context": ("Needs clarification before review", f"What is this context about, and is it useful for {subject}'s Source File?", "context classification before public use"),
    }
    if dimension in templates:
        out = {**common, **templates[dimension]}
    else:
        title, question, ans = generic.get(dimension, generic["unknown_context"])
        out = {**common, "displayTitle": title, "displayDecision": question, "displayWhatIsBeingChecked": "BNL needs a concrete safe evidence anchor before this can become a normal Source File review card.", "displayWhyItExists": "BNL should classify the dossier dimension from evidence context and suppress raw-label-only cards.", "displayBNLRecommendation": "BNL recommends asking for the missing context before approving public copy.", "displaySafetyDefault": "Keep internal until confirmed and approved for public use.", "displayApprovalInstruction": "Approve only exact public-safe wording or choose keep internal/reject.", "displayWhatYouAreApproving": ans, "recommendedFollowUpQuestion": question, "suggestedConfirmationAnswerType": ans, "allowedReviewActions": _public_approval_actions()}
    out.setdefault("evidenceSummary", "BNL found context that needs review; no public wording is ready yet.")
    out.setdefault("recommendedFollowUpReason", common["recommendedFollowUpReason"])
    return out

def _apply_dimension_card_copy(guidance: dict[str, Any], dimension_copy: dict[str, Any], dimension: str) -> None:
    if guidance.get("decisionState") == "needs_clarification" or guidance.get("actionability") == "needs_clarification":
        guidance.setdefault("allowedReviewActions", _actions(("ask_follow_up", "Ask follow-up"), ("keep_internal", "Keep internal"), ("reject", "Reject / not useful")))
    else:
        guidance.setdefault("allowedReviewActions", dimension_copy.get("allowedReviewActions", []))
    guidance.setdefault("cardIntent", _card_intent_for_dimension(dimension, bool(guidance.get("publicSafe"))))
    for key in ("displayWhyItExists", "displaySafetyDefault", "displayWhatYouAreApproving", "exampleApprovedText", "evidenceSummary"):
        if key in dimension_copy:
            guidance.setdefault(key, dimension_copy[key])
    non_role_dimensions = {"show_operations", "contest_event_activity", "collaboration_interest", "collaboration_status", "source_protected_context"}
    if guidance.get("actionability") != "weak_label" and (dimension in non_role_dimensions or guidance.get("displayTitle") == "Public role/title not confirmed"):
        for key, value in dimension_copy.items():
            if key == "suggestedPublicWording" and guidance.get("suggestedPublicWording"):
                continue
            guidance[key] = value
    if guidance.get("recommendedFollowUpQuestion"):
        guidance["suggestedConfirmationReason"] = guidance.get("recommendedFollowUpReason", guidance.get("suggestedConfirmationReason", ""))
        guidance["suggestedConfirmationQuestion"] = guidance["recommendedFollowUpQuestion"]
        guidance["suggestedMissingInfoQuestion"] = guidance["recommendedFollowUpQuestion"]
        guidance["verificationPacketQuestion"] = guidance["recommendedFollowUpQuestion"]
        guidance["verificationPacketQuestions"] = [guidance["recommendedFollowUpQuestion"]]
    guidance["displayWhyBNLFlaggedIt"] = guidance.get("displayWhyItExists", guidance.get("displayWhyBNLFlaggedIt", ""))
    guidance["displaySafeDefault"] = guidance.get("displaySafetyDefault", guidance.get("displaySafeDefault", ""))

_RAW_REVIEW_LABELS = {
    "contest/event activity", "contests/events", "show operations", "collaboration",
    "source_protected_context", "contest event activity", "unknown context",
}

def _is_raw_review_label(text: str) -> bool:
    clean = re.sub(r"[_\s]+", " ", (text or "").strip().lower())
    return clean in _RAW_REVIEW_LABELS or clean in {x.replace("/", " ") for x in _RAW_REVIEW_LABELS}

_EVIDENCE_ANCHOR_LABELS = {
    "contest_event_activity": ("contest_rules_link_context", "BNL found contest/rules/link context near {subject}, but no event name or subject role is confirmed."),
    "show_operations": ("show_operations_wording", "BNL found show-operations wording near {subject}, but no confirmed task or responsibility."),
    "collaboration_interest": ("collaboration_interest_wording", "BNL found collaboration-interest wording near {subject}, but no confirmed project or public collaboration."),
    "collaboration_status": ("collaboration_status_wording", "BNL found collaboration-status wording near {subject}, but the exact public project/context still needs approval."),
    "links_socials": ("public_link_music_context", "BNL found public link/music context near {subject}, but ownership and public-use approval are missing."),
    "music_artist_context": ("public_link_music_context", "BNL found public link/music context near {subject}, but ownership and public-use approval are missing."),
    "source_protected_context": ("protected_context", "BNL found protected context, but details cannot be shown; needs public wording or source."),
}

def _evidence_anchor_for_claim(subject: str, claim: dict[str, Any], resolved_memory: dict[str, Any] | None = None, packet: dict[str, Any] | None = None) -> dict[str, Any]:
    dimension = str(claim.get("dossierDimension") or claim.get("claimType") or "unknown_context")
    lane = str(claim.get("reviewLane") or "")
    raw = " ".join(str(claim.get(k) or "") for k in ("safeEvidenceSummary", "claimText", "why", "displayEvidenceSummary"))
    protected = lane == "source_blind" or dimension == "source_protected_context" or (claim.get("publicSafe") is not True and re.search(r"source[-_ ]blind|protected|withheld", raw, re.I))
    if protected:
        dimension = "source_protected_context"
    safe = _text(_redact_review_evidence_summary(raw, subject), 260)
    has_specific_text = bool(safe and not _is_raw_review_label(safe) and not re.search(r"BNL found (?:a Source File signal|dossier-relevant context|unknown context)", safe, re.I) and len(safe.split()) >= 6)
    anchor_type, fallback = _EVIDENCE_ANCHOR_LABELS.get(dimension, (dimension, ""))
    if protected:
        safe_text = _EVIDENCE_ANCHOR_LABELS["source_protected_context"][1]
        has_anchor = True
    elif dimension in _EVIDENCE_ANCHOR_LABELS:
        safe_text = fallback.format(subject=subject)
        has_anchor = has_specific_text or bool(re.search(r"contest|event|rules|show[- ]operations?|collaborat|link|music|song|social", raw, re.I))
    elif has_specific_text and dimension == "public_role_title" and re.search(r"\b(role|title|moderator|staff|artist|musician|producer|dj)\b", raw, re.I):
        safe_text = safe
        anchor_type = "public_role_title"
        has_anchor = True
    else:
        safe_text = safe if has_specific_text else ""
        has_anchor = has_specific_text and not _is_raw_review_label(dimension)
    if (_is_raw_review_label(str(claim.get("claimText") or "")) or _is_raw_review_label(str(claim.get("safeEvidenceSummary") or ""))) and not protected:
        has_anchor = False
        safe_text = ""
    if not has_anchor:
        answerability = "not_answerable"
        reason = "Only a raw label or generic Source File category is available."
    elif claim.get("publicSafe") is True:
        answerability = "answerable"
        reason = "Public-safe evidence supports an approval decision."
    elif dimension == "show_operations" and re.search(r"confirmed|yes|involved|helps with|responsib", safe, re.I):
        answerability = "answerable"
        reason = ""
    else:
        answerability = "partially_answerable"
        reason = "Safe context exists, but the exact public/internal decision still needs confirmation."
    if dimension == "contest_event_activity":
        not_know = f"BNL does not know the exact event relationship for {subject}."; cannot = "Do not infer host, organizer, participant, or public role/title from contest/event proximity."
    elif dimension == "show_operations":
        not_know = f"BNL does not know whether {subject} helps with operations, was discussed near that topic, or was only mentioned in planning context."; cannot = "Do not infer a show-operations role/title without explicit confirmation."
    elif dimension in {"collaboration_interest", "collaboration_status"}:
        not_know = "BNL does not know whether this is interest-only, planning, or an approved public collaboration."; cannot = "Do not infer an actual collaboration, project, song, BARCODE collaboration, or public collaborator status."
    elif dimension == "source_protected_context":
        not_know = "BNL does not have public replacement wording or a separate public source."; cannot = "Do not expose protected details or turn source-blind context into public copy."
    else:
        not_know = "BNL does not know the approved public wording or whether this belongs in the dossier."; cannot = "Do not infer a public fact from this signal alone."
    return {
        "hasAnchor": has_anchor, "anchorType": anchor_type if has_anchor else "none", "anchorLabel": anchor_type.replace("_", " ") if has_anchor else "No safe evidence anchor",
        "safeAnchorText": safe_text, "sourceLane": lane or "review_only", "sourceSafety": "source_protected" if protected else ("public_safe" if claim.get("publicSafe") else "review_only"),
        "whatBnlKnows": safe_text if has_anchor else "BNL only has a raw label or generic category.", "whatBnlDoesNotKnow": not_know, "whatCannotBeInferred": cannot,
        "answerable": answerability, "notAnswerableReason": reason,
    }

def _safe_review_context_note(note: Any, subject: str) -> str:
    raw = str(note or "")
    if not raw.strip():
        return ""
    if re.search(r"\b(source[-_ ]blind|protected|private|internal[- ]only|dm|token|raw id|customer|payment|stripe|secret)\b", raw, re.I):
        return "Protected internal note exists, but details are withheld from this card."
    return _text(_redact_review_evidence_summary(raw, subject), 180)

def _build_review_context(subject: str, claim_text: str, claim_type: str, lane: str, public_safe: bool, dimension: str, intent: str, evidence: str = "", resolved_memory: dict[str, Any] | None = None, packet: dict[str, Any] | None = None) -> dict[str, Any]:
    safe_summary = _text(_redact_review_evidence_summary(evidence or claim_text, subject), 240)
    protected = lane == "source_blind" or dimension == "source_protected_context" or "source-blind" in f"{claim_text} {evidence}".lower()
    if protected:
        safe_summary = "Protected context exists, but details are withheld from this card."
    signals = []
    hay = f"{claim_type} {claim_text} {evidence}".lower()
    for label, pattern in (
        ("contest/event", r"\b(contest|event|rules|submission|submitter)\b"),
        ("show operations", r"\b(show operations?|show[- ]ops|show[- ]operations?|broadcast operations?|run of show|production)\b"),
        ("collaboration", r"\b(collaborat|work together|featured|credited)\b"),
        ("links/music", r"\b(link|social|music|song|artist|suno|soundcloud|youtube|spotify)\b"),
        ("community presence", r"\b(community|viewer|participant|member|chat)\b"),
        ("protected context", r"\b(source[-_ ]blind|protected|private|withheld)\b"),
    ):
        if re.search(pattern, hay):
            signals.append(label)
    raw_label = claim_type.replace("_", " ")
    if _is_raw_review_label(claim_text):
        raw_label = claim_text
    what_knows = []
    if safe_summary and not _is_raw_review_label(safe_summary):
        what_knows.append(safe_summary)
    elif signals:
        what_knows.append(f"BNL detected {', '.join(signals[:3])} context near {subject}.")
    what_not = []
    cannot = []
    if dimension == "contest_event_activity":
        what_not.append(f"BNL does not know whether {subject} participated, submitted something, posted rules, shared a link, helped organize, hosted, or was only mentioned nearby.")
        cannot.append("Do not infer a contest/event role, host status, organizer status, or public title from proximity alone.")
    elif dimension == "show_operations":
        what_not.append(f"BNL does not know whether {subject} actually helps with show operations or was only discussed near that topic.")
        cannot.append("Do not infer a show-operations role/title without explicit confirmation.")
    elif dimension in {"collaboration_interest", "collaboration_status"}:
        what_not.append("BNL needs to distinguish collaboration interest from an actual approved public collaboration.")
        cannot.append("Do not call interest, planning, or nearby discussion an approved collaboration.")
    elif dimension in {"links_socials", "music_artist_context"}:
        what_not.append(f"BNL does not know which links/music/socials are {subject}-owned or approved for public use.")
        cannot.append("Do not publish or attribute links/music until ownership and public use are confirmed.")
    elif dimension == "community_presence":
        what_not.append(f"BNL does not know whether to describe {subject} as a participant/viewer/member or avoid public role wording.")
        cannot.append("Do not turn community presence into a formal role/title without approval.")
    elif dimension == "source_protected_context":
        what_not.append("BNL does not have approved public replacement wording or a separate public source for this protected context.")
        cannot.append("Do not expose, quote, or infer public facts from protected/source-blind context.")
    else:
        what_not.append("BNL does not have enough confirmed public-safe context to write dossier text yet.")
        cannot.append("Do not infer a public fact from this signal alone.")
    claim_for_anchor = {"claimText": claim_text, "claimType": claim_type, "reviewLane": lane, "publicSafe": public_safe, "safeEvidenceSummary": safe_summary, "dossierDimension": dimension, "why": evidence}
    anchor = _evidence_anchor_for_claim(subject, claim_for_anchor, resolved_memory, packet)
    if anchor.get("hasAnchor"):
        what_knows = [str(anchor.get("whatBnlKnows") or anchor.get("safeAnchorText") or "")]
        safe_summary = str(anchor.get("safeAnchorText") or safe_summary)
    known_facts = []
    public_facts = []
    internal_notes = []
    readiness_needs = []
    if isinstance(resolved_memory, dict):
        for key in ("publicSafeFacts", "publicSafeNotes", "publicCommunitySignals", "publicCreativeMusicSignals", "publicRoleSignals", "publicLinkSignals"):
            public_facts.extend([_text(x, 180) for x in (resolved_memory.get(key) or []) if _text(x, 180)])
        for ev in resolved_memory.get("reviewOnlyEvidence") or []:
            if isinstance(ev, dict) and ev.get("summary"):
                internal_notes.append(_text(_redact_review_evidence_summary(str(ev.get("summary")), subject), 180))
        for x in (resolved_memory.get("queueOrSubmissionSignals") or []) + (resolved_memory.get("relationshipOrContextSignals") or []):
            internal_notes.append(_text(_redact_review_evidence_summary(str(x), subject), 180))
    if isinstance(packet, dict):
        # TODO-safe: packet schemas vary; only hydrate explicit, already summarized Source File fields.
        for key in ("knownFacts", "sourceFileFacts", "facts"):
            known_facts.extend([_text(x, 180) for x in (packet.get(key) or []) if _text(x, 180)] if isinstance(packet.get(key), list) else [])
        for key in ("internalNotes", "notes", "bnlNotes"):
            if isinstance(packet.get(key), list):
                for x in packet.get(key) or []:
                    safe_note = _safe_review_context_note(x, subject)
                    if safe_note:
                        internal_notes.append(safe_note)
        readiness = packet.get("dossierReadiness") if isinstance(packet.get("dossierReadiness"), dict) else {}
        readiness_needs.extend([_text(q.get("question"), 180) for q in (readiness.get("questions") or []) if isinstance(q, dict) and _text(q.get("question"), 180)])
    return {
        "subjectName": subject, "claimText": claim_text, "claimType": claim_type,
        "dossierDimension": dimension, "cardIntent": intent, "sourceSafety": anchor.get("sourceSafety") or ("source_protected" if protected else ("public_safe" if public_safe else "review_only")),
        "confirmationTarget": "admin", "rawLabel": raw_label, "safeEvidenceSummary": safe_summary,
        "specificEvidenceSignals": signals, "knownSourceFileFacts": _dedupe_by_pattern(known_facts + public_facts, subject, 6), "missingConfirmations": what_not,
        "existingInternalNotes": list(dict.fromkeys(internal_notes))[:6], "existingPublicFacts": _dedupe_by_pattern(public_facts, subject, 6), "dossierReadinessNeeds": _dedupe_by_pattern(readiness_needs, subject, 6),
        "relatedSignals": signals, "whatBnlKnows": what_knows, "whatBnlDoesNotKnow": what_not,
        "whatCannotBeInferred": cannot, "safePublicCandidates": public_facts[:4], "blockedReasons": [] if public_safe else ["missing owner/admin confirmation", "missing public-safe provenance"],
        "evidenceAnchor": anchor, "answerability": anchor.get("answerable"), "notAnswerableReason": anchor.get("notAnswerableReason", ""),
    }

def _specificity_level(ctx: dict[str, Any]) -> str:
    if ctx.get("sourceSafety") == "source_protected":
        return "partially_specific"
    summary = str(ctx.get("safeEvidenceSummary") or "")
    signals = ctx.get("specificEvidenceSignals") or []
    if not summary and not signals:
        return "not_reviewable"
    if _is_raw_review_label(summary) and len(signals) <= 1:
        return "generic"
    if len(summary.split()) >= 7 or len(signals) >= 2:
        return "specific"
    return "partially_specific" if signals else "generic"

def _human_review_topic(subject: str, dimension: str, ctx: dict[str, Any]) -> str:
    if dimension == "contest_event_activity":
        return f"{subject}'s possible contest/event connection"
    if dimension == "show_operations":
        return f"{subject}'s possible show-operations involvement"
    if dimension in {"collaboration_interest", "collaboration_status"}:
        return f"{subject}'s possible collaboration status"
    if dimension in {"links_socials", "music_artist_context"}:
        return f"{subject}'s public links/music approval"
    if dimension == "source_protected_context":
        return f"Protected {subject} context that needs public-safe wording"
    if dimension == "public_role_title":
        return f"{subject}'s possible community role/title"
    if dimension == "queue_submission_activity":
        return f"{subject}'s queue/submission context"
    if dimension == "lore_context":
        return f"{subject}'s lore/context boundary"
    return f"{subject}'s Source File context needing review"

def _compose_source_file_review_card(subject: str, claim: dict[str, Any], dossier_dimension: str, card_intent: str, context: dict[str, Any]) -> dict[str, Any]:
    level = _specificity_level(context)
    topic = _human_review_topic(subject, dossier_dimension, context)
    anchor = context.get("evidenceAnchor") if isinstance(context.get("evidenceAnchor"), dict) else {}
    answerability = str(anchor.get("answerable") or context.get("answerability") or "not_answerable")
    protected = context.get("sourceSafety") == "source_protected"
    knows = context.get("whatBnlKnows") or []
    not_knows = context.get("whatBnlDoesNotKnow") or []
    cannot = context.get("whatCannotBeInferred") or []
    summary = knows[0] if knows else (context.get("safeEvidenceSummary") or f"BNL found {topic.lower()}, but does not have enough safe context to describe it yet.")
    if anchor.get("safeAnchorText"):
        summary = str(anchor.get("safeAnchorText"))
    elif _is_raw_review_label(str(summary)):
        summary = "BNL only has a raw label or generic category, so this is not answerable as a normal review card."
    decision = str(claim.get("displayDecision") or claim.get("recommendedFollowUpQuestion") or "")
    target_dimensions = {"contest_event_activity", "show_operations", "collaboration_interest", "collaboration_status", "source_protected_context"}
    public_wording = _clean_suggested_public_wording(str(claim.get("suggestedPublicWording") or ""))
    if _is_raw_review_label(public_wording) or protected:
        public_wording = ""
    has_valid_public_wording = bool(public_wording)
    has_public_safe_anchor = bool(claim.get("publicSafe") is True and anchor.get("hasAnchor"))
    has_existing_public_approval = bool(claim.get("recommendedAction") == "approve_public")
    public_approval_ready = bool(
        not protected
        and (
            has_valid_public_wording
            or (answerability != "not_answerable" and (has_public_safe_anchor or has_existing_public_approval))
        )
    )
    if public_approval_ready:
        answerability = "answerable"
    if dossier_dimension not in target_dimensions:
        out = {
            "displayTopic": topic,
            "evidenceSummary": summary,
            "specificityLevel": level,
            "reviewContext": context,
            "whatBnlKnows": knows,
            "whatBnlDoesNotKnow": not_knows,
            "whatCannotBeInferred": cannot,
        }
        out["answerability"] = answerability
        if public_wording:
            out["suggestedPublicWording"] = public_wording
        if public_approval_ready:
            existing_actions = claim.get("allowedReviewActions") or []
            if not any(a.get("action") == "approve_public_wording" for a in existing_actions if isinstance(a, dict)):
                existing_actions = _actions(("approve_public_wording", "Approve public wording")) + existing_actions
            out["allowedReviewActions"] = existing_actions or _public_approval_actions()
            out["decisionState"] = "decidable"
            out["recommendedAction"] = "approve_public"
        elif answerability == "not_answerable" or protected:
            out["allowedReviewActions"] = _actions(("keep_internal", "Keep internal"), ("ask_follow_up", "Ask follow-up"), ("reject", "Reject / not useful"))
        if not public_wording:
            out["cannotSuggestPublicReason"] = claim.get("cannotSuggestPublicReason") or "No public wording yet — BNL needs confirmation first."
        return out
    if claim.get("actionability") == "weak_label":
        return {
            "displayTopic": topic,
            "evidenceSummary": summary,
            "specificityLevel": level,
            "reviewContext": context,
            "whatBnlKnows": knows,
            "whatBnlDoesNotKnow": not_knows,
            "whatCannotBeInferred": cannot,
        }
    if dossier_dimension == "contest_event_activity":
        decision = f"Was {subject} involved in this contest or event, or was he only mentioned nearby?"
        note = f"BNL found contest/event context near {subject}, but the available context does not confirm whether {subject} participated, submitted something, posted rules, shared a link, helped organize, hosted, or was only mentioned nearby."
    elif dossier_dimension == "show_operations":
        decision = f"Is {subject} involved in show operations? If yes, what does he help with, and can BNL mention it publicly?"
        note = f"BNL found show-operations context near {subject}, but the available context does not confirm whether {subject} actually helps with show operations or was only discussed near that topic."
    elif dossier_dimension in {"collaboration_interest", "collaboration_status"}:
        decision = f"Did {subject} only show interest in collaborating, or is there an actual public collaboration BNL can mention publicly? If public, is it with BARCODE or another project, song, show, or context?"
        note = f"BNL found collaboration-related context for {subject}, but it needs to distinguish interest in collaborating from an actual approved collaboration before this can become dossier text."
    elif dossier_dimension == "source_protected_context" or protected:
        decision = "Should this protected context stay internal, or is there approved public wording BNL can use?"
        note = f"Protected context exists for {subject}. Keep it internal unless an owner/admin provides public wording or a separate public source."
    else:
        note = str(claim.get("suggestedInternalNote") or f"Possible {dossier_dimension.replace('_', ' ')} signal exists, but BNL does not have enough safe context to describe it yet.")
    if public_approval_ready:
        claim["decisionState"] = "decidable"
        claim["recommendedAction"] = "approve_public"
        existing_actions = claim.get("allowedReviewActions") or []
        if not any(a.get("action") == "approve_public_wording" for a in existing_actions if isinstance(a, dict)):
            existing_actions = _actions(("approve_public_wording", "Approve public wording")) + existing_actions
        claim["allowedReviewActions"] = existing_actions or _public_approval_actions()
    elif answerability == "not_answerable":
        claim["decisionState"] = "internal_audit"
        claim["recommendedAction"] = "keep_internal"
        claim["allowedReviewActions"] = _actions(("keep_internal", "Keep internal"), ("ask_follow_up", "Ask follow-up"), ("reject", "Reject / not useful"))
    elif answerability == "partially_answerable" and not protected:
        claim["decisionState"] = "needs_clarification"
        claim["recommendedAction"] = "needs_more_info"
        claim["allowedReviewActions"] = _actions(("ask_follow_up", "Ask follow-up"), ("keep_internal", "Keep internal"), ("reject", "Reject / not useful"))
    elif dossier_dimension == "show_operations":
        claim["allowedReviewActions"] = _actions(("confirm_show_operations", "Yes — involved"), ("not_involved", "No — not involved"), ("keep_internal", "Keep internal"), ("ask_follow_up", "Ask follow-up"), ("reject", "Reject / not useful"))
    out = {
        "displayTopic": topic,
        "displayTitle": f"Need approved public wording for {subject}" if protected else f"Clarify {topic}",
        "displayDecision": decision,
        "displayWhatIsBeingChecked": f"{summary} Admins are checking the exact meaning, missing confirmation, and public/internal boundary.",
        "displayWhyItExists": summary,
        "displayBNLRecommendation": "BNL recommends answering the clarification question before approving public copy.",
        "evidenceSummary": summary,
        "displaySafetyDefault": "Keep internal until the exact meaning and public-safe wording are confirmed." if not protected else "Keep internal. Do not use protected context publicly.",
        "displayApprovalInstruction": "Approve only the exact confirmed public-safe wording, or choose keep internal/reject.",
        "displayWhatYouAreApproving": f"A decision about {topic}, not the raw label \"{context.get('rawLabel') or dossier_dimension}\" and not a general role/title.",
        "recommendedFollowUpQuestion": decision,
        "recommendedFollowUpReason": "BNL needs the admin/owner to confirm what the evidence means, what remains unknown, and what should not be inferred.",
        "suggestedConfirmationAnswerType": str(claim.get("suggestedConfirmationAnswerType") or "plain-language confirmation, correction, public-safe wording, or keep-internal decision"),
        "suggestedInternalNote": note,
        "suggestedPublicWording": public_wording,
        "exampleApprovedText": public_wording if public_wording else "",
        "specificityLevel": level,
        "reviewContext": context,
        "whatBnlKnows": knows,
        "whatBnlDoesNotKnow": not_knows,
        "whatCannotBeInferred": cannot,
        "evidenceAnchor": anchor,
        "answerability": answerability,
    }
    if claim.get("allowedReviewActions"):
        out["allowedReviewActions"] = claim.get("allowedReviewActions")
    if public_approval_ready:
        out["decisionState"] = claim.get("decisionState")
        out["recommendedAction"] = claim.get("recommendedAction")
    if answerability == "not_answerable":
        out["displayDecision"] = "Internal audit only — BNL needs a concrete safe evidence anchor before review."
        out["displayApprovalInstruction"] = "Do not approve public wording from this item; ask for context, keep internal, or reject."
    if not public_wording:
        out["cannotSuggestPublicReason"] = "Source-blind memory cannot become public copy by itself." if protected else "No public wording yet — BNL needs confirmation first."
    return out

def _finalize_review_guidance(subject: str, claim_text: str, claim_type: str, lane: str, public_safe: bool, guidance: dict[str, Any], dimension: str, intent: str, evidence: str = "") -> dict[str, Any]:
    ctx = _build_review_context(subject, claim_text, claim_type, lane, public_safe, dimension, intent, evidence)
    guidance.update(_compose_source_file_review_card(subject, guidance, dimension, intent, ctx))
    if guidance.get("recommendedFollowUpQuestion"):
        guidance["suggestedConfirmationReason"] = guidance.get("recommendedFollowUpReason", guidance.get("suggestedConfirmationReason", ""))
        guidance["suggestedConfirmationQuestion"] = guidance["recommendedFollowUpQuestion"]
        guidance["suggestedMissingInfoQuestion"] = guidance["recommendedFollowUpQuestion"]
        guidance["verificationPacketQuestion"] = guidance["recommendedFollowUpQuestion"]
        guidance["verificationPacketQuestions"] = [guidance["recommendedFollowUpQuestion"]]
    guidance["displayWhyBNLFlaggedIt"] = guidance.get("displayWhyItExists", guidance.get("displayWhyBNLFlaggedIt", ""))
    guidance["displaySafeDefault"] = guidance.get("displaySafetyDefault", guidance.get("displaySafeDefault", ""))
    return guidance

def _review_guidance(subject: str, claim_text: str, claim_type: str, lane: str, public_safe: bool, evidence: str = "") -> dict[str, str]:
    actionability = _claim_actionability(claim_text, claim_type, lane, public_safe)
    low = (claim_text or "").lower()
    role_read_for_dimension = _public_role_candidate_for_subject(subject, claim_text, {"claimType": claim_type, "lane": lane})
    dimension = _dossier_dimension_for_claim(subject, claim_text, claim_type, lane, public_safe, role_read_for_dimension)
    intent = _card_intent_for_dimension(dimension, public_safe)
    dimension_copy = _dimension_template(subject, dimension)
    guidance: dict[str, Any] = {"actionability": actionability, "dossierDimension": dimension, "cardIntent": intent}
    public_wording = _clean_suggested_public_wording(_suggested_public_wording(subject, claim_text, claim_type, public_safe))
    if public_wording:
        guidance.update({
            "suggestedPublicWording": public_wording,
            "recommendedAction": "approve_public",
            "recommendedActionReason": "BNL found public-safe or owner/admin-confirmed support for exact public wording.",
        })
        guidance.update(_review_display_copy(subject, claim_text, claim_type, lane, public_safe, actionability))
        _apply_dimension_card_copy(guidance, dimension_copy, dimension)
        return _finalize_review_guidance(subject, claim_text, claim_type, lane, public_safe, guidance, dimension, intent, evidence)
    if actionability == "non_actionable_artifact":
        guidance.update({
            "recommendedAction": "reject",
            "suggestedRejectionReason": "This is a technical evidence artifact, not a reviewable Source File fact.",
            "suggestedInternalNote": f"BNL found local/keyed evidence for {subject}, but it is not specific enough to become a public or internal claim without more context.",
            "cannotSuggestPublicReason": "No concrete public-safe claim was identified.",
            "recommendedActionReason": "The item is audit metadata rather than a subject fact.",
        })
        guidance.update(_review_display_copy(subject, claim_text, claim_type, lane, public_safe, actionability))
        _apply_dimension_card_copy(guidance, dimension_copy, dimension)
        return _finalize_review_guidance(subject, claim_text, claim_type, lane, public_safe, guidance, dimension, intent, evidence)
    if actionability == "weak_label":
        label = _text(claim_text, 80)
        if "contest organizer" in low:
            label = "contest organizer"
        guidance.update({
            "recommendedAction": "keep_internal",
            "suggestedInternalNote": f"BNL found a weak pattern label: \"{label}\". Keep internal only if useful; reject it if inaccurate.",
            "suggestedMissingInfoQuestion": f"Is the label \"{label}\" accurate and useful for {subject}, or should it be rejected?",
            "suggestedRejectionReason": "This label is too thin to approve without supporting public-safe context.",
            "cannotSuggestPublicReason": "The label is not specific enough to become public copy.",
            "recommendedActionReason": "Weak labels need supporting context before approval.",
        })
        guidance.update(_review_display_copy(subject, claim_text, claim_type, lane, public_safe, actionability))
        _apply_dimension_card_copy(guidance, dimension_copy, dimension)
        return _finalize_review_guidance(subject, claim_text, claim_type, lane, public_safe, guidance, dimension, intent, evidence)
    if actionability == "source_blind_warning":
        guidance.update({
            "decisionState": "source_blind_blocked",
            "hasSafePublicSuggestion": False,
            "recommendedAction": "needs_public_source",
            "suggestedInternalNote": f"BNL found source-blind context for {subject}. Keep this internal until a public source or owner-approved wording is provided.",
            "suggestedMissingInfoQuestion": "Can an owner/admin provide public-safe replacement wording for this context, or should BNL keep it internal?",
            "cannotSuggestPublicReason": "Source-blind memory cannot become public copy by itself.",
            "recommendedActionReason": "Source-blind context needs separate public-safe provenance or owner/admin confirmation.",
        })
        guidance.update(_review_display_copy(subject, claim_text, claim_type, lane, public_safe, actionability))
        _apply_dimension_card_copy(guidance, dimension_copy, dimension)
        return _finalize_review_guidance(subject, claim_text, claim_type, lane, public_safe, guidance, dimension, intent, evidence)
    if claim_type == "contest_music_context":
        note = f"BNL found contest-related music/link context for {subject}. This does not prove {subject} hosted or organized a contest. Keep internal until links and role are confirmed."
        question = f"Which {subject} music or contest-related links are owned or approved for public reference?"
    elif claim_type in {"contest_submitter", "contest_rules_poster", "contest_public_link_context", "contest_channel_context", "contest_date_context", "contest_mention_only"}:
        scope = _contest_scope_for_text(subject, claim_text)
        note = f"BNL found contest-related context for {subject}, but the evidence does not establish that {subject} hosted or organized a contest."
        question = (f"BNL found contest context involving {scope['scopeLabel']} near {subject}. Was {subject} a participant, submitter, rules/link source, organizer, host, or just mentioned nearby?"
                    if scope["scopeKnown"] else f"BNL found unclear contest/rules wording near {subject}. What contest or event is this referring to, if any, and was {subject} actually involved?")
    elif claim_type == "rules_instructions_context":
        note = f"BNL found rules/instructions wording near {subject}, but it does not know which instructions, who they were for, or what role {subject} had."
        question = f"What rules or instructions is this referring to, who were they for, and what was {subject}'s actual role with them?"
    elif claim_type in {"music_link", "public_link"}:
        note = f"BNL found repeated music/link context for {subject}, but public ownership/use is not confirmed. Keep this internal until owner/admin approves links or wording."
        question = f"Which {subject} links are owned or approved for public reference?"
    elif claim_type == "relationship" and "orion" in low:
        note = f"BNL found Orion-related context for {subject}. Keep this internal unless owner/admin confirms it may be referenced publicly."
        question = f"Can BNL mention Orion in public {subject} context, or should Orion stay internal?"
    elif claim_type == "queue_submission":
        note = f"BNL found possible queue/submission context for {subject}. Keep this internal unless admin confirms it may be referenced publicly."
        question = f"Can {subject}'s queue/submission history be referenced publicly, or should it stay internal?"
    elif claim_type in {"role", "community_context"}:
        role_read = _public_role_candidate_for_subject(subject, claim_text)
        if role_read.get("isPublicRole") or role_read.get("roleCandidate"):
            note = f"BNL found possible role/community context for {subject}. Keep this internal until public wording is confirmed."
            question = f"What public role/title may BNL use for {subject}, if any?"
        else:
            note = f"BNL found {role_read.get('activityContextType', 'activity/context').replace('_', ' ')} for {subject}, but this does not prove a public role/title."
            question = f"Should BNL mention this activity/context publicly, keep it internal, or ignore it?"
    else:
        note = f"BNL found review-only context for {subject}. Keep this internal until a public source or owner-approved wording is provided."
        question = "What proof or approved wording should BNL use before saying this publicly?"
    guidance.update({
        "recommendedAction": "needs_more_info",
        "recommendedActionReason": "BNL cannot safely suggest public wording until the missing confirmation is resolved.",
        "suggestedInternalNote": note,
        "suggestedMissingInfoQuestion": question,
        "cannotSuggestPublicReason": "Contest role is ambiguous and needs owner/admin confirmation." if claim_type.startswith("contest_") and claim_type not in {"contest_host", "contest_organizer"} else "No confirmed public-safe wording or provenance was identified.",
    })
    guidance.update(_review_display_copy(subject, claim_text, claim_type, lane, public_safe, actionability))
    _apply_dimension_card_copy(guidance, dimension_copy, dimension)
    return _finalize_review_guidance(subject, claim_text, claim_type, lane, public_safe, guidance, dimension, intent, evidence)


def _safe_evidence_example(text: str, subject: str) -> str:
    clean = _redact_review_evidence_summary(text, subject)
    clean = re.sub(r"\b(cus|sub|pi|ch|tok)_[A-Za-z0-9_\-]+\b", "[redacted token]", clean, flags=re.I)
    clean = re.sub(r"\b\d{6,}\b", "[redacted id]", clean)
    return clean or f"Redacted protected memory about {subject}."


def _make_reviewable_claim(subject: str, claim_text: str, claim_type: str, lane: str, why: str, public_safe: bool, confidence: str, evidence: str = "", blocked: list[str] | None = None) -> dict[str, Any]:
    actionability = _claim_actionability(claim_text, claim_type, lane, public_safe)
    display_claim_text = "Weak internal signal — not a public fact" if actionability == "non_actionable_artifact" else claim_text
    item = {
        "claimText": _text(display_claim_text, 300),
        "claimType": claim_type,
        "reviewLane": lane,
        "suggestedDecision": "confirm_public" if public_safe else ("keep_boundary" if lane in {"source_blind", "private_internal_withheld"} else "needs_more_info"),
        "why": _text(why, 220),
        "publicSafe": bool(public_safe),
        "hasSafePublicSuggestion": bool(public_safe),
        "decisionState": "decidable" if public_safe else ("source_blind_blocked" if lane == "source_blind" else "decidable"),
        "confidence": confidence,
        "safeEvidenceSummary": _text(evidence or why, 240),
        "blockedBy": blocked or ([] if public_safe else ["missing owner/admin confirmation", "missing public-safe provenance"]),
    }
    role_candidate = _public_role_candidate_for_subject(subject, claim_text, {"claimType": claim_type, "lane": lane})
    item["publicRoleCandidate"] = role_candidate
    item["activityContextType"] = role_candidate.get("activityContextType") or _activity_context_type_for_text(claim_text, claim_type, lane)
    if (not public_safe) and actionability != "weak_label" and item.get("claimType") == "role" and not role_candidate.get("isPublicRole") and item["activityContextType"] in _ACTIVITY_CONTEXT_TYPES:
        item["claimType"] = item["activityContextType"]
    item.update(_review_guidance(subject, claim_text, item.get("claimType", claim_type), lane, public_safe, evidence or why))
    if item.get("dossierDimension") == "source_protected_context":
        item["safeEvidenceSummary"] = "Protected context exists, but details are withheld from this card."
    return item


def _is_human_visible_review_claim(claim: dict[str, Any]) -> bool:
    """Keep stale workflow/audit noise out of human-facing review cards."""
    text = " ".join(str(claim.get(k) or "") for k in ("claimText", "displayTitle", "displayDecision", "recommendedAction", "suggestedInternalNote")).lower()
    if claim.get("actionability") == "non_actionable_artifact":
        return False
    if claim.get("actionability") == "weak_label" and claim.get("claimType") not in {"role", "relationship", "contest_music_context", "contest_submitter"}:
        return False
    stale = (
        "review each sourcefilereviewclaims",
        "promote only confirmed public-safe facts",
        "attach owner-approved provenance",
        "admin review before use",
        "confirm source",
        "owner-approved provenance needed",
    )
    return not any(marker in text for marker in stale)



def _admin_blocked_claim(subject: str, claim_text: str, claim_type: str, correction: dict[str, Any]) -> dict[str, Any]:
    label = str(correction.get("label") or _detect_blocked_label(claim_text) or claim_type.replace("_", " "))
    item = {
        "claimText": _text(claim_text, 300),
        "claimType": claim_type,
        "reviewLane": "admin_correction",
        "suggestedDecision": "reject",
        "why": "Admin/source-file correction rejected or corrected this inference.",
        "publicSafe": False,
        "confidence": "high",
        "safeEvidenceSummary": f"Admin has already rejected or corrected the {label} label.",
        "blockedBy": ["admin correction"],
        "recommendedAction": "reject",
        "suggestedRejectionReason": f"Admin has already rejected the {label} label for {subject}.",
        "cannotSuggestPublicReason": "Admin correction blocks this inference.",
        "recommendedActionReason": "Rejected Source File labels are authoritative until later owner/admin confirmation reverses them.",
        "adminCorrectionApplied": True,
        "adminCorrectionSummary": _text(correction.get("normalizedMeaning") or f"{subject} is not a {label}.", 220),
        "rejectedLabel": label,
        "blockedInference": f"Admin rejected {label} for {subject}.",
    }
    item.update(_review_display_copy(subject, item["claimText"], claim_type, item["reviewLane"], False, "weak_label"))
    item["displayTitle"] = "Check weak label"
    item["primaryActionLabel"] = "Reject weak label"
    return item

def _admin_conflict_fields(subject: str, evidence: str, correction: dict[str, Any]) -> dict[str, Any]:
    label = str(correction.get("label") or _detect_blocked_label(evidence) or "this label")
    fields = {
        "reviewLane": "admin_correction_conflict",
        "recommendedAction": "needs_more_info",
        "suggestedDecision": "needs_more_info",
        "claimText": f"Possible conflict with admin correction: {subject} / {label}",
        "conflictingEvidenceSummary": _safe_evidence_example(evidence, subject),
        "adminCorrectionSummary": _text(correction.get("normalizedMeaning") or f"Admin previously rejected {label} for {subject}.", 220),
        "suggestedMissingInfoQuestion": "Admin previously rejected this label. Is there new owner-approved evidence that changes it?",
        "recommendedActionReason": "New evidence appears to conflict with an admin correction; do not override silently.",
    }
    fields.update(_review_display_copy(subject, fields["claimText"], "role", fields["reviewLane"], False, "missing_confirmation"))
    fields["suggestedMissingInfoQuestion"] = "Admin previously rejected this label. Is there new owner-approved evidence that changes it?"
    fields["suggestedConfirmationQuestion"] = fields["suggestedMissingInfoQuestion"]
    fields["recommendedFollowUpQuestion"] = fields["suggestedMissingInfoQuestion"]
    fields["verificationPacketQuestion"] = fields["suggestedMissingInfoQuestion"]
    fields["verificationPacketQuestions"] = [fields["suggestedMissingInfoQuestion"]]
    return fields

def _resolved_missing_info_types(corrections: list[dict[str, Any]]) -> set[str]:
    resolved: set[str] = set()
    for corr in corrections:
        if corr.get("kind") not in {"missing_info_answer", "confirmed_internal_note", "approved_public_fact"}:
            continue
        text = f"{corr.get('label','')} {corr.get('normalizedMeaning','')}".lower()
        if re.search(r"role|title|artist|producer|moderator|team|community member", text):
            resolved.add("role")
        if re.search(r"contest|organizer|host|judge|participant|submitter|rules", text):
            resolved.add("contest_music_context")
        if re.search(r"link|url|suno|social", text):
            resolved.add("music_link")
        if re.search(r"queue|submission|submitter", text):
            resolved.add("queue_submission")
        if re.search(r"relationship|orion|relay|alias", text):
            resolved.add("relationship")
    return resolved

def _withheld_audit(subject: str, resolved_memory: dict[str, Any], private: int, blind: int, review: int) -> dict[str, Any]:
    private_examples = [_safe_evidence_example(x.get("summary", ""), subject) for x in (resolved_memory.get("privateOrInternalEvidence") or [])[:2] if isinstance(x, dict)]
    blind_examples = [_safe_evidence_example(x, subject) for x in (resolved_memory.get("sourceSafetyWarnings") or [])[:2] if isinstance(x, str)]
    review_examples = [_safe_evidence_example(x.get("summary", ""), subject) for x in (resolved_memory.get("reviewOnlyEvidence") or [])[:2] if isinstance(x, dict)]
    cats = {
        "private_internal": {"count": private, "reason": "Private/internal, payment/customer, raw-ID, DM, database-row, or operational content is never public draft evidence.", "safeExamples": private_examples},
        "payment_customer_priority": {"count": private, "reason": "Protected payment/customer/Priority signals are counted only as withheld safety evidence.", "safeExamples": private_examples[:1]},
        "raw_ids_or_database_rows": {"count": private, "reason": "Raw IDs, tokens, and database-row style content are redacted and withheld.", "safeExamples": []},
        "source_blind": {"count": blind, "reason": "Source-blind memory can guide internal review but cannot prove public claims.", "safeExamples": blind_examples},
        "unsafe_public_copy": {"count": review, "reason": "Review-only claims may be meaningful but need confirmation before public use.", "safeExamples": review_examples},
        "duplicate_or_repetitive": {"count": 0, "reason": "Repetitive public-safe items are deduped before draft ingredients.", "safeExamples": []},
        "low_value_context": {"count": 0, "reason": "Thin context is kept out of public copy until it supports a specific claim.", "safeExamples": []},
    }
    return {"totalWithheld": private + blind + review, "categories": cats}

def build_subject_analyst_read(subject_name: str, resolved_memory: dict[str, Any], packet: dict[str, Any] | None = None) -> dict[str, Any]:
    """Synthesize resolver output into internal/read-review/public-draft buckets.

    The analyst read may reason from non-private review/source-blind counts, but
    public draft ingredients only come from sanitized public-safe resolver text.
    """
    subject = _text(subject_name, 120) or _text(resolved_memory.get("subjectName"), 120) or "Unnamed subject"
    counts = resolved_memory.get("evidenceCounts") if isinstance(resolved_memory.get("evidenceCounts"), dict) else {}
    public_items: list[str] = []
    admin_corrections = extract_admin_corrections(subject, resolved_memory, packet)
    for fact in _approved_public_correction_facts(admin_corrections, subject):
        if fact not in public_items:
            public_items.append(fact)
    for key in ("publicSafeFacts", "publicSafeNotes", "publicCommunitySignals", "publicCreativeMusicSignals", "publicRoleSignals", "publicLinkSignals"):
        for item in resolved_memory.get(key) or []:
            clean = _sanitize_public(str(item), subject, resolved_memory.get("matchedAliasesUsedPrivately") or [])
            if clean and clean not in public_items:
                public_items.append(clean)
    draft_ingredients = _dedupe_by_pattern(public_items, subject, limit=9)
    hay = " ".join(draft_ingredients).lower()
    has_music = bool(re.search(r"\b(music|artist|musician|track|song|album|producer|dj)\b", hay))
    has_community = bool(re.search(r"\b(barcode|bnl|community|dossier|source file|public interaction|public context|radio)\b", hay))
    admin_contest_organizer_rejected = _has_admin_contest_organizer_rejection(resolved_memory, packet) or any(c.get("blocksInference") and c.get("label") in {"contest organizer", "contest host"} for c in admin_corrections)
    if has_music:
        likely_type = "artist"
    elif has_community:
        likely_type = "community_participant"
    else:
        likely_type = "unknown"
    ps = int(counts.get("publicSafe") or 0)
    review = int(counts.get("reviewOnly") or 0)
    blind = int(counts.get("sourceBlind") or 0)
    private = int(counts.get("privateOrInternal") or 0)
    scanned = int(counts.get("totalScanned") or 0)
    confidence = "high" if len(draft_ingredients) >= 5 else ("medium" if len(draft_ingredients) >= 2 or ps >= 2 else "low")
    posture = "confident" if confidence == "high" and not (review or blind) else ("conservative" if draft_ingredients else "placeholder_only")
    if private and not draft_ingredients:
        posture = "do_not_draft_publicly_yet"
    strongest = []
    if has_community:
        strongest.append(f"Repeated public BARCODE/BNL/community context for {subject}.")
    if has_music:
        strongest.append(f"Clean public-safe music or artist wording is present for {subject}.")
    if review:
        strongest.append("Review-needed evidence suggests additional role, link, queue, or relationship context that needs confirmation.")
    if blind:
        strongest.append("Source-blind memory contributes internal pattern context but lacks public provenance.")
    if not strongest and scanned:
        strongest.append("Subject memory exists, but public-safe patterns are thin.")
    if likely_type == "artist":
        internal = f"{subject} appears to be an artist-facing BARCODE subject with public-safe music context."
    elif likely_type == "community_participant":
        internal = f"{subject} appears to be a recurring BARCODE-facing community subject with repeated public interaction/context around BNL or BARCODE."
    elif scanned:
        internal = f"{subject} has subject-memory matches, but BNL lacks enough clean public-safe evidence to make a specific public role claim."
    else:
        internal = f"{subject} has little or no usable subject-memory evidence for a public dossier read."
    if review or blind:
        internal += " Inferred role, link, queue, relationship, or source-blind claims should stay in owner/admin review rather than public copy."
    public_claims = draft_ingredients[:6]
    reviewable_claims: list[dict[str, Any]] = []
    review_claims: list[str] = []
    for txt in public_claims[:6]:
        ctype = _claim_type_from_text(txt)
        if ctype == "missing_confirmation":
            ctype = "community_context" if re.search(r"\b(barcode|bnl|community|radio)\b", txt, re.I) else "role"
        blocked_corr = _correction_blocks_claim(admin_corrections, txt, ctype)
        if blocked_corr:
            reviewable_claims.append(_admin_blocked_claim(subject, txt, ctype, blocked_corr))
            continue
        reviewable_claims.append(_make_reviewable_claim(subject, txt, ctype, "public_safe", "Public-safe subject memory supports this Source File fact.", True, confidence, _safe_evidence_example(txt, subject), []))
    review_sources: list[str] = []
    for ev in resolved_memory.get("reviewOnlyEvidence") or []:
        if isinstance(ev, dict):
            txt = humanize_internal_label(_redact_review_evidence_summary(str(ev.get("summary") or ""), subject))
            if txt and txt not in review_sources:
                review_sources.append(txt)
    for item in (resolved_memory.get("queueOrSubmissionSignals") or []) + (resolved_memory.get("relationshipOrContextSignals") or []):
        txt = humanize_internal_label(_redact_review_evidence_summary(str(item), subject))
        if txt and txt not in review_sources and txt not in draft_ingredients:
            review_sources.append(txt)
    if review and not review_sources:
        review_sources.append(f"{subject} may have role, link, queue/submission, music, or relationship context, but BNL needs confirmation before public use.")
    if admin_contest_organizer_rejected:
        reviewable_claims.append({
            "claimText": f"Contest organizer label for {subject} is blocked by admin correction.",
            "claimType": "contest_organizer",
            "reviewLane": "admin_correction",
            "suggestedDecision": "reject",
            "why": "Admin/source-file correction rejected this weak contest organizer inference.",
            "publicSafe": False,
            "confidence": "high",
            "safeEvidenceSummary": "Admin has rejected the contest organizer label.",
            "blockedBy": ["admin correction"],
            "recommendedAction": "reject",
            "suggestedRejectionReason": f"Admin has rejected the contest organizer label for {subject}.",
            "cannotSuggestPublicReason": "Admin correction blocks this inference.",
            "recommendedActionReason": "Rejected Source File labels are authoritative until owner/admin reverses them.",
            "adminCorrectionApplied": True,
            "adminCorrectionSummary": f"{subject} is not a contest organizer unless later owner/admin confirmation reverses this correction.",
            "rejectedLabel": "contest organizer",
            "blockedInference": "Do not infer contest organizer from contest/link/rules context.",
        })
    for txt in review_sources[:10]:
        conflict_corr = None
        ctype = _claim_type_from_text(txt)
        contest_role, event_hints = classify_contest_event_context(txt)
        contest_scope = _contest_scope_for_text(subject, txt)
        if re.search(r"\b(contest|challenge|tournament|battle|competition)\b", txt, re.I):
            if txt.strip().lower() == "contest organizer":
                ctype = "role"
            else:
                ctype = contest_role if contest_role != "contest_unknown" else ctype
            conflict_corr = _correction_blocks_claim(admin_corrections, txt, ctype)
            if ctype in {"contest_host", "contest_organizer"} and admin_contest_organizer_rejected:
                conflict_corr = conflict_corr or next((c for c in admin_corrections if c.get("label") in {"contest organizer", "contest host"}), None)
                ctype = "contest_mention_only"
        if ctype.startswith("contest_") and not contest_scope.get("shouldEmitReviewCard"):
            continue
        if ctype == "community_context" and not ps:
            ctype = "role"
        conflict_corr = conflict_corr or _correction_blocks_claim(admin_corrections, txt, ctype)
        if conflict_corr and re.search(r"\b(?:may|possible|weak label|label)\b", txt, re.I):
            blocked_claim = _admin_blocked_claim(subject, txt, ctype, conflict_corr)
            if blocked_claim["claimText"] not in review_claims:
                review_claims.append(blocked_claim["claimText"])
                reviewable_claims.append(blocked_claim)
            continue
        if ctype == "relationship" and "orion" in txt.lower():
            line = f"Possible relationship/context claim: {subject} references Orion as an AI or message relay context. Confirm whether this may be used publicly or should remain internal community context."
        elif ctype == "queue_submission":
            line = f"Possible queue/submission claim: {subject} may have queue/submission history, but public use needs confirmation. Evidence context: {txt}"
        elif ctype == "contest_music_context":
            line = f"Possible public music/link context: BNL found contest-related music/link signals for {subject}, but it does not know which links are {subject}-owned or approved for public reference."
        elif ctype.startswith("contest_"):
            line = f"{_claim_prefix(ctype)}: BNL found contest context involving {contest_scope.get('scopeLabel') or 'an unclear contest/event'} near {subject}, but the evidence does not establish {subject}'s role. Host/organizer must come from direct evidence, not proximity."
        elif ctype in {"music_link", "public_link"}:
            line = f"Possible music/link claim: {subject} may have public music or link context, but ownership/use needs confirmation. Evidence context: {txt}"
        elif ctype == "role":
            role_hint = "recurring BARCODE community participant" if re.search(r"barcode|community|participant|recurring", txt, re.I) else "a role or title BNL should not state publicly yet"
            line = f"Possible role claim: {subject} may be {role_hint}. Evidence context: {txt}"
        else:
            line = f"{_claim_prefix(ctype)}: {txt} Confirm the exact public-safe wording before use."
        if line not in review_claims:
            review_claims.append(line)
            claim = _make_reviewable_claim(subject, line, ctype, "needs_confirmation", f"Review-only subject memory mentions {ctype.replace('_', ' ')} context.", False, "low", humanize_internal_label(_safe_evidence_example(txt, subject)))
            if claim.get("decisionState") == "needs_clarification" and not claim.get("evidenceAnchor", {}).get("hasAnchor") and not re.search(r"\b(what|which|who|referring to|about)\b", str(claim.get("recommendedFollowUpQuestion") or ""), re.I):
                continue
            if conflict_corr:
                claim.update(_admin_conflict_fields(subject, txt, conflict_corr))
            if ctype.startswith("contest_"):
                claim["eventEvidenceHints"] = {k: v for k, v in event_hints.items() if v}
                claim["contestScope"] = contest_scope
                if ctype in {"contest_host", "contest_organizer"}:
                    claim["recommendedActionReason"] = "Strong host/organizer wording was detected; still require public-safe confirmation before public use."
            reviewable_claims.append(claim)
    blind_insights = []
    if blind:
        for warn in resolved_memory.get("sourceSafetyWarnings") or []:
            clean = _redact_review_evidence_summary(str(warn), subject)
            if clean and clean not in blind_insights:
                blind_insights.append(clean if clean.startswith("Source-blind") else f"Source-blind/internal context: {clean}")
    if blind and not blind_insights:
        blind_insights = ["Source-blind memory indicates possible additional subject context, but it is not public-copy provenance."]
    for txt in blind_insights[:5]:
        ctype = "source_blind_context"
        if "orion" in txt.lower() or "relay" in txt.lower():
            ctype = "relationship"
            line = f"Possible relationship/context claim: {subject} references Orion or relay context in source-blind memory. Keep internal unless admin confirms public-safe provenance."
            if line not in review_claims:
                review_claims.append(line)
        reviewable_claims.append(_make_reviewable_claim(subject, txt, ctype, "source_blind", "Context lacks public-safe provenance and must not be public copy.", False, "low", _safe_evidence_example(txt, subject), ["source-blind provenance", "missing public source"]))
    exclusions = []
    if private:
        exclusions.append(f"Excluded {private} protected/private memory item(s) from public and provenance text; see withheldEvidenceAudit category counts for safe details.")
    do_not = ["Do not state artist/music role, owned links, formal BARCODE role, queue/submission history, or relationships unless confirmed public-safe evidence supports that exact claim."]
    if private:
        do_not.append("Do not expose private, payment, customer, DM, raw ID, token, database-row, private alias, or internal operational details.")
    identity_needs_review = (
        not subject
        or subject.lower().startswith("unnamed")
        or bool((packet.get("identityAliasStatus") if isinstance(packet, dict) and isinstance(packet.get("identityAliasStatus"), dict) else {}).get("needsConfirmation"))
        or bool(re.search(r"\b(unknown|ambiguous|alias conflict|conflicting alias|source-blind identity|internal-only identity)\b", " ".join(review_sources + blind_insights).lower()))
    )
    missing = []
    if identity_needs_review:
        missing.insert(0, {"question": f"Confirm preferred public display name: What exact name may BNL use for {subject}?", "confirmationType": "needs_owner_confirmation", "suggestedReviewer": "owner_or_admin", "why": "Identity wording only blocks drafting when the name is missing, ambiguous, source-blind, or in conflict.", "canAutoResolve": False, "relatedClaimType": "identity"})
    role_source_has_explicit_role = any(
        _dossier_dimension_for_claim(subject, x, _claim_type_from_text(x), "needs_confirmation", False).startswith("public_role")
        and re.search(r"\b(role|title|moderator|staff|team member|artist|musician|producer|dj)\b", x, re.I)
        for x in review_sources
    )
    if has_community or has_music or role_source_has_explicit_role:
        missing.append({"question": f"Confirm public role/title: Is {subject} a community participant, artist, collaborator, or something else?", "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Role wording is only public when backed by clean public evidence, explicit role/title evidence, or admin-confirmed Source File fact.", "canAutoResolve": bool(has_community or has_music), "relatedClaimType": "role"})
    if has_music or any(re.search(r"\b(suno|music|song|track|link|social|url)\b", x, re.I) for x in review_sources):
        missing.append({"question": f"Confirm public links: Which {subject}/Suno/music/social links are owned or approved for public reference?", "confirmationType": "needs_owner_confirmation", "suggestedReviewer": "owner_or_admin", "why": "Link ownership/use must be explicit before public dossier use.", "canAutoResolve": False, "relatedClaimType": "music_link"})
    contest_scopes = [_contest_scope_for_text(subject, x) for x in review_claims + review_sources if "contest" in x.lower()]
    if contest_scopes:
        scoped = next((s for s in contest_scopes if s.get("scopeKnown")), contest_scopes[0])
        q = (f"BNL found contest context involving {scoped.get('scopeLabel')} near {subject}. Was {subject} a participant, submitter, rules/link source, organizer, host, or just mentioned nearby?"
             if scoped.get("scopeKnown") else f"BNL found unclear contest/rules wording near {subject}. What contest or event is this referring to, if any, and was {subject} actually involved?")
        missing.append({"question": q, "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Contest names/rules/channels/dates/links do not prove host or organizer role; host/organizer must come from direct evidence.", "canAutoResolve": False, "relatedClaimType": "contest_music_context"})
    rules_scopes = [_rules_instructions_scope_for_text(subject, x) for x in review_claims + review_sources if re.search(r"\b(rules?|instructions?)\b", x, re.I)]
    if rules_scopes and not contest_scopes:
        missing.append({"question": f"BNL found unclear rules/instructions context near {subject}. Were these contest rules, queue instructions, Discord/admin instructions, broadcast instructions, or something else? What was {subject}'s role?", "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Rules/instructions wording needs source, audience, purpose, and subject role before becoming a claim.", "canAutoResolve": False, "relatedClaimType": "rules_instructions_context"})
    if any("orion" in x.lower() or "relay" in x.lower() for x in review_claims + blind_insights):
        missing.append({"question": f"Confirm Orion context: Can BNL mention Orion as {subject}'s AI/message relay context, or keep it internal?", "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Named relationship/context should not become public copy without permission and provenance.", "canAutoResolve": False, "relatedClaimType": "relationship"})
    if review or re.search(r"queue|submission", " ".join(str(x) for x in resolved_memory.get("queueOrSubmissionSignals") or []), re.I):
        missing.append({"question": f"Confirm queue/submission history: Can {subject}'s queue or submission history be referenced publicly?", "confirmationType": "needs_admin_confirmation", "suggestedReviewer": "admin", "why": "Queue/submission evidence is review-only unless explicitly confirmed public-safe.", "canAutoResolve": False, "relatedClaimType": "queue_submission"})
    resolved_types = _resolved_missing_info_types(admin_corrections)
    missing = [m for m in missing if m.get("relatedClaimType") not in resolved_types]
    reviewable_claims = [c for c in reviewable_claims if _is_human_visible_review_claim(c)]
    reviewable_claims, dossier_clarification_needs, clarification_audit = _cluster_clarification_needs(subject, reviewable_claims)
    missing_lines = [m["question"] + f" ({m['confirmationType'].replace('_', ' ')}.)" for m in missing]
    readiness = _build_dossier_readiness(subject, reviewable_claims, missing, blind_insights)
    if dossier_clarification_needs:
        readiness = _build_dossier_readiness_from_clarifications(readiness, dossier_clarification_needs)
    actions = [q["question"] for q in readiness["questions"] if q.get("priority") == "high"]
    withheld_audit = _withheld_audit(subject, resolved_memory, private, blind, review) if (private or blind or review) else {}
    source_file_ingredients = []
    for item in [internal, *strongest[:6], *public_claims, *review_claims, *blind_insights, *actions, *missing_lines]:
        clean = _text(item, 320)
        if clean and clean not in source_file_ingredients:
            source_file_ingredients.append(clean)
    prov = [f"BNL subject memory resolver reviewed {scanned} subject-memory matches and reduced them to {len(draft_ingredients)} public-safe draft ingredients."]
    if review:
        prov.append(f"BNL held {len(review_claims) or review} specific review-needed role/link/music/queue/relationship claim(s) out of public copy pending confirmation.")
    if ps > len(draft_ingredients):
        prov.append("BNL treated repetitive public context as internal pattern evidence, not public dossier prose.")
    if private or blind:
        prov.append(f"BNL withheld {private + blind} private/internal or source-unproven item(s) from public draft copy; audit categories contain only redacted summaries.")
    return {
        "subjectName": subject,
        "internalRead": internal,
        "likelySubjectType": likely_type,
        "confidence": confidence,
        "publicDraftPosture": posture,
        "strongestSignals": strongest[:6],
        "publicSafeClaims": public_claims,
        "publicReadyClaims": public_claims,
        "reviewNeededClaims": review_claims,
        "reviewableClaims": reviewable_claims[:14],
        "dossierClarificationNeeds": dossier_clarification_needs[:10],
        "adminCorrections": admin_corrections,
        "sourceBlindInsights": blind_insights,
        "privateOrInternalExclusions": exclusions,
        "withheldEvidenceAudit": withheld_audit,
        "internalClarificationAudit": clarification_audit,
        "doNotSayPublicly": do_not,
        "missingInfoQuestions": missing_lines,
        "missingConfirmations": missing,
        "recommendedAdminActions": actions,
        "recommendedAdminActionCards": [_admin_action_card(a) for a in actions],
        "dossierReadinessQuestions": readiness["questions"],
        "dossierReadinessSummary": readiness["summary"],
        "dossierBlockedBy": readiness["blockedBy"],
        "readyForDraft": readiness["readyForDraft"],
        "draftReadinessReason": readiness["reason"],
        "draftIngredients": draft_ingredients,
        "sourceFileIngredients": source_file_ingredients[:18],
        "provenanceSummary": prov,
    }


def build_subject_memory_diagnostic(subject_name: str, db_path: str) -> dict[str, Any]:
    resolved = resolve_subject_memory(subject_name, db_path)
    counts = resolved["evidenceCounts"]
    return {
        "subjectName": resolved["subjectName"],
        "candidateMemoriesFound": counts["totalScanned"],
        "tablesScanned": resolved["diagnostic"]["tablesScanned"],
        "tablesContributed": resolved["diagnostic"]["tablesContributed"],
        "classificationCounts": counts,
        "classificationReasons": resolved["diagnostic"]["classificationReasons"],
        "safePublicSummary": resolved["publicSafeFacts"][:5],
        "adminConfirmationNeeded": resolved["missingInfoQuestions"][:5],
        "confidence": resolved["confidence"],
    }
