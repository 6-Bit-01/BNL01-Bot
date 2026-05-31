"""Review-only Source File enrichment helpers for BNL.

This module builds structured case-file notes from approved local BNL stores and
optionally posts them through the existing review-only dossier recommendation
endpoint. It never publishes, promotes, merges, or overwrites Source Files.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from bnl_dossier_recommendations import build_dossier_recommendation_payload, send_dossier_recommendation
from bnl_source_file_lookup import lookup_source_file

ENRICHMENT_INGEST_SOURCE = "bnl_source_file_enrichment"
FALLBACK_INGEST_SOURCE = "bnl_source_knowledge_bridge"
SOURCE_BLIND_WARNING = "source-blind memory is review-only; do not treat as confirmed fact"
SECTION_ORDER = [
    "Subject Overview",
    "Why This Subject Matters",
    "Known Roles / Category",
    "History With BARCODE / BNL / Discord / BARCODE Radio",
    "Observed Patterns",
    "Known Facts",
    "Claimed or Inferred Notes",
    "Possible Aliases / Connections",
    "Public-Safe Notes",
    "Internal-Only / Review-Only Notes",
    "Missing Info",
    "Do Not Say",
    "Suggested Next Action",
]
_APPROVED_TABLES = {
    "user_profiles",
    "user_memory_facts",
    "user_habits",
    "relationship_state",
    "relationship_journal",
    "conversations",
    "memory_tiers",
    "broadcast_memory",
    "community_presence",
    "rd_context",
    "source_file_lookup",
}
_DISCORD_MENTION_RE = re.compile(r"<@!?\d+>|<@&\d+>|<#\d+>")
_LONG_ID_RE = re.compile(r"\b\d{15,22}\b")
_URL_RE = re.compile(r"https?://\S+", re.I)
_EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
_WEAK_ALIAS_RE = re.compile(r"\b(?:alias|known as|aka|connection|connected to|via|through|introduced by)\b", re.I)


_ENRICHMENT_QUERY_KEYS = {
    "alias": "alias",
    "candidateid": "candidateId",
    "candidate_id": "candidateId",
    "normalizedname": "normalizedName",
    "normalized_name": "normalizedName",
    "subject": "subject",
}


def parse_source_enrichment_command(content: str) -> tuple[bool, dict[str, Any] | None, str]:
    """Parse operator Source File enrichment commands."""

    text = (content or "").strip()
    match = re.match(
        r"^!bnl\s+(?:(?:source\s+enrich)|(?:enrich\s+source)|(?:source\s+file\s+enrich))\s+(.+)$",
        text,
        flags=re.I | re.S,
    )
    if not match:
        return False, None, "not_a_source_enrichment_command"
    body = match.group(1).strip()
    if not body:
        return True, None, "Use: `!bnl source enrich <subject> [| dry_run=true]`"
    parts = [part.strip() for part in body.split("|")]
    raw_target = re.sub(r"\s+", " ", parts[0]).strip()
    if not raw_target:
        return True, None, "Subject is required."

    lookup_key = "subject"
    lookup_value = raw_target
    key_match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+)$", raw_target, flags=re.S)
    if key_match:
        requested_key = key_match.group(1).strip().lower()
        canonical_key = _ENRICHMENT_QUERY_KEYS.get(requested_key)
        if not canonical_key:
            return True, None, "Enrichment target must be subject, alias, candidateId, or normalizedName."
        lookup_key = canonical_key
        lookup_value = re.sub(r"\s+", " ", key_match.group(2).strip())
        if not lookup_value:
            return True, None, f"Lookup value for `{canonical_key}` is required."

    options: dict[str, Any] = {
        "subject": lookup_value,
        "lookupKey": lookup_key,
        "lookupValue": lookup_value,
        lookup_key: lookup_value,
        "dry_run": False,
    }
    for extra in parts[1:]:
        if not extra:
            continue
        opt = re.match(r"^([A-Za-z_]+)\s*=\s*(.+)$", extra, flags=re.S)
        if not opt:
            return True, None, "Supported option: dry_run=true|false."
        key = opt.group(1).strip().lower()
        value = opt.group(2).strip()
        if key in {"dry_run", "dryrun"}:
            options["dry_run"] = value.lower() in {"true", "1", "yes", "on"}
        else:
            return True, None, "Supported option: dry_run=true|false."
    return True, options, ""


def source_enrichment_ingest_source(environ: dict[str, str] | None = None) -> str:
    """Return the safest ingestSource value for the current site contract."""

    env = environ or {}
    if str(env.get("BNL_SOURCE_ENRICHMENT_INGEST_SUPPORTED") or "").strip().lower() in {"1", "true", "yes", "on"}:
        return ENRICHMENT_INGEST_SOURCE
    return FALLBACK_INGEST_SOURCE


def _subject_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-") or "unknown-subject"


def _safe_text(value: Any, limit: int = 220) -> str:
    text = str(value or "")
    text = _DISCORD_MENTION_RE.sub("[redacted-mention]", text)
    text = _LONG_ID_RE.sub("[redacted-id]", text)
    text = _EMAIL_RE.sub("[redacted-email]", text)
    text = _URL_RE.sub("[redacted-url]", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    if table not in _APPROVED_TABLES:
        return False
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _like_terms(subject: str) -> list[str]:
    words = [w for w in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", subject or "") if len(w) >= 3]
    terms = [subject.strip()] + words
    out: list[str] = []
    for term in terms:
        lowered = term.lower()
        if lowered not in {x.lower() for x in out}:
            out.append(term[:80])
    return out[:5]


def _matches_subject(text: str, terms: list[str]) -> bool:
    lower = str(text or "").lower()
    return any(term.lower() in lower for term in terms if term)


def _append_section(sections: dict[str, list[str]], name: str, value: str, *, limit: int = 5) -> None:
    clean = _safe_text(value, 240)
    if not clean:
        return
    bucket = sections.setdefault(name, [])
    if clean not in bucket and len(bucket) < limit:
        bucket.append(clean)


def _source_file_obj(lookup_result: dict[str, Any]) -> dict[str, Any]:
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    for key in ("sourceFile", "source_file", "file", "candidate", "result", "dossier"):
        if isinstance(data.get(key), dict):
            return data[key]
    return data if isinstance(data, dict) else {}


def _first(obj: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if obj.get(key) not in (None, "", []):
            return obj.get(key)
    return None


def _possible_match_items(lookup_result: dict[str, Any]) -> list[dict[str, str]]:
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    raw = data.get("possibleMatches") or data.get("possible_matches") or data.get("matches") or []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, (list, tuple)):
        return []
    items: list[dict[str, str]] = []
    for item in raw[:10]:
        if isinstance(item, dict):
            name = _safe_text(_first(item, ("name", "sourceFileName", "source_file_name", "subject", "displayName", "title", "normalizedName", "alias")), 90)
            kind = _safe_text(_first(item, ("matchKind", "match_kind", "kind", "reason", "status")) or "possible_match", 80)
            candidate_id = _safe_text(_first(item, ("candidateId", "candidate_id", "id", "sourceFileId", "source_file_id")), 90)
        else:
            name = _safe_text(item, 90)
            kind = "possible_match"
            candidate_id = ""
        if name:
            items.append({"name": name, "matchKind": kind, "candidateId": candidate_id})
    return items


def _is_confirmed_alias_lookup(lookup_result: dict[str, Any]) -> bool:
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    match_text = " ".join(str(x or "") for x in (
        lookup_result.get("matchKind"), data.get("matchKind"), data.get("match_kind"), data.get("aliasStatus"), data.get("alias_status"),
    )).lower()
    return "alias" in match_text and "confirm" in match_text


def _simple_expansion_note(subject: str, matches: list[dict[str, str]]) -> str:
    compact_subject = re.sub(r"[^a-z0-9]", "", (subject or "").lower())
    if not compact_subject:
        return ""
    for item in matches:
        compact_name = re.sub(r"[^a-z0-9]", "", item.get("name", "").lower())
        if compact_name and compact_name != compact_subject and (compact_name.startswith(compact_subject) or compact_name.endswith(compact_subject)):
            return f"This looks like a simple prefix/suffix expansion of “{_safe_text(subject, 60)}”, but it still needs candidateId or exact-name targeting before notes are sent."
    return ""


def _possible_match_review_result(subject: str, lookup_result: dict[str, Any], *, dry_run: bool, resolution_mode: str, match_note: str = "") -> dict[str, Any]:
    matches = _possible_match_items(lookup_result)
    return {
        "ok": True,
        "dryRun": dry_run,
        "subject": _safe_text(subject, 90),
        "status": "possible_match_review",
        "matchKind": "possible_match",
        "matchNote": match_note or "Possible match found, but enrichment needs an exact active Source File target.",
        "possibleMatches": matches,
        "possibleMatchCount": len(matches),
        "possibleMatchNames": [m["name"] for m in matches[:5]],
        "resolutionMode": resolution_mode,
        "sections": {},
        "sourceCounts": {},
        "warningCounts": {},
        "sourceTypes": [],
        "warnings": [],
        "sent": False,
        "sendResult": {},
        "runTime": datetime.now(timezone.utc).isoformat(),
    }


def classify_source_match(lookup_result: dict[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Classify lookup result as active Source File, intake, existing dossier update, or none."""

    if not lookup_result.get("ok"):
        return "error", _safe_text(lookup_result.get("error") or "lookup failed", 140), {}
    if not lookup_result.get("found"):
        return "none", "No active Source File, Candidate Intake item, or existing dossier target was confirmed.", {}
    obj = _source_file_obj(lookup_result)
    data = lookup_result.get("data") if isinstance(lookup_result.get("data"), dict) else {}
    raw = " ".join(str(x or "") for x in (
        lookup_result.get("matchKind"), data.get("matchKind"), obj.get("status"), obj.get("state"), obj.get("reviewStatus"),
        obj.get("kind"), obj.get("type"), obj.get("candidateType"), obj.get("sourceType"), obj.get("dossierStatus"),
    )).lower()
    if "candidate" in raw or "intake" in raw or "needs_review" in raw or "review" in raw:
        return "candidate_intake", "Candidate Intake enrichment only — not active case-file fact.", obj
    if "dossier" in raw or "public" in raw or obj.get("targetDossierId") or obj.get("dossierId"):
        return "existing_dossier_update", "Existing Dossier Update material for admin review.", obj
    return "active_source_file", "Active Source File enrichment target.", obj


def _add_source_file_context(sections: dict[str, list[str]], source_file: dict[str, Any], subject: str) -> None:
    if not source_file:
        return
    name = _first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")) or subject
    _append_section(sections, "Subject Overview", f"Existing Source File target: {name}.")
    _append_section(sections, "Known Roles / Category", _first(source_file, ("candidateType", "sourceType", "type", "kind", "recommendedCategory")) or "")
    _append_section(sections, "Known Facts", _first(source_file, ("knownFacts", "facts", "evidenceSummary", "reason", "summary")) or "")
    _append_section(sections, "Public-Safe Notes", _first(source_file, ("publicSafetyNotes", "safetyNotes", "publicUseNotes")) or "")
    _append_section(sections, "Missing Info", _first(source_file, ("missingInfo", "missing")) or "")
    _append_section(sections, "Do Not Say", _first(source_file, ("doNotSay", "do_not_say")) or "")
    _append_section(sections, "Suggested Next Action", _first(source_file, ("nextRecommendedAction", "suggestedAction", "nextAction")) or "")
    aliases = _first(source_file, ("aliases", "identityLinks", "identity_links", "possibleConnections"))
    if aliases:
        _append_section(sections, "Possible Aliases / Connections", f"Source File lists alias/connection material for review: {_safe_text(aliases, 180)}")


def collect_source_enrichment_evidence(db_path: str, guild_id: int | None, subject: str, *, rd_context: list[dict[str, Any]] | None = None, lookup_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Collect bounded evidence from approved local stores only."""

    terms = _like_terms(subject)
    sections: dict[str, list[str]] = {name: [] for name in SECTION_ORDER}
    source_counts: Counter[str] = Counter()
    warning_counts: Counter[str] = Counter()
    source_types: set[str] = set()
    public_safe_candidates: list[str] = []
    review_only_candidates: list[str] = []

    if lookup_result:
        match_kind, match_note, sf_obj = classify_source_match(lookup_result)
        source_counts["source_file_lookup"] += 1
        source_types.add("source_file_lookup")
        _add_source_file_context(sections, sf_obj, subject)
        _append_section(sections, "Internal-Only / Review-Only Notes", match_note)
    else:
        match_kind = "none"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if _table_exists(conn, "user_profiles"):
            cols = _columns(conn, "user_profiles")
            name_expr = "COALESCE(preferred_name, display_name, '')" if "preferred_name" in cols else "COALESCE(display_name, '')"
            rows = conn.execute(f"SELECT user_id, display_name, {name_expr} AS name FROM user_profiles WHERE guild_id=?", (guild_id,)).fetchall()
            matched_user_ids = []
            for row in rows:
                if _matches_subject(f"{row['display_name']} {row['name']}", terms):
                    matched_user_ids.append(int(row["user_id"]))
                    source_counts["user_profiles"] += 1
                    source_types.add("user_profiles")
                    _append_section(sections, "Subject Overview", f"Local profile/display-name match: {row['name'] or row['display_name']}.")
                    _append_section(sections, "Known Roles / Category", "Discord community subject or participant (from local profile match).")
            matched_user_ids = matched_user_ids[:8]
        else:
            matched_user_ids = []

        if _table_exists(conn, "user_memory_facts"):
            rows = conn.execute("SELECT user_id, fact_key, fact_value, confidence, is_core FROM user_memory_facts WHERE guild_id=? ORDER BY updated_at DESC LIMIT 500", (guild_id,)).fetchall()
            for row in rows:
                text = f"{row['fact_key']}: {row['fact_value']}"
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(text, terms):
                    source_counts["user_memory_facts"] += 1
                    source_types.add("user_memory_facts")
                    conf = float(row["confidence"] or 0)
                    target = "Known Facts" if conf >= 0.75 or int(row["is_core"] or 0) else "Claimed or Inferred Notes"
                    _append_section(sections, target, f"{row['fact_key']}: {row['fact_value']} (confidence {conf:.2f}).")

        if _table_exists(conn, "user_habits"):
            rows = conn.execute("SELECT user_id, total_messages, question_messages, humor_messages, late_night_messages, last_topic FROM user_habits WHERE guild_id=?", (guild_id,)).fetchall()
            for row in rows:
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(row["last_topic"], terms):
                    source_counts["user_habits"] += 1
                    source_types.add("user_habits")
                    _append_section(sections, "Observed Patterns", f"Local habit summary: messages={row['total_messages']}, questions={row['question_messages']}, humor={row['humor_messages']}, late-night={row['late_night_messages']}, last topic={row['last_topic'] or 'unknown'}.")

        if _table_exists(conn, "relationship_state"):
            rows = conn.execute("SELECT user_id, interaction_count, trust_stage, social_stance, last_topic FROM relationship_state WHERE guild_id=?", (guild_id,)).fetchall()
            for row in rows:
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(row["last_topic"], terms):
                    source_counts["relationship_state"] += 1
                    source_types.add("relationship_state")
                    _append_section(sections, "History With BARCODE / BNL / Discord / BARCODE Radio", f"Relationship state: {row['interaction_count']} interactions; stage={row['trust_stage']}; stance={row['social_stance']}; last topic={row['last_topic'] or 'unknown'}.")

        if _table_exists(conn, "relationship_journal"):
            rows = conn.execute("SELECT user_id, entry_type, summary FROM relationship_journal WHERE guild_id=? ORDER BY timestamp DESC LIMIT 300", (guild_id,)).fetchall()
            for row in rows:
                if int(row["user_id"] or 0) in matched_user_ids or _matches_subject(row["summary"], terms):
                    source_counts["relationship_journal"] += 1
                    source_types.add("relationship_journal")
                    _append_section(sections, "History With BARCODE / BNL / Discord / BARCODE Radio", f"Journal {row['entry_type']}: {row['summary']}.")

        if _table_exists(conn, "conversations"):
            rows = conn.execute("SELECT channel_name, channel_policy, role, content FROM conversations WHERE guild_id=? AND COALESCE(channel_policy,'') NOT IN ('dm','direct_message','private_dm') ORDER BY timestamp DESC LIMIT 600", (guild_id,)).fetchall()
            for row in rows:
                if row["role"] == "model":
                    continue
                if _matches_subject(row["content"], terms):
                    source_counts["conversations"] += 1
                    source_types.add("conversations")
                    label = _safe_text(row["channel_policy"] or row["channel_name"] or "approved channel", 60)
                    note = f"Mentioned in {label} context: {_safe_text(row['content'], 150)}"
                    if _WEAK_ALIAS_RE.search(row["content"] or ""):
                        _append_section(sections, "Possible Aliases / Connections", f"Possible connection only, not confirmed: {_safe_text(row['content'], 150)}")
                    else:
                        _append_section(sections, "Claimed or Inferred Notes", note)
                    review_only_candidates.append(note)

        if _table_exists(conn, "memory_tiers"):
            cols = _columns(conn, "memory_tiers")
            trust_col = "source_trust" if "source_trust" in cols else "'legacy_unknown'"
            policy_col = "source_channel_policy" if "source_channel_policy" in cols else "'legacy_unknown'"
            rows = conn.execute(f"SELECT tier, summary, salience, {trust_col} AS source_trust, {policy_col} AS source_channel_policy FROM memory_tiers WHERE guild_id=? ORDER BY salience DESC, updated_at DESC LIMIT 500", (guild_id,)).fetchall()
            for row in rows:
                if _matches_subject(row["summary"], terms):
                    source_counts["memory_tiers"] += 1
                    source_types.add("memory_tiers")
                    trust = str(row["source_trust"] or "legacy_unknown")
                    warning_counts["source_blind_memory"] += 1
                    _append_section(sections, "Internal-Only / Review-Only Notes", f"Source-blind/legacy memory ({row['tier']}, trust={trust}): {_safe_text(row['summary'], 160)}")
                    review_only_candidates.append(_safe_text(row["summary"], 160))

        if _table_exists(conn, "broadcast_memory"):
            rows = conn.execute("SELECT episode_date, cleaned_summary, entry_type, public_safe, usage_scope FROM broadcast_memory WHERE guild_id=? AND status='active' ORDER BY created_at DESC LIMIT 300", (guild_id,)).fetchall()
            for row in rows:
                text = row["cleaned_summary"]
                if _matches_subject(text, terms):
                    source_counts["broadcast_memory"] += 1
                    source_types.add("broadcast_memory")
                    note = f"Broadcast memory {row['episode_date']} ({row['entry_type']}): {text}"
                    _append_section(sections, "History With BARCODE / BNL / Discord / BARCODE Radio", note)
                    if int(row["public_safe"] or 0):
                        _append_section(sections, "Public-Safe Notes", text)
                        public_safe_candidates.append(_safe_text(text, 160))
                    else:
                        _append_section(sections, "Internal-Only / Review-Only Notes", note)

        if _table_exists(conn, "community_presence"):
            rows = conn.execute("SELECT display_name, source_lanes, approved_channel_labels, mention_count, direct_interaction_count, operator_mention_count, connection_notes, evidence_snippets, category FROM community_presence WHERE guild_id=?", (guild_id,)).fetchall()
            for row in rows:
                combined = " ".join(str(row[key] or "") for key in row.keys())
                if _matches_subject(combined, terms):
                    source_counts["community_presence"] += 1
                    source_types.add("community_presence")
                    _append_section(sections, "Observed Patterns", f"Community presence: {row['display_name']} has mention/direct/operator signals {row['mention_count']}/{row['direct_interaction_count']}/{row['operator_mention_count']}.")
                    _append_section(sections, "Known Roles / Category", row["category"] or "community presence candidate")
                    if row["connection_notes"]:
                        _append_section(sections, "Possible Aliases / Connections", f"Community connection note, not confirmed identity: {row['connection_notes']}")
    finally:
        conn.close()

    for item in rd_context or []:
        text = " ".join(str(item.get(k) or "") for k in ("summary", "content", "text", "reason", "title")) if isinstance(item, dict) else str(item)
        if _matches_subject(text, terms):
            source_counts["rd_context"] += 1
            source_types.add("rd_context")
            _append_section(sections, "Why This Subject Matters", f"R&D context flags this subject for operator review: {_safe_text(text, 160)}")
            review_only_candidates.append(_safe_text(text, 160))

    if not sections["Why This Subject Matters"] and (sum(source_counts.values()) > source_counts.get("source_file_lookup", 0)):
        _append_section(sections, "Why This Subject Matters", "BNL has local review signals for this subject; admins should decide what belongs in the Source File.")
    if not sections["Subject Overview"]:
        _append_section(sections, "Subject Overview", f"{subject} is known to this enrichment pass only through bounded local review signals; no confirmed overview is available yet.")
    if not sections["Missing Info"]:
        _append_section(sections, "Missing Info", "Confirmed public-safe summary, owner-approved role/category, and alias certainty are not fully established from supported sources.")
    if not sections["Do Not Say"]:
        _append_section(sections, "Do Not Say", "Do not present review-only memory, possible aliases, private identity, or source-blind notes as confirmed public fact.")
    if not sections["Suggested Next Action"]:
        _append_section(sections, "Suggested Next Action", "Admin should review these notes, confirm which items are public-safe, and attach only supported facts to the Source File.")

    return {
        "sections": {name: values for name, values in sections.items() if values},
        "sourceCounts": dict(source_counts),
        "warningCounts": dict(warning_counts),
        "sourceTypes": sorted(source_types),
        "warnings": ([SOURCE_BLIND_WARNING] if warning_counts.get("source_blind_memory") else []),
        "publicSafeCandidates": public_safe_candidates[:5],
        "reviewOnlyCandidates": review_only_candidates[:5],
    }


def _section_text(sections: dict[str, list[str]], name: str, *, limit: int = 4) -> str:
    items = sections.get(name) or []
    return "\n".join(f"- {_safe_text(item, 240)}" for item in items[:limit]) or "- currently unknown."


def build_enrichment_markdown(packet: dict[str, Any]) -> str:
    sections = packet.get("sections") or {}
    lines: list[str] = []
    for name in SECTION_ORDER:
        if name in sections:
            lines.append(f"## {name}\n{_section_text(sections, name)}")
    return "\n\n".join(lines)


def deterministic_enrichment_ingest_key(subject: str, match_kind: str, sections: dict[str, list[str]]) -> str:
    material = json.dumps({"subject": _subject_key(subject), "matchKind": match_kind, "sections": sections}, sort_keys=True)
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:12]
    return f"bnl:source-enrichment:{_subject_key(subject)}:{match_kind}:{digest}"


def build_enrichment_recommendation_payload(packet: dict[str, Any], *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    sections = packet.get("sections") or {}
    subject = str(packet.get("subject") or "").strip()
    match_kind = str(packet.get("matchKind") or "none")
    source_file = packet.get("sourceFile") if isinstance(packet.get("sourceFile"), dict) else {}
    target_candidate_id = _first(source_file, ("candidateId", "targetCandidateId", "id")) if match_kind in {"candidate_intake", "active_source_file"} else None
    target_dossier_id = _first(source_file, ("targetDossierId", "dossierId", "publicDossierId")) if match_kind == "existing_dossier_update" else None
    evidence = build_enrichment_markdown(packet)
    payload = {
        "type": "modify_existing_dossier" if match_kind in {"active_source_file", "existing_dossier_update", "candidate_intake"} else "new_subject",
        "targetCandidateId": target_candidate_id,
        "targetDossierId": target_dossier_id,
        "subjectName": subject,
        "reason": f"Review-only Source File enrichment generated by BNL. Match target: {match_kind}. No publishing, promotion, merge, or alias confirmation is requested.",
        "evidenceSummary": evidence,
        "publicSafetyNotes": _section_text(sections, "Public-Safe Notes", limit=5),
        "doNotSay": _section_text(sections, "Do Not Say", limit=5),
        "missingInfo": _section_text(sections, "Missing Info", limit=5),
        "suggestedAction": _section_text(sections, "Suggested Next Action", limit=3),
        "sourceLanes": ["source_file_enrichment"] + list(packet.get("sourceTypes") or []),
        "sourceTypes": list(packet.get("sourceTypes") or []),
        "safetyWarnings": list(packet.get("warnings") or []),
        "visibilityLabels": ["internal_review_only", "admin_review_required"],
        "confidence": "medium" if match_kind == "active_source_file" else "low",
        "createdBy": "bnl",
        "ingestSource": source_enrichment_ingest_source(environ),
        "ingestKey": deterministic_enrichment_ingest_key(subject, match_kind, sections),
    }
    return build_dossier_recommendation_payload(payload)


def run_source_file_enrichment(
    db_path: str,
    guild_id: int | None,
    subject: str,
    *,
    dry_run: bool = False,
    rd_context: list[dict[str, Any]] | None = None,
    lookup_func: Callable[[dict[str, str]], dict[str, Any]] = lookup_source_file,
    sender: Callable[[dict[str, Any]], dict[str, Any]] = send_dossier_recommendation,
    environ: dict[str, str] | None = None,
    lookup_key: str = "subject",
    lookup_value: str | None = None,
) -> dict[str, Any]:
    canonical_key = lookup_key if lookup_key in {"subject", "alias", "candidateId", "normalizedName"} else "subject"
    target_value = (lookup_value if lookup_value is not None else subject) or ""
    query = {"lookupKey": canonical_key, "lookupValue": target_value, canonical_key: target_value}
    lookup_result = lookup_func(query)
    match_kind, match_note, source_file = classify_source_match(lookup_result)
    resolution_mode = {"subject": "exact", "candidateId": "candidateId", "alias": "alias", "normalizedName": "exact"}.get(canonical_key, "exact")

    if match_kind == "none":
        possible_matches = _possible_match_items(lookup_result)
        if possible_matches:
            note = "Possible match found, but enrichment needs an exact active Source File target."
            if canonical_key == "alias":
                note = "Alias lookup only returned unconfirmed possible matches; use candidateId after review."
            expansion_note = _simple_expansion_note(target_value, possible_matches)
            if expansion_note:
                note = f"{note} {expansion_note}"
            return _possible_match_review_result(target_value, lookup_result, dry_run=dry_run, resolution_mode="possible_match_review", match_note=note)
        return {
            "ok": True,
            "dryRun": dry_run,
            "subject": _safe_text(target_value, 90),
            "status": "no_target",
            "matchKind": match_kind,
            "matchNote": match_note,
            "possibleMatches": [],
            "possibleMatchCount": 0,
            "possibleMatchNames": [],
            "resolutionMode": "no_target",
            "sections": {},
            "sourceCounts": {},
            "warningCounts": {},
            "sourceTypes": [],
            "warnings": [],
            "sent": False,
            "sendResult": {},
            "runTime": datetime.now(timezone.utc).isoformat(),
        }
    if match_kind == "error":
        return {
            "ok": False,
            "dryRun": dry_run,
            "subject": _safe_text(target_value, 90),
            "status": "lookup_failed",
            "matchKind": match_kind,
            "matchNote": match_note,
            "possibleMatches": [],
            "possibleMatchCount": 0,
            "possibleMatchNames": [],
            "resolutionMode": "no_target",
            "sections": {},
            "sourceCounts": {},
            "warningCounts": {},
            "sourceTypes": [],
            "warnings": [],
            "sent": False,
            "sendResult": {},
            "runTime": datetime.now(timezone.utc).isoformat(),
        }

    if canonical_key == "alias" and not _is_confirmed_alias_lookup(lookup_result):
        possible_result = dict(lookup_result)
        data = possible_result.get("data") if isinstance(possible_result.get("data"), dict) else {}
        source_name = _first(_source_file_obj(possible_result), ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName"))
        data = dict(data)
        data["possibleMatches"] = data.get("possibleMatches") or [{"name": source_name or target_value, "matchKind": lookup_result.get("matchKind") or "unconfirmed_alias", "candidateId": _first(_source_file_obj(possible_result), ("candidateId", "candidate_id", "id"))}]
        possible_result["data"] = data
        return _possible_match_review_result(target_value, possible_result, dry_run=dry_run, resolution_mode="possible_match_review", match_note="Alias lookup was not a confirmed alias match; use candidateId after review.")

    effective_subject = _first(source_file, ("name", "sourceFileName", "subject", "displayName", "title", "normalizedName")) or target_value
    evidence = collect_source_enrichment_evidence(db_path, guild_id, str(effective_subject), rd_context=rd_context, lookup_result=lookup_result)
    packet = {
        "ok": True,
        "dryRun": dry_run,
        "subject": _safe_text(effective_subject, 90),
        "status": "dry_run" if dry_run else "ready_to_send",
        "matchKind": match_kind,
        "matchNote": match_note,
        "sourceFile": source_file,
        "possibleMatches": [],
        "possibleMatchCount": 0,
        "possibleMatchNames": [],
        "resolutionMode": resolution_mode,
        "sections": evidence["sections"],
        "sourceCounts": evidence["sourceCounts"],
        "warningCounts": evidence["warningCounts"],
        "sourceTypes": evidence["sourceTypes"],
        "warnings": evidence["warnings"],
        "runTime": datetime.now(timezone.utc).isoformat(),
    }
    payload = build_enrichment_recommendation_payload(packet, environ=environ)
    packet["payload"] = payload
    packet["ingestKey"] = payload.get("ingestKey")
    packet["ingestSource"] = payload.get("ingestSource")
    if dry_run:
        packet["sent"] = False
        packet["sendResult"] = {}
        return packet
    send_result = sender(payload)
    packet["sendResult"] = send_result
    packet["sent"] = bool((send_result or {}).get("ok"))
    packet["status"] = "sent" if packet["sent"] else "send_failed"
    return packet


def format_source_enrichment_response(result: dict[str, Any]) -> str:
    subject = _safe_text(result.get("subject") or "subject", 90)
    match_kind = _safe_text(result.get("matchKind") or "none", 80)
    if result.get("status") == "possible_match_review":
        matches = result.get("possibleMatches") or []
        lines = [
            f"Source enrichment: No confirmed target found for “{subject}.”",
            "Possible match found, but enrichment needs an exact active Source File target.",
        ]
        if len(matches) == 1:
            item = matches[0]
            candidate = _safe_text(item.get("candidateId"), 90)
            lines.append(f"Possible match found: {_safe_text(item.get('name'), 90)}")
            lines.append(f"Match kind: {_safe_text(item.get('matchKind') or 'possible_match', 80)}")
            target_hint = "Use " + (f"candidateId={candidate} or " if candidate else "") + "exact name to target it."
            lines.append(target_hint)
        elif matches:
            lines.append("Possible matches (narrow with exact name or candidateId):")
            for item in matches[:5]:
                candidate = _safe_text(item.get("candidateId"), 90)
                suffix = f" — candidateId={candidate}" if candidate else ""
                lines.append(f"* {_safe_text(item.get('name'), 90)} — {_safe_text(item.get('matchKind') or 'possible_match', 80)}{suffix}")
        note = _safe_text(result.get("matchNote"), 220)
        if note and note not in lines:
            lines.append(note)
        lines.append("No notes were sent.")
        if len(matches) != 1:
            lines.append("Use exact name or candidateId=<id> to target one active Source File.")
        return "\n".join(lines)[:1900]
    if result.get("status") == "no_target":
        return (f"Source enrichment: No confirmed target found for “{subject}.”\n"
                "Next: create/promote a Candidate Intake item first or run candidate bridge/discovery. No notes were sent.")
    if result.get("status") == "lookup_failed":
        return f"Source enrichment lookup failed for “{subject}”: {_safe_text(result.get('matchNote'), 140)}"
    sections = result.get("sections") or {}
    names = [name for name in SECTION_ORDER if name in sections][:6]
    source_types = ", ".join(result.get("sourceTypes") or []) or "source_file_lookup"
    warnings = result.get("warnings") or []
    warning_text = "; ".join(_safe_text(w, 120) for w in warnings) or "none"
    attach_label = {
        "active_source_file": "active Source File",
        "candidate_intake": "Candidate Intake (intake-only)",
        "existing_dossier_update": "Existing Dossier Update",
    }.get(match_kind, match_kind)
    if result.get("dryRun"):
        preview_lines = [
            f"Dry run Source File enrichment for “{subject}.”",
            f"Match: {match_kind} — {attach_label}.",
            f"Would attach as: {attach_label}.",
            f"Top sections: {', '.join(names) if names else 'none'}.",
            f"Source types used: {source_types}.",
            f"Warnings: {warning_text}.",
            "No raw private transcripts were included; nothing was sent to the website.",
        ]
        return "\n".join(preview_lines)[:1900]
    if result.get("sent"):
        return f"Source enrichment sent for admin review: {subject}. Match: {match_kind}. Ingest key: {_safe_text(result.get('ingestKey'), 120)}."
    err = (result.get("sendResult") or {}).get("error") or "site ingest did not accept the review-only payload"
    return f"Source enrichment was built but not sent for “{subject}”: {_safe_text(err, 150)}"
