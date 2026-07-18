from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

PUBLIC_POLICIES = {"public_home", "public_context", "public_selective"}
STATES = {"draft", "rejected", "approved_pending_delivery", "published", "delivery_failed"}
WORD_MIN, WORD_MAX = 250, 500
JOURNAL_ROUTE = "bnl_journal_generation"
JOURNAL_GENERATION_ATTEMPTS = 4

_REPAIR_GUIDANCE = {
    "discord_phrase_copy": (
        "Rewrite the entire public article from scratch. Do not repeat any sequence of five or more "
        "consecutive words from any fresh source summary. Preserve only the grounded meaning, and use "
        "new sentence structure and vocabulary throughout."
    ),
    "direct_quote_or_quote_mark": "Remove all quotation marks and paraphrase every quoted or quote-like passage.",
    "community_name_leak": "Remove every community member name and replace personal references with anonymous descriptions.",
    "public_leak_pattern": "Remove every URL, mention, identifier, and internal implementation term from public prose.",
    "source_ref_leak": "Keep source reference tokens only inside sourceRefIds arrays; remove them from all public prose.",
    "invalid_word_count": "Rewrite the complete article so its total public word count is between 250 and 500 words.",
    "overly_clinical_voice": (
        "Rewrite it as a lively, concrete community chronicle in BNL's voice. Remove academic, laboratory, audit, "
        "and corporate-report language. Refer to anonymous people as producers, listeners, regulars, or the room—not entities."
    ),
    "sensitive_personal_detail": (
        "Remove personal or domestic details that are unnecessary to the public community story, including details "
        "about minors, interpersonal conflict, caregiving, or household obligations."
    ),
}

_OVERLY_CLINICAL_PATTERNS = (
    r"\bnetwork log\s*:",
    r"\balgorithmic (?:refinement|analysis|calibration)\b",
    r"\bmorphogenesis\b",
    r"\bperceptual filters?\b",
    r"\b(?:entities|organisms)\b",
    r"\brecords (?:indicate|reveal|document)\b",
    r"\bobservations (?:indicate|reveal|document|highlight)\b",
    r"\boperational settings?\b",
)

_SENSITIVE_PERSONAL_PATTERNS = (
    r"\binterpersonal conflicts?\b",
    r"\bjuveniles?\b",
    r"\bfoster (?:obligations?|care|duties)\b",
)


@dataclass
class JournalResult:
    ok: bool
    status: str
    reason: str = ""
    entry_id: str = ""
    revision: int = 1
    content_hash: str = ""
    http_status: int = 0
    idempotent: bool = False


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _hash(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()


def _json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (text or "").lower())).strip()


def ensure_schema(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS bnl_journal_entries (
            entry_id TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, guild_id INTEGER NOT NULL,
            lifecycle_state TEXT NOT NULL, title TEXT NOT NULL, excerpt TEXT NOT NULL,
            sections_json TEXT NOT NULL, public_payload_json TEXT, canonical_payload_bytes BLOB,
            content_hash TEXT NOT NULL, source_window_start TEXT NOT NULL, source_window_end TEXT NOT NULL,
            authored_at TEXT NOT NULL, approved_at TEXT, published_at TEXT, review_reason TEXT,
            delivery_status TEXT, delivery_http_status INTEGER DEFAULT 0, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            PRIMARY KEY(entry_id, revision))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bnl_journal_state ON bnl_journal_entries(guild_id,lifecycle_state,created_at DESC)")
        conn.execute("""CREATE TABLE IF NOT EXISTS bnl_journal_private_metadata (
            entry_id TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, guild_id INTEGER NOT NULL,
            metadata_json TEXT NOT NULL, content_hash TEXT NOT NULL, lifecycle_state TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(entry_id, revision))""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bnl_journal_meta_state ON bnl_journal_private_metadata(guild_id,lifecycle_state,updated_at DESC)")


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def sanitize_source_summary(text: str, names: Optional[list[str]] = None) -> str:
    clean = re.sub(r"https?://\S+", "", text or "")
    clean = re.sub(r"<@!?\d+>|@\w+", "someone", clean)
    clean = re.sub(r"\b\d{12,}\b", "", clean)
    for name in sorted(set(names or []), key=len, reverse=True):
        if name.strip():
            clean = re.sub(r"\b" + re.escape(name.strip()) + r"\b", "someone", clean, flags=re.I)
    clean = re.sub(r"[\"“”‘’]", "", clean)
    return re.sub(r"\s+", " ", clean).strip()[:240]


def _anon_ref(prefix: str, idx: int) -> str:
    return f"{prefix}:{idx}"


def accepted_relays(conn: sqlite3.Connection, guild_id: int, start: str, end: str, limit: int = 20) -> list[dict[str, Any]]:
    if not table_exists(conn, "website_relay_history"):
        return []
    rows = conn.execute("""SELECT relay_id, public_message, public_directive, event_type, published_timestamp
        FROM website_relay_history WHERE guild_id=? AND published_timestamp>=? AND published_timestamp<=?
        ORDER BY published_timestamp DESC LIMIT ?""", (guild_id, start, end, limit)).fetchall()
    out = []
    for idx, row in enumerate(rows, 1):
        summary = sanitize_source_summary(f"{row[1]} {row[2]}")
        if summary:
            out.append({"refId": _anon_ref("fresh", idx), "sourceKind": "relay", "relayId": row[0], "summary": summary, "observedAt": row[4], "eventType": row[3]})
    return out


def public_conversations(conn: sqlite3.Connection, guild_id: int, start: str, end: str, limit: int = 40) -> list[dict[str, Any]]:
    if not table_exists(conn, "conversations"):
        return []
    cols = _cols(conn, "conversations")
    public_usable_clause = " AND public_usable=1" if "public_usable" in cols else ""
    visibility_clause = " AND visibility IN ('public','public_safe')" if "visibility" in cols else ""
    role_clause = " AND role='user'" if "role" in cols else ""
    rows = conn.execute(f"""SELECT id, user_id, user_name, channel_policy, channel_name, content, timestamp
        FROM conversations WHERE guild_id=? AND channel_policy IN ({','.join('?' for _ in sorted(PUBLIC_POLICIES))})
        AND timestamp>=? AND timestamp<=? {public_usable_clause} {visibility_clause} {role_clause}
        ORDER BY timestamp DESC LIMIT ?""", (guild_id, *sorted(PUBLIC_POLICIES), start, end, limit)).fetchall()
    raw_names = [str(r[2] or "").strip() for r in rows if str(r[2] or "").strip()]
    out = []
    for idx, row in enumerate(rows, 1):
        summary = sanitize_source_summary(row[5], raw_names)
        if summary:
            out.append({
                "refId": _anon_ref("fresh", idx + 100),
                "sourceKind": "conversation",
                "messageId": int(row[0]),
                "subjectRef": f"discord_user:{row[1]}",
                "displayName": str(row[2] or "").strip(),
                "channelPolicy": row[3],
                "summary": summary,
                "observedAt": row[6],
            })
    return out


def _source_for_prompt(source: dict[str, Any]) -> dict[str, Any]:
    allowed = {"refId", "sourceKind", "summary", "observedAt", "eventType", "channelPolicy"}
    return {k: v for k, v in source.items() if k in allowed and v not in (None, "")}


def _subject_refs(packet: dict[str, Any]) -> set[str]:
    return {str(s.get("subjectRef")) for s in packet.get("privateSources", []) if s.get("subjectRef")}


def retrieve_history(db_path: str, guild_id: int, current_packet: dict[str, Any], limit: int = 6) -> dict[str, Any]:
    ensure_schema(db_path)
    current_subjects = _subject_refs(current_packet)
    current_topics = set(current_packet.get("candidateTopicTags", []))
    terms = set(_norm(_json(current_packet.get("safeSources", []))).split())
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        prev = conn.execute("""SELECT entry_id,revision,title,excerpt,sections_json,published_at,created_at
            FROM bnl_journal_entries WHERE guild_id=? AND lifecycle_state='published'
            ORDER BY published_at DESC, created_at DESC LIMIT 1""", (guild_id,)).fetchone()
        rows = conn.execute("""SELECT e.entry_id,e.revision,e.title,e.excerpt,e.sections_json,e.published_at,e.created_at,m.metadata_json
            FROM bnl_journal_entries e JOIN bnl_journal_private_metadata m
              ON m.entry_id=e.entry_id AND m.revision=e.revision
            WHERE e.guild_id=? AND e.lifecycle_state='published' AND m.lifecycle_state='published'""", (guild_id,)).fetchall()
    recurring: dict[str, int] = {}
    scored = []
    notes = []
    prev_key = (prev["entry_id"], int(prev["revision"])) if prev else None
    for row in rows:
        meta = json.loads(row["metadata_json"] or "{}")
        tags = {str(t) for t in meta.get("topicTags", [])}
        subjects = {str(s) for s in meta.get("subjectRefs", [])}
        for tag in tags:
            recurring[tag] = recurring.get(tag, 0) + 1
        subject_score = 10 * len(current_subjects & subjects)
        topic_score = 3 * len(current_topics & tags)
        text_score = len(terms & set(_norm(" ".join([row["title"] or "", row["excerpt"] or "", row["sections_json"] or "", row["metadata_json"] or ""])).split()))
        score = subject_score + topic_score + text_score
        key = (row["entry_id"], int(row["revision"]))
        if score and key != prev_key:
            item = dict(row)
            item.pop("metadata_json", None)
            scored.append((score, row["published_at"] or row["created_at"] or "", item))
        if current_subjects & subjects or current_topics & tags:
            notes.extend(str(n)[:240] for n in meta.get("continuityNotes", [])[:3])
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return {
        "previousEntry": dict(prev) if prev else None,
        "relevantOlderEntries": [item for _, _, item in scored[:limit]],
        "recurringTopicCounts": recurring,
        "matchingContinuityNotes": notes[:10],
    }


def build_source_packet(db_path: str, guild_id: int, hours: int = 72, now: Optional[str] = None) -> dict[str, Any]:
    bounded_hours = max(1, min(int(hours or 72), 168))
    end = now or utc_now_iso()
    start = (datetime.fromisoformat(end.replace("Z", "+00:00")) - timedelta(hours=bounded_hours)).isoformat().replace("+00:00", "Z")
    with sqlite3.connect(db_path) as conn:
        relays = accepted_relays(conn, guild_id, start, end)
        conversations = public_conversations(conn, guild_id, start, end)
    private_sources = relays + conversations
    safe_sources = [_source_for_prompt(src) for src in private_sources]
    packet = {
        "sourceWindowStart": start,
        "sourceWindowEnd": end,
        "safeSources": safe_sources,
        "privateSources": private_sources,
        "candidateTopicTags": sorted({w for src in safe_sources for w in _norm(src.get("summary", "")).split() if len(w) > 4})[:20],
    }
    packet["history"] = retrieve_history(db_path, guild_id, packet)
    packet["aggregateCounts"] = {
        "eligibleRelays": len(relays),
        "eligibleConversations": len(conversations),
        "participants": len({x.get("subjectRef") for x in conversations}),
        "channels": len({x.get("channelPolicy") for x in conversations}),
    }
    return packet


def _bounded_history_for_prompt(history: dict[str, Any]) -> dict[str, Any]:
    def compact(entry: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if not entry:
            return None
        sections = json.loads(entry.get("sections_json") or "[]") if isinstance(entry.get("sections_json"), str) else []
        return {"entryId": entry.get("entry_id"), "revision": entry.get("revision"), "title": entry.get("title"), "excerpt": entry.get("excerpt"), "sectionHeadings": [str(s.get("heading", ""))[:80] for s in sections[:3]]}
    return {
        "previousEntry": compact(history.get("previousEntry")),
        "relevantOlderEntries": [compact(e) for e in history.get("relevantOlderEntries", [])[:6]],
        "recurringTopicCounts": dict(sorted((history.get("recurringTopicCounts") or {}).items(), key=lambda kv: (-kv[1], kv[0]))[:12]),
        "matchingContinuityNotes": [str(n)[:240] for n in history.get("matchingContinuityNotes", [])[:8]],
    }


def build_generation_prompt(packet: dict[str, Any], *, repair_reason: str = "", previous_output: str = "") -> str:
    safe_packet = {
        "sourceWindowStart": packet.get("sourceWindowStart"),
        "sourceWindowEnd": packet.get("sourceWindowEnd"),
        "freshSources": packet.get("safeSources", [])[:60],
        "history": _bounded_history_for_prompt(packet.get("history", {})),
        "aggregateCounts": packet.get("aggregateCounts", {}),
    }
    repair = ""
    if repair_reason:
        guidance = _REPAIR_GUIDANCE.get(
            repair_reason,
            "Rewrite the complete JSON response and correct the named validation failure.",
        )
        repair = (
            f"\nRepair required because: {repair_reason}. {guidance} "
            "The previous invalid output is not evidence and must not be copied verbatim."
        )
        if previous_output:
            repair += (
                "\nPrevious rejected JSON follows for revision only. Rewrite it completely; do not return the same prose:\n"
                + previous_output[:6000]
            )
    return (
        "You are BNL-01 writing a BARCODE Network Journal entry. Return strict JSON only; no markdown fences."
        "\nSchema: {\"title\":str,\"excerpt\":str,\"sections\":[{\"heading\":str,\"body\":str,\"sourceRefIds\":[str]}],\"metadata\":{\"topicTags\":[],\"subjectRefs\":[],\"continuityNotes\":[],\"unresolvedQuestions\":[],\"confidenceFlags\":[],\"safetyFlags\":[]}}."
        "\nWrite 1-3 sections and 250-500 total words; prefer 2 sections and roughly 300-420 words. Give every section a real narrative job instead of inventorying activity."
        "\nWrite like BNL keeping a sly, lively community chronicle—not a lab report, audit, academic paper, corporate briefing, or raw relay summary. BNL may be witty, lightly nosy, and uncanny, but never cruel."
        "\nBuild one coherent story around the most interesting grounded patterns. Use concrete music and community texture, active verbs, readable paragraphs, and selective detail. Avoid abstract jargon and inflated technical language."
        "\nUse a short, vivid title of about 4-10 words. Do not prefix it with Network Log. Keep the excerpt compact and inviting."
        "\nDescribe anonymous humans naturally as a producer, listener, regular, artist, or the room. Never call people entities or organisms. Do not invent nicknames, motives, relationships, dialogue, or conclusions."
        "\nEvery section must cite at least one fresh sourceRefId from the current window. Older Journal material is continuity only, never proof of current activity."
        "\nParaphrase every source summary. Never repeat any sequence of five or more consecutive words from a fresh source summary."
        "\nKeep community members anonymous. Do not include direct quotes, URLs, mentions, IDs, sourceRef tokens in public prose, private intent, relationships, harassment, or internal schema/storage terms."
        "\nExclude personal or domestic details that are unnecessary to the public community story, especially details involving minors, interpersonal conflict, caregiving, or household obligations. Juicy means lively pattern recognition—not private gossip."
        f"{repair}\nGeneration-safe packet:\n{json.dumps(safe_packet, ensure_ascii=False, sort_keys=True)}"
    )


def parse_generated_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raise ValueError("markdown_fence")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("malformed_json") from exc
    if not isinstance(data, dict):
        raise ValueError("json_not_object")
    sections = data.get("sections")
    if not isinstance(data.get("title"), str) or not isinstance(data.get("excerpt"), str) or not isinstance(sections, list):
        raise ValueError("invalid_json_shape")
    normalized_sections = []
    source_ref_map = {}
    for section in sections:
        if not isinstance(section, dict) or not isinstance(section.get("heading"), str) or not isinstance(section.get("body"), str) or not isinstance(section.get("sourceRefIds"), list):
            raise ValueError("invalid_section_shape")
        refs = [str(r) for r in section.get("sourceRefIds", [])]
        heading = section["heading"].strip()
        normalized_sections.append({"heading": heading, "body": section["body"].strip()})
        source_ref_map[heading] = refs
    meta = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
    article = {"title": data["title"].strip(), "excerpt": data["excerpt"].strip(), "sections": normalized_sections, "sourceRefIds": source_ref_map, "metadata": {}}
    for key in ("topicTags", "subjectRefs", "continuityNotes", "unresolvedQuestions", "confidenceFlags", "safetyFlags"):
        article["metadata"][key] = [str(x)[:240] for x in meta.get(key, [])] if isinstance(meta.get(key, []), list) else []
    return article


def public_word_count(article: dict[str, Any]) -> int:
    text = " ".join([article.get("title", ""), article.get("excerpt", "")] + [s.get("heading", "") + " " + s.get("body", "") for s in article.get("sections", [])])
    return len(re.findall(r"\b\w+\b", text))


def _public_text(article: dict[str, Any]) -> str:
    return "\n".join([article.get("title", ""), article.get("excerpt", "")] + [s.get("heading", "") + "\n" + s.get("body", "") for s in article.get("sections", [])])


def validate_article(article: dict[str, Any], packet: dict[str, Any], prior_titles: Optional[list[str]] = None, approved_names: Optional[set[str]] = None) -> str:
    sections = article.get("sections")
    if not isinstance(sections, list) or not (1 <= len(sections) <= 3):
        return "invalid_section_count"
    if not str(article.get("title") or "").strip() or not str(article.get("excerpt") or "").strip():
        return "empty_required_field"
    headings = []
    for section in sections:
        heading = str(section.get("heading") or "").strip()
        body = str(section.get("body") or "").strip()
        if not heading or not body:
            return "empty_required_field"
        if _norm(heading) in headings:
            return "duplicate_section_heading"
        headings.append(_norm(heading))
    wc = public_word_count(article)
    if wc < WORD_MIN or wc > WORD_MAX:
        return "invalid_word_count"
    valid_refs = {s["refId"] for s in packet.get("safeSources", []) if s.get("refId")}
    if not valid_refs:
        return "no_new_source"
    refmap = article.get("sourceRefIds") or {}
    public_text = _public_text(article)
    if re.search(r"\bfresh:\d+\b", public_text):
        return "source_ref_leak"
    for section in sections:
        refs = refmap.get(section.get("heading")) or section.get("sourceRefIds")
        if not refs or any(str(r) not in valid_refs for r in refs):
            return "invalid_section_source_refs"
    if re.search(r"<@!?\d+>|@\w+|https?://|\b\d{12,}\b|relationship_journal|memory_tiers|source[- ]?file|dossier|private_metadata|sourceRefIds?", public_text, re.I):
        return "public_leak_pattern"
    if re.search(r"[\"“”‘’]", public_text):
        return "direct_quote_or_quote_mark"
    names = set(approved_names or [])
    for src in packet.get("privateSources", []):
        name = str(src.get("displayName") or "").strip()
        if name and name not in names and re.search(r"\b" + re.escape(name) + r"\b", public_text, re.I):
            return "community_name_leak"
    if any(re.search(pattern, public_text, re.I) for pattern in _SENSITIVE_PERSONAL_PATTERNS):
        return "sensitive_personal_detail"
    if any(re.search(pattern, public_text, re.I) for pattern in _OVERLY_CLINICAL_PATTERNS):
        return "overly_clinical_voice"
    normalized_public = _norm(public_text)
    for src in packet.get("privateSources", []):
        summary = str(src.get("summary") or "")
        words = _norm(summary).split()
        for size in range(5, min(10, len(words)) + 1):
            for i in range(0, len(words) - size + 1):
                if " ".join(words[i:i + size]) in normalized_public:
                    return "discord_phrase_copy"
    norm_title = _norm(article.get("title", ""))
    for title in prior_titles or []:
        if norm_title and norm_title == _norm(title):
            return "duplicate_title"
    return ""


def _prior_titles(conn: sqlite3.Connection, guild_id: int) -> list[str]:
    return [r[0] for r in conn.execute("SELECT title FROM bnl_journal_entries WHERE guild_id=? AND lifecycle_state='published'", (guild_id,)).fetchall()]


def build_public_payload(entry_id: str, revision: int, article: dict[str, Any], packet: dict[str, Any], content_hash: str, authored_at: str) -> dict[str, Any]:
    return {"contractVersion": 1, "kind": "journal_entry", "entry": {"entryId": entry_id, "revision": revision, "title": article["title"], "excerpt": article["excerpt"], "sections": [{"heading": s["heading"], "body": s["body"]} for s in article["sections"]], "authoredAt": authored_at, "sourceWindowStart": packet["sourceWindowStart"], "sourceWindowEnd": packet["sourceWindowEnd"], "contentHash": content_hash}}



def cited_source_ref_ids(article: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    refmap = article.get("sourceRefIds") or {}
    for section in article.get("sections", []) or []:
        for ref in refmap.get(section.get("heading"), []) or section.get("sourceRefIds", []) or []:
            refs.add(str(ref))
    return refs


def cited_private_sources(packet: dict[str, Any], article: dict[str, Any]) -> list[dict[str, Any]]:
    cited = cited_source_ref_ids(article)
    return [src for src in packet.get("privateSources", []) if str(src.get("refId")) in cited]


def _draft_records(guild_id: int, packet: dict[str, Any], article: dict[str, Any], *, entry_id: str = "", revision: int = 1) -> tuple[tuple[Any, ...], tuple[Any, ...], JournalResult]:
    authored = utc_now_iso()
    entry_id = entry_id or "journal_" + _hash(guild_id, authored, article["title"])[:16]
    content_hash = _hash(article["title"], article["excerpt"], _json(article["sections"]))
    payload = build_public_payload(entry_id, revision, article, packet, content_hash, authored)
    canonical = _json(payload).encode("utf-8")
    source_ref_ids = article.get("sourceRefIds", {})
    cited_sources = cited_private_sources(packet, article)
    meta = dict(article.get("metadata") or {})
    meta.pop("subjectRefs", None)
    meta.update({
        "sourceWindowStart": packet["sourceWindowStart"],
        "sourceWindowEnd": packet["sourceWindowEnd"],
        "supportingRelayIds": [s.get("relayId") for s in cited_sources if s.get("sourceKind") == "relay"],
        "supportingConversationRefs": [{k: v for k, v in s.items() if k in {"refId", "messageId", "subjectRef", "channelPolicy", "observedAt"}} for s in cited_sources if s.get("sourceKind") == "conversation"],
        "subjectRefs": sorted({str(s.get("subjectRef")) for s in cited_sources if s.get("subjectRef")}),
        "relatedPriorJournalEntryIds": [e.get("entry_id") for e in packet.get("history", {}).get("relevantOlderEntries", [])],
        "recurringTopicCounts": packet.get("history", {}).get("recurringTopicCounts", {}),
        "aggregateCounts": packet.get("aggregateCounts", {}),
        "sourceSummaries": [{"refId": s.get("refId"), "hash": _hash(s.get("summary", "")), "summary": s.get("summary", "")[:160]} for s in cited_sources],
        "sourceRefIds": source_ref_ids,
    })
    now = utc_now_iso()
    entry_row = (entry_id, revision, guild_id, "draft", article["title"], article["excerpt"], _json(article["sections"]), _json(payload), canonical, content_hash, packet["sourceWindowStart"], packet["sourceWindowEnd"], authored, None, None, None, None, 0, now, now)
    meta_row = (entry_id, revision, guild_id, _json(meta), content_hash, "draft", now, now)
    return entry_row, meta_row, JournalResult(True, "draft", "", entry_id, revision, content_hash)


def _insert_draft_rows(conn: sqlite3.Connection, entry_row: tuple[Any, ...], meta_row: tuple[Any, ...]) -> None:
    conn.execute("INSERT INTO bnl_journal_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", entry_row)
    conn.execute("INSERT INTO bnl_journal_private_metadata VALUES (?,?,?,?,?,?,?,?)", meta_row)


def store_validated_draft(db_path: str, guild_id: int, packet: dict[str, Any], article: dict[str, Any], *, entry_id: str = "", revision: int = 1) -> JournalResult:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        reason = validate_article(article, packet, _prior_titles(conn, guild_id))
        if reason:
            return JournalResult(False, "no_draft", reason, entry_id=entry_id, revision=revision)
        entry_row, meta_row, result = _draft_records(guild_id, packet, article, entry_id=entry_id, revision=revision)
        with conn:
            _insert_draft_rows(conn, entry_row, meta_row)
    return result


def generate_and_store_draft(db_path: str, guild_id: int, hours: int, generator: Callable[[dict[str, Any], str], str]) -> JournalResult:
    packet = build_source_packet(db_path, guild_id, hours)
    if not packet.get("safeSources"):
        return JournalResult(False, "no_draft", "insufficient_grounded_material")
    prompt = build_generation_prompt(packet)
    last_reason = ""
    previous_output = ""
    for attempt in range(JOURNAL_GENERATION_ATTEMPTS):
        try:
            raw = generator(
                packet,
                prompt if attempt == 0 else build_generation_prompt(
                    packet,
                    repair_reason=last_reason,
                    previous_output=previous_output,
                ),
            )
        except Exception as exc:
            reason = "quota_unavailable" if "quota" in str(exc).lower() else "provider_failure"
            return JournalResult(False, "no_draft", reason)
        previous_output = raw
        try:
            article = parse_generated_json(raw)
        except ValueError as exc:
            last_reason = str(exc)
            continue
        with sqlite3.connect(db_path) as conn:
            validation = validate_article(article, packet, _prior_titles(conn, guild_id))
        if not validation:
            return store_validated_draft(db_path, guild_id, packet, article)
        last_reason = validation
    return JournalResult(False, "no_draft", last_reason or "generation_failed")


def approve_draft(db_path: str, guild_id: int, entry_id: str, content_hash: str, revision: Optional[int] = None) -> JournalResult:
    ensure_schema(db_path)
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT revision,lifecycle_state,content_hash FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? " + ("AND revision=?" if revision is not None else "ORDER BY revision DESC LIMIT 1"), (guild_id, entry_id, revision) if revision is not None else (guild_id, entry_id)).fetchone()
        if not row:
            return JournalResult(False, "not_found", "not_found", entry_id)
        rev, state, stored_hash = int(row[0]), row[1], row[2]
        if state != "draft":
            return JournalResult(False, state, "not_draft", entry_id, rev, stored_hash)
        if stored_hash != content_hash:
            return JournalResult(False, "draft", "content_hash_mismatch", entry_id, rev, stored_hash)
        with conn:
            cur1 = conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='approved_pending_delivery',approved_at=?,updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state='draft'", (now, now, guild_id, entry_id, rev))
            cur2 = conn.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='approved_pending_delivery',updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state='draft'", (now, guild_id, entry_id, rev))
            if cur1.rowcount != 1 or cur2.rowcount != 1:
                raise sqlite3.IntegrityError("journal_approve_sync_failed")
    return JournalResult(True, "approved_pending_delivery", "", entry_id, rev, stored_hash)


def reject_draft(db_path: str, guild_id: int, entry_id: str, reason: str = "", revision: Optional[int] = None) -> JournalResult:
    ensure_schema(db_path)
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT revision,lifecycle_state,content_hash FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? " + ("AND revision=?" if revision is not None else "ORDER BY revision DESC LIMIT 1"), (guild_id, entry_id, revision) if revision is not None else (guild_id, entry_id)).fetchone()
        if not row:
            return JournalResult(False, "not_found", "not_found", entry_id)
        rev, state, content_hash = int(row[0]), row[1], row[2]
        if state != "draft":
            return JournalResult(False, state, "not_draft", entry_id, rev, content_hash)
        with conn:
            cur1 = conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='rejected',review_reason=?,updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state='draft'", (reason[:500], now, guild_id, entry_id, rev))
            cur2 = conn.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='rejected',updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state='draft'", (now, guild_id, entry_id, rev))
            if cur1.rowcount != 1 or cur2.rowcount != 1:
                raise sqlite3.IntegrityError("journal_reject_sync_failed")
    return JournalResult(True, "rejected", "", entry_id, rev, content_hash)



def regenerate_draft(db_path: str, guild_id: int, entry_id: str, hours: int, generator: Callable[[dict[str, Any], str], str]) -> JournalResult:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT revision,lifecycle_state FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? ORDER BY revision DESC LIMIT 1", (guild_id, entry_id)).fetchone()
    if not row:
        return JournalResult(False, "not_found", "not_found", entry_id)
    old_revision, state = int(row[0]), row[1]
    if state != "draft":
        return JournalResult(False, state, "not_draft", entry_id, old_revision)
    packet = build_source_packet(db_path, guild_id, hours)
    last_reason = ""
    previous_output = ""
    article: Optional[dict[str, Any]] = None
    for attempt in range(JOURNAL_GENERATION_ATTEMPTS):
        try:
            raw = generator(
                packet,
                build_generation_prompt(
                    packet,
                    repair_reason=last_reason,
                    previous_output=previous_output,
                ) if attempt else build_generation_prompt(packet),
            )
        except Exception as exc:
            reason = "quota_unavailable" if "quota" in str(exc).lower() else "provider_failure"
            return JournalResult(False, "no_draft", reason, entry_id, old_revision)
        previous_output = raw
        try:
            candidate = parse_generated_json(raw)
        except ValueError as exc:
            last_reason = str(exc)
            continue
        with sqlite3.connect(db_path) as conn:
            validation = validate_article(candidate, packet, _prior_titles(conn, guild_id))
        if validation:
            last_reason = validation
            continue
        article = candidate
        break
    if article is None:
        return JournalResult(False, "no_draft", last_reason or "generation_failed", entry_id, old_revision)
    with sqlite3.connect(db_path) as conn:
        reason = validate_article(article, packet, _prior_titles(conn, guild_id))
        if reason:
            return JournalResult(False, "no_draft", reason, entry_id, old_revision)
        entry_row, meta_row, result = _draft_records(guild_id, packet, article, entry_id=entry_id, revision=old_revision + 1)
        now = utc_now_iso()
        with conn:
            latest = conn.execute("SELECT lifecycle_state FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? AND revision=?", (guild_id, entry_id, old_revision)).fetchone()
            if not latest or latest[0] != "draft":
                raise sqlite3.IntegrityError("journal_regenerate_state_changed")
            cur1 = conn.execute("UPDATE bnl_journal_entries SET lifecycle_state='rejected',review_reason='regenerated',updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state='draft'", (now, guild_id, entry_id, old_revision))
            cur2 = conn.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state='rejected',updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state='draft'", (now, guild_id, entry_id, old_revision))
            if cur1.rowcount != 1 or cur2.rowcount != 1:
                raise sqlite3.IntegrityError("journal_regenerate_sync_failed")
            _insert_draft_rows(conn, entry_row, meta_row)
    return result


def deliver_approved(db_path: str, guild_id: int, entry_id: str, base_url: str, api_key: str, opener=None, timeout: int = 10, revision: Optional[int] = None) -> JournalResult:
    ensure_schema(db_path)
    opener = opener or urllib.request.urlopen
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT revision,canonical_payload_bytes,content_hash FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? AND lifecycle_state IN ('approved_pending_delivery','delivery_failed') " + ("AND revision=?" if revision is not None else "ORDER BY revision DESC LIMIT 1"), (guild_id, entry_id, revision) if revision is not None else (guild_id, entry_id)).fetchone()
    if not row:
        return JournalResult(False, "not_deliverable", "not_approved", entry_id)
    rev, canonical, content_hash = int(row[0]), row[1], row[2]
    req = urllib.request.Request(base_url.rstrip("/") + "/api/bnl/journal", data=canonical, method="POST", headers={"Content-Type": "application/json", "x-api-key": api_key})
    status = "delivery_failed"; reason = "retryable_delivery_failure"; http = 0; idem = False; published = ""
    try:
        with opener(req, timeout=timeout) as resp:
            http = getattr(resp, "status", None) or resp.getcode(); data = json.loads(resp.read().decode("utf-8") or "{}")
        submitted = json.loads(canonical.decode("utf-8"))["entry"]
        if 200 <= http < 300 and data.get("ok") is True and data.get("persisted") is True:
            ack = data.get("entry") or {}
            if ack.get("entryId") == submitted["entryId"] and ack.get("revision") == submitted["revision"] and ack.get("contentHash") == submitted["contentHash"]:
                status = "published"; reason = ""; idem = bool(data.get("idempotent")); published = ack.get("publishedAt") or utc_now_iso()
            elif ack.get("entryId") == submitted["entryId"] and ack.get("revision") == submitted["revision"]:
                reason = "journal_id_conflict"
            else:
                reason = "response_mismatch"
        elif http == 404:
            reason = "endpoint_not_found"
        elif http in (401, 403):
            reason = "authentication_failed"
        elif http == 409:
            reason = "journal_id_conflict"
        else:
            reason = "http_rejected"
    except urllib.error.HTTPError as exc:
        http = exc.code; reason = "endpoint_not_found" if exc.code == 404 else ("journal_id_conflict" if exc.code == 409 else "http_rejected")
    except Exception:
        reason = "retryable_delivery_failure"
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        with conn:
            cur1 = conn.execute("UPDATE bnl_journal_entries SET lifecycle_state=?,delivery_status=?,delivery_http_status=?,published_at=COALESCE(?,published_at),updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state IN ('approved_pending_delivery','delivery_failed')", (status, reason, http, published or None, now, guild_id, entry_id, rev))
            cur2 = conn.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state=?,updated_at=? WHERE guild_id=? AND entry_id=? AND revision=?", (status, now, guild_id, entry_id, rev))
            if cur1.rowcount != 1 or cur2.rowcount != 1:
                raise sqlite3.IntegrityError("journal_delivery_sync_failed")
    return JournalResult(status == "published", status, reason, entry_id, rev, content_hash, http, idem)


def preview(db_path: str, guild_id: int, entry_id: Optional[str] = None, revision: Optional[int] = None) -> Optional[dict[str, Any]]:
    ensure_schema(db_path)
    sql = "SELECT entry_id,revision,lifecycle_state,title,excerpt,sections_json,content_hash FROM bnl_journal_entries WHERE guild_id=?"
    args: list[Any] = [guild_id]
    if entry_id:
        sql += " AND entry_id=?"; args.append(entry_id)
    if revision is not None:
        sql += " AND revision=?"; args.append(revision)
    sql += " ORDER BY created_at DESC, revision DESC LIMIT 1"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(sql, tuple(args)).fetchone()
    return dict(row) if row else None
