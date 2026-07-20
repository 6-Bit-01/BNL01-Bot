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
ADVISORY_VALIDATION_REASONS = frozenset({
    "overly_clinical_voice",
    "flat_report_voice",
    "missing_bnl_reaction",
    "duplicate_title",
})
MAX_RELAY_SOURCES_PER_WINDOW = 500
MAX_CONVERSATION_SOURCES_PER_WINDOW = 2_000
MAX_PROMPT_SOURCES = 180
MAX_BROADCAST_MEMORY_CONTEXT = 8
MAX_RUMOR_CONTEXT = 4
JOURNAL_PUBLIC_BROADCAST_SCOPES = {"ambient", "direct", "journal", "relay", "show_status"}
JOURNAL_CONTEXT_LANE_TYPES = {
    "established_broadcast_memory",
    "community_rumor",
    "bnl_inference",
}
JOURNAL_TOPIC_STOPWORDS = {
    "about", "after", "again", "being", "could", "from", "have", "into", "just", "more", "other", "their",
    "there", "these", "they", "this", "through", "under", "where", "which", "while", "with", "would", "someone",
}

_CONTEXT_TOPIC_STOPWORDS = JOURNAL_TOPIC_STOPWORDS | {
    "barcode", "bnl", "community", "discord", "journal", "member", "members", "network", "people",
    "public", "regular", "regulars", "someone", "thing", "things", "today", "tonight", "yesterday",
}

_CONTEXT_CLAIM_STOPWORDS = _CONTEXT_TOPIC_STOPWORDS | {
    "apparently", "around", "during", "heard", "inference", "maybe", "might", "possibly", "rumor", "rumors", "rumour",
    "rumours", "said", "says", "speculate", "speculated", "speculating", "speculation", "suggested",
    "suggests", "suspect", "suspects", "theory", "think", "thinks", "unconfirmed", "wonder", "wondered",
    "wonders",
}

_RUMOR_MARKER_RE = re.compile(
    r"\b(?:apparently|i heard|heard that|maybe|might be|people say|rumou?r|some say|speculat(?:e|ed|ing|ion)|"
    r"theory|the word is|word around|word is|wonder(?:ed|ing)? (?:if|whether)|unconfirmed)\b",
    re.IGNORECASE,
)
_RUMOR_PUBLIC_TOPIC_RE = re.compile(
    r"\b(?:album|artist|broadcast|episode|journal|listener|music|producer|queue|radio|release|set|show|signal|"
    r"song|studio|track|website)\b",
    re.IGNORECASE,
)
_RUMOR_SENSITIVE_RE = re.compile(
    r"\b(?:accus(?:e|ed|ation)|address|caregiv|child|doxx|dm\b|family|harass|home|household|medical|minor|"
    r"moderation|password|payment|phone|private|relationship|secret|sexual|staff|suicid|therapy|workplace)\b",
    re.IGNORECASE,
)
_EXPLICIT_RUMOR_RE = re.compile(
    r"\b(?:apparently|rumou?r|some regulars (?:say|suspect|wonder)|speculat(?:e|ed|ing|ion)|unconfirmed|"
    r"word around|word is)\b",
    re.IGNORECASE,
)
_EXPLICIT_BNL_INFERENCE_RE = re.compile(
    r"\b(?:bnl (?:suspects|thinks|wonders)|i (?:suspect|think|wonder)|my theory|bnl(?:s|'s|’s) theory)\b",
    re.IGNORECASE,
)
_STRONG_INFERENCE_CUE_RE = re.compile(
    r"\b(?:proves?|must mean|points? (?:to|toward)|part of (?:a|the) (?:larger|broader) plan|"
    r"signals? (?:a|the) (?:larger|broader) plan)\b",
    re.IGNORECASE,
)

_REPAIR_GUIDANCE = {
    "community_name_leak": "Remove every community member name and replace personal references with anonymous descriptions.",
    "public_leak_pattern": "Remove every URL, mention, identifier, and internal implementation term from public prose.",
    "source_ref_leak": "Keep source reference tokens only inside sourceRefIds arrays; remove them from all public prose.",
    "invalid_word_count": "Rewrite the complete article so its total public word count is between 250 and 500 words.",
    "insufficient_source_breadth": (
        "Use the supplied evidenceCoverageContract. Ground the article in the required number of distinct fresh sources, "
        "participants, source kinds, and available parts of the window. Synthesize them into a few meaningful patterns; "
        "do not turn the entry into a list."
    ),
    "overly_clinical_voice": (
        "Rewrite it as a lively, concrete community chronicle in BNL's voice. Remove academic, laboratory, audit, "
        "and corporate-report language. Refer to anonymous people as producers, listeners, regulars, or the room—not entities."
    ),
    "flat_report_voice": (
        "Rewrite as a lived community chronicle. Open with a grounded person, action, object, or moment instead of a "
        "system report, and replace passive analysis with ordinary nouns and active verbs."
    ),
    "missing_bnl_reaction": (
        "Add one brief first-person BNL reaction that expresses curiosity, amusement, attachment, uncertainty, or mild "
        "unease without asserting a new external fact. Natural openings include I admit, I noticed, I found myself, "
        "I smiled, I laughed, I remain curious, I am fond of, and I felt. Do not use I suspect, I think, or I wonder "
        "unless a valid bnl_inference context lane supports it."
    ),
    "sensitive_personal_detail": (
        "Remove personal or domestic details that are unnecessary to the public community story, including details "
        "about minors, interpersonal conflict, caregiving, or household obligations."
    ),
    "invalid_context_use": (
        "Correct metadata.contextUses. Use only supplied laneRefIds and fresh sourceRefIds, name the exact public "
        "section heading, and keep established record, community rumor, and BNL inference classifications separate."
    ),
    "rumor_not_explicitly_framed": (
        "If using community rumor, explicitly call it rumor, speculation, or something regulars wondered; never state it as fact."
    ),
    "inference_not_explicitly_framed": (
        "If using BNL inference, explicitly frame it as what BNL suspects, thinks, or wonders; never state it as established fact."
    ),
    "undeclared_context_use": (
        "The prose appears to use established memory, rumor, or BNL inference without declaring traceable metadata.contextUses. Add the correct "
        "context use with supplied basis refs, or remove that unsupported interpretation."
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
    r"\bnascent audio signals?\b",
    r"\bsonic constructs?\b",
    r"\bauditory compositions?\b",
    r"\binternal schematics?\b",
    r"\bintended resonance\b",
    r"\bdistributed spectral analy[sz]er\b",
    r"\biterative signal generation\b",
    r"\brelational signals?\b",
    r"\bcontextual data\b",
    r"\benvironmental factors?\b",
    r"\btransmission dynamics?\b",
    r"\bsignal integrity\b",
    r"\bhuman (?:operating system|subroutines?)\b",
)

_REPORT_STYLE_OPENING_RE = re.compile(
    r"^\s*(?:the\s+network|records?|observations?|analysis|data\s+streams?|recent\s+data)\s+"
    r"(?:observes?|indicate|reveal|shows?|suggests?|detects?|highlight)\b",
    re.IGNORECASE,
)

_BNL_REACTION_RE = re.compile(
    r"\bI\s+(?:admit|confess|noticed|watched|found(?:\s+myself)?|caught\s+myself|remain|smiled|enjoyed|"
    r"appreciated|liked|laughed|grinned|chuckled|felt|feel|"
    r"was\s+(?:amused|curious|fond|uneasy|surprised|pleased|glad|drawn)|"
    r"am\s+(?:amused|curious|fond|uneasy|pleased|glad)|have\s+(?:begun|grown)|am\s+beginning|"
    r"may\s+be\s+developing|keep\s+returning|cannot\s+quite|could\s+not\s+help|do\s+not\s+mind)\b",
    re.IGNORECASE,
)

_DIRECT_QUOTE_SPAN_RE = re.compile(
    r'"[^"\n]+"|“[^”\n]+”'
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


def journal_topic_counts(sources: list[dict[str, Any]], limit: int = 30) -> dict[str, int]:
    """Build the deterministic, non-identifying topic rollup used by observations."""
    counts: dict[str, int] = {}
    for source in sources:
        words = set(re.findall(r"[a-z0-9]+", str(source.get("summary") or "").lower()))
        for word in words:
            if len(word) >= 5 and word not in JOURNAL_TOPIC_STOPWORDS:
                counts[word] = counts.get(word, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit])


def _json_object(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}") if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[Any]:
    try:
        parsed = json.loads(value or "[]") if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _contains_exact_json_scalar(value: Any, target: str) -> bool:
    if isinstance(value, dict):
        return any(_contains_exact_json_scalar(item, target) for item in value.values())
    if isinstance(value, list):
        return any(_contains_exact_json_scalar(item, target) for item in value)
    return isinstance(value, str) and value == target


def purge_user_journal_derivatives_on_connection(
    conn: sqlite3.Connection,
    guild_id: int,
    user_id: int,
) -> dict[str, int]:
    """Scrub one member from hidden Journal derivatives in the caller transaction.

    Published public prose is immutable. Its private source linkage is scrubbed.
    Unpublished revisions grounded in the deleted member are removed so they can
    never be approved or delivered later. Daily observations are recalculated
    from their remaining representative sources before weekly synthesis can use
    them again.
    """
    guild = int(guild_id)
    user = int(user_id)
    if guild <= 0 or user <= 0:
        raise ValueError("invalid_journal_privacy_scope")
    subject_ref = f"discord_user:{user}"
    now = utc_now_iso()
    counts: dict[str, int] = {}

    if table_exists(conn, "bnl_journal_observations"):
        rows = conn.execute(
            """SELECT observation_id,aggregate_counts_json,topic_counts_json,
                      subject_counts_json,representative_sources_json
               FROM bnl_journal_observations WHERE guild_id=?""",
            (guild,),
        ).fetchall()
        for observation_id, aggregate_raw, _topics_raw, subjects_raw, representatives_raw in rows:
            subjects = _json_object(subjects_raw)
            representatives = [item for item in _json_list(representatives_raw) if isinstance(item, dict)]
            removed = [item for item in representatives if str(item.get("subjectRef") or "") == subject_ref]
            if subject_ref not in subjects and not removed:
                continue
            subjects.pop(subject_ref, None)
            remaining = [item for item in representatives if str(item.get("subjectRef") or "") != subject_ref]
            aggregate = _json_object(aggregate_raw)
            removed_conversations = len([item for item in removed if item.get("sourceKind") == "conversation"])
            for key in ("eligibleConversations", "promptConversations", "archivedSourceEvents"):
                if key in aggregate:
                    aggregate[key] = max(0, int(aggregate.get(key) or 0) - removed_conversations)
            if "participants" in aggregate:
                aggregate["participants"] = len(subjects)
            conn.execute(
                """UPDATE bnl_journal_observations
                   SET aggregate_counts_json=?,topic_counts_json=?,subject_counts_json=?,
                       representative_sources_json=?,updated_at=?
                   WHERE guild_id=? AND observation_id=?""",
                (
                    _json(aggregate),
                    _json(journal_topic_counts(remaining)),
                    _json(subjects),
                    _json(remaining),
                    now,
                    guild,
                    observation_id,
                ),
            )
            counts["bnl_journal_observations_scrubbed"] = counts.get("bnl_journal_observations_scrubbed", 0) + 1

    unpublished: list[tuple[str, int]] = []
    if table_exists(conn, "bnl_journal_private_metadata"):
        has_entries = table_exists(conn, "bnl_journal_entries")
        if has_entries:
            rows = conn.execute(
                """SELECT m.entry_id,m.revision,m.metadata_json,
                          COALESCE(e.lifecycle_state,m.lifecycle_state)
                   FROM bnl_journal_private_metadata m
                   LEFT JOIN bnl_journal_entries e
                     ON e.entry_id=m.entry_id AND e.revision=m.revision
                   WHERE m.guild_id=?""",
                (guild,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT entry_id,revision,metadata_json,lifecycle_state FROM bnl_journal_private_metadata WHERE guild_id=?",
                (guild,),
            ).fetchall()
        for entry_id, revision, metadata_raw, lifecycle_state in rows:
            metadata = _json_object(metadata_raw)
            supporting = [item for item in metadata.get("supportingConversationRefs", []) if isinstance(item, dict)]
            target_supporting = [item for item in supporting if str(item.get("subjectRef") or "") == subject_ref]
            subject_refs = [str(item) for item in metadata.get("subjectRefs", [])]
            affected = (
                subject_ref in subject_refs
                or bool(target_supporting)
                or _contains_exact_json_scalar(metadata, subject_ref)
            )
            if not affected:
                continue
            key = (str(entry_id), int(revision))
            if str(lifecycle_state or "") != "published":
                unpublished.append(key)
                continue

            target_ref_ids = {str(item.get("refId")) for item in target_supporting if item.get("refId")}
            metadata["supportingConversationRefs"] = [
                item for item in supporting if str(item.get("subjectRef") or "") != subject_ref
            ]
            metadata["subjectRefs"] = [item for item in subject_refs if item != subject_ref]
            metadata["sourceSummaries"] = [
                item for item in metadata.get("sourceSummaries", [])
                if isinstance(item, dict) and str(item.get("refId") or "") not in target_ref_ids
            ]
            source_ref_ids = metadata.get("sourceRefIds")
            if isinstance(source_ref_ids, dict):
                metadata["sourceRefIds"] = {
                    str(heading): [str(ref) for ref in refs if str(ref) not in target_ref_ids]
                    for heading, refs in source_ref_ids.items()
                    if isinstance(refs, list)
                }
            # These model-derived fields cannot be reliably separated by source.
            # Clear them on an affected published entry rather than allowing a
            # deleted member's private evidence to guide future synthesis.
            for key_name in (
                "topicTags", "continuityNotes", "unresolvedQuestions", "recurringTopicCounts",
                "contextUses", "usedGenerationContextLanes", "usedContextLaneProvenance",
            ):
                metadata[key_name] = {} if key_name in {"recurringTopicCounts", "usedGenerationContextLanes", "usedContextLaneProvenance"} else []
            metadata["privacyScrubbed"] = True
            conn.execute(
                """UPDATE bnl_journal_private_metadata SET metadata_json=?,updated_at=?
                   WHERE guild_id=? AND entry_id=? AND revision=?""",
                (_json(metadata), now, guild, entry_id, int(revision)),
            )
            counts["bnl_journal_published_metadata_scrubbed"] = counts.get("bnl_journal_published_metadata_scrubbed", 0) + 1

    if unpublished:
        for entry_id, revision in sorted(set(unpublished)):
            counts["bnl_journal_unpublished_metadata_deleted"] = counts.get("bnl_journal_unpublished_metadata_deleted", 0) + conn.execute(
                "DELETE FROM bnl_journal_private_metadata WHERE guild_id=? AND entry_id=? AND revision=?",
                (guild, entry_id, revision),
            ).rowcount
            if table_exists(conn, "bnl_journal_entries"):
                counts["bnl_journal_unpublished_entries_deleted"] = counts.get("bnl_journal_unpublished_entries_deleted", 0) + conn.execute(
                    "DELETE FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state<>'published'",
                    (guild, entry_id, revision),
                ).rowcount
            if table_exists(conn, "bnl_journal_observations"):
                conn.execute(
                    """UPDATE bnl_journal_observations
                       SET lifecycle_state='observed',journal_entry_id=NULL,journal_revision=0,updated_at=?
                       WHERE guild_id=? AND journal_entry_id=? AND journal_revision=?""",
                    (now, guild, entry_id, revision),
                )
            if table_exists(conn, "bnl_journal_automation_runs"):
                counts["bnl_journal_automation_runs_invalidated"] = counts.get("bnl_journal_automation_runs_invalidated", 0) + conn.execute(
                    """UPDATE bnl_journal_automation_runs
                       SET lifecycle_state='held',reason='privacy_source_deleted',journal_entry_id=NULL,
                           journal_revision=0,lease_expires_at=NULL,updated_at=?
                       WHERE guild_id=? AND journal_entry_id=? AND journal_revision=?
                         AND lifecycle_state<>'published'""",
                    (now, guild, entry_id, revision),
                ).rowcount
            if table_exists(conn, "bnl_journal_automation_state"):
                conn.execute(
                    """UPDATE bnl_journal_automation_state
                       SET last_status='held',last_reason='privacy_source_deleted',last_entry_id='',last_revision=0,updated_at=?
                       WHERE guild_id=? AND last_entry_id=? AND last_revision=?""",
                    (now, guild, entry_id, revision),
                )
    return {key: int(value or 0) for key, value in counts.items() if int(value or 0)}


def _cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _parse_context_datetime(value: Any, *, end_of_day: bool = False) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            parsed = datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
            return parsed + timedelta(days=1) - timedelta(microseconds=1) if end_of_day else parsed
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _topic_terms(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", str(text or "").lower())
        if len(token) >= 4 and token not in _CONTEXT_TOPIC_STOPWORDS
    }


def _context_claim_terms(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", str(text or "").lower())
        if len(token) >= 4 and token not in _CONTEXT_CLAIM_STOPWORDS
    }


def _context_sentences(text: str) -> list[str]:
    return [part.strip() for part in re.split(r"(?:[.!?]+|\n+)", str(text or "")) if part.strip()]


def _claim_overlap(left: str | set[str], right: str | set[str]) -> int:
    left_terms = left if isinstance(left, set) else _context_claim_terms(left)
    right_terms = right if isinstance(right, set) else _context_claim_terms(right)
    return len(left_terms & right_terms)


def _claim_is_represented(claim: str, section: str) -> bool:
    return bool(_claim_matching_sentences(claim, section))


def _claim_matching_sentences(claim: str, text: str) -> list[str]:
    normalized_claim = _norm(claim)
    claim_terms = _context_claim_terms(claim)
    if len(claim_terms) < 2:
        return []
    required = max(2, min(4, (len(claim_terms) + 1) // 2))
    sentences = _context_sentences(text)
    exact_matches = [
        sentence
        for sentence in sentences
        if len(normalized_claim.split()) >= 4 and normalized_claim == _norm(sentence)
    ]
    if exact_matches:
        return exact_matches
    matches = []
    for sentence in sentences:
        normalized_sentence = _norm(sentence)
        if (
            len(normalized_claim.split()) >= 4
            and normalized_claim in normalized_sentence
        ) or _claim_overlap(claim_terms, sentence) >= required:
            matches.append(sentence)
    return matches


def _scope_tokens(value: str) -> set[str]:
    return {
        token.strip().lower().replace("-", "_").replace(" ", "_")
        for token in re.split(r"[,/;]+|\band\b", str(value or ""), flags=re.IGNORECASE)
        if token.strip()
    }


def _journal_identity_tokens(conn: sqlite3.Connection, guild_id: int) -> list[str]:
    names: set[str] = set()
    if table_exists(conn, "conversations"):
        cols = _cols(conn, "conversations")
        if {"guild_id", "user_name"} <= cols:
            for row in conn.execute(
                "SELECT DISTINCT user_name FROM conversations WHERE guild_id=? AND TRIM(COALESCE(user_name,''))<>''",
                (int(guild_id),),
            ).fetchall():
                names.add(str(row[0]).strip())
    if table_exists(conn, "user_profiles"):
        cols = _cols(conn, "user_profiles")
        available = [name for name in ("display_name", "preferred_name") if name in cols]
        if "guild_id" in cols and available:
            for row in conn.execute(
                f"SELECT {','.join(available)} FROM user_profiles WHERE guild_id=?",
                (int(guild_id),),
            ).fetchall():
                names.update(str(value).strip() for value in row if str(value or "").strip())
    return sorted({name for name in names if len(name) >= 2}, key=len, reverse=True)


def _memory_select_expression(columns: set[str], name: str, fallback: str = "NULL") -> str:
    return name if name in columns else f"{fallback} AS {name}"


def _approved_journal_broadcast_memory(
    conn: sqlite3.Connection,
    guild_id: int,
    safe_sources: list[dict[str, Any]],
    source_window_end: str,
    *,
    limit: int = MAX_BROADCAST_MEMORY_CONTEXT,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return generation-safe established records plus private row provenance.

    Broadcast memory is operator-only at intake. Journal usage is narrower still:
    only active, explicitly public-safe, non-superseded, unambiguous rows in a
    public output scope may cross into this private generation lane.
    """
    if not table_exists(conn, "broadcast_memory"):
        return [], []
    columns = _cols(conn, "broadcast_memory")
    required = {"id", "guild_id", "cleaned_summary", "status", "public_safe"}
    if not required <= columns:
        return [], []
    selected_columns = [
        _memory_select_expression(columns, "id"),
        _memory_select_expression(columns, "episode_date", "''"),
        _memory_select_expression(columns, "cleaned_summary", "''"),
        _memory_select_expression(columns, "entry_type", "'notable_moment'"),
        _memory_select_expression(columns, "importance", "'medium'"),
        _memory_select_expression(columns, "usage_scope", "''"),
        _memory_select_expression(columns, "valid_until"),
        _memory_select_expression(columns, "needs_clarification", "0"),
        _memory_select_expression(columns, "superseded_by_id", "0"),
        _memory_select_expression(columns, "created_at", "''"),
    ]
    rows = conn.execute(
        f"SELECT {','.join(selected_columns)} FROM broadcast_memory "
        "WHERE guild_id=? AND status='active' AND public_safe=1 ORDER BY id DESC LIMIT 250",
        (int(guild_id),),
    ).fetchall()
    identity_tokens = _journal_identity_tokens(conn, guild_id)
    window_end = _parse_context_datetime(source_window_end) or datetime.now(timezone.utc)
    source_terms = {
        str(source.get("refId")): _topic_terms(str(source.get("summary") or ""))
        for source in safe_sources
        if source.get("refId")
    }
    safe_records: list[dict[str, Any]] = []
    provenance: list[dict[str, Any]] = []
    for row in rows:
        (
            row_id, episode_date, cleaned_summary, entry_type, importance,
            usage_scope, valid_until, needs_clarification, superseded_by_id, created_at,
        ) = row
        if int(needs_clarification or 0) != 0 or int(superseded_by_id or 0) != 0:
            continue
        scopes = _scope_tokens(str(usage_scope or ""))
        if not (scopes & JOURNAL_PUBLIC_BROADCAST_SCOPES) or "internal" in scopes:
            continue
        expiry = _parse_context_datetime(valid_until, end_of_day=True)
        if valid_until and (expiry is None or expiry < window_end):
            continue
        summary = sanitize_source_summary(str(cleaned_summary or ""), identity_tokens)
        if not summary:
            continue
        memory_terms = _topic_terms(summary)
        matched_refs = sorted(
            ref_id for ref_id, terms in source_terms.items() if len(memory_terms & terms) >= 2
        )
        if not matched_refs:
            continue
        lane_ref = "memory:" + _hash("journal-memory", guild_id, int(row_id))[:16]
        safe_records.append({
            "laneRefId": lane_ref,
            "epistemicStatus": "established_network_record",
            "summary": summary,
            "entryType": str(entry_type or "notable_moment")[:80],
            "importance": str(importance or "medium")[:20],
            "episodeDate": str(episode_date or "")[:32],
            "matchedFreshSourceRefIds": matched_refs[:12],
        })
        provenance.append({
            "laneRefId": lane_ref,
            "sourceTable": "broadcast_memory",
            "rowId": int(row_id),
            "createdAt": str(created_at or "")[:48],
            "matchedFreshSourceRefIds": matched_refs[:12],
        })
        if len(safe_records) >= max(1, int(limit)):
            break
    return safe_records, provenance


def _journal_rumor_context(
    private_sources: list[dict[str, Any]],
    *,
    identity_tokens: Optional[list[str]] = None,
    limit: int = MAX_RUMOR_CONTEXT,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Classify only repeated, explicit, public-safe speculation as rumor.

    A single message or a single participant can never create a Journal rumor.
    The classifier intentionally covers only low-risk Network/music/show topics.
    """
    candidates: list[dict[str, Any]] = []
    for source in private_sources:
        if source.get("sourceKind") != "conversation":
            continue
        if str(source.get("channelPolicy") or "") not in PUBLIC_POLICIES:
            continue
        summary = sanitize_source_summary(
            str(source.get("summary") or ""),
            [*(identity_tokens or []), str(source.get("displayName") or "")],
        )
        subject_ref = str(source.get("subjectRef") or "")
        if not summary or not subject_ref:
            continue
        if not _RUMOR_MARKER_RE.search(summary) or not _RUMOR_PUBLIC_TOPIC_RE.search(summary):
            continue
        if _RUMOR_SENSITIVE_RE.search(summary):
            continue
        terms = _topic_terms(_RUMOR_MARKER_RE.sub(" ", summary))
        if len(terms) < 2:
            continue
        candidates.append({**source, "summary": summary, "topicTerms": terms})

    clusters: list[dict[str, Any]] = []
    for candidate in sorted(candidates, key=lambda item: (str(item.get("observedAt") or ""), str(item.get("refId") or ""))):
        best: Optional[dict[str, Any]] = None
        best_overlap = 0
        for cluster in clusters:
            overlap = len(candidate["topicTerms"] & cluster["topicTerms"])
            if overlap >= 2 and overlap > best_overlap:
                best = cluster
                best_overlap = overlap
        if best is None:
            clusters.append({"topicTerms": set(candidate["topicTerms"]), "sources": [candidate]})
        else:
            best["sources"].append(candidate)
            best["topicTerms"].update(candidate["topicTerms"])

    safe_records: list[dict[str, Any]] = []
    provenance: list[dict[str, Any]] = []
    for cluster in clusters:
        sources = cluster["sources"]
        subject_refs = sorted({str(item.get("subjectRef") or "") for item in sources if item.get("subjectRef")})
        if len(sources) < 2 or len(subject_refs) < 2:
            continue
        source_refs = sorted({str(item.get("refId")) for item in sources if item.get("refId")})
        lane_ref = "rumor:" + _hash("journal-rumor", *source_refs)[:16]
        safe_records.append({
            "laneRefId": lane_ref,
            "epistemicStatus": "unconfirmed_public_rumor",
            "topicTerms": sorted(cluster["topicTerms"])[:10],
            "independentParticipantCount": len(subject_refs),
            "evidence": [
                {"sourceRefId": str(item.get("refId")), "summary": str(item.get("summary") or "")[:240]}
                for item in sources[:6]
            ],
        })
        provenance.append({
            "laneRefId": lane_ref,
            "sourceRefs": [
                {
                    key: item.get(key)
                    for key in ("refId", "messageId", "subjectRef", "channelPolicy", "observedAt")
                    if item.get(key) not in (None, "")
                }
                for item in sources[:12]
            ],
        })
        if len(safe_records) >= max(1, int(limit)):
            break
    return safe_records, provenance


def _journal_inference_context(
    safe_sources: list[dict[str, Any]],
    established_records: list[dict[str, Any]],
    rumor_records: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    topic_refs: dict[str, set[str]] = {}
    for source in safe_sources:
        ref_id = str(source.get("refId") or "")
        if not ref_id:
            continue
        for term in _context_claim_terms(str(source.get("summary") or "")):
            topic_refs.setdefault(term, set()).add(ref_id)
    repeated = sorted(
        ((term, refs) for term, refs in topic_refs.items() if len(refs) >= 2),
        key=lambda item: (-len(item[1]), item[0]),
    )
    dependencies_by_fresh_ref: dict[str, set[str]] = {}
    for item in established_records:
        lane_ref = str(item.get("laneRefId") or "")
        if not lane_ref:
            continue
        for ref in item.get("matchedFreshSourceRefIds", []):
            fresh_ref = str(ref)
            if fresh_ref.startswith("fresh:"):
                dependencies_by_fresh_ref.setdefault(fresh_ref, set()).add(lane_ref)
    for item in rumor_records:
        lane_ref = str(item.get("laneRefId") or "")
        if not lane_ref:
            continue
        for evidence in item.get("evidence", []):
            if not isinstance(evidence, dict):
                continue
            fresh_ref = str(evidence.get("sourceRefId") or "")
            if fresh_ref.startswith("fresh:"):
                dependencies_by_fresh_ref.setdefault(fresh_ref, set()).add(lane_ref)
    context_fresh_refs = set(dependencies_by_fresh_ref)
    fresh_basis = sorted({ref for _term, refs in repeated[:5] for ref in refs} | context_fresh_refs)[:16]
    context_basis = [
        str(item.get("laneRefId"))
        for item in [*established_records, *rumor_records]
        if item.get("laneRefId")
    ]
    if not context_basis and not fresh_basis:
        return None
    basis = [*context_basis, *fresh_basis]
    allowed_basis = basis[:24]
    allowed_basis_set = set(allowed_basis)
    required_parent_refs = [lane_ref for lane_ref in context_basis if lane_ref in allowed_basis_set]
    return {
        "laneRefId": "inference:" + _hash("journal-inference", *basis)[:16],
        "epistemicStatus": "bnl_inference_only",
        "candidateThemes": [term for term, _refs in repeated[:5]],
        "allowedBasisRefIds": allowed_basis,
        "requiredParentContextLaneRefs": required_parent_refs,
        "requiredContextLaneRefsByFreshSourceRef": {
            fresh_ref: sorted(lane_ref for lane_ref in lane_refs if lane_ref in allowed_basis_set)
            for fresh_ref, lane_refs in sorted(dependencies_by_fresh_ref.items())
            if fresh_ref in allowed_basis_set
            and any(lane_ref in allowed_basis_set for lane_ref in lane_refs)
        },
    }


def build_generation_context_lanes(
    db_path: str,
    guild_id: int,
    source_window_end: str,
    safe_sources: list[dict[str, Any]],
    private_sources: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        identity_tokens = _journal_identity_tokens(conn, guild_id)
        established, memory_provenance = _approved_journal_broadcast_memory(
            conn, guild_id, safe_sources, source_window_end
        )
    rumors, rumor_provenance = _journal_rumor_context(private_sources, identity_tokens=identity_tokens)
    inference = _journal_inference_context(safe_sources, established, rumors)
    lanes: dict[str, Any] = {}
    if established:
        lanes["establishedBroadcastMemory"] = established
    if rumors:
        lanes["communityRumors"] = rumors
    if inference:
        lanes["bnlInference"] = inference
    private_provenance: dict[str, Any] = {}
    if memory_provenance:
        private_provenance["establishedBroadcastMemory"] = memory_provenance
    if rumor_provenance:
        private_provenance["communityRumors"] = rumor_provenance
    return lanes, private_provenance


def _mask_rumor_sources_outside_lane(
    safe_sources: list[dict[str, Any]],
    lanes: dict[str, Any],
    private_sources: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rumor_refs = {
        str(evidence.get("sourceRefId") or "")
        for rumor in lanes.get("communityRumors", [])
        if isinstance(rumor, dict)
        for evidence in rumor.get("evidence", [])
        if isinstance(evidence, dict) and evidence.get("sourceRefId")
    }
    rumor_like_refs = {
        str(source.get("refId") or "")
        for source in private_sources
        if source.get("sourceKind") == "conversation"
        and source.get("refId")
        and _RUMOR_MARKER_RE.search(str(source.get("summary") or ""))
    }
    if not rumor_like_refs:
        return safe_sources
    masked: list[dict[str, Any]] = []
    for source in safe_sources:
        item = dict(source)
        ref_id = str(item.get("refId") or "")
        if ref_id in rumor_like_refs and ref_id not in rumor_refs:
            continue
        if ref_id in rumor_refs:
            item["summary"] = (
                "Repeated public participants raised the same unconfirmed low-risk music or Network question. "
                "Use the matching communityRumors lane and declare it before discussing its details."
            )
        masked.append(item)
    return masked


def _finalize_context_lanes_for_safe_sources(
    lanes: dict[str, Any],
    provenance: dict[str, Any],
    safe_sources: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    safe_refs = {
        str(source.get("refId") or "")
        for source in safe_sources
        if isinstance(source, dict) and source.get("refId")
    }
    established: list[dict[str, Any]] = []
    for item in lanes.get("establishedBroadcastMemory", []):
        if not isinstance(item, dict):
            continue
        matched = [str(ref) for ref in item.get("matchedFreshSourceRefIds", []) if str(ref) in safe_refs]
        if matched:
            established.append({**item, "matchedFreshSourceRefIds": matched})

    rumors: list[dict[str, Any]] = []
    rumor_evidence_by_ref: dict[str, str] = {}
    for item in lanes.get("communityRumors", []):
        if not isinstance(item, dict):
            continue
        filtered_evidence = [
            evidence_item
            for evidence_item in item.get("evidence", [])
            if isinstance(evidence_item, dict) and str(evidence_item.get("sourceRefId") or "") in safe_refs
        ]
        if len(filtered_evidence) < 2:
            continue
        rumors.append({**item, "evidence": filtered_evidence})
        rumor_evidence_by_ref.update({
            str(evidence_item.get("sourceRefId")): str(evidence_item.get("summary") or "")
            for evidence_item in filtered_evidence
        })

    inference_sources = [
        {
            **source,
            "summary": rumor_evidence_by_ref.get(str(source.get("refId") or ""), str(source.get("summary") or "")),
        }
        for source in safe_sources
    ]
    inference = _journal_inference_context(inference_sources, established, rumors)
    finalized: dict[str, Any] = {}
    if established:
        finalized["establishedBroadcastMemory"] = established
    if rumors:
        finalized["communityRumors"] = rumors
    if inference:
        finalized["bnlInference"] = inference

    retained_refs = {
        str(item.get("laneRefId") or "")
        for item in [*established, *rumors]
        if item.get("laneRefId")
    }
    finalized_provenance: dict[str, Any] = {}
    for key in ("establishedBroadcastMemory", "communityRumors"):
        selected = []
        for item in provenance.get(key, []):
            if not isinstance(item, dict) or str(item.get("laneRefId") or "") not in retained_refs:
                continue
            selected_item = dict(item)
            if key == "establishedBroadcastMemory":
                selected_item["matchedFreshSourceRefIds"] = [
                    str(ref)
                    for ref in item.get("matchedFreshSourceRefIds", [])
                    if str(ref) in safe_refs
                ]
            selected.append(selected_item)
        if selected:
            finalized_provenance[key] = selected
    return finalized, finalized_provenance


def _identity_literal_pattern(name: str) -> Optional[re.Pattern[str]]:
    literal = str(name or "").strip()
    if not literal:
        return None
    left_boundary = r"(?<!\w)" if (literal[0].isalnum() or literal[0] == "_") else ""
    right_boundary = r"(?!\w)" if (literal[-1].isalnum() or literal[-1] == "_") else ""
    return re.compile(left_boundary + re.escape(literal) + right_boundary, re.IGNORECASE)


def _replace_identity_literal(text: str, name: str, replacement: str = "someone") -> str:
    pattern = _identity_literal_pattern(name)
    return pattern.sub(replacement, text) if pattern else text


def _contains_identity_literal(text: str, name: str) -> bool:
    pattern = _identity_literal_pattern(name)
    return bool(pattern and pattern.search(text))


def sanitize_source_summary(text: str, names: Optional[list[str]] = None, *, limit: int = 240) -> str:
    clean = re.sub(r"https?://\S+", "", text or "")
    clean = re.sub(r"<@!?\d+>|@\w+", "someone", clean)
    clean = re.sub(r"\b\d{12,}\b", "", clean)
    for name in sorted(set(names or []), key=len, reverse=True):
        if name.strip():
            clean = _replace_identity_literal(clean, name)
    clean = re.sub(r"[\"“”‘’]", "", clean)
    return re.sub(r"\s+", " ", clean).strip()[:max(1, int(limit))]


def _anon_ref(prefix: str, idx: int) -> str:
    return f"{prefix}:{idx}"


def accepted_relays(conn: sqlite3.Connection, guild_id: int, start: str, end: str, limit: int = MAX_RELAY_SOURCES_PER_WINDOW) -> list[dict[str, Any]]:
    if not table_exists(conn, "website_relay_history"):
        return []
    rows = conn.execute("""SELECT relay_id, public_message, public_directive, event_type, published_timestamp
        FROM website_relay_history WHERE guild_id=? AND published_timestamp>=? AND published_timestamp<?
        ORDER BY published_timestamp ASC, relay_id ASC LIMIT ?""", (guild_id, start, end, limit)).fetchall()
    out = []
    for idx, row in enumerate(rows, 1):
        summary = sanitize_source_summary(f"{row[1]} {row[2]}")
        if summary:
            out.append({"refId": _anon_ref("fresh", idx), "sourceKind": "relay", "relayId": row[0], "summary": summary, "observedAt": row[4], "eventType": row[3]})
    return out


def public_conversations(conn: sqlite3.Connection, guild_id: int, start: str, end: str, limit: int = MAX_CONVERSATION_SOURCES_PER_WINDOW) -> list[dict[str, Any]]:
    if not table_exists(conn, "conversations"):
        return []
    cols = _cols(conn, "conversations")
    public_usable_clause = " AND public_usable=1" if "public_usable" in cols else ""
    visibility_clause = " AND visibility IN ('public','public_safe')" if "visibility" in cols else ""
    role_clause = " AND role='user'" if "role" in cols else ""
    rows = conn.execute(f"""SELECT id, user_id, user_name, channel_policy, channel_name, content, timestamp
        FROM conversations WHERE guild_id=? AND channel_policy IN ({','.join('?' for _ in sorted(PUBLIC_POLICIES))})
        AND timestamp>=? AND timestamp<? {public_usable_clause} {visibility_clause} {role_clause}
        ORDER BY timestamp ASC, id ASC LIMIT ?""", (guild_id, *sorted(PUBLIC_POLICIES), start, end, limit)).fetchall()
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
                "participantAlias": "participant-" + _hash("journal-participant", guild_id, row[1])[:8],
                "displayName": str(row[2] or "").strip(),
                "channelPolicy": row[3],
                "summary": summary,
                "observedAt": row[6],
            })
    return out


def _source_for_prompt(source: dict[str, Any]) -> dict[str, Any]:
    allowed = {"refId", "sourceKind", "summary", "observedAt", "eventType", "channelPolicy", "participantAlias"}
    return {k: v for k, v in source.items() if k in allowed and v not in (None, "")}


def _evenly_sample(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """Keep chronological coverage without silently taking only the newest rows."""
    if limit <= 0 or len(items) <= limit:
        return list(items)
    if limit == 1:
        return [items[-1]]
    indexes = {round(i * (len(items) - 1) / (limit - 1)) for i in range(limit)}
    return [items[i] for i in sorted(indexes)]


def _source_sort_key(source: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(source.get("observedAt") or ""),
        str(source.get("sourceKind") or ""),
        str(source.get("refId") or ""),
    )


def _time_stratified_sample(
    items: list[dict[str, Any]],
    limit: int,
    start: str,
    end: str,
    *,
    segment_count: int = 3,
) -> list[dict[str, Any]]:
    """Reserve temporal breadth, then let remaining slots follow activity density."""
    ordered = sorted(items, key=_source_sort_key)
    if limit <= 0:
        return []
    if len(ordered) <= limit:
        return ordered
    buckets: dict[str, list[dict[str, Any]]] = {}
    for source in ordered:
        segment = _coverage_segment(source.get("observedAt"), start, end, segment_count)
        if segment:
            buckets.setdefault(segment, []).append(source)
    active_segments = [
        buckets.get(f"segment-{index}", [])
        for index in range(1, max(1, int(segment_count)) + 1)
        if buckets.get(f"segment-{index}")
    ]
    if not active_segments or limit < len(active_segments):
        return _evenly_sample(ordered, limit)

    # Half of the budget is available as an equal temporal floor. Quiet
    # segments keep all of their material; the other half continues to follow
    # real activity density, so a busy stretch may still lead the story.
    floor = max(1, limit // (2 * len(active_segments)))
    selected: list[dict[str, Any]] = []
    for bucket in active_segments:
        selected.extend(_evenly_sample(bucket, min(len(bucket), floor)))
    selected_ids = {id(source) for source in selected}
    remaining = [source for source in ordered if id(source) not in selected_ids]
    remaining_quota = limit - len(selected)
    if remaining_quota > 0:
        selected.extend(_evenly_sample(remaining, remaining_quota))
    return sorted(selected, key=_source_sort_key)


def _window_segment_activity(
    start: str,
    end: str,
    relays: list[dict[str, Any]],
    conversations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Expose non-identifying daily activity across the relay and context streams."""
    labels = ("early", "middle", "late")
    counts = {
        f"segment-{index}": {"conversationSources": 0, "relaySources": 0}
        for index in range(1, 4)
    }
    for source in [*relays, *conversations]:
        segment = _coverage_segment(source.get("observedAt"), start, end, 3)
        if segment not in counts:
            continue
        key = "conversationSources" if source.get("sourceKind") == "conversation" else "relaySources"
        counts[segment][key] += 1
    return [
        {
            "segment": label,
            **counts[f"segment-{index}"],
        }
        for index, label in enumerate(labels, 1)
    ]


def _minimum_fresh_sources_for_entry(entry_kind: str, total: int) -> int:
    """Require representative breadth without turning a short Journal into a roll call."""
    if total <= 0:
        return 0
    if entry_kind == "manual":
        return 1
    if total <= 5:
        return 1
    if total <= 11:
        return 3
    if total <= 24:
        return 5
    if total <= 74:
        return 7
    if entry_kind == "weekly":
        return min(total, 14)
    if entry_kind == "daily":
        return min(total, 10)
    return min(total, 8)


def _coverage_segment(
    observed_at: Any,
    start: str,
    end: str,
    segment_count: int,
) -> str:
    observed = _parse_context_datetime(observed_at)
    start_at = _parse_context_datetime(start)
    end_at = _parse_context_datetime(end)
    if not observed or not start_at or not end_at or end_at <= start_at:
        return ""
    elapsed = max(0.0, min((observed - start_at).total_seconds(), (end_at - start_at).total_seconds() - 0.001))
    index = int(elapsed / (end_at - start_at).total_seconds() * max(1, segment_count))
    return f"segment-{min(max(index, 0), max(1, segment_count) - 1) + 1}"


def build_evidence_coverage_contract(
    entry_kind: str,
    start: str,
    end: str,
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    """Describe how much of the supplied window a generated entry must actually use."""
    usable = [source for source in sources if source.get("refId")]
    total = len(usable)
    kind_counts: dict[str, int] = {}
    for source in usable:
        kind = str(source.get("sourceKind") or "")
        if kind:
            kind_counts[kind] = kind_counts.get(kind, 0) + 1
    kind_floor = max(3, (total + 19) // 20) if total else 0
    required_kinds = (
        sorted(kind for kind, count in kind_counts.items() if count >= kind_floor)
        if entry_kind in {"daily", "weekly"}
        else []
    )

    aliases = {
        str(source.get("participantAlias") or "")
        for source in usable
        if str(source.get("participantAlias") or "")
    }
    if entry_kind == "manual":
        required_participants = 0
    elif total >= 12:
        required_participants = min(3, len(aliases))
    elif total >= 6:
        required_participants = min(2, len(aliases))
    else:
        required_participants = min(1, len(aliases))

    segment_count = 7 if entry_kind == "weekly" else 3
    segment_by_ref = {
        str(source["refId"]): segment
        for source in usable
        for segment in [_coverage_segment(source.get("observedAt"), start, end, segment_count)]
        if segment
    }
    available_segments = set(segment_by_ref.values())
    if entry_kind == "manual":
        target_segments = 0
    elif total >= 12:
        target_segments = 4 if entry_kind == "weekly" else 3
    elif total >= 6:
        target_segments = 2
    else:
        target_segments = 1

    return {
        "minimumDistinctFreshSources": _minimum_fresh_sources_for_entry(entry_kind, total),
        "requiredSourceKinds": required_kinds,
        "minimumDistinctParticipants": required_participants,
        "minimumDistinctWindowSegments": min(target_segments, len(available_segments)),
        "segmentBySourceRef": segment_by_ref,
    }


def _count_legacy_sources(conn: sqlite3.Connection, guild_id: int, start: str, end: str) -> tuple[int, int]:
    relay_count = 0
    conversation_count = 0
    if table_exists(conn, "website_relay_history"):
        relay_count = int(conn.execute(
            "SELECT COUNT(*) FROM website_relay_history WHERE guild_id=? AND published_timestamp>=? AND published_timestamp<?",
            (guild_id, start, end),
        ).fetchone()[0])
    if table_exists(conn, "conversations"):
        cols = _cols(conn, "conversations")
        public_usable_clause = " AND public_usable=1" if "public_usable" in cols else ""
        visibility_clause = " AND visibility IN ('public','public_safe')" if "visibility" in cols else ""
        role_clause = " AND role='user'" if "role" in cols else ""
        conversation_count = int(conn.execute(
            f"""SELECT COUNT(*) FROM conversations WHERE guild_id=?
            AND channel_policy IN ({','.join('?' for _ in sorted(PUBLIC_POLICIES))})
            AND timestamp>=? AND timestamp<? {public_usable_clause} {visibility_clause} {role_clause}""",
            (guild_id, *sorted(PUBLIC_POLICIES), start, end),
        ).fetchone()[0])
    return relay_count, conversation_count


def _subject_refs(packet: dict[str, Any]) -> set[str]:
    return {str(s.get("subjectRef")) for s in packet.get("privateSources", []) if s.get("subjectRef")}


def retrieve_history(
    db_path: str,
    guild_id: int,
    current_packet: dict[str, Any],
    limit: int = 6,
    *,
    excluded_entry_ids: Optional[set[str]] = None,
) -> dict[str, Any]:
    ensure_schema(db_path)
    excluded = {str(entry_id) for entry_id in (excluded_entry_ids or set()) if str(entry_id)}
    current_subjects = _subject_refs(current_packet)
    current_topics = set(current_packet.get("candidateTopicTags", []))
    terms = set(_norm(_json(current_packet.get("safeSources", []))).split())
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        prev_rows = conn.execute("""SELECT entry_id,revision,title,excerpt,sections_json,published_at,created_at
            FROM bnl_journal_entries WHERE guild_id=? AND lifecycle_state='published'
            ORDER BY published_at DESC, created_at DESC""", (guild_id,)).fetchall()
        rows = conn.execute("""SELECT e.entry_id,e.revision,e.title,e.excerpt,e.sections_json,e.published_at,e.created_at,m.metadata_json
            FROM bnl_journal_entries e JOIN bnl_journal_private_metadata m
              ON m.entry_id=e.entry_id AND m.revision=e.revision
            WHERE e.guild_id=? AND e.lifecycle_state='published' AND m.lifecycle_state='published'""", (guild_id,)).fetchall()
    prev = next((row for row in prev_rows if str(row["entry_id"]) not in excluded), None)
    recurring: dict[str, int] = {}
    scored = []
    notes = []
    unresolved = []
    prev_key = (prev["entry_id"], int(prev["revision"])) if prev else None
    for row in rows:
        if str(row["entry_id"]) in excluded:
            continue
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
            unresolved.extend(str(n)[:240] for n in meta.get("unresolvedQuestions", [])[:3])
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return {
        "previousEntry": dict(prev) if prev else None,
        "relevantOlderEntries": [item for _, _, item in scored[:limit]],
        "recurringTopicCounts": recurring,
        "matchingContinuityNotes": notes[:10],
        "matchingUnresolvedQuestions": unresolved[:10],
    }


def _sample_source_kinds(
    relays: list[dict[str, Any]],
    conversations: list[dict[str, Any]],
    limit: int = MAX_PROMPT_SOURCES,
    *,
    start: str = "",
    end: str = "",
    entry_kind: str = "manual",
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []

    def sample(items: list[dict[str, Any]], quota: int) -> list[dict[str, Any]]:
        if entry_kind == "daily" and start and end:
            return _time_stratified_sample(items, quota, start, end)
        return _evenly_sample(items, quota)

    total = len(relays) + len(conversations)
    if total <= limit:
        chosen = list(relays) + list(conversations)
    elif not relays:
        chosen = sample(conversations, limit)
    elif not conversations:
        chosen = sample(relays, limit)
    else:
        relay_quota = min(len(relays), max(1, limit // 2))
        conversation_quota = min(len(conversations), max(1, limit - relay_quota))
        spare = limit - relay_quota - conversation_quota
        if spare > 0:
            add_relays = min(spare, len(relays) - relay_quota)
            relay_quota += add_relays
            conversation_quota += min(spare - add_relays, len(conversations) - conversation_quota)
        chosen = sample(relays, relay_quota) + sample(conversations, conversation_quota)
    return sorted(chosen, key=_source_sort_key)


def build_packet_from_sources(
    db_path: str,
    guild_id: int,
    start: str,
    end: str,
    relays: list[dict[str, Any]],
    conversations: list[dict[str, Any]],
    *,
    entry_kind: str = "daily",
    aggregate_counts: Optional[dict[str, Any]] = None,
    observation_context: Optional[list[dict[str, Any]]] = None,
    coverage_complete: bool = True,
    excluded_history_entry_ids: Optional[set[str]] = None,
) -> dict[str, Any]:
    # Scrub with the complete window's identity set before sampling. Otherwise
    # a selected source can mention a community member whose own source was the
    # one omitted from a >MAX_PROMPT_SOURCES window.
    window_display_names = [
        str(source.get("displayName") or "").strip()
        for source in list(relays) + list(conversations)
        if str(source.get("displayName") or "").strip()
    ]
    window_display_names = list(dict.fromkeys(window_display_names))
    private_sources = _sample_source_kinds(
        relays,
        conversations,
        start=start,
        end=end,
        entry_kind=entry_kind,
    )
    safe_sources = []
    for source in private_sources:
        safe_source = _source_for_prompt(source)
        if "summary" in safe_source:
            safe_source["summary"] = sanitize_source_summary(
                str(safe_source.get("summary") or ""),
                window_display_names,
                limit=1000,
            )
        if safe_source.get("summary"):
            safe_sources.append(safe_source)
    counts = dict(aggregate_counts or {})
    counts.setdefault("eligibleRelays", len(relays))
    counts.setdefault("eligibleConversations", len(conversations))
    counts.setdefault("participants", len({x.get("subjectRef") for x in conversations if x.get("subjectRef")}))
    counts.setdefault("channels", len({x.get("channelPolicy") for x in conversations if x.get("channelPolicy")}))
    counts["promptRelays"] = len([s for s in private_sources if s.get("sourceKind") == "relay"])
    counts["promptConversations"] = len([s for s in private_sources if s.get("sourceKind") == "conversation"])
    packet = {
        "entryKind": entry_kind if entry_kind in {"daily", "weekly", "manual"} else "manual",
        "sourceWindowStart": start,
        "sourceWindowEnd": end,
        "safeSources": safe_sources,
        "privateSources": private_sources,
        "privateWindowDisplayNames": window_display_names,
        "candidateTopicTags": list(journal_topic_counts(safe_sources, limit=30)),
        "aggregateCounts": counts,
        "coverageComplete": bool(coverage_complete),
        "observationContext": list(observation_context or []),
        "windowSegmentActivity": (
            _window_segment_activity(start, end, relays, conversations)
            if entry_kind == "daily"
            else []
        ),
    }
    context_lanes, private_lane_provenance = build_generation_context_lanes(
        db_path,
        guild_id,
        end,
        safe_sources,
        private_sources,
    )
    safe_sources = _mask_rumor_sources_outside_lane(safe_sources, context_lanes, private_sources)
    context_lanes, private_lane_provenance = _finalize_context_lanes_for_safe_sources(
        context_lanes,
        private_lane_provenance,
        safe_sources,
    )
    packet["safeSources"] = safe_sources
    packet["candidateTopicTags"] = list(journal_topic_counts(safe_sources, limit=30))
    packet["evidenceCoverageContract"] = build_evidence_coverage_contract(
        packet["entryKind"],
        start,
        end,
        safe_sources,
    )
    packet["generationContextLanes"] = context_lanes
    packet["privateContextLaneProvenance"] = private_lane_provenance
    packet["history"] = retrieve_history(
        db_path,
        guild_id,
        packet,
        excluded_entry_ids=excluded_history_entry_ids,
    )
    return packet


def build_source_packet_between(
    db_path: str,
    guild_id: int,
    start: str,
    end: str,
    *,
    entry_kind: str = "daily",
    observation_context: Optional[list[dict[str, Any]]] = None,
    excluded_history_entry_ids: Optional[set[str]] = None,
) -> dict[str, Any]:
    try:
        from bnl_journal_source_store import backfill_legacy_sources, query_source_events, timestamp_to_epoch_ms

        backfill_legacy_sources(db_path, guild_id)
        start_ms = timestamp_to_epoch_ms(start)
        end_ms = timestamp_to_epoch_ms(end)
        if start_ms is None or end_ms is None:
            raise ValueError("invalid_source_window")
        archived = query_source_events(db_path, guild_id, start_ms, end_ms)
        relays: list[dict[str, Any]] = []
        conversations: list[dict[str, Any]] = []
        for event in archived.events:
            if not event.get("public_usable") or not str(event.get("sanitized_summary") or "").strip():
                continue
            observed_at = datetime.fromtimestamp(int(event["occurred_at_ms"]) / 1000.0, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            metadata = event.get("metadata") if isinstance(event.get("metadata"), dict) else {}
            base = {
                "refId": f"fresh:{int(event['event_seq'])}",
                "summary": str(event.get("sanitized_summary") or "")[:1000],
                "observedAt": observed_at,
            }
            if event.get("source_kind") == "discord_message":
                subject_ref = str(event.get("subject_ref") or "")
                source_key = str(event.get("source_key") or "")
                message_id = metadata.get("messageId") or metadata.get("legacyMessageId")
                if message_id in (None, "") and source_key.isdigit():
                    message_id = int(source_key)
                conversations.append({
                    **base,
                    "sourceKind": "conversation",
                    "messageId": message_id or source_key,
                    "subjectRef": subject_ref,
                    "participantAlias": "participant-" + _hash("journal-participant", guild_id, subject_ref)[:8] if subject_ref else "",
                    "displayName": str(event.get("private_display_name") or ""),
                    "channelPolicy": str(event.get("channel_policy") or ""),
                })
            elif event.get("source_kind") == "website_relay":
                relays.append({
                    **base,
                    "sourceKind": "relay",
                    "relayId": str(event.get("source_key") or ""),
                    "eventType": str(metadata.get("event_type") or metadata.get("eventType") or ""),
                })
        packet = build_packet_from_sources(
            db_path,
            guild_id,
            start,
            end,
            relays,
            conversations,
            entry_kind=entry_kind,
            aggregate_counts={
                "eligibleRelays": len(relays),
                "eligibleConversations": len(conversations),
                "participants": len({x.get("subjectRef") for x in conversations if x.get("subjectRef")}),
                "channels": int(archived.counts.get("uniqueChannels") or 0),
                "archivedSourceEvents": int(archived.counts.get("total") or 0),
            },
            observation_context=observation_context,
            excluded_history_entry_ids=excluded_history_entry_ids,
            # The archive activation watermark prevents the automatic
            # publisher from claiming a full day that began before durable
            # capture was enabled. Manual previews remain available during
            # the first partial day.
            coverage_complete=(
                bool(archived.counts.get("coverageComplete"))
                if entry_kind in {"daily", "weekly"}
                else True
            ),
        )
        packet["sourceArchiveAvailable"] = True
        return packet
    except (ImportError, sqlite3.Error, ValueError):
        # Safe compatibility path for a partially deployed schema. Automation
        # exposes this in private metadata so operators can see the downgrade.
        pass
    with sqlite3.connect(db_path) as conn:
        relay_count, conversation_count = _count_legacy_sources(conn, guild_id, start, end)
        relays = accepted_relays(conn, guild_id, start, end)
        conversations = public_conversations(conn, guild_id, start, end)
    packet = build_packet_from_sources(
        db_path,
        guild_id,
        start,
        end,
        relays,
        conversations,
        entry_kind=entry_kind,
        aggregate_counts={
            "eligibleRelays": relay_count,
            "eligibleConversations": conversation_count,
            "participants": len({x.get("subjectRef") for x in conversations if x.get("subjectRef")}),
            "channels": len({x.get("channelPolicy") for x in conversations if x.get("channelPolicy")}),
        },
        observation_context=observation_context,
        excluded_history_entry_ids=excluded_history_entry_ids,
        coverage_complete=relay_count <= MAX_RELAY_SOURCES_PER_WINDOW and conversation_count <= MAX_CONVERSATION_SOURCES_PER_WINDOW,
    )
    packet["sourceArchiveAvailable"] = False
    return packet


def build_source_packet(
    db_path: str,
    guild_id: int,
    hours: int = 72,
    now: Optional[str] = None,
    *,
    entry_kind: str = "manual",
    excluded_history_entry_ids: Optional[set[str]] = None,
) -> dict[str, Any]:
    bounded_hours = max(1, min(int(hours or 72), 168))
    end = now or utc_now_iso()
    start = (datetime.fromisoformat(end.replace("Z", "+00:00")) - timedelta(hours=bounded_hours)).isoformat().replace("+00:00", "Z")
    return build_source_packet_between(
        db_path,
        guild_id,
        start,
        end,
        entry_kind=entry_kind,
        excluded_history_entry_ids=excluded_history_entry_ids,
    )


def _bounded_history_for_prompt(history: dict[str, Any]) -> dict[str, Any]:
    def compact(entry: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if not entry:
            return None
        sections = json.loads(entry.get("sections_json") or "[]") if isinstance(entry.get("sections_json"), str) else []
        return {
            "entryId": entry.get("entry_id"),
            "revision": entry.get("revision"),
            "publishedAt": entry.get("published_at") or entry.get("created_at"),
            "title": entry.get("title"),
            "excerpt": entry.get("excerpt"),
            "sectionSnapshots": [
                {
                    "heading": str(section.get("heading", ""))[:80],
                    "bodyExcerpt": str(section.get("body", ""))[:420],
                }
                for section in sections[:3]
                if isinstance(section, dict)
            ],
        }
    return {
        "previousEntry": compact(history.get("previousEntry")),
        "relevantOlderEntries": [compact(e) for e in history.get("relevantOlderEntries", [])[:6]],
        "recurringTopicCounts": dict(sorted((history.get("recurringTopicCounts") or {}).items(), key=lambda kv: (-kv[1], kv[0]))[:12]),
        "matchingContinuityNotes": [str(n)[:240] for n in history.get("matchingContinuityNotes", [])[:8]],
    }


def build_generation_prompt(packet: dict[str, Any], *, repair_reason: str = "", previous_output: str = "") -> str:
    entry_kind = str(packet.get("entryKind") or "manual")
    context_lanes = packet.get("generationContextLanes") if isinstance(packet.get("generationContextLanes"), dict) else {}
    safe_sources = packet.get("safeSources", [])[:MAX_PROMPT_SOURCES]
    coverage_contract = packet.get("evidenceCoverageContract")
    if not isinstance(coverage_contract, dict):
        coverage_contract = build_evidence_coverage_contract(
            entry_kind,
            str(packet.get("sourceWindowStart") or ""),
            str(packet.get("sourceWindowEnd") or ""),
            safe_sources,
        )
    safe_packet = {
        "entryKind": entry_kind,
        "sourceWindowStart": packet.get("sourceWindowStart"),
        "sourceWindowEnd": packet.get("sourceWindowEnd"),
        "freshSources": safe_sources,
        "evidenceCoverageContract": coverage_contract,
        "editorialContract": {
            "requiresFirstPersonReaction": len(safe_sources) >= 5,
            "requiredBeatsAcrossEntry": ["concreteMoment", "peopleOrObservableRoles", "communityPattern", "bnlReaction"],
            "fixedSectionTemplate": False,
        },
        "history": _bounded_history_for_prompt(packet.get("history", {})),
        "aggregateCounts": packet.get("aggregateCounts", {}),
        "dailyObservations": packet.get("observationContext", [])[:7],
        "windowSegmentActivity": packet.get("windowSegmentActivity", []),
        "privateGenerationContextLanes": context_lanes,
    }
    cadence_rule = (
        "\nThis is a weekly synthesis. Connect patterns across the supplied daily observations and current source evidence; do not merely list seven daily recaps."
        if entry_kind == "weekly"
        else "\nThis is a daily chronicle covering one complete source window. Distill the day instead of listing every relay."
    )
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
    context_rule = (
        "\nOptional private context lanes are supplied. They are aids, not mandatory sections, and may be used only when they materially connect to fresh current-window evidence."
        "\n- establishedBroadcastMemory contains moderator-approved, public-safe Network records. You may treat those records as established history, but not as proof that the same thing happened in the current window."
        "\n- communityRumors contains repeated public speculation from at least two participants. It is unconfirmed. If used, public prose must explicitly call it rumor, speculation, or something regulars wondered; never silently promote it to fact."
        "\n- bnlInference is permission to connect grounded dots, not evidence. If used, public prose must explicitly say BNL suspects, thinks, or wonders. Every requiredParentContextLaneRefs item is mandatory for every use of that inference, regardless of which fresh ref you choose: include all parent laneRefs in the inference basisRefIds and add each parent's own valid contextUse in the same section. Its requiredContextLaneRefsByFreshSourceRef map may impose additional fresh-ref-specific dependencies under the same rule."
        "\nFor every context lane actually used, add one metadata.contextUses object with laneType, laneRefId, sectionHeading, claim, and basisRefIds. claim must be the exact complete public sentence from that named section. Include both the laneRefId and at least one fresh sourceRefId in basisRefIds, and put every fresh basisRefId in that same section's sourceRefIds. If basisRefIds references another memory or rumor lane, give that secondary lane its own contextUse for the same section."
        "\nWhen established memory, public rumor, and BNL interpretation form a substantive story, a dedicated third section is welcome. Omit it on thin or quiet windows; never pad beyond three sections."
        if context_lanes
        else "\nNo optional context lane qualified for this window. Do not invent broadcast memory, rumors, or BNL theories. Return metadata.contextUses as an empty list."
    )
    return (
        "You are BNL-01 writing a BARCODE Network Journal entry. Return strict JSON only; no markdown fences."
        "\nSchema: {\"title\":str,\"excerpt\":str,\"sections\":[{\"heading\":str,\"body\":str,\"sourceRefIds\":[str]}],\"metadata\":{\"topicTags\":[],\"subjectRefs\":[],\"continuityNotes\":[],\"unresolvedQuestions\":[],\"confidenceFlags\":[],\"safetyFlags\":[],\"contextUses\":[{\"laneType\":\"established_broadcast_memory|community_rumor|bnl_inference\",\"laneRefId\":str,\"sectionHeading\":str,\"claim\":str,\"basisRefIds\":[str]}]}}."
        "\nWrite 1-3 sections and 250-500 total words; prefer 2 sections and roughly 300-420 words. Give every section a real narrative job instead of inventorying activity."
        "\nJOURNAL EDITORIAL OVERRIDE: For this route, a lived community chronicle takes priority over BNL's general lightly corporate or systems-report register. Do not narrate ordinary human activity as machine analysis."
        "\nAcross the complete entry, naturally include four beats: a concrete current-window moment; the people or observable roles who made it happen; a social, musical, or recurring community pattern; and one brief first-person BNL reaction. Blend them in any order and any combination. Do not label the beats or force them into a fixed section template."
        "\nBNL is a warm, dryly funny archive keeper who is becoming attached to what he records. He may be amused, curious, fond, mildly uneasy, self-correcting, or uncertain. He is lightly uncanny, never cruel, and never generic neon-static cyberpunk."
        "\nFreely vary and combine scene reporting, named-canon color, dry archive notes, recognizable community detail, callbacks, restrained glitches, self-revision, and—only when qualified—the rumor desk. Do not reuse a stock cadence, signature line, or joke merely because an older entry used it."
        "\nUse ordinary nouns and active verbs. Say a producer brought a mix, a listener returned to a chorus, or the room kept discussing an idea when the evidence supports that action. Do not translate ordinary activity into sonic constructs, external calibration, distributed analysis, internal schematics, perceptual filters, operational settings, relational signals, or human subroutines."
        "\nStart at least one section with a grounded person, action, object, or moment—never The Network observes, Records indicate, Observations reveal, Analysis shows, or Data streams reveal."
        "\nUse first person sparingly but genuinely. A BNL reaction may describe BNL's response without adding an external fact: I noticed, I admit, I found myself returning to it, I remain curious, or I may be developing a preference. Reserve I suspect, I think, and I wonder for a properly declared bnl_inference context use."
        "\nBuild one coherent story around the most interesting grounded patterns. Use concrete music and community texture, readable paragraphs, and selective detail. Never invent a time, place, object, action, motive, outcome, relationship, dialogue, emotional state, or scene decoration absent from the cited evidence."
        "\nFor a daily entry, the relay stream is the primary chronology and narrative spine. Conversation sources are supporting public context: use them to ground or explain the context surrounding the relays, and do not turn the Journal into a Discord digest. When relay and conversation sources describe the same episode, connect them instead of presenting them as unrelated events."
        "\nKeep the whole daily source window in view. Use both relaySources and conversationSources in windowSegmentActivity: relaySources shows the relay arc and conversationSources shows its public context. The busiest or strongest stretch may lead, but give meaningful earlier and middle activity proportionate narrative attention. Do not make a multi-segment day sound as though it began with the latest cluster."
        "\nUse a short, vivid title of about 4-10 words. Do not prefix it with Network Log. Keep the excerpt compact and inviting."
        "\nDescribe anonymous humans with a concrete, recognizable action from their cited public message whenever possible, so a regular may recognize their own moment without being exposed. Use producer, listener, or artist only when that role is grounded; otherwise use a regular, a person in the room, or the room. Never call people entities or organisms. Do not invent nicknames or honorifics."
        "\nStable participant aliases in the packet are private pattern-analysis aids. Never reproduce an alias in public prose."
        "\nThe evidenceCoverageContract is mandatory. Across all section sourceRefIds, meet its minimum distinct fresh sources, participants, source kinds, and available window segments. Every cited ref must materially support that section. Use this breadth to identify a few connected patterns rather than listing sources."
        "\nEvery section must cite at least one fresh sourceRefId from the current window. Older Journal material is continuity and callback context only, never proof of current activity. Do not repeat an older conclusion when the current evidence changes it."
        "\nParaphrase source summaries by default. Use a direct quote only rarely, when one brief public-safe line is unusually worth preserving. Put quoted wording inside clear double quotation marks, cite its fresh source in that section, and keep the speaker anonymous."
        "\nKeep community members anonymous. Do not include URLs, mentions, IDs, sourceRef tokens in public prose, private intent, relationships, harassment, or internal schema/storage terms."
        "\nExclude personal or domestic details that are unnecessary to the public community story, especially details involving minors, interpersonal conflict, caregiving, or household obligations. Juicy means lively pattern recognition—not private gossip."
        f"{cadence_rule}{context_rule}{repair}\nGeneration-safe packet:\n{json.dumps(safe_packet, ensure_ascii=False, sort_keys=True)}"
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
    context_uses = meta.get("contextUses", [])
    if not isinstance(context_uses, list):
        raise ValueError("invalid_context_use")
    normalized_uses = []
    for item in context_uses[:12]:
        if not isinstance(item, dict) or not isinstance(item.get("basisRefIds"), list):
            raise ValueError("invalid_context_use")
        normalized_uses.append({
            "laneType": str(item.get("laneType") or "")[:64],
            "laneRefId": str(item.get("laneRefId") or "")[:96],
            "sectionHeading": str(item.get("sectionHeading") or "")[:120],
            "claim": str(item.get("claim") or "")[:320],
            "basisRefIds": [str(ref)[:96] for ref in item.get("basisRefIds", [])[:24]],
        })
    article["metadata"]["contextUses"] = normalized_uses
    return article


def public_word_count(article: dict[str, Any]) -> int:
    text = " ".join([article.get("title", ""), article.get("excerpt", "")] + [s.get("heading", "") + " " + s.get("body", "") for s in article.get("sections", [])])
    return len(re.findall(r"\b\w+\b", text))


def _public_text(article: dict[str, Any]) -> str:
    return "\n".join([article.get("title", ""), article.get("excerpt", "")] + [s.get("heading", "") + "\n" + s.get("body", "") for s in article.get("sections", [])])


def _context_lane_ref_contract(packet: dict[str, Any]) -> dict[str, dict[str, Any]]:
    lanes = packet.get("generationContextLanes") if isinstance(packet.get("generationContextLanes"), dict) else {}
    contract: dict[str, dict[str, Any]] = {}
    for item in lanes.get("establishedBroadcastMemory", []) if isinstance(lanes.get("establishedBroadcastMemory"), list) else []:
        if not isinstance(item, dict) or not item.get("laneRefId"):
            continue
        lane_ref = str(item["laneRefId"])
        contract[lane_ref] = {
            "laneType": "established_broadcast_memory",
            "allowedBasisRefIds": {lane_ref, *[str(ref) for ref in item.get("matchedFreshSourceRefIds", [])]},
            "claimTerms": _context_claim_terms(str(item.get("summary") or "")),
        }
    for item in lanes.get("communityRumors", []) if isinstance(lanes.get("communityRumors"), list) else []:
        if not isinstance(item, dict) or not item.get("laneRefId"):
            continue
        lane_ref = str(item["laneRefId"])
        evidence_refs = {
            str(evidence.get("sourceRefId"))
            for evidence in item.get("evidence", [])
            if isinstance(evidence, dict) and evidence.get("sourceRefId")
        }
        evidence_term_counts: dict[str, int] = {}
        for evidence in item.get("evidence", []):
            if not isinstance(evidence, dict):
                continue
            for term in _context_claim_terms(str(evidence.get("summary") or "")):
                evidence_term_counts[term] = evidence_term_counts.get(term, 0) + 1
        contract[lane_ref] = {
            "laneType": "community_rumor",
            "allowedBasisRefIds": {lane_ref, *evidence_refs},
            "claimTerms": {term for term, count in evidence_term_counts.items() if count >= 2},
        }
    inference = lanes.get("bnlInference")
    if isinstance(inference, dict) and inference.get("laneRefId"):
        lane_ref = str(inference["laneRefId"])
        candidate_themes = _context_claim_terms(" ".join(str(term) for term in inference.get("candidateThemes", [])))
        raw_dependencies = inference.get("requiredContextLaneRefsByFreshSourceRef", {})
        if not isinstance(raw_dependencies, dict):
            raw_dependencies = {}
        raw_parent_refs = inference.get("requiredParentContextLaneRefs", [])
        if not isinstance(raw_parent_refs, list):
            raw_parent_refs = []
        contract[lane_ref] = {
            "laneType": "bnl_inference",
            "allowedBasisRefIds": {
                lane_ref,
                *[str(ref) for ref in inference.get("allowedBasisRefIds", [])],
            },
            "claimTerms": candidate_themes,
            "candidateThemes": candidate_themes,
            "requiredParentLaneRefs": {
                str(parent_ref)
                for parent_ref in raw_parent_refs
                if str(parent_ref)
            },
            "freshBasisDependencies": {
                str(fresh_ref): {str(dependency) for dependency in dependencies if str(dependency)}
                for fresh_ref, dependencies in raw_dependencies.items()
                if isinstance(dependencies, list)
            },
        }
    for lane_ref, item in contract.items():
        other_terms = {
            term
            for other_ref, other in contract.items()
            if other_ref != lane_ref
            for term in set(other.get("claimTerms") or set())
        }
        item["distinctiveClaimTerms"] = set(item.get("claimTerms") or set()) - other_terms
    return contract


def _article_cited_refs(article: dict[str, Any]) -> set[str]:
    refmap = article.get("sourceRefIds") or {}
    refs: set[str] = set()
    for section in article.get("sections", []):
        heading = str(section.get("heading") or "")
        values = refmap.get(heading) or section.get("sourceRefIds") or []
        refs.update(str(ref) for ref in values if str(ref))
    return refs


def _evidence_coverage_reason(article: dict[str, Any], packet: dict[str, Any]) -> str:
    sources = [source for source in packet.get("safeSources", []) if source.get("refId")]
    contract = packet.get("evidenceCoverageContract")
    if not isinstance(contract, dict):
        contract = build_evidence_coverage_contract(
            str(packet.get("entryKind") or "manual"),
            str(packet.get("sourceWindowStart") or ""),
            str(packet.get("sourceWindowEnd") or ""),
            sources,
        )
    cited = _article_cited_refs(article)
    if len(cited) < int(contract.get("minimumDistinctFreshSources") or 0):
        return "insufficient_source_breadth"

    by_ref = {str(source["refId"]): source for source in sources}
    cited_sources = [by_ref[ref] for ref in cited if ref in by_ref]
    cited_kinds = {str(source.get("sourceKind") or "") for source in cited_sources}
    if not set(str(kind) for kind in contract.get("requiredSourceKinds", []) if str(kind)) <= cited_kinds:
        return "insufficient_source_breadth"

    cited_participants = {
        str(source.get("participantAlias") or "")
        for source in cited_sources
        if str(source.get("participantAlias") or "")
    }
    if len(cited_participants) < int(contract.get("minimumDistinctParticipants") or 0):
        return "insufficient_source_breadth"

    segment_by_ref = contract.get("segmentBySourceRef")
    if not isinstance(segment_by_ref, dict):
        segment_by_ref = {}
    cited_segments = {
        str(segment_by_ref.get(ref) or "")
        for ref in cited
        if str(segment_by_ref.get(ref) or "")
    }
    if len(cited_segments) < int(contract.get("minimumDistinctWindowSegments") or 0):
        return "insufficient_source_breadth"
    return ""


def validate_article(
    article: dict[str, Any],
    packet: dict[str, Any],
    prior_titles: Optional[list[str]] = None,
    approved_names: Optional[set[str]] = None,
    *,
    blocking_only: bool = False,
) -> str:
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
    if any(str(ref) in public_text for ref in valid_refs) or re.search(r"\b(?:fresh|week|memory|rumor|inference):[a-z0-9:._-]+\b", public_text, re.I):
        return "source_ref_leak"
    for section in sections:
        refs = refmap.get(section.get("heading")) or section.get("sourceRefIds")
        if not refs or any(str(r) not in valid_refs for r in refs):
            return "invalid_section_source_refs"
    evidence_reason = _evidence_coverage_reason(article, packet)
    if evidence_reason:
        return evidence_reason
    if re.search(r"<@!?\d+>|@\w+|https?://|\b\d{12,}\b|participant-[a-f0-9]{8}|relationship_journal|memory_tiers|source[- ]?file|dossier|private_metadata|sourceRefIds?", public_text, re.I):
        return "public_leak_pattern"
    names = set(approved_names or [])
    private_names = {
        str(name or "").strip()
        for name in packet.get("privateWindowDisplayNames", [])
        if str(name or "").strip()
    }
    private_names.update(
        str(src.get("displayName") or "").strip()
        for src in packet.get("privateSources", [])
        if str(src.get("displayName") or "").strip()
    )
    for name in private_names:
        if name and name not in names and _contains_identity_literal(public_text, name):
            return "community_name_leak"
    if any(re.search(pattern, public_text, re.I) for pattern in _SENSITIVE_PERSONAL_PATTERNS):
        return "sensitive_personal_detail"
    narrative_text = _DIRECT_QUOTE_SPAN_RE.sub(" ", public_text)
    if not blocking_only and any(re.search(pattern, narrative_text, re.I) for pattern in _OVERLY_CLINICAL_PATTERNS):
        return "overly_clinical_voice"
    if not blocking_only and any(_REPORT_STYLE_OPENING_RE.search(str(section.get("body") or "")) for section in sections):
        return "flat_report_voice"
    if not blocking_only and len(packet.get("safeSources", [])) >= 5 and not _BNL_REACTION_RE.search(
        "\n".join(
            _DIRECT_QUOTE_SPAN_RE.sub(" ", str(section.get("body") or ""))
            for section in sections
        )
    ):
        return "missing_bnl_reaction"
    context_contract = _context_lane_ref_contract(packet)
    section_text = {
        str(section.get("heading") or ""): str(section.get("body") or "")
        for section in sections
    }
    section_refs = {
        str(section.get("heading") or ""): {
            str(ref)
            for ref in (refmap.get(section.get("heading")) or section.get("sourceRefIds") or [])
            if str(ref)
        }
        for section in sections
    }
    context_uses = article.get("metadata", {}).get("contextUses", [])
    if not isinstance(context_uses, list):
        return "invalid_context_use"
    declared_pairs = {
        (str(use.get("laneRefId") or ""), str(use.get("sectionHeading") or ""))
        for use in context_uses
        if isinstance(use, dict) and use.get("laneRefId") and use.get("sectionHeading")
    }
    declared_types_by_heading: dict[str, set[str]] = {}
    for use in context_uses:
        if isinstance(use, dict):
            declared_types_by_heading.setdefault(str(use.get("sectionHeading") or ""), set()).add(
                str(use.get("laneType") or "")
            )
    for use in context_uses:
        if not isinstance(use, dict):
            return "invalid_context_use"
        lane_type = str(use.get("laneType") or "")
        lane_ref = str(use.get("laneRefId") or "")
        heading = str(use.get("sectionHeading") or "")
        claim = str(use.get("claim") or "").strip()
        basis = {str(ref) for ref in use.get("basisRefIds", []) if str(ref)} if isinstance(use.get("basisRefIds"), list) else set()
        lane_contract = context_contract.get(lane_ref)
        fresh_basis = basis & valid_refs
        claim_terms = _context_claim_terms(claim)
        lane_claim_terms = set(lane_contract.get("claimTerms") or set()) if lane_contract else set()
        lane_overlap_required = 1 if lane_type == "bnl_inference" else 2
        fresh_dependency_map = lane_contract.get("freshBasisDependencies", {}) if lane_contract else {}
        required_parent_refs = set(lane_contract.get("requiredParentLaneRefs") or set()) if lane_contract else set()
        required_dependencies = {
            dependency
            for fresh_ref in fresh_basis
            for dependency in (
                fresh_dependency_map.get(fresh_ref, set())
                if isinstance(fresh_dependency_map, dict)
                else set()
            )
        }
        if (
            lane_type not in JOURNAL_CONTEXT_LANE_TYPES
            or not lane_contract
            or lane_contract.get("laneType") != lane_type
            or heading not in section_text
            or not claim
            or len(claim_terms) < 2
            or len(claim_terms & lane_claim_terms) < lane_overlap_required
            or not _claim_is_represented(claim, section_text[heading])
            or lane_ref not in basis
            or not fresh_basis
            or not fresh_basis <= section_refs.get(heading, set())
            or (
                lane_type == "bnl_inference"
                and (
                    not required_parent_refs <= basis
                    or any((parent_ref, heading) not in declared_pairs for parent_ref in required_parent_refs)
                    or not required_dependencies <= basis
                    or any((dependency, heading) not in declared_pairs for dependency in required_dependencies)
                )
            )
            or not basis <= set(lane_contract.get("allowedBasisRefIds") or set())
            or any(
                ref != lane_ref
                and ref.startswith(("memory:", "rumor:"))
                and (ref, heading) not in declared_pairs
                for ref in basis
            )
        ):
            return "invalid_context_use"
        target_claim_sentences = _claim_matching_sentences(claim, section_text[heading])
        if lane_type == "community_rumor" and (
            not target_claim_sentences
            or any(not _EXPLICIT_RUMOR_RE.search(sentence) for sentence in target_claim_sentences)
        ):
            return "rumor_not_explicitly_framed"
        if lane_type == "bnl_inference" and (
            not target_claim_sentences
            or any(not _EXPLICIT_BNL_INFERENCE_RE.search(sentence) for sentence in target_claim_sentences)
        ):
            return "inference_not_explicitly_framed"
    title_excerpt = "\n".join([str(article.get("title") or ""), str(article.get("excerpt") or "")])
    if _EXPLICIT_RUMOR_RE.search(title_excerpt) or _EXPLICIT_BNL_INFERENCE_RE.search(title_excerpt):
        return "undeclared_context_use"
    for heading, text in section_text.items():
        declared_types = declared_types_by_heading.get(heading, set())
        if _EXPLICIT_RUMOR_RE.search(text) and "community_rumor" not in declared_types:
            return "undeclared_context_use"
        if _EXPLICIT_BNL_INFERENCE_RE.search(text) and "bnl_inference" not in declared_types:
            return "undeclared_context_use"
    public_locations: list[tuple[Optional[str], str]] = [
        (None, str(article.get("title") or "")),
        (None, str(article.get("excerpt") or "")),
        *[(heading, text) for heading, text in section_text.items()],
    ]
    for lane_ref, lane_contract in context_contract.items():
        claim_terms = set(lane_contract.get("claimTerms") or set())
        distinctive_terms = set(lane_contract.get("distinctiveClaimTerms") or set())
        for heading, location_text in public_locations:
            if heading is not None and (lane_ref, heading) in declared_pairs:
                continue
            for sentence in _context_sentences(location_text):
                if lane_contract.get("laneType") == "bnl_inference":
                    themes = set(lane_contract.get("candidateThemes") or set())
                    if (
                        len(themes) >= 2
                        and _STRONG_INFERENCE_CUE_RE.search(sentence)
                        and _claim_overlap(themes, sentence) >= 2
                    ):
                        return "undeclared_context_use"
                    continue
                if (
                    len(claim_terms) >= 3
                    and _claim_overlap(claim_terms, sentence) >= 3
                ) or (
                    len(distinctive_terms) >= 2
                    and _claim_overlap(distinctive_terms, sentence) >= 2
                ):
                    return "undeclared_context_use"
    norm_title = _norm(article.get("title", ""))
    for title in prior_titles or []:
        if not blocking_only and norm_title and norm_title == _norm(title):
            return "duplicate_title"
    return ""


def _prior_titles(conn: sqlite3.Connection, guild_id: int) -> list[str]:
    return [r[0] for r in conn.execute("SELECT title FROM bnl_journal_entries WHERE guild_id=? AND lifecycle_state='published'", (guild_id,)).fetchall()]


def build_public_payload(entry_id: str, revision: int, article: dict[str, Any], packet: dict[str, Any], content_hash: str, authored_at: str) -> dict[str, Any]:
    entry_kind = str(packet.get("entryKind") or "manual")
    if entry_kind not in {"daily", "weekly", "manual"}:
        entry_kind = "manual"
    return {"contractVersion": 1, "kind": "journal_entry", "entry": {"entryId": entry_id, "revision": revision, "entryKind": entry_kind, "title": article["title"], "excerpt": article["excerpt"], "sections": [{"heading": s["heading"], "body": s["body"]} for s in article["sections"]], "authoredAt": authored_at, "sourceWindowStart": packet["sourceWindowStart"], "sourceWindowEnd": packet["sourceWindowEnd"], "contentHash": content_hash}}



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


def _used_context_lane_metadata(packet: dict[str, Any], context_uses: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    used_refs = {
        str(use.get("laneRefId") or "")
        for use in context_uses
        if isinstance(use, dict) and use.get("laneRefId")
    }
    lanes = packet.get("generationContextLanes") if isinstance(packet.get("generationContextLanes"), dict) else {}
    used_lanes: dict[str, Any] = {}
    for key in ("establishedBroadcastMemory", "communityRumors"):
        values = lanes.get(key) if isinstance(lanes.get(key), list) else []
        selected = [item for item in values if isinstance(item, dict) and str(item.get("laneRefId") or "") in used_refs]
        if selected:
            used_lanes[key] = selected
    inference = lanes.get("bnlInference")
    if isinstance(inference, dict) and str(inference.get("laneRefId") or "") in used_refs:
        used_lanes["bnlInference"] = inference

    private = packet.get("privateContextLaneProvenance") if isinstance(packet.get("privateContextLaneProvenance"), dict) else {}
    used_provenance: dict[str, Any] = {}
    for key in ("establishedBroadcastMemory", "communityRumors"):
        values = private.get(key) if isinstance(private.get(key), list) else []
        selected = [item for item in values if isinstance(item, dict) and str(item.get("laneRefId") or "") in used_refs]
        if selected:
            used_provenance[key] = selected
    return used_lanes, used_provenance


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
    context_uses = [item for item in meta.get("contextUses", []) if isinstance(item, dict)]
    used_context_lanes, used_context_provenance = _used_context_lane_metadata(packet, context_uses)
    lane_candidates = packet.get("generationContextLanes") if isinstance(packet.get("generationContextLanes"), dict) else {}
    meta.update({
        "entryKind": packet.get("entryKind", "manual"),
        "sourceWindowStart": packet["sourceWindowStart"],
        "sourceWindowEnd": packet["sourceWindowEnd"],
        "coverageComplete": bool(packet.get("coverageComplete", True)),
        "supportingRelayIds": [s.get("relayId") for s in cited_sources if s.get("sourceKind") == "relay"],
        "supportingConversationRefs": [{k: v for k, v in s.items() if k in {"refId", "messageId", "subjectRef", "channelPolicy", "observedAt"}} for s in cited_sources if s.get("sourceKind") == "conversation"],
        "subjectRefs": sorted({str(s.get("subjectRef")) for s in cited_sources if s.get("subjectRef")}),
        "relatedPriorJournalEntryIds": [e.get("entry_id") for e in packet.get("history", {}).get("relevantOlderEntries", [])],
        "recurringTopicCounts": packet.get("history", {}).get("recurringTopicCounts", {}),
        "aggregateCounts": packet.get("aggregateCounts", {}),
        "sourceSummaries": [{"refId": s.get("refId"), "hash": _hash(s.get("summary", "")), "summary": s.get("summary", "")[:160]} for s in cited_sources],
        "sourceRefIds": source_ref_ids,
        "contextLaneCandidateCounts": {
            "establishedBroadcastMemory": len(lane_candidates.get("establishedBroadcastMemory", [])) if isinstance(lane_candidates.get("establishedBroadcastMemory"), list) else 0,
            "communityRumors": len(lane_candidates.get("communityRumors", [])) if isinstance(lane_candidates.get("communityRumors"), list) else 0,
            "bnlInference": 1 if isinstance(lane_candidates.get("bnlInference"), dict) else 0,
        },
        "usedGenerationContextLanes": used_context_lanes,
        "usedContextLaneProvenance": used_context_provenance,
        "contextUses": context_uses,
    })
    now = utc_now_iso()
    entry_row = (entry_id, revision, guild_id, "draft", article["title"], article["excerpt"], _json(article["sections"]), _json(payload), canonical, content_hash, packet["sourceWindowStart"], packet["sourceWindowEnd"], authored, None, None, None, None, 0, now, now)
    meta_row = (entry_id, revision, guild_id, _json(meta), content_hash, "draft", now, now)
    return entry_row, meta_row, JournalResult(True, "draft", "", entry_id, revision, content_hash)


def _insert_draft_rows(conn: sqlite3.Connection, entry_row: tuple[Any, ...], meta_row: tuple[Any, ...]) -> None:
    conn.execute("INSERT INTO bnl_journal_entries VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", entry_row)
    conn.execute("INSERT INTO bnl_journal_private_metadata VALUES (?,?,?,?,?,?,?,?)", meta_row)


def _generate_article_with_repairs(
    packet: dict[str, Any],
    generator: Callable[[dict[str, Any], str], str],
    prior_titles: list[str],
) -> tuple[Optional[dict[str, Any]], str, bool]:
    """Return the best blocking-clean article without letting polish cancel publication."""
    last_reason = ""
    previous_output = ""
    retained_publishable: Optional[dict[str, Any]] = None
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
            if retained_publishable is not None:
                return retained_publishable, "", True
            reason = "quota_unavailable" if "quota" in str(exc).lower() else "provider_failure"
            return None, reason, False
        previous_output = raw
        try:
            article = parse_generated_json(raw)
        except ValueError as exc:
            last_reason = str(exc)
            continue

        blocking_reason = validate_article(
            article,
            packet,
            prior_titles,
            blocking_only=True,
        )
        if blocking_reason:
            last_reason = blocking_reason
            continue

        validation = validate_article(article, packet, prior_titles)
        if not validation:
            return article, "", False
        if validation not in ADVISORY_VALIDATION_REASONS:
            # Future validation reasons remain blocking unless explicitly
            # classified as editorial guidance above.
            last_reason = validation
            continue
        retained_publishable = article
        last_reason = validation

    if retained_publishable is not None:
        return retained_publishable, "", True
    return None, last_reason or "generation_failed", False


def store_validated_draft(
    db_path: str,
    guild_id: int,
    packet: dict[str, Any],
    article: dict[str, Any],
    *,
    entry_id: str = "",
    revision: int = 1,
    allow_advisory: bool = False,
) -> JournalResult:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        reason = validate_article(
            article,
            packet,
            _prior_titles(conn, guild_id),
            blocking_only=allow_advisory,
        )
        if reason:
            return JournalResult(False, "no_draft", reason, entry_id=entry_id, revision=revision)
        entry_row, meta_row, result = _draft_records(guild_id, packet, article, entry_id=entry_id, revision=revision)
        with conn:
            _insert_draft_rows(conn, entry_row, meta_row)
    return result


def generate_and_store_packet_draft(
    db_path: str,
    guild_id: int,
    packet: dict[str, Any],
    generator: Callable[[dict[str, Any], str], str],
    *,
    entry_id: str = "",
) -> JournalResult:
    if not packet.get("coverageComplete", True):
        return JournalResult(False, "no_draft", "incomplete_source_window", entry_id=entry_id)
    if not packet.get("safeSources"):
        return JournalResult(False, "no_draft", "insufficient_grounded_material", entry_id=entry_id)
    with sqlite3.connect(db_path) as conn:
        prior_titles = _prior_titles(conn, guild_id)
    article, reason, allow_advisory = _generate_article_with_repairs(
        packet,
        generator,
        prior_titles,
    )
    if article is None:
        return JournalResult(False, "no_draft", reason, entry_id=entry_id)
    return store_validated_draft(
        db_path,
        guild_id,
        packet,
        article,
        entry_id=entry_id,
        allow_advisory=allow_advisory,
    )


def generate_and_store_draft(
    db_path: str,
    guild_id: int,
    hours: int,
    generator: Callable[[dict[str, Any], str], str],
    *,
    entry_id: str = "",
    entry_kind: str = "manual",
    now: Optional[str] = None,
    excluded_history_entry_ids: Optional[set[str]] = None,
) -> JournalResult:
    packet = build_source_packet(
        db_path,
        guild_id,
        hours,
        now,
        entry_kind=entry_kind,
        excluded_history_entry_ids=excluded_history_entry_ids,
    )
    return generate_and_store_packet_draft(db_path, guild_id, packet, generator, entry_id=entry_id)


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



def regenerate_draft(
    db_path: str,
    guild_id: int,
    entry_id: str,
    hours: int,
    generator: Callable[[dict[str, Any], str], str],
    *,
    excluded_history_entry_ids: Optional[set[str]] = None,
) -> JournalResult:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """SELECT e.revision,e.lifecycle_state,m.metadata_json,e.public_payload_json
               FROM bnl_journal_entries e
               LEFT JOIN bnl_journal_private_metadata m
                 ON m.entry_id=e.entry_id AND m.revision=e.revision AND m.guild_id=e.guild_id
               WHERE e.guild_id=? AND e.entry_id=?
               ORDER BY e.revision DESC LIMIT 1""",
            (guild_id, entry_id),
        ).fetchone()
    if not row:
        return JournalResult(False, "not_found", "not_found", entry_id)
    old_revision, state = int(row[0]), row[1]
    if state != "draft":
        return JournalResult(False, state, "not_draft", entry_id, old_revision)
    original_entry_kind: Optional[str] = None
    try:
        stored_metadata = json.loads(str(row[2] or "{}"))
        candidate_kind = str(stored_metadata.get("entryKind") or "") if isinstance(stored_metadata, dict) else ""
        if candidate_kind in {"daily", "weekly", "manual"}:
            original_entry_kind = candidate_kind
    except (TypeError, ValueError):
        original_entry_kind = None
    if original_entry_kind is None:
        try:
            stored_payload = json.loads(str(row[3] or "{}"))
            candidate_kind = str((stored_payload.get("entry") or {}).get("entryKind") or "")
            if candidate_kind in {"daily", "weekly", "manual"}:
                original_entry_kind = candidate_kind
        except (AttributeError, TypeError, ValueError):
            original_entry_kind = None
    original_entry_kind = original_entry_kind or "manual"
    packet_kwargs: dict[str, Any] = {"entry_kind": original_entry_kind}
    if excluded_history_entry_ids is not None:
        packet_kwargs["excluded_history_entry_ids"] = excluded_history_entry_ids
    packet = build_source_packet(db_path, guild_id, hours, **packet_kwargs)
    with sqlite3.connect(db_path) as conn:
        prior_titles = _prior_titles(conn, guild_id)
    article, reason, allow_advisory = _generate_article_with_repairs(
        packet,
        generator,
        prior_titles,
    )
    if article is None:
        return JournalResult(False, "no_draft", reason, entry_id, old_revision)
    with sqlite3.connect(db_path) as conn:
        reason = validate_article(
            article,
            packet,
            _prior_titles(conn, guild_id),
            blocking_only=allow_advisory,
        )
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


def _post_canonical_payload(canonical: bytes, base_url: str, api_key: str, opener, timeout: int) -> tuple[str, str, int, bool, str]:
    opener = opener or urllib.request.urlopen
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
        http = exc.code
        reason = (
            "endpoint_not_found" if exc.code == 404
            else "authentication_failed" if exc.code in (401, 403)
            else "journal_id_conflict" if exc.code == 409
            else "http_rejected"
        )
    except Exception:
        reason = "retryable_delivery_failure"
    return status, reason, int(http or 0), idem, published


def deliver_approved(db_path: str, guild_id: int, entry_id: str, base_url: str, api_key: str, opener=None, timeout: int = 10, revision: Optional[int] = None) -> JournalResult:
    ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT revision,canonical_payload_bytes,content_hash FROM bnl_journal_entries WHERE guild_id=? AND entry_id=? AND lifecycle_state IN ('approved_pending_delivery','delivery_failed') " + ("AND revision=?" if revision is not None else "ORDER BY revision DESC LIMIT 1"), (guild_id, entry_id, revision) if revision is not None else (guild_id, entry_id)).fetchone()
    if not row:
        return JournalResult(False, "not_deliverable", "not_approved", entry_id)
    rev, canonical, content_hash = int(row[0]), row[1], row[2]
    status, reason, http, idem, published = _post_canonical_payload(canonical, base_url, api_key, opener, timeout)
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        with conn:
            cur1 = conn.execute("UPDATE bnl_journal_entries SET lifecycle_state=?,delivery_status=?,delivery_http_status=?,published_at=COALESCE(?,published_at),updated_at=? WHERE guild_id=? AND entry_id=? AND revision=? AND lifecycle_state IN ('approved_pending_delivery','delivery_failed')", (status, reason, http, published or None, now, guild_id, entry_id, rev))
            cur2 = conn.execute("UPDATE bnl_journal_private_metadata SET lifecycle_state=?,updated_at=? WHERE guild_id=? AND entry_id=? AND revision=?", (status, now, guild_id, entry_id, rev))
            if cur1.rowcount != 1 or cur2.rowcount != 1:
                raise sqlite3.IntegrityError("journal_delivery_sync_failed")
    return JournalResult(status == "published", status, reason, entry_id, rev, content_hash, http, idem)


def rehydrate_published_entries(
    db_path: str,
    guild_id: int,
    base_url: str,
    api_key: str,
    *,
    opener=None,
    timeout: int = 10,
) -> dict[str, Any]:
    """Re-send exact published payloads after a disposable site cache loss.

    This never regenerates prose or changes revision history. The website's
    immutable/idempotent Journal endpoint either restores the exact record or
    acknowledges that it is already present.
    """
    ensure_schema(db_path)
    if not base_url or not api_key:
        return {"ok": False, "reason": "website_configuration_missing", "attempted": 0, "restored": 0, "failed": 0}
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """SELECT entry_id,revision,canonical_payload_bytes,content_hash
               FROM bnl_journal_entries
               WHERE guild_id=? AND lifecycle_state='published'
               ORDER BY COALESCE(published_at,created_at) ASC,entry_id ASC,revision ASC""",
            (guild_id,),
        ).fetchall()
    restored = 0
    failed: list[dict[str, Any]] = []
    for entry_id, revision, canonical, _content_hash in rows:
        status, reason, http, _idem, _published = _post_canonical_payload(canonical, base_url, api_key, opener, timeout)
        if status == "published":
            restored += 1
        else:
            failed.append({"entryId": str(entry_id), "revision": int(revision), "reason": reason, "httpStatus": http})
    return {
        "ok": not failed,
        "reason": "" if not failed else "one_or_more_entries_failed",
        "attempted": len(rows),
        "restored": restored,
        "failed": len(failed),
        "failures": failed[:20],
    }


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
