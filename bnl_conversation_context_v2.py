"""Route-aware bounded conversation prompt context for BNL."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, Iterable

CONVERSATION_CONTEXT_VERSION = "conversation_context_v2"
SAME_ROOM_RECENCY_MINUTES = 45
CROSS_CHANNEL_RECENCY_MINUTES = 30
MAX_CANDIDATE_ROWS = 80
MAX_SAME_ROOM_PAIRS = 4
MAX_CROSS_CHANNEL_PAIRS = 1
MAX_UNPAIRED_ROWS = 2
MAX_RENDERED_CHARS = 2600
MAX_RENDERED_LINE_CHARS = 360
FUTURE_SKEW_SECONDS = 0
PUBLIC_SAFE_POLICIES = {"public_home", "public_context"}
SAME_CHANNEL_ONLY_PUBLIC_POLICIES = {"public_selective"}
SAME_CHANNEL_CONTEXT_POLICIES = PUBLIC_SAFE_POLICIES | SAME_CHANNEL_ONLY_PUBLIC_POLICIES | {"sealed_test"}
SEALED_POLICIES = {"sealed_test"}
BLOCKED_PUBLIC_POLICIES = {
    "internal_controlled", "broadcast_memory", "protected_system", "reference_canon",
    "ai_image_tool", "unknown", "legacy-unknown", "legacy_unknown", "",
}
CONVERSATION_CONTINUITY_ROUTES = {"normal_chat", "show_status_answer", "show_status", "direct_payload_task", "direct_payload"}
GREETING_ROUTES = {"simple_greeting"}
STOPWORDS = {
    "the", "and", "that", "with", "this", "those", "these", "them", "they", "you", "your", "for", "from",
    "was", "were", "are", "is", "a", "an", "it", "its", "why", "what", "how", "when", "where", "about",
    "into", "onto", "then", "than", "just", "like", "have", "has", "had", "but", "not", "can", "could", "would",
}

_WORD_RE = re.compile(r"[a-z0-9']+")
CORRECTION_RE = re.compile(r"\b(?:actually|correction|correcting|i meant|instead|not\s+that|that's wrong|that is wrong)\b", re.I)
BOUNDARY_RE = re.compile(r"\b(?:don't|do not|stop|never|please don't|no longer|boundary|avoid)\b", re.I)
OPEN_LOOP_RE = re.compile(r"\?|\b(?:which one|choose|pick|decide|i will|i'll|remind me|next time|use the first|use the second|second one|first one)\b", re.I)
STRONG_CONTINUATION_RE = re.compile(r"\b(?:continue(?:\s+\w+){0,6}\s+from before|earlier in (?:the )?(?:other )?conversation|same topic as before|pick up where we left off|from the other channel|from that other conversation)\b", re.I)
QUEUE_SUBJECT_RE = re.compile(r"\b(?:queue|session|payment|priority|priority signal|wheel|wheel spins|now playing|up next|current track|currently playing)\b", re.I)
QUEUE_ASSERTION_VERB_RE = re.compile(r"\b(?:is|are|has|have|contains?|includes?|holds?|waiting|queued|open|live|enabled|owed|paid|next|playing|active|currently|three|two|one|\d+)\b", re.I)
UNSAFE_HISTORY_RE = re.compile(r"(?:\b(?:provider|host|preview|embed|media_buffer|media-storage|media storage|storage_diagnostic|storage diagnostic)\s*=|\bstored[- ]visual[- ]description\b|\binternal diagnostic\b|\bsource-mode\b|\bmode contamination\b)", re.I)

@dataclass(frozen=True)
class ConversationContextRequest:
    guild_id: int
    current_user_id: int
    channel_id: int = 0
    channel_name: str = ""
    channel_policy: str = "unknown"
    route_mode: str = "normal_chat"
    conversation_surface: str = "unknown"
    current_message_ids: frozenset[int] = field(default_factory=frozenset)
    current_texts: tuple[str, ...] = ()
    current_participants: frozenset[int] = field(default_factory=frozenset)
    is_direct_target: bool = False
    is_reply_to_bnl: bool = False
    is_batch: bool = False
    is_deferred_payload_session: bool = False
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    route_allowed_sources: frozenset[str] = field(default_factory=frozenset)

@dataclass(frozen=True)
class ConversationContextResult:
    rendered_context: str
    selected_row_ids: tuple[int, ...]
    same_room_paired_turn_count: int
    unpaired_row_count: int
    cross_channel_paired_turn_count: int
    current_message_duplicates_removed: int
    visibility_policy_exclusions: int
    selection_reasons: tuple[str, ...]
    final_char_count: int
    contract_version: str = CONVERSATION_CONTEXT_VERSION
    enabled: bool = True
    fallback_reason: str = "selected"

@dataclass(frozen=True)
class _ParsedTime:
    valid: bool
    value: datetime | None = None


def normalize_text(text: str) -> str:
    return " ".join(_WORD_RE.findall((text or "").lower()))

def sanitize_history_text(text: str, limit: int = MAX_RENDERED_LINE_CHARS) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    cleaned = re.sub(r"\b(BNL-01|System|Current user request|User/member|User message)\s*:", lambda m: m.group(1).replace("-", "‑") + "﹕", cleaned, flags=re.I)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 1)].rstrip() + "…"

def _parse_time(value: Any) -> _ParsedTime:
    if value is None or str(value).strip() == "":
        return _ParsedTime(False)
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value or "").strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            try:
                dt = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                return _ParsedTime(False)
    dt = dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    return _ParsedTime(True, dt)

def _policy(row: dict) -> str:
    return str(row.get("channel_policy") or "unknown").strip().lower()

def _public_cross_compatible(source: str, target: str) -> bool:
    return source in PUBLIC_SAFE_POLICIES and target in PUBLIC_SAFE_POLICIES

def route_permits_continuity(route_mode: str, allowed_sources: Iterable[str] = ()) -> bool:
    route = (route_mode or "").strip().lower()
    return route in CONVERSATION_CONTINUITY_ROUTES or "conversation_continuity" in set(allowed_sources or ())

def _tokens(text: str) -> set[str]:
    return {w for w in _WORD_RE.findall((text or "").lower()) if len(w) > 2 and w not in STOPWORDS}

def _overlap(a: str, b: str) -> int:
    return len(_tokens(a) & _tokens(b))

def _is_current_duplicate(row: dict, req: ConversationContextRequest, current_norms: set[str]) -> bool:
    mid = int(row.get("message_id") or 0)
    if mid and mid in req.current_message_ids:
        return True
    role = str(row.get("role") or "").lower()
    participant_ids = set(req.current_participants or frozenset()) if req.is_batch else {int(req.current_user_id or 0)}
    if role == "user" and int(row.get("user_id") or 0) in participant_ids:
        return bool(normalize_text(row.get("content") or "") in current_norms)
    return False

def _same_channel_identity(a: dict, b: dict | ConversationContextRequest) -> bool:
    aid = int(a.get("channel_id") or 0)
    bid = int((b.get("channel_id") if isinstance(b, dict) else b.channel_id) or 0)
    if aid and bid:
        return aid == bid
    aname = str(a.get("channel_name") or "").strip().lower()
    bname = str((b.get("channel_name") if isinstance(b, dict) else b.channel_name) or "").strip().lower()
    return bool(aname and bname and aname == bname)

def _row_is_same_room(row: dict, req: ConversationContextRequest) -> bool:
    return _same_channel_identity(row, req)

def _can_pair(user_row: dict, model_row: dict) -> bool:
    return (
        int(user_row.get("user_id") or 0) == int(model_row.get("user_id") or 0)
        and _policy(user_row) == _policy(model_row)
        and _same_channel_identity(user_row, model_row)
    )

def _pair_rows(rows: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    pairs, unpaired_users, orphan_models = [], [], []
    pending: list[dict] = []
    for row in sorted(rows, key=lambda r: int(r.get("id") or 0)):
        role = str(row.get("role") or "").lower()
        if role == "user":
            pending.append(row)
            continue
        if role not in {"model", "assistant", "bnl"}:
            continue
        match_idx = None
        for idx in range(len(pending) - 1, -1, -1):
            if _can_pair(pending[idx], row):
                match_idx = idx
                break
        if match_idx is None:
            orphan_models.append(row)
            continue
        user_row = pending.pop(match_idx)
        pairs.append({"user": user_row, "model": row})
    unpaired_users.extend(pending)
    return pairs, unpaired_users, orphan_models

def _score_pair(pair: dict, req: ConversationContextRequest, current_text: str, same_room: bool, now: datetime) -> tuple[int, tuple[str, ...]]:
    u = pair["user"]; text = (u.get("content") or "") + " " + (pair["model"].get("content") or "")
    parsed = _parse_time(u.get("timestamp"))
    age_min = max(0, (now - parsed.value).total_seconds() / 60) if parsed.valid and parsed.value else 9999
    reasons = []
    score = 1000 if same_room else 100
    score += max(0, int(120 - age_min))
    score += 120; reasons.append("complete_pair")
    if int(u.get("user_id") or 0) == int(req.current_user_id or 0): score += 90; reasons.append("current_user")
    if int(u.get("user_id") or 0) in req.current_participants: score += 35; reasons.append("participant")
    if req.is_reply_to_bnl or req.is_direct_target: score += 20; reasons.append("direct_continuity")
    ov = _overlap(text, current_text)
    if ov: score += min(80, ov * 16); reasons.append("topic_overlap")
    if CORRECTION_RE.search(u.get("content") or ""): score += 70; reasons.append("correction")
    if BOUNDARY_RE.search(u.get("content") or ""): score += 65; reasons.append("boundary")
    if OPEN_LOOP_RE.search(text): score += 40; reasons.append("open_loop")
    return score, tuple(reasons)

def _row_age_ok(row: dict, now: datetime, minutes: int) -> bool:
    parsed = _parse_time(row.get("timestamp"))
    if not parsed.valid or not parsed.value:
        return False
    delta = (now - parsed.value).total_seconds()
    if delta < -FUTURE_SKEW_SECONDS:
        return False
    return delta <= minutes * 60

def _unsafe_row(row: dict) -> bool:
    role = str(row.get("role") or "").lower()
    content = str(row.get("content") or "")
    if UNSAFE_HISTORY_RE.search(content):
        return True
    if role in {"model", "assistant", "bnl"} and QUEUE_SUBJECT_RE.search(content) and QUEUE_ASSERTION_VERB_RE.search(content):
        return True
    return False

def _cross_channel_allowed(pair: dict, req: ConversationContextRequest, current_text: str) -> bool:
    if req.channel_policy in SAME_CHANNEL_ONLY_PUBLIC_POLICIES:
        return False
    if _policy(pair["user"]) in SAME_CHANNEL_ONLY_PUBLIC_POLICIES:
        return False
    pair_text = (pair["user"].get("content") or "") + " " + (pair["model"].get("content") or "")
    return bool(STRONG_CONTINUATION_RE.search(current_text or "") or _overlap(pair_text, current_text) >= 2)

def _append_block(lines: list[str], block: list[str], budget: int) -> bool:
    candidate = "\n".join(lines + block).strip()
    if len(candidate) > budget:
        return False
    lines.extend(block)
    return True

def assemble_conversation_context_v2(rows: Iterable[dict], req: ConversationContextRequest) -> ConversationContextResult:
    if not req.current_user_id or not route_permits_continuity(req.route_mode, req.route_allowed_sources) or (req.route_mode or "").lower() in GREETING_ROUTES:
        return ConversationContextResult("", (), 0, 0, 0, 0, 0, ("route_not_permitted",), 0, fallback_reason="route_not_permitted")
    target_policy = (req.channel_policy or "unknown").strip().lower()
    now = req.now.astimezone(timezone.utc) if req.now.tzinfo else req.now.replace(tzinfo=timezone.utc)
    current_norms = {normalize_text(t) for t in req.current_texts if normalize_text(t)}
    current_text = " ".join(req.current_texts)
    source_rows = list(rows)
    unsafe_paired_user_ids: set[int] = set()
    pending_for_unsafe: list[dict] = []
    for raw in sorted(source_rows, key=lambda r: int(r.get("id") or 0)):
        raw_role = str(raw.get("role") or "").lower()
        if raw_role == "user":
            pending_for_unsafe.append(raw)
            continue
        if raw_role in {"model", "assistant", "bnl"} and _unsafe_row(raw):
            for idx in range(len(pending_for_unsafe) - 1, -1, -1):
                if _can_pair(pending_for_unsafe[idx], raw):
                    unsafe_paired_user_ids.add(int(pending_for_unsafe[idx].get("id") or 0))
                    pending_for_unsafe.pop(idx)
                    break
    filtered, dupes, excluded = [], 0, 0
    for row in source_rows:
        p = _policy(row)
        same_room = _row_is_same_room(row, req)
        if int(row.get("id") or 0) in unsafe_paired_user_ids:
            excluded += 1; continue
        if _unsafe_row(row):
            excluded += 1; continue
        if same_room:
            if p != target_policy or (target_policy not in SAME_CHANNEL_CONTEXT_POLICIES): excluded += 1; continue
            if not _row_age_ok(row, now, SAME_ROOM_RECENCY_MINUTES): excluded += 1; continue
        else:
            if not _public_cross_compatible(p, target_policy): excluded += 1; continue
            if int(row.get("user_id") or 0) != int(req.current_user_id or 0): excluded += 1; continue
            if not _row_age_ok(row, now, CROSS_CHANNEL_RECENCY_MINUTES): excluded += 1; continue
        if p in BLOCKED_PUBLIC_POLICIES: excluded += 1; continue
        if _is_current_duplicate(row, req, current_norms): dupes += 1; continue
        filtered.append(dict(row, _same_room=same_room))
    pairs, unpaired_users, orphan_models = _pair_rows(filtered)
    scored_same, scored_cross = [], []
    for pair in pairs:
        same = bool(pair["user"].get("_same_room"))
        if not same and not _cross_channel_allowed(pair, req, current_text):
            excluded += 1; continue
        score, reasons = _score_pair(pair, req, current_text, same, now)
        (scored_same if same else scored_cross).append((score, pair, reasons))
    scored_same.sort(key=lambda x: (-x[0], int(x[1]["user"].get("id") or 0)))
    scored_cross.sort(key=lambda x: (-x[0], int(x[1]["user"].get("id") or 0)))
    selected_pairs = scored_same[:MAX_SAME_ROOM_PAIRS]
    selected_cross = [] if selected_pairs else scored_cross[:MAX_CROSS_CHANNEL_PAIRS]
    open_unpaired = []
    for r in sorted([r for r in unpaired_users if r.get("_same_room")], key=lambda r: int(r.get("id") or 0), reverse=True):
        text = r.get("content") or ""
        if OPEN_LOOP_RE.search(text) or _overlap(text, current_text) >= 1 or CORRECTION_RE.search(text) or BOUNDARY_RE.search(text):
            open_unpaired.append(r)
        if len(open_unpaired) >= MAX_UNPAIRED_ROWS:
            break
    candidates = []
    for _score, pair, why in selected_pairs:
        candidates.append((int(pair["user"].get("id") or 0), "same_pair", pair, why))
    for _score, pair, why in selected_cross:
        candidates.append((int(pair["user"].get("id") or 0), "cross_pair", pair, why))
    for row in open_unpaired:
        candidates.append((int(row.get("id") or 0), "unpaired_user", row, ("open_loop_unpaired",)))
    candidates.sort(key=lambda x: x[0])
    lines = []
    header = [
        "Conversation continuity (bounded; continuity-only, not canon/current-state evidence):",
        "- Prior BNL replies here are conversational continuity only. They do not prove canon, live show state, queue state, dossiers, payments, Priority, Wheel, or third-party facts.",
    ]
    row_ids: list[int] = []
    reasons: list[str] = []
    rendered_same = rendered_cross = rendered_unpaired = 0
    if candidates:
        if not _append_block(lines, header, MAX_RENDERED_CHARS):
            lines = []
        for _id, kind, item, why in candidates:
            if kind in {"same_pair", "cross_pair"}:
                block = [
                    f"User/member: {sanitize_history_text(item['user'].get('content') or '')}",
                    f"BNL-01: {sanitize_history_text(item['model'].get('content') or '')}",
                ]
                if not _append_block(lines, block, MAX_RENDERED_CHARS):
                    continue
                row_ids.extend([int(item["user"].get("id") or 0), int(item["model"].get("id") or 0)])
                if kind == "same_pair": rendered_same += 1
                else: rendered_cross += 1
                reasons.extend(why)
            elif kind == "unpaired_user":
                block = [f"User/member (open loop): {sanitize_history_text(item.get('content') or '')}"]
                if not _append_block(lines, block, MAX_RENDERED_CHARS):
                    continue
                row_ids.append(int(item.get("id") or 0)); rendered_unpaired += 1; reasons.extend(why)
    rendered = "\n".join(lines).strip()
    return ConversationContextResult(rendered, tuple(row_ids), rendered_same, rendered_unpaired, rendered_cross, dupes, excluded, tuple(sorted(set(reasons))) or ("no_relevant_context",), len(rendered), fallback_reason="selected" if rendered else "no_relevant_context")
