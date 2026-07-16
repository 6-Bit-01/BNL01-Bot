"""Route-aware bounded conversation prompt context for BNL."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
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
PUBLIC_SAFE_POLICIES = {"public_home", "public_context", "public_selective"}
SEALED_POLICIES = {"sealed_test"}
BLOCKED_PUBLIC_POLICIES = {
    "internal_controlled", "broadcast_memory", "protected_system", "reference_canon",
    "ai_image_tool", "unknown", "legacy-unknown", "legacy_unknown", "",
}
CONVERSATION_CONTINUITY_ROUTES = {"normal_chat", "show_status_answer", "show_status", "direct_payload_task", "direct_payload"}
GREETING_ROUTES = {"simple_greeting"}

_WORD_RE = re.compile(r"[a-z0-9']+")
CORRECTION_RE = re.compile(r"\b(?:actually|correction|correcting|i meant|instead|not\s+that|that's wrong|that is wrong)\b", re.I)
BOUNDARY_RE = re.compile(r"\b(?:don't|do not|stop|never|please don't|no longer|boundary|avoid)\b", re.I)
OPEN_LOOP_RE = re.compile(r"\?|\b(?:which one|choose|pick|decide|i will|i'll|remind me|next time|use the first|use the second|second one|first one)\b", re.I)
CONTINUATION_RE = re.compile(r"\b(?:that|this|those|these|it|them|why|use the|second one|first one|continue|from before|earlier|same topic|follow up)\b", re.I)

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


def normalize_text(text: str) -> str:
    return " ".join(_WORD_RE.findall((text or "").lower()))

def _parse_time(value: Any, now: datetime) -> datetime:
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
                return now
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)

def _policy(row: dict) -> str:
    return str(row.get("channel_policy") or "unknown").strip().lower()

def _public_compatible(source: str, target: str) -> bool:
    return source in PUBLIC_SAFE_POLICIES and target in PUBLIC_SAFE_POLICIES

def route_permits_continuity(route_mode: str, allowed_sources: Iterable[str] = ()) -> bool:
    route = (route_mode or "").strip().lower()
    return route in CONVERSATION_CONTINUITY_ROUTES or "conversation_continuity" in set(allowed_sources or ())

def _tokens(text: str) -> set[str]:
    return {w for w in _WORD_RE.findall((text or "").lower()) if len(w) > 2}

def _overlap(a: str, b: str) -> int:
    return len(_tokens(a) & _tokens(b))

def _is_current_duplicate(row: dict, req: ConversationContextRequest, current_norms: set[str]) -> bool:
    mid = int(row.get("message_id") or 0)
    if mid and mid in req.current_message_ids:
        return True
    role = str(row.get("role") or "").lower()
    if role == "user" and int(row.get("user_id") or 0) == int(req.current_user_id or 0):
        return bool(normalize_text(row.get("content") or "") in current_norms)
    return False

def _row_is_same_room(row: dict, req: ConversationContextRequest) -> bool:
    if req.channel_id and int(row.get("channel_id") or 0) == int(req.channel_id):
        return True
    return bool(req.channel_name and str(row.get("channel_name") or "").strip().lower() == req.channel_name.strip().lower())

def _pair_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    pairs, unpaired = [], []
    ordered = sorted(rows, key=lambda r: int(r.get("id") or 0))
    used = set()
    for i, row in enumerate(ordered):
        rid = int(row.get("id") or 0)
        if rid in used:
            continue
        if str(row.get("role") or "").lower() != "user":
            unpaired.append(row); used.add(rid); continue
        partner = None
        for nxt in ordered[i+1:i+5]:
            if int(nxt.get("id") or 0) in used:
                continue
            if str(nxt.get("role") or "").lower() in {"model", "assistant", "bnl"} and int(nxt.get("user_id") or 0) == int(row.get("user_id") or 0) and int(nxt.get("channel_id") or 0) == int(row.get("channel_id") or 0):
                partner = nxt; break
        if partner:
            pairs.append({"user": row, "model": partner})
            used.add(rid); used.add(int(partner.get("id") or 0))
        else:
            unpaired.append(row); used.add(rid)
    return pairs, unpaired

def _score_pair(pair: dict, req: ConversationContextRequest, current_text: str, same_room: bool, now: datetime) -> tuple[int, tuple[str, ...]]:
    u = pair["user"]; text = (u.get("content") or "") + " " + (pair["model"].get("content") or "")
    age_min = max(0, (now - _parse_time(u.get("timestamp"), now)).total_seconds() / 60)
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

def assemble_conversation_context_v2(rows: Iterable[dict], req: ConversationContextRequest) -> ConversationContextResult:
    if not req.current_user_id or not route_permits_continuity(req.route_mode, req.route_allowed_sources) or (req.route_mode or "").lower() in GREETING_ROUTES:
        return ConversationContextResult("", (), 0, 0, 0, 0, 0, ("route_not_permitted",), 0, fallback_reason="route_not_permitted")
    target_policy = (req.channel_policy or "unknown").strip().lower()
    now = req.now.astimezone(timezone.utc) if req.now.tzinfo else req.now.replace(tzinfo=timezone.utc)
    current_norms = {normalize_text(t) for t in req.current_texts if normalize_text(t)}
    current_text = " ".join(req.current_texts)
    filtered, dupes, excluded = [], 0, 0
    for row in rows:
        p = _policy(row)
        same_room = _row_is_same_room(row, req)
        age = (now - _parse_time(row.get("timestamp"), now)).total_seconds() / 60
        if same_room:
            if p != target_policy or (target_policy not in PUBLIC_SAFE_POLICIES | SEALED_POLICIES): excluded += 1; continue
            if age > SAME_ROOM_RECENCY_MINUTES: excluded += 1; continue
        else:
            if target_policy == "sealed_test" or p == "sealed_test" or not _public_compatible(p, target_policy): excluded += 1; continue
            if int(row.get("user_id") or 0) != int(req.current_user_id or 0): excluded += 1; continue
            if age > CROSS_CHANNEL_RECENCY_MINUTES: excluded += 1; continue
        if p in BLOCKED_PUBLIC_POLICIES: excluded += 1; continue
        if _is_current_duplicate(row, req, current_norms): dupes += 1; continue
        filtered.append(dict(row, _same_room=same_room))
    pairs, unpaired = _pair_rows(filtered)
    scored_same, scored_cross = [], []
    for pair in pairs:
        same = bool(pair["user"].get("_same_room"))
        if not same and not (CONTINUATION_RE.search(current_text or "") or _overlap((pair["user"].get("content") or "") + " " + (pair["model"].get("content") or ""), current_text) >= 2):
            excluded += 1; continue
        score, reasons = _score_pair(pair, req, current_text, same, now)
        (scored_same if same else scored_cross).append((score, pair, reasons))
    scored_same.sort(key=lambda x: (-x[0], int(x[1]["user"].get("id") or 0)))
    scored_cross.sort(key=lambda x: (-x[0], int(x[1]["user"].get("id") or 0)))
    selected_pairs = scored_same[:MAX_SAME_ROOM_PAIRS]
    selected_cross = [] if selected_pairs and not CONTINUATION_RE.search(current_text or "") else scored_cross[:MAX_CROSS_CHANNEL_PAIRS]
    selected_unpaired = sorted([r for r in unpaired if r.get("_same_room")], key=lambda r: int(r.get("id") or 0), reverse=True)[:MAX_UNPAIRED_ROWS]
    entries = []
    row_ids = []
    reasons = []
    for _score, pair, why in selected_pairs + selected_cross:
        entries.append((int(pair["user"].get("id") or 0), pair, why))
        row_ids += [int(pair["user"].get("id") or 0), int(pair["model"].get("id") or 0)]
        reasons.extend(why)
    for row in selected_unpaired:
        entries.append((int(row.get("id") or 0), {"unpaired": row}, ("open_loop_unpaired",)))
        row_ids.append(int(row.get("id") or 0)); reasons.append("open_loop_unpaired")
    entries.sort(key=lambda x: x[0])
    lines = []
    if entries:
        lines = ["Conversation continuity (bounded; continuity-only, not canon/current-state evidence):", "- Prior BNL replies here are conversational continuity only. They do not prove canon, live show state, queue state, dossiers, payments, Priority, Wheel, or third-party facts."]
        for _id, item, _why in entries:
            if "unpaired" in item:
                r = item["unpaired"]; lines.append(f"User/member (open loop): {str(r.get('content') or '').strip()[:360]}")
            else:
                lines.append(f"User/member: {str(item['user'].get('content') or '').strip()[:360]}")
                lines.append(f"BNL-01: {str(item['model'].get('content') or '').strip()[:360]}")
    rendered = "\n".join(lines).strip()
    if len(rendered) > MAX_RENDERED_CHARS:
        rendered = rendered[:MAX_RENDERED_CHARS].rstrip() + "…"
    return ConversationContextResult(rendered, tuple(row_ids), len(selected_pairs), len(selected_unpaired), len(selected_cross), dupes, excluded, tuple(sorted(set(reasons))) or ("no_relevant_context",), len(rendered), fallback_reason="selected" if rendered else "no_relevant_context")
