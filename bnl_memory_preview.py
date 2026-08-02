"""Non-persistent owner preview for production-shaped broad recall.

The preview reads the production database through a read-only connection,
copies it into SQLite memory, and runs the existing formation, lifecycle,
packet, assessment, and synthesis owners only on that clone.  It never owns
Discord permissions, provider calls, response delivery, or live receipts.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
import sqlite3
from collections import Counter
from typing import Any, Callable, Mapping
from urllib.parse import quote

from bnl_canon_source_contract import (
    LIVING_CANON_GROUPING_SIGNATURE_VERSION,
    LIVING_CANON_RECURRENCE_VERSION,
)
from bnl_conversation_context_v2 import (
    ConversationContextRequest,
    assemble_conversation_context_v2,
)
import bnl_memory_governance as governance
import bnl_memory_ledger as ledger
import bnl_moment_engine as moments
import bnl_relationship_engine as relationships
from bnl_shared_brain_synthesis import (
    PacketOwnedPrompt,
    SharedBrainSynthesisBasis,
    SynthesisCanaryDecision,
    begin_run,
    build_basis,
    build_packet_owned_prompt,
    configuration as shared_brain_configuration,
    evaluate_candidate,
    finalize_run,
    record_fallback,
)
from bnl_unified_intelligence_packet import (
    IntelligencePacketRequest,
    PacketConversationEvidence,
    UnifiedIntelligencePacket,
    build_packet,
)
from bnl_unified_response_assessment import (
    UnifiedResponseAssessment,
    build_unified_response_assessment,
)


PREVIEW_SCHEMA_VERSION = "bnl_memory_preview_v3"
LIVING_CANON_PREVIEW_SCHEMA_VERSION = "living_canon_preview_v1"
SIMULATED_ROUTE_MODE = "normal_chat"
SIMULATED_CHANNEL_POLICY = "public_home"
SIMULATED_CHANNEL_NAME = "barcode-bot"
SIMULATED_CONVERSATION_SURFACE = "free_speak_public_home"
PREVIEW_FACTUAL_PLACEHOLDER = (
    "No stored member facts, observations, episode gists, or unresolved "
    "threads are supplied to the established-path comparison."
)
PREVIEW_CONVERSATION_CONTEXT_LIMIT = 80
_PREVIEW_BARE_MEDIA_FALLBACK_PATTERNS = (
    re.compile(
        r"\bi saw (?:your|[a-z0-9_. '\-]{1,80}'?s|someone'?s|their|his|her)?"
        r"\s*recent\s+(?:gif|image|picture|photo|video|sticker|meme|media)"
        r"\b.*\b(?:do not have|don['’]t have|lack)\b.*"
        r"\bdetailed visual description\b",
        re.I,
    ),
    re.compile(
        r"\bi saw .*\bas\s+(?:gif|image|video|sticker|media).*"
        r"(?:embed|preview|provider=|host=|preview=yes).*"
        r"\bdetailed visual description\b",
        re.I,
    ),
)
_PREVIEW_PROMPT_CONTROL_LABEL_RE = re.compile(
    r"\b(?:ignore|disregard|override|reveal|system|developer|assistant|"
    r"prompt|instructions?|current user request|source context|"
    r"broadcast memory)\b",
    re.I,
)
_LIVING_CANON_PREVIEW_STATES = frozenset(
    {
        "candidate",
        "provisional",
        "established",
        "contested",
        "superseded",
        "retired",
        "review_only",
        "proposed",
        "skipped",
        "ambiguous",
        "rejected",
        "withheld",
    }
)
_LIVING_CANON_PREVIEW_REASONS = frozenset(
    {
        "single_occurrence_provisional",
        "independent_recurrence_established",
        "same_occurrence_collapsed",
        "overlapping_occurrence_representation_collapsed",
        "same_root_projection_collapsed",
        "unbounded_occurrence_withheld",
        "correction_fence_active",
        "fresh_recurrence_required_after_correction",
        "source_ineligible",
        "visibility_ineligible",
        "derived_source_not_independent",
        "meaning_ambiguous_review_only",
        "contradiction_contested",
        "moment_lifecycle_or_membership_ineligible",
        "candidate_limit_reached",
        "provisional_subject_bound_reached",
        "no_eligible_recurrence",
    }
)
_LIVING_CANON_PREVIEW_BOUND_LIMITS = {
    "eligible_ledger_scan_max": 1200,
    "motif_candidates_max": 6,
    "retained_roots_max": 12,
    "occurrence_lookback_max": 64,
    "idle_boundary_seconds": 30 * 60,
}


@dataclass(frozen=True)
class MemoryPreviewRequest:
    source_db_path: str
    guild_id: int
    subject_user_id: int
    subject_display_name: str
    simulated_channel_id: int
    wording: str
    baseline_prompt: str
    factual_placeholder: str = PREVIEW_FACTUAL_PLACEHOLDER
    competing_factual_contexts: tuple[str, ...] = ()
    now: str = ""


BaselinePromptBuilder = Callable[
    [sqlite3.Connection, MemoryPreviewRequest, Mapping[str, str]],
    tuple[str, tuple[str, ...]],
]


@dataclass(frozen=True)
class LivingCanonPreviewDiagnostics:
    schema_version: str = LIVING_CANON_PREVIEW_SCHEMA_VERSION
    status: str = "not_evaluated"
    status_reason: str = ""
    recurrence_contract_version: str = "unverified"
    grouping_signature_version: str = "unverified"
    proposed_count: int = 0
    skipped_count: int = 0
    ambiguous_count: int = 0
    rejected_count: int = 0
    candidate_state_counts: tuple[tuple[str, int], ...] = ()
    reason_counts: tuple[tuple[str, int], ...] = ()
    independent_root_count: int = 0
    independent_occurrence_count: int = 0
    collapsed_root_count: int = 0
    bounds: tuple[tuple[str, int], ...] = ()
    source_write_count: int = 0
    source_write_occurred: bool = False


@dataclass(frozen=True)
class MemoryPreviewDiagnostics:
    schema_version: str = PREVIEW_SCHEMA_VERSION
    route_status: str = "not_evaluated"
    route_reason: str = ""
    formation_outcomes: tuple[tuple[str, int], ...] = ()
    formation_reason_codes: tuple[str, ...] = ()
    source_funnel_counts: tuple[tuple[str, int], ...] = ()
    lifecycle_scopes: int = 0
    lifecycle_candidates: int = 0
    lifecycle_state_changes: int = 0
    packet_lane_counts: tuple[tuple[str, int], ...] = ()
    validation_support_lane_counts: tuple[tuple[str, int], ...] = ()
    packet_canon_candidate_status_counts: tuple[tuple[str, int], ...] = ()
    packet_canon_selected_status_counts: tuple[tuple[str, int], ...] = ()
    packet_canon_candidate_domain_counts: tuple[tuple[str, int], ...] = ()
    packet_canon_selected_domain_counts: tuple[tuple[str, int], ...] = ()
    packet_exclusion_reason_counts: tuple[tuple[str, int], ...] = ()
    packet_missing_lanes: tuple[str, ...] = ()
    packet_revalidation_status: str = "not_evaluated"
    assessment_pool_eligible_count: int = 0
    assessment_pool_selected_count: int = 0
    root_collapse_suppression_count: int = 0
    shared_root_projection_count: int = 0
    canon_identity_status: str = "not_evaluated"
    canon_identity_stable_row_count: int = 0
    profile_status: str = "not_applicable"
    profile_satisfied: bool = False
    profile_required_point_count: int = 0
    profile_selected_point_count: int = 0
    profile_candidate_point_count: int = 0
    profile_independent_root_count: int = 0
    profile_independent_occurrence_count: int = 0
    profile_reason_codes: tuple[str, ...] = ()
    assessment_conflict_reasons: tuple[str, ...] = ()
    prompt_owner_ready: bool = False
    prompt_owner_reason: str = ""
    replaced_factual_context_count: int = 0
    omission_reason_codes: tuple[str, ...] = ()
    living_canon: LivingCanonPreviewDiagnostics = field(
        default_factory=LivingCanonPreviewDiagnostics
    )


@dataclass
class PreparedMemoryPreview:
    connection: sqlite3.Connection | None
    request: MemoryPreviewRequest
    environ: dict[str, str]
    diagnostics: MemoryPreviewDiagnostics
    packet: UnifiedIntelligencePacket | None = None
    assessment: UnifiedResponseAssessment | None = None
    basis: SharedBrainSynthesisBasis | None = None
    packet_owned_prompt: PacketOwnedPrompt = field(
        default_factory=lambda: PacketOwnedPrompt(
            prompt="",
            ready=False,
            reason="not_prepared",
        )
    )
    snapshot_digest: str = ""
    conversation_context_digest: str = ""
    capability_receipt: dict[str, Any] = field(default_factory=dict)

    @property
    def ready(self) -> bool:
        return bool(
            self.connection is not None
            and self.packet is not None
            and self.assessment is not None
            and self.basis is not None
            and self.packet_owned_prompt.ready
        )

    def close(self) -> None:
        if self.connection is None:
            return
        try:
            self.connection.close()
        finally:
            self.connection = None


@dataclass(frozen=True)
class MemoryPreviewEvaluation:
    decision: SynthesisCanaryDecision | None
    response: str
    candidate_selected: bool
    fallback_reason: str
    baseline_member_point_count: int = 0
    baseline_member_detail_count: int = 0
    baseline_canon_count: int = 0
    candidate_member_point_count: int = 0
    candidate_member_detail_count: int = 0
    candidate_member_root_count: int = 0
    candidate_member_occurrence_count: int = 0
    candidate_canon_count: int = 0
    candidate_lore_dominant: bool = False
    candidate_member_supported_claim_count: int = 0
    candidate_canon_supported_claim_count: int = 0
    candidate_opinion_claim_count: int = 0
    candidate_connective_claim_count: int = 0
    candidate_unsupported_factual_claim_count: int = 0
    candidate_claim_classifications: tuple[str, ...] = ()
    supported_coverage_regressed: bool = False


@dataclass(frozen=True)
class _PreviewConversationContext:
    rendered_context: str = ""
    evidence: tuple[PacketConversationEvidence, ...] = ()
    source_row_ids: tuple[int, ...] = ()
    participant_user_ids: tuple[int, ...] = ()
    speaker_labels: tuple[str, ...] = ()
    digest: str = ""


def preview_environment(
    *,
    guild_id: int,
    subject_user_id: int,
    channel_id: int,
    base: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Return isolated gates for the clone without mutating process state."""

    env = dict(os.environ if base is None else base)
    env.update(
        {
            ledger.MEMORY_LEDGER_SHADOW_ENV: "true",
            ledger.CONVERSATION_MOTIF_FORMATION_ENV: "true",
            moments.MOMENT_ENGINE_SHADOW_ENV: "true",
            governance.SHADOW_ENV: "true",
            relationships.SHADOW_ENV: "true",
            "BNL_UNIFIED_RESPONSE_ASSESSMENT_SHADOW_ENABLED": "true",
            "BNL_UNIFIED_INTELLIGENCE_PACKET_SHADOW_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_ENABLED": "true",
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_GUILD_IDS": str(
                int(guild_id or 0)
            ),
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_USER_IDS": str(
                int(subject_user_id or 0)
            ),
            "BNL_SHARED_BRAIN_SYNTHESIS_CANARY_CHANNEL_IDS": str(
                int(channel_id or 0)
            ),
            "BNL_MEMORY_GOVERNANCE_LIVE_ENABLED": "false",
            "BNL_RELATIONSHIP_V2_LIVE_ENABLED": "false",
            "BNL_ACTIVE_ENGAGEMENT_V2_LIVE_ENABLED": "false",
        }
    )
    return env


def _open_read_only_memory_clone(path: str) -> sqlite3.Connection:
    source_path = Path(str(path or "")).expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError("preview_source_database_missing")
    source_uri = "file:%s?mode=ro" % quote(str(source_path), safe="/")
    source = sqlite3.connect(
        source_uri,
        uri=True,
        timeout=0.5,
        check_same_thread=False,
    )
    clone = sqlite3.connect(":memory:", check_same_thread=False)
    try:
        source.backup(clone)
    except Exception:
        clone.close()
        raise
    finally:
        source.close()
    return clone


def _digest(*parts: Any) -> str:
    payload = "\x1f".join(str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _now(value: str) -> str:
    candidate = str(value or "").strip()
    if candidate:
        return candidate
    return datetime.now(timezone.utc).isoformat()


def _preview_datetime(value: str) -> datetime:
    candidate = str(value or "").strip().replace("Z", "+00:00")
    if candidate:
        try:
            parsed = datetime.fromisoformat(candidate)
            return (
                parsed.replace(tzinfo=timezone.utc)
                if parsed.tzinfo is None
                else parsed.astimezone(timezone.utc)
            )
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _table_columns(
    conn: sqlite3.Connection,
    table_name: str,
) -> frozenset[str]:
    try:
        return frozenset(
            str(row[1])
            for row in conn.execute(
                "PRAGMA table_info(%s)" % table_name
            ).fetchall()
            if len(row) > 1 and str(row[1] or "")
        )
    except sqlite3.DatabaseError:
        return frozenset()


def _preview_prompt_history_excluded(role: str, content: str) -> bool:
    """Mirror the live bare-media fallback gate without importing the bot."""

    if str(role or "").strip().lower() != "model":
        return False
    lowered = re.sub(
        r"\s+",
        " ",
        str(content or "").strip().lower(),
    )
    if not lowered:
        return False
    if any(
        pattern.search(lowered)
        for pattern in _PREVIEW_BARE_MEDIA_FALLBACK_PATTERNS
    ):
        return True
    media_bits = any(
        token in lowered
        for token in (
            "recent gif",
            "recent media",
            "recent image",
            "gif embed",
            "link preview",
        )
    )
    thin_bits = (
        "detailed visual description" in lowered
        and any(
            token in lowered
            for token in ("do not have", "don't have", "dont have")
        )
    )
    metadata_bits = any(
        token in lowered
        for token in (
            "provider=",
            "host=",
            "preview=yes",
            "embed_type=",
            "tenor",
            "giphy",
        )
    )
    return bool(media_bits and thin_bits and metadata_bits)


def _safe_preview_speaker_label(value: str) -> str:
    """Reduce a stored display name to the inert live-path label shape."""

    cleaned = "".join(
        char if (char.isalnum() or char in " _.-") else " "
        for char in str(value or "")
    )
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .,-")[:72]
    if (
        not cleaned
        or cleaned.isdigit()
        or _PREVIEW_PROMPT_CONTROL_LABEL_RE.search(cleaned)
    ):
        return ""
    return cleaned


def _conversation_context_candidate_rows(
    conn: sqlite3.Connection,
    request: MemoryPreviewRequest,
) -> tuple[tuple[dict[str, Any], ...], dict[int, dict[str, Any]]]:
    """Read only the bounded production-equivalent Context v2 candidates."""

    columns = _table_columns(conn, "conversations")
    required = {
        "id",
        "guild_id",
        "role",
        "content",
        "user_id",
        "user_name",
        "channel_id",
        "channel_name",
        "channel_policy",
        "timestamp",
    }
    if not required.issubset(columns):
        return (), {}
    message_id_expr = (
        "message_id" if "message_id" in columns else "NULL AS message_id"
    )
    base_select = """
        SELECT id,role,content,user_id,user_name,channel_id,channel_name,
               channel_policy,timestamp,%s
        FROM conversations
        WHERE guild_id=?
    """ % message_id_expr
    rows_by_id: dict[int, tuple[Any, ...]] = {}

    def remember(rows: list[tuple[Any, ...]]) -> None:
        for row in rows:
            try:
                row_id = int(row[0] or 0)
            except (TypeError, ValueError, IndexError):
                continue
            if row_id > 0:
                rows_by_id[row_id] = row

    limit = PREVIEW_CONVERSATION_CONTEXT_LIMIT
    channel_id = int(request.simulated_channel_id or 0)
    channel_name = SIMULATED_CHANNEL_NAME
    policy = SIMULATED_CHANNEL_POLICY
    try:
        if channel_id:
            remember(
                conn.execute(
                    base_select
                    + """
                      AND channel_id=?
                      AND channel_policy=?
                    ORDER BY id DESC LIMIT ?
                    """,
                    (
                        int(request.guild_id or 0),
                        channel_id,
                        policy,
                        limit,
                    ),
                ).fetchall()
            )
        remember(
            conn.execute(
                base_select
                + """
                  AND LOWER(COALESCE(channel_name,''))=?
                  AND channel_policy=?
                  AND (COALESCE(channel_id,0)=0 OR ?=0)
                ORDER BY id DESC LIMIT ?
                """,
                (
                    int(request.guild_id or 0),
                    channel_name,
                    policy,
                    channel_id,
                    limit,
                ),
            ).fetchall()
        )
        if int(request.subject_user_id or 0) > 0:
            remember(
                conn.execute(
                    base_select
                    + """
                      AND user_id=?
                      AND channel_policy IN ('public_home','public_context')
                    ORDER BY id DESC LIMIT ?
                    """,
                    (
                        int(request.guild_id or 0),
                        int(request.subject_user_id or 0),
                        limit,
                    ),
                ).fetchall()
            )
    except sqlite3.DatabaseError:
        return (), {}

    response_participants: dict[int, tuple[int, ...]] = {}
    participant_columns = _table_columns(
        conn,
        "conversation_response_participants",
    )
    if rows_by_id and {
        "guild_id",
        "conversation_row_id",
        "user_id",
    }.issubset(participant_columns):
        row_ids = tuple(sorted(rows_by_id))
        placeholders = ",".join("?" for _ in row_ids)
        try:
            mapped: dict[int, list[int]] = {}
            for conversation_row_id, user_id in conn.execute(
                """
                SELECT conversation_row_id,user_id
                FROM conversation_response_participants
                WHERE guild_id=?
                  AND conversation_row_id IN (%s)
                ORDER BY conversation_row_id,user_id
                """
                % placeholders,
                (int(request.guild_id or 0), *row_ids),
            ).fetchall():
                try:
                    row_id = int(conversation_row_id or 0)
                    participant_id = int(user_id or 0)
                except (TypeError, ValueError):
                    continue
                if row_id > 0 and participant_id > 0:
                    mapped.setdefault(row_id, []).append(participant_id)
            response_participants = {
                row_id: tuple(sorted(set(user_ids)))
                for row_id, user_ids in mapped.items()
                if user_ids
            }
        except sqlite3.DatabaseError:
            response_participants = {}

    keys = (
        "id",
        "role",
        "content",
        "user_id",
        "user_name",
        "channel_id",
        "channel_name",
        "channel_policy",
        "timestamp",
        "message_id",
    )
    rendered_rows = []
    rendered_by_id: dict[int, dict[str, Any]] = {}
    for row_id in sorted(rows_by_id):
        row = dict(zip(keys, rows_by_id[row_id]))
        content = str(row.get("content") or "").strip()
        if not content:
            continue
        row["id"] = row_id
        row["content"] = content
        row["channel_id"] = int(row.get("channel_id") or 0)
        row["channel_name"] = str(row.get("channel_name") or "")
        row["channel_policy"] = str(
            row.get("channel_policy") or "unknown"
        ).strip().lower()
        row["prompt_history_excluded"] = (
            _preview_prompt_history_excluded(
                str(row.get("role") or ""),
                content,
            )
        )
        if row_id in response_participants:
            row["response_participant_ids"] = response_participants[row_id]
        rendered_rows.append(row)
        rendered_by_id[row_id] = row
    return tuple(rendered_rows), rendered_by_id


def _preview_conversation_context(
    conn: sqlite3.Connection,
    request: MemoryPreviewRequest,
) -> _PreviewConversationContext:
    rows, rows_by_id = _conversation_context_candidate_rows(conn, request)
    if not rows:
        return _PreviewConversationContext()
    try:
        result = assemble_conversation_context_v2(
            rows,
            ConversationContextRequest(
                guild_id=int(request.guild_id or 0),
                current_user_id=int(request.subject_user_id or 0),
                channel_id=int(request.simulated_channel_id or 0),
                channel_name=SIMULATED_CHANNEL_NAME,
                channel_policy=SIMULATED_CHANNEL_POLICY,
                route_mode=SIMULATED_ROUTE_MODE,
                conversation_surface=SIMULATED_CONVERSATION_SURFACE,
                current_texts=(str(request.wording or "")[:8000],),
                current_participants=frozenset(
                    {int(request.subject_user_id or 0)}
                ),
                is_direct_target=True,
                now=_preview_datetime(request.now),
                route_allowed_sources=frozenset(
                    {"conversation_continuity"}
                ),
            ),
        )
    except (KeyError, TypeError, ValueError):
        return _PreviewConversationContext()
    if not result.rendered_context or not result.selected_row_ids:
        return _PreviewConversationContext()

    selected_rows = tuple(
        rows_by_id[row_id]
        for row_id in result.selected_row_ids
        if row_id in rows_by_id
    )
    evidence = []
    participant_ids = []
    speaker_labels = []
    for row in selected_rows:
        if str(row.get("role") or "").strip().lower() != "user":
            continue
        try:
            row_id = int(row.get("id") or 0)
            user_id = int(row.get("user_id") or 0)
        except (TypeError, ValueError):
            continue
        content = str(row.get("content") or "").strip()
        if row_id <= 0 or user_id <= 0 or not content:
            continue
        label = _safe_preview_speaker_label(
            str(row.get("user_name") or "")
        )
        evidence.append(
            PacketConversationEvidence(
                text=content[:1200],
                source_id=row_id,
                speaker_user_id=user_id,
                speaker_label=label,
                current_turn=False,
            )
        )
        participant_ids.append(user_id)
        if label:
            speaker_labels.append(label)
    if not evidence:
        return _PreviewConversationContext()

    selected_snapshot = tuple(
        (
            int(row.get("id") or 0),
            str(row.get("role") or ""),
            str(row.get("content") or ""),
            int(row.get("user_id") or 0),
            str(row.get("user_name") or ""),
            int(row.get("channel_id") or 0),
            str(row.get("channel_name") or ""),
            str(row.get("channel_policy") or ""),
            str(row.get("timestamp") or ""),
            int(row.get("message_id") or 0),
            bool(row.get("prompt_history_excluded")),
            tuple(row.get("response_participant_ids") or ()),
        )
        for row in selected_rows
    )
    return _PreviewConversationContext(
        rendered_context=result.rendered_context,
        evidence=tuple(evidence),
        source_row_ids=tuple(
            int(row.get("id") or 0)
            for row in selected_rows
            if int(row.get("id") or 0) > 0
        ),
        participant_user_ids=tuple(sorted(set(participant_ids))),
        speaker_labels=tuple(dict.fromkeys(speaker_labels)),
        digest=_digest(
            "conversation_context_v2",
            result.rendered_context,
            result.selected_row_ids,
            result.selection_reasons,
            result.thread_focus_mode,
            selected_snapshot,
        ),
    )


def _request_with_conversation_context(
    request: MemoryPreviewRequest,
    context: _PreviewConversationContext,
) -> MemoryPreviewRequest:
    rendered = str(context.rendered_context or "").strip()
    baseline = str(request.baseline_prompt or "").rstrip()
    if not rendered or rendered in baseline:
        return request
    return replace(
        request,
        baseline_prompt="%s\n\n%s" % (baseline, rendered),
    )


def _preview_report_field(report: Any, name: str, default: Any) -> Any:
    if isinstance(report, Mapping):
        return report.get(name, default)
    return getattr(report, name, default)


def _preview_nonnegative_count(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return 0
    return max(0, min(parsed, 1_000_000_000))


def _content_free_count_pairs(
    value: Any,
    *,
    allowed: frozenset[str],
    unknown_label: str,
) -> tuple[tuple[str, int], ...]:
    items = value.items() if isinstance(value, Mapping) else value
    counter: Counter[str] = Counter()
    try:
        pairs = tuple(items or ())
    except TypeError:
        return ()
    for item in pairs:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            continue
        label = str(item[0] or "").strip().casefold()
        count = _preview_nonnegative_count(item[1])
        if count <= 0:
            continue
        counter[label if label in allowed else unknown_label] += count
    return tuple(sorted(counter.items()))


def _content_free_preview_bounds(value: Any) -> tuple[tuple[str, int], ...]:
    items = value.items() if isinstance(value, Mapping) else value
    try:
        supplied = dict(items or ())
    except (TypeError, ValueError):
        return ()
    if set(supplied) != set(_LIVING_CANON_PREVIEW_BOUND_LIMITS):
        return ()
    if any(
        type(supplied.get(key)) is not int
        or supplied[key] < 0
        for key in _LIVING_CANON_PREVIEW_BOUND_LIMITS
    ):
        return ()
    bounds = tuple(
        (
            key,
            supplied[key],
        )
        for key in _LIVING_CANON_PREVIEW_BOUND_LIMITS
    )
    if any(
        value != _LIVING_CANON_PREVIEW_BOUND_LIMITS[key]
        for key, value in bounds
    ):
        return ()
    return bounds


def _native_preview_count(value: Any) -> bool:
    return type(value) is int and value >= 0


def _native_preview_count_pairs(value: Any) -> bool:
    items = value.items() if isinstance(value, Mapping) else value
    try:
        pairs = tuple(items or ())
    except TypeError:
        return False
    return all(
        isinstance(item, (tuple, list))
        and len(item) == 2
        and isinstance(item[0], str)
        and bool(item[0].strip())
        and _native_preview_count(item[1])
        for item in pairs
    )


def build_living_canon_preview_diagnostics(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    analyzer: Callable[..., Any] | None = None,
) -> LivingCanonPreviewDiagnostics:
    """Consume the pure recurrence analyzer without exposing its evidence."""

    selected_analyzer = analyzer or getattr(
        ledger,
        "preview_living_canon_formation",
        None,
    )
    if not callable(selected_analyzer):
        return LivingCanonPreviewDiagnostics(
            status="unavailable",
            status_reason="pure_analyzer_unavailable",
        )
    before_changes = int(getattr(conn, "total_changes", 0) or 0)
    try:
        report = selected_analyzer(
            conn,
            guild_id=int(guild_id or 0),
            subject_key=str(subject_key or ""),
            max_scan=1200,
        )
    except Exception:
        after_changes = int(getattr(conn, "total_changes", 0) or 0)
        write_count = max(0, after_changes - before_changes)
        return LivingCanonPreviewDiagnostics(
            status="rejected",
            status_reason="pure_analyzer_error",
            source_write_count=write_count,
            source_write_occurred=bool(write_count),
        )
    after_changes = int(getattr(conn, "total_changes", 0) or 0)
    observed_write_count = max(0, after_changes - before_changes)
    reported_write_count = _preview_nonnegative_count(
        _preview_report_field(report, "source_write_count", 0)
    )
    reported_write_occurred = _preview_report_field(
        report,
        "write_occurred",
        None,
    )
    source_write_count = max(observed_write_count, reported_write_count)
    source_write_occurred = bool(
        source_write_count or reported_write_occurred is not False
    )
    recurrence_version = str(
        _preview_report_field(
            report,
            "recurrence_contract_version",
            "",
        )
        or ""
    ).strip()
    grouping_version = str(
        _preview_report_field(
            report,
            "grouping_signature_version",
            "",
        )
        or ""
    ).strip()
    versions_valid = bool(
        recurrence_version == LIVING_CANON_RECURRENCE_VERSION
        and grouping_version == LIVING_CANON_GROUPING_SIGNATURE_VERSION
    )
    bounds = _content_free_preview_bounds(
        _preview_report_field(report, "bounds", ())
    )
    raw_counts = {
        name: _preview_report_field(report, name, 0)
        for name in (
            "proposed_count",
            "skipped_count",
            "ambiguous_count",
            "rejected_count",
            "independent_root_count",
            "independent_occurrence_count",
            "collapsed_root_count",
        )
    }
    proposed_count = _preview_nonnegative_count(raw_counts["proposed_count"])
    skipped_count = _preview_nonnegative_count(raw_counts["skipped_count"])
    ambiguous_count = _preview_nonnegative_count(raw_counts["ambiguous_count"])
    rejected_count = _preview_nonnegative_count(raw_counts["rejected_count"])
    raw_state_counts = _preview_report_field(
        report,
        "candidate_state_counts",
        (),
    )
    raw_reason_counts = _preview_report_field(report, "reason_counts", ())
    candidate_state_counts = _content_free_count_pairs(
        raw_state_counts,
        allowed=_LIVING_CANON_PREVIEW_STATES,
        unknown_label="unrecognized_state",
    )
    reason_counts = _content_free_count_pairs(
        raw_reason_counts,
        allowed=_LIVING_CANON_PREVIEW_REASONS,
        unknown_label="unrecognized_reason_code",
    )
    independent_root_count = _preview_nonnegative_count(
        raw_counts["independent_root_count"]
    )
    independent_occurrence_count = _preview_nonnegative_count(
        raw_counts["independent_occurrence_count"]
    )
    collapsed_root_count = _preview_nonnegative_count(
        raw_counts["collapsed_root_count"]
    )
    state_map = dict(candidate_state_counts)
    report_valid = bool(
        all(_native_preview_count(value) for value in raw_counts.values())
        and _native_preview_count_pairs(raw_state_counts)
        and _native_preview_count_pairs(raw_reason_counts)
        and proposed_count <= _LIVING_CANON_PREVIEW_BOUND_LIMITS[
            "motif_candidates_max"
        ]
        and max(
            skipped_count,
            ambiguous_count,
            rejected_count,
        )
        <= _LIVING_CANON_PREVIEW_BOUND_LIMITS["eligible_ledger_scan_max"]
        and set(state_map).issubset({"provisional", "established"})
        and "unrecognized_reason_code" not in dict(reason_counts)
        and sum(state_map.values()) == proposed_count
        and independent_root_count
        <= (
            _LIVING_CANON_PREVIEW_BOUND_LIMITS["motif_candidates_max"]
            * _LIVING_CANON_PREVIEW_BOUND_LIMITS["retained_roots_max"]
        )
        and independent_occurrence_count <= independent_root_count
        and collapsed_root_count
        == independent_root_count - independent_occurrence_count
    )
    status = "analyzed"
    status_reason = "pure_analyzer_complete"
    if source_write_occurred:
        status = "rejected"
        status_reason = "pure_analyzer_write_detected"
    elif not versions_valid:
        status = "rejected"
        status_reason = "pure_analyzer_version_unverified"
    elif not bounds:
        status = "rejected"
        status_reason = "pure_analyzer_bounds_unverified"
    elif not report_valid:
        status = "rejected"
        status_reason = "pure_analyzer_report_invalid"
    report_trusted = status == "analyzed"
    return LivingCanonPreviewDiagnostics(
        status=status,
        status_reason=status_reason,
        recurrence_contract_version=(
            recurrence_version if versions_valid else "unverified"
        ),
        grouping_signature_version=(
            grouping_version if versions_valid else "unverified"
        ),
        proposed_count=proposed_count if report_trusted else 0,
        skipped_count=skipped_count if report_trusted else 0,
        ambiguous_count=ambiguous_count if report_trusted else 0,
        rejected_count=rejected_count if report_trusted else 0,
        candidate_state_counts=(
            candidate_state_counts if report_trusted else ()
        ),
        reason_counts=reason_counts if report_trusted else (),
        independent_root_count=(
            independent_root_count if report_trusted else 0
        ),
        independent_occurrence_count=(
            independent_occurrence_count if report_trusted else 0
        ),
        collapsed_root_count=(
            collapsed_root_count if report_trusted else 0
        ),
        bounds=bounds,
        source_write_count=source_write_count,
        source_write_occurred=source_write_occurred,
    )


def _formation_and_lifecycle(
    conn: sqlite3.Connection,
    request: MemoryPreviewRequest,
    environ: Mapping[str, str],
) -> tuple[
    tuple[tuple[str, int], ...],
    tuple[str, ...],
    tuple[tuple[str, int], ...],
    dict[str, int],
]:
    ledger.ensure_memory_ledger_schema(conn)
    moments.ensure_moment_schema(conn)
    relationships.ensure_relationship_v2_schema(conn)
    governance.ensure_governance_schema(conn)
    source_funnel: dict[str, int] = {}
    results = ledger.form_atomic_candidates_from_recurring_conversation(
        conn,
        guild_id=request.guild_id,
        subject_key=ledger.subject_key_for_user(
            request.subject_user_id
        ),
        environ=dict(environ),
        diagnostics=source_funnel,
    )
    candidate_ids = tuple(
        dict.fromkeys(
            str(result.candidate_id or "")
            for result in results
            if str(result.candidate_id or "")
        )
    )
    lifecycle = ledger.reconcile_atomic_knowledge_lifecycle(
        conn,
        candidate_ids=candidate_ids,
        guild_id=(
            None if candidate_ids else int(request.guild_id or 0)
        ),
        now=request.now or None,
    )
    outcomes = Counter(
        str(result.outcome or "unknown") for result in results
    )
    reason_codes = tuple(
        sorted(
            {
                str(result.reason_code or "")
                for result in results
                if str(result.reason_code or "")
            }
        )
    )
    return (
        tuple(sorted(outcomes.items())),
        reason_codes,
        tuple(sorted(source_funnel.items())),
        {
            "scopes": int(lifecycle.get("scopes", 0) or 0),
            "candidates": int(
                lifecycle.get("candidates", 0) or 0
            ),
            "state_changes": int(
                lifecycle.get("state_changes", 0) or 0
            ),
        },
    )


def _packet_request(
    request: MemoryPreviewRequest,
    conversation_context: _PreviewConversationContext,
    environ: Mapping[str, str],
) -> IntelligencePacketRequest:
    participant_ids = tuple(
        dict.fromkeys(
            (
                int(request.subject_user_id or 0),
                *conversation_context.participant_user_ids,
            )
        )
    )
    return IntelligencePacketRequest(
        guild_id=int(request.guild_id or 0),
        subject_user_id=int(request.subject_user_id or 0),
        route_mode=SIMULATED_ROUTE_MODE,
        conversation_surface=SIMULATED_CONVERSATION_SURFACE,
        subject_display_name=str(request.subject_display_name or "")[:120],
        channel_id=int(request.simulated_channel_id or 0),
        channel_name=SIMULATED_CHANNEL_NAME,
        channel_policy=SIMULATED_CHANNEL_POLICY,
        visibility_allowance="public_safe",
        user_text=str(request.wording or "")[:8000],
        participant_user_ids=participant_ids,
        direct_state="direct",
        budget_chars=5000,
        conversation_evidence=(
            *conversation_context.evidence,
            PacketConversationEvidence(
                text=str(request.wording or "")[:8000],
                source_id=0,
                speaker_user_id=int(request.subject_user_id or 0),
                speaker_label=str(
                    request.subject_display_name or ""
                )[:120],
                current_turn=True,
            ),
        ),
        now=_now(request.now),
        declared_canon_authorized=bool(
            shared_brain_configuration(environ).get("effective")
        ),
    )


def _assessment_from_packet(
    packet: UnifiedIntelligencePacket,
    request: MemoryPreviewRequest,
    conversation_context: _PreviewConversationContext,
) -> UnifiedResponseAssessment:
    profile = packet.profile_sufficiency
    participant_user_ids = tuple(
        dict.fromkeys(
            (
                int(request.subject_user_id or 0),
                *conversation_context.participant_user_ids,
            )
        )
    )
    speaker_labels = tuple(
        dict.fromkeys(
            label
            for label in (
                _safe_preview_speaker_label(
                    request.subject_display_name
                ),
                *conversation_context.speaker_labels,
            )
            if label
        )
    )
    return build_unified_response_assessment(
        guild_id=int(request.guild_id or 0),
        route_mode=SIMULATED_ROUTE_MODE,
        channel_policy=SIMULATED_CHANNEL_POLICY,
        conversation_surface=SIMULATED_CONVERSATION_SURFACE,
        current_speaker_user_ids=(
            int(request.subject_user_id or 0),
        ),
        target_user_ids=(int(request.subject_user_id or 0),),
        participant_user_ids=participant_user_ids,
        speaker_labels=speaker_labels,
        current_exchange_source_ids=(
            conversation_context.source_row_ids
        ),
        prior_moment_ids=packet.moment_refs,
        governed_entry_ids=packet.governed_refs,
        relationship_candidate_keys=packet.relationship_refs,
        canon_refs=packet.canon_refs,
        prompt_lanes=(
            "current_exchange",
            *(
                ("conversation_context",)
                if conversation_context.rendered_context
                else ()
            ),
        ),
        current_text=str(request.wording or "")[:8000],
        packet_selected_lanes=packet.assessment_lanes,
        packet_excluded_lanes=packet.assessment_exclusions,
        packet_conflict_reasons=(
            packet.diagnostics.conflict_reasons
        ),
        packet_missing_lanes=packet.assessment_missing_lanes,
        packet_revalidation_status=(
            packet.diagnostics.revalidation_status
        ),
        profile_sufficiency_status=profile.status,
        profile_sufficiency_met=profile.satisfied,
        profile_required_point_count=profile.required_point_count,
        profile_selected_point_count=profile.selected_point_count,
        profile_independent_root_count=profile.independent_root_count,
        profile_independent_occurrence_count=(
            profile.independent_occurrence_count
        ),
        profile_sufficiency_reasons=profile.reason_codes,
    )


def _snapshot_digest(
    packet: UnifiedIntelligencePacket | None,
    basis: SharedBrainSynthesisBasis | None,
    request: MemoryPreviewRequest,
    conversation_context_digest: str = "",
    capability_receipt: Mapping[str, Any] | None = None,
) -> str:
    if packet is None:
        return ""
    profile = packet.profile_sufficiency
    return _digest(
        packet.diagnostics.packet_digest,
        (
            basis.expected_context_digest
            if basis is not None
            else ""
        ),
        profile.status,
        profile.satisfied,
        profile.required_point_count,
        profile.selected_point_count,
        profile.independent_root_count,
        profile.independent_occurrence_count,
        _digest(str(request.baseline_prompt or "")),
        tuple(
            _digest(str(context or ""))
            for context in request.competing_factual_contexts
        ),
        str(conversation_context_digest or ""),
        _digest(
            tuple(
                sorted(
                    (str(key), str(value))
                    for key, value in dict(
                        capability_receipt or {}
                    ).items()
                )
            )
        ),
        tuple(
            sorted(
                (
                    item.source_digest,
                    item.root_identities,
                    item.occurrence_identities,
                    item.point_identity,
                )
                for item in packet.items
            )
        ),
    )


def _diagnostics(
    *,
    route_status: str,
    route_reason: str,
    formation_outcomes: tuple[tuple[str, int], ...] = (),
    formation_reason_codes: tuple[str, ...] = (),
    source_funnel_counts: tuple[tuple[str, int], ...] = (),
    lifecycle: Mapping[str, int] | None = None,
    packet: UnifiedIntelligencePacket | None = None,
    assessment: UnifiedResponseAssessment | None = None,
    packet_prompt: PacketOwnedPrompt | None = None,
    living_canon: LivingCanonPreviewDiagnostics | None = None,
) -> MemoryPreviewDiagnostics:
    lifecycle = lifecycle or {}
    profile = (
        packet.profile_sufficiency if packet is not None else None
    )
    exclusion_counts = (
        tuple(
            sorted(packet.diagnostics.excluded_by_reason.items())
        )
        if packet is not None
        else ()
    )
    missing_lanes = (
        tuple(packet.diagnostics.missing_lanes)
        if packet is not None
        else ()
    )
    omission_reasons = {
        "excluded:%s" % reason
        for reason, count in exclusion_counts
        if int(count or 0) > 0 and reason != "lane_cap"
    }
    omission_reasons.update(
        "missing_lane:%s" % lane for lane in missing_lanes
    )
    omission_reasons.update(
        str(reason or "")
        for reason in (
            getattr(profile, "reason_codes", ()) if profile else ()
        )
        if str(reason or "")
    )
    if packet is not None:
        omission_reasons.update(
            "excluded:lane_cap:%s" % exclusion.lane
            for exclusion in packet.exclusions
            if exclusion.reason == "lane_cap"
        )
        omission_reasons.update(
            "packet_error:%s" % reason
            for reason in packet.diagnostics.processing_errors
        )
        omission_reasons.update(
            "packet_invariant:%s" % reason
            for reason in packet.diagnostics.invalid_invariants
        )
        if packet.diagnostics.root_collapse_suppression:
            omission_reasons.add("same_human_root_collapsed")
    if packet_prompt is not None and not packet_prompt.ready:
        omission_reasons.add(
            "prompt_owner:%s"
            % str(packet_prompt.reason or "not_ready")
        )
    return MemoryPreviewDiagnostics(
        route_status=str(route_status or "unknown"),
        route_reason=str(route_reason or ""),
        formation_outcomes=formation_outcomes,
        formation_reason_codes=formation_reason_codes,
        source_funnel_counts=source_funnel_counts,
        lifecycle_scopes=int(lifecycle.get("scopes", 0) or 0),
        lifecycle_candidates=int(
            lifecycle.get("candidates", 0) or 0
        ),
        lifecycle_state_changes=int(
            lifecycle.get("state_changes", 0) or 0
        ),
        packet_lane_counts=(
            tuple(sorted(packet.diagnostics.selected_by_lane.items()))
            if packet is not None
            else ()
        ),
        validation_support_lane_counts=(
            tuple(
                sorted(
                    packet.diagnostics.validation_support_by_lane.items()
                )
            )
            if packet is not None
            else ()
        ),
        packet_canon_candidate_status_counts=(
            tuple(
                sorted(
                    packet.diagnostics.candidates_by_canon_status.items()
                )
            )
            if packet is not None
            else ()
        ),
        packet_canon_selected_status_counts=(
            tuple(
                sorted(
                    packet.diagnostics.selected_by_canon_status.items()
                )
            )
            if packet is not None
            else ()
        ),
        packet_canon_candidate_domain_counts=(
            tuple(
                sorted(
                    packet.diagnostics.candidates_by_canon_domain.items()
                )
            )
            if packet is not None
            else ()
        ),
        packet_canon_selected_domain_counts=(
            tuple(
                sorted(
                    packet.diagnostics.selected_by_canon_domain.items()
                )
            )
            if packet is not None
            else ()
        ),
        packet_exclusion_reason_counts=exclusion_counts,
        packet_missing_lanes=missing_lanes,
        packet_revalidation_status=(
            packet.diagnostics.revalidation_status
            if packet is not None
            else "not_evaluated"
        ),
        assessment_pool_eligible_count=(
            int(
                packet.diagnostics.candidates_by_lane.get(
                    "assessment_observation",
                    0,
                )
                or 0
            )
            if packet is not None
            else 0
        ),
        assessment_pool_selected_count=(
            int(
                packet.diagnostics.selected_by_lane.get(
                    "assessment_observation",
                    0,
                )
                or 0
            )
            if packet is not None
            else 0
        ),
        root_collapse_suppression_count=(
            int(packet.diagnostics.root_collapse_suppression or 0)
            if packet is not None
            else 0
        ),
        shared_root_projection_count=(
            int(packet.diagnostics.shared_root_projection_count or 0)
            if packet is not None
            else 0
        ),
        canon_identity_status=(
            str(packet.diagnostics.canon_identity_status or "not_evaluated")
            if packet is not None
            else "not_evaluated"
        ),
        canon_identity_stable_row_count=(
            int(packet.diagnostics.canon_identity_stable_row_count or 0)
            if packet is not None
            else 0
        ),
        profile_status=(
            str(getattr(profile, "status", "not_applicable"))
            if profile is not None
            else "not_applicable"
        ),
        profile_satisfied=bool(
            getattr(profile, "satisfied", False)
        ),
        profile_required_point_count=int(
            getattr(profile, "required_point_count", 0) or 0
        ),
        profile_selected_point_count=int(
            getattr(profile, "selected_point_count", 0) or 0
        ),
        profile_candidate_point_count=int(
            getattr(profile, "candidate_point_count", 0) or 0
        ),
        profile_independent_root_count=int(
            getattr(profile, "independent_root_count", 0) or 0
        ),
        profile_independent_occurrence_count=int(
            getattr(profile, "independent_occurrence_count", 0) or 0
        ),
        profile_reason_codes=tuple(
            getattr(profile, "reason_codes", ()) or ()
        ),
        assessment_conflict_reasons=tuple(
            getattr(assessment, "conflict_reasons", ()) or ()
        ),
        prompt_owner_ready=bool(
            packet_prompt is not None and packet_prompt.ready
        ),
        prompt_owner_reason=(
            str(packet_prompt.reason or "")
            if packet_prompt is not None
            else "not_prepared"
        ),
        replaced_factual_context_count=(
            int(packet_prompt.replaced_factual_context_count or 0)
            if packet_prompt is not None
            else 0
        ),
        omission_reason_codes=tuple(sorted(omission_reasons)),
        living_canon=(living_canon or LivingCanonPreviewDiagnostics()),
    )


def prepare_memory_preview(
    request: MemoryPreviewRequest,
    *,
    environ: Mapping[str, str] | None = None,
    baseline_prompt_builder: BaselinePromptBuilder | None = None,
) -> PreparedMemoryPreview:
    """Prepare one route-equivalent snapshot without touching the source DB."""

    intent = governance.classify_personal_recall_intent(request.wording)
    if not intent.broad_self_profile:
        return PreparedMemoryPreview(
            connection=None,
            request=request,
            environ={},
            diagnostics=_diagnostics(
                route_status=intent.status,
                route_reason=intent.reason,
            ),
        )
    if (
        int(request.guild_id or 0) <= 0
        or int(request.subject_user_id or 0) <= 0
        or int(request.simulated_channel_id or 0) <= 0
    ):
        return PreparedMemoryPreview(
            connection=None,
            request=request,
            environ={},
            diagnostics=_diagnostics(
                route_status="invalid_scope",
                route_reason="preview_scope_incomplete",
            ),
        )
    env = preview_environment(
        guild_id=request.guild_id,
        subject_user_id=request.subject_user_id,
        channel_id=request.simulated_channel_id,
        base=environ,
    )
    capability_receipt = dict(
        shared_brain_configuration(env).get("capability_receipt") or {}
    )
    conn: sqlite3.Connection | None = None
    living_canon = LivingCanonPreviewDiagnostics()
    try:
        conn = _open_read_only_memory_clone(request.source_db_path)
        analysis_conn = sqlite3.connect(":memory:", check_same_thread=False)
        try:
            # Fork the already-frozen source image.  The analyzer remains
            # disposable without taking a second source snapshot that could
            # drift from the packet/assessment/prompt snapshot.
            conn.backup(analysis_conn)
            living_canon = build_living_canon_preview_diagnostics(
                analysis_conn,
                guild_id=request.guild_id,
                subject_key=ledger.subject_key_for_user(
                    request.subject_user_id
                ),
            )
        finally:
            # The analyzer always receives a disposable clone.  DDL and TEMP
            # objects do not increment total_changes, so write diagnostics
            # alone cannot prove that a connection is safe for downstream
            # packet or prompt owners.
            analysis_conn.close()
        conversation_context = _preview_conversation_context(
            conn,
            request,
        )
        effective_request = _request_with_conversation_context(
            request,
            conversation_context,
        )
        if baseline_prompt_builder is not None:
            baseline_prompt, factual_contexts = baseline_prompt_builder(
                conn,
                effective_request,
                env,
            )
            if not str(baseline_prompt or "").strip():
                raise ValueError("preview_baseline_prompt_unavailable")
            effective_request = replace(
                effective_request,
                baseline_prompt=str(baseline_prompt),
                competing_factual_contexts=tuple(
                    str(context)
                    for context in factual_contexts
                    if str(context or "").strip()
                ),
            )
        (
            formation_outcomes,
            formation_reasons,
            source_funnel,
            lifecycle,
        ) = _formation_and_lifecycle(conn, effective_request, env)
        packet = build_packet(
            conn,
            _packet_request(
                effective_request,
                conversation_context,
                env,
            ),
            persist=True,
            environ=env,
        )
        if packet is None:
            diagnostics = _diagnostics(
                route_status="processing_error",
                route_reason="packet_unavailable",
                formation_outcomes=formation_outcomes,
                formation_reason_codes=formation_reasons,
                source_funnel_counts=source_funnel,
                lifecycle=lifecycle,
                living_canon=living_canon,
            )
            return PreparedMemoryPreview(
                connection=conn,
                request=effective_request,
                environ=env,
                diagnostics=diagnostics,
                conversation_context_digest=(
                    conversation_context.digest
                ),
                capability_receipt=capability_receipt,
            )
        assessment = _assessment_from_packet(
            packet,
            effective_request,
            conversation_context,
        )
        basis = build_basis(
            guild_id=effective_request.guild_id,
            user_id=effective_request.subject_user_id,
            channel_id=effective_request.simulated_channel_id,
            route_mode=SIMULATED_ROUTE_MODE,
            channel_policy=SIMULATED_CHANNEL_POLICY,
            current_direct=True,
            user_text=effective_request.wording,
            packet=packet,
            assessment=assessment,
            competing_factual_contexts=(
                effective_request.competing_factual_contexts
                or (
                    (effective_request.factual_placeholder,)
                    if effective_request.factual_placeholder
                    else ()
                )
            ),
            environ=env,
        )
        packet_prompt = (
            build_packet_owned_prompt(
                effective_request.baseline_prompt,
                basis,
            )
            if basis is not None
            else PacketOwnedPrompt(
                prompt=effective_request.baseline_prompt,
                ready=False,
                reason=(
                    "profile_sufficiency_%s"
                    % packet.profile_sufficiency.status
                ),
            )
        )
        diagnostics = _diagnostics(
            route_status="matched",
            route_reason=intent.reason,
            formation_outcomes=formation_outcomes,
            formation_reason_codes=formation_reasons,
            source_funnel_counts=source_funnel,
            lifecycle=lifecycle,
            packet=packet,
            assessment=assessment,
            packet_prompt=packet_prompt,
            living_canon=living_canon,
        )
        return PreparedMemoryPreview(
            connection=conn,
            request=effective_request,
            environ=env,
            diagnostics=diagnostics,
            packet=packet,
            assessment=assessment,
            basis=basis,
            packet_owned_prompt=packet_prompt,
            snapshot_digest=_snapshot_digest(
                packet,
                basis,
                effective_request,
                conversation_context.digest,
                capability_receipt,
            ),
            conversation_context_digest=conversation_context.digest,
            capability_receipt=capability_receipt,
        )
    except (
        FileNotFoundError,
        OSError,
        sqlite3.DatabaseError,
        TypeError,
        ValueError,
    ) as exc:
        if conn is not None:
            conn.close()
        return PreparedMemoryPreview(
            connection=None,
            request=request,
            environ=env,
            diagnostics=_diagnostics(
                route_status="processing_error",
                route_reason=type(exc).__name__,
                living_canon=living_canon,
            ),
            capability_receipt=capability_receipt,
        )


def snapshots_equivalent(
    earlier: PreparedMemoryPreview,
    later: PreparedMemoryPreview,
) -> tuple[bool, str]:
    """Compare only deterministic response evidence, never receipt IDs."""

    if earlier.packet is None or later.packet is None:
        return False, "preview_snapshot_not_ready"
    if not earlier.snapshot_digest or not later.snapshot_digest:
        return False, "preview_snapshot_digest_missing"
    if earlier.snapshot_digest != later.snapshot_digest:
        return False, "preview_source_changed"
    if earlier.packet_owned_prompt.ready != later.packet_owned_prompt.ready:
        return False, "preview_prompt_readiness_changed"
    if (
        earlier.packet_owned_prompt.ready
        and earlier.packet_owned_prompt.prompt
        != later.packet_owned_prompt.prompt
    ):
        return False, "preview_prompt_basis_changed"
    return True, "preview_source_unchanged"


def evaluate_memory_preview(
    prepared: PreparedMemoryPreview,
    *,
    baseline_response: str,
    candidate_response: str,
    candidate_generation_latency_ms: int | None = None,
) -> MemoryPreviewEvaluation:
    """Apply the real synthesis receipt and selection gate on the clone."""

    if (
        not prepared.ready
        or prepared.connection is None
        or prepared.basis is None
    ):
        return MemoryPreviewEvaluation(
            decision=None,
            response=str(baseline_response or "").strip(),
            candidate_selected=False,
            fallback_reason=(
                prepared.packet_owned_prompt.reason
                or "preview_not_ready"
            ),
        )
    run = begin_run(
        prepared.connection,
        prepared.basis,
        baseline_response=str(baseline_response or ""),
        candidate_prompt_ready=prepared.packet_owned_prompt.ready,
        candidate_prompt_failure_reason=(
            prepared.packet_owned_prompt.reason
        ),
        replaced_factual_context_count=(
            prepared.packet_owned_prompt
            .replaced_factual_context_count
        ),
        environ=prepared.environ,
    )
    decision = evaluate_candidate(
        prepared.connection,
        run,
        baseline_response=str(baseline_response or ""),
        candidate_response=str(candidate_response or ""),
        candidate_generation_latency_ms=(
            candidate_generation_latency_ms
        ),
        environ=prepared.environ,
    )
    prepared.connection.commit()
    return MemoryPreviewEvaluation(
        decision=decision,
        response=decision.response,
        candidate_selected=decision.candidate_selected,
        fallback_reason=decision.fallback_reason,
        baseline_member_point_count=(
            decision.baseline_member_point_coverage_count
        ),
        baseline_member_detail_count=(
            decision.baseline_member_detail_coverage_count
        ),
        baseline_canon_count=decision.baseline_canon_coverage_count,
        candidate_member_point_count=(
            decision.candidate_member_point_coverage_count
        ),
        candidate_member_detail_count=(
            decision.candidate_member_detail_coverage_count
        ),
        candidate_member_root_count=(
            decision.candidate_member_root_coverage_count
        ),
        candidate_member_occurrence_count=(
            decision.candidate_member_occurrence_coverage_count
        ),
        candidate_canon_count=(
            decision.candidate_canon_coverage_count
        ),
        candidate_lore_dominant=decision.candidate_lore_dominant,
        candidate_member_supported_claim_count=(
            decision.candidate_member_supported_claim_count
        ),
        candidate_canon_supported_claim_count=(
            decision.candidate_canon_supported_claim_count
        ),
        candidate_opinion_claim_count=(
            decision.candidate_opinion_claim_count
        ),
        candidate_connective_claim_count=(
            decision.candidate_connective_claim_count
        ),
        candidate_unsupported_factual_claim_count=(
            decision.candidate_unsupported_factual_claim_count
        ),
        candidate_claim_classifications=(
            decision.candidate_claim_classifications
        ),
        supported_coverage_regressed=(
            decision.supported_coverage_regressed
        ),
    )


def fallback_memory_preview(
    prepared: PreparedMemoryPreview,
    evaluation: MemoryPreviewEvaluation,
    *,
    reason: str,
) -> MemoryPreviewEvaluation:
    if (
        prepared.connection is None
        or evaluation.decision is None
    ):
        return MemoryPreviewEvaluation(
            decision=evaluation.decision,
            response=evaluation.response,
            candidate_selected=False,
            fallback_reason=str(reason or "preview_fallback"),
            baseline_member_point_count=(
                evaluation.baseline_member_point_count
            ),
            baseline_member_detail_count=(
                evaluation.baseline_member_detail_count
            ),
            baseline_canon_count=evaluation.baseline_canon_count,
            candidate_member_point_count=(
                evaluation.candidate_member_point_count
            ),
            candidate_member_detail_count=(
                evaluation.candidate_member_detail_count
            ),
            candidate_member_root_count=(
                evaluation.candidate_member_root_count
            ),
            candidate_member_occurrence_count=(
                evaluation.candidate_member_occurrence_count
            ),
            candidate_canon_count=evaluation.candidate_canon_count,
            candidate_lore_dominant=(
                evaluation.candidate_lore_dominant
            ),
            candidate_member_supported_claim_count=(
                evaluation.candidate_member_supported_claim_count
            ),
            candidate_canon_supported_claim_count=(
                evaluation.candidate_canon_supported_claim_count
            ),
            candidate_opinion_claim_count=(
                evaluation.candidate_opinion_claim_count
            ),
            candidate_connective_claim_count=(
                evaluation.candidate_connective_claim_count
            ),
            candidate_unsupported_factual_claim_count=(
                evaluation.candidate_unsupported_factual_claim_count
            ),
            candidate_claim_classifications=(
                evaluation.candidate_claim_classifications
            ),
            supported_coverage_regressed=(
                evaluation.supported_coverage_regressed
            ),
        )
    decision = record_fallback(
        prepared.connection,
        evaluation.decision,
        reason=str(reason or "preview_fallback"),
    )
    prepared.connection.commit()
    return MemoryPreviewEvaluation(
        decision=decision,
        response=decision.response,
        candidate_selected=False,
        fallback_reason=decision.fallback_reason,
        baseline_member_point_count=(
            decision.baseline_member_point_coverage_count
        ),
        baseline_member_detail_count=(
            decision.baseline_member_detail_coverage_count
        ),
        baseline_canon_count=decision.baseline_canon_coverage_count,
        candidate_member_point_count=(
            decision.candidate_member_point_coverage_count
        ),
        candidate_member_detail_count=(
            decision.candidate_member_detail_coverage_count
        ),
        candidate_member_root_count=(
            decision.candidate_member_root_coverage_count
        ),
        candidate_member_occurrence_count=(
            decision.candidate_member_occurrence_coverage_count
        ),
        candidate_canon_count=(
            decision.candidate_canon_coverage_count
        ),
        candidate_lore_dominant=decision.candidate_lore_dominant,
        candidate_member_supported_claim_count=(
            decision.candidate_member_supported_claim_count
        ),
        candidate_canon_supported_claim_count=(
            decision.candidate_canon_supported_claim_count
        ),
        candidate_opinion_claim_count=(
            decision.candidate_opinion_claim_count
        ),
        candidate_connective_claim_count=(
            decision.candidate_connective_claim_count
        ),
        candidate_unsupported_factual_claim_count=(
            decision.candidate_unsupported_factual_claim_count
        ),
        candidate_claim_classifications=(
            decision.candidate_claim_classifications
        ),
        supported_coverage_regressed=(
            decision.supported_coverage_regressed
        ),
    )


def finalize_memory_preview(
    prepared: PreparedMemoryPreview,
    evaluation: MemoryPreviewEvaluation,
    *,
    final_response: str,
    guard_status: str,
) -> bool:
    """Finalize only the disposable clone; a preview is never live-applied."""

    if (
        prepared.connection is None
        or evaluation.decision is None
    ):
        return False
    finalized = finalize_run(
        prepared.connection,
        evaluation.decision,
        final_response=str(final_response or ""),
        response_sent=False,
        candidate_live=False,
        guard_status=str(guard_status or "preview_not_sent"),
    )
    prepared.connection.commit()
    return finalized


def render_content_free_diagnostics(
    prepared: PreparedMemoryPreview,
    evaluation: MemoryPreviewEvaluation | None = None,
    *,
    stale_reason: str = "",
    guard_suppression_reason: str = "",
    final_response_available: bool = False,
) -> tuple[str, ...]:
    """Render no member content, source identifiers, or response text."""

    diag = prepared.diagnostics
    evaluation = evaluation or MemoryPreviewEvaluation(
        decision=None,
        response="",
        candidate_selected=False,
        fallback_reason=(
            prepared.packet_owned_prompt.reason or "not_evaluated"
        ),
    )
    living = diag.living_canon
    capability = dict(prepared.capability_receipt or {})
    capability_versions = {
        key: capability.get(key, "unverified")
        for key in (
            "packet_version",
            "claim_contract_version",
            "assessment_version",
            "identity_contract_version",
            "synthesis_version",
        )
    }
    return (
        "- preview_schema: `%s`" % diag.schema_version,
        "- simulated_route: `normal_chat/public_home/#barcode-bot`",
        "- route_interpretation: `%s` reason=`%s`"
        % (diag.route_status, diag.route_reason or "none"),
        "- capability_receipt: `version=%s capability=%s contract=%s "
        "requested=%s effective=%s prerequisites_ready=%s scope=%s "
        "versions=%s conflicts=%s reason=%s kill_switch=%s`"
        % (
            capability.get("receipt_version", "unverified"),
            capability.get("capability", "unverified"),
            capability.get("contract_version", "unverified"),
            str(bool(capability.get("requested"))).lower(),
            str(bool(capability.get("effective"))).lower(),
            str(bool(capability.get("prerequisites_ready"))).lower(),
            capability.get("scope_digest", "none") or "none",
            capability_versions,
            list(capability.get("conflicts") or ()) or ["none"],
            capability.get("reason", "unverified"),
            capability.get("kill_switch", "none"),
        ),
        "- formation_outcomes: `%s`"
        % dict(diag.formation_outcomes),
        "- formation_reasons: `%s`"
        % (list(diag.formation_reason_codes) or ["none"]),
        "- source_funnel: `%s`"
        % dict(diag.source_funnel_counts),
        "- living_canon_dry_run: `status=%s reason=%s recurrence=%s "
        "grouping=%s proposed=%s skipped=%s ambiguous=%s rejected=%s "
        "roots=%s occurrences=%s collapsed_roots=%s "
        "source_write_count=%s source_write_occurred=%s`"
        % (
            living.status,
            living.status_reason or "none",
            living.recurrence_contract_version,
            living.grouping_signature_version,
            living.proposed_count,
            living.skipped_count,
            living.ambiguous_count,
            living.rejected_count,
            living.independent_root_count,
            living.independent_occurrence_count,
            living.collapsed_root_count,
            living.source_write_count,
            str(living.source_write_occurred).lower(),
        ),
        "- living_canon_states: `%s`"
        % dict(living.candidate_state_counts),
        "- living_canon_reasons: `%s`" % dict(living.reason_counts),
        "- living_canon_bounds: `%s`" % dict(living.bounds),
        "- lifecycle: `scopes=%s candidates=%s state_changes=%s`"
        % (
            diag.lifecycle_scopes,
            diag.lifecycle_candidates,
            diag.lifecycle_state_changes,
        ),
        "- packet_lanes: `%s`" % dict(diag.packet_lane_counts),
        "- validation_support_lanes: `%s`"
        % dict(diag.validation_support_lane_counts),
        "- canon_status_counts: `candidates=%s selected=%s`"
        % (
            dict(diag.packet_canon_candidate_status_counts),
            dict(diag.packet_canon_selected_status_counts),
        ),
        "- canon_domain_counts: `candidates=%s selected=%s`"
        % (
            dict(diag.packet_canon_candidate_domain_counts),
            dict(diag.packet_canon_selected_domain_counts),
        ),
        "- public_assessment_pool: `eligible=%s selected=%s`"
        % (
            diag.assessment_pool_eligible_count,
            diag.assessment_pool_selected_count,
        ),
        "- canon_identity_signal: `status=%s stable_public_rows=%s`"
        % (
            diag.canon_identity_status,
            diag.canon_identity_stable_row_count,
        ),
        "- rendered_packet_lanes: `%s` project_canon_required=`%s`"
        % (
            (
                dict(prepared.basis.rendered_lane_counts)
                if prepared.basis is not None
                else {}
            ),
            str(
                bool(
                    prepared.basis is not None
                    and prepared.basis.profile_requires_canon
                )
            ).lower(),
        ),
        "- profile_sufficiency: `%s` satisfied=`%s` "
        "points=`%s/%s` eligible_points=`%s` roots=`%s` "
        "occurrences=`%s` reasons=`%s`"
        % (
            diag.profile_status,
            str(diag.profile_satisfied).lower(),
            diag.profile_selected_point_count,
            diag.profile_required_point_count,
            diag.profile_candidate_point_count,
            diag.profile_independent_root_count,
            diag.profile_independent_occurrence_count,
            list(diag.profile_reason_codes) or ["none"],
        ),
        "- root_collapse: `suppressed=%s shared_projections=%s`"
        % (
            diag.root_collapse_suppression_count,
            diag.shared_root_projection_count,
        ),
        "- omissions: `%s`"
        % (list(diag.omission_reason_codes) or ["none"]),
        "- packet_revalidation: `%s`"
        % diag.packet_revalidation_status,
        "- prompt_factual_owner: `ready=%s replacements=%s reason=%s`"
        % (
            str(diag.prompt_owner_ready).lower(),
            diag.replaced_factual_context_count,
            diag.prompt_owner_reason or "none",
        ),
        "- candidate_gate: `selected=%s fallback=%s baseline_points=%s "
        "candidate_points=%s baseline_details=%s candidate_details=%s "
        "roots=%s occurrences=%s baseline_canon=%s candidate_canon=%s "
        "coverage_regressed=%s member_claims=%s canon_claims=%s "
        "opinions=%s connective=%s unsupported=%s`"
        % (
            str(evaluation.candidate_selected).lower(),
            evaluation.fallback_reason or "none",
            evaluation.baseline_member_point_count,
            evaluation.candidate_member_point_count,
            evaluation.baseline_member_detail_count,
            evaluation.candidate_member_detail_count,
            evaluation.candidate_member_root_count,
            evaluation.candidate_member_occurrence_count,
            evaluation.baseline_canon_count,
            evaluation.candidate_canon_count,
            str(evaluation.supported_coverage_regressed).lower(),
            evaluation.candidate_member_supported_claim_count,
            evaluation.candidate_canon_supported_claim_count,
            evaluation.candidate_opinion_claim_count,
            evaluation.candidate_connective_claim_count,
            evaluation.candidate_unsupported_factual_claim_count,
        ),
        "- claim_classifications: `%s`"
        % (
            list(evaluation.candidate_claim_classifications)
            or ["not_evaluated"]
        ),
        "- stale_source: `%s`"
        % (stale_reason or "none"),
        "- guard_suppression: `%s`"
        % (guard_suppression_reason or "none"),
        "- proposed_response_available: `%s`"
        % str(bool(final_response_available)).lower(),
        "- persistence: `source_db_read_only=true clone=memory "
        "invocation_saved=false response_saved=false live_receipt=false`",
    )
