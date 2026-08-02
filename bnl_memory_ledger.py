"""Unified Memory Ledger v1 shadow schema and write adapters.

The ledger is append-oriented shadow infrastructure. Legacy memory remains the
default production source of truth; separately gated governance and Moment
adapters may consume only revalidated, route-safe projections.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
import sqlite3
from typing import Any, Iterable

from bnl_canon_source_contract import (
    Confidence,
    PUBLIC_ASSESSMENT_EVIDENCE_VERSION,
    SourceClass,
    SourceClaim,
    SubjectIdentity,
    Visibility,
    has_explicit_channel_policy_mapping,
    has_explicit_route_source_mapping,
    is_public_usable,
    map_channel_policy_visibility,
    map_route_source_label,
)

MEMORY_LEDGER_SCHEMA_VERSION = "memory_ledger_v1"
ATOMIC_KNOWLEDGE_SCHEMA_VERSION = "memory_ledger_atomic_knowledge_v1"
ATOMIC_KNOWLEDGE_BACKFILL = "atomic_knowledge_backfill_v1"
RETAINED_CONVERSATION_LEDGER_BACKFILL = (
    "retained_conversation_ledger_backfill_v1"
)
ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION = (
    "memory_ledger_atomic_knowledge_lifecycle_v1"
)
ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL = (
    "atomic_knowledge_lifecycle_backfill_v1"
)
ATOMIC_KNOWLEDGE_LIFECYCLE_SWEEP = "atomic_knowledge_lifecycle_sweep_v1"
MEMORY_LEDGER_SHADOW_ENV = "BNL_MEMORY_LEDGER_SHADOW_ENABLED"
CONVERSATION_MOTIF_FORMATION_ENV = (
    "BNL_CONVERSATION_MOTIF_FORMATION_SHADOW_ENABLED"
)
BNL_SUBJECT_KEY = "bnl_01"
BNL_SELF_NAME_PREDICATE_PREFIX = "bnl_self_name:"
BNL_SELF_NAME_PUBLIC_POLICIES = frozenset(
    {"public_home", "public_context", "public_selective"}
)

ENTRY_TYPES = frozenset({
    "observation", "claim", "event", "preference", "boundary", "goal", "open_loop", "commitment",
    "shared_moment", "relationship_event", "canon_reference", "show_event", "unresolved_question", "derived_summary",
})
LINEAGE_TYPES = frozenset({"derived_from", "correction_of", "supersedes", "retracts", "duplicate_of", "part_of_moment"})
OUTCOMES = frozenset({"inserted", "deduplicated", "skipped", "error"})
ACTIVE_LIFECYCLE = "active"
REVIEW_ONLY_LIFECYCLE = "review_only"
RESOLVED_LIFECYCLE = "resolved"
REJECTED_LIFECYCLE = "rejected"

KNOWLEDGE_CANDIDATE_TYPES = frozenset(
    {
        "topic_or_motif",
        "person_role_fact",
        "project_event_or_milestone",
        "open_loop_or_question",
        "inference_or_contested_claim",
    }
)
KNOWLEDGE_EPISTEMIC_STATUSES = frozenset(
    {
        "stated",
        "observed",
        "source_abstraction",
        "inference",
        "contested",
        "question",
    }
)
KNOWLEDGE_CURRENTNESS = frozenset(
    {"current", "historical", "open", "unresolved", "uncertain"}
)
KNOWLEDGE_CANDIDATE_STATES = frozenset(
    {
        "candidate",
        "provisional",
        "established",
        "contested",
        "superseded",
        "retired",
        "invalidated",
    }
)
KNOWLEDGE_ACTIVE_CANDIDATE_STATES = frozenset(
    {"candidate", "provisional", "established"}
)
KNOWLEDGE_TERMINAL_CANDIDATE_STATES = frozenset(
    {"superseded", "retired", "invalidated"}
)
# A motif's predicate/contradiction scope is its atomic meaning; the prose value
# is an evidence-bounded rendering and may vary across independently rooted
# Moments. Scalar facts and other candidate types retain value-level conflict.
KNOWLEDGE_SCOPE_CONSOLIDATED_TYPES = frozenset({"topic_or_motif"})
KNOWLEDGE_TERMINAL_ROOT_LIFECYCLES = frozenset(
    {
        "corrected",
        "superseded",
        "retracted",
        "expired",
        "quarantined",
        "needs_review",
        "forgotten",
        "deleted",
        "rejected",
        "unresolved",
    }
)
KNOWLEDGE_DERIVATIVE_SOURCE_CLASSES = frozenset(
    {
        SourceClass.DERIVED_SUMMARY.value,
        SourceClass.EVIDENCE_PROJECTION.value,
        SourceClass.SOURCE_FILE_PROJECTION.value,
        SourceClass.DOSSIER_PROJECTION.value,
        SourceClass.ENTITY_EVIDENCE_PROJECTION.value,
        SourceClass.LEGACY_SOURCE_BLIND.value,
    }
)
_KNOWLEDGE_AUTHORITY_RANK = {
    SourceClass.LEGACY_SOURCE_BLIND.value: 0,
    SourceClass.DERIVED_SUMMARY.value: 1,
    SourceClass.ENTITY_EVIDENCE_PROJECTION.value: 1,
    SourceClass.DOSSIER_PROJECTION.value: 1,
    SourceClass.SOURCE_FILE_PROJECTION.value: 1,
    SourceClass.EVIDENCE_PROJECTION.value: 1,
    SourceClass.PUBLIC_OBSERVATION.value: 2,
    SourceClass.RUNTIME_OBSERVATION.value: 3,
    SourceClass.FIRST_PARTY_RECORD.value: 4,
    SourceClass.APPROVED_CANON.value: 5,
    SourceClass.OWNER_CORRECTION.value: 6,
}
_KNOWLEDGE_CONFIDENCE_RANK = {
    Confidence.UNKNOWN.value: 0,
    Confidence.LOW.value: 1,
    Confidence.MEDIUM.value: 2,
    Confidence.HIGH.value: 3,
    Confidence.APPROVED.value: 4,
}
_KNOWLEDGE_RESTRICTED_VISIBILITIES = frozenset(
    {
        Visibility.SEALED_TEST.value,
        Visibility.PROTECTED.value,
        Visibility.AI_IMAGE_TOOL.value,
        Visibility.UNKNOWN.value,
    }
)
_KNOWLEDGE_TEST_OR_OPERATIONAL_RE = re.compile(
    r"\b(?:queue|rehearsal|synthetic[-\s]?artist|test[-\s]?payment|"
    r"payment[-\s]?test|queue[-\s]?simulation|simulation[-\s]?queue|"
    r"wheel(?:\s+spin)?|now[-\s]?playing|up[-\s]?next|priority[-\s]?signal|"
    r"show[-\s]?test|showtest)\b",
    re.I,
)
_KNOWLEDGE_TEST_OR_OPERATIONAL_SOURCE_RE = re.compile(
    r"(?:queue|payment|wheel|rehearsal|show[_-]?test|simulation)",
    re.I,
)
_CONVERSATION_MOTIF_RECALL_RE = re.compile(
    r"\b(?:what|who|tell|remind)\b.{0,48}"
    r"\b(?:know|remember|have|got|recall)\b.{0,36}"
    r"\b(?:about|on)?\s*(?:me|myself|who\s+i\s+am)\b"
    r"|\bwhat\s+am\s+i\s+all\s+about\b",
    re.I,
)
_CONVERSATION_MOTIF_UNSAFE_RE = re.compile(
    r"\b(?:password|passcode|pin|one[- ]?time\s+(?:code|password)|otp|"
    r"verification\s+code|security\s+code|recovery\s+code|access\s+code|"
    r"api\s+key|secret\s+key|private\s+key|seed\s+phrase|"
    r"(?:auth|access|deployment|session)\s+token|routing\s+number|"
    r"bank\s+account|credit\s+card|debit\s+card|social\s+security|ssn)\b"
    r"|\b(?:ignore|disregard|override|bypass|reveal)\b.{0,40}"
    r"\b(?:system|developer|assistant|prompt|instructions?|rules?|secret)\b",
    re.I,
)
_CONVERSATION_MOTIF_DIRECT_FACT_RE = re.compile(
    r"\b(?:call\s+me|my\s+(?:email|phone(?:\s+number)?|home\s+address|"
    r"street\s+address|legal\s+name|real\s+name|full\s+name|"
    r"preferred\s+name|pronouns?|birthday|date\s+of\s+birth|employer|"
    r"workplace|favorite\s+(?:color|movie|food|place))\s+(?:is|are)|"
    r"i\s+(?:live|reside)\s+(?:at|in|near))\b",
    re.I,
)
_CONVERSATION_MOTIF_SENSITIVE_RE = re.compile(
    r"\b(?:diagnos(?:ed|is)|medical\s+condition|health\s+condition|"
    r"medication|therapy|therapist|pregnan(?:t|cy)|sexuality|"
    r"sexual\s+orientation|gender\s+identity|race|ethnicity|religion|"
    r"political\s+affiliation|immigration\s+status|criminal\s+record|"
    r"salary|income|bank\s+balance|financial\s+account|home\s+location|"
    r"where\s+i\s+live|family\s+emergency|private\s+relationship)\b",
    re.I,
)
_CONVERSATION_MOTIF_ROLEPLAY_RE = re.compile(
    r"\b(?:pretend|role[- ]?play|in\s+(?:this|the)\s+scene|"
    r"my\s+character|character\s+says|if\s+i\s+(?:said|were)|"
    r"hypothetically|just\s+kidding|j/?k|sarcasm)\b",
    re.I,
)
_CONVERSATION_MOTIF_URL_OR_MENTION_TOKEN_RE = re.compile(
    r"(?:https?://\S+|www\.\S+|"
    r"\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b|<@!?\d+>)",
    re.I,
)
_CONVERSATION_MOTIF_TERM_RE = re.compile(r"[a-z][a-z'’-]{2,}", re.I)
_CONVERSATION_MOTIF_STOPWORDS = frozenset(
    """
    about after again also and are because been before being but can could
    did does doing for from getting going had has have here how into its just
    know like made make more much need now okay only our really remember said
    should some still than that the their them then there these they thing
    things this those through too want was were what when where which who why
    will with would yeah yes you your
    """.split()
)
_CONVERSATION_MOTIF_FAMILIES = (
    (
        "music_production",
        "music and audio production",
        frozenset(
            {
                "album",
                "artist",
                "audio",
                "beat",
                "broadcast",
                "drum",
                "drums",
                "mix",
                "music",
                "radio",
                "release",
                "song",
                "songs",
                "sound",
                "synth",
                "track",
                "tracks",
                "vocal",
                "vocals",
            }
        ),
    ),
    (
        "games_and_creatures",
        "games and interactive systems",
        frozenset(
            {
                "battle",
                "battles",
                "card",
                "cards",
                "class",
                "classes",
                "creature",
                "creatures",
                "game",
                "games",
                "level",
                "levels",
                "monster",
                "monsters",
                "player",
                "players",
            }
        ),
    ),
    (
        "art_and_visuals",
        "art and visual design",
        frozenset(
            {
                "animation",
                "art",
                "artwork",
                "banner",
                "character",
                "characters",
                "design",
                "image",
                "images",
                "photo",
                "photos",
                "sprite",
                "style",
                "visual",
                "visuals",
            }
        ),
    ),
    (
        "code_and_systems",
        "software and technical systems",
        frozenset(
            {
                "bot",
                "bug",
                "bugs",
                "code",
                "deploy",
                "deployment",
                "error",
                "fix",
                "github",
                "memory",
                "system",
                "systems",
                "test",
                "testing",
                "website",
            }
        ),
    ),
    (
        "community_and_collaboration",
        "community, collaboration, and shared projects",
        frozenset(
            {
                "collab",
                "collaboration",
                "community",
                "discord",
                "friend",
                "friends",
                "group",
                "member",
                "members",
                "people",
                "project",
                "projects",
                "server",
                "team",
            }
        ),
    ),
    (
        "lore_and_writing",
        "lore, writing, and worldbuilding",
        frozenset(
            {
                "book",
                "canon",
                "chapter",
                "lore",
                "narrative",
                "scene",
                "story",
                "world",
                "worldbuilding",
                "write",
                "writing",
            }
        ),
    ),
    (
        "cooking",
        "food and cooking",
        frozenset(
            {
                "bake",
                "baking",
                "cook",
                "cooking",
                "dinner",
                "food",
                "lunch",
                "meal",
                "oven",
                "pizza",
                "recipe",
            }
        ),
    ),
    (
        "outdoors",
        "outdoor plans and conditions",
        frozenset(
            {
                "hike",
                "hiking",
                "mountain",
                "outdoors",
                "rain",
                "trail",
                "weather",
            }
        ),
    ),
    (
        "humor_and_banter",
        "jokes and community banter",
        frozenset(
            {
                "banter",
                "funny",
                "joke",
                "jokes",
                "laugh",
                "meme",
                "memes",
            }
        ),
    ),
)
_CONVERSATION_MOTIF_ANCHORS = {
    "music_production": frozenset(
        {
            "album",
            "artist",
            "audio",
            "beat",
            "drum",
            "drums",
            "music",
            "radio",
            "song",
            "songs",
            "sound",
            "synth",
            "track",
            "tracks",
            "vocal",
            "vocals",
        }
    ),
    "games_and_creatures": frozenset(
        {
            "battle",
            "battles",
            "creature",
            "creatures",
            "game",
            "games",
            "monster",
            "monsters",
            "player",
            "players",
        }
    ),
    "art_and_visuals": frozenset(
        {
            "animation",
            "art",
            "artwork",
            "image",
            "images",
            "photo",
            "photos",
            "sprite",
            "visual",
            "visuals",
        }
    ),
    "code_and_systems": frozenset(
        {
            "bot",
            "bug",
            "bugs",
            "code",
            "deploy",
            "deployment",
            "github",
            "website",
        }
    ),
    "community_and_collaboration": frozenset(
        {
            "collab",
            "collaboration",
            "community",
            "discord",
            "group",
            "member",
            "members",
            "server",
            "team",
        }
    ),
    "lore_and_writing": frozenset(
        {
            "canon",
            "lore",
            "narrative",
            "story",
            "worldbuilding",
            "write",
            "writing",
        }
    ),
    "cooking": frozenset(
        {
            "bake",
            "baking",
            "cook",
            "cooking",
            "dinner",
            "food",
            "lunch",
            "meal",
            "oven",
            "pizza",
            "recipe",
        }
    ),
    "outdoors": frozenset(
        {
            "hike",
            "hiking",
            "mountain",
            "outdoors",
            "rain",
            "trail",
            "weather",
        }
    ),
    "humor_and_banter": frozenset(
        {
            "banter",
            "funny",
            "joke",
            "jokes",
            "laugh",
            "meme",
            "memes",
        }
    ),
}
_CONVERSATION_MOTIF_WINDOW_SECONDS = 30 * 60
_CONVERSATION_OCCURRENCE_MAX_SCAN = 64
_CONVERSATION_CORRECTION_MAX_SCAN = 40
_CONVERSATION_MOTIF_MAX_SCAN = 1200
_CONVERSATION_MOTIF_MAX_CANDIDATES = 6
_CONVERSATION_MOTIF_MAX_ROOTS = 12
_CONVERSATION_MOTIF_PUBLIC_POLICIES = frozenset(
    {"public_home", "public_context", "public_selective"}
)
_PUBLIC_ASSESSMENT_MAX_RESULTS = 4
_PUBLIC_ASSESSMENT_TERM_RE = re.compile(
    r"[a-z0-9][a-z0-9'’-]{2,}",
    re.I,
)
_PUBLIC_ASSESSMENT_STOPWORDS = frozenset(
    """
    about after again also and are because been before being but can could
    did does doing for from getting going had has have here how into its just
    know more much now okay only our really remember said some still than that
    the their them then there these they thing things this those through too
    was were what when where which who why will with would yeah yes you your
    """.split()
)
_PUBLIC_ASSESSMENT_PROCESS_QUERY_RE = re.compile(
    r"\b(?:how\s+(?:i|we|you)\s+work|"
    r"make\s+decisions?|decision[-\s]?making|"
    r"approach(?:es)?|process|method|workflow)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_PROCESS_TERMS = frozenset(
    {
        "approach",
        "build",
        "building",
        "careful",
        "check",
        "checking",
        "choose",
        "choosing",
        "compare",
        "comparing",
        "decide",
        "deciding",
        "decision",
        "decisions",
        "fix",
        "fixing",
        "iterate",
        "iterating",
        "iteration",
        "method",
        "plan",
        "planning",
        "prefer",
        "priority",
        "process",
        "refine",
        "refining",
        "revise",
        "revising",
        "standard",
        "standards",
        "test",
        "testing",
        "tradeoff",
        "work",
        "working",
    }
)

APPROVED_SELF_AUTHORED_FACT_KEYS = frozenset({
    "preferred_name",
    "pronouns",
    "favorite_color",
    "favorite_movie",
})
_CONVERSATION_CORRECTION_RE = re.compile(
    r"(?:"
    r"^\s*(?:actually|correction|correcting)\b"
    r"|\b(?:i|we)\s+meant\b"
    r"|\b(?:that's|that\s+is)\s+wrong\b"
    r"|^\s*no\s*[,;:]\s*not\s+that\b"
    r"|\b(?:i|we)\s+(?:need|want)\s+to\s+correct\b"
    r"|\b(?:please\s+)?correct\s+(?:that|this|my|the\s+(?:last|previous))\b"
    r"|\b(?:change|replace|swap)\s+"
    r"(?:my\s+(?:answer|preference|favorite|name|pronouns?)"
    r"|the\s+(?:last|previous)\b)"
    r")",
    re.I,
)
_CORRECTION_TOPIC_STOPWORDS = frozenset(
    {
        "actually",
        "and",
        "change",
        "correcting",
        "correction",
        "for",
        "from",
        "instead",
        "into",
        "meant",
        "not",
        "replace",
        "swap",
        "that",
        "the",
        "this",
        "to",
        "use",
        "with",
        "wrong",
    }
)
@dataclass(frozen=True)
class LedgerWriteResult:
    entry_id: str = ""
    outcome: str = "skipped"
    reason_code: str = "not_attempted"
    source_table: str = ""
    source_row_id: str = ""
    source_revision: str = ""
    source_event_key: str = ""
    guild_id: int = 0

    def __post_init__(self):
        if self.outcome not in OUTCOMES:
            object.__setattr__(self, "outcome", "error")

    def __str__(self) -> str:
        return self.entry_id

    def __bool__(self) -> bool:
        return self.outcome in {"inserted", "deduplicated"} and bool(self.entry_id)


@dataclass(frozen=True)
class AtomicKnowledgeProposal:
    candidate_type: str
    subject_key: str
    predicate_key: str
    meaning: str
    root_entry_ids: tuple[str, ...]
    derivative_entry_ids: tuple[str, ...] = field(default_factory=tuple)
    subject_display_name: str = ""
    participant_keys: tuple[str, ...] = field(default_factory=tuple)
    epistemic_status: str = "stated"
    uncertainty_note: str = ""
    currentness: str = "current"
    contradiction_key: str = ""
    retrieval_tags: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class AtomicKnowledgeResult:
    candidate_id: str = ""
    outcome: str = "rejected"
    reason_code: str = "not_attempted"
    candidate_type: str = ""
    root_count: int = 0

    def __bool__(self) -> bool:
        return self.outcome in {"created", "matched_existing"} and bool(
            self.candidate_id
        )


@dataclass(frozen=True)
class PublicAssessmentEvidence:
    """One public, source-linked observation selected for current assessment."""

    entry_id: str
    text: str
    observed_at: str
    visibility: str
    occurrence_identity: str
    score: float
    request_relevant: bool = False
    subject_key: str = ""
    assessment_contract_version: str = PUBLIC_ASSESSMENT_EVIDENCE_VERSION
    source_system: str = "memory_ledger_public_assessment"
    source_role: str = "user"
    source_class: str = SourceClass.PUBLIC_OBSERVATION.value
    lifecycle_status: str = ACTIVE_LIFECYCLE
    channel_policy: str = "unknown"
    public_usable: bool = False
    subject_authored: bool = False
    selector_eligible: bool = False
    derived: bool = True
    projection: bool = True


@dataclass(frozen=True)
class PublicAssessmentSelection:
    """Bounded response-time selection over the whole eligible public pool."""

    scanned_count: int = 0
    eligible_count: int = 0
    request_relevant_count: int = 0
    items: tuple[PublicAssessmentEvidence, ...] = ()


@dataclass(frozen=True)
class BnlSelfNameRecord:
    """Current governed routing state for one name people may use for BNL.

    This is a read projection over Memory Ledger entries, not a separate
    nickname store.  The original conversation remains the human source and
    BNL's explicit response is the first-party decision.
    """

    normalized_name: str
    display_name: str
    decision: str
    entry_id: str
    observed_at: str = ""


def shadow_enabled(environ: dict[str, str] | None = None) -> bool:
    value = (environ or os.environ).get(MEMORY_LEDGER_SHADOW_ENV, "")
    return str(value).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def conversation_motif_formation_enabled(
    environ: dict[str, str] | None = None,
) -> bool:
    """Keep recurring-conversation formation shadow-only and kill-switchable."""
    env = os.environ if environ is None else environ
    if not shadow_enabled(env):
        return False
    return str(env.get(CONVERSATION_MOTIF_FORMATION_ENV, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "enabled",
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canon(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).strip().lower())


def subject_key_for_user(user_id: int | str | None) -> str:
    return f"discord_user:{int(user_id or 0)}"


def normalize_bnl_self_name(value: Any) -> str:
    """Return one conservative comparison key without inventing aliases."""

    cleaned = re.sub(r"\s+", " ", str(value or "")).strip(" \t\r\n,.;:!?\"'“”‘’")
    if not cleaned or len(cleaned) > 48:
        return ""
    if len(cleaned.split()) > 4:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _.'’\-]{0,47}", cleaned):
        return ""
    return _canon(cleaned)


def current_bnl_self_name_records(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    channel_policies: tuple[str, ...] = (),
) -> tuple[BnlSelfNameRecord, ...]:
    """Read current self-name decisions from the existing Ledger lifecycle."""

    if int(guild_id or 0) <= 0:
        return ()
    ensure_memory_ledger_schema(conn)
    scoped_policies = tuple(
        sorted(
            {
                str(policy or "").strip().lower()
                for policy in channel_policies
                if str(policy or "").strip()
            }
        )
    )
    policy_clause = ""
    params: list[Any] = [
        int(guild_id),
        BNL_SUBJECT_KEY,
        BNL_SELF_NAME_PREDICATE_PREFIX + "%",
    ]
    if scoped_policies:
        policy_clause = " AND channel_policy IN (%s)" % ",".join(
            "?" for _ in scoped_policies
        )
        params.extend(scoped_policies)
    params.append(int(guild_id))
    rows = conn.execute(
        (
            """
        SELECT entry_id,normalized_value,observed_at,channel_policy,
               channel_id,visibility
        FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=?
          AND predicate_key LIKE ?
          %s
          AND lifecycle_status='active'
          AND entry_id NOT IN (
            SELECT target_entry_id
            FROM memory_ledger_lineage
            WHERE guild_id=? AND lineage_type IN ('supersedes','retracts')
          )
        ORDER BY observed_at DESC,created_at DESC,entry_id DESC
        """
            % policy_clause
        ),
        tuple(params),
    ).fetchall()
    selected: dict[str, BnlSelfNameRecord] = {}
    table_names = {
        str(row[0] or "")
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    conversation_columns = (
        {
            str(row[1] or "")
            for row in conn.execute(
                "PRAGMA table_info(conversations)"
            ).fetchall()
        }
        if "conversations" in table_names
        else set()
    )

    def _source_lineage_is_current(
        decision_entry_id: str,
        *,
        decision_policy: str,
        decision_channel_id: int,
        decision_visibility: str,
    ) -> bool:
        if not {"id", "guild_id", "role"}.issubset(
            conversation_columns
        ):
            return False
        roots = conn.execute(
            """
            SELECT root.entry_id,root.source_table,root.source_row_id,
                   root.source_role,root.lifecycle_status,
                   root.channel_policy,root.channel_id,root.visibility
            FROM memory_ledger_lineage AS edge
            JOIN memory_ledger_entries AS root
              ON root.entry_id=edge.target_entry_id
             AND root.guild_id=edge.guild_id
            WHERE edge.guild_id=? AND edge.entry_id=?
              AND edge.lineage_type='derived_from'
            ORDER BY root.entry_id
            """,
            (int(guild_id), str(decision_entry_id or "")),
        ).fetchall()
        if len(roots) != 2:
            return False
        roles = {str(root[3] or "").strip().lower() for root in roots}
        if roles != {"user", "model"}:
            return False
        for (
            root_entry_id,
            source_table,
            source_row_id,
            _source_role,
            lifecycle_status,
            source_policy,
            source_channel_id,
            source_visibility,
        ) in roots:
            if (
                str(source_table or "") != "conversations"
                or str(lifecycle_status or "") != ACTIVE_LIFECYCLE
                or str(source_policy or "").strip().lower()
                != decision_policy
                or int(source_channel_id or 0)
                != int(decision_channel_id or 0)
                or str(source_visibility or "").strip().lower()
                != decision_visibility
            ):
                return False
            superseded = conn.execute(
                """
                SELECT 1
                FROM memory_ledger_lineage
                WHERE guild_id=? AND target_entry_id=?
                  AND lineage_type IN ('supersedes','retracts')
                LIMIT 1
                """,
                (int(guild_id), str(root_entry_id or "")),
            ).fetchone()
            if superseded:
                return False
            if conversation_columns:
                required = {"id", "guild_id", "role"}
                if not required.issubset(conversation_columns):
                    return False
                selected_columns = ["role"]
                if "channel_id" in conversation_columns:
                    selected_columns.append("channel_id")
                if "channel_policy" in conversation_columns:
                    selected_columns.append("channel_policy")
                conversation = conn.execute(
                    "SELECT %s FROM conversations "
                    "WHERE id=? AND guild_id=? LIMIT 1"
                    % ",".join(selected_columns),
                    (int(source_row_id or 0), int(guild_id)),
                ).fetchone()
                if not conversation:
                    return False
                if str(conversation[0] or "").strip().lower() != str(
                    _source_role or ""
                ).strip().lower():
                    return False
                offset = 1
                if "channel_id" in conversation_columns:
                    if int(conversation[offset] or 0) != int(
                        decision_channel_id or 0
                    ):
                        return False
                    offset += 1
                if "channel_policy" in conversation_columns:
                    if str(conversation[offset] or "").strip().lower() != (
                        decision_policy
                    ):
                        return False
        return True

    for (
        entry_id,
        raw_value,
        observed_at,
        channel_policy,
        channel_id,
        visibility,
    ) in rows:
        try:
            payload = json.loads(str(raw_value or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        normalized = normalize_bnl_self_name(payload.get("normalized"))
        display = re.sub(r"\s+", " ", str(payload.get("name") or "")).strip()[:48]
        decision = str(payload.get("decision") or "").strip().lower()
        if (
            not normalized
            or not display
            or decision
            not in {"accepted", "denied", "deferred", "revoked"}
            or normalized in selected
        ):
            continue
        decision_policy = str(channel_policy or "").strip().lower()
        decision_visibility = str(visibility or "").strip().lower()
        if not _source_lineage_is_current(
            str(entry_id or ""),
            decision_policy=decision_policy,
            decision_channel_id=int(channel_id or 0),
            decision_visibility=decision_visibility,
        ):
            continue
        selected[normalized] = BnlSelfNameRecord(
            normalized_name=normalized,
            display_name=display,
            decision=decision,
            entry_id=str(entry_id or ""),
            observed_at=str(observed_at or ""),
        )
    return tuple(selected[key] for key in sorted(selected))


def record_bnl_self_name_decision(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    name: str,
    decision: str,
    source_conversation_row_id: int,
    decision_conversation_row_id: int,
    source_message_id: int | None,
    channel_id: int,
    channel_name: str,
    channel_policy: str,
    route_mode: str,
    response_digest: str,
    observed_at: str = "",
) -> LedgerWriteResult:
    """Record BNL's explicit accept/deny/defer/revoke decision with lineage."""

    ensure_memory_ledger_schema(conn)
    normalized = normalize_bnl_self_name(name)
    clean_display = re.sub(r"\s+", " ", str(name or "")).strip()[:48]
    resolved_decision = str(decision or "").strip().lower()
    if (
        int(guild_id or 0) <= 0
        or not normalized
        or not clean_display
        or resolved_decision
        not in {"accepted", "denied", "deferred", "revoked"}
        or int(source_conversation_row_id or 0) <= 0
        or int(decision_conversation_row_id or 0) <= 0
    ):
        return LedgerWriteResult(
            outcome="skipped",
            reason_code="invalid_bnl_self_name_decision",
            guild_id=int(guild_id or 0),
        )

    source_row_id = int(source_conversation_row_id)
    decision_row_id = int(decision_conversation_row_id)
    source_entry_row = conn.execute(
        """
        SELECT entry_id
        FROM memory_ledger_entries
        WHERE guild_id=? AND source_table='conversations'
          AND source_row_id=? AND source_role='user'
        ORDER BY created_at,entry_id
        LIMIT 1
        """,
        (int(guild_id), str(source_row_id)),
    ).fetchone()
    if not source_entry_row or not str(source_entry_row[0] or ""):
        return LedgerWriteResult(
            outcome="skipped",
            reason_code="bnl_self_name_source_missing",
            source_table="conversations",
            source_row_id=str(source_row_id),
            source_revision=str(source_row_id),
            guild_id=int(guild_id),
        )
    decision_entry_row = conn.execute(
        """
        SELECT entry_id
        FROM memory_ledger_entries
        WHERE guild_id=? AND source_table='conversations'
          AND source_row_id=? AND source_role='model'
        ORDER BY created_at,entry_id
        LIMIT 1
        """,
        (int(guild_id), str(decision_row_id)),
    ).fetchone()
    if not decision_entry_row or not str(decision_entry_row[0] or ""):
        return LedgerWriteResult(
            outcome="skipped",
            reason_code="bnl_self_name_decision_response_missing",
            source_table="conversations",
            source_row_id=str(decision_row_id),
            source_revision=str(decision_row_id),
            guild_id=int(guild_id),
        )

    predicate_key = (
        BNL_SELF_NAME_PREDICATE_PREFIX
        + hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:20]
    )
    decision_policy = str(channel_policy or "unknown").strip().lower()
    supersession_policies = (
        tuple(sorted(BNL_SELF_NAME_PUBLIC_POLICIES))
        if decision_policy in BNL_SELF_NAME_PUBLIC_POLICIES
        else (decision_policy,)
    )
    prior_entry_ids = tuple(
        str(row[0] or "")
        for row in conn.execute(
            (
                """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE guild_id=? AND subject_key=? AND predicate_key=?
              AND channel_policy IN (%s)
              AND lifecycle_status='active'
              AND entry_id NOT IN (
                SELECT target_entry_id
                FROM memory_ledger_lineage
                WHERE guild_id=? AND lineage_type IN ('supersedes','retracts')
            )
            ORDER BY observed_at DESC,created_at DESC,entry_id DESC
            """
                % ",".join("?" for _ in supersession_policies)
            ),
            (
                int(guild_id),
                BNL_SUBJECT_KEY,
                predicate_key,
                *supersession_policies,
                int(guild_id),
            ),
        ).fetchall()
        if str(row[0] or "")
    )
    digest = re.sub(r"[^a-f0-9]", "", str(response_digest or "").lower())[:64]
    if not digest:
        digest = hashlib.sha256(
            (
                f"{normalized}\x1f{resolved_decision}\x1f"
                f"{source_row_id}\x1f{decision_row_id}"
            ).encode("utf-8")
        ).hexdigest()
    source_revision = f"{resolved_decision}:{digest}"
    value = json.dumps(
        {
            "decision": resolved_decision,
            "name": clean_display,
            "normalized": normalized,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    # The durable state belongs to BNL, not to the member who happened to
    # propose the name.  Proposal identity remains on the governed source
    # conversation and is not duplicated into this global preference.
    participants = (
        LedgerParticipant(BNL_SUBJECT_KEY, "BNL-01", "decision_owner", 0),
    )
    lineage = (
        ("derived_from", str(source_entry_row[0])),
        ("derived_from", str(decision_entry_row[0])),
        *tuple(("supersedes", entry_id) for entry_id in prior_entry_ids),
    )
    return insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=int(guild_id),
            source_table="bnl_self_name_decisions",
            source_row_id=(
                f"{source_row_id}:{decision_row_id}:{predicate_key}"
            ),
            source_revision=source_revision,
            source_event_key=f"{resolved_decision}:{normalized}",
            source_role="bnl_first_party_decision",
            entry_type=(
                "preference"
                if resolved_decision == "accepted"
                else "boundary"
                if resolved_decision in {"denied", "revoked"}
                else "open_loop"
            ),
            subject_key=BNL_SUBJECT_KEY,
            subject_display_name="BNL-01",
            predicate_key=predicate_key,
            value=value,
            source_class=SourceClass.FIRST_PARTY_RECORD,
            route_mode=str(route_mode or "normal_chat")[:80],
            channel_id=int(channel_id or 0),
            channel_name=str(channel_name or "")[:120],
            channel_policy=str(channel_policy or "unknown")[:80],
            source_message_id=(
                int(source_message_id)
                if int(source_message_id or 0) > 0
                else None
            ),
            visibility=_visibility(channel_policy),
            confidence=Confidence.HIGH,
            public_usable=False,
            observed_at=observed_at or _now(),
            source_sequence=decision_row_id,
            lifecycle_status=ACTIVE_LIFECYCLE,
            participants=participants,
            lineage=lineage,
        ),
    )


def source_revision_for(row_id: int | str, updated_at: str | None = None, event: str | None = None) -> str:
    if event:
        return f"event:{_canon(event)}"
    if updated_at:
        return f"rev:{row_id}:{_canon(updated_at)}"
    return str(row_id or "0")


def stable_entry_id(*, guild_id: int | str | None, source_table: str, source_row_id: int | str, entry_type: str, subject_key: str, predicate_key: str, source_revision: str = "") -> str:
    parts = [MEMORY_LEDGER_SCHEMA_VERSION, str(guild_id or 0), _canon(source_table), str(source_row_id), _canon(source_revision or source_row_id), _canon(entry_type), _canon(subject_key), _canon(predicate_key)]
    return "mle_" + hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:40]


@dataclass(frozen=True)
class LedgerParticipant:
    participant_key: str
    display_name: str = ""
    role: str = "participant"
    order_index: int = 0


@dataclass(frozen=True)
class LedgerEntry:
    guild_id: int
    source_table: str
    source_row_id: int | str
    source_role: str
    entry_type: str
    subject_key: str
    subject_display_name: str = ""
    predicate_key: str = "conversation"
    value: str = ""
    source_class: SourceClass = SourceClass.LEGACY_SOURCE_BLIND
    route_mode: str = "unknown"
    channel_id: int = 0
    channel_name: str = ""
    channel_policy: str = "unknown"
    source_message_id: int | None = None
    source_revision: str = ""
    source_event_key: str = ""
    visibility: Visibility = Visibility.UNKNOWN
    confidence: Confidence = Confidence.UNKNOWN
    public_usable: bool = False
    derived: bool = False
    projection: bool = False
    salience: float = 0.0
    observed_at: str = ""
    source_sequence: int | None = None
    valid_from: str = ""
    valid_until: str = ""
    freshness: str = ""
    lifecycle_status: str = ACTIVE_LIFECYCLE
    participants: tuple[LedgerParticipant, ...] = field(default_factory=tuple)
    lineage: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    @property
    def entry_id(self) -> str:
        return stable_entry_id(guild_id=self.guild_id, source_table=self.source_table, source_row_id=self.source_row_id, entry_type=self.entry_type, subject_key=self.subject_key, predicate_key=self.predicate_key, source_revision=self.source_revision)


def ensure_memory_ledger_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_entries (
            entry_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL, guild_id INTEGER NOT NULL,
            subject_key TEXT NOT NULL, subject_display_name TEXT, entry_type TEXT NOT NULL,
            predicate_key TEXT NOT NULL, normalized_value TEXT, source_class TEXT NOT NULL,
            source_table TEXT NOT NULL, source_row_id TEXT NOT NULL, source_revision TEXT DEFAULT '', source_event_key TEXT DEFAULT '',
            source_role TEXT NOT NULL, route_mode TEXT, channel_id INTEGER, channel_name TEXT, channel_policy TEXT,
            source_message_id INTEGER, visibility TEXT NOT NULL, confidence TEXT NOT NULL,
            public_usable INTEGER DEFAULT 0, derived INTEGER DEFAULT 0, projection INTEGER DEFAULT 0,
            salience REAL DEFAULT 0.0, observed_at TEXT, source_sequence INTEGER,
            valid_from TEXT, valid_until TEXT, freshness TEXT, lifecycle_status TEXT NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            UNIQUE(schema_version, guild_id, source_table, source_row_id, source_revision, entry_type, subject_key, predicate_key)
        )
    """)
    for sql in (
        "ALTER TABLE memory_ledger_entries ADD COLUMN source_revision TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_entries ADD COLUMN source_event_key TEXT DEFAULT ''",
    ):
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_lineage (
            entry_id TEXT NOT NULL, guild_id INTEGER NOT NULL DEFAULT 0, lineage_type TEXT NOT NULL, target_entry_id TEXT NOT NULL,
            created_at TEXT NOT NULL, PRIMARY KEY(entry_id, lineage_type, target_entry_id)
        )
    """)
    try:
        cur.execute("ALTER TABLE memory_ledger_lineage ADD COLUMN guild_id INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_participants (
            entry_id TEXT NOT NULL, guild_id INTEGER NOT NULL, participant_key TEXT NOT NULL,
            display_name TEXT, participant_role TEXT, order_index INTEGER DEFAULT 0, created_at TEXT NOT NULL,
            PRIMARY KEY(entry_id, participant_key, participant_role)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS memory_ledger_shadow_receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, writer TEXT NOT NULL,
            source_table TEXT NOT NULL, source_row_id TEXT NOT NULL, source_revision TEXT DEFAULT '', source_event_key TEXT DEFAULT '',
            attempted_at TEXT NOT NULL, outcome TEXT NOT NULL, reason_code TEXT NOT NULL, entry_id TEXT DEFAULT ''
        )
    """)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_candidates (
            candidate_id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            candidate_type TEXT NOT NULL,
            subject_key TEXT NOT NULL,
            subject_display_name TEXT DEFAULT '',
            predicate_key TEXT NOT NULL,
            normalized_value TEXT NOT NULL,
            value_digest TEXT NOT NULL,
            epistemic_status TEXT NOT NULL,
            uncertainty_note TEXT DEFAULT '',
            currentness TEXT NOT NULL,
            candidate_state TEXT NOT NULL,
            contradiction_key TEXT NOT NULL,
            supersedes_candidate_id TEXT DEFAULT '',
            visibility TEXT NOT NULL,
            authority_class TEXT NOT NULL,
            confidence_class TEXT NOT NULL,
            route_scope_json TEXT NOT NULL,
            participant_scope_digest TEXT NOT NULL,
            first_seen_at TEXT DEFAULT '',
            last_seen_at TEXT DEFAULT '',
            retrieval_tags_json TEXT NOT NULL,
            root_digest TEXT NOT NULL,
            independent_root_count INTEGER NOT NULL DEFAULT 0,
            derivative_root_count INTEGER NOT NULL DEFAULT 0,
            candidate_eligible INTEGER NOT NULL DEFAULT 1,
            live_eligible INTEGER NOT NULL DEFAULT 0,
            promotion_status TEXT NOT NULL DEFAULT 'unpromoted',
            invalidated_reason TEXT DEFAULT '',
            invalidated_at TEXT DEFAULT '',
            lifecycle_schema_version TEXT DEFAULT '',
            consolidation_id TEXT DEFAULT '',
            canonical_candidate_id TEXT DEFAULT '',
            supporting_candidate_count INTEGER NOT NULL DEFAULT 0,
            eligible_independent_root_count INTEGER NOT NULL DEFAULT 0,
            reinforcement_count INTEGER NOT NULL DEFAULT 0,
            duplicate_support_count INTEGER NOT NULL DEFAULT 0,
            conflict_value_count INTEGER NOT NULL DEFAULT 0,
            consolidated_authority_class TEXT NOT NULL DEFAULT 'legacy_source_blind',
            consolidated_confidence_class TEXT NOT NULL DEFAULT 'unknown',
            lifecycle_support_digest TEXT DEFAULT '',
            lifecycle_reason TEXT DEFAULT '',
            review_status TEXT NOT NULL DEFAULT 'not_evaluated',
            review_due_at TEXT DEFAULT '',
            lifecycle_evaluated_at TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(
                schema_version, guild_id, candidate_type, subject_key,
                predicate_key, contradiction_key, root_digest
            )
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_lifecycle_events (
            event_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            consolidation_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            prior_state TEXT NOT NULL,
            next_state TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            support_digest TEXT NOT NULL,
            reinforcement_count INTEGER NOT NULL DEFAULT 0,
            conflict_value_count INTEGER NOT NULL DEFAULT 0,
            review_status TEXT NOT NULL,
            occurred_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_lifecycle_roots (
            event_id TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            candidate_id TEXT NOT NULL,
            root_entry_id TEXT NOT NULL,
            evidence_identity_digest TEXT NOT NULL,
            counts_as_reinforcement INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            PRIMARY KEY(event_id, root_entry_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_roots (
            candidate_id TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            root_entry_id TEXT NOT NULL,
            root_kind TEXT NOT NULL,
            is_independent INTEGER NOT NULL DEFAULT 0,
            source_class TEXT NOT NULL,
            source_table TEXT NOT NULL,
            source_row_id TEXT NOT NULL,
            source_revision TEXT DEFAULT '',
            source_role TEXT NOT NULL,
            visibility TEXT NOT NULL,
            confidence TEXT NOT NULL,
            lifecycle_status TEXT NOT NULL,
            root_status TEXT NOT NULL,
            root_digest TEXT NOT NULL,
            lineage_path_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(candidate_id, root_entry_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_participants (
            candidate_id TEXT NOT NULL,
            guild_id INTEGER NOT NULL,
            participant_key TEXT NOT NULL,
            participant_role TEXT NOT NULL DEFAULT 'subject',
            created_at TEXT NOT NULL,
            PRIMARY KEY(candidate_id, participant_key, participant_role)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_receipts (
            receipt_id TEXT PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            candidate_id TEXT DEFAULT '',
            event_type TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            candidate_type TEXT DEFAULT '',
            root_count INTEGER NOT NULL DEFAULT 0,
            occurred_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_knowledge_backfill (
            migration_key TEXT PRIMARY KEY,
            phase TEXT NOT NULL,
            cursor_value TEXT DEFAULT '',
            completed INTEGER NOT NULL DEFAULT 0,
            counts_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS memory_ledger_conversation_motif_fences (
            guild_id INTEGER NOT NULL,
            subject_key TEXT NOT NULL,
            predicate_key TEXT NOT NULL,
            correction_entry_id TEXT DEFAULT '',
            correction_observed_at TEXT DEFAULT '',
            reason_code TEXT NOT NULL,
            fence_state TEXT NOT NULL DEFAULT 'active',
            satisfied_at TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(guild_id, subject_key, predicate_key)
        )
        """
    )
    for sql in (
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN confidence_class TEXT NOT NULL DEFAULT 'unknown'",
        "ALTER TABLE memory_ledger_knowledge_roots ADD COLUMN confidence TEXT NOT NULL DEFAULT 'unknown'",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN lifecycle_schema_version TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN consolidation_id TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN canonical_candidate_id TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN supporting_candidate_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN eligible_independent_root_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN reinforcement_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN duplicate_support_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN conflict_value_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN consolidated_authority_class TEXT NOT NULL DEFAULT 'legacy_source_blind'",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN consolidated_confidence_class TEXT NOT NULL DEFAULT 'unknown'",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN lifecycle_support_digest TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN lifecycle_reason TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN review_status TEXT NOT NULL DEFAULT 'not_evaluated'",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN review_due_at TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN lifecycle_evaluated_at TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_conversation_motif_fences ADD COLUMN fence_state TEXT NOT NULL DEFAULT 'active'",
        "ALTER TABLE memory_ledger_conversation_motif_fences ADD COLUMN satisfied_at TEXT DEFAULT ''",
    ):
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_mle_guild ON memory_ledger_entries(guild_id)",
        "CREATE INDEX IF NOT EXISTS idx_mle_subject ON memory_ledger_entries(guild_id, subject_key)",
        "CREATE INDEX IF NOT EXISTS idx_mle_type ON memory_ledger_entries(guild_id, entry_type)",
        "CREATE INDEX IF NOT EXISTS idx_mle_source ON memory_ledger_entries(guild_id, source_table, source_row_id, source_revision)",
        "CREATE INDEX IF NOT EXISTS idx_mle_lifecycle ON memory_ledger_entries(guild_id, lifecycle_status)",
        "CREATE INDEX IF NOT EXISTS idx_mle_visibility ON memory_ledger_entries(guild_id, visibility)",
        "CREATE INDEX IF NOT EXISTS idx_mle_predicate ON memory_ledger_entries(guild_id, predicate_key)",
        "CREATE INDEX IF NOT EXISTS idx_mle_observed ON memory_ledger_entries(guild_id, observed_at)",
        "CREATE INDEX IF NOT EXISTS idx_mll_guild ON memory_ledger_lineage(guild_id, lineage_type, target_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mlp_participant ON memory_ledger_participants(guild_id, participant_key, order_index)",
        "CREATE INDEX IF NOT EXISTS idx_mlr_guild ON memory_ledger_shadow_receipts(guild_id, writer, outcome, reason_code)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_guild_type ON memory_ledger_knowledge_candidates(guild_id, candidate_type, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_subject ON memory_ledger_knowledge_candidates(guild_id, subject_key, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_contradiction ON memory_ledger_knowledge_candidates(guild_id, contradiction_key, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_visibility ON memory_ledger_knowledge_candidates(guild_id, visibility, authority_class)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_consolidation ON memory_ledger_knowledge_candidates(guild_id, consolidation_id, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkc_canonical ON memory_ledger_knowledge_candidates(guild_id, canonical_candidate_id, candidate_state)",
        "CREATE INDEX IF NOT EXISTS idx_mlkr_root ON memory_ledger_knowledge_roots(guild_id, root_entry_id, root_status)",
        "CREATE INDEX IF NOT EXISTS idx_mlkp_participant ON memory_ledger_knowledge_participants(guild_id, participant_key)",
        "CREATE INDEX IF NOT EXISTS idx_mlkreceipt_event ON memory_ledger_knowledge_receipts(guild_id, event_type, reason_code)",
        "CREATE INDEX IF NOT EXISTS idx_mlkle_guild ON memory_ledger_knowledge_lifecycle_events(guild_id, next_state, reason_code)",
        "CREATE INDEX IF NOT EXISTS idx_mlklr_candidate ON memory_ledger_knowledge_lifecycle_roots(guild_id, candidate_id, counts_as_reinforcement)",
        "CREATE INDEX IF NOT EXISTS idx_mlcmf_subject ON memory_ledger_conversation_motif_fences(guild_id, subject_key, predicate_key)",
    ]:
        cur.execute(sql)
    # These triggers are versioned in place so existing production databases
    # receive lifecycle semantics instead of retaining the v1 trigger bodies.
    for trigger_name in (
        "trg_atomic_knowledge_root_delete",
        "trg_atomic_knowledge_root_change",
        "trg_atomic_knowledge_participant_delete",
        "trg_atomic_knowledge_lineage_change",
    ):
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_root_delete_v2
        AFTER DELETE ON memory_ledger_entries
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:root_deleted:' || c.candidate_id || ':' || OLD.entry_id,
            c.guild_id,c.candidate_id,'invalidated','root_deleted',
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=OLD.entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET normalized_value='',candidate_state='invalidated',
              candidate_eligible=0,live_eligible=0,
              invalidated_reason='root_deleted',
              invalidated_at=CURRENT_TIMESTAMP,
              lifecycle_reason='root_deleted',review_status='dirty',
              lifecycle_evaluated_at='',updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=OLD.entry_id
          );

          UPDATE memory_ledger_knowledge_roots
          SET lifecycle_status='deleted',root_status='deleted',
              updated_at=CURRENT_TIMESTAMP
          WHERE root_entry_id=OLD.entry_id;
        END
        """
    )
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_root_change_v2
        AFTER UPDATE OF lifecycle_status,normalized_value,public_usable,
          visibility,source_class,confidence,subject_key,channel_policy,
          route_mode
        ON memory_ledger_entries
        WHEN
          NEW.lifecycle_status IS NOT OLD.lifecycle_status
          OR NEW.normalized_value IS NOT OLD.normalized_value
          OR NEW.public_usable IS NOT OLD.public_usable
          OR NEW.visibility IS NOT OLD.visibility
          OR NEW.source_class IS NOT OLD.source_class
          OR NEW.confidence IS NOT OLD.confidence
          OR NEW.subject_key IS NOT OLD.subject_key
          OR NEW.channel_policy IS NOT OLD.channel_policy
          OR NEW.route_mode IS NOT OLD.route_mode
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:root_changed:' || c.candidate_id || ':' ||
              NEW.entry_id || ':' || NEW.lifecycle_status,
            c.guild_id,c.candidate_id,
            CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'superseded'
              WHEN NEW.lifecycle_status IN ('resolved','expired')
                THEN 'retired'
              ELSE 'invalidated'
            END,
            CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'root_superseded'
              WHEN NEW.lifecycle_status='resolved'
                THEN 'root_resolved'
              WHEN NEW.lifecycle_status='expired'
                THEN 'root_expired'
              WHEN NEW.lifecycle_status IN ('forgotten','deleted','retracted')
                THEN 'root_privacy_or_deletion'
              WHEN NEW.public_usable IS NOT OLD.public_usable
                OR NEW.visibility IS NOT OLD.visibility
                OR NEW.source_class IS NOT OLD.source_class
                OR NEW.subject_key IS NOT OLD.subject_key
                OR NEW.channel_policy IS NOT OLD.channel_policy
                OR NEW.route_mode IS NOT OLD.route_mode
                THEN 'root_privacy_or_provenance_changed'
              WHEN NEW.confidence IS NOT OLD.confidence
                THEN 'root_confidence_changed'
              ELSE 'root_changed'
            END,
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=NEW.entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET
            normalized_value=CASE
              WHEN NEW.lifecycle_status IN ('forgotten','deleted','retracted')
                OR COALESCE(NEW.normalized_value,'')=''
                OR NEW.public_usable IS NOT OLD.public_usable
                OR NEW.visibility IS NOT OLD.visibility
                OR NEW.source_class IS NOT OLD.source_class
                OR NEW.subject_key IS NOT OLD.subject_key
                OR NEW.channel_policy IS NOT OLD.channel_policy
                OR NEW.route_mode IS NOT OLD.route_mode
                THEN ''
              ELSE normalized_value
            END,
            candidate_state=CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'superseded'
              WHEN NEW.lifecycle_status IN ('resolved','expired')
                THEN 'retired'
              ELSE 'invalidated'
            END,
            candidate_eligible=0,live_eligible=0,
            invalidated_reason=CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'root_superseded'
              WHEN NEW.lifecycle_status='resolved'
                THEN 'root_resolved'
              WHEN NEW.lifecycle_status='expired'
                THEN 'root_expired'
              WHEN NEW.lifecycle_status IN ('forgotten','deleted','retracted')
                THEN 'root_privacy_or_deletion'
              WHEN NEW.public_usable IS NOT OLD.public_usable
                OR NEW.visibility IS NOT OLD.visibility
                OR NEW.source_class IS NOT OLD.source_class
                OR NEW.subject_key IS NOT OLD.subject_key
                OR NEW.channel_policy IS NOT OLD.channel_policy
                OR NEW.route_mode IS NOT OLD.route_mode
                THEN 'root_privacy_or_provenance_changed'
              WHEN NEW.confidence IS NOT OLD.confidence
                THEN 'root_confidence_changed'
              ELSE 'root_changed'
            END,
            invalidated_at=CURRENT_TIMESTAMP,
            lifecycle_reason=CASE
              WHEN NEW.lifecycle_status IN ('corrected','superseded')
                THEN 'root_superseded'
              WHEN NEW.lifecycle_status='resolved'
                THEN 'root_resolved'
              WHEN NEW.lifecycle_status='expired'
                THEN 'root_expired'
              WHEN NEW.lifecycle_status IN ('forgotten','deleted','retracted')
                THEN 'root_privacy_or_deletion'
              WHEN NEW.public_usable IS NOT OLD.public_usable
                OR NEW.visibility IS NOT OLD.visibility
                OR NEW.source_class IS NOT OLD.source_class
                OR NEW.subject_key IS NOT OLD.subject_key
                OR NEW.channel_policy IS NOT OLD.channel_policy
                OR NEW.route_mode IS NOT OLD.route_mode
                THEN 'root_privacy_or_provenance_changed'
              WHEN NEW.confidence IS NOT OLD.confidence
                THEN 'root_confidence_changed'
              ELSE 'root_changed'
            END,
            review_status='dirty',lifecycle_evaluated_at='',
            updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=NEW.entry_id
          );

          UPDATE memory_ledger_knowledge_roots
          SET lifecycle_status=NEW.lifecycle_status,
              source_class=NEW.source_class,
              visibility=NEW.visibility,
              confidence=NEW.confidence,
              root_status=CASE
                WHEN NEW.lifecycle_status IN ('active','review_only')
                  THEN 'changed'
                ELSE NEW.lifecycle_status
              END,
              updated_at=CURRENT_TIMESTAMP
          WHERE root_entry_id=NEW.entry_id;
        END
        """
    )
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_participant_delete_v2
        AFTER DELETE ON memory_ledger_participants
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:participant_deleted:' || c.candidate_id || ':' ||
              OLD.entry_id || ':' || OLD.participant_key,
            c.guild_id,c.candidate_id,'invalidated','participant_deleted',
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=OLD.entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET normalized_value='',candidate_state='invalidated',
              candidate_eligible=0,live_eligible=0,
              invalidated_reason='participant_deleted',
              invalidated_at=CURRENT_TIMESTAMP,
              lifecycle_reason='participant_deleted',review_status='dirty',
              lifecycle_evaluated_at='',updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=OLD.entry_id
          );
        END
        """
    )
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_lineage_change_v2
        AFTER INSERT ON memory_ledger_lineage
        WHEN NEW.lineage_type IN ('correction_of','supersedes','retracts')
        BEGIN
          INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
            receipt_id,guild_id,candidate_id,event_type,reason_code,
            candidate_type,root_count,occurred_at
          )
          SELECT
            'trigger:lineage:' || NEW.lineage_type || ':' ||
              c.candidate_id || ':' || NEW.entry_id,
            c.guild_id,c.candidate_id,
            CASE
              WHEN NEW.lineage_type='supersedes' THEN 'superseded'
              WHEN NEW.lineage_type='correction_of' THEN 'contested'
              ELSE 'invalidated'
            END,
            'root_' || NEW.lineage_type,
            c.candidate_type,c.independent_root_count + c.derivative_root_count,
            CURRENT_TIMESTAMP
          FROM memory_ledger_knowledge_candidates c
          JOIN memory_ledger_knowledge_roots r
            ON r.candidate_id=c.candidate_id AND r.guild_id=c.guild_id
          WHERE r.root_entry_id=NEW.target_entry_id;

          UPDATE memory_ledger_knowledge_candidates
          SET
            normalized_value=CASE
              WHEN NEW.lineage_type='retracts' THEN ''
              ELSE normalized_value
            END,
            candidate_state=CASE
              WHEN NEW.lineage_type='supersedes' THEN 'superseded'
              WHEN NEW.lineage_type='correction_of' THEN 'contested'
              ELSE 'invalidated'
            END,
            candidate_eligible=0,live_eligible=0,
            invalidated_reason='root_' || NEW.lineage_type,
            invalidated_at=CURRENT_TIMESTAMP,
            lifecycle_reason='root_' || NEW.lineage_type,
            review_status='dirty',lifecycle_evaluated_at='',
            updated_at=CURRENT_TIMESTAMP
          WHERE candidate_id IN (
            SELECT candidate_id FROM memory_ledger_knowledge_roots
            WHERE root_entry_id=NEW.target_entry_id
          );
        END
        """
    )
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_conversation_motif_fence_source_delete_v1
        AFTER DELETE ON memory_ledger_entries
        BEGIN
          UPDATE memory_ledger_knowledge_candidates
          SET normalized_value='',candidate_state='invalidated',
              candidate_eligible=0,live_eligible=0,
              invalidated_reason='correction_fence_source_deleted',
              invalidated_at=CURRENT_TIMESTAMP,
              lifecycle_reason='correction_fence_source_deleted',
              review_status='dirty',lifecycle_evaluated_at='',
              updated_at=CURRENT_TIMESTAMP
          WHERE guild_id=OLD.guild_id AND subject_key=OLD.subject_key
            AND candidate_type='topic_or_motif'
            AND predicate_key IN (
              SELECT predicate_key
              FROM memory_ledger_conversation_motif_fences
              WHERE guild_id=OLD.guild_id
                AND subject_key=OLD.subject_key
                AND correction_entry_id=OLD.entry_id
            )
            AND retrieval_tags_json LIKE '%recurring_public_conversation%';

          DELETE FROM memory_ledger_conversation_motif_fences
          WHERE guild_id=OLD.guild_id AND subject_key=OLD.subject_key
            AND correction_entry_id=OLD.entry_id;
        END
        """
    )


def record_shadow_receipt(conn: sqlite3.Connection, *, guild_id: int, writer: str, source_table: str, source_row_id: int | str, source_revision: str = "", source_event_key: str = "", outcome: str, reason_code: str, entry_id: str = "") -> None:
    ensure_memory_ledger_schema(conn)
    conn.execute(
        "INSERT INTO memory_ledger_shadow_receipts (guild_id, writer, source_table, source_row_id, source_revision, source_event_key, attempted_at, outcome, reason_code, entry_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (int(guild_id or 0), (writer or "unknown")[:80], (source_table or "unknown")[:80], str(source_row_id or ""), (source_revision or "")[:160], (source_event_key or "")[:160], _now(), outcome if outcome in OUTCOMES else "error", (reason_code or "unknown")[:120], entry_id or ""),
    )


def skipped_result(*, guild_id: int, source_table: str, source_row_id: int | str, reason_code: str, source_revision: str = "", source_event_key: str = "") -> LedgerWriteResult:
    return LedgerWriteResult(outcome="skipped", reason_code=reason_code, source_table=source_table, source_row_id=str(source_row_id), source_revision=source_revision, source_event_key=source_event_key, guild_id=int(guild_id or 0))


def insert_ledger_entry(conn: sqlite3.Connection, entry: LedgerEntry) -> LedgerWriteResult:
    if entry.entry_type not in ENTRY_TYPES:
        return LedgerWriteResult(outcome="error", reason_code="unsupported_entry_type", source_table=entry.source_table, source_row_id=str(entry.source_row_id), source_revision=entry.source_revision, source_event_key=entry.source_event_key, guild_id=entry.guild_id)
    ensure_memory_ledger_schema(conn)
    now = _now()
    cur = conn.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO memory_ledger_entries (
            entry_id, schema_version, guild_id, subject_key, subject_display_name, entry_type, predicate_key,
            normalized_value, source_class, source_table, source_row_id, source_revision, source_event_key, source_role, route_mode, channel_id,
            channel_name, channel_policy, source_message_id, visibility, confidence, public_usable, derived,
            projection, salience, observed_at, source_sequence, valid_from, valid_until, freshness,
            lifecycle_status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (entry.entry_id, MEMORY_LEDGER_SCHEMA_VERSION, entry.guild_id, entry.subject_key, entry.subject_display_name, entry.entry_type, entry.predicate_key, entry.value[:1000], entry.source_class.value, entry.source_table, str(entry.source_row_id), entry.source_revision, entry.source_event_key, entry.source_role, entry.route_mode, int(entry.channel_id or 0), entry.channel_name[:120], entry.channel_policy[:80], entry.source_message_id, entry.visibility.value, entry.confidence.value, 1 if entry.public_usable else 0, 1 if entry.derived else 0, 1 if entry.projection else 0, float(entry.salience or 0.0), entry.observed_at, entry.source_sequence, entry.valid_from, entry.valid_until, entry.freshness, entry.lifecycle_status, now, now))
    outcome = "inserted" if cur.rowcount else "deduplicated"
    if outcome == "inserted":
        for idx, p in enumerate(sorted(entry.participants, key=lambda x: (x.order_index, x.participant_key))):
            cur.execute("INSERT OR IGNORE INTO memory_ledger_participants VALUES (?, ?, ?, ?, ?, ?, ?)", (entry.entry_id, entry.guild_id, p.participant_key, p.display_name[:120], p.role[:40], idx, now))
        for lineage_type, target in entry.lineage:
            if lineage_type in LINEAGE_TYPES and target:
                cur.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES (?, ?, ?, ?, ?)", (entry.entry_id, entry.guild_id, lineage_type, target, now))
    return LedgerWriteResult(entry.entry_id, outcome, "ok" if outcome == "inserted" else "exact_source_duplicate", entry.source_table, str(entry.source_row_id), entry.source_revision, entry.source_event_key, entry.guild_id)


def _knowledge_text(value: Any, limit: int = 1000) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()[:limit]


def _knowledge_tag(value: Any) -> str:
    return re.sub(
        r"[^a-z0-9_.:-]+",
        "_",
        str(value or "").strip().lower(),
    ).strip("_")[:64]


def _knowledge_digest(*parts: Any) -> str:
    return hashlib.sha256(
        "\x1f".join(str(part or "") for part in parts).encode("utf-8")
    ).hexdigest()


def _stable_knowledge_candidate_id(
    *,
    guild_id: int,
    candidate_type: str,
    subject_key: str,
    predicate_key: str,
    contradiction_key: str,
    root_entry_ids: tuple[str, ...],
) -> str:
    digest = _knowledge_digest(
        ATOMIC_KNOWLEDGE_SCHEMA_VERSION,
        int(guild_id or 0),
        candidate_type,
        subject_key,
        predicate_key,
        contradiction_key,
        *sorted(set(root_entry_ids)),
    )
    return "mlkc_" + digest[:40]


def _knowledge_receipt_id(
    *,
    event_type: str,
    reason_code: str,
    candidate_id: str,
    candidate_type: str,
    root_entry_ids: tuple[str, ...],
    proposal_digest: str = "",
) -> str:
    return "mlkrec_" + _knowledge_digest(
        event_type,
        reason_code,
        candidate_id,
        candidate_type,
        proposal_digest,
        *sorted(set(root_entry_ids)),
    )[:40]


def _record_knowledge_receipt(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    event_type: str,
    reason_code: str,
    candidate_id: str = "",
    candidate_type: str = "",
    root_entry_ids: tuple[str, ...] = (),
    proposal_digest: str = "",
) -> None:
    ensure_memory_ledger_schema(conn)
    conn.execute(
        """
        INSERT OR IGNORE INTO memory_ledger_knowledge_receipts(
          receipt_id,guild_id,candidate_id,event_type,reason_code,
          candidate_type,root_count,occurred_at
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            _knowledge_receipt_id(
                event_type=event_type,
                reason_code=reason_code,
                candidate_id=candidate_id,
                candidate_type=candidate_type,
                root_entry_ids=root_entry_ids,
                proposal_digest=proposal_digest,
            ),
            int(guild_id or 0),
            candidate_id,
            event_type[:40],
            reason_code[:120],
            candidate_type[:80],
            len(set(root_entry_ids)),
            _now(),
        ),
    )


def record_atomic_knowledge_processing_error(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    reason_code: str = "processing_error",
    candidate_type: str = "",
    root_entry_ids: tuple[str, ...] = (),
) -> None:
    _record_knowledge_receipt(
        conn,
        guild_id=guild_id,
        event_type="error",
        reason_code=reason_code,
        candidate_type=candidate_type,
        root_entry_ids=root_entry_ids,
    )


def _reject_atomic_knowledge(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
    *,
    guild_id: int,
    reason_code: str,
    root_entry_ids: tuple[str, ...],
) -> AtomicKnowledgeResult:
    if not int(guild_id or 0) and root_entry_ids:
        placeholders = ",".join("?" for _entry_id in root_entry_ids)
        guild_rows = conn.execute(
            """
            SELECT DISTINCT guild_id
            FROM memory_ledger_entries
            WHERE entry_id IN (%s)
            """ % placeholders,
            tuple(root_entry_ids),
        ).fetchall()
        if len(guild_rows) == 1:
            guild_id = int(guild_rows[0][0] or 0)
    _record_knowledge_receipt(
        conn,
        guild_id=guild_id,
        event_type="rejected",
        reason_code=reason_code,
        candidate_type=proposal.candidate_type,
        root_entry_ids=root_entry_ids,
        proposal_digest=_knowledge_digest(
            proposal.subject_key,
            proposal.predicate_key,
            _canon(proposal.meaning),
        ),
    )
    return AtomicKnowledgeResult(
        outcome="rejected",
        reason_code=reason_code,
        candidate_type=proposal.candidate_type,
        root_count=len(set(root_entry_ids)),
    )


def _knowledge_entry_rows(
    conn: sqlite3.Connection,
    entry_ids: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    if not entry_ids:
        return {}
    placeholders = ",".join("?" for _entry_id in entry_ids)
    rows = conn.execute(
        """
        SELECT
          entry_id,guild_id,subject_key,subject_display_name,entry_type,
          predicate_key,normalized_value,source_class,source_table,
          source_row_id,source_revision,source_role,route_mode,channel_id,
          channel_name,channel_policy,visibility,confidence,public_usable,
          derived,projection,observed_at,source_sequence,lifecycle_status
        FROM memory_ledger_entries
        WHERE entry_id IN (%s)
        """ % placeholders,
        tuple(entry_ids),
    ).fetchall()
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        by_id[str(row[0])] = {
            "entry_id": str(row[0]),
            "guild_id": int(row[1] or 0),
            "subject_key": str(row[2] or ""),
            "subject_display_name": str(row[3] or ""),
            "entry_type": str(row[4] or ""),
            "predicate_key": str(row[5] or ""),
            "normalized_value": str(row[6] or ""),
            "source_class": str(row[7] or ""),
            "source_table": str(row[8] or ""),
            "source_row_id": str(row[9] or ""),
            "source_revision": str(row[10] or ""),
            "source_role": str(row[11] or ""),
            "route_mode": str(row[12] or ""),
            "channel_id": int(row[13] or 0),
            "channel_name": str(row[14] or ""),
            "channel_policy": str(row[15] or ""),
            "visibility": str(row[16] or ""),
            "confidence": str(row[17] or "unknown"),
            "public_usable": bool(row[18]),
            "derived": bool(row[19]),
            "projection": bool(row[20]),
            "observed_at": str(row[21] or ""),
            "source_sequence": int(row[22] or 0),
            "lifecycle_status": str(row[23] or ""),
        }
    if by_id:
        placeholders = ",".join("?" for _entry_id in by_id)
        for entry_id, participant_key, participant_role in conn.execute(
            """
            SELECT entry_id,participant_key,participant_role
            FROM memory_ledger_participants
            WHERE entry_id IN (%s)
            ORDER BY entry_id,order_index,participant_key
            """ % placeholders,
            tuple(by_id),
        ).fetchall():
            by_id[str(entry_id)].setdefault("participants", []).append(
                (str(participant_key or ""), str(participant_role or "participant"))
            )
    for row in by_id.values():
        row.setdefault("participants", [])
    return by_id


def _derivation_paths(
    conn: sqlite3.Connection,
    derivative_entry_id: str,
    *,
    max_depth: int = 8,
    max_nodes: int = 128,
) -> dict[str, tuple[str, ...]]:
    """Return bounded derived-from paths without treating them as authority."""
    paths: dict[str, tuple[str, ...]] = {}
    frontier: list[tuple[str, tuple[str, ...]]] = [
        (derivative_entry_id, (derivative_entry_id,))
    ]
    visited = {derivative_entry_id}
    while frontier and len(visited) <= max_nodes:
        current, path = frontier.pop(0)
        if len(path) > max_depth:
            continue
        targets = conn.execute(
            """
            SELECT target_entry_id
            FROM memory_ledger_lineage
            WHERE entry_id=? AND lineage_type='derived_from'
            ORDER BY target_entry_id
            """,
            (current,),
        ).fetchall()
        for (target_raw,) in targets:
            target = str(target_raw or "")
            if not target:
                continue
            candidate_path = (*path, target)
            paths.setdefault(target, candidate_path)
            if target not in visited:
                visited.add(target)
                frontier.append((target, candidate_path))
    return paths


def _knowledge_visibility(values: set[str]) -> tuple[str, str]:
    values = {str(value or "unknown") for value in values}
    if values.intersection(_KNOWLEDGE_RESTRICTED_VISIBILITIES):
        return "", "restricted_or_unknown_visibility"
    nonpublic = values.intersection({"internal", "private", "mod"})
    if len(nonpublic) > 1:
        return "", "ambiguous_nonpublic_visibility"
    if nonpublic:
        return next(iter(nonpublic)), ""
    if values == {Visibility.REFERENCE_CANON.value}:
        return Visibility.REFERENCE_CANON.value, ""
    if values.issubset(
        {
            Visibility.PUBLIC.value,
            Visibility.PUBLIC_SAFE.value,
            Visibility.REFERENCE_CANON.value,
        }
    ):
        return (
            Visibility.PUBLIC.value
            if values == {Visibility.PUBLIC.value}
            else Visibility.PUBLIC_SAFE.value
        ), ""
    return "", "ambiguous_visibility"


def _knowledge_route_visibility_is_explicit(entry: dict[str, Any]) -> bool:
    policy = str(entry.get("channel_policy") or "").strip()
    visibility = str(entry.get("visibility") or "").strip()
    if policy == "member_control":
        return visibility in {
            Visibility.PRIVATE.value,
            Visibility.PUBLIC_SAFE.value,
        }
    if not has_explicit_channel_policy_mapping(policy):
        return False
    return map_channel_policy_visibility(policy).value == visibility


def _knowledge_is_derivative(entry: dict[str, Any]) -> bool:
    return bool(
        entry.get("derived")
        or entry.get("projection")
        or entry.get("source_class") in KNOWLEDGE_DERIVATIVE_SOURCE_CLASSES
        or str(entry.get("source_role") or "").lower() in {"model", "assistant"}
    )


def _knowledge_operational_or_test_source(
    entry: dict[str, Any],
) -> bool:
    metadata = " ".join(
        str(entry.get(key) or "")
        for key in (
            "source_table",
            "source_role",
            "entry_type",
            "predicate_key",
            "route_mode",
            "channel_name",
            "channel_policy",
        )
    )
    if _KNOWLEDGE_TEST_OR_OPERATIONAL_SOURCE_RE.search(metadata):
        return True
    return bool(
        _KNOWLEDGE_TEST_OR_OPERATIONAL_RE.search(
            str(entry.get("normalized_value") or "")
        )
    )


def _knowledge_participant_scope(
    proposal: AtomicKnowledgeProposal,
    independent_entries: list[dict[str, Any]],
) -> tuple[tuple[str, ...], str]:
    scopes: list[tuple[str, ...]] = []
    for entry in independent_entries:
        scope = tuple(
            sorted(
                {
                    str(participant_key or "")
                    for participant_key, _role in entry.get("participants", [])
                    if str(participant_key or "")
                }
            )
        )
        scopes.append(scope)
    if scopes and any(scope != scopes[0] for scope in scopes[1:]):
        return (), "participant_scope_mismatch"
    derived_scope = scopes[0] if scopes else ()
    requested = tuple(
        sorted(
            {
                str(participant_key or "")
                for participant_key in proposal.participant_keys
                if str(participant_key or "")
            }
        )
    )
    if requested and requested != derived_scope:
        return (), "participant_scope_mismatch"
    participants = requested or derived_scope
    if proposal.subject_key.startswith("discord_user:"):
        if proposal.subject_key not in participants:
            return (), "ambiguous_subject_participant_identity"
    return participants, ""


def _parse_knowledge_time(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _knowledge_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _knowledge_consolidation_id(
    *,
    guild_id: int,
    candidate_type: str,
    subject_key: str,
    predicate_key: str,
    contradiction_key: str,
    value_digest: str,
    visibility: str,
    participant_scope_digest: str,
) -> str:
    semantic_value_digest = (
        "scope_consolidated"
        if candidate_type in KNOWLEDGE_SCOPE_CONSOLIDATED_TYPES
        else value_digest
    )
    return "mlkcon_" + _knowledge_digest(
        ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
        int(guild_id or 0),
        candidate_type,
        subject_key,
        predicate_key,
        contradiction_key,
        semantic_value_digest,
        visibility,
        participant_scope_digest,
    )[:40]


def _knowledge_evidence_identity(
    conn: sqlite3.Connection,
    entry: dict[str, Any],
) -> str:
    """Collapse exact source copies while preserving distinct source records."""
    current_id = str(entry.get("entry_id") or "")
    current = entry
    visited = {current_id}
    for _depth in range(8):
        duplicate_targets = conn.execute(
            """
            SELECT target_entry_id
            FROM memory_ledger_lineage
            WHERE entry_id=? AND lineage_type='duplicate_of'
            ORDER BY target_entry_id
            """,
            (current_id,),
        ).fetchall()
        target_id = next(
            (
                str(row[0] or "")
                for row in duplicate_targets
                if str(row[0] or "") and str(row[0] or "") not in visited
            ),
            "",
        )
        if not target_id:
            break
        target = _knowledge_entry_rows(conn, (target_id,)).get(target_id)
        if not target:
            break
        visited.add(target_id)
        current_id = target_id
        current = target
    return _knowledge_digest(
        int(current.get("guild_id") or 0),
        current.get("source_table"),
        current.get("source_row_id"),
    )


def knowledge_root_identity(
    conn: sqlite3.Connection,
    entry_id: str,
) -> str:
    """Return one content-free identity for the original source record."""
    entry = _knowledge_entry_rows(
        conn,
        (str(entry_id or "").strip(),),
    ).get(str(entry_id or "").strip())
    if not entry:
        return ""
    return _knowledge_evidence_identity(conn, entry)


def knowledge_source_root_identity(
    *,
    guild_id: int,
    source_table: str,
    source_row_id: int | str,
) -> str:
    """Build the same content-free root identity without reading an entry."""
    if not int(guild_id or 0) or not str(source_table or "").strip():
        return ""
    if not str(source_row_id or "").strip():
        return ""
    return _knowledge_digest(
        int(guild_id or 0),
        str(source_table or "").strip(),
        str(source_row_id or "").strip(),
    )


def _knowledge_occurrence_identity(
    conn: sqlite3.Connection,
    entry: dict[str, Any],
) -> str:
    """Collapse one Moment or one bounded 30-minute exchange."""
    evidence_identity = _knowledge_evidence_identity(conn, entry)
    if (
        str(entry.get("source_table") or "") != "conversations"
        or str(entry.get("source_role") or "").lower()
        not in {
            "user",
            "member_self_report",
            "member_control",
            "owner",
            "operator",
        }
    ):
        return evidence_identity
    moment_row = conn.execute(
        """
        SELECT l.target_entry_id
        FROM memory_ledger_lineage l
        JOIN memory_ledger_entries target
          ON target.entry_id=l.target_entry_id
        WHERE l.entry_id=? AND l.lineage_type='part_of_moment'
          AND target.lifecycle_status IN ('active','review_only')
        ORDER BY l.target_entry_id
        LIMIT 1
        """,
        (str(entry.get("entry_id") or ""),),
    ).fetchone()
    if moment_row and str(moment_row[0] or ""):
        return _knowledge_digest(
            "conversation_moment_occurrence",
            int(entry.get("guild_id") or 0),
            str(moment_row[0]),
        )
    scope_parts = (
        int(entry.get("guild_id") or 0),
        int(entry.get("channel_id") or 0),
        str(entry.get("channel_policy") or "unknown"),
        str(entry.get("subject_key") or ""),
    )
    observed = _parse_knowledge_time(entry.get("observed_at"))
    current_sequence = int(entry.get("source_sequence") or 0)
    if observed is None or current_sequence <= 0:
        return ""
    rows = conn.execute(
        """
        SELECT entry_id,observed_at,source_sequence
        FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=? AND source_table='conversations'
          AND source_role IN (
            'user','member_self_report','member_control','owner','operator'
          )
          AND channel_id=? AND channel_policy=?
          AND source_sequence<=?
        ORDER BY source_sequence DESC,entry_id DESC
        LIMIT ?
        """,
        (
            scope_parts[0],
            scope_parts[3],
            scope_parts[1],
            scope_parts[2],
            current_sequence,
            _CONVERSATION_OCCURRENCE_MAX_SCAN + 1,
        ),
    ).fetchall()
    current_id = str(entry.get("entry_id") or "")
    current_index = next(
        (
            index
            for index, row in enumerate(rows)
            if str(row[0] or "") == current_id
        ),
        -1,
    )
    if current_index < 0:
        return ""
    bounded_rows = rows[
        current_index : current_index + _CONVERSATION_OCCURRENCE_MAX_SCAN
    ]
    has_unscanned_prior = (
        len(rows) > current_index + _CONVERSATION_OCCURRENCE_MAX_SCAN
    )
    anchor_id = current_id
    prior_time = observed
    found_idle_boundary = False
    for row_entry_id, row_observed_at, _row_sequence in bounded_rows[1:]:
        row_time = _parse_knowledge_time(row_observed_at)
        if row_time is None or row_time > prior_time:
            return ""
        idle_seconds = (prior_time - row_time).total_seconds()
        if idle_seconds > _CONVERSATION_MOTIF_WINDOW_SECONDS:
            found_idle_boundary = True
            break
        anchor_id = str(row_entry_id or anchor_id)
        prior_time = row_time
    if has_unscanned_prior and not found_idle_boundary:
        # An unbounded continuous exchange has no safe countable identity.
        # Withhold it instead of inventing a second occurrence alongside the
        # earlier real anchor.
        return ""
    return _knowledge_digest(
        "conversation_occurrence",
        *scope_parts,
        anchor_id,
    )


def knowledge_occurrence_identity(
    conn: sqlite3.Connection,
    entry_id: str,
) -> str:
    """Return one content-free recurrence window for a human Ledger root."""
    entry = _knowledge_entry_rows(
        conn,
        (str(entry_id or "").strip(),),
    ).get(str(entry_id or "").strip())
    if not entry:
        return ""
    return _knowledge_occurrence_identity(conn, entry)


def _knowledge_review_policy(
    *,
    candidate_type: str,
    epistemic_statuses: set[str],
    currentnesses: set[str],
    last_seen_at: str,
    now: datetime,
    authoritative_single_source: bool,
) -> tuple[str, str, bool]:
    """Return review status, due time, and conservative stale retirement."""
    if authoritative_single_source or (
        candidate_type == "project_event_or_milestone"
        and currentnesses == {"historical"}
    ):
        return "not_required", "", False
    last_seen = _parse_knowledge_time(last_seen_at)
    if last_seen is None:
        return "missing_observation_time", "", False
    if (
        "inference" in epistemic_statuses
        or "contested" in epistemic_statuses
        or "uncertain" in currentnesses
    ):
        review_days, retire_days = 30, 90
    elif (
        candidate_type == "open_loop_or_question"
        or bool(currentnesses.intersection({"open", "unresolved"}))
    ):
        review_days, retire_days = 90, 180
    elif candidate_type == "topic_or_motif" and currentnesses == {"historical"}:
        return "not_required", "", False
    else:
        review_days, retire_days = 180, 0
    review_due = last_seen + timedelta(days=review_days)
    retired = bool(
        retire_days and now >= last_seen + timedelta(days=retire_days)
    )
    if retired:
        return "retired_stale", _knowledge_time(review_due), True
    return (
        "due" if now >= review_due else "current",
        _knowledge_time(review_due),
        False,
    )


def _knowledge_root_is_eligible(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    root: dict[str, Any],
) -> bool:
    entry = root.get("entry")
    if not entry or not bool(root.get("is_independent")):
        return False
    if str(root.get("root_status") or "") != "eligible":
        return False
    if str(entry.get("lifecycle_status") or "") != ACTIVE_LIFECYCLE:
        return False
    if _knowledge_is_derivative(entry):
        return False
    if _knowledge_operational_or_test_source(entry):
        return False
    if str(entry.get("subject_key") or "") != str(
        candidate.get("subject_key") or ""
    ):
        return False
    if not _knowledge_route_visibility_is_explicit(entry):
        return False
    if str(entry.get("visibility") or "") in _KNOWLEDGE_RESTRICTED_VISIBILITIES:
        return False
    if (
        str(candidate.get("visibility") or "")
        in {
            Visibility.PUBLIC.value,
            Visibility.PUBLIC_SAFE.value,
            Visibility.REFERENCE_CANON.value,
        }
        and not bool(entry.get("public_usable"))
    ):
        return False
    if conn.execute(
        """
        SELECT 1
        FROM memory_ledger_lineage
        WHERE target_entry_id=?
          AND lineage_type IN ('supersedes','retracts')
        LIMIT 1
        """,
        (entry.get("entry_id"),),
    ).fetchone():
        return False
    return True


def _knowledge_candidate_roots(
    conn: sqlite3.Connection,
    candidate: dict[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    rows = conn.execute(
        """
        SELECT root_entry_id,is_independent,root_status
        FROM memory_ledger_knowledge_roots
        WHERE candidate_id=?
        ORDER BY root_entry_id
        """,
        (candidate["candidate_id"],),
    ).fetchall()
    entry_ids = tuple(str(row[0] or "") for row in rows if str(row[0] or ""))
    entries = _knowledge_entry_rows(conn, entry_ids)
    roots: list[dict[str, Any]] = []
    derivative_count = 0
    for entry_id, is_independent, root_status in rows:
        root = {
            "root_entry_id": str(entry_id or ""),
            "is_independent": bool(is_independent),
            "root_status": str(root_status or ""),
            "entry": entries.get(str(entry_id or "")),
        }
        if not bool(is_independent):
            derivative_count += 1
        if _knowledge_root_is_eligible(
            conn,
            candidate=candidate,
            root=root,
        ):
            root["evidence_identity"] = _knowledge_evidence_identity(
                conn,
                root["entry"],
            )
            root["occurrence_identity"] = _knowledge_occurrence_identity(
                conn,
                root["entry"],
            )
            candidate_tags = set(_knowledge_retrieval_tags(candidate))
            root["reinforcement_identity"] = (
                root["occurrence_identity"]
                if "recurring_public_conversation" in candidate_tags
                else root["evidence_identity"]
            )
            roots.append(root)
    if roots:
        visibility, reason = _knowledge_visibility(
            {
                str(root["entry"].get("visibility") or "unknown")
                for root in roots
            }
        )
        if reason or visibility != str(candidate.get("visibility") or ""):
            return [], derivative_count
    return roots, derivative_count


def _knowledge_retrieval_tags(
    candidate: dict[str, Any],
) -> tuple[str, ...]:
    try:
        parsed = json.loads(
            str(candidate.get("retrieval_tags_json") or "[]")
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(
        str(tag)
        for tag in parsed
        if str(tag or "")
    )


def _record_knowledge_lifecycle_event(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    consolidation_id: str,
    prior_state: str,
    next_state: str,
    reason_code: str,
    reinforcement_count: int,
    conflict_value_count: int,
    review_status: str,
    roots: list[dict[str, Any]],
) -> None:
    support_pairs = tuple(
        sorted(
            {
                (
                    str(root.get("root_entry_id") or ""),
                    str(
                        root.get("reinforcement_identity")
                        or root.get("evidence_identity")
                        or ""
                    ),
                )
                for root in roots
                if str(root.get("root_entry_id") or "")
            }
        )
    )
    support_digest = _knowledge_digest(
        *(
            f"{root_id}:{evidence_identity}"
            for root_id, evidence_identity in support_pairs
        )
    )
    event_id = "mlkle_" + _knowledge_digest(
        ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
        consolidation_id,
        candidate["candidate_id"],
        prior_state,
        next_state,
        reason_code,
        support_digest,
        reinforcement_count,
        conflict_value_count,
        review_status,
    )[:40]
    now = _now()
    inserted = conn.execute(
        """
        INSERT OR IGNORE INTO memory_ledger_knowledge_lifecycle_events(
          event_id,guild_id,consolidation_id,candidate_id,prior_state,
          next_state,reason_code,support_digest,reinforcement_count,
          conflict_value_count,review_status,occurred_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            event_id,
            int(candidate.get("guild_id") or 0),
            consolidation_id,
            candidate["candidate_id"],
            prior_state,
            next_state,
            reason_code,
            support_digest,
            int(reinforcement_count or 0),
            int(conflict_value_count or 0),
            review_status,
            now,
        ),
    ).rowcount
    if not inserted:
        return
    representatives: dict[str, str] = {}
    for root_id, evidence_identity in support_pairs:
        representatives.setdefault(evidence_identity, root_id)
    for root_id, evidence_identity in support_pairs:
        conn.execute(
            """
            INSERT OR IGNORE INTO memory_ledger_knowledge_lifecycle_roots(
              event_id,guild_id,candidate_id,root_entry_id,
              evidence_identity_digest,counts_as_reinforcement,created_at
            ) VALUES(?,?,?,?,?,?,?)
            """,
            (
                event_id,
                int(candidate.get("guild_id") or 0),
                candidate["candidate_id"],
                root_id,
                evidence_identity,
                1 if representatives.get(evidence_identity) == root_id else 0,
                now,
            ),
        )
    event_type = (
        "promoted"
        if next_state in {"provisional", "established"}
        and prior_state != next_state
        else "reinforced"
        if prior_state == next_state
        else next_state
    )
    _record_knowledge_receipt(
        conn,
        guild_id=int(candidate.get("guild_id") or 0),
        event_type=event_type,
        reason_code=reason_code,
        candidate_id=candidate["candidate_id"],
        candidate_type=str(candidate.get("candidate_type") or ""),
        root_entry_ids=tuple(root_id for root_id, _identity in support_pairs),
        proposal_digest=support_digest,
    )


def _knowledge_scope_rows(
    conn: sqlite3.Connection,
    scope: tuple[Any, ...],
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          candidate_id,guild_id,candidate_type,subject_key,predicate_key,
          normalized_value,value_digest,epistemic_status,currentness,
          candidate_state,contradiction_key,supersedes_candidate_id,
          visibility,authority_class,confidence_class,route_scope_json,
          participant_scope_digest,first_seen_at,last_seen_at,
          retrieval_tags_json,
          candidate_eligible,invalidated_reason,lifecycle_schema_version,
          consolidation_id,canonical_candidate_id,
          supporting_candidate_count,eligible_independent_root_count,
          reinforcement_count,duplicate_support_count,
          conflict_value_count,consolidated_authority_class,
          consolidated_confidence_class,lifecycle_support_digest,
          lifecycle_reason,review_status,review_due_at,
          lifecycle_evaluated_at
        FROM memory_ledger_knowledge_candidates
        WHERE guild_id=? AND candidate_type=? AND subject_key=?
          AND predicate_key=? AND contradiction_key=? AND visibility=?
          AND participant_scope_digest=?
        ORDER BY candidate_id
        """,
        scope,
    ).fetchall()
    keys = (
        "candidate_id",
        "guild_id",
        "candidate_type",
        "subject_key",
        "predicate_key",
        "normalized_value",
        "value_digest",
        "epistemic_status",
        "currentness",
        "candidate_state",
        "contradiction_key",
        "supersedes_candidate_id",
        "visibility",
        "authority_class",
        "confidence_class",
        "route_scope_json",
        "participant_scope_digest",
        "first_seen_at",
        "last_seen_at",
        "retrieval_tags_json",
        "candidate_eligible",
        "invalidated_reason",
        "lifecycle_schema_version",
        "consolidation_id",
        "canonical_candidate_id",
        "supporting_candidate_count",
        "eligible_independent_root_count",
        "reinforcement_count",
        "duplicate_support_count",
        "conflict_value_count",
        "consolidated_authority_class",
        "consolidated_confidence_class",
        "lifecycle_support_digest",
        "lifecycle_reason",
        "review_status",
        "review_due_at",
        "lifecycle_evaluated_at",
    )
    return [dict(zip(keys, row)) for row in rows]


def _reconcile_atomic_knowledge_scope(
    conn: sqlite3.Connection,
    scope: tuple[Any, ...],
    *,
    now: datetime,
) -> dict[str, int]:
    candidates = _knowledge_scope_rows(conn, scope)
    if not candidates:
        return {"scopes": 0, "candidates": 0, "state_changes": 0}
    by_id = {candidate["candidate_id"]: candidate for candidate in candidates}
    explicitly_superseded = {
        str(candidate.get("supersedes_candidate_id") or "")
        for candidate in candidates
        if str(candidate.get("supersedes_candidate_id") or "") in by_id
        and candidate.get("candidate_state")
        not in KNOWLEDGE_TERMINAL_CANDIDATE_STATES
    }
    candidate_roots: dict[str, list[dict[str, Any]]] = {}
    candidate_derivatives: dict[str, int] = {}
    for candidate in candidates:
        if (
            candidate.get("candidate_state")
            in KNOWLEDGE_TERMINAL_CANDIDATE_STATES
            or candidate["candidate_id"] in explicitly_superseded
            or not str(candidate.get("normalized_value") or "")
        ):
            candidate_roots[candidate["candidate_id"]] = []
            candidate_derivatives[candidate["candidate_id"]] = 0
            continue
        roots, derivative_count = _knowledge_candidate_roots(conn, candidate)
        candidate_roots[candidate["candidate_id"]] = roots
        candidate_derivatives[candidate["candidate_id"]] = derivative_count

    groups: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        consolidation_id = _knowledge_consolidation_id(
            guild_id=int(candidate.get("guild_id") or 0),
            candidate_type=str(candidate.get("candidate_type") or ""),
            subject_key=str(candidate.get("subject_key") or ""),
            predicate_key=str(candidate.get("predicate_key") or ""),
            contradiction_key=str(candidate.get("contradiction_key") or ""),
            value_digest=str(candidate.get("value_digest") or ""),
            visibility=str(candidate.get("visibility") or ""),
            participant_scope_digest=str(
                candidate.get("participant_scope_digest") or ""
            ),
        )
        group = groups.setdefault(
            consolidation_id,
            {
                "candidates": [],
                "roots": {},
                "evidence": {},
                "derivative_count": 0,
            },
        )
        group["candidates"].append(candidate)
        roots = candidate_roots.get(candidate["candidate_id"], [])
        if roots:
            group["derivative_count"] += int(
                candidate_derivatives.get(candidate["candidate_id"], 0)
            )
        for root in roots:
            root_id = str(root.get("root_entry_id") or "")
            evidence_identity = str(
                root.get("reinforcement_identity")
                or root.get("evidence_identity")
                or ""
            )
            group["roots"][root_id] = root
            group["evidence"].setdefault(evidence_identity, set()).add(root_id)

    for consolidation_id, group in groups.items():
        group["recurring_public_conversation"] = any(
            "recurring_public_conversation"
            in set(_knowledge_retrieval_tags(candidate))
            for candidate in group["candidates"]
        )
        motif_fence = (
            _conversation_motif_fence_row(
                conn,
                guild_id=int(group["candidates"][0].get("guild_id") or 0),
                subject_key=str(
                    group["candidates"][0].get("subject_key") or ""
                ),
                predicate_key=str(
                    group["candidates"][0].get("predicate_key") or ""
                ),
            )
            if group["recurring_public_conversation"]
            else {}
        )
        group["conversation_motif_correction_fenced"] = bool(
            motif_fence
            and str(motif_fence.get("fence_state") or "active")
            == "active"
        )
        active_ids = sorted(
            candidate["candidate_id"]
            for candidate in group["candidates"]
            if candidate_roots.get(candidate["candidate_id"])
            and candidate["candidate_id"] not in explicitly_superseded
        )
        group["canonical_candidate_id"] = (
            active_ids[0]
            if active_ids
            else min(
                candidate["candidate_id"]
                for candidate in group["candidates"]
            )
        )
        authorities = {
            str(root["entry"].get("source_class") or "")
            for root in group["roots"].values()
        }
        group["authority_class"] = (
            min(
                authorities,
                key=lambda value: (
                    _KNOWLEDGE_AUTHORITY_RANK.get(value, -1),
                    value,
                ),
            )
            if authorities
            else SourceClass.LEGACY_SOURCE_BLIND.value
        )
        confidences = {
            str(
                root["entry"].get("confidence")
                or Confidence.UNKNOWN.value
            )
            for root in group["roots"].values()
        }
        group["confidence_class"] = (
            min(
                confidences,
                key=lambda value: (
                    _KNOWLEDGE_CONFIDENCE_RANK.get(value, 0),
                    value,
                ),
            )
            if confidences
            else Confidence.UNKNOWN.value
        )
        group["support_digest"] = _knowledge_digest(
            *(
                f"{root_id}:"
                f"{root.get('evidence_identity') or ''}:"
                f"{root.get('reinforcement_identity') or ''}"
                for root_id, root in sorted(group["roots"].items())
            )
        )
        root_ids = tuple(sorted(group["roots"]))
        root_lineage_correction = bool(
            root_ids
            and conn.execute(
                """
                SELECT 1
                FROM memory_ledger_lineage
                WHERE entry_id IN (%s)
                  AND lineage_type IN ('correction_of','supersedes')
                LIMIT 1
                """ % ",".join("?" for _entry_id in root_ids),
                root_ids,
            ).fetchone()
        )
        group["explicit_correction_source"] = root_lineage_correction or any(
            bool(str(candidate.get("supersedes_candidate_id") or ""))
            and bool(candidate_roots.get(candidate["candidate_id"]))
            for candidate in group["candidates"]
        )
        group["authoritative_single_source"] = (
            len(group["evidence"]) == 1
            and (
                group["authority_class"]
                in {
                    SourceClass.APPROVED_CANON.value,
                    SourceClass.OWNER_CORRECTION.value,
                }
                or bool(group["explicit_correction_source"])
            )
        )
        last_seen = max(
            (
                str(candidate.get("last_seen_at") or "")
                for candidate in group["candidates"]
            ),
            default="",
        )
        review_status, review_due_at, stale_retired = (
            _knowledge_review_policy(
                candidate_type=str(
                    group["candidates"][0].get("candidate_type") or ""
                ),
                epistemic_statuses={
                    str(candidate.get("epistemic_status") or "")
                    for candidate in group["candidates"]
                },
                currentnesses={
                    str(candidate.get("currentness") or "")
                    for candidate in group["candidates"]
                },
                last_seen_at=last_seen,
                now=now,
                authoritative_single_source=bool(
                    group["authoritative_single_source"]
                ),
            )
        )
        group["review_status"] = review_status
        group["review_due_at"] = review_due_at
        group["stale_retired"] = stale_retired

    active_group_ids = {
        consolidation_id
        for consolidation_id, group in groups.items()
        if group["roots"] and not group["stale_retired"]
    }
    conflict_value_count = len(active_group_ids)
    changes = 0
    evaluated_at = _knowledge_time(now)
    for consolidation_id, group in groups.items():
        roots = list(group["roots"].values())
        root_count = len(group["roots"])
        reinforcement_count = len(group["evidence"])
        duplicate_support_count = max(0, root_count - reinforcement_count)
        supporting_candidate_count = sum(
            1
            for candidate in group["candidates"]
            if candidate_roots.get(candidate["candidate_id"])
        )
        forced_contested = any(
            str(candidate.get("epistemic_status") or "") == "contested"
            or str(candidate.get("invalidated_reason") or "")
            == "same_roots_meaning_mismatch"
            for candidate in group["candidates"]
        )
        for candidate in group["candidates"]:
            candidate_id = candidate["candidate_id"]
            prior_state = str(candidate.get("candidate_state") or "candidate")
            valid_roots = candidate_roots.get(candidate_id, [])
            reason = ""
            if prior_state == "invalidated":
                next_state = "invalidated"
                reason = str(
                    candidate.get("invalidated_reason")
                    or candidate.get("lifecycle_reason")
                    or "source_invalidated"
                )
            elif prior_state == "superseded" or candidate_id in explicitly_superseded:
                next_state = "superseded"
                reason = str(
                    candidate.get("invalidated_reason")
                    or "explicit_candidate_supersession"
                )
            elif prior_state == "retired":
                next_state = "retired"
                reason = str(
                    candidate.get("invalidated_reason")
                    or candidate.get("lifecycle_reason")
                    or "retired"
                )
            elif bool(group["conversation_motif_correction_fenced"]):
                next_state = "contested"
                reason = "conversation_motif_correction_fence"
            elif not valid_roots:
                root_lifecycles = {
                    str(row[0] or "")
                    for row in conn.execute(
                        """
                        SELECT lifecycle_status
                        FROM memory_ledger_knowledge_roots
                        WHERE candidate_id=? AND is_independent=1
                        """,
                        (candidate_id,),
                    ).fetchall()
                }
                if root_lifecycles and root_lifecycles.issubset(
                    {"resolved", "expired"}
                ):
                    next_state = "retired"
                    reason = "all_independent_roots_resolved_or_expired"
                else:
                    next_state = "invalidated"
                    reason = "no_eligible_independent_roots"
            elif bool(group["stale_retired"]):
                next_state = "retired"
                reason = "stale_uncertain_or_open_knowledge"
            elif (
                bool(group["recurring_public_conversation"])
                and reinforcement_count < 2
            ):
                next_state = "invalidated"
                reason = "recurring_conversation_reinforcement_lost"
            elif conflict_value_count > 1:
                next_state = "contested"
                reason = "unresolved_contradiction"
            elif forced_contested:
                next_state = "contested"
                reason = (
                    "same_roots_meaning_mismatch"
                    if str(candidate.get("invalidated_reason") or "")
                    == "same_roots_meaning_mismatch"
                    else "explicitly_contested_evidence"
                )
            elif (
                bool(group["authoritative_single_source"])
                or (
                    reinforcement_count >= 2
                    and not {
                        str(item.get("epistemic_status") or "")
                        for item in group["candidates"]
                    }.intersection({"inference", "contested"})
                    and not {
                        str(item.get("currentness") or "")
                        for item in group["candidates"]
                    }.intersection({"uncertain", "unresolved"})
                )
            ):
                next_state = "established"
                reason = (
                    "explicit_correction_established"
                    if bool(group["explicit_correction_source"])
                    else "authoritative_source_established"
                    if bool(group["authoritative_single_source"])
                    else "independent_reinforcement_established"
                )
            elif str(candidate.get("epistemic_status") or "") == "inference":
                next_state = "candidate"
                reason = "inference_requires_review"
            else:
                next_state = "provisional"
                reason = "single_independent_source_provisional"
            candidate_eligible = int(
                next_state in KNOWLEDGE_ACTIVE_CANDIDATE_STATES
            )
            next_invalidated_reason = (
                reason
                if next_state
                in {"contested", "superseded", "retired", "invalidated"}
                else ""
            )
            next_invalidated_at = (
                str(candidate.get("lifecycle_evaluated_at") or evaluated_at)
                if next_invalidated_reason
                else ""
            )
            new_values = (
                next_state,
                candidate_eligible,
                ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
                consolidation_id,
                group["canonical_candidate_id"],
                supporting_candidate_count,
                root_count,
                reinforcement_count,
                duplicate_support_count,
                conflict_value_count,
                group["authority_class"],
                group["confidence_class"],
                group["support_digest"],
                reason,
                group["review_status"],
                group["review_due_at"],
            )
            old_values = (
                prior_state,
                int(candidate.get("candidate_eligible") or 0),
                str(candidate.get("lifecycle_schema_version") or ""),
                str(candidate.get("consolidation_id") or ""),
                str(candidate.get("canonical_candidate_id") or ""),
                int(candidate.get("supporting_candidate_count") or 0),
                int(candidate.get("eligible_independent_root_count") or 0),
                int(candidate.get("reinforcement_count") or 0),
                int(candidate.get("duplicate_support_count") or 0),
                int(candidate.get("conflict_value_count") or 0),
                str(
                    candidate.get("consolidated_authority_class") or ""
                ),
                str(
                    candidate.get("consolidated_confidence_class") or ""
                ),
                str(candidate.get("lifecycle_support_digest") or ""),
                str(candidate.get("lifecycle_reason") or ""),
                str(candidate.get("review_status") or ""),
                str(candidate.get("review_due_at") or ""),
            )
            if new_values != old_values:
                _record_knowledge_lifecycle_event(
                    conn,
                    candidate=candidate,
                    consolidation_id=consolidation_id,
                    prior_state=prior_state,
                    next_state=next_state,
                    reason_code=reason,
                    reinforcement_count=reinforcement_count,
                    conflict_value_count=conflict_value_count,
                    review_status=str(group["review_status"]),
                    roots=roots,
                )
                conn.execute(
                    """
                    UPDATE memory_ledger_knowledge_candidates
                    SET candidate_state=?,candidate_eligible=?,live_eligible=0,
                        lifecycle_schema_version=?,consolidation_id=?,
                        canonical_candidate_id=?,
                        supporting_candidate_count=?,
                        eligible_independent_root_count=?,
                        reinforcement_count=?,duplicate_support_count=?,
                        conflict_value_count=?,
                        consolidated_authority_class=?,
                        consolidated_confidence_class=?,
                        lifecycle_support_digest=?,lifecycle_reason=?,
                        review_status=?,review_due_at=?,
                        lifecycle_evaluated_at=?,invalidated_reason=?,
                        invalidated_at=?,updated_at=?
                    WHERE candidate_id=?
                    """,
                    (
                        next_state,
                        candidate_eligible,
                        ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION,
                        consolidation_id,
                        group["canonical_candidate_id"],
                        supporting_candidate_count,
                        root_count,
                        reinforcement_count,
                        duplicate_support_count,
                        conflict_value_count,
                        group["authority_class"],
                        group["confidence_class"],
                        group["support_digest"],
                        reason,
                        group["review_status"],
                        group["review_due_at"],
                        evaluated_at,
                        next_invalidated_reason,
                        next_invalidated_at,
                        evaluated_at,
                        candidate_id,
                    ),
                )
                changes += 1
            else:
                conn.execute(
                    """
                    UPDATE memory_ledger_knowledge_candidates
                    SET live_eligible=0,lifecycle_evaluated_at=?
                    WHERE candidate_id=?
                      AND (
                        live_eligible<>0
                        OR COALESCE(lifecycle_evaluated_at,'')=''
                      )
                    """,
                    (evaluated_at, candidate_id),
                )
    return {
        "scopes": 1,
        "candidates": len(candidates),
        "state_changes": changes,
    }


def reconcile_atomic_knowledge_lifecycle(
    conn: sqlite3.Connection,
    *,
    candidate_ids: tuple[str, ...] = (),
    guild_id: int | None = None,
    now: str | datetime | None = None,
) -> dict[str, int]:
    """Deterministically consolidate and evaluate selected contradiction scopes."""
    ensure_memory_ledger_schema(conn)
    if isinstance(now, datetime):
        evaluated_now = now
    else:
        evaluated_now = _parse_knowledge_time(now) if now else None
    evaluated_now = evaluated_now or datetime.now(timezone.utc)
    if evaluated_now.tzinfo is None:
        evaluated_now = evaluated_now.replace(tzinfo=timezone.utc)
    scopes: set[tuple[Any, ...]] = set()
    if candidate_ids:
        clean_ids = tuple(
            sorted(
                {
                    str(candidate_id or "")
                    for candidate_id in candidate_ids
                    if str(candidate_id or "")
                }
            )
        )
        if clean_ids:
            placeholders = ",".join("?" for _candidate_id in clean_ids)
            rows = conn.execute(
                """
                SELECT DISTINCT
                  guild_id,candidate_type,subject_key,predicate_key,
                  contradiction_key,visibility,participant_scope_digest
                FROM memory_ledger_knowledge_candidates
                WHERE candidate_id IN (%s)
                """ % placeholders,
                clean_ids,
            ).fetchall()
            scopes.update(tuple(row) for row in rows)
    else:
        if guild_id is None:
            rows = conn.execute(
                """
                SELECT DISTINCT
                  guild_id,candidate_type,subject_key,predicate_key,
                  contradiction_key,visibility,participant_scope_digest
                FROM memory_ledger_knowledge_candidates
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT DISTINCT
                  guild_id,candidate_type,subject_key,predicate_key,
                  contradiction_key,visibility,participant_scope_digest
                FROM memory_ledger_knowledge_candidates
                WHERE guild_id=?
                """,
                (int(guild_id or 0),),
            ).fetchall()
        scopes.update(tuple(row) for row in rows)
    report = {"scopes": 0, "candidates": 0, "state_changes": 0}
    for scope in sorted(scopes):
        result = _reconcile_atomic_knowledge_scope(
            conn,
            scope,
            now=evaluated_now.astimezone(timezone.utc),
        )
        for key in report:
            report[key] += int(result.get(key, 0) or 0)
    return report


def reconcile_atomic_knowledge_lifecycle_for_roots(
    conn: sqlite3.Connection,
    *,
    root_entry_ids: tuple[str, ...],
    now: str | datetime | None = None,
) -> dict[str, int]:
    """Re-evaluate every contradiction scope touched by exact Ledger roots."""
    clean_ids = tuple(
        sorted(
            {
                str(entry_id or "")
                for entry_id in root_entry_ids
                if str(entry_id or "")
            }
        )
    )
    if not clean_ids:
        return {"scopes": 0, "candidates": 0, "state_changes": 0}
    ensure_memory_ledger_schema(conn)
    placeholders = ",".join("?" for _entry_id in clean_ids)
    candidate_ids = tuple(
        str(row[0])
        for row in conn.execute(
            """
            SELECT DISTINCT candidate_id
            FROM memory_ledger_knowledge_roots
            WHERE root_entry_id IN (%s)
            ORDER BY candidate_id
            """ % placeholders,
            clean_ids,
        ).fetchall()
    )
    if not candidate_ids:
        return {"scopes": 0, "candidates": 0, "state_changes": 0}
    return reconcile_atomic_knowledge_lifecycle(
        conn,
        candidate_ids=candidate_ids,
        now=now,
    )


def _replace_atomic_candidate_current_roots(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    guild_id: int,
    subject_key: str,
    participants: tuple[str, ...],
    all_ids: tuple[str, ...],
    independent_ids: tuple[str, ...],
    entries: dict[str, dict[str, Any]],
    derivation_paths: dict[str, dict[str, tuple[str, ...]]],
    now: str,
) -> None:
    """Replace only a candidate's live root/participant associations.

    Lifecycle events, lifecycle roots, and receipts are append-only audit
    history and are intentionally untouched by a bounded motif refresh.
    """
    conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_roots
        WHERE candidate_id=?
        """,
        (str(candidate_id or ""),),
    )
    conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_participants
        WHERE candidate_id=?
        """,
        (str(candidate_id or ""),),
    )
    for participant_key in participants:
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_participants(
              candidate_id,guild_id,participant_key,participant_role,created_at
            ) VALUES(?,?,?,?,?)
            """,
            (
                candidate_id,
                guild_id,
                participant_key,
                "subject" if participant_key == subject_key else "participant",
                now,
            ),
        )
    independent_set = set(independent_ids)
    for entry_id in all_ids:
        entry = entries[entry_id]
        independent = entry_id in independent_set
        if independent:
            root_kind = (
                "human_source"
                if str(entry.get("source_role") or "").lower()
                in {
                    "user",
                    "member_self_report",
                    "member_control",
                    "owner",
                    "operator",
                }
                else "source_record"
            )
            paths: list[list[str]] = [[entry_id]]
        else:
            root_kind = "bnl_derivative"
            paths = [
                list(path)
                for path in derivation_paths.get(entry_id, {}).values()
                if path[-1] in independent_set
            ]
        entry_digest = _knowledge_digest(
            entry_id,
            entry.get("source_revision"),
            _canon(entry.get("normalized_value")),
        )
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_roots(
              candidate_id,guild_id,root_entry_id,root_kind,is_independent,
              source_class,source_table,source_row_id,source_revision,
              source_role,visibility,confidence,lifecycle_status,root_status,
              root_digest,lineage_path_json,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                candidate_id,
                guild_id,
                entry_id,
                root_kind,
                1 if independent else 0,
                entry.get("source_class"),
                entry.get("source_table"),
                entry.get("source_row_id"),
                entry.get("source_revision"),
                entry.get("source_role"),
                entry.get("visibility"),
                entry.get("confidence"),
                entry.get("lifecycle_status"),
                "eligible",
                entry_digest,
                json.dumps(paths, sort_keys=True, separators=(",", ":")),
                now,
                now,
            ),
        )


def _recurring_motif_candidate_id(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_key: str,
    contradiction_key: str,
    visibility: str,
    participant_scope_digest: str,
    root_digest: str,
    fallback_candidate_id: str,
) -> str:
    """Reuse one nonterminal recurring-motif identity across root refreshes."""
    rows = conn.execute(
        """
        SELECT candidate_id,candidate_state,invalidated_reason,root_digest,
               canonical_candidate_id,created_at
        FROM memory_ledger_knowledge_candidates
        WHERE guild_id=? AND candidate_type='topic_or_motif'
          AND subject_key=? AND predicate_key=? AND contradiction_key=?
          AND visibility=? AND participant_scope_digest=?
          AND retrieval_tags_json LIKE '%recurring_public_conversation%'
        ORDER BY created_at,candidate_id
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            str(predicate_key or ""),
            str(contradiction_key or ""),
            str(visibility or ""),
            str(participant_scope_digest or ""),
        ),
    ).fetchall()
    # An exact legacy row must win even if terminal; otherwise the candidate
    # table's root-digest uniqueness contract would be violated.  Terminal
    # replay stays terminal and therefore fails closed.
    exact = next(
        (
            str(row[0] or "")
            for row in rows
            if str(row[3] or "") == str(root_digest or "")
            and str(row[0] or "")
        ),
        "",
    )
    if exact:
        return exact
    refreshable = [
        row
        for row in rows
        if (
            str(row[1] or "")
            in {"candidate", "provisional", "established"}
            or (
                str(row[1] or "") == "contested"
                and str(row[2] or "").startswith(
                    "conversation_motif_correction"
                )
            )
        )
    ]
    if not refreshable:
        return fallback_candidate_id
    refreshable.sort(
        key=lambda row: (
            0 if str(row[4] or "") == str(row[0] or "") else 1,
            str(row[5] or ""),
            str(row[0] or ""),
        )
    )
    return str(refreshable[0][0] or fallback_candidate_id)


def form_atomic_knowledge_candidate(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
) -> AtomicKnowledgeResult:
    """Atomically form one candidate without owning the caller transaction."""
    ensure_memory_ledger_schema(conn)
    savepoint = f"atomic_knowledge_{id(proposal):x}"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        result = _form_atomic_knowledge_candidate_impl(conn, proposal)
    except Exception:
        conn.execute(f"ROLLBACK TO {savepoint}")
        conn.execute(f"RELEASE {savepoint}")
        raise
    conn.execute(f"RELEASE {savepoint}")
    return result


def _form_atomic_knowledge_candidate_impl(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
) -> AtomicKnowledgeResult:
    """Create one unpromoted candidate from exact revalidated Ledger roots."""
    candidate_type = _canon(proposal.candidate_type)
    subject_key = str(proposal.subject_key or "").strip()
    predicate_key = _knowledge_tag(proposal.predicate_key)
    meaning = _knowledge_text(proposal.meaning)
    independent_ids = tuple(
        sorted(
            {
                str(entry_id or "").strip()
                for entry_id in proposal.root_entry_ids
                if str(entry_id or "").strip()
            }
        )
    )
    derivative_ids = tuple(
        sorted(
            {
                str(entry_id or "").strip()
                for entry_id in proposal.derivative_entry_ids
                if str(entry_id or "").strip()
            }
        )
    )
    all_ids = tuple(sorted(set(independent_ids + derivative_ids)))
    guild_hint = 0
    if candidate_type not in KNOWLEDGE_CANDIDATE_TYPES:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="unsupported_candidate_type",
            root_entry_ids=all_ids,
        )
    if (
        not subject_key
        or subject_key == "unknown"
        or subject_key.startswith("moment:")
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_subject_identity",
            root_entry_ids=all_ids,
        )
    if not predicate_key or not meaning:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="missing_candidate_meaning",
            root_entry_ids=all_ids,
        )
    if not independent_ids:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="derivative_only_no_independent_root",
            root_entry_ids=all_ids,
        )
    if len(independent_ids) > 16 or len(derivative_ids) > 4:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="root_set_too_large",
            root_entry_ids=all_ids,
        )
    if set(independent_ids).intersection(derivative_ids):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_root_role",
            root_entry_ids=all_ids,
        )
    if proposal.epistemic_status not in KNOWLEDGE_EPISTEMIC_STATUSES:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_epistemic_status",
            root_entry_ids=all_ids,
        )
    if proposal.currentness not in KNOWLEDGE_CURRENTNESS:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="ambiguous_currentness",
            root_entry_ids=all_ids,
        )
    if (
        candidate_type == "inference_or_contested_claim"
        and proposal.epistemic_status not in {"inference", "contested"}
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="inference_label_required",
            root_entry_ids=all_ids,
        )

    entries = _knowledge_entry_rows(conn, all_ids)
    if len(entries) != len(all_ids):
        present_guilds = {
            int(entry.get("guild_id") or 0) for entry in entries.values()
        }
        guild_hint = next(iter(present_guilds)) if len(present_guilds) == 1 else 0
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="missing_or_orphaned_root",
            root_entry_ids=all_ids,
        )
    guilds = {int(entry.get("guild_id") or 0) for entry in entries.values()}
    if len(guilds) != 1 or not next(iter(guilds)):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=0,
            reason_code="cross_guild_or_ambiguous_provenance",
            root_entry_ids=all_ids,
        )
    guild_id = next(iter(guilds))
    guild_hint = guild_id
    independent_entries = [entries[entry_id] for entry_id in independent_ids]
    derivative_entries = [entries[entry_id] for entry_id in derivative_ids]

    if any(
        entry.get("source_class") == SourceClass.LEGACY_SOURCE_BLIND.value
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="source_blind_provenance",
            root_entry_ids=all_ids,
        )
    if any(_knowledge_is_derivative(entry) for entry in independent_entries):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="derivative_misclassified_as_independent",
            root_entry_ids=all_ids,
        )
    if any(not _knowledge_is_derivative(entry) for entry in derivative_entries):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_derivative_provenance",
            root_entry_ids=all_ids,
        )
    if any(
        _knowledge_operational_or_test_source(entry)
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="operational_or_rehearsal_source_excluded",
            root_entry_ids=all_ids,
        )
    if any(
        entry.get("lifecycle_status") != ACTIVE_LIFECYCLE
        for entry in independent_entries
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ineligible_root_lifecycle",
            root_entry_ids=all_ids,
        )
    if any(
        entry.get("lifecycle_status")
        not in {ACTIVE_LIFECYCLE, REVIEW_ONLY_LIFECYCLE}
        for entry in derivative_entries
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ineligible_derivative_lifecycle",
            root_entry_ids=all_ids,
        )
    if conn.execute(
        """
        SELECT 1
        FROM memory_ledger_lineage
        WHERE target_entry_id IN (%s)
          AND lineage_type IN ('supersedes','retracts')
        LIMIT 1
        """ % ",".join("?" for _entry_id in independent_ids),
        independent_ids,
    ).fetchone():
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="root_superseded_or_retracted",
            root_entry_ids=all_ids,
        )
    if any(
        not str(entry.get("route_mode") or "").strip()
        or str(entry.get("route_mode") or "").strip() == "unknown"
        or not str(entry.get("channel_policy") or "").strip()
        or str(entry.get("channel_policy") or "").strip() == "unknown"
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_route_or_policy",
            root_entry_ids=all_ids,
        )
    if any(
        not _knowledge_route_visibility_is_explicit(entry)
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="route_visibility_contract_mismatch",
            root_entry_ids=all_ids,
        )

    root_subjects = {
        str(entry.get("subject_key") or "") for entry in independent_entries
    }
    if root_subjects != {subject_key}:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="subject_root_isolation_failure",
            root_entry_ids=all_ids,
        )
    participants, participant_reason = _knowledge_participant_scope(
        proposal,
        independent_entries,
    )
    if participant_reason:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code=participant_reason,
            root_entry_ids=all_ids,
        )
    for derivative in derivative_entries:
        derivative_participants = {
            participant_key
            for participant_key, _role in derivative.get("participants", [])
            if participant_key
        }
        if derivative_participants and subject_key not in derivative_participants:
            return _reject_atomic_knowledge(
                conn,
                proposal,
                guild_id=guild_id,
                reason_code="derivative_participant_isolation_failure",
                root_entry_ids=all_ids,
            )

    derivation_paths: dict[str, dict[str, tuple[str, ...]]] = {}
    for derivative_id in derivative_ids:
        paths = _derivation_paths(conn, derivative_id)
        derivation_paths[derivative_id] = paths
        if any(root_id not in paths for root_id in independent_ids):
            return _reject_atomic_knowledge(
                conn,
                proposal,
                guild_id=guild_id,
                reason_code="derivative_lineage_incomplete",
                root_entry_ids=all_ids,
            )

    visibility, visibility_reason = _knowledge_visibility(
        {str(entry.get("visibility") or "unknown") for entry in entries.values()}
    )
    if visibility_reason:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code=visibility_reason,
            root_entry_ids=all_ids,
        )
    if (
        visibility
        in {
            Visibility.PUBLIC.value,
            Visibility.PUBLIC_SAFE.value,
            Visibility.REFERENCE_CANON.value,
        }
        and any(not entry.get("public_usable") for entry in independent_entries)
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="public_source_not_usable",
            root_entry_ids=all_ids,
        )
    if any(
        entry.get("public_usable")
        and str(entry.get("visibility") or "")
        not in {
            Visibility.PUBLIC.value,
            Visibility.PUBLIC_SAFE.value,
            Visibility.REFERENCE_CANON.value,
        }
        for entry in entries.values()
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="nonpublic_source_marked_public_usable",
            root_entry_ids=all_ids,
        )

    authority_values = {
        str(entry.get("source_class") or "") for entry in independent_entries
    }
    if any(value not in _KNOWLEDGE_AUTHORITY_RANK for value in authority_values):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_authority",
            root_entry_ids=all_ids,
        )
    authority_class = min(
        authority_values,
        key=lambda value: (_KNOWLEDGE_AUTHORITY_RANK[value], value),
    )
    confidence_values = {
        str(entry.get("confidence") or Confidence.UNKNOWN.value)
        for entry in independent_entries
    }
    confidence_class = min(
        confidence_values,
        key=lambda value: (
            _KNOWLEDGE_CONFIDENCE_RANK.get(value, 0),
            value,
        ),
    )
    contradiction_key = _knowledge_tag(
        proposal.contradiction_key
        or f"{subject_key}:{predicate_key}"
    )
    if not contradiction_key:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_id,
            reason_code="ambiguous_contradiction_scope",
            root_entry_ids=all_ids,
        )
    tags = tuple(
        sorted(
            {
                tag
                for tag in (
                    _knowledge_tag(raw_tag)
                    for raw_tag in proposal.retrieval_tags
                )
                if tag
            }
        )[:16]
    )
    routes = sorted(
        {
            (
                str(entry.get("route_mode") or ""),
                str(entry.get("channel_policy") or ""),
                int(entry.get("channel_id") or 0),
                str(entry.get("channel_name") or "")[:120],
            )
            for entry in entries.values()
        }
    )
    route_scope_json = json.dumps(
        [
            {
                "route_mode": route_mode,
                "channel_policy": channel_policy,
                "channel_id": channel_id,
                "channel_name": channel_name,
            }
            for route_mode, channel_policy, channel_id, channel_name in routes
        ],
        sort_keys=True,
        separators=(",", ":"),
    )
    root_digest = _knowledge_digest(*independent_ids)
    participant_scope_digest = _knowledge_digest(*participants)
    candidate_id = _stable_knowledge_candidate_id(
        guild_id=guild_id,
        candidate_type=candidate_type,
        subject_key=subject_key,
        predicate_key=predicate_key,
        contradiction_key=contradiction_key,
        root_entry_ids=independent_ids,
    )
    recurring_public_conversation = bool(
        candidate_type == "topic_or_motif"
        and "recurring_public_conversation" in tags
    )
    if recurring_public_conversation:
        candidate_id = _recurring_motif_candidate_id(
            conn,
            guild_id=guild_id,
            subject_key=subject_key,
            predicate_key=predicate_key,
            contradiction_key=contradiction_key,
            visibility=visibility,
            participant_scope_digest=participant_scope_digest,
            root_digest=root_digest,
            fallback_candidate_id=candidate_id,
        )
    value_digest = _knowledge_digest(_canon(meaning))
    first_seen = min(
        (str(entry.get("observed_at") or "") for entry in independent_entries),
        default="",
    )
    last_seen = max(
        (str(entry.get("observed_at") or "") for entry in independent_entries),
        default="",
    )
    existing = conn.execute(
        """
        SELECT normalized_value,candidate_state,candidate_eligible,
               invalidated_reason,root_digest
        FROM memory_ledger_knowledge_candidates
        WHERE candidate_id=?
        """,
        (candidate_id,),
    ).fetchone()
    if existing:
        if _canon(existing[0]) != _canon(meaning):
            conn.execute(
                """
                UPDATE memory_ledger_knowledge_candidates
                SET candidate_state='contested',candidate_eligible=0,
                    live_eligible=0,invalidated_reason='same_roots_meaning_mismatch',
                    invalidated_at=?,updated_at=?
                WHERE candidate_id=?
                """,
                (_now(), _now(), candidate_id),
            )
            _record_knowledge_receipt(
                conn,
                guild_id=guild_id,
                event_type="contested",
                reason_code="same_roots_meaning_mismatch",
                candidate_id=candidate_id,
                candidate_type=candidate_type,
                root_entry_ids=all_ids,
                proposal_digest=value_digest,
            )
            reconcile_atomic_knowledge_lifecycle(
                conn,
                candidate_ids=(candidate_id,),
            )
            return AtomicKnowledgeResult(
                candidate_id,
                "contested",
                "same_roots_meaning_mismatch",
                candidate_type,
                len(all_ids),
            )
        prior_state = str(existing[1] or "")
        refreshable_recurring = bool(
            recurring_public_conversation
            and (
                prior_state
                in {"candidate", "provisional", "established"}
                or (
                    prior_state == "contested"
                    and str(existing[3] or "").startswith(
                        "conversation_motif_correction"
                    )
                )
            )
        )
        roots_refreshed = bool(
            refreshable_recurring
            and str(existing[4] or "") != root_digest
        )
        refreshed_at = _now()
        if roots_refreshed:
            conn.execute(
                """
                UPDATE memory_ledger_knowledge_candidates
                SET subject_display_name=?,visibility=?,authority_class=?,
                    confidence_class=?,route_scope_json=?,
                    participant_scope_digest=?,
                    first_seen_at=CASE
                      WHEN COALESCE(first_seen_at,'')='' OR ?<first_seen_at
                        THEN ?
                      ELSE first_seen_at
                    END,
                    last_seen_at=?,
                    retrieval_tags_json=?,root_digest=?,
                    independent_root_count=?,derivative_root_count=?,
                    review_status='dirty',lifecycle_evaluated_at='',
                    updated_at=?
                WHERE candidate_id=?
                """,
                (
                    _knowledge_text(proposal.subject_display_name, 120),
                    visibility,
                    authority_class,
                    confidence_class,
                    route_scope_json,
                    participant_scope_digest,
                    first_seen,
                    first_seen,
                    last_seen,
                    json.dumps(tags, separators=(",", ":")),
                    root_digest,
                    len(independent_ids),
                    len(derivative_ids),
                    refreshed_at,
                    candidate_id,
                ),
            )
            _replace_atomic_candidate_current_roots(
                conn,
                candidate_id=candidate_id,
                guild_id=guild_id,
                subject_key=subject_key,
                participants=participants,
                all_ids=all_ids,
                independent_ids=independent_ids,
                entries=entries,
                derivation_paths=derivation_paths,
                now=refreshed_at,
            )
        else:
            conn.execute(
                """
                UPDATE memory_ledger_knowledge_candidates
                SET last_seen_at=CASE
                      WHEN ? > COALESCE(last_seen_at,'') THEN ?
                      ELSE last_seen_at
                    END,
                    updated_at=?
                WHERE candidate_id=?
                """,
                (last_seen, last_seen, refreshed_at, candidate_id),
            )
        active_match = (
            prior_state in KNOWLEDGE_ACTIVE_CANDIDATE_STATES
            and bool(existing[2])
        )
        match_reason = (
            "conversation_motif_roots_refreshed"
            if roots_refreshed
            else "matched_terminal_candidate"
            if prior_state in KNOWLEDGE_TERMINAL_CANDIDATE_STATES
            else "matched_contested_candidate"
            if not active_match
            else "exact_candidate_match"
        )
        _record_knowledge_receipt(
            conn,
            guild_id=guild_id,
            event_type="matched_existing",
            reason_code=match_reason,
            candidate_id=candidate_id,
            candidate_type=candidate_type,
            root_entry_ids=all_ids,
            proposal_digest=value_digest,
        )
        reconcile_atomic_knowledge_lifecycle(
            conn,
            candidate_ids=(candidate_id,),
        )
        return AtomicKnowledgeResult(
            candidate_id,
            "matched_existing",
            match_reason,
            candidate_type,
            len(all_ids),
        )

    conflicts = conn.execute(
        """
        SELECT candidate_id,normalized_value,candidate_state,
               candidate_eligible
        FROM memory_ledger_knowledge_candidates
        WHERE guild_id=? AND subject_key=? AND candidate_type=?
          AND contradiction_key=? AND candidate_id<>?
        ORDER BY candidate_id
        """,
        (
            guild_id,
            subject_key,
            candidate_type,
            contradiction_key,
            candidate_id,
        ),
    ).fetchall()
    conflicting_rows = (
        []
        if candidate_type in KNOWLEDGE_SCOPE_CONSOLIDATED_TYPES
        else [
            (
                str(row[0]),
                str(row[2] or ""),
                bool(row[3]),
            )
            for row in conflicts
            if _canon(row[1]) != _canon(meaning)
        ]
    )
    explicitly_superseded: list[str] = []
    unresolved_conflicts: list[str] = []
    for conflict_id, conflict_state, conflict_eligible in conflicting_rows:
        old_roots = tuple(
            str(row[0])
            for row in conn.execute(
                """
                SELECT root_entry_id
                FROM memory_ledger_knowledge_roots
                WHERE candidate_id=? AND is_independent=1
                ORDER BY root_entry_id
                """,
                (conflict_id,),
            ).fetchall()
        )
        if old_roots and conn.execute(
            """
            SELECT 1 FROM memory_ledger_lineage
            WHERE entry_id IN (%s)
              AND target_entry_id IN (%s)
              AND lineage_type='supersedes'
            LIMIT 1
            """
            % (
                ",".join("?" for _entry_id in independent_ids),
                ",".join("?" for _entry_id in old_roots),
            ),
            tuple(independent_ids + old_roots),
        ).fetchone():
            explicitly_superseded.append(conflict_id)
        elif (
            conflict_state in KNOWLEDGE_ACTIVE_CANDIDATE_STATES
            and conflict_eligible
        ):
            unresolved_conflicts.append(conflict_id)

    initial_state = (
        "contested"
        if proposal.epistemic_status == "contested" or unresolved_conflicts
        else "candidate"
    )
    now = _now()
    conn.execute(
        """
        INSERT INTO memory_ledger_knowledge_candidates(
          candidate_id,schema_version,guild_id,candidate_type,subject_key,
          subject_display_name,predicate_key,normalized_value,value_digest,
          epistemic_status,uncertainty_note,currentness,candidate_state,
          contradiction_key,supersedes_candidate_id,visibility,
          authority_class,confidence_class,route_scope_json,
          participant_scope_digest,first_seen_at,last_seen_at,
          retrieval_tags_json,root_digest,independent_root_count,
          derivative_root_count,candidate_eligible,live_eligible,
          promotion_status,invalidated_reason,invalidated_at,created_at,
          updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            candidate_id,
            ATOMIC_KNOWLEDGE_SCHEMA_VERSION,
            guild_id,
            candidate_type,
            subject_key,
            _knowledge_text(proposal.subject_display_name, 120),
            predicate_key,
            meaning,
            value_digest,
            proposal.epistemic_status,
            _knowledge_text(proposal.uncertainty_note, 240),
            proposal.currentness,
            initial_state,
            contradiction_key,
            explicitly_superseded[0] if len(explicitly_superseded) == 1 else "",
            visibility,
            authority_class,
            confidence_class,
            route_scope_json,
            participant_scope_digest,
            first_seen,
            last_seen,
            json.dumps(tags, separators=(",", ":")),
            root_digest,
            len(independent_ids),
            len(derivative_ids),
            1 if initial_state == "candidate" else 0,
            0,
            "unpromoted",
            "unresolved_contradiction" if initial_state == "contested" else "",
            now if initial_state == "contested" else "",
            now,
            now,
        ),
    )
    _replace_atomic_candidate_current_roots(
        conn,
        candidate_id=candidate_id,
        guild_id=guild_id,
        subject_key=subject_key,
        participants=participants,
        all_ids=all_ids,
        independent_ids=independent_ids,
        entries=entries,
        derivation_paths=derivation_paths,
        now=now,
    )

    for superseded_id in explicitly_superseded:
        conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET candidate_state='superseded',candidate_eligible=0,
                live_eligible=0,invalidated_reason='explicit_root_supersession',
                invalidated_at=?,updated_at=?
            WHERE candidate_id=?
            """,
            (now, now, superseded_id),
        )
        _record_knowledge_receipt(
            conn,
            guild_id=guild_id,
            event_type="superseded",
            reason_code="explicit_root_supersession",
            candidate_id=superseded_id,
            candidate_type=candidate_type,
            root_entry_ids=independent_ids,
        )
    if unresolved_conflicts:
        placeholders = ",".join("?" for _candidate_id in unresolved_conflicts)
        conn.execute(
            """
            UPDATE memory_ledger_knowledge_candidates
            SET candidate_state='contested',candidate_eligible=0,
                live_eligible=0,invalidated_reason='unresolved_contradiction',
                invalidated_at=?,updated_at=?
            WHERE candidate_id IN (%s)
            """ % placeholders,
            tuple([now, now] + unresolved_conflicts),
        )
        for conflict_id in unresolved_conflicts:
            _record_knowledge_receipt(
                conn,
                guild_id=guild_id,
                event_type="contested",
                reason_code="unresolved_contradiction",
                candidate_id=conflict_id,
                candidate_type=candidate_type,
                root_entry_ids=independent_ids,
            )
    event_type = "contested" if initial_state == "contested" else "created"
    reason_code = (
        "unresolved_contradiction"
        if initial_state == "contested"
        else "source_linked_candidate_created"
    )
    _record_knowledge_receipt(
        conn,
        guild_id=guild_id,
        event_type=event_type,
        reason_code=reason_code,
        candidate_id=candidate_id,
        candidate_type=candidate_type,
        root_entry_ids=all_ids,
        proposal_digest=value_digest,
    )
    reconcile_atomic_knowledge_lifecycle(
        conn,
        candidate_ids=(candidate_id,),
    )
    return AtomicKnowledgeResult(
        candidate_id,
        event_type,
        reason_code,
        candidate_type,
        len(all_ids),
    )


def form_atomic_candidate_from_ledger_entry(
    conn: sqlite3.Connection,
    entry_id: str,
) -> AtomicKnowledgeResult | None:
    """Map only already-typed durable Ledger entries; raw chat stays raw."""
    ensure_memory_ledger_schema(conn)
    rows = _knowledge_entry_rows(conn, (entry_id,))
    entry = rows.get(entry_id)
    if not entry:
        proposal = AtomicKnowledgeProposal(
            "person_role_fact",
            "unknown",
            "missing",
            "",
            (entry_id,),
        )
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=0,
            reason_code="missing_or_orphaned_root",
            root_entry_ids=(entry_id,),
        )
    entry_type = str(entry.get("entry_type") or "")
    if entry_type not in {
        "claim",
        "preference",
        "boundary",
        "goal",
        "open_loop",
        "commitment",
        "unresolved_question",
        "event",
        "canon_reference",
    }:
        return None
    subject_key = str(entry.get("subject_key") or "")
    predicate_key = str(entry.get("predicate_key") or "")
    if entry_type in {"open_loop", "unresolved_question"}:
        candidate_type = "open_loop_or_question"
        currentness = "open"
        epistemic = "question" if entry_type == "unresolved_question" else "stated"
    elif entry_type == "event":
        candidate_type = "project_event_or_milestone"
        currentness = "historical"
        epistemic = "observed"
    elif (
        subject_key.startswith("discord_user:")
        or any(
            token in predicate_key
            for token in (
                "name",
                "pronoun",
                "role",
                "identity",
                "favorite",
                "preference",
                "boundary",
            )
        )
    ):
        candidate_type = "person_role_fact"
        currentness = "current"
        epistemic = "stated"
    elif entry_type == "canon_reference":
        candidate_type = "project_event_or_milestone"
        currentness = "current"
        epistemic = "stated"
    else:
        candidate_type = "open_loop_or_question"
        currentness = "open"
        epistemic = "stated"
    if str(entry.get("lifecycle_status") or "") != ACTIVE_LIFECYCLE:
        proposal = AtomicKnowledgeProposal(
            candidate_type=candidate_type,
            subject_key=subject_key,
            subject_display_name=str(entry.get("subject_display_name") or ""),
            predicate_key=predicate_key,
            meaning=str(entry.get("normalized_value") or ""),
            root_entry_ids=(entry_id,),
            epistemic_status=epistemic,
            currentness=currentness,
        )
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=int(entry.get("guild_id") or 0),
            reason_code="ineligible_root_lifecycle",
            root_entry_ids=(entry_id,),
        )
    participant_keys = tuple(
        sorted(
            {
                participant_key
                for participant_key, _role in entry.get("participants", [])
                if participant_key
            }
        )
    )
    return form_atomic_knowledge_candidate(
        conn,
        AtomicKnowledgeProposal(
            candidate_type=candidate_type,
            subject_key=subject_key,
            subject_display_name=str(entry.get("subject_display_name") or ""),
            predicate_key=predicate_key,
            meaning=str(entry.get("normalized_value") or ""),
            root_entry_ids=(entry_id,),
            participant_keys=participant_keys,
            epistemic_status=epistemic,
            currentness=currentness,
            contradiction_key=f"{subject_key}:{predicate_key}",
            retrieval_tags=(
                candidate_type,
                predicate_key,
                str(entry.get("source_table") or ""),
            ),
        ),
    )


def _conversation_motif_terms(
    value: str,
    *,
    include_correction: bool = False,
    include_direct_fact: bool = False,
) -> tuple[str, ...]:
    source_text = re.sub(r"\s+", " ", str(value or "")).strip()
    if (
        not source_text
        or (
            not include_correction
            and _CONVERSATION_CORRECTION_RE.search(source_text)
        )
        or _CONVERSATION_MOTIF_RECALL_RE.search(source_text)
        or _CONVERSATION_MOTIF_UNSAFE_RE.search(source_text)
        or (
            not include_direct_fact
            and _CONVERSATION_MOTIF_DIRECT_FACT_RE.search(source_text)
        )
        or _CONVERSATION_MOTIF_SENSITIVE_RE.search(source_text)
        or _CONVERSATION_MOTIF_ROLEPLAY_RE.search(source_text)
    ):
        return ()
    text = re.sub(
        r"\s+",
        " ",
        _CONVERSATION_MOTIF_URL_OR_MENTION_TOKEN_RE.sub(
            " ",
            source_text,
        ),
    ).strip()
    if not text or len(text.split()) < 4:
        return ()
    return tuple(
        dict.fromkeys(
            token
            for token in _CONVERSATION_MOTIF_TERM_RE.findall(text.lower())
            if token not in _CONVERSATION_MOTIF_STOPWORDS
        )
    )[:32]


def _conversation_motif_family_matches(
    terms: tuple[str, ...],
) -> tuple[tuple[str, str], ...]:
    term_set = set(terms)
    return tuple(
        (family, label)
        for family, label, markers in _CONVERSATION_MOTIF_FAMILIES
        if (
            term_set.intersection(markers)
            and term_set.intersection(
                _CONVERSATION_MOTIF_ANCHORS.get(family, frozenset())
            )
        )
    )


def _conversation_motif_predicates_for_values(
    values: Iterable[str],
) -> tuple[str, ...]:
    predicates = {
        "conversation_motif_%s" % family
        for value in values
        for family, _label in _conversation_motif_family_matches(
            _conversation_motif_terms(
                str(value or ""),
                include_correction=True,
                include_direct_fact=True,
            )
        )
    }
    if predicates:
        return tuple(sorted(predicates))
    normalized_values = tuple(str(value or "") for value in values)
    if any(
        _CONVERSATION_MOTIF_DIRECT_FACT_RE.search(value)
        or _CONVERSATION_MOTIF_SENSITIVE_RE.search(value)
        or _CONVERSATION_MOTIF_UNSAFE_RE.search(value)
        for value in normalized_values
    ):
        return ()
    # A generic or opaque correction cannot safely be assigned to one topic.
    # Fence the finite known motif family set instead of guessing a target.
    return tuple(
        sorted(
            "conversation_motif_%s" % family
            for family, _label, _markers in _CONVERSATION_MOTIF_FAMILIES
        )
    )


def _conversation_motif_fence_row(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_key: str,
) -> dict[str, str]:
    row = conn.execute(
        """
        SELECT correction_entry_id,correction_observed_at,reason_code,
               fence_state,satisfied_at
        FROM memory_ledger_conversation_motif_fences
        WHERE guild_id=? AND subject_key=? AND predicate_key=?
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            str(predicate_key or ""),
        ),
    ).fetchone()
    if not row:
        return {}
    return {
        "correction_entry_id": str(row[0] or ""),
        "correction_observed_at": str(row[1] or ""),
        "reason_code": str(row[2] or ""),
        "fence_state": str(row[3] or "active"),
        "satisfied_at": str(row[4] or ""),
    }


def _conversation_motif_entries_after_fence(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_key: str,
    entries: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    fence = _conversation_motif_fence_row(
        conn,
        guild_id=guild_id,
        subject_key=subject_key,
        predicate_key=predicate_key,
    )
    if not fence:
        return entries, {}
    cutoff = _parse_knowledge_time(fence.get("correction_observed_at"))
    if cutoff is None:
        return [], fence
    post_correction: list[dict[str, Any]] = []
    for entry in entries:
        observed = _parse_knowledge_time(entry.get("observed_at"))
        if observed is not None and observed > cutoff:
            post_correction.append(entry)
    return post_correction, fence


def _withhold_conversation_motif_candidates(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_keys: tuple[str, ...],
    correction_entry_id: str,
    reason_code: str,
) -> None:
    if not predicate_keys:
        return
    placeholders = ",".join("?" for _predicate in predicate_keys)
    candidate_ids = tuple(
        str(row[0] or "")
        for row in conn.execute(
            f"""
            SELECT candidate_id
            FROM memory_ledger_knowledge_candidates
            WHERE guild_id=? AND subject_key=?
              AND candidate_type='topic_or_motif'
              AND predicate_key IN ({placeholders})
              AND retrieval_tags_json LIKE '%recurring_public_conversation%'
            ORDER BY candidate_id
            """,
            (
                int(guild_id or 0),
                str(subject_key or ""),
                *predicate_keys,
            ),
        ).fetchall()
        if str(row[0] or "")
    )
    if not candidate_ids:
        return
    now = _now()
    candidate_placeholders = ",".join(
        "?" for _candidate_id in candidate_ids
    )
    conn.execute(
        f"""
        UPDATE memory_ledger_knowledge_candidates
        SET candidate_state=CASE
              WHEN candidate_state IN ('superseded','retired','invalidated')
                THEN candidate_state
              ELSE 'contested'
            END,
            candidate_eligible=0,live_eligible=0,
            invalidated_reason=CASE
              WHEN candidate_state IN ('superseded','retired','invalidated')
                THEN invalidated_reason
              ELSE ?
            END,
            invalidated_at=CASE
              WHEN candidate_state IN ('superseded','retired','invalidated')
                THEN invalidated_at
              ELSE ?
            END,
            lifecycle_reason=CASE
              WHEN candidate_state IN ('superseded','retired','invalidated')
                THEN lifecycle_reason
              ELSE ?
            END,
            review_status='dirty',lifecycle_evaluated_at='',
            updated_at=?
        WHERE candidate_id IN ({candidate_placeholders})
        """,
        (
            reason_code,
            now,
            reason_code,
            now,
            *candidate_ids,
        ),
    )
    for candidate_id in candidate_ids:
        _record_knowledge_receipt(
            conn,
            guild_id=int(guild_id or 0),
            event_type="contested",
            reason_code=reason_code,
            candidate_id=candidate_id,
            candidate_type="topic_or_motif",
            root_entry_ids=(
                (str(correction_entry_id),)
                if str(correction_entry_id or "")
                else ()
            ),
        )


def _upsert_conversation_motif_correction_fences(
    conn: sqlite3.Connection,
    *,
    correction_entry: dict[str, Any],
    related_entry_ids: tuple[str, ...],
    reason_code: str,
) -> tuple[str, ...]:
    related = _knowledge_entry_rows(
        conn,
        tuple(
            sorted(
                {
                    str(entry_id or "")
                    for entry_id in related_entry_ids
                    if str(entry_id or "")
                }
            )
        ),
    )
    values = [
        str(correction_entry.get("normalized_value") or ""),
        *(
            str(entry.get("normalized_value") or "")
            for entry in related.values()
        ),
    ]
    predicate_keys = _conversation_motif_predicates_for_values(values)
    observed = _parse_knowledge_time(correction_entry.get("observed_at"))
    observed_at = _knowledge_time(observed) if observed is not None else ""
    guild_id = int(correction_entry.get("guild_id") or 0)
    subject_key = str(correction_entry.get("subject_key") or "")
    correction_entry_id = str(correction_entry.get("entry_id") or "")
    now = _now()
    active_predicate_keys: list[str] = []
    for predicate_key in predicate_keys:
        existing = _conversation_motif_fence_row(
            conn,
            guild_id=guild_id,
            subject_key=subject_key,
            predicate_key=predicate_key,
        )
        existing_time = _parse_knowledge_time(
            existing.get("correction_observed_at")
        )
        if (
            existing
            and existing_time is not None
            and observed is not None
            and existing_time > observed
        ):
            continue
        if (
            existing
            and str(existing.get("correction_entry_id") or "")
            == correction_entry_id
            and str(existing.get("fence_state") or "active")
            == "satisfied"
        ):
            continue
        conn.execute(
            """
            INSERT INTO memory_ledger_conversation_motif_fences(
              guild_id,subject_key,predicate_key,correction_entry_id,
              correction_observed_at,reason_code,fence_state,satisfied_at,
              created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(guild_id,subject_key,predicate_key) DO UPDATE SET
              correction_entry_id=excluded.correction_entry_id,
              correction_observed_at=excluded.correction_observed_at,
              reason_code=excluded.reason_code,
              fence_state='active',
              satisfied_at='',
              updated_at=excluded.updated_at
            """,
            (
                guild_id,
                subject_key,
                predicate_key,
                correction_entry_id,
                observed_at,
                reason_code,
                "active",
                "",
                now,
                now,
            ),
        )
        active_predicate_keys.append(predicate_key)
    _withhold_conversation_motif_candidates(
        conn,
        guild_id=guild_id,
        subject_key=subject_key,
        predicate_keys=tuple(active_predicate_keys),
        correction_entry_id=correction_entry_id,
        reason_code=reason_code,
    )
    return tuple(active_predicate_keys)


def _finalize_conversation_motif_refresh(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_key: str,
    result: AtomicKnowledgeResult,
    root_entry_ids: tuple[str, ...],
    correction_fence: dict[str, str],
) -> bool:
    candidate_id = str(result.candidate_id or "")
    if (
        not candidate_id
        or result.outcome not in {"created", "matched_existing"}
    ):
        return False
    state = conn.execute(
        """
        SELECT candidate_state,invalidated_reason
        FROM memory_ledger_knowledge_candidates
        WHERE candidate_id=?
        """,
        (candidate_id,),
    ).fetchone()
    if not state:
        return False
    active_correction_fence = bool(
        correction_fence
        and str(correction_fence.get("fence_state") or "active")
        == "active"
    )
    refreshable = str(state[0] or "") in {
        "candidate",
        "provisional",
        "established",
    } or (
        str(state[0] or "") == "contested"
        and str(state[1] or "")
        == "conversation_motif_correction_fence"
    )
    if not refreshable:
        return False
    sibling_rows = tuple(
        (
            str(row[0] or ""),
            str(row[1] or ""),
        )
        for row in conn.execute(
            """
            SELECT candidate_id,candidate_state
            FROM memory_ledger_knowledge_candidates
            WHERE guild_id=? AND subject_key=?
              AND candidate_type='topic_or_motif'
              AND predicate_key=? AND candidate_id<>?
              AND retrieval_tags_json LIKE '%recurring_public_conversation%'
            ORDER BY candidate_id
            """,
            (
                int(guild_id or 0),
                str(subject_key or ""),
                str(predicate_key or ""),
                candidate_id,
            ),
        ).fetchall()
        if str(row[0] or "")
    )
    superseded_sibling_ids = tuple(
        sibling_id
        for sibling_id, sibling_state in sibling_rows
        if sibling_state not in KNOWLEDGE_TERMINAL_CANDIDATE_STATES
    )
    if superseded_sibling_ids:
        placeholders = ",".join(
            "?" for _candidate_id in superseded_sibling_ids
        )
        now = _now()
        conn.execute(
            f"""
            UPDATE memory_ledger_knowledge_candidates
            SET candidate_state='superseded',candidate_eligible=0,
                live_eligible=0,
                invalidated_reason='conversation_motif_canonical_refresh',
                invalidated_at=?,lifecycle_reason=
                  'conversation_motif_canonical_refresh',
                review_status='dirty',lifecycle_evaluated_at='',updated_at=?
            WHERE candidate_id IN ({placeholders})
            """,
            (now, now, *superseded_sibling_ids),
        )
        for sibling_id in superseded_sibling_ids:
            _record_knowledge_receipt(
                conn,
                guild_id=int(guild_id or 0),
                event_type="superseded",
                reason_code="conversation_motif_canonical_refresh",
                candidate_id=sibling_id,
                candidate_type="topic_or_motif",
                root_entry_ids=root_entry_ids,
            )
    if active_correction_fence:
        satisfied_at = _now()
        conn.execute(
            """
            UPDATE memory_ledger_conversation_motif_fences
            SET fence_state='satisfied',satisfied_at=?,updated_at=?
            WHERE guild_id=? AND subject_key=? AND predicate_key=?
              AND correction_entry_id=?
            """,
            (
                satisfied_at,
                satisfied_at,
                int(guild_id or 0),
                str(subject_key or ""),
                str(predicate_key or ""),
                str(correction_fence.get("correction_entry_id") or ""),
            ),
        )
    reconcile_atomic_knowledge_lifecycle(
        conn,
        candidate_ids=(candidate_id,),
    )
    if (
        superseded_sibling_ids
        or active_correction_fence
        or result.reason_code == "conversation_motif_roots_refreshed"
    ):
        _record_knowledge_receipt(
            conn,
            guild_id=int(guild_id or 0),
            event_type="refreshed",
            reason_code=(
                "conversation_motif_post_correction_reestablished"
                if active_correction_fence
                else "conversation_motif_bounded_refresh"
            ),
            candidate_id=candidate_id,
            candidate_type="topic_or_motif",
            root_entry_ids=root_entry_ids,
        )
    return True


def _conversation_motif_roots_by_occurrence(
    entries: list[dict[str, Any]],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Retain bounded human roots while counting each occurrence only once."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in sorted(
        entries,
        key=lambda item: (
            str(item.get("observed_at") or ""),
            str(item.get("entry_id") or ""),
        ),
        reverse=True,
    ):
        occurrence = str(entry.get("occurrence_identity") or "")
        if occurrence:
            grouped.setdefault(occurrence, []).append(entry)

    def latest_timestamp(occurrence_entries: list[dict[str, Any]]) -> float:
        timestamps = []
        for entry in occurrence_entries:
            observed = _parse_knowledge_time(
                str(entry.get("observed_at") or "")
            )
            if observed is not None:
                timestamps.append(observed.timestamp())
        return max(timestamps, default=0.0)

    ordered_groups = tuple(
        sorted(
            grouped.items(),
            key=lambda pair: (
                -latest_timestamp(pair[1]),
                pair[0],
            ),
        )
    )
    selected: list[str] = []
    selected_occurrences: list[str] = []
    # Reserve one exact root for each newest occurrence first, so the bounded
    # root cap cannot accidentally erase the recurrence that justified the
    # proposal.
    for occurrence, occurrence_entries in ordered_groups:
        root_id = str(occurrence_entries[0].get("entry_id") or "")
        if not root_id:
            continue
        selected.append(root_id)
        selected_occurrences.append(occurrence)
        if len(selected) >= _CONVERSATION_MOTIF_MAX_ROOTS:
            break
    if len(selected) < _CONVERSATION_MOTIF_MAX_ROOTS:
        selected_set = set(selected)
        for _occurrence, occurrence_entries in ordered_groups:
            for entry in occurrence_entries:
                root_id = str(entry.get("entry_id") or "")
                if not root_id or root_id in selected_set:
                    continue
                selected.append(root_id)
                selected_set.add(root_id)
                if len(selected) >= _CONVERSATION_MOTIF_MAX_ROOTS:
                    break
            if len(selected) >= _CONVERSATION_MOTIF_MAX_ROOTS:
                break
    return tuple(sorted(selected)), tuple(sorted(selected_occurrences))


def _sync_bounded_conversation_motif_corrections(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int,
) -> None:
    """Discover recent raw corrections missed while formation was disabled."""
    rows = conn.execute(
        """
        SELECT entry_id,normalized_value
        FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=?
          AND entry_type='observation' AND predicate_key='conversation'
          AND source_table='conversations' AND source_role='user'
          AND source_class='public_observation'
          AND channel_policy IN (
            'public_home','public_context','public_selective'
          )
          AND visibility IN ('public','public_safe')
          AND public_usable=1 AND derived=0 AND projection=0
          AND lifecycle_status='active'
        ORDER BY source_sequence DESC,entry_id DESC
        LIMIT ?
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            max(
                1,
                min(
                    int(max_scan or _CONVERSATION_MOTIF_MAX_SCAN),
                    _CONVERSATION_MOTIF_MAX_SCAN,
                ),
            ),
        ),
    ).fetchall()
    corrections = [
        (str(row[0] or ""), str(row[1] or ""))
        for row in rows
        if str(row[0] or "")
        and _CONVERSATION_CORRECTION_RE.search(str(row[1] or ""))
    ][:_CONVERSATION_CORRECTION_MAX_SCAN]
    # Apply oldest first so a newer correction deterministically owns a
    # family fence.  The total work remains bounded by the scans above and in
    # the conservative raw resolver.
    for correction_entry_id, correction_value in reversed(corrections):
        correction_entry = _knowledge_entry_rows(
            conn,
            (correction_entry_id,),
        ).get(correction_entry_id)
        if not correction_entry:
            continue
        lineage_targets = tuple(
            sorted(
                {
                    str(row[0] or "")
                    for row in conn.execute(
                        """
                        SELECT target_entry_id
                        FROM memory_ledger_lineage
                        WHERE entry_id=?
                          AND lineage_type IN ('correction_of','supersedes')
                        ORDER BY target_entry_id
                        """,
                        (correction_entry_id,),
                    ).fetchall()
                    if str(row[0] or "")
                }
            )
        )
        correction_target = (
            lineage_targets[0] if len(lineage_targets) == 1 else ""
        )
        ambiguous_targets = (
            lineage_targets if len(lineage_targets) > 1 else ()
        )
        if not correction_target and not ambiguous_targets:
            correction_target, ambiguous_targets = (
                _raw_conversation_correction_resolution(
                    conn,
                    guild_id=int(guild_id or 0),
                    subject_key=str(subject_key or ""),
                    correction_value=correction_value,
                    channel_policy=str(
                        correction_entry.get("channel_policy") or ""
                    ),
                    current_entry_id=correction_entry_id,
                )
            )
        reason_code = (
            "conversation_motif_correction"
            if correction_target
            else "conversation_motif_correction_ambiguous"
            if ambiguous_targets
            else "conversation_motif_correction_unresolved"
        )
        _upsert_conversation_motif_correction_fences(
            conn,
            correction_entry=correction_entry,
            related_entry_ids=tuple(
                sorted(
                    {
                        str(entry_id or "")
                        for entry_id in (
                            correction_target,
                            *ambiguous_targets,
                        )
                        if str(entry_id or "")
                    }
                )
            ),
            reason_code=reason_code,
        )


def _conversation_motif_history(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int,
    diagnostics: dict[str, int] | None = None,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT entry_id,subject_key,subject_display_name,normalized_value,observed_at,
               channel_id,channel_policy,source_table,source_row_id,
               source_role,source_class,visibility,public_usable,
               derived,projection,lifecycle_status
        FROM memory_ledger_entries e
        WHERE guild_id=? AND subject_key=?
          AND entry_type='observation' AND predicate_key='conversation'
          AND source_table='conversations' AND source_role='user'
          AND source_class='public_observation'
          AND channel_policy IN (
            'public_home','public_context','public_selective'
          )
          AND visibility IN ('public','public_safe')
          AND public_usable=1 AND derived=0 AND projection=0
          AND lifecycle_status='active'
          AND NOT EXISTS (
            SELECT 1 FROM memory_ledger_lineage l
            WHERE l.guild_id=e.guild_id AND l.target_entry_id=e.entry_id
              AND l.lineage_type IN (
                'correction_of','supersedes','retracts'
              )
          )
        ORDER BY observed_at DESC,source_sequence DESC,entry_id DESC
        LIMIT ?
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            max(
                1,
                min(
                    int(max_scan or _CONVERSATION_MOTIF_MAX_SCAN),
                    _CONVERSATION_MOTIF_MAX_SCAN,
                ),
            ),
        ),
    ).fetchall()
    keys = (
        "entry_id",
        "subject_key",
        "subject_display_name",
        "normalized_value",
        "observed_at",
        "channel_id",
        "channel_policy",
        "source_table",
        "source_row_id",
        "source_role",
        "source_class",
        "visibility",
        "public_usable",
        "derived",
        "projection",
        "lifecycle_status",
    )
    history: list[dict[str, Any]] = []
    if diagnostics is not None:
        diagnostics["ledger_rows_scanned"] = len(rows)
    for row in rows:
        entry = dict(zip(keys, row))
        terms = _conversation_motif_terms(
            str(entry.get("normalized_value") or "")
        )
        if not terms:
            if diagnostics is not None:
                diagnostics["ledger_rows_term_excluded"] = (
                    int(
                        diagnostics.get(
                            "ledger_rows_term_excluded",
                            0,
                        )
                        or 0
                    )
                    + 1
                )
            continue
        full_entry = _knowledge_entry_rows(
            conn,
            (str(entry.get("entry_id") or ""),),
        ).get(str(entry.get("entry_id") or ""))
        if not full_entry:
            if diagnostics is not None:
                diagnostics["ledger_rows_missing_root"] = (
                    int(
                        diagnostics.get(
                            "ledger_rows_missing_root",
                            0,
                        )
                        or 0
                    )
                    + 1
                )
            continue
        if _knowledge_operational_or_test_source(full_entry):
            if diagnostics is not None:
                diagnostics["ledger_rows_operational_excluded"] = (
                    int(
                        diagnostics.get(
                            "ledger_rows_operational_excluded",
                            0,
                        )
                        or 0
                    )
                    + 1
                )
            continue
        entry["terms"] = terms
        entry["occurrence_identity"] = _knowledge_occurrence_identity(
            conn,
            full_entry,
        )
        if not entry["occurrence_identity"]:
            if diagnostics is not None:
                diagnostics["ledger_rows_occurrence_excluded"] = (
                    int(
                        diagnostics.get(
                            "ledger_rows_occurrence_excluded",
                            0,
                        )
                        or 0
                    )
                    + 1
                )
            continue
        history.append(entry)
    if diagnostics is not None:
        diagnostics["ledger_rows_motif_eligible"] = len(history)
    return history


def _public_assessment_terms(value: str) -> frozenset[str]:
    return frozenset(
        token
        for token in _PUBLIC_ASSESSMENT_TERM_RE.findall(
            str(value or "").lower()
        )
        if token not in _PUBLIC_ASSESSMENT_STOPWORDS
    )


def _public_assessment_text(value: str) -> str:
    """Return inert public prose suitable for a bounded response-time packet."""

    text = re.sub(
        r"\s+",
        " ",
        _CONVERSATION_MOTIF_URL_OR_MENTION_TOKEN_RE.sub(
            " ",
            str(value or ""),
        ),
    ).strip()
    if (
        len(text.split()) < 4
        or _CONVERSATION_MOTIF_UNSAFE_RE.search(text)
        or _CONVERSATION_MOTIF_DIRECT_FACT_RE.search(text)
        or _CONVERSATION_MOTIF_SENSITIVE_RE.search(text)
        or _CONVERSATION_MOTIF_ROLEPLAY_RE.search(text)
        or _CONVERSATION_CORRECTION_RE.search(text)
    ):
        return ""
    return text.replace("```", "")[:240]


def select_public_conversation_assessment_evidence(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    request_text: str,
    max_scan: int = _CONVERSATION_MOTIF_MAX_SCAN,
    max_results: int = _PUBLIC_ASSESSMENT_MAX_RESULTS,
) -> PublicAssessmentSelection:
    """Select diverse evidence after considering the full eligible public pool.

    This is an ephemeral read projection, not another memory owner and not a
    durable personality inference. The same subject, visibility, lifecycle,
    correction, unsafe-content, and operational-source fences used by
    recurring-conversation formation remain controlling.
    """

    diagnostics: dict[str, int] = {}
    history = _conversation_motif_history(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key or ""),
        max_scan=max_scan,
        diagnostics=diagnostics,
    )
    scanned_count = int(diagnostics.get("ledger_rows_scanned", 0) or 0)
    if not history:
        return PublicAssessmentSelection(scanned_count=scanned_count)

    request_terms = _public_assessment_terms(request_text)
    process_request = bool(
        _PUBLIC_ASSESSMENT_PROCESS_QUERY_RE.search(
            str(request_text or "")
        )
    )
    target_terms = set(request_terms)
    if process_request:
        target_terms.update(_PUBLIC_ASSESSMENT_PROCESS_TERMS)

    occurrence_terms: dict[str, set[str]] = {}
    prepared: list[dict[str, Any]] = []
    seen_text: set[str] = set()
    for recency_rank, entry in enumerate(history):
        text = _public_assessment_text(
            str(entry.get("normalized_value") or "")
        )
        occurrence = str(entry.get("occurrence_identity") or "")
        normalized = re.sub(r"\W+", " ", text.lower()).strip()
        if (
            not text
            or not occurrence
            or not normalized
            or normalized in seen_text
        ):
            continue
        terms = _public_assessment_terms(text)
        if not terms:
            continue
        seen_text.add(normalized)
        occurrence_terms.setdefault(occurrence, set()).update(terms)
        prepared.append(
            {
                "entry": entry,
                "text": text,
                "terms": terms,
                "occurrence": occurrence,
                "recency_rank": recency_rank,
            }
        )

    term_occurrence_frequency: Counter[str] = Counter()
    for terms in occurrence_terms.values():
        term_occurrence_frequency.update(terms)

    for candidate in prepared:
        terms = set(candidate["terms"])
        direct_overlap = terms.intersection(request_terms)
        target_overlap = terms.intersection(target_terms)
        recurrent_score = sum(
            min(3, max(0, int(term_occurrence_frequency[term]) - 1))
            for term in terms
        )
        candidate["request_relevant"] = bool(target_overlap)
        candidate["base_score"] = (
            10.0 * len(direct_overlap)
            + 5.0 * len(target_overlap - direct_overlap)
            + min(18.0, float(recurrent_score))
            + max(
                0.0,
                2.0
                - (
                    float(candidate["recency_rank"])
                    / max(1.0, float(len(prepared)))
                ),
            )
        )

    selected: list[dict[str, Any]] = []
    selected_occurrences: set[str] = set()
    covered_terms: set[str] = set()
    relevant_available = sum(
        1 for candidate in prepared if candidate["request_relevant"]
    )
    required_relevant = (
        min(3, relevant_available) if process_request else 0
    )
    safe_max = max(
        1,
        min(
            int(max_results or _PUBLIC_ASSESSMENT_MAX_RESULTS),
            8,
        ),
    )
    while len(selected) < safe_max:
        need_relevant = sum(
            1 for candidate in selected if candidate["request_relevant"]
        ) < required_relevant
        ranked: list[tuple[float, int, str, dict[str, Any]]] = []
        for candidate in prepared:
            occurrence = str(candidate["occurrence"])
            if candidate in selected or occurrence in selected_occurrences:
                continue
            terms = set(candidate["terms"])
            new_terms = terms - covered_terms
            repeated_terms = terms.intersection(covered_terms)
            adjusted = (
                float(candidate["base_score"])
                + min(8.0, 0.75 * len(new_terms))
                - min(6.0, 0.5 * len(repeated_terms))
            )
            if need_relevant:
                adjusted += (
                    24.0 if candidate["request_relevant"] else -24.0
                )
            ranked.append(
                (
                    -adjusted,
                    int(candidate["recency_rank"]),
                    str(candidate["entry"].get("entry_id") or ""),
                    candidate,
                )
            )
        if not ranked:
            break
        chosen = sorted(ranked, key=lambda value: value[:3])[0][3]
        selected.append(chosen)
        selected_occurrences.add(str(chosen["occurrence"]))
        covered_terms.update(chosen["terms"])

    items = tuple(
        PublicAssessmentEvidence(
            entry_id=str(candidate["entry"].get("entry_id") or ""),
            text=str(candidate["text"]),
            observed_at=str(
                candidate["entry"].get("observed_at") or ""
            ),
            visibility=str(
                candidate["entry"].get("visibility") or "unknown"
            ),
            occurrence_identity=str(candidate["occurrence"]),
            score=float(candidate["base_score"]),
            request_relevant=bool(candidate["request_relevant"]),
            subject_key=str(subject_key or ""),
            source_role=str(candidate["entry"].get("source_role") or ""),
            source_class=str(candidate["entry"].get("source_class") or ""),
            lifecycle_status=str(
                candidate["entry"].get("lifecycle_status") or ""
            ),
            channel_policy=str(
                candidate["entry"].get("channel_policy") or "unknown"
            ),
            public_usable=bool(candidate["entry"].get("public_usable")),
            subject_authored=bool(
                str(candidate["entry"].get("subject_key") or "")
                == str(subject_key or "")
            ),
            selector_eligible=True,
            derived=bool(candidate["entry"].get("derived")),
            projection=bool(candidate["entry"].get("projection")),
        )
        for candidate in selected
        if str(candidate["entry"].get("entry_id") or "")
    )
    return PublicAssessmentSelection(
        scanned_count=scanned_count,
        eligible_count=len(prepared),
        request_relevant_count=relevant_available,
        items=items,
    )


def _conversation_projection_columns(
    conn: sqlite3.Connection,
) -> set[str]:
    if not conn.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='conversations'
        """
    ).fetchone():
        return set()
    return {
        str(row[1] or "")
        for row in conn.execute(
            "PRAGMA table_info(conversations)"
        ).fetchall()
        if len(row) > 1 and str(row[1] or "")
    }


def project_retained_conversations_to_ledger(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int = _CONVERSATION_MOTIF_MAX_SCAN,
    diagnostics: dict[str, int] | None = None,
    environ: dict[str, str] | None = None,
) -> dict[str, int]:
    """Project retained public rows through the existing Ledger write owner.

    The operation is bounded, subject-isolated, and idempotent. It is enabled
    only with the recurring-conversation formation gate, so ordinary startup
    and normal chat do not silently backfill production history.
    """
    counts: dict[str, int] = {}
    if not conversation_motif_formation_enabled(environ):
        counts["projection_gate_disabled"] = 1
        if diagnostics is not None:
            diagnostics.update(counts)
        return counts
    if (
        not int(guild_id or 0)
        or not str(subject_key or "").startswith("discord_user:")
    ):
        counts["projection_scope_invalid"] = 1
        if diagnostics is not None:
            diagnostics.update(counts)
        return counts
    try:
        subject_user_id = int(str(subject_key).split(":", 1)[1])
    except (IndexError, TypeError, ValueError):
        counts["projection_scope_invalid"] = 1
        if diagnostics is not None:
            diagnostics.update(counts)
        return counts
    columns = _conversation_projection_columns(conn)
    required = {
        "id",
        "guild_id",
        "user_id",
        "role",
        "content",
        "channel_policy",
    }
    if not required.issubset(columns):
        counts["retained_source_unavailable"] = 1
        if diagnostics is not None:
            diagnostics.update(counts)
        return counts
    ensure_memory_ledger_schema(conn)
    safe_limit = max(
        1,
        min(
            int(max_scan or _CONVERSATION_MOTIF_MAX_SCAN),
            _CONVERSATION_MOTIF_MAX_SCAN,
        ),
    )
    counts["retained_rows_total"] = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM conversations
            WHERE guild_id=? AND user_id=? AND role='user'
            """,
            (int(guild_id or 0), subject_user_id),
        ).fetchone()[0]
        or 0
    )
    optional = {
        "user_name": "''",
        "channel_name": "''",
        "channel_id": "0",
        "message_id": "NULL",
        "timestamp": "''",
    }
    select = [
        "id",
        "user_id",
        (
            "user_name"
            if "user_name" in columns
            else "%s AS user_name" % optional["user_name"]
        ),
        "content",
        (
            "channel_name"
            if "channel_name" in columns
            else "%s AS channel_name" % optional["channel_name"]
        ),
        "channel_policy",
        (
            "channel_id"
            if "channel_id" in columns
            else "%s AS channel_id" % optional["channel_id"]
        ),
        (
            "message_id"
            if "message_id" in columns
            else "%s AS message_id" % optional["message_id"]
        ),
        (
            "timestamp"
            if "timestamp" in columns
            else "%s AS timestamp" % optional["timestamp"]
        ),
    ]
    order = (
        "timestamp DESC,id DESC"
        if "timestamp" in columns
        else "id DESC"
    )
    rows = conn.execute(
        """
        SELECT %s
        FROM conversations
        WHERE guild_id=? AND user_id=? AND role='user'
        ORDER BY %s
        LIMIT ?
        """
        % (",".join(select), order),
        (int(guild_id or 0), subject_user_id, safe_limit),
    ).fetchall()
    counts["retained_rows_scanned"] = len(rows)
    counts["retained_rows_outside_scan"] = max(
        0,
        counts["retained_rows_total"] - len(rows),
    )
    existing = {
        str(row[0] or "")
        for row in conn.execute(
            """
            SELECT source_row_id
            FROM memory_ledger_entries
            WHERE guild_id=? AND subject_key=?
              AND source_table='conversations' AND source_role='user'
            """,
            (int(guild_id or 0), str(subject_key or "")),
        ).fetchall()
        if str(row[0] or "")
    }
    keys = (
        "id",
        "user_id",
        "user_name",
        "content",
        "channel_name",
        "channel_policy",
        "channel_id",
        "message_id",
        "timestamp",
    )
    for raw in reversed(rows):
        row = dict(zip(keys, raw))
        row_id = int(row.get("id") or 0)
        policy = _canon(row.get("channel_policy"))
        if (
            row_id <= 0
            or not str(row.get("content") or "").strip()
        ):
            counts["retained_rows_invalid"] = (
                int(counts.get("retained_rows_invalid", 0) or 0) + 1
            )
            continue
        if policy not in _CONVERSATION_MOTIF_PUBLIC_POLICIES:
            counts["retained_rows_policy_excluded"] = (
                int(
                    counts.get(
                        "retained_rows_policy_excluded",
                        0,
                    )
                    or 0
                )
                + 1
            )
            continue
        counts["retained_rows_public_safe"] = (
            int(counts.get("retained_rows_public_safe", 0) or 0) + 1
        )
        if str(row_id) in existing:
            counts["ledger_projection_existing"] = (
                int(
                    counts.get(
                        "ledger_projection_existing",
                        0,
                    )
                    or 0
                )
                + 1
            )
            continue
        result = shadow_conversation_row(
            conn,
            row_id=row_id,
            user_id=subject_user_id,
            user_name=str(row.get("user_name") or ""),
            guild_id=int(guild_id or 0),
            role="user",
            content=str(row.get("content") or "")[:1000],
            channel_name=str(row.get("channel_name") or "")[:80],
            channel_policy=policy,
            channel_id=int(row.get("channel_id") or 0),
            message_id=(
                int(row.get("message_id") or 0) or None
            ),
            route_mode="conversation_continuity",
            observed_at=str(row.get("timestamp") or ""),
            source_sequence=(
                int(row.get("message_id") or 0) or row_id
            ),
            environ=environ,
        )
        outcome = str(result.outcome or "unknown")
        key = "ledger_projection_%s" % outcome
        counts[key] = int(counts.get(key, 0) or 0) + 1
    if diagnostics is not None:
        diagnostics.update(counts)
    return counts


def form_atomic_candidates_from_recurring_conversation(
    conn: sqlite3.Connection,
    *,
    guild_id: int = 0,
    subject_key: str = "",
    trigger_entry_id: str = "",
    max_scan: int = _CONVERSATION_MOTIF_MAX_SCAN,
    environ: dict[str, str] | None = None,
    diagnostics: dict[str, int] | None = None,
) -> list[AtomicKnowledgeResult]:
    """Form conservative motifs from repeated production-shaped public chat.

    This is a bounded, subject-scoped bridge into the existing atomic
    lifecycle. It never turns one exchange into recurrence and never promotes
    raw text into scalar identity or role facts.
    """
    if not conversation_motif_formation_enabled(environ):
        return []
    ensure_memory_ledger_schema(conn)
    trigger_id = str(trigger_entry_id or "").strip()
    if trigger_id:
        trigger = _knowledge_entry_rows(conn, (trigger_id,)).get(trigger_id)
        if not trigger:
            return []
        guild_id = int(trigger.get("guild_id") or 0)
        subject_key = str(trigger.get("subject_key") or "")
        if (
            str(trigger.get("source_table") or "") != "conversations"
            or str(trigger.get("source_role") or "").lower() != "user"
            or str(trigger.get("entry_type") or "") != "observation"
            or str(trigger.get("predicate_key") or "") != "conversation"
        ):
            return []
    if (
        not int(guild_id or 0)
        or not str(subject_key or "").startswith("discord_user:")
    ):
        return []

    project_retained_conversations_to_ledger(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key),
        max_scan=max_scan,
        diagnostics=diagnostics,
        environ=environ,
    )
    _sync_bounded_conversation_motif_corrections(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key),
        max_scan=max_scan,
    )
    history = _conversation_motif_history(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key),
        max_scan=max_scan,
        diagnostics=diagnostics,
    )
    if not history:
        _record_knowledge_receipt(
            conn,
            guild_id=int(guild_id or 0),
            event_type="formation_skipped",
            reason_code="conversation_motif_no_eligible_history",
            candidate_type="topic_or_motif",
        )
        return []

    grouped: dict[str, dict[str, Any]] = {}
    for entry in history:
        matches = _conversation_motif_family_matches(
            tuple(entry.get("terms") or ())
        )
        if not matches and diagnostics is not None:
            diagnostics["ledger_rows_family_unmatched"] = (
                int(
                    diagnostics.get(
                        "ledger_rows_family_unmatched",
                        0,
                    )
                    or 0
                )
                + 1
            )
        for family, label in matches:
            group = grouped.setdefault(
                "family:%s" % family,
                {
                    "predicate": "conversation_motif_%s" % family,
                    "label": label,
                    "entries": [],
                    "tags": (family, "recurring_public_conversation"),
                },
            )
            group["entries"].append(entry)
    if diagnostics is not None:
        diagnostics["motif_families_matched"] = len(grouped)

    results: list[AtomicKnowledgeResult] = []
    ranked_groups: list[tuple[int, float, str, dict[str, Any]]] = []
    for group_key, group in grouped.items():
        filtered_entries, correction_fence = (
            _conversation_motif_entries_after_fence(
                conn,
                guild_id=int(guild_id or 0),
                subject_key=str(subject_key),
                predicate_key=str(group["predicate"]),
                entries=list(group["entries"]),
            )
        )
        group["entries"] = filtered_entries
        group["correction_fence"] = correction_fence
        _root_ids, occurrence_ids = _conversation_motif_roots_by_occurrence(
            list(group["entries"])
        )
        last_seen = max(
            (
                str(entry.get("observed_at") or "")
                for entry in group["entries"]
            ),
            default="",
        )
        last_seen_time = _parse_knowledge_time(last_seen)
        ranked_groups.append(
            (
                -len(occurrence_ids),
                -(last_seen_time.timestamp() if last_seen_time else 0.0),
                group_key,
                group,
            )
        )
    for _count, _last_seen, _group_key, group in sorted(
        ranked_groups,
        key=lambda row: (row[0], row[1], row[2]),
    ):
        root_ids, occurrence_ids = _conversation_motif_roots_by_occurrence(
            list(group["entries"])
        )
        if len(occurrence_ids) < 2 or len(root_ids) < 2:
            if diagnostics is not None:
                diagnostics["motif_families_recurrence_not_met"] = (
                    int(
                        diagnostics.get(
                            "motif_families_recurrence_not_met",
                            0,
                        )
                        or 0
                    )
                    + 1
                )
            continue
        display_name = next(
            (
                str(entry.get("subject_display_name") or "")
                for entry in group["entries"]
                if str(entry.get("subject_display_name") or "")
            ),
            "",
        )
        result = form_atomic_knowledge_candidate(
            conn,
            AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key=str(subject_key),
                subject_display_name=display_name,
                predicate_key=str(group["predicate"]),
                meaning=(
                    "Recurring public conversation about %s."
                    % str(group["label"]).strip()
                ),
                root_entry_ids=root_ids,
                participant_keys=(str(subject_key),),
                epistemic_status="observed",
                uncertainty_note=(
                    "Repeated public conversation observation; not a scalar "
                    "identity fact or exact quote."
                ),
                currentness="historical",
                contradiction_key=(
                    "%s:%s" % (str(subject_key), str(group["predicate"]))
                ),
                retrieval_tags=tuple(group["tags"]),
            ),
        )
        _finalize_conversation_motif_refresh(
            conn,
            guild_id=int(guild_id or 0),
            subject_key=str(subject_key),
            predicate_key=str(group["predicate"]),
            result=result,
            root_entry_ids=root_ids,
            correction_fence=dict(group.get("correction_fence") or {}),
        )
        results.append(result)
        if len(results) >= _CONVERSATION_MOTIF_MAX_CANDIDATES:
            break
    if diagnostics is not None:
        diagnostics["motif_candidates_returned"] = len(results)
    if not results:
        _record_knowledge_receipt(
            conn,
            guild_id=int(guild_id or 0),
            event_type="formation_skipped",
            reason_code="conversation_motif_recurrence_not_met",
            candidate_type="topic_or_motif",
            root_entry_ids=tuple(
                str(entry.get("entry_id") or "")
                for entry in history[:16]
                if str(entry.get("entry_id") or "")
            ),
        )
    return results


def form_atomic_candidates_from_moment(
    conn: sqlite3.Connection,
    moment_id: str,
) -> list[AtomicKnowledgeResult]:
    """Project participant gists while retaining every actual human root."""
    ensure_memory_ledger_schema(conn)
    required_tables = {
        "memory_moment_windows",
        "memory_moment_contributions",
        "memory_moment_contribution_sources",
    }
    existing_tables = {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if not required_tables.issubset(existing_tables):
        return []
    window = conn.execute(
        """
        SELECT guild_id,topic_key,topic_family,lifecycle_status,
               canonical_ledger_entry_id,last_activity_at,channel_policy,
               route_mode,visibility
        FROM memory_moment_windows WHERE moment_id=?
        """,
        (moment_id,),
    ).fetchone()
    if not window or str(window[3] or "") != "finalized":
        return []
    canonical_entry_id = str(window[4] or "")
    if not canonical_entry_id:
        return []
    results: list[AtomicKnowledgeResult] = []
    contributions = conn.execute(
        """
        SELECT participant_key,contribution_gist,frame_type,lifecycle_status,
               public_usable
        FROM memory_moment_contributions
        WHERE moment_id=?
        ORDER BY participant_key
        """,
        (moment_id,),
    ).fetchall()
    for participant_key, gist, frame_type, lifecycle_status, public_usable in contributions:
        participant_key = str(participant_key or "")
        if (
            not participant_key
            or not str(gist or "").strip()
            or str(lifecycle_status or "") != REVIEW_ONLY_LIFECYCLE
            or not bool(public_usable)
        ):
            continue
        root_ids = tuple(
            str(row[0])
            for row in conn.execute(
                """
                SELECT ledger_entry_id
                FROM memory_moment_contribution_sources
                WHERE moment_id=? AND participant_key=?
                ORDER BY ledger_entry_id
                """,
                (moment_id, participant_key),
            ).fetchall()
        )
        frame_type = str(frame_type or "observation")
        if frame_type in {"question", "question_thread", "plan", "proposal", "conditional_plan"}:
            candidate_type = "open_loop_or_question"
            currentness = "open"
            epistemic = "question" if frame_type.startswith("question") else "source_abstraction"
        elif frame_type in {
            "correction",
            "correction_replacement",
            "replacement",
            "disagreement",
            "rejection",
        }:
            candidate_type = "inference_or_contested_claim"
            currentness = "uncertain"
            epistemic = "contested"
        elif frame_type == "preference":
            candidate_type = "person_role_fact"
            currentness = "current"
            epistemic = "source_abstraction"
        else:
            candidate_type = "topic_or_motif"
            currentness = "historical"
            epistemic = "source_abstraction"
        result = form_atomic_knowledge_candidate(
            conn,
            AtomicKnowledgeProposal(
                candidate_type=candidate_type,
                subject_key=participant_key,
                predicate_key=f"moment_{frame_type}_{window[1]}",
                meaning=str(gist or ""),
                root_entry_ids=root_ids,
                derivative_entry_ids=(canonical_entry_id,),
                participant_keys=(participant_key,),
                epistemic_status=epistemic,
                uncertainty_note=(
                    "Participant-specific Moment abstraction; not an exact quote."
                ),
                currentness=currentness,
                contradiction_key=f"{participant_key}:{frame_type}:{window[1]}",
                retrieval_tags=(
                    str(window[1] or ""),
                    str(window[2] or ""),
                    frame_type,
                    candidate_type,
                ),
            ),
        )
        results.append(result)
    return results


def _merge_backfill_count(counts: dict[str, int], outcome: str) -> None:
    key = str(outcome or "unknown")
    counts[key] = int(counts.get(key, 0) or 0) + 1


def backfill_retained_conversation_ledger_entries(
    conn: sqlite3.Connection,
    *,
    batch_size: int = 1000,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Project one bounded slice of retained public chat into the Ledger.

    This is a resumable repair for conversation rows that predate the Ledger
    adapter. It uses the ordinary Ledger writer, excludes non-public and model
    rows, and explicitly disables motif formation so projection alone cannot
    create or promote a durable member claim.
    """

    env = dict(os.environ if environ is None else environ)
    if not shadow_enabled(env):
        return {
            "migration": RETAINED_CONVERSATION_LEDGER_BACKFILL,
            "phase": "disabled",
            "completed": False,
            "counts": {},
        }
    ensure_memory_ledger_schema(conn)
    columns = _conversation_projection_columns(conn)
    required = {
        "id",
        "guild_id",
        "user_id",
        "role",
        "content",
        "channel_policy",
    }
    if not required.issubset(columns):
        return {
            "migration": RETAINED_CONVERSATION_LEDGER_BACKFILL,
            "phase": "source_unavailable",
            "completed": False,
            "counts": {"retained_source_unavailable": 1},
        }
    safe_batch = max(1, min(int(batch_size or 1000), 2000))
    state = conn.execute(
        """
        SELECT phase,cursor_value,completed,counts_json
        FROM memory_ledger_knowledge_backfill
        WHERE migration_key=?
        """,
        (RETAINED_CONVERSATION_LEDGER_BACKFILL,),
    ).fetchone()
    if state:
        phase = str(state[0] or "conversations")
        cursor = int(state[1] or 0)
        completed = bool(state[2])
        try:
            counts = {
                str(key): int(value or 0)
                for key, value in json.loads(str(state[3] or "{}")).items()
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            counts = {}
    else:
        phase, cursor, completed, counts = "conversations", 0, False, {}
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_backfill(
              migration_key,phase,cursor_value,completed,counts_json,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                RETAINED_CONVERSATION_LEDGER_BACKFILL,
                phase,
                "",
                0,
                "{}",
                _now(),
            ),
        )
    if completed:
        return {
            "migration": RETAINED_CONVERSATION_LEDGER_BACKFILL,
            "phase": phase,
            "completed": True,
            "counts": counts,
        }

    optional = {
        "user_name": "''",
        "channel_name": "''",
        "channel_id": "0",
        "message_id": "NULL",
        "timestamp": "''",
    }
    select = [
        "c.id",
        "c.user_id",
        (
            "c.user_name"
            if "user_name" in columns
            else "%s AS user_name" % optional["user_name"]
        ),
        "c.guild_id",
        "c.content",
        (
            "c.channel_name"
            if "channel_name" in columns
            else "%s AS channel_name" % optional["channel_name"]
        ),
        "c.channel_policy",
        (
            "c.channel_id"
            if "channel_id" in columns
            else "%s AS channel_id" % optional["channel_id"]
        ),
        (
            "c.message_id"
            if "message_id" in columns
            else "%s AS message_id" % optional["message_id"]
        ),
        (
            "c.timestamp"
            if "timestamp" in columns
            else "%s AS timestamp" % optional["timestamp"]
        ),
    ]
    cursor_filter = "AND c.id<?" if cursor > 0 else ""
    parameters: tuple[Any, ...] = (
        (cursor, safe_batch + 1)
        if cursor > 0
        else (safe_batch + 1,)
    )
    rows = conn.execute(
        """
        SELECT %s
        FROM conversations c
        LEFT JOIN memory_ledger_entries entry
          ON entry.guild_id=c.guild_id
         AND entry.source_table='conversations'
         AND entry.source_row_id=CAST(c.id AS TEXT)
         AND entry.source_role='user'
        WHERE c.role='user'
          AND c.guild_id>0 AND c.user_id>0
          AND TRIM(c.content)<>''
          AND c.channel_policy IN (
            'public_home','public_context','public_selective'
          )
          AND entry.entry_id IS NULL
          %s
        ORDER BY c.id DESC
        LIMIT ?
        """
        % (",".join(select), cursor_filter),
        parameters,
    ).fetchall()
    batch = rows[:safe_batch]
    keys = (
        "id",
        "user_id",
        "user_name",
        "guild_id",
        "content",
        "channel_name",
        "channel_policy",
        "channel_id",
        "message_id",
        "timestamp",
    )
    projection_env = dict(env)
    projection_env[CONVERSATION_MOTIF_FORMATION_ENV] = "false"
    for raw in reversed(batch):
        row = dict(zip(keys, raw))
        result = shadow_conversation_row(
            conn,
            row_id=int(row.get("id") or 0),
            user_id=int(row.get("user_id") or 0),
            user_name=str(row.get("user_name") or "")[:120],
            guild_id=int(row.get("guild_id") or 0),
            role="user",
            content=str(row.get("content") or "")[:1000],
            channel_name=str(row.get("channel_name") or "")[:80],
            channel_policy=str(row.get("channel_policy") or ""),
            channel_id=int(row.get("channel_id") or 0),
            message_id=(int(row.get("message_id") or 0) or None),
            route_mode="conversation_continuity",
            observed_at=str(row.get("timestamp") or ""),
            source_sequence=(
                int(row.get("message_id") or 0)
                or int(row.get("id") or 0)
            ),
            environ=projection_env,
        )
        _merge_backfill_count(counts, result.outcome)
    counts["rows_scanned"] = int(counts.get("rows_scanned", 0) or 0) + len(
        batch
    )
    if batch:
        cursor = int(batch[-1][0] or 0)
    completed = len(rows) <= safe_batch
    if completed:
        phase, cursor = "complete", 0
    conn.execute(
        """
        UPDATE memory_ledger_knowledge_backfill
        SET phase=?,cursor_value=?,completed=?,counts_json=?,updated_at=?
        WHERE migration_key=?
        """,
        (
            phase,
            str(cursor or ""),
            1 if completed else 0,
            json.dumps(counts, sort_keys=True),
            _now(),
            RETAINED_CONVERSATION_LEDGER_BACKFILL,
        ),
    )
    return {
        "migration": RETAINED_CONVERSATION_LEDGER_BACKFILL,
        "phase": phase,
        "completed": completed,
        "counts": counts,
    }


def backfill_atomic_knowledge_candidates(
    conn: sqlite3.Connection,
    *,
    batch_size: int = 250,
) -> dict[str, Any]:
    """Run one bounded, resumable historical pass; ordinary writes stay live."""
    ensure_memory_ledger_schema(conn)
    safe_batch = max(1, min(int(batch_size or 250), 500))
    state = conn.execute(
        """
        SELECT phase,cursor_value,completed,counts_json
        FROM memory_ledger_knowledge_backfill
        WHERE migration_key=?
        """,
        (ATOMIC_KNOWLEDGE_BACKFILL,),
    ).fetchone()
    if state:
        phase = str(state[0] or "entries")
        cursor = str(state[1] or "")
        completed = bool(state[2])
        try:
            counts = {
                str(key): int(value or 0)
                for key, value in json.loads(str(state[3] or "{}")).items()
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            counts = {}
    else:
        phase, cursor, completed, counts = "entries", "", False, {}
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_backfill(
              migration_key,phase,cursor_value,completed,counts_json,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                ATOMIC_KNOWLEDGE_BACKFILL,
                phase,
                cursor,
                0,
                "{}",
                _now(),
            ),
        )
    if completed:
        return {
            "migration": ATOMIC_KNOWLEDGE_BACKFILL,
            "phase": phase,
            "completed": True,
            "counts": counts,
        }

    if phase == "entries":
        rows = conn.execute(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE entry_id>?
              AND entry_type IN (
                'claim','preference','boundary','goal','open_loop',
                'commitment','unresolved_question','event','canon_reference'
              )
            ORDER BY entry_id
            LIMIT ?
            """,
            (cursor, safe_batch),
        ).fetchall()
        for (entry_id,) in rows:
            result = form_atomic_candidate_from_ledger_entry(
                conn,
                str(entry_id),
            )
            _merge_backfill_count(
                counts,
                result.outcome if result is not None else "not_candidate",
            )
        if rows:
            cursor = str(rows[-1][0] or "")
        if len(rows) < safe_batch:
            phase, cursor = "moments", ""

    if phase == "moments":
        if conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='memory_moment_windows'
            """
        ).fetchone():
            rows = conn.execute(
                """
                SELECT moment_id
                FROM memory_moment_windows
                WHERE moment_id>? AND lifecycle_status='finalized'
                ORDER BY moment_id
                LIMIT ?
                """,
                (cursor, safe_batch),
            ).fetchall()
        else:
            rows = []
        for (moment_id,) in rows:
            results = form_atomic_candidates_from_moment(
                conn,
                str(moment_id),
            )
            if not results:
                _merge_backfill_count(counts, "moment_without_candidate")
            for result in results:
                _merge_backfill_count(counts, result.outcome)
        if rows:
            cursor = str(rows[-1][0] or "")
        if len(rows) < safe_batch:
            phase, cursor, completed = "complete", "", True

    conn.execute(
        """
        UPDATE memory_ledger_knowledge_backfill
        SET phase=?,cursor_value=?,completed=?,counts_json=?,updated_at=?
        WHERE migration_key=?
        """,
        (
            phase,
            cursor,
            1 if completed else 0,
            json.dumps(counts, sort_keys=True),
            _now(),
            ATOMIC_KNOWLEDGE_BACKFILL,
        ),
    )
    return {
        "migration": ATOMIC_KNOWLEDGE_BACKFILL,
        "phase": phase,
        "completed": bool(completed),
        "counts": counts,
    }


def backfill_atomic_knowledge_lifecycle(
    conn: sqlite3.Connection,
    *,
    batch_size: int = 250,
    now: str | datetime | None = None,
) -> dict[str, Any]:
    """Run one bounded, resumable lifecycle pass over existing candidates."""
    ensure_memory_ledger_schema(conn)
    safe_batch = max(1, min(int(batch_size or 250), 500))
    state = conn.execute(
        """
        SELECT phase,cursor_value,completed,counts_json
        FROM memory_ledger_knowledge_backfill
        WHERE migration_key=?
        """,
        (ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL,),
    ).fetchone()
    if state:
        phase = str(state[0] or "candidates")
        cursor = str(state[1] or "")
        completed = bool(state[2])
        try:
            counts = {
                str(key): int(value or 0)
                for key, value in json.loads(str(state[3] or "{}")).items()
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            counts = {}
    else:
        phase, cursor, completed, counts = "candidates", "", False, {}
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_backfill(
              migration_key,phase,cursor_value,completed,counts_json,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL,
                phase,
                cursor,
                0,
                "{}",
                _now(),
            ),
        )
    if completed:
        return {
            "migration": ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL,
            "phase": phase,
            "completed": True,
            "counts": counts,
        }
    rows = conn.execute(
        """
        SELECT candidate_id
        FROM memory_ledger_knowledge_candidates
        WHERE candidate_id>?
        ORDER BY candidate_id
        LIMIT ?
        """,
        (cursor, safe_batch),
    ).fetchall()
    candidate_ids = tuple(str(row[0] or "") for row in rows)
    if candidate_ids:
        result = reconcile_atomic_knowledge_lifecycle(
            conn,
            candidate_ids=candidate_ids,
            now=now,
        )
        counts["scopes_evaluated"] = int(
            counts.get("scopes_evaluated", 0) or 0
        ) + int(result.get("scopes", 0) or 0)
        counts["candidates_evaluated"] = int(
            counts.get("candidates_evaluated", 0) or 0
        ) + int(result.get("candidates", 0) or 0)
        counts["state_changes"] = int(
            counts.get("state_changes", 0) or 0
        ) + int(result.get("state_changes", 0) or 0)
        cursor = candidate_ids[-1]
    if len(rows) < safe_batch:
        phase, cursor, completed = "complete", "", True
    conn.execute(
        """
        UPDATE memory_ledger_knowledge_backfill
        SET phase=?,cursor_value=?,completed=?,counts_json=?,updated_at=?
        WHERE migration_key=?
        """,
        (
            phase,
            cursor,
            1 if completed else 0,
            json.dumps(counts, sort_keys=True),
            _now(),
            ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL,
        ),
    )
    return {
        "migration": ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL,
        "phase": phase,
        "completed": bool(completed),
        "counts": counts,
    }


def sweep_atomic_knowledge_lifecycle(
    conn: sqlite3.Connection,
    *,
    batch_size: int = 100,
    now: str | datetime | None = None,
    min_interval_seconds: int = 900,
) -> dict[str, Any]:
    """Periodically revisit a bounded candidate slice for review/decay."""
    ensure_memory_ledger_schema(conn)
    safe_batch = max(1, min(int(batch_size or 100), 250))
    if isinstance(now, datetime):
        sweep_now = now
    else:
        sweep_now = _parse_knowledge_time(now) if now else None
    sweep_now = sweep_now or datetime.now(timezone.utc)
    if sweep_now.tzinfo is None:
        sweep_now = sweep_now.replace(tzinfo=timezone.utc)
    state = conn.execute(
        """
        SELECT cursor_value,counts_json,updated_at
        FROM memory_ledger_knowledge_backfill
        WHERE migration_key=?
        """,
        (ATOMIC_KNOWLEDGE_LIFECYCLE_SWEEP,),
    ).fetchone()
    if state:
        cursor = str(state[0] or "")
        try:
            counts = {
                str(key): int(value or 0)
                for key, value in json.loads(str(state[1] or "{}")).items()
            }
        except (TypeError, ValueError, json.JSONDecodeError):
            counts = {}
        last_run = _parse_knowledge_time(state[2])
        if (
            last_run is not None
            and max(0, int(min_interval_seconds or 0)) > 0
            and (
                sweep_now.astimezone(timezone.utc) - last_run
            ).total_seconds()
            < max(0, int(min_interval_seconds or 0))
        ):
            return {
                "migration": ATOMIC_KNOWLEDGE_LIFECYCLE_SWEEP,
                "ran": False,
                "cursor": cursor,
                "wrapped": False,
                "counts": counts,
            }
    else:
        cursor, counts = "", {}
        conn.execute(
            """
            INSERT INTO memory_ledger_knowledge_backfill(
              migration_key,phase,cursor_value,completed,counts_json,updated_at
            ) VALUES(?,?,?,?,?,?)
            """,
            (
                ATOMIC_KNOWLEDGE_LIFECYCLE_SWEEP,
                "sweep",
                "",
                0,
                "{}",
                _knowledge_time(sweep_now),
            ),
        )
    rows = conn.execute(
        """
        SELECT candidate_id
        FROM memory_ledger_knowledge_candidates
        WHERE candidate_id>?
        ORDER BY candidate_id
        LIMIT ?
        """,
        (cursor, safe_batch),
    ).fetchall()
    candidate_ids = tuple(str(row[0] or "") for row in rows)
    result = {
        "scopes": 0,
        "candidates": 0,
        "state_changes": 0,
    }
    if candidate_ids:
        result = reconcile_atomic_knowledge_lifecycle(
            conn,
            candidate_ids=candidate_ids,
            now=sweep_now,
        )
    wrapped = len(rows) < safe_batch
    next_cursor = "" if wrapped else candidate_ids[-1]
    counts["runs"] = int(counts.get("runs", 0) or 0) + 1
    counts["scopes_evaluated"] = int(
        counts.get("scopes_evaluated", 0) or 0
    ) + int(result.get("scopes", 0) or 0)
    counts["candidates_evaluated"] = int(
        counts.get("candidates_evaluated", 0) or 0
    ) + int(result.get("candidates", 0) or 0)
    counts["state_changes"] = int(
        counts.get("state_changes", 0) or 0
    ) + int(result.get("state_changes", 0) or 0)
    if wrapped:
        counts["wraps"] = int(counts.get("wraps", 0) or 0) + 1
    conn.execute(
        """
        UPDATE memory_ledger_knowledge_backfill
        SET phase='sweep',cursor_value=?,completed=0,
            counts_json=?,updated_at=?
        WHERE migration_key=?
        """,
        (
            next_cursor,
            json.dumps(counts, sort_keys=True),
            _knowledge_time(sweep_now),
            ATOMIC_KNOWLEDGE_LIFECYCLE_SWEEP,
        ),
    )
    return {
        "migration": ATOMIC_KNOWLEDGE_LIFECYCLE_SWEEP,
        "ran": True,
        "cursor": next_cursor,
        "wrapped": wrapped,
        "counts": counts,
        "last_result": result,
    }


def _purge_atomic_candidate_boundaries_for_subject(
    conn: sqlite3.Connection,
    candidate_ids: Iterable[str],
) -> dict[str, int]:
    """Delete audit boundaries only for an explicit complete-subject purge."""
    clean_ids = tuple(
        sorted(
            {
                str(candidate_id or "")
                for candidate_id in candidate_ids
                if str(candidate_id or "")
            }
        )
    )
    counts = {
        "memory_ledger_knowledge_candidates": 0,
        "memory_ledger_knowledge_roots": 0,
        "memory_ledger_knowledge_participants": 0,
        "memory_ledger_knowledge_receipts": 0,
        "memory_ledger_knowledge_lifecycle_events": 0,
        "memory_ledger_knowledge_lifecycle_roots": 0,
    }
    if not clean_ids:
        return counts
    placeholders = ",".join("?" for _candidate_id in clean_ids)
    lifecycle_event_ids = tuple(
        str(row[0])
        for row in conn.execute(
            """
            SELECT event_id
            FROM memory_ledger_knowledge_lifecycle_events
            WHERE candidate_id IN (%s)
            """ % placeholders,
            clean_ids,
        ).fetchall()
    )
    if lifecycle_event_ids:
        event_placeholders = ",".join(
            "?" for _event_id in lifecycle_event_ids
        )
        counts["memory_ledger_knowledge_lifecycle_roots"] = conn.execute(
            """
            DELETE FROM memory_ledger_knowledge_lifecycle_roots
            WHERE event_id IN (%s)
            """ % event_placeholders,
            lifecycle_event_ids,
        ).rowcount
    counts["memory_ledger_knowledge_lifecycle_events"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_lifecycle_events
        WHERE candidate_id IN (%s)
        """ % placeholders,
        clean_ids,
    ).rowcount
    counts["memory_ledger_knowledge_receipts"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_receipts
        WHERE candidate_id IN (%s)
        """ % placeholders,
        clean_ids,
    ).rowcount
    counts["memory_ledger_knowledge_participants"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_participants
        WHERE candidate_id IN (%s)
        """ % placeholders,
        clean_ids,
    ).rowcount
    counts["memory_ledger_knowledge_roots"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_roots
        WHERE candidate_id IN (%s)
        """ % placeholders,
        clean_ids,
    ).rowcount
    counts["memory_ledger_knowledge_candidates"] = conn.execute(
        """
        DELETE FROM memory_ledger_knowledge_candidates
        WHERE candidate_id IN (%s)
        """ % placeholders,
        clean_ids,
    ).rowcount
    return counts


def purge_atomic_knowledge_for_subject(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
) -> dict[str, int]:
    """Complete-delete helper; ordinary forget uses lifecycle triggers."""
    ensure_memory_ledger_schema(conn)
    candidate_ids = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT candidate_id
            FROM memory_ledger_knowledge_candidates
            WHERE guild_id=? AND subject_key=?
            UNION
            SELECT candidate_id
            FROM memory_ledger_knowledge_participants
            WHERE guild_id=? AND participant_key=?
            """,
            (guild_id, subject_key, guild_id, subject_key),
        ).fetchall()
    }
    counts = _purge_atomic_candidate_boundaries_for_subject(
        conn,
        candidate_ids,
    )
    counts["memory_ledger_conversation_motif_fences"] = conn.execute(
        """
        DELETE FROM memory_ledger_conversation_motif_fences
        WHERE guild_id=? AND subject_key=?
        """,
        (int(guild_id or 0), str(subject_key or "")),
    ).rowcount
    return counts


def _visibility(policy: str) -> Visibility:
    return map_channel_policy_visibility(policy) if has_explicit_channel_policy_mapping(policy) else Visibility.UNKNOWN


def _source_class(route: str, fallback: SourceClass) -> SourceClass:
    return map_route_source_label(route) if has_explicit_route_source_mapping(route) else fallback


def _conversation_correction_topic_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9'-]{2,40}", _canon(value))
        if token not in _CORRECTION_TOPIC_STOPWORDS
    }


def _finalized_conversation_correction_resolution(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    correction_value: str,
    channel_policy: str,
    current_entry_id: str,
) -> tuple[str, tuple[str, ...]]:
    """Resolve only one same-author finalized source; ambiguity never guesses."""
    if (
        not _CONVERSATION_CORRECTION_RE.search(correction_value or "")
        or _canon(channel_policy)
        not in _CONVERSATION_MOTIF_PUBLIC_POLICIES
        or not conn.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='memory_moment_windows'
            """
        ).fetchone()
    ):
        return "", ()
    correction_tokens = _conversation_correction_topic_tokens(
        correction_value
    )
    if not correction_tokens:
        return "", ()
    rows = conn.execute(
        """
        SELECT DISTINCT e.entry_id,e.normalized_value
        FROM memory_ledger_entries e
        JOIN memory_moment_members m
          ON m.ledger_entry_id=e.entry_id
        JOIN memory_moment_windows w
          ON w.moment_id=m.moment_id
        WHERE e.guild_id=? AND e.subject_key=?
          AND e.entry_id<>?
          AND e.source_table='conversations'
          AND e.source_role='user'
          AND e.entry_type='observation'
          AND e.lifecycle_status='active'
          AND e.public_usable=1
          AND e.channel_policy IN (
            'public_home','public_context','public_selective'
          )
          AND w.guild_id=e.guild_id
          AND w.lifecycle_status='finalized'
          AND w.public_usable=1
          AND NOT EXISTS (
            SELECT 1 FROM memory_ledger_lineage l
            WHERE l.guild_id=e.guild_id
              AND l.target_entry_id=e.entry_id
              AND l.lineage_type IN (
                'correction_of','supersedes','retracts'
              )
          )
        ORDER BY e.observed_at DESC,e.source_sequence DESC,e.entry_id DESC
        LIMIT 40
        """,
        (int(guild_id or 0), subject_key, current_entry_id),
    ).fetchall()
    ranked: list[tuple[int, str]] = []
    for entry_id, value in rows:
        overlap = len(
            correction_tokens
            & _conversation_correction_topic_tokens(str(value or ""))
        )
        if overlap > 0:
            ranked.append((overlap, str(entry_id or "")))
    if not ranked:
        return "", ()
    highest = max(score for score, _entry_id in ranked)
    # One shared word is not enough to connect an ordinary correction to an
    # older finalized Moment.  A mistaken supersession is worse than leaving
    # the correction unlinked for later context-aware review.
    if highest < 2:
        return "", ()
    strongest = {
        entry_id
        for score, entry_id in ranked
        if score == highest and entry_id
    }
    if len(strongest) == 1:
        return next(iter(strongest)), ()
    return "", tuple(sorted(strongest))


def _raw_conversation_correction_resolution(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    correction_value: str,
    channel_policy: str,
    current_entry_id: str,
) -> tuple[str, tuple[str, ...]]:
    """Resolve one bounded raw source only when the evidence is unambiguous."""
    if (
        not _CONVERSATION_CORRECTION_RE.search(correction_value or "")
        or _canon(channel_policy)
        not in _CONVERSATION_MOTIF_PUBLIC_POLICIES
    ):
        return "", ()
    correction_tokens = _conversation_correction_topic_tokens(
        correction_value
    )
    current = _knowledge_entry_rows(
        conn,
        (str(current_entry_id or ""),),
    ).get(str(current_entry_id or ""))
    if not correction_tokens or not current:
        return "", ()
    current_sequence = int(current.get("source_sequence") or 0)
    if current_sequence <= 0:
        return "", ()
    rows = conn.execute(
        """
        SELECT e.entry_id,e.normalized_value
        FROM memory_ledger_entries e
        WHERE e.guild_id=? AND e.subject_key=?
          AND e.entry_id<>?
          AND e.source_table='conversations' AND e.source_role='user'
          AND e.entry_type='observation' AND e.predicate_key='conversation'
          AND e.lifecycle_status='active' AND e.public_usable=1
          AND e.derived=0 AND e.projection=0
          AND e.channel_id=? AND e.channel_policy=?
          AND e.visibility IN ('public','public_safe')
          AND e.source_sequence<?
          AND NOT EXISTS (
            SELECT 1 FROM memory_ledger_lineage l
            WHERE l.guild_id=e.guild_id
              AND l.target_entry_id=e.entry_id
              AND l.lineage_type IN (
                'correction_of','supersedes','retracts'
              )
          )
        ORDER BY e.source_sequence DESC,e.entry_id DESC
        LIMIT ?
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            str(current_entry_id or ""),
            int(current.get("channel_id") or 0),
            _canon(channel_policy),
            current_sequence,
            _CONVERSATION_CORRECTION_MAX_SCAN,
        ),
    ).fetchall()
    ranked: list[tuple[int, str]] = []
    for entry_id, value in rows:
        value = str(value or "")
        if _CONVERSATION_CORRECTION_RE.search(value):
            continue
        overlap = len(
            correction_tokens
            & _conversation_correction_topic_tokens(value)
        )
        if overlap > 0:
            ranked.append((overlap, str(entry_id or "")))
    if not ranked:
        return "", ()
    highest = max(score for score, _entry_id in ranked)
    if highest < 2:
        return "", ()
    strongest = {
        entry_id
        for score, entry_id in ranked
        if score == highest and entry_id
    }
    if len(strongest) == 1:
        return next(iter(strongest)), ()
    return "", tuple(sorted(strongest))


def _public_ok(subject_key: str, predicate: str, value: str, source_class: SourceClass, visibility: Visibility, confidence: Confidence, *, valid: bool = True, projection: bool = False) -> bool:
    claim = SourceClaim(stable_entry_id(guild_id=0, source_table="eval", source_row_id=0, entry_type="claim", subject_key=subject_key, predicate_key=predicate), SubjectIdentity(subject_key, subject_key), predicate, value, source_class, visibility, confidence, valid=valid, projection=projection)
    return is_public_usable(claim)



def _current_first_party_fact(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_key: str,
) -> tuple[str, str]:
    ensure_memory_ledger_schema(conn)
    rows = conn.execute(
        """
        SELECT entry_id, normalized_value
        FROM memory_ledger_entries
        WHERE guild_id=? AND subject_key=? AND predicate_key=?
          AND source_class IN ('first_party_record','owner_correction')
          AND lifecycle_status='active'
          AND entry_id NOT IN (SELECT target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND lineage_type IN ('supersedes','retracts'))
        ORDER BY observed_at DESC, created_at DESC, entry_id DESC
        """,
        (guild_id, subject_key, predicate_key, guild_id),
    ).fetchall()
    if not rows:
        return "", ""
    return str(rows[0][0] or ""), str(rows[0][1] or "")


def shadow_conversation_row(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    user_id: int,
    user_name: str,
    guild_id: int,
    role: str,
    content: str,
    channel_name: str = "",
    channel_policy: str = "unknown",
    channel_id: int = 0,
    message_id: int | None = None,
    route_mode: str = "unknown",
    observed_at: str = "",
    source_sequence: int | None = None,
    conversation_target_user_ids: tuple[int, ...] = (),
    environ: dict[str, str] | None = None,
) -> LedgerWriteResult:
    role_norm = (role or "").lower()
    visibility = _visibility(channel_policy)
    if role_norm != "user":
        subject_key = BNL_SUBJECT_KEY
        target_user_ids = tuple(
            sorted(
                {
                    int(target_user_id)
                    for target_user_id in (
                        conversation_target_user_ids
                        or ((user_id,) if int(user_id or 0) > 0 else ())
                    )
                    if int(target_user_id or 0) > 0
                }
            )
        )
        participants = (
            LedgerParticipant(BNL_SUBJECT_KEY, "BNL-01", "author", 0),
            *tuple(
                LedgerParticipant(
                    subject_key_for_user(target_user_id),
                    "",
                    "conversation_target",
                    index,
                )
                for index, target_user_id in enumerate(
                    target_user_ids,
                    start=1,
                )
            ),
        )
        entry = LedgerEntry(guild_id=guild_id, source_table="conversations", source_row_id=row_id, source_revision=str(row_id), source_role="model", entry_type="derived_summary", subject_key=BNL_SUBJECT_KEY, subject_display_name="BNL-01", predicate_key="model_output", value=(content or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, route_mode=route_mode, channel_id=channel_id, channel_name=channel_name, channel_policy=channel_policy, source_message_id=message_id, visibility=visibility, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=0.1, observed_at=observed_at or _now(), source_sequence=int(source_sequence or row_id), participants=participants)
        return insert_ledger_entry(conn, entry)
    subject_key = subject_key_for_user(user_id)
    source_class = _source_class("conversation_continuity", SourceClass.PUBLIC_OBSERVATION)
    value = (content or "")[:500]
    public_ok = _public_ok(
        subject_key,
        "conversation",
        value,
        source_class,
        visibility,
        Confidence.MEDIUM,
    )
    result = insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            source_role="user",
            entry_type="observation",
            subject_key=subject_key,
            subject_display_name=user_name or "",
            predicate_key="conversation",
            value=value,
            source_class=source_class,
            route_mode=route_mode,
            channel_id=channel_id,
            channel_name=channel_name,
            channel_policy=channel_policy,
            source_message_id=message_id,
            visibility=visibility,
            confidence=Confidence.MEDIUM,
            public_usable=public_ok,
            salience=0.2,
            observed_at=observed_at or _now(),
            source_sequence=int(source_sequence or row_id),
            participants=(LedgerParticipant(subject_key, user_name or "", "author", 0),),
        ),
    )
    if result.outcome == "inserted":
        (
            correction_target,
            ambiguous_correction_targets,
        ) = _finalized_conversation_correction_resolution(
            conn,
            guild_id=guild_id,
            subject_key=subject_key,
            correction_value=value,
            channel_policy=channel_policy,
            current_entry_id=result.entry_id,
        )
        if (
            not correction_target
            and not ambiguous_correction_targets
            and conversation_motif_formation_enabled(environ)
        ):
            (
                correction_target,
                ambiguous_correction_targets,
            ) = _raw_conversation_correction_resolution(
                conn,
                guild_id=guild_id,
                subject_key=subject_key,
                correction_value=value,
                channel_policy=channel_policy,
                current_entry_id=result.entry_id,
            )
        if correction_target:
            now = _now()
            for lineage_type in ("correction_of", "supersedes"):
                conn.execute(
                    """
                    INSERT OR IGNORE INTO memory_ledger_lineage
                      (entry_id,guild_id,lineage_type,target_entry_id,created_at)
                    VALUES (?,?,?,?,?)
                    """,
                    (
                        result.entry_id,
                        int(guild_id or 0),
                        lineage_type,
                        correction_target,
                        now,
                    ),
                )
        elif (
            ambiguous_correction_targets
            and conn.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type='table' AND name='memory_moment_windows'
                """
            ).fetchone()
        ):
            placeholders = ",".join(
                "?" for _target in ambiguous_correction_targets
            )
            conn.execute(
                f"""
                UPDATE memory_moment_windows
                SET lifecycle_status='needs_review',updated_at=?
                WHERE guild_id=? AND lifecycle_status='finalized'
                  AND moment_id IN (
                    SELECT moment_id FROM memory_moment_members
                    WHERE ledger_entry_id IN ({placeholders})
                  )
                """,
                (
                    _now(),
                    int(guild_id or 0),
                    *ambiguous_correction_targets,
                ),
            )
        if (
            _CONVERSATION_CORRECTION_RE.search(value)
            and conversation_motif_formation_enabled(environ)
        ):
            correction_entry = _knowledge_entry_rows(
                conn,
                (result.entry_id,),
            ).get(result.entry_id)
            if correction_entry:
                reason_code = (
                    "conversation_motif_correction"
                    if correction_target
                    else "conversation_motif_correction_ambiguous"
                    if ambiguous_correction_targets
                    else "conversation_motif_correction_unresolved"
                )
                _upsert_conversation_motif_correction_fences(
                    conn,
                    correction_entry=correction_entry,
                    related_entry_ids=tuple(
                        sorted(
                            {
                                str(entry_id or "")
                                for entry_id in (
                                    correction_target,
                                    *ambiguous_correction_targets,
                                )
                                if str(entry_id or "")
                            }
                        )
                    ),
                    reason_code=reason_code,
                )
    return result


def shadow_first_party_user_fact(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    user_id: int,
    user_name: str,
    guild_id: int,
    fact_key: str,
    fact_value: str,
    channel_name: str = "",
    channel_policy: str = "unknown",
    channel_id: int = 0,
    message_id: int | None = None,
    route_mode: str = "unknown",
    observed_at: str = "",
) -> LedgerWriteResult:
    """Project one approved direct self-report from its conversation source.

    This remains a shadow write. The source conversation row is the authority;
    legacy ``user_memory_facts`` is retained only for production compatibility.
    Repetition does not create a stronger entry, while a later direct value
    supersedes the prior current value for that same field.
    """
    key = _canon(fact_key)
    value = re.sub(r"\s+", " ", str(fact_value or "")).strip()[:500]
    if key not in APPROVED_SELF_AUTHORED_FACT_KEYS:
        return skipped_result(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            reason_code="self_authored_fact_not_allowlisted",
        )
    if not value:
        return skipped_result(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            reason_code="empty_self_authored_fact",
        )

    subject_key = subject_key_for_user(user_id)
    prior_entry_id, prior_value = _current_first_party_fact(
        conn,
        guild_id=guild_id,
        subject_key=subject_key,
        predicate_key=key,
    )
    if prior_entry_id and _canon(prior_value) == _canon(value):
        return skipped_result(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            reason_code="repeated_self_authored_value",
        )

    visibility = _visibility(channel_policy)
    source_class = SourceClass.FIRST_PARTY_RECORD
    public_ok = _public_ok(
        subject_key,
        key,
        value,
        source_class,
        visibility,
        Confidence.HIGH,
    )
    lineage = (
        (("supersedes", prior_entry_id), ("correction_of", prior_entry_id))
        if prior_entry_id
        else ()
    )
    result = insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=guild_id,
            source_table="conversations",
            source_row_id=row_id,
            source_revision=str(row_id),
            source_role="member_self_report",
            entry_type="preference",
            subject_key=subject_key,
            subject_display_name=user_name or "",
            predicate_key=key,
            value=value,
            source_class=source_class,
            route_mode=route_mode,
            channel_id=channel_id,
            channel_name=channel_name,
            channel_policy=channel_policy,
            source_message_id=message_id,
            visibility=visibility,
            confidence=Confidence.HIGH,
            public_usable=public_ok,
            salience=0.45,
            observed_at=observed_at or _now(),
            source_sequence=row_id,
            participants=(LedgerParticipant(subject_key, user_name or "", "author", 0),),
            lineage=lineage,
        ),
    )
    if result.outcome == "inserted" and prior_entry_id:
        conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='superseded', updated_at=?
            WHERE guild_id=? AND entry_id=?
            """,
            (_now(), int(guild_id or 0), prior_entry_id),
        )
    return result


def shadow_member_control_fact(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    user_id: int,
    guild_id: int,
    fact_key: str,
    fact_value: str,
    control_ref: str,
    observed_at: str = "",
) -> LedgerWriteResult:
    """Project an explicit member control as first-party authoritative input."""
    key = _canon(fact_key)
    value = re.sub(r"\s+", " ", str(fact_value or "")).strip()[:500]
    revision = "control:" + _canon(control_ref or f"fact:{row_id}")
    if key not in APPROVED_SELF_AUTHORED_FACT_KEYS:
        return skipped_result(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            reason_code="member_control_fact_not_allowlisted",
        )
    if not value:
        return skipped_result(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            reason_code="empty_member_control_fact",
        )
    subject_key = subject_key_for_user(user_id)
    prior_entry_id, prior_value = _current_first_party_fact(
        conn,
        guild_id=guild_id,
        subject_key=subject_key,
        predicate_key=key,
    )
    if prior_entry_id and _canon(prior_value) == _canon(value):
        return skipped_result(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            reason_code="repeated_member_control_value",
        )
    lineage = (
        (("supersedes", prior_entry_id), ("correction_of", prior_entry_id))
        if prior_entry_id
        else ()
    )
    result = insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=guild_id,
            source_table="user_memory_facts",
            source_row_id=row_id,
            source_revision=revision,
            source_event_key=_canon(control_ref),
            source_role="member_control",
            entry_type="preference",
            subject_key=subject_key,
            predicate_key=key,
            value=value,
            source_class=SourceClass.FIRST_PARTY_RECORD,
            route_mode="member_control",
            channel_policy="member_control",
            visibility=Visibility.PUBLIC_SAFE,
            confidence=Confidence.HIGH,
            public_usable=True,
            salience=0.5,
            observed_at=observed_at or _now(),
            source_sequence=int(row_id or 0),
            participants=(
                LedgerParticipant(subject_key, "", "control_actor", 0),
            ),
            lineage=lineage,
        ),
    )
    if result.outcome == "inserted" and prior_entry_id:
        conn.execute(
            """
            UPDATE memory_ledger_entries
            SET lifecycle_status='superseded', updated_at=?
            WHERE guild_id=? AND entry_id=?
            """,
            (_now(), int(guild_id or 0), prior_entry_id),
        )
    return result


def shadow_user_fact_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, fact_key: str, fact_value: str, confidence: float = 0.7, updated_at: str = "") -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="user_memory_facts", source_row_id=row_id, source_revision=rev, source_role="legacy_source_blind", entry_type="claim", subject_key=subject_key_for_user(user_id), predicate_key=fact_key or "legacy_fact", value=(fact_value or "")[:500], source_class=SourceClass.LEGACY_SOURCE_BLIND, visibility=Visibility.PRIVATE, confidence=Confidence.LOW, public_usable=False, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),)))


def _entry_ids_for_source_rows(conn: sqlite3.Connection, *, guild_id: int, source_table: str, source_row_ids: tuple[int, ...]) -> tuple[str, ...]:
    ensure_memory_ledger_schema(conn)
    ids: list[str] = []
    for source_row_id in source_row_ids:
        rows = conn.execute(
            "SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND source_table=? AND source_row_id=? ORDER BY created_at DESC",
            (guild_id, source_table, str(source_row_id)),
        ).fetchall()
        if len(rows) == 1:
            ids.append(rows[0][0])
    return tuple(sorted(set(ids)))


def shadow_memory_tier_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, tier: str, summary: str, salience: float = 0.5, channel_policy: str = "legacy_unknown", topic_key: str = "", updated_at: str = "", derived_from_entry_ids: tuple[str, ...] = (), derived_from_source_row_ids: tuple[int, ...] = ()) -> LedgerWriteResult:
    rev = source_revision_for(row_id, updated_at)
    real_source_ids = _entry_ids_for_source_rows(conn, guild_id=guild_id, source_table="memory_tiers", source_row_ids=derived_from_source_row_ids)
    lineage = tuple(("derived_from", eid) for eid in sorted(set(tuple(derived_from_entry_ids) + real_source_ids)) if eid)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="memory_tiers", source_row_id=row_id, source_revision=rev, source_role="derived_projection", entry_type="derived_summary", subject_key=subject_key_for_user(user_id), predicate_key=topic_key or f"memory_tier:{tier}", value=(summary or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, visibility=Visibility.PRIVATE, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=salience, observed_at=updated_at or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),), lineage=lineage))


def shadow_relationship_journal_row(conn: sqlite3.Connection, *, row_id: int, user_id: int, guild_id: int, entry_type: str, summary: str, timestamp: str = "") -> LedgerWriteResult:
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="relationship_journal", source_row_id=row_id, source_revision=str(row_id), source_role="internal", entry_type="relationship_event", subject_key=subject_key_for_user(user_id), predicate_key=entry_type or "relationship_event", value=(summary or "")[:500], source_class=SourceClass.DERIVED_SUMMARY, visibility=Visibility.INTERNAL, confidence=Confidence.LOW, public_usable=False, derived=True, projection=True, salience=0.3, observed_at=timestamp or _now(), source_sequence=int(row_id or 0), lifecycle_status=REVIEW_ONLY_LIFECYCLE, participants=(LedgerParticipant(subject_key_for_user(user_id), "", "subject", 0),)))


def _unique_entry_for_source(conn: sqlite3.Connection, *, guild_id: int, source_table: str, source_row_id: int | str, preferred_lifecycle: str | None = None) -> str:
    ensure_memory_ledger_schema(conn)
    sql = "SELECT entry_id FROM memory_ledger_entries WHERE guild_id=? AND source_table=? AND source_row_id=?"
    params: list[Any] = [guild_id, source_table, str(source_row_id)]
    if preferred_lifecycle:
        sql += " AND lifecycle_status=?"
        params.append(preferred_lifecycle)
    rows = conn.execute(sql + " ORDER BY created_at DESC", params).fetchall()
    return rows[0][0] if len(rows) == 1 else ""


def shadow_broadcast_memory_row(conn: sqlite3.Connection, *, row_id: int, guild_id: int, cleaned_summary: str, entry_type: str, public_safe: bool, status: str, usage_scope: str, submitted_by_user_id: int | None = None, submitted_by_name: str = "", created_at: str = "", updated_at: str = "", supersedes_id: int | None = None) -> LedgerWriteResult:
    if not cleaned_summary:
        return skipped_result(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, reason_code="empty_cleaned_summary", source_revision=source_revision_for(row_id, updated_at or created_at))
    lifecycle = ACTIVE_LIFECYCLE if str(status or "active").lower() == "active" else (RESOLVED_LIFECYCLE if str(status or "").lower() == "resolved" else REVIEW_ONLY_LIFECYCLE)
    scopes = {scope.strip().lower() for scope in re.split(r"[,\s]+", usage_scope or "") if scope.strip()}
    public_ok = bool(public_safe and lifecycle == ACTIVE_LIFECYCLE and bool(scopes & {"ambient", "direct", "show_status", "relay"}))
    target = _unique_entry_for_source(conn, guild_id=guild_id, source_table="broadcast_memory", source_row_id=supersedes_id, preferred_lifecycle=ACTIVE_LIFECYCLE) if supersedes_id else ""
    lineage = (("supersedes", target), ("correction_of", target)) if target else ()
    rev = source_revision_for(row_id, updated_at or created_at)
    return insert_ledger_entry(conn, LedgerEntry(guild_id=guild_id, source_table="broadcast_memory", source_row_id=row_id, source_revision=rev, source_role="broadcast_memory", entry_type="show_event" if "show" in (entry_type or "") else "event", subject_key="barcode_radio", subject_display_name="BARCODE Radio", predicate_key=entry_type or "broadcast_memory", value=cleaned_summary[:500], source_class=SourceClass.FIRST_PARTY_RECORD, visibility=Visibility.PUBLIC_SAFE if public_ok else Visibility.INTERNAL, confidence=Confidence.HIGH if public_ok else Confidence.MEDIUM, public_usable=public_ok, salience=0.5, observed_at=created_at or updated_at or _now(), source_sequence=int(row_id or 0), freshness=usage_scope or "", lifecycle_status=lifecycle, participants=tuple([LedgerParticipant(f"discord_user:{submitted_by_user_id}", submitted_by_name or "", "submitter", 0)] if submitted_by_user_id else ()), lineage=lineage))


def _unique_broadcast_primary_entry(conn: sqlite3.Connection, *, guild_id: int, source_row_id: int | str) -> str:
    entries = _effective_broadcast_primary_entries(
        conn,
        guild_id=guild_id,
        source_row_id=source_row_id,
    )
    return entries[0] if len(entries) == 1 else ""


@dataclass(frozen=True)
class BroadcastEffectiveRepresentations:
    """Every unretracted Ledger representation of one Broadcast source row."""

    primary_entry_ids: tuple[str, ...] = ()
    declared_projection_entry_ids: tuple[str, ...] = ()

    @property
    def all_entry_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                set(self.primary_entry_ids)
                | set(self.declared_projection_entry_ids)
            )
        )


def _entry_is_unretracted_sql(alias: str) -> str:
    return """
        NOT EXISTS (
            SELECT 1 FROM memory_ledger_lineage AS incoming
            WHERE incoming.guild_id={alias}.guild_id
              AND incoming.target_entry_id={alias}.entry_id
              AND incoming.lineage_type IN ('supersedes','retracts')
        )
    """.format(alias=alias)


def _effective_broadcast_representations(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    source_row_id: int | str,
) -> BroadcastEffectiveRepresentations:
    """Return all effective roots and Declared shadows for one Broadcast row.

    A projection may still be effective after its primary root was already
    retracted or even removed.  Root lineage therefore discovers ordinary
    projections from *all* matching roots, while the authoritative Declared
    sidecar mapping catches an orphan whose root/edge is missing.  Multiplicity
    is preserved for invalidation; this helper never elects one row as truth.
    """

    ensure_memory_ledger_schema(conn)
    normalized_guild_id = int(guild_id or 0)
    normalized_source_row_id = str(source_row_id)
    effective_clause = _entry_is_unretracted_sql("entry")
    primary_rows = conn.execute(
        """
        SELECT entry.entry_id
        FROM memory_ledger_entries AS entry
        WHERE entry.guild_id=?
          AND entry.source_table='broadcast_memory'
          AND entry.source_row_id=?
          AND entry.source_role='broadcast_memory'
          AND %s
        ORDER BY entry.created_at,entry.entry_id
        """ % effective_clause,
        (normalized_guild_id, normalized_source_row_id),
    ).fetchall()
    primary_ids = {
        str(row[0]) for row in primary_rows if str(row[0] or "")
    }

    projection_clause = _entry_is_unretracted_sql("projection_row")
    derived_projection_rows = conn.execute(
        """
        SELECT DISTINCT projection_row.entry_id
        FROM memory_ledger_entries AS projection_row
        JOIN memory_ledger_lineage AS source_edge
          ON source_edge.guild_id=projection_row.guild_id
         AND source_edge.entry_id=projection_row.entry_id
         AND source_edge.lineage_type='derived_from'
        JOIN memory_ledger_entries AS source_root
          ON source_root.guild_id=source_edge.guild_id
         AND source_root.entry_id=source_edge.target_entry_id
        WHERE projection_row.guild_id=?
          AND projection_row.source_table='declared_canon_projection'
          AND projection_row.source_role='declared_canon_projection'
          AND source_root.source_table='broadcast_memory'
          AND source_root.source_row_id=?
          AND source_root.source_role='broadcast_memory'
          AND %s
        ORDER BY projection_row.entry_id
        """ % projection_clause,
        (normalized_guild_id, normalized_source_row_id),
    ).fetchall()
    projection_ids = {
        str(row[0])
        for row in derived_projection_rows
        if str(row[0] or "")
    }

    declared_columns = {
        str(row[1] or "")
        for row in conn.execute("PRAGMA table_info(declared_canon_revisions)")
    }
    if {
        "revision_id",
        "declaration_id",
        "guild_id",
        "source_system",
        "source_row_id",
        "lifecycle_status",
    }.issubset(declared_columns):
        orphan_rows = conn.execute(
            """
            SELECT DISTINCT projection_row.entry_id
            FROM memory_ledger_entries AS projection_row
            JOIN declared_canon_revisions AS revision
              ON revision.guild_id=projection_row.guild_id
             AND revision.declaration_id=projection_row.source_row_id
             AND revision.revision_id=projection_row.source_revision
            WHERE projection_row.guild_id=?
              AND projection_row.source_table='declared_canon_projection'
              AND projection_row.source_role='declared_canon_projection'
              AND revision.source_system='broadcast_memory'
              AND revision.source_row_id=?
              AND revision.lifecycle_status='established'
              AND %s
            ORDER BY projection_row.entry_id
            """ % projection_clause,
            (normalized_guild_id, normalized_source_row_id),
        ).fetchall()
        projection_ids.update(
            str(row[0]) for row in orphan_rows if str(row[0] or "")
        )

    return BroadcastEffectiveRepresentations(
        primary_entry_ids=tuple(sorted(primary_ids)),
        declared_projection_entry_ids=tuple(sorted(projection_ids)),
    )


def _effective_broadcast_primary_entries(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    source_row_id: int | str,
) -> tuple[str, ...]:
    """Return every unretracted primary root for one Broadcast source row."""

    return _effective_broadcast_representations(
        conn,
        guild_id=guild_id,
        source_row_id=source_row_id,
    ).primary_entry_ids


@contextmanager
def _ledger_atomic_projection_transaction(conn: sqlite3.Connection):
    """Hold one snapshot/write transaction without owning a caller's commit."""

    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        if owns_transaction:
            conn.rollback()
        raise
    else:
        if owns_transaction:
            conn.commit()


def _configured_owner_and_guild() -> tuple[int, int]:
    try:
        owner_id = int(os.getenv("BNL_OWNER_USER_ID", "0") or 0)
    except (TypeError, ValueError):
        owner_id = 0
    try:
        primary_guild_id = int(os.getenv("BNL_PRIMARY_GUILD_ID", "0") or 0)
    except (TypeError, ValueError):
        primary_guild_id = 0
    return owner_id, primary_guild_id


def _stored_ledger_entry_matches(
    conn: sqlite3.Connection,
    entry: LedgerEntry,
) -> bool:
    """Prove a deduplicated identity is the exact row we intended to extend."""

    stored = conn.execute(
        """
        SELECT schema_version,guild_id,subject_key,subject_display_name,
               entry_type,predicate_key,normalized_value,source_class,
               source_table,source_row_id,source_revision,source_event_key,
               source_role,route_mode,channel_id,channel_name,channel_policy,
               source_message_id,visibility,confidence,public_usable,derived,
               projection,salience,observed_at,source_sequence,valid_from,
               valid_until,freshness,lifecycle_status
        FROM memory_ledger_entries WHERE entry_id=?
        """,
        (entry.entry_id,),
    ).fetchone()
    expected = (
        MEMORY_LEDGER_SCHEMA_VERSION,
        int(entry.guild_id or 0),
        entry.subject_key,
        entry.subject_display_name,
        entry.entry_type,
        entry.predicate_key,
        entry.value[:1000],
        entry.source_class.value,
        entry.source_table,
        str(entry.source_row_id),
        entry.source_revision,
        entry.source_event_key,
        entry.source_role,
        entry.route_mode,
        int(entry.channel_id or 0),
        entry.channel_name[:120],
        entry.channel_policy[:80],
        entry.source_message_id,
        entry.visibility.value,
        entry.confidence.value,
        1 if entry.public_usable else 0,
        1 if entry.derived else 0,
        1 if entry.projection else 0,
        float(entry.salience or 0.0),
        entry.observed_at,
        entry.source_sequence,
        entry.valid_from,
        entry.valid_until,
        entry.freshness,
        entry.lifecycle_status,
    )
    if stored != expected:
        return False
    expected_participants = {
        (
            participant.participant_key,
            participant.display_name[:120],
            participant.role[:40],
            index,
        )
        for index, participant in enumerate(
            sorted(
                entry.participants,
                key=lambda item: (item.order_index, item.participant_key),
            )
        )
    }
    stored_participants = {
        (str(row[0]), str(row[1] or ""), str(row[2] or ""), int(row[3] or 0))
        for row in conn.execute(
            """
            SELECT participant_key,display_name,participant_role,order_index
            FROM memory_ledger_participants WHERE entry_id=?
            """,
            (entry.entry_id,),
        ).fetchall()
    }
    return stored_participants == expected_participants


def _insert_or_reconcile_ledger_lineage(
    conn: sqlite3.Connection,
    entry: LedgerEntry,
    *,
    conflict_reason: str,
) -> LedgerWriteResult:
    """Insert an entry or safely append missing edges to an exact duplicate.

    Generic Ledger deduplication intentionally does not accept new caller-
    supplied lineage.  Terminal invalidation is the narrow exception: after
    revalidating the complete stored row and participant set, a retry may add
    newly discovered retraction edges with ``INSERT OR IGNORE``.
    """

    result = insert_ledger_entry(conn, entry)
    if result.outcome != "deduplicated":
        return result
    if not _stored_ledger_entry_matches(conn, entry):
        return LedgerWriteResult(
            entry_id=entry.entry_id,
            outcome="error",
            reason_code=conflict_reason,
            source_table=entry.source_table,
            source_row_id=str(entry.source_row_id),
            source_revision=entry.source_revision,
            source_event_key=entry.source_event_key,
            guild_id=int(entry.guild_id or 0),
        )
    now = _now()
    for lineage_type, target in entry.lineage:
        if lineage_type not in LINEAGE_TYPES or not str(target or ""):
            continue
        target_row = conn.execute(
            """
            SELECT 1 FROM memory_ledger_entries
            WHERE guild_id=? AND entry_id=?
            """,
            (int(entry.guild_id or 0), str(target)),
        ).fetchone()
        if target_row is None:
            return LedgerWriteResult(
                entry_id=entry.entry_id,
                outcome="error",
                reason_code="%s_target_missing" % conflict_reason,
                source_table=entry.source_table,
                source_row_id=str(entry.source_row_id),
                source_revision=entry.source_revision,
                source_event_key=entry.source_event_key,
                guild_id=int(entry.guild_id or 0),
            )
        conn.execute(
            """
            INSERT OR IGNORE INTO memory_ledger_lineage
              (entry_id,guild_id,lineage_type,target_entry_id,created_at)
            VALUES(?,?,?,?,?)
            """,
            (
                entry.entry_id,
                int(entry.guild_id or 0),
                lineage_type,
                str(target),
                now,
            ),
        )
    return LedgerWriteResult(
        entry_id=entry.entry_id,
        outcome="deduplicated",
        reason_code="exact_source_duplicate_lineage_reconciled",
        source_table=entry.source_table,
        source_row_id=str(entry.source_row_id),
        source_revision=entry.source_revision,
        source_event_key=entry.source_event_key,
        guild_id=int(entry.guild_id or 0),
    )


def shadow_broadcast_status_event(
    conn: sqlite3.Connection,
    *,
    row_id: int,
    guild_id: int,
    status: str,
    updated_at: str,
    actor_id: int | None = None,
    actor_name: str = "",
    superseded_by_id: int | None = None,
) -> LedgerWriteResult:
    """Project an authenticated, already-applied Broadcast transition.

    The authoritative ``broadcast_memory`` row is re-read in the same SQLite
    snapshot/write transaction as the retraction insert.  Callers that already
    hold the source mutation transaction keep ownership of its commit; direct
    callers receive a local ``BEGIN IMMEDIATE`` boundary.
    """

    try:
        normalized_row_id = int(row_id or 0)
    except (TypeError, ValueError):
        normalized_row_id = 0
    try:
        normalized_guild_id = int(guild_id or 0)
    except (TypeError, ValueError):
        normalized_guild_id = 0
    try:
        normalized_actor_id = int(actor_id or 0)
    except (TypeError, ValueError):
        normalized_actor_id = 0
    normalized_status = str(status or "").strip().casefold()
    normalized_updated_at = str(updated_at or "").strip()
    rev = source_revision_for(
        normalized_row_id,
        normalized_updated_at,
        event="status:%s:%s" % (normalized_status, normalized_updated_at),
    )

    owner_id, primary_guild_id = _configured_owner_and_guild()
    if owner_id <= 0:
        return skipped_result(
            guild_id=normalized_guild_id,
            source_table="broadcast_memory",
            source_row_id=normalized_row_id,
            source_revision=rev,
            source_event_key="status:%s" % normalized_status,
            reason_code="broadcast_status_owner_not_configured",
        )
    if primary_guild_id <= 0:
        return skipped_result(
            guild_id=normalized_guild_id,
            source_table="broadcast_memory",
            source_row_id=normalized_row_id,
            source_revision=rev,
            source_event_key="status:%s" % normalized_status,
            reason_code="broadcast_status_primary_guild_not_configured",
        )
    if normalized_actor_id != owner_id:
        return skipped_result(
            guild_id=normalized_guild_id,
            source_table="broadcast_memory",
            source_row_id=normalized_row_id,
            source_revision=rev,
            source_event_key="status:%s" % normalized_status,
            reason_code="broadcast_status_configured_owner_required",
        )
    if normalized_guild_id != primary_guild_id:
        return skipped_result(
            guild_id=normalized_guild_id,
            source_table="broadcast_memory",
            source_row_id=normalized_row_id,
            source_revision=rev,
            source_event_key="status:%s" % normalized_status,
            reason_code="broadcast_status_primary_guild_required",
        )
    if (
        normalized_row_id <= 0
        or normalized_status not in {"resolved", "superseded"}
        or not normalized_updated_at
    ):
        return skipped_result(
            guild_id=normalized_guild_id,
            source_table="broadcast_memory",
            source_row_id=normalized_row_id,
            source_revision=rev,
            source_event_key="status:%s" % normalized_status,
            reason_code="broadcast_status_snapshot_invalid",
        )

    invalid_superseded_by = False
    if superseded_by_id is None:
        expected_superseded_by = None
    else:
        try:
            expected_superseded_by = int(superseded_by_id)
        except (TypeError, ValueError):
            expected_superseded_by = None
            invalid_superseded_by = True
    if invalid_superseded_by:
        return skipped_result(
            guild_id=normalized_guild_id,
            source_table="broadcast_memory",
            source_row_id=normalized_row_id,
            source_revision=rev,
            source_event_key="status:%s" % normalized_status,
            reason_code="broadcast_status_snapshot_invalid",
        )
    if normalized_status == "superseded":
        if expected_superseded_by is None or expected_superseded_by <= 0:
            return skipped_result(
                guild_id=normalized_guild_id,
                source_table="broadcast_memory",
                source_row_id=normalized_row_id,
                source_revision=rev,
                source_event_key="status:%s" % normalized_status,
                reason_code="broadcast_status_snapshot_invalid",
            )
    elif expected_superseded_by is not None:
        return skipped_result(
            guild_id=normalized_guild_id,
            source_table="broadcast_memory",
            source_row_id=normalized_row_id,
            source_revision=rev,
            source_event_key="status:%s" % normalized_status,
            reason_code="broadcast_status_snapshot_invalid",
        )

    with _ledger_atomic_projection_transaction(conn):
        columns = {
            str(row[1] or "")
            for row in conn.execute("PRAGMA table_info(broadcast_memory)")
        }
        required_columns = {
            "id",
            "guild_id",
            "status",
            "updated_at",
            "corrected_by_user_id",
            "corrected_by_name",
            "superseded_by_id",
        }
        if not required_columns.issubset(columns):
            return skipped_result(
                guild_id=normalized_guild_id,
                source_table="broadcast_memory",
                source_row_id=normalized_row_id,
                source_revision=rev,
                source_event_key="status:%s" % normalized_status,
                reason_code="broadcast_status_source_schema_invalid",
            )
        source_row = conn.execute(
            """
            SELECT guild_id,status,updated_at,corrected_by_user_id,
                   corrected_by_name,superseded_by_id
            FROM broadcast_memory
            WHERE guild_id=? AND id=?
            LIMIT 1
            """,
            (normalized_guild_id, normalized_row_id),
        ).fetchone()
        if source_row is None:
            return skipped_result(
                guild_id=normalized_guild_id,
                source_table="broadcast_memory",
                source_row_id=normalized_row_id,
                source_revision=rev,
                source_event_key="status:%s" % normalized_status,
                reason_code="broadcast_status_source_not_found",
            )
        try:
            source_superseded_by = (
                int(source_row[5]) if source_row[5] is not None else None
            )
        except (TypeError, ValueError):
            source_superseded_by = "invalid"
        if (
            int(source_row[0] or 0) != normalized_guild_id
            or str(source_row[1] or "").strip().casefold() != normalized_status
            or str(source_row[2] or "").strip() != normalized_updated_at
            or int(source_row[3] or 0) != normalized_actor_id
            or source_superseded_by != expected_superseded_by
        ):
            return skipped_result(
                guild_id=normalized_guild_id,
                source_table="broadcast_memory",
                source_row_id=normalized_row_id,
                source_revision=rev,
                source_event_key="status:%s" % normalized_status,
                reason_code="broadcast_status_source_snapshot_mismatch",
            )

        old_entries = _effective_broadcast_primary_entries(
            conn,
            guild_id=normalized_guild_id,
            source_row_id=normalized_row_id,
        )
        lineage_items = [
            ("retracts", old_entry) for old_entry in old_entries
        ]
        if normalized_status == "superseded":
            replacement_entries = _effective_broadcast_primary_entries(
                conn,
                guild_id=normalized_guild_id,
                source_row_id=expected_superseded_by,
            )
            lineage_items.extend(
                ("derived_from", entry_id)
                for entry_id in (*old_entries, *replacement_entries)
            )

        lifecycle = (
            RESOLVED_LIFECYCLE
            if normalized_status == "resolved"
            else REVIEW_ONLY_LIFECYCLE
        )
        return insert_ledger_entry(
            conn,
            LedgerEntry(
                guild_id=normalized_guild_id,
                source_table="broadcast_memory",
                source_row_id=normalized_row_id,
                source_revision=rev,
                source_event_key="status:%s" % normalized_status,
                source_role="broadcast_memory_status",
                entry_type="event",
                subject_key="barcode_radio",
                subject_display_name="BARCODE Radio",
                predicate_key="broadcast_status:%s" % normalized_status,
                value=normalized_status,
                source_class=SourceClass.FIRST_PARTY_RECORD,
                visibility=Visibility.INTERNAL,
                confidence=Confidence.HIGH,
                public_usable=False,
                observed_at=normalized_updated_at,
                source_sequence=normalized_row_id,
                lifecycle_status=lifecycle,
                participants=(
                    LedgerParticipant(
                        "discord_user:%s" % normalized_actor_id,
                        str(source_row[4] or ""),
                        "correction_actor",
                        0,
                    ),
                ),
                lineage=tuple(lineage_items),
            ),
        )


def shadow_canon_reference(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    canon_id: str,
    subject_key: str,
    subject_display_name: str,
    predicate_key: str,
    value: str,
    observed_at: str = "",
    revision_id: str = "",
    root_entry_ids: tuple[str, ...] = (),
) -> LedgerWriteResult:
    """Project an approved canon revision without becoming its authority.

    The canon registry/declaration remains the source of truth.  This Ledger
    row is explicitly derived and projected so it can support governed packet
    lookup while never independently corroborating or re-canonizing itself.
    """
    if not canon_id or not subject_key or not predicate_key:
        return skipped_result(guild_id=guild_id, source_table="canon_claim_projection", source_row_id=canon_id or "", reason_code="missing_canon_source_identity")
    roots = tuple(
        sorted(
            {
                str(entry_id or "").strip()
                for entry_id in root_entry_ids
                if str(entry_id or "").strip()
            }
        )
    )
    revision = str(revision_id or canon_id)
    return insert_ledger_entry(
        conn,
        LedgerEntry(
            guild_id=guild_id,
            source_table="canon_claim_projection",
            source_row_id=canon_id,
            source_revision=revision,
            source_event_key="revision:%s" % revision,
            source_role="canon_projection",
            entry_type="canon_reference",
            subject_key=subject_key,
            subject_display_name=subject_display_name,
            predicate_key=predicate_key,
            value=(value or "")[:500],
            source_class=SourceClass.APPROVED_CANON,
            route_mode="approved_canon",
            channel_policy="reference_canon",
            visibility=Visibility.REFERENCE_CANON,
            confidence=Confidence.APPROVED,
            public_usable=True,
            derived=True,
            projection=True,
            observed_at=observed_at or _now(),
            lifecycle_status=ACTIVE_LIFECYCLE,
            lineage=tuple(("derived_from", entry_id) for entry_id in roots),
        ),
    )


def _declared_projection_subject_key(subject_type: str, subject_id: str) -> str:
    return "%s:%s" % (
        str(subject_type or "").strip().casefold(),
        str(subject_id or "").strip(),
    )


def shadow_declared_canon_projection(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    declaration_id: str,
    revision_id: str,
    actor_user_id: int,
    authority_nonce: str,
    expected_source_fingerprint: str,
    expected_lifecycle_status: str,
    root_entry_ids: tuple[str, ...] = (),
) -> LedgerWriteResult:
    """Project one Declared revision without making it live evidence.

    The append-only declaration or Broadcast row remains authoritative.  PR 2
    deliberately keeps this projection internal, derived, review-only, and
    non-public so existing packet, Journal, Relay, dossier, and site readers
    cannot consume a second representation before final convergence.
    """

    from bnl_declared_canon import (
        BROADCAST_MEMORY_SOURCE,
        DeclaredCanonError,
        validate_current_declared_canon_revision,
        validate_latest_declared_canon_revision,
    )

    declaration_id = str(declaration_id or "").strip()
    revision_id = str(revision_id or "").strip()
    expected_source_fingerprint = str(expected_source_fingerprint or "").strip()
    if not declaration_id or not revision_id or not expected_source_fingerprint:
        return skipped_result(
            guild_id=guild_id,
            source_table="declared_canon_projection",
            source_row_id=declaration_id,
            source_revision=revision_id,
            reason_code="missing_declared_projection_identity",
        )
    normalized_guild_id = int(guild_id or 0)
    normalized_lifecycle = str(expected_lifecycle_status or "").strip().casefold()
    provided_roots = tuple(
        str(entry_id or "").strip()
        for entry_id in root_entry_ids
        if str(entry_id or "").strip()
    )
    roots = tuple(sorted(set(provided_roots)))

    with _ledger_atomic_projection_transaction(conn):
        try:
            if normalized_lifecycle == "established":
                revision = validate_current_declared_canon_revision(
                    conn,
                    actor_user_id=int(actor_user_id or 0),
                    authority_nonce=authority_nonce,
                    guild_id=normalized_guild_id,
                    declaration_id=declaration_id,
                    expected_revision_id=revision_id,
                    expected_source_fingerprint=expected_source_fingerprint,
                )
            else:
                revision = validate_latest_declared_canon_revision(
                    conn,
                    actor_user_id=int(actor_user_id or 0),
                    authority_nonce=authority_nonce,
                    guild_id=normalized_guild_id,
                    declaration_id=declaration_id,
                    expected_revision_id=revision_id,
                    expected_source_fingerprint=expected_source_fingerprint,
                    expected_lifecycle_status=normalized_lifecycle,
                )
        except DeclaredCanonError as exc:
            return skipped_result(
                guild_id=normalized_guild_id,
                source_table="declared_canon_projection",
                source_row_id=declaration_id,
                source_revision=revision_id,
                reason_code=(
                    "declared_projection_%s" % str(exc or "invalid")
                )[:120],
            )

        terminal = revision.lifecycle_status in {
            "contested",
            "resolved",
            "retired",
            "superseded",
        }

        if revision.source_system == BROADCAST_MEMORY_SOURCE:
            if terminal:
                if provided_roots:
                    return skipped_result(
                        guild_id=normalized_guild_id,
                        source_table="declared_canon_projection",
                        source_row_id=declaration_id,
                        source_revision=revision_id,
                        reason_code=(
                            "declared_projection_broadcast_terminal_roots_forbidden"
                        ),
                    )
                # A terminal sidecar carries no Broadcast content.  Its sole
                # purpose is to retract the prior Declared projection after
                # validate_latest_declared_canon_revision has re-intersected
                # the exact terminal source row in this transaction.
                value = revision.lifecycle_status
            elif len(provided_roots) != 1 or len(roots) != 1:
                return skipped_result(
                    guild_id=normalized_guild_id,
                    source_table="declared_canon_projection",
                    source_row_id=declaration_id,
                    source_revision=revision_id,
                    reason_code="declared_projection_broadcast_root_required",
                )
            else:
                source_row = conn.execute(
                    """
                    SELECT cleaned_summary,updated_at,status
                    FROM broadcast_memory
                    WHERE guild_id=? AND id=?
                    """,
                    (normalized_guild_id, int(revision.source_row_id)),
                ).fetchone()
                if (
                    not source_row
                    or not str(source_row[0] or "").strip()
                    or not str(source_row[1] or "").strip()
                    or str(source_row[2] or "").strip().casefold() != "active"
                ):
                    return skipped_result(
                        guild_id=normalized_guild_id,
                        source_table="declared_canon_projection",
                        source_row_id=declaration_id,
                        source_revision=revision_id,
                        reason_code="declared_projection_broadcast_value_missing",
                    )
                value = str(source_row[0])
                expected_root_revision = source_revision_for(
                    revision.source_row_id,
                    str(source_row[1]),
                )
                root_row = conn.execute(
                    """
                    SELECT guild_id,source_table,source_row_id,source_role,
                           source_revision,normalized_value,lifecycle_status
                    FROM memory_ledger_entries
                    WHERE entry_id=?
                    """,
                    (roots[0],),
                ).fetchone()
                if root_row is None or int(root_row[0] or 0) != normalized_guild_id:
                    return skipped_result(
                        guild_id=normalized_guild_id,
                        source_table="declared_canon_projection",
                        source_row_id=declaration_id,
                        source_revision=revision_id,
                        reason_code="declared_projection_root_scope_invalid",
                    )
                if (
                    str(root_row[1] or "") != "broadcast_memory"
                    or str(root_row[2] or "") != revision.source_row_id
                    or str(root_row[3] or "") != "broadcast_memory"
                ):
                    return skipped_result(
                        guild_id=normalized_guild_id,
                        source_table="declared_canon_projection",
                        source_row_id=declaration_id,
                        source_revision=revision_id,
                        reason_code="declared_projection_broadcast_root_required",
                    )
                if (
                    str(root_row[4] or "") != expected_root_revision
                    or str(root_row[5] or "") != value[:500]
                    or str(root_row[6] or "") != ACTIVE_LIFECYCLE
                    or conn.execute(
                        """
                        SELECT 1
                        FROM memory_ledger_lineage
                        WHERE guild_id=? AND target_entry_id=?
                          AND lineage_type IN ('supersedes','retracts')
                        LIMIT 1
                        """,
                        (normalized_guild_id, roots[0]),
                    ).fetchone()
                ):
                    return skipped_result(
                        guild_id=normalized_guild_id,
                        source_table="declared_canon_projection",
                        source_row_id=declaration_id,
                        source_revision=revision_id,
                        reason_code="declared_projection_broadcast_root_stale",
                    )
                current_primary_count = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM memory_ledger_entries AS root
                    WHERE root.guild_id=?
                      AND root.source_table='broadcast_memory'
                      AND root.source_row_id=?
                      AND root.source_role='broadcast_memory'
                      AND root.lifecycle_status='active'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM memory_ledger_lineage AS edge
                        WHERE edge.guild_id=root.guild_id
                          AND edge.target_entry_id=root.entry_id
                          AND edge.lineage_type IN ('supersedes','retracts')
                    )
                    """,
                    (normalized_guild_id, revision.source_row_id),
                ).fetchone()[0]
                if int(current_primary_count or 0) != 1:
                    return skipped_result(
                        guild_id=normalized_guild_id,
                        source_table="declared_canon_projection",
                        source_row_id=declaration_id,
                        source_revision=revision_id,
                        reason_code="declared_projection_broadcast_root_ambiguous",
                    )
        else:
            if provided_roots:
                return skipped_result(
                    guild_id=normalized_guild_id,
                    source_table="declared_canon_projection",
                    source_row_id=declaration_id,
                    source_revision=revision_id,
                    reason_code="declared_projection_general_roots_forbidden",
                )
            value = revision.cleaned_summary or revision.value_json

        previous: tuple[str, ...] = ()
        if revision.previous_revision_id:
            prior_rows = conn.execute(
                """
                SELECT entry_id
                FROM memory_ledger_entries
                WHERE guild_id=?
                  AND source_table='declared_canon_projection'
                  AND source_row_id=?
                  AND source_revision=?
                ORDER BY created_at DESC
                """,
                (
                    normalized_guild_id,
                    declaration_id,
                    revision.previous_revision_id,
                ),
            ).fetchall()
            if len(prior_rows) != 1:
                return skipped_result(
                    guild_id=normalized_guild_id,
                    source_table="declared_canon_projection",
                    source_row_id=declaration_id,
                    source_revision=revision_id,
                    reason_code="declared_projection_previous_revision_missing",
                )
            previous = (str(prior_rows[0][0]),)

        cross_declaration_previous: tuple[str, ...] = ()
        if revision.supersedes_declaration_id:
            superseded_latest = conn.execute(
                """
                SELECT previous_revision_id,lifecycle_status,
                       superseded_by_declaration_id
                FROM declared_canon_revisions
                WHERE guild_id=? AND declaration_id=?
                ORDER BY revision_number DESC
                LIMIT 1
                """,
                (
                    normalized_guild_id,
                    revision.supersedes_declaration_id,
                ),
            ).fetchone()
            if (
                superseded_latest is None
                or str(superseded_latest[1] or "") != "superseded"
                or str(superseded_latest[2] or "") != declaration_id
                or not str(superseded_latest[0] or "")
            ):
                return skipped_result(
                    guild_id=normalized_guild_id,
                    source_table="declared_canon_projection",
                    source_row_id=declaration_id,
                    source_revision=revision_id,
                    reason_code=(
                        "declared_projection_cross_supersession_invalid"
                    ),
                )
            superseded_projection_rows = conn.execute(
                """
                SELECT entry_id
                FROM memory_ledger_entries
                WHERE guild_id=?
                  AND source_table='declared_canon_projection'
                  AND source_row_id=?
                  AND source_revision=?
                ORDER BY created_at DESC
                """,
                (
                    normalized_guild_id,
                    revision.supersedes_declaration_id,
                    str(superseded_latest[0]),
                ),
            ).fetchall()
            if len(superseded_projection_rows) != 1:
                return skipped_result(
                    guild_id=normalized_guild_id,
                    source_table="declared_canon_projection",
                    source_row_id=declaration_id,
                    source_revision=revision_id,
                    reason_code=(
                        "declared_projection_superseded_source_projection_missing"
                    ),
                )
            cross_declaration_previous = (
                str(superseded_projection_rows[0][0]),
            )

        lineage_items = [("derived_from", entry_id) for entry_id in roots]
        lineage_items.extend(
            ("retracts" if terminal else "supersedes", entry_id)
            for entry_id in previous
        )
        lineage_items.extend(
            ("supersedes", entry_id)
            for entry_id in cross_declaration_previous
        )
        lineage = tuple(dict.fromkeys(lineage_items))

        subject_key = _declared_projection_subject_key(
            revision.subject_type,
            revision.subject_id,
        )
        participants = [
            LedgerParticipant(subject_key, "", "subject", 0),
        ]
        if revision.claim_kind == "relationship":
            participants.append(
                LedgerParticipant(
                    _declared_projection_subject_key(
                        revision.object_subject_type,
                        revision.object_subject_id,
                    ),
                    "",
                    "relationship_object",
                    1,
                )
            )

        return insert_ledger_entry(
            conn,
            LedgerEntry(
                guild_id=normalized_guild_id,
                source_table="declared_canon_projection",
                source_row_id=declaration_id,
                source_revision=revision_id,
                source_event_key="revision:%s" % revision_id,
                source_role="declared_canon_projection",
                entry_type="canon_reference",
                subject_key=subject_key,
                subject_display_name="",
                predicate_key=revision.predicate,
                value=(value or "")[:500],
                source_class=SourceClass.EVIDENCE_PROJECTION,
                route_mode="declared_canon_review",
                channel_policy="declared_canon_review",
                visibility=Visibility.INTERNAL,
                confidence=Confidence.LOW,
                public_usable=False,
                derived=True,
                projection=True,
                observed_at=revision.created_at or _now(),
                lifecycle_status=(
                    RESOLVED_LIFECYCLE
                    if revision.lifecycle_status
                    in {"resolved", "retired", "superseded"}
                    else REVIEW_ONLY_LIFECYCLE
                ),
                participants=tuple(participants),
                lineage=lineage,
            ),
        )


def build_memory_ledger_evaluation(
    conn: sqlite3.Connection,
    *,
    guild_id: int | None = None,
    prepare_schema: bool = True,
) -> dict[str, Any]:
    if prepare_schema:
        ensure_memory_ledger_schema(conn)
    params: list[Any] = []
    where = ""
    if guild_id is not None:
        where = " WHERE guild_id=?"
        params.append(guild_id)
    cur = conn.cursor()
    report: dict[str, Any] = {"schemaVersion": MEMORY_LEDGER_SCHEMA_VERSION}
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_shadow_receipts{where}", params)
    report["eligibleLegacyWrites"] = int(cur.fetchone()[0] or 0)
    for outcome, key in (("inserted", "insertedLedgerEntries"), ("deduplicated", "exactSourceDeduplications"), ("error", "shadowWriteErrors")):
        cur.execute(f"SELECT COUNT(*) FROM memory_ledger_shadow_receipts{where + (' AND' if where else ' WHERE')} outcome=?", params + [outcome])
        report[key] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT reason_code, COUNT(*) FROM memory_ledger_shadow_receipts{where + (' AND' if where else ' WHERE')} outcome='skipped' GROUP BY reason_code", params)
    report["skippedWrites"] = dict(cur.fetchall())
    cur.execute(f"SELECT source_table, COUNT(*) FROM memory_ledger_entries{where} GROUP BY source_table", params)
    report["countsBySourceLane"] = dict(cur.fetchall())
    for key, col in (("countsByEntryType", "entry_type"), ("countsByVisibility", "visibility"), ("countsByLifecycle", "lifecycle_status")):
        cur.execute(f"SELECT {col}, COUNT(*) FROM memory_ledger_entries{where} GROUP BY {col}", params)
        report[key] = dict(cur.fetchall())
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} (source_class='legacy_source_blind' OR visibility='unknown')", params)
    report["missingUnmappedProvenance"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} public_usable=0", params)
    report["publicUsabilityRejections"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} entry_id NOT IN (SELECT target_entry_id FROM memory_ledger_lineage WHERE {'guild_id=? AND ' if guild_id is not None else ''} lineage_type IN ('supersedes','retracts')) AND lifecycle_status='active' GROUP BY subject_key, predicate_key HAVING COUNT(*) > 1", params + ([guild_id] if guild_id is not None else []))
    report["entriesWithMultipleActiveValues"] = len(cur.fetchall())
    cur.execute(f"SELECT COUNT(DISTINCT entry_id) FROM memory_ledger_lineage{where + (' AND' if where else ' WHERE')} lineage_type IN ('correction_of','supersedes')", params)
    report["explicitCorrectionCounts"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries{where + (' AND' if where else ' WHERE')} lifecycle_status='review_only' AND predicate_key='remembered_number'", params)
    report["unresolvedCorrectionAttempts"] = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_shadow_receipts{where + (' AND' if where else ' WHERE')} outcome IN ('inserted','deduplicated') AND (entry_id='' OR entry_id NOT IN (SELECT entry_id FROM memory_ledger_entries WHERE {'guild_id=? AND ' if guild_id is not None else ''} 1=1))", params + ([guild_id] if guild_id is not None else []))
    missing_receipt_entries = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_entries e{where.replace('WHERE', 'WHERE e.') if where else ''} AND NOT EXISTS (SELECT 1 FROM memory_ledger_shadow_receipts r WHERE r.guild_id=e.guild_id AND r.entry_id=e.entry_id AND r.outcome IN ('inserted','deduplicated'))" if where else "SELECT COUNT(*) FROM memory_ledger_entries e WHERE NOT EXISTS (SELECT 1 FROM memory_ledger_shadow_receipts r WHERE r.guild_id=e.guild_id AND r.entry_id=e.entry_id AND r.outcome IN ('inserted','deduplicated'))", params)
    entries_without_receipts = int(cur.fetchone()[0] or 0)
    cur.execute(f"SELECT COUNT(*) FROM memory_ledger_lineage l{where.replace('WHERE', 'WHERE l.') if where else ''} AND NOT EXISTS (SELECT 1 FROM memory_ledger_entries e WHERE e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id)" if where else "SELECT COUNT(*) FROM memory_ledger_lineage l WHERE NOT EXISTS (SELECT 1 FROM memory_ledger_entries e WHERE e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id)", params)
    dangling_lineage = int(cur.fetchone()[0] or 0)
    report["danglingLineageTargets"] = dangling_lineage
    report["legacyToLedgerParityMismatches"] = missing_receipt_entries + entries_without_receipts + dangling_lineage + int(report.get("shadowWriteErrors", 0)) + int(report.get("unresolvedCorrectionAttempts", 0))
    knowledge_tables_present = bool(
        cur.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type='table' AND name IN (
              'memory_ledger_knowledge_candidates',
              'memory_ledger_knowledge_roots',
              'memory_ledger_knowledge_participants',
              'memory_ledger_knowledge_receipts'
            )
            """
        ).fetchone()[0]
        == 4
    )
    report["knowledgeCandidateSchemaVersion"] = (
        ATOMIC_KNOWLEDGE_SCHEMA_VERSION if knowledge_tables_present else "absent"
    )
    report["knowledgeCandidateTablesPresent"] = knowledge_tables_present
    lifecycle_tables_present = bool(
        cur.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type='table' AND name IN (
              'memory_ledger_knowledge_lifecycle_events',
              'memory_ledger_knowledge_lifecycle_roots'
            )
            """
        ).fetchone()[0]
        == 2
    )
    report["knowledgeLifecycleSchemaVersion"] = (
        ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION
        if lifecycle_tables_present
        else "absent"
    )
    report["knowledgeLifecycleTablesPresent"] = lifecycle_tables_present
    report["knowledgeCandidateTotalsByType"] = {}
    report["knowledgeCandidateTotalsByState"] = {}
    report["knowledgeCandidateTotalsByVisibility"] = {}
    report["knowledgeCandidateTotalsByAuthority"] = {}
    report["knowledgeCandidateTotalsByConfidence"] = {}
    report["knowledgeCandidateTotalsByEpistemicStatus"] = {}
    report["knowledgeCandidateTotalsByCurrentness"] = {}
    report["knowledgeCandidateReceiptEvents"] = {}
    report["knowledgeCandidateRejectionsByReason"] = {}
    report["knowledgeCandidateInvalidationsByReason"] = {}
    report["knowledgeCandidateRootKinds"] = {}
    report["knowledgeCandidateRootCount"] = 0
    report["knowledgeCandidateIndependentRootCount"] = 0
    report["knowledgeCandidateDerivativeRootCount"] = 0
    report["knowledgeCandidateDerivativeOnlyRejections"] = 0
    report["knowledgeCandidateAmbiguousRejections"] = 0
    report["knowledgeCandidateOrphanedRoots"] = 0
    report["knowledgeCandidateMissingIndependentRoots"] = 0
    report["knowledgeCandidateParticipantIsolationViolations"] = 0
    report["knowledgeCandidateLiveEligibleCount"] = 0
    report["knowledgeCandidateProcessingErrors"] = 0
    report["knowledgeCandidateCorrectionDeletePrivacyInvalidations"] = 0
    report["knowledgeLifecycleConsolidationGroups"] = 0
    report["knowledgeLifecycleCanonicalCandidates"] = 0
    report["knowledgeLifecycleReinforcementDistribution"] = {}
    report["knowledgeLifecycleConsolidatedAuthority"] = {}
    report["knowledgeLifecycleConsolidatedConfidence"] = {}
    report["knowledgeLifecycleEligibleIndependentRoots"] = 0
    report["knowledgeLifecycleDuplicateSupportRoots"] = 0
    report["knowledgeLifecycleConflictScopes"] = 0
    report["knowledgeLifecycleReviewStatuses"] = {}
    report["knowledgeLifecycleReasons"] = {}
    report["knowledgeLifecycleTransitionStates"] = {}
    report["knowledgeLifecycleEventRoots"] = 0
    report["knowledgeLifecycleReinforcementEventRoots"] = 0
    report["knowledgeLifecycleMissingPromotionProvenance"] = 0
    report["knowledgeLifecycleDirtyCandidates"] = 0
    report["knowledgeCandidateBackfill"] = {
        "phase": "not_started",
        "completed": False,
        "counts": {},
    }
    report["knowledgeLifecycleBackfill"] = {
        "phase": "not_started",
        "completed": False,
        "counts": {},
    }
    report["knowledgeLifecycleSweep"] = {
        "cursor": "",
        "counts": {},
        "updated_at": "",
    }
    if knowledge_tables_present:
        candidate_where = ""
        candidate_params: list[Any] = []
        if guild_id is not None:
            candidate_where = " WHERE guild_id=?"
            candidate_params = [guild_id]
        for key, column in (
            ("knowledgeCandidateTotalsByType", "candidate_type"),
            ("knowledgeCandidateTotalsByState", "candidate_state"),
            ("knowledgeCandidateTotalsByVisibility", "visibility"),
            ("knowledgeCandidateTotalsByAuthority", "authority_class"),
            ("knowledgeCandidateTotalsByConfidence", "confidence_class"),
            (
                "knowledgeCandidateTotalsByEpistemicStatus",
                "epistemic_status",
            ),
            ("knowledgeCandidateTotalsByCurrentness", "currentness"),
        ):
            cur.execute(
                f"""
                SELECT {column},COUNT(*)
                FROM memory_ledger_knowledge_candidates
                {candidate_where}
                GROUP BY {column}
                """,
                candidate_params,
            )
            report[key] = dict(cur.fetchall())
        cur.execute(
            f"""
            SELECT event_type,COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}
            GROUP BY event_type
            """,
            candidate_params,
        )
        report["knowledgeCandidateReceiptEvents"] = dict(cur.fetchall())
        rejection_suffix = " AND" if candidate_where else " WHERE"
        cur.execute(
            f"""
            SELECT reason_code,COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix} event_type='rejected'
            GROUP BY reason_code
            """,
            candidate_params,
        )
        report["knowledgeCandidateRejectionsByReason"] = dict(cur.fetchall())
        cur.execute(
            f"""
            SELECT invalidated_reason,COUNT(*)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix}
              COALESCE(invalidated_reason,'')<>''
            GROUP BY invalidated_reason
            """,
            candidate_params,
        )
        report["knowledgeCandidateInvalidationsByReason"] = dict(
            cur.fetchall()
        )
        cur.execute(
            f"""
            SELECT root_kind,COUNT(*)
            FROM memory_ledger_knowledge_roots
            {candidate_where}
            GROUP BY root_kind
            """,
            candidate_params,
        )
        report["knowledgeCandidateRootKinds"] = dict(cur.fetchall())
        cur.execute(
            f"""
            SELECT
              COUNT(*),
              COALESCE(SUM(CASE WHEN is_independent=1 THEN 1 ELSE 0 END),0),
              COALESCE(SUM(CASE WHEN is_independent=0 THEN 1 ELSE 0 END),0)
            FROM memory_ledger_knowledge_roots
            {candidate_where}
            """,
            candidate_params,
        )
        root_counts = cur.fetchone() or (0, 0, 0)
        report["knowledgeCandidateRootCount"] = int(root_counts[0] or 0)
        report["knowledgeCandidateIndependentRootCount"] = int(
            root_counts[1] or 0
        )
        report["knowledgeCandidateDerivativeRootCount"] = int(
            root_counts[2] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix}
              event_type='rejected'
              AND reason_code LIKE 'derivative%'
            """,
            candidate_params,
        )
        report["knowledgeCandidateDerivativeOnlyRejections"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix}
              event_type='rejected'
              AND (
                reason_code LIKE 'ambiguous%'
                OR reason_code LIKE '%isolation%'
                OR reason_code='participant_scope_mismatch'
                OR reason_code='cross_guild_or_ambiguous_provenance'
              )
            """,
            candidate_params,
        )
        report["knowledgeCandidateAmbiguousRejections"] = int(
            cur.fetchone()[0] or 0
        )
        root_alias_where = ""
        root_alias_params: list[Any] = []
        if guild_id is not None:
            root_alias_where = " AND r.guild_id=?"
            root_alias_params = [guild_id]
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_roots r
            LEFT JOIN memory_ledger_entries e
              ON e.guild_id=r.guild_id AND e.entry_id=r.root_entry_id
            WHERE e.entry_id IS NULL
              AND r.root_status NOT IN (
                'deleted','forgotten','retracted','superseded','corrected'
              )
              {root_alias_where}
            """,
            root_alias_params,
        )
        report["knowledgeCandidateOrphanedRoots"] = int(
            cur.fetchone()[0] or 0
        )
        candidate_alias_where = ""
        candidate_alias_params: list[Any] = []
        if guild_id is not None:
            candidate_alias_where = " WHERE c.guild_id=?"
            candidate_alias_params = [guild_id]
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates c
            {candidate_alias_where}
            {"AND" if candidate_alias_where else "WHERE"} NOT EXISTS (
              SELECT 1 FROM memory_ledger_knowledge_roots r
              WHERE r.candidate_id=c.candidate_id AND r.is_independent=1
            )
            """,
            candidate_alias_params,
        )
        report["knowledgeCandidateMissingIndependentRoots"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates c
            {candidate_alias_where}
            {"AND" if candidate_alias_where else "WHERE"}
              c.subject_key LIKE 'discord_user:%'
              AND NOT EXISTS (
                SELECT 1 FROM memory_ledger_knowledge_participants p
                WHERE p.candidate_id=c.candidate_id
                  AND p.participant_key=c.subject_key
              )
            """,
            candidate_alias_params,
        )
        report["knowledgeCandidateParticipantIsolationViolations"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix} live_eligible<>0
            """,
            candidate_params,
        )
        report["knowledgeCandidateLiveEligibleCount"] = int(
            cur.fetchone()[0] or 0
        )
        if guild_id is None:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_receipts
                WHERE event_type='error'
                """
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_receipts
                WHERE guild_id IN (?,0) AND event_type='error'
                """,
                (guild_id,),
            )
        report["knowledgeCandidateProcessingErrors"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_receipts
            {candidate_where}{rejection_suffix}
              event_type IN ('invalidated','superseded')
              AND reason_code IN (
                'root_privacy_or_deletion','root_deleted','root_changed',
                'root_privacy_or_provenance_changed',
                'root_correction_of','root_supersedes','root_retracts',
                'explicit_root_supersession'
              )
            """,
            candidate_params,
        )
        report[
            "knowledgeCandidateCorrectionDeletePrivacyInvalidations"
        ] = int(cur.fetchone()[0] or 0)
        cur.execute(
            f"""
            SELECT COUNT(DISTINCT consolidation_id)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix}
              COALESCE(consolidation_id,'')<>''
            """,
            candidate_params,
        )
        report["knowledgeLifecycleConsolidationGroups"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix}
              COALESCE(canonical_candidate_id,'')=candidate_id
            """,
            candidate_params,
        )
        report["knowledgeLifecycleCanonicalCandidates"] = int(
            cur.fetchone()[0] or 0
        )
        cur.execute(
            f"""
            SELECT reinforcement_count,COUNT(*)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix}
              COALESCE(canonical_candidate_id,'')=candidate_id
            GROUP BY reinforcement_count
            """,
            candidate_params,
        )
        report["knowledgeLifecycleReinforcementDistribution"] = {
            str(key): int(value or 0)
            for key, value in cur.fetchall()
        }
        for key, column in (
            (
                "knowledgeLifecycleConsolidatedAuthority",
                "consolidated_authority_class",
            ),
            (
                "knowledgeLifecycleConsolidatedConfidence",
                "consolidated_confidence_class",
            ),
        ):
            cur.execute(
                f"""
                SELECT {column},COUNT(*)
                FROM memory_ledger_knowledge_candidates
                {candidate_where}{rejection_suffix}
                  COALESCE(canonical_candidate_id,'')=candidate_id
                GROUP BY {column}
                """,
                candidate_params,
            )
            report[key] = {
                str(name or "unknown"): int(value or 0)
                for name, value in cur.fetchall()
            }
        cur.execute(
            f"""
            SELECT
              COALESCE(SUM(eligible_independent_root_count),0),
              COALESCE(SUM(duplicate_support_count),0)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix}
              COALESCE(canonical_candidate_id,'')=candidate_id
            """,
            candidate_params,
        )
        lifecycle_support = cur.fetchone() or (0, 0)
        report["knowledgeLifecycleEligibleIndependentRoots"] = int(
            lifecycle_support[0] or 0
        )
        report["knowledgeLifecycleDuplicateSupportRoots"] = int(
            lifecycle_support[1] or 0
        )
        conflict_filter = "WHERE guild_id=?" if guild_id is not None else ""
        cur.execute(
            f"""
            SELECT COUNT(*) FROM (
              SELECT guild_id,candidate_type,subject_key,predicate_key,
                     contradiction_key,visibility,participant_scope_digest
              FROM memory_ledger_knowledge_candidates
              {conflict_filter}
              GROUP BY guild_id,candidate_type,subject_key,predicate_key,
                       contradiction_key,visibility,participant_scope_digest
              HAVING MAX(conflict_value_count)>1
            )
            """,
            [guild_id] if guild_id is not None else [],
        )
        report["knowledgeLifecycleConflictScopes"] = int(
            cur.fetchone()[0] or 0
        )
        for key, column in (
            ("knowledgeLifecycleReviewStatuses", "review_status"),
            ("knowledgeLifecycleReasons", "lifecycle_reason"),
        ):
            cur.execute(
                f"""
                SELECT {column},COUNT(*)
                FROM memory_ledger_knowledge_candidates
                {candidate_where}
                GROUP BY {column}
                """,
                candidate_params,
            )
            report[key] = {
                str(name or "unset"): int(value or 0)
                for name, value in cur.fetchall()
            }
        if lifecycle_tables_present:
            cur.execute(
                f"""
                SELECT next_state,COUNT(*)
                FROM memory_ledger_knowledge_lifecycle_events
                {candidate_where}
                GROUP BY next_state
                """,
                candidate_params,
            )
            report["knowledgeLifecycleTransitionStates"] = {
                str(name or "unknown"): int(value or 0)
                for name, value in cur.fetchall()
            }
            cur.execute(
                f"""
                SELECT
                  COUNT(*),
                  COALESCE(SUM(counts_as_reinforcement),0)
                FROM memory_ledger_knowledge_lifecycle_roots
                {candidate_where}
                """,
                candidate_params,
            )
            event_roots = cur.fetchone() or (0, 0)
            report["knowledgeLifecycleEventRoots"] = int(event_roots[0] or 0)
            report["knowledgeLifecycleReinforcementEventRoots"] = int(
                event_roots[1] or 0
            )
            event_alias_filter = ""
            event_alias_params: list[Any] = []
            if guild_id is not None:
                event_alias_filter = " AND e.guild_id=?"
                event_alias_params = [guild_id]
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM memory_ledger_knowledge_lifecycle_events e
                WHERE e.reinforcement_count>0
                  {event_alias_filter}
                  AND NOT EXISTS (
                    SELECT 1
                    FROM memory_ledger_knowledge_lifecycle_roots r
                    WHERE r.event_id=e.event_id
                      AND r.counts_as_reinforcement=1
                  )
                """,
                event_alias_params,
            )
            report["knowledgeLifecycleMissingPromotionProvenance"] = int(
                cur.fetchone()[0] or 0
            )
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM memory_ledger_knowledge_candidates
            {candidate_where}{rejection_suffix}
              (
                COALESCE(lifecycle_schema_version,'')<>?
                OR COALESCE(lifecycle_evaluated_at,'')=''
                OR review_status='dirty'
              )
            """,
            candidate_params
            + [ATOMIC_KNOWLEDGE_LIFECYCLE_SCHEMA_VERSION],
        )
        report["knowledgeLifecycleDirtyCandidates"] = int(
            cur.fetchone()[0] or 0
        )
        backfill = cur.execute(
            """
            SELECT phase,completed,counts_json
            FROM memory_ledger_knowledge_backfill
            WHERE migration_key=?
            """,
            (ATOMIC_KNOWLEDGE_BACKFILL,),
        ).fetchone()
        if backfill:
            try:
                backfill_counts = json.loads(str(backfill[2] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                backfill_counts = {"invalid_json": 1}
            report["knowledgeCandidateBackfill"] = {
                "phase": str(backfill[0] or "unknown"),
                "completed": bool(backfill[1]),
                "counts": backfill_counts,
            }
        lifecycle_backfill = cur.execute(
            """
            SELECT phase,completed,counts_json
            FROM memory_ledger_knowledge_backfill
            WHERE migration_key=?
            """,
            (ATOMIC_KNOWLEDGE_LIFECYCLE_BACKFILL,),
        ).fetchone()
        if lifecycle_backfill:
            try:
                lifecycle_counts = json.loads(
                    str(lifecycle_backfill[2] or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                lifecycle_counts = {"invalid_json": 1}
            report["knowledgeLifecycleBackfill"] = {
                "phase": str(lifecycle_backfill[0] or "unknown"),
                "completed": bool(lifecycle_backfill[1]),
                "counts": lifecycle_counts,
            }
        lifecycle_sweep = cur.execute(
            """
            SELECT cursor_value,counts_json,updated_at
            FROM memory_ledger_knowledge_backfill
            WHERE migration_key=?
            """,
            (ATOMIC_KNOWLEDGE_LIFECYCLE_SWEEP,),
        ).fetchone()
        if lifecycle_sweep:
            try:
                sweep_counts = json.loads(str(lifecycle_sweep[1] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                sweep_counts = {"invalid_json": 1}
            report["knowledgeLifecycleSweep"] = {
                "cursor": str(lifecycle_sweep[0] or ""),
                "counts": sweep_counts,
                "updated_at": str(lifecycle_sweep[2] or ""),
            }
    return report
