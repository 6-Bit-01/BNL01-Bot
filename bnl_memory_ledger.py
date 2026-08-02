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
from typing import Any, Iterable, Mapping
import unicodedata

from bnl_canon_source_contract import (
    Confidence,
    LIVING_CANON_RECURRENCE_VERSION,
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
LIVING_CANON_V1_FORMATION_ENV = (
    "BNL_LIVING_CANON_V1_FORMATION_SHADOW_ENABLED"
)
LIVING_CANON_GROUPING_SIGNATURE_VERSION = (
    "living_canon_exact_root_grouping_v1"
)
_LIVING_CANON_AUTHORITY_TABLES = frozenset(
    {
        "conversations",
        "bnl_journal_source_events",
        "memory_ledger_entries",
        "memory_ledger_lineage",
        "memory_ledger_participants",
        "memory_ledger_knowledge_candidates",
        "memory_ledger_knowledge_roots",
        "memory_ledger_knowledge_participants",
        "memory_ledger_conversation_motif_fences",
        "memory_moment_windows",
        "memory_moment_members",
        "memory_moment_participants",
    }
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
_CONVERSATION_MOTIF_NEUTRAL_PREFIX = "conversation_motif_neutral_"
_CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD = (
    "conversation_motif_neutral_*"
)
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
        "adjust",
        "adjusting",
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
_PUBLIC_ASSESSMENT_ALLOWED_ROUTES = frozenset(
    {"normal_chat", "conversation_continuity"}
)
_PUBLIC_ASSESSMENT_GENERIC_PROFILE_TERMS = frozenset(
    {
        "all",
        "barcode",
        "bnl",
        "bnl-01",
        "bnl01",
        "everything",
        "learn",
        "me",
        "my",
        "myself",
        "part",
        "tell",
    }
)
_PUBLIC_ASSESSMENT_SEMANTICS_VERSION = "public_assessment_semantics_v3"
_PUBLIC_ASSESSMENT_ROOT_STATE_VERSION = "public_assessment_root_state_v3"
_PUBLIC_ASSESSMENT_ACTION_ALIASES = {
    "ask": "ask",
    "asked": "ask",
    "asking": "ask",
    "question": "ask",
    "request": "ask",
    "suggest": "suggest",
    "propose": "suggest",
    "recommend": "suggest",
    "consider": "suggest",
    "imagine": "suggest",
    "discuss": "discuss",
    "mention": "discuss",
    "talk": "discuss",
    "describe": "discuss",
    "say": "discuss",
    "tell": "discuss",
    "note": "discuss",
    "raise": "discuss",
    "revisit": "return",
    "return": "return",
    "compare": "evaluate",
    "compar": "evaluate",
    "evaluat": "evaluate",
    "weigh": "evaluate",
    "evaluate": "evaluate",
    "assess": "evaluate",
    "asses": "evaluate",
    "inspect": "evaluate",
    "review": "evaluate",
    "test": "test",
    "tried": "test",
    "try": "test",
    "trying": "test",
    "verifi": "test",
    "validat": "test",
    "verify": "test",
    "validate": "test",
    "check": "test",
    "patch": "fix",
    "fix": "fix",
    "fixed": "fix",
    "repair": "fix",
    "debug": "fix",
    "tune": "adjust",
    "tun": "adjust",
    "tuned": "adjust",
    "tuning": "adjust",
    "adjust": "adjust",
    "calibrate": "adjust",
    "calibrat": "adjust",
    "refine": "adjust",
    "refin": "adjust",
    "build": "build",
    "make": "build",
    "mak": "build",
    "making": "build",
    "create": "build",
    "creat": "build",
    "craft": "build",
    "produce": "build",
    "design": "build",
    "choose": "choose",
    "choos": "choose",
    "decide": "choose",
    "decid": "choose",
    "select": "choose",
    "pick": "choose",
    "prefer": "choose",
    "plan": "plan",
    "organize": "plan",
    "organiz": "plan",
    "coordinate": "plan",
    "coordinat": "plan",
    "schedule": "plan",
    "schedul": "plan",
    "write": "write",
    "writ": "write",
    "draft": "write",
    "edit": "write",
    "share": "share",
    "shar": "share",
    "post": "share",
    "publish": "share",
    "release": "share",
    "releas": "share",
    "learn": "learn",
    "research": "learn",
    "investigate": "learn",
    "investigat": "learn",
    "study": "learn",
    "studi": "learn",
}
_PUBLIC_ASSESSMENT_TOPIC_ALIASES = {
    "audio": "audio",
    "sound": "audio",
    "mix": "audio",
    "music": "audio",
    "tone": "audio",
    "track": "audio",
    "synth": "audio",
    "harmonic": "audio",
    "release": "release",
    "publish": "release",
    "website": "interface",
    "site": "interface",
    "interface": "interface",
    "screen": "interface",
    "antenna": "signal",
    "signal": "signal",
    "modem": "signal",
    "meter": "signal",
    "network": "signal",
    "radio": "broadcast",
    "broadcast": "broadcast",
    "show": "broadcast",
    "stream": "broadcast",
    "visual": "visual",
    "image": "visual",
    "icon": "visual",
    "emote": "visual",
    "art": "visual",
    "transition": "transition",
    "interlude": "transition",
    "tape": "transition",
    "code": "software",
    "bot": "software",
    "software": "software",
    "system": "software",
    "memory": "software",
    "community": "community",
    "member": "community",
    "discord": "community",
    "team": "community",
    "lore": "lore",
    "canon": "lore",
    "story": "lore",
}
_PUBLIC_ASSESSMENT_MATERIAL_STOPWORDS = frozenset(
    """
    after again always anymore another around before barely careful carefully
    constantly
    change changes could daily day days did do does doing don't each evening
    evenings every final first frequently had hardly has have having here hour
    hours keep keeps last later may might month monthly months more morn morning
    mornings never night nightly nights no not occasionally often once only our
    quarterly rarely really regular regularly routinely seldom should smaller
    sometime sometimes still than then there today tomorrow usually very want wants week
    weekly weeks whenever will without would yesterday yearly years
    """.split()
)
_PUBLIC_ASSESSMENT_ACTION_SKIP = frozenset(
    """
    also am are be been being can could did do does had has have having keep keeps
    hardly longer may might must never no not often only rarely really seldom should
    sometimes tend tends usually was were will without would
    """.split()
)
_PUBLIC_ASSESSMENT_NEGATIVE_RE = re.compile(
    r"\b(?:never|not|no\s+longer|cannot|can't|don't|doesn't|didn't|"
    r"won't|wouldn't|shouldn't|without)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_CONDITIONAL_RE = re.compile(
    r"\b(?:barely|can|could|hardly|would|might|may|rarely|seldom|should|"
    r"will|if|unless)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_PASSIVE_ACTOR_RE = re.compile(
    r"\b(?:i|we|you)\s+(?:(?:have|had)\s+been|am|are|was|were|be|been|being)"
    r"\s+(?:[a-z]+ly\s+)?(?:asked|assigned|encouraged|forced|given|invited|"
    r"required|requested|reminded|told|warned)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_POST_ACTION_CONDITION_RE = re.compile(
    r"\b(?:only\s+if|if|unless|depending\s+on|as\s+long\s+as)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_CESSATION_RE = re.compile(
    r"\b(?:i|we|you)\s+(?:have\s+)?(?:stopped?|quit(?:s|ting)?)\s+"
    r"[a-z][a-z'’-]{2,}ing\b",
    re.I,
)
_PUBLIC_ASSESSMENT_SINGLE_OCCURRENCE_RE = re.compile(
    r"\b(?:once|one\s+time|this\s+time|just\s+once)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_INTERMITTENT_RE = re.compile(
    r"\b(?:sometimes|occasionally|rarely|seldom|now\s+and\s+then)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_HABITUAL_RE = re.compile(
    r"\b(?:always|constantly|daily|every\s+(?:day|morning|evening|night|"
    r"week|month)|frequently|often|regularly|routinely|usually|weekly|"
    r"monthly|yearly)\b",
    re.I,
)
_PUBLIC_ASSESSMENT_LEADING_ADDRESSEE_RE = re.compile(
    r"^\s*(?:<@!?\d+>\s*)?"
    r"(?:(?:hey|yo|hi|hello)[\s,]+)?"
    r"(?:@?bnl(?:-?0?1)?|barcode\s+(?:bot|network\s+layer)|b|bud|buddy)"
    r"(?:\s*[,;:!?—–-]+\s*|\s+)",
    re.I,
)
_PUBLIC_ASSESSMENT_CANDIDATE_EPISTEMIC_FRAME_RE = re.compile(
    r"^\s*(?:(?:from|based\s+on)\s+(?:your\s+public\s+"
    r"(?:messages|activity|appearances)|the\s+public\s+(?:thread|record)|"
    r"what\s+i(?:'ve|\s+have)?\s+(?:seen|noticed|observed))"
    r"\s*[,;:—–-]\s*(?:i(?:'ve|\s+have)?\s+"
    r"(?:noticed|observed|seen)\s+(?:that\s+)?)?|"
    r"i(?:'ve|\s+have)\s+"
    r"(?:noticed|observed|seen)\s+(?:that\s+)?)",
    re.I,
)
_PUBLIC_ASSESSMENT_SENSITIVE_DISCLOSURE_RE = re.compile(
    r"\b(?:add|adhd|aids|anxiety|autis(?:m|tic)|bipolar|cancer|clinic|"
    r"depression|diabetes|dyslexia|ocd|prozac|ptsd|schizophrenia|"
    r"diagnos(?:ed|is)|disease|disorder|doctor|hospital|insulin|medical|"
    r"medication|medicine|mental\s+health|nurse|patient|prescription|"
    r"psychiatr(?:y|ist|ic)|therapy|therapist|treatment)\b|"
    r"\b(?:arrest(?:ed)?|convict(?:ed|ion)?|court|crime|criminal|felony|"
    r"jail|lawsuit|misdemeanor|parole|prison|probation)\b|"
    r"\b(?:bank|bankrupt(?:cy)?|debt|dollars?|earn(?:ed|ing|s)?|finance|"
    r"financial|income|loan|mortgage|rent|salary|wage)\b|"
    r"\b(?:democrat(?:ic)?|election|liberal|politic(?:al(?:ly)?|s)?|republican|"
    r"socialist|vote[ds]?|voting)\b|"
    r"\b(?:bisexual|christian|gay|hindu|islam|jewish|lesbian|muslim|"
    r"nonbinary|queer|religious|transgender)\b|"
    r"\b(?:administrator|admin|moderator|server\s+owner|staff\s+member)\b|"
    r"\b(?:identify|identified|practice|practicing)\s+as\b",
    re.I,
)
_PUBLIC_ASSESSMENT_CONTEXTUAL_PRIVATE_RE = re.compile(
    r"\b(?:i|we|you)\s+(?:have|had|manage|managed|live\s+with|"
    r"suffer(?:ed|ing)?\s+from)\s+(?:a|an|the|my|our|your)?\s*"
    r"(?:[A-Z]{2,}[A-Z0-9-]*|(?:mental|medical|chronic)\s+"
    r"(?:condition|issue|disorder))\b|"
    r"\b(?:i|we|you)\s+(?:take|took|use|used)\s+(?:my|our|your)?\s*"
    r"(?:medication|medicine|prescription|[A-Z][A-Za-z0-9-]{2,})\b|"
    r"\b(?:i|we|you)\s+(?:moved?|relocat(?:e|ed|ing))\s+"
    r"(?:to|from|near)\b|"
    r"\b(?:i|we|you)\s+(?:owe|owed|owing)\b|"
    r"\b(?:credit\s+card|mastercard|money|visa)\b|"
    r"\b(?:i|we|you)\s+(?:strongly\s+)?(?:support|oppose|back)\s+"
    r"[A-Z][A-Za-z0-9'’_-]+\b",
    re.I,
)
_PUBLIC_ASSESSMENT_ROLE_REPORT_RE = re.compile(
    r"\b(?:i|we|you)\s+(?:work|serve|served|act|acted|volunteer|"
    r"volunteered)\s+as\b",
    re.I,
)
_PUBLIC_ASSESSMENT_EXACT_NUMBER_RE = re.compile(
    r"(?<![A-Za-z-])\b\d[\d,]*(?:\.\d+)?\b"
)
_PUBLIC_ASSESSMENT_THIRD_PARTY_LEAD_RE = re.compile(
    r"^\s*(?:<@!?\d+>|he|she|they|them|"
    r"[A-Z][A-Za-z0-9'’_-]*(?:\s+[A-Z][A-Za-z0-9'’_-]*){1,3}|"
    r"[A-Z]{2,}(?:-\d+)?)\s+"
    r"(?:is|are|was|were|has|have|had|can|could|will|would|should|"
    r"[a-z][a-z'’-]{2,})\b"
)
_PUBLIC_ASSESSMENT_REPORTED_THIRD_PARTY_RE = re.compile(
    r"\b(?:i|we)\s+(?:think|believe|heard|noticed|saw|know)\s+"
    r"(?:that\s+)?(?:<@!?\d+>|"
    r"[A-Z][A-Za-z0-9'’_-]*(?:\s+[A-Z][A-Za-z0-9'’_-]*){1,3})\b"
    ,
    re.I,
)
_PUBLIC_ASSESSMENT_SINGLE_PARTY_LEAD_RE = re.compile(
    r"^\s*[A-Z][A-Za-z0-9'’_-]{1,31}\s+"
    r"(?:(?:always|often|usually|sometimes|never)\s+)?"
    r"(?:is|was|has|does|like(?:s|d)?|test(?:s|ed)?|build(?:s|t)?|"
    r"make(?:s|d)?|use(?:s|d)?|check(?:s|ed)?|prefer(?:s|red)?|"
    r"keep(?:s|t)?|ask(?:s|ed)?|suggest(?:s|ed)?|review(?:s|ed)?|"
    r"tun(?:e|es|ed)|mix(?:es|ed)?|post(?:s|ed)?|share(?:s|d)?)\b"
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
    r"|^\s*(?:i|we)\s+no\s+longer\b"
    r"|^\s*(?:i|we)\s+(?:have\s+)?(?:stopped?|quit(?:s|ting)?)\s+"
    r"(?!by\b|to\b|at\b)[a-z][a-z'’-]{2,}"
    r"|^\s*(?:i|we)\s+(?:was|were)\s+wrong\b"
    r"|^\s*(?:i|we)\s+(?:do|did)\s+not\b[^.!?]{0,180}\banymore\b"
    r"|^\s*(?:i|we)\s+used\s+to\b[^.!?]{0,180}"
    r"\b(?:i|we)\s+(?:do|did)\s+not\b[^.!?]{0,80}\banymore\b"
    r"|\b(?:i|we)\s+(?:have\s+)?changed\s+(?:my|our)\s+mind\b"
    r"|\b(?:i|we)\s+(?:take|took)\s+(?:that|this|it)\s+back\b"
    r"|^\s*(?:i|we)\s+(?:have\s+)?switched\s+from\b"
    r"|^\s*(?:i|we)\s+(?:have\s+)?moved\s+on\s+from\b"
    r"|\b(?:that's|that\s+is)\s+wrong\b"
    r"|^\s*no\s*[,;:]\s*not\s+that\b"
    r"|^\s*no\s*[,;:]\s*[^.!?]{0,180}\binstead\b"
    r"|\bscratch\s+(?:that|this)\b"
    r"|\b(?:forget|ignore)\s+(?:that|what\s+(?:i|we)\s+said)\b"
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
    recurrence_contract_version: str = ""
    grouping_signature_version: str = ""
    grouping_identity: str = ""
    canon_domain: str = ""
    canon_claim_kind: str = ""
    occurrence_ids: tuple[str, ...] = field(default_factory=tuple)


_LIVING_CANON_FORMATION_AUTHORITY = object()


@dataclass(frozen=True)
class _LivingCanonFormationReceipt:
    """Unforgeable process-local authority minted by the formation owner."""

    authority: object


def _living_canon_formation_receipt() -> _LivingCanonFormationReceipt:
    return _LivingCanonFormationReceipt(
        authority=_LIVING_CANON_FORMATION_AUTHORITY,
    )


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
class LivingCanonDryRunReport:
    """Content-free result from the read-only PR4 recurrence analyzer."""

    recurrence_contract_version: str = LIVING_CANON_RECURRENCE_VERSION
    grouping_signature_version: str = LIVING_CANON_GROUPING_SIGNATURE_VERSION
    proposed_count: int = 0
    skipped_count: int = 0
    ambiguous_count: int = 0
    rejected_count: int = 0
    candidate_state_counts: tuple[tuple[str, int], ...] = ()
    reason_counts: tuple[tuple[str, int], ...] = ()
    independent_root_count: int = 0
    independent_occurrence_count: int = 0
    collapsed_root_count: int = 0
    bounds: tuple[tuple[str, int], ...] = (
        ("eligible_ledger_scan_max", _CONVERSATION_MOTIF_MAX_SCAN),
        ("motif_candidates_max", _CONVERSATION_MOTIF_MAX_CANDIDATES),
        ("retained_roots_max", _CONVERSATION_MOTIF_MAX_ROOTS),
        ("occurrence_lookback_max", _CONVERSATION_OCCURRENCE_MAX_SCAN),
        ("idle_boundary_seconds", _CONVERSATION_MOTIF_WINDOW_SECONDS),
    )
    source_write_count: int = 0
    write_occurred: bool = False


@dataclass(frozen=True)
class PublicAssessmentEvidence:
    """One public, source-linked observation selected for current assessment."""

    entry_id: str
    text: str
    observed_at: str
    visibility: str
    occurrence_identity: str
    score: float
    root_identity: str = ""
    source_digest: str = ""
    point_identity: str = ""
    attribution_mode: str = ""
    polarity: str = ""
    action_identity: str = ""
    material_facets: tuple[str, ...] = field(default_factory=tuple)
    request_relevant: bool = False
    subject_key: str = ""
    assessment_contract_version: str = PUBLIC_ASSESSMENT_EVIDENCE_VERSION
    source_system: str = "memory_ledger_public_assessment"
    source_role: str = "user"
    source_class: str = SourceClass.PUBLIC_OBSERVATION.value
    lifecycle_status: str = ACTIVE_LIFECYCLE
    channel_policy: str = "unknown"
    route_mode: str = "unknown"
    public_usable: bool = False
    subject_authored: bool = False
    selector_eligible: bool = False
    derived: bool = True
    projection: bool = True


@dataclass(frozen=True)
class PublicAssessmentSemantics:
    """Deterministic actor, polarity, and material-point interpretation."""

    attribution_mode: str = "third_party_or_ambiguous"
    polarity: str = "affirmative"
    action_identity: str = ""
    material_facets: tuple[str, ...] = field(default_factory=tuple)
    point_identity: str = ""


@dataclass(frozen=True)
class PublicAssessmentRootState:
    """One source-bound public observation state used at build and send."""

    entry_id: str
    subject_key: str
    text: str
    observed_at: str
    visibility: str
    channel_policy: str
    route_mode: str
    source_role: str
    source_class: str
    lifecycle_status: str
    source_row_id: str
    root_identity: str
    occurrence_identity: str
    source_digest: str
    semantics: PublicAssessmentSemantics
    public_usable: bool = True
    derived: bool = False
    projection: bool = False


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


def living_canon_v1_formation_enabled(
    environ: Mapping[str, str] | None = None,
) -> bool:
    """Require an explicit shadow gate for the PR4 recurrence contract."""

    env = os.environ if environ is None else environ
    if not shadow_enabled(dict(env)):
        return False
    return str(env.get(LIVING_CANON_V1_FORMATION_ENV, "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "enabled",
    }


def _living_canon_main_authority_unshadowed(
    conn: sqlite3.Connection,
) -> bool:
    """Reject PR4 evaluation when TEMP can shadow an authority table."""

    placeholders = ",".join("?" for _name in _LIVING_CANON_AUTHORITY_TABLES)
    return not bool(
        conn.execute(
            "SELECT 1 FROM temp.sqlite_master WHERE type='table' "
            "AND name IN (%s) LIMIT 1" % placeholders,
            tuple(sorted(_LIVING_CANON_AUTHORITY_TABLES)),
        ).fetchone()
    )


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
        FROM main.memory_ledger_entries
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
            "SELECT name FROM main.sqlite_master WHERE type='table'"
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
        FROM main.memory_ledger_entries
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
            recurrence_contract_version TEXT DEFAULT '',
            grouping_signature_version TEXT DEFAULT '',
            grouping_identity TEXT DEFAULT '',
            canon_domain TEXT DEFAULT '',
            canon_claim_kind TEXT DEFAULT '',
            independent_occurrence_count INTEGER NOT NULL DEFAULT 0,
            occurrence_ids_json TEXT NOT NULL DEFAULT '[]',
            occurrence_digest TEXT DEFAULT '',
            recurrence_proof_json TEXT NOT NULL DEFAULT '{}',
            public_usable INTEGER NOT NULL DEFAULT 0,
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
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN recurrence_contract_version TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN grouping_signature_version TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN grouping_identity TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN canon_domain TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN canon_claim_kind TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN independent_occurrence_count INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN occurrence_ids_json TEXT NOT NULL DEFAULT '[]'",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN occurrence_digest TEXT DEFAULT ''",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN recurrence_proof_json TEXT NOT NULL DEFAULT '{}'",
        "ALTER TABLE memory_ledger_knowledge_candidates ADD COLUMN public_usable INTEGER NOT NULL DEFAULT 0",
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
        "trg_atomic_knowledge_root_delete_v2",
        "trg_atomic_knowledge_root_change_v2",
        "trg_atomic_knowledge_participant_delete_v2",
        "trg_atomic_knowledge_lineage_change_v2",
        "trg_conversation_motif_fence_source_delete_v1",
    ):
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    cur.execute(
        """
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_root_delete_v3
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
              public_usable=CASE
                WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                  THEN 0 ELSE public_usable END,
              recurrence_proof_json=CASE
                WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                  THEN '{"candidate_eligible":false,"source_eligible":false,"roots_valid":false,"invalidated":true}'
                ELSE recurrence_proof_json END,
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
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_root_change_v3
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
            public_usable=CASE
              WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                THEN 0 ELSE public_usable END,
            recurrence_proof_json=CASE
              WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                THEN '{"candidate_eligible":false,"source_eligible":false,"roots_valid":false,"invalidated":true}'
              ELSE recurrence_proof_json END,
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
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_participant_delete_v3
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
              public_usable=CASE
                WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                  THEN 0 ELSE public_usable END,
              recurrence_proof_json=CASE
                WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                  THEN '{"candidate_eligible":false,"source_eligible":false,"roots_valid":false,"invalidated":true}'
                ELSE recurrence_proof_json END,
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
        CREATE TRIGGER IF NOT EXISTS trg_atomic_knowledge_lineage_change_v3
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
            public_usable=CASE
              WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                THEN 0 ELSE public_usable END,
            recurrence_proof_json=CASE
              WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                THEN '{"candidate_eligible":false,"source_eligible":false,"roots_valid":false,"invalidated":true}'
              ELSE recurrence_proof_json END,
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
        CREATE TRIGGER IF NOT EXISTS trg_conversation_motif_fence_source_delete_v2
        AFTER DELETE ON memory_ledger_entries
        BEGIN
          UPDATE memory_ledger_knowledge_candidates
          SET normalized_value='',candidate_state='invalidated',
              candidate_eligible=0,live_eligible=0,
              public_usable=CASE
                WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                  THEN 0 ELSE public_usable END,
              recurrence_proof_json=CASE
                WHEN COALESCE(recurrence_contract_version,'')='living_canon_recurrence_v1'
                  THEN '{"candidate_eligible":false,"source_eligible":false,"roots_valid":false,"invalidated":true}'
                ELSE recurrence_proof_json END,
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
        FROM main.memory_ledger_entries
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
            FROM main.memory_ledger_participants
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
    living_v1 = bool(
        str(candidate.get("recurrence_contract_version") or "")
        == LIVING_CANON_RECURRENCE_VERSION
    )
    if living_v1 and not _living_canon_main_authority_unshadowed(conn):
        return [], 0
    rows = conn.execute(
        """
        SELECT root_entry_id,is_independent,root_status
        FROM main.memory_ledger_knowledge_roots
        WHERE candidate_id=?
        ORDER BY root_entry_id
        """,
        (candidate["candidate_id"],),
    ).fetchall()
    entry_ids = tuple(str(row[0] or "") for row in rows if str(row[0] or ""))
    entries = _knowledge_entry_rows(conn, entry_ids)
    living_states: dict[str, PublicAssessmentRootState] = {}
    living_occurrences: dict[str, str] = {}
    candidate_tags = set(_knowledge_retrieval_tags(candidate))
    motif_fence = (
        _conversation_motif_fence_row(
            conn,
            guild_id=int(candidate.get("guild_id") or 0),
            subject_key=str(candidate.get("subject_key") or ""),
            predicate_key=str(candidate.get("predicate_key") or ""),
        )
        if "recurring_public_conversation" in candidate_tags
        else {}
    )
    motif_cutoff = _parse_knowledge_time(
        motif_fence.get("correction_observed_at")
    )
    if living_v1:
        independent_entry_ids = tuple(
            str(row[0] or "")
            for row in rows
            if bool(row[1]) and str(row[0] or "")
        )
        living_states, living_occurrences, _living_reasons = (
            _living_canon_root_states_and_occurrences(
                conn,
                guild_id=int(candidate.get("guild_id") or 0),
                subject_key=str(candidate.get("subject_key") or ""),
                entry_ids=independent_entry_ids,
            )
        )
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
            root_observed = _parse_knowledge_time(
                root["entry"].get("observed_at")
            )
            if motif_fence and (
                motif_cutoff is None
                or root_observed is None
                or root_observed <= motif_cutoff
            ):
                continue
            if living_v1:
                state = living_states.get(str(entry_id or ""))
                occurrence = living_occurrences.get(str(entry_id or ""), "")
                if state is None or not occurrence:
                    continue
                root["evidence_identity"] = state.root_identity
                root["occurrence_identity"] = occurrence
            else:
                root["evidence_identity"] = _knowledge_evidence_identity(
                    conn,
                    root["entry"],
                )
                root["occurrence_identity"] = _knowledge_occurrence_identity(
                    conn,
                    root["entry"],
                )
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
          lifecycle_evaluated_at,recurrence_contract_version,
          grouping_signature_version,grouping_identity,canon_domain,
          canon_claim_kind,independent_occurrence_count,
          occurrence_ids_json,occurrence_digest,recurrence_proof_json,
          public_usable
        FROM main.memory_ledger_knowledge_candidates
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
        "recurrence_contract_version",
        "grouping_signature_version",
        "grouping_identity",
        "canon_domain",
        "canon_claim_kind",
        "independent_occurrence_count",
        "occurrence_ids_json",
        "occurrence_digest",
        "recurrence_proof_json",
        "public_usable",
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
        if str(candidate.get("recurrence_contract_version") or ""):
            consolidation_id = _knowledge_digest(
                consolidation_id,
                str(candidate.get("recurrence_contract_version") or ""),
                str(candidate.get("grouping_signature_version") or ""),
                str(candidate.get("grouping_identity") or ""),
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
        group["living_canon_v1"] = any(
            str(candidate.get("recurrence_contract_version") or "")
            == LIVING_CANON_RECURRENCE_VERSION
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
    for consolidation_id, group in groups.items():
        group["conflict_value_count"] = sum(
            1
            for other_id in active_group_ids
            if bool(groups[other_id].get("living_canon_v1"))
            == bool(group.get("living_canon_v1"))
        )
    changes = 0
    evaluated_at = _knowledge_time(now)
    for consolidation_id, group in groups.items():
        roots = list(group["roots"].values())
        root_count = len(group["roots"])
        reinforcement_count = len(group["evidence"])
        conflict_value_count = int(group.get("conflict_value_count") or 0)
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
                if bool(group.get("living_canon_v1")) and reinforcement_count == 1:
                    next_state = "provisional"
                    reason = "single_occurrence_provisional"
                else:
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
                    else "independent_recurrence_established"
                    if bool(group.get("living_canon_v1"))
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
            if str(candidate.get("recurrence_contract_version") or "") == (
                LIVING_CANON_RECURRENCE_VERSION
            ):
                current_occurrence_ids = tuple(
                    sorted(
                        {
                            str(
                                root.get("reinforcement_identity")
                                or root.get("occurrence_identity")
                                or ""
                            )
                            for root in roots
                            if str(
                                root.get("reinforcement_identity")
                                or root.get("occurrence_identity")
                                or ""
                            )
                        }
                    )
                )
                current_occurrence_digest = (
                    _knowledge_digest(*current_occurrence_ids)
                    if current_occurrence_ids
                    else ""
                )
                conn.execute(
                    """
                    UPDATE memory_ledger_knowledge_candidates
                    SET independent_occurrence_count=?,occurrence_ids_json=?,
                        occurrence_digest=?
                    WHERE candidate_id=?
                    """,
                    (
                        len(current_occurrence_ids),
                        json.dumps(
                            current_occurrence_ids,
                            separators=(",", ":"),
                        ),
                        current_occurrence_digest,
                        candidate_id,
                    ),
                )
                _refresh_living_canon_recurrence_proof(
                    conn,
                    candidate_id=candidate_id,
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
    recurrence_contract_version: str = "",
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
          AND COALESCE(recurrence_contract_version,'')=?
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
            str(recurrence_contract_version or ""),
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
        if rows and str(recurrence_contract_version or ""):
            terminal_fingerprint = _knowledge_digest(
                "living_canon_terminal_generations_v1",
                str(fallback_candidate_id or ""),
                *tuple(
                    "%s:%s:%s:%s"
                    % (
                        str(row[0] or ""),
                        str(row[1] or ""),
                        str(row[2] or ""),
                        str(row[3] or ""),
                    )
                    for row in rows
                ),
            )
            return "mlkc_" + terminal_fingerprint[:40]
        return fallback_candidate_id
    refreshable.sort(
        key=lambda row: (
            0 if str(row[4] or "") == str(row[0] or "") else 1,
            str(row[5] or ""),
            str(row[0] or ""),
        )
    )
    return str(refreshable[0][0] or fallback_candidate_id)


def _store_living_canon_contract(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    recurrence_contract_version: str,
    grouping_signature_version: str,
    grouping_identity: str,
    canon_domain: str,
    canon_claim_kind: str,
    occurrence_ids: tuple[str, ...],
    occurrence_digest: str,
) -> None:
    """Store only formation-owner-minted recurrence metadata."""

    conn.execute(
        """
        UPDATE main.memory_ledger_knowledge_candidates
        SET recurrence_contract_version=?,grouping_signature_version=?,
            grouping_identity=?,canon_domain=?,canon_claim_kind=?,
            independent_occurrence_count=?,occurrence_ids_json=?,
            occurrence_digest=?,recurrence_proof_json='{}',public_usable=0,
            updated_at=?
        WHERE candidate_id=?
        """,
        (
            recurrence_contract_version,
            grouping_signature_version,
            grouping_identity,
            canon_domain,
            canon_claim_kind,
            len(occurrence_ids),
            json.dumps(occurrence_ids, separators=(",", ":")),
            occurrence_digest,
            _now(),
            str(candidate_id or ""),
        ),
    )


def _refresh_living_canon_recurrence_proof(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
) -> None:
    """Mirror current lifecycle state into a strict content-free proof."""

    row = conn.execute(
        """
        SELECT candidate_state,candidate_eligible,invalidated_reason,
               eligible_independent_root_count,conflict_value_count,root_digest,
               recurrence_contract_version,grouping_signature_version,
               grouping_identity,canon_domain,canon_claim_kind,
               independent_occurrence_count,occurrence_ids_json,
               occurrence_digest
        FROM main.memory_ledger_knowledge_candidates WHERE candidate_id=?
        """,
        (str(candidate_id or ""),),
    ).fetchone()
    if not row:
        return
    (
        candidate_state,
        candidate_eligible,
        invalidated_reason,
        independent_root_count,
        conflict_value_count,
        root_digest,
        recurrence_contract_version,
        grouping_signature_version,
        grouping_identity,
        _canon_domain,
        _canon_claim_kind,
        independent_occurrence_count,
        occurrence_ids_json,
        occurrence_digest,
    ) = row
    if str(recurrence_contract_version or "") != LIVING_CANON_RECURRENCE_VERSION:
        return
    correction_fence_clear = not str(invalidated_reason or "").startswith(
        "conversation_motif_correction"
    )
    contradiction_clear = bool(
        int(conflict_value_count or 0) <= 1
        and str(candidate_state or "") != "contested"
    )
    proof = {
        "recurrence_contract_version": str(recurrence_contract_version or ""),
        "grouping_signature_version": str(grouping_signature_version or ""),
        "grouping_identity": str(grouping_identity or ""),
        "candidate_state": str(candidate_state or ""),
        "candidate_eligible": bool(candidate_eligible),
        "source_eligible": True,
        "roots_valid": bool(int(independent_root_count or 0) > 0),
        "occurrence_bounded": bool(int(independent_occurrence_count or 0) > 0),
        "correction_fence_clear": correction_fence_clear,
        "contradiction_clear": contradiction_clear,
        "independent_root_count": int(independent_root_count or 0),
        "independent_occurrence_count": int(independent_occurrence_count or 0),
        "root_digest": str(root_digest or ""),
        "occurrence_digest": str(occurrence_digest or ""),
        "bounds": {
            "eligible_ledger_scan_max": _CONVERSATION_MOTIF_MAX_SCAN,
            "motif_candidates_max": _CONVERSATION_MOTIF_MAX_CANDIDATES,
            "retained_roots_max": _CONVERSATION_MOTIF_MAX_ROOTS,
            "occurrence_lookback_max": _CONVERSATION_OCCURRENCE_MAX_SCAN,
            "idle_boundary_seconds": _CONVERSATION_MOTIF_WINDOW_SECONDS,
        },
    }
    established_public = bool(
        str(candidate_state or "") == "established"
        and bool(candidate_eligible)
        and int(independent_root_count or 0) >= 2
        and int(independent_occurrence_count or 0) >= 2
        and correction_fence_clear
        and contradiction_clear
    )
    conn.execute(
        """
        UPDATE main.memory_ledger_knowledge_candidates
        SET recurrence_proof_json=?,public_usable=?,updated_at=?
        WHERE candidate_id=?
        """,
        (
            json.dumps(proof, sort_keys=True, separators=(",", ":")),
            int(established_public),
            _now(),
            str(candidate_id or ""),
        ),
    )


def form_atomic_knowledge_candidate(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
    *,
    _living_formation_receipt: _LivingCanonFormationReceipt | None = None,
) -> AtomicKnowledgeResult:
    """Atomically form one candidate without owning the caller transaction."""
    ensure_memory_ledger_schema(conn)
    savepoint = f"atomic_knowledge_{id(proposal):x}"
    conn.execute(f"SAVEPOINT {savepoint}")
    try:
        result = _form_atomic_knowledge_candidate_impl(
            conn,
            proposal,
            _living_formation_receipt=_living_formation_receipt,
        )
    except Exception:
        conn.execute(f"ROLLBACK TO {savepoint}")
        conn.execute(f"RELEASE {savepoint}")
        raise
    conn.execute(f"RELEASE {savepoint}")
    return result


def _form_atomic_knowledge_candidate_impl(
    conn: sqlite3.Connection,
    proposal: AtomicKnowledgeProposal,
    *,
    _living_formation_receipt: _LivingCanonFormationReceipt | None = None,
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
    recurrence_contract_version = str(
        proposal.recurrence_contract_version or ""
    ).strip()
    grouping_signature_version = str(
        proposal.grouping_signature_version or ""
    ).strip()
    grouping_identity = str(proposal.grouping_identity or "").strip()
    canon_domain = _knowledge_tag(proposal.canon_domain)
    canon_claim_kind = _knowledge_tag(proposal.canon_claim_kind)
    occurrence_ids = tuple(
        sorted(
            {
                str(identity or "").strip()
                for identity in proposal.occurrence_ids
                if str(identity or "").strip()
            }
        )
    )
    living_contract_requested = any(
        (
            recurrence_contract_version,
            grouping_signature_version,
            grouping_identity,
            canon_domain,
            canon_claim_kind,
            occurrence_ids,
        )
    )
    living_authorized = bool(
        _living_formation_receipt is not None
        and _living_formation_receipt.authority
        is _LIVING_CANON_FORMATION_AUTHORITY
    )
    if living_contract_requested and not living_authorized:
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="living_canon_formation_authority_missing",
            root_entry_ids=all_ids,
        )
    if living_authorized and not _living_canon_main_authority_unshadowed(conn):
        return AtomicKnowledgeResult(
            outcome="rejected",
            reason_code="living_canon_authority_shadowed",
            candidate_type=str(proposal.candidate_type or ""),
            root_count=len(all_ids),
        )
    if living_authorized and (
        recurrence_contract_version != LIVING_CANON_RECURRENCE_VERSION
        or grouping_signature_version
        != LIVING_CANON_GROUPING_SIGNATURE_VERSION
        or not re.fullmatch(r"[0-9a-f]{64}", grouping_identity)
        or grouping_identity
        != _knowledge_digest(
            LIVING_CANON_GROUPING_SIGNATURE_VERSION,
            subject_key,
            predicate_key,
            canon_domain,
        )
        or canon_domain not in {"real_community", "lore", "hybrid"}
        or canon_claim_kind not in {"behavior_pattern", "tradition_or_joke"}
        or not occurrence_ids
        or len(occurrence_ids) > _CONVERSATION_MOTIF_MAX_ROOTS
    ):
        return _reject_atomic_knowledge(
            conn,
            proposal,
            guild_id=guild_hint,
            reason_code="living_canon_contract_invalid",
            root_entry_ids=all_ids,
        )
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

    living_root_states: dict[str, PublicAssessmentRootState] = {}
    living_occurrences: dict[str, str] = {}
    if living_authorized:
        if (
            candidate_type != "topic_or_motif"
            or proposal.epistemic_status != "observed"
            or derivative_ids
            or not subject_key.startswith("discord_user:")
        ):
            return _reject_atomic_knowledge(
                conn,
                proposal,
                guild_id=guild_id,
                reason_code="living_canon_claim_kind_ineligible",
                root_entry_ids=all_ids,
            )
        (
            living_root_states,
            living_occurrences,
            living_reasons,
        ) = _living_canon_root_states_and_occurrences(
            conn,
            guild_id=guild_id,
            subject_key=subject_key,
            entry_ids=independent_ids,
        )
        if any(entry_id not in living_root_states for entry_id in independent_ids):
            return _reject_atomic_knowledge(
                conn,
                proposal,
                guild_id=guild_id,
                reason_code=(
                    living_reasons[0]
                    if living_reasons
                    else "source_ineligible"
                ),
                root_entry_ids=all_ids,
            )
        recomputed_occurrences = tuple(
            sorted(
                {
                    str(living_occurrences.get(entry_id) or "")
                    for entry_id in independent_ids
                    if str(living_occurrences.get(entry_id) or "")
                }
            )
        )
        if not recomputed_occurrences or recomputed_occurrences != occurrence_ids:
            return _reject_atomic_knowledge(
                conn,
                proposal,
                guild_id=guild_id,
                reason_code="living_canon_occurrence_proof_mismatch",
                root_entry_ids=all_ids,
            )

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
    occurrence_digest = (
        _knowledge_digest(*occurrence_ids) if living_authorized else ""
    )
    participant_scope_digest = _knowledge_digest(*participants)
    candidate_id = (
        "mlkc_"
        + _knowledge_digest(
            ATOMIC_KNOWLEDGE_SCHEMA_VERSION,
            LIVING_CANON_RECURRENCE_VERSION,
            grouping_identity,
            int(guild_id or 0),
            candidate_type,
            subject_key,
            predicate_key,
            contradiction_key,
            visibility,
            participant_scope_digest,
        )[:40]
        if living_authorized
        else _stable_knowledge_candidate_id(
            guild_id=guild_id,
            candidate_type=candidate_type,
            subject_key=subject_key,
            predicate_key=predicate_key,
            contradiction_key=contradiction_key,
            root_entry_ids=independent_ids,
        )
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
            recurrence_contract_version=recurrence_contract_version,
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
        if living_authorized:
            _store_living_canon_contract(
                conn,
                candidate_id=candidate_id,
                recurrence_contract_version=recurrence_contract_version,
                grouping_signature_version=grouping_signature_version,
                grouping_identity=grouping_identity,
                canon_domain=canon_domain,
                canon_claim_kind=canon_claim_kind,
                occurrence_ids=occurrence_ids,
                occurrence_digest=occurrence_digest,
            )
        reconcile_atomic_knowledge_lifecycle(
            conn,
            candidate_ids=(candidate_id,),
        )
        if living_authorized:
            _refresh_living_canon_recurrence_proof(
                conn,
                candidate_id=candidate_id,
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
    if living_authorized:
        _store_living_canon_contract(
            conn,
            candidate_id=candidate_id,
            recurrence_contract_version=recurrence_contract_version,
            grouping_signature_version=grouping_signature_version,
            grouping_identity=grouping_identity,
            canon_domain=canon_domain,
            canon_claim_kind=canon_claim_kind,
            occurrence_ids=occurrence_ids,
            occurrence_digest=occurrence_digest,
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
    if living_authorized:
        _refresh_living_canon_recurrence_proof(
            conn,
            candidate_id=candidate_id,
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


_CONVERSATION_MOTIF_EXACT_TOKEN_RE = re.compile(
    r"[a-z0-9]+(?:['’][a-z0-9]+)?",
    re.I,
)
_CONVERSATION_MOTIF_EXACT_REFERENCE_RE = re.compile(
    r"\b(?:this|that|these|those|same|above|earlier|previous|latter)\b",
    re.I,
)
_CONVERSATION_MOTIF_EXACT_DROP = frozenset(
    {
        "a",
        "an",
        "the",
        "am",
        "are",
        "be",
        "been",
        "being",
        "is",
        "was",
        "were",
        "really",
        "just",
    }
)


def _conversation_motif_exact_inflection(value: str) -> str:
    token = str(value or "")
    if len(token) > 5 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 5 and token.endswith(("ches", "shes", "sses", "xes", "zes")):
        return token[:-2]
    if len(token) > 5 and token.endswith("ing"):
        base = token[:-3]
        if len(base) >= 3 and base[-1:] == base[-2:-1] and base[-1] != "s":
            base = base[:-1]
        return base
    if len(token) > 4 and token.endswith("ed"):
        return token[:-2]
    if (
        len(token) > 3
        and token.endswith("s")
        and not token.endswith(("is", "ous", "ss", "us"))
    ):
        return token[:-1]
    return token


def _conversation_motif_exact_signature(value: str) -> tuple[str, ...]:
    """Return one conservative ordered signature for an authoritative root."""

    source_text = re.sub(r"\s+", " ", str(value or "")).strip()
    if (
        not _conversation_motif_terms(source_text)
        or _CONVERSATION_MOTIF_EXACT_REFERENCE_RE.search(source_text)
    ):
        return ()
    normalized = unicodedata.normalize("NFKC", source_text).casefold()
    normalized = normalized.replace("’", "'")
    tokens = list(_CONVERSATION_MOTIF_EXACT_TOKEN_RE.findall(normalized))
    signature: list[str] = []
    leading_actor = True
    for token in tokens:
        if token in _CONVERSATION_MOTIF_EXACT_DROP:
            continue
        if leading_actor and token in {"i", "we"}:
            continue
        leading_actor = False
        signature.append(_conversation_motif_exact_inflection(token))
    if not 3 <= len(signature) <= 32:
        return ()
    return tuple(signature)


def _conversation_motif_neutral_predicate(
    signature: tuple[str, ...],
    *,
    subject_key: str,
) -> str:
    if not signature or not str(subject_key or ""):
        return ""
    return "%s%s" % (
        _CONVERSATION_MOTIF_NEUTRAL_PREFIX,
        _knowledge_digest(
            LIVING_CANON_GROUPING_SIGNATURE_VERSION,
            str(subject_key or ""),
            "real_community",
            *signature,
        )[:20],
    )


def _conversation_motif_neutral_groups(
    entries: list[dict[str, Any]],
    *,
    subject_key: str,
    diagnostics: dict[str, int] | None = None,
) -> dict[str, dict[str, Any]]:
    """Group only identical ordered signatures; ambiguous bags fail closed."""

    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for entry in entries:
        signature = _conversation_motif_exact_signature(
            str(entry.get("validated_text") or entry.get("normalized_value") or "")
        )
        if not signature:
            if diagnostics is not None:
                diagnostics["meaning_ambiguous_review_only"] = int(
                    diagnostics.get("meaning_ambiguous_review_only", 0) or 0
                ) + 1
            continue
        grouped.setdefault(signature, []).append(entry)
    bag_orders: dict[tuple[str, ...], set[tuple[str, ...]]] = {}
    for signature in grouped:
        bag_orders.setdefault(tuple(sorted(signature)), set()).add(signature)
    ambiguous = {
        signature
        for signatures in bag_orders.values()
        if len(signatures) > 1
        for signature in signatures
    }
    if ambiguous and diagnostics is not None:
        diagnostics["meaning_ambiguous_review_only"] = int(
            diagnostics.get("meaning_ambiguous_review_only", 0) or 0
        ) + len(ambiguous)
    result: dict[str, dict[str, Any]] = {}
    for signature, signature_entries in sorted(
        grouped.items(),
        key=lambda item: (
            -len({str(row.get("occurrence_identity") or "") for row in item[1]}),
            item[0],
        ),
    ):
        if signature in ambiguous:
            continue
        predicate = _conversation_motif_neutral_predicate(
            signature,
            subject_key=str(subject_key or ""),
        )
        if not predicate:
            continue
        result["neutral:%s" % predicate] = {
            "predicate": predicate,
            "label": " ".join(signature),
            "entries": list(signature_entries),
            "tags": (
                "family_neutral",
                "recurring_public_conversation",
                LIVING_CANON_RECURRENCE_VERSION,
                LIVING_CANON_GROUPING_SIGNATURE_VERSION,
            ),
            "neutral": True,
            "living_v1": True,
            "signature": signature,
        }
    return result


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
    fence_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, str]:
    requested_predicate = str(predicate_key or "")
    include_neutral_wildcard = bool(
        requested_predicate.startswith(_CONVERSATION_MOTIF_NEUTRAL_PREFIX)
        and requested_predicate != _CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD
    )
    rows = conn.execute(
        """
        SELECT correction_entry_id,correction_observed_at,reason_code,
               fence_state,satisfied_at,predicate_key
        FROM main.memory_ledger_conversation_motif_fences
        WHERE guild_id=? AND subject_key=?
          AND (
            predicate_key=?
            OR (?=1 AND predicate_key='conversation_motif_neutral_*')
          )
        ORDER BY predicate_key
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            requested_predicate,
            int(include_neutral_wildcard),
        ),
    ).fetchall()
    by_predicate = {
        str(row[5] or ""): {
            "correction_entry_id": str(row[0] or ""),
            "correction_observed_at": str(row[1] or ""),
            "reason_code": str(row[2] or ""),
            "fence_state": str(row[3] or "active"),
            "satisfied_at": str(row[4] or ""),
            "predicate_key": str(row[5] or ""),
        }
        for row in rows
        if str(row[5] or "")
    }
    allowed_override_predicates = {requested_predicate}
    if include_neutral_wildcard:
        allowed_override_predicates.add(
            _CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD
        )
    for override_predicate, override in (fence_overrides or {}).items():
        normalized_predicate = str(override_predicate or "")
        if normalized_predicate in allowed_override_predicates and override:
            by_predicate[normalized_predicate] = {
                "correction_entry_id": str(
                    override.get("correction_entry_id") or ""
                ),
                "correction_observed_at": str(
                    override.get("correction_observed_at") or ""
                ),
                "reason_code": str(override.get("reason_code") or ""),
                "fence_state": str(
                    override.get("fence_state") or "active"
                ),
                "satisfied_at": str(override.get("satisfied_at") or ""),
                "predicate_key": normalized_predicate,
            }
    exact = by_predicate.get(requested_predicate)
    wildcard = by_predicate.get(_CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD)
    row = exact or wildcard
    if wildcard and str(wildcard.get("fence_state") or "active") == "active":
        exact_satisfies_wildcard = bool(
            exact
            and str(exact.get("fence_state") or "") == "satisfied"
            and str(exact.get("correction_entry_id") or "")
            == str(wildcard.get("correction_entry_id") or "")
            and _parse_knowledge_time(exact.get("satisfied_at")) is not None
            and _parse_knowledge_time(
                wildcard.get("correction_observed_at")
            )
            is not None
            and _parse_knowledge_time(exact.get("satisfied_at"))
            > _parse_knowledge_time(wildcard.get("correction_observed_at"))
        )
        if not exact_satisfies_wildcard:
            if (
                exact
                and str(exact.get("fence_state") or "active") == "active"
                and _parse_knowledge_time(
                    exact.get("correction_observed_at")
                )
                is not None
                and _parse_knowledge_time(
                    wildcard.get("correction_observed_at")
                )
                is not None
                and _parse_knowledge_time(exact.get("correction_observed_at"))
                > _parse_knowledge_time(wildcard.get("correction_observed_at"))
            ):
                row = exact
            else:
                row = wildcard
    if row is None:
        return {}
    return dict(row)


def _conversation_motif_entries_after_fence(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    predicate_key: str,
    entries: list[dict[str, Any]],
    fence_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    fence = _conversation_motif_fence_row(
        conn,
        guild_id=guild_id,
        subject_key=subject_key,
        predicate_key=predicate_key,
        fence_overrides=fence_overrides,
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
    wildcard_neutral = _CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD in set(
        predicate_keys
    )
    exact_predicates = tuple(
        predicate
        for predicate in predicate_keys
        if predicate != _CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD
    )
    predicate_clauses: list[str] = []
    predicate_params: list[str] = []
    if exact_predicates:
        predicate_clauses.append(
            "predicate_key IN (%s)"
            % ",".join("?" for _predicate in exact_predicates)
        )
        predicate_params.extend(exact_predicates)
    if wildcard_neutral:
        predicate_clauses.append("predicate_key LIKE 'conversation_motif_neutral_%'")
    if not predicate_clauses:
        return
    candidate_ids = tuple(
        str(row[0] or "")
        for row in conn.execute(
            f"""
            SELECT candidate_id
            FROM main.memory_ledger_knowledge_candidates
            WHERE guild_id=? AND subject_key=?
              AND candidate_type='topic_or_motif'
              AND ({' OR '.join(predicate_clauses)})
              AND retrieval_tags_json LIKE '%recurring_public_conversation%'
            ORDER BY candidate_id
            """,
            (
                int(guild_id or 0),
                str(subject_key or ""),
                *predicate_params,
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
        UPDATE main.memory_ledger_knowledge_candidates
        SET candidate_state=CASE
              WHEN candidate_state IN ('superseded','retired','invalidated')
                THEN candidate_state
              ELSE 'contested'
            END,
            candidate_eligible=0,live_eligible=0,
            public_usable=CASE
              WHEN COALESCE(recurrence_contract_version,'')=?
                THEN 0
              ELSE public_usable
            END,
            recurrence_proof_json=CASE
              WHEN COALESCE(recurrence_contract_version,'')=?
                THEN '{{"candidate_eligible":false,"source_eligible":false,"roots_valid":false,"correction_fence_clear":false,"invalidated":true}}'
              ELSE recurrence_proof_json
            END,
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
            LIVING_CANON_RECURRENCE_VERSION,
            LIVING_CANON_RECURRENCE_VERSION,
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


def _conversation_motif_correction_predicates(
    conn: sqlite3.Connection,
    *,
    correction_entry: Mapping[str, Any],
    related_entry_ids: tuple[str, ...],
    reason_code: str,
    include_neutral: bool = False,
) -> tuple[str, ...]:
    """Return the deterministic finite predicates affected by a correction."""

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
    predicate_keys = set(_conversation_motif_predicates_for_values(values))
    if include_neutral:
        neutral_predicates = {
            predicate
            for entry in related.values()
            for signature in (
                _conversation_motif_exact_signature(
                    str(entry.get("normalized_value") or "")
                ),
            )
            for predicate in (
                _conversation_motif_neutral_predicate(
                    signature,
                    subject_key=str(correction_entry.get("subject_key") or ""),
                ),
            )
            if signature and predicate
        }
        predicate_keys.update(neutral_predicates)
        if not neutral_predicates and reason_code in {
            "conversation_motif_correction_ambiguous",
            "conversation_motif_correction_unresolved",
        }:
            predicate_keys.add(_CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD)
    return tuple(sorted(predicate_keys))


def _upsert_conversation_motif_correction_fences(
    conn: sqlite3.Connection,
    *,
    correction_entry: dict[str, Any],
    related_entry_ids: tuple[str, ...],
    reason_code: str,
    include_neutral: bool = False,
) -> tuple[str, ...]:
    predicate_keys = _conversation_motif_correction_predicates(
        conn,
        correction_entry=correction_entry,
        related_entry_ids=related_entry_ids,
        reason_code=reason_code,
        include_neutral=include_neutral,
    )
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
        SELECT candidate_state,invalidated_reason,
               recurrence_contract_version,independent_occurrence_count
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
    living_v1 = bool(
        str(state[2] or "") == LIVING_CANON_RECURRENCE_VERSION
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
              AND COALESCE(recurrence_contract_version,'')=?
              AND retrieval_tags_json LIKE '%recurring_public_conversation%'
            ORDER BY candidate_id
            """,
            (
                int(guild_id or 0),
                str(subject_key or ""),
                str(predicate_key or ""),
                candidate_id,
                str(state[2] or ""),
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
    correction_recurrence_satisfied = bool(
        active_correction_fence
        and (not living_v1 or int(state[3] or 0) >= 2)
    )
    if correction_recurrence_satisfied:
        satisfied_at = _now()
        fence_predicate = str(correction_fence.get("predicate_key") or "")
        if fence_predicate == _CONVERSATION_MOTIF_NEUTRAL_FENCE_WILDCARD:
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
                  reason_code=excluded.reason_code,fence_state='satisfied',
                  satisfied_at=excluded.satisfied_at,updated_at=excluded.updated_at
                """,
                (
                    int(guild_id or 0),
                    str(subject_key or ""),
                    str(predicate_key or ""),
                    str(correction_fence.get("correction_entry_id") or ""),
                    str(correction_fence.get("correction_observed_at") or ""),
                    str(correction_fence.get("reason_code") or ""),
                    "satisfied",
                    satisfied_at,
                    satisfied_at,
                    satisfied_at,
                ),
            )
        else:
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
        or correction_recurrence_satisfied
        or result.reason_code == "conversation_motif_roots_refreshed"
    ):
        _record_knowledge_receipt(
            conn,
            guild_id=int(guild_id or 0),
            event_type="refreshed",
            reason_code=(
                "conversation_motif_post_correction_reestablished"
                if correction_recurrence_satisfied
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


def _bounded_conversation_motif_corrections(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int,
    include_neutral: bool = False,
) -> tuple[tuple[dict[str, Any], tuple[str, ...], str], ...]:
    """Resolve a bounded correction set without mutating lifecycle state."""

    rows = conn.execute(
        """
        SELECT entry_id,normalized_value
        FROM main.memory_ledger_entries
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
    resolved: list[tuple[dict[str, Any], tuple[str, ...], str]] = []
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
                        FROM main.memory_ledger_lineage
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
        related_entry_ids = tuple(
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
        )
        resolved.append((correction_entry, related_entry_ids, reason_code))
    return tuple(resolved)


def _preview_conversation_motif_correction_fences(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int,
    include_neutral: bool = False,
) -> dict[str, dict[str, str]]:
    """Compute the fences formation would sync, without writing any table."""

    planned: dict[str, dict[str, str]] = {}
    for correction_entry, related_entry_ids, reason_code in (
        _bounded_conversation_motif_corrections(
            conn,
            guild_id=int(guild_id or 0),
            subject_key=str(subject_key or ""),
            max_scan=max_scan,
            include_neutral=include_neutral,
        )
    ):
        observed = _parse_knowledge_time(correction_entry.get("observed_at"))
        if observed is None:
            continue
        observed_at = _knowledge_time(observed)
        correction_entry_id = str(correction_entry.get("entry_id") or "")
        for predicate_key in _conversation_motif_correction_predicates(
            conn,
            correction_entry=correction_entry,
            related_entry_ids=related_entry_ids,
            reason_code=reason_code,
            include_neutral=include_neutral,
        ):
            existing = _conversation_motif_fence_row(
                conn,
                guild_id=int(guild_id or 0),
                subject_key=str(subject_key or ""),
                predicate_key=predicate_key,
                fence_overrides=planned,
            )
            existing_time = _parse_knowledge_time(
                existing.get("correction_observed_at")
            )
            if existing and existing_time is not None and existing_time > observed:
                continue
            if (
                existing
                and str(existing.get("correction_entry_id") or "")
                == correction_entry_id
                and str(existing.get("fence_state") or "active")
                == "satisfied"
            ):
                continue
            planned[predicate_key] = {
                "correction_entry_id": correction_entry_id,
                "correction_observed_at": observed_at,
                "reason_code": reason_code,
                "fence_state": "active",
                "satisfied_at": "",
                "predicate_key": predicate_key,
            }
    return planned


def _sync_bounded_conversation_motif_corrections(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int,
    include_neutral: bool = False,
) -> None:
    """Discover recent raw corrections missed while formation was disabled."""

    for correction_entry, related_entry_ids, reason_code in (
        _bounded_conversation_motif_corrections(
            conn,
            guild_id=int(guild_id or 0),
            subject_key=str(subject_key or ""),
            max_scan=max_scan,
            include_neutral=include_neutral,
        )
    ):
        _upsert_conversation_motif_correction_fences(
            conn,
            correction_entry=correction_entry,
            related_entry_ids=related_entry_ids,
            reason_code=reason_code,
            include_neutral=include_neutral,
        )


def _conversation_motif_history(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int,
    diagnostics: dict[str, int] | None = None,
    require_legacy_occurrence: bool = True,
    normal_chat_only: bool = False,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT entry_id,subject_key,subject_display_name,normalized_value,observed_at,
               channel_id,channel_name,channel_policy,source_table,source_row_id,
               source_role,source_class,route_mode,visibility,public_usable,
               derived,projection,lifecycle_status
        FROM main.memory_ledger_entries e
        WHERE guild_id=? AND subject_key=?
          AND entry_type='observation' AND predicate_key='conversation'
          AND source_table='conversations' AND source_role='user'
          AND source_class='public_observation'
          AND route_mode IN ('normal_chat','conversation_continuity')
          AND (?=0 OR route_mode='normal_chat')
          AND channel_policy IN (
            'public_home','public_context','public_selective'
          )
          AND visibility IN ('public','public_safe')
          AND public_usable=1 AND derived=0 AND projection=0
          AND lifecycle_status='active'
          AND NOT EXISTS (
            SELECT 1 FROM main.memory_ledger_lineage l
            WHERE l.target_entry_id=e.entry_id
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
            int(bool(normal_chat_only)),
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
        "channel_name",
        "channel_policy",
        "source_table",
        "source_row_id",
        "source_role",
        "source_class",
        "route_mode",
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
        full_entry = (
            _main_public_assessment_entry(
                conn,
                str(entry.get("entry_id") or ""),
            )
            if require_legacy_occurrence
            else {
                **entry,
                "entry_type": "observation",
                "predicate_key": "conversation",
            }
        )
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
        if require_legacy_occurrence:
            # Durable motif formation predates retained raw-conversation
            # binding; keep its existing occurrence owner here.  Open Signal
            # selection independently recomputes the stricter main/raw-bound
            # occurrence and therefore skips this legacy prefilter.
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


def _public_assessment_term_stem(value: str) -> str:
    term = str(value or "").lower()
    if len(term) > 5 and term.endswith("ies"):
        return term[:-3] + "y"
    if len(term) > 4 and term.endswith("es") and term[:-2].endswith(
        ("s", "x", "z", "ch", "sh")
    ):
        return term[:-2]
    for suffix in ("ing", "ed"):
        if len(term) > len(suffix) + 3 and term.endswith(suffix):
            stem = term[: -len(suffix)]
            if len(stem) > 3 and stem[-1:] == stem[-2:-1]:
                stem = stem[:-1]
            return stem
    if len(term) > 4 and term.endswith("s") and not term.endswith("ss"):
        return term[:-1]
    return term


def _public_assessment_action_identity(value: str) -> str:
    stem = _public_assessment_term_stem(value)
    return _PUBLIC_ASSESSMENT_ACTION_ALIASES.get(stem, stem)


def _public_assessment_word_tokens(value: str) -> tuple[str, ...]:
    expanded = re.sub(
        r"\b(i'm|we're|you're)\b",
        lambda match: {
            "i'm": "i am",
            "we're": "we are",
            "you're": "you are",
        }[match.group(1).lower()],
        str(value or "").lower(),
    )
    for contraction, replacement in (
        ("can't", "can not"),
        ("cannot", "can not"),
        ("don't", "do not"),
        ("doesn't", "does not"),
        ("didn't", "did not"),
        ("won't", "will not"),
        ("wouldn't", "would not"),
        ("shouldn't", "should not"),
        ("i've", "i have"),
        ("we've", "we have"),
        ("i'd", "i would"),
        ("we'd", "we would"),
    ):
        expanded = expanded.replace(contraction, replacement)
    return tuple(re.findall(r"[a-z]+(?:'[a-z]+)?", expanded))


def _public_assessment_static_subject_status(value: str) -> bool:
    """Return whether a direct actor clause is a static identity/status."""

    tokens = _public_assessment_word_tokens(value)
    for index, token in enumerate(tokens):
        if token not in {"i", "we", "you"} or index + 1 >= len(tokens):
            continue
        if tokens[index + 1] not in {"am", "are", "was", "were"}:
            continue
        cursor = index + 2
        while cursor < len(tokens) and (
            tokens[cursor]
            in {"currently", "never", "not", "only", "really", "still"}
            or tokens[cursor].endswith("ly")
        ):
            cursor += 1
        if cursor >= len(tokens):
            return True
        return not tokens[cursor].endswith("ing")
    return False


def public_assessment_claim_restricted(value: str) -> bool:
    """Fail closed for identity/status and sensitive direct profile claims."""

    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return bool(
        not text
        or _public_assessment_static_subject_status(text)
        or _PUBLIC_ASSESSMENT_ROLE_REPORT_RE.search(text)
        or _PUBLIC_ASSESSMENT_SENSITIVE_DISCLOSURE_RE.search(text)
        or _PUBLIC_ASSESSMENT_CONTEXTUAL_PRIVATE_RE.search(text)
        or _PUBLIC_ASSESSMENT_EXACT_NUMBER_RE.search(text)
    )


def _public_assessment_actor_action_details(
    value: str,
    *,
    actor_tokens: frozenset[str],
) -> tuple[str, int, int, tuple[str, ...]]:
    tokens = _public_assessment_word_tokens(value)
    for index, token in enumerate(tokens):
        if token not in actor_tokens:
            continue
        tail = tokens[index + 1 : index + 9]
        for tail_index, candidate in enumerate(tail):
            if candidate in {"stop", "stopped", "stops", "stopping", "quit", "quits", "quitting"}:
                for action_offset, action_token in enumerate(
                    tail[tail_index + 1 :],
                    start=tail_index + 1,
                ):
                    if action_token in _PUBLIC_ASSESSMENT_ACTION_SKIP:
                        continue
                    if not action_token.endswith("ing"):
                        break
                    action = _public_assessment_action_identity(action_token)
                    if action and action not in {"i", "we", "you", "your"}:
                        return (
                            action,
                            index,
                            index + 1 + action_offset,
                            tokens,
                        )
                    break
        for candidate in tokens[index + 1 : index + 9]:
            if (
                candidate in _PUBLIC_ASSESSMENT_ACTION_SKIP
                or candidate in {"to", "the", "a", "an"}
                or candidate.endswith("ly")
            ):
                continue
            action = _public_assessment_action_identity(candidate)
            if action and action not in {"i", "we", "you", "your"}:
                return action, index, tokens.index(candidate, index + 1), tokens
            break
    return "", -1, -1, tokens


def _public_assessment_actor_action(
    value: str,
    *,
    actor_tokens: frozenset[str],
) -> str:
    return _public_assessment_actor_action_details(
        value,
        actor_tokens=actor_tokens,
    )[0]


def _public_assessment_first_action(value: str) -> str:
    for token in _public_assessment_word_tokens(value):
        if token in {"let", "let's", "lets", "s", "to", "the", "a", "an"}:
            continue
        if token in _PUBLIC_ASSESSMENT_ACTION_SKIP or token.endswith("ly"):
            continue
        action = _public_assessment_action_identity(token)
        if action:
            return action
    return ""


def _public_assessment_temporal_facets(
    value: str,
    *,
    attribution_mode: str,
    action_index: int,
    tokens: tuple[str, ...],
) -> tuple[str, ...]:
    if attribution_mode != "subject_action" or action_index < 0:
        return ("mode:intent",) if attribution_mode == "authored_topic" else ()
    prefix = tuple(tokens[max(0, action_index - 6) : action_index])
    action_token = str(tokens[action_index] if action_index < len(tokens) else "")
    if "will" in prefix:
        temporal = "future"
    elif "had" in prefix:
        temporal = "past_perfect"
    elif "have" in prefix or "has" in prefix:
        temporal = "perfect"
    elif (
        "did" in prefix
        or "was" in prefix
        or "were" in prefix
        or action_token.endswith("ed")
        or action_token
        in {
            "bought",
            "built",
            "chose",
            "did",
            "felt",
            "found",
            "gave",
            "grew",
            "made",
            "ran",
            "said",
            "saw",
            "taught",
            "told",
            "took",
            "wrote",
        }
    ):
        temporal = "past"
    else:
        temporal = "present"
    frequency = ""
    text = str(value or "")
    if _PUBLIC_ASSESSMENT_SINGLE_OCCURRENCE_RE.search(text):
        frequency = "single"
    elif _PUBLIC_ASSESSMENT_INTERMITTENT_RE.search(text):
        frequency = "intermittent"
    elif _PUBLIC_ASSESSMENT_HABITUAL_RE.search(text):
        frequency = "habitual"
    return tuple(
        ["temporal:%s" % temporal]
        + (["frequency:%s" % frequency] if frequency else [])
    )


def _public_assessment_material_facets(
    value: str,
    *,
    attribution_mode: str,
    action_identity: str,
    action_index: int = -1,
    tokens: tuple[str, ...] = (),
) -> tuple[str, ...]:
    canonical_topics: list[str] = []
    unknown_topics: list[str] = []
    details: list[str] = []
    for token in sorted(_public_assessment_terms(value)):
        stem = _public_assessment_term_stem(token)
        candidate_action = _public_assessment_action_identity(stem)
        if (
            not stem
            or stem in _PUBLIC_ASSESSMENT_MATERIAL_STOPWORDS
            or candidate_action == action_identity
            or stem in _PUBLIC_ASSESSMENT_ACTION_SKIP
            or stem in {"let", "let'", "lets", "let's"}
        ):
            continue
        canonical = _PUBLIC_ASSESSMENT_TOPIC_ALIASES.get(stem)
        if canonical:
            if canonical not in canonical_topics:
                canonical_topics.append(canonical)
            continue
        detail = (
            candidate_action
            if stem in _PUBLIC_ASSESSMENT_ACTION_ALIASES
            else stem
        )
        if detail and detail not in details:
            details.append(detail)
        if (
            attribution_mode in {"subject_action", "authored_topic"}
            and stem not in _PUBLIC_ASSESSMENT_ACTION_ALIASES
            and stem not in unknown_topics
        ):
            unknown_topics.append(stem)
    topics = tuple(sorted(canonical_topics)[:4])
    # Canonical topics define the material point when available.  Otherwise an
    # incidental modifier (for example "noisy" in "noisy sound") could mint a
    # second point for the same audio behavior and falsely satisfy breadth.
    # Unknown facets remain available for genuinely family-neutral evidence.
    if attribution_mode in {"subject_action", "authored_topic"} and not canonical_topics:
        topics += tuple(sorted(unknown_topics)[:3])
    relations: list[str] = []
    for relation, pattern in (
        ("after", r"\bafter\b"),
        ("before", r"\bbefore\b"),
        ("without", r"\bwithout\b"),
        ("with", r"\bwith\b"),
        ("quality_negative", r"\b(?:awful|bad|hate|horrible|terrible)\b"),
        ("quality_positive", r"\b(?:excellent|good|great|love|wonderful)\b"),
    ):
        if re.search(pattern, str(value or ""), re.I):
            relations.append("relation:%s" % relation)
    entities = tuple(
        "entity:%s" % token.lower()
        for token in re.findall(r"\b[A-Z][A-Za-z0-9'’_-]{2,}\b", str(value or ""))
        if token.lower()
        not in {
            "barcode",
            "bnl",
            "discord",
            "from",
            "let's",
            "lets",
            "the",
            "you",
            "your",
        }
    )
    temporal = _public_assessment_temporal_facets(
        value,
        attribution_mode=attribution_mode,
        action_index=action_index,
        tokens=tokens,
    )
    return tuple(
        ["action:%s" % action_identity]
        + ["topic:%s" % topic for topic in topics]
        + ["detail:%s" % detail for detail in sorted(set(details))]
        + sorted(set(relations))
        + sorted(set(entities))
        + list(temporal)
    )


def public_assessment_candidate_core_text(value: str) -> str:
    """Return the direct second-person core after one approved evidence frame."""

    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return ""
    core = _PUBLIC_ASSESSMENT_CANDIDATE_EPISTEMIC_FRAME_RE.sub(
        "",
        text,
        count=1,
    ).strip()
    return core if re.match(r"^you\b", core, re.I) else ""


def public_assessment_semantics(
    value: str,
    *,
    candidate_claim: bool = False,
) -> PublicAssessmentSemantics:
    """Derive conservative actor/action semantics without model inference."""

    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if not text:
        return PublicAssessmentSemantics()
    if candidate_claim:
        text = public_assessment_candidate_core_text(text)
        if not text:
            return PublicAssessmentSemantics()
    elif re.search(r"\b(?:i|we)\b", text, re.I) and not re.match(
        r"^\s*(?:i|we)\b",
        text,
        re.I,
    ):
        # A quoted or externally attributed first-person clause is not a
        # direct self-authored action assertion.
        return PublicAssessmentSemantics()
    if public_assessment_claim_restricted(text):
        return PublicAssessmentSemantics()
    # A first-person grammatical patient is not evidence that the author
    # performed the passive verb.  The deterministic contract has no patient
    # role, so reject instead of reversing actor and action.
    if _PUBLIC_ASSESSMENT_PASSIVE_ACTOR_RE.search(text):
        return PublicAssessmentSemantics()
    if candidate_claim:
        attribution_mode = (
            "subject_action"
            if re.search(r"\b(?:you|your)\b", text, re.I)
            else "third_party_or_ambiguous"
        )
        action, actor_index, action_index, tokens = (
            _public_assessment_actor_action_details(
            text,
            actor_tokens=frozenset({"you"}),
            )
        )
    else:
        if _PUBLIC_ASSESSMENT_REPORTED_THIRD_PARTY_RE.search(text):
            return PublicAssessmentSemantics()
        first_person = bool(re.search(r"\b(?:i|we|my|our)\b", text, re.I))
        action, actor_index, action_index, tokens = (
            _public_assessment_actor_action_details(
            text,
            actor_tokens=frozenset({"i", "we"}),
            )
        )
        if first_person and action:
            attribution_mode = "subject_action"
        elif re.match(r"^\s*let(?:'s|s)\b", text, re.I):
            attribution_mode = "authored_topic"
            action = "suggest"
            actor_index = -1
            action_index = -1
            tokens = _public_assessment_word_tokens(text)
        elif (
            _PUBLIC_ASSESSMENT_THIRD_PARTY_LEAD_RE.search(text)
            or _PUBLIC_ASSESSMENT_SINGLE_PARTY_LEAD_RE.search(text)
        ):
            return PublicAssessmentSemantics()
        elif text.endswith("?"):
            attribution_mode = "authored_topic"
            action = "ask"
        elif re.search(r"\b(?:could|should|might|may|would)\b", text, re.I):
            attribution_mode = "authored_topic"
            action = "suggest"
        else:
            attribution_mode = "authored_topic"
            action = "discuss"
    if not action or attribution_mode == "third_party_or_ambiguous":
        return PublicAssessmentSemantics()
    actor_clause = (
        " ".join(tokens[actor_index + 1 : action_index + 1])
        if attribution_mode == "subject_action"
        and actor_index >= 0
        and action_index > actor_index
        else ""
    )
    polarity = "affirmative"
    if attribution_mode == "subject_action":
        post_action = tuple(tokens[action_index + 1 :])
        first_post_action = post_action[0] if post_action else ""
        if (
            _PUBLIC_ASSESSMENT_NEGATIVE_RE.search(actor_clause)
            or _PUBLIC_ASSESSMENT_CESSATION_RE.search(text)
        ):
            polarity = (
                "conditional"
                if (
                    first_post_action in {"all", "always", "every", "only"}
                    or re.search(
                        r"\bnot\s+(?:all|always|every|only)\b",
                        actor_clause,
                        re.I,
                    )
                )
                else "negative"
            )
        else:
            if first_post_action in {"never", "no", "not", "without"}:
                polarity = "negative"
            elif (
                _PUBLIC_ASSESSMENT_CONDITIONAL_RE.search(actor_clause)
                or _PUBLIC_ASSESSMENT_POST_ACTION_CONDITION_RE.search(text)
            ):
                polarity = "conditional"
    facets = _public_assessment_material_facets(
        text,
        attribution_mode=attribution_mode,
        action_identity=action,
        action_index=action_index,
        tokens=tokens,
    )
    point_facets = tuple(
        facet
        for facet in facets
        if facet.startswith(("action:", "topic:"))
    )
    point_identity = _knowledge_digest(
        _PUBLIC_ASSESSMENT_SEMANTICS_VERSION,
        attribution_mode,
        action,
        point_facets,
    )
    return PublicAssessmentSemantics(
        attribution_mode=attribution_mode,
        polarity=polarity,
        action_identity=action,
        material_facets=facets,
        point_identity=point_identity,
    )


def public_assessment_claim_compatible(
    *,
    attribution_mode: str,
    polarity: str,
    action_identity: str,
    claim: str,
) -> bool:
    """Require actor, action, and polarity compatibility for direct claims."""

    if attribution_mode not in {"subject_action", "authored_topic"}:
        return False
    if attribution_mode == "authored_topic" and str(action_identity or "") not in {
        "ask",
        "discuss",
        "suggest",
    }:
        return False
    semantics = public_assessment_semantics(claim, candidate_claim=True)
    if semantics.attribution_mode != "subject_action":
        return False
    if semantics.action_identity != str(action_identity or ""):
        return False
    return semantics.polarity == str(polarity or "")


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
        or public_assessment_claim_restricted(text)
    ):
        return ""
    return text.replace("```", "")[:240]


def public_assessment_safe_text(value: str) -> str:
    """Expose the selector's exact inert-text normalization for revalidation.

    Packet assembly uses this read-only helper to bind a selector result back
    to the current authoritative Ledger row without maintaining a second text
    policy.
    """

    return _public_assessment_text(value)


def _main_table_columns(
    conn: sqlite3.Connection,
    table_name: str,
) -> set[str]:
    """Return columns from the authoritative main schema only."""

    safe_name = str(table_name or "")
    if safe_name not in {
        "conversations",
        "memory_ledger_entries",
        "memory_ledger_lineage",
        "memory_ledger_participants",
        "memory_ledger_conversation_motif_fences",
        "memory_moment_windows",
        "memory_moment_members",
        "memory_moment_participants",
        "bnl_journal_source_events",
    }:
        return set()
    if not conn.execute(
        "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?",
        (safe_name,),
    ).fetchone():
        return set()
    return {
        str(row[1] or "")
        for row in conn.execute(
            "PRAGMA main.table_info(%s)" % safe_name
        ).fetchall()
        if len(row) > 1 and str(row[1] or "")
    }


_PUBLIC_ASSESSMENT_LEDGER_COLUMNS = (
    "entry_id",
    "schema_version",
    "guild_id",
    "subject_key",
    "subject_display_name",
    "entry_type",
    "predicate_key",
    "normalized_value",
    "source_class",
    "source_table",
    "source_row_id",
    "source_revision",
    "source_role",
    "route_mode",
    "channel_id",
    "channel_name",
    "channel_policy",
    "source_message_id",
    "visibility",
    "confidence",
    "public_usable",
    "derived",
    "projection",
    "observed_at",
    "source_sequence",
    "lifecycle_status",
    "updated_at",
)


def _main_public_assessment_entry(
    conn: sqlite3.Connection,
    entry_id: str,
) -> dict[str, Any]:
    if not str(entry_id or ""):
        return {}
    required = set(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS)
    if not required.issubset(
        _main_table_columns(conn, "memory_ledger_entries")
    ):
        return {}
    row = conn.execute(
        "SELECT %s FROM main.memory_ledger_entries WHERE entry_id=?"
        % ",".join(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS),
        (str(entry_id),),
    ).fetchone()
    if not row:
        return {}
    return dict(zip(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS, row))


def _public_assessment_sql_identity(value: Any) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip()).lower()
    normalized = re.sub(r"\s*([(),=])\s*", r"\1", normalized)
    return normalized.rstrip(";")


def _main_public_assessment_journal_trigger_snapshot(
    conn: sqlite3.Connection,
) -> tuple[tuple[str, str], ...]:
    """Return the exact immutable Journal trigger contract or no authority."""

    expected_sql = {
        "trg_bnl_journal_sources_no_duplicate_insert": """
            CREATE TRIGGER trg_bnl_journal_sources_no_duplicate_insert
            BEFORE INSERT ON bnl_journal_source_events
            WHEN EXISTS (
                SELECT 1 FROM bnl_journal_source_events
                WHERE event_seq=NEW.event_seq
                   OR (
                        guild_id=NEW.guild_id
                        AND source_kind=NEW.source_kind
                        AND source_key=NEW.source_key
                   )
            )
            BEGIN
                SELECT RAISE(ABORT, 'bnl_journal_source_events_duplicate_identity');
            END
        """,
        "trg_bnl_journal_sources_no_update": """
            CREATE TRIGGER trg_bnl_journal_sources_no_update
            BEFORE UPDATE ON bnl_journal_source_events
            BEGIN
                SELECT RAISE(ABORT, 'bnl_journal_source_events_immutable');
            END
        """,
        "trg_bnl_journal_sources_no_delete": """
            CREATE TRIGGER trg_bnl_journal_sources_no_delete
            BEFORE DELETE ON bnl_journal_source_events
            BEGIN
                SELECT RAISE(ABORT, 'bnl_journal_source_events_immutable');
            END
        """,
    }
    rows = conn.execute(
        """
        SELECT name,tbl_name,sql
        FROM main.sqlite_master
        WHERE type='trigger' AND name IN (?,?,?)
        ORDER BY name
        """,
        tuple(sorted(expected_sql)),
    ).fetchall()
    snapshot = tuple(
        (str(name or ""), _public_assessment_sql_identity(sql))
        for name, table_name, sql in rows
        if str(table_name or "") == "bnl_journal_source_events"
    )
    expected = tuple(
        sorted(
            (name, _public_assessment_sql_identity(sql))
            for name, sql in expected_sql.items()
        )
    )
    return snapshot if snapshot == expected else ()


_PUBLIC_ASSESSMENT_JOURNAL_RECEIPT_COLUMNS = (
    "event_seq",
    "guild_id",
    "source_kind",
    "source_key",
    "occurred_at_ms",
    "channel_id",
    "channel_policy",
    "subject_ref",
    "private_display_name",
    "raw_text",
    "content_hash",
    "public_usable",
    "metadata_json",
)


def _public_assessment_journal_source_identity(
    raw: Mapping[str, Any],
) -> tuple[int, str] | None:
    raw_id = _public_assessment_int(raw.get("id"))
    raw_guild = _public_assessment_int(raw.get("guild_id"))
    raw_message = _public_assessment_int(raw.get("message_id"))
    if raw_id is None or raw_guild is None:
        return None
    source_key = (
        str(raw_message)
        if raw_message not in {None, 0}
        else "legacy_row:%s" % raw_id
    )
    return raw_guild, source_key


def _main_public_assessment_journal_receipt_map(
    conn: sqlite3.Connection,
    raw_rows: Iterable[Mapping[str, Any]],
    *,
    journal_trigger_snapshot: tuple[tuple[str, str], ...],
) -> dict[tuple[int, str], tuple[tuple[Any, ...], ...]]:
    """Bulk-read exact Journal receipts for a bounded raw source set."""

    if not journal_trigger_snapshot:
        return {}
    identities = {
        identity
        for raw in raw_rows
        if (identity := _public_assessment_journal_source_identity(raw))
        is not None
    }
    grouped: dict[tuple[int, str], list[tuple[Any, ...]]] = {}
    for guild_id in sorted({identity[0] for identity in identities}):
        source_keys = sorted(
            identity[1] for identity in identities if identity[0] == guild_id
        )
        for offset in range(0, len(source_keys), 250):
            chunk = source_keys[offset : offset + 250]
            if not chunk:
                continue
            placeholders = ",".join("?" for _key in chunk)
            row_limit = (len(chunk) * 2) + 1
            rows = conn.execute(
                "SELECT %s FROM main.bnl_journal_source_events "
                "WHERE guild_id=? AND source_kind='discord_message' "
                "AND source_key IN (%s) ORDER BY event_seq LIMIT ?"
                % (
                    ",".join(_PUBLIC_ASSESSMENT_JOURNAL_RECEIPT_COLUMNS),
                    placeholders,
                ),
                (guild_id, *chunk, row_limit),
            ).fetchall()
            if len(rows) >= row_limit:
                for source_key in chunk:
                    grouped[(guild_id, source_key)] = [(), ()]
                continue
            for row in rows:
                identity = (
                    _public_assessment_int(row[1]),
                    str(row[3] or ""),
                )
                if identity[0] is None or not identity[1]:
                    continue
                grouped.setdefault(identity, []).append(tuple(row))
    return {
        identity: tuple(rows)
        for identity, rows in grouped.items()
    }


def _main_public_assessment_route_authority(
    conn: sqlite3.Connection,
    raw: Mapping[str, Any],
    *,
    journal_columns: frozenset[str] | None = None,
    journal_trigger_snapshot: tuple[tuple[str, str], ...] | None = None,
    journal_receipts: Mapping[
        tuple[int, str], tuple[tuple[Any, ...], ...]
    ] | None = None,
) -> tuple[str, tuple[Any, ...]] | None:
    """Resolve route from raw capture or an exact immutable Journal receipt."""

    raw_route = str(raw.get("route_mode") or "").strip().lower()
    if raw_route == "unknown":
        raw_route = ""
    receipt_snapshot: tuple[Any, ...] = ()
    receipt_route = ""
    if journal_columns is None:
        journal_columns = frozenset(
            _main_table_columns(conn, "bnl_journal_source_events")
        )
    required_journal = {
        "event_seq",
        "guild_id",
        "source_kind",
        "source_key",
        "occurred_at_ms",
        "channel_id",
        "channel_policy",
        "subject_ref",
        "private_display_name",
        "raw_text",
        "content_hash",
        "public_usable",
        "metadata_json",
    }
    if required_journal.issubset(journal_columns):
        if journal_trigger_snapshot is None:
            journal_trigger_snapshot = (
                _main_public_assessment_journal_trigger_snapshot(conn)
            )
        if journal_trigger_snapshot:
            raw_id = _public_assessment_int(raw.get("id"))
            raw_guild = _public_assessment_int(raw.get("guild_id"))
            raw_user = _public_assessment_int(raw.get("user_id"))
            raw_channel = _public_assessment_int(raw.get("channel_id"))
            raw_message = _public_assessment_int(raw.get("message_id"))
            source_identity = _public_assessment_journal_source_identity(raw)
            if source_identity is None:
                return None
            source_key = source_identity[1]
            rows = (
                tuple(journal_receipts.get(source_identity, ()))
                if journal_receipts is not None
                else tuple(
                    conn.execute(
                        "SELECT %s FROM main.bnl_journal_source_events "
                        "WHERE guild_id=? AND source_kind='discord_message' "
                        "AND source_key=? ORDER BY event_seq LIMIT 2"
                        % ",".join(
                            _PUBLIC_ASSESSMENT_JOURNAL_RECEIPT_COLUMNS
                        ),
                        (int(raw_guild or 0), source_key),
                    ).fetchall()
                )
            )
            if len(rows) > 1:
                return None
            if rows:
                row = tuple(rows[0])
                try:
                    metadata = json.loads(str(row[12] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    return None
                if not isinstance(metadata, dict):
                    return None
                observed = _parse_knowledge_time(raw.get("timestamp"))
                expected_ms = (
                    int(observed.timestamp() * 1000) if observed is not None else None
                )
                metadata_row = _public_assessment_int(
                    metadata.get("conversationRowId")
                )
                metadata_message = _public_assessment_int(
                    metadata.get("messageId")
                )
                if (
                    None
                    in {
                        raw_id,
                        raw_guild,
                        raw_user,
                        raw_channel,
                        expected_ms,
                    }
                    or _public_assessment_int(row[1]) != raw_guild
                    or str(row[2] or "") != "discord_message"
                    or str(row[3] or "") != source_key
                    or _public_assessment_int(row[4]) != expected_ms
                    or _public_assessment_int(row[5]) != raw_channel
                    or str(row[6] or "") != str(raw.get("channel_policy") or "")
                    or str(row[7] or "") != subject_key_for_user(raw_user)
                    or str(row[8] or "") != str(raw.get("user_name") or "")
                    or str(row[9] or "") != str(raw.get("content") or "")
                    or str(row[10] or "")
                    != hashlib.sha256(
                        str(raw.get("content") or "").encode("utf-8")
                    ).hexdigest()
                    or _public_assessment_bool_state(row[11]) is not True
                    or metadata_row != raw_id
                    or (
                        raw_message not in {None, 0}
                        and metadata_message != raw_message
                    )
                ):
                    return None
                metadata_route = str(metadata.get("routeMode") or "").strip().lower()
                metadata_source = str(metadata.get("source") or "").strip().lower()
                if metadata_source == "discord_backfill":
                    if metadata_route not in {"", "unknown", "conversation_continuity"}:
                        return None
                    receipt_route = "conversation_continuity"
                elif metadata_route and metadata_route != "unknown":
                    receipt_route = metadata_route
                receipt_snapshot = (journal_trigger_snapshot, row)
    if raw_route and receipt_route and raw_route != receipt_route:
        return None
    resolved = raw_route or receipt_route
    if not resolved:
        return None
    # Conversation-continuity is a historical/backfill route.  A mutable raw
    # label is never sufficient authority for that route; require the exact
    # immutable Journal receipt.  Native normal_chat capture remains bound by
    # its raw route column as before.
    if resolved == "conversation_continuity" and receipt_route != resolved:
        return None
    return resolved, receipt_snapshot


def _main_public_assessment_root_identity(
    conn: sqlite3.Connection,
    entry: Mapping[str, Any],
) -> str:
    """Return the exact raw root; raw Open Signal never follows aliases."""

    entry_id = str(entry.get("entry_id") or "")
    guild_id = _public_assessment_int(entry.get("guild_id"))
    source_table = str(entry.get("source_table") or "")
    source_row_id = str(entry.get("source_row_id") or "")
    if (
        not entry_id
        or not guild_id
        or source_table != "conversations"
        or not source_row_id
    ):
        return ""
    # No production owner emits duplicate aliases for a raw conversation.  Any
    # such edge is therefore malformed authority, including cross-guild edges,
    # self loops, cycles, and dangling/cross-subject targets.
    if conn.execute(
        """
        SELECT 1
        FROM main.memory_ledger_lineage
        WHERE entry_id=? AND lineage_type='duplicate_of'
        LIMIT 1
        """,
        (entry_id,),
    ).fetchone():
        return ""
    return _knowledge_digest(guild_id, source_table, source_row_id)


def _main_public_assessment_moment_occurrence(
    conn: sqlite3.Connection,
    entry: Mapping[str, Any],
) -> str | None:
    """Return a validated canonical Moment occurrence, or None when absent.

    An empty string means a Moment-shaped edge exists but fails authority
    validation.  Callers must fail closed instead of falling back to a raw
    exchange window.
    """

    entry_id = str(entry.get("entry_id") or "")
    guild_id = _public_assessment_int(entry.get("guild_id"))
    subject_key = str(entry.get("subject_key") or "")
    rows = conn.execute(
        """
        SELECT guild_id,target_entry_id
        FROM main.memory_ledger_lineage
        WHERE entry_id=? AND lineage_type='part_of_moment'
        ORDER BY guild_id,target_entry_id
        """,
        (entry_id,),
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1 or guild_id is None:
        return ""
    edge_guild = _public_assessment_int(rows[0][0])
    target_id = str(rows[0][1] or "")
    if edge_guild != guild_id or not target_id or target_id == entry_id:
        return ""
    target = _main_public_assessment_entry(conn, target_id)
    target_guild = _public_assessment_int(target.get("guild_id")) if target else None
    target_public = (
        _public_assessment_bool_state(target.get("public_usable"))
        if target
        else None
    )
    target_derived = (
        _public_assessment_bool_state(target.get("derived")) if target else None
    )
    target_projection = (
        _public_assessment_bool_state(target.get("projection")) if target else None
    )
    moment_id = str(target.get("source_row_id") or "") if target else ""
    expected_target_id = (
        stable_entry_id(
            guild_id=guild_id,
            source_table="memory_moment_windows",
            source_row_id=moment_id,
            source_revision="1",
            entry_type="shared_moment",
            subject_key="moment:%s" % moment_id,
            predicate_key="shared_moment",
        )
        if moment_id
        else ""
    )
    if not target or (
        target_guild != guild_id
        or str(target.get("schema_version") or "") != MEMORY_LEDGER_SCHEMA_VERSION
        or target_id != expected_target_id
        or str(target.get("source_table") or "") != "memory_moment_windows"
        or str(target.get("source_revision") or "") != "1"
        or str(target.get("source_role") or "") != "derived_assessment"
        or str(target.get("entry_type") or "") != "shared_moment"
        or str(target.get("subject_key") or "") != "moment:%s" % moment_id
        or str(target.get("predicate_key") or "") != "shared_moment"
        or str(target.get("source_class") or "")
        != SourceClass.DERIVED_SUMMARY.value
        or str(target.get("lifecycle_status") or "") != REVIEW_ONLY_LIFECYCLE
        or str(target.get("visibility") or "")
        not in {Visibility.PUBLIC.value, Visibility.PUBLIC_SAFE.value}
        or target_public is not True
        or target_derived is not True
        or target_projection is not True
    ):
        return ""
    reverse = conn.execute(
        """
        SELECT guild_id
        FROM main.memory_ledger_lineage
        WHERE entry_id=? AND lineage_type='derived_from'
          AND target_entry_id=?
        ORDER BY guild_id
        """,
        (target_id, entry_id),
    ).fetchall()
    if len(reverse) != 1 or _public_assessment_int(reverse[0][0]) != guild_id:
        return ""
    target_lineage = conn.execute(
        """
        SELECT guild_id,lineage_type,target_entry_id
        FROM main.memory_ledger_lineage
        WHERE entry_id=?
        ORDER BY guild_id,lineage_type,target_entry_id
        """,
        (target_id,),
    ).fetchall()
    if not target_lineage or any(
        _public_assessment_int(edge[0]) != guild_id
        or str(edge[1] or "") != "derived_from"
        or not str(edge[2] or "")
        or str(edge[2] or "") == target_id
        for edge in target_lineage
    ):
        return ""
    target_subject_participants = conn.execute(
        """
        SELECT guild_id FROM main.memory_ledger_participants
        WHERE entry_id=? AND participant_key=?
        """,
        (target_id, subject_key),
    ).fetchall()
    if not target_subject_participants or any(
        _public_assessment_int(row[0]) != guild_id
        for row in target_subject_participants
    ):
        return ""
    derived_targets = {str(edge[2] or "") for edge in target_lineage}
    if any(
        _public_assessment_int(
            _main_public_assessment_entry(conn, source_id).get("guild_id")
        )
        != guild_id
        for source_id in derived_targets
    ):
        return ""
    required_moment_columns = {
        "moment_id",
        "guild_id",
        "channel_id",
        "channel_policy",
        "route_mode",
        "lifecycle_status",
        "visibility",
        "public_usable",
        "canonical_ledger_entry_id",
    }
    if not required_moment_columns.issubset(
        _main_table_columns(conn, "memory_moment_windows")
    ) or not {"moment_id", "ledger_entry_id"}.issubset(
        _main_table_columns(conn, "memory_moment_members")
    ) or not {"moment_id", "participant_key"}.issubset(
        _main_table_columns(conn, "memory_moment_participants")
    ):
        return ""
    window = conn.execute(
        """
        SELECT guild_id,channel_id,channel_policy,route_mode,lifecycle_status,
               visibility,public_usable,canonical_ledger_entry_id
        FROM main.memory_moment_windows
        WHERE moment_id=?
        """,
        (moment_id,),
    ).fetchone()
    if not window or (
        _public_assessment_int(window[0]) != guild_id
        or _public_assessment_int(window[1])
        != _public_assessment_int(entry.get("channel_id"))
        or str(window[2] or "") != str(entry.get("channel_policy") or "")
        or str(window[3] or "") != str(entry.get("route_mode") or "")
        or str(window[4] or "") != "finalized"
        or str(window[5] or "") != str(target.get("visibility") or "")
        or _public_assessment_bool_state(window[6]) is not True
        or str(window[7] or "") != target_id
    ):
        return ""
    all_member_rows = conn.execute(
        """
        SELECT ledger_entry_id FROM main.memory_moment_members
        WHERE moment_id=?
        ORDER BY ledger_entry_id
        """,
        (moment_id,),
    ).fetchall()
    subject_rows = conn.execute(
        """
        SELECT 1 FROM main.memory_moment_participants
        WHERE moment_id=? AND participant_key=?
        """,
        (moment_id, subject_key),
    ).fetchall()
    if (
        {str(row[0] or "") for row in all_member_rows} != derived_targets
        or entry_id not in derived_targets
        or len(subject_rows) < 1
    ):
        return ""
    return _knowledge_digest(
        "conversation_moment_occurrence",
        guild_id,
        target_id,
    )


def _main_public_assessment_occurrence_candidates(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    channel_id: int,
    channel_policy: str,
    max_observed_at: str,
) -> list[tuple[str, str, int]]:
    """Read only raw-bound eligible rows that may define an exchange window."""

    raw_columns = _main_table_columns(conn, "conversations")
    if not {
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
    }.issubset(raw_columns):
        return []
    ledger_select = ",".join(
        "e.%s" % column for column in _PUBLIC_ASSESSMENT_LEDGER_COLUMNS
    )
    raw_select = (
        "c.id,c.guild_id,c.user_id,c.user_name,c.role,c.content,"
        "c.channel_id,c.channel_policy,c.timestamp,"
        + ("c.channel_name" if "channel_name" in raw_columns else "''")
        + ","
        + ("c.message_id" if "message_id" in raw_columns else "NULL")
        + ","
        + ("c.route_mode" if "route_mode" in raw_columns else "NULL")
        + ","
        + ("c.public_usable" if "public_usable" in raw_columns else "NULL")
        + ","
        + ("c.visibility" if "visibility" in raw_columns else "NULL")
    )
    rows = conn.execute(
        """
        SELECT %s,%s
        FROM main.memory_ledger_entries e
        JOIN main.conversations c
          ON c.guild_id=e.guild_id
         AND CAST(c.id AS TEXT)=e.source_row_id
        WHERE e.guild_id=? AND e.subject_key=?
          AND e.entry_type='observation' AND e.predicate_key='conversation'
          AND e.source_table='conversations' AND e.source_role='user'
          AND e.source_class='public_observation'
          AND e.route_mode IN ('normal_chat','conversation_continuity')
          AND e.channel_id=? AND e.channel_policy=?
          AND e.visibility IN ('public','public_safe')
          AND e.confidence='medium'
          AND e.public_usable=1 AND e.derived=0 AND e.projection=0
          AND e.lifecycle_status='active'
          AND julianday(e.observed_at) <= julianday(?)
          AND NOT EXISTS (
            SELECT 1 FROM main.memory_ledger_lineage incoming
            WHERE incoming.target_entry_id=e.entry_id
              AND incoming.lineage_type IN (
                'correction_of','supersedes','retracts'
              )
          )
          AND NOT EXISTS (
            SELECT 1 FROM main.memory_ledger_lineage duplicate_edge
            WHERE duplicate_edge.entry_id=e.entry_id
              AND duplicate_edge.lineage_type='duplicate_of'
          )
          AND (
            SELECT COUNT(*) FROM main.memory_ledger_participants participant
            WHERE participant.entry_id=e.entry_id
          )=1
          AND EXISTS (
            SELECT 1 FROM main.memory_ledger_participants participant
            WHERE participant.entry_id=e.entry_id
              AND participant.guild_id=e.guild_id
              AND participant.participant_key=e.subject_key
              AND LOWER(participant.participant_role)='author'
              AND participant.order_index=0
          )
        ORDER BY julianday(e.observed_at) DESC,e.observed_at DESC,e.entry_id DESC
        LIMIT ?
        """
        % (ledger_select, raw_select),
        (
            int(guild_id or 0),
            str(subject_key or ""),
            int(channel_id or 0),
            str(channel_policy or ""),
            str(max_observed_at or ""),
            _CONVERSATION_OCCURRENCE_MAX_SCAN + 1,
        ),
    ).fetchall()
    ledger_count = len(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS)
    raw_keys = (
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
        "channel_name",
        "message_id",
        "route_mode",
        "public_usable",
        "visibility",
    )
    journal_columns = frozenset(
        _main_table_columns(conn, "bnl_journal_source_events")
    )
    required_journal = {
        "event_seq",
        "guild_id",
        "source_kind",
        "source_key",
        "occurred_at_ms",
        "channel_id",
        "channel_policy",
        "subject_ref",
        "private_display_name",
        "raw_text",
        "content_hash",
        "public_usable",
        "metadata_json",
    }
    journal_trigger_snapshot = (
        _main_public_assessment_journal_trigger_snapshot(conn)
        if required_journal.issubset(journal_columns)
        else ()
    )
    raw_mappings = tuple(
        dict(zip(raw_keys, row[ledger_count:])) for row in rows
    )
    journal_receipts = _main_public_assessment_journal_receipt_map(
        conn,
        raw_mappings,
        journal_trigger_snapshot=journal_trigger_snapshot,
    )
    eligible: list[tuple[str, str, int]] = []
    for row, raw in zip(rows, raw_mappings):
        candidate = dict(
            zip(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS, row[:ledger_count])
        )
        candidate_guild = _public_assessment_int(candidate.get("guild_id"))
        candidate_channel = _public_assessment_int(candidate.get("channel_id"))
        raw_id = _public_assessment_int(raw.get("id"))
        raw_guild = _public_assessment_int(raw.get("guild_id"))
        raw_user = _public_assessment_int(raw.get("user_id"))
        raw_channel = _public_assessment_int(raw.get("channel_id"))
        sequence = _public_assessment_int(candidate.get("source_sequence"))
        message_id = _public_assessment_int(raw.get("message_id"))
        expected_entry_id = stable_entry_id(
            guild_id=candidate_guild,
            source_table="conversations",
            source_row_id=str(candidate.get("source_row_id") or ""),
            source_revision=str(candidate.get("source_revision") or ""),
            entry_type="observation",
            subject_key=str(candidate.get("subject_key") or ""),
            predicate_key="conversation",
        )
        mapped_visibility = _visibility(
            str(candidate.get("channel_policy") or "")
        ).value
        if None in {
            candidate_guild,
            candidate_channel,
            raw_id,
            raw_guild,
            raw_user,
            raw_channel,
            sequence,
        } or (
            str(candidate.get("schema_version") or "")
            != MEMORY_LEDGER_SCHEMA_VERSION
            or str(candidate.get("entry_id") or "") != expected_entry_id
            or str(candidate.get("source_row_id") or "") != str(raw_id)
            or str(candidate.get("source_revision") or "") != str(raw_id)
            or sequence not in {raw_id, message_id if message_id else raw_id}
            or candidate_guild != raw_guild
            or candidate_channel != raw_channel
            or subject_key_for_user(raw_user) != str(candidate.get("subject_key") or "")
            or str(raw.get("user_name") or "")
            != str(candidate.get("subject_display_name") or "")
            or str(raw.get("role") or "").lower() != "user"
            or str(raw.get("content") or "")[:500]
            != str(candidate.get("normalized_value") or "")
            or str(raw.get("channel_policy") or "")
            != str(candidate.get("channel_policy") or "")
            or str(raw.get("timestamp") or "")
            != str(candidate.get("observed_at") or "")
            or str(candidate.get("visibility") or "") != mapped_visibility
            or (
                "channel_name" in raw_columns
                and str(raw.get("channel_name") or "")
                != str(candidate.get("channel_name") or "")
            )
            or (
                "message_id" in raw_columns
                and str(message_id or "")
                != str(_public_assessment_int(candidate.get("source_message_id")) or "")
            )
            or (
                "public_usable" in raw_columns
                and _public_assessment_bool_state(raw.get("public_usable"))
                is not True
            )
            or (
                "visibility" in raw_columns
                and str(raw.get("visibility") or "") != mapped_visibility
            )
            or not _public_assessment_text(
                str(candidate.get("normalized_value") or "")
            )
            or not public_assessment_semantics(
                str(candidate.get("normalized_value") or "")
            ).point_identity
            or _knowledge_operational_or_test_source(dict(candidate))
        ):
            continue
        route_authority = _main_public_assessment_route_authority(
            conn,
            raw,
            journal_columns=journal_columns,
            journal_trigger_snapshot=journal_trigger_snapshot,
            journal_receipts=journal_receipts,
        )
        if (
            route_authority is None
            or route_authority[0] != str(candidate.get("route_mode") or "")
            or route_authority[0] not in _PUBLIC_ASSESSMENT_ALLOWED_ROUTES
        ):
            continue
        participant = conn.execute(
            """
            SELECT display_name FROM main.memory_ledger_participants
            WHERE entry_id=? AND guild_id=? AND participant_key=?
              AND LOWER(participant_role)='author' AND order_index=0
            """,
            (
                str(candidate.get("entry_id") or ""),
                candidate_guild,
                str(candidate.get("subject_key") or ""),
            ),
        ).fetchone()
        if not participant or str(participant[0] or "") != str(raw.get("user_name") or ""):
            continue
        eligible.append(
            (
                str(candidate.get("entry_id") or ""),
                str(candidate.get("observed_at") or ""),
                int(sequence or 0),
            )
        )
    return eligible


def _main_public_assessment_occurrence_identity(
    conn: sqlite3.Connection,
    entry: Mapping[str, Any],
    *,
    raw_exchange_only: bool = False,
) -> str:
    """Recompute the bounded exchange identity from the current main state."""

    root_identity = _main_public_assessment_root_identity(conn, entry)
    if not root_identity:
        return ""
    if (
        str(entry.get("source_table") or "") != "conversations"
        or str(entry.get("source_role") or "").lower() != "user"
    ):
        return root_identity
    if not raw_exchange_only:
        moment_occurrence = _main_public_assessment_moment_occurrence(conn, entry)
        if moment_occurrence is not None:
            return moment_occurrence
    observed = _parse_knowledge_time(entry.get("observed_at"))
    current_sequence = _public_assessment_int(entry.get("source_sequence"))
    if observed is None or current_sequence is None or current_sequence <= 0:
        return ""
    scope = (
        _public_assessment_int(entry.get("guild_id")) or 0,
        _public_assessment_int(entry.get("channel_id")) or 0,
        str(entry.get("channel_policy") or "unknown"),
        str(entry.get("subject_key") or ""),
    )
    rows = _main_public_assessment_occurrence_candidates(
        conn,
        guild_id=scope[0],
        subject_key=scope[3],
        channel_id=scope[1],
        channel_policy=scope[2],
        max_observed_at=str(entry.get("observed_at") or ""),
    )
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
        if (
            prior_time - row_time
        ).total_seconds() > _CONVERSATION_MOTIF_WINDOW_SECONDS:
            found_idle_boundary = True
            break
        anchor_id = str(row_entry_id or anchor_id)
        prior_time = row_time
    if has_unscanned_prior and not found_idle_boundary:
        return ""
    return _knowledge_digest("conversation_occurrence", *scope, anchor_id)


def _main_public_assessment_fences(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
) -> tuple[tuple[str, ...], ...] | None:
    columns = _main_table_columns(
        conn,
        "memory_ledger_conversation_motif_fences",
    )
    required = {
        "guild_id",
        "subject_key",
        "predicate_key",
        "correction_entry_id",
        "correction_observed_at",
        "reason_code",
        "fence_state",
        "satisfied_at",
        "updated_at",
    }
    if not required.issubset(columns):
        return None
    return tuple(
        tuple(str(value or "") for value in row)
        for row in conn.execute(
            """
            SELECT predicate_key,correction_entry_id,
                   correction_observed_at,reason_code,fence_state,
                   satisfied_at,updated_at
            FROM main.memory_ledger_conversation_motif_fences
            WHERE guild_id=? AND subject_key=?
              AND reason_code IN (
                'conversation_motif_correction_ambiguous',
                'conversation_motif_correction_unresolved'
              )
            ORDER BY predicate_key,correction_entry_id
            """,
            (int(guild_id or 0), str(subject_key or "")),
        ).fetchall()
    )


def _public_assessment_state_digest(*parts: Any) -> str:
    payload = json.dumps(
        parts,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _public_assessment_bool_state(value: Any) -> bool | None:
    if value is True or value is False:
        return bool(value)
    if isinstance(value, int) and not isinstance(value, bool):
        if value in {0, 1}:
            return bool(value)
        return None
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return None


def _public_assessment_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()):
        try:
            return int(value.strip())
        except (ValueError, OverflowError):
            return None
    return None


def _main_public_assessment_later_guard_state(
    conn: sqlite3.Connection,
    *,
    entry: Mapping[str, Any],
    semantics: PublicAssessmentSemantics,
) -> tuple[tuple[tuple[Any, ...], ...], bool, bool] | None:
    """Bind later corrections and contradictions to retained raw authorship."""

    guild_id = _public_assessment_int(entry.get("guild_id"))
    subject_key = str(entry.get("subject_key") or "")
    entry_id = str(entry.get("entry_id") or "")
    source_row_id = str(entry.get("source_row_id") or "")
    observed_at = str(entry.get("observed_at") or "")
    observed = _parse_knowledge_time(observed_at)
    if (
        guild_id is None
        or not subject_key.startswith("discord_user:")
        or not entry_id
        or not source_row_id
        or observed is None
        or not semantics.point_identity
        or not semantics.polarity
    ):
        return None
    try:
        user_id = int(subject_key.split(":", 1)[1])
    except (TypeError, ValueError, IndexError):
        return None
    raw_columns = _main_table_columns(conn, "conversations")
    required_raw = {
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
    }
    if not required_raw.issubset(raw_columns):
        return None
    optional_raw = tuple(
        column
        for column in (
            "channel_name",
            "message_id",
            "route_mode",
            "public_usable",
            "visibility",
        )
        if column in raw_columns
    )
    raw_selected = (
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
        *optional_raw,
    )
    def text_flags(value: Any) -> tuple[bool, bool]:
        text = str(value or "")
        correction_like = bool(_CONVERSATION_CORRECTION_RE.search(text))
        candidate = public_assessment_semantics(text)
        opposite_point = bool(
            _public_assessment_text(text)
            and candidate.point_identity == semantics.point_identity
            and candidate.polarity
            and candidate.polarity != semantics.polarity
        )
        return correction_like, opposite_point

    malformed_raw = conn.execute(
        "SELECT content FROM main.conversations "
        "WHERE guild_id=? AND user_id=? AND role='user' "
        "AND julianday(timestamp) IS NULL ORDER BY id LIMIT ?",
        (guild_id, user_id, _CONVERSATION_MOTIF_MAX_SCAN + 1),
    ).fetchall()
    malformed_ledger = conn.execute(
        "SELECT normalized_value FROM main.memory_ledger_entries "
        "WHERE guild_id=? AND subject_key=? "
        "AND source_table='conversations' AND source_role='user' "
        "AND julianday(observed_at) IS NULL ORDER BY entry_id LIMIT ?",
        (guild_id, subject_key, _CONVERSATION_MOTIF_MAX_SCAN + 1),
    ).fetchall()
    if (
        len(malformed_raw) > _CONVERSATION_MOTIF_MAX_SCAN
        or len(malformed_ledger) > _CONVERSATION_MOTIF_MAX_SCAN
        or any(any(text_flags(row[0])) for row in (*malformed_raw, *malformed_ledger))
    ):
        return None
    raw_rows = conn.execute(
        "SELECT %s FROM main.conversations "
        "WHERE guild_id=? AND user_id=? AND role='user' "
        "AND CAST(id AS TEXT)<>? "
        "AND julianday(timestamp)>=julianday(?) "
        "ORDER BY julianday(timestamp),timestamp,id LIMIT ?"
        % ",".join(raw_selected),
        (
            guild_id,
            user_id,
            source_row_id,
            observed_at,
            _CONVERSATION_MOTIF_MAX_SCAN + 1,
        ),
    ).fetchall()
    ledger_rows = conn.execute(
        "SELECT %s FROM main.memory_ledger_entries "
        "WHERE guild_id=? AND subject_key=? AND source_table='conversations' "
        "AND source_role='user' AND entry_id<>? "
        "AND julianday(observed_at)>=julianday(?) "
        "ORDER BY julianday(observed_at),observed_at,entry_id LIMIT ?"
        % ",".join(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS),
        (
            guild_id,
            subject_key,
            entry_id,
            observed_at,
            _CONVERSATION_MOTIF_MAX_SCAN + 1,
        ),
    ).fetchall()
    if (
        len(raw_rows) > _CONVERSATION_MOTIF_MAX_SCAN
        or len(ledger_rows) > _CONVERSATION_MOTIF_MAX_SCAN
    ):
        return None
    raw_by_id: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        row_id = _public_assessment_int(row[0])
        if row_id is None or str(row_id) in raw_by_id:
            return None
        raw_by_id[str(row_id)] = dict(zip(raw_selected, row))
    ledger_by_row: dict[str, list[dict[str, Any]]] = {}
    for row in ledger_rows:
        candidate = dict(zip(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS, row))
        ledger_by_row.setdefault(
            str(candidate.get("source_row_id") or ""),
            [],
        ).append(candidate)
    # A later Ledger claim without its retained source is authority drift. It
    # cannot be allowed to disappear merely because its indexed text changed.
    if any(row_id not in raw_by_id for row_id in ledger_by_row):
        return None

    relevant: list[
        tuple[str, dict[str, Any], bool, bool, dict[str, Any]]
    ] = []
    for row_id, raw in raw_by_id.items():
        correction_like, opposite_point = text_flags(raw.get("content"))
        if not correction_like and not opposite_point:
            continue
        twins = ledger_by_row.get(row_id, [])
        if len(twins) != 1:
            return None
        relevant.append((row_id, raw, correction_like, opposite_point, twins[0]))

    participant_limit = (_CONVERSATION_MOTIF_MAX_SCAN * 4) + 1
    participant_rows = conn.execute(
        """
        SELECT p.entry_id,p.guild_id,p.participant_key,p.display_name,
               p.participant_role,p.order_index
        FROM main.memory_ledger_participants p
        JOIN main.memory_ledger_entries e ON e.entry_id=p.entry_id
        WHERE e.guild_id=? AND e.subject_key=?
          AND e.source_table='conversations' AND e.source_role='user'
          AND e.entry_id<>? AND julianday(e.observed_at)>=julianday(?)
        ORDER BY p.entry_id,p.guild_id,p.participant_role,
                 p.participant_key,p.order_index
        LIMIT ?
        """,
        (guild_id, subject_key, entry_id, observed_at, participant_limit),
    ).fetchall()
    if len(participant_rows) >= participant_limit:
        return None
    participants_by_entry: dict[str, list[tuple[Any, ...]]] = {}
    for row in participant_rows:
        participants_by_entry.setdefault(str(row[0] or ""), []).append(tuple(row[1:]))

    lineage_limit = (_CONVERSATION_MOTIF_MAX_SCAN * 4) + 1
    scoped_lineage_rows = conn.execute(
        """
        SELECT l.entry_id,l.guild_id,l.lineage_type,l.target_entry_id,l.created_at
        FROM main.memory_ledger_lineage l
        JOIN main.memory_ledger_entries e ON e.entry_id=l.entry_id
        WHERE e.guild_id=? AND e.subject_key=?
          AND e.source_table='conversations' AND e.source_role='user'
          AND e.entry_id<>? AND julianday(e.observed_at)>=julianday(?)
          AND l.lineage_type IN ('correction_of','supersedes','retracts')
        ORDER BY l.entry_id,l.guild_id,l.lineage_type,l.target_entry_id,l.created_at
        LIMIT ?
        """,
        (guild_id, subject_key, entry_id, observed_at, lineage_limit),
    ).fetchall()
    if len(scoped_lineage_rows) >= lineage_limit:
        return None
    lineage_by_entry: dict[str, list[tuple[Any, ...]]] = {}
    for row in scoped_lineage_rows:
        lineage_by_entry.setdefault(str(row[0] or ""), []).append(tuple(row[1:]))

    target_rows = conn.execute(
        "SELECT DISTINCT %s FROM main.memory_ledger_entries target "
        "WHERE target.entry_id IN ("
        "SELECT l.target_entry_id FROM main.memory_ledger_lineage l "
        "JOIN main.memory_ledger_entries source ON source.entry_id=l.entry_id "
        "WHERE source.guild_id=? AND source.subject_key=? "
        "AND source.source_table='conversations' AND source.source_role='user' "
        "AND source.entry_id<>? AND julianday(source.observed_at)>=julianday(?) "
        "AND l.lineage_type IN ('correction_of','supersedes','retracts')) "
        "ORDER BY target.entry_id LIMIT ?"
        % ",".join("target.%s" % column for column in _PUBLIC_ASSESSMENT_LEDGER_COLUMNS),
        (guild_id, subject_key, entry_id, observed_at, _CONVERSATION_MOTIF_MAX_SCAN + 1),
    ).fetchall()
    if len(target_rows) > _CONVERSATION_MOTIF_MAX_SCAN:
        return None
    targets = {
        str(row[0] or ""): dict(zip(_PUBLIC_ASSESSMENT_LEDGER_COLUMNS, row))
        for row in target_rows
        if str(row[0] or "")
    }
    target_raw_rows = conn.execute(
        "SELECT DISTINCT target.entry_id,%s "
        "FROM main.memory_ledger_entries target "
        "JOIN main.conversations c ON CAST(c.id AS TEXT)=target.source_row_id "
        "WHERE target.entry_id IN ("
        "SELECT l.target_entry_id FROM main.memory_ledger_lineage l "
        "JOIN main.memory_ledger_entries source ON source.entry_id=l.entry_id "
        "WHERE source.guild_id=? AND source.subject_key=? "
        "AND source.source_table='conversations' AND source.source_role='user' "
        "AND source.entry_id<>? AND julianday(source.observed_at)>=julianday(?) "
        "AND l.lineage_type IN ('correction_of','supersedes','retracts')) "
        "ORDER BY target.entry_id LIMIT ?"
        % ",".join("c.%s" % column for column in raw_selected),
        (guild_id, subject_key, entry_id, observed_at, _CONVERSATION_MOTIF_MAX_SCAN + 1),
    ).fetchall()
    if len(target_raw_rows) > _CONVERSATION_MOTIF_MAX_SCAN:
        return None
    target_raw = {
        str(row[0] or ""): dict(zip(raw_selected, row[1:]))
        for row in target_raw_rows
        if str(row[0] or "")
    }
    journal_columns = frozenset(
        _main_table_columns(conn, "bnl_journal_source_events")
    )
    required_journal = {
        "event_seq",
        "guild_id",
        "source_kind",
        "source_key",
        "occurred_at_ms",
        "channel_id",
        "channel_policy",
        "subject_ref",
        "private_display_name",
        "raw_text",
        "content_hash",
        "public_usable",
        "metadata_json",
    }
    journal_trigger_snapshot = (
        _main_public_assessment_journal_trigger_snapshot(conn)
        if required_journal.issubset(journal_columns)
        else ()
    )
    journal_receipts = _main_public_assessment_journal_receipt_map(
        conn,
        (
            *(raw for _row_id, raw, _correction, _opposite, _twin in relevant),
            *target_raw.values(),
        ),
        journal_trigger_snapshot=journal_trigger_snapshot,
    )

    def bound_source_snapshot(
        candidate: Mapping[str, Any],
        raw: Mapping[str, Any],
        *,
        allowed_routes: frozenset[str],
    ) -> tuple[Any, ...] | None:
        candidate_guild = _public_assessment_int(candidate.get("guild_id"))
        candidate_channel = _public_assessment_int(candidate.get("channel_id"))
        raw_id = _public_assessment_int(raw.get("id"))
        raw_guild = _public_assessment_int(raw.get("guild_id"))
        raw_user = _public_assessment_int(raw.get("user_id"))
        raw_channel = _public_assessment_int(raw.get("channel_id"))
        raw_message = _public_assessment_int(raw.get("message_id"))
        source_sequence = _public_assessment_int(candidate.get("source_sequence"))
        expected_entry_id = stable_entry_id(
            guild_id=candidate_guild,
            source_table=str(candidate.get("source_table") or ""),
            source_row_id=str(candidate.get("source_row_id") or ""),
            source_revision=str(candidate.get("source_revision") or ""),
            entry_type=str(candidate.get("entry_type") or ""),
            subject_key=str(candidate.get("subject_key") or ""),
            predicate_key=str(candidate.get("predicate_key") or ""),
        )
        expected_sequences = {
            value for value in (raw_id, raw_message) if value not in {None, 0}
        }
        public_state = _public_assessment_bool_state(candidate.get("public_usable"))
        derived_state = _public_assessment_bool_state(candidate.get("derived"))
        projection_state = _public_assessment_bool_state(candidate.get("projection"))
        mapped_visibility = _visibility(str(raw.get("channel_policy") or "")).value
        if None in {
            candidate_guild,
            candidate_channel,
            raw_id,
            raw_guild,
            raw_user,
            raw_channel,
            source_sequence,
        } or (
            str(candidate.get("schema_version") or "") != MEMORY_LEDGER_SCHEMA_VERSION
            or str(candidate.get("entry_id") or "") != expected_entry_id
            or candidate_guild != guild_id
            or raw_guild != guild_id
            or raw_user != user_id
            or candidate_channel != raw_channel
            or str(candidate.get("subject_key") or "") != subject_key
            or str(candidate.get("entry_type") or "") != "observation"
            or str(candidate.get("predicate_key") or "") != "conversation"
            or str(candidate.get("source_table") or "") != "conversations"
            or str(candidate.get("source_row_id") or "") != str(raw_id)
            or str(candidate.get("source_revision") or "") != str(raw_id)
            or str(candidate.get("source_role") or "").lower() != "user"
            or str(candidate.get("source_class") or "")
            != SourceClass.PUBLIC_OBSERVATION.value
            or str(candidate.get("route_mode") or "") not in allowed_routes
            or str(candidate.get("channel_policy") or "")
            not in _CONVERSATION_MOTIF_PUBLIC_POLICIES
            or str(candidate.get("channel_policy") or "")
            != str(raw.get("channel_policy") or "")
            or str(candidate.get("visibility") or "")
            not in {Visibility.PUBLIC.value, Visibility.PUBLIC_SAFE.value}
            or str(candidate.get("visibility") or "") != mapped_visibility
            or str(candidate.get("confidence") or "") != Confidence.MEDIUM.value
            or public_state is not True
            or derived_state is not False
            or projection_state is not False
            or str(candidate.get("lifecycle_status") or "") != ACTIVE_LIFECYCLE
            or str(raw.get("role") or "").lower() != "user"
            or str(candidate.get("normalized_value") or "")
            != str(raw.get("content") or "")[:500]
            or str(candidate.get("subject_display_name") or "")
            != str(raw.get("user_name") or "")
            or str(candidate.get("observed_at") or "")
            != str(raw.get("timestamp") or "")
            or _parse_knowledge_time(candidate.get("observed_at")) is None
            or source_sequence not in expected_sequences
            or (
                "channel_name" in raw
                and str(candidate.get("channel_name") or "")
                != str(raw.get("channel_name") or "")
            )
            or (
                "message_id" in raw
                and _public_assessment_int(candidate.get("source_message_id"))
                != raw_message
            )
            or (
                "public_usable" in raw
                and _public_assessment_bool_state(raw.get("public_usable")) is not True
            )
            or (
                "visibility" in raw
                and str(raw.get("visibility") or "") != mapped_visibility
            )
            or _knowledge_operational_or_test_source(dict(candidate))
        ):
            return None
        route_authority = _main_public_assessment_route_authority(
            conn,
            raw,
            journal_columns=journal_columns,
            journal_trigger_snapshot=journal_trigger_snapshot,
            journal_receipts=journal_receipts,
        )
        if (
            route_authority is None
            or route_authority[0] != str(candidate.get("route_mode") or "")
            or route_authority[0] not in allowed_routes
        ):
            return None
        return (
            tuple(candidate.get(column) for column in _PUBLIC_ASSESSMENT_LEDGER_COLUMNS),
            tuple(raw.get(column) for column in raw_selected),
            route_authority[1],
        )

    snapshots: list[tuple[Any, ...]] = []
    unresolved_correction = False
    polarity_conflict = False
    for row_id, raw, correction_like, opposite_point, twin in relevant:
        source_snapshot = bound_source_snapshot(
            twin,
            raw,
            allowed_routes=frozenset({"normal_chat", "conversation_continuity"}),
        )
        if source_snapshot is None:
            return None
        twin_id = str(twin.get("entry_id") or "")
        participant_snapshot = tuple(participants_by_entry.get(twin_id, ()))
        if len(participant_snapshot) != 1 or (
            _public_assessment_int(participant_snapshot[0][0]) != guild_id
            or str(participant_snapshot[0][1] or "") != subject_key
            or str(participant_snapshot[0][2] or "")
            != str(raw.get("user_name") or "")
            or str(participant_snapshot[0][3] or "").lower() != "author"
            or _public_assessment_int(participant_snapshot[0][4]) != 0
        ):
            return None
        lineage_rows: tuple[tuple[Any, ...], ...] = ()
        target_snapshot: tuple[Any, ...] = ()
        if correction_like:
            lineage_rows = tuple(lineage_by_entry.get(twin_id, ()))
            target_ids = {
                str(edge[2] or "")
                for edge in lineage_rows
                if str(edge[2] or "")
            }
            valid_target = False
            if (
                lineage_rows
                and len(target_ids) == 1
                and all(_public_assessment_int(edge[0]) == guild_id for edge in lineage_rows)
            ):
                target_id = next(iter(target_ids))
                target = targets.get(target_id, {})
                target_observed = _parse_knowledge_time(target.get("observed_at"))
                target_raw_row = target_raw.get(target_id, {})
                target_source_snapshot = bound_source_snapshot(
                    target,
                    target_raw_row,
                    allowed_routes=frozenset(
                        {"normal_chat", "conversation_continuity"}
                    ),
                )
                twin_observed = _parse_knowledge_time(twin.get("observed_at"))
                valid_target = bool(
                    target_source_snapshot is not None
                    and target_id != twin_id
                    and target_observed is not None
                    and twin_observed is not None
                    and target_observed < twin_observed
                )
                if valid_target:
                    target_snapshot = tuple(target_source_snapshot or ())
            if not valid_target:
                unresolved_correction = True
        if opposite_point:
            polarity_conflict = True
        snapshots.append(
            (
                source_snapshot,
                participant_snapshot,
                lineage_rows,
                target_snapshot,
                correction_like,
                opposite_point,
            )
        )
    return tuple(snapshots), unresolved_correction, polarity_conflict


def read_public_assessment_root_state(
    conn: sqlite3.Connection,
    *,
    entry_id: str,
    guild_id: int,
    subject_key: str,
    _living_formation_receipt: _LivingCanonFormationReceipt | None = None,
) -> PublicAssessmentRootState | None:
    """Return one content-bound Open Signal root or fail closed.

    The Ledger row is a shadow index, never sufficient authority by itself.
    This read binds it to the original retained conversation, exact author,
    current lineage/correction state, route, recurrence window, and deterministic
    claim semantics.  Packet assembly and send-time revalidation use the same
    state and digest.
    """

    living_validation = bool(
        _living_formation_receipt is not None
        and _living_formation_receipt.authority
        is _LIVING_CANON_FORMATION_AUTHORITY
    )
    required_ledger_tables = (
        "memory_ledger_entries",
        "memory_ledger_lineage",
        "memory_ledger_participants",
    )
    if any(not _main_table_columns(conn, table) for table in required_ledger_tables):
        return None
    entry = _main_public_assessment_entry(conn, str(entry_id or ""))
    expected_subject = str(subject_key or "")
    entry_guild_id = _public_assessment_int(entry.get("guild_id")) if entry else None
    entry_channel_id = _public_assessment_int(entry.get("channel_id")) if entry else None
    public_usable_state = (
        _public_assessment_bool_state(entry.get("public_usable"))
        if entry
        else None
    )
    derived_state = (
        _public_assessment_bool_state(entry.get("derived")) if entry else None
    )
    projection_state = (
        _public_assessment_bool_state(entry.get("projection")) if entry else None
    )
    expected_entry_id = (
        stable_entry_id(
            guild_id=entry_guild_id,
            source_table=str(entry.get("source_table") or ""),
            source_row_id=str(entry.get("source_row_id") or ""),
            source_revision=str(entry.get("source_revision") or ""),
            entry_type=str(entry.get("entry_type") or ""),
            subject_key=str(entry.get("subject_key") or ""),
            predicate_key=str(entry.get("predicate_key") or ""),
        )
        if entry
        else ""
    )
    if not entry or (
        entry_guild_id != int(guild_id or 0)
        or entry_channel_id is None
        or str(entry.get("schema_version") or "")
        != MEMORY_LEDGER_SCHEMA_VERSION
        or str(entry.get("entry_id") or "") != expected_entry_id
        or str(entry.get("subject_key") or "") != expected_subject
        or str(entry.get("entry_type") or "") != "observation"
        or str(entry.get("predicate_key") or "") != "conversation"
        or str(entry.get("source_table") or "") != "conversations"
        or str(entry.get("source_revision") or "")
        != str(entry.get("source_row_id") or "")
        or str(entry.get("source_role") or "") != "user"
        or str(entry.get("source_class") or "")
        != SourceClass.PUBLIC_OBSERVATION.value
        or str(entry.get("route_mode") or "")
        not in _PUBLIC_ASSESSMENT_ALLOWED_ROUTES
        or str(entry.get("channel_policy") or "")
        not in {"public_home", "public_context", "public_selective"}
        or str(entry.get("visibility") or "")
        not in {Visibility.PUBLIC.value, Visibility.PUBLIC_SAFE.value}
        or str(entry.get("confidence") or "") != Confidence.MEDIUM.value
        or public_usable_state is not True
        or derived_state is not False
        or projection_state is not False
        or str(entry.get("lifecycle_status") or "") != ACTIVE_LIFECYCLE
    ):
        return None

    participant_columns = _main_table_columns(
        conn,
        "memory_ledger_participants",
    )
    if not {
        "entry_id",
        "guild_id",
        "participant_key",
        "display_name",
        "participant_role",
        "order_index",
    }.issubset(participant_columns):
        return None
    participant_rows = tuple(
        tuple(row)
        for row in conn.execute(
            """
            SELECT guild_id,participant_key,display_name,
                   participant_role,order_index
            FROM main.memory_ledger_participants
            WHERE entry_id=?
            ORDER BY guild_id,participant_role,participant_key,order_index
            """,
            (str(entry_id),),
        ).fetchall()
    )
    participant_guilds = tuple(
        _public_assessment_int(row[0]) for row in participant_rows
    )
    if (
        len(participant_rows) != 1
        or any(value != int(guild_id or 0) for value in participant_guilds)
    ):
        return None
    author_rows = tuple(
        row
        for row in participant_rows
        if str(row[3] or "").lower() == "author"
    )
    if (
        len(author_rows) != 1
        or str(author_rows[0][1] or "") != expected_subject
        or str(author_rows[0][2] or "")
        != str(entry.get("subject_display_name") or "")
        or _public_assessment_int(author_rows[0][4]) != 0
    ):
        return None

    lineage_columns = _main_table_columns(conn, "memory_ledger_lineage")
    if not {
        "entry_id",
        "guild_id",
        "lineage_type",
        "target_entry_id",
        "created_at",
    }.issubset(lineage_columns):
        return None
    outgoing = tuple(
        tuple(str(value or "") for value in row)
        for row in conn.execute(
            """
            SELECT guild_id,lineage_type,target_entry_id,created_at
            FROM main.memory_ledger_lineage
            WHERE entry_id=?
            ORDER BY guild_id,lineage_type,target_entry_id,created_at
            """,
            (str(entry_id),),
        ).fetchall()
    )
    incoming = tuple(
        tuple(str(value or "") for value in row)
        for row in conn.execute(
            """
            SELECT guild_id,entry_id,lineage_type,created_at
            FROM main.memory_ledger_lineage
            WHERE target_entry_id=?
            ORDER BY guild_id,entry_id,lineage_type,created_at
            """,
            (str(entry_id),),
        ).fetchall()
    )
    lineage_guilds = tuple(
        _public_assessment_int(row[0]) for row in (*outgoing, *incoming)
    )
    if any(value != int(guild_id or 0) for value in lineage_guilds):
        return None
    if any(
        str(row[2] or "") in {"correction_of", "supersedes", "retracts"}
        for row in incoming
    ):
        return None

    raw_columns = _main_table_columns(conn, "conversations")
    required_raw = {
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
    }
    if not required_raw.issubset(raw_columns):
        return None
    optional_raw = tuple(
        column
        for column in (
            "channel_name",
            "message_id",
            "route_mode",
            "public_usable",
            "visibility",
        )
        if column in raw_columns
    )
    raw_selected = (
        "id",
        "guild_id",
        "user_id",
        "user_name",
        "role",
        "content",
        "channel_id",
        "channel_policy",
        "timestamp",
        *optional_raw,
    )
    raw_row = conn.execute(
        "SELECT %s FROM main.conversations WHERE id=?"
        % ",".join(raw_selected),
        (str(entry.get("source_row_id") or ""),),
    ).fetchone()
    if not raw_row:
        return None
    raw = dict(zip(raw_selected, raw_row))
    raw_id = _public_assessment_int(raw.get("id"))
    raw_guild_id = _public_assessment_int(raw.get("guild_id"))
    raw_user_id = _public_assessment_int(raw.get("user_id"))
    raw_channel_id = _public_assessment_int(raw.get("channel_id"))
    source_sequence = _public_assessment_int(entry.get("source_sequence"))
    raw_message_id = _public_assessment_int(raw.get("message_id"))
    expected_sequences = {
        int(raw_id or 0),
        int(raw_message_id or 0),
    } - {0}
    mapped_visibility = _visibility(str(raw.get("channel_policy") or "")).value
    if None in {raw_id, raw_guild_id, raw_user_id, raw_channel_id}:
        return None
    raw_subject = subject_key_for_user(raw_user_id)
    if (
        str(raw_id) != str(entry.get("source_row_id") or "")
        or raw_guild_id != int(guild_id or 0)
        or raw_subject != expected_subject
        or str(raw.get("user_name") or "")
        != str(entry.get("subject_display_name") or "")
        or str(raw.get("role") or "").lower() != "user"
        or str(raw.get("content") or "")[:500]
        != str(entry.get("normalized_value") or "")
        or raw_channel_id != entry_channel_id
        or str(raw.get("channel_policy") or "")
        != str(entry.get("channel_policy") or "")
        or str(raw.get("timestamp") or "")
        != str(entry.get("observed_at") or "")
        or source_sequence not in expected_sequences
        or str(entry.get("visibility") or "") != mapped_visibility
    ):
        return None
    if "channel_name" in raw and str(raw.get("channel_name") or "") != str(
        entry.get("channel_name") or ""
    ):
        return None
    if "message_id" in raw and str(raw_message_id or "") != str(
        _public_assessment_int(entry.get("source_message_id")) or ""
    ):
        return None
    route_authority = _main_public_assessment_route_authority(conn, raw)
    if (
        route_authority is None
        or route_authority[0] != str(entry.get("route_mode") or "")
        or route_authority[0] not in _PUBLIC_ASSESSMENT_ALLOWED_ROUTES
    ):
        return None
    route_authority_snapshot = route_authority[1]
    if "public_usable" in raw and _public_assessment_bool_state(
        raw.get("public_usable")
    ) is not True:
        return None
    if "visibility" in raw and str(raw.get("visibility") or "") not in {
        Visibility.PUBLIC.value,
        Visibility.PUBLIC_SAFE.value,
    }:
        return None
    if "visibility" in raw and str(raw.get("visibility") or "") != mapped_visibility:
        return None
    if _knowledge_operational_or_test_source(dict(entry)):
        return None

    safe_text = _public_assessment_text(str(entry.get("normalized_value") or ""))
    semantics = public_assessment_semantics(
        str(entry.get("normalized_value") or "")
    )
    root_identity = _main_public_assessment_root_identity(conn, entry)
    occurrence_identity = _main_public_assessment_occurrence_identity(
        conn,
        entry,
        raw_exchange_only=living_validation,
    )
    if (
        not safe_text
        or semantics.attribution_mode
        not in {"subject_action", "authored_topic"}
        or not semantics.action_identity
        or not semantics.point_identity
        or not root_identity
        or not occurrence_identity
    ):
        return None

    if living_validation:
        # Living formation applies correction fences only after the meaning
        # predicate has been determined.  Global subject fences here would
        # incorrectly erase unrelated topics.
        fences: tuple[tuple[str, ...], ...] = ()
        later_guards: tuple[Any, ...] = ()
    else:
        fences = _main_public_assessment_fences(
            conn,
            guild_id=int(guild_id or 0),
            subject_key=expected_subject,
        )
        if fences is None:
            return None
        later_guard_state = _main_public_assessment_later_guard_state(
            conn,
            entry=entry,
            semantics=semantics,
        )
        if later_guard_state is None:
            return None
        later_guards, unresolved_later_correction, polarity_conflict = (
            later_guard_state
        )
        if unresolved_later_correction or polarity_conflict:
            return None
    observed = _parse_knowledge_time(entry.get("observed_at"))
    if observed is None:
        return None
    for fence in fences:
        cutoff = _parse_knowledge_time(fence[2] if len(fence) > 2 else "")
        if cutoff is None or observed <= cutoff:
            return None

    source_digest = _public_assessment_state_digest(
        _PUBLIC_ASSESSMENT_ROOT_STATE_VERSION,
        tuple(entry.get(column) for column in _PUBLIC_ASSESSMENT_LEDGER_COLUMNS),
        tuple(raw.get(column) for column in raw_selected),
        route_authority_snapshot,
        participant_rows,
        outgoing,
        incoming,
        fences,
        later_guards,
        root_identity,
        occurrence_identity,
        semantics.attribution_mode,
        semantics.polarity,
        semantics.action_identity,
        semantics.material_facets,
        semantics.point_identity,
    )
    return PublicAssessmentRootState(
        entry_id=str(entry.get("entry_id") or ""),
        subject_key=expected_subject,
        text=safe_text,
        observed_at=str(entry.get("observed_at") or ""),
        visibility=str(entry.get("visibility") or ""),
        channel_policy=str(entry.get("channel_policy") or ""),
        route_mode=str(entry.get("route_mode") or ""),
        source_role=str(entry.get("source_role") or ""),
        source_class=str(entry.get("source_class") or ""),
        lifecycle_status=str(entry.get("lifecycle_status") or ""),
        source_row_id=str(entry.get("source_row_id") or ""),
        root_identity=root_identity,
        occurrence_identity=occurrence_identity,
        source_digest=source_digest,
        semantics=semantics,
        public_usable=True,
        derived=False,
        projection=False,
    )


def _living_canon_raw_exchange_component(
    conn: sqlite3.Connection,
    entry: Mapping[str, Any],
) -> tuple[str, tuple[str, ...], tuple[str, int, str]]:
    """Return a bounded exchange keyed by its earliest raw-authoritative root."""

    current_id = str(entry.get("entry_id") or "")
    observed_at = str(entry.get("observed_at") or "")
    observed = _parse_knowledge_time(observed_at)
    scope = (
        int(entry.get("guild_id") or 0),
        int(entry.get("channel_id") or 0),
        str(entry.get("channel_policy") or ""),
        str(entry.get("subject_key") or ""),
    )
    if not current_id or observed is None or not all((scope[0], scope[1], scope[2], scope[3])):
        return "", (), ("", 0, "")
    params = (
        scope[0],
        scope[3],
        scope[1],
        scope[2],
        observed_at,
        _CONVERSATION_OCCURRENCE_MAX_SCAN + 1,
    )
    before = conn.execute(
        """
        SELECT entry_id,observed_at,source_sequence
        FROM main.memory_ledger_entries
        WHERE guild_id=? AND subject_key=? AND source_table='conversations'
          AND source_role='user' AND channel_id=? AND channel_policy=?
          AND observed_at<=?
        ORDER BY observed_at DESC,source_sequence DESC,entry_id DESC LIMIT ?
        """,
        params,
    ).fetchall()
    after = conn.execute(
        """
        SELECT entry_id,observed_at,source_sequence
        FROM main.memory_ledger_entries
        WHERE guild_id=? AND subject_key=? AND source_table='conversations'
          AND source_role='user' AND channel_id=? AND channel_policy=?
          AND observed_at>=?
        ORDER BY observed_at,source_sequence,entry_id LIMIT ?
        """,
        params,
    ).fetchall()
    row_map = {
        str(row[0] or ""): (
            str(row[0] or ""),
            str(row[1] or ""),
            int(row[2] or 0),
        )
        for row in (*before, *after)
        if str(row[0] or "")
    }
    ordered = sorted(
        row_map.values(),
        key=lambda row: (
            _parse_knowledge_time(row[1])
            or datetime.min.replace(tzinfo=timezone.utc),
            row[2],
            row[0],
        ),
    )
    current_index = next(
        (index for index, row in enumerate(ordered) if row[0] == current_id),
        -1,
    )
    if current_index < 0:
        return "", (), ("", 0, "")
    left = current_index
    while left > 0:
        newer = _parse_knowledge_time(ordered[left][1])
        older = _parse_knowledge_time(ordered[left - 1][1])
        if newer is None or older is None or newer < older:
            return "", (), ("", 0, "")
        if (newer - older).total_seconds() > _CONVERSATION_MOTIF_WINDOW_SECONDS:
            break
        left -= 1
    right = current_index
    while right + 1 < len(ordered):
        older = _parse_knowledge_time(ordered[right][1])
        newer = _parse_knowledge_time(ordered[right + 1][1])
        if newer is None or older is None or newer < older:
            return "", (), ("", 0, "")
        if (newer - older).total_seconds() > _CONVERSATION_MOTIF_WINDOW_SECONDS:
            break
        right += 1
    members = ordered[left : right + 1]
    if (
        len(members) > _CONVERSATION_OCCURRENCE_MAX_SCAN
        or (left == 0 and len(before) > _CONVERSATION_OCCURRENCE_MAX_SCAN)
        or (right == len(ordered) - 1 and len(after) > _CONVERSATION_OCCURRENCE_MAX_SCAN)
    ):
        return "", (), ("", 0, "")
    anchor = members[0] if members else ("", "", 0)
    if not anchor[0]:
        return "", (), ("", 0, "")
    identity = _knowledge_digest("conversation_occurrence", *scope, anchor[0])
    return identity, tuple(row[0] for row in members), (
        anchor[1],
        anchor[2],
        anchor[0],
    )


def _living_canon_validated_moment_members(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    target_id: str,
) -> tuple[str, ...] | None:
    """Validate one finalized Moment and return only this subject's roots."""

    target = _main_public_assessment_entry(conn, str(target_id or ""))
    moment_id = str(target.get("source_row_id") or "") if target else ""
    expected_target_id = (
        stable_entry_id(
            guild_id=int(guild_id or 0),
            source_table="memory_moment_windows",
            source_row_id=moment_id,
            source_revision="1",
            entry_type="shared_moment",
            subject_key="moment:%s" % moment_id,
            predicate_key="shared_moment",
        )
        if moment_id
        else ""
    )
    if not target or (
        str(target_id or "") != expected_target_id
        or int(target.get("guild_id") or 0) != int(guild_id or 0)
        or str(target.get("source_table") or "") != "memory_moment_windows"
        or str(target.get("source_revision") or "") != "1"
        or str(target.get("source_role") or "") != "derived_assessment"
        or str(target.get("entry_type") or "") != "shared_moment"
        or str(target.get("subject_key") or "") != "moment:%s" % moment_id
        or str(target.get("predicate_key") or "") != "shared_moment"
        or str(target.get("source_class") or "")
        != SourceClass.DERIVED_SUMMARY.value
        or str(target.get("lifecycle_status") or "") != REVIEW_ONLY_LIFECYCLE
        or str(target.get("visibility") or "")
        not in {Visibility.PUBLIC.value, Visibility.PUBLIC_SAFE.value}
        or _public_assessment_bool_state(target.get("public_usable")) is not True
        or _public_assessment_bool_state(target.get("derived")) is not True
        or _public_assessment_bool_state(target.get("projection")) is not True
    ):
        return None
    required_tables = {
        "memory_moment_windows",
        "memory_moment_members",
        "memory_moment_participants",
    }
    main_tables = {
        str(row[0] or "")
        for row in conn.execute(
            "SELECT name FROM main.sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if not required_tables.issubset(main_tables):
        return None
    lineage_rows = conn.execute(
        """
        SELECT guild_id,lineage_type,target_entry_id
        FROM main.memory_ledger_lineage
        WHERE entry_id=?
        ORDER BY guild_id,lineage_type,target_entry_id
        LIMIT ?
        """,
        (str(target_id or ""), _CONVERSATION_OCCURRENCE_MAX_SCAN + 1),
    ).fetchall()
    if (
        not lineage_rows
        or len(lineage_rows) > _CONVERSATION_OCCURRENCE_MAX_SCAN
        or any(
            int(row[0] or 0) != int(guild_id or 0)
            or str(row[1] or "") != "derived_from"
            or not str(row[2] or "")
            for row in lineage_rows
        )
    ):
        return None
    derived_targets = {str(row[2] or "") for row in lineage_rows}
    window = conn.execute(
        """
        SELECT guild_id,channel_id,channel_policy,route_mode,lifecycle_status,
               visibility,public_usable,canonical_ledger_entry_id
        FROM main.memory_moment_windows
        WHERE moment_id=?
        """,
        (moment_id,),
    ).fetchone()
    if not window or (
        int(window[0] or 0) != int(guild_id or 0)
        or int(window[1] or 0) != int(target.get("channel_id") or 0)
        or str(window[2] or "") != str(target.get("channel_policy") or "")
        or str(window[3] or "") != str(target.get("route_mode") or "")
        or str(window[4] or "") != "finalized"
        or str(window[5] or "") != str(target.get("visibility") or "")
        or _public_assessment_bool_state(window[6]) is not True
        or str(window[7] or "") != str(target_id or "")
    ):
        return None
    member_rows = conn.execute(
        """
        SELECT ledger_entry_id,membership_role
        FROM main.memory_moment_members
        WHERE moment_id=?
        ORDER BY ledger_entry_id
        LIMIT ?
        """,
        (moment_id, _CONVERSATION_OCCURRENCE_MAX_SCAN + 1),
    ).fetchall()
    member_ids = tuple(str(row[0] or "") for row in member_rows)
    if (
        not member_ids
        or len(member_ids) > _CONVERSATION_OCCURRENCE_MAX_SCAN
        or set(member_ids) != derived_targets
        or any(
            str(row[1] or "") not in {"human_author", "bnl_participant"}
            for row in member_rows
        )
    ):
        return None
    participant_rows = conn.execute(
        """
        SELECT participant_key,participant_role
        FROM main.memory_moment_participants
        WHERE moment_id=?
        ORDER BY participant_key,participant_role
        LIMIT ?
        """,
        (moment_id, _CONVERSATION_OCCURRENCE_MAX_SCAN + 1),
    ).fetchall()
    if (
        not participant_rows
        or len(participant_rows) > _CONVERSATION_OCCURRENCE_MAX_SCAN
        or len(participant_rows) != len(set(participant_rows))
    ):
        return None
    expected_participants: set[tuple[str, str]] = set()
    subject_member_ids: list[str] = []
    for member_id, membership_role in member_rows:
        member_id = str(member_id or "")
        membership_role = str(membership_role or "")
        member = _main_public_assessment_entry(conn, member_id)
        source_role = str(member.get("source_role") or "") if member else ""
        expected_role = (
            "human_author" if source_role == "user" else "bnl_participant"
        )
        participant_key = (
            str(member.get("subject_key") or "")
            if source_role == "user" and member
            else BNL_SUBJECT_KEY
        )
        if not member or (
            int(member.get("guild_id") or 0) != int(guild_id or 0)
            or str(member.get("source_table") or "") != "conversations"
            or str(member.get("entry_type") or "")
            not in {"observation", "derived_summary"}
            or int(member.get("channel_id") or 0) != int(window[1] or 0)
            or str(member.get("channel_policy") or "") != str(window[2] or "")
            or str(member.get("route_mode") or "") != str(window[3] or "")
            or str(member.get("visibility") or "") != str(window[5] or "")
            or str(member.get("lifecycle_status") or "")
            not in {ACTIVE_LIFECYCLE, REVIEW_ONLY_LIFECYCLE}
            or membership_role != expected_role
            or not participant_key
            or (
                source_role == "user"
                and _public_assessment_bool_state(member.get("public_usable"))
                is not True
            )
            or conn.execute(
                """
                SELECT 1 FROM main.memory_ledger_lineage
                WHERE guild_id=? AND target_entry_id=?
                  AND lineage_type IN ('correction_of','supersedes','retracts')
                LIMIT 1
                """,
                (int(guild_id or 0), member_id),
            ).fetchone()
        ):
            return None
        expected_participants.add((participant_key, expected_role))
        if (
            expected_role == "human_author"
            and participant_key == str(subject_key or "")
        ):
            subject_member_ids.append(member_id)
        edges = conn.execute(
            """
            SELECT guild_id,target_entry_id
            FROM main.memory_ledger_lineage
            WHERE entry_id=? AND lineage_type='part_of_moment'
            ORDER BY guild_id,target_entry_id
            LIMIT ?
            """,
            (member_id, _CONVERSATION_OCCURRENCE_MAX_SCAN + 1),
        ).fetchall()
        if (
            len(edges) > _CONVERSATION_OCCURRENCE_MAX_SCAN
            or (int(guild_id or 0), str(target_id or "")) not in edges
            or any(
                int(edge[0] or 0) != int(guild_id or 0)
                or not str(edge[1] or "")
                for edge in edges
            )
        ):
            return None
    if (
        set(
            (str(row[0] or ""), str(row[1] or ""))
            for row in participant_rows
        )
        != expected_participants
        or (str(subject_key or ""), "human_author")
        not in expected_participants
        or not subject_member_ids
    ):
        return None
    return tuple(sorted(subject_member_ids))


def _living_canon_root_states_and_occurrences(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    entry_ids: tuple[str, ...],
) -> tuple[
    dict[str, PublicAssessmentRootState],
    dict[str, str],
    tuple[str, ...],
]:
    """Bind roots to raw state, then collapse validated Moment representations."""

    if not _living_canon_main_authority_unshadowed(conn):
        return {}, {}, ("living_canon_authority_shadowed",)
    requested = tuple(sorted({str(value or "") for value in entry_ids if str(value or "")}))
    if not requested or len(requested) > _CONVERSATION_MOTIF_MAX_ROOTS:
        return {}, {}, ("source_ineligible",)
    states: dict[str, PublicAssessmentRootState] = {}
    entries: dict[str, dict[str, Any]] = {}
    exchanges: dict[str, str] = {}
    anchors: dict[str, tuple[str, int, str]] = {}

    def load_root(entry_id: str) -> bool:
        if entry_id in states:
            return True
        state = read_public_assessment_root_state(
            conn,
            entry_id=entry_id,
            guild_id=int(guild_id or 0),
            subject_key=str(subject_key or ""),
            _living_formation_receipt=_living_canon_formation_receipt(),
        )
        entry = _main_public_assessment_entry(conn, entry_id)
        if state is None or not entry:
            return False
        exchange, _members, anchor = _living_canon_raw_exchange_component(
            conn,
            entry,
        )
        if not exchange or not anchor[2]:
            return False
        states[entry_id] = state
        entries[entry_id] = entry
        exchanges[entry_id] = exchange
        anchors[exchange] = min(anchors.get(exchange, anchor), anchor)
        return True

    for entry_id in requested:
        if not load_root(entry_id):
            return states, {}, ("source_ineligible",)

    parent: dict[str, str] = {exchange: exchange for exchange in set(exchanges.values())}

    def find(value: str) -> str:
        parent.setdefault(value, value)
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root == right_root:
            return
        left_anchor = anchors.get(left_root, ("", 0, left_root))
        right_anchor = anchors.get(right_root, ("", 0, right_root))
        winner, loser = (
            (left_root, right_root)
            if left_anchor <= right_anchor
            else (right_root, left_root)
        )
        parent[loser] = winner
        anchors[winner] = min(left_anchor, right_anchor)

    reasons: set[str] = set()
    pending = list(requested)
    seen_roots: set[str] = set()
    seen_moments: set[str] = set()
    while pending:
        root_id = pending.pop()
        if root_id in seen_roots:
            continue
        seen_roots.add(root_id)
        if len(seen_roots) > _CONVERSATION_OCCURRENCE_MAX_SCAN:
            return states, {}, ("unbounded_occurrence_withheld",)
        links = conn.execute(
            """
            SELECT guild_id,target_entry_id
            FROM main.memory_ledger_lineage
            WHERE entry_id=? AND lineage_type='part_of_moment'
            ORDER BY guild_id,target_entry_id
            LIMIT ?
            """,
            (root_id, _CONVERSATION_OCCURRENCE_MAX_SCAN + 1),
        ).fetchall()
        if len(links) > _CONVERSATION_OCCURRENCE_MAX_SCAN:
            return states, {}, ("unbounded_occurrence_withheld",)
        for edge_guild, target_id in links:
            target_id = str(target_id or "")
            if int(edge_guild or 0) != int(guild_id or 0) or not target_id:
                return states, {}, ("moment_lifecycle_or_membership_ineligible",)
            if target_id in seen_moments:
                continue
            member_ids = _living_canon_validated_moment_members(
                conn,
                guild_id=int(guild_id or 0),
                subject_key=str(subject_key or ""),
                target_id=target_id,
            )
            if not member_ids or root_id not in member_ids:
                return states, {}, ("moment_lifecycle_or_membership_ineligible",)
            if any(not load_root(member_id) for member_id in member_ids):
                return states, {}, ("moment_lifecycle_or_membership_ineligible",)
            seen_moments.add(target_id)
            first_exchange = exchanges[member_ids[0]]
            for member_id in member_ids:
                union(first_exchange, exchanges[member_id])
                pending.append(member_id)
            reasons.add("same_root_projection_collapsed")
            if len(member_ids) > 1 or len(seen_moments) > 1:
                reasons.add("overlapping_occurrence_representation_collapsed")

    component_identity: dict[str, str] = {}
    for exchange in tuple(parent):
        root = find(exchange)
        component_identity[exchange] = root
    occurrences = {
        entry_id: component_identity.get(exchange, exchange)
        for entry_id, exchange in exchanges.items()
        if entry_id in requested
    }
    return states, occurrences, tuple(sorted(reasons))


def _public_assessment_scope_text(value: str) -> str:
    """Remove only a leading bot mention/vocative from request scope."""

    normalized = re.sub(r"^\s*<@!?\d+>\s*[,;:!?—–-]*\s*", "", str(value or ""))
    return _PUBLIC_ASSESSMENT_LEADING_ADDRESSEE_RE.sub(
        "",
        normalized,
        count=1,
    ).strip()


def public_assessment_process_request(value: str) -> bool:
    """Return whether assessment selection must require process relevance."""

    return bool(
        _PUBLIC_ASSESSMENT_PROCESS_QUERY_RE.search(
            _public_assessment_scope_text(value)
        )
    )


def public_assessment_relevance_required(value: str) -> bool:
    """Return whether a profile request names a specific question scope."""

    scope_text = _public_assessment_scope_text(value)
    if public_assessment_process_request(scope_text):
        return True
    request_stems = {
        _public_assessment_term_stem(term)
        for term in _public_assessment_terms(scope_text)
    }
    generic_stems = {
        _public_assessment_term_stem(term)
        for term in _PUBLIC_ASSESSMENT_GENERIC_PROFILE_TERMS
    }
    return bool(request_stems - generic_stems)


def _select_public_conversation_assessment_evidence_in_snapshot(
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
        require_legacy_occurrence=False,
        normal_chat_only=False,
    )
    scanned_count = int(diagnostics.get("ledger_rows_scanned", 0) or 0)
    if not history:
        return PublicAssessmentSelection(scanned_count=scanned_count)

    scope_text = _public_assessment_scope_text(request_text)
    request_terms = _public_assessment_terms(scope_text)
    process_request = public_assessment_process_request(scope_text)
    relevance_required = public_assessment_relevance_required(scope_text)
    target_terms = set(request_terms)
    if process_request:
        target_terms.update(_PUBLIC_ASSESSMENT_PROCESS_TERMS)
    process_stems = {
        _public_assessment_term_stem(term)
        for term in _PUBLIC_ASSESSMENT_PROCESS_TERMS
    }
    request_non_process_stems = {
        _public_assessment_term_stem(term)
        for term in request_terms
    } - process_stems
    request_topic_facets = {
        "topic:%s" % canonical
        for term in request_terms
        for canonical in (
            _PUBLIC_ASSESSMENT_TOPIC_ALIASES.get(
                _public_assessment_term_stem(term)
            ),
        )
        if canonical
    }
    if "barcode" in request_terms:
        request_topic_facets.update(
            "topic:%s" % topic
            for topic in set(_PUBLIC_ASSESSMENT_TOPIC_ALIASES.values())
        )

    def request_relevant(
        terms: frozenset[str],
        semantics: PublicAssessmentSemantics,
    ) -> bool:
        candidate_stems = {
            _public_assessment_term_stem(term) for term in terms
        }
        process_overlap = candidate_stems.intersection(process_stems)
        direct_non_process_overlap = candidate_stems.intersection(
            request_non_process_stems
        )
        candidate_topic_facets = {
            facet
            for facet in semantics.material_facets
            if str(facet).startswith("topic:")
        }
        return bool(
            (
                len(process_overlap) >= 2
                or (
                    len(process_overlap) >= 1
                    and bool(direct_non_process_overlap)
                )
            )
            if process_request
            else (
                set(terms).intersection(target_terms)
                or candidate_stems.intersection(request_non_process_stems)
                or candidate_topic_facets.intersection(request_topic_facets)
            )
        )

    safe_max = max(
        1,
        min(
            int(max_results or _PUBLIC_ASSESSMENT_MAX_RESULTS),
            8,
        ),
    )
    # Rank the full bounded Ledger pool using non-authoritative text only, then
    # run the expensive content-bound validator over a fixed backfill budget.
    # Preliminary rows can influence order or cause a safe omission, but they
    # can never enter the returned packet without central root validation.
    provisional: list[dict[str, Any]] = []
    provisional_seen_text: set[str] = set()
    provisional_frequency: Counter[str] = Counter()
    for recency_rank, entry in enumerate(history):
        text = _public_assessment_text(
            str(entry.get("normalized_value") or "")
        )
        semantics = public_assessment_semantics(text)
        normalized = re.sub(r"\W+", " ", text.lower()).strip()
        terms = _public_assessment_terms(text)
        if (
            not text
            or not semantics.point_identity
            or not normalized
            or normalized in provisional_seen_text
            or not terms
        ):
            continue
        provisional_seen_text.add(normalized)
        provisional_frequency.update(terms)
        provisional.append(
            {
                "entry": entry,
                "text": text,
                "terms": terms,
                "semantics": semantics,
                "recency_rank": recency_rank,
            }
        )
    for candidate in provisional:
        terms = set(candidate["terms"])
        direct_overlap = terms.intersection(request_terms)
        target_overlap = terms.intersection(target_terms)
        recurrent_score = sum(
            min(3, max(0, int(provisional_frequency[term]) - 1))
            for term in terms
        )
        candidate["request_relevant"] = request_relevant(
            candidate["terms"],
            candidate["semantics"],
        )
        candidate["base_score"] = (
            10.0 * len(direct_overlap)
            + 5.0 * len(target_overlap - direct_overlap)
            + min(18.0, float(recurrent_score))
            + max(
                0.0,
                2.0
                - (
                    float(candidate["recency_rank"])
                    / max(1.0, float(len(provisional)))
                ),
            )
        )

    validation_budget = min(12, max(8, safe_max * 2))
    validation_order: list[dict[str, Any]] = []
    provisional_points: set[str] = set()
    provisional_terms: set[str] = set()
    while provisional and len(validation_order) < validation_budget:
        ranked: list[tuple[float, int, str, dict[str, Any]]] = []
        for candidate in provisional:
            point_identity = str(
                candidate["semantics"].point_identity or ""
            )
            terms = set(candidate["terms"])
            adjusted = (
                float(candidate["base_score"])
                + (14.0 if point_identity not in provisional_points else 0.0)
                + min(8.0, 0.75 * len(terms - provisional_terms))
                + (
                    24.0
                    if relevance_required and candidate["request_relevant"]
                    else 0.0
                )
            )
            ranked.append(
                (
                    -adjusted,
                    int(candidate["recency_rank"]),
                    str(candidate["entry"].get("entry_id") or ""),
                    candidate,
                )
            )
        chosen = sorted(ranked, key=lambda value: value[:3])[0][3]
        provisional.remove(chosen)
        validation_order.append(chosen)
        provisional_points.add(str(chosen["semantics"].point_identity or ""))
        provisional_terms.update(chosen["terms"])

    occurrence_terms: dict[str, set[str]] = {}
    prepared: list[dict[str, Any]] = []
    seen_text: set[str] = set()
    for provisional_candidate in validation_order:
        entry = provisional_candidate["entry"]
        recency_rank = int(provisional_candidate["recency_rank"])
        state = read_public_assessment_root_state(
            conn,
            entry_id=str(entry.get("entry_id") or ""),
            guild_id=int(guild_id or 0),
            subject_key=str(subject_key or ""),
        )
        if state is None:
            continue
        text = state.text
        occurrence = state.occurrence_identity
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
                "state": state,
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
        candidate["request_relevant"] = request_relevant(
            candidate["terms"],
            candidate["state"].semantics,
        )
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
        min(3, relevant_available) if relevance_required else 0
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
            entry_id=str(candidate["state"].entry_id or ""),
            text=str(candidate["text"]),
            observed_at=str(candidate["state"].observed_at or ""),
            visibility=str(candidate["state"].visibility or "unknown"),
            occurrence_identity=str(candidate["occurrence"]),
            score=float(candidate["base_score"]),
            root_identity=str(candidate["state"].root_identity or ""),
            source_digest=str(candidate["state"].source_digest or ""),
            point_identity=str(
                candidate["state"].semantics.point_identity or ""
            ),
            attribution_mode=str(
                candidate["state"].semantics.attribution_mode or ""
            ),
            polarity=str(candidate["state"].semantics.polarity or ""),
            action_identity=str(
                candidate["state"].semantics.action_identity or ""
            ),
            material_facets=tuple(
                candidate["state"].semantics.material_facets
            ),
            request_relevant=bool(candidate["request_relevant"]),
            subject_key=str(subject_key or ""),
            source_role=str(candidate["state"].source_role or ""),
            source_class=str(candidate["state"].source_class or ""),
            lifecycle_status=str(candidate["state"].lifecycle_status or ""),
            channel_policy=str(candidate["state"].channel_policy or "unknown"),
            route_mode=str(candidate["state"].route_mode or "unknown"),
            public_usable=bool(candidate["state"].public_usable),
            subject_authored=True,
            selector_eligible=True,
            derived=bool(candidate["state"].derived),
            projection=bool(candidate["state"].projection),
        )
        for candidate in selected
        if str(candidate["state"].entry_id or "")
    )
    return PublicAssessmentSelection(
        scanned_count=scanned_count,
        eligible_count=len(prepared),
        request_relevant_count=relevant_available,
        items=items,
    )


def select_public_conversation_assessment_evidence(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    request_text: str,
    max_scan: int = _CONVERSATION_MOTIF_MAX_SCAN,
    max_results: int = _PUBLIC_ASSESSMENT_MAX_RESULTS,
) -> PublicAssessmentSelection:
    """Select Open evidence from one coherent, read-only snapshot."""

    owns_snapshot = not conn.in_transaction
    if owns_snapshot:
        conn.execute("BEGIN")
    try:
        return _select_public_conversation_assessment_evidence_in_snapshot(
            conn,
            guild_id=guild_id,
            subject_key=subject_key,
            request_text=request_text,
            max_scan=max_scan,
            max_results=max_results,
        )
    finally:
        if owns_snapshot and conn.in_transaction:
            conn.rollback()


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
        "route_mode": "'unknown'",
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
            "route_mode"
            if "route_mode" in columns
            else "%s AS route_mode" % optional["route_mode"]
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
        "route_mode",
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
            route_mode=(
                _canon(row.get("route_mode"))
                if _canon(row.get("route_mode")) not in {"", "unknown"}
                else "conversation_continuity"
            ),
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


_LIVING_CANON_STRICT_ROOTS_PER_GROUP = 2


def _living_canon_prevalidation_entries(
    entries: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Choose at most two likely-independent roots before strict validation.

    Ledger timestamps can only affect bounded nomination here.  They never
    confer eligibility; the selected roots are subsequently rebound to main
    raw authority.  Prefer roots separated by an idle boundary, then fill.
    """

    ordered = sorted(
        (dict(entry) for entry in entries),
        key=lambda entry: (
            str(entry.get("observed_at") or ""),
            str(entry.get("entry_id") or ""),
        ),
        reverse=True,
    )
    selected: list[dict[str, Any]] = []
    for entry in ordered:
        observed = _parse_knowledge_time(entry.get("observed_at"))
        scope = (
            int(entry.get("channel_id") or 0),
            str(entry.get("channel_policy") or ""),
        )
        if observed is None:
            continue
        if all(
            scope
            != (
                int(prior.get("channel_id") or 0),
                str(prior.get("channel_policy") or ""),
            )
            or abs(
                (
                    observed
                    - (_parse_knowledge_time(prior.get("observed_at")) or observed)
                ).total_seconds()
            )
            > _CONVERSATION_MOTIF_WINDOW_SECONDS
            for prior in selected
        ):
            selected.append(entry)
            if len(selected) >= _LIVING_CANON_STRICT_ROOTS_PER_GROUP:
                return selected
    selected_ids = {str(entry.get("entry_id") or "") for entry in selected}
    for entry in ordered:
        if str(entry.get("entry_id") or "") in selected_ids:
            continue
        selected.append(entry)
        if len(selected) >= _LIVING_CANON_STRICT_ROOTS_PER_GROUP:
            break
    return selected


def _living_canon_rejection_reason_counts(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int,
) -> Counter[str]:
    """Classify bounded rows excluded by the eligible-history SQL."""

    reasons: Counter[str] = Counter()
    rows = conn.execute(
        """
        SELECT visibility,public_usable,derived,projection,source_role,
               source_class,channel_policy,lifecycle_status
        FROM main.memory_ledger_entries
        WHERE guild_id=? AND subject_key=? AND source_table='conversations'
          AND entry_type='observation' AND predicate_key='conversation'
        ORDER BY observed_at DESC,source_sequence DESC,entry_id DESC
        LIMIT ?
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            max(1, min(int(max_scan or 1), _CONVERSATION_MOTIF_MAX_SCAN)),
        ),
    ).fetchall()
    for row in rows:
        if (
            str(row[0] or "")
            not in {Visibility.PUBLIC.value, Visibility.PUBLIC_SAFE.value}
            or _public_assessment_bool_state(row[1]) is not True
            or str(row[6] or "") not in _CONVERSATION_MOTIF_PUBLIC_POLICIES
        ):
            reasons["visibility_ineligible"] += 1
        if (
            _public_assessment_bool_state(row[2]) is not False
            or _public_assessment_bool_state(row[3]) is not False
            or str(row[4] or "").lower() != "user"
            or str(row[5] or "") != SourceClass.PUBLIC_OBSERVATION.value
        ):
            reasons["derived_source_not_independent"] += 1
    return reasons


def _living_canon_analyze_groups(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    trigger_entry_id: str = "",
    max_scan: int = _CONVERSATION_MOTIF_MAX_SCAN,
    diagnostics: dict[str, int] | None = None,
    fence_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], Counter[str], int, int]:
    """Group cheaply, then strictly validate at most 6 x 2 roots."""

    reasons = _living_canon_rejection_reason_counts(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key or ""),
        max_scan=max_scan,
    )
    history = _conversation_motif_history(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key or ""),
        max_scan=max_scan,
        diagnostics=diagnostics,
        require_legacy_occurrence=False,
    )
    grouped: dict[str, dict[str, Any]] = {}
    unmatched: list[dict[str, Any]] = []
    for entry in history:
        matches = _conversation_motif_family_matches(
            tuple(entry.get("terms") or ())
        )
        if not matches:
            unmatched.append(entry)
        for family, label in matches:
            group = grouped.setdefault(
                "family:%s" % family,
                {
                    "predicate": "conversation_motif_%s" % family,
                    "family": family,
                    "label": label,
                    "entries": [],
                    "tags": (
                        family,
                        "recurring_public_conversation",
                        LIVING_CANON_RECURRENCE_VERSION,
                        LIVING_CANON_GROUPING_SIGNATURE_VERSION,
                    ),
                    "living_v1": True,
                },
            )
            group["entries"].append(entry)
    grouped.update(
        _conversation_motif_neutral_groups(
            unmatched,
            subject_key=str(subject_key or ""),
            diagnostics=diagnostics,
        )
    )
    ranked: list[tuple[int, float, str, dict[str, Any]]] = []
    for group_key, group in grouped.items():
        nominated = _living_canon_prevalidation_entries(group["entries"])
        last_seen = max(
            (str(entry.get("observed_at") or "") for entry in nominated),
            default="",
        )
        parsed = _parse_knowledge_time(last_seen)
        ranked.append(
            (-len(nominated), -(parsed.timestamp() if parsed else 0.0), group_key, group)
        )

    existing_rows = conn.execute(
        """
        SELECT predicate_key,candidate_state,invalidated_reason
        FROM main.memory_ledger_knowledge_candidates
        WHERE guild_id=? AND subject_key=? AND candidate_type='topic_or_motif'
          AND COALESCE(recurrence_contract_version,'')=?
        ORDER BY candidate_id
        """,
        (
            int(guild_id or 0),
            str(subject_key or ""),
            LIVING_CANON_RECURRENCE_VERSION,
        ),
    ).fetchall()
    existing_by_predicate: dict[str, list[tuple[str, str]]] = {}
    active_provisional_count = 0
    for predicate, state, invalidated_reason in existing_rows:
        existing_by_predicate.setdefault(str(predicate or ""), []).append(
            (str(state or ""), str(invalidated_reason or ""))
        )
        if str(state or "") == "provisional":
            active_provisional_count += 1

    accepted: list[dict[str, Any]] = []
    rejected = 0
    for _count, _last_seen, _group_key, group in sorted(ranked)[:
        _CONVERSATION_MOTIF_MAX_CANDIDATES
    ]:
        fenced_entries, fence = _conversation_motif_entries_after_fence(
            conn,
            guild_id=int(guild_id or 0),
            subject_key=str(subject_key or ""),
            predicate_key=str(group["predicate"]),
            entries=list(group["entries"]),
            fence_overrides=fence_overrides,
        )
        active_fence = bool(
            fence and str(fence.get("fence_state") or "active") == "active"
        )
        if active_fence:
            reasons["correction_fence_active"] += 1
        nominated = _living_canon_prevalidation_entries(fenced_entries)
        nominated_ids = tuple(
            str(entry.get("entry_id") or "")
            for entry in nominated
            if str(entry.get("entry_id") or "")
        )
        if not nominated_ids:
            rejected += 1
            if fence and str(fence.get("fence_state") or "active") == "active":
                reasons["fresh_recurrence_required_after_correction"] += 1
            continue
        states, occurrence_map, root_reasons = (
            _living_canon_root_states_and_occurrences(
                conn,
                guild_id=int(guild_id or 0),
                subject_key=str(subject_key or ""),
                entry_ids=nominated_ids,
            )
        )
        reasons.update(root_reasons)
        if (
            any(entry_id not in states for entry_id in nominated_ids)
            or any(entry_id not in occurrence_map for entry_id in nominated_ids)
            or not occurrence_map
        ):
            rejected += 1
            if not root_reasons:
                reasons["source_ineligible"] += 1
            continue
        authoritative: list[dict[str, Any]] = []
        expected_signature = tuple(group.get("signature") or ())
        expected_family = str(group.get("family") or "")
        for nominated_entry in nominated:
            entry_id = str(nominated_entry.get("entry_id") or "")
            state = states.get(entry_id)
            occurrence = occurrence_map.get(entry_id, "")
            if state is None or not occurrence:
                continue
            terms = _conversation_motif_terms(state.text)
            if expected_family and expected_family not in {
                family for family, _label in _conversation_motif_family_matches(terms)
            }:
                continue
            if expected_signature and _conversation_motif_exact_signature(
                state.text
            ) != expected_signature:
                reasons["meaning_ambiguous_review_only"] += 1
                continue
            item = dict(nominated_entry)
            item["normalized_value"] = state.text
            item["validated_text"] = state.text
            item["terms"] = terms
            item["root_identity"] = state.root_identity
            item["occurrence_identity"] = occurrence
            authoritative.append(item)
        root_ids, occurrence_ids = _conversation_motif_roots_by_occurrence(
            authoritative
        )
        if not root_ids or not occurrence_ids:
            rejected += 1
            reasons["source_ineligible"] += 1
            continue
        collapsed = max(0, len(root_ids) - len(occurrence_ids))
        if collapsed:
            reasons["same_occurrence_collapsed"] += collapsed
        if active_fence and len(occurrence_ids) < 2:
            reasons["fresh_recurrence_required_after_correction"] += 1
        existing = existing_by_predicate.get(str(group["predicate"]), [])
        if any(
            state == "contested"
            and invalidated_reason
            in {"unresolved_contradiction", "explicitly_contested_evidence"}
            for state, invalidated_reason in existing
        ):
            reasons["contradiction_contested"] += 1
        if (
            len(occurrence_ids) == 1
            and str(trigger_entry_id or "")
            and str(trigger_entry_id or "") not in root_ids
        ):
            rejected += 1
            continue
        if (
            len(occurrence_ids) == 1
            and not existing
            and active_provisional_count >= _CONVERSATION_MOTIF_MAX_CANDIDATES
        ):
            rejected += 1
            reasons["provisional_subject_bound_reached"] += 1
            continue
        if len(occurrence_ids) == 1 and not existing:
            active_provisional_count += 1
        result_group = dict(group)
        result_group["entries"] = authoritative
        result_group["root_ids"] = root_ids
        result_group["occurrence_ids"] = occurrence_ids
        result_group["correction_fence"] = fence
        accepted.append(result_group)
    if diagnostics is not None:
        for reason, count in reasons.items():
            diagnostics[reason] = int(diagnostics.get(reason, 0) or 0) + int(count)
        diagnostics["strict_validation_group_count"] = len(
            sorted(ranked)[:_CONVERSATION_MOTIF_MAX_CANDIDATES]
        )
        diagnostics["strict_validation_root_count"] = sum(
            len(group.get("root_ids") or ()) for group in accepted
        )
    return accepted, reasons, rejected, len(history)


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
    legacy_formation = conversation_motif_formation_enabled(environ)
    living_v1_formation = living_canon_v1_formation_enabled(environ)
    if not legacy_formation and not living_v1_formation:
        return []
    if living_v1_formation and not _living_canon_main_authority_unshadowed(conn):
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

    # Preserve the legacy path exactly.  Living v1 never silently projects
    # retained history; historical projection remains a separately gated
    # operation.
    if legacy_formation and not living_v1_formation:
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
        include_neutral=living_v1_formation,
    )
    if living_v1_formation:
        analyzed_groups, _analysis_reasons, _rejected, history_count = (
            _living_canon_analyze_groups(
                conn,
                guild_id=int(guild_id or 0),
                subject_key=str(subject_key),
                trigger_entry_id=trigger_id,
                max_scan=max_scan,
                diagnostics=diagnostics,
            )
        )
        grouped = {
            str(group.get("predicate") or ""): group
            for group in analyzed_groups
        }
        history: list[dict[str, Any]] = [
            entry for group in analyzed_groups for entry in group["entries"]
        ]
        if history_count <= 0:
            _record_knowledge_receipt(
                conn,
                guild_id=int(guild_id or 0),
                event_type="formation_skipped",
                reason_code="conversation_motif_no_eligible_history",
                candidate_type="topic_or_motif",
            )
            return []
    else:
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
        grouped = {}
        for entry in history:
            matches = _conversation_motif_family_matches(
                tuple(entry.get("terms") or ())
            )
            if not matches and diagnostics is not None:
                diagnostics["ledger_rows_family_unmatched"] = (
                    int(diagnostics.get("ledger_rows_family_unmatched", 0) or 0)
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
                        "living_v1": False,
                    },
                )
                group["entries"].append(entry)
    if diagnostics is not None:
        diagnostics["motif_families_matched"] = sum(
            1 for group in grouped.values() if not group.get("neutral")
        )
        diagnostics["motif_neutral_groups_proposed"] = sum(
            1 for group in grouped.values() if group.get("neutral")
        )

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
        minimum_recurrence = 1 if group.get("living_v1") else 2
        if (
            len(occurrence_ids) < minimum_recurrence
            or len(root_ids) < minimum_recurrence
        ):
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
        living_v1 = bool(group.get("living_v1"))
        grouping_identity = (
            _knowledge_digest(
                LIVING_CANON_GROUPING_SIGNATURE_VERSION,
                str(subject_key),
                str(group["predicate"]),
                "real_community",
            )
            if living_v1
            else ""
        )
        result = form_atomic_knowledge_candidate(
            conn,
            AtomicKnowledgeProposal(
                candidate_type="topic_or_motif",
                subject_key=str(subject_key),
                subject_display_name=display_name,
                predicate_key=str(group["predicate"]),
                meaning=(
                    "Possible public conversation theme about %s."
                    % str(group["label"]).strip()
                ),
                root_entry_ids=root_ids,
                participant_keys=(str(subject_key),),
                epistemic_status="observed",
                uncertainty_note=(
                    "Cautious public conversation observation; not a scalar "
                    "identity fact or exact quote."
                ),
                currentness="historical",
                contradiction_key=(
                    "%s:%s" % (str(subject_key), str(group["predicate"]))
                ),
                retrieval_tags=tuple(group["tags"]),
                recurrence_contract_version=(
                    LIVING_CANON_RECURRENCE_VERSION if living_v1 else ""
                ),
                grouping_signature_version=(
                    LIVING_CANON_GROUPING_SIGNATURE_VERSION if living_v1 else ""
                ),
                grouping_identity=grouping_identity,
                canon_domain="real_community" if living_v1 else "",
                canon_claim_kind="behavior_pattern" if living_v1 else "",
                occurrence_ids=occurrence_ids if living_v1 else (),
            ),
            _living_formation_receipt=(
                _living_canon_formation_receipt() if living_v1 else None
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
        if diagnostics is not None and living_v1:
            reason_key = (
                "independent_recurrence_established"
                if len(occurrence_ids) >= 2 and len(root_ids) >= 2
                else "single_occurrence_provisional"
            )
            diagnostics[reason_key] = int(
                diagnostics.get(reason_key, 0) or 0
            ) + 1
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


def preview_living_canon_formation(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    subject_key: str,
    max_scan: int = _CONVERSATION_MOTIF_MAX_SCAN,
) -> LivingCanonDryRunReport:
    """Analyze existing source-bound Ledger roots without schema or data writes."""

    before = int(conn.total_changes)
    if (
        int(guild_id or 0) <= 0
        or not str(subject_key or "").startswith("discord_user:")
        or not {
            "memory_ledger_entries",
            "memory_ledger_lineage",
            "memory_ledger_participants",
            "memory_ledger_conversation_motif_fences",
            "conversations",
        }.issubset(
            {
                str(row[0] or "")
                for row in conn.execute(
                    "SELECT name FROM main.sqlite_master WHERE type='table'"
                ).fetchall()
            }
        )
        or not _living_canon_main_authority_unshadowed(conn)
    ):
        return LivingCanonDryRunReport(
            skipped_count=1,
            reason_counts=(("source_ineligible", 1),),
            source_write_count=int(conn.total_changes) - before,
            write_occurred=int(conn.total_changes) != before,
        )

    diagnostics: dict[str, int] = {}
    pending_fences = _preview_conversation_motif_correction_fences(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key),
        max_scan=max_scan,
        include_neutral=True,
    )
    groups, reasons, rejected, _history_count = _living_canon_analyze_groups(
        conn,
        guild_id=int(guild_id or 0),
        subject_key=str(subject_key),
        max_scan=max_scan,
        diagnostics=diagnostics,
        fence_overrides=pending_fences,
    )
    state_counts: Counter[str] = Counter()
    root_total = occurrence_total = collapsed_total = 0
    for group in groups:
        roots = tuple(group.get("root_ids") or ())
        occurrences = tuple(group.get("occurrence_ids") or ())
        root_total += len(roots)
        occurrence_total += len(occurrences)
        collapsed_total += max(0, len(roots) - len(occurrences))
        state = "established" if len(roots) >= 2 and len(occurrences) >= 2 else "provisional"
        state_counts[state] += 1
        reasons[
            "independent_recurrence_established"
            if state == "established"
            else "single_occurrence_provisional"
        ] += 1
    changes = int(conn.total_changes) - before
    return LivingCanonDryRunReport(
        proposed_count=len(groups),
        skipped_count=sum(
            value
            for key, value in reasons.items()
            if key in {"source_ineligible", "unbounded_occurrence_withheld"}
        ),
        ambiguous_count=int(reasons.get("meaning_ambiguous_review_only", 0)),
        rejected_count=int(rejected),
        candidate_state_counts=tuple(sorted(state_counts.items())),
        reason_counts=tuple(sorted(reasons.items())),
        independent_root_count=root_total,
        independent_occurrence_count=occurrence_total,
        collapsed_root_count=collapsed_total,
        source_write_count=changes,
        write_occurred=bool(changes),
    )


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
        "route_mode": "'unknown'",
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
            "c.route_mode"
            if "route_mode" in columns
            else "%s AS route_mode" % optional["route_mode"]
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
        "route_mode",
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
            route_mode=(
                _canon(row.get("route_mode"))
                if _canon(row.get("route_mode")) not in {"", "unknown"}
                else "conversation_continuity"
            ),
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
        FROM main.memory_ledger_entries e
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
            SELECT 1 FROM main.memory_ledger_lineage l
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
        FROM main.memory_ledger_entries e
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
            SELECT 1 FROM main.memory_ledger_lineage l
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
            SELECT 1 FROM main.memory_ledger_lineage AS incoming
            WHERE incoming.guild_id={alias}.guild_id
              AND incoming.target_entry_id={alias}.entry_id
              AND incoming.lineage_type IN ('supersedes','retracts')
        )
    """.format(alias=alias)


def effective_broadcast_representations(
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
        FROM main.memory_ledger_entries AS entry
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
        FROM main.memory_ledger_entries AS projection_row
        JOIN main.memory_ledger_lineage AS source_edge
          ON source_edge.guild_id=projection_row.guild_id
         AND source_edge.entry_id=projection_row.entry_id
         AND source_edge.lineage_type='derived_from'
        JOIN main.memory_ledger_entries AS source_root
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
        for row in conn.execute(
            "PRAGMA main.table_info(declared_canon_revisions)"
        )
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
            FROM main.memory_ledger_entries AS projection_row
            JOIN main.declared_canon_revisions AS revision
              ON revision.guild_id=projection_row.guild_id
             AND revision.declaration_id=projection_row.source_row_id
             AND revision.revision_id=projection_row.source_revision
            WHERE projection_row.guild_id=?
              AND projection_row.source_table='declared_canon_projection'
              AND projection_row.source_role='declared_canon_projection'
              AND revision.source_system='broadcast_memory'
              AND revision.source_row_id=?
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


def _effective_broadcast_representations(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    source_row_id: int | str,
) -> BroadcastEffectiveRepresentations:
    """Compatibility alias for callers introduced before the public helper."""

    return effective_broadcast_representations(
        conn,
        guild_id=guild_id,
        source_row_id=source_row_id,
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
        FROM main.memory_ledger_entries WHERE entry_id=?
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
            FROM main.memory_ledger_participants WHERE entry_id=?
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
            SELECT 1 FROM main.memory_ledger_entries
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
            INSERT OR IGNORE INTO main.memory_ledger_lineage
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
            for row in conn.execute("PRAGMA main.table_info(broadcast_memory)")
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
            FROM main.broadcast_memory
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

        old_representations = _effective_broadcast_representations(
            conn,
            guild_id=normalized_guild_id,
            source_row_id=normalized_row_id,
        )
        old_entries = old_representations.primary_entry_ids
        old_declared_projections = (
            old_representations.declared_projection_entry_ids
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
        participants = (
            LedgerParticipant(
                "discord_user:%s" % normalized_actor_id,
                str(source_row[4] or ""),
                "correction_actor",
                0,
            ),
        )
        status_entry = LedgerEntry(
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
            participants=participants,
            lineage=tuple(lineage_items),
        )
        status_result = _insert_or_reconcile_ledger_lineage(
            conn,
            status_entry,
            conflict_reason="broadcast_status_identity_conflict",
        )
        if status_result.outcome not in {"inserted", "deduplicated"}:
            raise RuntimeError(
                "broadcast_status_lineage_write_failed:%s"
                % status_result.reason_code
            )

        # Keep projection retractions on their own internal event so the
        # established bot boundary can continue verifying the primary event's
        # exact root set. Both events are nevertheless written and verified in
        # this same source transaction.
        if old_declared_projections:
            projection_rev = source_revision_for(
                normalized_row_id,
                normalized_updated_at,
                event=(
                    "declared_projection_status_v1:%s:%s"
                    % (normalized_status, normalized_updated_at)
                ),
            )
            projection_entry = LedgerEntry(
                guild_id=normalized_guild_id,
                source_table="broadcast_memory",
                source_row_id=normalized_row_id,
                source_revision=projection_rev,
                source_event_key=(
                    "declared_projection_status_v1:%s" % normalized_status
                ),
                source_role="broadcast_memory_projection_status",
                entry_type="event",
                subject_key="barcode_radio",
                subject_display_name="BARCODE Radio",
                predicate_key=(
                    "broadcast_projection_status:%s" % normalized_status
                ),
                value=normalized_status,
                source_class=SourceClass.FIRST_PARTY_RECORD,
                visibility=Visibility.INTERNAL,
                confidence=Confidence.HIGH,
                public_usable=False,
                derived=True,
                projection=True,
                observed_at=normalized_updated_at,
                source_sequence=normalized_row_id,
                lifecycle_status=lifecycle,
                participants=participants,
                lineage=tuple(
                    ("retracts", entry_id)
                    for entry_id in old_declared_projections
                ),
            )
            projection_result = _insert_or_reconcile_ledger_lineage(
                conn,
                projection_entry,
                conflict_reason=(
                    "broadcast_projection_status_identity_conflict"
                ),
            )
            if projection_result.outcome not in {"inserted", "deduplicated"}:
                raise RuntimeError(
                    "broadcast_projection_lineage_write_failed:%s"
                    % projection_result.reason_code
                )
            record_shadow_receipt(
                conn,
                guild_id=normalized_guild_id,
                writer="broadcast_memory_projection_status",
                source_table=projection_result.source_table,
                source_row_id=projection_result.source_row_id,
                source_revision=projection_result.source_revision,
                source_event_key=projection_result.source_event_key,
                outcome=projection_result.outcome,
                reason_code=projection_result.reason_code,
                entry_id=projection_result.entry_id,
            )

        remaining = _effective_broadcast_representations(
            conn,
            guild_id=normalized_guild_id,
            source_row_id=normalized_row_id,
        )
        if remaining.all_entry_ids:
            raise RuntimeError("broadcast_terminal_retraction_incomplete")
        return status_result


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


def _effective_declared_projection_entries(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    declaration_id: str,
    exclude_revision_id: str = "",
) -> tuple[str, ...]:
    """Return every unretracted projection for one Declared declaration.

    The declaration revision store decides which revision is current.  This
    helper deliberately preserves multiplicity in the Ledger so a validated
    current/terminal projection can invalidate every stale representation
    instead of electing one duplicate and leaving the others effective.
    """

    ensure_memory_ledger_schema(conn)
    sql = """
        SELECT projection_row.entry_id
        FROM main.memory_ledger_entries AS projection_row
        WHERE projection_row.guild_id=?
          AND projection_row.source_table='declared_canon_projection'
          AND projection_row.source_role='declared_canon_projection'
          AND projection_row.source_row_id=?
          AND NOT EXISTS (
              SELECT 1 FROM main.memory_ledger_lineage AS incoming
              WHERE incoming.guild_id=projection_row.guild_id
                AND incoming.target_entry_id=projection_row.entry_id
                AND incoming.lineage_type IN ('supersedes','retracts')
          )
    """
    params: list[Any] = [int(guild_id or 0), str(declaration_id or "")]
    normalized_exclusion = str(exclude_revision_id or "").strip()
    if normalized_exclusion:
        sql += " AND projection_row.source_revision!=?"
        params.append(normalized_exclusion)
    sql += " ORDER BY projection_row.created_at,projection_row.entry_id"
    return tuple(
        sorted(
            {
                str(row[0])
                for row in conn.execute(sql, params).fetchall()
                if str(row[0] or "")
            }
        )
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
                    FROM main.broadcast_memory
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
                    FROM main.memory_ledger_entries
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
                        FROM main.memory_ledger_lineage
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
                    FROM main.memory_ledger_entries AS root
                    WHERE root.guild_id=?
                      AND root.source_table='broadcast_memory'
                      AND root.source_row_id=?
                      AND root.source_role='broadcast_memory'
                      AND root.lifecycle_status='active'
                      AND NOT EXISTS (
                        SELECT 1
                        FROM main.memory_ledger_lineage AS edge
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

        previous = _effective_declared_projection_entries(
            conn,
            guild_id=normalized_guild_id,
            declaration_id=declaration_id,
            exclude_revision_id=revision_id,
        )

        cross_declaration_previous: tuple[str, ...] = ()
        if revision.supersedes_declaration_id:
            superseded_latest = conn.execute(
                """
                SELECT previous_revision_id,lifecycle_status,
                       superseded_by_declaration_id
                FROM main.declared_canon_revisions
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
            cross_declaration_previous = _effective_declared_projection_entries(
                conn,
                guild_id=normalized_guild_id,
                declaration_id=revision.supersedes_declaration_id,
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

        projection_entry = LedgerEntry(
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
        )
        return _insert_or_reconcile_ledger_lineage(
            conn,
            projection_entry,
            conflict_reason="declared_projection_identity_conflict",
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
