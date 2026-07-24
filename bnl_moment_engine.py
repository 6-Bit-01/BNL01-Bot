"""Moment Engine v1 shadow infrastructure.

Builds bounded, auditable conversational moments from Unified Memory Ledger
conversation entries only. Construction remains shadow-first; a separately
allowlisted prompt canary may render only revalidated public-safe gist.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
import sqlite3
from typing import Any

from bnl_canon_source_contract import Confidence, SourceClass, Visibility
from bnl_memory_ledger import (
    BNL_SUBJECT_KEY,
    LedgerEntry,
    LedgerParticipant,
    ensure_memory_ledger_schema,
    insert_ledger_entry,
    shadow_enabled as ledger_shadow_enabled,
)

MOMENT_ENGINE_SHADOW_ENV = "BNL_MOMENT_ENGINE_SHADOW_ENABLED"
MOMENT_SCHEMA_VERSION = "memory_moment_v1"
REMEMBERED_NUMBER_QUARANTINE_MIGRATION = "remembered_number_quarantine_v1"
SAFE_MOMENT_PROJECTION_MIGRATION = "safe_moment_projection_v1"
MOMENT_CONTRIBUTION_BACKFILL_MIGRATION = "moment_contribution_backfill_v1"
MAX_WINDOW_SECONDS = 5 * 60
INACTIVITY_SECONDS = 2 * 60
CONTRIBUTION_GIST_VERSION = "moment_contribution_gist_v1"

STOP = set("a an and are as at be but by for from how i in is it me my of on or our that the this to was we what when where who why with you your did do does about into can could would should just yep yes no ok okay hey hi hello thanks thank lol lmao got noted also ask asked".split())
LOW_SIGNAL = set("hi hey hello thanks thank you ok okay yep yes no lol lmao cool nice got it noted".split())
STRONG_MARKERS = (
    "correction",
    "actually",
    "replace",
    "commit",
    "promise",
    "boundary",
    "milestone",
    "follow up",
    "follow-up",
    "celebrate",
)
VIS_RANK = {"public": 0, "public_safe": 0, "reference_canon": 0, "internal": 2, "private": 3, "mod": 3, "sealed_test": 4, "protected": 4, "ai_image_tool": 4, "unknown": 5}
PUBLIC_CROSS_CHANNEL_POLICIES = frozenset({"public_home", "public_context"})
SOURCE_LIFECYCLES_USABLE_FOR_MOMENTS = frozenset({"active", "review_only"})
TOPIC_GIST_LABELS = {
    "music_production": "music-production",
    "cooking": "cooking and food",
    "outdoors": "outdoor-planning",
    "topic_other": "general recurring",
}


@dataclass(frozen=True)
class MomentObservationResult:
    outcome: str = "skipped"
    reason_code: str = "not_attempted"
    moment_id: str = ""
    ledger_entry_id: str = ""


@dataclass(frozen=True)
class SourceEntry:
    entry_id: str
    guild_id: int
    source_table: str
    source_role: str
    entry_type: str
    predicate_key: str
    normalized_value: str
    route_mode: str
    channel_id: int
    channel_name: str
    channel_policy: str
    visibility: str
    public_usable: bool
    observed_at: str
    source_sequence: int
    lifecycle_status: str
    subject_key: str
    subject_display_name: str

    @property
    def is_human(self) -> bool:
        return self.source_role == "user"

    @property
    def is_model(self) -> bool:
        return self.source_role != "user"


def shadow_enabled(environ: dict[str, str] | None = None) -> bool:
    return str((environ or os.environ).get(MOMENT_ENGINE_SHADOW_ENV, "")).strip().lower() in {"1", "true", "yes", "on", "enabled"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_ts(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _canon(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _tokens(text: str) -> tuple[str, ...]:
    return tuple(t for t in re.findall(r"[a-z0-9]{2,}", _canon(text)) if t not in STOP)[:24]


def _topic_family(text: str, predicate_key: str) -> str:
    del predicate_key
    canon = _canon(text)
    toks = set(_tokens(canon))
    families = (
        ("music_production", {"synth", "drum", "drums", "bass", "riff", "vocal", "mix", "patch", "bridge", "production"}),
        ("cooking", {"pizza", "oven", "dough", "sauce", "bake", "baking", "cheese"}),
        ("outdoors", {"hiking", "hike", "trail", "rain", "weather", "mountain", "boots", "conditions"}),
    )
    for name, markers in families:
        if toks & markers:
            return name
    if toks:
        # The family is durable metadata. Unknown source terms therefore must
        # not be copied into it.
        return "topic_other"
    return "low_signal"


def _topic_token_digest(token: str) -> str:
    return "tok_" + hashlib.sha256(
        f"{MOMENT_SCHEMA_VERSION}\x1f{token}".encode("utf-8")
    ).hexdigest()[:16]


def _topic_signature(text: str, predicate_key: str) -> tuple[str, ...]:
    del predicate_key
    # Signatures are used only for coherence matching. Persisting one-way
    # digests preserves that behavior without retaining exact words, names,
    # numbers, or codes from the conversation.
    return tuple(
        sorted(
            {
                _topic_token_digest(token)
                for token in _tokens(text)
                if token.isalpha()
            }
        )
    )[:16]


def _topic_key(family: str, signature: tuple[str, ...]) -> str:
    base = family + "|" + " ".join(signature)
    return "topic_" + hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]


def stable_moment_id(guild_id: int, channel_id: int, topic_key: str, started_at: str) -> str:
    return "mom_" + hashlib.sha256(f"{MOMENT_SCHEMA_VERSION}\x1f{guild_id}\x1f{channel_id}\x1f{topic_key}\x1f{started_at}".encode("utf-8")).hexdigest()[:32]


def _meaningful(text: str, role: str, predicate_key: str) -> bool:
    canon = _canon(text)
    if role != "user":
        return False
    if any(marker in canon for marker in STRONG_MARKERS):
        return True
    if re.fullmatch(r"[!?.\s]+", canon or ""):
        return False
    toks = [t for t in _tokens(canon) if t not in LOW_SIGNAL]
    return len(toks) >= 2 or len(canon) >= 24


def _strong_marker(text: str, predicate_key: str) -> bool:
    canon = _canon(text)
    return any(marker in canon for marker in STRONG_MARKERS)


_OPAQUE_REMEMBER_NUMBER_RE = re.compile(
    r"\b(?:remember|save|hold onto|keep)\b.{0,40}\b(?:number|code|pin)\b"
    r"|(?:\b(?:remember|save|hold onto|keep)\b.{0,20}[:#]?\s*)\d{3,}\b",
    re.I,
)
_DIRECT_SECRET_RE = re.compile(
    r"\b(?:password|passcode|pin|one[- ]?time (?:code|password)|otp|"
    r"verification code|security code|recovery code|access code|door code|"
    r"api key|secret key|private key|seed phrase|"
    r"(?:auth|access|deployment|session) token|routing number|"
    r"bank account|credit card|debit card|social security|ssn)\b",
    re.I,
)
_DIRECT_PERSONAL_FACT_RE = re.compile(
    r"\b(?:call me|my (?:email|phone(?: number)?|home address|street address|"
    r"legal name|real name|full name|preferred name|pronouns?|birthday|"
    r"date of birth|employer|workplace|favorite "
    r"(?:color|movie|food|place))\s+(?:is|are)|"
    r"i (?:live|reside)\s+(?:at|in|near))\b",
    re.I,
)
_EMAIL_OR_URL_RE = re.compile(
    r"(?:\bhttps?://|\bwww\.|\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b|"
    r"<@!?\d+>)",
    re.I,
)
_PHONE_OR_ACCOUNT_NUMBER_RE = re.compile(
    r"(?<!\d)(?:\+?\d[\s().-]*){7,}(?!\d)"
)
_OPAQUE_CODE_TOKEN_RE = re.compile(
    r"(?<![a-z0-9])(?=[a-z0-9_-]{6,}(?![a-z0-9_-]))"
    r"(?=[a-z0-9_-]*[a-z])(?=[a-z0-9_-]*\d)[a-z0-9_-]+",
    re.I,
)
_OPAQUE_NUMERIC_TOKEN_RE = re.compile(r"(?<!\d)\d{6,}(?!\d)")
_PROMPT_CONTROL_RE = re.compile(
    r"\b(?:ignore|disregard|override|bypass|forget)\b.{0,40}"
    r"\b(?:previous|prior|system|developer|assistant|prompt|instructions?|rules?)\b"
    r"|\b(?:follow|obey|execute)\b.{0,40}"
    r"\b(?:prompt|instructions?|rules?|commands?)\b"
    r"|\b(?:respond|reply|output|print)\b.{0,24}"
    r"\b(?:with|only|exactly)\b"
    r"|\b(?:you are now|act as)\b"
    r"|\b(?:system|developer|assistant)\s+(?:message|prompt|instructions?)\b"
    r"|\b(?:jailbreak|prompt injection|hidden prompt|chain of thought)\b",
    re.I,
)
_DISPLAY_INSTRUCTION_RE = re.compile(
    r"\b(?:ignore|disregard|override|bypass|obey|execute|reveal|"
    r"respond|reply|output|print|pretend|instructions?|prompt|"
    r"act\s+as|you\s+are|do\s+not|must)\b",
    re.I,
)
_DIRECT_SENSITIVE_PERSONAL_RE = re.compile(
    r"\b(?:diagnos(?:ed|is)|medical condition|health condition|medication|"
    r"therapy|therapist|pregnan(?:t|cy)|sexuality|sexual orientation|"
    r"gender identity|race|ethnicity|religion|political affiliation|"
    r"immigration status|criminal record|salary|income|"
    r"bank balance|financial account|home location|where i live|"
    r"family emergency|private relationship)\b",
    re.I,
)
_EXACT_AUTHORITY_REQUEST_RE = re.compile(
    r"\b(?:exact(?:ly)?|literal(?:ly| wording)?|verbatim|"
    r"word[- ]for[- ]word|direct quote|"
    r"quote(?:d|s| me)?|quotation|exact words?|prove(?:s|d)?|"
    r"dispute|settle(?:s|d)? (?:the|a|this) dispute)\b",
    re.I,
)
_ATTRIBUTION_REQUEST_PATTERNS = (
    re.compile(
        r"\bwhat\s+(?:did|does)\s+(?P<target>.+?)\s+"
        r"(?P<verb>say|said|mean|meant|think|thought|contribute)"
        r"(?:\s+(?:about|regarding|on)\s+(?P<topic>.+?))?[?!.]*$",
        re.I,
    ),
    re.compile(
        r"\bremind\s+me\s+what\s+(?P<target>.+?)\s+"
        r"(?P<verb>said|meant|thought|contributed)"
        r"(?:\s+(?:about|regarding|on)\s+(?P<topic>.+?))?[?!.]*$",
        re.I,
    ),
    re.compile(
        r"\bwhat\s+was\s+(?P<target>.+?)\s+"
        r"(?P<verb>saying|meaning|thinking)"
        r"(?:\s+(?:about|regarding|on)\s+(?P<topic>.+?))?[?!.]*$",
        re.I,
    ),
)
_DISCORD_MENTION_RE = re.compile(r"<@!?(\d+)>")
_SAFE_CONTRIBUTION_TOKEN_RE = re.compile(r"[a-z][a-z'-]{2,30}", re.I)
_CONTRIBUTION_STOP = STOP | {
    "participant",
    "contribution",
    "contributed",
    "discussion",
    "discussed",
    "talked",
    "said",
    "saying",
    "mean",
    "meant",
    "think",
    "thought",
    "thing",
    "things",
    "really",
    "still",
    "then",
    "than",
    "there",
    "they",
    "them",
    "their",
    "something",
    "anything",
    "everything",
    "bnl",
    "actually",
    "correction",
    "corrected",
    "replace",
    "instead",
    "decided",
    "agreed",
    "chosen",
    "chose",
    "finalized",
    "settled",
    "plan",
    "planned",
    "planning",
    "going",
    "intend",
    "will",
    "prefer",
    "preferred",
    "rather",
    "want",
    "wanted",
    "suggest",
    "suggested",
    "propose",
    "proposed",
    "recommend",
    "maybe",
    "wonder",
    "wondered",
    "question",
    "whether",
    "noticed",
    "observed",
    "found",
    "saw",
    "happened",
    "reported",
}
def _is_opaque_remember_number_request(text: str, predicate_key: str = "") -> bool:
    return bool(
        predicate_key == "remembered_number"
        or _OPAQUE_REMEMBER_NUMBER_RE.search(str(text or ""))
    )


def _contains_sensitive_moment_source(
    text: str,
    predicate_key: str = "",
) -> bool:
    value = str(text or "")
    predicate = _canon(predicate_key)
    if _is_opaque_remember_number_request(value, predicate):
        return True
    if predicate in {
        "password",
        "passcode",
        "pin",
        "secret",
        "api_key",
        "phone_number",
        "email",
        "home_address",
        "street_address",
        "remembered_number",
    }:
        return True
    return any(
        pattern.search(value)
        for pattern in (
            _DIRECT_SECRET_RE,
            _DIRECT_PERSONAL_FACT_RE,
            _DIRECT_SENSITIVE_PERSONAL_RE,
            _EMAIL_OR_URL_RE,
            _PHONE_OR_ACCOUNT_NUMBER_RE,
            _OPAQUE_CODE_TOKEN_RE,
            _OPAQUE_NUMERIC_TOKEN_RE,
            _PROMPT_CONTROL_RE,
        )
    )


def _safe_participant_display_name(value: str) -> str:
    raw = str(value or "")
    if any(ord(char) < 32 or ord(char) == 127 for char in raw):
        return ""
    name = re.sub(r"\s+", " ", raw).strip()
    if not name or len(name) > 80:
        return ""
    if (
        _EMAIL_OR_URL_RE.search(name)
        or _PHONE_OR_ACCOUNT_NUMBER_RE.search(name)
        or _OPAQUE_CODE_TOKEN_RE.search(name)
        or _PROMPT_CONTROL_RE.search(name)
        or _DISPLAY_INSTRUCTION_RE.search(name)
        or re.search(r"^(?:system|assistant|developer|user|tool)\s*:", name, re.I)
        or re.search(r"@(?:everyone|here)\b", name, re.I)
        or any(
            char in name
            for char in (
                '"', "“", "”", "`", "[", "]", "{", "}", "*", "_", "~",
                "|", ">", "<", "@", "#", ":", ";", "=", "\\",
            )
        )
    ):
        return ""
    return name


def _label_key(value: str) -> str:
    label = re.sub(r"\s+", " ", str(value or "").strip())
    if label.startswith("@") and not label.startswith("@@"):
        label = label[1:].lstrip()
    return label.casefold()


def _source_digest(rows: list[SourceEntry]) -> str:
    payload = "\x1e".join(
        "\x1f".join(
            (
                row.entry_id,
                str(row.guild_id),
                str(row.channel_id),
                row.channel_policy,
                row.route_mode,
                row.visibility,
                row.lifecycle_status,
                row.normalized_value,
            )
        )
        for row in sorted(rows, key=lambda item: item.entry_id)
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalized_words(value: str) -> tuple[str, ...]:
    return tuple(re.findall(r"[a-z][a-z'-]*", _canon(value)))


def _contains_meaningful_source_ngram(
    gist: str,
    source_texts: list[str],
    *,
    size: int = 4,
) -> bool:
    gist_words = _normalized_words(gist)
    if len(gist_words) < size:
        return False
    gist_ngrams = {
        gist_words[index : index + size]
        for index in range(len(gist_words) - size + 1)
    }
    for source_text in source_texts:
        words = _normalized_words(source_text)
        if any(
            words[index : index + size] in gist_ngrams
            for index in range(len(words) - size + 1)
        ):
            return True
    return False


_SEMANTIC_CONCEPT_STOP = _CONTRIBUTION_STOP | {
    "against",
    "around",
    "because",
    "before",
    "being",
    "choose",
    "chooses",
    "choosing",
    "connect",
    "connecting",
    "direction",
    "during",
    "either",
    "favor",
    "favors",
    "favored",
    "guide",
    "guides",
    "guiding",
    "hold",
    "holds",
    "keep",
    "keeps",
    "keeping",
    "move",
    "moves",
    "moving",
    "option",
    "rather",
    "select",
    "selected",
    "selecting",
    "than",
    "through",
    "toward",
    "towards",
    "until",
    "use",
    "uses",
    "using",
    "versus",
    "while",
}


@dataclass(frozen=True)
class ContributionSemanticFrame:
    frame_type: str
    primary: tuple[str, ...]
    secondary: tuple[str, ...] = ()


def _semantic_concepts(
    text: str,
    source: SourceEntry,
    *,
    minimum: int = 1,
    maximum: int = 3,
) -> tuple[str, ...]:
    banned = {
        token.casefold()
        for token in _SAFE_CONTRIBUTION_TOKEN_RE.findall(
            str(source.subject_display_name or "")
        )
    }
    concepts: list[str] = []
    for token in _SAFE_CONTRIBUTION_TOKEN_RE.findall(_canon(text)):
        token = token.casefold()
        if (
            token in _SEMANTIC_CONCEPT_STOP
            or token in banned
            or token in concepts
            or _DIRECT_SECRET_RE.fullmatch(token)
            or _PROMPT_CONTROL_RE.search(token)
            or _DISPLAY_INSTRUCTION_RE.search(token)
        ):
            continue
        concepts.append(token)
        if len(concepts) >= maximum:
            break
    return tuple(concepts) if len(concepts) >= minimum else ()


def _make_semantic_frame(
    source: SourceEntry,
    frame_type: str,
    primary_text: str,
    secondary_text: str = "",
    *,
    minimum_primary: int = 2,
    minimum_secondary: int = 1,
) -> ContributionSemanticFrame | None:
    primary = _semantic_concepts(
        primary_text,
        source,
        minimum=minimum_primary,
    )
    if not primary:
        return None
    secondary: tuple[str, ...] = ()
    if secondary_text:
        secondary = _semantic_concepts(
            secondary_text,
            source,
            minimum=minimum_secondary,
        )
        if not secondary:
            return None
    return ContributionSemanticFrame(frame_type, primary, secondary)


def _parse_contribution_semantic_frame(
    source: SourceEntry,
) -> ContributionSemanticFrame | None:
    text = re.sub(r"\s+", " ", str(source.normalized_value or "")).strip()
    if (
        not text
        or _contains_sensitive_moment_source(text, source.predicate_key)
    ):
        return None

    conditional = re.search(
        r"\bif\s+(?P<premise>[^,;.!?]+)[,;]\s*"
        r"(?:then\s+)?(?P<action>[^.!?]+)",
        text,
        re.I,
    )
    if conditional and re.search(
        r"\b(?:will|would|plan|intend|should|could|choose|select|"
        r"use|keep|move|delay|start|make|go|let(?:'|’)s)\b",
        conditional.group("action"),
        re.I,
    ):
        return _make_semantic_frame(
            source,
            "conditional_plan",
            conditional.group("premise"),
            conditional.group("action"),
            minimum_primary=1,
            minimum_secondary=1,
        )

    replacement_patterns = (
        re.compile(
            r"\b(?:replace|swap)\s+(?P<old>.+?)\s+"
            r"(?:with|for)\s+(?P<new>[^.!?]+)",
            re.I,
        ),
        re.compile(
            r"\bchange\s+(?P<old>.+?)\s+(?:to|into)\s+"
            r"(?P<new>[^.!?]+)",
            re.I,
        ),
        re.compile(
            r"\b(?:not|reject|avoid|drop)\s+(?P<old>[^,;.!?]+)"
            r"[,;]\s*(?:instead[, ]*)?(?:choose|select|use|keep|"
            r"prefer|go with)\s+(?P<new>[^.!?]+)",
            re.I,
        ),
        re.compile(
            r"\b(?:choose|select|use|keep|prefer|go with)\s+"
            r"(?P<new>.+?)\s+(?:instead of|over|rather than)\s+"
            r"(?P<old>[^.!?]+)",
            re.I,
        ),
    )
    for pattern in replacement_patterns:
        replacement = pattern.search(text)
        if replacement:
            correction = bool(
                re.search(r"\b(?:actually|correction|corrected)\b", text, re.I)
            )
            return _make_semantic_frame(
                source,
                (
                    "correction_replacement"
                    if correction
                    else "replacement"
                ),
                replacement.group("old"),
                replacement.group("new"),
                minimum_primary=1,
                minimum_secondary=1,
            )

    disagreement = re.search(
        r"\b(?:(?:i\s+)?(?:do not|don't)\s+agree\s+with|"
        r"(?:i\s+)?disagree\s+with|oppose|push back on)\s+"
        r"(?P<direction>[^,;.!?]+)"
        r"(?:[,;]\s*(?:but\s+)?(?:i\s+)?"
        r"(?:prefer|favor|choose|select|would rather)\s+"
        r"(?P<alternative>[^.!?]+))?",
        text,
        re.I,
    )
    if disagreement:
        return _make_semantic_frame(
            source,
            "disagreement",
            disagreement.group("direction"),
            disagreement.group("alternative") or "",
            minimum_primary=1,
            minimum_secondary=1,
        )

    agreement = re.search(
        r"\b(?:i\s+)?agree\s+(?:with|that|on)\s+"
        r"(?P<direction>[^.!?]+)",
        text,
        re.I,
    )
    if agreement:
        return _make_semantic_frame(
            source,
            "agreement",
            agreement.group("direction"),
            minimum_primary=1,
        )

    comparative_preference = re.search(
        r"\b(?:i\s+)?(?:prefer|favor|choose|select|would rather)\s+"
        r"(?P<preferred>.+?)\s+(?:over|instead of|rather than|than)\s+"
        r"(?P<other>[^.!?]+)",
        text,
        re.I,
    )
    if comparative_preference:
        return _make_semantic_frame(
            source,
            "preference",
            comparative_preference.group("preferred"),
            comparative_preference.group("other"),
            minimum_primary=1,
            minimum_secondary=1,
        )

    rejection = re.search(
        r"\b(?:reject|avoid|drop|do not want|don't want)\s+"
        r"(?P<direction>[^.!?]+)",
        text,
        re.I,
    )
    if rejection:
        return _make_semantic_frame(
            source,
            "rejection",
            rejection.group("direction"),
            minimum_primary=1,
        )

    correction = re.search(
        r"\b(?:actually|correction|corrected)\b\s*[:,;-]?\s*"
        r"(?:(?:choose|select|use|keep|prefer|favor)\s+)?"
        r"(?P<direction>[^.!?]+)",
        text,
        re.I,
    )
    if correction:
        return _make_semantic_frame(
            source,
            "correction",
            correction.group("direction"),
            minimum_primary=1,
        )

    preference = re.search(
        r"\b(?:i\s+)?(?:prefer|favor|choose|chose|select|selected|"
        r"decided on|settled on)\s+(?P<direction>[^.!?]+)",
        text,
        re.I,
    )
    if preference:
        return _make_semantic_frame(
            source,
            "preference",
            preference.group("direction"),
            minimum_primary=1,
        )

    plan = re.search(
        r"\b(?:i\s+|we\s+)?(?:plan(?:ned)?(?:\s+to)?|intend(?:ed)?"
        r"(?:\s+to)?|will|let(?:'|’)s)\s+(?P<direction>[^.!?]+)",
        text,
        re.I,
    )
    if plan:
        return _make_semantic_frame(
            source,
            "plan",
            plan.group("direction"),
        )
    if re.search(r"\b(?:should|could)\b", text, re.I):
        return _make_semantic_frame(source, "proposal", text)

    proposal = re.search(
        r"\b(?:i\s+|we\s+)?(?:propose|suggest|recommend)\s+"
        r"(?:that\s+)?(?P<direction>[^.!?]+)",
        text,
        re.I,
    )
    if proposal:
        return _make_semantic_frame(
            source,
            "proposal",
            proposal.group("direction"),
        )

    if "?" in text:
        return _make_semantic_frame(source, "question", text)
    observation = re.search(
        r"\b(?:i\s+|we\s+)?(?:noticed|observed|found|saw|reported)\s+"
        r"(?P<direction>[^.!?]+)",
        text,
        re.I,
    )
    if observation:
        return _make_semantic_frame(
            source,
            "observation",
            observation.group("direction"),
        )
    if _AMBIGUOUS_UNFRAMED_SEMANTICS_RE.search(text):
        return None
    return _make_semantic_frame(
        source,
        "topic_observation",
        text,
        minimum_primary=2,
    )


_SEMANTIC_FRAME_PRIORITY = {
    "correction_replacement": 90,
    "replacement": 80,
    "correction": 70,
    "conditional_plan": 60,
    "disagreement": 55,
    "rejection": 50,
    "preference": 40,
    "plan": 35,
    "proposal": 30,
    "agreement": 25,
    "question": 20,
    "observation": 10,
    "topic_observation": 5,
}
_POSITIVE_FRAME_TYPES = frozenset(
    {"preference", "plan", "proposal", "agreement"}
)
_NEGATIVE_FRAME_TYPES = frozenset({"disagreement", "rejection"})
_AMBIGUOUS_UNFRAMED_SEMANTICS_RE = re.compile(
    r"\b(?:not|never|unless|instead|rather|prefer|choose|select|"
    r"reject|avoid|drop|disagree|oppose|against|wrong|bad|terrible|"
    r"if|conditional|maybe|should|could|must|will)\b|\?",
    re.I,
)


def _select_contribution_semantic_frame(
    rows: list[SourceEntry],
) -> ContributionSemanticFrame | None:
    parsed: list[tuple[int, ContributionSemanticFrame]] = []
    unframed: list[tuple[int, SourceEntry]] = []
    for index, source in enumerate(rows):
        frame = _parse_contribution_semantic_frame(source)
        if frame is None:
            unframed.append((index, source))
        else:
            parsed.append((index, frame))
    if not parsed:
        return None
    selected_index, selected = max(
        parsed,
        key=lambda item: (
            _SEMANTIC_FRAME_PRIORITY.get(item[1].frame_type, 0),
            item[0],
        ),
    )
    if unframed:
        if selected.frame_type not in {
            "correction",
            "correction_replacement",
            "replacement",
        }:
            return None
        for index, source in unframed:
            if (
                index >= selected_index
                or _AMBIGUOUS_UNFRAMED_SEMANTICS_RE.search(
                    source.normalized_value
                )
            ):
                return None
    selected_terms = set(selected.primary) | set(selected.secondary)
    for index, frame in parsed:
        if index == selected_index:
            continue
        frame_terms = set(frame.primary) | set(frame.secondary)
        if not selected_terms.intersection(frame_terms):
            return None
        if (
            selected.frame_type not in {
                "correction",
                "correction_replacement",
                "replacement",
            }
            and (
                selected.frame_type in _POSITIVE_FRAME_TYPES
                and frame.frame_type in _NEGATIVE_FRAME_TYPES
                or selected.frame_type in _NEGATIVE_FRAME_TYPES
                and frame.frame_type in _POSITIVE_FRAME_TYPES
            )
        ):
            return None
    return selected


def _semantic_concept_text(concepts: tuple[str, ...]) -> str:
    if len(concepts) == 1:
        return concepts[0]
    if len(concepts) == 2:
        return f"{concepts[0]} and {concepts[1]}"
    return f"{concepts[0]}, {concepts[1]}, and {concepts[2]}"


def _render_contribution_semantic_frame(
    frame: ContributionSemanticFrame,
) -> str:
    primary = _semantic_concept_text(frame.primary)
    secondary = (
        _semantic_concept_text(frame.secondary)
        if frame.secondary
        else ""
    )
    if frame.frame_type == "proposal":
        return (
            "The participant proposed a direction centered on "
            f"{primary}."
        )
    if frame.frame_type == "plan":
        return f"The participant described a plan centered on {primary}."
    if frame.frame_type == "preference":
        if secondary:
            return (
                "The participant preferred an option centered on "
                f"{primary} over an alternative centered on {secondary}."
            )
        return (
            "The participant preferred a direction centered on "
            f"{primary}."
        )
    if frame.frame_type in {"replacement", "correction_replacement"}:
        prefix = (
            "The participant corrected the direction by"
            if frame.frame_type == "correction_replacement"
            else "The participant changed the direction by"
        )
        return (
            f"{prefix} rejecting an option centered on {primary} "
            f"and replacing it with one centered on {secondary}."
        )
    if frame.frame_type == "correction":
        return (
            "The participant corrected an earlier direction and favored "
            f"an option centered on {primary}."
        )
    if frame.frame_type == "conditional_plan":
        return (
            "The participant made a conditional plan: if factors around "
            f"{primary} hold, the planned direction centers on {secondary}."
        )
    if frame.frame_type == "agreement":
        return (
            "The participant agreed with a direction centered on "
            f"{primary}."
        )
    if frame.frame_type == "disagreement":
        if secondary:
            return (
                "The participant disagreed with a direction centered on "
                f"{primary} and favored an alternative centered on "
                f"{secondary}."
            )
        return (
            "The participant disagreed with a direction centered on "
            f"{primary}."
        )
    if frame.frame_type == "rejection":
        return (
            "The participant rejected a direction centered on "
            f"{primary}."
        )
    if frame.frame_type == "question":
        return (
            "The participant raised a question centered on "
            f"{primary}."
        )
    if frame.frame_type == "observation":
        return (
            "The participant reported an observation centered on "
            f"{primary}."
        )
    if frame.frame_type == "topic_observation":
        if len(frame.primary) == 1:
            return (
                "The participant discussed a topic centered on "
                f"{frame.primary[0]}."
            )
        if len(frame.primary) == 2:
            return (
                "The participant discussed a topic involving "
                f"{frame.primary[0]}, with {frame.primary[1]} as a "
                "related focus."
            )
        return (
            "The participant discussed a topic involving "
            f"{frame.primary[0]}, with {frame.primary[1]} and "
            f"{frame.primary[2]} as related focuses."
        )
    return ""


def _build_contribution_projection(
    rows: list[SourceEntry],
) -> tuple[str, str]:
    """Build a conservative typed semantic projection, never a source excerpt."""
    if not rows or any(
        _contains_sensitive_moment_source(row.normalized_value, row.predicate_key)
        for row in rows
    ):
        return "", ""
    frame = _select_contribution_semantic_frame(rows)
    if frame is None:
        return "", ""
    # A neutral sentence without an explicit semantic frame is too weak to
    # attribute as durable participant meaning by itself. Two coherent source
    # rows are the minimum corroboration for a generic topic observation;
    # explicit proposals, choices, corrections, and other typed frames remain
    # eligible from their own authoritative source.
    if frame.frame_type == "topic_observation" and len(rows) < 2:
        return "", ""
    gist = _render_contribution_semantic_frame(frame)
    if not _contribution_gist_is_safe(
        gist,
        [row.normalized_value for row in rows],
    ):
        return "", ""
    return frame.frame_type, gist


def _contribution_gist_is_safe(
    gist: str,
    source_texts: list[str],
) -> bool:
    value = re.sub(r"\s+", " ", str(gist or "")).strip()
    if (
        not value
        or len(value) > 320
        or not value.startswith("The participant ")
        or any(mark in value for mark in ('"', "“", "”"))
        or _contains_sensitive_moment_source(value)
        or _EXACT_AUTHORITY_REQUEST_RE.search(value)
        or _contains_meaningful_source_ngram(value, source_texts)
    ):
        return False
    return True


@dataclass(frozen=True)
class AttributionRequest:
    requested: bool = False
    target_mention_key: str = ""
    target_label: str = ""
    topic_text: str = ""
    exact_authority_requested: bool = False


def _parse_attribution_request(text: str) -> AttributionRequest:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    exact = bool(_EXACT_AUTHORITY_REQUEST_RE.search(value))
    for pattern in _ATTRIBUTION_REQUEST_PATTERNS:
        match = pattern.search(value)
        if not match:
            continue
        target = str(match.groupdict().get("target") or "").strip(" \t,;:-")
        topic = str(match.groupdict().get("topic") or "").strip(" \t,;:-?!.")
        mention = _DISCORD_MENTION_RE.fullmatch(target)
        return AttributionRequest(
            requested=True,
            target_mention_key=(
                f"discord_user:{int(mention.group(1))}"
                if mention and int(mention.group(1) or 0) > 0
                else ""
            ),
            target_label="" if mention else target,
            topic_text=topic,
            exact_authority_requested=exact,
        )
    return AttributionRequest(exact_authority_requested=exact)


def _coherent(family: str, signature: tuple[str, ...], window_family: str, window_signature: tuple[str, ...]) -> bool:
    if not signature or family == "low_signal":
        return False
    if family == window_family and family in {
        "music_production",
        "cooking",
        "outdoors",
    }:
        return True
    overlap = set(signature) & set(window_signature)
    if len(overlap) >= 2:
        return True
    denom = max(1, min(len(set(signature)), len(set(window_signature))))
    return (len(overlap) / denom) >= 0.5 and len(overlap) >= 1


def _contribution_topic_coherent(
    signature: tuple[str, ...],
    window_signature: tuple[str, ...],
) -> bool:
    if not signature:
        return True
    overlap = set(signature).intersection(window_signature)
    return bool(
        len(overlap) >= 2
        or len(set(signature)) == 1
        and len(overlap) == 1
    )


def _json_sig(sig: tuple[str, ...]) -> str:
    return json.dumps(list(sig), sort_keys=True)


def _load_sig(raw: str) -> tuple[str, ...]:
    try:
        value = json.loads(raw or "[]")
        return tuple(str(v) for v in value if str(v))
    except Exception:
        return ()


def ensure_moment_schema(conn: sqlite3.Connection) -> None:
    ensure_memory_ledger_schema(conn)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_windows (
      moment_id TEXT PRIMARY KEY, guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL, channel_name TEXT,
      channel_policy TEXT, route_mode TEXT, topic_key TEXT NOT NULL, window_started_at TEXT NOT NULL,
      last_activity_at TEXT NOT NULL, finalized_at TEXT, qualification_type TEXT, qualification_reason TEXT,
      lifecycle_status TEXT NOT NULL, visibility TEXT, public_usable INTEGER DEFAULT 0, salience REAL DEFAULT 0,
      human_entry_count INTEGER DEFAULT 0, model_entry_count INTEGER DEFAULT 0, participant_count INTEGER DEFAULT 0,
      summary TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    for sql in (
        "ALTER TABLE memory_moment_windows ADD COLUMN topic_family TEXT DEFAULT ''",
        "ALTER TABLE memory_moment_windows ADD COLUMN topic_signature TEXT DEFAULT '[]'",
        "ALTER TABLE memory_moment_windows ADD COLUMN canonical_ledger_entry_id TEXT DEFAULT ''",
    ):
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_members (
      moment_id TEXT NOT NULL, ledger_entry_id TEXT NOT NULL, source_sequence INTEGER DEFAULT 0, observed_at TEXT,
      membership_role TEXT, created_at TEXT NOT NULL, PRIMARY KEY(moment_id, ledger_entry_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_participants (
      moment_id TEXT NOT NULL, participant_key TEXT NOT NULL, safe_display_name TEXT, participant_role TEXT,
      first_seen_at TEXT, last_seen_at TEXT, authored_entry_count INTEGER DEFAULT 0, participation_order INTEGER DEFAULT 0,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(moment_id, participant_key, participant_role))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_contributions (
      moment_id TEXT NOT NULL, participant_key TEXT NOT NULL, contribution_gist TEXT DEFAULT '',
      frame_type TEXT NOT NULL, source_digest TEXT NOT NULL,
      source_count INTEGER DEFAULT 0, gist_version TEXT NOT NULL,
      lifecycle_status TEXT NOT NULL, public_usable INTEGER DEFAULT 0,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      PRIMARY KEY(moment_id, participant_key))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_contribution_sources (
      moment_id TEXT NOT NULL, participant_key TEXT NOT NULL, ledger_entry_id TEXT NOT NULL,
      gist_version TEXT NOT NULL, created_at TEXT NOT NULL,
      PRIMARY KEY(moment_id, participant_key, ledger_entry_id))""")
    try:
        cur.execute(
            "ALTER TABLE memory_moment_contribution_sources "
            "ADD COLUMN gist_version TEXT DEFAULT ''"
        )
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute(
            "ALTER TABLE memory_moment_contributions "
            "ADD COLUMN frame_type TEXT DEFAULT ''"
        )
    except sqlite3.OperationalError:
        pass
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_diagnostics (
      id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER DEFAULT 0, moment_id TEXT DEFAULT '', event_type TEXT NOT NULL,
      reason_code TEXT DEFAULT '', ledger_entry_id TEXT DEFAULT '', created_at TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_migrations (
      migration_key TEXT PRIMARY KEY, applied_at TEXT NOT NULL)""")
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_mmw_scope ON memory_moment_windows(guild_id, channel_id, lifecycle_status, last_activity_at)",
        "CREATE INDEX IF NOT EXISTS idx_mmw_canonical ON memory_moment_windows(guild_id, canonical_ledger_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmm_entry ON memory_moment_members(ledger_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmp_participant ON memory_moment_participants(participant_key, moment_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmc_participant ON memory_moment_contributions(participant_key, moment_id, lifecycle_status)",
        "CREATE INDEX IF NOT EXISTS idx_mmcs_entry ON memory_moment_contribution_sources(ledger_entry_id, moment_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmd_guild ON memory_moment_diagnostics(guild_id, event_type, reason_code)",
    ]:
        cur.execute(sql)
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_moment_contribution_source_delete
        AFTER DELETE ON memory_ledger_entries
        BEGIN
          UPDATE memory_moment_contributions
          SET contribution_gist='', public_usable=0,
              lifecycle_status='retracted', updated_at=CURRENT_TIMESTAMP
          WHERE (moment_id,participant_key) IN (
            SELECT moment_id,participant_key
            FROM memory_moment_contribution_sources
            WHERE ledger_entry_id=OLD.entry_id
          );
        END"""
    )
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_moment_contribution_source_lifecycle
        AFTER UPDATE OF lifecycle_status,normalized_value,public_usable
        ON memory_ledger_entries
        WHEN NEW.lifecycle_status NOT IN ('active','review_only')
          OR NEW.normalized_value IS NOT OLD.normalized_value
          OR NEW.public_usable IS NOT OLD.public_usable
        BEGIN
          UPDATE memory_moment_contributions
          SET contribution_gist='', public_usable=0,
              lifecycle_status='needs_review', updated_at=CURRENT_TIMESTAMP
          WHERE (moment_id,participant_key) IN (
            SELECT moment_id,participant_key
            FROM memory_moment_contribution_sources
            WHERE ledger_entry_id=NEW.entry_id
          );
        END"""
    )
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_moment_contribution_window_lifecycle
        AFTER UPDATE OF lifecycle_status ON memory_moment_windows
        WHEN NEW.lifecycle_status NOT IN ('open','finalized')
        BEGIN
          UPDATE memory_moment_contributions
          SET contribution_gist='', public_usable=0,
              lifecycle_status=NEW.lifecycle_status,
              updated_at=CURRENT_TIMESTAMP
          WHERE moment_id=NEW.moment_id;
        END"""
    )
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_moment_contribution_participant_delete
        AFTER DELETE ON memory_moment_participants
        BEGIN
          DELETE FROM memory_moment_contribution_sources
          WHERE moment_id=OLD.moment_id
            AND participant_key=OLD.participant_key;
          DELETE FROM memory_moment_contributions
          WHERE moment_id=OLD.moment_id
            AND participant_key=OLD.participant_key;
        END"""
    )
    if not cur.execute(
        "SELECT 1 FROM memory_moment_migrations WHERE migration_key=?",
        (REMEMBERED_NUMBER_QUARANTINE_MIGRATION,),
    ).fetchone():
        quarantine_legacy_remembered_number_artifacts(conn)
        cur.execute(
            "INSERT OR IGNORE INTO memory_moment_migrations VALUES(?,?)",
            (REMEMBERED_NUMBER_QUARANTINE_MIGRATION, _now()),
        )
    if not cur.execute(
        "SELECT 1 FROM memory_moment_migrations WHERE migration_key=?",
        (SAFE_MOMENT_PROJECTION_MIGRATION,),
    ).fetchone():
        quarantine_legacy_unsafe_moment_projections(conn)
        cur.execute(
            "INSERT OR IGNORE INTO memory_moment_migrations VALUES(?,?)",
            (SAFE_MOMENT_PROJECTION_MIGRATION, _now()),
        )
    if not cur.execute(
        "SELECT 1 FROM memory_moment_migrations WHERE migration_key=?",
        (MOMENT_CONTRIBUTION_BACKFILL_MIGRATION,),
    ).fetchone():
        backfill_safe_moment_contributions(conn)
        cur.execute(
            "INSERT OR IGNORE INTO memory_moment_migrations VALUES(?,?)",
            (MOMENT_CONTRIBUTION_BACKFILL_MIGRATION, _now()),
        )


def quarantine_legacy_remembered_number_artifacts(
    conn: sqlite3.Connection,
) -> dict[str, int]:
    """Scrub obsolete durable number recall while preserving audit lineage.

    Legacy releases could turn an immediate "remember this number" exchange
    into a durable Moment and a live fact. The raw conversation owner remains
    intact, but every derived artifact is made unusable. Queries deliberately
    include already-quarantined sources so an interrupted first pass can finish
    cleaning linked artifacts on a later, idempotent pass.
    """
    counts = {
        "ledger_entries": 0,
        "moments": 0,
        "canonical_entries": 0,
        "live_facts": 0,
    }
    remembered_entry_ids = {
        str(row[0])
        for row in conn.execute(
            """
            SELECT entry_id
            FROM memory_ledger_entries
            WHERE predicate_key='remembered_number'
            """
        ).fetchall()
    }
    opaque_conversation_ids = {
        str(entry_id)
        for entry_id, predicate_key, normalized_value in conn.execute(
            """
            SELECT entry_id,predicate_key,normalized_value
            FROM memory_ledger_entries
            WHERE source_table='conversations'
            """
        ).fetchall()
        if _is_opaque_remember_number_request(
            str(normalized_value or ""),
            str(predicate_key or ""),
        )
    }
    affected_source_ids = remembered_entry_ids | opaque_conversation_ids
    moment_ids: set[str] = set()
    if affected_source_ids:
        placeholders = ",".join("?" for _ in affected_source_ids)
        moment_ids.update(
            str(row[0])
            for row in conn.execute(
                f"""
                SELECT DISTINCT moment_id
                FROM memory_moment_members
                WHERE ledger_entry_id IN ({placeholders})
                """,
                tuple(sorted(affected_source_ids)),
            ).fetchall()
        )
    moment_ids.update(
        str(row[0])
        for row in conn.execute(
            """
            SELECT moment_id
            FROM memory_moment_windows
            WHERE LOWER(COALESCE(summary,'')) LIKE '%remembered_number%'
               OR LOWER(COALESCE(summary,'')) LIKE '%remembered number%'
            """
        ).fetchall()
    )
    legacy_canonical_rows = conn.execute(
        """
        SELECT entry_id,source_row_id
        FROM memory_ledger_entries
        WHERE source_table='memory_moment_windows'
          AND entry_type='shared_moment'
          AND (
            LOWER(COALESCE(normalized_value,'')) LIKE '%remembered_number%'
            OR LOWER(COALESCE(normalized_value,'')) LIKE '%remembered number%'
          )
        """
    ).fetchall()
    legacy_canonical_ids = {str(row[0]) for row in legacy_canonical_rows}
    moment_ids.update(str(row[1]) for row in legacy_canonical_rows)

    if remembered_entry_ids:
        placeholders = ",".join("?" for _ in remembered_entry_ids)
        cursor = conn.execute(
            f"""
            UPDATE memory_ledger_entries
            SET normalized_value='', public_usable=0,
                lifecycle_status='quarantined', updated_at=?
            WHERE entry_id IN ({placeholders})
              AND (
                lifecycle_status!='quarantined'
                OR COALESCE(normalized_value,'')!=''
                OR public_usable!=0
              )
            """,
            (_now(), *sorted(remembered_entry_ids)),
        )
        counts["ledger_entries"] = max(0, int(cursor.rowcount or 0))
    canonical_ids = set(legacy_canonical_ids)
    if moment_ids:
        moment_placeholders = ",".join("?" for _ in moment_ids)
        canonical_ids.update(
            str(row[0])
            for row in conn.execute(
                f"""
                SELECT canonical_ledger_entry_id
                FROM memory_moment_windows
                WHERE moment_id IN ({moment_placeholders})
                  AND COALESCE(canonical_ledger_entry_id,'')!=''
                """,
                tuple(sorted(moment_ids)),
            ).fetchall()
            if str(row[0] or "")
        )
        cursor = conn.execute(
            f"""
            UPDATE memory_moment_windows
            SET summary='', public_usable=0, lifecycle_status='retracted',
                updated_at=?
            WHERE moment_id IN ({moment_placeholders})
              AND (
                COALESCE(summary,'')!=''
                OR public_usable!=0
                OR lifecycle_status!='retracted'
              )
            """,
            (_now(), *sorted(moment_ids)),
        )
        counts["moments"] = max(0, int(cursor.rowcount or 0))
    if canonical_ids:
        canonical_placeholders = ",".join("?" for _ in canonical_ids)
        cursor = conn.execute(
            f"""
            UPDATE memory_ledger_entries
            SET normalized_value='', public_usable=0,
                lifecycle_status='quarantined', updated_at=?
            WHERE entry_id IN ({canonical_placeholders})
              AND (
                lifecycle_status!='quarantined'
                OR COALESCE(normalized_value,'')!=''
                OR public_usable!=0
              )
            """,
            (_now(), *sorted(canonical_ids)),
        )
        counts["canonical_entries"] = max(0, int(cursor.rowcount or 0))
    fact_table = conn.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type='table' AND name='user_memory_facts'
        """
    ).fetchone()
    if fact_table:
        columns = {
            str(row[1])
            for row in conn.execute("PRAGMA table_info(user_memory_facts)").fetchall()
        }
        if {"fact_key", "fact_value"} <= columns:
            assignments = ["fact_value=''"]
            conditions = ["COALESCE(fact_value,'')!=''"]
            if "lifecycle_status" in columns:
                assignments.append("lifecycle_status='quarantined'")
                conditions.append("COALESCE(lifecycle_status,'')!='quarantined'")
            if "updated_at" in columns:
                assignments.append("updated_at=?")
                params: tuple[Any, ...] = (_now(),)
            else:
                params = ()
            cursor = conn.execute(
                f"""
                UPDATE user_memory_facts
                SET {", ".join(assignments)}
                WHERE fact_key='remembered_number'
                  AND ({" OR ".join(conditions)})
                """,
                params,
            )
            counts["live_facts"] = max(0, int(cursor.rowcount or 0))
    return counts


def _topic_signature_is_non_extractive(raw: str) -> bool:
    try:
        values = json.loads(str(raw or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return bool(
        isinstance(values, list)
        and all(re.fullmatch(r"tok_[0-9a-f]{16}", str(value or "")) for value in values)
    )


def quarantine_legacy_unsafe_moment_projections(
    conn: sqlite3.Connection,
) -> dict[str, int]:
    """Retract old derived Moment copies that retained source wording."""
    unsafe: set[str] = set()
    for moment_id, summary, topic_family, topic_signature in conn.execute(
        """
        SELECT moment_id,summary,topic_family,topic_signature
        FROM memory_moment_windows
        """
    ).fetchall():
        family = str(topic_family or "")
        if (
            (summary and not _is_safe_gist_summary(str(summary)))
            or family
            not in {"", "low_signal", "music_production", "cooking", "outdoors", "topic_other"}
            or not _topic_signature_is_non_extractive(str(topic_signature or "[]"))
        ):
            unsafe.add(str(moment_id))
    counts = {"moments": 0, "canonical_entries": 0, "participant_names": 0}
    now = _now()
    for moment_id in sorted(unsafe):
        canonical = conn.execute(
            "SELECT canonical_ledger_entry_id FROM memory_moment_windows WHERE moment_id=?",
            (moment_id,),
        ).fetchone()
        counts["moments"] += conn.execute(
            """
            UPDATE memory_moment_windows
            SET summary='',topic_family='',topic_signature='[]',
                public_usable=0,lifecycle_status='retracted',
                qualification_reason='legacy_extractive_projection',
                updated_at=?
            WHERE moment_id=?
            """,
            (now, moment_id),
        ).rowcount
        if canonical and canonical[0]:
            counts["canonical_entries"] += conn.execute(
                """
                UPDATE memory_ledger_entries
                SET normalized_value='',public_usable=0,
                    lifecycle_status='quarantined',updated_at=?
                WHERE entry_id=?
                """,
                (now, str(canonical[0])),
            ).rowcount
    for moment_id, participant_key, participant_role, safe_name in conn.execute(
        """
        SELECT moment_id,participant_key,participant_role,safe_display_name
        FROM memory_moment_participants
        """
    ).fetchall():
        cleaned = _safe_participant_display_name(str(safe_name or ""))
        if cleaned != str(safe_name or ""):
            counts["participant_names"] += conn.execute(
                """
                UPDATE memory_moment_participants
                SET safe_display_name=?,updated_at=?
                WHERE moment_id=? AND participant_key=? AND participant_role=?
                """,
                (
                    cleaned,
                    now,
                    str(moment_id),
                    str(participant_key),
                    str(participant_role),
                ),
            ).rowcount
    return counts


def _replace_moment_contributions(
    conn: sqlite3.Connection,
    moment_id: str,
    rows: list[SourceEntry],
    *,
    public_usable: bool,
) -> int:
    human_rows: dict[str, list[SourceEntry]] = {}
    for row in rows:
        if row.is_human and _meaningful(
            row.normalized_value,
            row.source_role,
            row.predicate_key,
        ):
            human_rows.setdefault(row.subject_key, []).append(row)
    conn.execute(
        "DELETE FROM memory_moment_contribution_sources WHERE moment_id=?",
        (moment_id,),
    )
    conn.execute(
        "DELETE FROM memory_moment_contributions WHERE moment_id=?",
        (moment_id,),
    )
    inserted = 0
    now = _now()
    for participant_key, source_rows in sorted(human_rows.items()):
        frame_type, gist = _build_contribution_projection(source_rows)
        participant_public = bool(
            public_usable
            and source_rows
            and all(
                row.public_usable
                and row.visibility in {"public", "public_safe"}
                for row in source_rows
            )
        )
        if not gist or not participant_public:
            continue
        conn.execute(
            """
            INSERT OR REPLACE INTO memory_moment_contributions(
              moment_id,participant_key,contribution_gist,frame_type,source_digest,
              source_count,gist_version,lifecycle_status,public_usable,
              created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                moment_id,
                participant_key,
                gist,
                frame_type,
                _source_digest(source_rows),
                len(source_rows),
                CONTRIBUTION_GIST_VERSION,
                "review_only",
                1,
                now,
                now,
            ),
        )
        for source in source_rows:
            conn.execute(
                """
                INSERT OR IGNORE INTO memory_moment_contribution_sources(
                  moment_id,participant_key,ledger_entry_id,gist_version,created_at
                ) VALUES(?,?,?,?,?)
                """,
                (
                    moment_id,
                    participant_key,
                    source.entry_id,
                    CONTRIBUTION_GIST_VERSION,
                    now,
                ),
            )
        inserted += 1
    return inserted


def backfill_safe_moment_contributions(conn: sqlite3.Connection) -> dict[str, int]:
    """Prepare contribution gists for already-safe finalized shadow Moments."""
    counts = {"moments_considered": 0, "contributions_inserted": 0}
    for (moment_id,) in conn.execute(
        """
        SELECT moment_id FROM memory_moment_windows
        WHERE lifecycle_status='finalized'
        ORDER BY window_started_at,moment_id
        """
    ).fetchall():
        counts["moments_considered"] += 1
        _recount(conn, str(moment_id))
        win = conn.execute(
            """
            SELECT guild_id,channel_id,channel_policy,route_mode,visibility,
                   public_usable,summary
            FROM memory_moment_windows WHERE moment_id=?
            """,
            (moment_id,),
        ).fetchone()
        if not win or not bool(win[5]) or not _is_safe_gist_summary(str(win[6] or "")):
            continue
        rows = _entries(conn, str(moment_id))
        failure, _failure_lifecycle = _moment_source_failure(
            conn,
            moment_id=str(moment_id),
            rows=rows,
            guild_id=int(win[0] or 0),
            channel_id=int(win[1] or 0),
            channel_policy=str(win[2] or ""),
            route_mode=str(win[3] or "unknown"),
            visibility=str(win[4] or "unknown"),
            public_usable=True,
        )
        if failure:
            continue
        counts["contributions_inserted"] += _replace_moment_contributions(
            conn,
            str(moment_id),
            rows,
            public_usable=True,
        )
    return counts


def _diag(conn: sqlite3.Connection, guild_id: int, event: str, reason: str = "", moment_id: str = "", entry_id: str = "") -> None:
    conn.execute(
        "INSERT INTO memory_moment_diagnostics(guild_id,moment_id,event_type,reason_code,ledger_entry_id,created_at) VALUES(?,?,?,?,?,?)",
        (int(guild_id or 0), moment_id or "", event[:80], reason[:120], entry_id or "", _now()),
    )


def _fetch_entry(conn: sqlite3.Connection, entry_id: str) -> SourceEntry | None:
    row = conn.execute(
        """
        SELECT entry_id,guild_id,source_table,source_role,entry_type,predicate_key,normalized_value,route_mode,
               channel_id,channel_name,channel_policy,visibility,public_usable,observed_at,source_sequence,
               lifecycle_status,subject_key,subject_display_name
        FROM memory_ledger_entries WHERE entry_id=?
        """,
        (entry_id,),
    ).fetchone()
    if not row:
        return None
    return SourceEntry(
        entry_id=row[0], guild_id=int(row[1] or 0), source_table=row[2] or "", source_role=row[3] or "",
        entry_type=row[4] or "", predicate_key=row[5] or "", normalized_value=row[6] or "", route_mode=row[7] or "unknown",
        channel_id=int(row[8] or 0), channel_name=row[9] or "", channel_policy=row[10] or "unknown", visibility=row[11] or "unknown",
        public_usable=bool(row[12]), observed_at=row[13] or _now(), source_sequence=int(row[14] or 0), lifecycle_status=row[15] or "",
        subject_key=row[16] or "", subject_display_name=row[17] or "",
    )


def _entries(conn: sqlite3.Connection, moment_id: str) -> list[SourceEntry]:
    rows = conn.execute(
        """
        SELECT e.entry_id,e.guild_id,e.source_table,e.source_role,e.entry_type,e.predicate_key,e.normalized_value,e.route_mode,
               e.channel_id,e.channel_name,e.channel_policy,e.visibility,e.public_usable,e.observed_at,e.source_sequence,
               e.lifecycle_status,e.subject_key,e.subject_display_name
        FROM memory_moment_members m JOIN memory_ledger_entries e ON e.entry_id=m.ledger_entry_id
        WHERE m.moment_id=? ORDER BY e.observed_at, e.source_sequence, e.entry_id
        """,
        (moment_id,),
    ).fetchall()
    return [SourceEntry(r[0], int(r[1] or 0), r[2] or "", r[3] or "", r[4] or "", r[5] or "", r[6] or "", r[7] or "unknown", int(r[8] or 0), r[9] or "", r[10] or "unknown", r[11] or "unknown", bool(r[12]), r[13] or _now(), int(r[14] or 0), r[15] or "", r[16] or "", r[17] or "") for r in rows]


def _mark_targets_for_correction(conn: sqlite3.Connection, source: SourceEntry) -> int:
    targets = [r[0] for r in conn.execute(
        "SELECT target_entry_id FROM memory_ledger_lineage WHERE guild_id=? AND entry_id=? AND lineage_type IN ('correction_of','supersedes','retracts')",
        (source.guild_id, source.entry_id),
    ).fetchall()]
    count = 0
    for target in targets:
        count += handle_source_correction(conn, target, guild_id=source.guild_id)
    return count


def observe_ledger_entry(conn: sqlite3.Connection, ledger_entry_id: str) -> MomentObservationResult:
    if not shadow_enabled():
        return MomentObservationResult(reason_code="moment_gate_disabled", ledger_entry_id=ledger_entry_id)
    if not ledger_shadow_enabled():
        ensure_moment_schema(conn)
        _diag(conn, 0, "moment_processing_skipped", "ledger_shadow_unavailable", entry_id=ledger_entry_id)
        return MomentObservationResult(reason_code="ledger_shadow_unavailable", ledger_entry_id=ledger_entry_id)
    ensure_moment_schema(conn)
    try:
        conn.execute("SAVEPOINT moment_observe")
        existing = conn.execute("SELECT moment_id FROM memory_moment_members WHERE ledger_entry_id=? ORDER BY created_at LIMIT 1", (ledger_entry_id,)).fetchone()
        if existing:
            conn.execute("RELEASE moment_observe")
            return MomentObservationResult("deduplicated", "exact_source_duplicate", existing[0], ledger_entry_id)
        source = _fetch_entry(conn, ledger_entry_id)
        if not source:
            conn.execute("RELEASE moment_observe")
            return MomentObservationResult(reason_code="missing_ledger_entry", ledger_entry_id=ledger_entry_id)
        # Corrections and scalar supersessions must invalidate any Moment that
        # depended on the prior source even when the new ledger entry is not
        # itself eligible to become a Moment member.
        _mark_targets_for_correction(conn, source)
        if source.source_table != "conversations" or source.lifecycle_status not in {"active", "review_only"} or source.entry_type not in {"observation", "derived_summary"}:
            conn.execute("RELEASE moment_observe")
            return MomentObservationResult(reason_code="ineligible_source", ledger_entry_id=source.entry_id)
        if _contains_sensitive_moment_source(
            source.normalized_value,
            source.predicate_key,
        ):
            reason = (
                "non_durable_immediate_recall"
                if _is_opaque_remember_number_request(
                    source.normalized_value,
                    source.predicate_key,
                )
                else "sensitive_source_excluded"
            )
            _diag(
                conn,
                source.guild_id,
                "moment_processing_skipped",
                reason,
                entry_id=source.entry_id,
            )
            conn.execute("RELEASE moment_observe")
            return MomentObservationResult(
                reason_code=reason,
                ledger_entry_id=source.entry_id,
            )
        _diag(conn, source.guild_id, "eligible_ledger_entry_observed", "ok", entry_id=source.entry_id)
        family = _topic_family(source.normalized_value, source.predicate_key)
        signature = _topic_signature(source.normalized_value, source.predicate_key)
        meaningful = _meaningful(source.normalized_value, source.source_role, source.predicate_key)
        ts = _parse_ts(source.observed_at)
        chosen = ""
        open_rows = conn.execute(
            """
            SELECT moment_id,window_started_at,last_activity_at,channel_policy,visibility,topic_family,topic_signature
            FROM memory_moment_windows
            WHERE guild_id=? AND channel_id=? AND lifecycle_status='open'
            ORDER BY last_activity_at DESC, moment_id
            """,
            (source.guild_id, source.channel_id),
        ).fetchall()
        for row in open_rows:
            mid, started, last, policy, visibility, win_family, win_sig_raw = row
            expired = (ts - _parse_ts(last)).total_seconds() > INACTIVITY_SECONDS or (ts - _parse_ts(started)).total_seconds() > MAX_WINDOW_SECONDS
            incompatible = policy != source.channel_policy or visibility != source.visibility
            if expired or incompatible:
                finalize_moment(conn, mid)
                continue
            if source.is_model or not meaningful:
                if not chosen:
                    chosen = mid
                continue
            if _coherent(family, signature, win_family or "", _load_sig(win_sig_raw)):
                chosen = mid
                break
            finalize_moment(conn, mid)
            _diag(conn, source.guild_id, "window_split", "topic_coherence_mismatch", mid, source.entry_id)
        if not chosen:
            topic_key = _topic_key(family, signature)
            started = source.observed_at or _now()
            chosen = stable_moment_id(source.guild_id, source.channel_id, topic_key, started)
            conn.execute(
                """
                INSERT OR IGNORE INTO memory_moment_windows(
                    moment_id,guild_id,channel_id,channel_name,channel_policy,route_mode,topic_key,topic_family,topic_signature,
                    window_started_at,last_activity_at,lifecycle_status,visibility,public_usable,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (chosen, source.guild_id, source.channel_id, source.channel_name, source.channel_policy, source.route_mode, topic_key, family, _json_sig(signature), started, started, "open", source.visibility, int(source.public_usable), _now(), _now()),
            )
            _diag(conn, source.guild_id, "window_opened", "ok", chosen, source.entry_id)
        _insert_membership(conn, chosen, source, meaningful, family, signature)
        conn.execute("RELEASE moment_observe")
        return MomentObservationResult("observed", "ok", chosen, source.entry_id)
    except Exception:
        try:
            conn.execute("ROLLBACK TO moment_observe")
            conn.execute("RELEASE moment_observe")
        except Exception:
            pass
        try:
            _diag(conn, 0, "moment_processing_error", "exception", entry_id=ledger_entry_id)
        except Exception:
            pass
        return MomentObservationResult("error", "exception", ledger_entry_id=ledger_entry_id)


def _insert_membership(conn: sqlite3.Connection, moment_id: str, source: SourceEntry, meaningful: bool, family: str, signature: tuple[str, ...]) -> None:
    role_name = "human_author" if source.is_human else "bnl_participant"
    before = conn.execute("SELECT COUNT(*) FROM memory_moment_members WHERE moment_id=? AND ledger_entry_id=?", (moment_id, source.entry_id)).fetchone()[0]
    conn.execute("INSERT OR IGNORE INTO memory_moment_members VALUES(?,?,?,?,?,?)", (moment_id, source.entry_id, source.source_sequence, source.observed_at, role_name, _now()))
    if before:
        _diag(conn, source.guild_id, "exact_source_duplicate_ignored", "duplicate", moment_id, source.entry_id)
        return
    pkey = source.subject_key if source.is_human else BNL_SUBJECT_KEY
    pname = (
        _safe_participant_display_name(source.subject_display_name)
        if source.is_human
        else "BNL-01"
    )
    existing = conn.execute("SELECT participation_order FROM memory_moment_participants WHERE moment_id=? AND participant_key=? AND participant_role=?", (moment_id, pkey, role_name)).fetchone()
    if existing:
        conn.execute(
            "UPDATE memory_moment_participants SET last_seen_at=?, authored_entry_count=authored_entry_count+?, updated_at=? WHERE moment_id=? AND participant_key=? AND participant_role=?",
            (source.observed_at, 1 if source.is_human else 0, _now(), moment_id, pkey, role_name),
        )
    else:
        order = conn.execute("SELECT COUNT(*) FROM memory_moment_participants WHERE moment_id=?", (moment_id,)).fetchone()[0]
        conn.execute("INSERT INTO memory_moment_participants VALUES(?,?,?,?,?,?,?,?,?,?)", (moment_id, pkey, pname, role_name, source.observed_at, source.observed_at, 1 if source.is_human else 0, order, _now(), _now()))
    if source.is_human and meaningful:
        old = _load_sig(conn.execute("SELECT topic_signature FROM memory_moment_windows WHERE moment_id=?", (moment_id,)).fetchone()[0])
        merged = tuple(sorted(set(old) | set(signature)))[:24]
        conn.execute("UPDATE memory_moment_windows SET topic_family=?, topic_signature=? WHERE moment_id=?", (family, _json_sig(merged), moment_id))
    _recount(conn, moment_id)
    _diag(conn, source.guild_id, "window_extended", "ok", moment_id, source.entry_id)


def _recount(conn: sqlite3.Connection, moment_id: str) -> None:
    rows = _entries(conn, moment_id)
    humans = [r for r in rows if _meaningful(r.normalized_value, r.source_role, r.predicate_key)]
    models = [r for r in rows if r.is_model]
    parts = len({r.subject_key for r in humans})
    visibility = max([r.visibility for r in rows] or ["unknown"], key=lambda v: VIS_RANK.get(v, 5))
    # BNL turns prove conversational structure but are derived model output and
    # therefore never become public content authority. Public eligibility is
    # based only on eligible human contributions while every row must still
    # share a public visibility boundary.
    public_usable = (
        bool(humans)
        and all(r.public_usable for r in humans)
        and all(
            r.visibility in {"public", "public_safe"}
            for r in rows
        )
        and VIS_RANK.get(visibility, 5) <= VIS_RANK.get("public_safe", 0)
    )
    conn.execute(
        """
        UPDATE memory_moment_windows
        SET human_entry_count=?, model_entry_count=?, participant_count=?, visibility=?, public_usable=?,
            last_activity_at=COALESCE((SELECT MAX(observed_at) FROM memory_moment_members WHERE moment_id=?), last_activity_at), updated_at=?
        WHERE moment_id=?
        """,
        (len(humans), len(models), parts, visibility, int(public_usable), moment_id, _now(), moment_id),
    )


def _qualify(rows: list[SourceEntry]) -> tuple[str, str, list[SourceEntry], list[SourceEntry]]:
    humans = [r for r in rows if _meaningful(r.normalized_value, r.source_role, r.predicate_key)]
    models = [r for r in rows if r.is_model]
    human_parts = {r.subject_key for r in humans}
    strong = any(_strong_marker(r.normalized_value, r.predicate_key) for r in humans)
    if len(human_parts) >= 2 and len(humans) >= 3:
        return "shared_activity", "two_humans_three_meaningful_entries", humans, models
    if len(human_parts) == 1 and models and ((len(humans) >= 2 and strong) or len(humans) >= 3):
        return "conversational", "one_human_bnl_continuity", humans, models
    return "", "low_signal_or_insufficient_continuity", humans, models


def _summary(rows: list[SourceEntry], qtype: str, reason: str) -> str:
    del reason
    families = {
        _topic_family(row.normalized_value, row.predicate_key)
        for row in rows
        if row.is_human
    }
    known_families = sorted(family for family in families if family != "low_signal")
    family = known_families[0] if len(known_families) == 1 else "topic_other"
    topic_label = TOPIC_GIST_LABELS.get(family, TOPIC_GIST_LABELS["topic_other"])
    if qtype == "shared_activity":
        return (
            "Derived moment gist (shared public activity): members developed "
            f"a shared {topic_label} discussion."
        )
    return (
        "Derived moment gist (member and BNL continuity): "
        f"a {topic_label} discussion developed across several turns."
    )


def _is_safe_gist_summary(summary: str) -> bool:
    value = str(summary or "")
    allowed = set()
    for topic_label in TOPIC_GIST_LABELS.values():
        allowed.add(
            "Derived moment gist (shared public activity): members developed "
            f"a shared {topic_label} discussion."
        )
        allowed.add(
            "Derived moment gist (member and BNL continuity): "
            f"a {topic_label} discussion developed across several turns."
        )
    return value in allowed


def _existing_moment_entry_id(conn: sqlite3.Connection, moment_id: str, guild_id: int | None = None) -> str:
    row = conn.execute("SELECT canonical_ledger_entry_id FROM memory_moment_windows WHERE moment_id=?", (moment_id,)).fetchone()
    if row and row[0] and conn.execute("SELECT 1 FROM memory_ledger_entries WHERE entry_id=?", (row[0],)).fetchone():
        return row[0]
    params: list[Any] = [str(moment_id)]
    where = "source_table='memory_moment_windows' AND source_row_id=? AND entry_type='shared_moment'"
    if guild_id is not None:
        where += " AND guild_id=?"
        params.append(guild_id)
    row = conn.execute(f"SELECT entry_id FROM memory_ledger_entries WHERE {where} ORDER BY created_at LIMIT 1", params).fetchone()
    return row[0] if row else ""


def _moment_source_failure(
    conn: sqlite3.Connection,
    *,
    moment_id: str,
    rows: list[SourceEntry],
    guild_id: int,
    channel_id: int,
    channel_policy: str,
    route_mode: str,
    visibility: str,
    public_usable: bool,
) -> tuple[str, str]:
    member_count = int(
        conn.execute(
            "SELECT COUNT(*) FROM memory_moment_members WHERE moment_id=?",
            (moment_id,),
        ).fetchone()[0]
        or 0
    )
    if member_count != len(rows):
        return "dangling_source", "needs_review"
    for source in rows:
        if _contains_sensitive_moment_source(
            source.normalized_value,
            source.predicate_key,
        ):
            return "sensitive_source_excluded", "retracted"
        if source.lifecycle_status not in SOURCE_LIFECYCLES_USABLE_FOR_MOMENTS:
            return "source_lifecycle_not_usable", "needs_review"
        if source.source_table != "conversations" or source.entry_type not in {
            "observation",
            "derived_summary",
        }:
            return "source_owner_not_usable", "needs_review"
        if (
            source.guild_id != guild_id
            or source.channel_id != channel_id
            or source.channel_policy != channel_policy
            or source.route_mode != route_mode
            or source.visibility != visibility
        ):
            return "source_scope_mismatch", "needs_review"
        if public_usable and source.is_human and not source.public_usable:
            return "source_public_contract_mismatch", "needs_review"
    return "", ""


def finalize_moment(conn: sqlite3.Connection, moment_id: str) -> MomentObservationResult:
    ensure_moment_schema(conn)
    win = conn.execute(
        "SELECT guild_id,channel_id,channel_name,channel_policy,route_mode,topic_key,window_started_at,last_activity_at,visibility,public_usable,lifecycle_status,canonical_ledger_entry_id FROM memory_moment_windows WHERE moment_id=?",
        (moment_id,),
    ).fetchone()
    if not win:
        return MomentObservationResult(reason_code="missing_window", moment_id=moment_id)
    existing = _existing_moment_entry_id(conn, moment_id, int(win[0] or 0))
    lifecycle = win[10] or ""
    if lifecycle == "finalized":
        return MomentObservationResult("deduplicated", "already_finalized", moment_id, existing)
    if lifecycle in {"needs_review", "rejected", "superseded", "retracted", "expired"}:
        return MomentObservationResult("deduplicated", f"terminal_{lifecycle}", moment_id, existing)
    if lifecycle != "open":
        return MomentObservationResult("skipped", f"not_open_{lifecycle or 'unknown'}", moment_id, existing)
    rows = _entries(conn, moment_id)
    source_failure, failure_lifecycle = _moment_source_failure(
        conn,
        moment_id=moment_id,
        rows=rows,
        guild_id=int(win[0] or 0),
        channel_id=int(win[1] or 0),
        channel_policy=win[3] or "",
        route_mode=win[4] or "unknown",
        visibility=win[8] or "unknown",
        public_usable=bool(win[9]),
    )
    if source_failure:
        conn.execute(
            """
            UPDATE memory_moment_windows
            SET lifecycle_status=?, qualification_reason=?, summary='',
                public_usable=0, updated_at=?
            WHERE moment_id=?
            """,
            (failure_lifecycle, source_failure, _now(), moment_id),
        )
        _diag(
            conn,
            int(win[0] or 0),
            "window_source_rejected",
            source_failure,
            moment_id,
        )
        return MomentObservationResult(
            failure_lifecycle,
            source_failure,
            moment_id,
        )
    qtype, reason, humans, _models = _qualify(rows)
    if not qtype:
        conn.execute("UPDATE memory_moment_windows SET lifecycle_status='rejected', qualification_reason=?, finalized_at=?, updated_at=? WHERE moment_id=?", (reason, _now(), _now(), moment_id))
        _diag(conn, int(win[0] or 0), "window_rejected", reason, moment_id)
        return MomentObservationResult("rejected", reason, moment_id)
    source_ids = [r.entry_id for r in rows]
    participant_count = len({r.subject_key for r in humans})
    salience = min(1.0, 0.15 + 0.10 * len(humans) + 0.10 * participant_count + (0.08 if qtype == "conversational" else 0.12))
    summary = _summary(rows, qtype, reason)
    participants = [LedgerParticipant(p[0], p[1] or "", p[2], int(p[3] or 0)) for p in conn.execute("SELECT participant_key,safe_display_name,participant_role,participation_order FROM memory_moment_participants WHERE moment_id=? ORDER BY participation_order,participant_key", (moment_id,)).fetchall()]
    visibility = win[8] or "unknown"
    public_usable = bool(win[9]) and VIS_RANK.get(visibility, 5) <= VIS_RANK.get("public_safe", 0)
    value = json.dumps({
        "schema": MOMENT_SCHEMA_VERSION, "moment_id": moment_id, "summary": summary, "window_started_at": win[6], "last_activity_at": win[7],
        "topic_key": win[5], "qualification_type": qtype, "qualification_reason": reason, "salience": salience,
        "participant_scope": "separate_audited_records", "public_usable": public_usable, "lifecycle_status": "finalized", "source_revision": "1",
    }, sort_keys=True)
    entry = LedgerEntry(
        guild_id=int(win[0] or 0), source_table="memory_moment_windows", source_row_id=moment_id, source_revision="1",
        source_role="derived_assessment", entry_type="shared_moment", subject_key=f"moment:{moment_id}", predicate_key="shared_moment",
        value=value, source_class=SourceClass.DERIVED_SUMMARY, route_mode=win[4] or "unknown", channel_id=int(win[1] or 0),
        channel_name=win[2] or "", channel_policy=win[3] or "", visibility=Visibility(visibility), confidence=Confidence.LOW,
        public_usable=public_usable, derived=True, projection=True, salience=salience, observed_at=win[7], source_sequence=0,
        valid_from=win[6], valid_until=win[7], freshness="shadow_moment_v1", lifecycle_status="review_only",
        participants=tuple(participants), lineage=tuple(("derived_from", source_id) for source_id in source_ids),
    )
    result = insert_ledger_entry(conn, entry)
    moment_entry_id = result.entry_id or _existing_moment_entry_id(conn, moment_id, int(win[0] or 0))
    for source_id in source_ids:
        conn.execute("INSERT OR IGNORE INTO memory_ledger_lineage VALUES(?,?,?,?,?)", (source_id, int(win[0] or 0), "part_of_moment", moment_entry_id, _now()))
    conn.execute(
        """
        UPDATE memory_moment_windows SET lifecycle_status='finalized', finalized_at=?, qualification_type=?, qualification_reason=?,
            salience=?, summary=?, canonical_ledger_entry_id=?, updated_at=? WHERE moment_id=?
        """,
        (_now(), qtype, reason, salience, summary, moment_entry_id, _now(), moment_id),
    )
    _replace_moment_contributions(
        conn,
        moment_id,
        rows,
        public_usable=public_usable,
    )
    _diag(conn, int(win[0] or 0), "window_finalized", reason, moment_id, moment_entry_id)
    return MomentObservationResult(result.outcome, result.reason_code, moment_id, moment_entry_id)


def sweep_expired_windows(conn: sqlite3.Connection, *, guild_id: int | None = None, now: str | None = None) -> list[MomentObservationResult]:
    if not shadow_enabled() or not ledger_shadow_enabled():
        return []
    ensure_moment_schema(conn)
    base = _parse_ts(now or _now())
    params: list[Any] = []
    where = "lifecycle_status='open'"
    if guild_id is not None:
        where += " AND guild_id=?"
        params.append(guild_id)
    results: list[MomentObservationResult] = []
    for moment_id, last_activity_at in conn.execute(f"SELECT moment_id,last_activity_at FROM memory_moment_windows WHERE {where}", params).fetchall():
        if (base - _parse_ts(last_activity_at)).total_seconds() >= INACTIVITY_SECONDS:
            results.append(finalize_moment(conn, moment_id))
    return results


def handle_source_correction(conn: sqlite3.Connection, source_entry_id: str, *, guild_id: int | None = None) -> int:
    ensure_moment_schema(conn)
    params: list[Any] = [source_entry_id]
    sql = "SELECT DISTINCT w.moment_id,w.guild_id FROM memory_moment_members m JOIN memory_moment_windows w ON w.moment_id=m.moment_id WHERE m.ledger_entry_id=?"
    if guild_id is not None:
        sql += " AND w.guild_id=?"
        params.append(guild_id)
    rows = conn.execute(sql, params).fetchall()
    for moment_id, gid in rows:
        conn.execute("UPDATE memory_moment_windows SET lifecycle_status='needs_review', updated_at=? WHERE moment_id=? AND lifecycle_status IN ('open','finalized')", (_now(), moment_id))
        _diag(conn, int(gid or 0), "moment_awaiting_review", "source_corrected", moment_id, source_entry_id)
    return len(rows)


def _moment_is_renderable(
    conn: sqlite3.Connection,
    *,
    moment_id: str,
    summary: str,
    guild_id: int,
    channel_id: int,
    channel_policy: str,
    route_mode: str,
    visibility: str,
    canonical_ledger_entry_id: str,
) -> bool:
    if not _is_safe_gist_summary(summary) or not canonical_ledger_entry_id:
        return False
    canonical = conn.execute(
        """
        SELECT guild_id,source_table,source_row_id,entry_type,channel_id,
               channel_policy,visibility,public_usable,lifecycle_status,
               normalized_value
        FROM memory_ledger_entries
        WHERE entry_id=?
        """,
        (canonical_ledger_entry_id,),
    ).fetchone()
    if not canonical:
        return False
    if (
        int(canonical[0] or 0) != guild_id
        or canonical[1] != "memory_moment_windows"
        or str(canonical[2] or "") != moment_id
        or canonical[3] != "shared_moment"
        or int(canonical[4] or 0) != channel_id
        or str(canonical[5] or "") != channel_policy
        or str(canonical[6] or "") != visibility
        or not bool(canonical[7])
        or str(canonical[8] or "")
        not in SOURCE_LIFECYCLES_USABLE_FOR_MOMENTS
    ):
        return False
    try:
        canonical_value = json.loads(str(canonical[9] or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    if not isinstance(canonical_value, dict):
        return False
    if (
        canonical_value.get("schema") != MOMENT_SCHEMA_VERSION
        or canonical_value.get("moment_id") != moment_id
        or canonical_value.get("summary") != summary
        or canonical_value.get("public_usable") is not True
    ):
        return False
    rows = _entries(conn, moment_id)
    failure, _failure_lifecycle = _moment_source_failure(
        conn,
        moment_id=moment_id,
        rows=rows,
        guild_id=guild_id,
        channel_id=channel_id,
        channel_policy=channel_policy,
        route_mode=route_mode,
        visibility=visibility,
        public_usable=True,
    )
    return not failure


def _human_participant_present(
    conn: sqlite3.Connection,
    moment_id: str,
    participant_key: str,
) -> bool:
    return bool(
        participant_key
        and conn.execute(
            """
            SELECT 1 FROM memory_moment_participants
            WHERE moment_id=? AND participant_key=?
              AND participant_role='human_author'
              AND authored_entry_count>0
            """,
            (moment_id, participant_key),
        ).fetchone()
    )


def _safe_historical_participant_label(
    conn: sqlite3.Connection,
    moment_id: str,
    participant_key: str,
) -> str:
    row = conn.execute(
        """
        SELECT safe_display_name FROM memory_moment_participants
        WHERE moment_id=? AND participant_key=?
          AND participant_role='human_author'
        ORDER BY participation_order LIMIT 1
        """,
        (moment_id, participant_key),
    ).fetchone()
    return _safe_participant_display_name(str(row[0] or "")) if row else ""


def _resolve_attribution_target(
    conn: sqlite3.Connection,
    candidate_rows: list[tuple[Any, ...]],
    request: AttributionRequest,
    requester_key: str,
    attribution_target_key: str = "",
) -> str:
    if not request.requested or not requester_key:
        return ""
    if attribution_target_key:
        if not re.fullmatch(r"discord_user:[1-9]\d*", attribution_target_key):
            return ""
        if (
            request.target_mention_key
            and request.target_mention_key != attribution_target_key
        ):
            return ""
        return (
            attribution_target_key
            if any(
                _human_participant_present(
                    conn,
                    str(row[0]),
                    attribution_target_key,
                )
                for row in candidate_rows
            )
            else ""
        )
    if request.target_mention_key:
        target_key = request.target_mention_key
        return (
            target_key
            if any(
                _human_participant_present(conn, str(row[0]), target_key)
                for row in candidate_rows
            )
            else ""
        )
    target_label = _label_key(request.target_label)
    if not target_label:
        return ""
    matches: set[str] = set()
    for row in candidate_rows:
        moment_id = str(row[0] or "")
        for participant_key, safe_name in conn.execute(
            """
            SELECT participant_key,safe_display_name
            FROM memory_moment_participants
            WHERE moment_id=? AND participant_role='human_author'
              AND authored_entry_count>0
            """,
            (moment_id,),
        ).fetchall():
            cleaned = _safe_participant_display_name(str(safe_name or ""))
            if cleaned and _label_key(cleaned) == target_label:
                matches.add(str(participant_key))
    return next(iter(matches)) if len(matches) == 1 else ""


def _contribution_is_renderable(
    conn: sqlite3.Connection,
    *,
    moment_id: str,
    participant_key: str,
    guild_id: int,
    channel_id: int,
    channel_policy: str,
    route_mode: str,
    visibility: str,
) -> tuple[str, str]:
    row = conn.execute(
        """
        SELECT contribution_gist,frame_type,source_digest,source_count,gist_version,
               lifecycle_status,public_usable
        FROM memory_moment_contributions
        WHERE moment_id=? AND participant_key=?
        """,
        (moment_id, participant_key),
    ).fetchone()
    if (
        not row
        or not str(row[1] or "")
        or str(row[4] or "") != CONTRIBUTION_GIST_VERSION
        or str(row[5] or "") not in SOURCE_LIFECYCLES_USABLE_FOR_MOMENTS
        or not bool(row[6])
        or not _human_participant_present(conn, moment_id, participant_key)
    ):
        return "", ""
    link_rows = conn.execute(
        """
        SELECT source.ledger_entry_id
        FROM memory_moment_contribution_sources source
        JOIN memory_ledger_entries entry
          ON entry.entry_id=source.ledger_entry_id
        WHERE source.moment_id=? AND source.participant_key=?
          AND source.gist_version=?
        ORDER BY entry.source_sequence,entry.observed_at,entry.entry_id
        """,
        (moment_id, participant_key, CONTRIBUTION_GIST_VERSION),
    ).fetchall()
    linked_ids = [str(link[0]) for link in link_rows]
    if not linked_ids or len(linked_ids) != int(row[3] or 0):
        return "", ""
    sources: list[SourceEntry] = []
    for entry_id in linked_ids:
        source = _fetch_entry(conn, entry_id)
        if (
            not source
            or not source.is_human
            or source.subject_key != participant_key
            or source.guild_id != guild_id
            or source.channel_id != channel_id
            or source.channel_policy != channel_policy
            or source.route_mode != route_mode
            or source.visibility != visibility
            or not source.public_usable
            or source.lifecycle_status not in SOURCE_LIFECYCLES_USABLE_FOR_MOMENTS
            or _contains_sensitive_moment_source(
                source.normalized_value,
                source.predicate_key,
            )
            or not conn.execute(
                """
                SELECT 1 FROM memory_moment_members
                WHERE moment_id=? AND ledger_entry_id=?
                """,
                (moment_id, entry_id),
            ).fetchone()
            or conn.execute(
                """
                SELECT 1 FROM memory_ledger_lineage
                WHERE guild_id=? AND target_entry_id=?
                  AND lineage_type IN ('correction_of','supersedes','retracts')
                """,
                (guild_id, entry_id),
            ).fetchone()
        ):
            return "", ""
        sources.append(source)
    # Projection selection is intentionally chronology-aware (for example, a
    # later correction or replacement outranks an earlier direction). Rebuild
    # in the same authoritative order used at finalization rather than the
    # opaque ledger-entry-id order used only to enumerate the link set.
    sources.sort(
        key=lambda source: (
            source.observed_at,
            source.source_sequence,
            source.entry_id,
        )
    )
    expected_ids = {
        source.entry_id
        for source in _entries(conn, moment_id)
        if source.is_human
        and source.subject_key == participant_key
        and _meaningful(
            source.normalized_value,
            source.source_role,
            source.predicate_key,
        )
    }
    if set(linked_ids) != expected_ids or _source_digest(sources) != str(row[2] or ""):
        return "", ""
    gist = str(row[0] or "")
    rebuilt_frame_type, rebuilt_gist = _build_contribution_projection(sources)
    if (
        rebuilt_frame_type != str(row[1] or "")
        or rebuilt_gist != gist
    ):
        return "", ""
    if not _contribution_gist_is_safe(
        gist,
        [source.normalized_value for source in sources],
    ):
        return "", ""
    label = _safe_historical_participant_label(
        conn,
        moment_id,
        participant_key,
    )
    return gist, label


def render_shadow_moment_context(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    channel_id: int,
    participant_key: str = "",
    visibility: str = "public_safe",
    topic_text: str = "",
    token_budget: int = 120,
    freshness_days: int = 3650,
    allow_cross_channel: bool = False,
    allowed_channel_policies: tuple[str, ...] = (),
    attribution_target_key: str = "",
) -> str:
    ensure_moment_schema(conn)
    attribution = _parse_attribution_request(topic_text)
    if attribution.exact_authority_requested:
        return ""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=freshness_days)).isoformat()
    relevance_text = (
        attribution.topic_text
        if attribution.requested
        else topic_text
    )
    family = _topic_family(relevance_text, "conversation")
    signature = _topic_signature(relevance_text, "conversation")
    if attribution.requested and not signature:
        # A topicless "what did X mean?" must resolve from the immediate room
        # exchange. Searching months of Moments without a topic can select an
        # unrelated high-salience event and misattribute it as the referent.
        return ""
    params: list[Any] = [int(guild_id or 0)]
    if allow_cross_channel:
        # Cross-channel continuity is never an implicit widening. It requires a
        # participant scope and an explicit policy tuple, intersected with the
        # engine's hard public allowlist.
        policies = tuple(
            sorted(
                {
                    _canon(policy)
                    for policy in (allowed_channel_policies or ())
                    if _canon(policy) in PUBLIC_CROSS_CHANNEL_POLICIES
                }
            )
        )
        if not participant_key or not policies:
            return ""
        policy_placeholders = ",".join("?" for _ in policies)
        scope_sql = f"guild_id=? AND channel_policy IN ({policy_placeholders})"
        params.extend(policies)
    else:
        scope_sql = "guild_id=? AND channel_id=?"
        params.append(int(channel_id or 0))
    params.append(cutoff)
    lines: list[str] = []
    used = 0
    candidate_rows = conn.execute(
        f"""
        SELECT moment_id,summary,topic_family,topic_signature,visibility,
               last_activity_at,salience,channel_id,channel_policy,route_mode,
               canonical_ledger_entry_id
        FROM memory_moment_windows
        WHERE {scope_sql} AND lifecycle_status='finalized'
          AND public_usable=1 AND last_activity_at>=?
        ORDER BY salience DESC,last_activity_at DESC
        """,
        params,
    ).fetchall()
    renderable_rows: list[tuple[Any, ...]] = []
    for row in candidate_rows:
        if VIS_RANK.get(row[4], 5) > VIS_RANK.get(visibility, 0):
            continue
        if (
            participant_key
            and not attribution.requested
            and not _human_participant_present(
            conn,
            str(row[0] or ""),
            participant_key,
            )
        ):
            continue
        if not _moment_is_renderable(
            conn,
            moment_id=str(row[0] or ""),
            summary=str(row[1] or ""),
            guild_id=int(guild_id or 0),
            channel_id=int(row[7] or 0),
            channel_policy=str(row[8] or ""),
            route_mode=str(row[9] or "unknown"),
            visibility=str(row[4] or "unknown"),
            canonical_ledger_entry_id=str(row[10] or ""),
        ):
            continue
        renderable_rows.append(row)
    target_key = ""
    if attribution.requested:
        target_key = _resolve_attribution_target(
            conn,
            renderable_rows,
            attribution,
            participant_key,
            attribution_target_key,
        )
        if not target_key:
            return ""
    for row in renderable_rows:
        window_signature = _load_sig(row[3])
        if signature and not _coherent(
            family,
            signature,
            row[2] or "",
            window_signature,
        ):
            continue
        if attribution.requested:
            if not _contribution_topic_coherent(
                signature,
                window_signature,
            ):
                continue
            if not _human_participant_present(
                conn,
                str(row[0] or ""),
                target_key,
            ):
                continue
            gist, label = _contribution_is_renderable(
                conn,
                moment_id=str(row[0] or ""),
                participant_key=target_key,
                guild_id=int(guild_id or 0),
                channel_id=int(row[7] or 0),
                channel_policy=str(row[8] or ""),
                route_mode=str(row[9] or "unknown"),
                visibility=str(row[4] or "unknown"),
            )
            if not gist:
                continue
            line = (
                "[Derived participant contribution gist; paraphrase only, "
                "never exact wording] "
                + (f"{label}: " if label else "That participant: ")
                + gist
                + " This is attributed meaning only; it cannot support a "
                "quotation or settle a dispute."
            )
        else:
            line = (
                "[Derived moment gist; paraphrase only, never exact wording] "
                + str(row[1] or "")
            )
            participant_gist = ""
            if (
                participant_key
                and signature
                and _contribution_topic_coherent(
                    signature,
                    window_signature,
                )
            ):
                participant_gist, _participant_label = (
                    _contribution_is_renderable(
                        conn,
                        moment_id=str(row[0] or ""),
                        participant_key=participant_key,
                        guild_id=int(guild_id or 0),
                        channel_id=int(row[7] or 0),
                        channel_policy=str(row[8] or ""),
                        route_mode=str(row[9] or "unknown"),
                        visibility=str(row[4] or "unknown"),
                    )
                )
                if participant_gist:
                    line += (
                        "\n[Derived current-participant contribution gist; "
                        "paraphrase only, never exact wording] "
                        + participant_gist
                    )
            if participant_key and not participant_gist:
                # A generic topic-family label is not enough evidence for
                # personal continuity. It invites the model to invent what the
                # member contributed. Only inject a concrete, source-checked
                # participant gist on the scoped canary path.
                continue
        words = line.split()
        if used + len(words) > token_budget:
            if attribution.requested:
                return ""
            if not lines:
                lines.append(" ".join(words[:max(1, token_budget)]))
            break
        lines.append(line)
        used += len(words)
        if attribution.requested:
            break
    return "\n".join(lines)


def build_moment_evaluation_report(
    conn: sqlite3.Connection,
    *,
    guild_id: int | None = None,
    prepare_schema: bool = True,
) -> dict[str, Any]:
    if prepare_schema:
        ensure_moment_schema(conn)
    where = ""
    params: list[Any] = []
    if guild_id is not None:
        where = " WHERE guild_id=?"
        params = [guild_id]
    def one(sql: str, bind: list[Any] | None = None) -> Any:
        return conn.execute(sql, [] if bind is None else bind).fetchone()[0]
    scoped = "guild_id=? AND " if guild_id is not None else ""
    p = [guild_id] if guild_id is not None else []
    report = {
        "eligible_entries_observed": one(f"SELECT COUNT(*) FROM memory_moment_diagnostics{where + (' AND' if where else ' WHERE')} event_type='eligible_ledger_entry_observed'", params),
        "open_windows": one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='open'", params),
        "finalized_moments": one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='finalized'", params),
        "processing_errors": one(f"SELECT COUNT(*) FROM memory_moment_diagnostics{where + (' AND' if where else ' WHERE')} event_type='moment_processing_error'", params),
        "rejected_windows_by_reason": dict(conn.execute(f"SELECT qualification_reason,COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='rejected' GROUP BY qualification_reason", params).fetchall()),
        "moments_by_qualification_type": dict(conn.execute(f"SELECT qualification_type,COUNT(*) FROM memory_moment_windows{where} GROUP BY qualification_type", params).fetchall()),
        "moments_by_visibility": dict(conn.execute(f"SELECT visibility,COUNT(*) FROM memory_moment_windows{where} GROUP BY visibility", params).fetchall()),
        "moments_by_lifecycle": dict(conn.execute(f"SELECT lifecycle_status,COUNT(*) FROM memory_moment_windows{where} GROUP BY lifecycle_status", params).fetchall()),
        "one_human_conversational_moments": one(f"SELECT COUNT(*) FROM memory_moment_windows WHERE {scoped}qualification_type='conversational'", p),
        "multi_human_shared_moments": one(f"SELECT COUNT(*) FROM memory_moment_windows WHERE {scoped}qualification_type='shared_activity'", p),
        "average_participant_count": one(f"SELECT COALESCE(AVG(participant_count),0) FROM memory_moment_windows WHERE {scoped}lifecycle_status='finalized'", p),
        "duplicate_memberships": one(f"SELECT COUNT(*) FROM (SELECT m.moment_id,m.ledger_entry_id,COUNT(*) c FROM memory_moment_members m JOIN memory_moment_windows w ON w.moment_id=m.moment_id WHERE {scoped}1=1 GROUP BY m.moment_id,m.ledger_entry_id HAVING c>1)", p),
        "bnl_only_violations": one(f"SELECT COUNT(*) FROM memory_moment_windows WHERE {scoped}lifecycle_status='finalized' AND human_entry_count=0", p),
        "cross_guild_violations": one(f"SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE {('mw.guild_id=? AND ' if guild_id is not None else '')}e.guild_id<>mw.guild_id", p),
        "cross_channel_violations": one(f"SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE {('mw.guild_id=? AND ' if guild_id is not None else '')}e.channel_id<>mw.channel_id", p),
        "incompatible_visibility_violations": one(f"SELECT COUNT(*) FROM memory_moment_members mm JOIN memory_moment_windows mw ON mw.moment_id=mm.moment_id JOIN memory_ledger_entries e ON e.entry_id=mm.ledger_entry_id WHERE {('mw.guild_id=? AND ' if guild_id is not None else '')}e.visibility<>mw.visibility", p),
        "dangling_lineage_targets": one(f"SELECT COUNT(*) FROM memory_ledger_lineage l LEFT JOIN memory_ledger_entries e ON e.guild_id=l.guild_id AND e.entry_id=l.target_entry_id WHERE {('l.guild_id=? AND ' if guild_id is not None else '')}e.entry_id IS NULL", p),
        "affected_moments_awaiting_correction_review": one(f"SELECT COUNT(*) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} lifecycle_status='needs_review'", params),
        "finalization_latency": one(f"SELECT COALESCE(AVG(strftime('%s',finalized_at)-strftime('%s',last_activity_at)),0) FROM memory_moment_windows{where + (' AND' if where else ' WHERE')} finalized_at IS NOT NULL", params),
    }
    return report
