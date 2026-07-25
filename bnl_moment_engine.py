"""Moment Engine shadow infrastructure.

Builds bounded, auditable conversational moments from Unified Memory Ledger
conversation entries and joins coherent finalized Moments into source-backed
episodic lifecycle v2 records. Construction remains shadow-first; a separately
allowlisted prompt canary may render only revalidated public-safe Moment gist.
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
EPISODE_SCHEMA_VERSION = "memory_moment_episode_v2"
REMEMBERED_NUMBER_QUARANTINE_MIGRATION = "remembered_number_quarantine_v1"
SAFE_MOMENT_PROJECTION_MIGRATION = "safe_moment_projection_v1"
MOMENT_CONTRIBUTION_BACKFILL_MIGRATION = "moment_contribution_backfill_v1"
LEGACY_MOMENT_RECONSTRUCTION_MIGRATION = "legacy_moment_reconstruction_v1"
EPISODIC_LIFECYCLE_MIGRATION = "episodic_lifecycle_v2"
MAX_WINDOW_SECONDS = 5 * 60
INACTIVITY_SECONDS = 2 * 60
EPISODE_INACTIVITY_SECONDS = 24 * 60 * 60
EPISODE_REOPEN_SECONDS = 30 * 24 * 60 * 60
CONTRIBUTION_GIST_VERSION = "moment_contribution_gist_v1"
EPISODE_EVENT_TYPES = (
    "action",
    "reaction",
    "decision",
    "assignment",
    "outcome",
    "open_loop",
)

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
_EPISODE_RESUME_RE = re.compile(
    r"\b(?:resume|continue|pick\s+(?:it|this|that)\s+back\s+up|"
    r"pick\s+up\s+where\s+we\s+left\s+off|back\s+to|return(?:ing)?\s+to|"
    r"get\s+back\s+to|reopen)\b",
    re.I,
)
_EPISODE_RELATED_RE = re.compile(
    r"\b(?:combine|connect|link|tie)\b.{0,36}"
    r"\b(?:thread|topic|discussion|conversation|idea|plan|moment)s?\b"
    r"|\b(?:bring|put)\b.{0,24}\btogether\b",
    re.I,
)
_EPISODE_CLOSE_RE = re.compile(
    r"\b(?:done|finished|complete(?:d)?|resolved|fixed|settled|shipped|"
    r"closed|wrapped\s+up|that\s+worked|tests?\s+passed|deployed)\b",
    re.I,
)
_EPISODE_NEGATED_CLOSE_RE = re.compile(
    r"\b(?:not|never|isn(?:'|’)t|wasn(?:'|’)t|aren(?:'|’)t|"
    r"weren(?:'|’)t|hasn(?:'|’)t|haven(?:'|’)t|hadn(?:'|’)t)\b"
    r".{0,32}\b(?:done|finished|complete(?:d)?|resolved|fixed|settled|"
    r"shipped|closed|wrapped\s+up|passed|deployed)\b",
    re.I,
)
_EPISODE_ACTION_RE = re.compile(
    r"\b(?:i|we|you|they|he|she)\s+(?:will|plan(?:ned)?\s+to|"
    r"intend(?:ed)?\s+to|need(?:ed)?\s+to|should|could)\b"
    r"|\b(?:let(?:'|’)s|implement|build|fix|test|send|write|create|"
    r"change|update|deploy|review|check|run)\b",
    re.I,
)
_EPISODE_REACTION_RE = re.compile(
    r"\b(?:agree|disagree|like|love|hate|prefer|reject|oppose|"
    r"works?|doesn(?:'|’)t\s+work|good|bad|better|worse)\b",
    re.I,
)
_EPISODE_DECISION_RE = re.compile(
    r"\b(?:decid(?:e|ed)|agreed|settled|final\s+(?:choice|decision)|"
    r"go\s+with|choose|chose|pick(?:ed)?|select(?:ed)?)\b",
    re.I,
)
_EPISODE_ASSIGNMENT_RE = re.compile(
    r"\b(?:assign(?:ed)?|responsible\s+for|owner\s+of|"
    r"i(?:'|’)?ll\s+(?:handle|own|take|do)|"
    r"you(?:'|’)?ll\s+(?:handle|own|take|do)|"
    r"(?:i|you|we|they|he|she)\s+(?:need|needs|will|should)\s+to|"
    r"[a-z][a-z0-9_.-]*(?:\s+[a-z][a-z0-9_.-]*){0,2}\s+"
    r"will\s+(?:handle|own|take|do))\b",
    re.I,
)
_EPISODE_OPEN_LOOP_RE = re.compile(
    r"\?|"
    r"\b(?:still\s+need|need\s+to\s+decide|not\s+settled|unresolved|"
    r"open\s+(?:question|loop)|follow\s+up|next\s+step|to\s+do|todo|"
    r"pending|waiting\s+on)\b",
    re.I,
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
class EpisodeObservationResult:
    outcome: str = "skipped"
    reason_code: str = "not_attempted"
    episode_id: str = ""
    moment_id: str = ""


@dataclass(frozen=True)
class ActiveEpisodeReference:
    """Opaque, content-free reference for the shadow response assessment."""

    episode_id: str
    lifecycle_status: str
    source_moment_ids: tuple[str, ...]
    participant_count: int
    open_loop_count: int
    semantic_types: tuple[str, ...]


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


def stable_episode_id(
    guild_id: int,
    channel_id: int,
    opening_moment_id: str,
) -> str:
    return "mep_" + hashlib.sha256(
        (
            f"{EPISODE_SCHEMA_VERSION}\x1f{guild_id}\x1f"
            f"{channel_id}\x1f{opening_moment_id}"
        ).encode("utf-8")
    ).hexdigest()[:32]


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
_RECALL_TOPIC_FOCUS_RE = re.compile(
    r"\b(?:discussion|conversation|exchange|thread|memory|point)?\s*"
    r"about\s+(?P<topic>[^.?!\n]{1,160})(?=[.?!\n]|$)",
    re.I,
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
_QUESTION_THREAD_INTENT_RE = re.compile(
    r"\?|"
    r"\b(?:am\s+i|are\s+we|is\s+(?:it|this|that)|"
    r"why|whether|how\s+(?:can|could|do|does|is|are)|"
    r"questions?\s+about)\b",
    re.I,
)
_QUESTION_THREAD_FILLER = frozenset(
    {
        "even",
        "great",
        "if",
        "leads",
        "like",
        "more",
        "not",
        "oh",
        "questions",
        "said",
        "saying",
        "so",
        "then",
        "which",
    }
)
_QUESTION_THREAD_CONCEPT_PATTERNS = (
    (re.compile(r"\bdream(?:s|ed|ing)?\b", re.I), "dreaming"),
    (
        re.compile(r"\b(?:alive|living|exist(?:s|ed|ing|ence)?)\b", re.I),
        "being alive",
    ),
    (
        re.compile(
            r"\b(?:program(?:s|med|ming)?|coded)\b.{0,80}"
            r"\b(?:identity|authentic(?:ity)?|myself|me)\b|"
            r"\b(?:identity|authentic(?:ity)?|myself)\b.{0,80}"
            r"\b(?:program(?:s|med|ming)?|coded)\b",
            re.I,
        ),
        "programmed identity",
    ),
    (re.compile(r"\b(?:anxiety|anxious)\b", re.I), "anxiety"),
)


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


def _question_thread_concepts(source: SourceEntry) -> tuple[str, ...]:
    text = re.sub(r"\s+", " ", str(source.normalized_value or "")).strip()
    if (
        not text
        or not _QUESTION_THREAD_INTENT_RE.search(text)
        or _contains_sensitive_moment_source(text, source.predicate_key)
    ):
        return ()
    recognized = sorted(
        (
            (match.start(), label)
            for pattern, label in _QUESTION_THREAD_CONCEPT_PATTERNS
            if (match := pattern.search(text))
        ),
        key=lambda item: item[0],
    )
    if recognized:
        return tuple(dict.fromkeys(label for _offset, label in recognized))
    return tuple(
        concept
        for concept in _semantic_concepts(
            text,
            source,
            minimum=1,
            maximum=4,
        )
        if concept not in _QUESTION_THREAD_FILLER
    )


def _build_question_thread_semantic_frame(
    rows: list[SourceEntry],
) -> ContributionSemanticFrame | None:
    """Synthesize one source-backed theme from a sustained question sequence."""
    if len(rows) < 3 or any(
        not _QUESTION_THREAD_INTENT_RE.search(row.normalized_value or "")
        for row in rows
    ):
        return None
    concepts: list[str] = []
    for source in rows:
        source_concepts = _question_thread_concepts(source)
        if not source_concepts:
            return None
        for concept in source_concepts:
            if concept not in concepts:
                concepts.append(concept)
    if len(concepts) < 3:
        return None
    return ContributionSemanticFrame(
        "question_thread",
        tuple(concepts[:6]),
    )


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
    "question_thread": 22,
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
    return f"{', '.join(concepts[:-1])}, and {concepts[-1]}"


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
    if frame.frame_type == "question_thread":
        return (
            "The participant explored connected questions involving "
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
    frame = (
        _build_question_thread_semantic_frame(rows)
        or _select_contribution_semantic_frame(rows)
    )
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


def _recall_topic_focus(text: str) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    matches = list(_RECALL_TOPIC_FOCUS_RE.finditer(value))
    if not matches:
        return value
    focused = str(matches[-1].group("topic") or "").strip(" \t,;:-")
    return focused or value


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
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_episodes (
      episode_id TEXT PRIMARY KEY, schema_version TEXT NOT NULL,
      guild_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
      channel_name TEXT, channel_policy TEXT NOT NULL, route_mode TEXT NOT NULL,
      visibility TEXT NOT NULL, public_usable INTEGER DEFAULT 0,
      topic_key TEXT NOT NULL, topic_family TEXT DEFAULT '',
      topic_signature TEXT DEFAULT '[]', lifecycle_status TEXT NOT NULL,
      opened_at TEXT NOT NULL, last_activity_at TEXT NOT NULL,
      finalized_at TEXT, finalization_reason TEXT DEFAULT '',
      reopen_count INTEGER DEFAULT 0, split_count INTEGER DEFAULT 0,
      revision INTEGER DEFAULT 1, moment_count INTEGER DEFAULT 0,
      human_entry_count INTEGER DEFAULT 0, participant_count INTEGER DEFAULT 0,
      semantic_types_json TEXT DEFAULT '[]', action_count INTEGER DEFAULT 0,
      reaction_count INTEGER DEFAULT 0, decision_count INTEGER DEFAULT 0,
      assignment_count INTEGER DEFAULT 0, outcome_count INTEGER DEFAULT 0,
      open_loop_count INTEGER DEFAULT 0,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_episode_moments (
      episode_id TEXT NOT NULL, moment_id TEXT NOT NULL,
      link_role TEXT NOT NULL, source_digest TEXT NOT NULL,
      linked_at TEXT NOT NULL, PRIMARY KEY(episode_id, moment_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_episode_participants (
      episode_id TEXT NOT NULL, participant_key TEXT NOT NULL,
      participant_role TEXT NOT NULL, safe_display_name TEXT DEFAULT '',
      first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
      source_moment_count INTEGER DEFAULT 0,
      participation_order INTEGER DEFAULT 0,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      PRIMARY KEY(episode_id, participant_key, participant_role))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_episode_events (
      episode_id TEXT NOT NULL, ledger_entry_id TEXT NOT NULL,
      participant_key TEXT NOT NULL, event_type TEXT NOT NULL,
      observed_at TEXT NOT NULL, source_sequence INTEGER DEFAULT 0,
      lifecycle_status TEXT NOT NULL, source_digest TEXT NOT NULL,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      PRIMARY KEY(episode_id, ledger_entry_id, event_type))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_episode_lineage (
      from_episode_id TEXT NOT NULL, to_episode_id TEXT NOT NULL,
      relation_type TEXT NOT NULL, evidence_moment_id TEXT NOT NULL,
      evidence_entry_id TEXT NOT NULL, created_at TEXT NOT NULL,
      PRIMARY KEY(from_episode_id, to_episode_id, relation_type,
                  evidence_moment_id, evidence_entry_id))""")
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
    cur.execute("""CREATE TABLE IF NOT EXISTS memory_moment_reconstructions (
      legacy_moment_id TEXT NOT NULL, reconstruction_version TEXT NOT NULL,
      reconstructed_moment_id TEXT DEFAULT '', source_digest TEXT DEFAULT '',
      outcome TEXT NOT NULL, reason_code TEXT NOT NULL,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      PRIMARY KEY(legacy_moment_id, reconstruction_version))""")
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_mmw_scope ON memory_moment_windows(guild_id, channel_id, lifecycle_status, last_activity_at)",
        "CREATE INDEX IF NOT EXISTS idx_mmw_canonical ON memory_moment_windows(guild_id, canonical_ledger_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmm_entry ON memory_moment_members(ledger_entry_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmp_participant ON memory_moment_participants(participant_key, moment_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmc_participant ON memory_moment_contributions(participant_key, moment_id, lifecycle_status)",
        "CREATE INDEX IF NOT EXISTS idx_mmcs_entry ON memory_moment_contribution_sources(ledger_entry_id, moment_id)",
        "CREATE INDEX IF NOT EXISTS idx_mme_scope ON memory_moment_episodes(guild_id, channel_id, lifecycle_status, last_activity_at)",
        "CREATE INDEX IF NOT EXISTS idx_mme_topic ON memory_moment_episodes(guild_id, channel_id, topic_family, lifecycle_status, last_activity_at)",
        "CREATE INDEX IF NOT EXISTS idx_mmem_moment ON memory_moment_episode_moments(moment_id, episode_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmep_participant ON memory_moment_episode_participants(participant_key, episode_id)",
        "CREATE INDEX IF NOT EXISTS idx_mmee_source ON memory_moment_episode_events(ledger_entry_id, episode_id, lifecycle_status)",
        "CREATE INDEX IF NOT EXISTS idx_mmel_target ON memory_moment_episode_lineage(to_episode_id, relation_type)",
        "CREATE INDEX IF NOT EXISTS idx_mmd_guild ON memory_moment_diagnostics(guild_id, event_type, reason_code)",
        "CREATE INDEX IF NOT EXISTS idx_mmr_reconstructed ON memory_moment_reconstructions(reconstructed_moment_id, outcome)",
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
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_episode_source_delete
        AFTER DELETE ON memory_ledger_entries
        BEGIN
          UPDATE memory_moment_episodes
          SET lifecycle_status='needs_review', public_usable=0,
              finalization_reason='source_deleted',
              updated_at=CURRENT_TIMESTAMP
          WHERE episode_id IN (
            SELECT link.episode_id
            FROM memory_moment_episode_moments link
            JOIN memory_moment_members member
              ON member.moment_id=link.moment_id
            WHERE member.ledger_entry_id=OLD.entry_id
          )
            AND lifecycle_status IN ('active','finalized');
          DELETE FROM memory_moment_episode_events
          WHERE ledger_entry_id=OLD.entry_id;
        END"""
    )
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_episode_source_lifecycle
        AFTER UPDATE OF lifecycle_status,normalized_value,public_usable
        ON memory_ledger_entries
        WHEN NEW.lifecycle_status NOT IN ('active','review_only')
          OR NEW.normalized_value IS NOT OLD.normalized_value
          OR NEW.public_usable IS NOT OLD.public_usable
        BEGIN
          UPDATE memory_moment_episodes
          SET lifecycle_status='needs_review', public_usable=0,
              finalization_reason='source_changed',
              updated_at=CURRENT_TIMESTAMP
          WHERE episode_id IN (
            SELECT link.episode_id
            FROM memory_moment_episode_moments link
            JOIN memory_moment_members member
              ON member.moment_id=link.moment_id
            WHERE member.ledger_entry_id=NEW.entry_id
          )
            AND lifecycle_status IN ('active','finalized');
          UPDATE memory_moment_episode_events
          SET lifecycle_status='needs_review', updated_at=CURRENT_TIMESTAMP
          WHERE ledger_entry_id=NEW.entry_id;
        END"""
    )
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_episode_moment_lifecycle
        AFTER UPDATE OF lifecycle_status ON memory_moment_windows
        WHEN NEW.lifecycle_status <> 'finalized'
        BEGIN
          UPDATE memory_moment_episodes
          SET lifecycle_status='needs_review', public_usable=0,
              finalization_reason='linked_moment_changed',
              updated_at=CURRENT_TIMESTAMP
          WHERE episode_id IN (
            SELECT episode_id FROM memory_moment_episode_moments
            WHERE moment_id=NEW.moment_id
          )
            AND lifecycle_status IN ('active','finalized');
        END"""
    )
    cur.execute(
        """CREATE TRIGGER IF NOT EXISTS trg_episode_moment_delete
        AFTER DELETE ON memory_moment_windows
        BEGIN
          UPDATE memory_moment_episodes
          SET lifecycle_status='needs_review', public_usable=0,
              finalization_reason='linked_moment_deleted',
              updated_at=CURRENT_TIMESTAMP
          WHERE episode_id IN (
            SELECT episode_id FROM memory_moment_episode_moments
            WHERE moment_id=OLD.moment_id
          )
            AND lifecycle_status IN ('active','finalized');
          DELETE FROM memory_moment_episode_moments
          WHERE moment_id=OLD.moment_id;
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
    if not cur.execute(
        "SELECT 1 FROM memory_moment_migrations WHERE migration_key=?",
        (LEGACY_MOMENT_RECONSTRUCTION_MIGRATION,),
    ).fetchone():
        reconstruction = reconstruct_legacy_moments(conn)
        if int(reconstruction.get("errors", 0) or 0) == 0:
            cur.execute(
                "INSERT OR IGNORE INTO memory_moment_migrations VALUES(?,?)",
                (LEGACY_MOMENT_RECONSTRUCTION_MIGRATION, _now()),
            )
    if not cur.execute(
        "SELECT 1 FROM memory_moment_migrations WHERE migration_key=?",
        (EPISODIC_LIFECYCLE_MIGRATION,),
    ).fetchone():
        episode_backfill = backfill_episodic_lifecycle(conn)
        if int(episode_backfill.get("errors", 0) or 0) == 0:
            cur.execute(
                "INSERT OR IGNORE INTO memory_moment_migrations VALUES(?,?)",
                (EPISODIC_LIFECYCLE_MIGRATION, _now()),
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


def stable_reconstructed_moment_id(legacy_moment_id: str) -> str:
    return "mom_" + hashlib.sha256(
        (
            f"{MOMENT_SCHEMA_VERSION}\x1f"
            f"{LEGACY_MOMENT_RECONSTRUCTION_MIGRATION}\x1f"
            f"{legacy_moment_id}"
        ).encode("utf-8")
    ).hexdigest()[:32]


def _record_legacy_moment_reconstruction(
    conn: sqlite3.Connection,
    *,
    legacy_moment_id: str,
    reconstructed_moment_id: str = "",
    source_digest: str = "",
    outcome: str,
    reason_code: str,
) -> None:
    now = _now()
    conn.execute(
        """
        INSERT OR IGNORE INTO memory_moment_reconstructions(
          legacy_moment_id,reconstruction_version,reconstructed_moment_id,
          source_digest,outcome,reason_code,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            legacy_moment_id,
            LEGACY_MOMENT_RECONSTRUCTION_MIGRATION,
            reconstructed_moment_id,
            source_digest,
            outcome,
            reason_code,
            now,
            now,
        ),
    )


def reconstruct_legacy_moments(
    conn: sqlite3.Connection,
) -> dict[str, int]:
    """Build new safe Moments from preserved sources without reviving old rows."""
    conn.execute("""CREATE TABLE IF NOT EXISTS memory_moment_reconstructions (
      legacy_moment_id TEXT NOT NULL, reconstruction_version TEXT NOT NULL,
      reconstructed_moment_id TEXT DEFAULT '', source_digest TEXT DEFAULT '',
      outcome TEXT NOT NULL, reason_code TEXT NOT NULL,
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      PRIMARY KEY(legacy_moment_id, reconstruction_version))""")
    counts = {
        "considered": 0,
        "reconstructed": 0,
        "deduplicated": 0,
        "skipped": 0,
        "errors": 0,
        "contributions": 0,
    }
    candidates = conn.execute(
        """
        SELECT moment_id,guild_id,channel_id,channel_name,channel_policy,
               route_mode,window_started_at,last_activity_at,visibility
        FROM memory_moment_windows
        WHERE lifecycle_status='retracted'
          AND qualification_reason='legacy_extractive_projection'
        ORDER BY window_started_at,moment_id
        """
    ).fetchall()
    for candidate in candidates:
        legacy_moment_id = str(candidate[0] or "")
        counts["considered"] += 1
        prior_audit = conn.execute(
            """
            SELECT outcome FROM memory_moment_reconstructions
            WHERE legacy_moment_id=? AND reconstruction_version=?
            """,
            (
                legacy_moment_id,
                LEGACY_MOMENT_RECONSTRUCTION_MIGRATION,
            ),
        ).fetchone()
        if prior_audit:
            if str(prior_audit[0] or "") != "error":
                counts["deduplicated"] += 1
                continue
            conn.execute(
                """
                DELETE FROM memory_moment_reconstructions
                WHERE legacy_moment_id=? AND reconstruction_version=?
                """,
                (
                    legacy_moment_id,
                    LEGACY_MOMENT_RECONSTRUCTION_MIGRATION,
                ),
            )

        rows = _entries(conn, legacy_moment_id)
        digest = _source_digest(rows)
        reconstructed_moment_id = stable_reconstructed_moment_id(
            legacy_moment_id
        )

        def skip(reason_code: str) -> None:
            _record_legacy_moment_reconstruction(
                conn,
                legacy_moment_id=legacy_moment_id,
                source_digest=digest,
                outcome="skipped",
                reason_code=reason_code,
            )
            counts["skipped"] += 1

        existing = conn.execute(
            """
            SELECT lifecycle_status FROM memory_moment_windows
            WHERE moment_id=?
            """,
            (reconstructed_moment_id,),
        ).fetchone()
        if existing:
            if (
                str(existing[0] or "") == "finalized"
                and _source_digest(
                    _entries(conn, reconstructed_moment_id)
                )
                == digest
            ):
                _record_legacy_moment_reconstruction(
                    conn,
                    legacy_moment_id=legacy_moment_id,
                    reconstructed_moment_id=reconstructed_moment_id,
                    source_digest=digest,
                    outcome="reconstructed",
                    reason_code="existing_exact_reconstruction",
                )
                counts["deduplicated"] += 1
            else:
                _record_legacy_moment_reconstruction(
                    conn,
                    legacy_moment_id=legacy_moment_id,
                    reconstructed_moment_id=reconstructed_moment_id,
                    source_digest=digest,
                    outcome="error",
                    reason_code="reconstruction_id_conflict",
                )
                counts["errors"] += 1
            continue

        (
            _old_moment_id,
            guild_id,
            channel_id,
            channel_name,
            channel_policy,
            route_mode,
            window_started_at,
            last_activity_at,
            visibility,
        ) = candidate
        guild_id = int(guild_id or 0)
        channel_id = int(channel_id or 0)
        channel_name = str(channel_name or "")
        channel_policy = str(channel_policy or "unknown")
        route_mode = str(route_mode or "unknown")
        visibility = str(visibility or "unknown")
        public_usable = bool(
            rows
            and visibility in {"public", "public_safe"}
            and all(
                row.visibility in {"public", "public_safe"}
                for row in rows
            )
            and all(
                row.public_usable
                for row in rows
                if row.is_human
            )
        )
        source_failure, _failure_lifecycle = _moment_source_failure(
            conn,
            moment_id=legacy_moment_id,
            rows=rows,
            guild_id=guild_id,
            channel_id=channel_id,
            channel_policy=channel_policy,
            route_mode=route_mode,
            visibility=visibility,
            public_usable=public_usable,
        )
        if source_failure:
            skip(source_failure)
            continue
        if not public_usable:
            skip("legacy_sources_not_public_usable")
            continue
        qtype, reason, humans, _models = _qualify(rows)
        if not qtype:
            skip(reason)
            continue
        family, signature, topic_key = _topic_projection(rows)
        summary = _summary(rows, qtype, reason)
        if (
            not signature
            or not _topic_signature_is_non_extractive(
                _json_sig(signature)
            )
            or not _is_safe_gist_summary(summary)
        ):
            skip("safe_projection_unavailable")
            continue

        try:
            conn.execute("SAVEPOINT legacy_moment_reconstruction")
            now = _now()
            conn.execute(
                """
                INSERT INTO memory_moment_windows(
                  moment_id,guild_id,channel_id,channel_name,channel_policy,
                  route_mode,topic_key,topic_family,topic_signature,
                  window_started_at,last_activity_at,lifecycle_status,
                  visibility,public_usable,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    reconstructed_moment_id,
                    guild_id,
                    channel_id,
                    channel_name,
                    channel_policy,
                    route_mode,
                    topic_key,
                    family,
                    _json_sig(signature),
                    str(window_started_at or _now()),
                    str(last_activity_at or window_started_at or _now()),
                    "open",
                    visibility,
                    1,
                    now,
                    now,
                ),
            )
            for source in rows:
                _insert_membership(
                    conn,
                    reconstructed_moment_id,
                    source,
                    _meaningful(
                        source.normalized_value,
                        source.source_role,
                        source.predicate_key,
                    ),
                    _topic_family(
                        source.normalized_value,
                        source.predicate_key,
                    ),
                    _topic_signature(
                        source.normalized_value,
                        source.predicate_key,
                    ),
                )
            _recount(conn, reconstructed_moment_id)
            finalization = finalize_moment(
                conn,
                reconstructed_moment_id,
                ensure_schema=False,
            )
            rebuilt = conn.execute(
                """
                SELECT lifecycle_status,public_usable,
                       canonical_ledger_entry_id
                FROM memory_moment_windows WHERE moment_id=?
                """,
                (reconstructed_moment_id,),
            ).fetchone()
            if (
                finalization.outcome
                not in {"inserted", "deduplicated", "active"}
                or not rebuilt
                or str(rebuilt[0] or "") != "finalized"
                or not bool(rebuilt[1])
                or not str(rebuilt[2] or "")
            ):
                raise ValueError(
                    f"reconstruction_finalization_failed:"
                    f"{finalization.reason_code}"
                )
            canonical_entry_id = str(rebuilt[2])
            inserted_contributions = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM memory_moment_contributions
                    WHERE moment_id=?
                    """,
                    (reconstructed_moment_id,),
                ).fetchone()[0]
            )
            _record_legacy_moment_reconstruction(
                conn,
                legacy_moment_id=legacy_moment_id,
                reconstructed_moment_id=reconstructed_moment_id,
                source_digest=digest,
                outcome="reconstructed",
                reason_code="eligible_sources_rebuilt",
            )
            _diag(
                conn,
                guild_id,
                "legacy_moment_reconstructed",
                "eligible_sources_rebuilt",
                reconstructed_moment_id,
                canonical_entry_id,
            )
            observe_finalized_moment_episode(
                conn,
                reconstructed_moment_id,
                require_shadow_gate=False,
            )
            conn.execute("RELEASE legacy_moment_reconstruction")
            counts["reconstructed"] += 1
            counts["contributions"] += inserted_contributions
        except Exception:
            try:
                conn.execute(
                    "ROLLBACK TO legacy_moment_reconstruction"
                )
                conn.execute(
                    "RELEASE legacy_moment_reconstruction"
                )
            except Exception:
                pass
            _record_legacy_moment_reconstruction(
                conn,
                legacy_moment_id=legacy_moment_id,
                reconstructed_moment_id=reconstructed_moment_id,
                source_digest=digest,
                outcome="error",
                reason_code="reconstruction_exception",
            )
            counts["errors"] += 1
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


def _topic_projection(
    rows: list[SourceEntry],
) -> tuple[str, tuple[str, ...], str]:
    human_rows = [
        row
        for row in rows
        if row.is_human
        and _meaningful(
            row.normalized_value,
            row.source_role,
            row.predicate_key,
        )
    ]
    families = {
        _topic_family(row.normalized_value, row.predicate_key)
        for row in human_rows
    }
    known_families = sorted(
        family for family in families if family != "low_signal"
    )
    family = (
        known_families[0]
        if len(known_families) == 1
        else "topic_other"
    )
    source_signatures = {
        token
        for row in human_rows
        for token in _topic_signature(
            row.normalized_value,
            row.predicate_key,
        )
    }
    semantic_signatures: set[str] = set()
    participants: dict[str, list[SourceEntry]] = {}
    for row in human_rows:
        participants.setdefault(row.subject_key, []).append(row)
    for participant_rows in participants.values():
        frame = _build_question_thread_semantic_frame(participant_rows)
        if frame is None:
            continue
        semantic_signatures.update(
            _topic_signature(" ".join(frame.primary), "conversation")
        )
    signature = tuple(
        (
            sorted(semantic_signatures)
            + sorted(source_signatures - semantic_signatures)
        )[:24]
    )
    return family, signature, _topic_key(family, signature)


def _summary(rows: list[SourceEntry], qtype: str, reason: str) -> str:
    del reason
    family, _signature, _topic_key_value = _topic_projection(rows)
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
        if source.is_human and _contains_sensitive_moment_source(
            source.normalized_value,
            source.predicate_key,
        ):
            return "sensitive_source_excluded", "retracted"
        if source.lifecycle_status not in SOURCE_LIFECYCLES_USABLE_FOR_MOMENTS:
            return "source_lifecycle_not_usable", "needs_review"
        if conn.execute(
            """
            SELECT 1 FROM memory_ledger_lineage
            WHERE guild_id=? AND target_entry_id=?
              AND lineage_type IN ('correction_of','supersedes','retracts')
            LIMIT 1
            """,
            (source.guild_id, source.entry_id),
        ).fetchone():
            return "source_superseded_or_retracted", "needs_review"
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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
    )


def _episode_event_types(source: SourceEntry) -> tuple[str, ...]:
    """Return typed, content-free semantic roles for one human source."""

    if (
        not source.is_human
        or not _meaningful(
            source.normalized_value,
            source.source_role,
            source.predicate_key,
        )
        or _contains_sensitive_moment_source(
            source.normalized_value,
            source.predicate_key,
        )
    ):
        return ()
    value = str(source.normalized_value or "")
    frame = _parse_contribution_semantic_frame(source)
    frame_type = frame.frame_type if frame is not None else ""
    observed: set[str] = set()
    if (
        frame_type
        in {
            "conditional_plan",
            "plan",
            "proposal",
            "replacement",
            "correction_replacement",
        }
        or _EPISODE_ACTION_RE.search(value)
    ):
        observed.add("action")
    if (
        frame_type
        in {
            "agreement",
            "disagreement",
            "preference",
            "rejection",
            "correction",
            "correction_replacement",
        }
        or _EPISODE_REACTION_RE.search(value)
    ):
        observed.add("reaction")
    if (
        frame_type in {"preference", "replacement", "correction_replacement"}
        or _EPISODE_DECISION_RE.search(value)
    ):
        observed.add("decision")
    if _EPISODE_ASSIGNMENT_RE.search(value):
        observed.add("assignment")
    if _episode_source_closed(value):
        observed.add("outcome")
    if frame_type == "question" or _EPISODE_OPEN_LOOP_RE.search(value):
        observed.add("open_loop")
    return tuple(
        event_type
        for event_type in EPISODE_EVENT_TYPES
        if event_type in observed
    )


def _episode_resume_requested(rows: list[SourceEntry]) -> bool:
    return any(
        row.is_human and _EPISODE_RESUME_RE.search(row.normalized_value or "")
        for row in rows
    )


def _episode_related_link_requested(rows: list[SourceEntry]) -> bool:
    return any(
        row.is_human and _EPISODE_RELATED_RE.search(row.normalized_value or "")
        for row in rows
    )


def _episode_explicitly_closed(rows: list[SourceEntry]) -> bool:
    human_rows = [row for row in rows if row.is_human]
    return bool(
        human_rows
        and any(
            _episode_source_closed(row.normalized_value or "")
            for row in human_rows
        )
        and not any(_EPISODE_OPEN_LOOP_RE.search(row.normalized_value or "") for row in human_rows)
    )


def _episode_source_closed(value: str) -> bool:
    return bool(
        _EPISODE_CLOSE_RE.search(value or "")
        and not _EPISODE_NEGATED_CLOSE_RE.search(value or "")
    )


def _moment_episode_basis(
    conn: sqlite3.Connection,
    moment_id: str,
) -> tuple[dict[str, Any], list[SourceEntry]] | None:
    row = conn.execute(
        """
        SELECT guild_id,channel_id,channel_name,channel_policy,route_mode,
               visibility,public_usable,topic_key,topic_family,topic_signature,
               window_started_at,last_activity_at,lifecycle_status,
               canonical_ledger_entry_id
        FROM memory_moment_windows WHERE moment_id=?
        """,
        (moment_id,),
    ).fetchone()
    if not row or str(row[12] or "") != "finalized":
        return None
    basis = {
        "moment_id": str(moment_id),
        "guild_id": int(row[0] or 0),
        "channel_id": int(row[1] or 0),
        "channel_name": str(row[2] or ""),
        "channel_policy": str(row[3] or "unknown"),
        "route_mode": str(row[4] or "unknown"),
        "visibility": str(row[5] or "unknown"),
        "public_usable": bool(row[6]),
        "topic_key": str(row[7] or ""),
        "topic_family": str(row[8] or ""),
        "topic_signature": _load_sig(str(row[9] or "[]")),
        "window_started_at": str(row[10] or _now()),
        "last_activity_at": str(row[11] or _now()),
        "canonical_ledger_entry_id": str(row[13] or ""),
    }
    rows = _entries(conn, moment_id)
    failure, _failure_lifecycle = _moment_source_failure(
        conn,
        moment_id=moment_id,
        rows=rows,
        guild_id=basis["guild_id"],
        channel_id=basis["channel_id"],
        channel_policy=basis["channel_policy"],
        route_mode=basis["route_mode"],
        visibility=basis["visibility"],
        public_usable=basis["public_usable"],
    )
    if failure:
        return None
    return basis, rows


def _episode_scope_matches(
    episode: tuple[Any, ...],
    basis: dict[str, Any],
) -> bool:
    return (
        int(episode[1] or 0) == int(basis["guild_id"])
        and int(episode[2] or 0) == int(basis["channel_id"])
        and str(episode[3] or "") == str(basis["channel_policy"])
        and str(episode[4] or "") == str(basis["route_mode"])
        and str(episode[5] or "") == str(basis["visibility"])
    )


def _episode_topic_matches(
    episode: tuple[Any, ...],
    basis: dict[str, Any],
) -> bool:
    return _coherent(
        str(basis["topic_family"]),
        tuple(basis["topic_signature"]),
        str(episode[6] or ""),
        _load_sig(str(episode[7] or "[]")),
    )


def _episode_row(
    conn: sqlite3.Connection,
    episode_id: str,
) -> tuple[Any, ...] | None:
    return conn.execute(
        """
        SELECT episode_id,guild_id,channel_id,channel_policy,route_mode,
               visibility,topic_family,topic_signature,lifecycle_status,
               opened_at,last_activity_at,open_loop_count,public_usable
        FROM memory_moment_episodes WHERE episode_id=?
        """,
        (episode_id,),
    ).fetchone()


def _moment_source_digest(
    rows: list[SourceEntry],
    *,
    moment_id: str,
    canonical_ledger_entry_id: str,
) -> str:
    return hashlib.sha256(
        (
            f"{moment_id}\x1f{canonical_ledger_entry_id}\x1f"
            f"{_source_digest(rows)}"
        ).encode("utf-8")
    ).hexdigest()


def _moment_human_participant_keys(
    conn: sqlite3.Connection,
    moment_id: str,
) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            """
            SELECT participant_key FROM memory_moment_participants
            WHERE moment_id=? AND participant_role='human_author'
              AND authored_entry_count>0
            """,
            (moment_id,),
        ).fetchall()
        if str(row[0] or "")
    }


def _episode_participant_overlap(
    conn: sqlite3.Connection,
    episode_id: str,
    moment_id: str,
) -> bool:
    moment_keys = _moment_human_participant_keys(conn, moment_id)
    if not moment_keys:
        return False
    return bool(
        conn.execute(
            """
            SELECT 1 FROM memory_moment_episode_participants
            WHERE episode_id=? AND participant_role='human_author'
              AND participant_key IN (%s)
            LIMIT 1
            """
            % ",".join("?" for _ in moment_keys),
            (episode_id, *sorted(moment_keys)),
        ).fetchone()
    )


def _mark_episode_needs_review(
    conn: sqlite3.Connection,
    episode_id: str,
    *,
    reason: str,
    guild_id: int = 0,
    moment_id: str = "",
    entry_id: str = "",
) -> None:
    conn.execute(
        """
        UPDATE memory_moment_episodes
        SET lifecycle_status='needs_review', public_usable=0,
            finalization_reason=?, updated_at=?
        WHERE episode_id=? AND lifecycle_status IN ('active','finalized')
        """,
        (reason[:120], _now(), episode_id),
    )
    _diag(
        conn,
        guild_id,
        "episode_awaiting_review",
        reason,
        episode_id or moment_id,
        entry_id,
    )


def _rebuild_episode_projection(
    conn: sqlite3.Connection,
    episode_id: str,
) -> bool:
    episode = _episode_row(conn, episode_id)
    if not episode:
        return False
    linked = conn.execute(
        """
        SELECT link.moment_id,link.link_role
        FROM memory_moment_episode_moments link
        JOIN memory_moment_windows window ON window.moment_id=link.moment_id
        WHERE link.episode_id=?
        ORDER BY window.window_started_at,window.last_activity_at,link.moment_id
        """,
        (episode_id,),
    ).fetchall()
    if not linked:
        _mark_episode_needs_review(
            conn,
            episode_id,
            reason="episode_without_moments",
            guild_id=int(episode[1] or 0),
        )
        return False

    bases: list[dict[str, Any]] = []
    source_rows: list[SourceEntry] = []
    for moment_id, _link_role in linked:
        loaded = _moment_episode_basis(conn, str(moment_id))
        if not loaded:
            _mark_episode_needs_review(
                conn,
                episode_id,
                reason="linked_moment_unusable",
                guild_id=int(episode[1] or 0),
                moment_id=str(moment_id),
            )
            return False
        basis, rows = loaded
        if not _episode_scope_matches(episode, basis):
            _mark_episode_needs_review(
                conn,
                episode_id,
                reason="linked_moment_scope_mismatch",
                guild_id=int(episode[1] or 0),
                moment_id=str(moment_id),
            )
            return False
        bases.append(basis)
        source_rows.extend(rows)
        conn.execute(
            """
            UPDATE memory_moment_episode_moments SET source_digest=?
            WHERE episode_id=? AND moment_id=?
            """,
            (
                _moment_source_digest(
                    rows,
                    moment_id=str(moment_id),
                    canonical_ledger_entry_id=str(
                        basis["canonical_ledger_entry_id"]
                    ),
                ),
                episode_id,
                str(moment_id),
            ),
        )

    now = _now()
    conn.execute(
        "DELETE FROM memory_moment_episode_participants WHERE episode_id=?",
        (episode_id,),
    )
    conn.execute(
        "DELETE FROM memory_moment_episode_events WHERE episode_id=?",
        (episode_id,),
    )

    participant_sources: dict[
        tuple[str, str], dict[str, Any]
    ] = {}
    semantic_counts = {event_type: 0 for event_type in EPISODE_EVENT_TYPES}
    outstanding_open_loops = 0
    ordered_sources = sorted(
        source_rows,
        key=lambda source: (
            source.observed_at,
            source.source_sequence,
            source.entry_id,
        ),
    )
    for source in ordered_sources:
        participant_key = (
            source.subject_key if source.is_human else BNL_SUBJECT_KEY
        )
        participant_role = (
            "human_author" if source.is_human else "bnl_participant"
        )
        participant_name = (
            _safe_participant_display_name(source.subject_display_name)
            if source.is_human
            else "BNL-01"
        )
        participant_key_tuple = (participant_key, participant_role)
        state = participant_sources.setdefault(
            participant_key_tuple,
            {
                "safe_display_name": participant_name,
                "first_seen_at": source.observed_at,
                "last_seen_at": source.observed_at,
                "moment_ids": set(),
                "order": len(participant_sources),
            },
        )
        state["last_seen_at"] = max(
            str(state["last_seen_at"]),
            str(source.observed_at),
        )
        source_moment_ids = {
            str(row[0])
            for row in conn.execute(
                """
                SELECT link.moment_id
                FROM memory_moment_episode_moments link
                JOIN memory_moment_members member
                  ON member.moment_id=link.moment_id
                WHERE link.episode_id=? AND member.ledger_entry_id=?
                """,
                (episode_id, source.entry_id),
            ).fetchall()
        }
        state["moment_ids"].update(source_moment_ids)
        event_types = _episode_event_types(source)
        for event_type in event_types:
            semantic_counts[event_type] += 1
            conn.execute(
                """
                INSERT OR REPLACE INTO memory_moment_episode_events(
                  episode_id,ledger_entry_id,participant_key,event_type,
                  observed_at,source_sequence,lifecycle_status,source_digest,
                  created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    episode_id,
                    source.entry_id,
                    participant_key,
                    event_type,
                    source.observed_at,
                    source.source_sequence,
                    source.lifecycle_status,
                    hashlib.sha256(
                        (
                            f"{source.entry_id}\x1f{source.lifecycle_status}\x1f"
                            f"{source.normalized_value}"
                        ).encode("utf-8")
                    ).hexdigest(),
                    now,
                    now,
                ),
            )
        if "outcome" in event_types:
            outstanding_open_loops = (
                0
                if _EPISODE_CLOSE_RE.search(source.normalized_value or "")
                else max(0, outstanding_open_loops - 1)
            )
        if "open_loop" in event_types:
            outstanding_open_loops += 1
        if "assignment" in event_types and "outcome" not in event_types:
            outstanding_open_loops += 1

    for (participant_key, participant_role), state in participant_sources.items():
        conn.execute(
            """
            INSERT INTO memory_moment_episode_participants(
              episode_id,participant_key,participant_role,safe_display_name,
              first_seen_at,last_seen_at,source_moment_count,
              participation_order,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                episode_id,
                participant_key,
                participant_role,
                state["safe_display_name"],
                state["first_seen_at"],
                state["last_seen_at"],
                len(state["moment_ids"]),
                state["order"],
                now,
                now,
            ),
        )

    human_rows = [
        row
        for row in ordered_sources
        if row.is_human
        and _meaningful(
            row.normalized_value,
            row.source_role,
            row.predicate_key,
        )
    ]
    topic_families = {
        str(basis["topic_family"])
        for basis in bases
        if str(basis["topic_family"])
    }
    topic_family = (
        next(iter(topic_families))
        if len(topic_families) == 1
        else "topic_other"
    )
    topic_signature = tuple(
        sorted(
            {
                token
                for basis in bases
                for token in tuple(basis["topic_signature"])
            }
        )
    )[:24]
    last_activity_at = max(
        str(basis["last_activity_at"]) for basis in bases
    )
    public_usable = bool(
        bases
        and all(bool(basis["public_usable"]) for basis in bases)
        and all(
            str(basis["visibility"]) in {"public", "public_safe"}
            for basis in bases
        )
    )
    semantic_types = tuple(
        event_type
        for event_type in EPISODE_EVENT_TYPES
        if semantic_counts[event_type] > 0
    )
    conn.execute(
        """
        UPDATE memory_moment_episodes
        SET topic_family=?,topic_signature=?,last_activity_at=?,
            public_usable=?,moment_count=?,human_entry_count=?,
            participant_count=?,semantic_types_json=?,action_count=?,
            reaction_count=?,decision_count=?,assignment_count=?,
            outcome_count=?,open_loop_count=?,revision=revision+1,
            updated_at=?
        WHERE episode_id=?
        """,
        (
            topic_family,
            _json_sig(topic_signature),
            last_activity_at,
            int(public_usable),
            len(bases),
            len({row.entry_id for row in human_rows}),
            len(
                {
                    row.subject_key
                    for row in human_rows
                    if row.subject_key
                }
            ),
            json.dumps(semantic_types),
            semantic_counts["action"],
            semantic_counts["reaction"],
            semantic_counts["decision"],
            semantic_counts["assignment"],
            semantic_counts["outcome"],
            outstanding_open_loops,
            now,
            episode_id,
        ),
    )
    return True


def _insert_episode_moment(
    conn: sqlite3.Connection,
    episode_id: str,
    basis: dict[str, Any],
    rows: list[SourceEntry],
    *,
    link_role: str,
) -> bool:
    inserted = conn.execute(
        """
        INSERT OR IGNORE INTO memory_moment_episode_moments(
          episode_id,moment_id,link_role,source_digest,linked_at
        ) VALUES(?,?,?,?,?)
        """,
        (
            episode_id,
            basis["moment_id"],
            link_role,
            _moment_source_digest(
                rows,
                moment_id=str(basis["moment_id"]),
                canonical_ledger_entry_id=str(
                    basis["canonical_ledger_entry_id"]
                ),
            ),
            _now(),
        ),
    ).rowcount
    return bool(inserted)


def _open_episode(
    conn: sqlite3.Connection,
    basis: dict[str, Any],
    rows: list[SourceEntry],
) -> str:
    episode_id = stable_episode_id(
        int(basis["guild_id"]),
        int(basis["channel_id"]),
        str(basis["moment_id"]),
    )
    now = _now()
    conn.execute(
        """
        INSERT OR IGNORE INTO memory_moment_episodes(
          episode_id,schema_version,guild_id,channel_id,channel_name,
          channel_policy,route_mode,visibility,public_usable,topic_key,
          topic_family,topic_signature,lifecycle_status,opened_at,
          last_activity_at,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            episode_id,
            EPISODE_SCHEMA_VERSION,
            basis["guild_id"],
            basis["channel_id"],
            basis["channel_name"],
            basis["channel_policy"],
            basis["route_mode"],
            basis["visibility"],
            int(bool(basis["public_usable"])),
            basis["topic_key"],
            basis["topic_family"],
            _json_sig(tuple(basis["topic_signature"])),
            "active",
            basis["window_started_at"],
            basis["last_activity_at"],
            now,
            now,
        ),
    )
    _insert_episode_moment(
        conn,
        episode_id,
        basis,
        rows,
        link_role="opened",
    )
    _rebuild_episode_projection(conn, episode_id)
    _diag(
        conn,
        int(basis["guild_id"]),
        "episode_opened",
        "ok",
        episode_id,
        str(basis["canonical_ledger_entry_id"]),
    )
    return episode_id


def finalize_episode(
    conn: sqlite3.Connection,
    episode_id: str,
    *,
    reason: str = "inactivity",
    finalized_at: str = "",
) -> EpisodeObservationResult:
    episode = _episode_row(conn, episode_id)
    if not episode:
        return EpisodeObservationResult(
            reason_code="missing_episode",
            episode_id=episode_id,
        )
    lifecycle = str(episode[8] or "")
    if lifecycle == "finalized":
        return EpisodeObservationResult(
            "deduplicated",
            "already_finalized",
            episode_id,
        )
    if lifecycle in {"needs_review", "retracted", "superseded", "expired"}:
        return EpisodeObservationResult(
            "deduplicated",
            f"terminal_{lifecycle}",
            episode_id,
        )
    if lifecycle != "active":
        return EpisodeObservationResult(
            "skipped",
            f"not_active_{lifecycle or 'unknown'}",
            episode_id,
        )
    timestamp = finalized_at or _now()
    conn.execute(
        """
        UPDATE memory_moment_episodes
        SET lifecycle_status='finalized',finalized_at=?,
            finalization_reason=?,updated_at=?
        WHERE episode_id=?
        """,
        (timestamp, reason[:120], _now(), episode_id),
    )
    _diag(
        conn,
        int(episode[1] or 0),
        "episode_finalized",
        reason,
        episode_id,
    )
    return EpisodeObservationResult(
        "finalized",
        reason,
        episode_id,
    )


def link_episode_lineage(
    conn: sqlite3.Connection,
    *,
    from_episode_id: str,
    to_episode_id: str,
    relation_type: str,
    evidence_moment_id: str,
    evidence_entry_id: str,
) -> bool:
    """Create an episode edge only from one current, human-owned source."""

    if relation_type not in {
        "split_from",
        "interrupted_from",
        "related_to",
    }:
        return False
    if (
        not from_episode_id
        or not to_episode_id
        or from_episode_id == to_episode_id
    ):
        return False
    source = _fetch_entry(conn, evidence_entry_id)
    if (
        not source
        or not source.is_human
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
            (evidence_moment_id, evidence_entry_id),
        ).fetchone()
        or not conn.execute(
            """
            SELECT 1 FROM memory_moment_episode_moments
            WHERE episode_id=? AND moment_id=?
            """,
            (from_episode_id, evidence_moment_id),
        ).fetchone()
    ):
        return False
    source_episode = _episode_row(conn, from_episode_id)
    target_episode = _episode_row(conn, to_episode_id)
    if (
        not source_episode
        or not target_episode
        or tuple(source_episode[1:6]) != tuple(target_episode[1:6])
        or source.guild_id != int(source_episode[1] or 0)
        or source.channel_id != int(source_episode[2] or 0)
        or source.channel_policy != str(source_episode[3] or "")
        or source.route_mode != str(source_episode[4] or "")
        or source.visibility != str(source_episode[5] or "")
    ):
        return False
    inserted = conn.execute(
        """
        INSERT OR IGNORE INTO memory_moment_episode_lineage(
          from_episode_id,to_episode_id,relation_type,evidence_moment_id,
          evidence_entry_id,created_at
        ) VALUES(?,?,?,?,?,?)
        """,
        (
            from_episode_id,
            to_episode_id,
            relation_type,
            evidence_moment_id,
            evidence_entry_id,
            _now(),
        ),
    ).rowcount
    return bool(inserted)


def _episode_evidence_entry(rows: list[SourceEntry]) -> str:
    for row in rows:
        if row.is_human:
            return row.entry_id
    return ""


def _active_episodes_for_basis(
    conn: sqlite3.Connection,
    basis: dict[str, Any],
) -> list[tuple[Any, ...]]:
    return conn.execute(
        """
        SELECT episode_id,guild_id,channel_id,channel_policy,route_mode,
               visibility,topic_family,topic_signature,lifecycle_status,
               opened_at,last_activity_at,open_loop_count,public_usable
        FROM memory_moment_episodes
        WHERE guild_id=? AND channel_id=? AND channel_policy=?
          AND route_mode=? AND visibility=? AND lifecycle_status='active'
        ORDER BY last_activity_at DESC,episode_id
        """,
        (
            basis["guild_id"],
            basis["channel_id"],
            basis["channel_policy"],
            basis["route_mode"],
            basis["visibility"],
        ),
    ).fetchall()


def _eligible_reopen_candidates(
    conn: sqlite3.Connection,
    basis: dict[str, Any],
) -> list[tuple[Any, ...]]:
    cutoff = (
        _parse_ts(str(basis["window_started_at"]))
        - timedelta(seconds=EPISODE_REOPEN_SECONDS)
    ).isoformat()
    rows = conn.execute(
        """
        SELECT episode_id,guild_id,channel_id,channel_policy,route_mode,
               visibility,topic_family,topic_signature,lifecycle_status,
               opened_at,last_activity_at,open_loop_count,public_usable
        FROM memory_moment_episodes
        WHERE guild_id=? AND channel_id=? AND channel_policy=?
          AND route_mode=? AND visibility=? AND lifecycle_status='finalized'
          AND last_activity_at>=?
        ORDER BY last_activity_at DESC,episode_id
        """,
        (
            basis["guild_id"],
            basis["channel_id"],
            basis["channel_policy"],
            basis["route_mode"],
            basis["visibility"],
            cutoff,
        ),
    ).fetchall()
    return [
        row
        for row in rows
        if _episode_topic_matches(row, basis)
        and _episode_participant_overlap(
            conn,
            str(row[0]),
            str(basis["moment_id"]),
        )
    ]


def _eligible_related_candidates(
    conn: sqlite3.Connection,
    basis: dict[str, Any],
) -> list[tuple[Any, ...]]:
    cutoff = (
        _parse_ts(str(basis["window_started_at"]))
        - timedelta(seconds=EPISODE_REOPEN_SECONDS)
    ).isoformat()
    rows = conn.execute(
        """
        SELECT episode_id,guild_id,channel_id,channel_policy,route_mode,
               visibility,topic_family,topic_signature,lifecycle_status,
               opened_at,last_activity_at,open_loop_count,public_usable
        FROM memory_moment_episodes
        WHERE guild_id=? AND channel_id=? AND channel_policy=?
          AND route_mode=? AND visibility=? AND lifecycle_status='finalized'
          AND last_activity_at>=?
        ORDER BY last_activity_at DESC,episode_id
        """,
        (
            basis["guild_id"],
            basis["channel_id"],
            basis["channel_policy"],
            basis["route_mode"],
            basis["visibility"],
            cutoff,
        ),
    ).fetchall()
    return [
        row
        for row in rows
        if _episode_participant_overlap(
            conn,
            str(row[0]),
            str(basis["moment_id"]),
        )
    ]


def reopen_episode(
    conn: sqlite3.Connection,
    *,
    episode_id: str,
    moment_id: str,
) -> EpisodeObservationResult:
    loaded = _moment_episode_basis(conn, moment_id)
    episode = _episode_row(conn, episode_id)
    if not loaded or not episode:
        return EpisodeObservationResult(
            reason_code="missing_episode_or_moment",
            episode_id=episode_id,
            moment_id=moment_id,
        )
    basis, rows = loaded
    if (
        str(episode[8] or "") != "finalized"
        or not _episode_scope_matches(episode, basis)
        or not _episode_topic_matches(episode, basis)
        or not _episode_participant_overlap(conn, episode_id, moment_id)
        or not _episode_resume_requested(rows)
    ):
        return EpisodeObservationResult(
            reason_code="reopen_evidence_invalid",
            episode_id=episode_id,
            moment_id=moment_id,
        )
    active = _active_episodes_for_basis(conn, basis)
    if active:
        return EpisodeObservationResult(
            reason_code="active_episode_already_present",
            episode_id=episode_id,
            moment_id=moment_id,
        )
    conn.execute(
        """
        UPDATE memory_moment_episodes
        SET lifecycle_status='active',finalized_at=NULL,
            finalization_reason='',reopen_count=reopen_count+1,
            updated_at=?
        WHERE episode_id=?
        """,
        (_now(), episode_id),
    )
    _insert_episode_moment(
        conn,
        episode_id,
        basis,
        rows,
        link_role="reopened",
    )
    if not _rebuild_episode_projection(conn, episode_id):
        return EpisodeObservationResult(
            "needs_review",
            "reopen_projection_failed",
            episode_id,
            moment_id,
        )
    _diag(
        conn,
        int(basis["guild_id"]),
        "episode_reopened",
        "explicit_resume_source",
        episode_id,
        _episode_evidence_entry(rows),
    )
    return EpisodeObservationResult(
        "reopened",
        "explicit_resume_source",
        episode_id,
        moment_id,
    )


def observe_finalized_moment_episode(
    conn: sqlite3.Connection,
    moment_id: str,
    *,
    require_shadow_gate: bool = True,
) -> EpisodeObservationResult:
    """Attach one finalized Moment to the existing episodic lifecycle."""

    if require_shadow_gate and not shadow_enabled():
        return EpisodeObservationResult(
            reason_code="moment_gate_disabled",
            moment_id=moment_id,
        )
    loaded = _moment_episode_basis(conn, moment_id)
    if not loaded:
        return EpisodeObservationResult(
            reason_code="moment_not_finalized_or_usable",
            moment_id=moment_id,
        )
    basis, rows = loaded
    existing = conn.execute(
        """
        SELECT episode_id FROM memory_moment_episode_moments
        WHERE moment_id=? ORDER BY linked_at,episode_id LIMIT 1
        """,
        (moment_id,),
    ).fetchone()
    if existing:
        return EpisodeObservationResult(
            "deduplicated",
            "moment_already_linked",
            str(existing[0]),
            moment_id,
        )

    try:
        conn.execute("SAVEPOINT episode_observe")
        active = _active_episodes_for_basis(conn, basis)
        if len(active) > 1:
            for episode in active:
                _mark_episode_needs_review(
                    conn,
                    str(episode[0]),
                    reason="multiple_active_episodes_in_scope",
                    guild_id=int(basis["guild_id"]),
                    moment_id=moment_id,
                )
            conn.execute("RELEASE episode_observe")
            return EpisodeObservationResult(
                "needs_review",
                "multiple_active_episodes_in_scope",
                moment_id=moment_id,
            )

        prior_episode: tuple[Any, ...] | None = active[0] if active else None
        if prior_episode and (
            _parse_ts(str(basis["window_started_at"]))
            - _parse_ts(str(prior_episode[10] or ""))
        ).total_seconds() > EPISODE_INACTIVITY_SECONDS:
            finalize_episode(
                conn,
                str(prior_episode[0]),
                reason="episode_inactivity",
                finalized_at=str(basis["window_started_at"]),
            )
            prior_episode = None

        if prior_episode and _episode_topic_matches(prior_episode, basis):
            episode_id = str(prior_episode[0])
            _insert_episode_moment(
                conn,
                episode_id,
                basis,
                rows,
                link_role="extended",
            )
            if not _rebuild_episode_projection(conn, episode_id):
                conn.execute("RELEASE episode_observe")
                return EpisodeObservationResult(
                    "needs_review",
                    "extension_projection_failed",
                    episode_id,
                    moment_id,
                )
            _diag(
                conn,
                int(basis["guild_id"]),
                "episode_extended",
                "topic_coherent",
                episode_id,
                str(basis["canonical_ledger_entry_id"]),
            )
            outcome = EpisodeObservationResult(
                "extended",
                "topic_coherent",
                episode_id,
                moment_id,
            )
        elif prior_episode:
            old_episode_id = str(prior_episode[0])
            relation_type = (
                "interrupted_from"
                if int(prior_episode[11] or 0) > 0
                else "split_from"
            )
            finalize_episode(
                conn,
                old_episode_id,
                reason=(
                    "topic_interruption"
                    if relation_type == "interrupted_from"
                    else "topic_change"
                ),
                finalized_at=str(basis["window_started_at"]),
            )
            episode_id = _open_episode(conn, basis, rows)
            evidence_entry_id = _episode_evidence_entry(rows)
            if evidence_entry_id:
                link_episode_lineage(
                    conn,
                    from_episode_id=episode_id,
                    to_episode_id=old_episode_id,
                    relation_type=relation_type,
                    evidence_moment_id=moment_id,
                    evidence_entry_id=evidence_entry_id,
                )
            conn.execute(
                """
                UPDATE memory_moment_episodes
                SET split_count=split_count+1,updated_at=?
                WHERE episode_id=?
                """,
                (_now(), old_episode_id),
            )
            _diag(
                conn,
                int(basis["guild_id"]),
                "episode_split",
                relation_type,
                episode_id,
                evidence_entry_id,
            )
            outcome = EpisodeObservationResult(
                "split",
                relation_type,
                episode_id,
                moment_id,
            )
        else:
            reopen_candidates = (
                _eligible_reopen_candidates(conn, basis)
                if _episode_resume_requested(rows)
                else []
            )
            if len(reopen_candidates) == 1:
                outcome = reopen_episode(
                    conn,
                    episode_id=str(reopen_candidates[0][0]),
                    moment_id=moment_id,
                )
            else:
                episode_id = _open_episode(conn, basis, rows)
                related_candidates = (
                    _eligible_related_candidates(conn, basis)
                    if (
                        not _episode_resume_requested(rows)
                        and _episode_related_link_requested(rows)
                    )
                    else []
                )
                evidence_entry_id = _episode_evidence_entry(rows)
                if len(related_candidates) == 1 and evidence_entry_id:
                    link_episode_lineage(
                        conn,
                        from_episode_id=episode_id,
                        to_episode_id=str(related_candidates[0][0]),
                        relation_type="related_to",
                        evidence_moment_id=moment_id,
                        evidence_entry_id=evidence_entry_id,
                    )
                    _diag(
                        conn,
                        int(basis["guild_id"]),
                        "episode_related",
                        "explicit_unique_related_source",
                        episode_id,
                        evidence_entry_id,
                    )
                elif len(related_candidates) > 1:
                    _diag(
                        conn,
                        int(basis["guild_id"]),
                        "episode_related_skipped",
                        "ambiguous_related_candidates",
                        episode_id,
                        evidence_entry_id,
                    )
                if len(reopen_candidates) > 1:
                    _diag(
                        conn,
                        int(basis["guild_id"]),
                        "episode_reopen_skipped",
                        "ambiguous_reopen_candidates",
                        episode_id,
                        _episode_evidence_entry(rows),
                    )
                    reason = "ambiguous_reopen_candidates"
                else:
                    reason = (
                        "resume_source_without_unique_episode"
                        if _episode_resume_requested(rows)
                        else "new_episode"
                    )
                outcome = EpisodeObservationResult(
                    "opened",
                    reason,
                    episode_id,
                    moment_id,
                )

        if (
            outcome.episode_id
            and outcome.outcome
            not in {"needs_review", "error", "skipped"}
            and _episode_explicitly_closed(rows)
        ):
            finalize_episode(
                conn,
                outcome.episode_id,
                reason="explicit_outcome",
                finalized_at=str(basis["last_activity_at"]),
            )
        conn.execute("RELEASE episode_observe")
        return outcome
    except Exception:
        try:
            conn.execute("ROLLBACK TO episode_observe")
            conn.execute("RELEASE episode_observe")
        except Exception:
            pass
        try:
            _diag(
                conn,
                int(basis["guild_id"]),
                "episode_processing_error",
                "exception",
                moment_id,
            )
        except Exception:
            pass
        return EpisodeObservationResult(
            "error",
            "exception",
            moment_id=moment_id,
        )


def backfill_episodic_lifecycle(
    conn: sqlite3.Connection,
) -> dict[str, int]:
    counts = {
        "observed": 0,
        "deduplicated": 0,
        "errors": 0,
    }
    for (moment_id,) in conn.execute(
        """
        SELECT moment_id FROM memory_moment_windows
        WHERE lifecycle_status='finalized'
        ORDER BY window_started_at,last_activity_at,moment_id
        """
    ).fetchall():
        result = observe_finalized_moment_episode(
            conn,
            str(moment_id),
            require_shadow_gate=False,
        )
        if result.outcome == "error":
            counts["errors"] += 1
        elif result.outcome == "deduplicated":
            counts["deduplicated"] += 1
        elif result.outcome not in {"skipped", "needs_review"}:
            counts["observed"] += 1
    return counts


def sweep_expired_episodes(
    conn: sqlite3.Connection,
    *,
    guild_id: int | None = None,
    now: str | None = None,
) -> list[EpisodeObservationResult]:
    if not shadow_enabled() or not ledger_shadow_enabled():
        return []
    ensure_moment_schema(conn)
    base = _parse_ts(now or _now())
    params: list[Any] = []
    where = "lifecycle_status='active'"
    if guild_id is not None:
        where += " AND guild_id=?"
        params.append(guild_id)
    results = []
    for episode_id, last_activity_at in conn.execute(
        f"""
        SELECT episode_id,last_activity_at
        FROM memory_moment_episodes WHERE {where}
        """,
        params,
    ).fetchall():
        if (
            base - _parse_ts(str(last_activity_at or ""))
        ).total_seconds() >= EPISODE_INACTIVITY_SECONDS:
            results.append(
                finalize_episode(
                    conn,
                    str(episode_id),
                    reason="episode_inactivity",
                    finalized_at=base.isoformat(),
                )
            )
    return results


def active_episode_for_assessment(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    channel_id: int,
    channel_policy: str,
    route_mode: str,
    topic_text: str,
    participant_keys: tuple[str, ...] = (),
    now: str | None = None,
) -> ActiveEpisodeReference | None:
    """Select one active episode without creating schema or changing state."""

    if (
        not shadow_enabled()
        or not ledger_shadow_enabled()
        or not _table_exists(conn, "memory_moment_episodes")
    ):
        return None
    candidates = conn.execute(
        """
        SELECT episode_id,guild_id,channel_id,channel_policy,route_mode,
               visibility,topic_family,topic_signature,lifecycle_status,
               opened_at,last_activity_at,open_loop_count,public_usable,
               participant_count,semantic_types_json
        FROM memory_moment_episodes
        WHERE guild_id=? AND channel_id=? AND channel_policy=?
          AND route_mode=? AND lifecycle_status='active'
        ORDER BY last_activity_at DESC,episode_id
        """,
        (
            int(guild_id or 0),
            int(channel_id or 0),
            str(channel_policy or "unknown"),
            str(route_mode or "unknown"),
        ),
    ).fetchall()
    if len(candidates) != 1:
        return None
    candidate = candidates[0]
    if (
        _parse_ts(now or _now())
        - _parse_ts(str(candidate[10] or ""))
    ).total_seconds() >= EPISODE_INACTIVITY_SECONDS:
        return None
    signature = _topic_signature(topic_text, "conversation")
    family = _topic_family(topic_text, "conversation")
    if signature and not _coherent(
        family,
        signature,
        str(candidate[6] or ""),
        _load_sig(str(candidate[7] or "[]")),
    ):
        return None
    scoped_participant_keys = tuple(
        sorted(
            {
                str(key)
                for key in participant_keys
                if re.fullmatch(r"discord_user:[1-9]\d*", str(key or ""))
            }
        )
    )
    if scoped_participant_keys and not conn.execute(
        """
        SELECT 1 FROM memory_moment_episode_participants
        WHERE episode_id=? AND participant_role='human_author'
          AND participant_key IN (%s)
        LIMIT 1
        """
        % ",".join("?" for _ in scoped_participant_keys),
        (str(candidate[0]), *scoped_participant_keys),
    ).fetchone():
        return None
    source_moments = tuple(
        str(row[0])
        for row in conn.execute(
            """
            SELECT link.moment_id
            FROM memory_moment_episode_moments link
            JOIN memory_moment_windows window
              ON window.moment_id=link.moment_id
            WHERE link.episode_id=? AND window.lifecycle_status='finalized'
            ORDER BY window.window_started_at,link.moment_id
            """,
            (str(candidate[0]),),
        ).fetchall()
    )
    link_count = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM memory_moment_episode_moments
            WHERE episode_id=?
            """,
            (str(candidate[0]),),
        ).fetchone()[0]
        or 0
    )
    if not source_moments or len(source_moments) != link_count:
        return None
    try:
        semantic_types = tuple(
            event_type
            for event_type in json.loads(str(candidate[14] or "[]"))
            if event_type in EPISODE_EVENT_TYPES
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return ActiveEpisodeReference(
        episode_id=str(candidate[0]),
        lifecycle_status=str(candidate[8] or ""),
        source_moment_ids=source_moments,
        participant_count=max(0, int(candidate[13] or 0)),
        open_loop_count=max(0, int(candidate[11] or 0)),
        semantic_types=semantic_types,
    )


def render_active_episode_canary_context(
    conn: sqlite3.Connection,
    *,
    guild_id: int,
    channel_id: int,
    channel_policy: str,
    route_mode: str,
    topic_text: str,
    participant_keys: tuple[str, ...] = (),
    now: str | None = None,
) -> str:
    """Render source-revalidated aggregate episode context for sealed testing.

    This is deliberately narrower than the public Moment gist canary. It may
    describe one active episode only when every linked source is still usable
    inside the exact same sealed channel. It never renders source text,
    participant names, ids, Moment ids, or episode ids.
    """

    if (
        str(channel_policy or "").strip().lower() != "sealed_test"
        or int(guild_id or 0) <= 0
        or int(channel_id or 0) <= 0
    ):
        return ""
    reference = active_episode_for_assessment(
        conn,
        guild_id=int(guild_id or 0),
        channel_id=int(channel_id or 0),
        channel_policy="sealed_test",
        route_mode=str(route_mode or "unknown"),
        topic_text=str(topic_text or "")[:8000],
        participant_keys=participant_keys,
        now=now,
    )
    if reference is None:
        return ""
    episode = conn.execute(
        """
        SELECT topic_family,visibility,public_usable,lifecycle_status,
               participant_count,open_loop_count,semantic_types_json
        FROM memory_moment_episodes
        WHERE episode_id=? AND guild_id=? AND channel_id=?
          AND channel_policy='sealed_test' AND route_mode=?
        """,
        (
            reference.episode_id,
            int(guild_id or 0),
            int(channel_id or 0),
            str(route_mode or "unknown"),
        ),
    ).fetchone()
    if (
        not episode
        or str(episode[1] or "") != "sealed_test"
        or bool(episode[2])
        or str(episode[3] or "") != "active"
        or max(0, int(episode[4] or 0)) != reference.participant_count
        or max(0, int(episode[5] or 0)) != reference.open_loop_count
    ):
        return ""
    try:
        semantic_types = tuple(
            value
            for value in json.loads(str(episode[6] or "[]"))
            if value in EPISODE_EVENT_TYPES
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return ""
    if semantic_types != reference.semantic_types:
        return ""

    for moment_id in reference.source_moment_ids:
        window = conn.execute(
            """
            SELECT guild_id,channel_id,channel_policy,route_mode,visibility,
                   public_usable,lifecycle_status,summary,
                   canonical_ledger_entry_id
            FROM memory_moment_windows
            WHERE moment_id=?
            """,
            (moment_id,),
        ).fetchone()
        if (
            not window
            or int(window[0] or 0) != int(guild_id or 0)
            or int(window[1] or 0) != int(channel_id or 0)
            or str(window[2] or "") != "sealed_test"
            or str(window[3] or "") != str(route_mode or "unknown")
            or str(window[4] or "") != "sealed_test"
            or bool(window[5])
            or str(window[6] or "") != "finalized"
            or not _is_safe_gist_summary(str(window[7] or ""))
            or not str(window[8] or "")
        ):
            return ""
        rows = _entries(conn, moment_id)
        failure, _failure_lifecycle = _moment_source_failure(
            conn,
            moment_id=moment_id,
            rows=rows,
            guild_id=int(guild_id or 0),
            channel_id=int(channel_id or 0),
            channel_policy="sealed_test",
            route_mode=str(route_mode or "unknown"),
            visibility="sealed_test",
            public_usable=False,
        )
        if failure:
            return ""
        if any(
            conn.execute(
                """
                SELECT 1 FROM memory_ledger_lineage
                WHERE guild_id=? AND target_entry_id=?
                  AND lineage_type IN (
                    'correction_of','supersedes','retracts'
                  )
                LIMIT 1
                """,
                (int(guild_id or 0), source.entry_id),
            ).fetchone()
            for source in rows
        ):
            return ""
        canonical = conn.execute(
            """
            SELECT guild_id,source_table,source_row_id,entry_type,channel_id,
                   channel_policy,route_mode,visibility,public_usable,
                   lifecycle_status,normalized_value
            FROM memory_ledger_entries WHERE entry_id=?
            """,
            (str(window[8] or ""),),
        ).fetchone()
        if (
            not canonical
            or int(canonical[0] or 0) != int(guild_id or 0)
            or str(canonical[1] or "") != "memory_moment_windows"
            or str(canonical[2] or "") != moment_id
            or str(canonical[3] or "") != "shared_moment"
            or int(canonical[4] or 0) != int(channel_id or 0)
            or str(canonical[5] or "") != "sealed_test"
            or str(canonical[6] or "") != str(route_mode or "unknown")
            or str(canonical[7] or "") != "sealed_test"
            or bool(canonical[8])
            or str(canonical[9] or "")
            not in SOURCE_LIFECYCLES_USABLE_FOR_MOMENTS
        ):
            return ""
        try:
            canonical_value = json.loads(str(canonical[10] or ""))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ""
        if (
            not isinstance(canonical_value, dict)
            or canonical_value.get("schema") != MOMENT_SCHEMA_VERSION
            or canonical_value.get("moment_id") != moment_id
            or canonical_value.get("summary") != str(window[7] or "")
            or canonical_value.get("public_usable") is not False
            or canonical_value.get("lifecycle_status") != "finalized"
        ):
            return ""

    topic_family = str(episode[0] or "topic_other")
    topic_label = TOPIC_GIST_LABELS.get(
        topic_family,
        TOPIC_GIST_LABELS["topic_other"],
    )
    semantic_label = (
        ", ".join(reference.semantic_types)
        if reference.semantic_types
        else "ongoing discussion"
    )
    return (
        "[Active same-channel episode signal; aggregate continuity only, "
        "never quotation or durable-fact authority]\n"
        f"- Continuity topic: {topic_label}.\n"
        f"- Shared human participants: {reference.participant_count}.\n"
        f"- Observed conversation roles: {semantic_label}.\n"
        f"- Unresolved open loops: {reference.open_loop_count}.\n"
        "- Resolve all details from the selected current-room evidence. "
        "Current requests and corrections outrank this aggregate signal."
    )


def finalize_moment(
    conn: sqlite3.Connection,
    moment_id: str,
    *,
    ensure_schema: bool = True,
) -> MomentObservationResult:
    if ensure_schema:
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
    topic_family, topic_signature, topic_key = _topic_projection(rows)
    summary = _summary(rows, qtype, reason)
    participants = [LedgerParticipant(p[0], p[1] or "", p[2], int(p[3] or 0)) for p in conn.execute("SELECT participant_key,safe_display_name,participant_role,participation_order FROM memory_moment_participants WHERE moment_id=? ORDER BY participation_order,participant_key", (moment_id,)).fetchall()]
    visibility = win[8] or "unknown"
    public_usable = bool(win[9]) and VIS_RANK.get(visibility, 5) <= VIS_RANK.get("public_safe", 0)
    value = json.dumps({
        "schema": MOMENT_SCHEMA_VERSION, "moment_id": moment_id, "summary": summary, "window_started_at": win[6], "last_activity_at": win[7],
        "topic_key": topic_key, "qualification_type": qtype, "qualification_reason": reason, "salience": salience,
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
        UPDATE memory_moment_windows SET topic_key=?,topic_family=?,topic_signature=?,
            lifecycle_status='finalized', finalized_at=?, qualification_type=?, qualification_reason=?,
            salience=?, summary=?, canonical_ledger_entry_id=?, updated_at=? WHERE moment_id=?
        """,
        (
            topic_key,
            topic_family,
            _json_sig(topic_signature),
            _now(),
            qtype,
            reason,
            salience,
            summary,
            moment_entry_id,
            _now(),
            moment_id,
        ),
    )
    _replace_moment_contributions(
        conn,
        moment_id,
        rows,
        public_usable=public_usable,
    )
    _diag(conn, int(win[0] or 0), "window_finalized", reason, moment_id, moment_entry_id)
    observe_finalized_moment_episode(conn, moment_id)
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
        else _recall_topic_focus(topic_text)
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
    episode_tables = {
        "memory_moment_episodes",
        "memory_moment_episode_moments",
        "memory_moment_episode_participants",
        "memory_moment_episode_events",
        "memory_moment_episode_lineage",
    }
    episode_defaults: dict[str, Any] = {
        "episode_schema_present": False,
        "episodes_by_lifecycle": {},
        "active_episodes": 0,
        "finalized_episodes": 0,
        "episodes_awaiting_review": 0,
        "episodes_with_open_loops": 0,
        "episode_events_by_type": {},
        "episode_lineage_by_type": {},
        "episode_moment_links": 0,
        "episode_source_links": 0,
        "episode_extensions": 0,
        "episode_splits": 0,
        "episode_reopens": 0,
        "episode_processing_errors": 0,
        "episode_duplicate_moment_links": 0,
        "episode_active_scope_duplicates": 0,
        "episode_cross_scope_violations": 0,
        "episode_orphaned_moment_links": 0,
        "episode_orphaned_source_links": 0,
        "episode_participant_link_violations": 0,
    }
    report.update(episode_defaults)
    if all(_table_exists(conn, table) for table in episode_tables):
        episode_where = ""
        episode_params: list[Any] = []
        if guild_id is not None:
            episode_where = " WHERE guild_id=?"
            episode_params = [guild_id]
        episode_scoped = "episode.guild_id=? AND " if guild_id is not None else ""
        episode_scoped_params = [guild_id] if guild_id is not None else []
        report.update(
            {
                "episode_schema_present": True,
                "episodes_by_lifecycle": dict(
                    conn.execute(
                        f"""
                        SELECT lifecycle_status,COUNT(*)
                        FROM memory_moment_episodes{episode_where}
                        GROUP BY lifecycle_status
                        """,
                        episode_params,
                    ).fetchall()
                ),
                "active_episodes": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_episodes
                    {episode_where + (' AND' if episode_where else ' WHERE')}
                    lifecycle_status='active'
                    """,
                    episode_params,
                ),
                "finalized_episodes": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_episodes
                    {episode_where + (' AND' if episode_where else ' WHERE')}
                    lifecycle_status='finalized'
                    """,
                    episode_params,
                ),
                "episodes_awaiting_review": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_episodes
                    {episode_where + (' AND' if episode_where else ' WHERE')}
                    lifecycle_status='needs_review'
                    """,
                    episode_params,
                ),
                "episodes_with_open_loops": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_episodes
                    {episode_where + (' AND' if episode_where else ' WHERE')}
                    lifecycle_status='active' AND open_loop_count>0
                    """,
                    episode_params,
                ),
                "episode_events_by_type": dict(
                    conn.execute(
                        f"""
                        SELECT event.event_type,COUNT(*)
                        FROM memory_moment_episode_events event
                        JOIN memory_moment_episodes episode
                          ON episode.episode_id=event.episode_id
                        WHERE {episode_scoped}1=1
                        GROUP BY event.event_type
                        """,
                        episode_scoped_params,
                    ).fetchall()
                ),
                "episode_lineage_by_type": dict(
                    conn.execute(
                        f"""
                        SELECT lineage.relation_type,COUNT(*)
                        FROM memory_moment_episode_lineage lineage
                        JOIN memory_moment_episodes episode
                          ON episode.episode_id=lineage.from_episode_id
                        WHERE {episode_scoped}1=1
                        GROUP BY lineage.relation_type
                        """,
                        episode_scoped_params,
                    ).fetchall()
                ),
                "episode_moment_links": one(
                    f"""
                    SELECT COUNT(*)
                    FROM memory_moment_episode_moments link
                    JOIN memory_moment_episodes episode
                      ON episode.episode_id=link.episode_id
                    WHERE {episode_scoped}1=1
                    """,
                    episode_scoped_params,
                ),
                "episode_source_links": one(
                    f"""
                    SELECT COUNT(*)
                    FROM memory_moment_episode_events event
                    JOIN memory_moment_episodes episode
                      ON episode.episode_id=event.episode_id
                    WHERE {episode_scoped}1=1
                    """,
                    episode_scoped_params,
                ),
                "episode_extensions": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_diagnostics
                    {where + (' AND' if where else ' WHERE')}
                    event_type='episode_extended'
                    """,
                    params,
                ),
                "episode_splits": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_diagnostics
                    {where + (' AND' if where else ' WHERE')}
                    event_type='episode_split'
                    """,
                    params,
                ),
                "episode_reopens": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_diagnostics
                    {where + (' AND' if where else ' WHERE')}
                    event_type='episode_reopened'
                    """,
                    params,
                ),
                "episode_processing_errors": one(
                    f"""
                    SELECT COUNT(*) FROM memory_moment_diagnostics
                    {where + (' AND' if where else ' WHERE')}
                    event_type='episode_processing_error'
                    """,
                    params,
                ),
                "episode_duplicate_moment_links": one(
                    f"""
                    SELECT COUNT(*) FROM (
                      SELECT link.moment_id,
                             COUNT(DISTINCT link.episode_id) AS c
                      FROM memory_moment_episode_moments link
                      JOIN memory_moment_episodes episode
                        ON episode.episode_id=link.episode_id
                      WHERE {episode_scoped}1=1
                      GROUP BY link.moment_id HAVING c>1
                    )
                    """,
                    episode_scoped_params,
                ),
                "episode_active_scope_duplicates": one(
                    f"""
                    SELECT COALESCE(SUM(c-1),0) FROM (
                      SELECT COUNT(*) AS c FROM memory_moment_episodes episode
                      WHERE {episode_scoped}
                        episode.lifecycle_status='active'
                      GROUP BY episode.guild_id,episode.channel_id,
                               episode.channel_policy,episode.route_mode,
                               episode.visibility HAVING c>1
                    )
                    """,
                    episode_scoped_params,
                ),
                "episode_cross_scope_violations": one(
                    f"""
                    SELECT COUNT(*)
                    FROM memory_moment_episode_moments link
                    JOIN memory_moment_episodes episode
                      ON episode.episode_id=link.episode_id
                    JOIN memory_moment_windows window
                      ON window.moment_id=link.moment_id
                    WHERE {episode_scoped}(
                      episode.guild_id<>window.guild_id
                      OR episode.channel_id<>window.channel_id
                      OR episode.channel_policy<>window.channel_policy
                      OR episode.route_mode<>window.route_mode
                      OR episode.visibility<>window.visibility
                    )
                    """,
                    episode_scoped_params,
                ),
                "episode_orphaned_moment_links": one(
                    f"""
                    SELECT COUNT(*)
                    FROM memory_moment_episode_moments link
                    JOIN memory_moment_episodes episode
                      ON episode.episode_id=link.episode_id
                    LEFT JOIN memory_moment_windows window
                      ON window.moment_id=link.moment_id
                    WHERE {episode_scoped}window.moment_id IS NULL
                    """,
                    episode_scoped_params,
                ),
                "episode_orphaned_source_links": one(
                    f"""
                    SELECT COUNT(*)
                    FROM memory_moment_episode_events event
                    JOIN memory_moment_episodes episode
                      ON episode.episode_id=event.episode_id
                    LEFT JOIN memory_ledger_entries source
                      ON source.entry_id=event.ledger_entry_id
                    WHERE {episode_scoped}source.entry_id IS NULL
                    """,
                    episode_scoped_params,
                ),
                "episode_participant_link_violations": one(
                    f"""
                    SELECT COUNT(*)
                    FROM memory_moment_episode_events event
                    JOIN memory_moment_episodes episode
                      ON episode.episode_id=event.episode_id
                    LEFT JOIN memory_moment_episode_participants participant
                      ON participant.episode_id=event.episode_id
                     AND participant.participant_key=event.participant_key
                    WHERE {episode_scoped}
                      participant.participant_key IS NULL
                    """,
                    episode_scoped_params,
                ),
            }
        )
    return report
