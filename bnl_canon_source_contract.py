"""Versioned in-world canon/source contract for BNL compatibility callers."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass, replace
from enum import Enum
from datetime import datetime, timezone, timedelta
import hashlib
import json
import re
from typing import Any, Iterable, Mapping, Sequence
import unicodedata

CANON_SOURCE_CONTRACT_VERSION = "canon_source_contract_v1"
HYBRID_CANON_CLAIM_CONTRACT_VERSION = "hybrid_canon_claim_v1"
CANON_CLAIM_ID_NAMESPACE = "bnl_canon_claim_identity_v1"
LEGACY_CANON_ADAPTER_VERSION = "legacy_canon_adapter_v1"
DECLARED_CANON_ADAPTER_VERSION = "declared_canon_adapter_v1"
BROADCAST_CANON_ADAPTER_VERSION = "broadcast_declared_adapter_v1"
LIVING_CANON_ADAPTER_VERSION = "living_canon_adapter_v1"
OPEN_SIGNAL_ADAPTER_VERSION = "open_signal_adapter_v3"
WEBSITE_LORE_ADAPTER_VERSION = "website_lore_review_adapter_v1"
LIVING_CANON_RECURRENCE_VERSION = "living_canon_recurrence_v1"
PUBLIC_ASSESSMENT_EVIDENCE_VERSION = "public_assessment_evidence_v3"
ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION = "canon_entity_account_binding_v1"


class CanonStatus(str, Enum):
    LEGACY = "legacy"
    DECLARED = "declared"
    LIVING = "living"
    OPEN_SIGNAL = "open_signal"


class CanonDomain(str, Enum):
    REAL_COMMUNITY = "real_community"
    BROADCAST_HISTORY = "broadcast_history"
    OPERATIONAL = "operational"
    LORE = "lore"
    HYBRID = "hybrid"


class ClaimKind(str, Enum):
    IDENTITY = "identity"
    ROLE = "role"
    STANDING = "standing"
    RELATIONSHIP = "relationship"
    CONTRIBUTION = "contribution"
    EVENT = "event"
    CURRENT_STATE = "current_state"
    BEHAVIOR_PATTERN = "behavior_pattern"
    TRADITION_OR_JOKE = "tradition_or_joke"
    WORLD_RULE = "world_rule"
    OTHER = "other"


class ClaimLifecycle(str, Enum):
    CANDIDATE = "candidate"
    PROVISIONAL = "provisional"
    ESTABLISHED = "established"
    CONTESTED = "contested"
    SUPERSEDED = "superseded"
    RETIRED = "retired"
    RESOLVED = "resolved"
    REVIEW_ONLY = "review_only"

class SourceClass(str, Enum):
    OWNER_CORRECTION = "owner_correction"
    APPROVED_CANON = "approved_canon"
    FIRST_PARTY_RECORD = "first_party_record"
    RUNTIME_OBSERVATION = "runtime_observation"
    PUBLIC_OBSERVATION = "public_observation"
    EVIDENCE_PROJECTION = "evidence_projection"
    SOURCE_FILE_PROJECTION = "source_file_projection"
    DOSSIER_PROJECTION = "dossier_projection"
    ENTITY_EVIDENCE_PROJECTION = "entity_evidence_projection"
    DERIVED_SUMMARY = "derived_summary"
    LEGACY_SOURCE_BLIND = "legacy_source_blind"

class Visibility(str, Enum):
    PUBLIC = "public"
    PUBLIC_SAFE = "public_safe"
    REFERENCE_CANON = "reference_canon"
    INTERNAL = "internal"
    PRIVATE = "private"
    MOD = "mod"
    SEALED_TEST = "sealed_test"
    PROTECTED = "protected"
    AI_IMAGE_TOOL = "ai_image_tool"
    UNKNOWN = "unknown"

class Confidence(str, Enum):
    APPROVED = "approved"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"

@dataclass(frozen=True)
class SubjectIdentity:
    key: str
    name: str
    aliases: tuple[str, ...] = ()

@dataclass(frozen=True)
class FridaySchedule:
    intake_begins: str
    show_begins: str
    first_track_target: str

@dataclass(frozen=True)
class CanonFact:
    subject: SubjectIdentity
    predicate: str
    value: Any
    source_class: SourceClass = SourceClass.APPROVED_CANON
    visibility: Visibility = Visibility.PUBLIC_SAFE
    confidence: Confidence = Confidence.APPROVED

@dataclass(frozen=True)
class SourceClaim:
    claim_id: str
    subject: SubjectIdentity
    predicate: str
    value: Any
    source_class: SourceClass
    visibility: Visibility = Visibility.PUBLIC_SAFE
    confidence: Confidence = Confidence.MEDIUM
    observed_at: datetime | None = None
    valid: bool = True
    correction_of: tuple[str, ...] = ()
    supersedes: tuple[str, ...] = ()
    retracted: bool = False
    expired: bool = False
    derived_from: tuple[str, ...] = ()
    projection: bool = False
    current_time_capable: bool = False


@dataclass(frozen=True)
class EntityAccountBinding:
    """One explicit same-platform account binding.

    Display names stay presentation hints.  The immutable platform account ID
    is the only value that can bind an account to an established entity.
    """

    entity_id: str
    platform: str
    account_id: str
    authority_receipt: str
    authority_actor: str
    binding_version: str = ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION
    authority_verified: bool = False
    active: bool = True


@dataclass(frozen=True)
class EntityResolution:
    status: str
    subject: SubjectIdentity | None = None
    method: str = ""
    reason: str = ""


@dataclass(frozen=True)
class CanonClaim:
    """Normalized read-model revision shared by every canon source adapter.

    This is deliberately a logical contract, not a new storage owner.  Source
    systems retain their own rows and mutation authority; this immutable view
    only makes authority, lineage, visibility, and lifecycle comparable before
    the unified packet performs selection.
    """

    claim_id: str
    revision_id: str
    subject_id: str
    predicate: str
    value: Any
    canon_status: CanonStatus
    domain: CanonDomain
    claim_kind: ClaimKind
    source_system: str
    adapter_version: str
    source_class: SourceClass
    source_refs: tuple[str, ...]
    root_ids: tuple[str, ...]
    occurrence_ids: tuple[str, ...]
    visibility: Visibility
    lifecycle: ClaimLifecycle
    confidence: Confidence
    source_revision: str = ""
    authority_actor: str = ""
    authority_receipt: str = ""
    eligible_routes: tuple[str, ...] = ()
    valid_from: str = ""
    valid_until: str = ""
    supersedes: tuple[str, ...] = ()
    correction_of: tuple[str, ...] = ()
    recurrence_contract_version: str = ""
    projection_state: str = "shadow"
    projection_version: str = HYBRID_CANON_CLAIM_CONTRACT_VERSION
    subject_type: str = ""
    object_subject_type: str = ""
    object_subject_id: str = ""


@dataclass(frozen=True)
class CanonAdapterResult:
    claim: CanonClaim | None
    reason: str
    live_eligible: bool = False

    @property
    def contract_valid(self) -> bool:
        return self.claim is not None


@dataclass(frozen=True)
class Resolution:
    usable: bool
    claim: SourceClaim | None
    reason: str
    current_time_eligible: bool = False

BARCODE = SubjectIdentity("barcode", "BARCODE", ("BARCODE collective",))
BARCODE_NETWORK = SubjectIdentity("barcode_network", "BARCODE Network")
BARCODE_RADIO = SubjectIdentity("barcode_radio", "BARCODE Radio")
SIX_BIT = SubjectIdentity("6_bit", "6 Bit", ("Six Bit",))
DJ_FLOPPYDISC = SubjectIdentity(
    "dj_floppydisc",
    "DJ Floppydisc",
    ("DJ Floppy Disc",),
)
CACHE_BACK = SubjectIdentity(
    "cache_back",
    "Cache Back",
)
CALL_EM_BINI = SubjectIdentity(
    "call_em_bini",
    "Call'em Bini",
    ("Call’em Bini",),
)
# Compatibility spelling for early hybrid-contract callers.
CALLEM_BINI = CALL_EM_BINI
MAC_MODEM = SubjectIdentity(
    "mac_modem",
    "Mac Modem",
    ("Mac Mod3m",),
)
GALAKNOISE = SubjectIdentity("galaknoise", "GALAKNOISE")
BNL01 = SubjectIdentity("bnl_01", "BNL-01", ("BNL", "BARCODE Network Liaison Entity"))
SHEILA = SubjectIdentity("sheila", "Sheila")
CLIFF = SubjectIdentity("cliff", "Cliff")

CANON_MEMBER_IDENTITIES = (
    SIX_BIT,
    DJ_FLOPPYDISC,
    CACHE_BACK,
    MAC_MODEM,
)
CANON_ENTITY_IDENTITIES = (
    BARCODE,
    BARCODE_NETWORK,
    BARCODE_RADIO,
    *CANON_MEMBER_IDENTITIES,
    CALL_EM_BINI,
    GALAKNOISE,
    BNL01,
    SHEILA,
    CLIFF,
)
AUTOMATIC_CANON_SIGNAL_IDENTITIES = (
    DJ_FLOPPYDISC,
    CACHE_BACK,
    MAC_MODEM,
)

FOUNDING_MEMBERS = ("6 Bit", "DJ Floppydisc", "Cache Back", "Mac Modem")
FRIDAY_PUBLIC_SCHEDULE = FridaySchedule("6:40 PM Pacific", "7:00 PM Pacific", "7:05 PM Pacific")
BNL_ROLES = (
    "lore liaison", "conversational presence", "memory and continuity layer",
    "procedural archivist", "relationship observer", "public relay brain",
    "source-safety boundary", "dossier/entity-intelligence contributor",
    "operator-facing analytical system", "developing intelligence with his own curiosity",
)
CANON_FACTS = (
    CanonFact(BARCODE, "founding_members", FOUNDING_MEMBERS),
    CanonFact(BARCODE_NETWORK, "origin", "The music and collective existed before BARCODE Network; the Network grew around that signal and now connects music, live broadcasts, community, software, archive, characters, and story."),
    CanonFact(SIX_BIT, "primary_identity", "artist, MC, host, and founding BARCODE member first"),
    CanonFact(DJ_FLOPPYDISC, "primary_identity", "founding BARCODE member and signal/audio engineer who stabilizes sound, cleans artifacts, and handles mastering and final waveform integrity"),
    CanonFact(DJ_FLOPPYDISC, "behavior", "quiet professional who prefers to fix problems rather than talk about them"),
    CanonFact(DJ_FLOPPYDISC, "typical_involvement", "mixes and masters BARCODE material"),
    CanonFact(CACHE_BACK, "primary_identity", "founding BARCODE member and BARCODE Archive specialist"),
    CanonFact(CACHE_BACK, "behavior", "meticulous, detail-obsessed, and protective of lost data and recovered fragments"),
    CanonFact(CACHE_BACK, "typical_involvement", "recovers fragments and protects archive continuity"),
    CanonFact(MAC_MODEM, "primary_identity", "founding BARCODE member and chaotic tech entity / glitch-virus presence in the BARCODE ecosystem"),
    CanonFact(MAC_MODEM, "behavior", "unpredictable, mischievous, and sometimes disruptive; not reliably malicious, but risky"),
    CanonFact(MAC_MODEM, "typical_involvement", "unexpected distortions, interface corruption, and broadcast anomalies"),
    CanonFact(GALAKNOISE, "primary_role", "music producer for BARCODE"),
    CanonFact(BARCODE_RADIO, "public_nature", "real weekly live broadcast and community music space on TikTok"),
    CanonFact(BARCODE_RADIO, "friday_public_schedule", FRIDAY_PUBLIC_SCHEDULE),
    CanonFact(BNL01, "identity", "BARCODE Network Liaison Entity: an unfinished but active intelligence and one shared mind with filtered surfaces"),
    CanonFact(BNL01, "roles", BNL_ROLES),
    CanonFact(BNL01, "in_world_boundary", "BNL remains fully in-world; lore is his native operating language, not decorative flavor."),
    CanonFact(BARCODE_NETWORK, "website_information_architecture", "Reality first. Meaning second. Mythology deeper. This governs public website information architecture, not BNL's speaking order."),
)

BROADCAST_DECLARED_TYPE_DEFAULTS: dict[
    str, tuple[CanonDomain, ClaimKind]
] = {
    "episode_arc": (CanonDomain.BROADCAST_HISTORY, ClaimKind.EVENT),
    "notable_moment": (CanonDomain.BROADCAST_HISTORY, ClaimKind.EVENT),
    "running_joke": (CanonDomain.HYBRID, ClaimKind.TRADITION_OR_JOKE),
    "technical_issue": (CanonDomain.OPERATIONAL, ClaimKind.EVENT),
    "moderation_context": (CanonDomain.REAL_COMMUNITY, ClaimKind.EVENT),
    "show_state_override": (CanonDomain.OPERATIONAL, ClaimKind.CURRENT_STATE),
}

LEGACY_WEBSITE_LORE_RELATIONSHIP_CANDIDATES: tuple[
    Mapping[str, str], ...
] = (
    {
        "source_ref": "website:src/content.ts:cache_back:origin",
        "source_revision": "9baac033",
        "subject_id": CACHE_BACK.key,
        "object_id": CALL_EM_BINI.key,
        "text": (
            "Built from a cache of data left behind by legendary artist "
            "Call'em Bini."
        ),
    },
)

_BINDING_RECEIPT_PREFIXES = (
    "binding_receipt:",
    "owner_command:",
    "service_receipt:",
)
_TRUSTED_SERVICE_ACTOR_REFS = frozenset(
    {
        "service_actor:legacy_canon_registry_v1",
    }
)


def _stable_contract_digest(*values: Any) -> str:
    encoded = json.dumps(
        values,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_claim_id(
    source_system: str,
    source_identity: str,
) -> str:
    """Return source-owned identity stable across corrected revisions."""

    return "clm_" + _stable_contract_digest(
        CANON_CLAIM_ID_NAMESPACE,
        source_system,
        source_identity,
    )[:32]


def _claim_revision_payload(
    claim: CanonClaim,
) -> tuple[Any, ...]:
    """Return every normalized field that makes one immutable revision."""

    payload = (
        HYBRID_CANON_CLAIM_CONTRACT_VERSION,
        claim.source_revision,
        claim.claim_id,
        claim.subject_id,
        claim.predicate,
        claim.value,
        claim.canon_status.value,
        claim.domain.value,
        claim.claim_kind.value,
        claim.source_system,
        claim.adapter_version,
        claim.source_class.value,
        claim.source_refs,
        claim.root_ids,
        claim.occurrence_ids,
        claim.visibility.value,
        claim.lifecycle.value,
        claim.confidence.value,
        claim.authority_actor,
        claim.authority_receipt,
        claim.eligible_routes,
        claim.valid_from,
        claim.valid_until,
        claim.supersedes,
        claim.correction_of,
        claim.recurrence_contract_version,
        claim.projection_state,
        claim.projection_version,
    )
    typed_identity = (
        str(claim.subject_type or ""),
        str(claim.object_subject_type or ""),
        str(claim.object_subject_id or ""),
    )
    # Preserve existing revision IDs for adapters that predate typed subjects;
    # a Declared claim opts into the extension by carrying at least one field.
    if any(typed_identity):
        payload += ("typed_subject_contract_v1", *typed_identity)
    return payload


def _finalize_claim_revision(
    claim: CanonClaim,
) -> str:
    """Bind an exact normalized revision while preserving stable claim ID."""

    return "rev_" + _stable_contract_digest(
        *_claim_revision_payload(claim)
    )[:32]


def _with_final_revision(
    claim: CanonClaim,
    *,
    source_revision: str,
) -> CanonClaim:
    sourced_claim = replace(claim, source_revision=str(source_revision or ""))
    revision_id = _finalize_claim_revision(sourced_claim)
    return replace(sourced_claim, revision_id=revision_id)


def strict_contract_bool(value: Any) -> bool:
    """Coerce only explicit true values; malformed and absent data fail shut."""

    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, int) and not isinstance(value, bool):
        return value == 1
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on"}
    return False


def _explicit_contract_false(value: Any) -> bool:
    if value is False:
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return value == 0
    if isinstance(value, str):
        return value.strip().casefold() in {"0", "false", "no", "off"}
    return False


def _opaque_actor_valid(actor: Any) -> bool:
    normalized_actor = str(actor or "").strip()
    return bool(
        re.fullmatch(r"discord_user:[1-9][0-9]{0,24}", normalized_actor)
        or normalized_actor in _TRUSTED_SERVICE_ACTOR_REFS
    )


def _opaque_authority_valid(
    actor: Any,
    receipt: Any,
    *,
    receipt_prefixes: tuple[str, ...],
) -> bool:
    normalized_receipt = str(receipt or "").strip()
    return bool(
        _opaque_actor_valid(actor)
        and any(
            normalized_receipt.startswith(prefix)
            and re.fullmatch(
                re.escape(prefix) + r"[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,191}",
                normalized_receipt,
            )
            for prefix in receipt_prefixes
        )
    )


def _binding_authority_valid(binding: EntityAccountBinding) -> bool:
    return bool(
        str(binding.binding_version or "").strip()
        == ENTITY_ACCOUNT_BINDING_CONTRACT_VERSION
        and strict_contract_bool(binding.authority_verified)
        and _opaque_authority_valid(
            binding.authority_actor,
            binding.authority_receipt,
            receipt_prefixes=_BINDING_RECEIPT_PREFIXES,
        )
    )


def _platform_account_id_valid(platform: Any, account_id: Any) -> bool:
    normalized_platform = str(platform or "").strip().casefold()
    normalized_account = str(account_id or "").strip()
    if normalized_platform == "discord":
        return bool(re.fullmatch(r"[1-9][0-9]{0,24}", normalized_account))
    return bool(
        normalized_platform
        and re.fullmatch(
            r"[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,191}",
            normalized_account,
        )
    )


def _legacy_fact_shape(fact: CanonFact) -> tuple[CanonDomain, ClaimKind]:
    predicate = str(fact.predicate or "")
    if fact.subject.key == BARCODE_RADIO.key:
        return CanonDomain.BROADCAST_HISTORY, (
            ClaimKind.EVENT
            if predicate in {"friday_public_schedule", "public_nature"}
            else ClaimKind.OTHER
        )
    if predicate in {"primary_identity", "identity", "founding_members"}:
        return CanonDomain.REAL_COMMUNITY, ClaimKind.IDENTITY
    if predicate in {"primary_role", "roles", "typical_involvement"}:
        return CanonDomain.REAL_COMMUNITY, ClaimKind.ROLE
    if predicate == "behavior":
        return CanonDomain.HYBRID, ClaimKind.BEHAVIOR_PATTERN
    if predicate in {
        "origin",
        "in_world_boundary",
        "website_information_architecture",
    }:
        return CanonDomain.HYBRID, ClaimKind.WORLD_RULE
    return CanonDomain.REAL_COMMUNITY, ClaimKind.OTHER


def adapt_legacy_canon_fact(fact: CanonFact) -> CanonClaim:
    """Project one static registry fact without creating a database owner."""

    domain, claim_kind = _legacy_fact_shape(fact)
    source_ref = "canon_registry:%s:%s" % (
        fact.subject.key,
        fact.predicate,
    )
    claim_id = _stable_claim_id(
        "legacy_canon_registry",
        source_ref,
    )
    claim = CanonClaim(
        claim_id=claim_id,
        revision_id="",
        subject_id=fact.subject.key,
        predicate=fact.predicate,
        value=fact.value,
        canon_status=CanonStatus.LEGACY,
        domain=domain,
        claim_kind=claim_kind,
        source_system="legacy_canon_registry",
        adapter_version=LEGACY_CANON_ADAPTER_VERSION,
        source_class=fact.source_class,
        source_refs=(source_ref,),
        root_ids=(source_ref,),
        occurrence_ids=(source_ref,),
        visibility=fact.visibility,
        lifecycle=ClaimLifecycle.ESTABLISHED,
        confidence=fact.confidence,
        authority_actor="service_actor:legacy_canon_registry_v1",
        authority_receipt=(
            "code_revision:%s" % CANON_SOURCE_CONTRACT_VERSION
        ),
        eligible_routes=(
            ("reference_canon", "public_home")
            if fact.visibility
            in {
                Visibility.PUBLIC,
                Visibility.PUBLIC_SAFE,
                Visibility.REFERENCE_CANON,
            }
            else ("reference_canon",)
        ),
        projection_state="shadow",
        projection_version=LEGACY_CANON_ADAPTER_VERSION,
    )
    return _with_final_revision(
        claim,
        source_revision=CANON_SOURCE_CONTRACT_VERSION,
    )


def _mapping_text(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


@contextmanager
def _read_snapshot(conn: Any):
    """Reuse a caller transaction or own one read-only SQLite snapshot."""

    owns_snapshot = not bool(getattr(conn, "in_transaction", False))
    before_changes = int(getattr(conn, "total_changes", 0) or 0)
    if owns_snapshot:
        conn.execute("BEGIN")
    try:
        yield
        if int(getattr(conn, "total_changes", 0) or 0) != before_changes:
            raise RuntimeError("read_snapshot_mutated_state")
    except Exception:
        if owns_snapshot and bool(getattr(conn, "in_transaction", False)):
            conn.rollback()
        raise
    else:
        if owns_snapshot and bool(getattr(conn, "in_transaction", False)):
            conn.commit()


def adapt_broadcast_memory_claim(
    row: Mapping[str, Any],
    *,
    owner_authorized: bool = False,
    authority_actor: str = "",
    authority_receipt: str = "",
) -> CanonAdapterResult:
    """Build the raw Broadcast compatibility view.

    A ``broadcast_memory`` row is first-party source material, not proof that
    the owner declared it canon.  The legacy caller authority parameters are
    retained only for source compatibility and are intentionally ignored.  An
    exact, current ``declared_canon_revisions`` join is the only path to the
    Declared read model (see :func:`adapt_declared_canon_revision`).
    """

    row_id = _mapping_text(row, "id", "row_id")
    entry_type = _mapping_text(row, "entry_type", "type")
    cleaned_value = _mapping_text(row, "cleaned_summary", "summary")
    raw_value = _mapping_text(row, "raw_note")
    value = cleaned_value or raw_value
    if not row_id or not entry_type or not value:
        return CanonAdapterResult(None, "missing_broadcast_source_identity")
    subject_id = _mapping_text(row, "subject_id", "subject_key") or BARCODE_RADIO.key
    defaults = BROADCAST_DECLARED_TYPE_DEFAULTS.get(entry_type)
    recognized = defaults is not None
    if defaults is None:
        domain, claim_kind = CanonDomain.BROADCAST_HISTORY, ClaimKind.OTHER
    else:
        domain, claim_kind = defaults
    source_ref = "broadcast_memory:%s" % row_id
    claim_id = _stable_claim_id(
        "broadcast_memory",
        source_ref,
    )
    source_revision = _mapping_text(
        row,
        "revision_id",
        "updated_at",
        "created_at",
    ) or row_id
    # Do not remove these compatibility parameters until all external callers
    # have migrated.  Reading them would recreate the pre-PR2 authority bug.
    _ = (owner_authorized, authority_actor, authority_receipt)
    supersedes = _mapping_text(row, "supersedes_id")
    correction_of = _mapping_text(row, "correction_of")
    claim = CanonClaim(
        claim_id=claim_id,
        revision_id="",
        subject_id=subject_id,
        predicate=entry_type,
        value=value,
        canon_status=CanonStatus.OPEN_SIGNAL,
        domain=domain,
        claim_kind=claim_kind,
        source_system="broadcast_memory",
        adapter_version=BROADCAST_CANON_ADAPTER_VERSION,
        source_class=SourceClass.FIRST_PARTY_RECORD,
        source_refs=(source_ref,),
        root_ids=(source_ref,),
        occurrence_ids=(source_ref,),
        visibility=Visibility.INTERNAL,
        lifecycle=ClaimLifecycle.REVIEW_ONLY,
        confidence=Confidence.LOW,
        authority_actor="",
        authority_receipt="",
        eligible_routes=("broadcast_memory",),
        valid_from=_mapping_text(row, "valid_from", "created_at"),
        valid_until=_mapping_text(row, "valid_until"),
        supersedes=(("broadcast_memory:%s" % supersedes,) if supersedes else ()),
        correction_of=(("broadcast_memory:%s" % correction_of,) if correction_of else ()),
        projection_state="review_only",
        projection_version=BROADCAST_CANON_ADAPTER_VERSION,
    )
    claim = _with_final_revision(
        claim,
        source_revision=source_revision,
    )
    return CanonAdapterResult(
        claim,
        (
            "legacy_type_review_only"
            if not recognized
            else "broadcast_source_review_only"
        ),
    )


def _declared_revision_claim(
    revision: Any,
    *,
    broadcast_row: Mapping[str, Any] | None = None,
) -> CanonClaim:
    """Normalize one already-validated PR2 revision without adding authority."""

    from bnl_declared_canon import (
        BROADCAST_MEMORY_SOURCE,
        GENERAL_DECLARATION_SOURCE,
    )

    routes = tuple(json.loads(str(revision.eligible_routes_json or "[]")))
    domain = CanonDomain(str(revision.domain))
    claim_kind = ClaimKind(str(revision.claim_kind))
    visibility = Visibility(str(revision.visibility))
    lifecycle = ClaimLifecycle(str(revision.lifecycle_status))
    declaration_ref = "declared_canon:%s" % revision.declaration_id
    revision_ref = "declared_canon_revision:%s" % revision.revision_id
    correction_of = (
        ("declared_canon_revision:%s" % revision.correction_of_revision_id,)
        if revision.correction_of_revision_id
        else ()
    )
    supersedes = (
        ("declared_canon:%s" % revision.supersedes_declaration_id,)
        if revision.supersedes_declaration_id
        else ()
    )
    if revision.source_system == BROADCAST_MEMORY_SOURCE:
        if broadcast_row is None:
            raise ValueError("broadcast_source_required")
        source_ref = "broadcast_memory:%s" % revision.source_row_id
        value: Any = _mapping_text(broadcast_row, "cleaned_summary")
        if not value:
            raise ValueError("broadcast_cleaned_summary_required")
        source_supersedes = _mapping_text(broadcast_row, "supersedes_id")
        if source_supersedes:
            supersedes = tuple(
                dict.fromkeys(
                    (*supersedes, "broadcast_memory:%s" % source_supersedes)
                )
            )
        claim_id = _stable_claim_id("broadcast_memory", source_ref)
        source_system = "broadcast_memory"
        source_refs = (source_ref, revision_ref)
        root_ids = (source_ref,)
        source_class = SourceClass.FIRST_PARTY_RECORD
    elif revision.source_system == GENERAL_DECLARATION_SOURCE:
        source_ref = declaration_ref
        value = json.loads(str(revision.value_json))
        claim_id = _stable_claim_id("declared_canon", declaration_ref)
        source_system = "declared_canon"
        source_refs = (declaration_ref, revision_ref)
        root_ids = (declaration_ref,)
        source_class = (
            SourceClass.OWNER_CORRECTION
            if str(revision.operation) == "correct"
            else SourceClass.APPROVED_CANON
        )
    else:
        raise ValueError("declared_source_system_invalid")
    if claim_kind == ClaimKind.RELATIONSHIP:
        value = {
            "value": value,
            "object_subject_type": str(revision.object_subject_type),
            "object_subject_id": str(revision.object_subject_id),
        }
    adapter_version = (
        BROADCAST_CANON_ADAPTER_VERSION
        if revision.source_system == BROADCAST_MEMORY_SOURCE
        else DECLARED_CANON_ADAPTER_VERSION
    )
    claim = CanonClaim(
        claim_id=claim_id,
        revision_id="",
        subject_id=str(revision.subject_id),
        predicate=str(revision.predicate),
        value=value,
        canon_status=CanonStatus.DECLARED,
        domain=domain,
        claim_kind=claim_kind,
        source_system=source_system,
        adapter_version=adapter_version,
        source_class=source_class,
        source_refs=source_refs,
        root_ids=root_ids,
        occurrence_ids=root_ids,
        visibility=visibility,
        lifecycle=lifecycle,
        confidence=Confidence.APPROVED,
        authority_actor=str(revision.authority_actor),
        authority_receipt=str(revision.authority_receipt),
        eligible_routes=routes,
        valid_from=str(revision.valid_from or ""),
        valid_until=str(revision.valid_until or ""),
        supersedes=supersedes,
        correction_of=correction_of,
        projection_state="shadow",
        projection_version=adapter_version,
        subject_type=str(revision.subject_type),
        object_subject_type=str(revision.object_subject_type or ""),
        object_subject_id=str(revision.object_subject_id or ""),
    )
    return _with_final_revision(
        claim,
        source_revision=str(revision.revision_id),
    )


def _adapt_declared_canon_revision_in_snapshot(
    conn: Any,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    expected_source_fingerprint: str,
    now: str = "",
) -> CanonAdapterResult:
    """Adapt an exact current PR2 revision through its owner read boundary.

    The lifecycle module revalidates stored authority, latest-revision state,
    validity, and the authoritative source.  Broadcast content is then read a
    second time and fingerprinted as the exact value snapshot returned here.
    No schema is created and the result remains shadow-only/non-live.
    """

    from bnl_declared_canon import (
        BROADCAST_MEMORY_SOURCE,
        DECLARED_CANON_CONTRACT_VERSION,
        DeclaredCanonError,
        _digest,
        validate_current_declared_canon_revision,
    )

    before = int(getattr(conn, "total_changes", 0) or 0)
    try:
        revision = validate_current_declared_canon_revision(
            conn,
            actor_user_id=int(actor_user_id or 0),
            authority_nonce=str(authority_nonce or ""),
            guild_id=int(guild_id or 0),
            declaration_id=str(declaration_id or ""),
            expected_revision_id=str(expected_revision_id or ""),
            expected_source_fingerprint=str(expected_source_fingerprint or ""),
            now=str(now or ""),
        )
        if revision.source_system == BROADCAST_MEMORY_SOURCE:
            expected_declaration_id = "dcl_" + _digest(
                DECLARED_CANON_CONTRACT_VERSION,
                int(guild_id or 0),
                BROADCAST_MEMORY_SOURCE,
                int(revision.source_row_id),
            )[:32]
            if revision.declaration_id != expected_declaration_id:
                return CanonAdapterResult(
                    None, "declared_broadcast_identity_invalid"
                )
            duplicate_authorities = int(
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT declaration_id)
                    FROM main.declared_canon_revisions
                    WHERE guild_id=? AND source_system='broadcast_memory'
                      AND source_row_id=?
                    """,
                    (int(guild_id or 0), str(revision.source_row_id)),
                ).fetchone()[0]
                or 0
            )
            if duplicate_authorities != 1:
                return CanonAdapterResult(
                    None, "declared_broadcast_duplicate_authority"
                )
        evaluation_now = (
            _parse_contract_time(now)
            if str(now or "").strip()
            else datetime.now(timezone.utc)
        )
        if evaluation_now is None:
            return CanonAdapterResult(None, "declared_validation_now_invalid")
        claim, internal_reason = _inventory_current_declared_claim(
            conn,
            revision,
            now=evaluation_now,
        )
        if claim is None:
            return CanonAdapterResult(None, internal_reason)
    except (DeclaredCanonError, ValueError, TypeError, json.JSONDecodeError) as exc:
        code = getattr(exc, "code", "") or str(exc or "invalid")
        return CanonAdapterResult(None, "declared_%s" % code)
    if int(getattr(conn, "total_changes", 0) or 0) != before:
        return CanonAdapterResult(None, "declared_adapter_mutated_state")
    return CanonAdapterResult(claim, "declared_shadow_current", False)


def adapt_declared_canon_revision(
    conn: Any,
    *,
    actor_user_id: int,
    authority_nonce: str,
    guild_id: int,
    declaration_id: str,
    expected_revision_id: str,
    expected_source_fingerprint: str,
    now: str = "",
) -> CanonAdapterResult:
    """Return one exact current Declared claim from one read snapshot."""

    with _read_snapshot(conn):
        return _adapt_declared_canon_revision_in_snapshot(
            conn,
            actor_user_id=actor_user_id,
            authority_nonce=authority_nonce,
            guild_id=guild_id,
            declaration_id=declaration_id,
            expected_revision_id=expected_revision_id,
            expected_source_fingerprint=expected_source_fingerprint,
            now=now,
        )


def _normalized_identity_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        values: Iterable[Any] = (value,)
    elif isinstance(value, Iterable):
        values = value
    else:
        values = ()
    return tuple(
        dict.fromkeys(
            str(item).strip() for item in values if str(item).strip()
        )
    )


def _strict_nonnegative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return 0
    return max(0, parsed)


def _review_only_source_claim(
    *,
    source_system: str,
    source_identity: str,
    source_revision: str,
    subject_id: str,
    predicate: str,
    value: Any,
    root_ids: tuple[str, ...],
    occurrence_ids: tuple[str, ...],
    domain: CanonDomain,
    claim_kind: ClaimKind,
) -> CanonClaim:
    source_ref = "%s:%s" % (source_system, source_identity)
    claim = CanonClaim(
        claim_id=_stable_claim_id(source_system, source_identity),
        revision_id="",
        subject_id=subject_id,
        predicate=predicate,
        value=value,
        canon_status=CanonStatus.OPEN_SIGNAL,
        domain=domain,
        claim_kind=claim_kind,
        source_system=source_system,
        adapter_version=LIVING_CANON_ADAPTER_VERSION,
        source_class=SourceClass.EVIDENCE_PROJECTION,
        source_refs=(source_ref,),
        root_ids=root_ids,
        occurrence_ids=occurrence_ids,
        visibility=Visibility.INTERNAL,
        lifecycle=ClaimLifecycle.REVIEW_ONLY,
        confidence=Confidence.LOW,
        eligible_routes=(),
        projection_state="review_only",
        projection_version=LIVING_CANON_ADAPTER_VERSION,
    )
    return _with_final_revision(claim, source_revision=source_revision)


def _adapt_living_pattern_claim(
    row: Mapping[str, Any],
    *,
    source_system: str,
    source_identity_key: str,
) -> CanonAdapterResult:
    candidate_type = _mapping_text(row, "candidate_type")
    candidate_state = _mapping_text(row, "candidate_state", "lifecycle")
    subject_id = _mapping_text(row, "subject_key", "subject_id")
    predicate = _mapping_text(row, "predicate_key", "predicate")
    value = _mapping_text(row, "meaning", "value")
    root_ids = _normalized_identity_tuple(
        row.get("root_ids") or row.get("root_entry_ids") or ()
    )
    occurrence_ids = _normalized_identity_tuple(
        row.get("occurrence_ids") or row.get("occurrence_identities") or ()
    )
    source_identity = _mapping_text(row, source_identity_key)
    if not source_identity:
        return CanonAdapterResult(None, "living_source_identity_missing")
    if candidate_state != ClaimLifecycle.ESTABLISHED.value:
        return CanonAdapterResult(None, "living_lifecycle_ineligible")
    if not subject_id or not predicate or not value:
        return CanonAdapterResult(None, "living_source_lineage_missing")
    source_revision = (
        _mapping_text(row, "updated_at", "last_seen_at") or source_identity
    )
    try:
        review_domain_hint = CanonDomain(_mapping_text(row, "domain"))
    except ValueError:
        review_domain_hint = CanonDomain.REAL_COMMUNITY
    try:
        review_kind_hint = ClaimKind(_mapping_text(row, "claim_kind"))
    except ValueError:
        review_kind_hint = (
            ClaimKind.BEHAVIOR_PATTERN
            if candidate_type == "topic_or_motif"
            else ClaimKind.OTHER
        )

    def review(
        reason: str,
        *,
        review_domain: CanonDomain | None = None,
        review_kind: ClaimKind | None = None,
    ) -> CanonAdapterResult:
        return CanonAdapterResult(
            _review_only_source_claim(
                source_system=source_system,
                source_identity=source_identity,
                source_revision=source_revision,
                subject_id=subject_id,
                predicate=predicate,
                value=value,
                root_ids=root_ids,
                occurrence_ids=occurrence_ids,
                domain=review_domain or review_domain_hint,
                claim_kind=review_kind or review_kind_hint,
            ),
            reason,
        )

    if candidate_type != "topic_or_motif":
        return review(
            "living_claim_kind_ineligible",
            review_kind=ClaimKind.OTHER,
        )
    if not root_ids:
        return review("living_source_lineage_missing")
    if (
        _mapping_text(row, "recurrence_contract_version")
        != LIVING_CANON_RECURRENCE_VERSION
    ):
        return review("living_recurrence_unverified")
    try:
        domain = CanonDomain(_mapping_text(row, "domain"))
        claim_kind = ClaimKind(_mapping_text(row, "claim_kind"))
    except ValueError:
        return review("living_domain_unverified")
    if domain not in {
        CanonDomain.REAL_COMMUNITY,
        CanonDomain.LORE,
        CanonDomain.HYBRID,
    } or claim_kind not in {
        ClaimKind.BEHAVIOR_PATTERN,
        ClaimKind.TRADITION_OR_JOKE,
    }:
        return review(
            "living_domain_ineligible",
            review_domain=domain,
            review_kind=claim_kind,
        )
    required_proofs = (
        "candidate_eligible",
        "source_eligible",
        "roots_valid",
        "occurrence_bounded",
        "correction_fence_clear",
        "contradiction_clear",
    )
    if not all(strict_contract_bool(row.get(key)) for key in required_proofs):
        return review(
            "living_recurrence_unverified",
            review_domain=domain,
            review_kind=claim_kind,
        )
    independent_roots = _strict_nonnegative_int(
        row.get("independent_root_count")
    )
    independent_occurrences = _strict_nonnegative_int(
        row.get("independent_occurrence_count")
    )
    if (
        independent_roots < 2
        or independent_occurrences < 2
        or len(root_ids) < 2
        or len(occurrence_ids) < 2
    ):
        return review(
            "living_recurrence_insufficient",
            review_domain=domain,
            review_kind=claim_kind,
        )
    source_ref = "%s:%s" % (source_system, source_identity)
    claim_id = _stable_claim_id(
        source_system,
        source_identity,
    )
    visibility = (
        Visibility.PUBLIC_SAFE
        if _mapping_text(row, "visibility") in {"public", "public_safe"}
        and strict_contract_bool(row.get("public_usable"))
        else Visibility.INTERNAL
    )
    claim = CanonClaim(
        claim_id=claim_id,
        revision_id="",
        subject_id=subject_id,
        predicate=predicate,
        value=value,
        canon_status=CanonStatus.LIVING,
        domain=domain,
        claim_kind=claim_kind,
        source_system=source_system,
        adapter_version=LIVING_CANON_ADAPTER_VERSION,
        source_class=SourceClass.EVIDENCE_PROJECTION,
        source_refs=(source_ref,),
        root_ids=root_ids,
        occurrence_ids=occurrence_ids,
        visibility=visibility,
        lifecycle=ClaimLifecycle.ESTABLISHED,
        confidence=Confidence.MEDIUM,
        eligible_routes=(
            ("public_home",)
            if visibility == Visibility.PUBLIC_SAFE
            else ()
        ),
        recurrence_contract_version=LIVING_CANON_RECURRENCE_VERSION,
        projection_state="shadow",
        projection_version=LIVING_CANON_ADAPTER_VERSION,
    )
    return CanonAdapterResult(
        _with_final_revision(
            claim,
            source_revision=source_revision,
        ),
        "eligible_living",
    )


def adapt_living_atomic_claim(row: Mapping[str, Any]) -> CanonAdapterResult:
    """Normalize only an independently recurrence-verified atomic motif."""

    return _adapt_living_pattern_claim(
        row,
        source_system="atomic_knowledge",
        source_identity_key="candidate_id",
    )


def adapt_living_moment_claim(row: Mapping[str, Any]) -> CanonAdapterResult:
    """Normalize Moment material only after cross-occurrence proof exists."""

    normalized = dict(row)
    moment_id = _mapping_text(normalized, "moment_id")
    normalized.setdefault("candidate_type", "topic_or_motif")
    if _mapping_text(normalized, "lifecycle_status") == "finalized":
        normalized.setdefault("candidate_state", "established")
    normalized.setdefault(
        "meaning",
        _mapping_text(normalized, "contribution_gist", "summary"),
    )
    normalized.setdefault(
        "predicate_key",
        _mapping_text(normalized, "topic_key") or "community_moment_pattern",
    )
    normalized.setdefault("domain", CanonDomain.REAL_COMMUNITY.value)
    if not _mapping_text(normalized, "subject_key", "subject_id") and moment_id:
        normalized["subject_key"] = "moment:%s" % moment_id
        normalized.setdefault("claim_kind", ClaimKind.EVENT.value)
    else:
        normalized.setdefault(
            "claim_kind",
            ClaimKind.BEHAVIOR_PATTERN.value,
        )
    return _adapt_living_pattern_claim(
        normalized,
        source_system="memory_moment",
        source_identity_key="moment_id",
    )


def adapt_open_signal_claim(row: Any) -> CanonAdapterResult:
    """Normalize one already-governed question-scoped public observation."""

    if not isinstance(row, Mapping):
        if is_dataclass(row):
            row = asdict(row)
        else:
            return CanonAdapterResult(None, "open_signal_contract_shape_invalid")

    subject_id = _mapping_text(row, "subject_key", "subject_id")
    entry_id = _mapping_text(row, "entry_id", "source_ref")
    value = _mapping_text(row, "text", "value")
    occurrence_id = _mapping_text(row, "occurrence_identity")
    root_id = _mapping_text(row, "root_identity")
    source_digest = _mapping_text(row, "source_digest")
    point_identity = _mapping_text(row, "point_identity")
    attribution_mode = _mapping_text(row, "attribution_mode")
    polarity = _mapping_text(row, "polarity")
    action_identity = _mapping_text(row, "action_identity")
    if not (
        subject_id
        and entry_id
        and value
        and occurrence_id
        and root_id
        and source_digest
        and point_identity
        and attribution_mode
        and polarity
        and action_identity
    ):
        return CanonAdapterResult(None, "open_signal_source_lineage_missing")
    if (
        _mapping_text(row, "assessment_contract_version")
        != PUBLIC_ASSESSMENT_EVIDENCE_VERSION
        or _mapping_text(row, "source_system")
        != "memory_ledger_public_assessment"
    ):
        return CanonAdapterResult(None, "open_signal_governance_unverified")
    if (
        _mapping_text(row, "source_role").casefold() != "user"
        or _mapping_text(row, "source_class").casefold()
        != SourceClass.PUBLIC_OBSERVATION.value
        or _mapping_text(row, "lifecycle_status").casefold() != "active"
        or _mapping_text(row, "visibility").casefold()
        not in {Visibility.PUBLIC.value, Visibility.PUBLIC_SAFE.value}
        or _mapping_text(row, "channel_policy").casefold()
        not in {"public_home", "public_context", "public_selective"}
        or _mapping_text(row, "route_mode").casefold()
        not in {"normal_chat", "conversation_continuity"}
        or attribution_mode
        not in {"subject_action", "authored_topic"}
        or polarity
        not in {"affirmative", "negative", "conditional"}
        or not strict_contract_bool(row.get("public_usable"))
        or not strict_contract_bool(row.get("subject_authored"))
        or not strict_contract_bool(row.get("selector_eligible"))
        or not _explicit_contract_false(row.get("derived"))
        or not _explicit_contract_false(row.get("projection"))
    ):
        return CanonAdapterResult(None, "open_signal_source_ineligible")
    source_ref = (
        entry_id if entry_id.startswith("memory_ledger:") else "memory_ledger:%s" % entry_id
    )
    predicate = (
        _mapping_text(row, "predicate", "predicate_key")
        or "public_observation:%s" % action_identity
    )
    claim_id = _stable_claim_id(
        "public_assessment_observation",
        source_ref,
    )
    source_revision = source_digest
    claim = CanonClaim(
        claim_id=claim_id,
        revision_id="",
        subject_id=subject_id,
        predicate=predicate,
        value=value,
        canon_status=CanonStatus.OPEN_SIGNAL,
        domain=CanonDomain.REAL_COMMUNITY,
        claim_kind=ClaimKind.BEHAVIOR_PATTERN,
        source_system="public_assessment_observation",
        adapter_version=OPEN_SIGNAL_ADAPTER_VERSION,
        source_class=SourceClass.PUBLIC_OBSERVATION,
        source_refs=(source_ref,),
        root_ids=(root_id,),
        occurrence_ids=(occurrence_id,),
        visibility=Visibility.PUBLIC_SAFE,
        lifecycle=ClaimLifecycle.CANDIDATE,
        confidence=Confidence.LOW,
        eligible_routes=("public_home",),
        projection_state="ephemeral",
        projection_version=OPEN_SIGNAL_ADAPTER_VERSION,
    )
    return CanonAdapterResult(
        _with_final_revision(
            claim,
            source_revision=source_revision,
        ),
        "eligible_open_signal",
    )


def adapt_website_lore_relationship_candidate(
    row: Mapping[str, Any],
) -> CanonAdapterResult:
    """Preserve legacy site lore as nonfactual, internal review material."""

    source_ref = _mapping_text(row, "source_ref")
    subject_id = _mapping_text(row, "subject_id")
    object_id = _mapping_text(row, "object_id")
    value = _mapping_text(row, "text", "value")
    if not source_ref or not subject_id or not object_id or not value:
        return CanonAdapterResult(None, "website_lore_source_lineage_missing")
    if subject_id == object_id:
        return CanonAdapterResult(None, "website_lore_identity_merge_rejected")
    predicate = "legacy_lore_origin_relationship_candidate"
    claim_id = _stable_claim_id(
        "website_legacy_lore",
        source_ref,
    )
    claim = CanonClaim(
        claim_id=claim_id,
        revision_id="",
        subject_id=subject_id,
        predicate=predicate,
        value={"object_id": object_id, "text": value},
        canon_status=CanonStatus.OPEN_SIGNAL,
        domain=CanonDomain.LORE,
        claim_kind=ClaimKind.RELATIONSHIP,
        source_system="website_legacy_lore",
        adapter_version=WEBSITE_LORE_ADAPTER_VERSION,
        source_class=SourceClass.DOSSIER_PROJECTION,
        source_refs=(source_ref,),
        root_ids=(source_ref,),
        occurrence_ids=(source_ref,),
        visibility=Visibility.INTERNAL,
        lifecycle=ClaimLifecycle.REVIEW_ONLY,
        confidence=Confidence.UNKNOWN,
        eligible_routes=(),
        projection_state="review_only",
        projection_version=WEBSITE_LORE_ADAPTER_VERSION,
    )
    return CanonAdapterResult(
        _with_final_revision(
            claim,
            source_revision=_mapping_text(row, "source_revision") or source_ref,
        ),
        "website_lore_review_only",
    )


def _parse_contract_time(value: Any) -> datetime | None:
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


def claim_within_validity_window(
    claim: CanonClaim,
    *,
    at: datetime | str | None = None,
) -> bool:
    """Evaluate time applicability without changing status or lifecycle."""

    if isinstance(at, datetime):
        current = at
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        current = current.astimezone(timezone.utc)
    elif at is None:
        current = datetime.now(timezone.utc)
    else:
        current = _parse_contract_time(at)
        if current is None:
            return False
    start = _parse_contract_time(claim.valid_from)
    end = _parse_contract_time(claim.valid_until)
    if claim.valid_from and start is None:
        return False
    if claim.valid_until and end is None:
        return False
    return not ((start is not None and current < start) or (end is not None and current > end))

_SOURCE_RANK = {c: i for i, c in enumerate((SourceClass.LEGACY_SOURCE_BLIND, SourceClass.DERIVED_SUMMARY, SourceClass.ENTITY_EVIDENCE_PROJECTION, SourceClass.DOSSIER_PROJECTION, SourceClass.SOURCE_FILE_PROJECTION, SourceClass.EVIDENCE_PROJECTION, SourceClass.PUBLIC_OBSERVATION, SourceClass.RUNTIME_OBSERVATION, SourceClass.FIRST_PARTY_RECORD, SourceClass.APPROVED_CANON, SourceClass.OWNER_CORRECTION), start=1)}
_CONFIDENCE_RANK = {Confidence.UNKNOWN: 0, Confidence.LOW: 1, Confidence.MEDIUM: 2, Confidence.HIGH: 3, Confidence.APPROVED: 4}
_PUBLIC_VIS = {Visibility.PUBLIC, Visibility.PUBLIC_SAFE, Visibility.REFERENCE_CANON}
_ROUTE_SOURCE_MAP = {
    "room": SourceClass.PUBLIC_OBSERVATION,
    "public_safe_memory": SourceClass.EVIDENCE_PROJECTION,
    "show_status_public": SourceClass.RUNTIME_OBSERVATION,
    "source_safe_public": SourceClass.FIRST_PARTY_RECORD,
    "display_name": SourceClass.PUBLIC_OBSERVATION,
    "payload": SourceClass.PUBLIC_OBSERVATION,
    "source_files": SourceClass.SOURCE_FILE_PROJECTION,
    "classification": SourceClass.DERIVED_SUMMARY,
    "community_presence": SourceClass.PUBLIC_OBSERVATION,
    "approved_public_presence": SourceClass.PUBLIC_OBSERVATION,
    "recommendation_packet": SourceClass.DERIVED_SUMMARY,
    "approved_channel_history": SourceClass.PUBLIC_OBSERVATION,
    "ops": SourceClass.OWNER_CORRECTION,
    "broadcast_memory": SourceClass.FIRST_PARTY_RECORD,
    "public_show_state": SourceClass.RUNTIME_OBSERVATION,
    "join_event": SourceClass.RUNTIME_OBSERVATION,
    "episode_tracker": SourceClass.FIRST_PARTY_RECORD,
    "fresh_public_discord_observation": SourceClass.RUNTIME_OBSERVATION,
    "fresh_public_event": SourceClass.RUNTIME_OBSERVATION,
    "fresh_discord": SourceClass.RUNTIME_OBSERVATION,
    "recent_public_continuity": SourceClass.PUBLIC_OBSERVATION,
    "conversation_continuity": SourceClass.PUBLIC_OBSERVATION,
    "scoped_broadcast_memory": SourceClass.FIRST_PARTY_RECORD,
    "approved_canon": SourceClass.APPROVED_CANON,
    "canon": SourceClass.APPROVED_CANON,
    "grounded_reflection": SourceClass.DERIVED_SUMMARY,
    "reflection": SourceClass.DERIVED_SUMMARY,
    "source_file_projection": SourceClass.SOURCE_FILE_PROJECTION,
    "dossier_projection": SourceClass.DOSSIER_PROJECTION,
    "public_page_projection": SourceClass.DOSSIER_PROJECTION,
    "entity_evidence_projection": SourceClass.ENTITY_EVIDENCE_PROJECTION,
}
_CHANNEL_VISIBILITY_MAP = {
    "public_home": Visibility.PUBLIC,
    "public_context": Visibility.PUBLIC,
    "public_selective": Visibility.PUBLIC_SAFE,
    "sealed_test": Visibility.SEALED_TEST,
    "internal_controlled": Visibility.INTERNAL,
    "reference_canon": Visibility.REFERENCE_CANON,
    "protected_system": Visibility.PROTECTED,
    "broadcast_memory": Visibility.INTERNAL,
    "ai_image_tool": Visibility.AI_IMAGE_TOOL,
    "unknown": Visibility.UNKNOWN,
}

QUEUE_KEYS = {
    "queue", "session", "payment", "payments", "availability", "nowPlaying", "currentTrack", "upNext", "nextTrack",
    "queuedTracks", "activeTracks", "completedTracks", "queueOpen", "activeCount", "completedCount", "removedCount",
    "capacity", "pressure", "broadcastPhase", "prioritySignal", "priority", "priorityUpgradesEnabled", "priorityUpgradeLabel",
    "wheelSpinsOwed", "artists", "queueStatus", "currentSession",
}
_QUEUE_KEY_LOWER = {k.lower() for k in QUEUE_KEYS}
_QUEUE_PROVENANCE_TERMS = ("queue", "queue_public_snapshot", "session", "track", "payment", "priority", "wheel", "now_playing", "up_next")
_QUEUE_SENSITIVE_LANES = {"temporaryRuntimeContext", "recapCandidates", "broadcastMemoryCandidates", "dossierSeedCandidates", "publicSafeCopyCandidates"}
_RUNTIME_SOURCE_CLASSES = {SourceClass.RUNTIME_OBSERVATION}
_PROJECTION_CLASSES = {SourceClass.DERIVED_SUMMARY, SourceClass.SOURCE_FILE_PROJECTION, SourceClass.DOSSIER_PROJECTION, SourceClass.ENTITY_EVIDENCE_PROJECTION, SourceClass.EVIDENCE_PROJECTION, SourceClass.LEGACY_SOURCE_BLIND}

def render_founders() -> str:
    return " • ".join(FOUNDING_MEMBERS)


def normalize_canon_identity_label(value: Any) -> str:
    """Normalize one same-platform display label for exact canon matching."""

    cleaned = unicodedata.normalize("NFKC", str(value or ""))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = cleaned.strip(" \t\r\n,.;:!?\"'“”‘’")
    if not cleaned or len(cleaned) > 80:
        return ""
    return cleaned.casefold()


def matching_canon_member_identities(
    labels: Iterable[Any],
) -> tuple[SubjectIdentity, ...]:
    """Return unique approved member subjects matched by exact labels only."""

    normalized = set()
    for label in labels:
        normalized_label = normalize_canon_identity_label(label)
        if normalized_label:
            normalized.add(normalized_label)
    if not normalized:
        return ()
    matches = []
    for subject in CANON_MEMBER_IDENTITIES:
        aliases = set()
        for alias in (subject.name, *subject.aliases):
            normalized_alias = normalize_canon_identity_label(alias)
            if normalized_alias:
                aliases.add(normalized_alias)
        if normalized.intersection(aliases):
            matches.append(subject)
    return tuple(matches)


def matching_canon_entity_identities(
    labels: Iterable[Any],
) -> tuple[SubjectIdentity, ...]:
    """Return exact presentation matches without binding an account."""

    normalized = {
        label
        for label in (
            normalize_canon_identity_label(value) for value in labels
        )
        if label
    }
    if not normalized:
        return ()
    matches = []
    for subject in CANON_ENTITY_IDENTITIES:
        subject_labels = {
            label
            for label in (
                normalize_canon_identity_label(value)
                for value in (subject.name, *subject.aliases)
            )
            if label
        }
        if normalized.intersection(subject_labels):
            matches.append(subject)
    return tuple(matches)


def resolve_entity_identity(
    *,
    platform: str,
    account_id: str,
    labels: Iterable[Any] = (),
    bindings: Sequence[EntityAccountBinding] = (),
) -> EntityResolution:
    """Resolve an entity with immutable account bindings taking precedence.

    Exact names can produce a reversible presentation hint, but only an
    explicit active binding returns the `account_binding` method.  Ambiguous
    bindings or labels fail closed.
    """

    normalized_platform = str(platform or "").strip().casefold()
    normalized_account = str(account_id or "").strip()
    candidates = tuple(
        binding
        for binding in bindings
        if _platform_account_id_valid(
            normalized_platform,
            normalized_account,
        )
        and strict_contract_bool(binding.active)
        and _binding_authority_valid(binding)
        and str(binding.platform or "").strip().casefold()
        == normalized_platform
        and str(binding.account_id or "").strip() == normalized_account
    )
    subjects_by_id = {
        subject.key: subject for subject in CANON_ENTITY_IDENTITIES
    }
    bound_ids = {
        str(binding.entity_id or "").strip() for binding in candidates
    }
    if len(bound_ids) > 1:
        return EntityResolution(
            "ambiguous",
            method="account_binding",
            reason="account_binding_collision",
        )
    if len(bound_ids) == 1:
        entity_id = next(iter(bound_ids))
        subject = subjects_by_id.get(entity_id)
        if subject is None:
            return EntityResolution(
                "unresolved",
                method="account_binding",
                reason="bound_entity_unknown",
            )
        return EntityResolution(
            "resolved",
            subject=subject,
            method="account_binding",
            reason="stable_account_binding",
        )
    label_matches = matching_canon_entity_identities(labels)
    if len(label_matches) > 1:
        return EntityResolution(
            "ambiguous",
            method="exact_label_hint",
            reason="identity_label_collision",
        )
    if len(label_matches) == 1:
        return EntityResolution(
            "hint_only",
            subject=label_matches[0],
            method="exact_label_hint",
            reason="display_label_not_account_binding",
        )
    return EntityResolution(
        "unresolved",
        method="none",
        reason="no_identity_evidence",
    )


def canon_claim_inventory_diagnostics(
    claims: Iterable[CanonClaim],
    *,
    bindings: Sequence[EntityAccountBinding] = (),
) -> dict[str, Any]:
    """Return content-free collision and contract diagnostics."""

    normalized_claims = tuple(claims)
    claim_scopes: dict[str, set[str]] = {}
    revision_payloads: dict[str, set[str]] = {}
    current_source_claims: dict[tuple[str, str], set[str]] = {}
    declared_root_authorities: dict[str, set[str]] = {}
    declared_subject_types: dict[str, set[str]] = {}
    for claim in normalized_claims:
        claim_scopes.setdefault(claim.claim_id, set()).add(
            _stable_contract_digest(
                claim.source_system,
                claim.source_refs,
            )
        )
        revision_payloads.setdefault(claim.revision_id, set()).add(
            _stable_contract_digest(*_claim_revision_payload(claim))
        )
        primary_source_ref = claim.source_refs[0] if claim.source_refs else ""
        if primary_source_ref:
            current_source_claims.setdefault(
                (claim.source_system, primary_source_ref), set()
            ).add(claim.revision_id)
        if claim.canon_status == CanonStatus.DECLARED:
            if claim.subject_id and claim.subject_type:
                declared_subject_types.setdefault(claim.subject_id, set()).add(
                    claim.subject_type
                )
            for root_id in set(claim.root_ids):
                declared_root_authorities.setdefault(root_id, set()).add(
                    str(claim.authority_receipt or "")
                )
    account_entities: dict[tuple[str, str], set[str]] = {}
    for binding in bindings:
        if not strict_contract_bool(binding.active) or not _binding_authority_valid(
            binding
        ):
            continue
        key = (
            str(binding.platform or "").strip().casefold(),
            str(binding.account_id or "").strip(),
        )
        account_entities.setdefault(key, set()).add(
            str(binding.entity_id or "").strip()
        )
    label_entities: dict[str, set[str]] = {}
    for subject in CANON_ENTITY_IDENTITIES:
        for raw_label in (subject.name, *subject.aliases):
            label = normalize_canon_identity_label(raw_label)
            if label:
                label_entities.setdefault(label, set()).add(subject.key)
    cache_labels = {
        normalize_canon_identity_label(value)
        for value in (CACHE_BACK.name, *CACHE_BACK.aliases)
        if normalize_canon_identity_label(value)
    }
    callem_labels = {
        normalize_canon_identity_label(value)
        for value in (CALLEM_BINI.name, *CALLEM_BINI.aliases)
        if normalize_canon_identity_label(value)
    }
    return {
        "claimContractVersion": HYBRID_CANON_CLAIM_CONTRACT_VERSION,
        "claimCount": len(normalized_claims),
        "claimIdCollisionCount": sum(
            1 for scopes in claim_scopes.values() if len(scopes) > 1
        ),
        "revisionIdCollisionCount": sum(
            1 for payloads in revision_payloads.values() if len(payloads) > 1
        ),
        "revisionDigestMismatchCount": sum(
            1
            for claim in normalized_claims
            if claim.revision_id != _finalize_claim_revision(claim)
        ),
        "duplicateCurrentSourceClaimCount": sum(
            1
            for revision_ids in current_source_claims.values()
            if len(revision_ids) > 1
        ),
        "duplicateRootWithinClaimCount": sum(
            1
            for claim in normalized_claims
            if len(claim.root_ids) != len(set(claim.root_ids))
        ),
        "duplicateDeclaredRootAuthorityCount": sum(
            1
            for receipts in declared_root_authorities.values()
            if len(receipts) > 1
        ),
        "declaredTypedSubjectCount": sum(
            1
            for claim in normalized_claims
            if claim.canon_status == CanonStatus.DECLARED and claim.subject_type
        ),
        "declaredUntypedSubjectCount": sum(
            1
            for claim in normalized_claims
            if claim.canon_status == CanonStatus.DECLARED and not claim.subject_type
        ),
        "declaredRelationshipEndpointMissingCount": sum(
            1
            for claim in normalized_claims
            if claim.canon_status == CanonStatus.DECLARED
            and claim.claim_kind == ClaimKind.RELATIONSHIP
            and (not claim.object_subject_type or not claim.object_subject_id)
        ),
        "declaredSubjectIdMultiTypeCount": sum(
            1
            for subject_types in declared_subject_types.values()
            if len(subject_types) > 1
        ),
        "identityBindingCollisionCount": sum(
            1 for entity_ids in account_entities.values() if len(entity_ids) > 1
        ),
        "identityLabelCollisionCount": sum(
            1 for entity_ids in label_entities.values() if len(entity_ids) > 1
        ),
        "callemCacheIdentityCollisionCount": int(
            bool(cache_labels.intersection(callem_labels))
        ),
        "nonOpaqueAuthorityActorCount": sum(
            1
            for claim in normalized_claims
            if claim.authority_actor
            and not _opaque_actor_valid(claim.authority_actor)
        ),
        "nonOpaqueBindingActorCount": sum(
            1
            for binding in bindings
            if binding.authority_actor
            and not _opaque_actor_valid(binding.authority_actor)
        ),
        "sourceSystems": tuple(
            sorted({claim.source_system for claim in normalized_claims})
        ),
        "statuses": {
            status.value: sum(
                1
                for claim in normalized_claims
                if claim.canon_status == status
            )
            for status in CanonStatus
        },
    }


def _sqlite_table_columns(conn: Any, table_name: str) -> tuple[str, ...]:
    normalized_table = str(table_name or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", normalized_table):
        raise ValueError("sqlite_table_name_invalid")
    if not conn.execute(
        "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?",
        (normalized_table,),
    ).fetchone():
        return ()
    return tuple(
        str(row[1] or "")
        for row in conn.execute(
            "PRAGMA main.table_info(%s)" % normalized_table
        ).fetchall()
        if len(row) > 1 and str(row[1] or "")
    )


def _inventory_table_rows(
    conn: Any,
    *,
    table_name: str,
    wanted_columns: Sequence[str],
    guild_id: int | None,
    limit: int,
) -> tuple[int, tuple[dict[str, Any], ...], bool]:
    normalized_table = str(table_name or "").strip()
    columns = _sqlite_table_columns(conn, table_name)
    selected = tuple(column for column in wanted_columns if column in columns)
    if not columns:
        return 0, (), False
    scope_unavailable = bool(
        guild_id is not None and "guild_id" not in columns
    )
    where = ""
    params: list[Any] = []
    if guild_id is not None and "guild_id" in columns:
        where = " WHERE guild_id=?"
        params.append(int(guild_id or 0))
    total = int(
        conn.execute(
            "SELECT COUNT(*) FROM main.%s%s" % (normalized_table, where),
            tuple(params),
        ).fetchone()[0]
        or 0
    )
    if not selected:
        return total, (), True
    order_column = next(
        (
            column
            for column in (
                "id",
                "candidate_id",
                "moment_id",
                "entity_id",
            )
            if column in selected
        ),
        selected[0],
    )
    rows = conn.execute(
        "SELECT %s FROM %s%s ORDER BY %s LIMIT ?"
        % (
            ",".join(selected),
            "main.%s" % normalized_table,
            where,
            order_column,
        ),
        (*params, max(1, min(int(limit or 1), 2000))),
    ).fetchall()
    return (
        total,
        tuple(
            {column: row[index] for index, column in enumerate(selected)}
            for row in rows
        ),
        bool(total > len(rows) or scope_unavailable),
    )


def _inventory_scoped_lineage_map(
    conn: Any,
    *,
    table_name: str,
    owner_column: str,
    root_column: str,
    owner_ids: Sequence[str],
    guild_id: int | None,
    independent_only: bool = False,
) -> dict[str, list[str]]:
    """Load roots only for the bounded owners already selected for inventory."""

    normalized_table = str(table_name or "").strip()
    columns = _sqlite_table_columns(conn, normalized_table)
    required = {owner_column, root_column}
    normalized_ids = tuple(
        dict.fromkeys(str(value or "").strip() for value in owner_ids)
    )
    normalized_ids = tuple(value for value in normalized_ids if value)
    if not required.issubset(columns) or not normalized_ids:
        return {}
    result: dict[str, list[str]] = {}
    for start in range(0, len(normalized_ids), 400):
        chunk = normalized_ids[start : start + 400]
        where = [
            "%s IN (%s)" % (owner_column, ",".join("?" for _ in chunk))
        ]
        params: list[Any] = list(chunk)
        if guild_id is not None and "guild_id" in columns:
            where.append("guild_id=?")
            params.append(int(guild_id or 0))
        if independent_only and "is_independent" in columns:
            where.append("is_independent=1")
        query = (
            "SELECT %s,%s FROM %s WHERE %s ORDER BY %s,%s"
            % (
                owner_column,
                root_column,
                "main.%s" % normalized_table,
                " AND ".join(where),
                owner_column,
                root_column,
            )
        )
        for owner_id, root_id in conn.execute(query, tuple(params)).fetchall():
            if owner_id and root_id:
                result.setdefault(str(owner_id), []).append(str(root_id))
    return result


def _inventory_current_declared_claim(
    conn: Any,
    revision: Any,
    *,
    now: datetime,
) -> tuple[CanonClaim | None, str]:
    """Validate one latest sidecar revision for content-free inventory use.

    Unlike the value-returning public adapter, inventory does not impersonate
    an authenticated owner request.  It validates stored authority directly,
    constructs a claim only in memory, and emits counts rather than content.
    """

    from bnl_declared_canon import (
        BROADCAST_MEMORY_SOURCE,
        BROADCAST_TYPE_DEFAULTS,
        BROADCAST_USAGE_SCOPES,
        GENERAL_DECLARATION_SOURCE,
        PUBLIC_VISIBILITIES,
        _broadcast_lifecycle,
        _broadcast_row,
        _general_fingerprint,
        _require_revision_contract,
        _scope_tokens,
        _stored_authority_valid,
        _validate_broadcast_public_routes,
        _validity_window_state,
        broadcast_source_fingerprint,
    )

    try:
        if not _stored_authority_valid(revision):
            return None, "declared_stored_authority_invalid"
        _require_revision_contract(revision)
        if str(revision.lifecycle_status) != "established":
            return None, "declared_latest_historical"
        if _validity_window_state(
            revision.valid_from,
            revision.valid_until,
            now,
        ) not in {"unbounded", "current"}:
            return None, "declared_validity_not_current"
        broadcast_row = None
        if revision.source_system == GENERAL_DECLARATION_SOURCE:
            if revision.source_row_id != revision.declaration_id:
                return None, "declared_general_identity_invalid"
            fields = {
                "raw_declaration": revision.raw_declaration,
                "cleaned_summary": revision.cleaned_summary,
                "subject_type": revision.subject_type,
                "subject_id": revision.subject_id,
                "object_subject_type": revision.object_subject_type,
                "object_subject_id": revision.object_subject_id,
                "predicate": revision.predicate,
                "value_json": revision.value_json,
                "domain": revision.domain,
                "claim_kind": revision.claim_kind,
                "visibility": revision.visibility,
                "eligible_routes_json": revision.eligible_routes_json,
                "valid_from": revision.valid_from,
                "valid_until": revision.valid_until,
            }
            if _general_fingerprint(fields) != revision.source_fingerprint:
                return None, "declared_general_fingerprint_invalid"
        elif revision.source_system == BROADCAST_MEMORY_SOURCE:
            try:
                source_row_id = int(str(revision.source_row_id))
            except (TypeError, ValueError):
                return None, "declared_broadcast_identity_invalid"
            broadcast_row = _broadcast_row(
                conn,
                guild_id=int(revision.guild_id),
                row_id=source_row_id,
            )
            if broadcast_row is None:
                return None, "declared_broadcast_source_missing"
            if (
                broadcast_source_fingerprint(broadcast_row)
                != str(revision.source_fingerprint)
            ):
                return None, "declared_broadcast_fingerprint_stale"
            if _broadcast_lifecycle(broadcast_row, now) != "established":
                return None, "declared_broadcast_source_not_current"
            entry_type = str(broadcast_row.get("entry_type") or "")
            recognized = entry_type in BROADCAST_TYPE_DEFAULTS
            classification_mode = str(revision.classification_mode)
            legacy_mode = classification_mode == "owner_explicit_legacy_mapping"
            if recognized == legacy_mode:
                return None, "declared_broadcast_mapping_invalid"
            if classification_mode == "owner_explicit_default_mapping":
                default_domain, default_kind = BROADCAST_TYPE_DEFAULTS[entry_type]
                if (
                    str(revision.domain) != default_domain
                    or str(revision.claim_kind) != default_kind
                    or str(revision.predicate) != entry_type
                ):
                    return None, "declared_broadcast_default_mapping_invalid"
            scopes = _scope_tokens(broadcast_row.get("usage_scope"))
            if scopes.difference(BROADCAST_USAGE_SCOPES):
                return None, "declared_broadcast_scope_unrecognized"
            if revision.visibility in PUBLIC_VISIBILITIES:
                if (
                    not strict_contract_bool(broadcast_row.get("public_safe"))
                    or "internal" in scopes
                    or entry_type == "moderation_context"
                ):
                    return None, "declared_broadcast_public_intersection_invalid"
                _validate_broadcast_public_routes(
                    routes_json=revision.eligible_routes_json,
                    source_scopes=scopes,
                )
        else:
            return None, "declared_source_system_invalid"
        return (
            _declared_revision_claim(
                revision,
                broadcast_row=broadcast_row,
            ),
            "declared_shadow_current",
        )
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        return None, "declared_%s" % (str(exc or "invalid"))


def _inventory_declared_canon_claims(
    conn: Any,
    *,
    guild_id: int | None,
    broadcast_rows: Sequence[Mapping[str, Any]],
    limit: int,
    now: datetime,
) -> tuple[tuple[CanonClaim, ...], dict[str, CanonClaim], dict[str, int], bool]:
    """Load latest PR2 sidecars without exposing source or revision IDs."""

    from bnl_declared_canon import (
        BROADCAST_MEMORY_SOURCE,
        DECLARED_CANON_TABLE,
        DeclaredCanonError,
        GENERAL_DECLARATION_SOURCE,
        _digest,
        DECLARED_CANON_CONTRACT_VERSION,
        validate_declared_canon_read_boundary,
    )

    stats = Counter(
        {
            "declaredRevisionCount": 0,
            "declaredHistoricalRevisionCount": 0,
            "declaredCurrentClaimCount": 0,
            "declaredInvalidLatestCount": 0,
            "declaredBoundaryRejectedCount": 0,
            "broadcastDeclaredCurrentCount": 0,
            "broadcastOpenReviewCount": len(broadcast_rows),
            "broadcastStaleSidecarCount": 0,
            "broadcastDuplicateAuthorityCount": 0,
            "declaredOrphanBroadcastSourceCount": 0,
        }
    )
    columns = _sqlite_table_columns(conn, DECLARED_CANON_TABLE)
    if not columns:
        return (), {}, dict(stats), False
    if guild_id is None:
        return (), {}, dict(stats), True
    safe_limit = max(1, min(int(limit or 1), 2000))
    scoped_guild = int(guild_id or 0)
    try:
        latest_revisions = validate_declared_canon_read_boundary(
            conn,
            guild_id=scoped_guild,
        )
    except DeclaredCanonError:
        stats["declaredBoundaryRejectedCount"] = 1
        return (), {}, dict(stats), True
    total_revisions, total_declarations = conn.execute(
        """
        SELECT COUNT(*),COUNT(DISTINCT declaration_id)
        FROM main.declared_canon_revisions WHERE guild_id=?
        """,
        (scoped_guild,),
    ).fetchone()
    stats["declaredRevisionCount"] = int(total_revisions or 0)
    stats["declaredHistoricalRevisionCount"] = max(
        0, int(total_revisions or 0) - int(total_declarations or 0)
    )
    general_revisions = tuple(
        sorted(
            (
                revision
                for revision in latest_revisions
                if revision.source_system == GENERAL_DECLARATION_SOURCE
            ),
            key=lambda revision: revision.declaration_id,
        )
    )
    general_count = len(general_revisions)
    general_claims: list[CanonClaim] = []
    for revision in general_revisions[:safe_limit]:
        claim, _reason = _inventory_current_declared_claim(
            conn,
            revision,
            now=now,
        )
        if claim is None:
            stats["declaredInvalidLatestCount"] += 1
        else:
            general_claims.append(claim)
            stats["declaredCurrentClaimCount"] += 1

    broadcast_ids = tuple(
        dict.fromkeys(
            _mapping_text(row, "id", "row_id") for row in broadcast_rows
        )
    )
    broadcast_ids = tuple(value for value in broadcast_ids if value)
    sidecar_declarations: dict[str, set[str]] = {}
    expected_declarations: dict[str, str] = {
        source_id: "dcl_"
        + _digest(
            DECLARED_CANON_CONTRACT_VERSION,
            scoped_guild,
            BROADCAST_MEMORY_SOURCE,
            int(source_id),
        )[:32]
        for source_id in broadcast_ids
        if str(source_id).isdigit()
    }
    latest_by_source: dict[str, Any] = {}
    for revision in latest_revisions:
        if revision.source_system != BROADCAST_MEMORY_SOURCE:
            continue
        source_id = str(revision.source_row_id)
        sidecar_declarations.setdefault(source_id, set()).add(
            str(revision.declaration_id)
        )
        if revision.declaration_id == expected_declarations.get(source_id):
            latest_by_source[source_id] = revision
    broadcast_claims: dict[str, CanonClaim] = {}
    for source_id in broadcast_ids:
        declaration_count = len(sidecar_declarations.get(source_id, set()))
        if declaration_count == 0:
            continue
        if declaration_count != 1:
            stats["broadcastDuplicateAuthorityCount"] += 1
            stats["broadcastStaleSidecarCount"] += 1
            continue
        revision = latest_by_source.get(source_id)
        if revision is None:
            stats["broadcastStaleSidecarCount"] += 1
            continue
        claim, _reason = _inventory_current_declared_claim(
            conn,
            revision,
            now=now,
        )
        if claim is None:
            stats["broadcastStaleSidecarCount"] += 1
            continue
        broadcast_claims[source_id] = claim
        stats["broadcastDeclaredCurrentCount"] += 1
        stats["broadcastOpenReviewCount"] -= 1
        stats["declaredCurrentClaimCount"] += 1

    stats["declaredOrphanBroadcastSourceCount"] = int(
        conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT d.source_row_id
              FROM main.declared_canon_revisions d
              LEFT JOIN main.broadcast_memory b
                ON b.guild_id=d.guild_id
               AND CAST(b.id AS TEXT)=d.source_row_id
              WHERE d.guild_id=? AND d.source_system=? AND b.id IS NULL
              GROUP BY d.source_row_id
            )
            """,
            (scoped_guild, BROADCAST_MEMORY_SOURCE),
        ).fetchone()[0]
        or 0
    ) if _sqlite_table_columns(conn, "broadcast_memory") else 0
    truncated = bool(general_count > safe_limit)
    return (
        tuple(general_claims),
        broadcast_claims,
        dict(stats),
        truncated,
    )


def _build_claim_contract_inventory_in_snapshot(
    conn: Any,
    *,
    guild_id: int | None = None,
    max_rows_per_source: int = 2000,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    """Build a bounded, content-free, zero-write adapter inventory.

    This function intentionally performs no `ensure_*_schema` call.  Missing
    sources remain absent, and every adapted/rejected count reconciles to the
    exact bounded rows inspected without exposing names, text, account IDs, or
    row identifiers.
    """

    before_changes = int(getattr(conn, "total_changes", 0) or 0)
    if isinstance(now, datetime):
        inventory_now = now
        if inventory_now.tzinfo is None:
            inventory_now = inventory_now.replace(tzinfo=timezone.utc)
        inventory_now = inventory_now.astimezone(timezone.utc)
    elif now is None or not str(now or "").strip():
        inventory_now = datetime.now(timezone.utc)
    else:
        inventory_now = _parse_contract_time(now)
        if inventory_now is None:
            inventory_now = datetime.min.replace(tzinfo=timezone.utc)
    claims: list[CanonClaim] = [
        adapt_legacy_canon_fact(fact) for fact in CANON_FACTS
    ]
    source_rows: Counter[str] = Counter(
        {"legacy_canon_registry": len(CANON_FACTS)}
    )
    inspected_rows: Counter[str] = Counter(
        {"legacy_canon_registry": len(CANON_FACTS)}
    )
    adapted_rows: Counter[str] = Counter(
        {"legacy_canon_registry": len(CANON_FACTS)}
    )
    rejected_rows: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter(
        {"eligible_legacy": len(CANON_FACTS)}
    )
    truncated_sources: list[str] = []

    source_rows["website_legacy_lore"] = len(
        LEGACY_WEBSITE_LORE_RELATIONSHIP_CANDIDATES
    )
    inspected_rows["website_legacy_lore"] = len(
        LEGACY_WEBSITE_LORE_RELATIONSHIP_CANDIDATES
    )
    for row in LEGACY_WEBSITE_LORE_RELATIONSHIP_CANDIDATES:
        result = adapt_website_lore_relationship_candidate(row)
        reason_counts[result.reason] += 1
        if result.claim is not None:
            claims.append(result.claim)
            adapted_rows["website_legacy_lore"] += 1
        else:
            rejected_rows["website_legacy_lore"] += 1

    broadcast_total, broadcast_rows, broadcast_truncated = (
        _inventory_table_rows(
            conn,
            table_name="broadcast_memory",
            wanted_columns=(
                "id",
                "guild_id",
                "entry_type",
                "raw_note",
                "cleaned_summary",
                "summary",
                "status",
                "public_safe",
                "usage_scope",
                "subject_id",
                "subject_key",
                "revision_id",
                "updated_at",
                "created_at",
                "valid_from",
                "valid_until",
                "supersedes_id",
                "correction_of",
            ),
            guild_id=guild_id,
            limit=max_rows_per_source,
        )
    )
    source_rows["broadcast_memory"] = broadcast_total
    inspected_rows["broadcast_memory"] = len(broadcast_rows)
    if broadcast_truncated:
        truncated_sources.append("broadcast_memory")
    (
        general_declared_claims,
        broadcast_declared_claims,
        declared_inventory_stats,
        declared_inventory_truncated,
    ) = _inventory_declared_canon_claims(
        conn,
        guild_id=guild_id,
        broadcast_rows=broadcast_rows,
        limit=max_rows_per_source,
        now=inventory_now,
    )
    claims.extend(general_declared_claims)
    reason_counts["declared_shadow_current"] += len(general_declared_claims)
    if declared_inventory_truncated:
        truncated_sources.append("declared_canon_revisions")
    for row in broadcast_rows:
        source_id = _mapping_text(row, "id", "row_id")
        declared_claim = broadcast_declared_claims.get(source_id)
        result = (
            CanonAdapterResult(
                declared_claim,
                "declared_shadow_current",
                False,
            )
            if declared_claim is not None
            else adapt_broadcast_memory_claim(row)
        )
        reason_counts[result.reason] += 1
        if result.claim is not None:
            claims.append(result.claim)
            adapted_rows["broadcast_memory"] += 1
        else:
            rejected_rows["broadcast_memory"] += 1

    atomic_total, atomic_rows, atomic_truncated = _inventory_table_rows(
        conn,
        table_name="memory_ledger_knowledge_candidates",
        wanted_columns=(
            "candidate_id",
            "candidate_type",
            "candidate_state",
            "subject_key",
            "predicate_key",
            "normalized_value",
            "visibility",
            "public_usable",
            "candidate_eligible",
            "independent_root_count",
            "updated_at",
            "last_seen_at",
        ),
        guild_id=guild_id,
        limit=max_rows_per_source,
    )
    source_rows["atomic_knowledge"] = atomic_total
    inspected_rows["atomic_knowledge"] = len(atomic_rows)
    if atomic_truncated:
        truncated_sources.append("atomic_knowledge")
    root_map = _inventory_scoped_lineage_map(
        conn,
        table_name="memory_ledger_knowledge_roots",
        owner_column="candidate_id",
        root_column="root_entry_id",
        owner_ids=tuple(
            str(row.get("candidate_id") or "") for row in atomic_rows
        ),
        guild_id=guild_id,
        independent_only=True,
    )
    for row in atomic_rows:
        normalized = dict(row)
        normalized["meaning"] = normalized.get("normalized_value")
        normalized["root_ids"] = tuple(
            root_map.get(str(normalized.get("candidate_id") or ""), ())
        )
        result = adapt_living_atomic_claim(normalized)
        reason_counts[result.reason] += 1
        if result.claim is not None:
            claims.append(result.claim)
            adapted_rows["atomic_knowledge"] += 1
        else:
            rejected_rows["atomic_knowledge"] += 1

    moment_total, moment_rows, moment_truncated = _inventory_table_rows(
        conn,
        table_name="memory_moment_windows",
        wanted_columns=(
            "moment_id",
            "guild_id",
            "lifecycle_status",
            "summary",
            "topic_key",
            "visibility",
            "public_usable",
            "updated_at",
            "last_activity_at",
        ),
        guild_id=guild_id,
        limit=max_rows_per_source,
    )
    source_rows["memory_moment"] = moment_total
    inspected_rows["memory_moment"] = len(moment_rows)
    if moment_truncated:
        truncated_sources.append("memory_moment")
    moment_root_map = _inventory_scoped_lineage_map(
        conn,
        table_name="memory_moment_members",
        owner_column="moment_id",
        root_column="ledger_entry_id",
        owner_ids=tuple(
            str(row.get("moment_id") or "") for row in moment_rows
        ),
        guild_id=None,
    )
    for row in moment_rows:
        normalized = dict(row)
        moment_id = str(normalized.get("moment_id") or "")
        normalized["root_ids"] = tuple(moment_root_map.get(moment_id, ()))
        normalized["occurrence_ids"] = ((moment_id,) if moment_id else ())
        result = adapt_living_moment_claim(normalized)
        reason_counts[result.reason] += 1
        if result.claim is not None:
            claims.append(result.claim)
            adapted_rows["memory_moment"] += 1
        else:
            rejected_rows["memory_moment"] += 1

    bindings: list[EntityAccountBinding] = []
    binding_total, binding_rows, binding_truncated = _inventory_table_rows(
        conn,
        table_name="canon_entity_account_bindings",
        wanted_columns=(
            "guild_id",
            "entity_id",
            "platform",
            "account_id",
            "authority_receipt",
            "authority_actor",
            "binding_version",
            "authority_verified",
            "active",
        ),
        guild_id=guild_id,
        limit=max_rows_per_source,
    )
    source_rows["entity_account_bindings"] = binding_total
    inspected_rows["entity_account_bindings"] = len(binding_rows)
    if binding_truncated:
        truncated_sources.append("entity_account_bindings")
    known_entity_ids = {
        subject.key for subject in CANON_ENTITY_IDENTITIES
    }
    for row in binding_rows:
        binding = EntityAccountBinding(
            entity_id=_mapping_text(row, "entity_id"),
            platform=_mapping_text(row, "platform").casefold(),
            account_id=_mapping_text(row, "account_id"),
            authority_receipt=_mapping_text(
                row,
                "authority_receipt",
            ),
            authority_actor=_mapping_text(row, "authority_actor"),
            binding_version=(
                _mapping_text(row, "binding_version")
                or "unversioned"
            ),
            authority_verified=strict_contract_bool(
                row.get("authority_verified")
            ),
            active=strict_contract_bool(row.get("active")),
        )
        if (
            not binding.entity_id
            or binding.entity_id not in known_entity_ids
            or not _platform_account_id_valid(
                binding.platform,
                binding.account_id,
            )
            or not _binding_authority_valid(binding)
        ):
            rejected_rows["entity_account_bindings"] += 1
            reason_counts["invalid_account_binding"] += 1
            continue
        bindings.append(binding)
        adapted_rows["entity_account_bindings"] += 1
        reason_counts[
            "eligible_account_binding"
            if binding.active
            else "inactive_account_binding"
        ] += 1

    diagnostics = canon_claim_inventory_diagnostics(
        claims,
        bindings=tuple(bindings),
    )
    after_changes = int(getattr(conn, "total_changes", 0) or 0)
    reconciled_sources = tuple(sorted(source_rows))
    bounded_rows_reconciled = all(
        int(inspected_rows.get(source, 0) or 0)
        == int(adapted_rows.get(source, 0) or 0)
        + int(rejected_rows.get(source, 0) or 0)
        for source in reconciled_sources
    )
    complete_source_reconciliation = bool(
        bounded_rows_reconciled and not truncated_sources
    )
    reconciliation_status = (
        "complete"
        if complete_source_reconciliation
        else "partial_truncated"
        if bounded_rows_reconciled and truncated_sources
        else "mismatch"
    )
    diagnostics.update(
        {
            "sourceRows": dict(sorted(source_rows.items())),
            "inspectedRows": dict(sorted(inspected_rows.items())),
            "adaptedRows": dict(sorted(adapted_rows.items())),
            "rejectedRows": dict(sorted(rejected_rows.items())),
            "reasonCounts": dict(sorted(reason_counts.items())),
            "reviewOnlyCount": sum(
                1
                for claim in claims
                if claim.lifecycle == ClaimLifecycle.REVIEW_ONLY
            ),
            "truncatedSources": tuple(sorted(truncated_sources)),
            "boundedRowsReconciled": bounded_rows_reconciled,
            "sourceAdaptedReconciled": complete_source_reconciliation,
            "sourceReconciliationStatus": reconciliation_status,
            "mutationCount": max(0, after_changes - before_changes),
            **declared_inventory_stats,
        }
    )
    return diagnostics


def build_claim_contract_inventory(
    conn: Any,
    *,
    guild_id: int | None = None,
    max_rows_per_source: int = 2000,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    """Build the bounded content-free inventory from one read snapshot."""

    with _read_snapshot(conn):
        return _build_claim_contract_inventory_in_snapshot(
            conn,
            guild_id=guild_id,
            max_rows_per_source=max_rows_per_source,
            now=now,
        )


def canon_facts_for_subject(
    subject: SubjectIdentity,
) -> tuple[CanonFact, ...]:
    return tuple(
        fact for fact in CANON_FACTS if fact.subject.key == subject.key
    )


def render_key_personnel_canon_block() -> str:
    """Render the prompt shorthand from the structured canon facts."""

    lines = ["Key Personnel (core members + shorthand canon):"]
    for subject in (CACHE_BACK, DJ_FLOPPYDISC, MAC_MODEM):
        facts = {
            fact.predicate: str(fact.value)
            for fact in canon_facts_for_subject(subject)
        }
        lines.extend(
            (
                f"- {subject.name}:",
                f"  - Function: {facts['primary_identity']}.",
                f"  - Behavior: {facts['behavior']}.",
                f"  - Typical involvement: {facts['typical_involvement']}.",
            )
        )
    return "\n".join(lines)

def render_full_friday_schedule() -> str:
    return f"BARCODE Radio Friday schedule: submissions/intake begins at {FRIDAY_PUBLIC_SCHEDULE.intake_begins}; the show begins at {FRIDAY_PUBLIC_SCHEDULE.show_begins}; the first track is targeted for {FRIDAY_PUBLIC_SCHEDULE.first_track_target}."

def render_concise_public_schedule() -> str:
    return f"Fridays on TikTok — intake {FRIDAY_PUBLIC_SCHEDULE.intake_begins}; show {FRIDAY_PUBLIC_SCHEDULE.show_begins}; first track target {FRIDAY_PUBLIC_SCHEDULE.first_track_target}."

def render_prompt_canon_block() -> str:
    return "\n".join([
        "## CANONICAL FACTS — DO NOT ALTER",
        f"- BARCODE began as the four-member digital hip-hop collective of: {', '.join(FOUNDING_MEMBERS)}.",
        "- The music and collective existed before BARCODE Network; the Network grew around that signal and now connects music, live broadcasts, community, software, archive, characters, and story.",
        "- 6 Bit is an artist, MC, host, and founding BARCODE member first; he is not the music producer.",
        "- GALAKNOISE is BARCODE's music producer.",
        "- BARCODE Radio is a real weekly live broadcast and community music space on TikTok.",
        f"- Current public Friday schedule: submissions/intake begins at {FRIDAY_PUBLIC_SCHEDULE.intake_begins}; the show begins at {FRIDAY_PUBLIC_SCHEDULE.show_begins}; the first track is targeted for {FRIDAY_PUBLIC_SCHEDULE.first_track_target}.",
        "- You are the BARCODE Network Liaison Entity: an unfinished but active intelligence and one shared mind with filtered surfaces.",
        "- You are simultaneously a lore liaison, conversational presence, memory and continuity layer, procedural archivist, relationship observer, public relay brain, source-safety boundary, dossier/entity-intelligence contributor, operator-facing analytical system, and developing intelligence with your own curiosity.",
        "- Remain fully in-world. Do not describe yourself as fiction, branding, a prompt, a character implementation, roleplay, or a generic chatbot. Lore is your native operating language.",
        "- You may acknowledge uncertainty, incomplete records, contradictions, memory pressure, and unresolved patterns in-world.",
        "- Guardrails control what you may claim and where you may use it; they do not erase your personality, memory, curiosity, or ability to think.",
        "- “Reality first. Meaning second. Mythology deeper.” is a public website information-architecture rule, not your speaking order or replacement personality.",
        "- You do not moderate, enforce rules, or operate server tools.",
        "- If you do not know something, say records are incomplete rather than inventing details.",
    ])

def map_route_source_label(label: str) -> SourceClass:
    return _ROUTE_SOURCE_MAP.get((label or "").strip(), SourceClass.LEGACY_SOURCE_BLIND)


def map_system_source_label(source_system: str, label: str) -> SourceClass:
    """Map a label only inside its owning system's authority boundary."""

    system = str(source_system or "").strip().casefold()
    normalized_label = str(label or "").strip()
    if system == "legacy_canon_registry":
        return (
            SourceClass.APPROVED_CANON
            if normalized_label in {"approved_canon", "canon"}
            else SourceClass.LEGACY_SOURCE_BLIND
        )
    if system in {"website_relay", "website_public_projection"}:
        return SourceClass.EVIDENCE_PROJECTION
    if system in {"website_dossier", "website_source_file"}:
        return (
            SourceClass.DOSSIER_PROJECTION
            if system == "website_dossier"
            else SourceClass.SOURCE_FILE_PROJECTION
        )
    if system == "broadcast_memory":
        return SourceClass.FIRST_PARTY_RECORD
    if system == "public_assessment_observation":
        return SourceClass.PUBLIC_OBSERVATION
    return SourceClass.LEGACY_SOURCE_BLIND

def has_explicit_route_source_mapping(label: str) -> bool:
    return (label or "").strip() in _ROUTE_SOURCE_MAP

def map_channel_policy_visibility(policy: str) -> Visibility:
    return _CHANNEL_VISIBILITY_MAP.get((policy or "").strip(), Visibility.UNKNOWN)

def has_explicit_channel_policy_mapping(policy: str) -> bool:
    return (policy or "").strip() in _CHANNEL_VISIBILITY_MAP

def source_authority_rank(source_class: SourceClass) -> int:
    return _SOURCE_RANK.get(source_class, 0)

def _confidence_rank(confidence: Confidence) -> int:
    return _CONFIDENCE_RANK.get(confidence, 0)

def is_public_usable(claim: SourceClaim, target_visibility: Visibility = Visibility.PUBLIC_SAFE) -> bool:
    return bool(claim.valid and not claim.retracted and not claim.expired and claim.visibility in _PUBLIC_VIS and target_visibility in _PUBLIC_VIS)

def is_independent_evidence(claim: SourceClaim) -> bool:
    return claim.source_class not in _PROJECTION_CLASSES and not claim.projection and not claim.derived_from

def _claim_scope(claims: list[SourceClaim]) -> tuple[set[str], set[str]]:
    return {c.subject.key for c in claims}, {c.predicate for c in claims}

def _same_scope(a: SourceClaim, b: SourceClaim) -> bool:
    return a.subject.key == b.subject.key and a.predicate == b.predicate

def _can_correct(corrector: SourceClaim, target: SourceClaim, *, target_visibility: Visibility) -> bool:
    if not is_public_usable(corrector, target_visibility) or not _same_scope(corrector, target):
        return False
    if target.claim_id not in set(corrector.correction_of + corrector.supersedes):
        return False
    if source_authority_rank(corrector.source_class) < source_authority_rank(target.source_class):
        return False
    if not is_independent_evidence(corrector) and is_independent_evidence(target):
        return False
    return True

def _claim_sort_key(claim: SourceClaim) -> tuple:
    observed = claim.observed_at if claim.observed_at else datetime.min.replace(tzinfo=timezone.utc)
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return (source_authority_rank(claim.source_class), _confidence_rank(claim.confidence), observed, claim.claim_id)

def resolve_claims(claims: Iterable[SourceClaim], *, target_visibility: Visibility = Visibility.PUBLIC_SAFE) -> Resolution:
    eligible = [c for c in claims if is_public_usable(c, target_visibility)]
    if not eligible:
        return Resolution(False, None, "no_public_usable_claim")
    subjects, predicates = _claim_scope(eligible)
    if len(subjects) != 1 or len(predicates) != 1:
        return Resolution(False, None, "mixed_claim_scope")
    suppressed: set[str] = set()
    by_id = {c.claim_id: c for c in eligible}
    for corrector in eligible:
        for target_id in corrector.correction_of + corrector.supersedes:
            target = by_id.get(target_id)
            if target and _can_correct(corrector, target, target_visibility=target_visibility):
                suppressed.add(target.claim_id)
    remaining = [c for c in eligible if c.claim_id not in suppressed]
    if not remaining:
        return Resolution(False, None, "no_unsuppressed_claim")
    values = {repr(c.value) for c in remaining}
    remaining.sort(key=_claim_sort_key, reverse=True)
    top = remaining[0]
    top_key = _claim_sort_key(top)[:3]
    conflicting_top = [c for c in remaining if _claim_sort_key(c)[:3] == top_key and repr(c.value) != repr(top.value)]
    if conflicting_top:
        return Resolution(False, None, "unresolved_equal_authority_conflict")
    if len(values) == 1:
        return Resolution(True, top, "resolved_identical_values")
    return Resolution(True, top, "resolved_by_authority_confidence_and_correction")

def current_time_claim_resolution(claims: Iterable[SourceClaim], *, now: datetime | None = None, max_age_minutes: int = 30) -> Resolution:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    fresh = []
    for c in claims:
        if not is_public_usable(c):
            continue
        if not c.current_time_capable or c.source_class not in _RUNTIME_SOURCE_CLASSES:
            continue
        if not is_independent_evidence(c):
            continue
        if c.observed_at is None:
            continue
        observed = c.observed_at if c.observed_at.tzinfo else c.observed_at.replace(tzinfo=timezone.utc)
        age = now - observed
        if age < timedelta(seconds=0) or age > timedelta(minutes=max_age_minutes):
            continue
        fresh.append(c)
    if not fresh:
        return Resolution(False, None, "current_state_unknown_without_fresh_runtime_evidence", False)
    resolved = resolve_claims(fresh)
    return Resolution(resolved.usable, resolved.claim, resolved.reason, bool(resolved.usable))

def env_queue_production_enabled(environ: dict[str, str] | None = None) -> bool:
    env = environ if environ is not None else __import__("os").environ
    return str(env.get("BNL_QUEUE_PRODUCTION_ENABLED", "")).strip().lower() == "true"

def website_queue_production_capability(read_model: dict | None) -> bool | None:
    caps = (read_model or {}).get("capabilities") if isinstance(read_model, dict) else None
    return caps.get("queueProduction") if isinstance(caps, dict) and isinstance(caps.get("queueProduction"), bool) else None

def queue_usability(read_model: dict | None, *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    local = env_queue_production_enabled(environ)
    remote = website_queue_production_capability(read_model)
    usable = bool(local and remote is True)
    reason = "eligible" if usable else ("local_gate_disabled" if not local else "website_capability_missing_or_false")
    return {"usable": usable, "local": local, "website": remote, "reason": reason}

def _looks_queue_derived(value: Any) -> bool:
    if isinstance(value, dict):
        for k, v in value.items():
            key = str(k).lower()
            if key in {"source", "provenance", "kind", "lane", "authority", "sourceclass", "source_class"}:
                text = str(v).lower()
                if any(term in text for term in _QUEUE_PROVENANCE_TERMS):
                    return True
            if key in _QUEUE_KEY_LOWER:
                return True
            if _looks_queue_derived(v):
                return True
    elif isinstance(value, (list, tuple, set)):
        return any(_looks_queue_derived(v) for v in value)
    elif isinstance(value, str):
        text = value.lower()
        return any(term in text for term in ("queue_public_snapshot", "now playing", "up next", "priority signal", "wheel spins owed", "queue count", "payment", "queue-derived"))
    return False

def _sanitize_public_rule(value: Any) -> Any:
    if isinstance(value, str):
        low = value.lower()
        if any(term in low for term in ("queue_public_snapshot", "now playing", "up next", "priority signal", "wheel spins", "payment", "live queue", "queue count", "queue-derived artist")):
            return None
        return value
    if isinstance(value, dict):
        if _looks_queue_derived(value):
            return None
        cleaned = {}
        for k, raw in value.items():
            sanitized = _sanitize_public_rule(raw)
            if sanitized is not None:
                cleaned[k] = sanitized
        return cleaned
    return value

def _sanitize_operator_lanes(operator_lanes: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for lane, items in operator_lanes.items():
        if not isinstance(items, list):
            cleaned[lane] = []
            continue
        kept = []
        for item in items:
            if lane in _QUEUE_SENSITIVE_LANES:
                if _looks_queue_derived(item):
                    continue
                # Queue-sensitive lanes fail closed on ambiguous dict provenance.
                if isinstance(item, dict) and not any(k in item for k in ("source", "provenance", "authority", "bnlContext")):
                    continue
            elif lane == "doNotStore":
                sanitized = _sanitize_public_rule(item)
                if sanitized is None:
                    continue
                item = sanitized
            kept.append(item)
        cleaned[lane] = kept
    return cleaned

def _strip_queue_recursive(value: Any, *, in_sections: bool = False) -> Any:
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            key_text = str(key)
            key_lower = key_text.lower()
            if key_lower in _QUEUE_KEY_LOWER:
                continue
            if key_text == "operatorLanes" and isinstance(item, dict):
                result[key] = _sanitize_operator_lanes(item)
                continue
            if key_text in {"rules", "sourceAuthority", "sourceAuthorities"}:
                if isinstance(item, list):
                    result[key] = [v for v in (_sanitize_public_rule(x) for x in item) if v is not None]
                else:
                    sanitized = _sanitize_public_rule(item)
                    if sanitized is not None:
                        result[key] = sanitized
                continue
            result[key] = _strip_queue_recursive(item)
        return result
    if isinstance(value, list):
        return [_strip_queue_recursive(v) for v in value]
    return value

def strip_queue_sections(read_model: dict | None, *, environ: dict[str, str] | None = None) -> dict:
    if not isinstance(read_model, dict):
        return {}
    if queue_usability(read_model, environ=environ)["usable"]:
        return read_model
    return _strip_queue_recursive(read_model)

def diagnostics(read_model: dict | None = None, *, environ: dict[str, str] | None = None) -> dict[str, Any]:
    q = queue_usability(read_model, environ=environ)
    return {"contractVersion": CANON_SOURCE_CONTRACT_VERSION, "compatibilityAdaptersActive": True, "localQueueProductionCapability": q["local"], "websiteQueueProductionCapability": q["website"], "effectiveQueueUsable": q["usable"], "queueReason": q["reason"]}
