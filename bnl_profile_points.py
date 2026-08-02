"""Pure, shared material-point identity for broad member profiles.

Packet sufficiency and response coverage must agree about whether two public
observations express one material point.  This module owns no evidence and
performs only deterministic, order-independent grouping.
"""
from __future__ import annotations

import re
from typing import Any, Sequence


_TERM_RE = re.compile(r"[a-z0-9][a-z0-9'’-]*", re.I)
_TERM_STOPWORDS = frozenset(
    {
        "a",
        "about",
        "all",
        "am",
        "an",
        "and",
        "are",
        "at",
        "be",
        "do",
        "does",
        "for",
        "from",
        "have",
        "i",
        "in",
        "is",
        "it",
        "know",
        "me",
        "my",
        "of",
        "on",
        "or",
        "remember",
        "tell",
        "that",
        "the",
        "this",
        "to",
        "what",
        "who",
        "with",
        "you",
    }
)


def _profile_term_stem(value: str) -> str:
    term = str(value or "").lower()
    if len(term) > 5 and term.endswith("ies"):
        return term[:-3] + "y"
    for suffix in ("ing", "ed"):
        if len(term) > len(suffix) + 3 and term.endswith(suffix):
            stem = term[: -len(suffix)]
            if len(stem) > 3 and stem[-1:] == stem[-2:-1]:
                stem = stem[:-1]
            return stem
    if len(term) > 4 and term.endswith("s") and not term.endswith("ss"):
        return term[:-1]
    return term


def _material_terms(value: Any) -> frozenset[str]:
    return frozenset(
        _profile_term_stem(term)
        for term in _TERM_RE.findall(str(value or "").lower())
        if len(term) > 1 and term not in _TERM_STOPWORDS
    )


def _near_paraphrase(
    left: frozenset[str],
    right: frozenset[str],
) -> bool:
    if not left or not right:
        return False
    if left == right and len(left) >= 2:
        return True
    shared = len(left.intersection(right))
    smaller = min(len(left), len(right))
    union = len(left.union(right))
    return bool(
        shared >= 3
        and shared / smaller >= 0.8
        and shared / union >= 0.75
    )


def _point_identity(item: Any) -> str:
    return str(
        getattr(item, "point_group_identity", "")
        or getattr(item, "point_identity", "")
        or ""
    )


def material_profile_point_map(items: Sequence[Any]) -> dict[str, str]:
    """Map raw point IDs to complete-link material point identities.

    Only immediate public observations receive lexical paraphrase collapse.
    Complete-link clustering prevents a chain of weak paraphrases from merging
    materially distinct endpoints.  Sorting makes the result independent of
    retrieval or packet order.
    """

    point_items: dict[str, tuple[str, str]] = {}
    point_roots: dict[str, set[str]] = {}
    for item in items:
        point_id = _point_identity(item)
        if not point_id:
            continue
        candidate = (
            str(getattr(item, "lane", "") or ""),
            str(getattr(item, "text", "") or ""),
        )
        if point_id not in point_items or candidate < point_items[point_id]:
            point_items[point_id] = candidate
        point_roots.setdefault(point_id, set()).update(
            str(root or "")
            for root in tuple(getattr(item, "root_identities", ()) or ())
            if str(root or "")
        )

    # A durable item and its immediate Open/context projections are one
    # material point, even when the durable item is supported by several raw
    # roots.  Root equality is lineage equality, not semantic equality:
    # distinct durable points remain distinct even when the same compound
    # human observations support both.  An immediate projection is assigned
    # to exactly one deterministic carrier so a shared root cannot chain
    # unrelated motifs together.
    immediate_lanes = {"assessment_observation", "conversation_context"}
    durable_carriers = tuple(
        sorted(
            (
                (frozenset(point_roots.get(point_id, ())), point_id)
                for point_id, (lane, _text) in point_items.items()
                if lane not in immediate_lanes
                and point_roots.get(point_id)
            ),
            key=lambda carrier: (
                len(carrier[0]),
                tuple(sorted(carrier[0])),
                carrier[1],
            ),
        )
    )
    assigned_to_durable: dict[str, str] = {}
    for point_id, (lane, _text) in point_items.items():
        roots = frozenset(point_roots.get(point_id, ()))
        if not roots:
            continue
        if lane not in immediate_lanes:
            assigned_to_durable[point_id] = point_id
            continue
        if lane in immediate_lanes:
            carriers = tuple(
                (len(carrier_roots), canonical)
                for carrier_roots, canonical in durable_carriers
                if roots.issubset(carrier_roots)
            )
            if carriers:
                assigned_to_durable[point_id] = min(carriers)[1]

    observations = tuple(
        (point_id, _material_terms(text))
        for point_id, (lane, text) in sorted(point_items.items())
        if lane in {"assessment_observation", "conversation_context"}
        and point_id not in assigned_to_durable
    )
    clusters: list[list[tuple[str, frozenset[str]]]] = []
    for point_id, terms in observations:
        compatible = tuple(
            cluster
            for cluster in clusters
            if all(
                _near_paraphrase(terms, member_terms)
                for _member_id, member_terms in cluster
            )
        )
        if compatible:
            min(compatible, key=lambda cluster: cluster[0][0]).append(
                (point_id, terms)
            )
        else:
            clusters.append([(point_id, terms)])

    point_map = {
        point_id: assigned_to_durable.get(point_id, point_id)
        for point_id in sorted(point_items)
    }
    for cluster in clusters:
        canonical = min(point_id for point_id, _terms in cluster)
        for point_id, _terms in cluster:
            point_map[point_id] = canonical
    return point_map
