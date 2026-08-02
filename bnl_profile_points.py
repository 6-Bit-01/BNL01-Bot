"""Shared material-point identity for broad member profiles.

Packet sufficiency and candidate coverage must agree about whether two Open
Signal observations express the same material point.  This module is kept
pure so both owners can consume one deterministic contract without importing
each other.
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
    shared = len(left.intersection(right))
    smaller = min(len(left), len(right))
    union = len(left.union(right))
    return bool(
        shared >= 3
        and smaller
        and shared / smaller >= 0.8
        and union
        and shared / union >= 0.75
    )


def material_profile_point_map(items: Sequence[Any]) -> dict[str, str]:
    """Map raw point hashes to deterministic material-point identities.

    Only question-scoped Open Signal observations receive near-paraphrase
    collapse. Other evidence owners retain their explicit point identities.
    Complete-link paraphrase groups use the lexicographically smallest raw
    point identity, making the result independent of packet or validation
    ordering without transitively bridging distinct endpoints.
    """

    point_items: dict[str, tuple[str, str]] = {}
    for item in items:
        point_id = str(getattr(item, "point_identity", "") or "")
        if not point_id:
            continue
        candidate = (
            str(getattr(item, "lane", "") or ""),
            str(getattr(item, "text", "") or ""),
        )
        if point_id not in point_items or candidate < point_items[point_id]:
            point_items[point_id] = candidate

    observations = tuple(
        (point_id, _material_terms(text))
        for point_id, (lane, text) in sorted(point_items.items())
        if lane == "assessment_observation"
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
        point_id: point_id
        for point_id in sorted(point_items)
    }
    for cluster in clusters:
        canonical = cluster[0][0]
        for point_id, _member_terms in cluster:
            point_map[point_id] = canonical
    return point_map
