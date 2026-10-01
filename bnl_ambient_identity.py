"""Resolve Ambient credits from eligible source-owned Discord identities.

This is a formatting boundary, not an identity store or an alias resolver.
The existing source adapter owns public eligibility and the caller must
revalidate those sources and current guild membership immediately before send.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, Mapping, Sequence


_DISCORD_SUBJECT = re.compile(r"discord_user:([1-9][0-9]{0,19})\Z")
_USER_MENTION = re.compile(r"<@!?([0-9]+)>")
_ROLE_MENTION = re.compile(r"<@&[0-9]+>")
_BROAD_MENTION = re.compile(r"@(everyone|here)\b", re.IGNORECASE)


@dataclass(frozen=True)
class AmbientMention:
    subject_ref: str
    user_id: int
    label: str
    source_refs: tuple[str, ...]


@dataclass(frozen=True)
class AmbientMentionRender:
    content: str = ""
    user_ids: tuple[int, ...] = ()


def _public_label(value: Any) -> str:
    label = " ".join(str(value or "").split())[:120]
    # Public display names are data; even a verified member cannot turn their
    # display name into a notification for somebody else.
    label = _USER_MENTION.sub("", label)
    label = _ROLE_MENTION.sub("", label)
    label = _BROAD_MENTION.sub(lambda match: "@\u200b" + match.group(1), label)
    return label.strip()


def plan_ambient_mentions(
    items: Sequence[Mapping[str, Any]],
    featured_source_refs: Iterable[str],
    *,
    guild_id: int,
    featured_subject_refs: Iterable[str],
) -> dict[str, AmbientMention]:
    """Select exact authors explicitly featured from cited Discord sources.

    The two selections are intentional: citing an episode or a large exchange
    does not authorize notifying every participant. A model-supplied identifier
    must intersect an eligible same-guild item and its public author label.
    TikTok correlations and Source File alias labels cannot authorize pings.
    """
    if isinstance(guild_id, bool) or not isinstance(guild_id, int) or guild_id <= 0:
        return {}
    selected_sources = {str(ref) for ref in featured_source_refs}
    selected_subjects = {str(ref) for ref in featured_subject_refs}
    result: dict[str, AmbientMention] = {}
    for item in items:
        if not isinstance(item, Mapping):
            continue
        source_ref = str(item.get("ref") or "")
        if (
            source_ref not in selected_sources
            or item.get("guild_id") != guild_id
            or isinstance(item.get("guild_id"), bool)
            or item.get("kind") != "conversation"
            or item.get("conversation_surface") != "discord"
            or item.get("scope") != "window_activity"
        ):
            continue
        labels = item.get("subject_labels")
        subjects = item.get("subject_refs")
        if not isinstance(labels, Mapping) or not isinstance(subjects, (list, tuple)):
            continue
        for subject in subjects:
            if not isinstance(subject, str) or subject not in selected_subjects:
                continue
            match = _DISCORD_SUBJECT.fullmatch(subject)
            label = _public_label(labels.get(subject))
            if not match or not label:
                continue
            previous = result.get(subject)
            refs = tuple(dict.fromkeys((*previous.source_refs, source_ref))) if previous else (source_ref,)
            result[subject] = AmbientMention(subject, int(match.group(1)), label, refs)
    return result


def render_ambient_mentions(
    plan: Mapping[str, AmbientMention],
    *,
    current_member_ids: Iterable[int],
) -> AmbientMentionRender:
    """Render one ping per featured member after the caller confirms membership.

    The caller must use the returned IDs as Discord's explicit AllowedMentions
    user allowlist, with roles/everyone/replied_user disabled. Unknown members
    retain their ordinary name in the article and receive no notification.
    """
    members = {member for member in current_member_ids if isinstance(member, int) and not isinstance(member, bool)}
    user_ids = tuple(dict.fromkeys(
        mention.user_id for subject, mention in plan.items()
        if isinstance(mention, AmbientMention)
        and subject == mention.subject_ref == "discord_user:%s" % mention.user_id
        and mention.user_id in members
    ))
    if not user_ids:
        return AmbientMentionRender()
    return AmbientMentionRender("Featuring: " + " · ".join("<@%s>" % user_id for user_id in user_ids), user_ids)


def sanitize_ambient_mentions(
    text: str,
    plan: Mapping[str, AmbientMention] | None = None,
) -> str:
    """Keep generated prose free of executable Discord user/role/broad mentions."""
    labels = {mention.user_id: mention.label for mention in (plan or {}).values()
              if isinstance(mention, AmbientMention)}
    value = _USER_MENTION.sub(lambda match: labels.get(int(match.group(1)), "a community member"), str(text or ""))
    value = _ROLE_MENTION.sub("a community role", value)
    return _BROAD_MENTION.sub(lambda match: "@\u200b" + match.group(1), value)
