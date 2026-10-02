"""Read-only community-edition projection of the existing governed owners.

No new source archive, identity authority, generation call or publication path.
Only ``items`` and the window/coverage fields are model-facing. Private source
bases stay in memory until the existing Ambient send fence has revalidated them.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from contextlib import closing
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from urllib.parse import quote, urlsplit

from bnl_journal import build_source_packet_between, _eligible_reflection_basis, _evenly_sample
from bnl_journal_source_store import query_source_events, timestamp_to_epoch_ms
from bnl_tiktok_live_context import tiktok_show_evidence_key, tiktok_show_records


MAX_ACTIVITY_ITEMS = 48
MAX_REFLECTION_ITEMS = 8
MAX_ITEMS = 64


def evidence_role(item):
    """Classify owner-projected source kinds, never a model's authority claim.

    A contribution establishes what someone expressed, not independent proof
    of every claim in it. A BNL publication establishes what BNL published.
    Neither storage nor a current-window label upgrades derived narration.
    """
    kind = str(item.get("kind") or "")
    if kind in {"journal", "published_journal", "relay", "published_relay",
                "website_relay", "published_ballad", "accepted_relay_continuity"}:
        return "bnl_expression"
    if kind in {"conversation", "discord_message", "tiktok_live_chat"}:
        return "original_contribution"
    if kind == "public_source_history" and str(item.get("source_type") or "") in {"discord_message", "tiktok_live_chat"}:
        return "original_contribution"
    if kind in {"finalized_show", "tiktok_live_engagement"}:
        return "recorded_event"
    if kind in {"approved_canon", "established_broadcast_memory"}:
        return "established_context"
    return "governed_interpretation"


def _with_evidence_roles(items):
    for item in items:
        item["evidence_role"] = evidence_role(item)
    return items


def _utc(value):
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    # The existing Discord/source archive's offset-free timestamps are UTC.
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _iso(value):
    return value.isoformat().replace("+00:00", "Z")


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _origin(bot):
    value = str(bot._journal_website_base_url() or "").rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path:
        return ""
    return value


def _owner_url(bot, value):
    """Retain only owner-returned links on the configured public site."""
    value = str(value or "")
    origin = _origin(bot)
    parsed = urlsplit(value)
    return value if origin and value.startswith(origin + "/") and not parsed.username and not parsed.password else ""


def _subjects(source, original):
    # Only an original Discord author receives a tag candidate. Names in prose,
    # quoted recipients, TikTok handles and inferred aliases never become IDs.
    subject = str(original.get("subjectRef") or "")
    label = str(source.get("publicSpeakerName") or "").strip()
    if (label and source.get("conversationSurface") == "discord"
            and re.fullmatch(r"discord_user:[1-9]\d{0,19}", subject)):
        return [subject], {subject: label}
    return [], {}


def _episode_links(bot, packet, guild_id):
    """Link exact show IDs only while the existing public website owner allows them."""
    roots = {str(p.get("sourceId")): str(p.get("refId"))
             for p in packet.get("privateSharedSourceProvenance", [])
             if isinstance(p, dict) and p.get("sourceKind") == "finalized_show"}
    origin = _origin(bot)
    if not roots or not origin or guild_id != getattr(bot, "BNL_PRIMARY_GUILD_ID", None):
        return {}
    try:
        archive = bot.public_show_evidence_archive(bot.fetch_bnl_read_model(force=True))
        result = {}
        for show in tiktok_show_records(archive):
            ref = roots.get(tiktok_show_evidence_key(show))
            session_id = str(show.get("sessionId") or "")
            if ref and session_id:
                # Existing website broadcastArchiveShowHref, joined only to
                # an exact, currently public, owner-supplied session ID.
                result[ref] = origin + "/radio/archive?view=shows&show=" + quote(session_id, safe="")
        return result
    except (OSError, ValueError, TypeError, AttributeError):
        return {}


def _interleave(groups, limit):
    """Share a fixed source budget among groups that actually have material."""
    remaining = [iter(group) for group in groups if group]
    chosen = []
    while remaining and len(chosen) < limit:
        active = []
        for group in remaining:
            item = next(group, None)
            if item is None:
                continue
            chosen.append(item)
            active.append(group)
            if len(chosen) == limit:
                break
        remaining = active
    return chosen


def _fair_activity(items):
    """Keep the owner's chronology, with room for quiet speakers and source kinds."""
    if len(items) <= MAX_ACTIVITY_ITEMS:
        return items
    buckets = {}
    for item in sorted(items, key=lambda s: (s["occurred_at"], s["ref"])):
        # Equal access to the bounded projection; prominence is editorial, not
        # a reward for posting the most messages or being a familiar member.
        speaker = tuple(item["subject_refs"]) or item.get("participant_alias") or item["label"]
        buckets.setdefault((item["kind"], str(speaker)), []).append(item)
    # Spread each speaker's slots through their chronology instead of taking
    # only their newest/oldest cluster.
    kinds = {}
    for key, rows in buckets.items():
        order = [0, len(rows) - 1]
        order.extend(range(1, len(rows) - 1))
        kinds.setdefault(key[0], []).append([rows[i] for i in dict.fromkeys(order)])
    # A flattened (kind, speaker) queue let many early speakers consume every
    # slot before a later completed show. Share by kind first, then by author.
    # Missing kinds reserve nothing; these are input candidates, not required
    # article sections or a demand to feature particular people.
    slots = {kind: 0 for kind in kinds}
    for kind in _interleave([[kind] * sum(map(len, authors)) for kind, authors in kinds.items()], MAX_ACTIVITY_ITEMS):
        slots[kind] += 1
    chosen = []
    for kind, authors in kinds.items():
        # More distinct authors than slots must not always discard the later
        # part of the day. Reuse the owner's chronological sampling primitive.
        candidates = _evenly_sample(authors, slots[kind])
        chosen.extend(_interleave(candidates, slots[kind]))
    return sorted(chosen, key=lambda s: (s["occurred_at"], s["ref"]))


def _fair_reflections(packet, start, end):
    eligible = []
    for source in _eligible_reflection_basis(packet):
        observed = _utc(source.get("sourceObservedAt"))
        timeless = source.get("basisKind") == "approved_canon" and not source.get("sourceObservedAt")
        if timeless or (observed is not None and observed < end):
            eligible.append(source)
    eligible.sort(key=lambda s: (
        s.get("basisKind") == "published_ballad" and start <= (_utc(s.get("sourceObservedAt")) or datetime.min.replace(tzinfo=timezone.utc)) < end,
        _utc(s.get("sourceObservedAt")) or datetime.min.replace(tzinfo=timezone.utc),
        str(s.get("refId") or "")), reverse=True)
    kinds = {}
    for source in eligible:
        kinds.setdefault(source["basisKind"], []).append(source)
    return _interleave(list(kinds.values()), MAX_REFLECTION_ITEMS)


def _packet_items(bot, packet, start, end, guild_id):
    originals = {str(s.get("refId")): s for s in packet.get("privateSources", []) if isinstance(s, dict)}
    original_context = packet.get("_ambient_original_context", {})
    episode_links = _episode_links(bot, packet, guild_id)
    activity = []
    for source in packet.get("safeSources", []):
        if not isinstance(source, dict) or not source.get("refId") or not str(source.get("summary") or "").strip():
            continue
        observed = _utc(source.get("observedAt"))
        if observed is None or not start <= observed < end:
            continue
        subjects, labels = _subjects(source, originals.get(str(source["refId"]), {}))
        kind = str(source.get("sourceKind") or "community_activity")
        item = {
            "ref": str(source["refId"]), "kind": kind,
            "text": str(source["summary"])[:4000 if kind in {"finalized_show", "tiktok_live_engagement"} else 1000],
            "label": str(source.get("publicSpeakerName") or source.get("conversationSurface") or kind),
            "url": episode_links.get(str(source["refId"]), ""), "occurred_at": _iso(observed), "published_at": "",
            "subject_refs": subjects, "subject_labels": labels, "scope": "window_activity",
            "participant_alias": str(source.get("participantAlias") or ""),
            "conversation_surface": str(source.get("conversationSurface") or ""),
        }
        # Only the current original-owner fence supplies room identity. A
        # shared room and nearby time are context, not an inferred reply edge.
        room_ref = original_context.get(str(source["refId"]), {}).get("room_ref")
        if room_ref:
            item["room_ref"] = room_ref
        activity.append(item)
    items = _fair_activity(activity)
    for source in _fair_reflections(packet, start, end):
        observed = _utc(source.get("sourceObservedAt"))
        kind = str(source["basisKind"])
        published = kind == "published_ballad"
        item = {
            "ref": str(source["refId"]), "kind": kind,
            "source_type": str(source.get("sourceType") or ""),
            "text": str(source["summary"])[:6000 if published else 1200],
            "label": "Broadcast Ballad" if published else kind.replace("_", " "),
            "url": _owner_url(bot, source.get("showLink")) if published else "",
            "occurred_at": "" if published or observed is None else _iso(observed),
            "published_at": _iso(observed) if published else "",
            "subject_refs": [], "subject_labels": {},
            "scope": "window_publication" if published and observed >= start else "historical_context",
        }
        room_ref = original_context.get(str(source["refId"]), {}).get("room_ref")
        if room_ref:
            item["room_ref"] = room_ref
        if published:
            card = _ballad_publication_card(source.get("publication_card"))
            if card:
                item["publication_card"] = card
                if card.get("title"):
                    item["label"] = card["title"]
        # These are governed public contributions, not private participant keys.
        if kind == "public_moment":
            item["contributions"] = [
                {"speaker": str(c.get("publicSpeakerName") or ""), "summary": str(c.get("summary") or "")[:600]}
                for c in source.get("contributions", [])[:3] if isinstance(c, dict)
            ]
        items.append(item)
    return _with_evidence_roles(items)


def _root_digests(packet, refs):
    """Pin exact owner roots as well as prose; derived text is not its own proof."""
    originals = {str(s.get("refId")): s for s in packet.get("privateSources", []) if isinstance(s, dict)}
    shared = {str(s.get("refId")): s for s in packet.get("privateSharedSourceProvenance", []) if isinstance(s, dict)}
    reflection = {str(s.get("refId")): s for s in _eligible_reflection_basis(packet)}
    return {ref: _digest({"original": originals.get(ref), "shared": shared.get(ref),
                          "reflection": reflection.get(ref)}) for ref in refs}


def _fence_discord_originals(bot, guild_id, packet, start, end):
    """Captured archive visibility cannot override an original's current controls."""
    packet = {**packet, "_ambient_original_context": {}}
    originals = [source for source in packet.get("privateSources", [])
                 if source.get("sourceKind") == "conversation" and (
                     source.get("conversationSurface") == "discord"
                     or (not packet.get("sourceArchiveAvailable")
                         and str(source.get("subjectRef") or "").startswith("discord_user:")))]
    history_provenance = (packet.get("privateReflectionBasisProvenance") or {}).get("historicalSourceEvents", [])
    history_by_ref = {str(source.get("refId")): source for source in history_provenance if isinstance(source, dict)}
    historical = []
    for source in _eligible_reflection_basis(packet):
        if source.get("basisKind") != "public_source_history" or source.get("sourceType") != "discord_message":
            continue
        provenance = history_by_ref.get(str(source["refId"]), {})
        historical.append({"refId": str(source["refId"]), "subjectRef": provenance.get("subjectRef", ""),
                           "_historical": True, "_occurred_ms": provenance.get("occurredAtMs")})
    originals.extend(historical)
    saved = {"guild_id": guild_id}
    if not originals:
        return packet, saved
    events = {}
    if packet.get("sourceArchiveAvailable"):
        result = query_source_events(bot.DB_FILE, guild_id, timestamp_to_epoch_ms(start), timestamp_to_epoch_ms(end),
                                     prepare_schema=False)
        events = {"fresh:" + str(event["event_seq"]): event for event in result.events
                  if event.get("source_kind") == "discord_message"}
    # Historical reflection has explicit archived-event provenance. Resolve
    # only its bounded exact timestamps instead of scanning a month of chat.
    for occurred_ms in {item["_occurred_ms"] for item in historical if isinstance(item["_occurred_ms"], int)}:
        result = query_source_events(bot.DB_FILE, guild_id, occurred_ms, occurred_ms + 1, prepare_schema=False)
        events.update({"reflection:event:" + str(event["event_seq"]): event for event in result.events
                       if event.get("source_kind") == "discord_message"})
    bindings = {}
    for source in originals:
        ref = str(source.get("refId") or "")
        event = events.get(ref, {})
        metadata = event.get("metadata") or {}
        row_id = metadata.get("conversationRowId") or metadata.get("legacyRowId")
        if not row_id and not packet.get("sourceArchiveAvailable") and not source.get("_historical"):
            # The legacy Journal reader's messageId is explicitly the row ID.
            row_id = source.get("messageId")
        if not row_id:
            match = re.fullmatch(r"legacy_row:([1-9]\d*)", str(event.get("source_key") or ""))
            row_id = match.group(1) if match else None
        if str(row_id or "").isdigit() and int(row_id) > 0:
            bindings[ref] = (int(row_id), source, event)
    with closing(sqlite3.connect(Path(bot.DB_FILE).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)) as conn:
        current = {row["id"]: row for row in bot._ambient_source_rows(
            conn, "conversations", guild_id, row_ids={value[0] for value in bindings.values()})}
    eligible, rows = set(), []
    for ref, (row_id, source, event) in bindings.items():
        row = current.get(row_id)
        if row is None or str(source.get("subjectRef")) != "discord_user:" + str(row["user_id"]):
            continue
        # The archive may outlive an edited original. A public original is not
        # permission to repeat its previous text from an unsynchronised copy.
        if event and str(event.get("raw_text") or "") != str(row.get("content") or ""):
            continue
        eligible.add(ref)
        rows.append(row)
        channel_id = row.get("channel_id")
        if str(channel_id or "").isdigit() and int(channel_id) > 0:
            packet["_ambient_original_context"][ref] = {
                "room_ref": "discord-room:" + _digest([guild_id, int(channel_id)])[:24],
            }
    bot._remember_ambient_sources(saved, "conversations", rows)
    rejected = {str(source.get("refId")) for source in originals} - eligible
    if not rejected:
        return packet, saved
    packet = dict(packet)
    for key in ("safeSources", "privateSources", "reflectionBasis"):
        packet[key] = [source for source in packet.get(key, []) if str(source.get("refId")) not in rejected]
    if history_provenance:
        packet["privateReflectionBasisProvenance"] = {**packet["privateReflectionBasisProvenance"],
            "historicalSourceEvents": [source for source in history_provenance if str(source.get("refId")) not in rejected]}
    rejected_fresh = len(rejected - {item["refId"] for item in historical})
    packet["aggregateCounts"] = {**packet.get("aggregateCounts", {}),
        "eligibleConversations": max(0, int(packet.get("aggregateCounts", {}).get("eligibleConversations", 0)) - rejected_fresh)}
    return packet, saved


def _card_text(value, limit):
    return value.strip()[:limit] if isinstance(value, str) else ""


def _ballad_publication_card(value):
    """Copy structured fields already sanitized by the publication owner."""
    if not isinstance(value, dict):
        return {}
    from bnl_broadcast_ballads import PUBLICATION_CARD_LIMITS
    return {key: text for key, limit in PUBLICATION_CARD_LIMITS.items()
            if (text := _card_text(value.get(key), limit))}


def _journal_publication_card(publication):
    try:
        sections = json.loads(publication.sections_json)
    except (ValueError, TypeError, AttributeError):
        sections = []
    if not isinstance(sections, list):
        sections = []
    return {
        "title": _card_text(getattr(publication, "title", ""), 240),
        "excerpt": _card_text(getattr(publication, "excerpt", ""), 800),
        "section_headings": [heading for section in sections[:8] if isinstance(section, dict)
                             and (heading := _card_text(section.get("heading"), 140))],
    }


def _publication_items(bot, guild_id, start, end):
    items, bases = [], []
    for kind in ("journal", "relay"):
        source_basis = bot._build_publication_prompt_source_basis(
            guild_id=guild_id, user_text=kind, source_kind=kind)
        if source_basis is None:
            continue
        selected = []
        for publication in source_basis.publications:
            published = _utc(getattr(publication, "published_at", "") or getattr(publication, "published_timestamp", ""))
            if published is None or not start <= published < end:
                continue
            if kind == "journal":
                from bnl_journal import render_journal_publication
                ref = "journal:" + publication.entry_id
                label = publication.title
                text = render_journal_publication(publication, limit=3000)
                # This is the website owner's actual journalEntryHref route.
                origin = _origin(bot)
                url = origin + "/journal/" + quote(publication.entry_id, safe="") if origin else ""
                occurred = ""  # Publication of historical reporting is not a new event.
            else:
                ref = "relay:" + publication.relay_id
                label = "BNL Relay"
                text = publication.public_message
                url, occurred = "", ""
            item = {"ref": ref, "kind": "published_" + kind, "text": text,
                    "label": label, "url": url, "occurred_at": occurred,
                    "published_at": _iso(published), "subject_refs": [], "subject_labels": {},
                    "scope": "window_publication"}
            if kind == "journal":
                item["reported_window_start"] = publication.source_window_start
                item["reported_window_end"] = publication.source_window_end
                item["publication_card"] = _journal_publication_card(publication)
            items.append(item)
            selected.append(ref)
        if selected:
            bases.append(source_basis)
    return _with_evidence_roles(items), bases


def build_context(bot, guild_id, channel_id, *, basis, now=None):
    """Gather a fixed rolling day through existing public-source owners."""
    end = _utc(now if now is not None else bot._pacific_now())
    if end is None or int(basis.get("guild_id", guild_id)) != int(guild_id):
        raise ValueError("ambient_edition_scope_invalid")
    start = end - timedelta(hours=24)
    packet = build_source_packet_between(bot.DB_FILE, guild_id, _iso(start), _iso(end),
                                         entry_kind="daily", prepare_schema=False)
    packet, discord_basis = _fence_discord_originals(bot, guild_id, packet, _iso(start), _iso(end))
    for table, rows in discord_basis.get("rows", {}).items():
        bot._merge_ambient_source_hashes(basis, table, rows)
    packet_items = _packet_items(bot, packet, start, end, guild_id)
    publications, publication_bases = _publication_items(bot, guild_id, start, end)
    items = (publications + packet_items)[:MAX_ITEMS]
    for item in items:
        item["guild_id"] = guild_id
    selected = {item["ref"] for item in items}
    context = {
        "guild_id": guild_id, "channel_id": channel_id,
        "window_start": _iso(start), "window_end": _iso(end), "items": items,
        "coverage": {"sampled_items": len(items),
                     "source_archive_available": packet.get("sourceArchiveAvailable") is True,
                     "eligible_conversations": int(packet.get("aggregateCounts", {}).get("eligibleConversations", 0)),
                     "coverage_complete": packet.get("coverageComplete") is True},
        "_packet_digests": {item["ref"]: _digest(item) for item in packet_items if item["ref"] in selected},
        "_root_digests": _root_digests(packet, {item["ref"] for item in packet_items if item["ref"] in selected}),
        "_publication_bases": publication_bases,
        "_items_digest": _digest(items),
        "_packet": packet,
        "_discord_basis": discord_basis,
    }
    basis["edition_context"] = context
    return context


def revalidate(bot, guild_id, context):
    """Re-project original roots and refresh publication visibility before send."""
    try:
        if context.get("guild_id") != guild_id or context.get("_items_digest") != _digest(context["items"]):
            return False
        start, end = _utc(context["window_start"]), _utc(context["window_end"])
        if start is None or end is None or end - start != timedelta(hours=24):
            return False
        if not bot.revalidate_ambient_local_sources(guild_id, context["_discord_basis"]):
            return False
        packet = build_source_packet_between(bot.DB_FILE, guild_id, context["window_start"], context["window_end"],
                                             entry_kind="daily", prepare_schema=False)
        packet, _discord_basis = _fence_discord_originals(bot, guild_id, packet, context["window_start"], context["window_end"])
        current = {item["ref"]: _digest({**item, "guild_id": guild_id}) for item in _packet_items(bot, packet, start, end, guild_id)}
        if any(current.get(ref) != digest for ref, digest in context["_packet_digests"].items()):
            return False
        if _root_digests(packet, context["_packet_digests"]) != context["_root_digests"]:
            return False
        for publication_basis in context["_publication_bases"]:
            refreshed, changed = bot._refresh_publication_prompt_source_basis(publication_basis)
            if changed or not refreshed.publications:
                return False
        return True
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError, AttributeError):
        return False
