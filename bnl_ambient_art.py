"""Optional self-directed art on the existing Ambient delivery path.

No commands, member commissions, new scheduler, or Journal publication calls.
An atomic Pacific-day claim is consumed even by a failed/uncertain attempt.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import closing
from datetime import timezone
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import sqlite3
import urllib.request

from bnl_own_art import (IMAGE_EXTENSIONS, _NoRedirect, _private_write, build_own_art_creative_context,
                         generate_private_image, parse_own_art_concept, build_art_context,
                         render_art_sources, continuity_for_prompt, art_context_current,
                         saved_creative_continuity, develop_art_concept, bind_art_visual_references,
                         visual_reference_availability, visual_reference_snapshot_current)

PUBLIC_MAX_IMAGE_BYTES = 2 * 1024 * 1024


def enabled(bot, guild_id):
    return (os.getenv("BNL_OWN_ART_ENABLED", "").strip().lower() == "true"
            and guild_id == bot.BNL_PRIMARY_GUILD_ID and bool(guild_id))


def _ensure_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS bnl_own_art_delivery (
        pacific_day TEXT PRIMARY KEY, art_id TEXT UNIQUE NOT NULL,
        guild_id INTEGER NOT NULL, status TEXT NOT NULL,
        discord_message_id TEXT NOT NULL DEFAULT '',
        website_status TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL DEFAULT '{}'
    )""")


def _db(bot):
    conn = sqlite3.connect(bot.DB_FILE, timeout=0.5)
    try:
        _ensure_schema(conn)
        conn.commit()
        return conn
    except BaseException:
        conn.close()
        raise


def available(bot, guild_id):
    if not enabled(bot, guild_id):
        return False
    try:
        with closing(_db(bot)) as conn:
            return conn.execute("SELECT 1 FROM bnl_own_art_delivery WHERE pacific_day=?",
                            (bot._pacific_now().date().isoformat(),)).fetchone() is None
    except (sqlite3.Error, OSError):
        return False


def claim(bot, guild_id):
    if not enabled(bot, guild_id):
        return None
    day = bot._pacific_now().date().isoformat()
    art_id = "bnl-art-" + day
    with closing(_db(bot)) as conn, conn:
        inserted = conn.execute("INSERT OR IGNORE INTO bnl_own_art_delivery(pacific_day,art_id,guild_id,status) "
                                "VALUES(?,?,?,'claimed')", (day, art_id, guild_id)).rowcount
    return art_id if inserted else None


def record(bot, art_id, status, *, metadata=None, message_id=None, website_status=None):
    # Retry only this local, atomic receipt through the existing SQLite owner.
    # Generation, filesystem preparation and external delivery remain outside.
    values = (status, json.dumps(metadata) if metadata is not None else None,
              str(message_id) if message_id is not None else None, website_status, art_id)

    def write(conn):
        _ensure_schema(conn)
        conn.execute("UPDATE bnl_own_art_delivery SET status=?, "
                     "metadata_json=COALESCE(?,metadata_json), "
                     "discord_message_id=COALESCE(?,discord_message_id), "
                     "website_status=COALESCE(?,website_status) WHERE art_id=?", values)

    bot._persist_reply_transaction(write, operation="ambient_art_receipt")


def journal_context(bot, guild_id):
    # Same canonical publication reader and independent visibility/reuse controls.
    return bot._build_publication_prompt_source_basis(
        guild_id=guild_id, user_text="latest journal", source_kind="journal")


def moment_context(bot, guild_id, topic_text, *, source_basis):
    """Reuse the public Moment selector and retain its exact original lineage."""
    from bnl_moment_engine import public_moment_source_basis, select_public_situation_moment_gists

    now = bot._pacific_now().astimezone(timezone.utc).isoformat()
    records = []
    with closing(sqlite3.connect(Path(bot.DB_FILE).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)) as conn:
        conn.execute("BEGIN")
        selected = select_public_situation_moment_gists(
            conn, guild_id=guild_id, topic_text=topic_text, token_budget=400, max_results=2,
            allowed_channel_policies=("public_home", "public_context"),
            require_topic_overlap=True, apply_date_scope=False, prepare_schema=False,
            observed_before=now, now=now,
        )
        for item in selected:
            basis = public_moment_source_basis(conn, guild_id=guild_id, moment_id=item.moment_id)
            if basis is None:
                continue
            source_basis.setdefault("moments", {})[item.moment_id] = basis["sourceVersion"]
            records.append({"ref": "moment:" + item.moment_id, "kind": "public_moment",
                            "observedAt": item.last_activity_at, "summary": item.summary})
    return records


def parse_response(raw, *, allowed_refs=(), journals=()):
    """Legacy plain text remains valid; malformed structured output never leaks."""
    raw = str(raw or "").strip()
    if raw.startswith("```"):
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    if not raw.startswith("{"):
        return raw, None, False
    try:
        value = json.loads(raw)
        if value.get("action") == "skip":
            return "", None, True
        if value.get("action") != "post" or not isinstance(value.get("text"), str):
            return "", None, False
        art = None
        if isinstance(value.get("art"), dict):
            try:
                art = parse_own_art_concept(json.dumps(value["art"]), set(allowed_refs))
                if art["action"] != "create":
                    art = None
                elif value["art"].get("journalEntryId"):
                    journal = next((p for p in journals if p.entry_id == value["art"]["journalEntryId"]), None)
                    if journal is None:
                        art = None
                    else:
                        art["journal"] = {"entryId": journal.entry_id, "revision": journal.revision,
                                          "contentHash": journal.content_hash}
            except (ValueError, TypeError):
                art = None  # A bad optional art proposal cannot discard valid prose.
        return value["text"], art, False
    except (ValueError, TypeError, AttributeError):
        return "", None, False


async def prepare(bot, guild_id, basis):
    concept = basis.get("art")
    if not concept or not enabled(bot, guild_id):
        return None
    art_id = None
    stage = "claim"
    try:
        art_id = await asyncio.to_thread(claim, bot, guild_id)
        if not art_id:
            return None
        if basis.get("art_context"):
            stage = "concept_development"
            concept = await asyncio.to_thread(develop_art_concept, bot, guild_id, concept,
                                              basis["art_context"], ambient_text=basis.get("art_caption", ""))
            if concept["action"] == "skip":
                stage = "declined_receipt"
                await asyncio.to_thread(record, bot, art_id, "bnl_declined_after_development")
                return None
            basis["art"] = concept
        context = basis.get("art_context") or {"sourceBases": []}
        reference_inputs, reference_snapshot = await asyncio.to_thread(
            bind_art_visual_references, bot, guild_id, concept, context)
        if reference_snapshot:
            basis["art_visual_references"] = reference_snapshot
        continuity = (saved_creative_continuity(guild_id, concept, context, ambient_basis=basis)
                      if basis.get("art_context") else None)
        stage = "source_check_before_image"
        if not await bot.revalidate_ambient_sources(guild_id, basis, stage="before_image"):
            raise ValueError("art_sources_changed")
        stage = "image_generation"
        reference_kwargs = ({"reference_inputs": reference_inputs, "reference_snapshot": reference_snapshot}
                            if reference_snapshot else {})
        image, receipt = await asyncio.to_thread(generate_private_image, bot, concept["imagePrompt"],
                                                **reference_kwargs)
        if len(image) > PUBLIC_MAX_IMAGE_BYTES:
            raise ValueError("art_public_image_too_large")
        stage = "source_check_after_image"
        if not await bot.revalidate_ambient_sources(guild_id, basis, stage="after_image"):
            raise ValueError("art_sources_changed")
        now = bot._pacific_now().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        metadata = {"artId": art_id, "title": concept["title"], "meaning": concept["meaning"],
                    "createdAt": now, "sha256": receipt["sha256"], "mimeType": receipt["mimeType"], "journal": concept.get("journal"),
                    "sourceJournals": [{"entryId": p.entry_id, "revision": p.revision, "contentHash": p.content_hash}
                                       for p in getattr(basis.get("art_journal_basis"), "publications", ())]}
        folder = Path(bot.DB_FILE).resolve().parent / "bnl-own-art" / art_id
        stage = "private_receipt"
        folder.mkdir(mode=0o700, parents=True, exist_ok=False)
        _private_write(folder / ("image" + IMAGE_EXTENSIONS[receipt["mimeType"]]), image)
        private_metadata = dict(metadata)
        if reference_snapshot:
            private_metadata["visualReferenceSnapshot"] = reference_snapshot
        if continuity:
            private_metadata["privateCreativeContinuity"] = continuity
        # Keep the original private lineage beside the already-rendered image
        # even if database persistence exhausts its bounded contention retries.
        _private_write(folder / "receipt.json", json.dumps({"metadata": private_metadata, "image": receipt}).encode())
        stage = "draft_receipt"
        await asyncio.to_thread(record, bot, art_id, "draft_ready", metadata=private_metadata)
        return {"image": image, "metadata": metadata}
    except Exception as exc:
        if art_id:
            try:
                await asyncio.to_thread(record, bot, art_id, "generation_failed_or_withdrawn")
            except (sqlite3.Error, OSError):
                pass  # The durable claim already closes this day to another image.
        category = ("sqlite_busy" if bot._sqlite_busy(exc) else
                    "sqlite_failure" if isinstance(exc, sqlite3.Error) else "operation_failed")
        logging.warning("ambient_art_unavailable stage=%s error_type=%s category=%s sqlite_code=%s sqlite_name=%s",
                        stage, type(exc).__name__, category, getattr(exc, "sqlite_errorcode", None),
                        getattr(exc, "sqlite_errorname", None))
        return None


def discord_file(bot, art):
    extension = IMAGE_EXTENSIONS[art["metadata"].get("mimeType", "image/png")]
    return bot.discord.File(io.BytesIO(art["image"]), filename=art["metadata"]["artId"] + extension,
                            description=art["metadata"]["meaning"][:1000])


def publish_website(bot, art):
    """A separate receipt; a confirmed Discord send is not website delivery."""
    art_id = art["metadata"]["artId"]
    status = "unconfirmed"
    try:
        base = bot._journal_website_base_url()
        if not base.startswith("https://") or not bot.BNL_API_KEY:
            status = "configuration_missing"
            return
        payload = {"contractVersion": 2, "kind": "bnl_own_art", "art": art["metadata"],
                   "imageBase64": base64.b64encode(art["image"]).decode("ascii")}
        request = urllib.request.Request(base + "/api/bnl/art", data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "x-api-key": bot.BNL_API_KEY}, method="POST")
        with urllib.request.build_opener(_NoRedirect).open(request, timeout=20) as response:
            result = json.loads(response.read(8192))
        if result.get("ok") is True and result.get("artId") == art_id and result.get("sha256") == hashlib.sha256(art["image"]).hexdigest():
            status = "confirmed"
    except Exception:
        pass
    finally:
        record(bot, art_id, "discord_confirmed", website_status=status)
        logging.info("ambient_art_website art_id=%s outcome=%s", art_id, status)
