"""Content-free, read-only Journal/Relay/shared-brain production evidence.

No schema initialization, provider calls, website requests, or database writes.
Run from the repository with: python -m scripts.journal_relay_health --help
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
import time
from zoneinfo import ZoneInfo


def inspect(db_path: str, guild_id: int, *, now: datetime = None) -> dict:
    now = now or datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=24)).isoformat()
    result = {"observedAt": now.isoformat(), "scope": "local evidence; live delivery and website controls require runtime confirmation"}
    # mode=ro rejects a missing file instead of quietly creating an empty DB.
    with sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=3) as conn:
        conn.execute("PRAGMA query_only=ON")
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        deadline = time.monotonic() + 15
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)

        def rows(table, query, params=()):
            if table not in tables:
                return None
            try:
                return conn.execute(query, params).fetchall()
            except sqlite3.OperationalError:
                return None

        def snapshot(key, table, time_column, group_column=None, *, millis=False):
            if table not in tables:
                result[key] = {"available": False}
                return
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            if not {"guild_id", time_column} <= columns:
                result[key] = {"available": False, "reason": "schema_not_supported"}
                return
            time_expr = f"datetime({time_column}/1000.0,'unixepoch')" if millis else f"datetime({time_column})"
            data = rows(table, f"SELECT COUNT(*),MAX({time_expr}) FROM {table} WHERE guild_id=?", (guild_id,))
            recent = rows(table, f"SELECT COUNT(*) FROM {table} WHERE guild_id=? AND {time_expr}>=datetime(?)", (guild_id, cutoff))
            record = {"available": data is not None and recent is not None}
            if data is not None and recent is not None:
                record.update(total=data[0][0], latest=data[0][1], last24h=recent[0][0])
            if group_column and group_column in columns:
                groups = rows(table, f"SELECT {group_column},COUNT(*) FROM {table} WHERE guild_id=? AND {time_expr}>=datetime(?) GROUP BY {group_column}", (guild_id, cutoff))
                record["last24hByState"] = {str(k or "unknown"): v for k, v in groups or []}
            result[key] = record

        snapshot("journalSources", "bnl_journal_source_events", "occurred_at_ms", "source_kind", millis=True)
        snapshot("journals", "bnl_journal_entries", "created_at", "lifecycle_state")
        snapshot("journalRuns", "bnl_journal_automation_runs", "updated_at", "lifecycle_state")
        snapshot("journalAttempts", "bnl_journal_preparation_attempts", "started_at", "result_status")
        snapshot("relayPublications", "website_relay_history", "published_timestamp", "event_type")
        snapshot("relayAttempts", "website_relay_attempts", "started_at", "outcome")
        snapshot("relayPending", "website_relay_pending_v2", "prepared_at")
        shared_inputs = {}
        for label, table, stamp in (("pending", "website_relay_pending_v2", "prepared_at"),
                                    ("accepted", "website_relay_history", "published_timestamp")):
            selected = rows(table, f"SELECT source_basis_json FROM {table} WHERE guild_id=? ORDER BY {stamp} DESC LIMIT 10", (guild_id,))
            kinds, unreadable = Counter(), 0
            for (raw,) in selected or []:
                try:
                    basis = json.loads(raw)
                    if not isinstance(basis, list):
                        raise ValueError("invalid_basis")
                    kinds.update(item["sourceKind"] for item in basis if isinstance(item, dict)
                                 and item.get("sourceKind") in {"public_moment", "finalized_show", "published_journal"})
                except (TypeError, ValueError):
                    unreadable += 1
            shared_inputs[label] = {"available": selected is not None, "rowsScanned": len(selected or []),
                                    "sourceCounts": dict(kinds), "unreadableBases": unreadable}
        result["relaySharedInputs"] = shared_inputs
        holds = rows("website_relay_attempts", """SELECT reason,COUNT(*) FROM website_relay_attempts
            WHERE guild_id=? AND datetime(started_at)>=datetime(?)
              AND reason IN ('relay_source_changed','relay_source_basis_invalid','relay_source_unavailable')
            GROUP BY reason""", (guild_id, cutoff))
        result["relaySourceHolds24h"] = dict(holds or [])
        snapshot("moments", "memory_moment_windows", "last_activity_at", "lifecycle_status")
        snapshot("showEpisodes", "tiktok_show_evidence_ledgers", "ended_at_ms", "lifecycle_status", millis=True)
        snapshot("memoryLedger", "memory_ledger_entries", "created_at", "lifecycle_status")

        rejected = rows("memory_moment_windows", """SELECT qualification_reason,COUNT(*)
            FROM memory_moment_windows WHERE guild_id=? AND datetime(last_activity_at)>=datetime(?)
              AND lifecycle_status='rejected' GROUP BY qualification_reason""", (guild_id, cutoff))
        result["momentRejectionReasons24h"] = {
            "available": rejected is not None,
            "counts": {str(reason or "unknown"): count for reason, count in rejected or []},
        }
        profiles = rows("memory_moment_windows", """SELECT qualification_reason,
                human_entry_count,model_entry_count,participant_count,COUNT(*)
            FROM memory_moment_windows WHERE guild_id=? AND datetime(last_activity_at)>=datetime(?)
              AND lifecycle_status='rejected'
            GROUP BY qualification_reason,human_entry_count,model_entry_count,participant_count
            ORDER BY COUNT(*) DESC,qualification_reason,human_entry_count,model_entry_count,participant_count
            LIMIT 50""", (guild_id, cutoff))
        result["momentRejectionProfiles24h"] = {
            "available": profiles is not None,
            "profileLimit": 50,
            "profiles": [dict(reason=str(reason or "unknown"), meaningfulHumanEntries=int(humans or 0),
                              modelEntries=int(models or 0), humanParticipants=int(people or 0), windows=count)
                         for reason, humans, models, people, count in profiles or []],
            "meaning": "stored qualification counts per window, not deleted messages or a content-quality verdict",
        }

        errors = rows("bnl_journal_automation_runs", """SELECT reason,COUNT(*)
            FROM bnl_journal_automation_runs WHERE guild_id=? AND datetime(updated_at)>=datetime(?)
            AND lifecycle_state NOT IN ('published','quiet','superseded') GROUP BY reason""", (guild_id, cutoff))
        result["journalHoldReasons24h"] = {str(k or "unknown"): v for k, v in errors or []}
        usage = rows("token_usage_events", """SELECT route,COUNT(*),SUM(total_tokens)
            FROM token_usage_events WHERE usage_date=? GROUP BY route ORDER BY SUM(total_tokens) DESC LIMIT 10""",
            (now.astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat(),))
        result["pacificDayUsageAllGuilds"] = [dict(route=r[0], calls=r[1], tokens=r[2]) for r in usage or []]

        metadata = rows("bnl_journal_private_metadata", """SELECT metadata_json
            FROM bnl_journal_private_metadata WHERE guild_id=? ORDER BY updated_at DESC LIMIT 10""", (guild_id,))
        summaries = []
        for (raw,) in metadata or []:
            try:
                item = json.loads(raw)
                summaries.append({
                    "editorialVersion": item.get("editorialVersion", "legacy-anonymous"),
                    "sharedInputVersion": item.get("sharedInputVersion", "none"),
                    "reflectionVersion": item.get("reflectionVersion", "none"),
                    "creativeReflectionAllowed": bool(item.get("creativeReflectionAllowed")),
                    "currentActivityRelays": (item.get("aggregateCounts") or {}).get("currentActivityRelays"),
                    "retrospectiveRelays": (item.get("aggregateCounts") or {}).get("retrospectiveRelays"),
                    "sharedInputCandidates": len(item.get("sharedInputSourceProvenance", [])),
                    "citedPublicMoments": sum(source.get("sourceKind") == "public_moment"
                                              for source in item.get("usedSharedSourceProvenance", []) if isinstance(source, dict)),
                    "citedFinalizedShows": sum(source.get("sourceKind") == "finalized_show"
                                               for source in item.get("usedSharedSourceProvenance", []) if isinstance(source, dict)),
                    "namedParticipants": len(item.get("publicPeople", [])),
                    "citedConversations": len(item.get("supportingConversationRefs", [])),
                    "citedRelays": len(item.get("supportingRelayIds", [])),
                    "priorEntries": len(item.get("relatedPriorJournalEntryIds", [])),
                    "recurringTopics": len(item.get("recurringTopicCounts", {})),
                    "contextUses": len(item.get("contextUses", [])),
                    "coverageComplete": item.get("coverageComplete"),
                    "relaySourceHealth": (item.get("sourceHealth") or {}).get("status", "unknown"),
                })
            except (ValueError, TypeError, AttributeError):
                summaries.append({"readable": False})
        result["recentJournalEvidence"] = summaries

        receipts = rows("memory_governance_shared_brain_synthesis_runs", """
            SELECT rendered_lane_counts_json,prompt_applied,response_sent,source_revalidation_status
            FROM memory_governance_shared_brain_synthesis_runs
            WHERE guild_id=? AND datetime(created_at)>=datetime(?) ORDER BY created_at DESC LIMIT 500
        """, (guild_id, cutoff))
        lanes, fences = Counter(), Counter()
        for raw, applied, sent, fence in receipts or []:
            fences[fence] += 1
            if applied and sent:
                try:
                    lanes.update({k: int(v) for k, v in json.loads(raw).items() if isinstance(v, (int, float))})
                except (ValueError, TypeError, AttributeError):
                    pass
        result["sharedBrainReceipts24h"] = {
            "available": receipts is not None,
            "runsScanned": len(receipts or []), "scanLimit": 500,
            "lanesInSentPrompts": dict(lanes), "sourceRevalidation": dict(fences),
            "meaning": "prompt receipts, not proof of what the final prose said",
        }
        if {"website_relay_history", "bnl_journal_source_events"} <= tables:
            gaps = rows("website_relay_history", """SELECT COUNT(*) FROM website_relay_history r
                WHERE r.guild_id=? AND datetime(r.published_timestamp)>=datetime(?)
                  AND NOT EXISTS (SELECT 1 FROM bnl_journal_source_events e
                      WHERE e.guild_id=r.guild_id AND e.source_kind='website_relay' AND e.source_key=r.relay_id)
            """, (guild_id, cutoff))
            result["acceptedRelaysMissingArchiveReceipt24h"] = gaps[0][0] if gaps else None
    return result


def runtime_configuration(pid: int) -> dict:
    from bnl_shared_brain_synthesis import ordinary_chat_configuration
    values = dict(part.decode().split("=", 1) for part in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0") if b"=" in part)
    config = ordinary_chat_configuration(values)
    result = {key: config.get(key) for key in ("effective", "public_effective", "prerequisites_ready", "reason")}
    result["journalAutomationEnabled"] = values.get("BNL_JOURNAL_AUTOMATION_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    result["relayEnabled"] = values.get("BNL_WEBSITE_RELAY_ENABLED", "true").strip().lower() not in {"false", "0", "off"}
    result["scope"] = "process configuration; website pause/visibility controls remain authoritative"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="bnl01_conversations.db")
    parser.add_argument("--guild-id", type=int, required=True)
    parser.add_argument("--pid", type=int, help="Optional running bot PID; prints only shared-brain configuration status.")
    args = parser.parse_args()
    report = inspect(args.db, args.guild_id)
    if args.pid:
        report["sharedBrainRuntime"] = runtime_configuration(args.pid)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
