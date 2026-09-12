# Shared-brain recovery: September 12 batch review and focused follow-up

This continues the original recovery plan and the consolidated work in PR #540.
All four Discord replies, their immediate route-debug outputs, and the recovered
00:03–00:10 UTC VPS export have been received. The deployment at
`a90c7cabcf7c14c96cf358f70ada36f78824568d` matches the tested tree.

## What the completed batch establishes

| Case | Correlated evidence | Result |
| --- | --- | --- |
| September 10 retained recall | Context selected original rows 8430–8432 from `mom_2493da7796b6542f63133d5a3b8b0d8a`; the applied packet included two conversation items; source revalidation passed; delivery succeeded | Accepted for this retained-source recall |
| September 11 named-member continuation | Context selected rows 8455–8460 from `mom_eb349cd3d126dccbd006ad604144c611`; the packet then excluded five items as `frame_subject_ambiguous` and applied only current intent | Failed source application; a plausible answer does not establish grounded recall |
| Broad TikTok/Discord song | Both September 4 and August 28 show sources were loaded and revalidated on the established generation path; delivery and its no-store assessment were recorded | Broad show retrieval and assessment recording confirmed; the extra 2088 reference and third style remain an instruction-following failure |
| Shows + Journal + current queue | Both shows and website context reached the established prompt; delivery and its no-store assessment were recorded | Source-path composition and assessment recording confirmed; the packet was built but was not the applied generation owner |

The first two provider calls took 3.903 and 7.092 seconds. The song made two
physical calls, 18.442 and 17.984 seconds. The mixed answer made a 7.944-second
answer call plus an 8.777-second `cross_universe_bleed` decoration call. All six
reported success, with retry and fallback false. These facts do not resolve the
older long provider stalls. The two show-related packets had `prompt_applied=0`
and `live_applied=0`; their evidence was delivered through the established
show/website path. The two synthesis rows belong to the applied recall packets.
There are four successful delivery assessments, including the two no-store turns.

Public activation rollback/restoration, general name/spacing recall, qualifying
two-person Moment formation, and earlier scoped acceptance results stay accepted.

## Repairs selected from this batch

1. **Separate episode uncertainty from person resolution.** An unresolved episode
   no longer prevents the existing account-binding owner from resolving a single
   named member. Missing, competing, blocked, retired, or invalid identities keep
   their existing validation. Original conversation sources still revalidate.
2. **Separate numeric bounds from named alternatives.** A request such as choosing
   a year or tempo between two bounds no longer turns the endpoints into payload
   names that a valid answer must repeat. Quoted alternatives, actual named
   choices, and explicit endpoint choices keep their existing behavior.

The range defect is reproduced locally: a valid interior-year answer was marked
`current_payload_unanswered`. The saved live export does not include the first
song draft or its correction reason, so this is not proof that this defect alone
caused the live song's second call. It also does not guarantee that a model will
obey every style constraint. The retest below checks the delivered result and
captures correction reasons along with the physical attempts.

These changes extend the existing Context v2 and Intelligence Packet owners.
They add no song/member-specific rules, memory stores, access gates, extra model
calls, or new response validators. Reading a retained conversation does not by
itself establish episode reopening or public tier growth.

Validation: the new regressions failed on the deployed baseline and passed with
these changes. All 162 focused tests and all 2,996 tests in the required
`make check PYTHON=python` gate passed. Both copy/paste shell blocks and their
embedded Python parse successfully. Live acceptance of this follow-up is pending.

## After review and merge: deploy once

Paste this entire block on the VPS. The capture setup is included and saved on
disk, so the later export also works from a new SSH shell. If the block fails,
stop before Discord testing and retain the error.

```bash
(
set -euo pipefail
cd /home/ubuntu/bnl01
git pull --ff-only origin main
venv/bin/python -m py_compile bnl01_bot.py bnl_conversation_context_v2.py bnl_unified_intelligence_packet.py
sudo systemctl restart bnl01
sudo systemctl is-active --quiet bnl01
venv/bin/python - <<'PY'
import json, subprocess, tempfile
from datetime import datetime, timezone
from pathlib import Path

capture = {
    "start_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    "directory": tempfile.mkdtemp(prefix="bnl-context-scope-retest."),
    "deployed_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True).strip(),
}
Path("/tmp/bnl-context-scope-retest.json").write_text(json.dumps(capture))
print("Service active; capture setup saved:")
print(json.dumps(capture, indent=2))
PY
)
```

## Discord: two sequential cases, then one return

Both messages are sent by **6 Bit** in **bnl-testing**. No BNL prefix is needed.
Miss Bit does not need to repeat her earlier contribution. Wait for each complete
answer, immediately save the output of `!bnl debug last route`, then continue.
A wrong answer is recorded and the independent next case can proceed. If no
answer arrives within two minutes, record the timeout, stop sending additional
prompts into that generation, and run the same final export.

**1. Named-member retained recall**

```text
Return to the playback correction discussion that Miss Bit joined on September 11, 2026 (UTC). What did each of us contribute, and what remains unresolved?
```

```text
!bnl debug last route
```

Expected: the original contributions are correctly attributed, unverified
playback/record changes remain unresolved, and the applied packet includes the
validated conversation evidence. An intent-only packet does not pass even if
the reply sounds reasonable. Debug should identify `bnl-testing` / `sealed_test`.

**2. Broad source use and bounded creative instructions**

```text
Let's make a song based on your TikTok chat and discord data, based on all you can see like recurring topics or behaviors. No less than 1400 characters of lyrics, and for the style prompt choose a year between 1974 and 2008 and combine 2 styles that don't normally belong together.
```

```text
!bnl debug last route
```

Expected: multiple available show sources, at least 1,400 lyric characters, one
chosen year in the requested range, and two styles. The range endpoints should
not be reported as named choice anchors. A single remark must not become a
claimed recurring behavior. Preserve the entire answer, including split messages.

## VPS: one export after both cases

Paste this whole block. It reads the saved capture setup and the existing logs
and database; it changes no source record or service configuration. It includes
correction/guard reasons that the previous narrow log filter omitted.

```bash
cd /home/ubuntu/bnl01 && venv/bin/python - <<'PY'
import json, sqlite3, subprocess
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic

capture = json.loads(Path("/tmp/bnl-context-scope-retest.json").read_text())
start = capture["start_utc"]
end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
folder = Path(capture["directory"])
pattern = (
    "conversation_context_v2|situation_frame|show_episode_evidence_context|"
    "tiktok_show_analysis|bnl_read_model_context_loaded|shared_brain_synthesis|"
    "ordinary_chat_single_packet|gemini_(model_attempt|generation_completed|model_retry)|"
    "model_generation_attempt|response_send_(succeeded|failed)|"
    "guard|regenerat|rewrite|response_obligation|source_revalidation|frame_revalidation|"
    "batch_response_persistence_skipped|unified_response_assessment_shadow_recorded|"
    "\\[batch:(active_packet_decision|generation_started_after_wait|response_send_commit_complete)\\]"
)
log = subprocess.run([
    "sudo", "journalctl", "-u", "bnl01", "--utc", "--no-pager", "-o", "short-iso",
    "--since", start + " UTC", "--until", end + " UTC", "--grep", pattern,
], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
(folder / "service.log").write_text(log.stdout + log.stderr)
print(log.stdout)
if log.stderr:
    print(log.stderr)
queries = {
    "assessments": ("unified_response_assessment_shadow_runs",
        "run_id,created_at,current_speaker_count,participant_count,"
        "source_basis_kinds_json,prompt_lanes_json,source_basis_changed_before_send,response_sent"),
    "packets": ("memory_governance_intelligence_packet_runs",
        "run_id,created_at,item_count,selected_lane_counts_json,excluded_by_reason_json,"
        "subject_resolution_status,episode_query_status,revalidation_status,prompt_applied,live_applied"),
    "delivery": ("memory_governance_shared_brain_synthesis_runs",
        "run_id,packet_run_id,created_at,provider_call_count,corrective_call_count,"
        "source_revalidation_status,prompt_applied,live_applied,response_sent,guard_status"),
}
report = {**capture, "end_utc": end, "journal_exit_code": log.returncode}
with closing(sqlite3.connect("file:bnl01_conversations.db?mode=ro", uri=True, timeout=3)) as db:
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    deadline = monotonic() + 5
    db.set_progress_handler(lambda: int(monotonic() > deadline), 10000)
    db.execute("BEGIN")
    for name, (table, columns) in queries.items():
        try:
            rows = [dict(row) for row in db.execute(
                f"SELECT {columns} FROM {table} WHERE guild_id=? AND channel_policy=? "
                "AND julianday(created_at) BETWEEN julianday(?) AND julianday(?) "
                "ORDER BY created_at LIMIT 26",
                (1288269405209235551, "sealed_test", start, end))]
            report[name] = {"rows": rows[:25], "truncated": len(rows) > 25}
        except sqlite3.Error as exc:
            report[name] = {"error": str(exc)}
output = json.dumps(report, indent=2)
(folder / "receipts.json").write_text(output + "\n")
print(output)
print("Saved capture:", folder)
PY
```

Return **once** with both complete Discord answers, both immediate debug outputs,
and this export. Empty, errored, or truncated receipts are incomplete evidence.
Review both cases together before any further repair or deployment.

## Original-plan remainder

- Episode linking/reopening is distinct from source-backed retained recall.
- Public/community/show/Ambient consumption, public tier/relationship growth,
  recurrence/retention over time, and applicable natural source corrections still
  require their recorded acceptance evidence; sealed tests cannot certify them.
- The historical long provider stalls and two earlier Relay rejections remain
  unclassified where the necessary evidence is missing.
- Final owner acceptance follows reconciliation of the remaining evidence.
