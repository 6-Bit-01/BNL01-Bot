# Relay overload recovery

## Observed cause and comparison

The owner's service log at 2026-09-25 15:10:16 UTC (08:10 Pacific), PID1762276,
shows `website_relay_event` on `gemini-3.6-flash` returning HTTP503 / UNAVAILABLE.
Google explicitly states that the model is experiencing high demand. The input
was 29,266 characters with a 4,096 output-token ceiling. The failed attempt has
reservation `fc5f98fa245149bf8b7f3ce92093ae63`, with retry and fallback both false.
This confirms PR586's diagnostic behavior after the owner installed d5eb40d.
It establishes Google's stated reason for this request, not every earlier 503,
a global incident, billing status, or the account's capacity tier.

The bounded prior capture first records a 503 on September 23 at 15:10:51 UTC,
before PR574/575's source changes. The previously working Relay already used
one primary attempt: PR444 / 0cfd84b7 disabled background retries and fallback
on August 25 at 07:11:14 UTC. Yesterday's changes did not introduce that policy.
The history includes successful generation with the same source classes that
failed; no single deterministically broken source was established.

Google's [503 guidance](https://ai.google.dev/gemini-api/docs/generate-content/api-errors)
recommends waiting/retrying or trying another model. Its
[status page](https://aistudio.google.com/status) showed all systems operational
when checked September 25; no listed incident negates the request-level error.

## Change

Only `website_relay_event` gets one backup attempt following primary HTTP503.
It uses the existing configured fallback (default `gemini-3.5-flash`) after a
0.5–1 second jittered pause. No per-model retries: at most two physical calls.
Other statuses, absent/identical backup, and other route policies do not gain
fallback. A successful primary still uses one call.

Existing token and dollar admission reserve the complete two-model envelope
before generation; accounting records each attempt and its actual model.
Hard caps, protected reserves and unknown-price restrictions remain enforced.
Reservation can deny a request when the full envelope cannot fit. A backup
shares the Google project and may also be unavailable; recovery is bounded,
not a promise that provider overload cannot affect BNL.

Cancellation of the Relay's existing async generation propagates to the worker:
if a primary returns 503 after timeout, it cannot begin a backup request. An
already-running SDK request may finish and is still accounted; its abandoned
draft cannot publish. No new scheduler, forced public generation, gate change,
schema change, template fallback, or changed source/validation/publication owner.
Both models failing leaves the last accepted Relay in place until a usual tick.

## Verification and deployment

Focused mocked tests cover primary success, 503 recovery, both failures,
non-503 rejection, missing/identical backup, cancellation, hard dollar admission,
concurrent reservations and actual SQLite accounting. Existing Relay tests cover
source validation, duplicates, cursor behavior and rejection of late drafts.
Run `make check` before publishing. Tests do not call Google or post to the site.

After merging, use the normal owner deployment:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
sudo systemctl restart bnl01 &&
systemctl show bnl01 -p ActiveState -p SubState -p MainPID &&
git rev-parse HEAD
```

After the next ordinary :10 Relay tick, read the last two hours once:

```bash
sudo journalctl -u bnl01 --since "2 hours ago" --utc --no-pager -o short-iso \
  --grep='gemini_.*route=website_relay_event|model_generation_attempt.*route=website_relay_event|website_relay_(generation|no_publish|delivery_failed)' -n 50
```

Correlate the post-deployment reservation: primary503 then exactly one fallback
attempt and its result. Primary success is valid generation evidence but does
not exercise recovery. Model success alone does not prove publication: inspect
the existing read-only `scripts/journal_relay_health.py` report for the same
guild and post-deployment timestamp, or read the retained attempt outcome and
`accepted_relay_id`/`website_published_at` receipt in `website_relay_attempts`.
Carry forward candidate rejection, budget restriction, delivery failure or
missing evidence separately. Never count old publications as post-fix success.

A bounded, content-free receipt read (all guilds, newest six in two hours):

```bash
cd /home/ubuntu/bnl01 && ./venv/bin/python - <<'PY'
import json, sqlite3
with sqlite3.connect('file:bnl01_conversations.db?mode=ro', uri=True, timeout=3) as c:
    c.row_factory = sqlite3.Row
    rows = c.execute("""SELECT started_at, completed_at, outcome, reason,
        accepted_relay_id, website_published_at FROM website_relay_attempts
        WHERE datetime(started_at)>=datetime('now','-2 hours')
        ORDER BY started_at DESC LIMIT 6""").fetchall()
    print(json.dumps([dict(r) for r in rows], indent=2))
PY
```

No live show testing or extra polling task is required. If both models report
503, preserve the bounded provider details for Google support and wait for the
normal cadence; do not automatically raise budgets or disable source checks.
