# Private Journal test

After deploying this change, the configured owner can run this in `#bnl-testing`
or an approved operator channel:

```text
!bnl journal test | hours=24
```

BNL sends a delivery check and then the result to that owner's Discord DMs.
The channel receives only a status response. Server DMs must be enabled before
generation starts. The test is a daily-style Journal over a rolling 24-hour
window, not the scheduled publication period; `hours` can select 1–168 hours.

The test uses the current Journal source selection, public speaker projection,
context lanes, prior published history, and editorial prompt. Live website
public-hidden and memory-excluded Journal IDs stay excluded. A missing control
snapshot or incomplete archive is reported before any generation.

There is one writing attempt, no Journal repair loop, and no automatic retry of
the test. Existing provider transport retry/fallback and normal budget accounting
still apply. A safe draft with an editorial advisory is kept for inspection. A
parseable, privacy-clean response is also kept when publication validation finds
an attribution, coverage, or structure issue. The DM includes the writing and
the first publication finding, with structural locations when available. For
example, `undeclared_context_use` can identify the excerpt or a section body; it
does not hide the article. `ok=true` means the preview was available for review,
not that publication validation passed. No approval is created.

Privacy checks run independently before preview delivery, so an earlier citation
or coverage error cannot mask an identity, sensitive-detail, or source-token
leak. Parse/privacy/provider failures return a content-free reason without
saving the response or requesting another draft. Budget limits stay in effect.
Production Journal validation and its existing repair behavior are unchanged.

`journal-private-test-2` fixes the September 17 test that stopped at
`undeclared_context_use`. The old test did not retain its prose, so its exact
offending wording cannot be recovered from the completion log. The code can
flag missing context declarations or heuristic wording/overlap; the reason
alone does not establish which occurred in that specific response.

The command is dispatched before room, conversation, profile, and memory intake.
The source path opens SQLite read-only and skips schema initialization and legacy
backfill. Generated text stays in process memory until DM delivery. It creates no
Journal entry, private metadata, automation run, approval, pending delivery,
publication, Relay, memory fact, or Moment. The preview cannot be approved because
it has no entry ID. The bot ignores its own messages and DMs at ordinary ingress.
The DM remains visible in Discord; it is not ephemeral or automatically deleted.
Normal model usage receipts and a content-free completion log remain available.

The existing `journal create` and `journal preview` commands retain their saved
draft behavior. `run-daily` / `run-weekly` are production commands and are not this
test path. The scheduled Journal is unaffected by running a test.

## Evidence and remaining connections

The September 17 production report supplied by 6 Bit confirms active public
shared-brain synthesis, two published Journals, 19 accepted Relays, no pending
Relay payloads, and no missing Relay-to-Journal archive receipts in the reported
24-hour window. Sent-prompt receipts include conversation, canon, episode,
show-episode, and Relay-publication sources. These receipts do not establish
which facts appeared in the final response.

All 32 recent Moment windows were rejected with
`low_signal_or_insufficient_continuity`. This is a consolidation qualification
result, not deletion of the underlying conversation or ledger entries. It alone
cannot distinguish quiet windows from lost useful continuity.
`momentRejectionReasons24h` exposes reason counts; `momentRejectionProfiles24h`
adds grouped stored counts of meaningful human entries, model entries, and human
participants per rejected window. Both are scoped to the same guild and 24-hour
window, without content, participant names, or source IDs. At most 50 distinct
profiles are returned, largest groups first. A legacy schema without those
count columns reports profiles unavailable rather than inventing zero counts.

The current engine qualifies at least three meaningful human entries from at
least two people, or one person with BNL/model participation plus either three
meaningful entries or two with a strong continuity marker. Model entries never
become independent factual authority. The profile does not inspect text or claim
to measure that marker. Frequent one-entry windows suggest inspecting window
splitting; sustained single-person windows with zero model entries suggest
inspecting model-turn intake. These are follow-up leads, not diagnoses. This
change does not lower Moment qualification thresholds without that evidence.
The 13 ambiguous and one unresolved subject revalidations also warrant a focused
identity check; aggregate counts do not prove accidental identity blending.

| Direction | Current status |
| --- | --- |
| Public Discord / eligible TikTok archive / approved broadcast memory → Journal | Existing writer inputs |
| Accepted Relays → Journal; published Journal and Relay → ordinary chat | Existing readers and publication receipts |
| Discord evidence → Moment / episode formation; eligible Moments → chat | Implemented, with qualification and source revalidation; production rejections need explanation |
| Finalized show ledger → shared-brain chat | Implemented and present in the supplied prompt receipts |
| Journal → Relay writing | Direct writer input still missing |
| Public Moments and finalized show ledger → Journal / Relay writing | Direct writer inputs still missing |
| Selected published Ballad → Journal / Relay writing | Site-authoritative adapter still missing |

The private test does not add the missing writer adapters. Their implementation
plan remains in `journal_relay_shared_brain_review_2026-09-17.md`.

The intended shared brain supports relevant cross-source recall in both
directions. It does not copy every generated output into every store. A comment
about a track can enrich the remembered show, but queue/playback facts retain
their operational authority. A Journal or Relay can become a remembered BNL
publication without turning its interpretations into independent evidence or
counting the same underlying event multiple times.

## Deployment and focused check

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
venv/bin/python -c "from bnl_journal import JOURNAL_TEST_PREVIEW_VERSION; assert JOURNAL_TEST_PREVIEW_VERSION == 'journal-private-test-2'; print(JOURNAL_TEST_PREVIEW_VERSION)" &&
sudo systemctl restart bnl01 &&
systemctl show bnl01 -p ActiveState -p MainPID
```

Run the Discord command above once. Check that the article arrives in DMs even
if it has an attribution/coverage review note, and that the channel gets only
status. A budget/provider/parse/privacy failure can still prevent delivery.
To inspect the content-free completion receipt:

```bash
sudo journalctl -u bnl01 --since "10 minutes ago" --no-pager \
  --grep='journal_private_test_finished' -n 5
```

A delivered preview logs `ok=True reason=none`,
`publication_check=none` or the first publication finding, and
`stored=false published=false`. DMs/control failures before generation instead
return a channel status without a completion receipt. The log contains no prose.

Obtain the Moment profiles without generating anything:

```bash
venv/bin/python - <<'PY'
import json
from scripts.journal_relay_health import inspect
report = inspect("bnl01_conversations.db", 1288269405209235551)
print(json.dumps({key: report[key] for key in (
    "momentRejectionReasons24h", "momentRejectionProfiles24h"
)}, indent=2))
PY
```
