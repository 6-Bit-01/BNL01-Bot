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
parse or blocking validation failure returns its reason without saving the
response or requesting another draft. It never raises or bypasses budget limits.

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

All 32 recent Moment windows were rejected. This alone cannot distinguish normal
low-signal filtering from lost useful continuity. `momentRejectionReasons24h` in
the health report now exposes the existing qualification-reason counts, scoped
to the same guild and 24-hour window, without content or participant names.
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

This PR adds the isolated test, not the missing writer adapters. The implementation
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
venv/bin/python -c "from bnl_journal import JOURNAL_TEST_PREVIEW_VERSION; assert JOURNAL_TEST_PREVIEW_VERSION == 'journal-private-test-1'; print(JOURNAL_TEST_PREVIEW_VERSION)" &&
sudo systemctl restart bnl01 &&
systemctl show bnl01 -p ActiveState -p MainPID
```

Run the Discord command above once. Check that the article or failure reason
arrives in DMs and that the channel gets only status. To inspect the content-free
completion receipt:

```bash
sudo journalctl -u bnl01 --since "10 minutes ago" --no-pager \
  --grep='journal_private_test_finished' -n 5
```

A completed test logs `stored=false published=false`. DMs/control failures before
generation instead return a channel status without a completion receipt. Rerun
the same read-only health command used for the prior report to obtain
`momentRejectionReasons24h`; it makes no model calls.
