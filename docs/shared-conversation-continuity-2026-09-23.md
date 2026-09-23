# Shared conversation continuity — first completion package

The September 23 private transcript exposed a source-selection failure after a
short September 4 recap. The next request for actual comments retained the
older clock/live questions, lost the recap, and resolved no show-analysis
intent. Local reproduction matched those selected human rows: a period lost
the recap, a question mark or overlapping words retained it. A deliberately
unsaved BNL reply must not erase the preceding human request.

The existing Context owner now retains a bounded recent human tail for the
same requester and room. It keeps the existing age, privacy, source and prompt
bounds, reserves room for those turns before older pairs, and renders them in
chronological order. An explicit new topic clears prior scope. Exact Discord
reply sources keep their structural priority. This does not persist private
model replies or create another conversation store.

The shared person/topic test exposed a second handoff gap: retaining the human
question did not cause a pronoun follow-up to reload original memory. Context
now passes the selected requester's human source references to the existing
Frame/member resolver and Discord/show readers. A unique public member binding
can continue across a pronoun follow-up. The guild resolver still owns identity;
ambiguous members, another speaker, an intervening unrelated topic, and explicit
replacement scope cannot silently inherit that binding. Original authored
sources supply quotes and attribution; prior BNL prose does not corroborate them.

Current explicit dates and recent-show counts also outrank an old conversation
date in the finalized-show fallback. The recent-four edge case was reproduced
locally; the owner's actual four-show runtime log already loaded all four dates.
It is not evidence that the runtime four-show answer used the wrong scope.
Likewise, `quote_lookups=[]` does not mean that ordinary authored excerpts were
absent. The clock/live reply's loaded operational contents were not supplied,
so missing operational evidence is not asserted as its cause.

## Verification and acceptance

Focused tests exercise Context, real source readers, direct assembly, real batch
generation/delivery boundaries, corrections, source refresh, Frame binding and
existing cross-source memory. Public and sealed policies and packet on/off paths
are covered. Provider and Discord transport are fixtures; supported fixed
responses prove source delivery and single-send behavior, not live model quality.
The full required gate is `make check PYTHON=.venv/bin/python`. Exact results and
tested/published identifiers are recorded in the draft PR and current checkpoint.

After normal owner deployment, run one focused sealed-channel batch:

1. Ask for a short historical recap, then actual comments and speakers without
   repeating its date. Follow with a different explicit date, then last four
   shows. The selected sources and answer must follow each current scope.
2. Ask about a known public member/topic, then their exact words. Check original
   text and attribution; change the person or topic and verify the old subject
   is not substituted. Missing or ambiguous evidence must remain explicit.
3. Include one historical-show/current-queue mixed request. Each part must use
   its existing authority and time scope. Preserve the accepted Pacific clock
   behavior and the distinction between schedule and actual live state.

Retain the deployed SHA, message IDs/timestamps, selected source IDs/show dates,
provider/send receipts and actual answers for those turns. Diagnose a failure at
capture, selection, assembly, interpretation, revalidation or delivery before
changing code. Do not rerun previously accepted unrelated cases. Do not use a
public rehearsal or repeat an entire archive audit for this package.

## Deployment and rollback

Review and merge first, then use the existing clean VPS main checkout:

```bash
cd /home/ubuntu/bnl01
git status --short --branch
git pull --ff-only origin main
git rev-parse HEAD
sudo systemctl restart bnl01
sudo systemctl status bnl01 --no-pager -l
```

Preserve a dirty or differently branched checkout rather than overwriting it.
No migration, dependency, configuration or gate change is required. Roll back
with a normal revert of this merged package and the same deployment process.

## Remaining shared-brain completion work

The existing master checkpoint is the finite completion ledger. This package
does not close all memory, time or editorial work. Next dependencies remain
useful Moment meaning/contributions and continuation/reopening; short/medium/
long/Core recall, aging and correction/deletion propagation; event versus
recorded versus published time; Journal input from eligible public Moments and
finalized shows; Relay input from Moments/shows/published Journals; and published
Ballad editorial context. Existing Ambient, relationship-informed tone and
approved website/source reads are required consumers in acceptance.

Recovery is checked with each affected package: bounded retries and timeouts,
source changes during generation, single delivery, preserved pending/owed work,
restart recovery and phase timing. Natural scheduled or group events retain
their explicit acceptance conditions. Full dossiers, new autonomous modes and
new feature work remain separate; website item 3 stays paused.
