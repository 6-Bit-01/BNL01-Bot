# Journal public Moment and finalized-show inputs

## Position in the locked plan

This is the Journal input package in C5, following merged PR572's memory
chronology repair. PR572's merge is `88aecfbd3bdf9d6025d8b47988a53e38fb96d1bd`;
all three CI jobs passed. Merge does not establish the running VPS version.

The September 17 Journal/Relay review identified these missing reverse
connections. Journal already read public Discord/TikTok source events, accepted
Relays, approved broadcast memory, canon and earlier Journals. It did not read
the public Moment owner or completed show's recorded operations. Two tests
against the merged Journal reproduce those omissions with populated, eligible
source stores.

## What changes

- The existing Journal packet selects up to eight authorized, finalized shows
  whose recorded completion falls inside its half-open source window. It uses
  the same show ledger and operational chronology renderer as ordinary recall.
  A completed show can supply real current-window material when Discord is
  quiet. Chat and Relay counts remain truthful; Daily segments and Weekly
  periods include the show's completion in its actual period.
- Up to four relevant public Moments provide dated reflection/continuity.
  Selection uses the current material's topics, the existing public selector,
  and a 600-word selection budget. Original contributors retain their stable
  identities in private provenance and separate governed names/aliases in the
  writing projection. Current participants are not assigned to older exchanges.
  Historical Moment material never increases fresh-source or participant counts.
- The Moment selector has an optional read-only path and an upper observation
  bound. Its normal chat defaults stay intact. A reusable source-basis reader
  rechecks canonical Moment eligibility, original ledger sources, corrections,
  contributions and privacy without creating schemas or generating meaning.
- Journal's existing naming rules apply, including confirmed nicknames,
  ambiguous-label suppression and the owner label 6 Bit. Public Moment summaries
  remain paraphrases; jokes and unanswered questions retain their framing.
- Frozen packets and saved private metadata carry exact source identities and
  versions. All supplied candidates are fenced, even if the writer omitted a
  citation; separately stored cited lineage identifies the sources actually
  declared by the article. Moment lineage includes original ledger/conversation
  references. Show lineage retains its authorized parent key and version.
- Revalidation runs when freezing/reusing a packet, saving after generation,
  approving, claiming prepared work and checking delivery. A changed or
  ineligible source retires stale prepared work through the existing recovery
  path, leaving its scheduled occurrence owed. Retries cannot reinterpret an
  old source by selecting a newly ranked record.
- Forgetting a member also removes the new private source bases and naming
  metadata from affected published records. Existing public-publication
  retention policy is unchanged.
- The read-only health report adds `sharedInputVersion`, candidate count,
  `citedPublicMoments` and `citedFinalizedShows`. These are evidence receipts,
  not a verdict on prose quality.

No schema migration, new memory store, new scheduler, extra generation loop or
gate change is introduced. The existing 6:30 PM Pacific preparation, 7 PM saved
payload release, Tuesday–Sunday Daily/Monday Weekly cadence, four-attempt
generation ceiling, exact-byte delivery and idempotency remain authoritative.

## Verification and limits

The new offline tests use the actual Moment meaning and authorized show owners,
not a prompt-only stub. They exercise populated and empty read-only stores,
original dates and contributor separation, public eligibility and window bounds,
active and quiet Journal windows, Weekly period assignment, source correction
during generation, frozen packet reuse after reopening the database, saved
lineage, manual delivery refusal, scheduled exact-byte single delivery, revised
show invalidation, owed-occurrence preservation, and private forget scrubbing.

The focused Journal/Moment/show regression set and required `make check` must
pass before publication. Exact counts and the tested tree are recorded in the
PR/checkpoint receipt. Tests use deterministic writer fixtures and a local
receiver stub. They do not establish live-model prose quality or production
publication success.

The new finalized-show feed is specifically recorded operations. Eligible
TikTok/public Discord dialogue already reaches Journal through its source
archive. This package does not promote cached show interpretations, model
responses or lyrics into additional witnesses. It does not replace the existing
archive with a full-show transcript feed.

## Deployment and natural acceptance

After merge, use the normal VPS pull/restart procedure and verify the running
revision. No public rehearsal or paid forced generation is needed. At the next
natural Journal preparation/release, use the existing health command:

```bash
sudo /home/ubuntu/bnl01/venv/bin/python -m scripts.journal_relay_health \
  --db /home/ubuntu/bnl01/bnl01_conversations.db \
  --guild-id 1288269405209235551 \
  --pid "$(systemctl show bnl01 -p MainPID --value)"
```

Newly built packets report `journal-shared-inputs-1`. A source category need not
appear in every entry: relevance and the window decide selection. For a natural
entry that uses it, check correct historical tense, contributor credit, cited
source count and actual receiver acceptance. An older already-prepared payload
is still its saved revision and gains no new source authority automatically.

## Remaining sequence

C6 connects relevant public Moments, finalized shows and published Journals to
the existing Relay writer with durable pending-source metadata and revalidation.
C7 handles eligible published Ballad metadata/show links, with site selection
authority and lyrics excluded as fact evidence. C8 covers existing Ambient,
relationship tone and approved website/source-file consumers. C9 recovery
checks continue across those packages; the known interrupted Moment-meaning
`generating` state remains explicit and is not automatically replayed here.

The combined sealed continuity batch and natural semantic acceptance for Moment
meaning, attribution and later reopening remain distinct live evidence tasks.
This Journal package does not mark the whole shared brain complete.
