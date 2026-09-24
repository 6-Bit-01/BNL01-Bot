# Artist credit and historical queue submission

PR583 is deployed at `0399b69c7466e7cdbb6bb5fd1952f2871b374d16`.
The owner supplied an active/running receipt for PID 1754132, started at
2026-09-24 04:08:48 UTC in `/home/ubuntu/bnl01`.

The 04:09 UTC mixed-history reply now has Conversation, FinalizedShow, Memory
and Publication source revalidation, all unchanged. Generation took 9.841s;
generation completion to Discord send success was 3.293s, including 0.217s
from send start to success. These observations establish delivery with both
original-conversation and finalized-show sources. They do not establish every
claim in the generated answer.

The answer incorrectly denied any historical queue participation. The public
read model fetched at 2026-09-24T04:14:31.345Z contains a September 11 roster
record credited to **6 Bit**, titled **Party Time!**, submitted as **@chris**.
It is record 49 of 50 in that show's public roster, with `finished` outcome and
`external_host_finished` broadcast evidence. This establishes the recorded
credit, submission and host-reported outcome, not instrumented playback.

Source: <https://barcode-network.com/api/bnl/read-model>,
`sections.publicHistory.shows[].trackRoster`.

## Cause and repair

The compact history reader prioritized the requested person's submitter
handle, then clipped the roster to 12 records. A different person's submission
of the artist's song was outside that selection. The single-show reader also
clipped its roster before considering the requested artist. The packet
operations view carried selected milestones without the independent roster.

The existing show reader now ranks the full retained roster by requested
track/artist labels, the subject's public display labels, and submitter
handles before applying its existing display bound. Artist/project credit
and submission attribution remain separate fields. Both native renderings
use the same selection, and the existing packet operations view includes it.
The roster reports its displayed and retained counts; omission does not prove
absence. A public label match is a retrieval cue, not a new identity binding.

There are no identity writes, source-row changes, schema changes, new gates,
provider routes or name-specific response rules. Existing show/date/subject
selection and source-currentness checks remain in force. The owner is only
identified publicly as 6 Bit.

## Verification and remaining acceptance

A 50-record fictional roster reproduces the defect at position 49. The artist's
song is submitted by another handle, while the artist's own handle submits a
different artist's song at position 50. All six self/named/dated combinations
with packet on/off failed on the deployed baseline; the separate packet
operations assertion failed too. Direct and batch delivery now retain the
correct record and submitter, with fresh source validation. Existing tests
continue to check private-source exclusion and named-subject scope.

An offline render of the actual public September 11 roster now places
6 Bit — Party Time! first while preserving Chris's submitter attribution.
This is independent public source verification, not a capture of the VPS
provider prompt. Full-suite and CI results belong in the PR receipt.

After merge/deploy, ask naturally about past music in the queue, optionally
with Discord/TikTok history. BNL should be able to name this recorded song
and distinguish artist from submitter. Preserve the already accepted public
quote, Pacific-time and source-delivery results. Friday's live rehearsal and
the agreed eligible scheduled follow-through remain separate acceptance.
