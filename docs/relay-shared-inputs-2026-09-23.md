# Relay shared inputs — September 23, 2026

This is C6 in the locked shared-brain completion plan, built on merged Journal
PR573 (`891339098c0220061bd1f21608f8ba279e6bf12e`). PR573's merged tree matches
its tested tree; transport and both Python CI jobs passed in run 35893426270.
A merged commit is not evidence of a running VPS revision.

## Confirmed gap and repair

Relay's approved quiet-source selector previously read recent public Discord,
scoped broadcast memory and canon. Public Moment, finalized-show and canonical
published-Journal readers were absent. The v2 pending payload preserved exact
delivery bytes but had no reconstructible basis for those shared sources.

The existing selector now offers one bounded candidate from each new category.
Its normal accepted-publication rotation chooses one source; there is no quota
to include every category or a second writer/router. Fresh Discord retains its
existing priority. Quiet-source cadence, generation limits and control gates
remain unchanged.

| Input | Authority and scope | Saved revalidation basis |
| --- | --- | --- |
| Public Moment | Existing public, finalized, source-revalidated Moment selector; dated summary and distinct original contributions | Moment ID, original ledger references and participant IDs, canonical root and source version |
| Finalized show | Existing authorized, completed-show operations reader; completion time, never sync time or schedule | Exact show key, version and selection window |
| Published Journal | Existing canonical published revision plus fresh authenticated visibility/reuse controls | Entry, revision, digest, query mode, source period and publication time |

Selection uses a 30-day historical window and existing bounded readers. When
public conversation supplies a topic, continuity must match it. Otherwise a
recent eligible source can supply the angle. Missing stores are read without
schema initialization. Selection, control requests and delivery checks run off
the event loop. Journal callbacks obey memory exclusions even when the topic
mentions an exact Journal title or ID.

Historical source prompts preserve participant roles, separate banter from
fact, and identify Journal prose as a prior publication rather than another
witness. Historical input cannot establish current playback, intake or live
state. Both generated lines are checked for current-state claims, including
during a regular scheduled show window. Same-event retellings do not establish
recurrence. The show adapter is recorded operations, not a new ungoverned chat
or profile feed. Published Ballad context remains C7.

## Delivery and privacy

The existing pending and accepted-history tables gain `source_basis_json`,
defaulting to an empty list for legacy records. Source references and versions
participate in the stable Relay fingerprint. They stay outside the website
envelope and the public backup format.

New shared candidates are revalidated after generation, before every v2 HTTP
attempt, and on saved replay after restart. Moment checks return to the original
contributions; show checks return to the exact authorized ledger; Journal checks
fetch fresh visibility/reuse authority. The existing HTTP retry ceiling remains
one retry. No additional generation retry is introduced.

- Changed, withdrawn or unreconstructible sources retire the pending candidate,
  record the blocked reason and leave the conversation cursor unchanged. The
  same transaction does not generate another candidate.
- Temporary database/control failures retain the exact pending payload for a
  later normal revalidation. Transport failures also retain its stable identity.
- A successful replay uses identical bytes and records one acceptance. Startup
  reconciliation can confirm a payload already accepted by the site without
  another POST, preserving its source lineage.
- Existing member/guild forget paths remove affected unsent derivatives and
  scrub private lineage from accepted public records. The existing public-prose
  retention policy is unchanged.
- Existing legacy pending payloads retain their original replay behavior; no
  source lineage is invented for them. The v1 compatibility path gains a
  post-generation check; durable replay and per-attempt fencing belong to v2.

The existing read-only health report now includes `relaySharedInputs` counts
for pending and recently accepted records, plus `relaySourceHolds24h`. It does
not print source text, people, IDs, payloads or private linkage. The explicit
backup allowlist accepts the new private column while excluding it from exports.

## Verification and remaining evidence

Twenty new integration tests use real public Moment, show and Journal stores,
with deterministic writer and receiver fixtures. They cover actual selection
and prompt assembly, eligibility, source correction during generation, saved
restart replay, a source change between HTTP attempts, show digest changes,
Journal visibility/reuse changes, temporary authority failures, forget cleanup,
startup acknowledgment, event-loop responsiveness and content-free diagnostics.
The actual-selector test fails against merged PR573: an eligible populated
Moment is omitted and the canon path wins. The repaired focused run passes 187
tests; `make check PYTHON=.venv/bin/python` passes all 3,260 tests in 173.321
seconds. The exact published tree is recorded in the PR and checkpoint receipt.

Offline results establish these integration and control boundaries. They do not
establish live prose quality, deployment or a future website acceptance. After
normal owner deployment, observe the next eligible natural Relay through the
existing health report and receiver receipt. A category absent from an
irrelevant or ineligible window is not by itself a failure. No public rehearsal
or paid test generation is needed for this package.

C7 published Ballad metadata is the next offline owner. The combined C2 sealed
continuity batch, C3 natural meaning/group attribution/reopening/aging, C4 actual
operational freshness and final C8/C9 reconciliation remain explicit. The known
interrupted Moment meaning attempt left in `generating` is not repaired here;
no automatic replay is added. Website item 3 remains paused.
