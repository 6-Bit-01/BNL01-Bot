# Memory chronology — locked completion rows C3/C4

Base: merged PR571, `eea6d1463e9976741bc216c1ac687748a4ca2e62`.
PR571's three CI jobs passed. Its production revision and focused live acceptance
remain separate from the user's merge confirmation.

## Confirmed gap and repair

The merged baseline selected a different retained exchange when asked for a
Pacific evening's named or ISO date. The retained reader compared stored UTC
dates; derived readers did not apply the requested date before ranking and
deduplicating gists. The packet retained a Moment timestamp internally but
omitted it from the model's evidence. Five initial regression tests exposed
seven failing assertions/subcases against that baseline.

The existing owners now carry the same chronology through selection and use:

- Moment transcript, participant, situation and legacy gist readers match
  explicit dates, yesterday and last night against the original exchange in
  Pacific time. An explicit UTC request uses UTC. A short exchange spanning
  midnight can match either calendar date. Missing/invalid dated source times
  cannot silently become the current time.
- Date selection happens before gist deduplication. A source's later meaning
  save does not change its conversation date. Unsolicited topic associations
  remain associations: a new topic's date is not an instruction to replace
  historical related context with same-day memories.
- Context stores the request's reference time in its existing source basis.
  The pre-send check rereads current sources and controls without reinterpreting
  yesterday after a midnight crossing. Packet selection and revalidation use
  the existing frozen request time. A later turn resolves its own date anew.
- Retained transcript prompts carry original conversation dates; Moment and
  episode evidence carries its last-activity timestamp and Pacific offset.
  Those timestamps date the exchange, not an event merely mentioned in it,
  a later memory revision, or a publication.

There is no new memory store, date router, scheduler, provider attempt, schema,
dependency, configuration or gate change. Existing visibility, participant,
source digest, ambiguity, candidate and character limits remain in force.
This does not turn a dated gist into an exact quote or present operational state.

## Verification and retained evidence

The PR records the final full-suite and remote-tree receipts.

| Boundary | Evidence |
| --- | --- |
| New chronology regressions | Nine tests; named/ISO/relative dates, explicit UTC, naive UTC storage, DST, year change, invalid dates, midnight-spanning sources, request anchoring, new-turn resolution, database reopening, source withdrawal and deletion. |
| Actual reply integration | Public/sealed direct and batch assembly, ordinary packet on/off. Each batch reaches one provider invocation and one send with the retained original sources and correct date. Provider output and Discord transport are controlled fixtures. |
| Existing Moment lifecycle | All 127 Moment tests pass: meaning/contributions, topic association, reply continuity, episode extension/reopening, source invalidation and retained-source pruning. Existing passes are credited, not renamed as new live observations. |
| Adjacent memory/context paths | 246 focused tests pass, including the nine new tests, tier source lineage, adaptive aging, production-shaped formation, Context, Packet, synthesis, clock and cross-source recall. Total focused checks: 373. |

Database reopening proves persisted source recovery, not recovery of an in-flight
provider call. The pre-existing meaning worker deliberately does not replay an
interrupted provider attempt; a hard exit can leave `generating`. That explicit
recovery condition remains on C9 and is not claimed fixed here.

## Deployment and one private acceptance addition

Review and merge precede the normal owner VPS deployment. No production action
was performed for this package. Combine this check with the already pending
PR571 sealed acceptance batch; do not start another public rehearsal.

Use an existing, identifiable eligible conversation from a Pacific evening:
ask to return to that topic using its Pacific date, then ask what each participant
contributed and what remained unresolved. Keep any exact-words request tied to
original messages. If a later natural request uses yesterday, compare it to the
same original exchange; do not change the server clock to manufacture midnight.

Save the complete answer and its matching `!bnl debug last route` result. The
selected originals must belong to the requested occurrence; the assembled
evidence must preserve its date; attribution and uncertainty must match those
originals. Wrong sources fail selection, omitted dates fail assembly, and a
wrong interpretation of correctly supplied evidence fails semantic acceptance.

Natural public meaning, group attribution, association/reopening and later
aged recall remain pending until an actual eligible exchange supplies evidence.
Keep previously accepted privacy, deletion, retention and episode receipts.
The locked Journal → Relay → published-Ballad source work remains in the plan;
this package does not certify those consumers. Website item 3 remains paused.

Rollback: revert this commit and deploy normally. No data migration is required.
