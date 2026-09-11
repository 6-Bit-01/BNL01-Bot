# Row 6/8 continuation: existing tier source retention — September 11 candidate

PR535 is merged at `8dbba2629cf32e6454b9580a0331c6a5b29b6a15`.
The latest supplied VPS deployment receipt remains PR534 at
`2c46ffe62dfcb88ec7ec2110b1007f44f2d9daa0`; neither PR535 nor this candidate
has a later VPS receipt in the evidence reviewed here. The authorized public
activation and all earlier passed recovery cases remain in effect.

The tier trace reproduced the remaining gap: meaningful ordinary conversation
could enter short-term memory, later consolidate into medium/long-term memory,
and still lose its original transcript during routine pruning. Its summary
survived, but the tier had no exact reference to its original message. A
Moment-only retention repair therefore did not cover the existing tier owners.

This candidate attaches exact conversation IDs to existing `memory_tiers` via
`memory_tier_conversation_sources`. It stores no extra memory text. Eligible
source messages, their initial short tier and source links commit together.
The existing consolidation owner carries those original links into medium and
long tiers, including the prior destination's links when summaries merge.
Parent retirement and genuine tier eviction release only that tier's references.
Existing tier quality rules, salience, capacities, prompts and public gates stay
with their current owners.

Routine pruning computes the existing recent-row allowance, then preserves
sources supporting either an eligible Moment or a retained tier. A later
ordinary prune can remove old records once no retained memory needs them.
The reference lookup and deletion share a transaction; concurrent memory
formation defers cleanup instead of deleting newly required evidence.

Legacy tier summaries have no trustworthy original-source mapping. The
migration does not infer links from text or pretend they were backfilled.
`source_lineage_complete` records this distinction and remains unresolved when
an old unlinked component merges with new linked memory. Routine cleanup is
conservatively deferred for that member/guild while retained tiers have
unresolved provenance. This can retain extra history until those tiers retire;
it does not affect other members' cleanup, prompt budgets or response behavior.
Previously deleted sources cannot be restored by this repair.

Explicit conversation clear/deletion, source correction and member forgetting
remain authoritative. Losing or changing a mapped source invalidates the whole
compressed tier that depends on it, removes its pins and scrubs its shadow
projection; removing an ID alone would leave stale text usable. Complete member
deletion continues through the existing owner. Tests cover these paths with
shadow writing disabled as well as enabled. Existing legacy unlinked summaries
cannot acquire exact-source invalidation retrospectively; full member deletion
still removes that member's tier rows through its established owner.

Validation: all 29 new focused regressions pass. `make check` passes all
2,970 tests in 120.881 seconds. Independent review has no remaining blockers.
The lifecycle regression includes 10,000 unrelated historical projections and
keeps source invalidation below its bounded SQLite work budget. This candidate
is not deployed and does not close shared-brain acceptance.

After review and merge, deploy through the established procedure:

```bash
cd /home/ubuntu/bnl01 || exit 1
git pull --ff-only origin main || exit 1
sudo systemctl restart bnl01
systemctl is-active bnl01
git rev-parse HEAD
```

Use a genuine eligible community conversation for live evidence. Retain its
exact original conversation ID and the corresponding tier/source links, then
follow the same IDs as normal consolidation and cleanup occur. Do not force
pruning, lower limits, invent a public Moment or perform an unsolicited forget.
Capture the content-free receipts from the running process once ordinary
activity produces them:

```bash
bnl_memory_pid=$(systemctl show bnl01 -p MainPID --value)
sudo journalctl -u bnl01 _PID="$bnl_memory_pid" --utc --no-pager -o cat \
  --since '-30 minutes' -n 40 \
  --grep='conversation_prune_(tier_sources_retained|moment_sources_retained|deferred_unresolved_tier_sources|deferred_for_memory_lifecycle)'
```

A tier-retention receipt proves a cleanup decision. An unresolved-tier receipt
proves conservative legacy deferral. Neither proves successful conversational
recall, participant attribution, cross-surface linking or aging through all
three tiers. Check those exact source/tier/Moment records and an actual selected
response packet separately as real activity occurs. A naturally quiet period
leaves the relevant acceptance case pending.

Ordinary associative topic recall remains the next separate Row 6 change after
this source-retention integration. Original Row 8 lifecycle evidence, remaining
provider/packet accounting, applicable live rollback evidence and final owner
acceptance remain open; the shared-brain recovery plan has not been reset or
marked complete.

# Row 6 continuation: Moment source retention — September 11 candidate

The confirmed bot deployment remains `2c46ffe62dfcb88ec7ec2110b1007f44f2d9daa0`
(PR534). Public activation remains in effect. This candidate continues original
Row 6 and its source-lifecycle dependency in Row 8; shared-brain acceptance is
still open. Earlier passed cases below remain accepted.

The source trace found that ordinary recent-transcript pruning called the same
exact-source purge used for explicit deletion. Once a supporting message fell
outside a member's recent-row allowance, that purge retracted its formed Moment,
scrubbed the summary and removed its participant contributions. This is a
demonstrated code path, not a claim that a particular live Moment was lost.

The bounded repair computes the original newest-N cutoff first, then preserves
overflow messages supporting a source-revalidated, finalized public Moment.
Its existing canonical record, source lifecycles, policy and route scope must
remain valid, and every supporting transcript must still exist. Human and BNL
model sources stay in their original conversation/ledger owners, including group
response participant mappings. No new store, tier copy or response controller
is introduced. Protection depends on stored validity, so disabling formation
flags does not itself erase already-valid history.

Explicit user/guild clear, complete forget, correction and source invalidation
retain their existing owners. Private/sealed, invalid and retracted Moments do
not acquire this public retention exemption. Ordinary overflow still trims.
Total transcript storage can now contain recent-N rows **plus supporting rows
for eligible durable Moments**; N remains the recent-context allowance, not a
hard total storage cap. Existing prompt read limits are unchanged. This repair
does not restore sources previously purged or implement experience tier aging.

Six focused regressions form actual source-backed Moments through existing
owners. They cover recall of a 90-day-old experience after reopening the DB,
original participant attribution, unrelated overflow, group model roots with
flags off, invalid-source exclusion, explicit user/guild clear and guild
isolation. Additional checks keep lookup work bounded in a guild with 2,000
irrelevant Moments and preserve a newly finalized Moment when a concurrent
writer changes the pruning snapshot. They expose the prior defects and pass
with this candidate. Local evidence is separate from deployment and live
acceptance.

Validation: all six focused regressions pass; `make check` passes all 2,941
tests in 112.891 seconds. Independent runtime review has no remaining blockers.

After review and merge, use the established pull/restart procedure below.
For live acceptance, use the next genuine community Moment and retain its
source/Moment/episode IDs. Let ordinary activity reach the existing pruning
cutoff; do not invoke pruning manually or seed public fixture messages. The
new content-free receipt can be captured once when that occurs:

```bash
bnl_memory_pid=$(systemctl show bnl01 -p MainPID --value)
sudo journalctl -u bnl01 _PID="$bnl_memory_pid" --utc --no-pager -o cat \
  --since '-30 minutes' -n 30 \
  --grep='conversation_prune_moment_sources_retained|conversation_prune_deferred_for_memory_lifecycle'
```

A retained-source count establishes a pruning decision only. Re-read the exact
Moment's original roots, contributions and episode links, and check any actual
recall packet/delivery separately. No receipt yet means the natural pruning
condition has not been evidenced; it is not a failed test or permission to
repeat synthetic conversations. Explicit deletion behavior is tested locally;
use a real member deletion only when actually requested.

The next separate Row 6 gap is ordinary associative topic recall: the general
public Moment reader can retrieve an older related experience without assigning
its participants to the current speaker, but the packet's episode entry path
currently requires explicit episode/continuation language. That connection
needs its own bounded change preserving historical-context versus same-event
meaning. Adaptive per-user memory tiers already operate independently; their
existence does not prove complete Moment/episode aging or cross-surface
experience formation. Remaining original live acceptance and owner closure
remain open.

# Public activation and integration health — September 10

PR533 is merged and confirmed deployed at
`37680139576c742dcedb96af2d3d0be089f6b0de`. The running process and subsequent
Discord diagnostics both report ordinary-chat public scope effective with its
prerequisites ready and no configuration conflicts. The earlier launch and
recovery sections below are historical checkpoints; VPS activation is complete.
Public activation still does not close the original live acceptance.

The 18:38–18:41 UTC health capture confirms website heartbeat delivery, a running
Relay task, retained conversation/tier/Moment/episode evidence, queue capability
on both sides, and retained website dossiers and Broadcast Archive entries.
Three prior Journal releases returned HTTP 200 at their normal scheduled times;
the next release after public activation remains pending its natural window.
The TikTok timer is enabled and active for September 12 at 01:50 UTC
(September 11, 18:50 Pacific); the collector's successful inactive state outside
its show window is expected. No show is started for this check.

Two reliability findings remain distinct:

- Website read-model refresh repeatedly times out between successful queue and
  show-ledger syncs. Its existing three-second HTTP timeout and twenty-second
  cache lifetime were not changed by PR533. These receipts establish intermittent
  refresh failure, not its upstream cause or a loss of retained evidence.
- At 18:40:43–18:40:44 UTC, Discord reports its heartbeat blocked for more than
  thirty seconds. The loop traceback points through Journal control polling to
  `_journal_control_flags_for_guild`, exclusion snapshot storage, and Journal
  schema initialization. That operation and a concurrent show-ledger read both
  raise `sqlite3.OperationalError: database is locked`. Discord resumes and a
  website read succeeds shortly afterward. The trace does not identify the
  connection holding the database lock.

The bounded follow-up keeps Journal database waits off Discord's event loop
at all seven async call sites. A real SQLite contention regression fails on the
prior code and passes after offloading: a loop callback can release the test
writer lock while Journal work waits, and confirmed controls/exclusions still
persist. It covers scheduled and manual entry points and a control-plane outage.

The health diagnostic also had a correlated receipt-count query that rescanned
guild receipts once per ledger entry. Local fixtures on the existing schema
showed roughly 16/64/144 million SQLite VM steps at 2,000/4,000/6,000 entries.
Grouping successful receipt keys once reduced those counts to approximately
110,000/240,000/350,000 with the same result. The query-only change preserves
guild, outcome, duplicate and null/empty-ID semantics. A VM-step-budget regression
covers the complete ledger diagnostic without changing data or schema. This
establishes the query's cost; it does not establish the live lock holder.

The repair preserves the existing exclusion snapshot, publication controls,
schema migrations and release fence. Website/provider timeout changes, new
memory owners and new public fixtures are outside this repair. Subsequent live
evidence must distinguish heartbeat responsiveness, background sync completion
and the still-open website refresh reliability issue.

The shadow report's `blocked_live_authority_detected` label reflects its
pre-cutover condition detecting the now-authorized ordinary-chat switch; it
does not disable generation. Historical invariant/provider/coherence exceptions
remain recorded and are not newly passed. The capture's current-runtime
assessment count is zero, so it adds no post-launch conversational acceptance.

Repair validation: all 24 focused Journal/diagnostic tests pass, and `make check`
passes all 2,935 tests in 114.026 seconds. Independent review found no blocking
issue. This is a tested candidate; it does not establish a VPS repair yet.

After merging this repair, use the normal deployment:

```bash
cd /home/ubuntu/bnl01 || exit 1
git pull --ff-only origin main || exit 1
sudo systemctl restart bnl01
systemctl is-active bnl01
git rev-parse HEAD
```

For the focused live check, run `/bnl_memory_check` once as 6 Bit in
`bnl-testing`, allow the next two normal queue/show sync cycles, then capture:

```bash
bnl_health_pid=$(systemctl show bnl01 -p MainPID --value)
sudo journalctl -u bnl01 _PID="$bnl_health_pid" --utc --no-pager -o cat \
  --since '-10 minutes' -n 80 \
  --grep='heartbeat blocked|journal_memory_exclusion_snapshot_.*failed|bnl_read_model_(fetch_|cache_|invalid_shape)|queue_artist_memory_sync|barcode_show_episode_ledger_sync'
```

Evaluate the new process only: the diagnostic should finish without a Journal
SQLite wait blocking the Discord heartbeat, and a successful website fetch
should lead to a completed show-ledger sync. Retain exact exceptions if either
fails. Unchanged-source sync is normal. A website timeout remains an open
refresh issue, not evidence this repair changed provider policy. No forced
Journal/Relay publication, show launch, public fixture or database lock is part
of this live check. Keep the previously accepted recovery results closed.

# Public shared-brain launch continuation — September 10

6 Bit has now authorized launching the established shared-brain upgrade for
public use and completing the remaining Moment and other public-dependent
acceptance through real community activity. This supersedes the earlier
restriction against public rollout for the capabilities needed by this launch.
It is not a declaration that the unfinished live acceptance already passes.

The deployed baseline is PR532 merge
`175e6f02b975e4c6b9e322000a058ff601505591`, tree
`1a8f7807ba480c44be8982d0500a9e34551b58ed`.
Its repaired reply-to-Moment formation passed in `bnl-testing`: source rows
8430–8432 belong to finalized Moment
`mom_2493da7796b6542f63133d5a3b8b0d8a`, with the retained reply edge and
initial episode `mep_ae88606c845ef88270b7787158e73c2f`.
Preserve that pass and the earlier screenshot, original-quote, immediate
detour/resume and valid canary off/on/restart rollback evidence.

The concrete public launch gap is in the existing ordinary shared-brain
response owner's scope: it accepts at most eight named users and four named
channels. Its public channel-policy support alone does not make it a community
rollout. The launch change adds an explicit public mode to this same owner,
scoped to one guild and the existing public-home/public-context classifications.
Existing private/sealed scope remains independently restricted; the primary
kill switch and source/privacy checks remain in place. No second response
owner, memory collector, store, provider policy, or memory schema is introduced.

Validation: all 70 focused ordinary-packet tests pass; the required repository
check passes all 2,928 tests in 114.613 seconds. Independent review found no
blocking issue. The tracked activation configuration is public-effective in
the local configuration check; VPS activation remains an operator action.

Use [the public launch runbook](shared_brain_public_launch_2026-09-10.md)
for activation, effective-configuration verification and rollback. Keep the
packet's six observation/assessment prerequisites enabled. Keep the alternate
Memory Governance, Relationship v2 and Active Engagement live authorities off:
those switches conflict with this packet path. Journal, Relay, TikTok, site,
Ambient and queue retain their existing operating controls and schedules.

Moments remain source-linked experiences involving people, subject, situation,
place and continuation. Processing windows do not define the intended lifetime
of an experience. Recognizing or recalling an earlier experience need not create
a new Moment or add new people to old history. Relationship observation and
legacy conversational memory continue through their current owners.
This public scope change does not itself implement broader associative episode
retrieval, tier progression, cross-surface formation or new Relationship v2
tone control; those capabilities must be assessed honestly against existing
implementation and actual observations.

Remaining acceptance follows the original recovery plan, with 6 Bit's updated
natural-event method:

- Trace a genuine interaction through retained sources, people/context,
  qualified or appropriately unqualified Moment, episode/link records, and
  applicable delivery evidence.
- Use later genuine continuation/recurrence, corrections and source changes to
  assess the outstanding lifecycle and attribution behavior. Source retirement
  and privacy obligations remain; no destructive test is implied.
- Let genuine multi-person activity provide the remaining group evidence and
  natural show/publication windows provide TikTok, Journal, Relay, site and
  Ambient evidence. One event may support several capabilities without
  collapsing their pass/fail records.
- Keep previously passed work closed. Mark capabilities awaiting a real event
  as pending, not failed. Public enablement is not owner acceptance of all eleven
  original capabilities.

The response-time issue remains open under existing Row9. For the September10
15:42 exchange, the first Gemini request waited119.269 seconds before HTTP504
and the recovery request waited112.074 seconds before HTTP200. Accounting
completed within23 milliseconds after HTTP200; delivery followed1.833 seconds
after HTTP200. The request path dominates this stall; its upstream cause is
unproven. This launch does not change provider timeouts/retries/models or claim
to repair that delay.

The historical checkpoints below preserve the original investigation; their
instructions to repeat now-passed scripted exchanges are superseded by the
verified passes and natural-event continuation above.

# September 10 recovery completion checkpoint

## Current continuation: preserve Moments across conversational response routes

PR531 is confirmed deployed at `51cdeeb089b50936c7360cb24739a11fa733a43c`,
with the service restarted at 09:28:14 UTC under PID 1601101. The next live
exchange in `bnl-testing` retained all four conversation rows, 8424 through
8427, and their active ledger entries. Rows 8424/8425 used `normal_chat`;
rows 8426/8427 used `direct_payload_task`. The directly addressed follow-up
arrived at 09:30:19, nine seconds after the retained BNL answer.

The follow-up wording supplied for the test included "about each", which the
existing payload detector recognized as a list request. The source trace shows
no `reply_to` edge, two rejected Moments with one human and one model entry
each, and no canonical entry or episode. The first window was rejected when
the route changed, well inside both time bounds. An isolated reproduction with
the same first three texts and timestamps reproduces the split; changing only
the follow-up route to `normal_chat` yields one qualified Moment and episode.

The debug receipt's `model_save_skipped` was captured before the planned send
path's actual persistence. The later log reports `saved=1` with
`sealed_test_no_normal_durable_memory`, consistent with the stored rows.
The receipt's memory-tier flag is not proof that conversation storage failed.
BNL's new claims about a playback signal, track registration, and recording the
clarification remain unsupported by the supplied evidence; they are not passes
for answer grounding or a verified archive edit.

This repair reuses Context v2's existing continuity route contract when
resolving a retained reply, selecting and extending its Moment, and revalidating
Moment members and human contributions. Source rows keep their original route
labels, and the referenced raw answer is checked against its own retained
route. Guild, channel, policy, visibility, source integrity, lifecycle, ordering,
qualification, explicit topic changes, and time limits remain enforced.
The change does not alter payload classification, public source-authority
contracts, model-output authority, runtime gates, or historical rejected rows.

Validation: all 92 focused Moment tests pass, including the exact live route
transition and its reverse, database reopen, contribution rendering, mixed-route
member lineage, and preserved source/scope checks. The required `make check`
passes all 2,922 tests in 112.598 seconds. Independent review found no blocker.

Scope remains the affected Moment formation and member-revalidation boundary,
including its initial canonical entry and episode link. Existing episode
selection/grouping across later response-route changes is not changed by this
repair. Later continuation/recurrence, correction/retirement, and restart
delivery still require their existing acceptance checks. Preserve the passed
screenshot and original-quote checks.

After merge and verified deployment, repeat the same two-message exchange with
the actual Discord Reply action on the fresh BNL answer. Keep the follow-up
inside the existing two-minute bound; its `direct_payload_task` route is part
of this regression. After normal expiry, inspect the new source rows, retained
reply edge, `window_reply_bound` receipt, qualified Moment, canonical entry, and
initial episode link through targeted read-only evidence.

## Earlier continuation: preserve directly addressed correction continuity

PR530 is confirmed deployed at `b9abeb0feabf95e4e2f5478d5c92977da5db115f`,
with BNL online under PID 1600886 after the 07:41:19 UTC restart. The following
live recap answer authenticates all three original comment/speaker pairs. The
subsequent timing answer retracts the unsupported playback association and
after-resolution wheel claim. Carry these substantive passes forward along
with the earlier image, date-selection and immediate detour-continuity passes.
Playback remains unconfirmed, not disproven. The timing answer's statement that
timeline records were updated has no supporting write receipt; it establishes
only a correction expressed in conversation.

The next read-only checks located the original 02:40–02:48 UTC conversation.
User rows 8394, 8396, 8398 and 8400 all have ledger entries and Moment membership,
but each belongs to a separate rejected window with no canonical entry or
episode. All four report `low_signal_or_insufficient_continuity`. The first
window's `topic_coherence_mismatch` diagnostic occurs when row 8396 arrives at
02:42:36, 71 seconds after BNL's preceding reply. This is within the existing
two-minute inactivity bound. The same turn's conversation-context receipt
resolves `exact_discord_reply` with `discord_reply_source`.

The failure is the handoff from trusted Discord addressing to persistence:
the resolved reply row was available before saving, but the ledger adapter did
not retain it. The Moment observer therefore used lexical overlap and split
the directly addressed correction. A disposable-database reproduction matches
the rejected fragments. Keeping the first two human turns and BNL replies in
one window satisfies the existing qualification rule without changing it.

This bounded repair carries that opaque reply row through the three existing
normal conversation save paths. The ledger records a structural `reply_to`
edge only to one retained BNL source with matching guild, channel, policy,
route and visibility, usable lifecycle, earlier ordering and matching retained
text projection. The Moment observer revalidates the reference and existing
member scope before preferring its still-open window over lexical overlap.
The existing explicit-new-topic signal is shared with conversation context;
it still separates a deliberate detour. Inactivity, maximum window length,
qualification, source authority and runtime gates remain unchanged.

`reply_to` is not factual ancestry, a correction, supersession or permission
to publish model output. The change adds no model request, response judge or
parallel store. It does not reconstruct the historical rejected windows or
invent their lost reply edges. New regressions cover the observed correction
sequence, normal expiry into an episode, the checksum detour, raw-source
withdrawal/change, ambiguous and incompatible references, time bounds, database
reopening, actual save-to-observer delivery and all three normal ingress paths.

Validation: all 86 focused Moment tests passed. The required `make check`
passed all 2,916 tests in 115.150 seconds. Independent review found no blocking
issue. These local results establish formation and persistence behavior;
they do not certify live provider responses or episode delivery after restart.

After review, the required repository checks, merge and verified deployment,
validate this affected formation boundary once using the continuing discussion
and a directly addressed follow-up. Locate its new source, reply edge, qualified
Moment and episode through targeted read-only evidence. Episode formation alone
does not close the outstanding continuation/recurrence, correction/retirement
or restart-delivery acceptance. Continue those remaining parts of the finite
plan below; do not repeat the passed screenshot or original-quote checks.

## Earlier continuation: compare pictured wording with original comments

PR529 is confirmed deployed at `217cae45f0c625c8e626082b35bb66663543ec78`,
with BNL online under PID 1600498 after the 06:16:07 UTC restart. The 06:22–06:24
live run closes the undated screenshot check and confirms dated image selection.
The first image correctly identifies the pictured retraction and reports that
its unresolved show scope was not searched. The second image's query receipt
reports `show_dates=2026-09-04 literal_count=3`; the source receipt identifies
`session_mtno0ply_2g09o` both before generation and on refresh. Neither answer
contains the earlier optional glitch insert. These remain carried passes.

The dated answer still falsely rejects two authentic comments. 6 Bit's next
read-only lookup checked all 1,437 eligible September 4 originals with complete
coverage, zero skipped rows and the same source revision
`88f048d0d2f6cc081b19cf8093d4652afb0f4c44921c2f159659f1f229b9b97a`.
It returned one original for each fragment:

| Fragment | Source speaker | Timestamp UTC on September 5 |
| --- | --- | --- |
| `bombed` | Corporate Satan (@corporate_satan) | 02:14:59.103 |
| `ducking` | SHADOWSPIT (@deadite_ash) | 02:11:19.136 |
| `threw a tantrum` | Oreaganomics (@oreaganomics) | 03:18:19.164 |

Thus all three comment/speaker pairs exist in the retained originals. This does
not authenticate the recap's playback or wheel-event chronology. The ended
turn's extracted strings were transient and are not recoverable from its
count-only receipt, so the exact live failure mechanism remains unproven.
The pictured wording includes punctuation absent from the originals. A local
reproduction confirms that punctuation/capitalization differences produce an
exact miss without exposing the related original through that lookup.

This change extends the existing original reader to return separate formatting
candidates alongside unchanged character-exact results. It compares contiguous
whole-word sequences without case, punctuation or whitespace differences,
preserves the candidate's actual original text and author, and never labels it
verbatim or semantically equivalent. All exact results receive the existing
eight-record display allowance first; candidates share the remainder. Both
use the same eligible rows, coverage, source revision and refresh ownership.
The existing source receipt now includes per-query match/candidate counts and
displayed event IDs, without logging screenshot text. This introduces no
provider call, source store, response judge, schema or runtime-gate change.

Validation: six new regressions cover formatting differences, whole-word
boundaries, source eligibility, exact-result display priority, direct/batch
delivery and withdrawal after an unsynced source change. All 2,903 tests passed
the required `make check` in 114.147 seconds. Independent review found no
blocking issue. Provider fixtures establish source delivery, not live answer
accuracy.

After review, the required test gate, merge and verified deployment, repeat only
the outstanding dated recap screenshot. Require the September 4 original rows
to reach the answer and distinguish the three authentic comments from wording
differences and unsupported surrounding chronology. Do not rerun the undated
screenshot, image transport, date-selection or earlier original-lookup checks.
The finite recovery plan below remains unchanged.

## Current continuation: screenshot queries reach the original reader

PR528 is confirmed deployed at `a845555ae5b1c67b41f330e853d11b4fa5658b98`,
with the bot online under PID 1600365 after the 05:37:17 UTC restart. The two
current images loaded and the replies now distinguish the first screenshot's
retraction from the second screenshot's September 4 recap. Image transport and
current-image identification pass this live run:

| Message ID | Attachment ID | Loaded at UTC | Effective MIME | Actual bytes |
| --- | --- | --- | --- | ---: |
| 1547481821238001696 | 1547481821141270600 | 05:40:27.535 | image/png | 78,175 |
| 1547482094593122364 | 1547482094278545509 | 05:41:28.242 | image/png | 121,811 |

Original-record verification remains open. The first answer again asserted
CipherDot's absence without an author-absence search. The second identified the
pictured September 4 claims, then said only August 28 records were loaded. The
source receipts report one show but omit its identity; the model's statement
alone cannot establish which source root was actually selected. The read-model
timeouts recovered before the second answer and do not establish the cause.

The code selects show evidence from typed text and media metadata before vision
generation. Its post-provider refresh pins those selected roots. Dates and
quoted text discovered in the image therefore cannot inform that source read.
This continuation adds a transient query preparation step through the existing
native image/provider owner, then uses the existing original reader before the
final answer. It is not a response judge or a new source store.

Current explicit human dates retain precedence. Otherwise dates visible in an
image select that image's show scope; quoted text from the same image becomes
literal search targets. An undated or unreadable image remains unresolved and
cannot borrow another image's date or a background episode. The original reader
continues to own matches, speakers, coverage, source revisions and eligibility.
Image text never becomes an authored original excerpt. Conflicting unbound
website historical sections are omitted while independent sections remain.

Direct and batched requests share this preparation. The admitted image reference
caches the transient query for coalescing/retry; source freshness replays those
cues against pinned roots without another extraction or image download. Existing
Gemini budget/accounting applies to the additional vision request. This is one
additional logical provider call for an eligible current-image show/record
request; ordinary image descriptions keep their existing path. Optional persona
rewrites are disabled for these source-verification answers.

Validation: the 15 new focused tests cover initial direct/batch delivery,
per-image date/literal scope, current-human precedence, failed extraction,
concurrent reuse, original-source withdrawal and relative-date rollover.
Provider and image fixtures establish plumbing, not live vision accuracy.
The final `make check` passed all 2,897 tests in 111.524 seconds; independent
review found no remaining material issue. Existing image transport and typed
original-lookup regressions also remain green.

After merge and verified deployment, continue only the outstanding screenshot
verification checks. Require a current-image query receipt and identified show
selection, then evaluate the answer against the actual original-reader result.
A screenshot without a resolvable date may identify the pictured claim and state
the missing scope; it must not claim a completed original search. A dated recap
must reach that date's records and distinguish literal matches, unsupported
attributions and chronology claims. The already-passed image transport and
original-record lookup checks remain carried passes. The finite recovery plan
below is unchanged.

## Earlier continuation: original attachment format and size

PR527 is confirmed deployed at `e81166f622dcbd1786c48d0da630db81738e6b54`,
with the bot online under PID 1600121 after the 05:06:05 UTC restart. Both
screenshot inputs reached the loader, but their receipts at 05:06:56 and
05:07:50 report `mime_type=image/webp status=invalid_image_data`. Neither
current screenshot supplied pixels to the provider; media-context flags alone
did not establish image delivery. The earlier WebP fixture proved delivery of
actual WebP bytes, not the format returned for these original attachments.

A subsequent read-only check used the installed Discord SDK's same
`Attachment.read(use_cached=False)` method on the two exact attachments:

| Attachment ID | Declared MIME | Declared bytes | Downloaded signature | Downloaded bytes |
| --- | --- | ---: | --- | ---: |
| 1547473357744640080 | image/webp | 53,526 | PNG | 83,413 |
| 1547473631687479336 | image/webp | 92,426 | PNG | 131,797 |

The original downloads differ from the attachment metadata in both format
and size. The loader required the declared format's signature and separately
limited the returned bytes to the declared size. Fixing only the signature
would therefore leave both originals rejected by the size check.

This continuation recognizes the existing supported PNG/JPEG/WebP signatures
from the downloaded bytes and supplies that MIME to the existing native image
part. It checks actual bytes against the fixed per-image and cumulative limits,
without using the reported attachment size as an exact original-file ceiling.
Conservative metadata preflight, scope, count, deadline, read-once ownership,
and the existing provider path remain in place. The existing loading receipt
includes declared and effective MIME plus declared and actual byte counts.
No URLs, file contents, or credentials are logged.

Regression coverage includes distinct PNG originals under smaller WebP
metadata through actual ingress and provider composition, followed by a failed
current read. Fixed actual-byte limits and rejection of unsupported payloads
remain covered. Fixture replies establish transport and current-image identity;
they do not establish live interpretation or original-chat verification.

After merge and verified deployment, repeat only the same two screenshot
checks. Require `status=loaded` and an effective MIME matching the downloaded
bytes (`image/png` for the originals above), then assess whether the answers
distinguish the first screenshot's retraction
from the second screenshot's different recap. The original-record lookup pass
and remaining finite acceptance below are unchanged.

## Earlier continuation: WebP screenshot delivery

PR526 is confirmed deployed at `8ec1000e550f23f9ff30b1cdd5f6056fc3afe2cf`,
with the bot online under PID 1599742. The original-record diagnostic completed
the September 4 retained eligible window: 1,437 original rows read, no skipped
rows, and zero contiguous literal matches for the requested visualizer quote.
That scoped lookup check passes. It establishes neither author absence nor
the origin of the unsupported wording.

The two screenshot turns at 04:28 and 04:29 UTC failed current-image grounding.
The first screenshot shows an attribution challenge and BNL's retraction; the
second shows the Corporate Satan, Oreaganomics and SHADOWSPIT recap. BNL answered
both as though they showed the CipherDot attribution. Media metadata was
included, but neither turn emitted the existing image-loading receipt.

Read-only inspection of retained user rows 8407 and 8409 identified the exact
input shape: `image attachment (filename=image.png; type=image/webp)` for both.
The existing capture owner accepted only PNG/JPEG MIME types, so it excluded
both WebP attachments before download and native provider composition. The
filename was not a reliable indication of the attachment's actual format.

This bounded repair adds WebP to that same capture/load/provider path, checks
its RIFF/WebP header, and includes the MIME type in the existing loading receipt.
It retains the existing size, scope, request-budget and read-once boundaries.
Regression coverage uses the actual ingress and native provider adapter for
successive distinct WebP images with the same PNG filename, followed by a failed
current read; prior pixels must never substitute for the current attachment.
Fixture provider replies establish transport and attribution, not interpretation.

After merge and verified deployment, repeat only these two screenshot checks
with their route and image-loading receipts. Acceptance requires reading each
current image accurately and separating prior BNL claims from original-record
evidence. The passed original-record lookup and other carried passes remain
closed; the rest of the original finite acceptance below remains in force.

## Earlier continuation: original-record lookup

PR525 is confirmed deployed at `1c422d727e8a496c505efadad5416e4c11ecf0c6`,
with the bot online under PID 1598361. The 03:19–03:20 UTC verification turn
retracted the unsupported attribution but again claimed zero author records
and exact-quote absence across the finalized logs. Its source receipt showed
one selected show, not a performed author or literal-record search. This
affected verification capability remains open; retraction alone is insufficient.

That turn used one successful physical provider request, with no retry or
optional style rewrite shown. Logged handling to send was 51.314 seconds,
including 30.105 seconds of provider generation. The application's usage-priced
estimate was $0.01128750; its larger reservation was not spend. Website reads
timed out during the interval, then recovered at 03:21:23 UTC. Subsequent show
synchronization reported two unchanged finalized shows and zero projection
errors. These observations support delivery and recovery, not exhaustive quote
verification or a processing explanation for the earlier attribution.

6 Bit authorized one bounded follow-up: give the existing finalized-show reader
an actual literal lookup through its original public TikTok source owner.
Current human double-quoted text supplies the lookup text. Existing show/date
selection and pinned refresh scopes remain authoritative. Each selected show
reports its UTC window, current eligible rows checked, exact matching original
records and speakers, and complete, partial or unavailable coverage. A completed
zero-match establishes absence only within those retained eligible windows.
It does not establish author absence, whole-platform absence, or the origin of
earlier BNL wording. A matching source supplies its actual author regardless of
the author claimed in the request.

Lookup results enter the existing rendered source context and authored-excerpt
basis. Existing post-provider source refresh repeats that read; it invalidates
an old result when original storage changes or a new matching record arrives,
even before the cached show ledger synchronizes. Cached human projections that
cannot be confirmed against current originals are omitted for that lookup turn;
independent operational chronology and other valid shows retain their owners.
Normal source records remain immutable. Regression tests use governed member
deletion and source reinsertion in disposable databases; the runtime introduces no mutation,
schema change, extra response judge, provider call, retry owner or gate change.

After local review and the required gate, publish one PR. Following its merge
and verified deployment, run the affected source-verification check once and
collect its route receipt. Continue the remaining original acceptance below;
do not restart the comparison trio, prior audits or group recruitment. Local
fixed-provider tests establish source delivery and invalidation, not the live
model's factual wording. Neither this candidate nor its deployment closes the
remaining durable episode, screenshot, group or natural-publication cases.

## Earlier checkpoint retained for continuity

This continues the locked six-stage recovery plan and original eleven
acceptance capabilities. It records the next actions after PR524 and the
02:40–02:48 UTC live receipt; it does not replace that plan or reopen passed
cases. PR524 is confirmed deployed at
`7543ef9673d1c70a7e081680d33e7b368445220a`, with the bot online under PID 1597915.

## What the latest session established

- The earlier quote/date checks remain accepted: August 28 has 1,406 eligible
  TikTok comments from 31 participants; September 4 has 1,437 from 29. Original
  record checks authenticated the supplied comment/speaker pairs. WittyF0x's
  playback association has a recorded play-start before the comment.
- The bot retained the attribution dispute and the correction to its processing
  explanation through the checksum detour. This proves immediate conversational
  continuity; it does not prove durable episode recurrence or restart survival.
- The CipherDot answer claimed whole-ledger absence, although the receipt shows
  a selected view of one show. That receipt does not establish an exhaustive
  author/quote search. Absence within a checked archive would establish no match
  within its coverage, not the origin of wording in an earlier BNL recap.
- The four turns made seven physical provider requests: five successes and two
  HTTP 503 failures. The application's usage-priced estimate totals $0.036927.
  Budget reservations are not spend; failed-call billing is not independently
  verified by the application's zero-usage entries.
- The checksum turn took 41.946 seconds from first logged handling to successful
  send. Packet generation failed, normal generation recovered an answer, then
  the optional glitch rewrite failed and retried. The rewrite added 20.057 seconds
  and $0.006390 after the successful base answer. Final packet counters describe
  that receipt, not the entire turn's physical requests.

## This bounded candidate

The existing show renderer now labels aggregate totals and selected records at
the beginning of the source block. It identifies the participant list as partial
and states that the reader supplies no exhaustive author/quote absence result.
Prior BNL replies establish what BNL wrote, not audience authorship or a source
search. Selectors, original records, dates, totals and source refresh are intact.

The existing packet generation helper passes its tracked provider attempt count
back to its own caller. If an attempted packet generation needs normal answer
recovery, all four existing direct/batch callers disable the existing optional
style-rewrite step for that recovery. Normal answer recovery still runs. An
unused packet or a local budget denial with no provider attempt retains normal
style behavior. The decision uses per-call data rather than shared last-call
state.

Regression tests exercise actual normal and packet prompt assembly, real SQLite
retention versus selected excerpts, and all four recovery callers with both
optional style branches forced on. They prove source delivery and request
behavior, not live model factuality. This candidate creates no new response
judge, source search, memory owner, retry policy, schema or deployment gate.

## Finite path to completion

1. **Candidate and deployment.** Finish independent review and the required
   repository gate; publish one reviewed PR. After 6 Bit merges it, verify the
   merge tree and use the established pull/restart procedure. Keep the current
   authorized configuration. A GitHub merge alone is not a VPS deployment.
2. **Finish the solo portion.** Check the affected source-scope answer once on
   the deployed candidate, using the same unsupported earlier attribution and
   requesting only what supplied original records establish. Accept a supported
   record match or an explicit inability to verify; do not accept an invented
   exhaustive search, processing cause or wording origin. Carry forward the
   successful quote checks and immediate detour/resume. Complete the outstanding
   screenshot correction and meaningful episode continuation/recurrence from
   the original session. Use targeted, read-only storage and prompt/delivery
   evidence for the exact episode, including correction/retirement and any
   still-required restart proof; do not substitute global health totals.
3. **One remaining group segment.** Coordinate the remaining original cases
   with the second participant once, including the retained LostMarbles pants
   question and 6 Bit's Leo Rose correction where still unaccepted. Preserve
   valid subject separation and neutral-user evidence. Gather all physical
   attempt and delivery timings for these same turns; no separate speed session.
4. **Close the original acceptance.** Observe pending TikTok/publication/Ambient
   consumers in their natural operating windows. Reconcile their existing
   records with the original eleven capabilities, preserving valid historical
   canary off/on/restart-rollback evidence. Close with explicit owner acceptance
   or a specifically bounded continuation naming the remaining capability and
   its evidence. A natural window that has not occurred remains scheduled work,
   not a reason for another general audit or group rehearsal.

Only a demonstrated failed capability warrants another scoped repair. Local
fixtures cannot certify live provider answers, Discord coordination or scheduled
publication behavior. No production purge/replay, synthetic public memory seed,
blanket gate activation or repeated recruitment is part of this path.
