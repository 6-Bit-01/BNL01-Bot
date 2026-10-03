# Ballad source attribution review

The version-7 private audition assigned a speaker's joking suggestion to the
person being addressed, then described that person performing an action.
Current-source checks passed because the original evidence had not changed.
They did not compare the generated claims with that evidence.

Version 9 preserves the approved writer and its independent source-fidelity
review before a generated or polished draft can be saved or returned to the
website. It reviews lyrics, title, Style, palette and liner notes against the
same complete eligible episode. It distinguishes speaker, addressee and actor;
preserves questions, negation, hypothetical speech and banter; and checks concrete
numbers and temporal/causal connections. It distinguishes recorded facts and
captured measurements from member reports, including the difference between
cumulative views and a simultaneous audience. Useful reported details can stay
with attribution or uncertainty. A person reporting another person's action is
not its actor; time, location and manner stay with the right action. Liner notes
distinguish documented inspiration from a song's imagined scenes. Obvious
fictional imagery, metaphor, supported paraphrase and musical montage remain
eligible. This is not a taste,
novelty, rhyme or genre gate, and it does not require exact quotations.

Each human chat row now repeats its public speaker name next to its source
person reference, so the writer need not resolve a distant directory entry to
distinguish an author from an inline mention. This changes only the Ballad
projection; original sources, identity records and memories are unchanged.

The reviewer receives original evidence and the actual draft, without the
songwriter persona, prior-song catalog or producer instructions. A complete,
valid `supported` verdict with no issues is required. Rejected, uncertain,
malformed, truncated, unavailable or budget-blocked reviews cannot deliver
unchecked copy. Complete negative or uncertain reviews with specific issues can
receive one correction using that actual feedback and the freshly revalidated
originals, followed by one new source review. The correction preserves the
composition and fixes only unsupported claims or their explanation; it may
qualify a report rather than remove good detail. Feedback remains untrusted data
and cannot override the originals. If the result is still unsupported, nothing
is saved as a song version. Malformed, truncated, unavailable and budget-blocked
reviews never start a correction. A failure preserves existing versions and
saves a replayable safe error receipt without rejected lyrics or review prose.
There is no provider retry, fallback or repeated correction loop.

All calls use existing Gemini accounting and limits. A request uses one writer
and one reviewer call, with at most one correction and one re-review: four
physical calls maximum. Each call separately passes the existing spending
controls; the extra pair is not guaranteed or exempt from limits. Review routes
retain the manual/background writer's spending lane and automatic-show protection, with a
4,096-token output bound and no provider retry/fallback. No configured spending
limit or reserve changes. The review adds input cost for the episode and draft
and adds latency; if its allowance is unavailable after writing, the command
fails rather than returning the unchecked candidate.

Sources are revalidated after writing and after every review, including a
negative review before its feedback can drive a correction. A corrected draft
is revalidated before its new review. A correction, withdrawal or privacy
change prevents saving the draft even when its review
passed. A passing receipt records the final source and draft digests and whether
the one correction was used in the existing immutable version document. Human
edits and restores do not inherit a stale
review certificate. Existing producer editing/publication authority is unchanged;
legacy drafts and manually edited content are not retroactively certified.

Offline tests exercise source-speaker preservation, reviewer isolation and
provider schemas, both budget lanes, rejection and unavailable-review handling,
receipt replay, saved-version preservation, edits/restores and source changes
during each review/correction boundary. They also cover a successful correction,
an unresolved correction, unavailable feedback, budget refusal, correction
provider-error privacy and zero-call
receipt replay. Mocked review verdicts prove enforcement, not model judgment.
Private model evaluation must show that the observed speaker/recipient and
joke/action failures are rejected while faithful paraphrase and creative imagery
remain accepted. A model review reduces risk; it is not an infallible factual
proof. Do not declare live semantic acceptance from code checks alone.

This is a bot-only change. No new production store, scheduler, factual authority,
website control contract or memory gate is introduced. Deployment remains a
separate decision after review. Rollback is a code revert and ordinary bot
restart; no data migration is required.
