# Show context composition after deployment

The September 12 tests after merge `2b8335c` showed a broader named-track answer, but the timeline answer listed wheel results and removals while omitting other requested operations. The preparation answer reached linked sources but overstated some observations as successful technical checks. A service restart and component tests do not establish feature acceptance.

## Reproduced causes

The request listed session start, submissions, wheel spins, track starts and stops, removals, and show end. The interval resolver treated the category words `track` and `wheel` as a reason to exclude the whole-show timeline. A controlled fixture with all 13 operations therefore resolved to an unavailable wheel interval. The earlier production audit's aggregate has 258 operations, including session creation, 50 submissions, 20 playback-start events, 41 finishes and session archival. Those aggregate counts do not provide every original timestamp, but they do show why a wheel-only answer cannot establish that the other event categories are absent.

Preparation also returned early from the show reader. That prevented a request for preparation plus a timeline from composing both. Preparation and on-air dialogue used the same source-reference identity, which would collide when included together. Finally, the batch route treated website context as a separate owner even when finalized show evidence was already available through the shared packet. The single-message path already recognized that shared show owner; preparation recognition was incomplete there too.

## Change

- Event-category lists stay on the requested show chronology. An actual named-track, relative-track, elapsed-time or wheel-interval reference can still narrow the time range.
- The timeline keeps retained operations before sampling chat when the existing size bound is reached. It reports operation and message coverage separately. If the TikTok archive read fails, the available website chronology still renders, with chat explicitly unavailable.
- Combined requests compose preparation, linked existing Discord Moments, chronology and chat through the existing show owner. Preparation keeps its original timing and a separate source reference. A preparation-only question keeps its smaller scope.
- Finalized show evidence participates in the ordinary shared packet even when the website supplied context, including explicit comparisons between show dates. Relevant conversation, memory and other eligible packet sources remain available; source selection is not a winner-takes-all domain switch. The existing provider path makes one generation call.
- Preparation guidance requires original evidence for check results, decisions, removal reasons and unresolved tasks. Pre-show timing alone does not turn banter into a technical issue.

This change uses the existing stores and source revalidation. It does not admit TikTok chat to the general Moment engine, change production flags, or run a historical rewrite. A retained source is not promoted to truth merely because it joins the show context.

## Verification and next check

Focused tests cover the enumerated request through the native website reader, durable reader and packet renderer; bounded interval semantics; operation preservation in a busy room; operation availability during a chat-read failure; combined preparation, a genuine fixture Discord Moment, on-air chat and a separate conversation correction; unique source references and source-change invalidation; and the actual batch route's single generation call. The existing mixed Journal/current-queue route remains covered.

The PR records the required repository check and exact tested tree. These are controlled local tests, not a new live provider acceptance result.

After merge and deployment, repeat the failed timeline question and then ask a combined question: “Give me the September 11, 2026 BARCODE Radio show timeline: session start, submissions, wheel spins, track starts and stops, removals, and show end. Include the linked preparation and what TikTok and Discord chat discussed.” Capture each reply and its immediate route. The expected route uses the shared packet; the answer must cover the requested event categories from source records and keep preparation, operational facts and attributed conversation distinct. Missing source coverage must be stated precisely.

Preparation claim accuracy, complete production source-to-answer coverage and genuine active-session awareness remain open until observed. The separate dropped-correction and natural new-Moment recovery checks keep their existing status.

## Review follow-up before VPS deployment

PR545 merged as `d99f82b90d0ef1bfd906ce5568931b06694d9fd8`. Its complete tree matched the tested tree. Automated review posted two findings after the merge, and both reproduced through the website reader, durable reader and rendered shared packet:

- A category list introduced by "for track", "of song", or "for wheel" still selected a narrower interval. The relative-interval expression now requires an actual relative qualifier. Known named tracks, explicitly scoped unknown tracks, elapsed ranges and the existing relative interval cases retain their behavior.
- "Preparation and during/throughout/after the session" took preparation-only early returns. Session follow-ons now compose with the show evidence just as show and broadcast follow-ons do. Chat questions about the session also use the whole-show reader rather than an unresolved track interval. The website reader carries the same combined-request decision into its existing show-analysis path, so removing the early return actually retrieves the additional show evidence. A basic preparation-only request remains focused.

The added integration coverage failed on the merged version before these two conditions changed. It checks retained operations and original preparation/on-air messages through all three readers, including packet revalidation. This is a correction to the same show-context work, not evidence of live acceptance. PR544 remains the last deployment confirmed by a VPS receipt until a newer receipt is supplied.

## Retrieval hints compose evidence

6 Bit clarified that words must not switch BNL into exclusive source modes. An interval now provides a focused transcript while the same packet retains independent show operations and eligible community/preparation context. Native website and durable views also compose relevant parent-show records outside that conversation interval. Recorded session/broadcast boundaries retain space within the existing bounded operation selection, so a named track cannot crowd out the same show's recorded ending. These additional records do not expand the interval's transcript coverage or turn unrelated later chat into track reactions.

The combined-source regression also exposed the old 950-character source view and 700-character rendering limits cutting off an already selected archive event. The bounded first-party operation view now survives the packet and rendering boundaries, using the same source-owner budget accounting as the transcript and preparation views. Broad timelines still carry the full retained chronology; the independent operation context is a bounded selection, not an exhaustive timeline.

The same-PR review tests also preserve definite singular wheel/sponsor intervals, keep plural event-category lists on the requested show timeline, and prevent a later show/session noun from overriding a named-track reference. The integration acceptance case asks about track chat, linked preparation and the session ending together; all three source views must retain the original chat, preparation report and archive event. Hints scope evidence within the existing source owners; they do not change the conversation route or instruct BNL to recite a data dump.

## Continuation after PR546 merged

PR546 merged at `bbe83a4620366e1fae7e9eb1b9f5336939b54103`, with the tested full tree `b086c0d3a8c3baf42cb04ebf4e0611492eb41b0b`. Its final review identified one remaining issue: qualified references such as "during the BARCODE Radio show" and "during the entire TikTok live" could become an unresolved track window. The prior chat stopped with the regression test started. PR544 remains the last supplied VPS deployment receipt; the merge does not establish deployment.

The continuation consolidates the duplicated direct-show-reference checks used by interval recognition, timeline selection and preparation composition. Dates, source qualifiers and whole-show qualifiers are handled consistently. Only the direct show phrase is removed from the interval-reference scan; a separate named-track, unknown-track, operation or elapsed-time request remains available. A later show noun does not consume an earlier track request. Preparation followed by a qualified show reference composes the existing on-air evidence and parent-show records.

The affected integration cases fail against the merged PR546 runtime. The corrected focused suite passes 74 tests across interval, preparation/awareness and durable show-ledger coverage. Whole-show chat retains the existing bounded examples; a requested timeline keeps its retained chronology and transcript coverage. These tests do not establish complete production source coverage or removal of every keyword condition elsewhere in BNL. Existing source revalidation, privacy boundaries, one-provider generation, public activation, Journal/Relay schedules and general Moment admission are unchanged.

After review and merge, use the ordinary bot deployment commands:

```bash
cd /home/ubuntu/bnl01
git pull origin main
sudo systemctl restart bnl01
sudo systemctl status bnl01 --no-pager -l
git rev-parse HEAD
```

Capture the deployed SHA and service output. After the existing sync, run the two already prepared historical-show questions separately in `bnl-testing`, adding the qualified whole-show reference to the first:

1. "Give me the September 11, 2026 timeline for track starts and stops, submissions, wheel spins, removals, and show end during the entire BARCODE Radio show. Include the linked preparation and what TikTok and Discord chat discussed."
2. "For the September 11, 2026 BARCODE Radio show, what did TikTok chat say during Bludgeon💔Heart — self therapy, what preparation was linked to that show, and how did the session end?"

Capture each reply and immediately run `!bnl debug last route`. Compare the requested operation categories, attributed original chat, preparation claims and session ending with their original evidence. A shared-packet flag is insufficient for acceptance. Active-show awareness, the dropped correction, natural new-Moment evidence and all other open recovery items retain their prior status. Rollback is a revert of this bounded commit followed by the same ordinary deployment; this change introduces no schema or configuration migration.
