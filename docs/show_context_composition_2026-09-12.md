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

## September 13: optimize the deployed revert

The deployment and revision status in this section supersedes the earlier deployment notes above. PR547 merged as `964e83b91dc019c786fa781a0883ae48fb2ab21b` and was subsequently reverted by PR549. The confirmed reverted baseline is `8ef3805d03fbab34d2827b1af165693ec25f352e`, with the PR546 source tree. The owner reported a good show answer after that revert and a quick ordinary greeting. The remaining work is performance optimization of that working baseline.

The post-revert timing capture placed the comprehensive show reply at approximately 76 seconds before generation, 26.5 seconds in generation and 108 seconds afterward. The greeting took about 9.3 seconds total. Both recorded one ordinary-packet provider call and zero corrective calls. These measurements locate substantial delay outside the provider, but do not assign all of it to one reader or prove that reverting PR547 alone resolves it.

The earlier comparison with PR539's fast conversational deployment found that per-item show reconstruction and synchronous final validation already existed there. PR543 added fuller timeline evidence, PR544 added preparation discovery with per-copy original-record lookups, and PR545–547 expanded composition. Their additional source work exposed those costs without establishing that earlier ordinary-chat timings were a complete-show benchmark.

PR548's first published candidate (`9c15b77446729833a2e26b46edf1dbf1439491f0`) combined this performance work with receipt-write, provider parsing and output-budget changes. That candidate is superseded by this narrower revision on the reverted baseline. The temporary development-workspace interruption during preparation did not change the VPS.

### Current runtime scope

- `bnl_tiktok_show_ledger.py` reads original conversation identities in bulk. Original private, edited and scan-limited records retain precedence over Journal copies. Saved Discord and TikTok sources still join the existing show on read, including records added after its archive was synchronized.
- `bnl_unified_intelligence_packet.py` rebuilds selected show versions together once per component packet's fresh validation snapshot. The result lasts only for that validation. The next validation reads current sources again and detects edits, deletions or privacy changes.
- `bnl01_bot.py` moves blocking show/read-model reads and final source validation to the existing thread-offload mechanism. Discord's event loop remains available while those reads run. Final interruption checks continue to prevent obsolete replies from sending.

The website queue, Broadcast Deck and archive already derive from the same QueueStore/session/show log. BNL's existing show projection links that source identity to original timestamped Discord and TikTok records. Current-queue questions and historical show questions continue through the established conversational context and memory owners. This revision requires no store migration, historical rewrite or website change. Provider allowance, parser, packet-receipt policy, source selection, routing and production flags retain the reverted baseline's behavior.

### Controlled comparison and verification

A fixture containing 8,000 additional original conversations and 4,000 retained Journal sources was read by both the exact reverted helper and the revised helper. All 12,007 returned records and the complete coverage report were identical. Across three runs per version, conversation-query count fell from 8,001 to 2 and median reader time fell from 1.3978 to 0.1153 seconds. This measures the source reader locally, not end-to-end VPS latency.

Regression coverage checks bounded original-identity reads, private-original precedence, one selected-show rebuild per validation, fresh invalidation after an original edit, stored-chat backfill without a live fetch or show resync, and event-loop progress through the actual batch reader/provider/send path. Existing tests retain current-queue lookup, requested historical dates, comparisons, direct and batch delivery, source identity and privacy behavior. Required `make check` passed: compilation and all 3,073 tests in 140.199 seconds on Python 3.12. The PR records the verified published tree.

The two existing historical-show retests remain the post-deployment comparison. Capture every reply chunk, each immediate `!bnl debug last route`, and the matching generation/send timestamps. Compare the before-provider and after-provider delays with the recorded post-revert baseline; retain the greeting as the ordinary-chat comparison. Deployment, measured VPS improvement and live answer completeness remain pending. Earlier scoped passes remain credited, and preparation claim accuracy, production source coverage, active-session awareness, dropped-correction and natural new-Moment recovery keep their existing acceptance status.
