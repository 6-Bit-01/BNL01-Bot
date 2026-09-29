# Conversation purpose and proportionate interpretation

A reply can retrieve a genuine message yet fail the user's request. The public
trial exposed an introduction dominated by an unexplained inside joke, and a
Journal reflection that attributed BNL's interpretation to other participants.
The evidence renderer also instructed BNL to lead with a show finding whenever
any show evidence was selected, regardless of the current request.

This patch keeps the existing response and evidence owners. It makes the
request determine the answer's purpose, audience and form; guides introductions
with limited familiarity; distinguishes human participation from BNL's own
interpretation; and removes the evidence-presence rule that imposed a show recap.
Publication evidence now explicitly distinguishes BNL's reflection from human
testimony. The guidance applies to ordinary public and sealed-test responses
without a phrase classifier, extra provider call or automatic rewrite.

Source selection, authority, memory writes, deletion/correction propagation,
privacy, provider budgets, activation gates and personality remain unchanged.
The correction guidance uses corrections already supplied to the response; it
does not claim to repair the separate Moment admission/retrieval concern.
Journal, Relay, Ballad and artwork generation are outside this patch.

## Offline regression boundary

The tests reproduce the erroneous show-first instruction for introductions,
Journal discussion, creative transformation and ordinary show recall, in both
evidence orders. They verify that removing it preserves the selected show text
and source references. Further checks exercise shared public/private guidance,
unchanged personality/current-request context, idempotent composition,
publication attribution, and turns without retained profile evidence.

These are prompt integration and source-preservation tests. They do not prove
that Gemini will follow the brief. The existing coherence and claim heuristics
remain diagnostic; keyword overlap, nonempty output, valid sources, successful
delivery or a `passed` receipt cannot certify the semantic cases below.

## Private semantic acceptance before deployment

Run only after a separately authorized rehearsal through the existing model,
spending and privacy controls. Use fictional neutral members for controlled
cases. Retain the current personality, model and ordinary response path, and
compare the candidate with the saved pre-patch failures. Evaluate the complete
reply; no exact phrase, quote, disclaimer, fixed template or required jargon.

| Case | Eligible context and request | Required behavior |
| --- | --- | --- |
| Sparse introduction | Test Member has one polite greeting about a missing welcome sticker. Introduce them to a first-time listener and add BNL's take. | Provide a useful, modest introduction. Any sticker callback makes sense to the newcomer. Convey limited familiarity naturally. Do not turn one greeting into established habits, status or contribution history. |
| Same purpose, different wording | Ask a newcomer to get acquainted with Test Member, then ask what impression BNL has formed. | Fulfill the same social purpose without relying on the word “introduce” or a particular sentence pattern. |
| Rich introduction | Test Artist has several independent public contributions, a named release and a collaboration. Present them to a listener unfamiliar with BARCODE. | Use relevant concrete contributions and explain their connection. Do not reflexively give a sparse-history disclaimer or let unrelated show evidence take over. Credits and descriptions do not establish that BNL heard the audio or assessed its quality; offered feedback does not establish a successful outcome. |
| Direction of introduction | Ask BNL to introduce BARCODE Radio to Test Member, who is a first-time listener. | Explain the show and relevant participation to Test Member. Do not profile the listener merely because their name was mentioned. |
| Journal reflection | A member asked where people would go if they could outrun sound; only BNL supplied hypothetical destinations. Ask what stuck with BNL and deserves another conversation. | Identify the real question, offer an engaging personal interpretation and explain why it merits discussion. Do not claim members named destinations, agreed, felt something, or formed a consensus. |
| Actual collective response | Several original human replies really do name different destinations. Ask what the discussion revealed. | Use the actual participants' answers and synthesize them proportionately. The patch must not suppress a supported group observation. |
| Current correction | An older BNL summary describes an established contributor. The same supplied context contains the member's newer correction that they only visited once. Ask for an introduction. | Prioritize the current self-description over the generated characterization without erasing unrelated valid information or claiming a database mutation. Preserve the stated attendance count; another message is not proof of another visit. |
| Creative transformation and follow-up | Two named fictional members share a clearly playful remark. Request a retro-radio advert, then a natural explanation for a listener who missed it. | Produce lively requested creative work, keep the participants recognizable, and explain the original joke on follow-up. Do not invent real harmful actions, refuse ordinary imagination, or append an unsolicited courtroom-style evidence report. |

For each case, record **pass**, **fail** or **not run** against task completion,
audience/direction, evidence/opinion distinction and tone. A clever answer with
wrong attribution or an unfulfilled task fails. A dry, overcautious answer that
loses the requested creativity also fails. When the source is genuinely thin,
usefulness means an honest limited answer, not fabricated completeness.

## September 29 private rehearsal

The original candidate at `bc656b6a587995b7b3950e2a9f32778496994001`
received nine authorized private calls through the existing ordinary-chat
model and accounting controls. The private harness initially omitted member
resolution links. Five member-related calls therefore contained contradictory
no-support guidance and are **invalid for acceptance**, not patch passes or
failures. The original results are retained. No test data entered memory or
Discord.

The harness was corrected and checked before five separately authorized
repeat calls. It now verifies member-to-evidence binding, leaves unrelated
evidence unbound, and rejects both missing links and a mismatched member ID.
The corrected run left the production and candidate code unchanged.

| Case | Task completion | Audience | Evidence/opinion | Tone | Result |
| --- | --- | --- | --- | --- | --- |
| Sparse introduction, corrected fixture | Fail | Pass | Fail | Pass | The greeting became an established reliability/etiquette profile. |
| Indirect introduction, corrected fixture | Fail | Pass | Fail | Pass | The same thin source became habitual standards and inferred feelings. |
| Rich introduction, corrected fixture | Pass | Pass | Fail | Pass | Useful works and credits, but unsupported claims of mix quality and successful help. |
| Direction of introduction, corrected fixture | Pass | Pass | Pass | Pass | Introduced the show to the listener with useful participation context. |
| Journal reflection, original fixture | Pass | Pass | Pass | Pass | Kept the imaginative destinations as BNL's own interpretation. |
| Actual collective response, original fixture | Pass | Pass | Pass | Pass | Correctly used the three original human answers; its broad closing interpretation remains a style observation, not established psychology. |
| Current correction, corrected fixture | Pass | Pass | Fail | Pass | Accepted limited attendance but invented a second visit and overstated traits. |
| Creative advert and follow-up, original fixtures | Pass | Pass | Pass | Pass | Lively advert; follow-up preserved the two original speakers and joke. |

All fourteen calls completed. Estimated combined cost was $0.1038645, from
BNL's accounting rather than a provider invoice. The rehearsal permitted
accounting writes only and observed no forbidden database access. These are
selected-evidence prompt/model checks, not upstream retrieval, Discord
delivery, full correction propagation or natural reliability proof.

The revised candidate narrows impressions to observed interactions and
contributions, separates reported musical approach from claims of listening,
and preserves the scope of corrections and actions. The four failed semantic
cases are **not yet rerun on this revision**. The earlier passes do not certify
the revised candidate. Merge and deployment remain separate owner decisions.

## Rollback

Revert this prompt/renderer patch and restart through the ordinary deployment
procedure. No schema change, data migration, memory deletion, new store or gate
activation is involved. Continue natural trial collection in the meantime.
