# BNL songwriting standards and live continuation repair

## Purpose and current acceptance

This follows merged #554 (`9de5e56fb559269a733eb995826b535de61a6097`).
6 Bit requested short-to-medium Style copy, multiple deliberate style blends,
specific lyrics, researched failure patterns and constructive creative pushback.
The new live receipts also showed an incomplete feedback answer and a short
override that lost identifiable rehearsal facts. Those are not accepted passes.

The pasted feedback retained the named rehearsal and B2/B3 credits but ended
mid-verse. Its receipt used website context. The following eight-line/2020/no-Style
request returned generic lyrics without the credits; its receipt switched to the
ordinary single-packet path. No Style section and clean characters are retained
successes. Eight separate rendered lyric lines, useful 2020 treatment, full-session
coverage, playback interpretation and creative quality still require live judgment.
The subsequent VPS receipt confirms checkout #554, an active process with PID
1687136, and restart at 2026-09-17 01:06:24 UTC, before these tests. A bounded
journal read for 01:06–01:13 UTC timed out after ten seconds without events.
That establishes deployment, but supplies no provider payload or cutoff diagnosis.

## Changes

- The existing creative protocol targets 250–400 characters of Style, with a
  500-character ceiling excluding the heading. It calls for 2–4 contrasting
  genres/styles with distinct jobs, preserving the requested leading genre.
  The default era remains 1970–2010; explicit era, lyric length and no-Style
  requests still work. These numeric limits are BNL house policy, not Suno limits.
- Production copy puts the blend first, then the essential groove, instrument and
  vocal roles and one useful dynamic change. Existing meaning-led rhyme, natural
  stress, concrete imagery and anti-cliche guidance remain. Simple hooks and
  deliberate repetition are welcome; padding and rhyme-driven nonsense are not.
- When asked for a huge inventory, conflicting directions or generic filler, BNL
  should briefly explain the concrete tradeoff and immediately deliver a stronger
  version of the person's idea. This is constructive judgment, not an insult,
  permission loop, blanket refusal or claim that musical taste is objective.
- The common visible-response boundary bounds explicitly headed Suno Style copy
  after generation. It first compacts whitespace, then keeps complete clauses or
  words within 500 characters. It leaves lyrics, separate Exclude/Notes fields,
  exact quotations and unrelated prose alone. The prompt front-loads essentials;
  an overlong model answer can lose later directions at this final boundary.
  Genre coherence and lyric quality remain model-guided and need live review.
- Context v2 now admits four relevant unpaired human turns, matching its four
  paired exchanges. Website-backed replies deliberately remain no-store, so the
  former two-row limit could drop the named source request after feedback and a
  check-in. The existing relevance, recency, visibility and 2,600-character limits
  still apply. This is bounded conversational continuity, not indefinite memory.
- The Gemini adapter concatenates all visible text parts of the first candidate,
  excluding thought/nontext parts, instead of taking only the first part. A safe
  diagnostic logs the finish reason, part count and text length without content.

The no-store continuation failure is reproduced locally with the actual pattern:
named lookup, song, feedback, `yo!`, `You get that BNL?`, then the short override.
Before the context adjustment, fresh rehearsal evidence disappears; afterward the
direct and batch prompts retain the named session. Changing the feed session or
moving to a public channel still prevents private/stale facts from being reused.
Earlier tests seeded saved model replies and therefore missed this case.

The first-part-only adapter is also a demonstrated defect with multipart test
responses, including a split in the middle of a credit. It is a possible cause of
the live cutoff, not a proven diagnosis of that particular response. A provider
token-limit stop remains distinguishable through the new finish-reason diagnostic.
No extra model call, automatic retry, model-budget increase, storage of website
facts, new routing owner or configuration gate is added.

## Research and limits

Reviewed 2026-09-17; primary sources, paraphrased. The runtime instructions are
our editorial synthesis, not a guarantee of Suno audio quality.

| Source | Application and limitation |
| --- | --- |
| [Suno: How to Make a Song](https://suno.com/hub/how-to-make-a-song) | Specific musical direction, structure and targeted iteration are more useful than an interchangeable mood label. It also gives detailed examples; it does not establish that all long prompts fail. |
| [Suno: Exclude](https://help.suno.com/en/articles/3161921) | Unwanted instruments or elements have a separate Exclude control. Keep the main Style paragraph focused on the desired sound. |
| [Suno: v6 FAQ](https://help.suno.com/en/articles/13924481) | Complex direction and targeted editing are supported. Do not invent a universal prompt-length failure threshold or promise deterministic execution from text. |
| [Pat Pattison: Language and Songwriting](https://www.patpattison.com/language-and-songwriting) | Natural speech stress carries meaning. Avoid awkward emphasis and syntax chosen merely to complete a rhyme. |
| [Google: GenerateContent, Content and Part](https://ai.google.dev/api/generate-content) | A message has ordered parts; thought parts are distinct. Preserve visible text across parts. `MAX_TOKENS` identifies the requested token limit, while `STOP` is a natural/model stop. |

Avoiding genre overload, competing instrument roles, gratuitous tag lists and
generic cyberpunk imagery is a BNL editorial policy. A surprising blend can work;
the brief assigns roles rather than banning experimentation. The code cannot
listen to generated audio, certify a masterpiece, or learn permanently from this
single conversation. Suno controls and models may change; the linked research is
dated rather than presented as permanent platform law.

## Verification and next acceptance

Focused checks cover the no-store sequence, source scope/session changes,
private/public isolation, multipart and typed-packet text, existing model budgets,
Style boundaries, intact lyrics/notes/quotes, and delivery without a second model
call. Focused subsystem checks passed; `make check` compiled the code and passed
all 3,106 tests. The changed Python also parses with Python 3.9 syntax, and
`git diff --check` passed. These verify implementation, not generated audio or
live creative acceptance.

After merge and confirmed deployment, resume only the failed private song and
revision checks. Judge the complete output, not just a route receipt. Then test
one deliberately overloaded Style request: BNL should give one brief reason,
retain the intended idea and deliver a coherent blend within the ceiling.
Do not repeat accepted show, queue or commercial rehearsals.

The B3 partial-play versus missing-start discrepancy from #554 remains open:
missing start evidence is not proof that no sound played. This change does not
invent a start or certify that interpretation.

Rollback is a normal revert; there is no data migration.
