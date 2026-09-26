# Common sense, connected retrieval, Moments, and latency — 2026-09-26

The audit confirms failures in several boundaries around Gemini. Relevant
information can exist and pass its own reader while being omitted by a later
owner. This patch repairs reproduced failures in the existing owners. It does
not establish universal wording independence or complete Moment acceptance.

## Production evidence

The September 26 18:00:50 UTC TEST after-show capture for
`session_muhnvmtf_jm740` has packet ID
`6c30ad3d4395a98efc07da9ffd31f42010a8c4b99bf28c72565a71a7cd4b49e7`.
Both evidence attachments match their manifest byte counts and SHA-256 hashes.
The bot was active on `4b99b7f36fc1ad83f6a45f553f0b4e68738cf1df`, with a clean
checkout. The capture is still partial because `website.playbackAtLimit` limits
that export; SMTP acceptance is not shared-brain acceptance.

The prior show-grounding fix reached production. The three supplied replies
used archived show evidence and the quote follow-up reopened originals. The
first answer nevertheless used the wrong pronoun for Ash. Ash's earlier public
correction exists in retained Discord row 9158. The first show answer's packet
selected show/canon material without conversation context carrying that earlier
correction. The export does not prove a durable pronoun write. The later phrase
about adjusting a classification index is not a write receipt.

The mixed correction/chemistry follow-up selected five show records instead of
preserving the current show thread. The initial recap and quote follow-up each
loaded one show. These are source-selection observations, not a claim that all
of BNL's interpretations of the participants were correct.

| Supplied turn | User message to stored reply | Gemini generation in logs |
| --- | ---: | ---: |
| September 25 show interpretation | about 58 seconds | 13.9 seconds |
| Correction plus chemistry follow-up | about 61 seconds | 9.4 seconds |
| Request for quotations | about 43 seconds | 12.4 seconds |

These elapsed totals include preparation and delivery/persistence work. The
capture does not resolve every interval before batching. Repeated archive reads
are visible before generation, after generation, and around send. It is not
accurate to attribute the whole delay to Gemini.

## Reproduced failures and changes

| Boundary | Reproduction | Change |
| --- | --- | --- |
| Ordinary readers to shared packet | Topic questions obtained relevant Journal/Relay prose with packet ownership off, then lost it with ownership on unless they named those systems | The packet uses the same relevance-aware publication adapters. Journal candidates are probed locally before requesting visibility controls. |
| Independent publication sources | A missing or unavailable publication marked the entire packet invalid, suppressing usable companion evidence | The unavailable source supplies no evidence; other validated sources remain usable. A task without evidence retains its hold. A selected Journal source still needs fresh controls. |
| Request clauses | A separate question cancelled a clear self-report anywhere in the message | Question clauses are excluded individually; independent direct declarations can reach the existing governed fact writer. Quoted, hypothetical and role-play material retains its exclusions. |
| Show selection | Concrete content such as “green visuals,” or a dated crowd question, could lose a useful source score without a show-system keyword | Explicit dates and concrete matches in original authored material remain retrieval cues. Similar names and unresolved author requests cannot borrow another person's messages. |
| Follow-up scope | “What made you think that?” selected BNL's answer but lost its original human question | Context retains the participant-aware human request paired with the referenced answer. The show reader can reopen the original source through that request; BNL's prose remains nonauthoritative. |
| Mixed correction and request | A person's name in a correction broadened an independent audience question into that person's history | Existing task segmentation determines whether the person is actually the requested subject. A community follow-up can retain its selected show; explicit broader-history requests still expand it. |
| Object and referent binding | “Tell me about the green visuals” invented a missing person; a pointer in one sentence combined with a request in another | Object binding distinguishes common noun phrases from unresolved people. Structural contribution references are evaluated within clauses. Known people, exact replies and genuine ambiguity retain their checks. |
| Dated comparison | “Compare the September 4 show with the one before it” was treated as an unresolved nearby-message reference | The existing date owner selects the closest preceding retained eligible show. It does not assume a seven-day interval or complete archive coverage. |
| Typing and archive refresh | Typing started after source preparation; pinned refresh decoded unrelated retained shows | The existing managed typing session starts after the response decision, before source preparation. Pinned refresh filters by source keys in SQL and still rereads selected records at each fence. |

The normal-chat contract also distinguishes acknowledging a correction from
confirming a durable write, calls for supplied self-identification to be
respected, and uses a person's name when pronouns are not established. This is
answer guidance, not proof that every relevant correction is now retrieved.

No runtime gates are enabled, model calls added, memory store introduced,
publication sent, or art request made by this patch. It uses the current source
owners, packet, participant bindings, privacy controls and provider route.

## Source coverage and unresolved integration

| Existing source or consumer | What the audit establishes | Remaining limitation |
| --- | --- | --- |
| Original public Discord and TikTok; finalized shows; queue chronology | Real production show access and local cross-source tests; dated scope and original attribution remain checked | A bounded excerpt selection is not an exhaustive transcript search. |
| Current queue/site facts and canon | Existing independent authorities remain in place; their regression coverage runs with this change | Historical prose cannot establish current operational state. No production acceptance is inferred for an unexercised route. |
| Member facts and short/medium/long memory | Direct self-reports survive an independent question; topic recall can coexist with publications | Deterministic fact extraction is still limited. The patch does not infer pronouns from jokes or attach one platform identity to another by similar names. |
| Journal and accepted Relay publications | Relevant context now survives ordinary packet ownership; unavailable companions do not clear healthy sources | A publication is BNL's published interpretation, not an independent factual root or audience quotation. |
| Moments and episodes | Admission, meaning, source lineage and recall have distinct stages; natural admission failures are reproduced below | Semantic continuity and slow conversations remain incompletely handled. |
| Relationship and engagement layers | Their existing source checks and configured gates remain unchanged | Passing source retrieval does not establish relationship/engagement quality. |
| Journal/Relay/Ballad/Ambient and private own-art consumers | They share some source owners and derived publications | They do not all receive an identical situation packet. The thin own-art adapter remains an open integration, and image generation remains paused. |
| Gemini | It receives the prepared context through the existing provider | The current provider route does not expose native, model-directed read tools to retrieve an omitted source on demand. |

The Ash example therefore remains a live acceptance concern: retaining a
correction, extracting a governed fact, linking an identity, selecting that fact
for a show participant, and using it correctly are separate steps. Improving
one step is not proof of the others.

## What Moments currently does

The existing engine observes eligible conversation-ledger entries. It groups
them using source boundaries, exact reply relationships and topic coherence.
A window expires after two minutes of inactivity or five minutes total. Topic
changes, incompatible visibility and route boundaries can also end it.

A shared-activity Moment normally needs two human participants and three
meaningful human entries. A one-person conversation with BNL normally needs
three meaningful human entries, or two plus a stronger event marker. BNL's
messages contribute conversation structure, not public factual authority.

Qualified public Moments enter the existing background Gemini meaning worker.
That worker writes a derived gist and attributed contributions only after
source checks. Episodes can connect qualified Moments over longer periods;
ordinary memory and originals remain separate. A rejected Moment window does
not delete its conversation originals. In particular, a meaningful single
announcement need not satisfy the current multi-turn Moment contract.

The natural replay below called ordinary admission; it did not force membership
or inject a model-produced meaning. Every original conversation-ledger row
remained present.

| Natural scenario | Actual admission result |
| --- | --- |
| Three turns of a shared music-production discussion | One finalized Moment, meaning pending |
| Six-turn running-joke exchange from the existing reporter fixture | Two rejected fragments; no finalized Moment |
| Nine-turn containment/typo exchange from the existing fixture | One finalized Moment, meaning pending |
| One shared courtyard-lighting plan expressed with different vocabulary | Three rejected fragments |
| Single album-release announcement | One rejected window under the current qualification contract |
| Music discussion followed by an explicit switch to hiking | Two distinct finalized Moments |
| Same music topic, messages 2.5 minutes apart | Three rejected windows |

The production capture contains 25 rejected, one finalized and one open window.
Those counts alone do not classify each rejection as a bug. The export does
not include meaning status for these windows, so it cannot prove that the
finalized window received useful Gemini meaning or was later recalled well.

The important architectural limit is admission order: semantic meaning runs
after qualification. A coherent exchange split by the lexical/time rules can
be excluded before Gemini evaluates its meaning. Changing personality prose
cannot repair that ordering.

## Established methods for the next architectural work

Google documents native function calling, including parallel/compositional
tool use: <https://ai.google.dev/gemini-api/docs/function-calling>.
Anthropic's context-engineering guidance describes combining prepared context
with on-demand retrieval:
<https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents>.

The applicable design is a bounded read capability over BNL's existing source
owners: give Gemini a source inventory and permit it to ask for missing
evidence, resolve a follow-up and inspect originals before concluding that data
is unavailable. Reuse the same stable identities, visibility decisions,
provenance, budgets and final source fences. Keep independent reads parallel
where possible and measure the additional provider round trip. This is an
architectural recommendation; native read tools are not implemented here.

For Moments, use the existing background meaning worker to evaluate semantic
continuity and salience before permanently discarding otherwise eligible
candidates. Evaluation must include paraphrases, pacing, explicit topic changes,
corrections, source withdrawal and later recall. It must not turn every message
into a permanent fact or add an independent memory pipeline. This admission
work is still open; the existing meaning tests alone do not settle it.

## Verification

The exploratory matrix covers 15 standalone show questions, eight follow-ups,
eight direct-fact inputs, 11 publication/member-memory questions with packet
ownership both off and on, and seven natural Moment scenarios. Website,
Gemini and Discord transports are fixtures; actual context/frame assembly,
SQLite owners, packet selection and source checks run locally. These runs
measure retrieval and integration, not live Gemini response quality.

Regression tests include mixed self-reports, source-name-free questions,
multi-source requests, an unavailable Journal with usable Relay/member memory,
orphan BNL replies, topic switches, incidental person corrections, nearby names,
date comparisons, source deletion and typing cleanup. Existing suites also
exercise privacy, changed controls, correction/retraction, multi-person
attribution, current state and route ownership.

A synthetic benchmark with 31 retained episodes (38,563,840-byte SQLite file)
and one pinned episode produced identical rendered text. Median refresh time
over five reads fell from 462.207 ms to 3.066 ms. This verifies the local work
reduction; it is not a VPS response-time prediction. New content-free
`response_stage_timing` logs split message capture into original storage,
Journal capture, ledger/Moment work and remaining maintenance, and time show
source reads. Deployment evidence must establish the production improvement.

`make check PYTHON=.venv/bin/python` passed compile checks and **3,452 tests**
in 187.335 seconds. Tests run on local Python 3.12; all 14 changed Python files
also parse with Python 3.9 grammar. Production
runtime, provider quality and timing remain post-deploy acceptance work.

## Post-merge deployment and focused acceptance

After this PR is merged, use the existing VPS deployment:

```sh
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
sudo systemctl restart bnl01 &&
systemctl is-active bnl01
```

In the normal BNL Discord conversation, send these sequentially, waiting for
each reply. This is an archived-show test; no live show or new image is needed.

1. `BNL, what stood out about how people interacted at the September 25, 2026 BARCODE Radio show?`
2. `What made you think that? Quote a few actual comments and identify the speakers.`
3. `Ash is a guy. He told you that earlier. Also, what else did you notice about the chemistry between people in that same chat?`
4. `BNL, connect what you have written about the BARCODE community with that September 25 show conversation. Which parts come from actual comments, and which parts are your interpretation?`

Then capture once with the already-working interpreter and collector:

```sh
/home/ubuntu/bnl01/venv/bin/python "$HOME/.local/share/barcode-after-show/after_show.py" --test
```

Inspect the deployed revision and sent-response hashes; original evidence and
date scope for turns 1–3; independent publication/original-source selection for
turn 4; and capture, typing, preparation, provider and send timestamps. Verify
actual quotation/speaker matches and natural use of the correction. Do not
accept an unsupported profile-write claim or treat omitted selected evidence
as proof that nothing exists. A fluent answer and a successful SMTP test are
not substitutes for these checks. No new Moments acceptance is claimed from
this four-question sequence.
