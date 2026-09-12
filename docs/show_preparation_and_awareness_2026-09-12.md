# Show preparation and light awareness — September 12, 2026

This candidate extends the merged timeline/interval work in PR543. The show
episode remains the parent for the running order, tracks and captured TikTok
and Discord conversation. Preparation is connected to that parent without
changing its original time or implying it happened on air.

## Owner clarification

6 Bit clarified during implementation that the whole show is already an
episode/Moment and that TikTok chat need not enter the general conversational
Moment engine. This candidate follows that structure. An unpublished experiment
to admit TikTok sources to that engine was removed. `bnl_moment_engine.py` and
`bnl_memory_ledger.py` retain the merged baseline. There are no new Moment
engine admissions, gates, source classifications or provider calls.

## Existing owners and behavior

- `bnl_tiktok_show_ledger.py` adds a `preparationMoment` inside the existing show
  ledger. Its stable identity is the show key plus `:preparation`. It contains
  original-source references, timestamps, roles, preparation events, and links
  to existing source-revalidated Discord Moments.
- A single substantive preparation report can be retained under the show;
  it requires no artificial conversation with multiple people. A human report
  remains an attributed report, not independent proof that a check passed.
- Recorded session creation supplies a preparation interval ending at broadcast
  start. Chat in that interval is contextual evidence and can be unrelated
  banter. An exact session reference or an explicit show date can associate
  earlier preparation, including material recorded before session creation.
  An ambiguous date shared by known shows needs an exact session reference.
  There is no arbitrary day lookback and no nearest-date assignment.
- Existing Discord Moments retain their identities, dates, participants,
  contributions, lifecycle and normal retrieval. Their original member sources
  can connect them to preparation. Linking does not reopen or extend a Moment.
- Original TikTok and Discord source stores remain the recording owners. The
  existing show sync persists the preparation projection. Preparation recall
  rebuilds the view from current sources and existing retained show evidence.
  Ordinary source pruning can preserve already admitted preparation under the
  show owner. An extant changed/private source or retraction defeats that copy;
  complete member deletion also removes a show containing preparation-only data.
- Native recall and intelligence-packet recall use the same preparation view.
  The selected record text survives the old synopsis limits. Source scanning
  retains the existing limits of 20,000 conversation rows and 50,000 Journal
  rows, and reports missing/limited/invalid coverage. Rendering is bounded to
  96,000 characters, omits whole records and reports its rendered/selected count.
  A source scan is not evidence that every platform message was captured.

## Small show-context nudge

The existing show sync/read-model cache supplies the active public session.
Ordinary public conversation and the sealed mirror can then receive a short
context block with the recorded phase, current track and supplied public links.
The existing authenticated fetch owner refreshes an expired active-session
snapshot; stale context is never used as the current answer. Off-show casual
conversation starts no additional website request. The configured primary guild
and existing source permissions continue to apply.

Explicit subjects, other dates, topic changes and corrections prevail. Warmup
and submissions are not described as playing. Ended/archived or unavailable
sessions do not activate the nudge. Only supplied public links are used. The
ordinary reply and capture policies remain intact; the temporary context block
itself is never passed to a memory writer. There is no second recorder, show
controller, classifier or announcement loop.

The website's actual `broadcast_active` phase is also accepted by the existing
live interval-boundary code; its legacy `live` fixture spelling remains supported.

## Verification and remaining acceptance

The focused suite exercises single-person preparation, pre-session association,
Discord Moment identity preservation, all captured pre-show chat, ambiguous show
dates, packet/native rendering and provenance, correction, pruning and deletion,
active preparation, source limits, public/private boundaries, current links and
read-model expiry. Existing interval and show-ledger tests remain regression
checks. The candidate's publication receipt records the actual tested commit,
tree and full-check result.

The inspected foundations are bot main `b195165f5bdf205993655881beb5bd0cb99b7c6d`
and site main `72853f4ec5a2adbaf65cb4d6c5e2e23ac784b902`. The site already owns
session/submission/playback/wheel/removal events; no second event producer or
site schema is introduced here. Its show-log contract has no dedicated machine
preflight-result event. Existing captured preparation reports are supported;
uncaptured equipment checks cannot be reconstructed as observed events.

This candidate does not establish deployment or live acceptance. The original
11 recovery rows, six stages, 17 owners and unrelated pending checks remain
open at their recorded statuses. The next acceptance uses actual deployment
evidence, the supplied September 11 track recall and real preparation/current
show conversation. No fabricated public conversation is required to force a
Moment. The accepted Oreaganomics exchange is unchanged.
