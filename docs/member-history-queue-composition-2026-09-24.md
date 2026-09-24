# Member history and queue composition

The September 24, 03:20 UTC live reply answered a month of Discord/TikTok
activity and historical queue participation with a current queue snapshot.
It also claimed TikTok history was unindexed. That is a functional failure.

The owner-supplied process1753961 trace used `get_gemini_response`, with
memory/publication source checks but no finalized-show check. Generation to
Discord send success was1.174s. This is not a comparable speedup acceptance
for PR582: the earlier successful recall used a different evidence path.
GitHub confirms PR582 merged as96915f73fcfccd037afa0553aa0fe4e5bdcbb7e0.
The new logging is present in the observed process; its exact deployed SHA
was not supplied in a service/checkout receipt.

## Reproduced causes

- Requester scope recognized narrow possessive forms but missed ordinary
  first-person public activity requests. The frame and show reader share this
  scope; neither should require the old request sentence.
- The website adapter rendered a current queue/show date, and the next reader
  extracted every `showDate` string as a historical selection constraint.
  Operational snapshot dates could therefore displace independent history.
- Relative multi-day history was not a shared temporal constraint. A TikTok
  chat phrase could still select current live context despite “last month.”
- The compact multi-show projection retained chat and a few events but dropped
  roster submission attribution needed by a combined queue-history question.

## Changes

Existing source owners now compose independently. The website renderer passes
explicit show-selection metadata; a queue snapshot's date cannot become the
member-history date. The shared requester scope accepts first-person activity
clauses; resolved named subjects take precedence over an implicit requester
lookup, while explicit possessive activity can request both members.

The existing date owner supplies a rolling Pacific history window to Discord
originals and both show readers. “Last/past month” means the same date in the
previous calendar month through today, with month-end clamping. Explicit dates
keep their existing precedence. Relative days/weeks use the same owner.
The original time wording remains in the provider prompt.

A historical window uses retained history, with a separate current queue
readout only when also requested. Historical chat is not classified as current
live chat. Compact show history includes bounded roster records, prioritizing
existing matching source handles and retaining their attribution boundary.
Submission, artist authorship, outcome and actual playback remain distinct.

This does not force every mixed turn onto the ordinary-chat canary. The queue
can retain its specialized route while independent, revalidatable public
history reaches the same final provider prompt. Source selection is bounded;
a missing example does not establish zero participation or an empty archive.

No identity links, production gates, schemas or stored source rows are changed.
Existing consent, privacy, source-currentness and final-send checks remain.
Private authority labels are not rendered as public identity evidence.

## Verification and handoff

Three mixed-request variants failed against merged main, in packet-enabled
and packet-disabled fixtures. Integration tests now exercise direct and batch
assembly with a separate empty current queue, two historical shows, Discord
originals, recorded submission attribution, and fresh source-basis validation.
Additional cases cover outside-window sources, packet/native window agreement,
named-person scope, Pacific dates and calendar month boundaries.

Focused and full-suite results are recorded in the PR. Test providers are
fixtures: these checks prove source delivery and safeguards, not production
model wording or the completeness of a particular member's retained history.

After merge, use the normal verified-tree deployment. In bnl-testing, ask one
natural combined question about your Discord/TikTok activity over the last
month and historical queue participation. A supported answer must use dated
historical evidence, distinguish unknown submission attribution, and never
substitute an empty current session for the historical result. A separate
current queue clause must remain current. Retain the reply and model/source/
send timing lines; do not rerun the isolated VPS profiler.

Keep the accepted Chris quote/Pacific-time and earlier cross-source receipts.
Friday September25 live rehearsal and the previously locked eligible
Journal/Relay/Ambient/later-memory follow-through remain separate acceptance.
