# Channel observation and BNL's own art

Owner decisions, September 25, 2026 (America/Los_Angeles): BNL should monitor
all accessible channels. `#ai-image-generator` is the community's tool; BNL
must not post, reply, react, or handle commands there. His images are his own
artistic expression, not an image-generation service for members.

## Observation

New readable guild message surfaces use the existing source archive, not a
second memory database. Existing approved conversation capture remains owned
by its current path. Additional observation covers other text rooms, active
threads, and text in voice/stage channels. It does not listen to voice audio,
change Discord permissions, poll history, or backfill old messages.

Public human messages from additional rooms may join the existing selective
conversation memory only when Discord's default role can view and read that
room and it is not a private thread or a protected/internal/test/canon source.
The source receipt retains the participation policy separately. This does not
make that room a new conversational or unsolicited-posting destination.
Private and external-bot observations keep their author/source attribution in
the existing archive with public reuse disabled. They do not become evidence
that a person spoke, public activity counts, or public Journal/Relay material.
Existing scoped forget operations also cover these new observations.

Each additional observation retains the original message time and ID and at
most 1,000 characters with a truncation marker. Attachment metadata is observed;
pixels are not downloaded or interpreted in this passive path. External image
bot outputs are recorded as bot activity, not attributed to a requesting member.
Unknown image-tool identity and visual interpretation remain separate follow-up
work; do not call metadata capture visual understanding.

The existing channel audit now lists active cached threads and reports incoming
observation eligibility/counts separately from conversational capture/posting
policy. Missing `View Channel` or `Read Message History` remains a concrete
coverage blocker. Offline events and inaccessible/uncached private or archived
threads are not claimed as captured. No gateway permission or intent was changed.

The image-generator boundary applies before any command or reply dispatch and
also to threads, slash commands, active-channel selection, common outbound
conversation senders, and show-day destination selection. The existing ambient
policy already excludes this channel. The community tool remains independent.

## Own-art private preview

`scripts/preview_bnl_own_art.py` is an operator acceptance tool, not a Discord
command. It accepts no member prompt, commission, or public destination.

Its default mode builds a readiness receipt from the existing governed Journal
source projection, which already combines shared observations, memories and
eligible reflections. It makes zero generation calls. With the explicit
`--generate` flag, the existing BNL character/model chooses a concept or chooses
to make nothing. Imagination needs no fabricated evidence or mandatory wording.
Source refs must resolve to the supplied public projection. Raw/private source
fields are excluded from the creative prompt.

If BNL chooses a concept, one Gemini image request creates a private PNG and
receipt. It uses the Google Interactions REST endpoint directly so the pinned
conversation SDK and working conversation/Relay/Journal model calls do not need
to change. No Pollinate command or community bot is invoked. No retry, fallback,
background task, recurring quota, Discord sender or website publisher is added.
Generation, stored draft and public delivery are distinct states; `published`
is always false in this tool.

The new destination directory must not already exist; it is created with mode
0700, and image/receipt files use 0600. Existing files are never overwritten.
Receipts include exact physical call counts, source window/hash, model, byte
count/hash, and precise preparation/failure state. No API keys, response payloads,
private shared-memory fields or raw error text enter receipts.

Both calls use existing daily token and shared dollar reservations. The concept
is an ordinary background request. The separate image route reserves against
`gemini-3.1-flash-image`, not the conversation model. Until modality-specific
prices are added to the shared ledger, every image-route output token is charged
locally at the higher image rate: an explicit conservative upper estimate,
not an invoice or exact charge. Existing budget ceilings and protected lanes
are unchanged. Unknown transport or accounting outcomes retain their cost
reservation; they are not automatically retried. Stateless image requests set
`store=false`, request one 1K PNG, and enable no search/tools.

## Acceptance and remaining choices

Local tests exercise real SQLite source capture, source exclusion and forget,
image-channel ingress silence, threads, command exclusion, exact chronology,
private preview lifecycle, optional refusal, provider-call boundaries and budget
denial. Provider responses/images are fixtures. No live image API call, natural
image posting, VPS deployment or physical Discord permission coverage is implied.

The remaining product choices are where BNL shares his own art and what natural
creative occasion should make him consider it. There is no invented hourly/daily
art schedule. Recommend an existing BNL-owned surface, keeping the community
image-generator untouched. Autonomous consideration, publication, private visual
acceptance and a chosen image spend allowance remain pending; the preview code
does not silently activate them. This release does not enable any existing global
memory/Relationship/Active Engagement gate.

Normal service deployment adds observation only. The art preview is not imported
or called by the bot service. Do not run paid acceptance or do live testing during
the show. Tonight's accepted rehearsals, PR584 waiver, after-show collection plan
and open `website.playbackAtLimit` coverage issue are unchanged.

## Provider references checked September 25, 2026

- https://ai.google.dev/gemini-api/docs/image-generation
- https://ai.google.dev/api/interactions-api-v1
- https://ai.google.dev/gemini-api/docs/interactions-overview
- https://ai.google.dev/gemini-api/docs/pricing

Google documents the image model, stateless interactions, inline image response,
generation limits and usage metadata. Standard image-model input is $0.50 per
million tokens, text/thought output $3, image output $60. A 1K image is 1,120 image
tokens ($0.0672), plus other input/output; this is not an approved recurring
budget or proof of this account's model access.
