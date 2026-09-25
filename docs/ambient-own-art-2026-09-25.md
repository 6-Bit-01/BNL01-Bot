# Ambient refresh and BNL's own art

Owner decision: BNL may attach up to one of his own images per Pacific day to
an existing ambient Discord post. This is a ceiling, not a quota or commission
service. The community image-generator channel remains observe-only.

## Ambient changes

- The existing governed conversation reader now includes `public_selective`
  observations. Recent context is bounded to 24 hours and retains timestamps.
  Historical memory is separately labeled and still requires original-source
  lineage and public permission. Owner labels normalize to 6 Bit at read time.
- BNL chooses the subject and voice. Quiet periods can support reflection or
  imagination without invented member activity. He may choose silence, which
  schedules the next normal day rather than treating silence as a failed call.
- No random compulsory mode or internal-process word rejection. Existing
  completeness, duplicate, source-withdrawal and actual-live-state checks remain.
- At most two text-generation calls per due cycle. Existing normal/active-day
  text caps, cooldowns, occasion reservations, shared budget and schedule remain.
- An ambiguous Discord send is deferred to the next day instead of retried on
  the next five-minute scheduler tick.

## Optional image lifecycle

`BNL_OWN_ART_ENABLED=true` is required on the bot. It defaults OFF. Only the
primary guild is eligible. The same ambient generation can propose an original
image, without an extra concept call. Image generation uses PR590's bounded
Gemini image client and shared cost reservation, with no retry or fallback.

The SQLite `bnl_own_art_delivery` table atomically claims a Pacific date before
the image request. Failed, withdrawn and uncertain attempts still consume that
date: no regeneration or second image after a restart. This conservative first
release permits at most one attempt and one Discord image for each local date.
A midnight crossing drops yesterday's attachment before sending.

Image failure leaves valid ambient prose available. Source authority is checked
after generation and again before sending. The image is attached to the same
message; it is never a second post. Claims, private draft, confirmed Discord
message ID, and website receipt are separate states. Private files use mode 600.

After confirmed Discord delivery, the same image can be published at
`POST /api/bnl/art` using the existing website service credential. A failed or
uncertain website upload is recorded, not reported as a published gallery item.
There is no automatic ambiguous-upload replay in this first release. The
private image/receipt remains available for a scoped recovery after revalidating
sources. No new polling task is installed.

An image may optionally relate to a **published**, public, memory-eligible
Journal selected by the existing publication reader. The site verifies exact
entry ID, revision and content hash. It can appear beside that entry after
publication, including a regular Daily entry; it never enters or blocks the
19:00 Journal publishing path. Standalone artwork needs no Journal link.

## Acceptance and rollout

Code-level tests are not actual Gemini image or Discord acceptance. Keep the
bot and site's separate `BNL_OWN_ART_ENABLED` variables absent/false until a
private image preview has been inspected. PR590's
`scripts/preview_bnl_own_art.py --output-dir NEW_PRIVATE_DIRECTORY --generate`
is the existing operator-only, non-publishing acceptance tool. It makes paid
provider calls only with `--generate`; its default is a zero-call readiness check.

Deploy the site contract first, then the bot release in the usual service
environment. Activation is a later explicit, scoped action after private
acceptance; this change does not set either environment variable or operate the
VPS. Confirm the first natural ambient image using its actual Discord message
and separate website receipt. No live-show testing, extra announcements or
community commissions are required.

Unrelated queue, payments, Relay, Journal timing, global memory, Relationship,
Active Engagement, submitter-edit and optional announcement gates are unchanged.
