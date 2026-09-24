# Remove a duplicate source-validation pass before delivery

## Measured problem

On deployed PR581 (`3e4d0a98e2208f7f0bfaa9ddbad88a5938cfea6c`), the
September24 process1753676 logs place most of the observed post-generation
delay before Discord delivery:

| Recorded turn | Model completion to send start | Discord send | Post-send receipt completion |
|---|---:|---:|---:|
| 6 Bit cross-platform summary | 22.897s | 0.234s | 0.114s |
| Chris dated quotation | 6.183s | 0.192s | 0.010s |

The corresponding context-selection-to-send-success intervals are49.107s
and22.275s. These begin after the user's message arrives, so they are not full
message-to-response measurements. The older49.221s/22.285s intervals ended
after receipt finalization. Neither Discord transport nor that final receipt
accounts for the large pre-send gap in these two turns.

The successful isolated VPS profile measured native show selection1.3075s,
native revalidation0.9115s, packet show selection1.0033s and packet show
revalidation1.0045s, with unchanged projections/versions. This is4.2268s of
isolated profiled work, not the cost of an entire live response. Do not subtract
it from the live intervals or claim that it identifies every expensive owner.

## Change

The response guard rebuilt selected source evidence at its start and again
at its end. Delivery owners then rebuilt it once more after their final
route-specific await. The logs contain three post-generation native show
rebuilds; integration tests reproduce three refreshes for an unchanged source
in both batch and direct delivery.

Batch, planned direct and payload-session senders now explicitly own the
final source check. The guard retains its initial fresh source refresh and
response/quote checks, and returns the current source bases. These senders
retain their existing fresh, off-loop validation after typing, quote or other
route-specific awaits. The guard's intervening full-source check is deferred
to that existing send boundary. Standalone guard calls keep their own final
check by default.

No source snapshot is cached across awaits. Source selection, public-memory
eligibility, identity resolution, quote/date handling and response-repair
ownership are unchanged. Metadata-only timing logs identify the refresh/check
phase, source-basis class, elapsed seconds and changed status; they contain no
source text, prompt, response or member identity. These logs measure the source
work itself, not thread scheduling delay or the entire response.

## Verification and limits

- Before the runtime change, the new batch/direct integration assertions
  reproduced three source refreshes where two suffice.
- Normal delivery now uses two fresh show-source refreshes, with packet
  ownership enabled and disabled in batch tests; the supported answer is
  delivered once with no additional provider call.
- Real database changes to source privacy, content and author after the batch
  guard are detected before send; the old quote is neither delivered nor saved.
- A privacy change after the direct response guard is detected before delivery.
- Standalone guard validation still rejects evidence changed during a provider
  repair. Existing source/consent, exact-quote and lifecycle regressions remain
  part of verification.

This removes one complete repeated source pass in the observed path. It does
not establish a production speedup percentage or resolve every remaining
millisecond of the pre-send delay. Confirm the deployed behavior with one
ordinary memory conversation and its metadata timing lines; do not rerun the
isolated profiler or the previous seven-question recall sequence. Friday's
live-show rehearsal and eligible scheduled-delivery acceptance remain separate.
