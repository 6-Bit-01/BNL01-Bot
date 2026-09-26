# Show-chat interpretation and temporal referents — 2026-09-26

The September 25 supplemental after-show packet recovered a real response
failure. Public chat was collected, but ordinary wording failed both context
interpretation and source selection. Passing source checks alone also did not
establish that a later follow-up answered the question.

## Recovered evidence

The original packet remains an original production record. The supplemental
packet is explicitly a TEST capture, not a new show or a successful acceptance
run:

- Session: `session_muhnvmtf_jm740`.
- Supplemental packet:
  `ec9d645d18ba68a7b3dd702b4b7146129ce2d3b8415e374504d25c621d5a58de`.
- Capture: September 26 at 17:07 UTC; both evidence files match the manifest's
  SHA-256 and byte counts.
- The updated collector recovered 13 stored BNL replies, complete synthesis
  receipt fields and the health reader. The packet remains `partial` /
  `acceptancePassed=false`; `website.playbackAtLimit` remains a coverage limit.

At 04:01:06 UTC BNL described the current track and TikTok reactions. At
04:02:42, answering a request for his read on the chat that evening, he claimed
that he had no active chat telemetry. The final response hash matches
`sbsr_7288b03a4c954991a93cf844aa31bd9e`, whose packet run is
`uipr_40e7638116d44574951fc13ace7e4869`. Its recorded source failure is
`subject_ambiguous`, its selected packet contains only current intent, and its
final guard is `batch_single_packet_repaired_response_sent` with one corrective
call. This was a sent repair response, not an ambiguous candidate sent unchanged.

At 08:07:06 UTC, a follow-up asking about tension received mechanical-friction
banter instead of a supported interpretation of the comments. Its exact response
hash matches `sbsr_44c42c03a44340a6a6cd723cef341761`: source validation passed,
with two conversation-context items and one episode item rendered. The source
receipt does not establish answer quality or capture the complete original
prompt. Local reproduction shows the follow-up did not request the show reader.

The relevant three runtime files have identical Git blobs in `48a225f` and
`ca52801`. Reproduction therefore tests the same source owners as the show's
reported checkout. A checkout snapshot still cannot independently attest the
contents of an already-running Python process.

## Change

- Conversation Context ignores calendar/time modifiers when detecting a nearby
  message pointer or choosing the nearest contribution. The original request
  remains intact for date and source selection. Explicit Discord reply IDs,
  genuine message references, attribution and genuine ambiguity retain their
  existing rules.
- The existing show reader recognizes audience interpretation requests and
  selects original comment evidence through its existing chat-topics path.
  This includes a read, impression, mood or tension question, rather than only
  an explicit recap/ranking request. Past-day constraints still choose that
  recorded day; an unavailable day does not fall back to another show.
- Interpretive follow-ups can reload the existing show evidence when a bounded
  earlier human request establishes the show thread. Prior BNL prose cannot
  establish that scope, and explicit new-topic boundaries stop continuation.

This changes existing owners. It adds no model call, memory store, packet type,
queue control, runtime gate or publication. It does not force an interpretation
of the crowd or prescribe BNL's voice. Original comments must support the answer.
Private own-art source selection remains a separate, open consumer integration.

## Verification and limits

Focused conversation/show-reader regressions pass. The final
`make check PYTHON=.venv/bin/python` passed compile checks and **3,438 tests**.
The modified runtime and integration test also parse with Python 3.9 grammar;
the executable test run used the local Python 3.12 environment.

Regression cases reproduce the false person/contribution resolution with both
one and multiple nearby rows, direct and grouped turns, and varied temporal
wording. Controls retain genuine ambiguity and exact reply identity.

Integration fixtures use real conversation/frame assembly, SQLite show sources,
packet selection and response guards. Website transport, model transport and
Discord delivery are local fixtures. The original questions and their
follow-ups receive original public comments and each grouped turn sends once.
The temporal interpretation records `source_revalidation_status=passed` and
zero corrective calls. The follow-up uses the existing website show-analysis
route, which intentionally takes precedence over the ordinary packet route.
Private source exclusions and the original recorded show date remain present.

These are local regression receipts. They are not production Gemini replies or
proof that every consumer and every interpretation is accepted. Previously
accepted evidence and the previously waived PR 584 retest remain unchanged.

## Deployment and focused evidence

After this PR is merged, deploy on the existing VPS:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
sudo systemctl restart bnl01 &&
systemctl is-active bnl01
```

Use the next ordinary show-chat interpretation and follow-up as the focused live
observation. Check that the answer addresses the actual comments, uses their
recorded show scope and does not invent missing telemetry. Preserve the wording
and source evidence; do not require a particular phrase or a new staged public
test. A valid receipt without a relevant answer is not acceptance.

The existing collector is already installed and its private TEST delivery was
verified. When collecting that next observation, use its working interpreter:

```bash
/home/ubuntu/bnl01/venv/bin/python "$HOME/.local/share/barcode-after-show/after_show.py" --test
```

Match the reply's exact response hash to its synthesis receipt where available,
then inspect the corresponding packet receipt and original comments. The
temporal room question must not acquire `subject_ambiguous` from unrelated
nearby contributors. Genuine ambiguity and source changes must still be checked.
Keep the TEST label and original show packet; no image request is needed.
