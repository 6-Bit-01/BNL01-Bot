# Normal conversation restoration from the PR #488 reference

This change implements the September 7 restoration decision after reviewing
PRs #484–502 and the original runtime receipts. It is a selective restoration,
not an exact historical checkout or a completed shared-brain rollout.

## References and operating scope

- Last decent reference: PR #488, `6d043755f2ef491b9d1b8617738c293901fbfde8`,
  tree `277fb01796039ebc809f11ebb4c22a4da84f8dc8`.
- Reviewed current main: PR #510, `89055dab09fc6670746aa96f90356f782d16f76b`,
  tree `049bbf766f3b5000eaec1f95f893524620c26536`.
- #488 already contains shared-brain infrastructure. It precedes the final
  episode/public-answer testing spiral, not the memory foundations.
- PR #509 already removed #498–508. #492 was exactly reversed by #493;
  #497 never merged. Their removal is not new work in this PR.

The ordinary packet generator stays off. Normal Gemini uses the existing
public knowledge and memory readers. Public-home and private-test conversation
share applicable public knowledge; the test conversation remains private.
Other permitted channels retain their tag/reply admission. Queue production,
Journal/Relay scheduling, TikTok ingestion and site operations keep their
current operating configuration.

## Why an exact rollback is insufficient

The historical #488 receipt shows packet evidence reaching a repaired answer.
The current failed turn used packet-off normal conversation. #488 could remove
normal queue context before proving a replacement packet existed and excluded
public durable memory in testing. Those later data-delivery fixes are retained.

The latest original runtime log also shows a normal mixed Journal/queue answer
rejected twice by a show-clock checker, then repaired through the packet route.
The earlier queue read had timed out despite a nearby successful refresh.
Those functions predate #488; copying its tree would leave the failure class.

## Restoration disposition

| Area | Result |
| --- | --- |
| #489–490 episode language and criteria | Restore earlier episode/Frame interpretation, preserving necessary source-Moment and expected-episode identities. Exclude the broad model-source exemption and current-turn-only criteria overcorrections. |
| #495–496 packet public-answer exceptions | Restore the #488 review baseline. This dormant path is not used as ordinary-response recovery. |
| #487–488 context and replies | Keep existing authorized context alongside publication/current-state information and retain exact Discord reply continuity for unstored operational answers. |
| #491/#494 personal/show memory | Keep eligible response persistence, requester-specific show evidence and composition with personal memory. |
| #510 public reads and queue retention | Keep normal Journal/Relay inputs, public Discord/durable-memory access in testing and preservation of normal queue context without a real replacement packet. |
| Ordinary answer delivery | Remove show-only whole-answer validation from ordinary response control. Retain existing actual private-data, provider and delivery handling. Recovery uses normal Gemini. |
| Queue reader | Reuse the existing read-model cache and freshness/access semantics at the HTTP boundary. A transient failure must not erase an eligible still-fresh successful snapshot. Expired or differently scoped data remains unavailable. |
| Stores and external operations | No database migration or reset, memory deletion, TikTok replay, site deployment, queue/payment/playback mutation, or scheduler/model change. |

## Verification

Full `make check` passed: **2,624 tests in 83.259 seconds**, plus compilation,
using Python 3.12 with the repository's pinned requirements. Independent review
found no remaining blocking issues after preserving the separate durable
TikTok-analysis candidate check during normal recovery.

Local integration coverage uses actual queue/publication parsing, temporary
SQLite memory, Context/Frame, prompt assembly, normal Gemini dispatch, response
checks and send/write handling. External HTTP, the Gemini SDK transport and
Discord transport are simulated. It does not certify production database
health, real Gemini answer quality or VPS deployment.

- Seven delivered turns cover home/test public-source composition, multiple
  speakers, an exact Discord reply and tagged/reply admission in another room;
  each uses the normal system prompt and one SDK generation.
- The original mixed-source clock failure is reproduced with distinct Journal,
  queue and show timestamps. Both home and test paths deliver once without a
  show-clock rewrite.
- The real queue reader covers overlapping refreshes, timeout recovery within
  the existing 20-second TTL, expiry, changed source scope and unavailable data.
- A deliberately exhausted generic answer still recovers through normal Gemini
  and reports regeneration. The retained TikTok-analysis check rejects its
  known invalid fixture while keeping the supplied evidence for normal repair.
- Sealed conversation writes stay scoped to the test room and do not create
  public Journal events or modify public durable-memory tiers.

Admission, scheduling, model selection, ingestion, publication and memory
writers are unchanged by source comparison. The foreground HTTP timeout remains
three seconds; this change does not claim measured live latency improvement.

Use the actual queue reader, publication readers, Context/Frame, prompt
assembly and normal dispatch/send path for mixed-source checks. Preserve
public/test write isolation and multi-speaker attribution. Original tests
whose assertions require rejected lexical authority are not a reason to keep
that authority running; its historical code and fixtures remain in Git.

Provider attempt logs are the source of total physical generation counts.
The ordinary-packet debug counter describes that subsystem only. One normal
response owner does not promise one attempt when a genuine provider or private
content recovery is required.

## Deployment after this PR is merged

Use the VPS virtual environment, not system Python 3.8. Deploy only the reviewed
merged revision. Do not change the existing queue/source/publication flags or
turn on ordinary packet generation to imitate a historical receipt.

```bash
cd /home/ubuntu/bnl01 || exit 1
git pull --ff-only origin main || exit 1
git rev-parse HEAD
make check PYTHON=./venv/bin/python || exit 1
sudo systemctl restart bnl01 || exit 1
sudo systemctl status bnl01 --no-pager -l
```

After deployment, use the existing mixed question in the test room: “What was
your latest Journal about, and is the queue open right now?” Reply directly to
BNL's answer to ask which information came from the Journal and which is current.
Run the debug command as its own separate message. Check the visible answer,
normal route, relevant source availability, actual generation-attempt logs
and successful send. Preserve earlier basic-conversation acceptance; do not
restart the entire historical live test sequence.

A service restart is not Discord readiness, a source-injected flag is not proof
of queue acquisition, and a passing local test is not live acceptance. If the
site supplies no eligible current queue data, BNL should state that limitation
without losing the rest of the answer.

## Remaining shared-brain work

This restores the ordinary conversation foundation and its existing public
connections. It does not mark all episode/recurrence behavior complete. The
later active-episode projection gap remains a named existing-owner connection;
its repair must not bring typed withholding or a new language classifier back.
Existing multi-speaker normal batches do not automatically inject every speaker's
memory tiers: group context and attribution use the existing Context/Moment
providers. The new group regression proves those inputs and public-source
composition, not complete per-person tier recall. That remains an explicit
shared-brain connection to finish after restoration.

Continue existing memory capture, retention, correction and useful recall,
then their Journal/Relay/ambient consumers, from demonstrated working inputs.
