# Shared-brain response handoff — 2026-09-06

## Current operation

The owner approved the September 6 shared-brain completion plan and one bounded
implementation. The owner then rejected prioritizing the TikTok deletion/replay
edge case: public TikTok conversation is authorized durable show knowledge.
That unpublished experiment is excluded from this repair and is not an acceptance
blocker. No production data, gates, deployment, or source-retention behavior is
changed by this work.

This repair continues the existing response/context owners. It addresses rejected
or regenerated answers losing the typed task/evidence contract, schema envelopes
being treated as prose, and actual turn directness being omitted before packet
construction. The user-facing goal is an informed, natural BNL response that uses
the available context consistently.

Base: bot `48e4911c7a2037251868dc80b39df883b3caef74`, tree
`bb9a05439c83ed85e943c7acfaa21ad86eca9761`. Site baseline:
`03c482c5829e518e6ffd02891ae060ee36b09b4d`; no site change is included.

## Required behavior

- Reuse the original frozen task/evidence basis where it still applies. Decode
  and validate typed repairs through the existing contract before extracting
  visible answer text; never send the internal JSON envelope.
- Preserve the ordinary response obligation and BNL's natural voice. Recoverable
  draft problems continue through the existing bounded natural-repair path.
- Preserve established source/currentness, frame, privacy, exact-reply, and
  stale-send controls. A genuinely source-neutral repair must not invent support
  from an invalidated basis.
- Carry actual directness through the nonbatch prompt/assessment/packet handoff.
- Account for actual corrective provider work in the existing turn receipt where
  the repaired paths perform it; distinguish generation from final delivery.
- Keep authorized conversation and specialist evidence under their existing
  owners. Introduce no memory store, grammar blacklist, competing response owner,
  public test, or global capability activation.

## Verification and remaining scope

Reproduce the typed-repair bypass, schema-as-prose path, and passive directness
handoff on the baseline. Test the real affected function chain with provider
responses mocked only at the external boundary, then run the relevant existing
suite and full `make check` on the exact tree published for review.

Existing accepted row receipts remain valid for their recorded scope. Original
Row 10 remains the kill-switch rollback test; integrated replay cross-references
the relevant named capabilities rather than redefining a row.

This bounded repair does not complete multi-speaker/cross-channel convergence,
all resume/applicability behavior, memory retention proof, specialist composition,
publication-consumer wiring, or natural-operation acceptance. Those remain in the
approved compact completion pack. No fresh VPS census or live acceptance is
claimed from source inspection or local tests. Merge, deployment, gates, and live
acceptance remain separately authorized steps.

## Post-merge deployment and focused evidence

Run this only after the PR is merged and deployment is authorized. Record the
actual merged commit as the expected deployed commit; a draft PR head is not a
merge receipt. Use the bot's normal Python environment for the focused tests.

```bash
cd /home/ubuntu/bnl01
git pull origin main
sudo systemctl restart bnl01
sudo systemctl status bnl01 --no-pager -l
git rev-parse HEAD
PYTHONPATH=tests python3 -m unittest test_shared_brain_response_handoff test_shared_brain_synthesis_bot_path test_ordinary_chat_single_packet_canary
sudo journalctl -u bnl01 --since '10 minutes ago' --no-pager | rg 'ordinary_chat_turn_completed|ordinary_chat_response_rewrite|single_packet|Traceback'
```

The focused tests use temporary databases and mocked external generation. They
must preserve valid prose, code and JSON replies; retry an invalid internal task
envelope; retain passive directness; and total primary plus corrective work.

After an already-enabled ordinary route completes a real turn, read its existing
receipt with the following query. This reads counts and statuses only. If the
new usage column is absent, first confirm that this version initialized the
existing synthesis schema; do not infer a successful runtime repair from startup.

```sql
SELECT run_id, provider_call_count, corrective_call_count,
       candidate_total_tokens, turn_generation_usage_json,
       live_applied, response_sent, guard_status
FROM memory_governance_shared_brain_synthesis_runs
ORDER BY created_at DESC, run_id DESC
LIMIT 5;
```

Use a read-only connection to `bnl01_conversations.db`. Match the receipt's run ID
to `ordinary_chat_turn_completed`. Total provider calls must include corrective
calls; the primary candidate's tokens remain primary-only. A repaired answer can
have `live_applied=0` and `response_sent=1`. Confirm the actual visible answer
and final route together using traffic within the already authorized scope. No
new Discord/TikTok post or expanded room scope is part of these instructions.

If deployment regresses, use the established rollback procedure to return the
bot code to the recorded previous deployed commit. Preserve databases and their
existing receipts; this change adds only an optional accounting field to the
existing receipt table and requires no destructive data rollback.
