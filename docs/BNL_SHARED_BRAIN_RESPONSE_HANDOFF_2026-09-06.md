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
PYTHONPATH=tests ./venv/bin/python -m unittest test_shared_brain_response_handoff test_shared_brain_synthesis_bot_path test_ordinary_chat_single_packet_canary test_conversation_batching
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

## Live follow-up after PR 506

The owner merged and deployed `9f4e2de7d29d22537e4cf0896df4617ab4eeb2b3`.
The service connected to Discord, and the focused VPS suite passed: 112 tests in
17.023 seconds using the service's Python 3.9 virtual environment. The earlier
generic `python3` command invoked system Python 3.8; use `./venv/bin/python` above.

The subsequent untagged checksum question in `bnl-testing` failed live. Run
`sbsr_7cdf94604de44ec18b3d83d401aed5bf` recorded three provider calls, two corrective
calls, 9,580 generation tokens and no delivery. The initial review reason was
`typed_contract_task_text_unsupported`. The debug output did not expose the raw
drafts or final repair verdict, so the exact rejected wording remains unknown.

Source reproductions found valid public explanations rejected for generic
possessives such as “your file,” and public clauses inheriting another task's
member/event context. The same typed repair mechanism could repeat a rejection.
The follow-up extends the existing owners in two places:

- Review each resolved public task in its own request scope, retaining original
  evidence, labels and explicit governed-claim checks. This audit-only view does
  not trim the generation prompt or alter stored memory.
- When every frozen task is a subject-free stable public answer and the typed
  task/reference contract passed but text review rejected it, use the existing
  bounded natural-response repair format. Retain all authorized context and
  source bases; preserve final source, Frame and delivery checks. Packet/member,
  current-state, mixed-authority and structurally invalid envelopes retain typed
  repair. Any internal envelope returned during natural repair is still checked.

Natural PUBLIC repairs use the established prose/source/delivery guards. They do
not carry a typed proof of each prose assertion; this change does not close all
known semantic-comparator limitations or certify unsupported personal claims.

The final route debug now distinguishes the initial review reason, final repair
status and final delivery status. No extra provider attempt, language exception,
memory store, capability gate or TikTok retention change is introduced. This
follow-up starts from deployed main `9f4e2de`; it does not certify broader shared-
brain completion. After an authorized deployment, reuse the existing untagged
checksum prompt and `!bnl debug last route` to check the visible answer, run ID,
actual calls and confirmed delivery. If it is repaired, inspect the final status
alongside the original rejection instead of treating the latter as final.
