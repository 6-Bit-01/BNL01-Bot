# Test-channel baseline restoration — 2026-09-06

## Owner direction and current state

`bnl-testing` mirrors `barcode-bot` conversation. Anyone already admitted to the
Discord room can converse without a bot-specific participant permission. It can
read eligible public information. Its own contributions and derived test content
remain sealed and must not enter public memory, Journal, Relay or show history.
Other public rooms retain their existing tag/direct-reply admission rules.

Restore this baseline before continuing shared-brain semantic-ownership work.
The unpublished parser experiment is rejected and excluded. Do not revert all
ten PRs, rebuild memory, change queue/show behavior, or activate global gates.

Verified bot main/deployed receipt: `c2b40ce6bbca60fc92c1a504d0f903078de4092d`,
tree `1c4b002ce0ce24493632eabb9559ed44991c5306`. The latest supplied service
receipt confirms startup, Discord connection and provider success. The checksum
run `sbsr_e87f52fc76ae40ae8746606d7e2244ba` still ended unsent after three calls,
two corrections and 10,343 tokens. Its automatic referent/clarification decision
flowed into the experimental typed response contract; raw drafts were not supplied.

## What the last ten merged PRs did

| PR | Change and disposition |
|---|---|
| [498](https://github.com/6-Bit-01/BNL01-Bot/pull/498) | Stopped an unrelated Moment automatically overriding public-knowledge answers; later event-dependency gap addressed in 499. Retain useful evidence composition. |
| [499](https://github.com/6-Bit-01/BNL01-Bot/pull/499) | Distinguished addressee/subject and event authority, but expanded language classification in shared Frame code. This was not exclusively test-room code. |
| [500](https://github.com/6-Bit-01/BNL01-Bot/pull/500) | Added typed output and extensive prose checks, with one-call/no-correction withholding. Also made valuable shared room-admission/directness/reply and episode fixes. Whole-PR reversal would undo approved public behavior. |
| [501](https://github.com/6-Bit-01/BNL01-Bot/pull/501) | Restored repair after live silence; repair validation/schema defects were subsequently fixed in 506. |
| [502](https://github.com/6-Bit-01/BNL01-Bot/pull/502) | Removed sealed participant allowlists. Initially too broad across rooms; 503 restricted room identity. Do not restore participant restrictions. |
| [503](https://github.com/6-Bit-01/BNL01-Bot/pull/503) | Bound the channel-wide mirror to its configured Discord channel ID; also changed a shared canon packet helper. Retain channel identity. |
| [504](https://github.com/6-Bit-01/BNL01-Bot/pull/504) | Sent passive sealed turns through the experimental packet owner while passive public-home turns used normal generation. This is the principal response-path divergence. |
| [505](https://github.com/6-Bit-01/BNL01-Bot/pull/505) | Added a generic-you exception to typed public prose review. It did not solve the broader semantic-authority problem. |
| [506](https://github.com/6-Bit-01/BNL01-Bot/pull/506) | Fixed repair parsing/evidence validation, directness and actual call/delivery accounting. Some helpers also serve public/direct-payload recovery; retain those repairs. |
| [507](https://github.com/6-Bit-01/BNL01-Bot/pull/507) | Added task-local public review and narrowly eligible natural repair. Live false ambiguity prevented eligibility; the fixture paraphrased away “it” and disabled upstream Context V2. Some decoder changes serve shared recovery too. |

The ten are 498–507. No blanket claim that public source behavior was untouched
is justified: 499/500 and shared helpers in later PRs changed public-reachable
code. Saved September 6 runtime flags, however, scoped the experimental ordinary
route only to testing channel `1500225396900237363`; public `barcode-bot`
`1341835523249016942` was outside that scope. The 06:47 capture confirms the
deployed commit but does not repeat the complete environment or prove current
public conversational behavior.

## Narrow restoration

1. Use the existing `BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED=false` rollback.
   Keep memory, Context, Moment, Assessment, Journal, Relay, TikTok and queue
   settings intact. No source/data rollback or PR is required for this switch.
2. Repair the existing read boundaries: sealed conversation can retrieve the
   same eligible public continuity and public-safe durable memory projection.
   Actual channel policy and write ownership remain sealed. Operator status
   does not widen this ordinary mirror conversation into private admin sources.
3. Reuse the normal-generation route for both rooms. Do not add another route,
   participant allowlist or English parser. Continue the shared-brain ownership
   correction separately after baseline runtime confirmation.

The offline current-main probe used the exact checksum request, enabled Context
V2, retained unrelated history, and the existing shadow switches. With only the
ordinary packet switch off, both policies admitted passive batching, bypassed
typed execution, used `get_gemini_response` once and delivered the same fixture
answer through the real guards. External generation was simulated. Existing
heuristic ambiguity remained in both policies; the probe proves route/delivery
parity, not infallible live Gemini interpretation.

The code correction changes only sealed reads. Existing public target conditions,
participant admission, generation, privacy/write contracts and source projections
are unchanged by this PR. Tests cover public-to-sealed SQL and final selection,
the reverse exclusion, other private rooms, public-safe durable reads for normal
members and operators, unchanged memory rows and sealed governance receipts.

## Deployment and live verification

The owner authorized restoring the test baseline. This workspace has no VPS
connection, so the operator must apply and return the runtime receipt. Do not
claim this setting has changed until the running process confirms it.

The existing switch can be disabled immediately on current main:

```bash
sudo install -d /etc/systemd/system/bnl01.service.d
sudo tee /etc/systemd/system/bnl01.service.d/zzzz-bnl-testing-baseline.conf >/dev/null <<'EOF'
[Service]
Environment="BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED=false"
EOF
sudo systemctl daemon-reload
sudo systemctl restart bnl01
```

After the read-boundary PR is merged, use the normal deployment block:

```bash
cd /home/ubuntu/bnl01
git pull origin main
sudo systemctl restart bnl01
sudo systemctl status bnl01 --no-pager -l
git rev-parse HEAD
PYTHONPATH=tests ./venv/bin/python -m unittest test_conversation_context_v2 test_adaptive_memory_lifecycle test_route_memory_governance test_conversation_batching
```

Check the running process, rather than trusting a drop-in filename:

```bash
sudo /home/ubuntu/bnl01/venv/bin/python - <<'PY'
import pathlib, subprocess
pid = subprocess.check_output(['systemctl', 'show', 'bnl01', '-p', 'MainPID', '--value'], text=True).strip()
values = dict(item.split(b'=', 1) for item in pathlib.Path('/proc', pid, 'environ').read_bytes().split(b'\0') if b'=' in item)
key = b'BNL_ORDINARY_CHAT_SINGLE_PACKET_ENABLED'
print('MainPID=' + pid)
print(key.decode() + '=' + values.get(key, b'<unset>').decode())
assert values.get(key, b'').lower() not in (b'1', b'true', b'yes', b'on'), 'Existing environment override still enables experimental route'
PY
```

After the service logs `BNL-01 online as`, reuse the exact untagged checksum
question in `bnl-testing`, then `!bnl debug last route`. Confirm a visible answer,
normal generation and no new ordinary-packet response run. Preserve a current
public route receipt from existing traffic to establish runtime parity without
manufacturing a public test. The private-read tests prove boundaries locally;
runtime acceptance still requires the deployed version and actual response.

Keep the switch off during the subsequent shared-brain repair. Re-enabling the
experimental owner is a later explicit decision after corrected handoffs are
verified. Removing the new drop-in would restore the previous environment, but
is not part of this restoration. Preserve all databases and public settings.
