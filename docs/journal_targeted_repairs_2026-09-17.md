# Journal targeted repairs

The September 16 Pacific usage report recorded 711,635 Journal tokens across
19 calls. The supplied service logs showed a daily preparation held for
`public_leak_pattern`, two weekly preparations held for
`undeclared_context_use`, and a later weekly preparation held for unavailable
local budget. These logs establish repeated unsuccessful preparation cycles;
they do not identify every paid call or the exact rejected sentences.

The existing repair path gave the model a broad rejection reason, asked it to
rewrite the complete article, and supplied only the first 6,000 characters of
the previous response. That prefix could cut off trailing `metadata.contextUses`
and citations. We cannot establish whether a particular live response was cut
off because rejected prose is intentionally not retained in attempt ledgers.

## Change

`JOURNAL_REPAIR_VERSION` is `journal-targeted-repair-1`.

- Repairs receive the complete preceding response. Valid JSON is compacted
  instead of truncated, preserving the original schema and all trailing fields.
- The repair instruction follows the source packet and asks for corrections to
  failing passages while retaining grounded prose, voice, and valid citations.
- The existing validator supplies up to twelve field pointers for the two
  observed rejection types. Leak pointers identify a category and character
  span. Context pointers identify the affected field and, when applicable, the
  supplied lane reference and sentence index.
- Title and excerpt repairs explain that context declarations belong to body
  sections. A topic match alone never authorizes an invented claim or citation.
- `journal_repair_requested` logs the repair version, attempt number, reason,
  and structural field/check names. It never logs rejected prose, identity
  literals, source text, lane references, or full prompts.

Validation verdicts, privacy rules, source eligibility, release checks, the
four-attempt ceiling, scheduler backoff, protected reservations, and spending
limits are unchanged. No schema migration or Ballad change is required.

A complete repair input can be longer than the old truncated prefix. Existing
request budgeting still applies. This change removes a concrete repair defect;
tests with controlled responses cannot establish live model success or savings.

## Verification

Focused regressions exercise a response whose context metadata falls beyond
character 6,000, a repair that preserves the article while adding its valid
context declaration, multiple leak locations, bounded diagnostics, cleared
diagnostics on success, and logs without rejected text. Existing Journal tests
continue to cover privacy, unsupported claims, publication, and retry behavior.

Run:

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_bnl_journal*.py'
make check PYTHON=.venv/bin/python
```

## Deployment and live evidence

After merging:

```bash
cd /home/ubuntu/bnl01 &&
git pull --ff-only origin main &&
venv/bin/python -c "from bnl_journal import JOURNAL_REPAIR_VERSION; assert JOURNAL_REPAIR_VERSION == 'journal-targeted-repair-1'; print(JOURNAL_REPAIR_VERSION)" &&
sudo systemctl restart bnl01 &&
systemctl show bnl01 -p ActiveState -p MainPID -p ActiveEnterTimestamp
```

Allow the existing scheduler to resume eligible pending work. Do not issue
repeated Run Now requests or reset usage accounting to test the change.
After its next preparation cycle, collect this read-only evidence:

```bash
sudo journalctl -u bnl01 --since "1 hour ago" --no-pager \
  --grep='journal_repair_requested|journal_preparation_finished' -n 30
```

If repair is needed, the repair log should identify
`version=journal-targeted-repair-1` and the failing fields. A clean first attempt
does not need a repair log. The preparation result establishes whether it
prepared successfully or still needs investigation; publication continues
through the existing release workflow.
