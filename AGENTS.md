# BNL01 Bot — Agent Instructions

## Owner identity privacy

- In BARCODE-facing code, tests, fixtures, documentation, prompts, seed data,
  logs, diagnostics, and UI copy, identify the project owner only as **6 Bit**
  when a human-readable BARCODE identity is required.
- Never hard-code, infer, retrieve, emit, or preserve the owner's real-world
  personal or legal name.
- Scope owner-only controls with `BNL_OWNER_USER_ID` or another opaque stable
  identifier, never a personal name.
- Keep public identity separate from private authority. Public BNL output may
  recognize 6 Bit as an artist, host, and founding BARCODE figure, but must not
  expose private account, owner, controller, admin, operator, or infrastructure
  facts.
- If private source evidence contains a real-world owner name, treat it as
  ineligible for BARCODE/public projection and redact or normalize the label at
  the existing governed read boundary.
- Use clearly fictional, neutral labels such as `Test Member` in automated
  fixtures.

## Change discipline

- Extend the existing conversation, memory, governance, Journal, Relay, and
  source-authority owners. Do not create parallel systems.
- Keep global memory, Relationship, Active Engagement, and queue-production
  gates off unless a newer explicit owner decision authorizes the exact gate.
- Preserve unrelated work and make the smallest coherent change that satisfies
  the requested scope.

## Verification

- Run focused tests for the changed subsystem.
- Run `make check` before publishing a runtime or test-suite change.
- Confirm forbidden private identity data is absent from the current tracked
  tree without adding that data to a scanner, fixture, or policy file.
