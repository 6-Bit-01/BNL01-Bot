# Relay historical-source validation repair

## Reproduced failure

A September 25 private full-Relay probe received HTTP200 from Gemini, then
BNL rejected the draft with `lane_validation_failure / missing_residual_anchor`.
The selected source was `published_journal` and the lane was `residual_echo`.
The capture recorded no publication and did not retain generated prose; it
proves the rejection path, not semantic approval of that particular draft.
Earlier HTTP503 failures remain separate provider errors.

The mandatory vocabulary test originated in commit
`b15fa74f251ab045f4b6d89bcbb78a3e50668a93`, merged through
[PR141](https://github.com/6-Bit-01/BNL01-Bot/pull/141) on May 6 Pacific time.
It required one of six substrings: residue, echo, afterimage, archive, still on,
or recent. This was an invalid proxy for historical grounding. An approved
Journal callback could lack every substring; unsupported prose could contain
one. Previously successful code also contained this defect. Equality of selected
Gemini request functions never established correctness of the entire Relay path.

## Repair

The lane now uses the approved selector's historical source class and nonempty
context. The existing legacy low-signal path passes its existing source decision.
Generated wording supplies no authority. Source visibility, provenance refresh,
historical/current-claim checks, duplicate checks, transport acceptance, and
cursor advancement remain owned by their existing checks.

No extra generation request, retry, model change, schedule change, or gate change
is introduced. The historical lane remains a creative direction, with no required
words. A missing source is reported as `missing_residual_source`.

## Verification

Two transaction regression tests failed against the old implementation at the
same missing-anchor check. With this repair, a published Journal callback without
those words reaches an accepted mocked website response, retaining its source
basis and publication receipt. The same wording with a withdrawn Journal is held
before sending. Tests also cover approved historical source classes, missing or
excluded context, words that cannot substitute for a source, and the legacy caller.

Run the focused Relay tests and `make check` before publication. All provider and
website calls in these tests are mocked; no public test Relay is sent.

After normal deployment, verify the deployed revision and inspect one ordinary
Relay tick's retained `website_relay_attempts` outcome, `accepted_relay_id`, and
`website_published_at`. Generation success alone is not publication acceptance.
Carry forward provider failures, other validation failures, or transport failures
separately. No paid private probe or forced public publication is required.
