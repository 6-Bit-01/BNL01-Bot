# Gemini 503 investigation: provider detail capture

## Evidence and limits

The owner's read-only capture at 2026-09-25 11:46:28 UTC covers seven days of
recorded model attempts: 328 attempts, 312 successes, and 16 HTTP 503 failures.
Fourteen failures were Relay generations and two were Journal generations.
The first retained 503 in this bounded history was 2026-09-23 15:10:51 UTC
(08:10:51 America/Los_Angeles), before the PR574/575 Relay source changes.
This is the earliest error in the supplied window, not a lifetime first error.

The detailed recent window contains 25 Relay attempts: 13 provider failures,
8 publications, and 4 rejected generated candidates. Model success and public
publication are separate outcomes. Failures span conversation continuity,
finalized-show, and published-Journal sources; all three also generated
successfully during the window. The Journal failed at 2026-09-25 01:30:12 and
02:04:27 UTC before generating successfully at 02:49:59 UTC. Relay failure
continued at 11:10:48 UTC after the separately verified PR585 restart.

These observations support intermittent shared-provider failure over one
deterministically broken Relay source. They do not establish Google's internal
cause, capacity tier, billing condition, or a model-wide outage. Failed rows
contain zero reported tokens, not proof of zero billable processing. Relay
attempt history retains at most 100 rows per guild; provider history is scoped
to the requested window and includes all guilds. No extraction was truncated.

## Diagnostic correction

The existing classifier substitutes a generic server-error sentence for the
SDK's provider message. Keep that public/result behavior and add a private
`gemini_provider_server_error` log at the provider boundary for Relay/Journal
5xx errors. The log contains the actual model, route, reservation, attempt,
HTTP status, input character count, output ceiling, and bounded SDK fields:
message/status, ErrorInfo reason/domain, recognized request-ID headers, and
numeric Retry-After when supplied. Missing fields remain missing.

The SDK's separate message/status attributes are documented in its pinned
[v1.47.0 implementation](https://github.com/googleapis/python-genai/blob/v1.47.0/google/genai/errors.py).
Do not dump the raw exception, response body, request, ErrorInfo metadata, or
arbitrary headers. Credentials and URLs are redacted; payload-like or oversized
messages are omitted; message truncation is explicit. Images are excluded.
The additional diagnostic is independent of successful SQLite accounting.

There are no retry, fallback, cadence, budget, gate, schema, or publication
changes. The patch improves diagnosis; it does not fix or prove the cause of
the observed provider 503s. No live provider request is needed for its tests.

## Deployment evidence

After normal merge/deployment, use the next ordinary scheduled attempt. Read
`gemini_provider_server_error` records from the bnl01 service and preserve the
actual UTC timestamp, model, provider message, reason and request ID. If no
new failure occurs, detail capture remains unobserved; do not generate an
extra public Relay or declare the provider incident resolved from that alone.

Use the returned reason to choose a targeted remedy. Capacity/overload points
to bounded recovery or provider escalation; a specific request/model error
points to correcting that configuration. A generic message still requires
provider-side investigation using the captured identifiers; never invent a
more specific root cause than the returned evidence supports.
