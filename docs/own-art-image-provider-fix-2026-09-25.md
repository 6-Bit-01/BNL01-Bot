# Own-art image request correction

Release `6a8be5ec26ff32ca4fd6c1b93831caa7f9fd0072` was observed active/running.
At 2026-09-25 22:10 UTC, the private preview
made one successful concept call and one rejected image call (HTTP 400).
Nothing was published and no image was saved. The original image error body
was discarded, so the exact original provider explanation is unavailable.

Google's current v1 OpenAPI schema only permits `image/jpeg` as an explicit
`ImageResponseFormat.mime_type` override. The old client sent `image/png`, a
concrete contract defect consistent with the 400. PNG is not a product
requirement. The corrected request omits the MIME
override and asks for inline image data. It accepts PNG or JPEG, verifies the
actual header and dimensions, and retains original bytes without conversion.
Private files and Discord attachments use the actual extension. Website
contract v2 carries `imageBase64` and `art.mimeType`; deploy the compatible
website first. The website keeps support for original v1 PNG packets.

Bounded redacted Google diagnostics now reach the private failure receipt and
log. Explicit 400/401/403/404 pre-generation rejection status pairs release
the new estimate; unknown responses, transport failures and server failures
retain it. This does not modify the original retained reservation or establish
the actual Google charge for that failed request. Missing zero-valued cached
and thought counters are accepted; core usage totals remain required.

Provider-schema and PNG/JPEG fixtures cover the corrected request, actual
MIME and filename, original-byte hashes, private receipt, Discord preparation,
and website handoff. Fixtures do not prove live provider or delivery success.
The next step is a fresh private preview. Keep artwork disabled until that
image has been inspected, then verify natural Discord and website receipts.
No Journal/Relay schedule, queue, payment, submission, daily image ceiling or
community image-generator policy changes in this fix.

Primary references, retrieved 2026-09-25:

- https://ai.google.dev/static/api/interactions-v1.openapi.json
- https://ai.google.dev/api/interactions-api-v1
- https://ai.google.dev/gemini-api/docs/image-generation
