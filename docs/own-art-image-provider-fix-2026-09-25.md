# Own-art image request correction

## Follow-up: omit the delivery override

The 22:45 UTC private preview on `48a225f96dc5013ad59cb0956f26fca491e925f3`
completed its concept call, then received HTTP 400 with the provider message
`Image delivery mode is not supported.` No image was saved or published.
The previous fix added `delivery: inline`; that unnecessary setting caused
this rejection. The current request omits both delivery and MIME overrides
and accepts the provider's default inline output through the existing decoder.

The published schema lists delivery values, but that does not prove live
support. Google's image-generation examples omit this setting; an existing
firsthand report on Google's developer forum reproduces the exact error and
reports inline output when the field is omitted. The provider-boundary test
now reproduces that rejection instead of accepting every mocked request.

This follow-up changes only the image request. It adds no runtime function,
gate, retry, model switch, conversion, or publication rule. A successful
private image remains unobserved. The existing concept can be reused for the
next private image check without another concept-generation call.

Reference: https://discuss.ai.google.dev/t/response-format-delivery-mechanism-seems-off/177260

## Earlier format correction

Release `6a8be5ec26ff32ca4fd6c1b93831caa7f9fd0072` was observed active/running.
At 2026-09-25 22:10 UTC, the private preview
made one successful concept call and one rejected image call (HTTP 400).
Nothing was published and no image was saved. The original image error body
was discarded, so the exact original provider explanation is unavailable.

Google's current v1 OpenAPI schema only permits `image/jpeg` as an explicit
`ImageResponseFormat.mime_type` override. The old client sent `image/png`, a
concrete contract defect consistent with the 400. PNG is not a product
requirement. The format correction omits the MIME
override. It accepts PNG or JPEG, verifies the
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
