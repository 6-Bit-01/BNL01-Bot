# Canon entity account-binding lifecycle v1

## Purpose

`canon_entity_account_binding_lifecycle_v1` completes the approved write side
of `canon_entity_account_binding_v1`. It links one Discord account to one
existing canon entity so the packet can decide whose canon applies to that
account. The link does not merge entities, create aliases, supply interaction
evidence, or declare a relationship.

The configured `BNL_OWNER_USER_ID` in the configured
`BNL_PRIMARY_GUILD_ID` is also treated as the explicit same-platform binding
for 6 Bit. A stored binding lifecycle decision takes precedence over that
runtime binding. Display names remain reversible hints only.

## Trust and storage

The `canon_entity_account_bindings` table is append-only. Every bind and
retirement is a new immutable revision with:

- exact configured-owner and primary-guild authorization;
- a numeric Discord account ID and an existing canon entity ID;
- an HMAC receipt using `BNL_DECLARED_CANON_AUTHORITY_SECRET`;
- optimistic expected-revision checks for retirement;
- idempotent operation IDs derived from the authenticated Discord command;
- one active entity per Discord account, with collisions rejected.

Startup creates the empty schema but creates no binding rows. A retired
binding remains as history and blocks display-name fallback for that account.
Stored authority and revision chains are revalidated whenever the packet reads
the binding and again before a generated response can be selected.

## Owner controls

The existing `!bnl canon` command owner in `#research-and-development` adds:

```text
!bnl canon binding-preview
!bnl canon bind-account {"account_id":"<discord-id>","entity_id":"call_em_bini","reason":"<owner reason>"}
!bnl canon retire-binding {"binding_id":"<binding-ref>","expected_revision_id":"<revision-ref>","reason":"<owner reason>"}
```

Replies and previews never echo the Discord account ID. Preview returns only
aggregate entity/state counts and opaque binding/revision references.

## Call'em Bini and Cache Back

Call'em Bini and Cache Back remain distinct canon entities. Binding a Discord
account to `call_em_bini` makes approved claims whose subject—or typed
relationship endpoint—is Call'em Bini applicable to that account. Those
claims stay in the canon lane and never count as Discord activity or a member
profile point.

Any relationship between Call'em Bini and Cache Back must still be added as a
separate owner-approved Declared Canon relationship with typed subject and
object endpoints. The binding command does not infer, copy, or approve the
review-only website origin sentence.

## Response coherence

When a question explicitly asks whether two identities are the same or asks
for their relationship, BNL may state the supported distinction once in one
plain sentence. It must not repeat negative identity wording or turn the
distinction into glitch/desync theater. Eligible Discord activity remains the
main profile evidence; canon identifies who that activity belongs to. If the
bound account has no eligible Discord activity, an exact current Declared
Canon relationship may answer that identity question by itself, but it cannot
imply participation, history, behavior, or interaction that was not observed.
