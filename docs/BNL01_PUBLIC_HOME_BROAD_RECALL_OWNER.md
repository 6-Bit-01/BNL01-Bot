# Public-Home Broad-Recall Owner

This route promotes the accepted shared-brain packet selector for one narrow
conversation family: direct, first-person broad-profile requests in the
configured `public_home` channel.

It does not enable global Memory Governance, Relationship v2, Active
Engagement, or any queue capability. The established response is still
generated first and remains the fallback whenever packet construction,
factual-prompt ownership, candidate evaluation, guard checks, source
revalidation, or delivery preparation fails.

## Default-off gate

The route is effective only when all three values are present:

- `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED=true`
- `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_GUILD_IDS=<one guild id>`
- `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_CHANNEL_IDS=<one public-home channel id>`

The scoped synthesis canary and the public-home owner are mutually exclusive.
If both enable switches are true, synthesis fails closed with
`authority_conflict`. The public-home owner also fails closed when a global
Memory Governance, Relationship v2, or Active Engagement live gate is active,
or when the packet and response-assessment shadows are unavailable.

## Authority and safety

- The governed packet is the only stored factual prompt owner for this route.
- Competing legacy factual context is replaced before the candidate call.
- Current-turn and exact-reply authority remain ahead of stored memory.
- The subject is always the requesting member; cross-member facts remain
  ineligible.
- Relationship stays tone-only and is not rendered as factual evidence.
- BNL-authored derivatives cannot corroborate themselves.
- Every candidate is revalidated before send; any failure returns to the
  established response.

## Diagnostics and rollback

Content-free synthesis receipts record `authority_mode` as either
`scoped_canary` or `public_home_broad_recall_owner`. Owner diagnostics report
the requested/effective state, authority mode, scope status, kill switch,
route counts, selection/fallback totals, revalidation, prompt ownership, guard
outcomes, and live applications.

Rollback is the independent kill switch:

1. Set `BNL_PUBLIC_HOME_BROAD_RECALL_OWNER_ENABLED=false` or remove it.
2. Restart the bot.
3. Confirm diagnostics show the owner as requested/effective `off`.

With the gate off, no packet candidate is generated and the established path
is preserved byte-for-byte.
