# BNL-01 Unified Memory / Canon / Dossier / Event-Source Architecture Audit (2026-05-28)

## Executive summary
This audit maps BNL-01’s current source lanes and proposes a future **unified, source-aware BARCODE Knowledge Layer**.

Current state in `BNL01-Bot` is functional but lane-fragmented: canonical identity/lore exists in system prompt text, durable user memory exists in DB tables, broadcast continuity exists in `broadcast_memory`, and public-context shaping exists via `conversations` and relay/ambient summarizers. These are not yet represented in a single authority-ranked source registry.

Primary recommendation: keep one shared intelligence layer, but implement it as **multi-lane, authority-ranked, visibility-gated records** rather than a flat memory bucket.

This PR intentionally does **not** implement runtime or schema behavior changes.

## Scope and inspection log
### Bot repository inspected
- `bnl01_bot.py`

### Related website repository requested for audit
- Repository: `6-Bit-01/barcode-network-site`
- Status in this environment: clone/fetch blocked by network policy (`403 CONNECT tunnel failed`), so direct file inspection of:
  - `src/content.ts`
  - `BARCODE_NETWORK_MASTER_SOURCE_OF_TRUTH_2026-05-27.md`
  - routes for `/database`, `/terminal`, `/radio`, `/releases`, `/transmissions`, `/queue`, `/admin/queue`
  could not be completed from this workspace.

### Consequence
Website sections below provide:
1) architecture-ready classification framework,
2) known website-source anchors from available project references, and
3) explicit follow-up checklist for PR #164 to reconcile implementation design with those existing references once repo access is available.

## Current source inventory (bot)

## A) System canon and identity source
- Source: `BNL01_SYSTEM_PROMPT` in code.
- Contains locked identity, canonical facts, role boundaries, restricted topics, and lore anchors.
- Classification:
  - `system canon`
  - authority tier A (highest)
  - visibility: internal prompt only
  - confidence: high (authoritative)
  - freshness: manually updated

## B) Conversation/logging lane
- Source: `conversations` table (`save_user_message`, `save_model_message`, and reads for context/relay/ambient).
- Used for immediate and recent context, relay source extraction, signal summary generation.
- Classification:
  - `public conversation context` (channel-policy filtered)
  - partially `website relay source`
  - authority low-medium depending on channel policy and recency
  - confidence variable
  - freshness high but quickly stale

## C) Durable user memory lane
- Sources:
  - `user_memory_facts`
  - `relationship_state`
  - `relationship_journal`
  - `user_habits`
  - `memory_tiers`
  - `maybe_add_memory_trace(...)`
- Classification:
  - `durable user memory`
  - `relationship memory`
  - `unsafe / source-blind / legacy` (for `legacy_unknown` traces in memory tiers)
- Notes:
  - Source metadata fields exist on `memory_tiers` (`source_role`, `source_origin`, `source_trust`, etc.).
  - Existing “legacy_unknown down-rank” behavior is a strong foundation for future authority-aware scoring.

## D) Broadcast memory / show-state lane
- Sources:
  - `broadcast_memory` table and helper functions
  - `build_broadcast_memory_context(...)`
  - `build_scoped_broadcast_memory_context(...)`
  - correction/resolve/supersede controls in `#bnl-broadcast-memory`
- Classification:
  - `show-state / episode continuity`
  - operator-ingested continuity source with correction workflow
- Notes:
  - Includes `target_show_date`, `valid_until`, `override_span_count`, supersession links.
  - Already behaves like an operational override lane (good fit for future tier D/E authority).

## E) Ambient and relay generation lanes
- Sources:
  - `generate_dynamic_ambient(...)`
  - `generate_dynamic_website_relay(...)`
  - `get_recent_signal_summary(...)`
  - relay lane selection and strength gating
- Classification:
  - `ambient/curiosity influence`
  - `website relay source`
  - low authority context summarization lane
- Notes:
  - Strongly useful for “current room temperature,” not canon.

## F) Diagnostics lanes
- Slash commands:
  - `/bnl_memory_check`
  - `/bnl_status`
  - `/bnl_source_check`
  - `/bnl_context_check`
- Classification:
  - `diagnostics only`
- Notes:
  - Read-only posture and safety truncation in several outputs are compatible with future source-index diagnostics.

## G) Not connected / missing sources
Current bot code does **not** show first-class connected lanes for:
- `website/dossier canon`
- `release/catalog canon`
- `queue/session operational state`
- `payment operational state`
- `artist/member dossier data`

These appear as intended future integrations, not present unified runtime sources today.

## Bot memory/context map (function-level)

## Prompt assembly today (observed)
High-level observed build path:
1. `BNL01_SYSTEM_PROMPT` base.
2. Current message and immediate context.
3. `build_user_memory_context(...)` for user durable/relationship/habits tiers.
4. `build_broadcast_memory_context(...)` when relevance trigger is met.
5. Channel-policy controls affect what public/internal context is eligible.

## Memory write/read map
- Writes:
  - user and model messages -> `conversations`
  - relationship updates and habits updated on each save
  - `maybe_add_memory_trace(...)` into `memory_tiers`
  - broadcast note ingestion into `broadcast_memory`
- Reads:
  - prompt assembly reads user memory + broadcast memory
  - ambient/relay build from public-filtered recent conversation lanes
  - diagnostics aggregate source counts and source metadata health

## Existing source-safety controls
- Channel policy resolution gates context/relay inclusion.
- Broadcast memory supports `public_safe` and correction/supersession.
- Diagnostics indicate source metadata availability and legacy status counts.

## Website canon/dossier/source map (framework + pending verification)
Because direct website repo inspection was blocked, this section defines required classification targets and expected source-lane boundaries to validate in PR #164:

## Verified website-source anchors to incorporate later
Even though direct clone/fetch failed in this environment, available project references already establish that the website side is **not blank** and already includes canon/dossier/release structure:

1. `src/content.ts` includes a structured `databasePage` model with:
   - designation prefixes (`EN`, `PE`, `SP`, `IF`, `PD`)
   - categories (`Entity`, `Personnel`, `Sponsor`, `Interface`, `Production`)
   - status, clearance, origin
   - entries with fields including `id`, `name`, `image`, `category`, `status`, `clearance`, `role`, `origin`, `summary`, `tags`, `notes`, `link`, `files`

2. `src/content.ts` includes public-facing dossier/entity records, including references for:
   - 6 Bit
   - BARCODE Radio
   - Discord Community
   - Auxchord
   - TikTok Live
   - BNL-01
   - BARCODE personnel/mods
   - BARCODE entities such as Mac Modem, DJ Floppydisc, Cache Back, Sheila, Cliff, Studio Rats, 9 Bit, GALAKNOISE
   - Sponsor/interface entries such as Oreaganomics and Vouch’d

3. `src/content.ts` includes release/catalog anchors, including:
   - BARCODE: Signal Breach
   - BARCODE Vol. 1
   - archived/redacted BARCODE Vol. 0 material

4. `BARCODE_NETWORK_MASTER_SOURCE_OF_TRUTH_2026-05-27.md` defines the intended BNL memory source-safety lanes:
   - Canon memory
   - Project/source-doc memory
   - User/member memory
   - Artist/dossier memory
   - Channel-scoped context
   - Session/show memory
   - Temporary conversation memory
   - Restricted/admin memory
   - BNL internal operational state

5. The same master source doc defines source labels:
   - `source_doc`
   - `github_source`
   - `discord_message`
   - `admin_note`
   - `user_declared`
   - `inferred_low_confidence`
   - `queue_event`
   - `payment_event`
   - `show_event`
   - `manual_mod_note`

6. The same master source doc defines visibility levels:
   - `public_safe`
   - `bnl_safe`
   - `admin_only`
   - `restricted`
   - `do_not_repeat`

7. The same master source doc defines confidence levels:
   - `verified`
   - `likely`
   - `unverified`
   - `stale`
   - `contradicted`

8. The same master source doc defines dossier types:
   - Artist dossier
   - Member/community dossier
   - Mod/staff dossier
   - BARCODE collaborator dossier
   - Character/lore dossier
   - Sponsor/ad dossier
   - Track/song dossier
   - Show/session dossier

9. The same master source doc defines eventual dossier fields, including:
   - public display name
   - Discord identity
   - TikTok handle
   - email/contact if provided
   - submitted tracks
   - played/completed/removed/no-show history
   - Spotlight history
   - Wheel Chosen history
   - Priority Signal/payment history where appropriate
   - station consideration status
   - permissions/release status
   - admin notes
   - BNL-safe memory notes
   - restricted notes

## Required website source classes
1. **Public canon**
   - BARCODE world entities/program pages approved for public consumption.
2. **Public dossier/entity data**
   - artist/community/character/sponsor profiles safe for public display.
3. **Internal/restricted dossier data**
   - notes that must not be publicly repeated by BNL.
4. **Release/catalog data**
   - tracks, releases, metadata, status fields.
5. **Operational radio/queue copy**
   - UX/marketing text that should not automatically become canonical fact.
6. **Queue/session/payment state**
   - dynamic operational data requiring freshness metadata.
7. **Admin-only data**
   - must remain admin/restricted visibility.
8. **Future account/identity data**
   - linkage records and ownership proofs.

## Required validation checklist once site repo is available
- Confirm each route/data module is tagged with source type, visibility, authority, freshness class.
- Separate “public copy” from “operational state” from “canon assertions”.
- Confirm queue/payment signals are event records, not canon facts.
- Confirm any admin route payloads are never exposed to public prompt lanes.

## Current disconnects
1. No shared source registry unifying all lanes under one authority model.
2. Website canon/dossiers/releases are not currently first-class prompt sources in this repo, even though existing project references indicate website-side structures already exist.
3. Queue/payment/event state not currently normalized as source-aware knowledge records.
4. Memory tiers still contain legacy-source records requiring cautious down-rank.
5. Public chatter and user memory lanes can influence tone/context but are not fully entity-linked with provenance.
6. Website repo file-by-file validation was not completed in this Codex environment; however, follow-on implementation should reconcile architecture with existing `databasePage`, release catalog anchors, and master-source taxonomy rather than inventing a net-new dossier system from scratch.

## Current risks
1. **Over-centering risk** (e.g., recently tested entities like Cliff) due to recency/chatter prominence.
2. **Source confusion risk** where public chatter may be mistaken for canon absent explicit provenance labels.
3. **Visibility leakage risk** if future admin/internal notes are not hard-gated by visibility policy.
4. **Staleness risk** for operational queue/payment/show data if freshness isn’t embedded in source records.
5. **Correction lineage risk** outside broadcast lane if future dossier/event facts lack supersession history.
6. **Architecture drift risk** if future PRs model dossiers/events without first reconciling to known website structures in `src/content.ts` and `BARCODE_NETWORK_MASTER_SOURCE_OF_TRUTH_2026-05-27.md`.

## Future dossier requirements (first-class objects)
Dossiers should be structured records, not plain memory blobs.

## Dossier types
- Artist dossier
- Member/community dossier
- Mod/staff dossier
- BARCODE collaborator dossier
- Character/lore dossier
- Sponsor/ad dossier
- Track/song dossier
- Show/session dossier

## Core dossier fields
- `dossier_id`, `dossier_type`, `entity_id`
- identity fields: display name, aliases, Discord ID, TikTok, contact (restricted)
- lifecycle fields: created/updated, source refs, freshness
- participation fields: submissions, play/remove/no-show history
- feature fields: Spotlight history, Wheel Chosen history, station consideration
- permissions fields: release/permission status
- note partitions:
  - public profile fields
  - BNL-safe notes
  - admin-only notes
  - restricted/do-not-repeat notes
- trust fields: authority tier, confidence score, verification status

## Future event-source requirements
## Event types to model as source records
- submit/accept/reject/load/now-playing/finished/removed
- no-show / missing artist
- Wheel spin owed / Wheel Chosen
- Priority checkout started / payment confirmed / paid-needs-attention
- Spotlight marked
- station consideration marked
- permission/release status updated
- artist checked in
- account claimed
- show archived
- recap generated

## Event record minimum schema
- `event_id`, `event_type`, `occurred_at`, `ingested_at`
- `source_system` (queue, admin queue, payment, broadcast memory, manual mod note)
- linked entities (`artist_id`, `track_id`, `show_id`, `session_id`)
- `visibility` (public, bnl_safe, admin_only, restricted, do_not_repeat)
- `authority_tier`, `confidence`, `freshness_class`
- supersession/correction fields (`supersedes_event_id`, `corrected_by`, `reason`)
- `public_usable` boolean separate from “exists in system”

## Authority hierarchy (recommended)
Adopt explicit rank ordering (higher wins conflicts):

A. System prompt / locked identity canon  
B. Source-of-truth markdown docs  
C. Website public canon/dossiers/releases/entity pages  
D. Verified queue/session/payment/show runtime state  
E. Broadcast memory + show-state overrides  
F. Operator/mod-approved notes and corrections  
G. Dossier BNL-safe notes  
H. Public conversation/chatter  
I. User/member relationship memory  
J. Ambient/curiosity impressions  
K. Legacy/source-blind traces

## Conflict resolution rules
- Higher authority always wins.
- Chatter cannot override canon.
- Runtime operational state can override generic copy.
- Broadcast overrides can override normal schedule for affected dates.
- User relationship memory cannot override BARCODE canon.
- Ambient and legacy traces never become hard facts without verification.

## Visibility / confidence model
## Visibility classes
- `public` (BNL may say externally)
- `bnl_safe_internal` (BNL may use for guidance but not directly quote as fact unless policy allows)
- `admin_only` (never exposed to standard users)
- `restricted` (limited role access)
- `do_not_repeat` (can influence moderation/operator workflows only)

## Confidence classes
- `verified` (authoritative source)
- `probable` (trusted lane but awaiting confirmation)
- `unverified` (low confidence)
- `legacy_unknown` (down-ranked)

## Freshness classes
- `real_time` (seconds/minutes)
- `session_fresh` (current show/day)
- `recent` (days/weeks)
- `archival` (long-term historical)
- `stale_review_needed`

## Recommended unified architecture (do not implement yet)
## 1) Central source registry
Introduce a registry model (logical design):
- `source_index`
  - one row per source feed/object family
  - source type, authority tier, default visibility, freshness policy

## 2) Knowledge item store
- `barcode_knowledge_items`
  - normalized, provenance-rich statements or structured records
  - references source_index, entity links, confidence/freshness/visibility

## 3) Entity + dossier layers
- `entity_index` (canonical entity IDs and aliases)
- `dossier_records` + `dossier_sections` (public, bnl-safe, restricted partitions)

## 4) Event ledger
- `event_source_records`
  - immutable-ish event intake with correction/supersession chain

## 5) Optional materialized prompt views
- read-only views for prompt assembly by lane and authority (e.g., `v_prompt_canon`, `v_prompt_runtime_state`, `v_prompt_low_authority_context`).

## Website integration options (choose in PR #164)
1. **Website API pull (recommended long-term):** freshest runtime state; strict auth needed.
2. **Static JSON export artifact:** deterministic and simple deployment; less fresh.
3. **Bot-side import/cache job:** resilient with offline cache; needs explicit invalidation/freshness handling.
4. **Git-based source fetch at deploy time:** simple bootstrap, weaker runtime freshness.

Recommended phased approach: start with signed/static export or read-only API subset for public-safe canon+dossiers, then add runtime queue/payment lane separately.

## Source reference storage
Each knowledge item should store:
- `source_type` (system_prompt, source_doc, website_dossier, release_page, queue_event, payment_event, broadcast_memory, public_chatter, mod_note, user_memory, inference)
- `source_locator` (URL/path/row-id/message-id)
- `source_observed_at`
- `source_authority_tier`
- `visibility`
- `confidence`
- `freshness`
- `entity_links[]`

## Corrections and theory handling
- Never mutate history silently; append correction/supersession records.
- Represent theories/rumors as separate low-authority items (`source_type=public_chatter`, `confidence=unverified`).
- Require explicit promotion workflow to elevate rumor->canon.

## Anti-over-centering controls
- cap per-entity mention injection from low-authority lanes per prompt window.
- require multi-source corroboration before promoting high salience.
- time-decay weight for chatter-heavy entities.
- prioritize entity diversity when selecting optional context examples.

## Prompt assembly recommendations (future)
Recommended source-aware order:
1. Current user message
2. Immediate conversation/channel context
3. Current runtime operational state (if relevant)
4. Active show-state/broadcast memory (if relevant)
5. Relevant entity/dossier/canon facts
6. Relevant release/catalog/source-doc facts
7. Public chatter (if relevant, low authority)
8. User relationship memory (if relevant)
9. Ambient/curiosity traces (style only)

## Internal prompt source labels
Use explicit headers in assembled context blocks:
- `OFFICIAL CANON`
- `VERIFIED SITE DOSSIER`
- `CURRENT SHOW STATE`
- `QUEUE RUNTIME STATE`
- `PUBLIC CHATTER / LOW AUTHORITY`
- `USER MEMORY / PERSONAL CONTEXT`
- `LOW-CONFIDENCE INFERENCE`

## Public chatter boundary recommendations
- Public chatter is context, not canon.
- Chatter can suggest “topics people are discussing,” not assert institutional fact.
- No automatic write-back from chatter into canonical dossier fields.
- Chatter-derived claims require verification before future durable promotion.

## Dossier boundary recommendations
- Dossiers must be fielded/typed and visibility-partitioned.
- BNL-safe notes separate from admin-only/restricted notes.
- Permissions/release/payment-related details require explicit visibility policy.
- Personal identity/contact fields default restricted unless explicit consent and policy.

## Queue/payment/show-event boundary recommendations
- Treat events as operational ledger items with freshness/authority metadata.
- Runtime event state can drive near-term prompt behavior but does not rewrite canon history directly.
- Archive/recap synthesis should read from verified event ledger + approved dossier fields.

## Diagnostics recommendations (future, no implementation in this PR)
Proposed tools:
- `/bnl_source_index`
- `/bnl_dossier_check`
- `/bnl_entity_check`
- expanded `/bnl_memory_check`
- `/bnl_forget` (scoped correction workflow)
- `/bnl_canon_check`
- `/bnl_show_memory_check`
- `/bnl_public_context_check`

Diagnostic output principles:
- report existence/authority/visibility/confidence/staleness
- avoid raw dumps of restricted content
- show correction lineage and last-verified timestamps

## Future PR roadmap (recommended)
## PR #163 — Source index diagnostics
- read-only source-lane inventory, authority/visibility/confidence/freshness reporting
- no behavior change

## PR #164 — Website canon/dossier export or import v1
- begin by reconciling implementation with existing website references (`src/content.ts` structures + `BARCODE_NETWORK_MASTER_SOURCE_OF_TRUTH_2026-05-27.md` taxonomy)
- expose/import public-safe website canon/dossier/release entities
- read-only integration
- no chatter writes
- complete deferred website file-level validation from this audit (because direct clone/fetch was blocked in this environment)

## PR #165 — Unified source-aware prompt assembly v1
- inject ranked context blocks by source authority
- include internal source labels
- no raw transcript dumps

## PR #166 — Queue/show/payment event-source bridge audit + data contract
- map event payloads into event-source ledger design
- no production behavior change without explicit approval

## PR #167 — Public chatter context v1
- temporary, low-authority chatter context lane
- no memory writes

## PR #168 — Dossier diagnostics + BNL-safe notes design
- planning or first read-only `/bnl_dossier_check`

## PR #169 — Operator/mod source controls manual
- safe correction workflows, promotion/demotion rules, governance docs

## Explicit do-not-implement-yet guardrails
This audit intentionally does **not**:
- change `bnl01_bot.py` runtime behavior
- change DB schema/migrations
- add live commands/fetches
- alter broadcast memory logic/correction controls
- implement dossier import, queue/payment bridges, or account identity features
- alter website production behavior

Implementation is deferred to staged PRs above.
