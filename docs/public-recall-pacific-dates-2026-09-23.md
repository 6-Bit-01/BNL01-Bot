# Pacific dates in original public Discord recall

The deployed layered-recall sequence recovered authentic statements but called
an earlier Pacific-evening exchange "that same morning." The named original
reader passed stored UTC timestamps to the model without a timezone. Its SQL
date bounds also attached a pytz timezone using `replace`, producing a historical
offset rather than the date's actual Pacific offset. A May-day query omitted
its first 53 minutes; winter and daylight-saving boundaries were also wrong.

The existing reader now localizes the two calendar midnights independently,
preserving 23-, 24- and 25-hour days. Original-message timestamps render in
Pacific time with explicit PDT/PST labels. Naive stored timestamps remain UTC;
aware timestamps retain their actual instant. Compact labels preserve the
bounded allowance for complete utterances. Invalid stored dates are marked
unavailable, without assigning the current date or inventing an event time.

This changes selection and prompt evidence, not stored source timestamps.
Existing subject, privacy, source-correction and send-time validation controls
remain in place. There is no schema, dependency, global clock, configuration,
provider, identity-link or gate change.

Five new regression tests cover naive UTC, Z and explicit offsets; both ends of
winter/summer days; spring/fall transitions; the repeated fall hour; malformed
dates; and actual direct/batch provider prompts across public/sealed and packet
on/off routes. The existing source-change test now also checks timestamp edits.
Before the repair, the focused reproduction produced 16 failed subcases.
Provider and Discord transports are fixtures: these checks prove selected
originals, dates, attribution and delivery controls, not live model semantics.

After the normal owner-authorized merge/deployment, a targeted existing-source
reply must place the previously supplied UTC-evening originals on their Pacific
date. Preserve the accepted quotation/person-switch results. Do not repeat the
inventory or whole seven-turn batch. The separately prepared missing-source
receipt, learned identity, natural memory/consumer acceptance and unassigned
non-generation latency remain in the completion ledger.

Rollback is an ordinary revert and deployment; no data migration is involved.
