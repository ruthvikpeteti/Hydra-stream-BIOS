# ADR-0007: Promotion eligibility from committed attestations, break-glass for lag only

Status: Accepted

## Context
At failover the old active region is unreachable, so the candidate cannot learn the true lag. Operators also need a way forward when no replica qualifies, without opening a path around legal policy.

## Decision
Eligibility is computed by the control-plane promotion engine from control-plane-committed, signed attestations. A pessimistic bound `lag_upper_bound_ms = attested_lag_ms + age` is used, and attestations older than `attestation_ttl_ms` are treated as unknown lag (ineligible). Policy checks (INV-5) are never waivable. Lag and attestation-staleness may be waived only by a dual-approved `WAIVE_LAG` break-glass, which records the estimated loss window. Automatic and manual promotions share one evaluation path.

## Consequences
- False negatives: an idle source makes the bound over-estimate lag, so a perfectly synced replica may be refused. Safe direction; resolved by break-glass.
- The replicator's attestation cadence directly drives RTO and eligibility. `attestation_interval_ms` and `attestation_ttl_ms` are important tunables.
- No "best available replica" automatic fallback.

## Alternatives
- Candidate self-reports lag: rejected (cannot know the source head).
- Always promote the most up-to-date replica: rejected (may violate the RPO objective and does not address policy).
