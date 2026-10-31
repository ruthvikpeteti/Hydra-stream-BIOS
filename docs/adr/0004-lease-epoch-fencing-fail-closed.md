# ADR-0004: Lease-based epoch fencing, fail closed, with quiet periods

Status: Accepted

## Context
A partitioned active region cannot be told it has been replaced. The control plane cannot promote safely unless the old region has provably stopped writing.

## Decision
- The active region's gateway writes only while it holds an unexpired write lease (TTL `lease_ttl_ms`, renewed every `lease_renew_interval_ms`), measured on its **monotonic clock** from the moment the renewal was sent.
- The control plane grants a new epoch only after the old lease is released (committed) or `last_granted_lease_expiry_upper_bound + max_clock_drift_ms` has passed. A new Raft leader first waits a quiet period of `lease_ttl_ms + max_clock_drift_ms`.
- Every sink rejects records below `max_epoch_seen`. Divergent suffixes are quarantined (INV-9).
- Without quorum, no renewals occur, so all regions stop accepting writes within a TTL (INV-6).

## Consequences
- Safety over availability: control-plane quorum loss stops writes everywhere within one TTL. This coupling is deliberate. Operators lengthen `lease_ttl_ms` for more tolerance to control-plane blips, at the cost of longer RTO.
- RTO has a floor of detection + fence wait. A voluntary release removes the fence wait when the old region is alive but write-unavailable.
- Safety depends on bounded monotonic clock drift over one TTL (documented assumption, SECURITY §4, §10).
- Renewals are batched per region to limit Raft load.

## Alternatives
- Fencing by revoking Kafka ACLs: rejected as primary mechanism (cannot reach a partitioned region). Used only as best-effort defence.
- Continue-writing-without-quorum modes: rejected (permits two leaders).
