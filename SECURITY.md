# SovereignStream — Security and Trust Boundaries

Related: [ARCHITECTURE.md](ARCHITECTURE.md) · [API_SPEC.md](API_SPEC.md) · [DATA_MODEL.md](DATA_MODEL.md) · [FAILURE_MATRIX.md](FAILURE_MATRIX.md).

## 1. Goals and non-goals

Goals: (1) data never stored, processed or transported outside policy (INV-1, INV-2, INV-7); (2) at most one writer per topic (INV-3); (3) every authoritative decision is attributable and auditable (INV-8).

Non-goals in Phase 0: defending against a compromise of a majority of control-plane members; defending against a malicious cloud provider with hypervisor access; DoS protection beyond rate limits.

## 2. Trust boundaries

| ID | Boundary | Crossing | Controls |
|---|---|---|---|
| B1 | Client ↔ Application gateway | Untrusted → trusted | TLS 1.3, OAuth2/OIDC bearer tokens (JWT) with per-topic scopes, rate limits, size limits, schema/envelope validation. Clients have no Kafka credentials. |
| B2 | Gateway ↔ regional Kafka | Same region | mTLS, a per-region gateway principal with Kafka ACLs: WRITE and READ only on application topics, no admin, no access to `_ss.offset_map`. Gateways hold credentials for their own region's cluster only. |
| B3 | Gateway ↔ control plane | Cross-trust | mTLS, workload identity (SPIFFE-style SVID per gateway instance). Control plane returns only signed epoch tokens and bundles. |
| B4 | Replicator ↔ source/destination Kafka | Cross-region | mTLS. Source reader has READ on source topics only. The sink writer has WRITE only on replica topics in its region and transactional ID ACLs. Neither has admin rights. |
| B5 | Replicator ↔ control plane | Cross-trust | mTLS, signed attestations (per-replicator key registered in the registry). Bundle fetch verified by signature, not by transport. |
| B6 | Inter-region WAN | Untrusted network | Encrypted in transit on every hop (TLS). Paths and transit zones come from the link registry. Traffic never uses a path outside the registry (INV-2). |
| B7 | Operator ↔ control plane | Human → authority | OIDC with MFA, RBAC (`policy-admin`, `operator`, `break-glass-approver`, `auditor`), every call audited. |
| B8 | Dashboard ↔ everything | Read-only | Read-only credentials for metrics and the watch stream. No path to mutate. Dashboard users authenticate via OIDC. Policy contents shown according to the viewer's role. |
| B9 | Control-plane member ↔ member | Consensus | mTLS with member certificates pinned to the cluster membership. Membership changes are themselves Raft-committed and require RBAC approval. |

Kafka ACLs are the last line of defence: only the gateway principal may write application topics in the active region, and only the replicator-sink principal may write a replica topic in a passive region.

## 3. Identity and authorization

- **Workloads:** Short-lived X.509 certificates (≤ 24 h) from a per-region CA chained to an offline root. A region's CA can issue only identities with that region's name in the SAN, so a gateway cannot claim to be in another region.
- **Clients:** JWTs carry scopes `topic:{name}:produce|consume`. The gateway checks the scope and the policy (`allowed_processing_regions` contains the gateway's region).
- **Operators:** RBAC roles above. `BreakGlassGrant` needs one requester and at least two approvers who are distinct people and are not the requester. Approvals expire (default 1 h).
- **Control-plane signing keys:** Ed25519 keys held in HSM/KMS, separate keys for epoch tokens (`kid-epoch`), policy bundles (`kid-bundle`), and audit hash-chain anchors. Verifying keys are distributed through the registry bundle and rotated by key id.

## 4. Fencing and token security

- **Epoch token:** `{topic, region, epoch, generation, policy_revision, cluster_incarnation, ttl_ms, kid}` signed by the control plane. A gateway must hold a token whose `ttl_ms` has not elapsed on its **local monotonic clock**, measured from the moment it *sent* the renewal request (conservative: it ignores network delay in the safe direction). A wall-clock jump therefore cannot extend a lease.
- **Anti-replay:** A token is bound to the gateway's workload identity (region + instance) and carries a nonce echoed from the renewal request. A captured token cannot be used by another region (gateways in another region have no credentials to the active region's Kafka anyway, boundary B2).
- **`cluster_incarnation`:** Increments only on a forced quorum reconfiguration (FAILURE_MATRIX F5). Gateways reject tokens with an incarnation lower than the highest they have seen.
- **Sink fencing:** Every sink rejects records with `leadership_epoch < max_epoch_seen` (INV-4). Even a stale gateway that somehow writes cannot get its data replicated.
- **Control-plane fence wait:** Grants of a new epoch wait for the old lease to expire (or be released) plus `max_clock_drift_ms`. A new Raft leader also waits through a quiet period.
- **Clock assumption:** Bounded drift of monotonic clocks relative to real time over one lease TTL (`max_clock_drift_ms`). If a deployment cannot meet the assumption, increase `lease_ttl_ms` margin. Wall-clock synchronisation (NTP) is not needed for safety. It is needed only for event timestamps and audit time.

## 5. Policy enforcement points (defence in depth)

INV-1 and INV-2 are enforced independently in several places, so one bug or compromise does not break residency:

1. **Control plane (placement):** Policy validation (V1–V6) refuses policies that name ineligible regions, and the placement planner never creates an `EpochGrant` or replica assignment for a region outside the set.
2. **Regional control-plane agent:** It creates the replica topic in a region only for a (topic, region) pair the bundle approves. Replica topics do not exist elsewhere, and ACLs allow the sink to write only to existing replica topics (it cannot auto-create).
3. **Source replicator:** Checks INV-1 and INV-2 for (topic, destination, path) against the signed bundle before opening a stream. Missing, expired or unverifiable bundle: stream is closed.
4. **Destination sink:** Independently verifies that its own region ∈ `allowed_storage_regions` and that the replicated record's `policy_revision` is not newer than its bundle. A mismatch rejects the batch.
5. **Gateway:** Refuses produce/consume if its region ∉ `allowed_processing_regions`.
6. **Network:** Egress policy per region allows replicator traffic only to endpoints of approved destinations.
7. **Audit:** Rejections are counted and logged (`policy_violation_rejections_total`). A non-zero count on an unexpected topic pages security.

Policy bundles are valid for `bundle_ttl_ms` (configurable, default 5 min). A component that cannot refresh by then treats policy as unknown and stops the affected actions (INV-6). Reads of already-stored data stay allowed where the component's region is already in `allowed_processing_regions` of the last known good bundle.

## 6. Data protection

- **In transit:** TLS everywhere (B1–B9).
- **At rest:** Disk encryption on brokers. For `CONFIDENTIAL` and `RESTRICTED`, `payload` is envelope-encrypted by the gateway with a data key wrapped under a KMS key for the topic's `legal_zone`. Replica regions in the same legal zone can decrypt; regions outside the zone never receive the data (INV-1), so they never need the key.
- **Quarantine:** `_ss.quarantine` inherits the originating topic's encryption and retention.
- **Secrets:** No secrets in policy, the bundle, or audit entries. Secrets are in a regional secrets manager.
- **Logging:** No payloads in logs. Keys, event ids and metadata only.

## 7. Audit

An append-only, hash-chained audit log in the control plane (`AuditEntry`, DATA_MODEL §5). Covers: every policy change, epoch grant, promotion decision (with eligibility snapshot), lease release, break-glass request and approval, and quorum reconfiguration. The chain head is periodically anchored to external write-once storage. Auditors have read-only access.

## 8. Threat model summary

| Threat | Mitigation |
|---|---|
| Operator or script replicates to a disallowed region | INV-1/2 enforcement at 6 points (§5). Replica topics cannot be auto-created. No override flag exists (INV-7). |
| Replicator tricked into an unapproved path | Path chosen from the link registry only. Unknown links are not approved. Egress rules. |
| Compromised gateway claims to be active | Token requires control-plane signature and binds identity. Gateway only has credentials for its region's cluster. Sinks fence by epoch. |
| Stolen epoch token | Short TTL, identity-bound, nonce-bound. |
| Forged policy bundle | Signed with `kid-bundle`. Components verify. Expired bundle means fail closed. |
| Insider promotes a lagging or non-compliant region | Eligibility checked by the promotion engine, not by callers. Policy never waivable. Lag waiver needs dual approval. Everything audited. |
| Compromise of one control-plane member | Needs a majority to commit. Member certs pinned. |
| Replay of old attestations to fake freshness | Attestations signed, carry `generation` and `reported_at_cp_ms` (set by the control plane when committed), and a monotonic sequence. The control plane rejects non-increasing sequences. |
| Tampering with audit | Hash chain plus external anchor. |
| Direct produce to Kafka bypassing the gateway | ACLs allow only gateway principal. No client credentials. |
| Clock manipulation to extend a lease | Leases use monotonic clocks, measured from request send time. Bounded drift is an explicit deployment assumption. |

## 9. Split-brain scenario catalogue

"Split-brain" means two regions both believing they may accept writes for a topic (violating INV-3), or two histories being merged (violating INV-9).

| ID | Scenario | Prevention | Detection | Recovery |
|---|---|---|---|---|
| SB-1 | Active region A is partitioned from control plane; the quorum promotes B | A self-fences on lease TTL. Quorum waits TTL + drift before the grant. Tokens are monotonic-clock bounded. | Lease-expired gateway metric; divergent suffix at A when it returns | A reseeds under the new generation. Suffix goes to quarantine. |
| SB-2 | Control-plane leader loses quorum but doesn't know (stale leader) | Leader must commit renewals through the quorum. A stale leader cannot commit, so it cannot extend a lease. | Raft term changes; renewal errors | None needed. |
| SB-3 | New Raft leader grants a new epoch while the old leader's lease is still valid | New-leader quiet period of TTL + drift before any grant. | — | — |
| SB-4 | Gateway clock jumps and extends its own lease | Monotonic clock only. Leases measured from request send time. | Gateway self-check of clock discontinuity (marks the lease invalid) | Re-acquire a lease. |
| SB-5 | Forced quorum reconfiguration (break-glass) while old regions are alive | `cluster_incarnation` bump. Tokens with an older incarnation are rejected. Tool enforces a TTL + drift wait since isolation evidence. Dual approval. | Audit entry; incarnation mismatch alarms | Old regions self-fence on seeing the new incarnation. Their suffix is quarantined. |
| SB-6 | Two replicators pump into one destination from different epochs | Sink rejects `leadership_epoch < max_epoch_seen`. Transactional IDs fence zombie producers. | `_ss.quarantine` counts | Review quarantine. |
| SB-7 | Old active returns and reads/serves stale data to consumers | Gateway on restart has no lease and a stale epoch. It must fetch `TopicState` and serve reads only if policy and the current state allow. Responses from a non-active region carry `X-SS-Region-Role: passive`. | Role header, metrics | Optional policy: `consume` redirect to the active region. |
| SB-8 | Promotion due to two overlapping promotions (operator double-click, race) | `PromotionIntent` is a CAS on `from_epoch`; only one succeeds. | Failed CAS logged (`PRECONDITION_FAILED`) | None. |
| SB-9 | Network heals and two replicas of different generations exchange data | Replication generation compared. Lower generation must truncate to `epoch_start_offsets` or reseed. No auto-merge. | Generation-mismatch alarm | Truncate/reseed. |
| SB-10 | Replica is promoted by two control-plane partitions in a multi-quorum design | Not applicable by design: exactly one quorum exists. A second quorum is never created except by break-glass (SB-5). | — | — |

## 10. Residual risks (accepted, documented)

- The design depends on bounded monotonic clock drift over one lease TTL.
- Control-plane quorum loss halts writes everywhere within one TTL (ADR-0004: safety over availability).
- With only 3 regions, losing two regions halts the control plane. Break-glass is a manual, audited process with a residual human-error risk, mitigated by tool-enforced waits and dual approval.
- Unreplicated writes at failover under `ASYNC_REGIONAL` are lost from the live log (they are preserved in quarantine for reconciliation).
