# ADR-0006: Application gateway as the sole client entry point

Status: Accepted

## Context
Fencing and envelope stamping must apply to every write. Letting clients produce to Kafka directly would bypass the lease, stamping and processing-region checks.

## Decision
Clients interact only with the regional gateway over HTTP/gRPC. They have no Kafka credentials. Kafka ACLs allow only the gateway principal to write application topics in the active region and only the replicator-sink principal to write replica topics. The gateway stamps `leadership_epoch`, `replication_generation`, `origin_region`, `event_id`, `event_timestamp` and ignores client-supplied values.

## Consequences
- Extra hop and a throughput ceiling at the gateway. Mitigated by horizontal scaling (≥2 instances per region in production).
- Client libraries are simple. Kafka protocol features (transactions across topics) are not exposed.
- The gateway becomes critical for availability and a high-value security target (SECURITY B1-B3).

## Alternatives
- Client-side Kafka producers with an interceptor: rejected (cannot be trusted or fenced).
- Kafka proxy/sidecar per client: rejected (more credentials, harder to fence).
