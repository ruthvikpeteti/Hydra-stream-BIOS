"""Gateway settings, read once from the environment. Kafka credentials live here only."""

import os
import ssl
from dataclasses import dataclass, field


def _secret(name: str) -> str | None:
    """Value of NAME, or the contents of the file named by NAME_FILE (for mounted secrets)."""
    path = os.environ.get(f"{name}_FILE")
    if path:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    return os.environ.get(name)


@dataclass(frozen=True)
class Settings:
    region: str = "region-a"
    cluster_id: str = "kafka-a"  # logical name stamped as origin_cluster
    kafka_bootstrap: str = "localhost:9092"
    registry_path: str = "/srv/config/registry.json"
    control_state_path: str = "/etc/ss/control/dev-control-state.json"
    lease_ttl_ms: int = 10_000
    lease_renew_ms: int = 3_000
    coord_endpoints: tuple[str, ...] = ()  # etcd client URLs; empty = dev control-state file
    api_keys: tuple[str, ...] = ()
    max_event_bytes: int = 1_048_576
    idempotency_ttl_s: int = 86_400
    idempotency_max_entries: int = 500_000
    max_consumers: int = 16
    produce_timeout_ms: int = 15_000
    # Durability modes this deployment can honour. SYNC_REGIONAL needs the replication plane.
    supported_durability: tuple[str, ...] = ("LOCAL_DURABLE", "ASYNC_REGIONAL")
    # Policy engine (Phase 3). Empty URL = no engine (unit tests / legacy); then only local validation runs.
    policy_url: str = ""
    policy_key: str = field(default="", repr=False)
    restricted_policy_ttl_s: float = 30.0
    policy_timeout_s: float = 3.0
    kafka_client_kwargs: dict = field(default_factory=dict, repr=False)

    @classmethod
    def from_env(cls) -> "Settings":
        e = os.environ.get
        protocol = e("SS_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        kw: dict = {}
        if protocol != "PLAINTEXT":
            kw["security_protocol"] = protocol
            if protocol.startswith("SASL"):
                kw["sasl_mechanism"] = e("SS_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
                kw["sasl_plain_username"] = _secret("SS_KAFKA_SASL_USERNAME")
                kw["sasl_plain_password"] = _secret("SS_KAFKA_SASL_PASSWORD")
            if protocol.endswith("SSL"):
                kw["ssl_context"] = ssl.create_default_context(cafile=e("SS_KAFKA_CA_FILE"))
        keys = tuple(k.strip() for k in (_secret("SS_API_KEYS") or "").split(",") if k.strip())
        return cls(
            region=e("SS_REGION", cls.region),
            cluster_id=e("SS_CLUSTER_ID", cls.cluster_id),
            kafka_bootstrap=e("SS_KAFKA_BOOTSTRAP", cls.kafka_bootstrap),
            registry_path=e("SS_REGISTRY_PATH", cls.registry_path),
            control_state_path=e("SS_CONTROL_STATE_PATH", cls.control_state_path),
            lease_ttl_ms=int(e("SS_LEASE_TTL_MS", cls.lease_ttl_ms)),
            lease_renew_ms=int(e("SS_LEASE_RENEW_MS", cls.lease_renew_ms)),
            coord_endpoints=tuple(x.strip() for x in e("SS_COORD_ENDPOINTS", "").split(",") if x.strip()),
            api_keys=keys,
            max_event_bytes=int(e("SS_MAX_EVENT_BYTES", cls.max_event_bytes)),
            idempotency_ttl_s=int(e("SS_IDEMPOTENCY_TTL_S", cls.idempotency_ttl_s)),
            max_consumers=int(e("SS_MAX_CONSUMERS", cls.max_consumers)),
            produce_timeout_ms=int(e("SS_PRODUCE_TIMEOUT_MS", cls.produce_timeout_ms)),
            supported_durability=tuple(e("SS_SUPPORTED_DURABILITY", ",".join(cls.supported_durability)).split(",")),
            policy_url=e("SS_POLICY_URL", "").rstrip("/"),
            policy_key=_secret("SS_POLICY_KEY") or "",
            restricted_policy_ttl_s=float(e("SS_RESTRICTED_POLICY_TTL_S", cls.restricted_policy_ttl_s)),
            policy_timeout_s=float(e("SS_POLICY_TIMEOUT_S", cls.policy_timeout_s)),
            kafka_client_kwargs=kw,
        )
