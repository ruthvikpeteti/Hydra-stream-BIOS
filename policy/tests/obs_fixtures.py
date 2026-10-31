"""Realistic raw source data for the dashboard API tests, with hostile content mixed in (payloads, secrets) on purpose."""

SECRET = "s3cr3t-service-key-value"
NOW = 1_800_000_000.0


def src(data, ok=True, age=0.5, error=None):
    return {"ok": ok, "data": data, "fetched_at": None if age is None else NOW - age, "error": error}


def route(current="a-b-direct", changes=3):
    def m(rtt, breaker="CLOSED", loss=0.0):
        return {
            "breaker": breaker,
            "rtt_ms": rtt,
            "loss": loss,
            "bandwidth_bps": 4.2e6,
            "samples": 12,
            "consecutive_failures": 0,
            "timeouts": 0,
            "last_sample_age_s": 0.4,
            "invalid_samples": 0,
        }

    return {
        "current": current,
        "changes": changes,
        "last_change": NOW - 90,
        "reason": "SUSTAINED_IMPROVEMENT",
        "scores": {"a-b-direct": {"score": 0.04}, "a-b-peering": {"score": 0.11}, "a-b-relay": {"score": 0.07}},
        "excluded": {"a-b-relay": "RELAY_REGION_NOT_ALLOWED"},
        "policy_rejected": {},
        "measurements": {"a-b-direct": m(7.4), "a-b-peering": m(120.0, "OPEN", 0.4), "a-b-relay": m(9.1)},
    }


def raw(brokers_up=3, active="region-a", phase="HEALTHY_A", fenced=None, stale=()):
    state = {
        "phase": phase,
        "active_region": active,
        "leadership_epoch": 4,
        "replication_generation": 4,
        "fenced": fenced or {},
        "transition": None,
        "seq": 9,
        "last_failover": {
            "from": "region-a",
            "to": "region-b",
            "epoch": 2,
            "generation": 2,
            "failure_boundary_ms": 1_000_000,
            "detected_ms": 1_000_100,
            "fenced_ms": 1_000_300,
            "topology_ms": 1_007_400,
            "activated_ms": 1_010_000,
            "degraded_rpo": False,
            "approved_by": None,
            "expected_rpo_ms": {"t1": 3400.0},
            "replication_ready_ms": 1_015_000,
        },
    }
    health = {
        "region-a": {
            "declared": "UP" if brokers_up == 3 else "DEGRADED",
            "sample": {"kafka": True, "gateway": True, "agent": True, "brokers_up": brokers_up, "brokers_expected": 3, "controller": True},
        },
        "region-b": {"declared": "UP", "sample": {"kafka": True, "gateway": True, "agent": True, "brokers_up": 1, "brokers_expected": 1, "controller": True}},
        "region-c": {
            "declared": "DOWN",
            "sample": {"kafka": False, "gateway": False, "agent": False, "brokers_up": 0, "brokers_expected": 1, "controller": False},
        },
    }
    flows = [
        {
            "flow_id": 1,
            "source_region": "region-a",
            "target_region": "region-b",
            "source_cluster": "kafka-a",
            "target_cluster": "kafka-b",
            "state": "ENABLED",
            "paused": False,
            "generation": 4,
        },
        {
            "flow_id": 2,
            "source_region": "region-a",
            "target_region": "region-c",
            "source_cluster": "kafka-a",
            "target_cluster": "kafka-c",
            "state": "ENABLED",
            "paused": False,
            "generation": 4,
        },
    ]
    out = {
        "coordinator": src(
            {
                "state": state,
                "members": {"e1": True, "e2": True, "e3": True, "e4": False, "e5": True},
                "audit": [
                    {
                        "seq": 8,
                        "event": "PROMOTED",
                        "from": "PROMOTING_B",
                        "to": "HEALTHY_B",
                        "active_region": "region-b",
                        "epoch": 2,
                        "actor": "dr-1",
                        "wall_ms": int((NOW - 500) * 1000),
                    }
                ],
            }
        ),
        "dr": src(
            {
                "instance": "dr-1",
                "leader": True,
                "auto": True,
                "allow_degraded_rpo": False,
                "state": state,
                "health": health,
                "eligibility": {
                    "region-b": [
                        {
                            "region": "region-b",
                            "topic": "t1",
                            "eligible": False,
                            "degraded_rpo": False,
                            "reasons": ["LAG_EXCEEDS_RPO"],
                            "lag_ms": 9000.0,
                            "max_lag_ms": 5000,
                        }
                    ]
                },
            }
        ),
        "engine": src(
            {
                "regions": [],
                "paths": [{"path_id": "a-b-direct", "kind": "DIRECT"}, {"path_id": "a-b-peering", "kind": "PEERING"}],
                "policies": [
                    {
                        "topic": "t1",
                        "classification": "RESTRICTED",
                        "legal_zone": "EU",
                        "home_region": "region-a",
                        "allowed_storage_regions": ["region-a", "region-b"],
                        "allowed_processing_regions": ["region-a", "region-b"],
                        "prohibited_regions": ["region-c"],
                        "policy_version": 3,
                        "excluded_regions": [],
                        "replicas": [{"region": "region-b", "status": "IN_SYNC"}],
                        "payload": "LEAK-ME",
                    }
                ],
                "topics": [{"topic": "t1", "active_region": "region-a", "leadership_epoch": 4, "partitions": 2, "flows": flows}],
                "backlog": [
                    {
                        "flow_id": 1,
                        "topic": "t1",
                        "source_region": "region-a",
                        "target_region": "region-b",
                        "backlog_records": 40,
                        "backlog_bytes": 8000,
                        "drain_seconds": 2.5,
                        "throughput_bps": 3200.0,
                        "backlog_state": "OK",
                        "route": "a-b-direct",
                        "report_age_s": 1.0,
                    },
                    {
                        "flow_id": 2,
                        "topic": "t1",
                        "source_region": "region-a",
                        "target_region": "region-c",
                        "backlog_records": 900,
                        "backlog_bytes": 180000,
                        "drain_seconds": None,
                        "throughput_bps": 0.0,
                        "backlog_state": "HARD",
                        "route": None,
                        "report_age_s": 1.0,
                    },
                ],
                "recent": [
                    {
                        "id": 5,
                        "ts": "2026-01-01T00:00:03+00:00",
                        "source": "engine",
                        "operation": "PLACEMENT",
                        "enforcement_point": "TOPIC_CREATION",
                        "topic": "t9",
                        "policy_version": 1,
                        "subject": {"region": "region-c", "value": "LEAK-ME"},
                        "decision": "REJECTED",
                        "reasons": [{"code": "LEGAL_STORAGE", "message": "m"}],
                        "requester": "svc",
                    },
                    {
                        "id": 4,
                        "ts": "2026-01-01T00:00:02+00:00",
                        "source": "engine",
                        "operation": "ROUTE_SWITCH",
                        "enforcement_point": "ROUTING",
                        "topic": "t1",
                        "policy_version": 1,
                        "subject": {},
                        "decision": "ALLOWED",
                        "reasons": [],
                        "requester": "agent",
                    },
                ],
                "stats": {
                    "decisions": [
                        {"operation": "PLACEMENT", "enforcement_point": "TOPIC_CREATION", "decision": "REJECTED", "count": 3},
                        {"operation": "PLACEMENT", "enforcement_point": "TOPIC_CREATION", "decision": "ALLOWED", "count": 20},
                    ],
                    "replica_assignments": {"IN_SYNC": 2, "REVOKED": 1},
                },
                "details": {
                    "t1": {
                        "checkpoints": [
                            {
                                "source_cluster": "kafka-a",
                                "target_cluster": "kafka-b",
                                "source_partition": 0,
                                "last_acknowledged_source_offset": 120,
                                "last_known_destination_offset": 120,
                                "last_success_timestamp": "2026-01-01T00:00:01+00:00",
                                "value": "LEAK-ME",
                            },
                            {
                                "source_cluster": "kafka-a",
                                "target_cluster": "kafka-b",
                                "source_partition": 1,
                                "last_acknowledged_source_offset": 99,
                                "last_known_destination_offset": 99,
                                "last_success_timestamp": "2026-01-01T00:00:02+00:00",
                            },
                        ]
                    }
                },
            }
        ),
        "replicators": src(
            {
                "region-a": {
                    "region": "region-a",
                    "flows": [
                        {
                            "flow_id": 1,
                            "topic": "t1",
                            "source": "region-a",
                            "target": "region-b",
                            "generation": 4,
                            "paused": False,
                            "route": route(),
                            "backlog": {"records": 40, "bytes_estimate": 8000, "max_pending_batches": 1},
                            "partitions": {"0": {"state": "RUNNING"}, "1": {"state": "RUNNING"}},
                        }
                    ],
                },
                "region-b": {"error": "ConnectError"},
                "region-c": {"region": "region-c", "flows": []},
            }
        ),
        "gateways": src(
            {
                "region-a": {
                    "healthy": True,
                    "metrics": 'ss_producer_ack_latency_seconds_bucket{region="region-a",le="0.1"} 5\nss_producer_ack_latency_seconds_count{region="region-a"} 5\n',
                },
                "region-b": {"healthy": True, "metrics": ""},
                "region-c": {"healthy": False, "error": "ConnectError"},
            }
        ),
    }
    for name in stale:
        out[name] = src(out[name]["data"], ok=False, age=120.0, error="ConnectError: down")
    return out
