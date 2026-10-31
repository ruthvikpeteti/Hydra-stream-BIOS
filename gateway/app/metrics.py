"""Minimal Prometheus text exposition for the gateway (no extra dependency). Labels are fixed and low-cardinality:
region and outcome only. Never event ids, topics, keys or payloads."""

import threading

BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0)
_lock = threading.Lock()
_ack_counts = [0] * len(BUCKETS)
_ack_sum = 0.0
_ack_total = 0
_writes: dict[str, int] = {}


def observe_ack(seconds: float) -> None:
    global _ack_sum, _ack_total
    with _lock:
        _ack_sum += seconds
        _ack_total += 1
        for i, b in enumerate(BUCKETS):
            if seconds <= b:
                _ack_counts[i] += 1


def count_write(outcome: str) -> None:
    with _lock:
        _writes[outcome] = _writes.get(outcome, 0) + 1


def render(region: str, lease_valid: bool, epoch: int) -> str:
    with _lock:
        lines = [
            "# HELP ss_producer_ack_latency_seconds Time from handing an event to Kafka until the acks=all acknowledgement.",
            "# TYPE ss_producer_ack_latency_seconds histogram",
        ]
        for b, c in zip(BUCKETS, _ack_counts):
            lines.append(f'ss_producer_ack_latency_seconds_bucket{{region="{region}",le="{b}"}} {c}')
        lines += [
            f'ss_producer_ack_latency_seconds_bucket{{region="{region}",le="+Inf"}} {_ack_total}',
            f'ss_producer_ack_latency_seconds_sum{{region="{region}"}} {_ack_sum:.6f}',
            f'ss_producer_ack_latency_seconds_count{{region="{region}"}} {_ack_total}',
            "# TYPE ss_gateway_writes_total counter",
        ]
        for o, n in sorted(_writes.items()):
            lines.append(f'ss_gateway_writes_total{{region="{region}",outcome="{o}"}} {n}')
        lines += [
            "# TYPE ss_gateway_lease_valid gauge",
            f'ss_gateway_lease_valid{{region="{region}"}} {1 if lease_valid else 0}',
            "# TYPE ss_gateway_leadership_epoch gauge",
            f'ss_gateway_leadership_epoch{{region="{region}"}} {epoch}',
        ]
    return "\n".join(lines) + "\n"
