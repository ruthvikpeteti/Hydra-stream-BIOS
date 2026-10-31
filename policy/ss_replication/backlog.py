"""Backlog, throughput and the bounded-backlog policy (pure)."""

from collections import deque
from dataclasses import dataclass
from typing import Optional

OK, SOFT, HARD = "OK", "SOFT", "HARD"


def classify(backlog_bytes: float, soft: Optional[float], hard: Optional[float]) -> str:
    """Bounded-backlog policy: SOFT = warn and alert, HARD = signal backpressure to producers."""
    if hard is not None and backlog_bytes >= hard:
        return HARD
    if soft is not None and backlog_bytes >= soft:
        return SOFT
    return OK


class ThroughputWindow:
    """Bytes and records acknowledged by the destination, over a rolling window."""

    def __init__(self, window_s: float = 30.0):
        self.window_s = window_s
        self._s: deque = deque()

    def add(self, ts: float, nbytes: int, records: int) -> None:
        self._s.append((ts, nbytes, records))

    def rates(self, now: float) -> tuple[float, float]:
        while self._s and now - self._s[0][0] > self.window_s:
            self._s.popleft()
        if not self._s:
            return 0.0, 0.0
        span = max(now - self._s[0][0], 1.0)
        return sum(b for _, b, _ in self._s) / span, sum(r for _, _, r in self._s) / span


class RecordSize:
    """Running estimate of bytes per record, from batches actually read."""

    def __init__(self, default: float = 512.0, alpha: float = 0.2):
        self.value, self.alpha, self._seen = default, alpha, False

    def update(self, nbytes: int, records: int) -> None:
        if records <= 0:
            return
        x = nbytes / records
        self.value = x if not self._seen else (1 - self.alpha) * self.value + self.alpha * x
        self._seen = True


@dataclass(frozen=True)
class Backlog:
    records: int
    bytes_estimate: int
    drain_seconds: Optional[float]
    throughput_bps: float
    state: str


def compute_backlog(
    end_offset: int,
    last_acked: int,
    bytes_per_record: float,
    throughput_bps: float,
    path_bandwidth_bps: Optional[float],
    soft: Optional[float],
    hard: Optional[float],
) -> Backlog:
    """Records are exact (log end minus last acknowledged). Bytes are an estimate (records x observed average size)."""
    records = max(0, end_offset - (last_acked + 1))
    b = int(records * bytes_per_record)
    rate = throughput_bps if throughput_bps > 0 else (path_bandwidth_bps or 0.0)
    drain = (b / rate) if rate > 0 and b > 0 else (0.0 if b == 0 else None)
    return Backlog(records, b, drain, throughput_bps, classify(b, soft, hard))
