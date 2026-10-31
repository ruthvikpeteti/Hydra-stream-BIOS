"""Idempotent-consumer demonstration. At-least-once delivery can deliver an event twice; a consumer that remembers
which event_ids it has processed turns that into effectively-once *processing* (the duplicates still exist in the log)."""

from dataclasses import dataclass, field
from typing import Callable


def delivery_id(record: dict) -> str:
    """Stable identity of an event across retries and hops: its event_id."""
    return record["value"]["event_id"]


@dataclass
class IdempotentConsumer:
    handler: Callable[[dict], None]
    seen: set = field(default_factory=set)
    processed: int = 0
    skipped_duplicates: int = 0

    def consume(self, record: dict) -> bool:
        """Process the record unless its event_id was already processed. Returns True if processed."""
        eid = delivery_id(record)
        if eid in self.seen:
            self.skipped_duplicates += 1
            return False
        self.handler(record)  # in production: do this and record `eid` in ONE transaction
        self.seen.add(eid)
        self.processed += 1
        return True


def find_duplicates(records: list[dict]) -> dict[str, list[dict]]:
    """event_id -> every delivery of it, for events delivered more than once."""
    by_id: dict[str, list[dict]] = {}
    for r in records:
        by_id.setdefault(delivery_id(r), []).append(r)
    return {k: v for k, v in by_id.items() if len(v) > 1}
