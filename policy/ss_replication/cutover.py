"""Cutover after a promotion (pure). New source B takes over feeding target C, which was fed by the old source A.

B's log is [A-origin records replicated from A][B-native records written after promotion]. C holds the A-origin
records up to A-offset `acked_by_target`. B->C must start at the first B record C does NOT have, without
discarding anything C already holds, and the lineage of every range must be explicit."""

from dataclasses import dataclass
from typing import Optional


class CutoverInvalid(Exception):
    pass


@dataclass(frozen=True)
class Scanned:
    """One record of the new source's partition log, reduced to what the cutover needs."""

    offset: int
    origin_cluster: Optional[str]  # None: no origin header, i.e. written natively in this cluster
    origin_offset: Optional[int]


def compute_partition_cutover(
    partition: int,
    scanned: list[Scanned],
    old_source_cluster: str,
    acked_by_target: int,
    checkpoint_acked_by_new_source: int,
    end_offset: int,
    last_known_destination_offset: int = -1,
) -> dict:
    """`acked_by_target`: highest old-source offset the target acknowledged (-1: none).
    `checkpoint_acked_by_new_source`: what the old flow's checkpoint into the new source said (-1: none)."""
    a_origin = [r for r in scanned if r.origin_cluster == old_source_cluster]
    native_idx = next((i for i, r in enumerate(scanned) if r.origin_cluster != old_source_cluster), None)
    boundary = scanned[native_idx].offset if native_idx is not None else end_offset
    if native_idx is not None:  # no old-source record may appear after native ones
        late = [r.offset for r in scanned[native_idx:] if r.origin_cluster == old_source_cluster]
        if late:
            raise CutoverInvalid(
                f"partition {partition}: old-source records at offsets {late[:5]} follow native records "
                f"(first native offset {boundary}); the old flow was not fenced before writes began"
            )
    if any(r.origin_offset is None for r in a_origin):
        raise CutoverInvalid(f"partition {partition}: a record claiming origin {old_source_cluster} has no origin offset")
    holds_through = max((r.origin_offset for r in a_origin), default=-1)
    if holds_through < checkpoint_acked_by_new_source:
        raise CutoverInvalid(
            f"partition {partition}: new source's log holds old-source offsets only through {holds_through}, "
            f"but the old flow's checkpoint claims {checkpoint_acked_by_new_source}"
        )
    if last_known_destination_offset >= 0 and a_origin and last_known_destination_offset >= boundary:
        raise CutoverInvalid(
            f"partition {partition}: checkpointed destination offset {last_known_destination_offset} is beyond the first native offset {boundary}"
        )
    start = next((r.offset for r in a_origin if r.origin_offset > acked_by_target), boundary)
    a_missing = [r for r in a_origin if r.offset >= start]
    target_ahead = acked_by_target > holds_through
    return {
        "partition": partition,
        "old_source_cluster": old_source_cluster,
        "target_holds_old_source_through": acked_by_target,
        "new_source_checkpoint_through": checkpoint_acked_by_new_source,
        "new_source_holds_old_source_through": holds_through,
        "native_start_offset": boundary,
        "new_source_end_offset": end_offset,
        "cutover_start_offset": start,
        # lineage of this partition, as explicit ranges and counts
        "already_in_target": {"origin": old_source_cluster, "through_origin_offset": min(acked_by_target, holds_through)},
        "missing_from_target": {
            "old_source_origin_records": len(a_missing),
            "old_source_origin_range": ([a_missing[0].origin_offset, a_missing[-1].origin_offset] if a_missing else None),
            "native_records": end_offset - boundary,
            "native_range": [boundary, end_offset - 1] if end_offset > boundary else None,
        },
        # the target holds old-source records the new source never received: kept, never merged or deleted
        "target_ahead": target_ahead,
        "orphan_in_target": ({"origin": old_source_cluster, "origin_offset_range": [holds_through + 1, acked_by_target]} if target_ahead else None),
        "status": "OK_WITH_TARGET_AHEAD" if target_ahead else "OK",
    }
