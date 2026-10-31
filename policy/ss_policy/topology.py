"""Replication topology rules (pure). A topic's flows must form a tree rooted at its active region:
every non-root region has exactly one source, nothing replicates INTO the active region, and there are no cycles."""

from .models import Reason


def _reaches(edges: list[tuple[str, str]], start: str, goal: str) -> bool:
    seen, stack = set(), [start]
    while stack:
        cur = stack.pop()
        if cur == goal:
            return True
        if cur in seen:
            continue
        seen.add(cur)
        stack.extend(d for s, d in edges if s == cur)
    return False


def validate_new_flow(active: str, edges: list[tuple[str, str]], new: tuple[str, str]) -> list[Reason]:
    """`edges` are the existing non-disabled flows. Returns the reasons the new (source, target) flow is refused."""
    src, dst = new
    out: list[Reason] = []
    if src == dst:
        out.append(Reason(code="SAME_REGION", message="a flow needs two different regions", category="TOPOLOGY"))
    if dst == active:
        out.append(
            Reason(
                code="REPLICATION_INTO_ACTIVE",
                message=f"{dst} is the active region; replicating into it would let a replica overwrite the primary's history",
                category="TOPOLOGY",
                region=dst,
            )
        )
    if any(d == dst for _, d in edges):
        out.append(Reason(code="TARGET_ALREADY_HAS_SOURCE", message=f"{dst} already has a source for this topic", category="TOPOLOGY", region=dst))
    if _reaches(edges, dst, src):
        out.append(Reason(code="REPLICATION_CYCLE", message=f"{src} -> {dst} would close a cycle", category="TOPOLOGY"))
    if src != active and not _reaches(edges, active, src):
        out.append(Reason(code="SOURCE_NOT_AUTHORIZED", message=f"{src} is neither the active region nor fed from it", category="TOPOLOGY", region=src))
    return out
