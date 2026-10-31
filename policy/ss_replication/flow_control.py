"""Small pure helpers for flow control."""

import random


def backoff_delay(attempt: int, base: float, cap: float, rng=random.random) -> float:
    """Exponential backoff with 'equal jitter': between half of and the full min(cap, base * 2**attempt)."""
    ceiling = min(cap, base * (2 ** max(0, attempt)))
    return ceiling * (0.5 + 0.5 * rng())
