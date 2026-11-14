"""Retry delays: exponential backoff with full jitter (D18)."""

import random

# 2^32 * base is far past any sane cap; capping the exponent first stops
# float overflow for absurd attempt counts.
_MAX_EXPONENT = 32


def full_jitter(attempt: int, base: float, cap: float, rng: random.Random = random) -> float:
    """Seconds to wait after failed attempt number `attempt` (1-based).

    Uniform in [0, min(cap, base * 2^(attempt-1))]. The exponential sets the
    *window*; the random pick inside it spreads jobs that failed together
    so they don't retry together (the thundering herd).
    """
    window = min(cap, base * 2 ** min(attempt - 1, _MAX_EXPONENT))
    return rng.uniform(0, window)
