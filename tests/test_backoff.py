import random

import psycopg
import pytest

from jobq.backoff import full_jitter
from jobq.errors import PermanentError, RetryableError, is_retryable


@pytest.mark.parametrize("attempt, window", [(1, 1), (2, 2), (3, 4), (5, 16), (9, 256), (10, 300)])
def test_full_jitter_stays_inside_the_capped_exponential_window(attempt, window):
    rng = random.Random(42)
    delays = [full_jitter(attempt, base=1, cap=300, rng=rng) for _ in range(2000)]

    assert all(0 <= d <= window for d in delays)
    # Spread across the whole window, not bunched: roughly uniform.
    assert 0.4 * window < sum(delays) / len(delays) < 0.6 * window
    assert min(delays) < 0.05 * window and max(delays) > 0.95 * window


def test_full_jitter_survives_absurd_attempt_numbers():
    assert 0 <= full_jitter(10_000, base=1, cap=300) <= 300


def test_same_seed_same_delays():
    assert full_jitter(4, 1, 300, random.Random(7)) == full_jitter(4, 1, 300, random.Random(7))


@pytest.mark.parametrize(
    "exc, retryable",
    [
        (ConnectionResetError(), True),  # a ConnectionError
        (TimeoutError(), True),
        (psycopg.errors.SerializationFailure(), True),  # an OperationalError
        (KeyError("amount"), False),  # a bug
        (ValueError("invalid card number"), False),
        (RetryableError("429, try later"), True),
        (PermanentError("card declined"), False),
        # Explicit classes win over the type: a timeout the handler knows is hopeless.
        (type("HopelessTimeout", (PermanentError, TimeoutError), {})(), False),
    ],
)
def test_error_classification(exc, retryable):
    assert is_retryable(exc) is retryable
