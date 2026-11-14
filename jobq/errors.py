"""Which handler failures are worth retrying (D17).

Handlers can be explicit by raising RetryableError or PermanentError. Anything
else is classified by type: network and database errors are retried,
everything else (including bugs) is permanent and goes straight to the DLQ.
"""

import psycopg


class RetryableError(Exception):
    """A failure that may succeed if the job runs again later."""


class PermanentError(Exception):
    """A failure that will repeat on every attempt: skip remaining attempts."""


# Transient by nature. psycopg.OperationalError covers lost connections,
# serialization failures and deadlocks. HTTP client errors (e.g. httpx's) are
# not subclasses of these: handlers wrap them in RetryableError.
RETRYABLE_TYPES = (ConnectionError, TimeoutError, psycopg.OperationalError)


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, RetryableError):
        return True
    if isinstance(exc, PermanentError):
        return False
    return isinstance(exc, RETRYABLE_TYPES)
