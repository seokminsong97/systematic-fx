"""Narrow whole-operation retries for PostgreSQL serialization failures."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Final

import psycopg

_SERIALIZATION_RETRY_DELAYS_SECONDS: Final = (0.01, 0.05, 0.2)


def retry_serialization_failures[**P, R](
    operation: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> R:
    """Retry an entire operation after an unambiguously aborted SQLSTATE 40001.

    ``operation`` must own its connection and transaction so every invocation
    starts from a fresh PostgreSQL transaction boundary.  Other database errors
    are deliberately not retried because their commit outcome may be ambiguous.
    """

    for attempt in range(len(_SERIALIZATION_RETRY_DELAYS_SECONDS) + 1):
        try:
            return operation(*args, **kwargs)
        except psycopg.errors.SerializationFailure:
            if attempt == len(_SERIALIZATION_RETRY_DELAYS_SECONDS):
                raise
            time.sleep(_SERIALIZATION_RETRY_DELAYS_SECONDS[attempt])
    raise AssertionError("serialization retry loop exhausted without returning or raising")
