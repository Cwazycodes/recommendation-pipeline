"""Retry DynamoDB writes that were throttled or hit a server error."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass

from botocore.exceptions import ClientError

# Throttling comes back as HTTP 400 with one of these codes; 5xx is retried too.
THROTTLING_CODES = frozenset(
    {
        "ProvisionedThroughputExceededException",
        "RequestLimitExceeded",
        "ThrottlingException",
    }
)


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with full jitter, for at most max_attempts tries."""

    max_attempts: int = 5
    base_delay_s: float = 0.1
    max_delay_s: float = 5.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

    def delay(
        self, attempt: int, uniform: Callable[[float, float], float] = random.uniform
    ) -> float:
        """Wait after failed try number `attempt`: random up to base * 2^(attempt - 1).

        Full jitter spreads retries out, so writers that were throttled
        together do not all retry at the same moment.
        """
        ceiling = min(self.max_delay_s, self.base_delay_s * 2 ** (attempt - 1))
        return uniform(0, ceiling)


@dataclass(frozen=True)
class WriteFailure:
    """Why a write gave up, and after how many tries."""

    error: str
    attempts: int


def is_retryable(error: ClientError) -> bool:
    """Throttling and server errors can succeed later; anything else will not."""
    code = error.response.get("Error", {}).get("Code", "")
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)

    return code in THROTTLING_CODES or status >= 500


def write_with_retry(
    write: Callable[[], None],
    policy: RetryPolicy,
    sleep: Callable[[float], None] = time.sleep,
    uniform: Callable[[float, float], float] = random.uniform,
) -> WriteFailure | None:
    """Run write, retrying retryable errors. None on success, else the failure.

    Errors other than ClientError, such as a refused connection, are not
    caught here: ResultsWriter waits those out without committing.
    """
    attempt = 1
    while True:
        try:
            write()
            return None
        except ClientError as error:
            if not is_retryable(error) or attempt >= policy.max_attempts:
                return WriteFailure(error=str(error), attempts=attempt)

        sleep(policy.delay(attempt, uniform))
        attempt += 1
