"""
Retry logic and policies for async operations.

This module provides retry mechanisms with exponential backoff for transient
database failures in async operations.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, TypeVar

from sqlalchemy.exc import (
    DisconnectionError,
    OperationalError,
)
from sqlalchemy.exc import (
    TimeoutError as SQLTimeoutError,
)

from pandalchemy.exceptions import ConnectionError, TransactionError

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class AsyncRetryPolicy:
    """
    Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of retry attempts (default: 3)
        initial_delay: Initial delay in seconds before first retry (default: 0.1)
        max_delay: Maximum delay between retries in seconds (default: 30.0)
        exponential_base: Base for exponential backoff (default: 2.0)
        jitter: Whether to add random jitter to delays (default: True)
        jitter_max: Maximum jitter in seconds (default: 1.0)
    """

    max_attempts: int = 3
    initial_delay: float = 0.1
    max_delay: float = 30.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_max: float = 1.0

    def calculate_delay(self, attempt: int) -> float:
        """
        Calculate delay for a given attempt number.

        Args:
            attempt: The attempt number (0-based, first retry is attempt 1)

        Returns:
            Delay in seconds
        """
        # Exponential backoff: initial_delay * (base ^ attempt)
        delay = self.initial_delay * (self.exponential_base**attempt)

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Add jitter if enabled
        if self.jitter:
            jitter_amount = random.uniform(0, self.jitter_max)
            delay += jitter_amount

        return delay


def is_retryable_error(error: Exception) -> bool:
    """
    Check if an error is retryable (transient failure).

    Args:
        error: The exception to check

    Returns:
        True if the error is retryable, False otherwise
    """
    # Connection and timeout errors are typically retryable
    if isinstance(
        error, (ConnectionError, DisconnectionError, SQLTimeoutError, asyncio.TimeoutError)
    ):
        return True

    # Operational errors might be retryable (network issues, deadlocks)
    if isinstance(error, OperationalError):
        error_str = str(error).lower()
        # Check for common retryable conditions
        retryable_conditions = [
            "connection",
            "timeout",
            "deadlock",
            "lock",
            "temporary",
            "network",
            "broken pipe",
            "connection reset",
            "connection refused",
        ]
        return any(condition in error_str for condition in retryable_conditions)

    # Transaction errors might be retryable (deadlocks, lock timeouts)
    if isinstance(error, TransactionError):
        error_str = str(error).lower()
        retryable_conditions = ["deadlock", "lock timeout", "could not serialize"]
        return any(condition in error_str for condition in retryable_conditions)

    # Check for database-specific deadlock errors
    error_str = str(error).lower()
    deadlock_indicators = [
        "deadlock detected",
        "deadlock found",
        "could not serialize access",
        "serialization failure",
        "lock wait timeout",
    ]
    return any(indicator in error_str for indicator in deadlock_indicators)


def is_deadlock_error(error: Exception) -> bool:
    """
    Check if an error is a deadlock.

    Args:
        error: The exception to check

    Returns:
        True if the error is a deadlock, False otherwise
    """
    error_str = str(error).lower()
    deadlock_indicators = [
        "deadlock detected",
        "deadlock found",
        "could not serialize access",
        "serialization failure",
    ]
    return any(indicator in error_str for indicator in deadlock_indicators)


def async_retry(
    policy: AsyncRetryPolicy | None = None,
    retryable_check: Callable[[Exception], bool] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to retry async functions with exponential backoff.

    Args:
        policy: Retry policy configuration (default: AsyncRetryPolicy())
        retryable_check: Custom function to check if error is retryable
            (default: is_retryable_error)

    Returns:
        Decorator function

    Example:
        @async_retry(policy=AsyncRetryPolicy(max_attempts=5))
        async def my_async_function():
            # Function that may fail transiently
            pass
    """
    if policy is None:
        policy = AsyncRetryPolicy()
    if retryable_check is None:
        retryable_check = is_retryable_error

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Exception | None = None
            attempt = 0

            while attempt < policy.max_attempts:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_error = e

                    # Check if error is retryable
                    if not retryable_check(e):
                        # Not retryable, re-raise immediately
                        raise

                    # Check if we've exhausted retries
                    if attempt >= policy.max_attempts - 1:
                        # Last attempt failed, re-raise with context
                        logger.warning(
                            f"Function {func.__name__} failed after {policy.max_attempts} attempts. "
                            f"Last error: {type(e).__name__}: {str(e)}"
                        )
                        raise

                    # Calculate delay and wait
                    delay = policy.calculate_delay(attempt)
                    logger.info(
                        f"Function {func.__name__} failed (attempt {attempt + 1}/{policy.max_attempts}): "
                        f"{type(e).__name__}: {str(e)}. Retrying in {delay:.2f}s..."
                    )

                    await asyncio.sleep(delay)
                    attempt += 1

            # Should never reach here, but just in case
            if last_error:
                raise last_error

            raise RuntimeError("Retry loop exited without error or return value")

        return wrapper

    return decorator


def retry_with_policy(
    func: Callable[..., Any],
    policy: AsyncRetryPolicy | None = None,
    retryable_check: Callable[[Exception], bool] | None = None,
) -> Callable[..., Any]:
    """
    Apply retry logic to an async function call.

    Args:
        func: The async function to retry
        policy: Retry policy configuration (default: AsyncRetryPolicy())
        retryable_check: Custom function to check if error is retryable

    Returns:
        Result of the function call

    Example:
        result = await retry_with_policy(
            my_async_function,
            policy=AsyncRetryPolicy(max_attempts=5)
        )
    """
    if policy is None:
        policy = AsyncRetryPolicy()
    if retryable_check is None:
        retryable_check = is_retryable_error

    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        last_error: Exception | None = None
        attempt = 0

        while attempt < policy.max_attempts:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_error = e

                if not retryable_check(e):
                    raise

                if attempt >= policy.max_attempts - 1:
                    logger.warning(
                        f"Function {func.__name__} failed after {policy.max_attempts} attempts. "
                        f"Last error: {type(e).__name__}: {str(e)}"
                    )
                    raise

                delay = policy.calculate_delay(attempt)
                logger.info(
                    f"Function {func.__name__} failed (attempt {attempt + 1}/{policy.max_attempts}): "
                    f"{type(e).__name__}: {str(e)}. Retrying in {delay:.2f}s..."
                )

                await asyncio.sleep(delay)
                attempt += 1

        if last_error:
            raise last_error

        raise RuntimeError("Retry loop exited without error or return value")

    return wrapper
