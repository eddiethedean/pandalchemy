"""
Connection management and health checks for async operations.

This module provides connection pooling optimization, health checks, and
connection reuse strategies for async database operations.
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import NullPool

logger = logging.getLogger(__name__)


class SyncEngineCache:
    """
    Cache for sync engines created from async engines.

    This avoids creating multiple sync engines from the same async engine URL,
    which improves performance and resource usage.
    """

    def __init__(self) -> None:
        """Initialize the cache."""
        self._cache: dict[str, Engine] = {}
        self._lock = threading.Lock()

    def get_sync_engine(self, async_engine: AsyncEngine) -> Engine:
        """
        Get or create a sync engine from an async engine URL.

        Args:
            async_engine: The async engine to convert

        Returns:
            A sync engine with the same URL
        """
        # Convert async URL to sync URL
        url_str = str(async_engine.url)
        sync_url = (
            url_str.replace("+asyncpg", "").replace("+aiomysql", "").replace("+aiosqlite", "")
        )

        with self._lock:
            if sync_url not in self._cache:
                # Create new sync engine
                sync_engine = create_engine(
                    sync_url,
                    poolclass=NullPool,  # Use NullPool to avoid connection pool conflicts
                    echo=False,
                )
                self._cache[sync_url] = sync_engine
                logger.debug(f"Created new sync engine cache entry for {sync_url}")
            else:
                logger.debug(f"Reusing cached sync engine for {sync_url}")

            return self._cache[sync_url]

    def clear(self) -> None:
        """Clear the cache and dispose of all engines."""
        with self._lock:
            for engine in self._cache.values():
                try:
                    engine.dispose()
                except Exception as e:
                    logger.warning(f"Error disposing engine: {e}")
            self._cache.clear()

    def remove(self, url: str) -> None:
        """
        Remove a specific engine from the cache.

        Args:
            url: The URL to remove
        """
        with self._lock:
            if url in self._cache:
                try:
                    self._cache[url].dispose()
                except Exception as e:
                    logger.warning(f"Error disposing engine: {e}")
                del self._cache[url]


# Global cache instance
_sync_engine_cache = SyncEngineCache()


def get_sync_engine_cached(async_engine: AsyncEngine) -> Engine:
    """
    Get a sync engine from cache, creating if necessary.

    Args:
        async_engine: The async engine to convert

    Returns:
        A cached sync engine
    """
    return _sync_engine_cache.get_sync_engine(async_engine)


async def check_connection_health(engine: AsyncEngine, timeout: float = 5.0) -> bool:
    """
    Check if a database connection is healthy.

    Args:
        engine: The async engine to check
        timeout: Timeout in seconds for the health check

    Returns:
        True if connection is healthy, False otherwise
    """
    import asyncio

    from sqlalchemy import text

    try:
        async with asyncio.timeout(timeout):
            async with engine.begin() as connection:
                # Simple query to check connection
                result = await connection.execute(text("SELECT 1"))
                result.fetchone()
                return True
    except asyncio.TimeoutError:
        logger.warning(f"Connection health check timed out after {timeout}s")
        return False
    except Exception as e:
        logger.warning(f"Connection health check failed: {type(e).__name__}: {str(e)}")
        return False


def get_connection_pool_status(engine: AsyncEngine) -> dict[str, Any]:
    """
    Get connection pool status information.

    Args:
        engine: The async engine

    Returns:
        Dictionary with pool status information
    """
    status: dict[str, Any] = {
        "url": str(engine.url),
        "pool_size": None,
        "checked_in": None,
        "checked_out": None,
        "overflow": None,
    }

    try:
        pool = engine.pool
        if hasattr(pool, "size"):
            status["pool_size"] = pool.size()
        if hasattr(pool, "checkedin"):
            status["checked_in"] = pool.checkedin()
        if hasattr(pool, "checkedout"):
            status["checked_out"] = pool.checkedout()
        if hasattr(pool, "overflow"):
            status["overflow"] = pool.overflow()
    except Exception as e:
        logger.warning(f"Could not get pool status: {e}")
        status["error"] = str(e)

    return status
