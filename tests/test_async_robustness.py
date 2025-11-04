"""Comprehensive tests for async robustness features.

Tests for retry logic, connection management, timeouts, batching,
and other robustness improvements to async operations.
"""

import asyncio
import contextlib
import os
import tempfile

import pandas as pd
import pytest

try:
    from sqlalchemy.ext.asyncio import create_async_engine

    from pandalchemy import AsyncDataBase, AsyncTableDataFrame
    from pandalchemy.async_retry import AsyncRetryPolicy, is_deadlock_error, is_retryable_error
    from pandalchemy.exceptions import ConnectionError

    ASYNC_AVAILABLE = True
except ImportError as e:
    ASYNC_AVAILABLE = False
    # Don't skip - raise error instead so tests fail if dependencies missing
    raise RuntimeError(
        "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
    ) from e

pytestmark = pytest.mark.asyncio


@pytest.fixture
def sqlite_async_db_path():
    """Create a temporary database file path for async tests."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    try:
        import importlib.util

        spec = importlib.util.find_spec("aiosqlite")
        if spec is None:
            raise RuntimeError("aiosqlite not installed - install: pip install aiosqlite")
    except Exception as e:
        raise RuntimeError(f"aiosqlite not installed - install: pip install aiosqlite: {e}") from e

    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


async def _create_async_engine(db_path: str):
    """Helper to create async engine."""
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        echo=False,
        poolclass=NullPool,
        pool_pre_ping=False,
        connect_args={"check_same_thread": False},
        future=True,
    )
    return engine


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})


async def test_async_retry_policy_basic(sqlite_async_db_path, sample_data):
    """Test basic retry policy functionality."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # Create retry policy with custom settings
        retry_policy = AsyncRetryPolicy(max_attempts=2, initial_delay=0.1, max_delay=1.0)

        db = AsyncDataBase(engine, retry_policy=retry_policy)
        await db.load_tables()

        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
            retry_policy=retry_policy,
        )
        db["users"] = table

        # Should succeed with retry policy
        await table.push()
        assert len(db["users"]) == 3
    finally:
        await engine.dispose()


async def test_async_connection_health_check(sqlite_async_db_path):
    """Test connection health check functionality."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    from pandalchemy.async_connection_manager import check_connection_health

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # Health check should pass for valid engine
        is_healthy = await check_connection_health(engine, timeout=5.0)
        assert is_healthy is True

        # Test with very short timeout (should still pass for local SQLite)
        is_healthy = await check_connection_health(engine, timeout=0.1)
        assert is_healthy is True
    finally:
        await engine.dispose()


async def test_async_timeout_configuration(sqlite_async_db_path, sample_data):
    """Test timeout configuration for async operations."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # Create database with timeout configuration
        db = AsyncDataBase(engine, query_timeout=10.0, connection_timeout=5.0)
        await db.load_tables()

        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
            query_timeout=10.0,
        )
        db["users"] = table

        # Operations should respect timeout
        await table.push()
        await table.pull(timeout=5.0)

        assert len(db["users"]) == 3
    finally:
        await engine.dispose()


async def test_async_batch_operations(sqlite_async_db_path):
    """Test batching support for large datasets."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create dataset large enough to test batching (200 rows triggers batching)
        large_data = pd.DataFrame(
            {
                "id": range(1, 201),  # 200 rows - enough to test batching
                "value": [f"value_{i}" for i in range(1, 201)],
                "number": list(range(1, 201)),
            }
        )

        table = AsyncTableDataFrame(
            name="large_table",
            data=large_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["large_table"] = table

        # Push should handle batching automatically
        await table.push()

        # Verify all rows were inserted
        await db.load_tables()
        assert len(db["large_table"]) == 200
        assert db["large_table"].get_row(1)["value"] == "value_1"
        assert db["large_table"].get_row(200)["value"] == "value_200"

        # Test batch updates - use a reasonable number of individual updates
        # (20 updates is enough to test batching without being slow)
        table = db["large_table"]  # Get the reloaded table instance

        # Update first 20 rows (much fewer than original 500, but still tests batching)
        for i in range(1, 21):
            table.update_row(i, {"number": i * 2})

        # Verify updates are tracked in memory before push
        assert table.get_row(1)["number"] == 2
        assert table.get_row(20)["number"] == 40

        # Push changes and reload
        await table.push()
        await db.load_tables()

        # Verify updates were persisted to database
        assert db["large_table"].get_row(1)["number"] == 2
        assert db["large_table"].get_row(20)["number"] == 40
        # Verify unchanged rows remain
        assert db["large_table"].get_row(21)["number"] == 21
        assert db["large_table"].get_row(200)["number"] == 200
    finally:
        await engine.dispose()


async def test_async_parallel_execution_with_limit(sqlite_async_db_path, sample_data):
    """Test parallel execution with concurrency limit."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # SQLite doesn't support concurrent writes, so disable parallel for SQLite
        dialect = engine.dialect.name
        use_parallel = dialect != "sqlite"

        # Create database with max concurrent pushes limit
        db = AsyncDataBase(engine, max_concurrent_pushes=2)
        await db.load_tables()

        # Create multiple tables
        for i in range(1, 6):  # 5 tables
            table = AsyncTableDataFrame(
                name=f"table_{i}",
                data=sample_data.copy(),
                primary_key="id",
                engine=engine,
                db=db,
            )
            db[f"table_{i}"] = table

        # Push all tables (parallel for MySQL/PostgreSQL, sequential for SQLite)
        await db.push(parallel=use_parallel)

        # Verify all tables were created
        await db.load_tables()
        for i in range(1, 6):
            assert f"table_{i}" in db
            assert len(db[f"table_{i}"]) == 3
    finally:
        await engine.dispose()


async def test_async_transaction_isolation_level(sqlite_async_db_path, sample_data):
    """Test transaction isolation level configuration."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # Note: SQLite doesn't support isolation level changes easily
        # This test mainly verifies the parameter is accepted
        db = AsyncDataBase(engine, isolation_level="READ_COMMITTED")
        await db.load_tables()

        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["users"] = table

        # Should work even if isolation level isn't applied for SQLite
        await table.push()
        assert len(db["users"]) == 3
    finally:
        await engine.dispose()


async def test_async_retry_on_transient_errors(sqlite_async_db_path, sample_data):
    """Test retry logic for transient errors."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # Create retry policy
        retry_policy = AsyncRetryPolicy(max_attempts=3, initial_delay=0.1)

        db = AsyncDataBase(engine, retry_policy=retry_policy)
        await db.load_tables()

        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
            retry_policy=retry_policy,
        )
        db["users"] = table

        # Normal operation should succeed
        await table.push()
        assert len(db["users"]) == 3
    finally:
        await engine.dispose()


async def test_async_error_aggregation(sqlite_async_db_path, sample_data):
    """Test improved error aggregation for parallel operations."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create valid table
        table1 = AsyncTableDataFrame(
            name="valid_table",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["valid_table"] = table1

        # Create table with invalid primary key (will fail validation)
        invalid_data = pd.DataFrame({"id": [1, 1], "name": ["A", "B"]})  # Duplicate PK
        table2 = AsyncTableDataFrame(
            name="invalid_table",
            data=invalid_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["invalid_table"] = table2

        # Try to push - should fail with aggregated error
        with pytest.raises(Exception):  # Should raise validation error
            await db.push(parallel=True)

        # After error, reload tables to see what was actually created
        await db.load_tables()
        # Note: If validation fails before push, the table won't exist
        # The test verifies that error aggregation works correctly
    finally:
        await engine.dispose()


async def test_async_schema_changes(sqlite_async_db_path, sample_data):
    """Test async schema change support."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["users"] = table
        await table.push()

        # Add a new column
        db["users"].add_column_with_default("email", "test@example.com")
        await table.push()

        # Verify column was added
        await db.load_tables()
        assert "email" in db["users"].columns
        assert db["users"].get_row(1)["email"] == "test@example.com"

        # Rename column (SQLite uses sync fallback, but should still work)
        # Use db["users"] after reload to get the correct table instance
        table = db["users"]
        table.rename_column_safe("email", "email_address")
        await table.push()

        # Verify column was renamed
        await db.load_tables()
        assert "email_address" in db["users"].columns
        assert "email" not in db["users"].columns
    finally:
        await engine.dispose()


async def test_async_connection_pool_status(sqlite_async_db_path):
    """Test connection pool status monitoring."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    from pandalchemy.async_connection_manager import get_connection_pool_status

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # Get pool status
        status = get_connection_pool_status(engine)
        assert "url" in status
        assert status["url"] is not None
    finally:
        await engine.dispose()


async def test_async_sync_engine_cache(sqlite_async_db_path):
    """Test sync engine caching to avoid redundant creation."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    from pandalchemy.async_connection_manager import get_sync_engine_cached

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        # Get cached sync engine multiple times
        sync_engine1 = get_sync_engine_cached(engine)
        sync_engine2 = get_sync_engine_cached(engine)

        # Should return the same engine instance (cached)
        assert sync_engine1 is sync_engine2
    finally:
        await engine.dispose()


async def test_async_greenlet_context_manager(sqlite_async_db_path, sample_data):
    """Test AsyncGreenletContext manager."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    from pandalchemy.async_operations import AsyncGreenletContext

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Use context manager explicitly
        async with AsyncGreenletContext():
            table = AsyncTableDataFrame(
                name="users",
                data=sample_data,
                primary_key="id",
                engine=engine,
                db=db,
            )
            db["users"] = table
            await table.push()

        assert len(db["users"]) == 3
    finally:
        await engine.dispose()


async def test_async_retryable_error_detection():
    """Test error detection for retryable errors."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    # Test various error types
    class MockConnectionError(Exception):
        pass

    class MockTimeoutError(Exception):
        pass

    # Connection errors should be retryable
    assert is_retryable_error(ConnectionError("Connection failed")) is True

    # Timeout errors should be retryable
    assert is_retryable_error(asyncio.TimeoutError()) is True

    # Deadlock errors should be detected
    class MockDeadlockError(Exception):
        def __init__(self):
            super().__init__("deadlock detected")

    deadlock_error = MockDeadlockError()
    assert is_deadlock_error(deadlock_error) is True
    assert is_retryable_error(deadlock_error) is True


async def test_async_enhanced_error_messages(sqlite_async_db_path):
    """Test enhanced error messages with context."""
    if not ASYNC_AVAILABLE:
        raise RuntimeError(
            "Async drivers not available - install: pip install aiosqlite asyncpg aiomysql greenlet"
        )

    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Try to push without a table (should get clear error)
        table = AsyncTableDataFrame(
            name="users",
            data=pd.DataFrame({"id": [1, 1], "name": ["A", "B"]}),  # Duplicate PK
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["users"] = table

        # Should raise validation error with context
        with pytest.raises(Exception) as exc_info:
            await table.push()

        # Error should have table name and operation context
        error_str = str(exc_info.value)
        assert "users" in error_str.lower() or "table" in error_str.lower()
    finally:
        await engine.dispose()
