"""Tests for async AsyncDataBase and AsyncTableDataFrame classes.

Note: These tests require async SQLAlchemy drivers and pytest-green-light:
    pip install pytest-asyncio pytest-green-light aiosqlite greenlet

The pytest-green-light plugin automatically establishes greenlet context for SQLAlchemy
async engines, allowing these tests to run without MissingGreenlet errors.
"""

import contextlib
import os
import tempfile

import pandas as pd
import pytest

try:
    import pytest_asyncio
    from sqlalchemy.ext.asyncio import create_async_engine

    from pandalchemy import AsyncDataBase, AsyncTableDataFrame

    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
    pytest_asyncio = None

# pytest-green-light automatically establishes greenlet context for async tests
# Use pytest-asyncio marker for async test functions
pytestmark = pytest.mark.asyncio


@pytest.fixture
def sqlite_async_db_path():
    """Create a temporary database file path for async tests."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # Check if aiosqlite is available (import check only)
    try:
        import importlib.util

        spec = importlib.util.find_spec("aiosqlite")
        if spec is None:
            pytest.skip("aiosqlite not installed - async tests require: pip install aiosqlite")
    except Exception:
        pytest.skip("aiosqlite not installed - async tests require: pip install aiosqlite")

    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


# Helper function to create async engine in test context
async def _create_async_engine(db_path: str):
    """Helper to create async engine within test async context.

    Creates engine with lazy connection - actual connection happens on first use.
    Note: pytest-green-light should establish greenlet context via its autouse fixture,
    but if it doesn't work, we establish it here as a workaround.
    """
    from sqlalchemy.pool import NullPool

    # Workaround: Establish greenlet context right before engine operations
    # This is needed because pytest-green-light's fixture may not establish
    # context in the right async task context for SQLAlchemy's connection logic
    try:
        from sqlalchemy.util._concurrency_py3k import greenlet_spawn
    except ImportError:
        try:
            from sqlalchemy.util import greenlet_spawn
        except ImportError:
            greenlet_spawn = None

    # Create engine with connect_args to configure aiosqlite
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        echo=False,
        poolclass=NullPool,
        pool_pre_ping=False,
        # Configure aiosqlite connection args
        connect_args={"check_same_thread": False},
        future=True,
    )

    # Establish greenlet context right before first connection
    # This ensures it's in the same async context as the connection
    if greenlet_spawn is not None:

        def _noop() -> None:
            pass

        await greenlet_spawn(_noop)

    return engine


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})


async def test_async_database_initialization(sqlite_async_db_path):
    """Test AsyncDataBase initialization."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
    # but we also establish it here as a workaround since the plugin's
    # fixture context doesn't persist to the test's async context
    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        # load_tables() will establish greenlet context internally
        await db.load_tables()

        assert len(db) == 0
        assert isinstance(db, AsyncDataBase)
    finally:
        await engine.dispose()


async def test_async_table_creation_and_push(sqlite_async_db_path, sample_data):
    """Test creating a table and pushing it."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create table
        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )

        db["users"] = table
        await table.push()

        # Refresh and verify
        await db.load_tables()
        assert "users" in db
        assert len(db["users"]) == 3
        assert list(db["users"]["name"]) == ["Alice", "Bob", "Charlie"]
    finally:
        await engine.dispose()


async def test_async_table_modification_and_push(sqlite_async_db_path, sample_data):
    """Test modifying a table and pushing changes."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create and push initial table
        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["users"] = table
        await table.push()

        # Modify table
        db["users"].update_row(1, {"age": 26})
        db["users"].update_row(2, {"name": "Robert"})

        # Push changes
        await db.push()

        # Verify changes
        await db.load_tables()
        assert db["users"].get_row(1)["age"] == 26
        assert db["users"].get_row(2)["name"] == "Robert"
    finally:
        await engine.dispose()


async def test_async_table_pull(sqlite_async_db_path, sample_data):
    """Test pulling table data from database."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create and push initial table
        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["users"] = table
        await table.push()

        # Modify directly in database (using sync connection for simplicity)
        from sqlalchemy import create_engine, text

        sync_url = str(engine.url).replace("+aiosqlite", "")
        sync_engine = create_engine(sync_url)
        with sync_engine.begin() as conn:
            conn.execute(text("UPDATE users SET age = 100 WHERE id = 1"))
        sync_engine.dispose()

        # Pull and verify
        await db["users"].pull()
        assert db["users"].get_row(1)["age"] == 100
    finally:
        await engine.dispose()


async def test_async_insert_and_delete(sqlite_async_db_path, sample_data):
    """Test inserting and deleting rows."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create and push initial table
        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        db["users"] = table
        await table.push()

        # Add new row
        db["users"].add_row({"id": 4, "name": "David", "age": 40})

        # Delete a row
        db["users"].delete_row(2)

        # Push changes
        await db.push()

        # Verify
        await db.load_tables()
        assert len(db["users"]) == 3  # 4 total - 1 deleted = 3
        assert 4 in db["users"].index
        assert 2 not in db["users"].index
    finally:
        await engine.dispose()


async def test_async_database_push_parallel(sqlite_async_db_path, sample_data):
    """Test parallel push operations for multiple tables."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create multiple tables
        table1 = AsyncTableDataFrame(
            name="users1",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
        )
        table2 = AsyncTableDataFrame(
            name="users2",
            data=sample_data.copy(),
            primary_key="id",
            engine=engine,
            db=db,
        )

        db["users1"] = table1
        db["users2"] = table2

        # Push both tables
        await db.push(parallel=True)

        # Verify both tables exist
        await db.load_tables()
        assert "users1" in db
        assert "users2" in db
        assert len(db["users1"]) == 3
        assert len(db["users2"]) == 3
    finally:
        await engine.dispose()


async def test_async_table_with_conflict_resolution(sqlite_async_db_path, sample_data):
    """Test conflict resolution with async operations."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
    engine = await _create_async_engine(sqlite_async_db_path)
    try:
        db = AsyncDataBase(engine)
        await db.load_tables()

        # Create table with last_writer_wins strategy
        table = AsyncTableDataFrame(
            name="users",
            data=sample_data,
            primary_key="id",
            engine=engine,
            db=db,
            conflict_strategy="last_writer_wins",
        )
        db["users"] = table
        await table.push()

        # Modify locally
        db["users"].update_row(1, {"age": 99})

        # Modify in database directly (simulating concurrent modification)
        from sqlalchemy import create_engine, text

        sync_url = str(engine.url).replace("+aiosqlite", "")
        sync_engine = create_engine(sync_url)
        with sync_engine.begin() as conn:
            conn.execute(text("UPDATE users SET age = 88 WHERE id = 1"))
        sync_engine.dispose()

        # Push should resolve conflict (last_writer_wins)
        await db["users"].push()

        # Verify our local change won
        await db["users"].pull()
        assert db["users"].get_row(1)["age"] == 99
    finally:
        await engine.dispose()


async def test_async_table_dataframe_pandas_operations(sqlite_async_db_path, sample_data):
    """Test that AsyncTableDataFrame supports all pandas operations."""
    if not ASYNC_AVAILABLE:
        pytest.skip("Async drivers not available")

    # pytest-green-light automatically establishes greenlet context
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

        # Test pandas operations (should work synchronously)
        assert len(db["users"]) == 3
        assert "name" in db["users"].columns
        assert db["users"].loc[1, "name"] == "Alice"

        # Test filtering
        filtered = db["users"][db["users"]["age"] > 28]
        assert len(filtered) == 2

        # Test assignment
        db["users"].loc[1, "age"] = 27
        assert db["users"].loc[1, "age"] == 27
    finally:
        await engine.dispose()
