"""Pytest configuration and shared fixtures."""

import asyncio
import contextlib
import os
import pathlib
import tempfile

import pytest
from sqlalchemy import create_engine, text

# Add project bin directory to PATH for MySQL compatibility wrapper
# This allows mysql_install_db wrapper to be found for MySQL 8.0+
_project_root = pathlib.Path(__file__).parent.parent
_bin_dir = _project_root / "bin"
if _bin_dir.exists():
    _bin_path = str(_bin_dir.absolute())
    if _bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{_bin_path}:{os.environ.get('PATH', '')}"


# Event loop fixture for async tests
# Using session scope may help with SQLAlchemy async greenlet context
# pytest-asyncio in auto mode works with this pattern
@pytest.fixture(scope="session")
def event_loop():
    """Create a session-scoped event loop for async tests.

    Session scope may help maintain greenlet context for SQLAlchemy async engines.
    """
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def temp_db_path():
    """Create a temporary database path for the test session."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


# Database type parameterization
# Dynamically determine available databases
def _get_available_databases():
    """Get list of available database types for testing."""
    available = ["sqlite"]  # SQLite is always available

    # Check PostgreSQL
    try:
        from testing.postgresql import Postgresql

        # Try to create a test instance (will fail if not possible)
        try:
            test_pg = Postgresql()
            test_pg.stop()
            available.append("postgres")
        except Exception:
            pass  # PostgreSQL not available, skip it
    except ImportError:
        pass  # testing.postgresql not installed

    # Check MySQL
    try:
        from testing.mysqld import Mysqld

        # Try to create a test instance (will fail if not possible)
        try:
            test_mysql = Mysqld()
            test_mysql.stop()
            available.append("mysql")
        except Exception:
            pass  # MySQL not available, skip it
    except ImportError:
        pass  # testing.mysqld not installed

    return available


DATABASE_TYPES = _get_available_databases()


# Session-scoped database instances for parallel execution
# Using pytest-xdist worker identification to ensure one instance per worker
_postgres_instances = {}
_mysql_instances = {}


@pytest.fixture(scope="session")
def _postgres_session(request):
    """Session-scoped PostgreSQL instance per pytest-xdist worker."""
    import os

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

    if worker_id not in _postgres_instances:
        try:
            from testing.postgresql import Postgresql

            _postgres_instances[worker_id] = Postgresql()
        except ImportError as e:
            raise RuntimeError(f"testing.postgresql not installed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"PostgreSQL not available: {e}") from e

    yield _postgres_instances[worker_id]


@pytest.fixture(scope="session")
def _mysql_session(request):
    """Session-scoped MySQL instance per pytest-xdist worker."""
    import os

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

    if worker_id not in _mysql_instances:
        try:
            from testing.mysqld import Mysqld

            _mysql_instances[worker_id] = Mysqld()
        except ImportError as e:
            raise RuntimeError(f"testing.mysqld not installed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"MySQL not available: {e}") from e

    yield _mysql_instances[worker_id]


def _get_postgres_instance():
    """Lazy initialization of PostgreSQL instance."""
    import os

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

    if worker_id not in _postgres_instances:
        try:
            from testing.postgresql import Postgresql

            _postgres_instances[worker_id] = Postgresql()
        except ImportError as e:
            raise RuntimeError(f"testing.postgresql not installed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"PostgreSQL not available: {e}") from e
    return _postgres_instances[worker_id]


def _get_mysql_instance():
    """Lazy initialization of MySQL instance."""
    import os

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

    if worker_id not in _mysql_instances:
        try:
            from testing.mysqld import Mysqld

            _mysql_instances[worker_id] = Mysqld()
        except ImportError as e:
            raise RuntimeError(f"testing.mysqld not installed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"MySQL not available: {e}") from e
    return _mysql_instances[worker_id]


@pytest.fixture(autouse=True)
def clean_database(db_engine):
    """Auto-use fixture that cleans database before each test.

    Drops all tables in PostgreSQL/MySQL to ensure test isolation.
    """
    # Only clean PostgreSQL and MySQL (SQLite is already isolated)
    if db_engine.dialect.name in ("postgresql", "mysql"):
        from sqlalchemy import MetaData, inspect

        inspector = inspect(db_engine)
        table_names = inspector.get_table_names()

        # Drop all tables
        if table_names:
            metadata = MetaData()
            metadata.reflect(bind=db_engine)
            with db_engine.begin() as conn:
                metadata.drop_all(conn, checkfirst=True)

        # Clean up sequences for PostgreSQL
        if db_engine.dialect.name == "postgresql":
            with db_engine.begin() as conn:
                result = conn.execute(
                    text("""
                    SELECT sequence_name
                    FROM information_schema.sequences
                    WHERE sequence_schema = 'public'
                """)
                )
                for row in result:
                    conn.execute(text(f'DROP SEQUENCE IF EXISTS "{row[0]}" CASCADE'))

    yield

    # Clean up after test too
    if db_engine.dialect.name in ("postgresql", "mysql"):
        from sqlalchemy import MetaData, inspect

        inspector = inspect(db_engine)
        table_names = inspector.get_table_names()

        if table_names:
            metadata = MetaData()
            metadata.reflect(bind=db_engine)
            with db_engine.begin() as conn:
                metadata.drop_all(conn, checkfirst=True)


@pytest.fixture(params=DATABASE_TYPES)
def db_engine(request):
    """Parametrized fixture for database engines.

    Creates engines for SQLite, PostgreSQL, and MySQL based on availability.
    Tests are skipped if the database is not available.
    """
    db_type = request.param

    if db_type == "sqlite":
        # Create a temporary SQLite database (function-scoped for isolation)
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        engine = create_engine(f"sqlite:///{path}")

        yield engine

        engine.dispose()
        with contextlib.suppress(OSError, PermissionError):
            os.remove(path)

    elif db_type == "postgres":
        # Use session-scoped PostgreSQL instance (lazy initialization)
        postgres = _get_postgres_instance()
        engine = create_engine(postgres.url())
        yield engine
        engine.dispose()

    elif db_type == "mysql":
        # Use session-scoped MySQL instance (lazy initialization)
        mysql = _get_mysql_instance()
        engine = create_engine(mysql.url())
        yield engine
        engine.dispose()


@pytest.fixture
def postgres_engine():
    """PostgreSQL-specific engine fixture."""
    # Raise error if PostgreSQL not available (don't skip)
    if "postgres" not in DATABASE_TYPES:
        raise RuntimeError(
            "PostgreSQL not available - not in available databases. "
            "Install PostgreSQL or testing.postgresql to enable PostgreSQL tests."
        )
    postgres = _get_postgres_instance()
    engine = create_engine(postgres.url())
    yield engine
    engine.dispose()


@pytest.fixture
def mysql_engine():
    """MySQL-specific engine fixture."""
    # Raise error if MySQL not available (don't skip)
    if "mysql" not in DATABASE_TYPES:
        raise RuntimeError(
            "MySQL not available - not in available databases. "
            "Install MySQL or testing.mysqld to enable MySQL tests."
        )
    mysql = _get_mysql_instance()
    engine = create_engine(mysql.url())
    yield engine
    engine.dispose()


@pytest.fixture
def sqlite_engine(tmp_path):
    """SQLite-specific engine fixture."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    yield engine
    engine.dispose()


def pytest_collection_modifyitems(config, items):
    """Modify test collection to deselect tests that require unavailable databases."""
    # Deselect PostgreSQL-specific tests if PostgreSQL is not available
    if "postgres" not in DATABASE_TYPES:
        deselected = []
        for item in items[:]:  # Copy list to iterate safely
            # Check if test uses postgres_engine fixture or has @pytest.mark.postgres
            if "postgres_engine" in item.fixturenames or item.get_closest_marker("postgres"):
                items.remove(item)
                deselected.append(item)
        if deselected:
            config.hook.pytest_deselected(items=deselected)

    # Deselect MySQL-specific tests if MySQL is not available
    if "mysql" not in DATABASE_TYPES:
        deselected = []
        for item in items[:]:  # Copy list to iterate safely
            # Check if test uses mysql_engine fixture or has @pytest.mark.mysql
            if "mysql_engine" in item.fixturenames or item.get_closest_marker("mysql"):
                items.remove(item)
                deselected.append(item)
        if deselected:
            config.hook.pytest_deselected(items=deselected)
