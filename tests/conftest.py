"""Pytest configuration and shared fixtures."""

import asyncio
import contextlib
import os
import tempfile

import pytest
from sqlalchemy import create_engine, text


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
DATABASE_TYPES = ["sqlite", "postgres", "mysql"]


def _is_postgres_available() -> bool:
    """Check if PostgreSQL is available for testing."""
    postgres_url = os.environ.get("TEST_POSTGRES_URL")
    if not postgres_url:
        return False
    try:
        from sqlalchemy import create_engine

        engine = create_engine(postgres_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


def _is_mysql_available() -> bool:
    """Check if MySQL is available for testing."""
    mysql_url = os.environ.get("TEST_MYSQL_URL")
    if not mysql_url:
        return False
    try:
        from sqlalchemy import create_engine

        engine = create_engine(mysql_url)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


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
        except ImportError:
            pytest.skip("testing.postgresql not installed")
        except Exception as e:
            # If we can't create PostgreSQL (e.g., shared memory limit), skip
            pytest.skip(f"PostgreSQL not available: {e}")

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
        except ImportError:
            pytest.skip("testing.mysqld not installed")
        except Exception as e:
            # If we can't create MySQL, skip
            pytest.skip(f"MySQL not available: {e}")

    yield _mysql_instances[worker_id]


def _get_postgres_instance():
    """Lazy initialization of PostgreSQL instance."""
    import os

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

    if worker_id not in _postgres_instances:
        try:
            from testing.postgresql import Postgresql

            _postgres_instances[worker_id] = Postgresql()
        except ImportError:
            pytest.skip("testing.postgresql not installed")
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")
    return _postgres_instances[worker_id]


def _get_mysql_instance():
    """Lazy initialization of MySQL instance."""
    import os

    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")

    if worker_id not in _mysql_instances:
        try:
            from testing.mysqld import Mysqld

            _mysql_instances[worker_id] = Mysqld()
        except ImportError:
            pytest.skip("testing.mysqld not installed")
        except Exception as e:
            pytest.skip(f"MySQL not available: {e}")
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
    postgres = _get_postgres_instance()
    engine = create_engine(postgres.url())
    yield engine
    engine.dispose()


@pytest.fixture
def mysql_engine():
    """MySQL-specific engine fixture."""
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
