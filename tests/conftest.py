"""Pytest configuration and shared fixtures."""

import contextlib
import os
import tempfile
from collections.abc import Generator

import pytest
from sqlalchemy import Engine, create_engine, text


@pytest.fixture(scope="session")
def temp_db_path():
    """Create a temporary database path for the test session."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


@pytest.fixture
def sqlite_engine():
    """Create a fresh SQLite engine for each test."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    engine = create_engine(f"sqlite:///{path}")

    yield engine

    engine.dispose()
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


@pytest.fixture
def memory_engine():
    """Create an in-memory SQLite engine."""
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


def _is_postgres_available() -> bool:
    """Check if PostgreSQL is available via testing.postgresql."""
    try:
        import testing.postgresql

        # Try to create a test instance to verify PostgreSQL is available
        with testing.postgresql.Postgresql() as postgresql:
            engine = create_engine(postgresql.url())
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
        return True
    except Exception:
        return False


def _is_mysql_available() -> bool:
    """Check if MySQL is available via testing.mysqld."""
    try:
        import testing.mysqld

        # Try to create a test instance to verify MySQL is available
        with testing.mysqld.Mysqld() as mysqld:
            engine = create_engine(mysqld.url())
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
        return True
    except Exception:
        return False


@pytest.fixture
def postgres_engine() -> Generator[Engine, None, None]:
    """Create a PostgreSQL engine for testing using testing.postgresql.

    This creates an isolated PostgreSQL instance for each test.
    No TEST_POSTGRES_URL required - uses temporary PostgreSQL instance.
    """
    try:
        import testing.postgresql

        # Create isolated PostgreSQL instance for this test
        postgresql = testing.postgresql.Postgresql()

        # Use NullPool to avoid connection pool issues
        from sqlalchemy.pool import NullPool

        engine = create_engine(postgresql.url(), poolclass=NullPool, echo=False)

        yield engine

        # Cleanup: dispose engine and stop PostgreSQL instance
        engine.dispose()
        postgresql.stop()
    except ImportError:
        pytest.skip("testing.postgresql not installed - run: pip install testing.postgresql")
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")


@pytest.fixture
def mysql_engine() -> Generator[Engine, None, None]:
    """Create a MySQL engine for testing using testing.mysqld.

    This creates an isolated MySQL instance for each test.
    No TEST_MYSQL_URL required - uses temporary MySQL instance.
    """
    try:
        import testing.mysqld

        # Create isolated MySQL instance for this test
        mysqld = testing.mysqld.Mysqld()

        # Use NullPool to avoid connection pool issues
        from sqlalchemy.pool import NullPool

        engine = create_engine(mysqld.url(), poolclass=NullPool, echo=False)

        yield engine

        # Cleanup: dispose engine and stop MySQL instance
        engine.dispose()
        mysqld.stop()
    except ImportError:
        pytest.skip("testing.mysqld not installed - run: pip install testing.mysqld")
    except Exception as e:
        pytest.skip(f"MySQL not available: {e}")


# Database types that can be tested
DATABASE_TYPES = ["sqlite"]


# Add PostgreSQL if available
if _is_postgres_available():
    DATABASE_TYPES.append("postgres")

# Add MySQL if available
if _is_mysql_available():
    DATABASE_TYPES.append("mysql")


@pytest.fixture(params=DATABASE_TYPES)
def db_engine(request) -> Generator[Engine, None, None]:
    """Parametrized fixture that provides engines for multiple database types.

    Tests using this fixture will run once for each available database.
    Defaults to SQLite. Add PostgreSQL/MySQL by setting TEST_POSTGRES_URL and/or TEST_MYSQL_URL.

    Usage:
        def test_something(db_engine):
            db = DataBase(db_engine)
            # test code...
    """
    if request.param == "sqlite":
        # Use in-memory SQLite for speed
        engine = create_engine("sqlite:///:memory:")
        yield engine
        engine.dispose()

    elif request.param == "postgres":
        try:
            import testing.postgresql

            # Create isolated PostgreSQL instance for this test
            postgresql = testing.postgresql.Postgresql()

            # Use NullPool to avoid connection pool issues
            from sqlalchemy.pool import NullPool

            engine = create_engine(postgresql.url(), poolclass=NullPool, echo=False)

            yield engine

            # Cleanup: dispose engine and stop PostgreSQL instance
            engine.dispose()
            postgresql.stop()
        except ImportError:
            pytest.skip("testing.postgresql not installed")
        except Exception as e:
            pytest.skip(f"PostgreSQL not available: {e}")

    elif request.param == "mysql":
        try:
            import testing.mysqld

            # Create isolated MySQL instance for this test
            mysqld = testing.mysqld.Mysqld()

            # Use NullPool to avoid connection pool issues
            from sqlalchemy.pool import NullPool

            engine = create_engine(mysqld.url(), poolclass=NullPool, echo=False)

            yield engine

            # Cleanup: dispose engine and stop MySQL instance
            engine.dispose()
            mysqld.stop()
        except ImportError:
            pytest.skip("testing.mysqld not installed")
        except Exception as e:
            pytest.skip(f"MySQL not available: {e}")


def pytest_collection_modifyitems(config, items):
    """Automatically mark PostgreSQL and MySQL parameterizations with markers.

    This allows running all PostgreSQL tests with: pytest -m postgres
    And all MySQL tests with: pytest -m mysql
    """
    for item in items:
        # Mark tests with postgres parameter (e.g., test_something[postgres])
        if hasattr(item, "callspec") and item.callspec:
            params = item.callspec.params
            if "db_engine" in params:
                if params["db_engine"] == "postgres":
                    item.add_marker(pytest.mark.postgres)
                elif params["db_engine"] == "mysql":
                    item.add_marker(pytest.mark.mysql)

        # Mark tests with postgres in their nodeid
        if "[postgres]" in item.nodeid:
            item.add_marker(pytest.mark.postgres)

        # Mark tests with mysql in their nodeid
        if "[mysql]" in item.nodeid:
            item.add_marker(pytest.mark.mysql)
