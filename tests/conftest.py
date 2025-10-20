"""Pytest configuration and shared fixtures."""

import contextlib
import os
import tempfile

import pytest
from sqlalchemy import create_engine


@pytest.fixture(scope='session')
def temp_db_path():
    """Create a temporary database path for the test session."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    yield path
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


@pytest.fixture
def sqlite_engine():
    """Create a fresh SQLite engine for each test."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    engine = create_engine(f'sqlite:///{path}')

    yield engine

    engine.dispose()
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


@pytest.fixture
def memory_engine():
    """Create an in-memory SQLite engine."""
    engine = create_engine('sqlite:///:memory:')
    yield engine
    engine.dispose()

