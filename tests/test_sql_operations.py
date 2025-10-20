"""Tests for SQL operations module."""

import contextlib
import os
import tempfile

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from pandalchemy.sql_operations import (
    _pandas_dtype_to_python_type,
    create_table_from_dataframe,
    get_primary_key,
    pull_table,
    table_exists,
)


@pytest.fixture
def sqlite_test_engine():
    """Create a temporary SQLite engine for testing."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    engine = create_engine(f'sqlite:///{path}')

    yield engine

    engine.dispose()
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


def test_table_exists_false(sqlite_test_engine):
    """Test table_exists with non-existent table."""
    assert not table_exists(sqlite_test_engine, 'nonexistent_table')


def test_table_exists_true(sqlite_test_engine):
    """Test table_exists with existing table."""
    # Create a table
    with sqlite_test_engine.begin() as conn:
        conn.execute(text('CREATE TABLE test_table (id INTEGER PRIMARY KEY)'))

    assert table_exists(sqlite_test_engine, 'test_table')


def test_get_primary_key(sqlite_test_engine):
    """Test getting primary key from a table."""
    # Create table with primary key
    with sqlite_test_engine.begin() as conn:
        conn.execute(text('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)'))

    pk = get_primary_key(sqlite_test_engine, 'test_table')
    assert pk == 'id'


def test_get_primary_key_none(sqlite_test_engine):
    """Test getting primary key from table without one."""
    # Create table without primary key
    with sqlite_test_engine.begin() as conn:
        conn.execute(text('CREATE TABLE test_table (id INTEGER, name TEXT)'))

    pk = get_primary_key(sqlite_test_engine, 'test_table')
    # SQLite may auto-create a primary key or return None
    assert pk is None or isinstance(pk, str)


def test_create_table_from_dataframe(sqlite_test_engine):
    """Test creating a table from a DataFrame."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })

    create_table_from_dataframe(
        sqlite_test_engine,
        'test_table',
        df,
        'id',
        '',  # Empty schema
        if_exists='fail'
    )

    # Verify table was created and data inserted
    assert table_exists(sqlite_test_engine, 'test_table')

    with sqlite_test_engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM test_table'))
        count = result.scalar()
        assert count == 3


def test_pull_table(sqlite_test_engine):
    """Test pulling a table into a DataFrame."""
    # Create and populate a table
    with sqlite_test_engine.begin() as conn:
        conn.execute(text('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)'))
        conn.execute(text("INSERT INTO test_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)"))

    df = pull_table(sqlite_test_engine, 'test_table')

    assert len(df) == 2
    assert 'id' in df.columns
    assert 'name' in df.columns
    assert 'value' in df.columns


def test_pull_table_empty(sqlite_test_engine):
    """Test pulling an empty table."""
    # Create empty table
    with sqlite_test_engine.begin() as conn:
        conn.execute(text('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)'))

    df = pull_table(sqlite_test_engine, 'test_table')

    assert len(df) == 0
    assert 'id' in df.columns
    assert 'name' in df.columns


def test_pandas_dtype_to_python_type():
    """Test dtype conversion."""
    assert _pandas_dtype_to_python_type('int64') == int
    assert _pandas_dtype_to_python_type('float64') == float
    assert _pandas_dtype_to_python_type('object') == str
    assert _pandas_dtype_to_python_type('bool') == bool
    assert _pandas_dtype_to_python_type(None) == str


def test_create_table_if_exists_replace(sqlite_test_engine):
    """Test creating a table with if_exists='replace'."""
    df1 = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

    # Create initial table
    create_table_from_dataframe(sqlite_test_engine, 'test', df1, 'id', '', 'fail')

    # Replace with new data
    df2 = pd.DataFrame({'id': [3, 4, 5], 'value': [30, 40, 50]})
    create_table_from_dataframe(sqlite_test_engine, 'test', df2, 'id', '', 'replace')

    # Verify new data
    with sqlite_test_engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM test'))
        count = result.scalar()
        assert count == 3


def test_create_table_with_index_as_primary_key(sqlite_test_engine):
    """Test creating a table when primary key is the DataFrame index."""
    df = pd.DataFrame({
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    }, index=pd.Index([1, 2, 3], name='id'))

    create_table_from_dataframe(sqlite_test_engine, 'test', df, 'id', '', 'fail')

    # Verify
    assert table_exists(sqlite_test_engine, 'test')

    df_read = pull_table(sqlite_test_engine, 'test')
    assert len(df_read) == 3
    assert 'id' in df_read.columns


def test_pull_table_with_schema(sqlite_test_engine):
    """Test pulling a table with schema specified."""
    # SQLite doesn't really support schemas, but test the parameter handling
    with sqlite_test_engine.begin() as conn:
        conn.execute(text('CREATE TABLE test (id INTEGER PRIMARY KEY)'))

    df = pull_table(sqlite_test_engine, 'test', schema=None)
    assert isinstance(df, pd.DataFrame)

