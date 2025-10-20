"""Tests for transaction safety and rollback behavior."""

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from pandalchemy import DataBase
from pandalchemy.sql_operations import (
    _pandas_dtype_to_python_type,
    create_table_from_dataframe,
    pull_table,
)


@pytest.fixture
def memory_db():
    """Create an in-memory database for testing."""
    engine = create_engine('sqlite:///:memory:')
    return DataBase(engine)


def test_successful_transaction_commit(memory_db):
    """Test that successful operations commit correctly."""
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [10, 20, 30]
    })

    memory_db.create_table('test', data, primary_key='id')

    # Make changes
    memory_db['test']['value'] = [11, 22, 33]

    # Push should commit
    memory_db['test'].push()

    # Verify changes persisted
    memory_db.pull()
    values = list(memory_db['test']['value'])
    assert values == [11, 22, 33]


def test_multi_table_transaction(memory_db):
    """Test that multi-table push uses single transaction."""
    data1 = pd.DataFrame({'id': [1, 2], 'val': ['a', 'b']})
    data2 = pd.DataFrame({'id': [1, 2], 'val': ['x', 'y']})

    memory_db.create_table('table1', data1, primary_key='id')
    memory_db.create_table('table2', data2, primary_key='id')

    # Modify both tables
    memory_db['table1']['val'] = ['a1', 'b1']
    memory_db['table2']['val'] = ['x1', 'y1']

    # Push all tables in single transaction
    memory_db.push()

    # Verify both tables updated
    memory_db.pull()
    assert list(memory_db['table1']['val']) == ['a1', 'b1']
    assert list(memory_db['table2']['val']) == ['x1', 'y1']


def test_transaction_isolation(memory_db):
    """Test that changes aren't visible until commit."""
    data = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    # Get initial state
    with memory_db.engine.connect() as conn:
        result = conn.execute(text('SELECT value FROM test WHERE id = 1'))
        initial_value = result.scalar()

    # Modify but don't push yet
    memory_db['test'].loc[1, 'value'] = 999

    # Value shouldn't change in database until push
    with memory_db.engine.connect() as conn:
        result = conn.execute(text('SELECT value FROM test WHERE id = 1'))
        current_value = result.scalar()

    assert current_value == initial_value  # Still original value

    # Now push
    memory_db['test'].push()

    # Value should now be updated
    with memory_db.engine.connect() as conn:
        result = conn.execute(text('SELECT value FROM test WHERE id = 1'))
        final_value = result.scalar()

    assert final_value == 999


def test_complex_transaction_atomicity(memory_db):
    """Test that complex operations are atomic."""
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'category': ['A', 'B', 'A', 'B', 'A'],
        'value': [10, 20, 30, 40, 50]
    })

    memory_db.create_table('test', data, primary_key='id')

    # Make multiple types of changes
    memory_db['test']['score'] = [100, 200, 300, 400, 500]  # Add column
    memory_db['test'].loc[1, 'value'] = 15  # Update
    memory_db['test'].drop(5, inplace=True)  # Delete

    # All changes should be applied together
    memory_db['test'].push()

    # Verify all changes applied
    memory_db.pull()
    assert 'score' in memory_db['test'].columns
    assert memory_db['test'].loc[1, 'value'] == 15
    assert 5 not in memory_db['test'].index
    assert len(memory_db['test']) == 4


def test_empty_transaction(memory_db):
    """Test that push with no changes doesn't error."""
    data = pd.DataFrame({'id': [1], 'value': [10]})

    memory_db.create_table('test', data, primary_key='id')

    # Don't make any changes
    # Push should succeed without doing anything
    memory_db['test'].push()

    # Verify data unchanged
    memory_db.pull()
    assert len(memory_db['test']) == 1


def test_large_batch_transaction(memory_db):
    """Test transaction with large number of operations."""
    # Create large dataset
    n = 500
    data = pd.DataFrame({
        'id': range(1, n + 1),
        'value': range(100, 100 + n)
    })

    memory_db.create_table('test', data, primary_key='id')

    # Make many updates
    memory_db['test']['value'] = memory_db['test']['value'] * 2

    # Should handle large batch
    memory_db['test'].push()

    # Verify
    memory_db.pull()
    assert memory_db['test'].loc[1, 'value'] == 200  # 100 * 2


def test_concurrent_table_updates(memory_db):
    """Test updating multiple tables concurrently."""
    data1 = pd.DataFrame({'id': [1, 2], 'val1': [10, 20]})
    data2 = pd.DataFrame({'id': [1, 2], 'val2': [30, 40]})
    data3 = pd.DataFrame({'id': [1, 2], 'val3': [50, 60]})

    memory_db.create_table('t1', data1, primary_key='id')
    memory_db.create_table('t2', data2, primary_key='id')
    memory_db.create_table('t3', data3, primary_key='id')

    # Modify all three
    memory_db['t1']['val1'] = [11, 22]
    memory_db['t2']['val2'] = [33, 44]
    memory_db['t3']['val3'] = [55, 66]

    # Push all in single transaction
    memory_db.push()

    # All should be updated
    memory_db.pull()
    assert list(memory_db['t1']['val1']) == [11, 22]
    assert list(memory_db['t2']['val2']) == [33, 44]
    assert list(memory_db['t3']['val3']) == [55, 66]


def test_transaction_with_schema_and_data_changes(memory_db):
    """Test transaction combining schema and data changes."""
    data = pd.DataFrame({'id': [1, 2], 'old_col': ['a', 'b']})

    memory_db.create_table('test', data, primary_key='id')

    # Schema change: add column
    memory_db['test']['new_col'] = ['x', 'y']

    # Data change: update existing
    memory_db['test'].loc[1, 'old_col'] = 'a_modified'

    # Both should be applied in one transaction
    memory_db['test'].push()

    # Verify
    memory_db.pull()
    assert 'new_col' in memory_db['test'].columns
    assert memory_db['test'].loc[1, 'old_col'] == 'a_modified'


def test_pull_table_with_data(memory_db):
    """Test pull_table reads data correctly."""
    # Create and populate table
    with memory_db.engine.begin() as conn:
        conn.execute(text('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'))
        conn.execute(text("INSERT INTO test VALUES (1, 'Alice', 25), (2, 'Bob', 30)"))

    df = pull_table(memory_db.engine, 'test')

    assert len(df) == 2
    assert list(df['name']) == ['Alice', 'Bob']
    assert list(df['age']) == [25, 30]


def test_create_table_if_exists_fail(memory_db):
    """Test that if_exists='fail' raises error for existing table."""
    df = pd.DataFrame({'id': [1], 'val': [10]})

    # Create once
    create_table_from_dataframe(memory_db.engine, 'test', df, 'id', '', 'fail')

    # Try to create again - should fail
    with pytest.raises(Exception):  # pandas raises ValueError
        create_table_from_dataframe(memory_db.engine, 'test', df, 'id', '', 'fail')


def test_pandas_dtype_conversions():
    """Test various dtype conversions."""
    assert _pandas_dtype_to_python_type('int32') == int
    assert _pandas_dtype_to_python_type('int64') == int
    assert _pandas_dtype_to_python_type('float32') == float
    assert _pandas_dtype_to_python_type('float64') == float
    assert _pandas_dtype_to_python_type('object') == str
    assert _pandas_dtype_to_python_type('string') == str
    assert _pandas_dtype_to_python_type('boolean') == bool
    assert _pandas_dtype_to_python_type('datetime64[ns]') == str

