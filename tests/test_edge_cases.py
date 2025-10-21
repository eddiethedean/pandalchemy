"""Tests for edge cases across modules."""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame


@pytest.fixture
def memory_db():
    """Create an in-memory database for testing."""
    engine = create_engine('sqlite:///:memory:')
    return DataBase(engine)


def test_empty_dataframe_table(memory_db):
    """Test working with completely empty DataFrame."""
    # Create table from empty DataFrame
    empty_df = pd.DataFrame(columns=['id', 'name', 'value'])
    empty_df = empty_df.astype({'id': 'int64'})

    # This might not work for all databases, but test the structure
    table = TableDataFrame('empty_test', data=empty_df, primary_key='id', engine=memory_db.engine)

    assert len(table) == 0
    assert 'name' in table.columns


def test_single_row_table(memory_db):
    """Test table with only one row."""
    data = pd.DataFrame({'id': [1], 'value': [100]})

    memory_db.create_table('single', data, primary_key='id')

    # Modify the single row
    memory_db['single'].loc[1, 'value'] = 200

    memory_db['single'].push()

    # Verify
    memory_db.pull()
    assert memory_db['single'].loc[1, 'value'] == 200


def test_single_column_table(memory_db):
    """Test table with only one column (plus primary key)."""
    data = pd.DataFrame({'id': [1, 2, 3]})

    memory_db.create_table('single_col', data, primary_key='id')

    # Add a row using proper API
    memory_db['single_col'].add_row({'id': 4})

    memory_db['single_col'].push()

    memory_db.pull()
    assert len(memory_db['single_col']) == 4


def test_table_with_none_schema():
    """Test table creation with None schema."""
    engine = create_engine('sqlite:///:memory:')

    data = pd.DataFrame({'id': [1, 2], 'val': [10, 20]})

    table = TableDataFrame('test', data=data, primary_key='id', engine=engine, schema=None)

    assert table.schema is None
    assert len(table) == 2


def test_database_with_no_tables():
    """Test database with no tables."""
    engine = create_engine('sqlite:///:memory:')

    db = DataBase(engine)

    assert len(db) == 0
    assert list(db.table_names) == []


def test_table_len_method(memory_db):
    """Test __len__ method on Table."""
    data = pd.DataFrame({'id': [1, 2, 3, 4, 5], 'val': list(range(5))})

    memory_db.create_table('test', data, primary_key='id')

    assert len(memory_db['test']) == 5


def test_table_repr(memory_db):
    """Test __repr__ method."""
    data = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})

    memory_db.create_table('test', data, primary_key='id')

    repr_str = repr(memory_db['test'])

    assert 'TableDataFrame' in repr_str
    # Data should be shown in repr
    assert 'name' in repr_str or 'A' in repr_str


def test_database_repr(memory_db):
    """Test DataBase __repr__ method."""
    data = pd.DataFrame({'id': [1], 'val': [10]})

    memory_db.create_table('test', data, primary_key='id')

    repr_str = repr(memory_db)

    assert 'DataBase' in repr_str
    assert 'test' in repr_str


def test_table_copy_method(memory_db):
    """Test Table.copy() method."""
    data = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    # Copy the table
    table_copy = memory_db['test'].copy()

    # Modify copy
    table_copy['value'] = [100, 200]

    # Original should be unchanged
    assert list(memory_db['test']['value']) == [10, 20]


def test_table_head_method(memory_db):
    """Test Table.head() method."""
    data = pd.DataFrame({'id': range(1, 11), 'val': range(10, 20)})

    memory_db.create_table('test', data, primary_key='id')

    head_df = memory_db['test'].head(3)

    assert len(head_df) == 3


def test_table_tail_method(memory_db):
    """Test Table.tail() method."""
    data = pd.DataFrame({'id': range(1, 11), 'val': range(10, 20)})

    memory_db.create_table('test', data, primary_key='id')

    tail_df = memory_db['test'].tail(3)

    assert len(tail_df) == 3


def test_table_shape_property(memory_db):
    """Test Table.shape property."""
    data = pd.DataFrame({'id': [1, 2, 3], 'val1': [10, 20, 30], 'val2': [100, 200, 300]})

    memory_db.create_table('test', data, primary_key='id')

    assert memory_db['test'].shape == (3, 2)  # 3 rows, 2 columns (excluding index)


def test_table_index_property(memory_db):
    """Test Table.index property."""
    data = pd.DataFrame({'id': [1, 2, 3], 'val': [10, 20, 30]})

    memory_db.create_table('test', data, primary_key='id')

    assert list(memory_db['test'].index) == [1, 2, 3]


def test_table_columns_property(memory_db):
    """Test Table.columns property."""
    data = pd.DataFrame({'id': [1, 2], 'col_a': ['A', 'B'], 'col_b': [10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    cols = list(memory_db['test'].columns)
    assert 'col_a' in cols
    assert 'col_b' in cols


def test_table_loc_property(memory_db):
    """Test Table.loc property access."""
    data = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})

    memory_db.create_table('test', data, primary_key='id')

    value = memory_db['test'].loc[2, 'value']
    assert value == 20


def test_table_iloc_property(memory_db):
    """Test Table.iloc property access."""
    data = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})

    memory_db.create_table('test', data, primary_key='id')

    value = memory_db['test'].iloc[1, 0]  # Second row, first column
    assert value == 20


def test_table_at_property(memory_db):
    """Test Table.at property access."""
    data = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    value = memory_db['test'].at[1, 'value']
    assert value == 10


def test_table_iat_property(memory_db):
    """Test Table.iat property access."""
    data = pd.DataFrame({'id': [1, 2], 'value': [10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    value = memory_db['test'].iat[0, 0]  # First row, first column
    assert value == 10


def test_add_table_method(memory_db):
    """Test DataBase.add_table() method."""
    data = pd.DataFrame({'id': [1, 2], 'val': [10, 20]})

    table = TableDataFrame('new_table', data=data, primary_key='id', engine=memory_db.engine)

    memory_db.add_table(table, push=False)

    assert 'new_table' in memory_db.table_names


def test_add_table_with_push(memory_db):
    """Test DataBase.add_table() with immediate push."""
    data = pd.DataFrame({'id': [1], 'val': [10]})

    table = TableDataFrame('pushed_table', data=data, primary_key='id', engine=memory_db.engine)

    memory_db.add_table(table, push=True)

    # Verify table exists in database
    memory_db.pull()
    assert 'pushed_table' in memory_db.table_names


def test_table_sort_values(memory_db):
    """Test Table.sort_values() method."""
    data = pd.DataFrame({'id': [3, 1, 2], 'value': [30, 10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    # Sort by value
    memory_db['test'].sort_values('value', inplace=True)

    # Check order
    values = list(memory_db['test']['value'])
    assert values == [10, 20, 30]


def test_table_rename_method(memory_db):
    """Test Table.rename() method."""
    data = pd.DataFrame({'id': [1, 2], 'old': ['A', 'B']})

    memory_db.create_table('test', data, primary_key='id')

    memory_db['test'].rename(columns={'old': 'new'}, inplace=True)

    assert 'new' in memory_db['test'].columns
    assert 'old' not in memory_db['test'].columns


def test_table_get_changes_summary(memory_db):
    """Test Table.get_changes_summary() method."""
    data = pd.DataFrame({'id': [1, 2], 'val': [10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    # Make a change
    memory_db['test']['new_col'] = [100, 200]

    summary = memory_db['test'].get_changes_summary()

    assert 'has_changes' in summary
    assert summary['has_changes'] is True
    assert summary['columns_added'] == 1


def test_database_len(memory_db):
    """Test DataBase __len__ method."""
    data1 = pd.DataFrame({'id': [1], 'val': [10]})
    data2 = pd.DataFrame({'id': [1], 'val': [20]})

    memory_db.create_table('t1', data1, primary_key='id')
    memory_db.create_table('t2', data2, primary_key='id')

    assert len(memory_db) == 2


def test_database_setitem(memory_db):
    """Test DataBase __setitem__ method."""
    data = pd.DataFrame({'id': [1], 'val': [10]})

    table = TableDataFrame('new', data=data, primary_key='id', engine=memory_db.engine)

    memory_db['new'] = table

    assert 'new' in memory_db.table_names


def test_table_column_names_property(memory_db):
    """Test Table.column_names property (for ITable interface)."""
    data = pd.DataFrame({'id': [1], 'col1': ['A'], 'col2': [10]})

    memory_db.create_table('test', data, primary_key='id')

    # TableDataFrame uses .columns directly (pandas attribute)
    col_names = memory_db['test'].columns

    assert 'col1' in col_names
    assert 'col2' in col_names


def test_table_to_pandas(memory_db):
    """Test Table.to_pandas() conversion."""
    data = pd.DataFrame({'id': [1, 2], 'val': [10, 20]})

    memory_db.create_table('test', data, primary_key='id')

    pandas_df = memory_db['test'].to_pandas()

    assert isinstance(pandas_df, pd.DataFrame)
    assert len(pandas_df) == 2

