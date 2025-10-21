"""Tests for auto-increment primary key functionality."""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame
from pandalchemy.exceptions import DataValidationError


@pytest.fixture
def sample_df_with_index():
    """Create a sample DataFrame with integer PK as index."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }).set_index('id')


# ============================================================================
# TableDataFrame get_next_pk_value() Tests
# ============================================================================

def test_get_next_pk_value_empty_dataframe():
    """Test get_next_pk_value returns 1 for empty DataFrame."""
    df = pd.DataFrame({'id': [], 'name': []}).set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    next_pk = tdf.get_next_pk_value()
    assert next_pk == 1


def test_get_next_pk_value_with_data(sample_df_with_index):
    """Test get_next_pk_value returns max + 1."""
    tdf = TableDataFrame(data=sample_df_with_index, primary_key='id')

    next_pk = tdf.get_next_pk_value()
    assert next_pk == 4  # max is 3, so next is 4


def test_get_next_pk_value_with_gaps(sample_df_with_index):
    """Test get_next_pk_value handles gaps in sequence."""
    tdf = TableDataFrame(data=sample_df_with_index, primary_key='id')

    # Delete row 2 (creates gap)
    tdf.delete_row(2)

    # Should still return max + 1 (4), not fill gap
    next_pk = tdf.get_next_pk_value()
    assert next_pk == 4


def test_get_next_pk_value_composite_key_raises_error():
    """Test get_next_pk_value raises error for composite keys."""
    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    }).set_index(['user_id', 'org_id'])

    tdf = TableDataFrame(data=df, primary_key=['user_id', 'org_id'])

    with pytest.raises(ValueError, match="single-column primary keys"):
        tdf.get_next_pk_value()


def test_get_next_pk_value_non_integer_raises_error():
    """Test get_next_pk_value raises error for non-integer PK."""
    df = pd.DataFrame({
        'user_id': ['u1', 'u2', 'u3'],
        'name': ['Alice', 'Bob', 'Charlie']
    }).set_index('user_id')

    tdf = TableDataFrame(data=df, primary_key='user_id')

    with pytest.raises(ValueError, match="must be integer type"):
        tdf.get_next_pk_value()


# ============================================================================
# add_row() with auto_increment Tests
# ============================================================================

def test_add_row_auto_increment_generates_pk(sample_df_with_index):
    """Test add_row with auto_increment generates next PK."""
    tdf = TableDataFrame(data=sample_df_with_index, primary_key='id')

    # Add row without PK, using auto_increment
    tdf.add_row({'name': 'Dave', 'age': 40}, auto_increment=True)

    # Should have auto-generated id = 4
    assert tdf.row_exists(4)
    assert tdf.get_row(4)['name'] == 'Dave'


def test_add_row_auto_increment_empty_table():
    """Test auto_increment starts at 1 for empty table."""
    df = pd.DataFrame({'id': [], 'name': [], 'age': []}).set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    tdf.add_row({'name': 'Alice', 'age': 25}, auto_increment=True)

    assert tdf.row_exists(1)
    assert tdf.get_row(1)['name'] == 'Alice'


def test_add_row_auto_increment_multiple_sequential(sample_df_with_index):
    """Test multiple auto_increment additions create sequential PKs."""
    tdf = TableDataFrame(data=sample_df_with_index, primary_key='id')

    tdf.add_row({'name': 'Dave', 'age': 40}, auto_increment=True)
    tdf.add_row({'name': 'Eve', 'age': 45}, auto_increment=True)
    tdf.add_row({'name': 'Frank', 'age': 50}, auto_increment=True)

    assert tdf.row_exists(4)
    assert tdf.row_exists(5)
    assert tdf.row_exists(6)


def test_add_row_explicit_pk_overrides_auto_increment(sample_df_with_index):
    """Test that explicit PK value works even with auto_increment=True."""
    tdf = TableDataFrame(data=sample_df_with_index, primary_key='id')

    # Provide explicit PK even though auto_increment=True
    tdf.add_row({'id': 100, 'name': 'Dave', 'age': 40}, auto_increment=True)

    assert tdf.row_exists(100)
    assert tdf.get_row(100)['name'] == 'Dave'


def test_add_row_auto_increment_composite_pk_raises_error():
    """Test auto_increment raises error for composite PKs."""
    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    }).set_index(['user_id', 'org_id'])

    tdf = TableDataFrame(data=df, primary_key=['user_id', 'org_id'])

    with pytest.raises(DataValidationError, match="(Auto-increment failed|missing required primary key)"):
        tdf.add_row({'role': 'guest'}, auto_increment=True)


# ============================================================================
# Table get_next_pk_value() Tests
# ============================================================================

def test_table_get_next_pk_value_without_auto_increment_raises_error(tmp_path):
    """Test Table.get_next_pk_value raises error if auto_increment=False."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    table = TableDataFrame('users', df, 'id', engine, auto_increment=False)

    with pytest.raises(ValueError, match="not configured for auto-increment"):
        table.get_next_pk_value()


def test_table_get_next_pk_value_queries_database(tmp_path):
    """Test Table.get_next_pk_value queries database for max PK."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table in database with some data
    df1 = pd.DataFrame({'id': [1, 2, 3, 4, 5], 'name': ['A', 'B', 'C', 'D', 'E']})
    table = TableDataFrame('users', df1, 'id', engine, auto_increment=True)
    table.push()

    # Create new Table instance with only partial data locally
    df2 = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
    table2 = TableDataFrame('users', df2, 'id', engine, auto_increment=True)

    # Should query DB and return 6 (DB max is 5)
    next_pk = table2.get_next_pk_value()
    assert next_pk == 6


def test_table_get_next_pk_value_uses_local_if_higher(tmp_path):
    """Test Table.get_next_pk_value uses local max if higher than DB."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table in database
    df1 = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
    table = TableDataFrame('users', df1, 'id', engine, auto_increment=True)
    table.push()

    # Load table - note that auto_increment status is not persisted
    # So we need to set it when loading from DB
    db = DataBase(engine)
    
    # TableDataFrame pulled from DB won't have auto_increment=True unless we recreate with it
    # This test shows the limitation that auto_increment is a client-side setting
    # For this test, let's create a new TableDataFrame with auto_increment
    table_with_auto = TableDataFrame(name='users', data=db['users'].to_pandas(), 
                                      primary_key='id', engine=engine, auto_increment=True)
    table_with_auto.add_row({'id': 100, 'name': 'Z'})
    
    # Should return 101 (local max is 100)
    next_pk = table_with_auto.get_next_pk_value()
    assert next_pk == 101


def test_table_auto_increment_integration(tmp_path):
    """Test full auto-increment workflow with Table."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table with auto_increment
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    table = TableDataFrame('users', df, 'id', engine, auto_increment=True)
    table.push()

    # Reload from database
    db = DataBase(engine)
    db['users'].auto_increment = True  # Enable auto_increment

    # Add row without PK (only use columns that exist in table)
    db['users'].add_row({'name': 'Dave'}, auto_increment=True)

    # Should have auto-generated id = 4
    assert db['users'].row_exists(4)

    # Push and verify it persisted
    db['users'].push()
    db.pull()

    assert len(db['users']) >= 4


# ============================================================================
# Edge Cases
# ============================================================================

def test_add_row_without_auto_increment_requires_pk(sample_df_with_index):
    """Test that add_row without auto_increment requires PK."""
    tdf = TableDataFrame(data=sample_df_with_index, primary_key='id')

    # Without auto_increment, PK is required
    with pytest.raises(DataValidationError, match="missing required primary key"):
        tdf.add_row({'name': 'Dave', 'age': 40}, auto_increment=False)


def test_auto_increment_handles_non_sequential_pks():
    """Test auto_increment works with non-sequential PKs."""
    df = pd.DataFrame({
        'id': [1, 5, 10],  # Non-sequential
        'name': ['Alice', 'Bob', 'Charlie']
    }).set_index('id')

    tdf = TableDataFrame(data=df, primary_key='id')

    # Should use max + 1 = 11
    tdf.add_row({'name': 'Dave'}, auto_increment=True)

    assert tdf.row_exists(11)

