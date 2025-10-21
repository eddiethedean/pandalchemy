"""Tests for SQL helper methods in TableDataFrame."""

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame
from pandalchemy.exceptions import DataValidationError, SchemaError

# Test fixtures

@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })


@pytest.fixture
def composite_pk_df():
    """Create a DataFrame with composite primary key."""
    return pd.DataFrame({
        'user_id': ['u1', 'u1', 'u2', 'u2'],
        'org_id': ['org1', 'org2', 'org1', 'org2'],
        'role': ['admin', 'user', 'user', 'admin'],
        'active': [True, True, False, True]
    })


# ============================================================================
# Primary Key Helper Tests
# ============================================================================

def test_get_pk_columns_single(sample_df):
    """Test _get_pk_columns with single column PK."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    assert tdf._get_pk_columns() == ['id']


def test_get_pk_columns_composite(composite_pk_df):
    """Test _get_pk_columns with composite PK."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    assert tdf._get_pk_columns() == ['user_id', 'org_id']


def test_get_pk_condition_single(sample_df):
    """Test _get_pk_condition with single PK."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    condition = tdf._get_pk_condition(2)
    assert condition.sum() == 1
    assert tdf.to_pandas()[condition]['name'].iloc[0] == 'Bob'


def test_get_pk_condition_composite(composite_pk_df):
    """Test _get_pk_condition with composite PK."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    condition = tdf._get_pk_condition(('u1', 'org1'))
    assert condition.sum() == 1
    assert tdf.to_pandas()[condition]['role'].iloc[0] == 'admin'


def test_get_pk_condition_composite_wrong_format(composite_pk_df):
    """Test _get_pk_condition raises error for wrong format."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    with pytest.raises(ValueError, match="Composite key requires tuple/list"):
        tdf._get_pk_condition('u1')


def test_get_pk_condition_composite_wrong_length(composite_pk_df):
    """Test _get_pk_condition raises error for wrong length."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    with pytest.raises(ValueError, match="Expected 2 values"):
        tdf._get_pk_condition(('u1',))


# ============================================================================
# CRUD Operations - Single PK
# ============================================================================

def test_add_row_single_pk(sample_df):
    """Test add_row with single PK."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.add_row({'id': 4, 'name': 'Dave', 'age': 40})

    assert len(tdf) == 4
    assert tdf.row_exists(4)
    assert tdf.get_row(4)['name'] == 'Dave'


def test_add_row_duplicate_pk_raises_error(sample_df):
    """Test add_row with duplicate PK raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(DataValidationError, match="Primary key value 1 already exists"):
        tdf.add_row({'id': 1, 'name': 'Duplicate', 'age': 99})


def test_add_row_missing_pk_raises_error(sample_df):
    """Test add_row without PK raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(DataValidationError, match="missing required primary key"):
        tdf.add_row({'name': 'Missing', 'age': 99})


def test_update_row_single_pk(sample_df):
    """Test update_row with single PK."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.update_row(2, {'age': 31, 'name': 'Robert'})

    row = tdf.get_row(2)
    assert row['age'] == 31
    assert row['name'] == 'Robert'


def test_update_row_nonexistent_raises_error(sample_df):
    """Test update_row with nonexistent PK raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(ValueError, match="No row found"):
        tdf.update_row(999, {'age': 100})


def test_delete_row_single_pk(sample_df):
    """Test delete_row with single PK."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    assert len(tdf) == 3

    tdf.delete_row(2)

    assert len(tdf) == 2
    assert not tdf.row_exists(2)
    assert tdf.row_exists(1)
    assert tdf.row_exists(3)


def test_delete_row_nonexistent_raises_error(sample_df):
    """Test delete_row with nonexistent PK raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(ValueError, match="No row found"):
        tdf.delete_row(999)


def test_upsert_row_insert(sample_df):
    """Test upsert_row performs insert when row doesn't exist."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.upsert_row({'id': 4, 'name': 'Dave', 'age': 40})

    assert len(tdf) == 4
    assert tdf.get_row(4)['name'] == 'Dave'


def test_upsert_row_update(sample_df):
    """Test upsert_row performs update when row exists."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.upsert_row({'id': 2, 'name': 'Robert', 'age': 31})

    assert len(tdf) == 3  # Same count
    row = tdf.get_row(2)
    assert row['name'] == 'Robert'
    assert row['age'] == 31


def test_bulk_insert(sample_df):
    """Test bulk_insert with multiple rows."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    new_rows = [
        {'id': 4, 'name': 'Dave', 'age': 40},
        {'id': 5, 'name': 'Eve', 'age': 45},
        {'id': 6, 'name': 'Frank', 'age': 50}
    ]
    tdf.bulk_insert(new_rows)

    assert len(tdf) == 6
    assert tdf.row_exists(4)
    assert tdf.row_exists(5)
    assert tdf.row_exists(6)


def test_bulk_insert_duplicate_in_batch_raises_error(sample_df):
    """Test bulk_insert with duplicate PKs in batch raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    new_rows = [
        {'id': 4, 'name': 'Dave', 'age': 40},
        {'id': 4, 'name': 'Duplicate', 'age': 45}
    ]
    with pytest.raises(DataValidationError, match="duplicate primary key"):
        tdf.bulk_insert(new_rows)


def test_bulk_insert_conflict_with_existing_raises_error(sample_df):
    """Test bulk_insert with existing PK raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    new_rows = [
        {'id': 1, 'name': 'Conflict', 'age': 99},  # Conflicts with existing
        {'id': 4, 'name': 'Dave', 'age': 40}
    ]
    with pytest.raises(DataValidationError, match="primary key.*already exist"):
        tdf.bulk_insert(new_rows)


# ============================================================================
# CRUD Operations - Composite PK
# ============================================================================

def test_add_row_composite_pk(composite_pk_df):
    """Test add_row with composite PK."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    tdf.add_row({'user_id': 'u3', 'org_id': 'org1', 'role': 'user', 'active': True})

    assert len(tdf) == 5
    assert tdf.row_exists(('u3', 'org1'))


def test_add_row_composite_pk_duplicate_raises_error(composite_pk_df):
    """Test add_row with duplicate composite PK raises error."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    with pytest.raises(DataValidationError, match="Primary key combination.*already exists"):
        tdf.add_row({'user_id': 'u1', 'org_id': 'org1', 'role': 'duplicate', 'active': False})


def test_update_row_composite_pk(composite_pk_df):
    """Test update_row with composite PK."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    tdf.update_row(('u1', 'org1'), {'role': 'superadmin', 'active': False})

    row = tdf.get_row(('u1', 'org1'))
    assert row['role'] == 'superadmin'
    assert not row['active']


def test_delete_row_composite_pk(composite_pk_df):
    """Test delete_row with composite PK."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    assert len(tdf) == 4

    tdf.delete_row(('u1', 'org1'))

    assert len(tdf) == 3
    assert not tdf.row_exists(('u1', 'org1'))
    assert tdf.row_exists(('u1', 'org2'))


def test_upsert_row_composite_pk(composite_pk_df):
    """Test upsert_row with composite PK."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])

    # Update existing
    tdf.upsert_row({'user_id': 'u1', 'org_id': 'org1', 'role': 'owner', 'active': True})
    assert tdf.get_row(('u1', 'org1'))['role'] == 'owner'

    # Insert new
    tdf.upsert_row({'user_id': 'u3', 'org_id': 'org1', 'role': 'guest', 'active': False})
    assert tdf.row_exists(('u3', 'org1'))


# ============================================================================
# Query Operations
# ============================================================================

def test_get_row_found(sample_df):
    """Test get_row returns row when found."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    row = tdf.get_row(2)

    assert row is not None
    # get_row returns a dict
    assert row['name'] == 'Bob'
    assert row['age'] == 30


def test_get_row_not_found(sample_df):
    """Test get_row returns None when not found."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    row = tdf.get_row(999)

    assert row is None


def test_row_exists_true(sample_df):
    """Test row_exists returns True when row exists."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    assert tdf.row_exists(1)
    assert tdf.row_exists(2)
    assert tdf.row_exists(3)


def test_row_exists_false(sample_df):
    """Test row_exists returns False when row doesn't exist."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    assert not tdf.row_exists(0)
    assert not tdf.row_exists(999)


def test_has_changes_false_initially(sample_df):
    """Test has_changes returns False initially."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    assert not tdf.has_changes()


def test_has_changes_true_after_modification(sample_df):
    """Test has_changes returns True after modification."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.add_row({'id': 4, 'name': 'Dave', 'age': 40})
    assert tdf.has_changes()


def test_get_changes_summary(sample_df):
    """Test get_changes_summary returns summary dict."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.add_row({'id': 4, 'name': 'Dave', 'age': 40})

    summary = tdf.get_changes_summary()
    assert isinstance(summary, dict)
    assert 'has_changes' in summary
    assert summary['has_changes']


# ============================================================================
# Primary Key Operations
# ============================================================================

def test_get_primary_key_single(sample_df):
    """Test get_primary_key returns single column name."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    assert tdf.get_primary_key() == 'id'


def test_get_primary_key_composite(composite_pk_df):
    """Test get_primary_key returns list for composite."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])
    assert tdf.get_primary_key() == ['user_id', 'org_id']


def test_set_primary_key_single_to_single(sample_df):
    """Test changing primary key from one column to another."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.set_primary_key('name')

    assert tdf.get_primary_key() == 'name'


def test_set_primary_key_single_to_composite(sample_df):
    """Test changing from single to composite PK."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.set_primary_key(['name', 'age'])

    assert tdf.get_primary_key() == ['name', 'age']


def test_set_primary_key_nonexistent_column_raises_error(sample_df):
    """Test set_primary_key with nonexistent column raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(SchemaError, match="do not exist"):
        tdf.set_primary_key('nonexistent')


def test_set_primary_key_duplicate_values_raises_error():
    """Test set_primary_key with non-unique values raises error."""
    df = pd.DataFrame({'id': [1, 2, 3], 'category': ['A', 'A', 'B']})
    tdf = TableDataFrame(data=df, primary_key='id')

    with pytest.raises(DataValidationError, match="duplicate values"):
        tdf.set_primary_key('category')


def test_set_primary_key_with_nulls_raises_error():
    """Test set_primary_key with null values raises error."""
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', None, 'Charlie']})
    tdf = TableDataFrame(data=df, primary_key='id')

    with pytest.raises(DataValidationError, match="null values"):
        tdf.set_primary_key('name')


# ============================================================================
# Schema Operations
# ============================================================================

def test_add_column_with_default(sample_df):
    """Test add_column_with_default adds column with value."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.add_column_with_default('email', np.nan)

    assert 'email' in tdf.columns
    assert all(tdf.to_pandas()['email'].isna())


def test_add_column_with_default_existing_raises_error(sample_df):
    """Test add_column_with_default with existing column raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(SchemaError, match="already exists"):
        tdf.add_column_with_default('name', 'default')


def test_drop_column_safe(sample_df):
    """Test drop_column_safe removes column."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.drop_column_safe('age')

    assert 'age' not in tdf.columns
    # 'id' is now in the index (PK), not columns
    assert 'name' in tdf.columns


def test_drop_column_safe_nonexistent_raises_error(sample_df):
    """Test drop_column_safe with nonexistent column raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(SchemaError, match="does not exist"):
        tdf.drop_column_safe('nonexistent')


def test_rename_column_safe(sample_df):
    """Test rename_column_safe renames column."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.rename_column_safe('age', 'user_age')

    assert 'user_age' in tdf.columns
    assert 'age' not in tdf.columns


def test_rename_column_safe_updates_pk_if_renamed(sample_df):
    """Test rename_column_safe for non-PK columns (PK is now in index)."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    # Rename a non-PK column
    tdf.rename_column_safe('name', 'full_name')

    assert 'full_name' in tdf.columns
    assert 'name' not in tdf.columns
    # PK should still be 'id' in index
    assert tdf.get_primary_key() == 'id'


def test_rename_column_safe_nonexistent_raises_error(sample_df):
    """Test rename_column_safe with nonexistent column raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(SchemaError, match="does not exist"):
        tdf.rename_column_safe('nonexistent', 'new_name')


def test_rename_column_safe_existing_target_raises_error(sample_df):
    """Test rename_column_safe to existing name raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(SchemaError, match="already exists"):
        tdf.rename_column_safe('age', 'name')


def test_change_column_type(sample_df):
    """Test change_column_type changes dtype."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    tdf.change_column_type('age', float)

    assert tdf.to_pandas()['age'].dtype == float


def test_change_column_type_nonexistent_raises_error(sample_df):
    """Test change_column_type with nonexistent column raises error."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    with pytest.raises(SchemaError, match="does not exist"):
        tdf.change_column_type('nonexistent', int)


# ============================================================================
# Validation Methods
# ============================================================================

def test_validate_primary_key_valid(sample_df):
    """Test validate_primary_key passes for valid data."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    assert tdf.validate_primary_key()


def test_validate_primary_key_missing_column_raises_error(sample_df):
    """Test validate_primary_key raises error when PK column dropped."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    
    # Manually corrupt: reset index and drop PK
    tdf._data = tdf._data.reset_index().drop(columns=['id'])

    with pytest.raises(SchemaError, match="have been dropped|Primary key"):
        tdf.validate_primary_key()


def test_validate_primary_key_null_values_raises_error():
    """Test validate_primary_key raises error for null PKs."""
    df = pd.DataFrame({'id': [1, None, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TableDataFrame(data=df, primary_key='id')

    with pytest.raises(DataValidationError, match="null values"):
        tdf.validate_primary_key()


def test_validate_primary_key_duplicate_values_raises_error():
    """Test validate_primary_key raises error for duplicate PKs."""
    df = pd.DataFrame({'id': [1, 1, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TableDataFrame(data=df, primary_key='id')

    with pytest.raises(DataValidationError, match="duplicate values"):
        tdf.validate_primary_key()


def test_validate_data_returns_empty_for_valid(sample_df):
    """Test validate_data returns empty list for valid data."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    errors = tdf.validate_data()

    assert errors == []


def test_validate_data_detects_missing_pk(sample_df):
    """Test validate_data detects missing PK column."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')
    
    # Manually corrupt by resetting index and dropping PK
    tdf._data = tdf._data.reset_index().drop(columns=['id'])

    errors = tdf.validate_data()
    assert len(errors) > 0
    assert any('dropped' in error or 'Primary key' in error for error in errors)


def test_validate_data_detects_null_pk():
    """Test validate_data detects null PK values."""
    df = pd.DataFrame({'id': [1, None, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TableDataFrame(data=df, primary_key='id')

    errors = tdf.validate_data()
    assert len(errors) > 0
    assert any('null' in error for error in errors)


def test_validate_data_detects_duplicate_pk():
    """Test validate_data detects duplicate PK values."""
    df = pd.DataFrame({'id': [1, 1, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TableDataFrame(data=df, primary_key='id')

    errors = tdf.validate_data()
    assert len(errors) > 0
    assert any('duplicate' in error for error in errors)


# ============================================================================
# Push Validation Integration Tests
# ============================================================================

def test_push_fails_when_single_pk_dropped(tmp_path):
    """Test that index-based PK handles missing PK column correctly."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table and manually corrupt by removing PK
    df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
    tdf = TableDataFrame(data=df, primary_key='id')
    
    # Manually corrupt: reset index and drop PK column
    corrupted_df = tdf.to_pandas().reset_index().drop(columns=['id'])
    
    # With index-based PK support, this now works - index is named 'id'
    table = TableDataFrame(name='users', data=corrupted_df, primary_key='id', engine=engine)

    # Push should succeed - index becomes the primary key
    table.push()
    assert table._data.index.name == 'id'


def test_push_fails_when_composite_pk_partially_dropped(tmp_path):
    """Test TableDataFrame creation raises error when part of composite PK is missing."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    })

    # Create TableDataFrame and manually corrupt by removing part of composite PK
    tdf = TableDataFrame(data=df, primary_key=['user_id', 'org_id'])
    
    # Manually corrupt: reset index and drop org_id
    corrupted_df = tdf.to_pandas().reset_index().drop(columns=['org_id'])
    
    # TableDataFrame creation should fail immediately with better validation
    with pytest.raises(ValueError, match="Composite primary key.*not found"):
        table = TableDataFrame(name='memberships', data=corrupted_df, primary_key=['user_id', 'org_id'], engine=engine)


def test_push_succeeds_when_non_pk_columns_dropped(tmp_path):
    """Test push succeeds when only non-PK columns are dropped."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create TableDataFrame and drop non-PK column
    df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob'], 'age': [25, 30]})
    tdf = TableDataFrame(data=df, primary_key='id')
    tdf.drop_column_safe('age')

    table = TableDataFrame(name='users', data=tdf.to_pandas(), primary_key='id', engine=engine)

    # Push should succeed (creates new table without 'age')
    table.push()  # Should not raise

    # Verify
    table.pull()
    assert 'age' not in table.columns
    # 'id' should be in index
    assert table.to_pandas().index.name == 'id' or 'id' in table.columns


def test_database_push_validates_all_tables(tmp_path):
    """Test DataBase.push() works with index-based PKs."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create database with tables
    db = DataBase(engine)

    # Add table 1
    df1 = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
    table1 = TableDataFrame(name='users', data=df1, primary_key='id', engine=engine)
    db.add_table(table1)

    # Add table 2 without PK column (index-based PK)
    df2 = pd.DataFrame({'id': [1, 2], 'title': ['Product A', 'Product B']})
    tdf2 = TableDataFrame(data=df2, primary_key='id')
    
    # Reset index and drop PK column - index becomes the PK
    corrupted_df2 = tdf2.to_pandas().reset_index().drop(columns=['id'])
    
    table2 = TableDataFrame(name='products', data=corrupted_df2, primary_key='id', engine=engine)
    db.add_table(table2)

    # Database push should succeed - index-based PK works
    db.push()
    assert table2._data.index.name == 'id'


# Tests for update_where convenience method

def test_update_where_single_column_with_lambda(sample_df):
    """Test update_where with single column and lambda function."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Increment age for people over 28
    tdf.update_where(tdf['age'] > 28, {'age': lambda x: x + 1})

    assert tdf._data.loc[1, 'age'] == 25  # Unchanged
    assert tdf._data.loc[2, 'age'] == 31  # 30 + 1
    assert tdf._data.loc[3, 'age'] == 36  # 35 + 1

    # Verify changes are tracked
    assert tdf.has_changes()
    assert len(tdf.get_tracker().row_changes) == 2


def test_update_where_multiple_columns(sample_df):
    """Test update_where with multiple columns."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Update multiple columns at once
    tdf.update_where(tdf['age'] < 30, {
        'age': lambda x: x + 10,
        'name': lambda x: x + ' Jr.'
    })

    assert tdf._data.loc[1, 'age'] == 35  # 25 + 10
    assert tdf._data.loc[1, 'name'] == 'Alice Jr.'
    assert tdf._data.loc[2, 'age'] == 30  # Unchanged
    assert tdf._data.loc[2, 'name'] == 'Bob'  # Unchanged

    assert tdf.has_changes()
    assert len(tdf.get_tracker().row_changes) == 1


def test_update_where_simple_value_assignment(sample_df):
    """Test update_where with simple value assignment."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Set all ages over 30 to exactly 30
    tdf.update_where(tdf['age'] > 30, {'age': 30})

    assert tdf._data.loc[1, 'age'] == 25
    assert tdf._data.loc[2, 'age'] == 30
    assert tdf._data.loc[3, 'age'] == 30  # Changed from 35

    assert tdf.has_changes()


def test_update_where_shorthand_syntax(sample_df):
    """Test update_where with shorthand column syntax."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Using shorthand: value and column parameter
    tdf.update_where(tdf['age'] > 30, 99, column='age')

    assert tdf._data.loc[3, 'age'] == 99
    assert tdf.has_changes()


def test_update_where_empty_condition(sample_df):
    """Test update_where when no rows match condition."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # No rows match
    tdf.update_where(tdf['age'] > 100, {'age': 50})

    # Nothing changed
    assert not tdf.has_changes()
    assert tdf._data.loc[1, 'age'] == 25


def test_update_where_all_rows_match(sample_df):
    """Test update_where when all rows match condition."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # All rows match
    tdf.update_where(tdf['age'] > 0, {'age': lambda x: x * 2})

    assert tdf._data.loc[1, 'age'] == 50  # 25 * 2
    assert tdf._data.loc[2, 'age'] == 60  # 30 * 2
    assert tdf._data.loc[3, 'age'] == 70  # 35 * 2

    assert len(tdf.get_tracker().row_changes) == 3


def test_update_where_pk_column_raises_error(sample_df):
    """Test that updating PK column raises DataValidationError."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Try to update PK - should fail
    with pytest.raises(DataValidationError, match="Cannot update primary key"):
        tdf.update_where(tdf['age'] > 30, {'id': 999})


def test_update_where_nonexistent_column_raises_error(sample_df):
    """Test that updating non-existent column raises ValueError."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    with pytest.raises(ValueError, match="does not exist"):
        tdf.update_where(tdf['age'] > 30, {'nonexistent': 'value'})


def test_update_where_invalid_shorthand_raises_error(sample_df):
    """Test that invalid shorthand syntax raises ValueError."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Can't use dict with column parameter
    with pytest.raises(ValueError, match="should be a single value"):
        tdf.update_where(tdf['age'] > 30, {'age': 50}, column='age')


def test_update_where_composite_pk(composite_pk_df):
    """Test update_where with composite primary key."""
    df = composite_pk_df.set_index(['user_id', 'org_id'])
    tdf = TableDataFrame(data=df, primary_key=['user_id', 'org_id'])

    # Update role for inactive users
    tdf.update_where(tdf['active'] == False, {'role': 'suspended'})

    assert tdf._data.loc[('u2', 'org1'), 'role'] == 'suspended'
    assert tdf.has_changes()


def test_update_where_with_integration(tmp_path):
    """Test update_where with full database integration."""
    db_path = tmp_path / "update_where.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create table
    employees = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'department': ['Sales', 'Engineering', 'Sales', 'HR', 'Engineering'],
        'salary': [50000, 80000, 55000, 60000, 90000],
        'bonus': [0, 0, 0, 0, 0]
    })
    db.create_table('employees', employees, primary_key='id')

    # Give 10% raise to Engineering department
    db['employees'].update_where(
        db['employees']['department'] == 'Engineering',
        {'salary': lambda x: x * 1.1}
    )

    # Give $5000 bonus to high earners
    db['employees'].update_where(
        db['employees']['salary'] >= 80000,
        {'bonus': 5000}
    )

    db.push()
    db.pull()

    # Verify Engineering got raises
    assert db['employees'].get_row(2)['salary'] == 88000  # 80000 * 1.1
    assert db['employees'].get_row(5)['salary'] == 99000  # 90000 * 1.1

    # Verify high earners got bonus
    assert db['employees'].get_row(2)['bonus'] == 5000
    assert db['employees'].get_row(5)['bonus'] == 5000
    assert db['employees'].get_row(1)['bonus'] == 0  # No bonus


def test_update_where_mixed_callables_and_values(sample_df):
    """Test update_where with mix of callable and direct values."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Mix of lambda and direct value
    tdf.update_where(tdf['age'] >= 30, {
        'age': lambda x: x + 5,
        'name': 'Senior'
    })

    assert tdf._data.loc[2, 'age'] == 35  # 30 + 5
    assert tdf._data.loc[2, 'name'] == 'Senior'
    assert tdf._data.loc[3, 'age'] == 40  # 35 + 5
    assert tdf._data.loc[3, 'name'] == 'Senior'

    assert len(tdf.get_tracker().row_changes) == 2


# Tests for delete_where convenience method

def test_delete_where_single_row(sample_df):
    """Test delete_where with single row deletion."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Delete one person
    deleted = tdf.delete_where(tdf['age'] == 35)

    assert deleted == 1
    assert len(tdf) == 2
    assert 3 not in tdf._data.index
    assert tdf.has_changes()
    assert len(tdf.get_tracker().get_deletes()) == 1


def test_delete_where_multiple_rows(sample_df):
    """Test delete_where with multiple row deletion."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Delete people 30 or older
    deleted = tdf.delete_where(tdf['age'] >= 30)

    assert deleted == 2  # Bob and Charlie
    assert len(tdf) == 1
    assert list(tdf._data.index) == [1]
    assert len(tdf.get_tracker().get_deletes()) == 2


def test_delete_where_empty_condition(sample_df):
    """Test delete_where when no rows match condition."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # No rows match
    deleted = tdf.delete_where(tdf['age'] > 100)

    assert deleted == 0
    assert len(tdf) == 3
    assert not tdf.has_changes()


def test_delete_where_all_rows(sample_df):
    """Test delete_where when all rows match condition."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # All rows match
    deleted = tdf.delete_where(tdf['age'] > 0)

    assert deleted == 3
    assert len(tdf) == 0
    assert tdf.has_changes()
    assert len(tdf.get_tracker().get_deletes()) == 3


def test_delete_where_complex_condition(sample_df):
    """Test delete_where with complex AND/OR conditions."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Delete people who are either 30 or older OR named Alice
    deleted = tdf.delete_where((tdf['age'] >= 30) | (tdf['name'] == 'Alice'))

    assert deleted == 3  # All of them (Alice, Bob >= 30, Charlie >= 30)
    assert len(tdf) == 0
    assert 1 not in tdf._data.index
    assert 2 not in tdf._data.index
    assert 3 not in tdf._data.index


def test_delete_where_composite_pk(composite_pk_df):
    """Test delete_where with composite primary key."""
    df = composite_pk_df.set_index(['user_id', 'org_id'])
    tdf = TableDataFrame(data=df, primary_key=['user_id', 'org_id'])

    # Delete inactive users
    deleted = tdf.delete_where(tdf['active'] == False)

    assert deleted == 1
    assert len(tdf) == 3
    assert ('u2', 'org1') not in tdf._data.index
    assert tdf.has_changes()


def test_delete_where_with_integration(tmp_path):
    """Test delete_where with full database integration."""
    db_path = tmp_path / "delete_where.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create table
    logs = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 6],
        'level': ['INFO', 'ERROR', 'INFO', 'WARNING', 'ERROR', 'DEBUG'],
        'timestamp': pd.date_range('2025-01-01', periods=6),
        'message': ['msg1', 'msg2', 'msg3', 'msg4', 'msg5', 'msg6']
    })
    db.create_table('logs', logs, primary_key='id')

    # Delete ERROR and WARNING logs
    deleted = db['logs'].delete_where(
        (db['logs']['level'] == 'ERROR') | (db['logs']['level'] == 'WARNING')
    )

    assert deleted == 3  # 2 ERROR + 1 WARNING

    db.push()
    db.pull()

    # Verify deletions persisted
    assert len(db['logs']) == 3
    assert 2 not in db['logs'].index  # ERROR deleted
    assert 4 not in db['logs'].index  # WARNING deleted
    assert 5 not in db['logs'].index  # ERROR deleted
    assert 1 in db['logs'].index  # INFO kept
    assert 3 in db['logs'].index  # INFO kept
    assert 6 in db['logs'].index  # DEBUG kept


def test_delete_where_returns_count(sample_df):
    """Test that delete_where returns correct count."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Delete and check return value
    count = tdf.delete_where(tdf['age'] >= 30)

    assert count == 2
    assert count == len(tdf.get_tracker().get_deletes())


def test_delete_where_chained_operations(sample_df):
    """Test delete_where followed by other operations."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Delete then update (delete Charlie who is 35, then update Alice)
    tdf.delete_where(tdf['age'] > 30)  # Deletes Charlie (35)
    tdf.update_where(tdf['age'] < 30, {'age': 30})  # Updates Alice (25)

    assert len(tdf) == 2  # Alice and Bob remain
    assert tdf._data.loc[1, 'age'] == 30
    assert tdf.has_changes()
    assert len(tdf.get_tracker().get_deletes()) == 1
    assert len(tdf.get_tracker().get_updates()) == 1


def test_delete_where_with_string_condition(sample_df):
    """Test delete_where with string matching."""
    df = sample_df.set_index('id')
    tdf = TableDataFrame(data=df, primary_key='id')

    # Delete names starting with 'C'
    deleted = tdf.delete_where(tdf['name'].str.startswith('C'))

    assert deleted == 1  # Charlie
    assert 3 not in tdf._data.index
    assert len(tdf) == 2

