"""Tests for SQL helper methods in TrackedDataFrame."""

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, Table, TrackedDataFrame
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
    tdf = TrackedDataFrame(sample_df, 'id')
    assert tdf._get_pk_columns() == ['id']


def test_get_pk_columns_composite(composite_pk_df):
    """Test _get_pk_columns with composite PK."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    assert tdf._get_pk_columns() == ['user_id', 'org_id']


def test_get_pk_condition_single(sample_df):
    """Test _get_pk_condition with single PK."""
    tdf = TrackedDataFrame(sample_df, 'id')
    condition = tdf._get_pk_condition(2)
    assert condition.sum() == 1
    assert tdf.to_pandas()[condition]['name'].iloc[0] == 'Bob'


def test_get_pk_condition_composite(composite_pk_df):
    """Test _get_pk_condition with composite PK."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    condition = tdf._get_pk_condition(('u1', 'org1'))
    assert condition.sum() == 1
    assert tdf.to_pandas()[condition]['role'].iloc[0] == 'admin'


def test_get_pk_condition_composite_wrong_format(composite_pk_df):
    """Test _get_pk_condition raises error for wrong format."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    with pytest.raises(ValueError, match="Composite key requires tuple/list"):
        tdf._get_pk_condition('u1')


def test_get_pk_condition_composite_wrong_length(composite_pk_df):
    """Test _get_pk_condition raises error for wrong length."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    with pytest.raises(ValueError, match="Expected 2 values"):
        tdf._get_pk_condition(('u1',))


# ============================================================================
# CRUD Operations - Single PK
# ============================================================================

def test_add_row_single_pk(sample_df):
    """Test add_row with single PK."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.add_row({'id': 4, 'name': 'Dave', 'age': 40})

    assert len(tdf) == 4
    assert tdf.row_exists(4)
    assert tdf.get_row(4)['name'] == 'Dave'


def test_add_row_duplicate_pk_raises_error(sample_df):
    """Test add_row with duplicate PK raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(DataValidationError, match="Primary key value 1 already exists"):
        tdf.add_row({'id': 1, 'name': 'Duplicate', 'age': 99})


def test_add_row_missing_pk_raises_error(sample_df):
    """Test add_row without PK raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(DataValidationError, match="missing required primary key"):
        tdf.add_row({'name': 'Missing', 'age': 99})


def test_update_row_single_pk(sample_df):
    """Test update_row with single PK."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.update_row(2, {'age': 31, 'name': 'Robert'})

    row = tdf.get_row(2)
    assert row['age'] == 31
    assert row['name'] == 'Robert'


def test_update_row_nonexistent_raises_error(sample_df):
    """Test update_row with nonexistent PK raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(ValueError, match="No row found"):
        tdf.update_row(999, {'age': 100})


def test_delete_row_single_pk(sample_df):
    """Test delete_row with single PK."""
    tdf = TrackedDataFrame(sample_df, 'id')
    assert len(tdf) == 3

    tdf.delete_row(2)

    assert len(tdf) == 2
    assert not tdf.row_exists(2)
    assert tdf.row_exists(1)
    assert tdf.row_exists(3)


def test_delete_row_nonexistent_raises_error(sample_df):
    """Test delete_row with nonexistent PK raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(ValueError, match="No row found"):
        tdf.delete_row(999)


def test_upsert_row_insert(sample_df):
    """Test upsert_row performs insert when row doesn't exist."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.upsert_row({'id': 4, 'name': 'Dave', 'age': 40})

    assert len(tdf) == 4
    assert tdf.get_row(4)['name'] == 'Dave'


def test_upsert_row_update(sample_df):
    """Test upsert_row performs update when row exists."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.upsert_row({'id': 2, 'name': 'Robert', 'age': 31})

    assert len(tdf) == 3  # Same count
    row = tdf.get_row(2)
    assert row['name'] == 'Robert'
    assert row['age'] == 31


def test_bulk_insert(sample_df):
    """Test bulk_insert with multiple rows."""
    tdf = TrackedDataFrame(sample_df, 'id')
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
    tdf = TrackedDataFrame(sample_df, 'id')
    new_rows = [
        {'id': 4, 'name': 'Dave', 'age': 40},
        {'id': 4, 'name': 'Duplicate', 'age': 45}
    ]
    with pytest.raises(DataValidationError, match="duplicate primary key"):
        tdf.bulk_insert(new_rows)


def test_bulk_insert_conflict_with_existing_raises_error(sample_df):
    """Test bulk_insert with existing PK raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
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
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    tdf.add_row({'user_id': 'u3', 'org_id': 'org1', 'role': 'user', 'active': True})

    assert len(tdf) == 5
    assert tdf.row_exists(('u3', 'org1'))


def test_add_row_composite_pk_duplicate_raises_error(composite_pk_df):
    """Test add_row with duplicate composite PK raises error."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    with pytest.raises(DataValidationError, match="Primary key combination.*already exists"):
        tdf.add_row({'user_id': 'u1', 'org_id': 'org1', 'role': 'duplicate', 'active': False})


def test_update_row_composite_pk(composite_pk_df):
    """Test update_row with composite PK."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    tdf.update_row(('u1', 'org1'), {'role': 'superadmin', 'active': False})

    row = tdf.get_row(('u1', 'org1'))
    assert row['role'] == 'superadmin'
    assert not row['active']


def test_delete_row_composite_pk(composite_pk_df):
    """Test delete_row with composite PK."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    assert len(tdf) == 4

    tdf.delete_row(('u1', 'org1'))

    assert len(tdf) == 3
    assert not tdf.row_exists(('u1', 'org1'))
    assert tdf.row_exists(('u1', 'org2'))


def test_upsert_row_composite_pk(composite_pk_df):
    """Test upsert_row with composite PK."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])

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
    tdf = TrackedDataFrame(sample_df, 'id')
    row = tdf.get_row(2)

    assert row is not None
    assert row['id'] == 2
    assert row['name'] == 'Bob'
    assert row['age'] == 30


def test_get_row_not_found(sample_df):
    """Test get_row returns None when not found."""
    tdf = TrackedDataFrame(sample_df, 'id')
    row = tdf.get_row(999)

    assert row is None


def test_row_exists_true(sample_df):
    """Test row_exists returns True when row exists."""
    tdf = TrackedDataFrame(sample_df, 'id')
    assert tdf.row_exists(1)
    assert tdf.row_exists(2)
    assert tdf.row_exists(3)


def test_row_exists_false(sample_df):
    """Test row_exists returns False when row doesn't exist."""
    tdf = TrackedDataFrame(sample_df, 'id')
    assert not tdf.row_exists(0)
    assert not tdf.row_exists(999)


def test_has_changes_false_initially(sample_df):
    """Test has_changes returns False initially."""
    tdf = TrackedDataFrame(sample_df, 'id')
    assert not tdf.has_changes()


def test_has_changes_true_after_modification(sample_df):
    """Test has_changes returns True after modification."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.add_row({'id': 4, 'name': 'Dave', 'age': 40})
    assert tdf.has_changes()


def test_get_changes_summary(sample_df):
    """Test get_changes_summary returns summary dict."""
    tdf = TrackedDataFrame(sample_df, 'id')
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
    tdf = TrackedDataFrame(sample_df, 'id')
    assert tdf.get_primary_key() == 'id'


def test_get_primary_key_composite(composite_pk_df):
    """Test get_primary_key returns list for composite."""
    tdf = TrackedDataFrame(composite_pk_df, ['user_id', 'org_id'])
    assert tdf.get_primary_key() == ['user_id', 'org_id']


def test_set_primary_key_single_to_single(sample_df):
    """Test changing primary key from one column to another."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.set_primary_key('name')

    assert tdf.get_primary_key() == 'name'


def test_set_primary_key_single_to_composite(sample_df):
    """Test changing from single to composite PK."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.set_primary_key(['name', 'age'])

    assert tdf.get_primary_key() == ['name', 'age']


def test_set_primary_key_nonexistent_column_raises_error(sample_df):
    """Test set_primary_key with nonexistent column raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(SchemaError, match="do not exist"):
        tdf.set_primary_key('nonexistent')


def test_set_primary_key_duplicate_values_raises_error():
    """Test set_primary_key with non-unique values raises error."""
    df = pd.DataFrame({'id': [1, 2, 3], 'category': ['A', 'A', 'B']})
    tdf = TrackedDataFrame(df, 'id')

    with pytest.raises(DataValidationError, match="duplicate values"):
        tdf.set_primary_key('category')


def test_set_primary_key_with_nulls_raises_error():
    """Test set_primary_key with null values raises error."""
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', None, 'Charlie']})
    tdf = TrackedDataFrame(df, 'id')

    with pytest.raises(DataValidationError, match="null values"):
        tdf.set_primary_key('name')


# ============================================================================
# Schema Operations
# ============================================================================

def test_add_column_with_default(sample_df):
    """Test add_column_with_default adds column with value."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.add_column_with_default('email', np.nan)

    assert 'email' in tdf.columns
    assert all(tdf.to_pandas()['email'].isna())


def test_add_column_with_default_existing_raises_error(sample_df):
    """Test add_column_with_default with existing column raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(SchemaError, match="already exists"):
        tdf.add_column_with_default('name', 'default')


def test_drop_column_safe(sample_df):
    """Test drop_column_safe removes column."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.drop_column_safe('age')

    assert 'age' not in tdf.columns
    assert 'id' in tdf.columns
    assert 'name' in tdf.columns


def test_drop_column_safe_nonexistent_raises_error(sample_df):
    """Test drop_column_safe with nonexistent column raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(SchemaError, match="does not exist"):
        tdf.drop_column_safe('nonexistent')


def test_rename_column_safe(sample_df):
    """Test rename_column_safe renames column."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.rename_column_safe('age', 'user_age')

    assert 'user_age' in tdf.columns
    assert 'age' not in tdf.columns


def test_rename_column_safe_updates_pk_if_renamed(sample_df):
    """Test rename_column_safe updates PK if PK column is renamed."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.rename_column_safe('id', 'user_id')

    assert tdf.get_primary_key() == 'user_id'
    assert 'user_id' in tdf.columns


def test_rename_column_safe_nonexistent_raises_error(sample_df):
    """Test rename_column_safe with nonexistent column raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(SchemaError, match="does not exist"):
        tdf.rename_column_safe('nonexistent', 'new_name')


def test_rename_column_safe_existing_target_raises_error(sample_df):
    """Test rename_column_safe to existing name raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(SchemaError, match="already exists"):
        tdf.rename_column_safe('age', 'name')


def test_change_column_type(sample_df):
    """Test change_column_type changes dtype."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.change_column_type('age', float)

    assert tdf.to_pandas()['age'].dtype == float


def test_change_column_type_nonexistent_raises_error(sample_df):
    """Test change_column_type with nonexistent column raises error."""
    tdf = TrackedDataFrame(sample_df, 'id')
    with pytest.raises(SchemaError, match="does not exist"):
        tdf.change_column_type('nonexistent', int)


# ============================================================================
# Validation Methods
# ============================================================================

def test_validate_primary_key_valid(sample_df):
    """Test validate_primary_key passes for valid data."""
    tdf = TrackedDataFrame(sample_df, 'id')
    assert tdf.validate_primary_key()


def test_validate_primary_key_missing_column_raises_error(sample_df):
    """Test validate_primary_key raises error when PK column dropped."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.drop_column_safe('id')

    with pytest.raises(SchemaError, match="have been dropped"):
        tdf.validate_primary_key()


def test_validate_primary_key_null_values_raises_error():
    """Test validate_primary_key raises error for null PKs."""
    df = pd.DataFrame({'id': [1, None, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TrackedDataFrame(df, 'id')

    with pytest.raises(DataValidationError, match="null values"):
        tdf.validate_primary_key()


def test_validate_primary_key_duplicate_values_raises_error():
    """Test validate_primary_key raises error for duplicate PKs."""
    df = pd.DataFrame({'id': [1, 1, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TrackedDataFrame(df, 'id')

    with pytest.raises(DataValidationError, match="duplicate values"):
        tdf.validate_primary_key()


def test_validate_data_returns_empty_for_valid(sample_df):
    """Test validate_data returns empty list for valid data."""
    tdf = TrackedDataFrame(sample_df, 'id')
    errors = tdf.validate_data()

    assert errors == []


def test_validate_data_detects_missing_pk(sample_df):
    """Test validate_data detects missing PK column."""
    tdf = TrackedDataFrame(sample_df, 'id')
    tdf.drop_column_safe('id')

    errors = tdf.validate_data()
    assert len(errors) > 0
    assert any('dropped' in error for error in errors)


def test_validate_data_detects_null_pk():
    """Test validate_data detects null PK values."""
    df = pd.DataFrame({'id': [1, None, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TrackedDataFrame(df, 'id')

    errors = tdf.validate_data()
    assert len(errors) > 0
    assert any('null' in error for error in errors)


def test_validate_data_detects_duplicate_pk():
    """Test validate_data detects duplicate PK values."""
    df = pd.DataFrame({'id': [1, 1, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    tdf = TrackedDataFrame(df, 'id')

    errors = tdf.validate_data()
    assert len(errors) > 0
    assert any('duplicate' in error for error in errors)


# ============================================================================
# Push Validation Integration Tests
# ============================================================================

def test_push_fails_when_single_pk_dropped(tmp_path):
    """Test push raises error when single PK column is dropped."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table and drop PK before pushing
    df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
    tdf = TrackedDataFrame(df, 'id')
    tdf.drop_column_safe('id')

    table = Table('users', tdf, 'id', engine)

    # Push should fail
    with pytest.raises(SchemaError, match="Cannot push table.*dropped"):
        table.push()


def test_push_fails_when_composite_pk_partially_dropped(tmp_path):
    """Test push raises error when part of composite PK is dropped."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    })

    # Create TrackedDataFrame and drop part of composite PK
    tdf = TrackedDataFrame(df, ['user_id', 'org_id'])
    tdf.drop_column_safe('org_id')

    table = Table('memberships', tdf, ['user_id', 'org_id'], engine)

    # Push should fail
    with pytest.raises(SchemaError, match="Cannot push table.*dropped"):
        table.push()


def test_push_succeeds_when_non_pk_columns_dropped(tmp_path):
    """Test push succeeds when only non-PK columns are dropped."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create TrackedDataFrame and drop non-PK column
    df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob'], 'age': [25, 30]})
    tdf = TrackedDataFrame(df, 'id')
    tdf.drop_column_safe('age')

    table = Table('users', tdf, 'id', engine)

    # Push should succeed (creates new table without 'age')
    table.push()  # Should not raise

    # Verify
    table.pull()
    assert 'age' not in table.data.columns
    # 'id' might be in columns or index
    assert 'id' in table.data.columns or table.data.to_pandas().index.name == 'id'


def test_database_push_validates_all_tables(tmp_path):
    """Test DataBase.push() validates all tables."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create database with tables
    db = DataBase(engine)

    # Add table 1
    df1 = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
    table1 = Table('users', df1, 'id', engine)
    db.add_table(table1)

    # Add table 2 and drop its PK
    df2 = pd.DataFrame({'id': [1, 2], 'title': ['Product A', 'Product B']})
    tdf2 = TrackedDataFrame(df2, 'id')
    tdf2.drop_column_safe('id')
    table2 = Table('products', tdf2, 'id', engine)
    db.add_table(table2)

    # Database push should fail
    with pytest.raises(SchemaError, match="Cannot push table.*dropped"):
        db.push()

