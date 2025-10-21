"""Tests for immutable index (primary key immutability)."""

import pandas as pd
import pytest

from pandalchemy import TableDataFrame
from pandalchemy.exceptions import DataValidationError


@pytest.fixture
def sample_df():
    """Create a sample DataFrame with index as PK."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }).set_index('id')


@pytest.fixture
def composite_pk_df():
    """Create a DataFrame with composite PK as MultiIndex."""
    df = pd.DataFrame({
        'user_id': ['u1', 'u1', 'u2'],
        'org_id': ['org1', 'org2', 'org1'],
        'role': ['admin', 'user', 'user'],
        'active': [True, True, False]
    })
    return df.set_index(['user_id', 'org_id'])


# ============================================================================
# Index Setter Tests
# ============================================================================

def test_index_setter_raises_error(sample_df):
    """Test that setting index directly raises DataValidationError."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')

    with pytest.raises(DataValidationError, match="Cannot modify index directly"):
        tdf.index = [10, 20, 30]


def test_index_setter_with_multiindex_raises_error(composite_pk_df):
    """Test that setting MultiIndex raises error."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])

    new_index = pd.MultiIndex.from_tuples([('x', 'y')], names=['user_id', 'org_id'])
    with pytest.raises(DataValidationError, match="Cannot modify index directly"):
        tdf.index = new_index


# ============================================================================
# update_row() PK Immutability Tests
# ============================================================================

def test_update_row_cannot_update_single_pk(sample_df):
    """Test that update_row cannot update primary key column."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')

    # Trying to update PK should raise error
    with pytest.raises(DataValidationError, match="Cannot update primary key"):
        tdf.update_row(1, {'id': 999, 'age': 30})


def test_update_row_can_update_non_pk_columns(sample_df):
    """Test that update_row CAN update non-PK columns."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')

    # Updating non-PK columns should work fine
    tdf.update_row(1, {'age': 26, 'name': 'Alicia'})

    row = tdf.get_row(1)
    assert row['age'] == 26
    assert row['name'] == 'Alicia'


def test_update_row_cannot_update_composite_pk(composite_pk_df):
    """Test that update_row cannot update composite PK columns."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])

    # Trying to update any part of composite PK should raise error
    with pytest.raises(DataValidationError, match="Cannot update primary key"):
        tdf.update_row(('u1', 'org1'), {'user_id': 'u999', 'role': 'owner'})

    with pytest.raises(DataValidationError, match="Cannot update primary key"):
        tdf.update_row(('u1', 'org1'), {'org_id': 'org999'})


def test_update_row_composite_pk_non_pk_columns_ok(composite_pk_df):
    """Test that update_row CAN update non-PK columns with composite PK."""
    tdf = TableDataFrame(data=composite_pk_df, primary_key=['user_id', 'org_id'])

    # Updating non-PK columns should work
    tdf.update_row(('u1', 'org1'), {'role': 'superadmin', 'active': False})

    row = tdf.get_row(('u1', 'org1'))
    assert row['role'] == 'superadmin'
    assert not row['active']


# ============================================================================
# Delete + Insert Pattern (Workaround for "Changing" PK)
# ============================================================================

def test_delete_and_insert_to_change_pk(sample_df):
    """Test that deleting and inserting is the way to 'change' a PK."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')

    # Get original data
    original_row = tdf.get_row(1)
    original_name = original_row['name']
    original_age = original_row['age']

    # "Change" PK from 1 to 999: delete old, insert new
    tdf.delete_row(1)
    tdf.add_row({'id': 999, 'name': original_name, 'age': original_age})

    # Verify
    assert not tdf.row_exists(1)
    assert tdf.row_exists(999)
    assert tdf.get_row(999)['name'] == original_name


# ============================================================================
# upsert_row() with PK Immutability
# ============================================================================

def test_upsert_row_updates_non_pk(sample_df):
    """Test that upsert_row updates non-PK columns when row exists."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')

    # Upsert existing row (should update non-PK columns)
    tdf.upsert_row({'id': 2, 'name': 'Robert', 'age': 31})

    row = tdf.get_row(2)
    assert row['name'] == 'Robert'
    assert row['age'] == 31


def test_upsert_row_inserts_when_not_exists(sample_df):
    """Test that upsert_row inserts when row doesn't exist."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')

    # Upsert new row
    tdf.upsert_row({'id': 4, 'name': 'Dave', 'age': 40})

    assert tdf.row_exists(4)
    assert tdf.get_row(4)['name'] == 'Dave'


# ============================================================================
# Validation Tests
# ============================================================================

def test_validate_data_allows_delete_and_insert(sample_df):
    """Test that validation allows normal delete/insert operations."""
    tdf = TableDataFrame(data=sample_df, primary_key='id')

    # Delete a row
    tdf.delete_row(2)

    # Verify delete worked correctly
    assert not tdf.row_exists(2)
    assert tdf.row_exists(1)
    assert tdf.row_exists(3)

    # Add a new row
    tdf.add_row({'id': 4, 'name': 'Dave', 'age': 40})

    # Verify add worked correctly
    assert tdf.row_exists(4)

    # Validation should pass (PK should be unique and non-null)
    errors = tdf.validate_data()
    assert errors == [], f"Unexpected validation errors: {errors}"


def test_pk_immutability_documented_in_docstring():
    """Test that immutability is documented."""
    # Check that update_row docstring mentions immutability
    assert "immutable" in TableDataFrame.update_row.__doc__.lower()
    assert "cannot" in TableDataFrame.update_row.__doc__.lower()

