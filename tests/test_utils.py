"""Tests for utils module."""

import pandas as pd
import pytest

from pandalchemy.exceptions import DataValidationError
from pandalchemy.utils import validate_dataframe_for_sql


def test_validate_dataframe_valid():
    """Test validation with valid DataFrame."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C']
    }).set_index('id')

    # Should not raise
    validate_dataframe_for_sql(df)


def test_validate_dataframe_duplicate_index():
    """Test validation fails with duplicate index."""
    df = pd.DataFrame({
        'id': [1, 1, 2],  # Duplicate id
        'name': ['A', 'B', 'C']
    }).set_index('id')

    with pytest.raises(DataValidationError) as exc_info:
        validate_dataframe_for_sql(df)

    assert 'unique values' in str(exc_info.value).lower()


def test_validate_dataframe_duplicate_columns():
    """Test validation fails with duplicate column names."""
    # Create DataFrame with duplicate columns
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=['a', 'a', 'b'])

    with pytest.raises(DataValidationError) as exc_info:
        validate_dataframe_for_sql(df)

    assert 'unique names' in str(exc_info.value).lower()


def test_validate_empty_dataframe():
    """Test validation with empty DataFrame."""
    df = pd.DataFrame(columns=['id', 'name'])

    # Empty DataFrame is valid
    validate_dataframe_for_sql(df)


def test_validate_single_row():
    """Test validation with single row DataFrame."""
    df = pd.DataFrame({
        'id': [1],
        'value': [100]
    }).set_index('id')

    # Single row is valid
    validate_dataframe_for_sql(df)


def test_validate_single_column():
    """Test validation with single column DataFrame."""
    df = pd.DataFrame({
        'id': [1, 2, 3]
    }).set_index('id')

    # Single column is valid
    validate_dataframe_for_sql(df)

