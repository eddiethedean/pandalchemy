"""
Utility functions for pandalchemy.

This module provides helper functions used across the pandalchemy codebase,
including type conversions, data cleaning, and common operations.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def convert_numpy_types(value: Any) -> Any:
    """
    Convert numpy types to Python native types.

    This is necessary because some database operations don't handle numpy
    types correctly (e.g., numpy.int64 in SQL WHERE clauses).

    Args:
        value: Value that may be a numpy type

    Returns:
        Python native type equivalent

    Examples:
        >>> import numpy as np
        >>> convert_numpy_types(np.int64(5))
        5
        >>> convert_numpy_types("hello")
        'hello'
    """
    if hasattr(value, 'item'):
        # numpy scalar types have an item() method
        return value.item()
    return value


def convert_record_types(record: dict[str, Any]) -> dict[str, Any]:
    """
    Convert all numpy types in a dictionary to Python native types.

    Args:
        record: Dictionary potentially containing numpy types

    Returns:
        Dictionary with all values converted to Python native types

    Examples:
        >>> import numpy as np
        >>> record = {'id': np.int64(1), 'value': np.float64(2.5), 'name': 'test'}
        >>> convert_record_types(record)
        {'id': 1, 'value': 2.5, 'name': 'test'}
    """
    return {k: convert_numpy_types(v) for k, v in record.items()}


def convert_records_list(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert all numpy types in a list of dictionaries to Python native types.

    Args:
        records: List of dictionaries potentially containing numpy types

    Returns:
        List of dictionaries with all values converted to Python native types
    """
    return [convert_record_types(record) for record in records]


def pandas_dtype_to_python_type(dtype: Any) -> type:
    """
    Convert pandas dtype to Python type.

    This is used for schema migrations where Python types are expected
    (e.g., transmutation library).

    Args:
        dtype: Pandas dtype

    Returns:
        Corresponding Python type (int, float, str, bool)

    Examples:
        >>> import pandas as pd
        >>> pandas_dtype_to_python_type(pd.Int64Dtype())
        <class 'int'>
        >>> pandas_dtype_to_python_type('object')
        <class 'str'>
    """
    if dtype is None:
        return str

    dtype_str = str(dtype).lower()

    if 'int' in dtype_str:
        return int
    elif 'float' in dtype_str:
        return float
    elif 'bool' in dtype_str:
        return bool
    elif 'datetime' in dtype_str:
        return str  # datetime stored as string
    elif 'object' in dtype_str:
        return str
    else:
        return str


def normalize_schema(schema: str | None) -> str | None:
    """
    Normalize schema name by converting empty strings to None.

    This ensures consistent behavior across different database operations
    where None is the standard way to indicate "no schema".

    Args:
        schema: Schema name (may be None or empty string)

    Returns:
        Schema name or None

    Examples:
        >>> normalize_schema('')
        None
        >>> normalize_schema('public')
        'public'
        >>> normalize_schema(None)
        None
    """
    return schema if schema else None


def get_table_reference(table_name: str, schema: str | None = None) -> str:
    """
    Get the full table reference string for SQL queries.

    Args:
        table_name: Name of the table
        schema: Optional schema name

    Returns:
        Full table reference (e.g., "schema.table" or just "table")

    Examples:
        >>> get_table_reference('users', 'public')
        'public.users'
        >>> get_table_reference('users', None)
        'users'
    """
    if schema:
        return f"{schema}.{table_name}"
    return table_name


def ensure_dataframe_copy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure we have a copy of the DataFrame to avoid modifying the original.

    Args:
        df: DataFrame to copy

    Returns:
        Copy of the DataFrame
    """
    return df.copy()


def extract_primary_key_column(
    df: pd.DataFrame,
    primary_key: str | list[str]
) -> pd.DataFrame:
    """
    Ensure primary key is a column (not index) in the DataFrame.

    Handles both single-column and composite (multi-column) primary keys.

    Args:
        df: DataFrame to process
        primary_key: Name of the primary key (str) or list of names (list[str])

    Returns:
        DataFrame with primary key as column(s)
    """
    df_copy = df.copy()

    # Handle single column primary key
    if isinstance(primary_key, str):
        if df_copy.index.name == primary_key:
            df_copy = df_copy.reset_index()
    else:
        # Handle composite primary key (MultiIndex)
        pk_cols = list(primary_key)
        if isinstance(df_copy.index, pd.MultiIndex):
            # Check if index names match PK columns
            if all(name in pk_cols for name in df_copy.index.names):
                df_copy = df_copy.reset_index()
        elif df_copy.index.name in pk_cols:
            df_copy = df_copy.reset_index()

    return df_copy


def validate_dataframe_for_sql(df: pd.DataFrame) -> None:
    """
    Validate that a DataFrame meets requirements for SQL operations.

    Args:
        df: DataFrame to validate

    Raises:
        DataValidationError: If DataFrame doesn't meet requirements
    """
    from pandalchemy.exceptions import DataValidationError

    if not df.index.is_unique:
        raise DataValidationError(
            "DataFrame index must have unique values for SQL operations"
        )

    if not df.columns.is_unique:
        raise DataValidationError(
            "DataFrame columns must have unique names for SQL operations"
        )

