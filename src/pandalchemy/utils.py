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


def prepare_primary_key_for_table_creation(
    df: pd.DataFrame,
    primary_key: str | list[str]
) -> pd.DataFrame:
    """
    Prepare DataFrame for table creation by handling primary key naming and validation.

    This function allows using the DataFrame's index as the primary key by naming it
    with the provided primary_key parameter. It validates against ambiguous situations.

    Rules:
    - If primary_key is NOT in columns:
      - If index has a different name: raise ValueError (mismatch)
      - If index is unnamed: name it with primary_key value
      - Use the index as the primary key
    - If primary_key exists as BOTH column AND index name: raise ValueError (ambiguity)
    - If primary_key is only in columns: use existing behavior (no change needed)

    Args:
        df: DataFrame to process
        primary_key: Name of the primary key (str) or list of names (list[str])

    Returns:
        DataFrame with properly named index (if using index as PK)

    Raises:
        ValueError: If there's a mismatch between index name and primary_key,
                   or if primary_key exists in both columns and index (ambiguity)

    Examples:
        >>> # Index-based PK (unnamed index)
        >>> df = pd.DataFrame({'name': ['Alice', 'Bob']})
        >>> result = prepare_primary_key_for_table_creation(df, 'id')
        >>> result.index.name  # 'id'

        >>> # Column-based PK
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
        >>> result = prepare_primary_key_for_table_creation(df, 'id')
        >>> 'id' in result.columns  # True
    """
    from pandalchemy.pk_utils import normalize_primary_key

    df_copy = df.copy()
    pk_cols = normalize_primary_key(primary_key)

    # Handle single vs composite keys
    if len(pk_cols) == 1:
        pk_name = pk_cols[0]
        pk_in_columns = pk_name in df_copy.columns

        # Check index name
        if isinstance(df_copy.index, pd.MultiIndex):
            index_names = [n for n in df_copy.index.names if n is not None]
            pk_in_index = pk_name in index_names
        else:
            pk_in_index = df_copy.index.name == pk_name

        # Validation: Check for ambiguity
        if pk_in_columns and pk_in_index:
            raise ValueError(
                f"Ambiguous primary key '{pk_name}': exists as both a column and index name. "
                f"Please remove one or rename to avoid ambiguity."
            )

        # If PK is not in columns, handle index naming
        if not pk_in_columns:
            # Check if index has a different name
            if df_copy.index.name is not None and df_copy.index.name != pk_name:
                raise ValueError(
                    f"Primary key mismatch: index is named '{df_copy.index.name}' "
                    f"but primary_key parameter is '{pk_name}'. "
                    f"Either rename the index or use index name as primary_key parameter."
                )
            # Name the index with the primary_key value
            df_copy.index.name = pk_name
    else:
        # Composite key
        pk_in_columns = all(pk in df_copy.columns for pk in pk_cols)

        # Check MultiIndex names
        if isinstance(df_copy.index, pd.MultiIndex):
            index_names = [n for n in df_copy.index.names if n is not None]
            pk_in_index = all(pk in index_names for pk in pk_cols)

            # Check for ambiguity
            if pk_in_columns and pk_in_index:
                raise ValueError(
                    f"Ambiguous composite primary key {pk_cols}: exists as both columns and index names. "
                    f"Please remove one or rename to avoid ambiguity."
                )

            # If PK is not in columns, validate index names
            if not pk_in_columns:
                # Check if any index names differ
                current_names = list(df_copy.index.names)
                if len(current_names) != len(pk_cols):
                    if any(n is not None for n in current_names):
                        raise ValueError(
                            f"Primary key mismatch: MultiIndex has {len(current_names)} levels "
                            f"but primary_key specifies {len(pk_cols)} columns: {pk_cols}"
                        )
                    # Unnamed MultiIndex - name it
                    df_copy.index.names = pk_cols  # type: ignore[assignment]
                else:
                    # Check each name
                    for i, (current, expected) in enumerate(zip(current_names, pk_cols)):
                        if current is not None and current != expected:
                            raise ValueError(
                                f"Primary key mismatch: MultiIndex level {i} is named '{current}' "
                                f"but primary_key parameter specifies '{expected}'. "
                                f"Either rename the index or use index names as primary_key parameter."
                            )
                    # Set names (handles partially named indexes)
                    df_copy.index.names = pk_cols  # type: ignore[assignment]
        else:
            # Single index but composite key requested - not in index
            if not pk_in_columns:
                raise ValueError(
                    f"Composite primary key {pk_cols} not found in DataFrame columns. "
                    f"For composite keys, all columns must exist in the DataFrame."
                )

    return df_copy


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
    from pandalchemy.pk_utils import locate_primary_key

    df_copy = df.copy()

    # Check if PK is in index
    _, in_index = locate_primary_key(df_copy, primary_key)

    # If PK is in index, reset it to columns
    if in_index:
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

