"""
Primary Key Utilities

Shared utilities for handling primary key operations across pandalchemy.
Eliminates duplicate code and provides single source of truth for PK handling.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


def normalize_primary_key(primary_key: str | list[str]) -> list[str]:
    """
    Convert primary key to list format.

    Args:
        primary_key: Single column name (str) or list of column names

    Returns:
        List of primary key column names

    Example:
        >>> normalize_primary_key('id')
        ['id']
        >>> normalize_primary_key(['user_id', 'org_id'])
        ['user_id', 'org_id']
    """
    return [primary_key] if isinstance(primary_key, str) else list(primary_key)


def locate_primary_key(
    df: pd.DataFrame,
    primary_key: str | list[str]
) -> tuple[bool, bool]:
    """
    Determine if primary key is in columns vs index.

    Args:
        df: DataFrame to check
        primary_key: Primary key column name(s)

    Returns:
        Tuple of (in_columns, in_index) booleans

    Example:
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        >>> locate_primary_key(df, 'id')
        (True, False)
        >>> df_indexed = df.set_index('id')
        >>> locate_primary_key(df_indexed, 'id')
        (False, True)
    """
    pk_cols = normalize_primary_key(primary_key)

    # Check if in columns
    in_columns = all(col in df.columns for col in pk_cols)

    # Check if in index
    in_index = False
    if len(pk_cols) == 1:
        # Single column PK
        in_index = df.index.name == pk_cols[0]
    else:
        # Composite PK
        if isinstance(df.index, pd.MultiIndex):
            in_index = all(col in df.index.names for col in pk_cols)

    return in_columns, in_index


def extract_pk_values(
    df: pd.DataFrame,
    primary_key: str | list[str]
) -> set[Any]:
    """
    Extract all primary key values from DataFrame.

    Handles primary keys in both columns and index.
    For composite keys, returns set of tuples.

    Args:
        df: DataFrame to extract from
        primary_key: Primary key column name(s)

    Returns:
        Set of primary key values (tuples for composite keys)

    Example:
        >>> df = pd.DataFrame({'id': [1, 2, 3], 'name': ['a', 'b', 'c']})
        >>> extract_pk_values(df, 'id')
        {1, 2, 3}
        >>> df_comp = pd.DataFrame({'uid': [1, 1], 'oid': [1, 2]})
        >>> extract_pk_values(df_comp, ['uid', 'oid'])
        {(1, 1), (1, 2)}
    """
    pk_cols = normalize_primary_key(primary_key)

    if len(pk_cols) == 1:
        # Single column primary key
        pk_col = pk_cols[0]
        if pk_col in df.columns:
            return set(df[pk_col].values)
        elif df.index.name == pk_col:
            return set(df.index.values)
        else:
            return set()
    else:
        # Composite primary key
        if all(col in df.columns for col in pk_cols):
            # PK columns are in DataFrame columns
            return {tuple(row) for row in df[pk_cols].values}
        elif isinstance(df.index, pd.MultiIndex) and \
             all(name in df.index.names for name in pk_cols):
            # PK is in MultiIndex
            return set(df.index.values)
        else:
            return set()


def set_pk_as_index(
    df: pd.DataFrame,
    pk_cols: list[str]
) -> pd.DataFrame:
    """
    Set primary key column(s) as DataFrame index.

    Handles both single-column and composite keys.
    Returns a new DataFrame with PK as index.

    Args:
        df: DataFrame to modify
        pk_cols: List of primary key column names

    Returns:
        DataFrame with primary key as index

    Raises:
        KeyError: If any pk_cols not in DataFrame

    Example:
        >>> df = pd.DataFrame({'id': [1, 2], 'name': ['a', 'b']})
        >>> result = set_pk_as_index(df, ['id'])
        >>> result.index.name
        'id'
        >>> df_comp = pd.DataFrame({'uid': [1, 1], 'oid': [1, 2], 'val': [10, 20]})
        >>> result = set_pk_as_index(df_comp, ['uid', 'oid'])
        >>> isinstance(result.index, pd.MultiIndex)
        True
    """
    # Validate columns exist
    missing = [col for col in pk_cols if col not in df.columns]
    if missing:
        raise KeyError(f"Primary key columns not found in DataFrame: {missing}")

    # Set index
    if len(pk_cols) == 1:
        return df.set_index(pk_cols[0])
    else:
        # Creates MultiIndex
        return df.set_index(pk_cols)

