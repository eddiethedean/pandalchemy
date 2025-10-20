"""
Tracked DataFrame wrapper for monitoring changes.

This module provides the TrackedDataFrame class that wraps a pandas DataFrame
and intercepts all modification operations to track changes.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pandas import DataFrame

from pandalchemy.change_tracker import ChangeTracker


class TrackedDataFrame:
    """
    A wrapper around pandas DataFrame that tracks all modifications.

    This class delegates all operations to an internal pandas DataFrame while
    intercepting modification operations to track changes via a ChangeTracker.
    Provides complete pandas 2.0+ API compatibility with automatic change tracking.

    Supports all pandas DataFrame mutating operations including:
    - Data modification: drop, fillna, replace, update, etc.
    - Filling methods: bfill, ffill, interpolate
    - Conditional methods: clip, mask, where
    - Index operations: sort_values, reset_index, reindex, truncate
    - Column operations: rename, add_prefix, add_suffix, insert, pop
    - Expression evaluation: eval

    Supports comprehensive DataFrame-returning operations:
    - Selection: head, tail, sample, query, filter, nlargest, nsmallest
    - Transformation: assign, transform, apply, abs, round, diff, shift
    - Combining: merge, join, combine_first
    - Reshaping: pivot, pivot_table, melt, stack, unstack, transpose
    - Statistics: corr, cov, corrwith
    - Type conversion: astype, convert_dtypes, infer_objects

    Important: Non-mutating methods (head, tail, filter, etc.) return NEW
    TrackedDataFrame instances with independent change trackers. This means
    modifications to the returned DataFrame won't affect the original's tracker.

    Example:
        >>> tdf = TrackedDataFrame(df, 'id')
        >>> subset = tdf.head(10)  # Independent tracker
        >>> subset['age'] = 100    # Only tracked in subset
        >>> tdf.get_tracker().has_changes()  # False - original unchanged

    Attributes:
        _data: The underlying pandas DataFrame
        _tracker: The ChangeTracker instance monitoring changes
        _primary_key: Name of the primary key column
    """

    # Methods that modify the DataFrame and should be tracked
    _MUTATING_METHODS = {
        # Original methods with inplace support
        'drop', 'drop_duplicates', 'dropna', 'fillna', 'replace',
        'sort_values', 'sort_index', 'reset_index', 'set_index',
        'rename', 'insert', 'pop', 'update',
        # Pandas 2.0 additional mutating methods with inplace
        'bfill', 'ffill', 'interpolate',  # Filling methods
        'clip', 'mask', 'where',  # Conditional methods
        'eval'  # Expression evaluation
    }

    # Methods that return a new DataFrame (should not track on call, but on assignment)
    _RETURNING_METHODS = {
        # Basic selection
        'copy', 'head', 'tail', 'sample', 'query', 'filter',
        'select_dtypes', 'nlargest', 'nsmallest', 'between_time', 'at_time',

        # Type operations
        'astype', 'convert_dtypes', 'infer_objects',

        # Application and transformation
        'apply', 'applymap', 'map', 'transform', 'aggregate', 'agg',
        'assign', 'abs', 'round', 'pct_change', 'diff', 'shift',

        # Ranking and sorting
        'rank',

        # Combining DataFrames
        'merge', 'join', 'combine_first', 'append',

        # Reshaping and pivoting
        'pivot', 'pivot_table', 'melt', 'stack', 'unstack',
        'transpose', 'explode', 'squeeze',

        # Statistics and correlation
        'corr', 'cov', 'corrwith',

        # Column operations
        'add_prefix', 'add_suffix',

        # Indexing operations
        'align', 'reindex', 'truncate',

        # Set operations
        'isin',
    }

    def __init__(self, data: DataFrame, primary_key: str, tracker: ChangeTracker | None = None):
        """
        Initialize a TrackedDataFrame.

        Args:
            data: The pandas DataFrame to wrap
            primary_key: Name of the primary key column
            tracker: Optional existing ChangeTracker, creates new one if None
        """
        object.__setattr__(self, '_data', data)
        object.__setattr__(self, '_primary_key', primary_key)

        if tracker is None:
            tracker = ChangeTracker(primary_key, data)
        object.__setattr__(self, '_tracker', tracker)

    def __getattr__(self, name: str) -> Any:
        """
        Delegate attribute access to the underlying DataFrame.

        For mutating methods, wrap them to track changes.
        For returning methods, wrap result if it's a DataFrame.
        """
        attr = getattr(self._data, name)

        if name in self._MUTATING_METHODS:
            # Wrap mutating methods to track changes
            def wrapped_method(*args, **kwargs):
                # Check if inplace operation
                inplace = kwargs.get('inplace', False)

                # Record the operation
                self._tracker.record_operation(name, *args, **kwargs)

                # Call the original method
                result = attr(*args, **kwargs)

                if inplace or result is None:
                    # Track column changes for specific operations
                    if name == 'drop' and kwargs.get('axis') == 1:
                        # Dropping columns
                        if isinstance(args[0], (list, tuple)):
                            for col in args[0]:
                                self._tracker.track_column_drop(col)
                        else:
                            self._tracker.track_column_drop(args[0])
                    elif name == 'rename' and 'columns' in kwargs:
                        # Renaming columns
                        for old_name, new_name in kwargs['columns'].items():
                            self._tracker.track_column_rename(old_name, new_name)

                    # Recompute row changes after mutation
                    self._tracker.compute_row_changes(self._data)
                    return self
                else:
                    # Return NEW TrackedDataFrame with INDEPENDENT tracker
                    if isinstance(result, DataFrame):
                        return TrackedDataFrame(result, self._primary_key, tracker=None)
                    return result

            return wrapped_method
        elif name in self._RETURNING_METHODS:
            # Wrap returning methods to wrap DataFrame results with INDEPENDENT trackers
            def wrapped_returning_method(*args, **kwargs):
                result = attr(*args, **kwargs)
                # Wrap DataFrame results with NEW independent tracker
                if isinstance(result, DataFrame):
                    return TrackedDataFrame(result, self._primary_key, tracker=None)
                return result

            return wrapped_returning_method

        return attr

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Intercept attribute setting to track changes.
        """
        if name.startswith('_'):
            # Internal attributes
            object.__setattr__(self, name, value)
        else:
            # Check if this attribute has a property setter defined
            prop = getattr(type(self), name, None)
            if isinstance(prop, property) and prop.fset is not None:
                # Use the property setter
                prop.fset(self, value)
            else:
                # Setting DataFrame attributes directly
                self._tracker.record_operation('__setattr__', name, value)
                setattr(self._data, name, value)
                self._tracker.compute_row_changes(self._data)

    def __getitem__(self, key: Any) -> Any:
        """
        Delegate item access to the underlying DataFrame.

        Returns a new TrackedDataFrame with independent tracker if result is a DataFrame.
        """
        result = self._data[key]

        # If result is a DataFrame, wrap it with NEW independent tracker
        if isinstance(result, DataFrame):
            return TrackedDataFrame(result, self._primary_key, tracker=None)

        return result

    def __setitem__(self, key: Any, value: Any) -> None:
        """
        Intercept item setting to track changes.
        """
        self._tracker.record_operation('__setitem__', key, value)

        # Track new columns
        if isinstance(key, str) and key not in self._data.columns:
            self._tracker.track_column_addition(key)

        self._data[key] = value
        self._tracker.compute_row_changes(self._data)

    def __delitem__(self, key: Any) -> None:
        """
        Intercept item deletion to track changes.
        """
        self._tracker.record_operation('__delitem__', key)

        if isinstance(key, str):
            self._tracker.track_column_drop(key)

        del self._data[key]
        self._tracker.compute_row_changes(self._data)

    def __len__(self) -> int:
        """Return the length of the DataFrame."""
        return len(self._data)

    def __repr__(self) -> str:
        """Return string representation."""
        return f"TrackedDataFrame(\n{repr(self._data)}\n)"

    def __str__(self) -> str:
        """Return string representation."""
        return str(self._data)

    def _repr_html_(self) -> str:
        """Return HTML representation for Jupyter notebooks."""
        return self._data._repr_html_()

    @property
    def loc(self):
        """Access via loc indexer with change tracking."""
        return TrackedLocIndexer(self)

    @property
    def iloc(self):
        """Access via iloc indexer with change tracking."""
        return TrackedIlocIndexer(self)

    @property
    def at(self):
        """Access via at indexer with change tracking."""
        return TrackedAtIndexer(self)

    @property
    def iat(self):
        """Access via iat indexer with change tracking."""
        return TrackedIatIndexer(self)

    @property
    def shape(self):
        """Return the shape of the DataFrame."""
        return self._data.shape

    @property
    def columns(self):
        """Return the columns of the DataFrame."""
        return self._data.columns

    @columns.setter
    def columns(self, value):
        """Set the columns of the DataFrame with tracking."""
        old_cols = list(self._data.columns)
        new_cols = list(value)

        # Track renamed columns
        for old, new in zip(old_cols, new_cols):
            if old != new:
                self._tracker.track_column_rename(old, new)

        # Record the operation
        self._tracker.record_operation('set_columns', value)

        self._data.columns = value

    @property
    def index(self):
        """Return the index of the DataFrame."""
        return self._data.index

    @index.setter
    def index(self, value):
        """
        Prevent direct index modification since index contains primary keys.

        Primary key values are immutable. Use add_row() to create new records
        or delete_row() to remove records.

        Raises:
            DataValidationError: Primary keys are immutable
        """
        from pandalchemy.exceptions import DataValidationError

        raise DataValidationError(
            "Cannot modify index directly. Primary key values are immutable. "
            "Use add_row() to create new records or delete_row() to remove records.",
            details={'operation': 'set_index', 'attempted_value': str(value)}
        )

    @property
    def dtypes(self):
        """Return the dtypes of the DataFrame."""
        return self._data.dtypes

    @property
    def values(self):
        """Return the values of the DataFrame."""
        return self._data.values

    def to_pandas(self) -> DataFrame:
        """
        Return the underlying pandas DataFrame.

        Returns:
            The wrapped pandas DataFrame
        """
        return self._data.copy()

    def get_tracker(self) -> ChangeTracker:
        """
        Get the change tracker.

        Returns:
            The ChangeTracker instance
        """
        return self._tracker

    def copy(self, deep: bool = True) -> TrackedDataFrame:
        """
        Create a copy of the TrackedDataFrame.

        Args:
            deep: Whether to make a deep copy

        Returns:
            New TrackedDataFrame instance
        """
        new_data = self._data.copy(deep=deep)
        new_tracker = ChangeTracker(self._primary_key, new_data)
        return TrackedDataFrame(new_data, self._primary_key, new_tracker)

    # ============================================================================
    # SQL Helper Methods
    # ============================================================================

    def _get_pk_columns(self) -> list[str]:
        """
        Get primary key columns as a list.

        Returns:
            List of primary key column names
        """
        if isinstance(self._primary_key, str):
            return [self._primary_key]
        return list(self._primary_key)

    def _get_pk_condition(self, pk_value: Any) -> Any:
        """
        Create boolean mask for primary key matching.

        Handles primary keys in both columns and index.

        Args:
            pk_value: Single value or tuple/list of values matching PK columns

        Returns:
            Boolean Series indicating matching rows

        Raises:
            ValueError: If pk_value format doesn't match primary key structure
        """
        pk_cols = self._get_pk_columns()

        if len(pk_cols) == 1:
            # Single column PK
            pk_col = pk_cols[0]

            # Check if PK is in index
            if self._data.index.name == pk_col:
                return self._data.index == pk_value
            # Otherwise it's in columns
            elif pk_col in self._data.columns:
                return self._data[pk_col] == pk_value
            else:
                # PK not found
                return pd.Series([False] * len(self._data), index=self._data.index)
        else:
            # Composite key - pk_value should be a tuple or list
            if not isinstance(pk_value, (tuple, list)):
                raise ValueError(
                    f"Composite key requires tuple/list of {len(pk_cols)} values, "
                    f"got {type(pk_value).__name__}"
                )
            if len(pk_value) != len(pk_cols):
                raise ValueError(
                    f"Expected {len(pk_cols)} values for composite key {pk_cols}, "
                    f"got {len(pk_value)}"
                )

            # Check if PK is in MultiIndex
            if isinstance(self._data.index, pd.MultiIndex) and all(
                name in self._data.index.names for name in pk_cols
            ):
                return self._data.index == tuple(pk_value)
            # Otherwise it's in columns
            elif all(col in self._data.columns for col in pk_cols):
                condition = pd.Series([True] * len(self._data), index=self._data.index)
                for col, val in zip(pk_cols, pk_value):
                    condition &= (self._data[col] == val)
                return condition
            else:
                # PK not found
                return pd.Series([False] * len(self._data), index=self._data.index)

    # Validation Methods

    def validate_primary_key(self) -> bool:
        """
        Validate primary key integrity.

        Returns:
            True if valid

        Raises:
            SchemaError: If primary key columns have been dropped
            DataValidationError: If primary key values are not unique or contain nulls
        """
        from pandalchemy.exceptions import DataValidationError, SchemaError

        pk_cols = self._get_pk_columns()

        # Check columns exist (can be in columns or index)
        missing_cols = []
        for col in pk_cols:
            if col not in self._data.columns and self._data.index.name != col:
                # For composite keys, check if all are in index
                if isinstance(self._data.index, pd.MultiIndex):
                    if col not in self._data.index.names:
                        missing_cols.append(col)
                else:
                    missing_cols.append(col)

        if missing_cols:
            raise SchemaError(
                f"Primary key column(s) {missing_cols} have been dropped from DataFrame",
                details={'primary_key': pk_cols, 'missing': missing_cols}
            )

        # Check for nulls
        for col in pk_cols:
            if col in self._data.columns:
                if self._data[col].isnull().any():
                    raise DataValidationError(
                        f"Primary key column '{col}' contains null values",
                        details={'column': col}
                    )
            elif self._data.index.name == col:
                # Single index
                if self._data.index.isnull().any():
                    raise DataValidationError(
                        f"Primary key '{col}' in index contains null values",
                        details={'column': col}
                    )
            elif isinstance(self._data.index, pd.MultiIndex) and col in self._data.index.names:
                # MultiIndex - check specific level
                level_idx = self._data.index.names.index(col)
                if self._data.index.get_level_values(level_idx).isnull().any():
                    raise DataValidationError(
                        f"Primary key '{col}' in index contains null values",
                        details={'column': col}
                    )

        # Check uniqueness
        if len(pk_cols) == 1:
            col = pk_cols[0]
            if col in self._data.columns:
                if self._data[col].duplicated().any():
                    raise DataValidationError(
                        f"Primary key column '{col}' contains duplicate values",
                        details={'column': col}
                    )
            elif self._data.index.name == col and self._data.index.duplicated().any():
                raise DataValidationError(
                    f"Primary key in index '{col}' contains duplicate values",
                    details={'column': col}
                )
        else:
            # Composite key
            # Check if all PK cols are in columns
            if all(col in self._data.columns for col in pk_cols):
                if self._data[pk_cols].duplicated().any():
                    raise DataValidationError(
                        f"Primary key combination {pk_cols} contains duplicate values",
                        details={'columns': pk_cols}
                    )
            elif isinstance(self._data.index, pd.MultiIndex) and all(col in self._data.index.names for col in pk_cols):
                if self._data.index.duplicated().any():
                    raise DataValidationError(
                        f"Primary key combination in index {pk_cols} contains duplicate values",
                        details={'columns': pk_cols}
                    )

        return True

    def validate_data(self) -> list[str]:
        """
        Validate data integrity before push.

        Checks primary key in both columns and index.

        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []

        pk_cols = self._get_pk_columns()

        # Check PK columns still exist (can be in columns or index)
        missing_cols = []
        for col in pk_cols:
            if col not in self._data.columns and self._data.index.name != col:
                # For composite keys, check if all are in index
                if isinstance(self._data.index, pd.MultiIndex):
                    if col not in self._data.index.names:
                        missing_cols.append(col)
                else:
                    missing_cols.append(col)

        if missing_cols:
            errors.append(f"Primary key column(s) {missing_cols} have been dropped")

        # Check for nulls in PK
        for col in pk_cols:
            if col in self._data.columns and self._data[col].isnull().any():
                errors.append(f"Primary key column '{col}' contains null values")
            elif self._data.index.name == col:
                # Single index
                if self._data.index.isnull().any():
                    errors.append(f"Primary key '{col}' in index contains null values")
            elif isinstance(self._data.index, pd.MultiIndex) and col in self._data.index.names:
                # MultiIndex - check specific level
                level_idx = self._data.index.names.index(col)
                if self._data.index.get_level_values(level_idx).isnull().any():
                    errors.append(f"Primary key '{col}' in index contains null values")

        # Check uniqueness
        if len(pk_cols) == 1:
            col = pk_cols[0]
            if col in self._data.columns:
                if self._data[col].duplicated().any():
                    errors.append(f"Primary key column '{col}' contains duplicates")
            elif self._data.index.name == col:
                if self._data.index.duplicated().any():
                    errors.append(f"Primary key in index '{col}' contains duplicates")
        else:
            # Composite key
            if all(col in self._data.columns for col in pk_cols):
                if self._data[pk_cols].duplicated().any():
                    errors.append(f"Primary key combination {pk_cols} contains duplicates")
            elif isinstance(self._data.index, pd.MultiIndex) and all(col in self._data.index.names for col in pk_cols):
                if self._data.index.duplicated().any():
                    errors.append(f"Primary key combination in index {pk_cols} contains duplicates")

        # Check for duplicate column names
        if len(self._data.columns) != len(set(self._data.columns)):
            errors.append("DataFrame contains duplicate column names")

        return errors

    def has_changes(self) -> bool:
        """
        Check if there are any tracked changes.

        Returns:
            True if changes have been tracked
        """
        return self._tracker.has_changes()

    def get_changes_summary(self) -> dict:
        """
        Get a human-readable summary of all tracked changes.

        Returns:
            Dictionary with counts and details of changes
        """
        return self._tracker.get_summary()

    # Primary Key Operations

    def get_primary_key(self) -> str | list[str]:
        """
        Get the current primary key column name(s).

        Returns:
            String for single column PK, list of strings for composite PK
        """
        return self._primary_key

    def set_primary_key(self, column_name: str | list[str], validate: bool = True) -> None:
        """
        Change the primary key column(s).

        This method automatically moves the specified column(s) to the DataFrame index
        to maintain consistency with the convention that primary keys are the index.

        Args:
            column_name: Name of column (str) or list of columns (list[str])
            validate: Whether to validate uniqueness of the key combination

        Example:
            # Single column - moves 'user_id' to index
            df.set_primary_key('user_id')

            # Composite key - creates MultiIndex with both columns
            df.set_primary_key(['user_id', 'org_id'])

        Raises:
            SchemaError: If columns don't exist
            DataValidationError: If values aren't unique (when validate=True)
        """
        from pandalchemy.exceptions import DataValidationError, SchemaError

        # Normalize to list
        cols = [column_name] if isinstance(column_name, str) else list(column_name)

        # Check columns exist
        missing = [col for col in cols if col not in self._data.columns]
        if missing:
            raise SchemaError(
                f"Cannot set primary key: column(s) {missing} do not exist",
                details={'columns': missing}
            )

        # Validate uniqueness if requested
        if validate:
            if len(cols) == 1:
                if self._data[cols[0]].duplicated().any():
                    raise DataValidationError(
                        f"Column '{cols[0]}' contains duplicate values",
                        details={'column': cols[0]}
                    )
                if self._data[cols[0]].isnull().any():
                    raise DataValidationError(
                        f"Column '{cols[0]}' contains null values",
                        details={'column': cols[0]}
                    )
            else:
                if self._data[cols].duplicated().any():
                    raise DataValidationError(
                        f"Column combination {cols} contains duplicate values",
                        details={'columns': cols}
                    )
                for col in cols:
                    if self._data[col].isnull().any():
                        raise DataValidationError(
                            f"Primary key column '{col}' contains null values",
                            details={'column': col}
                        )

        # Update primary key
        old_pk = self._primary_key
        self._primary_key = column_name
        self._tracker._primary_key = column_name  # Update tracker too

        # Move new PK columns to index
        # First reset current index if it has a name
        if self._data.index.name is not None or isinstance(self._data.index, pd.MultiIndex):
            self._data = self._data.reset_index()

        # Now set new PK as index
        if len(cols) == 1:
            self._data = self._data.set_index(cols[0])
        else:
            self._data = self._data.set_index(cols)  # Creates MultiIndex

        # Record operation
        self._tracker.record_operation('set_primary_key', old_pk, column_name)

    # Auto-Increment Support

    def get_next_pk_value(self) -> int:
        """
        Get the next primary key value for auto-increment.

        Returns:
            Next available PK value (current max + 1)

        Raises:
            ValueError: If PK is not auto-incrementable (composite or non-integer)
        """
        pk_cols = self._get_pk_columns()

        # Only works for single column PK
        if len(pk_cols) != 1:
            raise ValueError(
                "Auto-increment only works with single-column primary keys, "
                f"but found composite key: {pk_cols}"
            )

        pk_col = pk_cols[0]

        # Get max value from current DataFrame
        if self._data.index.name == pk_col:
            # PK is in index
            if len(self._data) == 0:
                current_max = 0
            else:
                try:
                    current_max = int(self._data.index.max())
                except (ValueError, TypeError):
                    raise ValueError(
                        f"Primary key '{pk_col}' must be integer type for auto-increment"
                    )
        elif pk_col in self._data.columns:
            # PK is in columns
            if len(self._data) == 0:
                current_max = 0
            else:
                try:
                    current_max = int(self._data[pk_col].max())
                except (ValueError, TypeError):
                    raise ValueError(
                        f"Primary key '{pk_col}' must be integer type for auto-increment"
                    )
        else:
            current_max = 0

        return current_max + 1

    # CRUD Operations

    def add_row(self, row_data: dict | None = None, validate: bool = True, auto_increment: bool = False) -> None:
        """
        Add a new row to the DataFrame with tracking.

        Args:
            row_data: Dictionary with column names as keys (PK optional if auto_increment=True)
            validate: Whether to validate primary key uniqueness
            auto_increment: If True, auto-generate PK value if missing (single integer PKs only)

        Example:
            # With explicit PK
            df.add_row({'id': 4, 'name': 'Dave'})

            # With auto-increment
            df.add_row({'name': 'Eve', 'age': 45}, auto_increment=True)
            # Auto-generates next id

        Raises:
            DataValidationError: If primary key already exists (when validate=True)
            DataValidationError: If required primary key columns are missing
            ValueError: If auto_increment used with composite or non-integer PK
        """
        from pandalchemy.exceptions import DataValidationError

        if row_data is None:
            row_data = {}

        pk_cols = self._get_pk_columns()

        # Check if PK columns are missing
        missing = [col for col in pk_cols if col not in row_data]

        if missing:
            if auto_increment and len(pk_cols) == 1:
                # Auto-generate PK value
                try:
                    next_pk = self.get_next_pk_value()
                    row_data[pk_cols[0]] = next_pk
                except ValueError as e:
                    raise DataValidationError(
                        f"Auto-increment failed: {str(e)}",
                        details={'primary_key': pk_cols[0]}
                    ) from e
            else:
                raise DataValidationError(
                    f"Row data missing required primary key column(s): {missing}",
                    details={'missing': missing, 'primary_key': pk_cols}
                )

        # Validate PK doesn't already exist
        if validate:
            if len(pk_cols) == 1:
                pk_value = row_data[pk_cols[0]]
                # Check if PK is in columns or index
                if pk_cols[0] in self._data.columns:
                    if pk_value in self._data[pk_cols[0]].values:
                        raise DataValidationError(
                            f"Primary key value {pk_value} already exists",
                            details={'primary_key': pk_cols[0], 'value': pk_value}
                        )
                elif self._data.index.name == pk_cols[0]:
                    if pk_value in self._data.index.values:
                        raise DataValidationError(
                            f"Primary key value {pk_value} already exists",
                            details={'primary_key': pk_cols[0], 'value': pk_value}
                        )
            else:
                pk_value = tuple(row_data[col] for col in pk_cols)
                condition = self._get_pk_condition(pk_value)
                if condition.any():
                    raise DataValidationError(
                        f"Primary key combination {pk_value} already exists",
                        details={'primary_key': pk_cols, 'value': pk_value}
                    )

        # Add the row
        pk_cols = self._get_pk_columns()
        self._tracker.record_operation('add_row', row_data)

        # Check if PK is in index
        if (len(pk_cols) == 1 and self._data.index.name == pk_cols[0]) or \
           (isinstance(self._data.index, pd.MultiIndex) and all(name in self._data.index.names for name in pk_cols)):
            # PK is in index - create new row with proper index
            if len(pk_cols) == 1:
                new_index = row_data[pk_cols[0]]
                row_data_without_pk = {k: v for k, v in row_data.items() if k != pk_cols[0]}
                new_row = pd.DataFrame([row_data_without_pk], index=[new_index])
                new_row.index.name = pk_cols[0]
            else:
                new_index = tuple(row_data[col] for col in pk_cols)
                row_data_without_pk = {k: v for k, v in row_data.items() if k not in pk_cols}
                new_row = pd.DataFrame([row_data_without_pk], index=[new_index])
                new_row.index = pd.MultiIndex.from_tuples([new_index], names=pk_cols)

            self._data = pd.concat([self._data, new_row])
        else:
            # PK is in columns - use ignore_index
            new_row = pd.DataFrame([row_data])
            self._data = pd.concat([self._data, new_row], ignore_index=True)

        self._tracker.compute_row_changes(self._data)

    def update_row(self, primary_key_value: Any, updates: dict) -> None:
        """
        Update a specific row identified by primary key.

        Note: Cannot update primary key values - they are immutable.
        To "change" a primary key, delete the row and insert a new one.

        Args:
            primary_key_value: Value of the primary key (single value or tuple for composite keys)
            updates: Dictionary of column: value pairs to update (CANNOT include PK columns)

        Example:
            # Single PK - OK
            df.update_row(1, {'age': 30, 'name': 'Alice'})

            # Composite PK - OK
            df.update_row(('user123', 'org456'), {'role': 'admin'})

            # Trying to update PK - ERROR
            df.update_row(1, {'id': 999})  # Raises DataValidationError

        Raises:
            ValueError: If row with primary key doesn't exist
            DataValidationError: If updates include primary key columns
        """
        from pandalchemy.exceptions import DataValidationError

        pk_cols = self._get_pk_columns()

        # VALIDATE: updates should NOT include PK columns
        pk_in_updates = [col for col in pk_cols if col in updates]
        if pk_in_updates:
            raise DataValidationError(
                f"Cannot update primary key column(s): {pk_in_updates}. "
                "Primary keys are immutable. To change a primary key, "
                "delete the row and insert a new one with the desired key.",
                details={'attempted_pk_updates': pk_in_updates, 'primary_key': pk_cols}
            )

        condition = self._get_pk_condition(primary_key_value)

        if not condition.any():
            raise ValueError(f"No row found with primary key value: {primary_key_value}")

        self._tracker.record_operation('update_row', primary_key_value, updates)

        for col, value in updates.items():
            self._data.loc[condition, col] = value

        self._tracker.compute_row_changes(self._data)

    def delete_row(self, primary_key_value: Any) -> None:
        """
        Delete a row identified by primary key.

        Args:
            primary_key_value: Value of the primary key (single value or tuple for composite keys)

        Example:
            # Single PK
            df.delete_row(1)

            # Composite PK
            df.delete_row(('user123', 'org456'))

        Raises:
            ValueError: If row with primary key doesn't exist
        """
        condition = self._get_pk_condition(primary_key_value)

        if not condition.any():
            raise ValueError(f"No row found with primary key value: {primary_key_value}")

        self._tracker.record_operation('delete_row', primary_key_value)

        # Delete the row, preserving index if PK is in index
        pk_cols = self._get_pk_columns()
        if (len(pk_cols) == 1 and self._data.index.name == pk_cols[0]) or \
           (isinstance(self._data.index, pd.MultiIndex) and all(name in self._data.index.names for name in pk_cols)):
            # PK is in index - just filter out the row, keep index intact
            self._data = self._data[~condition]
        else:
            # PK is in columns - reset index
            self._data = self._data[~condition].reset_index(drop=True)

        self._tracker.compute_row_changes(self._data)

    def upsert_row(self, row_data: dict) -> None:
        """
        Update row if exists, insert if not (based on primary key).

        Args:
            row_data: Dictionary with column names as keys (must include all primary key columns)

        Raises:
            DataValidationError: If any primary key columns are missing
        """
        from pandalchemy.exceptions import DataValidationError

        pk_cols = self._get_pk_columns()

        # Check all PK columns are present
        missing = [col for col in pk_cols if col not in row_data]
        if missing:
            raise DataValidationError(
                f"Row data missing required primary key column(s): {missing}",
                details={'missing': missing, 'primary_key': pk_cols}
            )

        # Extract PK value
        if len(pk_cols) == 1:
            pk_value = row_data[pk_cols[0]]
        else:
            pk_value = tuple(row_data[col] for col in pk_cols)

        # Check if exists
        condition = self._get_pk_condition(pk_value)

        if condition.any():
            # Update existing row
            updates = {k: v for k, v in row_data.items() if k not in pk_cols}
            self.update_row(pk_value, updates)
        else:
            # Insert new row
            self.add_row(row_data, validate=False)

    def bulk_insert(self, rows: list[dict]) -> None:
        """
        Insert multiple rows efficiently.

        Args:
            rows: List of dictionaries, each representing a row

        Raises:
            DataValidationError: If any rows have missing/duplicate primary keys
        """
        from pandalchemy.exceptions import DataValidationError

        if not rows:
            return

        pk_cols = self._get_pk_columns()

        # Validate all rows have PK columns
        for i, row in enumerate(rows):
            missing = [col for col in pk_cols if col not in row]
            if missing:
                raise DataValidationError(
                    f"Row {i} missing required primary key column(s): {missing}",
                    details={'row_index': i, 'missing': missing}
                )

        # Check for duplicate PKs in the new rows
        new_df = pd.DataFrame(rows)
        if len(pk_cols) == 1:
            if new_df[pk_cols[0]].duplicated().any():
                raise DataValidationError(
                    "Bulk insert contains duplicate primary key values",
                    details={'column': pk_cols[0]}
                )
        else:
            if new_df[pk_cols].duplicated().any():
                raise DataValidationError(
                    "Bulk insert contains duplicate primary key combinations",
                    details={'columns': pk_cols}
                )

        # Check for conflicts with existing data
        if len(pk_cols) == 1:
            pk_col = pk_cols[0]
            # Check if PK is in index or columns
            if self._data.index.name == pk_col:
                existing_values = set(self._data.index.values)
            elif pk_col in self._data.columns:
                existing_values = set(self._data[pk_col].values)
            else:
                existing_values = set()

            new_values = set(new_df[pk_col].values)
            conflicts = existing_values & new_values
            if conflicts:
                raise DataValidationError(
                    f"Bulk insert contains {len(conflicts)} primary key(s) that already exist",
                    details={'conflicts': list(conflicts)[:10]}  # Show first 10
                )

        self._tracker.record_operation('bulk_insert', len(rows))

        # Handle concat based on whether PK is in index
        if (len(pk_cols) == 1 and self._data.index.name == pk_cols[0]) or \
           (isinstance(self._data.index, pd.MultiIndex) and all(name in self._data.index.names for name in pk_cols)):
            # PK is in index - set new rows with proper index
            if len(pk_cols) == 1:
                new_df_indexed = new_df.set_index(pk_cols[0])
            else:
                new_df_indexed = new_df.set_index(pk_cols)
            self._data = pd.concat([self._data, new_df_indexed])
        else:
            # PK is in columns
            self._data = pd.concat([self._data, new_df], ignore_index=True)

        self._tracker.compute_row_changes(self._data)

    # Query Operations

    def get_row(self, primary_key_value: Any) -> dict | None:
        """
        Get a row by primary key value.

        Args:
            primary_key_value: Value of the primary key

        Returns:
            Dictionary of column: value pairs, or None if not found
        """
        condition = self._get_pk_condition(primary_key_value)

        if not condition.any():
            return None

        row = self._data[condition].iloc[0]
        return row.to_dict()

    def row_exists(self, primary_key_value: Any) -> bool:
        """
        Check if a row with the given primary key exists.

        Args:
            primary_key_value: Value of the primary key

        Returns:
            True if row exists, False otherwise
        """
        condition = self._get_pk_condition(primary_key_value)
        return condition.any()

    # Schema Operations

    def add_column_with_default(
        self,
        name: str,
        default_value: Any,
        dtype: type | None = None
    ) -> None:
        """
        Add a new column with a default value.

        Args:
            name: Column name
            default_value: Value to set for all rows
            dtype: Optional data type to cast to

        Raises:
            SchemaError: If column already exists
        """
        from pandalchemy.exceptions import SchemaError

        if name in self._data.columns:
            raise SchemaError(
                f"Column '{name}' already exists",
                details={'column': name}
            )

        self._tracker.record_operation('add_column_with_default', name, default_value, dtype)
        self._data[name] = default_value

        if dtype is not None:
            self._data[name] = self._data[name].astype(dtype)

        self._tracker.track_column_addition(name)

    def drop_column_safe(self, name: str) -> None:
        """
        Drop a column with automatic tracking.

        Args:
            name: Column name to drop

        Raises:
            SchemaError: If column doesn't exist
        """
        from pandalchemy.exceptions import SchemaError

        if name not in self._data.columns:
            raise SchemaError(
                f"Column '{name}' does not exist",
                details={'column': name}
            )

        self._tracker.record_operation('drop_column_safe', name)
        self._data = self._data.drop(columns=[name])
        self._tracker.track_column_drop(name)

    def rename_column_safe(self, old_name: str, new_name: str) -> None:
        """
        Rename a column with automatic tracking.

        Args:
            old_name: Current column name
            new_name: New column name

        Raises:
            SchemaError: If old column doesn't exist or new name already exists
        """
        from pandalchemy.exceptions import SchemaError

        if old_name not in self._data.columns:
            raise SchemaError(
                f"Column '{old_name}' does not exist",
                details={'column': old_name}
            )

        if new_name in self._data.columns:
            raise SchemaError(
                f"Column '{new_name}' already exists",
                details={'column': new_name}
            )

        self._tracker.record_operation('rename_column_safe', old_name, new_name)
        self._data = self._data.rename(columns={old_name: new_name})
        self._tracker.track_column_rename(old_name, new_name)

        # Update primary key if renamed
        if isinstance(self._primary_key, str) and self._primary_key == old_name:
            self._primary_key = new_name
            self._tracker._primary_key = new_name
        elif isinstance(self._primary_key, list) and old_name in self._primary_key:
            pk_list = list(self._primary_key)
            pk_list[pk_list.index(old_name)] = new_name
            self._primary_key = pk_list
            self._tracker._primary_key = pk_list

    def change_column_type(self, column: str, new_type: type) -> None:
        """
        Change the data type of a column.

        Args:
            column: Column name
            new_type: Target data type (int, float, str, etc.)

        Raises:
            SchemaError: If column doesn't exist
            ValueError: If type conversion fails
        """
        from pandalchemy.exceptions import SchemaError

        if column not in self._data.columns:
            raise SchemaError(
                f"Column '{column}' does not exist",
                details={'column': column}
            )

        self._tracker.record_operation('change_column_type', column, new_type)

        try:
            self._data[column] = self._data[column].astype(new_type)
        except Exception as e:
            raise ValueError(
                f"Failed to convert column '{column}' to type {new_type.__name__}: {str(e)}"
            ) from e


class TrackedLocIndexer:
    """Wrapper for loc indexer with change tracking."""

    def __init__(self, parent: TrackedDataFrame):
        self.parent = parent
        self.indexer = parent._data.loc

    def __getitem__(self, key):
        result = self.indexer[key]
        # Return NEW independent TrackedDataFrame for DataFrame results
        if isinstance(result, DataFrame):
            return TrackedDataFrame(result, self.parent._primary_key, tracker=None)
        return result

    def __setitem__(self, key, value):
        self.parent._tracker.record_operation('loc.__setitem__', key, value)
        self.indexer[key] = value
        self.parent._tracker.compute_row_changes(self.parent._data)


class TrackedIlocIndexer:
    """Wrapper for iloc indexer with change tracking."""

    def __init__(self, parent: TrackedDataFrame):
        self.parent = parent
        self.indexer = parent._data.iloc

    def __getitem__(self, key):
        result = self.indexer[key]
        # Return NEW independent TrackedDataFrame for DataFrame results
        if isinstance(result, DataFrame):
            return TrackedDataFrame(result, self.parent._primary_key, tracker=None)
        return result

    def __setitem__(self, key, value):
        self.parent._tracker.record_operation('iloc.__setitem__', key, value)
        self.indexer[key] = value
        self.parent._tracker.compute_row_changes(self.parent._data)


class TrackedAtIndexer:
    """Wrapper for at indexer with change tracking."""

    def __init__(self, parent: TrackedDataFrame):
        self.parent = parent
        self.indexer = parent._data.at

    def __getitem__(self, key):
        return self.indexer[key]

    def __setitem__(self, key, value):
        self.parent._tracker.record_operation('at.__setitem__', key, value)
        self.indexer[key] = value
        self.parent._tracker.compute_row_changes(self.parent._data)


class TrackedIatIndexer:
    """Wrapper for iat indexer with change tracking."""

    def __init__(self, parent: TrackedDataFrame):
        self.parent = parent
        self.indexer = parent._data.iat

    def __getitem__(self, key):
        return self.indexer[key]

    def __setitem__(self, key, value):
        self.parent._tracker.record_operation('iat.__setitem__', key, value)
        self.indexer[key] = value
        self.parent._tracker.compute_row_changes(self.parent._data)

