"""
TableDataFrame - pandas DataFrame with change tracking and database synchronization.

This module provides the TableDataFrame class that combines a pandas DataFrame interface
with automatic change tracking and database I/O operations.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Engine

from pandalchemy.change_tracker import ChangeTracker


class TableDataFrame:
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
    TableDataFrame instances with independent change trackers. This means
    modifications to the returned DataFrame won't affect the original's tracker.

    Example:
        >>> tdf = TableDataFrame(df, 'id')
        >>> subset = tdf.head(10)  # Independent tracker
        >>> subset['age'] = 100    # Only tracked in subset
        >>> tdf.get_tracker().has_changes()  # False - original unchanged

    Attributes:
        _data: The underlying pandas DataFrame
        _tracker: The ChangeTracker instance monitoring changes
        _primary_key: Name of the primary key column
    """

    # Type annotations for mypy
    _data: DataFrame
    _primary_key: str | list[str]
    _tracker: ChangeTracker

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

    def __init__(
        self,
        name: str | DataFrame | None = None,
        data: DataFrame | None = None,
        primary_key: str | list[str] | None = None,
        engine: Engine | None = None,
        db: Any = None,
        schema: str | None = None,
        auto_increment: bool = False,
        tracker: ChangeTracker | None = None
    ):
        """
        Initialize a TableDataFrame.

        Can be called in two ways:
        1. As a database table: TableDataFrame('users', df, 'id', engine)
        2. As standalone: TableDataFrame(data=df, primary_key='id')

        Args:
            name: Table name (first param for Table compatibility) OR DataFrame if used standalone
            data: pandas DataFrame with table data
            primary_key: Name of the primary key column(s) - single string or list for composite
            engine: SQLAlchemy engine for database connection
            db: Parent DataBase object
            schema: Optional schema name
            auto_increment: If True, enable auto-increment for primary key
            tracker: Optional existing ChangeTracker, creates new one if None
        """
        from pandalchemy.sql_operations import get_primary_key, pull_table, table_exists

        # Handle dual calling patterns
        # Pattern 1: TableDataFrame(data=df, primary_key='id') - standalone
        # Pattern 2: TableDataFrame('users', df, 'id', engine) - database table

        if isinstance(name, DataFrame):
            # Standalone mode: first param is actually the DataFrame
            data = name
            name = None

        # Store database-related attributes (use object.__setattr__ to avoid tracker during init)
        object.__setattr__(self, 'name', name)
        object.__setattr__(self, 'engine', engine)
        object.__setattr__(self, 'schema', schema)
        object.__setattr__(self, 'db', db)
        object.__setattr__(self, 'auto_increment', auto_increment)
        object.__setattr__(self, 'key', primary_key)

        # Handle case where name is a table name and we should pull from database
        if isinstance(name, str) and engine is not None and data is None:
            # Database table mode - pull from database
            if table_exists(engine, name, schema):
                # Get primary key if not specified
                if primary_key is None:
                    primary_key = get_primary_key(engine, name, schema)
                    if primary_key is None:
                        primary_key = 'id'
                object.__setattr__(self, '_primary_key', primary_key)
                object.__setattr__(self, 'key', primary_key)

                # Pull data from database
                data = pull_table(engine, name, schema, primary_key=primary_key, set_index=True)
            else:
                # Table doesn't exist yet
                data = pd.DataFrame()
                if primary_key is None:
                    primary_key = 'id'
                object.__setattr__(self, '_primary_key', primary_key)
                object.__setattr__(self, 'key', primary_key)
        else:
            # Data is provided (either as DataFrame or needs to be created)
            if data is None:
                data = pd.DataFrame()

            if primary_key is None:
                # Try to infer from index
                if isinstance(data.index, pd.MultiIndex):
                    primary_key = [str(name) for name in data.index.names if name is not None] or ['id']  # type: ignore[misc]
                elif data.index.name:
                    primary_key = str(data.index.name)  # type: ignore[assignment]
                else:
                    primary_key = 'id'

            object.__setattr__(self, '_primary_key', primary_key)
            object.__setattr__(self, 'key', primary_key)

            # Prepare primary key (handles index naming and validation)
            from pandalchemy.utils import prepare_primary_key_for_table_creation
            data = prepare_primary_key_for_table_creation(data, primary_key)

            # Ensure PK is set as index if not already
            if isinstance(primary_key, list):
                # Composite key
                if not isinstance(data.index, pd.MultiIndex) or list(data.index.names) != primary_key:
                    if all(col in data.columns for col in primary_key):
                        data = data.set_index(primary_key)
            else:
                # Single key
                if data.index.name != primary_key and primary_key in data.columns:
                    data = data.set_index(primary_key)

        object.__setattr__(self, '_data', data)

        if tracker is None:
            tracker = ChangeTracker(self._primary_key, data)
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
                    # Return NEW TableDataFrame with INDEPENDENT tracker
                    if isinstance(result, DataFrame):
                        return TableDataFrame(data=result, primary_key=self._primary_key, tracker=None)
                    return result

            return wrapped_method
        elif name in self._RETURNING_METHODS:
            # Wrap returning methods to wrap DataFrame results with INDEPENDENT trackers
            def wrapped_returning_method(*args, **kwargs):
                result = attr(*args, **kwargs)
                # Wrap DataFrame results with NEW independent tracker
                if isinstance(result, DataFrame):
                    return TableDataFrame(data=result, primary_key=self._primary_key, tracker=None)
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

        Returns a new TableDataFrame with independent tracker if result is a DataFrame.
        """
        result = self._data[key]

        # If result is a DataFrame, wrap it with NEW independent tracker
        if isinstance(result, DataFrame):
            return TableDataFrame(data=result, primary_key=self._primary_key, tracker=None)

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
        return f"TableDataFrame(\n{repr(self._data)}\n)"

    def __str__(self) -> str:
        """Return string representation."""
        return str(self._data)

    def _repr_html_(self) -> str:
        """Return HTML representation for Jupyter notebooks."""
        return self._data._repr_html_()  # type: ignore[operator]

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

    def copy(self, deep: bool = True) -> TableDataFrame:
        """
        Create a copy of the TableDataFrame.

        Args:
            deep: Whether to make a deep copy

        Returns:
            New TableDataFrame instance
        """
        new_data = self._data.copy(deep=deep)
        new_tracker = ChangeTracker(self._primary_key, new_data)
        return TableDataFrame(
            name=self.name,
            data=new_data,
            primary_key=self._primary_key,
            engine=self.engine,
            db=self.db,
            schema=self.schema,
            auto_increment=self.auto_increment,
            tracker=new_tracker
        )

    # ============================================================================
    # SQL Helper Methods
    # ============================================================================

    def _get_pk_columns(self) -> list[str]:
        """
        Get primary key columns as a list.

        Returns:
            List of primary key column names
        """
        from pandalchemy.pk_utils import normalize_primary_key
        return normalize_primary_key(self._primary_key)

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
                if self._data.index.isnull().any():  # type: ignore[operator]
                    raise DataValidationError(
                        f"Primary key '{col}' in index contains null values",
                        details={'column': col}
                    )
            elif isinstance(self._data.index, pd.MultiIndex) and col in self._data.index.names:
                # MultiIndex - check specific level
                level_idx = self._data.index.names.index(col)
                if self._data.index.get_level_values(level_idx).isnull().any():  # type: ignore[operator]
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

    def _validate_pk_integrity(
        self,
        pk_cols: list[str],
        raise_on_error: bool = False
    ) -> list[str]:
        """
        Validate primary key integrity (nulls and duplicates).

        Checks PK columns in both DataFrame columns and index.

        Args:
            pk_cols: List of primary key column names
            raise_on_error: If True, raise DataValidationError; if False, return error list

        Returns:
            List of error messages (empty if valid)

        Raises:
            DataValidationError: If raise_on_error=True and validation fails
        """
        from pandalchemy.exceptions import DataValidationError

        errors = []

        # Check for nulls in PK
        for col in pk_cols:
            if col in self._data.columns and self._data[col].isnull().any():
                error_msg = f"Primary key column '{col}' contains null values"
                if raise_on_error:
                    raise DataValidationError(error_msg, details={'column': col})
                errors.append(error_msg)
            elif self._data.index.name == col:
                # Single index
                if self._data.index.isnull().any():  # type: ignore[operator]
                    error_msg = f"Primary key '{col}' in index contains null values"
                    if raise_on_error:
                        raise DataValidationError(error_msg, details={'column': col})
                    errors.append(error_msg)
            elif isinstance(self._data.index, pd.MultiIndex) and col in self._data.index.names:
                # MultiIndex - check specific level
                level_idx = self._data.index.names.index(col)
                if self._data.index.get_level_values(level_idx).isnull().any():  # type: ignore[operator]
                    error_msg = f"Primary key '{col}' in index contains null values"
                    if raise_on_error:
                        raise DataValidationError(error_msg, details={'column': col})
                    errors.append(error_msg)

        # Check uniqueness
        if len(pk_cols) == 1:
            col = pk_cols[0]
            if col in self._data.columns:
                if self._data[col].duplicated().any():
                    error_msg = f"Column '{col}' contains duplicate values" if raise_on_error else f"Primary key column '{col}' contains duplicates"
                    if raise_on_error:
                        raise DataValidationError(error_msg, details={'column': col})
                    errors.append(error_msg)
            elif self._data.index.name == col:
                if self._data.index.duplicated().any():
                    error_msg = f"Primary key in index '{col}' contains duplicates"
                    if raise_on_error:
                        raise DataValidationError(error_msg, details={'column': col})
                    errors.append(error_msg)
        else:
            # Composite key
            if all(col in self._data.columns for col in pk_cols):
                if self._data[pk_cols].duplicated().any():
                    error_msg = f"Column combination {pk_cols} contains duplicate values" if raise_on_error else f"Primary key combination {pk_cols} contains duplicates"
                    if raise_on_error:
                        raise DataValidationError(error_msg, details={'columns': pk_cols})
                    errors.append(error_msg)
            elif isinstance(self._data.index, pd.MultiIndex) and all(col in self._data.index.names for col in pk_cols):
                if self._data.index.duplicated().any():
                    error_msg = f"Primary key combination in index {pk_cols} contains duplicates"
                    if raise_on_error:
                        raise DataValidationError(error_msg, details={'columns': pk_cols})
                    errors.append(error_msg)

        return errors

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

        # Use consolidated validation for nulls and duplicates
        pk_errors = self._validate_pk_integrity(pk_cols, raise_on_error=False)
        errors.extend(pk_errors)

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
        from pandalchemy.exceptions import SchemaError

        # Normalize to list
        cols = [column_name] if isinstance(column_name, str) else list(column_name)

        # Check columns exist
        missing = [col for col in cols if col not in self._data.columns]
        if missing:
            raise SchemaError(
                f"Cannot set primary key: column(s) {missing} do not exist",
                details={'columns': missing}
            )

        # Validate uniqueness if requested - use consolidated validation
        if validate:
            self._validate_pk_integrity(cols, raise_on_error=True)

        # Update primary key
        old_pk = self._primary_key
        self._primary_key = column_name
        self._tracker.primary_key = column_name  # Update tracker too

        # Move new PK columns to index
        from pandalchemy.pk_utils import set_pk_as_index

        # First reset current index if it has a name
        if self._data.index.name is not None or isinstance(self._data.index, pd.MultiIndex):
            self._data = self._data.reset_index()

        # Now set new PK as index
        self._data = set_pk_as_index(self._data, cols)

        # Record operation
        self._tracker.record_operation('set_primary_key', old_pk, column_name)

    # Auto-Increment Support

    def get_next_pk_value(self) -> int:
        """
        Get the next primary key value for auto-increment.

        For database-backed tables, queries both local and database to find the
        highest value. For standalone tables, uses only local data.

        Returns:
            Next available PK value (max + 1)

        Raises:
            ValueError: If PK is not auto-incrementable (composite or non-integer)
            ValueError: If table not configured for auto-increment (database-backed only)
        """
        # For database-backed tables, check auto_increment flag
        if self.engine is not None and not self.auto_increment:
            raise ValueError(
                f"Table '{self.name}' is not configured for auto-increment. "
                "Set auto_increment=True when creating the table."
            )

        # Delegate to _generate_next_pk which handles both local and DB queries
        return self._generate_next_pk()

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
                    next_pk = self._generate_next_pk()
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

        # Check for null values in PK
        for pk_col in pk_cols:
            if row_data.get(pk_col) is None or pd.isna(row_data.get(pk_col)):
                raise DataValidationError(
                    f"Primary key column '{pk_col}' cannot be null",
                    details={'primary_key': pk_col, 'row_data': row_data}
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

    def update_where(
        self,
        condition: pd.Series | Any,
        updates: dict[str, Any] | Any,
        column: str | None = None
    ) -> None:
        """
        Update rows matching a condition - similar to SQL UPDATE...WHERE.

        This provides a more intuitive syntax for conditional updates compared to
        using loc directly. Supports updating multiple columns with values or functions.

        Args:
            condition: Boolean Series or array indicating which rows to update
            updates: Dictionary of {column: value_or_function} or a single value if column specified
            column: Optional column name for shorthand syntax (when updates is a single value)

        Examples:
            # Single column with lambda
            df.update_where(df['age'] > 30, {'age': lambda x: x + 1})

            # Multiple columns
            df.update_where(df['active'] == True, {
                'last_seen': datetime.now(),
                'login_count': lambda x: x + 1
            })

            # Simple value assignment
            df.update_where(df['status'] == 'pending', {'status': 'approved'})

            # Shorthand for single column
            df.update_where(df['age'] > 30, 31, column='age')

            # Increment multiple numeric columns
            df.update_where(df['tier'] == 'gold', {
                'discount': lambda x: x + 5,
                'points': lambda x: x * 1.1
            })

        Raises:
            DataValidationError: If trying to update primary key columns
            ValueError: If column doesn't exist or invalid parameters

        Note:
            This is a convenience method that internally uses loc for updates.
            All changes are tracked automatically.
        """
        from pandalchemy.exceptions import DataValidationError

        # Handle shorthand syntax: update_where(condition, value, column='col')
        if column is not None:
            if isinstance(updates, dict):
                raise ValueError("When 'column' is specified, 'updates' should be a single value, not a dict")
            updates = {column: updates}

        # Validate updates is a dictionary
        if not isinstance(updates, dict):
            raise ValueError("'updates' must be a dictionary of {column: value_or_function} or a single value with column parameter")

        # Get PK columns for validation
        pk_cols = self._get_pk_columns()

        # Validate no PK columns in updates
        pk_in_updates = [col for col in pk_cols if col in updates]
        if pk_in_updates:
            raise DataValidationError(
                f"Cannot update primary key column(s): {pk_in_updates}. "
                "Primary keys are immutable. To change a primary key, "
                "delete the row and insert a new one with the desired key.",
                details={'attempted_pk_updates': pk_in_updates, 'primary_key': pk_cols}
            )

        # Validate all columns exist
        for col in updates:
            if col not in self._data.columns:
                raise ValueError(f"Column '{col}' does not exist in DataFrame")

        # Record operation
        self._tracker.record_operation('update_where', condition, updates)

        # Apply each update
        for col, value in updates.items():
            if callable(value):
                # Apply function to existing values
                self._data.loc[condition, col] = self._data.loc[condition, col].apply(value)
            else:
                # Direct assignment
                self._data.loc[condition, col] = value

        # Compute row changes
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

    def delete_where(self, condition: pd.Series | Any) -> int:
        """
        Delete rows matching a condition - similar to SQL DELETE...WHERE.

        This provides an intuitive way to delete multiple rows at once based on
        a condition, similar to SQL DELETE FROM table WHERE condition.

        Args:
            condition: Boolean Series or array indicating which rows to delete

        Returns:
            Number of rows deleted

        Examples:
            # Delete old records
            deleted = df.delete_where(df['age'] > 65)
            print(f"Deleted {deleted} rows")

            # Delete inactive users
            deleted = df.delete_where(df['active'] == False)

            # Delete by multiple conditions
            deleted = df.delete_where((df['status'] == 'expired') & (df['last_login'] < cutoff_date))

            # Delete all rows matching criteria
            deleted = df.delete_where(df['balance'] == 0)

        Note:
            This is a convenience method that filters the DataFrame.
            All deletions are tracked automatically and will be executed
            when push() is called.
        """
        # Record operation
        self._tracker.record_operation('delete_where', condition)

        # Count rows to be deleted
        rows_to_delete = condition.sum() if hasattr(condition, 'sum') else len([x for x in condition if x])

        # Delete matching rows (keep rows where condition is False)
        pk_cols = self._get_pk_columns()
        if (len(pk_cols) == 1 and self._data.index.name == pk_cols[0]) or \
           (isinstance(self._data.index, pd.MultiIndex) and all(name in self._data.index.names for name in pk_cols)):
            # PK is in index - filter out matching rows, keep index intact
            self._data = self._data[~condition]
        else:
            # PK is in columns - reset index after filtering
            self._data = self._data[~condition].reset_index(drop=True)

        # Compute row changes
        self._tracker.compute_row_changes(self._data)

        return rows_to_delete  # type: ignore[return-value]

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
            self._tracker.primary_key = new_name
        elif isinstance(self._primary_key, list) and old_name in self._primary_key:
            pk_list = list(self._primary_key)
            pk_list[pk_list.index(old_name)] = new_name
            self._primary_key = pk_list
            self._tracker.primary_key = pk_list

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
        self._tracker.track_column_type_change(column, new_type)

        try:
            self._data[column] = self._data[column].astype(new_type)
        except Exception as e:
            raise ValueError(
                f"Failed to convert column '{column}' to type {new_type.__name__}: {str(e)}"
            ) from e

    # Database synchronization methods

    def push(self, engine: Engine | None = None, schema: str | None = None) -> None:
        """
        Push all changes to the database.

        This method generates an optimized execution plan from tracked changes
        and executes it within a transaction. All changes are rolled back if
        any operation fails.

        Args:
            engine: Optional engine to use (overrides instance engine)
            schema: Optional schema to use (overrides instance schema)

        Raises:
            SchemaError: If primary key columns have been dropped
            DataValidationError: If primary key validation fails
            SQLAlchemyError: If any database operation fails
        """
        from pandalchemy.exceptions import DataValidationError, SchemaError
        from pandalchemy.execution_plan import ExecutionPlan
        from pandalchemy.sql_operations import (
            create_table_from_dataframe,
            execute_plan,
            table_exists,
        )

        if engine is not None:
            object.__setattr__(self, 'engine', engine)
        if schema is not None:
            object.__setattr__(self, 'schema', schema)

        # Ensure engine is available
        if self.engine is None:
            raise ValueError("Cannot push without an engine")
        if self._primary_key is None:
            raise ValueError("Cannot push without a primary key")
        if self.name is None:
            raise ValueError("Cannot push without a table name")

        # VALIDATE PRIMARY KEY BEFORE PUSH
        try:
            self.validate_primary_key()
        except (SchemaError, DataValidationError) as e:
            raise SchemaError(
                f"Cannot push table '{self.name}': {str(e)}",
                details={
                    'table': self.name,
                    'primary_key': self._primary_key,
                    'error': str(e)
                }
            ) from e

        # Get the current pandas DataFrame
        current_df = self.to_pandas()

        # Build execution plan
        plan = ExecutionPlan(self._tracker, current_df)

        # Check if table exists
        if not table_exists(self.engine, self.name, self.schema):
            # Create new table
            create_table_from_dataframe(
                self.engine,
                self.name,
                current_df,
                self._primary_key,
                self.schema,
                if_exists='fail'
            )
        else:
            # Execute the plan to update existing table
            execute_plan(
                self.engine,
                self.name,
                plan,
                self.schema,
                self._primary_key
            )

        # Refresh the table after successful push
        self.pull()

    def _push_with_connection(self, connection: Any) -> None:
        """
        Internal method to push changes using an existing connection.

        Used by DataBase.push() to execute multiple table pushes in one transaction.

        Args:
            connection: SQLAlchemy connection within a transaction

        Raises:
            SchemaError: If primary key columns have been dropped
            DataValidationError: If primary key validation fails
        """
        from pandalchemy.exceptions import DataValidationError, SchemaError
        from pandalchemy.execution_plan import ExecutionPlan
        from pandalchemy.sql_operations import (
            create_table_from_dataframe,
            execute_plan,
            table_exists,
        )

        # Note: connection parameter reserved for future optimization
        _ = connection  # Reserved for future use  # noqa: F841

        # VALIDATE PRIMARY KEY BEFORE PUSH
        try:
            self.validate_primary_key()
        except (SchemaError, DataValidationError) as e:
            raise SchemaError(
                f"Cannot push table '{self.name}': {str(e)}",
                details={
                    'table': self.name,
                    'primary_key': self._primary_key,
                    'error': str(e)
                }
            ) from e

        current_df = self.to_pandas()
        plan = ExecutionPlan(self._tracker, current_df)

        # Ensure engine and key are set
        assert self.engine is not None, "Engine must be set before push"
        assert self._primary_key is not None, "Primary key must be set before push"
        assert self.name is not None, "Table name must be set before push"

        if not table_exists(self.engine, self.name, self.schema):
            # Create new table
            create_table_from_dataframe(
                self.engine,
                self.name,
                current_df,
                self._primary_key,
                self.schema,
                if_exists='fail'
            )
        else:
            # Execute plan
            execute_plan(
                self.engine,
                self.name,
                plan,
                self.schema,
                self._primary_key
            )

    def pull(self, engine: Engine | None = None, schema: str | None = None) -> None:
        """
        Refresh the table with current database data.

        Args:
            engine: Optional engine to use (overrides instance engine)
            schema: Optional schema to use (overrides instance schema)
        """
        from pandalchemy.sql_operations import get_primary_key, pull_table

        if engine is not None:
            object.__setattr__(self, 'engine', engine)
        if schema is not None:
            object.__setattr__(self, 'schema', schema)

        # Re-initialize from database
        if self.engine and self.name:
            # Get primary key if not set
            if self._primary_key is None:  # type: ignore[unreachable]
                pk = get_primary_key(self.engine, self.name, self.schema)  # type: ignore[unreachable]
                if pk:  # type: ignore[unreachable]
                    object.__setattr__(self, '_primary_key', pk)  # type: ignore[unreachable]
                    object.__setattr__(self, 'key', pk)

            # Pull fresh data
            data = pull_table(
                self.engine,
                self.name,
                self.schema,
                primary_key=self._primary_key,
                set_index=True
            )
            object.__setattr__(self, '_data', data)

            # Reset tracker with fresh data
            tracker = ChangeTracker(self._primary_key, data)
            object.__setattr__(self, '_tracker', tracker)

    def _generate_next_pk(self) -> int:
        """
        Internal method to generate next primary key value.

        Used by both get_next_pk_value() and add_row(auto_increment=True).

        Returns:
            Next available PK value

        Raises:
            ValueError: If PK is not suitable for auto-increment
        """

        from pandalchemy.sql_operations import table_exists

        # Validate PK is suitable for auto-increment
        if isinstance(self._primary_key, list):
            raise ValueError("Auto-increment only works with single-column primary keys")

        # Get max from local data
        local_max = 0
        if len(self._data) > 0:
            try:
                if self._data.index.name == self._primary_key:
                    # Check if PK is integer type
                    if not pd.api.types.is_integer_dtype(self._data.index.dtype):
                        raise ValueError("Primary key must be integer type for auto-increment")
                    local_max = int(self._data.index.max())
                elif self._primary_key in self._data.columns:
                    # Check if PK is integer type
                    if not pd.api.types.is_integer_dtype(self._data[self._primary_key].dtype):
                        raise ValueError("Primary key must be integer type for auto-increment")
                    local_max = int(self._data[self._primary_key].max())
            except (ValueError, TypeError) as e:
                # If it's a type error from int conversion, raise proper message
                if "Primary key must be integer type" in str(e):
                    raise
                raise ValueError("Primary key must be integer type for auto-increment") from e

        # Get max from database (if table exists and we have engine)
        db_max = 0
        if self.engine and self.name and table_exists(self.engine, self.name, self.schema):
            try:
                with self.engine.connect() as conn:
                    # Build query safely using SQLAlchemy (prevents SQL injection)
                    from sqlalchemy import column, func, select
                    from sqlalchemy import table as sa_table
                    from sqlalchemy.sql.elements import ColumnClause

                    table_ref = sa_table(self.name, schema=self.schema)
                    pk_column: ColumnClause[Any] = column(self._primary_key)
                    query = select(func.max(pk_column).label('max_id')).select_from(table_ref)
                    result = conn.execute(query).fetchone()
                    if result and result[0] is not None:
                        db_max = int(result[0])
            except Exception as e:
                # If query fails, use local max
                import warnings
                warnings.warn(
                    f"Failed to query database for max PK value in table '{self.name}', "
                    f"using local max only. Error: {type(e).__name__}: {str(e)}",
                    UserWarning,
                    stacklevel=2
                )

        return max(local_max, db_max) + 1


class TrackedLocIndexer:
    """Wrapper for loc indexer with change tracking."""

    def __init__(self, parent: TableDataFrame):
        self.parent = parent
        self.indexer = parent._data.loc

    def __getitem__(self, key):
        result = self.indexer[key]
        # Return NEW independent TableDataFrame for DataFrame results
        if isinstance(result, DataFrame):
            return TableDataFrame(data=result, primary_key=self.parent._primary_key, tracker=None)
        return result

    def __setitem__(self, key, value):
        self.parent._tracker.record_operation('loc.__setitem__', key, value)
        self.indexer[key] = value
        self.parent._tracker.compute_row_changes(self.parent._data)


class TrackedIlocIndexer:
    """Wrapper for iloc indexer with change tracking."""

    def __init__(self, parent: TableDataFrame):
        self.parent = parent
        self.indexer = parent._data.iloc

    def __getitem__(self, key):
        result = self.indexer[key]
        # Return NEW independent TableDataFrame for DataFrame results
        if isinstance(result, DataFrame):
            return TableDataFrame(data=result, primary_key=self.parent._primary_key, tracker=None)
        return result

    def __setitem__(self, key, value):
        self.parent._tracker.record_operation('iloc.__setitem__', key, value)
        self.indexer[key] = value
        self.parent._tracker.compute_row_changes(self.parent._data)


class TrackedAtIndexer:
    """Wrapper for at indexer with change tracking."""

    def __init__(self, parent: TableDataFrame):
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

    def __init__(self, parent: TableDataFrame):
        self.parent = parent
        self.indexer = parent._data.iat

    def __getitem__(self, key):
        return self.indexer[key]

    def __setitem__(self, key, value):
        self.parent._tracker.record_operation('iat.__setitem__', key, value)
        self.indexer[key] = value
        self.parent._tracker.compute_row_changes(self.parent._data)

