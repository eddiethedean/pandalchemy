"""
Tracked DataFrame wrapper for monitoring changes.

This module provides the TrackedDataFrame class that wraps a pandas DataFrame
and intercepts all modification operations to track changes.
"""

from __future__ import annotations

from typing import Any

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
        """Set the index of the DataFrame with tracking."""
        self._tracker.record_operation('set_index', value)
        self._data.index = value
        self._tracker.compute_row_changes(self._data)

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

