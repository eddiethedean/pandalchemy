"""
Change tracking module for monitoring DataFrame modifications.

This module provides the ChangeTracker class that monitors both operation-level
and row-level changes to pandas DataFrames for efficient SQL synchronization.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd


class ChangeType(Enum):
    """Types of changes that can be tracked."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    COLUMN_ADD = "column_add"
    COLUMN_DROP = "column_drop"
    COLUMN_RENAME = "column_rename"


@dataclass
class Operation:
    """Represents a single operation performed on the DataFrame."""
    operation_type: str
    method_name: str
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=lambda: pd.Timestamp.now().timestamp())


@dataclass
class RowChange:
    """Represents a change to a specific row."""
    change_type: ChangeType
    primary_key_value: Any
    old_data: dict[str, Any] | None = None
    new_data: dict[str, Any] | None = None


class ChangeTracker:
    """
    Tracks changes made to a DataFrame at both operation and row levels.

    This class monitors modifications to a pandas DataFrame and maintains
    a record of all changes for generating optimized SQL execution plans.

    Attributes:
        primary_key: The name of the primary key column
        operations: List of operations performed on the DataFrame
        row_changes: Dictionary mapping primary key values to row changes
        added_columns: Set of column names that were added
        dropped_columns: Set of column names that were dropped
        renamed_columns: Dictionary mapping old column names to new ones
    """

    def __init__(self, primary_key: str | list[str], original_data: pd.DataFrame):
        """
        Initialize the ChangeTracker.

        Args:
            primary_key: The name of the primary key column(s) - single string or list for composite
            original_data: The original DataFrame state for comparison
        """
        self.primary_key = primary_key
        self.original_data = original_data.copy()
        self.original_columns = set(original_data.columns)

        # Operation-level tracking
        self.operations: list[Operation] = []

        # Row-level tracking
        self.row_changes: dict[Any, RowChange] = {}

        # Column-level tracking
        self.added_columns: set[str] = set()
        self.dropped_columns: set[str] = set()
        self.renamed_columns: dict[str, str] = {}
        self.altered_column_types: dict[str, type] = {}  # column_name -> new_type

        # Track original index for row identification
        self.original_index = self._extract_original_index(original_data)

    def _extract_original_index(self, data: pd.DataFrame) -> set[Any]:
        """
        Extract primary key values from DataFrame.

        Handles both single column and composite primary keys.
        Handles primary keys in both columns and index.

        Args:
            data: DataFrame to extract PK values from

        Returns:
            Set of primary key values (tuples for composite keys)
        """
        from pandalchemy.pk_utils import extract_pk_values
        return extract_pk_values(data, self.primary_key)

    def record_operation(self, method_name: str, *args, **kwargs) -> None:
        """
        Record an operation performed on the DataFrame.

        Args:
            method_name: Name of the method called
            *args: Positional arguments passed to the method
            **kwargs: Keyword arguments passed to the method
        """
        operation = Operation(
            operation_type="method_call",
            method_name=method_name,
            args=args,
            kwargs=kwargs
        )
        self.operations.append(operation)

    def track_column_addition(self, column_name: str) -> None:
        """
        Track the addition of a new column.

        Args:
            column_name: Name of the added column
        """
        if column_name not in self.original_columns:
            self.added_columns.add(column_name)
            # Remove from dropped if it was previously dropped and re-added
            self.dropped_columns.discard(column_name)

    def track_column_drop(self, column_name: str) -> None:
        """
        Track the removal of a column.

        Args:
            column_name: Name of the dropped column
        """
        if column_name in self.original_columns:
            self.dropped_columns.add(column_name)
            # Remove from added if it was added and then dropped
            self.added_columns.discard(column_name)
        else:
            # Column was added in this session and then dropped
            self.added_columns.discard(column_name)

    def track_column_rename(self, old_name: str, new_name: str) -> None:
        """
        Track the renaming of a column.

        Args:
            old_name: Original column name
            new_name: New column name
        """
        self.renamed_columns[old_name] = new_name

    def track_column_type_change(self, column_name: str, new_type: type) -> None:
        """
        Track a column type change.

        Args:
            column_name: Name of the column
            new_type: New data type for the column
        """
        self.altered_column_types[column_name] = new_type

    def compute_row_changes(self, current_data: pd.DataFrame) -> None:
        """
        Compute row-level changes by comparing current data to original.

        Args:
            current_data: The current state of the DataFrame
        """
        self.row_changes.clear()

        # Handle both single and composite primary keys
        if isinstance(self.primary_key, str):
            # Single column primary key
            if self.primary_key in current_data.columns:
                current_keys = set(current_data[self.primary_key].values)
                current_df = current_data.set_index(self.primary_key)
            elif current_data.index.name == self.primary_key:
                current_keys = set(current_data.index.values)
                current_df = current_data
            else:
                # No primary key available, can't track row changes
                return

            # Prepare original data
            if self.primary_key in self.original_data.columns:
                original_df = self.original_data.set_index(self.primary_key)
            elif self.original_data.index.name == self.primary_key:
                original_df = self.original_data
            else:
                return

            original_keys = set(original_df.index.values)
        else:
            # Composite primary key
            pk_cols = list(self.primary_key)

            # Check if PK is in columns or MultiIndex
            if all(col in current_data.columns for col in pk_cols):
                # PK in columns
                current_keys = {tuple(row) for row in current_data[pk_cols].values}
                current_df = current_data.set_index(pk_cols)
            elif isinstance(current_data.index, pd.MultiIndex) and \
                 all(name in current_data.index.names for name in pk_cols):
                # PK in MultiIndex
                current_keys = set(current_data.index.values)
                current_df = current_data
            else:
                # Primary key columns missing, can't track row changes
                return

            # Prepare original data
            if all(col in self.original_data.columns for col in pk_cols):
                # PK in columns
                original_df = self.original_data.set_index(pk_cols)
                original_keys = {tuple(row) for row in self.original_data[pk_cols].values}
            elif isinstance(self.original_data.index, pd.MultiIndex) and \
                 all(name in self.original_data.index.names for name in pk_cols):
                # PK in MultiIndex
                original_df = self.original_data
                original_keys = set(self.original_data.index.values)
            else:
                return

        # Find inserts: keys in current but not in original
        inserted_keys = current_keys - original_keys
        for key in inserted_keys:
            row_data = current_df.loc[key].to_dict()
            self.row_changes[key] = RowChange(
                change_type=ChangeType.INSERT,
                primary_key_value=key,
                new_data=row_data
            )

        # Find deletes: keys in original but not in current
        deleted_keys = original_keys - current_keys
        for key in deleted_keys:
            row_data = original_df.loc[key].to_dict()
            self.row_changes[key] = RowChange(
                change_type=ChangeType.DELETE,
                primary_key_value=key,
                old_data=row_data
            )

        # Find updates: keys in both, but with different values
        common_keys = current_keys & original_keys
        for key in common_keys:
            # Compare rows, considering only common columns
            common_cols = list(set(current_df.columns) & set(original_df.columns))
            if not common_cols:
                continue

            current_row = current_df.loc[key, common_cols]
            original_row = original_df.loc[key, common_cols]

            # Check if any values differ
            try:
                if not current_row.equals(original_row):
                    # Handle NaN comparisons
                    is_different = False
                    for col in common_cols:
                        curr_val = current_row[col]
                        orig_val = original_row[col]

                        # Check if both are NaN
                        if pd.isna(curr_val) and pd.isna(orig_val):  # type: ignore[unreachable]
                            continue  # type: ignore[unreachable]
                        # Check if values differ
                        if curr_val != orig_val:  # type: ignore[unreachable]
                            is_different = True
                            break

                    if is_different:
                        self.row_changes[key] = RowChange(
                            change_type=ChangeType.UPDATE,
                            primary_key_value=key,
                            old_data=original_row.to_dict(),  # type: ignore[arg-type]
                            new_data=current_row.to_dict()  # type: ignore[arg-type]
                        )
            except (ValueError, TypeError):
                # If comparison fails, assume they're different
                self.row_changes[key] = RowChange(
                    change_type=ChangeType.UPDATE,
                    primary_key_value=key,
                    old_data=original_row.to_dict(),  # type: ignore[arg-type]
                    new_data=current_row.to_dict()  # type: ignore[arg-type]
                )

    def get_inserts(self) -> list[RowChange]:
        """Get all row insertions."""
        return [rc for rc in self.row_changes.values()
                if rc.change_type == ChangeType.INSERT]

    def get_updates(self) -> list[RowChange]:
        """Get all row updates."""
        return [rc for rc in self.row_changes.values()
                if rc.change_type == ChangeType.UPDATE]

    def get_deletes(self) -> list[RowChange]:
        """Get all row deletions."""
        return [rc for rc in self.row_changes.values()
                if rc.change_type == ChangeType.DELETE]

    def has_changes(self) -> bool:
        """
        Check if any changes have been tracked.

        Returns:
            True if there are any changes, False otherwise
        """
        return (
            len(self.row_changes) > 0 or
            len(self.added_columns) > 0 or
            len(self.dropped_columns) > 0 or
            len(self.renamed_columns) > 0 or
            len(self.altered_column_types) > 0
        )

    def reset(self, new_data: pd.DataFrame) -> None:
        """
        Reset the tracker with new original data.

        Args:
            new_data: The new baseline DataFrame
        """
        self.original_data = new_data.copy()
        self.original_columns = set(new_data.columns)
        self.operations.clear()
        self.row_changes.clear()
        self.added_columns.clear()
        self.dropped_columns.clear()
        self.renamed_columns.clear()
        self.altered_column_types.clear()

        # Use same extraction logic as __init__ - handles both single and composite keys
        self.original_index = self._extract_original_index(new_data)

    def get_summary(self) -> dict[str, Any]:
        """
        Get a summary of all tracked changes.

        Returns:
            Dictionary containing change statistics
        """
        return {
            "total_operations": len(self.operations),
            "inserts": len(self.get_inserts()),
            "updates": len(self.get_updates()),
            "deletes": len(self.get_deletes()),
            "columns_added": len(self.added_columns),
            "columns_dropped": len(self.dropped_columns),
            "columns_renamed": len(self.renamed_columns),
            "columns_type_changed": len(self.altered_column_types),
            "has_changes": self.has_changes()
        }

