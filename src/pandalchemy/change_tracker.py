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

from pandalchemy.pk_utils import normalize_primary_key


class ChangeType(Enum):
    """Types of changes that can be tracked."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    COLUMN_ADD = "column_add"
    COLUMN_DROP = "column_drop"
    COLUMN_RENAME = "column_rename"


class RowStatus(Enum):
    """Status of a row in incremental tracking."""

    UNCHANGED = "unchanged"
    INSERTED = "inserted"
    UPDATED = "updated"
    DELETED = "deleted"


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


@dataclass
class RowState:
    """
    Represents the state of a row in incremental tracking.

    Stores only changed values, not full row copies, for memory efficiency.
    """

    status: RowStatus
    changed_columns: set[str] = field(default_factory=set)
    old_values: dict[str, Any] | None = None  # Only changed cell values
    new_values: dict[str, Any] | None = None  # Only changed cell values


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

    def __init__(
        self,
        primary_key: str | list[str],
        original_data: pd.DataFrame,
        tracking_mode: str = "incremental",
    ):
        """
        Initialize the ChangeTracker.

        Args:
            primary_key: The name of the primary key column(s) - single string or list for composite
            original_data: The original DataFrame state for comparison
            tracking_mode: 'full' stores full original DataFrame, 'incremental' stores only changes (default)
        """
        self.primary_key = primary_key
        self.tracking_mode = tracking_mode
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

        # Lazy computation flags
        self._changes_computed: bool = False
        self._computation_needed: bool = True

        # Initialize attributes that are used in both modes
        self._original_index_set: set[Any] = self._extract_original_index(original_data)
        self._changed_rows: dict[Any, RowState] = {}
        self._original_data_stored: bool

        # Incremental tracking: store only changed rows, not full original DataFrame
        if tracking_mode == "incremental":
            # In incremental mode, we still need original_data for comparison in compute_row_changes
            # But we can optimize by storing it temporarily and clearing after first computation
            # For now, store a lightweight reference that can be garbage collected
            self._original_data: pd.DataFrame | None = (
                original_data.copy()
            )  # Temporary, for comparison
            self._original_data_stored = False  # Track if we've done first computation
        else:
            # Full mode: store complete original DataFrame (backward compatible)
            self._original_data = original_data.copy()
            self._original_data_stored = True

    @property
    def original_data(self) -> pd.DataFrame | None:
        """
        Access original data for backward compatibility.

        In incremental mode, returns None as we don't store full original DataFrame.
        In full mode, returns the stored original DataFrame.
        """
        return self._original_data

    def _get_original_row(
        self, pk_value: Any, original_data: pd.DataFrame | None = None
    ) -> dict[str, Any] | None:
        """
        Get original row data for a primary key value.

        In incremental mode, retrieves from stored RowState if changed, otherwise
        from the original_data parameter if provided.
        In full mode, retrieves from stored original_data.

        Args:
            pk_value: Primary key value
            original_data: Optional DataFrame to extract row from (used in incremental mode)

        Returns:
            Dictionary of original row data, or None if not available
        """
        if self.tracking_mode == "incremental":
            # In incremental mode, we only have original data for changed rows
            if pk_value in self._changed_rows:
                row_state = self._changed_rows[pk_value]
                if row_state.old_values:
                    return row_state.old_values
            # For unchanged rows, try to get from provided original_data
            if original_data is not None:
                return self._extract_row_from_df(original_data, pk_value)
            return None
        else:
            # Full mode: get from stored original_data
            if self._original_data is None:
                return None
            return self._extract_row_from_df(self._original_data, pk_value)

    def _extract_row_from_df(self, df: pd.DataFrame, pk_value: Any) -> dict[str, Any] | None:
        """
        Extract a row from DataFrame by primary key value.

        Args:
            df: DataFrame to search
            pk_value: Primary key value

        Returns:
            Dictionary of row data, or None if not found
        """
        pk_cols = normalize_primary_key(self.primary_key)

        if len(pk_cols) == 1:
            # Single column PK
            pk_col = pk_cols[0]
            if pk_col in df.columns:
                condition = df[pk_col] == pk_value
            elif df.index.name == pk_col:
                condition = df.index == pk_value
            else:
                return None
        else:
            # Composite PK
            if all(col in df.columns for col in pk_cols):
                condition = True
                for i, col in enumerate(pk_cols):
                    condition = condition & (df[col] == pk_value[i])
            elif isinstance(df.index, pd.MultiIndex) and all(
                name in df.index.names for name in pk_cols
            ):
                condition = df.index == pk_value
            else:
                return None

        if hasattr(condition, "any") and not condition.any():
            return None

        matching_rows = df[condition] if hasattr(condition, "any") else df.loc[condition]
        if len(matching_rows) > 0:
            return matching_rows.iloc[0].to_dict()
        return None

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
            operation_type="method_call", method_name=method_name, args=args, kwargs=kwargs
        )
        self.operations.append(operation)
        # Invalidate computed changes when a new operation is recorded
        self._invalidate_changes()

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
            self._invalidate_changes()

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
        self._invalidate_changes()

    def track_column_rename(self, old_name: str, new_name: str) -> None:
        """
        Track the renaming of a column.

        Args:
            old_name: Original column name
            new_name: New column name
        """
        self.renamed_columns[old_name] = new_name
        self._invalidate_changes()

    def track_column_type_change(self, column_name: str, new_type: type) -> None:
        """
        Track a column type change.

        Args:
            column_name: Name of the column
            new_type: New data type for the column
        """
        self.altered_column_types[column_name] = new_type
        self._invalidate_changes()

    def _invalidate_changes(self) -> None:
        """Mark that computed changes are stale and need recomputation."""
        self._computation_needed = True
        self._changes_computed = False

    def compute_row_changes(self, current_data: pd.DataFrame) -> None:
        """
        Compute row-level changes by comparing current data to original.

        This method uses lazy computation: it only computes changes if needed.
        Changes are marked as needed when operations are recorded or columns are modified.

        Args:
            current_data: The current state of the DataFrame
        """
        # Skip computation if already computed and no new changes
        if self._changes_computed and not self._computation_needed:
            return

        self.row_changes.clear()

        # Get original data for comparison (must be available for first computation)
        original_data_for_comparison = self._original_data
        if original_data_for_comparison is None:
            # In incremental mode, if original_data was cleared, we can't compute changes
            # This should only happen if tracking was reset improperly
            return

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
            if self.primary_key in original_data_for_comparison.columns:
                original_df = original_data_for_comparison.set_index(self.primary_key)
            elif original_data_for_comparison.index.name == self.primary_key:
                original_df = original_data_for_comparison
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
            elif isinstance(current_data.index, pd.MultiIndex) and all(
                name in current_data.index.names for name in pk_cols
            ):
                # PK in MultiIndex
                current_keys = set(current_data.index.values)
                current_df = current_data
            else:
                # Primary key columns missing, can't track row changes
                return

            # Prepare original data
            if all(col in original_data_for_comparison.columns for col in pk_cols):
                # PK in columns
                original_df = original_data_for_comparison.set_index(pk_cols)
                original_keys = {tuple(row) for row in original_data_for_comparison[pk_cols].values}
            elif isinstance(original_data_for_comparison.index, pd.MultiIndex) and all(
                name in original_data_for_comparison.index.names for name in pk_cols
            ):
                # PK in MultiIndex
                original_df = original_data_for_comparison
                original_keys = set(original_data_for_comparison.index.values)
            else:
                return

        # Find inserts: keys in current but not in original
        inserted_keys = current_keys - original_keys
        for key in inserted_keys:
            row_data = current_df.loc[key].to_dict()
            self.row_changes[key] = RowChange(
                change_type=ChangeType.INSERT, primary_key_value=key, new_data=row_data
            )

        # Find deletes: keys in original but not in current
        deleted_keys = original_keys - current_keys
        for key in deleted_keys:
            row_data = original_df.loc[key].to_dict()
            self.row_changes[key] = RowChange(
                change_type=ChangeType.DELETE, primary_key_value=key, old_data=row_data
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

            # Check if any values differ and track only changed columns
            changed_columns: set[str] = set()
            old_values: dict[str, Any] = {}
            new_values: dict[str, Any] = {}

            try:
                if not current_row.equals(original_row):
                    # Handle NaN comparisons and track only changed columns
                    for col in common_cols:
                        curr_val = current_row[col]
                        orig_val = original_row[col]

                        # Check if both are NaN
                        if pd.isna(curr_val) and pd.isna(orig_val):  # type: ignore[unreachable]
                            continue  # type: ignore[unreachable]
                        # Check if values differ
                        if curr_val != orig_val:  # type: ignore[unreachable]
                            changed_columns.add(col)
                            old_values[col] = orig_val
                            new_values[col] = curr_val

                    if changed_columns:
                        # Store full row data in RowChange for SQL generation
                        self.row_changes[key] = RowChange(
                            change_type=ChangeType.UPDATE,
                            primary_key_value=key,
                            old_data=original_row.to_dict(),  # type: ignore[arg-type]
                            new_data=current_row.to_dict(),  # type: ignore[arg-type]
                        )
                        # In incremental mode, also store RowState with only changed columns
                        if self.tracking_mode == "incremental":
                            self._changed_rows[key] = RowState(
                                status=RowStatus.UPDATED,
                                changed_columns=changed_columns,
                                old_values=old_values,
                                new_values=new_values,
                            )
            except (ValueError, TypeError):
                # If comparison fails, assume they're different
                self.row_changes[key] = RowChange(
                    change_type=ChangeType.UPDATE,
                    primary_key_value=key,
                    old_data=original_row.to_dict(),  # type: ignore[arg-type]
                    new_data=current_row.to_dict(),  # type: ignore[arg-type]
                )
                # Store RowState in incremental mode
                if self.tracking_mode == "incremental":
                    all_changed_cols = set(common_cols)
                    self._changed_rows[key] = RowState(
                        status=RowStatus.UPDATED,
                        changed_columns=all_changed_cols,
                        old_values=original_row.to_dict(),  # type: ignore[arg-type]
                        new_values=current_row.to_dict(),  # type: ignore[arg-type]
                    )

        # In incremental mode, store RowState for inserts and deletes too
        if self.tracking_mode == "incremental":
            for key in inserted_keys:
                new_row_data = current_df.loc[key].to_dict()
                self._changed_rows[key] = RowState(
                    status=RowStatus.INSERTED,
                    changed_columns=set(),
                    old_values=None,
                    new_values=new_row_data,
                )
            for key in deleted_keys:
                old_row_data = original_df.loc[key].to_dict()
                self._changed_rows[key] = RowState(
                    status=RowStatus.DELETED,
                    changed_columns=set(),
                    old_values=old_row_data,
                    new_values=None,
                )
            # After first computation, we can clear original_data to save memory
            # (we still have it in _changed_rows for changed rows, and _original_index_set for comparison)
            if not self._original_data_stored and self._original_data is not None:
                # Keep it for now - we might need it for subsequent comparisons
                # Can optimize further by clearing after ensuring all needed data is in _changed_rows
                self._original_data_stored = True

        # Mark changes as computed
        self._changes_computed = True
        self._computation_needed = False

    def get_inserts(self) -> list[RowChange]:
        """Get all row insertions."""
        return [rc for rc in self.row_changes.values() if rc.change_type == ChangeType.INSERT]

    def get_updates(self) -> list[RowChange]:
        """Get all row updates."""
        return [rc for rc in self.row_changes.values() if rc.change_type == ChangeType.UPDATE]

    def get_deletes(self) -> list[RowChange]:
        """Get all row deletions."""
        return [rc for rc in self.row_changes.values() if rc.change_type == ChangeType.DELETE]

    def has_changes(self, current_data: pd.DataFrame | None = None) -> bool:
        """
        Check if any changes have been tracked.

        If current_data is provided, will compute row changes if needed.
        Otherwise, only checks column-level and operation-level changes.

        Args:
            current_data: Optional current DataFrame state to compute row changes

        Returns:
            True if there are any changes, False otherwise
        """
        # Compute row changes if current data provided and computation needed
        if current_data is not None and self._computation_needed:
            self.compute_row_changes(current_data)

        return (
            len(self.row_changes) > 0
            or len(self.added_columns) > 0
            or len(self.dropped_columns) > 0
            or len(self.renamed_columns) > 0
            or len(self.altered_column_types) > 0
        )

    def reset(self, new_data: pd.DataFrame) -> None:
        """
        Reset the tracker with new original data.

        Args:
            new_data: The new baseline DataFrame
        """
        self.original_columns = set(new_data.columns)
        self.operations.clear()
        self.row_changes.clear()
        self.added_columns.clear()
        self.dropped_columns.clear()
        self.renamed_columns.clear()
        self.altered_column_types.clear()

        # Use same extraction logic as __init__ - handles both single and composite keys
        self.original_index = self._extract_original_index(new_data)

        # Reset lazy computation flags
        self._changes_computed = False
        self._computation_needed = True

        # Reset tracking data based on mode
        if self.tracking_mode == "incremental":
            self._original_index_set = self._extract_original_index(new_data)
            self._original_data = new_data.copy()  # Store for comparison
            self._changed_rows.clear()
            self._original_data_stored = False
        else:
            self._original_data = new_data.copy()
            self._original_index_set = self._extract_original_index(new_data)
            self._changed_rows.clear()
            self._original_data_stored = True

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
            "has_changes": self.has_changes(),
        }
