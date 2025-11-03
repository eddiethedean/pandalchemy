"""
Conflict resolution for concurrent modifications.

This module provides conflict detection and resolution strategies for handling
concurrent modifications to the same data by multiple processes.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Callable

import pandas as pd

from pandalchemy.exceptions import ConflictError


class ConflictStrategy(Enum):
    """Conflict resolution strategies."""

    LAST_WRITER_WINS = "last_writer_wins"  # Default: overwrite with local changes
    FIRST_WRITER_WINS = "first_writer_wins"  # Keep remote changes, discard local
    ABORT = "abort"  # Raise ConflictError
    MERGE = "merge"  # Merge non-conflicting columns, raise on conflicts
    CUSTOM = "custom"  # Use custom resolver function


def detect_conflicts(
    _local_data: pd.DataFrame,
    remote_data: pd.DataFrame,
    _primary_key: str | list[str],
    local_changes: dict[Any, dict[str, Any]],
    original_data: pd.DataFrame | None = None,
) -> dict[Any, dict[str, Any]]:
    """
    Detect conflicts between local changes and remote data.

    A conflict occurs when:
    - A row was updated both locally and remotely
    - The remote data differs from what was originally read (base state)

    Args:
        local_data: Current local DataFrame state
        remote_data: Current remote DataFrame state from database
        primary_key: Primary key column name(s)
        local_changes: Dict mapping PK values to local changes (from tracker)
        original_data: Original data state when tracking started (optional, for better conflict detection)

    Returns:
        Dict mapping PK values to conflict details:
        {
            pk_value: {
                'local': {...},  # Local changes
                'remote': {...},  # Remote changes
                'conflicting_columns': [...]  # Columns with different values
            }
        }
    """
    conflicts: dict[Any, dict[str, Any]] = {}

    # Get update changes only (inserts/deletes don't conflict)
    updates = {pk: changes for pk, changes in local_changes.items() if pk in remote_data.index}

    if not updates:
        return conflicts

    # For each updated row, check if remote data differs from what we expected
    for pk_value, local_row_changes in updates.items():
        if pk_value not in remote_data.index:
            continue  # Row was deleted remotely, not a conflict (handled elsewhere)

        remote_row = remote_data.loc[pk_value]
        conflicting_columns: list[str] = []
        remote_changes: dict[str, Any] = {}

        # Get original row if available for better conflict detection
        original_row = None
        if original_data is not None and pk_value in original_data.index:
            original_row = original_data.loc[pk_value]

        # Compare changed columns with remote values
        for col, local_value in local_row_changes.items():
            if col not in remote_row.index:
                continue  # Column doesn't exist remotely (schema change)

            remote_value = remote_row[col]

            # If we have original data, only conflict if remote changed from original
            # Otherwise, conflict if remote differs from local
            if original_row is not None and col in original_row.index:
                original_value = original_row[col]
                # Conflict only if remote changed from original AND differs from local
                try:
                    if pd.isna(original_value) and pd.isna(remote_value):
                        # Remote unchanged from original, no conflict
                        continue
                    if original_value == remote_value:
                        # Remote unchanged from original, no conflict
                        continue
                    # Remote changed from original, check if conflicts with local
                    if pd.isna(local_value) and pd.isna(remote_value):
                        continue  # Both NaN, no conflict
                    if local_value != remote_value:
                        conflicting_columns.append(col)
                        remote_changes[col] = remote_value
                except (ValueError, TypeError):
                    # If comparison fails, assume conflict
                    conflicting_columns.append(col)
                    remote_changes[col] = remote_value
            else:
                # No original data, conflict if remote differs from local
                try:
                    if pd.isna(local_value) and pd.isna(remote_value):
                        continue  # Both NaN, no conflict
                    if local_value != remote_value:
                        conflicting_columns.append(col)
                        remote_changes[col] = remote_value
                except (ValueError, TypeError):
                    # If comparison fails, assume conflict
                    conflicting_columns.append(col)
                    remote_changes[col] = remote_value

        if conflicting_columns:
            conflicts[pk_value] = {
                "local": local_row_changes,
                "remote": remote_changes,
                "conflicting_columns": conflicting_columns,
            }

    return conflicts


def resolve_conflicts(
    conflicts: dict[Any, dict[str, Any]],
    strategy: ConflictStrategy,
    table_name: str | None = None,
    _primary_key: str | list[str] | None = None,
    custom_resolver: Callable[[Any, dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[Any, dict[str, Any]]:
    """
    Resolve conflicts using the specified strategy.

    Args:
        conflicts: Dict of conflicts from detect_conflicts()
        strategy: Resolution strategy to use
        table_name: Name of table (for error messages)
        primary_key: Primary key for error messages
        custom_resolver: Custom resolver function (required for CUSTOM strategy)

    Returns:
        Dict mapping PK values to resolved changes to apply

    Raises:
        ConflictError: If strategy is ABORT or MERGE with unresolvable conflicts
        ValueError: If CUSTOM strategy used without custom_resolver
    """
    resolved: dict[Any, dict[str, Any]] = {}

    for pk_value, conflict_details in conflicts.items():
        local_changes = conflict_details["local"]
        remote_changes = conflict_details["remote"]
        conflicting_cols = conflict_details["conflicting_columns"]

        if strategy == ConflictStrategy.LAST_WRITER_WINS:
            # Use local changes, discard remote
            resolved[pk_value] = local_changes

        elif strategy == ConflictStrategy.FIRST_WRITER_WINS:
            # Use remote changes, discard local
            # Return empty dict to skip this update
            resolved[pk_value] = {}

        elif strategy == ConflictStrategy.ABORT:
            # Raise error for any conflict
            raise ConflictError(
                f"Concurrent modification conflict detected for row {pk_value}",
                table_name=table_name,
                primary_key=pk_value,
                local_changes=local_changes,
                remote_changes=remote_changes,
                conflicting_columns=conflicting_cols,
                suggested_fix=(
                    "Pull latest data and resolve manually:\n"
                    "    table.pull()\n"
                    "    # Review and merge changes\n"
                    "    table.push()"
                ),
            )

        elif strategy == ConflictStrategy.MERGE:
            # Merge non-conflicting columns, keep local for conflicts
            merged = local_changes.copy()
            # Add non-conflicting remote changes
            for col, remote_val in remote_changes.items():
                if col not in conflicting_cols:
                    merged[col] = remote_val
            resolved[pk_value] = merged

        elif strategy == ConflictStrategy.CUSTOM:
            if custom_resolver is None:
                raise ValueError("custom_resolver required for CUSTOM strategy")
            resolved_changes = custom_resolver(pk_value, local_changes, remote_changes)
            resolved[pk_value] = resolved_changes

        else:
            raise ValueError(f"Unknown conflict strategy: {strategy}")

    return resolved
