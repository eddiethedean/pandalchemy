"""
Execution plan builder for optimizing SQL operations.

This module provides the ExecutionPlan class that analyzes tracked changes
and generates an optimized sequence of SQL operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

import pandas as pd

from pandalchemy.change_tracker import ChangeTracker


class OperationType(Enum):
    """Types of SQL operations in the execution plan."""
    SCHEMA_CHANGE = "schema_change"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class PlanStep:
    """
    Represents a single step in the execution plan.

    Attributes:
        operation_type: The type of operation to perform
        description: Human-readable description of the operation
        data: Data needed to execute the operation
        priority: Priority order (lower executes first)
    """
    operation_type: OperationType
    description: str
    data: Any
    priority: int = 100

    def __lt__(self, other):
        """Compare steps by priority for sorting."""
        return self.priority < other.priority


@dataclass
class SchemaChange:
    """Represents a schema modification."""
    change_type: str  # 'add_column', 'drop_column', 'rename_column', 'alter_column_type'
    column_name: str
    new_column_name: str | None = None
    column_type: Any | None = None
    new_column_type: Any | None = None


class ExecutionPlan:
    """
    Generates an optimized execution plan from tracked changes.

    This class analyzes the changes tracked by a ChangeTracker and creates
    an ordered sequence of SQL operations that efficiently synchronize the
    database with the current DataFrame state.

    Attributes:
        steps: List of operations to execute in order
        tracker: The ChangeTracker providing change information
    """

    def __init__(self, tracker: ChangeTracker, current_data: pd.DataFrame):
        """
        Initialize the ExecutionPlan.

        Args:
            tracker: The ChangeTracker with recorded changes
            current_data: The current state of the DataFrame
        """
        self.tracker = tracker
        self.current_data = current_data
        self.steps: list[PlanStep] = []
        self._build_plan()

    def _build_plan(self) -> None:
        """Build the execution plan from tracked changes."""
        # Recompute row changes to ensure they're up to date
        self.tracker.compute_row_changes(self.current_data)

        # Priority 1: Schema changes (must happen first)
        self._add_schema_changes()

        # Priority 2: Deletes (remove old data before inserting new)
        self._add_deletes()

        # Priority 3: Updates (modify existing data)
        self._add_updates()

        # Priority 4: Inserts (add new data last)
        self._add_inserts()

        # Sort steps by priority
        self.steps.sort()

    def _add_schema_changes(self) -> None:
        """Add schema modification operations to the plan."""
        # Handle column renames first
        for old_name, new_name in self.tracker.renamed_columns.items():
            schema_change = SchemaChange(
                change_type='rename_column',
                column_name=old_name,
                new_column_name=new_name
            )
            step = PlanStep(
                operation_type=OperationType.SCHEMA_CHANGE,
                description=f"Rename column '{old_name}' to '{new_name}'",
                data=schema_change,
                priority=1
            )
            self.steps.append(step)

        # Handle column drops
        for col_name in self.tracker.dropped_columns:
            # Don't drop if it was renamed
            if col_name not in self.tracker.renamed_columns:
                schema_change = SchemaChange(
                    change_type='drop_column',
                    column_name=col_name
                )
                step = PlanStep(
                    operation_type=OperationType.SCHEMA_CHANGE,
                    description=f"Drop column '{col_name}'",
                    data=schema_change,
                    priority=2
                )
                self.steps.append(step)

        # Handle column additions
        for col_name in self.tracker.added_columns:
            # Determine column type from current data
            if col_name in self.current_data.columns:
                col_type = self.current_data[col_name].dtype
            else:
                col_type = None

            schema_change = SchemaChange(
                change_type='add_column',
                column_name=col_name,
                column_type=col_type
            )
            step = PlanStep(
                operation_type=OperationType.SCHEMA_CHANGE,
                description=f"Add column '{col_name}'",
                data=schema_change,
                priority=3
            )
            self.steps.append(step)

        # Handle column type changes
        for col_name, new_type in self.tracker.altered_column_types.items():
            schema_change = SchemaChange(
                change_type='alter_column_type',
                column_name=col_name,
                new_column_type=new_type
            )
            step = PlanStep(
                operation_type=OperationType.SCHEMA_CHANGE,
                description=f"Alter column '{col_name}' type to {new_type.__name__}",
                data=schema_change,
                priority=4
            )
            self.steps.append(step)

    def _add_deletes(self) -> None:
        """Add delete operations to the plan."""
        deletes = self.tracker.get_deletes()

        if deletes:
            # Batch all deletes together
            delete_keys = [rc.primary_key_value for rc in deletes]
            step = PlanStep(
                operation_type=OperationType.DELETE,
                description=f"Delete {len(deletes)} row(s)",
                data=delete_keys,
                priority=10
            )
            self.steps.append(step)

    def _add_updates(self) -> None:
        """Add update operations to the plan."""
        updates = self.tracker.get_updates()

        # Also check if new columns were added with data for existing rows
        update_records = []

        if updates:
            # Batch all updates together
            for rc in updates:
                # Handle both single and composite primary keys
                if isinstance(self.tracker.primary_key, str):
                    update_rec = {self.tracker.primary_key: rc.primary_key_value}
                else:
                    # Composite PK - unpack tuple into individual key columns
                    update_rec = dict(zip(self.tracker.primary_key, rc.primary_key_value))  # type: ignore[arg-type]
                if rc.new_data:
                    update_rec.update(rc.new_data)
                update_records.append(update_rec)

        # If columns were added and there are existing rows, we need to update them
        if self.tracker.added_columns and not updates:
            # Check if there are rows that need the new column values populated
            if self.tracker.primary_key in self.current_data.columns:
                current_keys = set(self.current_data[self.tracker.primary_key].values)
            elif self.current_data.index.name == self.tracker.primary_key:
                current_keys = set(self.current_data.index.values)
            else:
                current_keys = set()

            original_keys = self.tracker.original_index

            # Only existing rows (not new inserts)
            existing_keys = current_keys & original_keys

            if existing_keys and self.tracker.added_columns:
                # Generate update records for existing rows to populate new columns
                if self.tracker.primary_key in self.current_data.columns:
                    df = self.current_data.set_index(self.tracker.primary_key)
                else:
                    df = self.current_data

                for key in existing_keys:
                    record: dict[str, Any] = {self.tracker.primary_key: key}  # type: ignore[dict-item]
                    # Only include the new columns
                    for col in self.tracker.added_columns:
                        if col in df.columns:
                            record[col] = df.loc[key, col]
                    update_records.append(record)

        if update_records:
            step = PlanStep(
                operation_type=OperationType.UPDATE,
                description=f"Update {len(update_records)} row(s)",
                data=update_records,
                priority=20
            )
            self.steps.append(step)

    def _add_inserts(self) -> None:
        """Add insert operations to the plan."""
        inserts = self.tracker.get_inserts()

        if inserts:
            # Batch all inserts together
            insert_records = []
            for rc in inserts:
                # Handle both single and composite primary keys
                if isinstance(self.tracker.primary_key, str):
                    record = {self.tracker.primary_key: rc.primary_key_value}
                else:
                    # Composite PK - unpack tuple into individual key columns
                    record = dict(zip(self.tracker.primary_key, rc.primary_key_value))  # type: ignore[arg-type]
                if rc.new_data:
                    record.update(rc.new_data)
                insert_records.append(record)

            step = PlanStep(
                operation_type=OperationType.INSERT,
                description=f"Insert {len(inserts)} row(s)",
                data=insert_records,
                priority=30
            )
            self.steps.append(step)

    def has_changes(self) -> bool:
        """
        Check if the plan has any operations to execute.

        Returns:
            True if there are operations in the plan, False otherwise
        """
        return len(self.steps) > 0

    def get_summary(self) -> dict[str, Any]:
        """
        Get a summary of the execution plan.

        Returns:
            Dictionary containing plan statistics
        """
        schema_changes = sum(1 for s in self.steps
                           if s.operation_type == OperationType.SCHEMA_CHANGE)
        inserts = sum(1 for s in self.steps
                     if s.operation_type == OperationType.INSERT)
        updates = sum(1 for s in self.steps
                     if s.operation_type == OperationType.UPDATE)
        deletes = sum(1 for s in self.steps
                     if s.operation_type == OperationType.DELETE)

        return {
            "total_steps": len(self.steps),
            "schema_changes": schema_changes,
            "insert_operations": inserts,
            "update_operations": updates,
            "delete_operations": deletes
        }

    def get_steps_by_type(self, operation_type: OperationType) -> list[PlanStep]:
        """
        Get all steps of a specific operation type.

        Args:
            operation_type: The type of operation to filter by

        Returns:
            List of steps matching the operation type
        """
        return [step for step in self.steps if step.operation_type == operation_type]

    def __repr__(self) -> str:
        """Return string representation of the plan."""
        lines = ["ExecutionPlan:"]
        for i, step in enumerate(self.steps, 1):
            lines.append(f"  {i}. [{step.operation_type.value}] {step.description}")
        return "\n".join(lines)

    def __str__(self) -> str:
        """Return string representation of the plan."""
        return self.__repr__()

