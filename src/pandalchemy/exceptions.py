"""
Custom exceptions for pandalchemy.

This module provides specific exception types for different error conditions
in pandalchemy operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ErrorContext:
    """Context information captured automatically for error reporting."""

    table_name: str | None = None
    operation: str | None = None
    affected_rows: list[Any] | None = None
    column_name: str | None = None
    primary_key_value: Any | None = None


class PandalchemyError(Exception):
    """
    Base exception for all pandalchemy errors.

    Enhanced with detailed context for better error messages and debugging.
    """

    def __init__(
        self,
        message: str,
        details: Any | None = None,
        table_name: str | None = None,
        operation: str | None = None,
        affected_rows: list[Any] | None = None,
        error_code: str | None = None,
        suggested_fix: str | None = None,
    ):
        """
        Initialize a PandalchemyError.

        Args:
            message: The error message
            details: Optional additional details about the error
            table_name: Name of the table where the error occurred
            operation: Operation being performed when error occurred
            affected_rows: Primary keys of affected rows
            error_code: Categorizable error code (e.g., 'PK_IMMUTABLE')
            suggested_fix: Actionable solution with code example
        """
        super().__init__(message)
        self.message = message
        self.details = details
        self.table_name = table_name
        self.operation = operation
        self.affected_rows = affected_rows or []
        self.error_code = error_code
        self.suggested_fix = suggested_fix

    def format_error(self) -> str:
        """
        Generate a user-friendly formatted error message.

        Returns:
            Formatted error message with context and suggestions
        """
        lines = [f"{self.__class__.__name__}: {self.message}"]

        if self.table_name:
            lines.append(f"  Table: '{self.table_name}'")

        if self.operation:
            lines.append(f"  Operation: {self.operation}")

        if self.affected_rows:
            if len(self.affected_rows) <= 5:
                lines.append(f"  Affected Rows: {self.affected_rows}")
            else:
                lines.append(
                    f"  Affected Rows: {len(self.affected_rows)} rows (showing first 5: {self.affected_rows[:5]}...)"
                )

        if self.error_code:
            lines.append(f"  Error Code: {self.error_code}")

        if self.suggested_fix:
            lines.append(f"  Fix: {self.suggested_fix}")

        if self.details:
            if isinstance(self.details, dict):
                detail_strs = [f"    {k}: {v}" for k, v in self.details.items()]
                if detail_strs:
                    lines.append("  Details:")
                    lines.extend(detail_strs)
            else:
                lines.append(f"  Details: {self.details}")

        return "\n".join(lines)

    def __str__(self) -> str:
        """Return formatted error message."""
        return self.format_error()


class SchemaError(PandalchemyError):
    """
    Exception raised for schema-related errors.

    Examples:
        - Column already exists
        - Column doesn't exist
        - Invalid column type
        - Schema migration failure
    """

    pass


class TransactionError(PandalchemyError):
    """
    Exception raised for transaction-related errors.

    Examples:
        - Transaction rollback
        - Commit failure
        - Nested transaction issues
    """

    pass


class DataValidationError(PandalchemyError):
    """
    Exception raised for data validation errors.

    Examples:
        - Duplicate primary keys
        - Invalid data types
        - Constraint violations
    """

    pass


class ConnectionError(PandalchemyError):
    """
    Exception raised for database connection errors.

    Examples:
        - Connection failure
        - Invalid connection string
        - Connection timeout
    """

    pass


class ChangeTrackingError(PandalchemyError):
    """
    Exception raised for change tracking errors.

    Examples:
        - Tracking state corruption
        - Invalid change operation
        - Change application failure
    """

    pass


class ConflictError(PandalchemyError):
    """
    Exception raised for concurrent modification conflicts.

    This error occurs when the same row is modified both locally and remotely,
    and conflict resolution strategy requires raising an error.

    Examples:
        - Same row updated in both local changes and database
        - Concurrent modifications detected during push
    """

    def __init__(
        self,
        message: str,
        table_name: str | None = None,
        primary_key: Any | None = None,
        local_changes: dict[str, Any] | None = None,
        remote_changes: dict[str, Any] | None = None,
        conflicting_columns: list[str] | None = None,
        **kwargs,
    ):
        """
        Initialize a ConflictError.

        Args:
            message: The error message
            table_name: Name of the table with conflict
            primary_key: Primary key value of conflicting row
            local_changes: Local changes to the row
            remote_changes: Remote changes to the row
            conflicting_columns: List of columns with conflicting values
            **kwargs: Additional arguments passed to PandalchemyError
        """
        super().__init__(
            message,
            table_name=table_name,
            affected_rows=[primary_key] if primary_key is not None else None,
            **kwargs,
        )
        self.primary_key = primary_key
        self.local_changes = local_changes or {}
        self.remote_changes = remote_changes or {}
        self.conflicting_columns = conflicting_columns or []

    def format_error(self) -> str:
        """Generate formatted error message with conflict details."""
        lines = [f"{self.__class__.__name__}: {self.message}"]

        if self.table_name:
            lines.append(f"  Table: '{self.table_name}'")

        if self.primary_key is not None:
            lines.append(f"  Primary Key: {self.primary_key}")

        if self.conflicting_columns:
            lines.append(f"  Conflicting Columns: {self.conflicting_columns}")

        if self.local_changes:
            lines.append(f"  Local Changes: {self.local_changes}")

        if self.remote_changes:
            lines.append(f"  Remote Changes: {self.remote_changes}")

        if self.suggested_fix:
            lines.append(f"  Fix: {self.suggested_fix}")

        return "\n".join(lines)
