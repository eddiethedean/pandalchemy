"""
Custom exceptions for pandalchemy.

This module provides specific exception types for different error conditions
in pandalchemy operations.
"""

from __future__ import annotations

from typing import Any


class PandalchemyError(Exception):
    """Base exception for all pandalchemy errors."""

    def __init__(self, message: str, details: Any | None = None):
        """
        Initialize a PandalchemyError.

        Args:
            message: The error message
            details: Optional additional details about the error
        """
        super().__init__(message)
        self.details = details


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

