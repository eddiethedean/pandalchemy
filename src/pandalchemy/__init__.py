"""
Pandalchemy: Pandas + SQLAlchemy with Change Tracking

A modern package for managing SQL databases with pandas DataFrames,
featuring automatic change tracking and optimized SQL operations.
"""

import pandalchemy.sql_operations as sql_ops
from pandalchemy._version import version
from pandalchemy.change_tracker import ChangeTracker, ChangeType
from pandalchemy.conflict_resolution import ConflictStrategy
from pandalchemy.exceptions import (
    ChangeTrackingError,
    ConflictError,
    DataValidationError,
    ErrorContext,
    PandalchemyError,
    SchemaError,
    TransactionError,
)
from pandalchemy.execution_plan import ExecutionPlan
from pandalchemy.pandalchemy_base import DataBase
from pandalchemy.tracked_dataframe import TableDataFrame

__version__ = version

__all__ = [
    # Core classes
    "DataBase",
    "TableDataFrame",
    "ChangeTracker",
    "ChangeType",
    "ExecutionPlan",
    # Conflict resolution
    "ConflictStrategy",
    # Modules
    "sql_ops",
    # Exceptions
    "PandalchemyError",
    "SchemaError",
    "TransactionError",
    "DataValidationError",
    "ChangeTrackingError",
    "ConflictError",
    "ErrorContext",
    # Version
    "__version__",
]

# Async support (optional - only available if SQLAlchemy async drivers are installed)
try:
    from pandalchemy.async_base import AsyncDataBase, AsyncTableDataFrame

    __all__.extend(["AsyncDataBase", "AsyncTableDataFrame"])
except ImportError:
    # Async support not available (e.g., missing async drivers)
    pass
