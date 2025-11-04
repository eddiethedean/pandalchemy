"""
Async versions of DataBase and TableDataFrame.

This module provides async-compatible classes that mirror the synchronous
API but use async/await for all database operations.

Note: SQLite async support is provided for API consistency, but the synchronous
DataBase class is recommended for SQLite databases. SQLite uses database-level
locking and doesn't support concurrent writes, so async provides no performance
benefit and adds complexity (greenlet context management).
"""

from __future__ import annotations

import warnings
from typing import Any

from pandas import DataFrame
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncEngine

from pandalchemy.async_connection_manager import get_sync_engine_cached
from pandalchemy.async_retry import AsyncRetryPolicy
from pandalchemy.change_tracker import ChangeTracker
from pandalchemy.sql_operations import get_primary_key, table_exists
from pandalchemy.tracked_dataframe import TableDataFrame


class AsyncTableDataFrame(TableDataFrame):
    """
    Async-compatible version of TableDataFrame.

    Extends TableDataFrame with async methods for push/pull operations.
    All pandas operations remain synchronous for compatibility.
    """

    def __init__(
        self,
        name: str | DataFrame | None = None,
        data: DataFrame | None = None,
        primary_key: str | list[str] | None = None,
        engine: AsyncEngine | None = None,
        db: Any = None,
        schema: str | None = None,
        auto_increment: bool = False,
        tracker: ChangeTracker | None = None,
        tracking_mode: str = "incremental",
        conflict_strategy: str = "last_writer_wins",
        retry_policy: AsyncRetryPolicy | None = None,
        connection_timeout: float | None = None,
        query_timeout: float | None = None,
    ):
        """
        Initialize an AsyncTableDataFrame.

        Args:
            name: Table name or DataFrame
            data: pandas DataFrame with table data
            primary_key: Name of the primary key column(s)
            engine: SQLAlchemy async engine
            db: Parent AsyncDataBase object
            schema: Optional schema name
            auto_increment: If True, enable auto-increment for primary key
            tracker: Optional existing ChangeTracker
            tracking_mode: 'full' or 'incremental' (default)
            conflict_strategy: Conflict resolution strategy (default: 'last_writer_wins')
            retry_policy: Custom retry policy for async operations
            connection_timeout: Timeout for connection operations in seconds
            query_timeout: Timeout for query operations in seconds
        """
        # Convert async engine to sync for base class operations using cache
        # The async engine will be stored separately
        sync_engine = None
        if engine is not None:
            # Use cached sync engine to avoid creating multiple engines
            sync_engine = get_sync_engine_cached(engine)

        super().__init__(
            name=name,
            data=data,
            primary_key=primary_key,
            engine=sync_engine,
            db=db,
            schema=schema,
            auto_increment=auto_increment,
            tracker=tracker,
            tracking_mode=tracking_mode,
            conflict_strategy=conflict_strategy,
        )

        # Store async engine and configuration separately
        object.__setattr__(self, "_async_engine", engine)
        object.__setattr__(self, "_retry_policy", retry_policy)
        object.__setattr__(self, "_connection_timeout", connection_timeout)
        object.__setattr__(self, "_query_timeout", query_timeout)

    async def push(  # type: ignore[override]
        self, engine: AsyncEngine | None = None, schema: str | None = None
    ) -> None:
        """
        Push all changes to the database (async version).

        Args:
            engine: Optional async engine to use
            schema: Optional schema to use

        Raises:
            SchemaError: If primary key columns have been dropped
            DataValidationError: If primary key validation fails
            TransactionError: If any database operation fails
        """
        from pandalchemy.async_operations import AsyncGreenletContext, async_execute_plan
        from pandalchemy.exceptions import DataValidationError, SchemaError
        from pandalchemy.sql_operations import create_table_from_dataframe

        # Use greenlet context manager
        async with AsyncGreenletContext():
            pass  # Context established

        if engine is not None:
            object.__setattr__(self, "_async_engine", engine)
        if schema is not None:
            object.__setattr__(self, "schema", schema)

        engine_to_use = self._async_engine
        if engine_to_use is None:
            raise ValueError("Cannot push without an async engine")
        if self._primary_key is None:
            raise ValueError("Cannot push without a primary key")
        if self.name is None:
            raise ValueError("Cannot push without a table name")

        # Validate primary key
        try:
            self.validate_primary_key()
        except (SchemaError, DataValidationError) as e:
            raise SchemaError(
                f"Cannot push table '{self.name}': {str(e)}",
                details={"table": self.name, "primary_key": self._primary_key, "error": str(e)},
            ) from e

        current_df = self.to_pandas()

        # Check if table exists (use cached sync engine for this check)
        sync_engine = self.engine
        if sync_engine is None:
            sync_engine = get_sync_engine_cached(engine_to_use)

        if not table_exists(sync_engine, self.name, self.schema):
            # Create new table (use sync for schema creation)
            create_table_from_dataframe(
                sync_engine, self.name, current_df, self._primary_key, self.schema, if_exists="fail"
            )
        else:
            # Detect and resolve conflicts before executing plan
            plan = self._build_plan_with_conflict_resolution(current_df)

            # Get retry policy, timeouts, and isolation level
            retry_policy = getattr(self, "_retry_policy", None)
            query_timeout = getattr(self, "_query_timeout", None)
            isolation_level = None
            if hasattr(self, "db") and self.db is not None:
                isolation_level = getattr(self.db, "isolation_level", None)

            # Execute the plan (async) with retry, timeout, and isolation level support
            await async_execute_plan(
                engine_to_use,
                self.name,
                plan,
                self.schema,
                self._primary_key,
                timeout=query_timeout,
                retry_policy=retry_policy,
                isolation_level=isolation_level,
            )

        # Refresh the table after successful push
        await self.pull(timeout=getattr(self, "_query_timeout", None))

    async def pull(  # type: ignore[override]
        self,
        engine: AsyncEngine | None = None,
        schema: str | None = None,
        timeout: float | None = None,
    ) -> None:
        """
        Refresh the table with current database data (async version).

        Args:
            engine: Optional async engine to use
            schema: Optional schema to use
            timeout: Optional timeout for the pull operation
        """
        from pandalchemy.async_operations import AsyncGreenletContext, async_pull_table

        # Use greenlet context manager
        async with AsyncGreenletContext():
            pass  # Context established

        if engine is not None:
            object.__setattr__(self, "_async_engine", engine)
        if schema is not None:
            object.__setattr__(self, "schema", schema)

        if self._async_engine and self.name:
            # Get primary key if not set
            if self._primary_key is None:
                # Use cached sync engine for introspection
                sync_engine = get_sync_engine_cached(self._async_engine)
                self._primary_key = get_primary_key(sync_engine, self.name, self.schema)
                if self._primary_key is None:
                    self._primary_key = "id"

            # Use provided timeout or instance timeout
            pull_timeout = timeout or getattr(self, "_query_timeout", None)

            # Pull table data (async) with timeout support
            data = await async_pull_table(
                self._async_engine,
                self.name,
                self.schema,
                self._primary_key,
                set_index=True,
                timeout=pull_timeout,
            )
            object.__setattr__(self, "_data", data)

            # Reset tracker with fresh data
            tracker = ChangeTracker(self._primary_key, data, tracking_mode=self._tracking_mode)
            object.__setattr__(self, "_tracker", tracker)


class AsyncDataBase:
    """
    Async-compatible version of DataBase.

    Manages multiple AsyncTableDataFrame objects with async push/pull operations.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        lazy: bool = False,
        schema: str | None = None,
        connection_timeout: float | None = None,
        query_timeout: float | None = None,
        retry_policy: AsyncRetryPolicy | None = None,
        isolation_level: str | None = None,
        max_concurrent_pushes: int | None = None,
    ):
        """
        Initialize an AsyncDataBase instance.

        Args:
            engine: SQLAlchemy async engine
            lazy: If True, tables are loaded only when accessed
            schema: Optional schema name
            connection_timeout: Timeout for connection operations in seconds
            query_timeout: Timeout for query operations in seconds
            retry_policy: Custom retry policy for async operations
            isolation_level: Transaction isolation level (e.g., 'READ_COMMITTED', 'SERIALIZABLE')
            max_concurrent_pushes: Maximum number of concurrent table pushes (None = unlimited)
        """
        self.engine = engine
        self.schema = schema
        self.lazy = lazy
        self.connection_timeout = connection_timeout
        self.query_timeout = query_timeout
        self.retry_policy = retry_policy
        self.isolation_level = isolation_level
        self.max_concurrent_pushes = max_concurrent_pushes

        # Warn users about SQLite async limitations
        if engine.dialect.name == "sqlite":
            warnings.warn(
                "SQLite async support is provided for API consistency, but the synchronous "
                "DataBase class is recommended for SQLite databases. SQLite uses database-level "
                "locking and doesn't support concurrent writes, so async provides no performance "
                "benefit and adds complexity (greenlet context management). "
                "Consider using: DataBase(create_engine('sqlite:///...')) instead.",
                UserWarning,
                stacklevel=2,
            )

        self.db: dict[str, AsyncTableDataFrame] = {}
        # Note: _load_tables needs to be async, so we'll defer it
        # User should call await db.load_tables() after initialization

    async def load_tables(self) -> None:
        """
        Load or reload all tables from the database (async version).

        This method should be called after initialization to populate tables.
        """
        from pandalchemy.async_operations import AsyncGreenletContext

        # Use greenlet context manager
        async with AsyncGreenletContext():
            pass  # Context established

        # Clear existing tables
        self.db.clear()

        # Use cached sync engine for introspection
        sync_engine = get_sync_engine_cached(self.engine)
        inspector = inspect(sync_engine)
        table_names = inspector.get_table_names(schema=self.schema)

        if not self.lazy:
            for name in table_names:
                self.db[name] = AsyncTableDataFrame(
                    name=name,
                    engine=self.engine,
                    db=self,
                    schema=self.schema,
                    retry_policy=self.retry_policy,
                    connection_timeout=self.connection_timeout,
                    query_timeout=self.query_timeout,
                )
                # Load table data asynchronously
                await self.db[name].pull(timeout=self.query_timeout)
        else:
            for name in table_names:
                self.db[name] = None  # type: ignore

    def __getitem__(self, key: str) -> AsyncTableDataFrame:
        """Get a table by name."""
        if self.lazy and self.db.get(key) is None:
            self.db[key] = AsyncTableDataFrame(
                name=key,
                engine=self.engine,
                db=self,
                schema=self.schema,
                retry_policy=self.retry_policy,
                connection_timeout=self.connection_timeout,
                query_timeout=self.query_timeout,
            )
        return self.db[key]

    def __setitem__(self, key: str, value: AsyncTableDataFrame) -> None:
        """Set a table by name."""
        self.db[key] = value

    def __len__(self) -> int:
        """Return the number of tables."""
        return len(self.db)

    def __contains__(self, key: str) -> bool:
        """Check if a table exists."""
        return key in self.db

    @property
    def table_names(self):
        """Get iterator of table names."""
        return iter(self.db.keys())

    def __repr__(self) -> str:
        """Return string representation."""
        table_list = ", ".join(self.db.keys())
        return f"AsyncDataBase(tables=[{table_list}], url={self.engine.url})"

    async def push(self, parallel: bool = True, _max_workers: int | None = None) -> None:
        """
        Push all table changes to the database (async version).

        Args:
            parallel: If True, execute independent tables in parallel (default True)
            max_workers: Maximum number of parallel workers (None = auto-detect)

        Note:
            SQLite doesn't support multi-threaded writes, so parallel execution is
            automatically disabled for SQLite databases.
        """
        import asyncio

        from pandalchemy.async_operations import AsyncGreenletContext

        # SQLite doesn't support multi-threaded writes - disable parallel for SQLite
        if self.engine.dialect.name == "sqlite":
            parallel = False

        # Use greenlet context manager
        async with AsyncGreenletContext():
            pass  # Context established

        # Get tables that need to be pushed
        tables_with_changes = {}
        all_tables_for_validation = {}

        for name, table in self.db.items():
            if table is not None and hasattr(table, "push"):
                all_tables_for_validation[name] = table

                # Check if table has changes to push
                should_push = False
                if hasattr(table, "has_changes") and table.has_changes():
                    should_push = True
                elif hasattr(table, "_tracker"):
                    tracker = table._tracker
                    if (
                        len(tracker.added_columns) > 0
                        or len(tracker.dropped_columns) > 0
                        or len(tracker.renamed_columns) > 0
                        or len(tracker.altered_column_types) > 0
                    ):
                        should_push = True

                # Also push if table doesn't exist in database yet (new table)
                if (
                    not should_push
                    and hasattr(table, "_async_engine")
                    and table._async_engine is not None
                    and table.name
                ):
                    from pandalchemy.sql_operations import table_exists

                    sync_engine = get_sync_engine_cached(table._async_engine)
                    if not table_exists(sync_engine, table.name, table.schema):
                        should_push = True

                if should_push:
                    tables_with_changes[name] = table

        if not tables_with_changes:
            return

        # Validate all tables before starting
        for _table_name, table in all_tables_for_validation.items():
            if hasattr(table, "validate_primary_key"):
                try:
                    table.validate_primary_key()
                except Exception as e:
                    from pandalchemy.exceptions import SchemaError

                    raise SchemaError(
                        f"Validation failed for table '{_table_name}': {str(e)}",
                        table_name=_table_name,
                    ) from e

        # Execute pushes (async, can run in parallel)
        if parallel and len(tables_with_changes) > 1:
            # Limit concurrent pushes if specified
            max_concurrent = self.max_concurrent_pushes
            if max_concurrent is None or max_concurrent >= len(tables_with_changes):
                # Execute all in parallel
                tasks = [table.push() for table in tables_with_changes.values()]
                results = await asyncio.gather(*tasks, return_exceptions=True)
            else:
                # Execute with concurrency limit using semaphore
                semaphore = asyncio.Semaphore(max_concurrent)

                async def push_with_limit(table: AsyncTableDataFrame) -> None:
                    async with semaphore:
                        await table.push()

                tasks = [push_with_limit(table) for table in tables_with_changes.values()]
                results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for errors with better error aggregation
            errors = [
                (name, result)
                for name, result in zip(tables_with_changes.keys(), results)
                if isinstance(result, Exception)
            ]

            if errors:
                from pandalchemy.exceptions import TransactionError

                error_messages = [f"{name}: {str(e)}" for name, e in errors]
                failed_tables = [name for name, _ in errors]
                raise TransactionError(
                    f"Failed to push {len(errors)} table(s): " + "; ".join(error_messages),
                    details={
                        "failed_tables": failed_tables,
                        "total_tables": len(tables_with_changes),
                        "successful_tables": len(tables_with_changes) - len(errors),
                    },
                    operation="push_all_tables",
                )
        else:
            # Execute sequentially
            for table in tables_with_changes.values():
                await table.push()

        # Refresh all tables
        await self.load_tables()

    async def pull(self) -> None:
        """Refresh all tables with current database data (async version)."""
        await self.load_tables()

    async def add_table(self, table: AsyncTableDataFrame, push: bool = False) -> None:
        """Add a new table to the database."""
        self.db[table.name] = table
        object.__setattr__(table, "db", self)
        object.__setattr__(table, "engine", self.engine)
        object.__setattr__(table, "schema", self.schema)
        if push:
            await self.push()
