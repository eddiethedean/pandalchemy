"""
Async versions of DataBase and TableDataFrame.

This module provides async-compatible classes that mirror the synchronous
API but use async/await for all database operations.
"""

from __future__ import annotations

from typing import Any

from pandas import DataFrame
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncEngine

from pandalchemy.change_tracker import ChangeTracker
from pandalchemy.sql_operations import table_exists
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
        """
        # Convert async engine to sync for base class operations
        # The async engine will be stored separately
        sync_engine = None
        if engine is not None:
            # Create a sync engine from async engine URL
            from sqlalchemy import create_engine

            sync_engine = create_engine(
                str(engine.url)
                .replace("+asyncpg", "")
                .replace("+aiomysql", "")
                .replace("+aiosqlite", "")
            )

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

        # Store async engine separately
        object.__setattr__(self, "_async_engine", engine)

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
        from pandalchemy.async_operations import (
            _ensure_greenlet_context,
            async_execute_plan,
        )
        from pandalchemy.exceptions import DataValidationError, SchemaError
        from pandalchemy.sql_operations import create_table_from_dataframe

        # Workaround: Ensure greenlet context before async operations
        # TODO: Remove this once pytest-green-light fixes greenlet context persistence
        await _ensure_greenlet_context()

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

        # Check if table exists (use sync engine for this check)
        sync_engine = self.engine
        if sync_engine is None:
            from sqlalchemy import create_engine

            sync_engine = create_engine(
                str(engine_to_use.url)
                .replace("+asyncpg", "")
                .replace("+aiomysql", "")
                .replace("+aiosqlite", "")
            )

        if not table_exists(sync_engine, self.name, self.schema):
            # Create new table (use sync for schema creation)
            create_table_from_dataframe(
                sync_engine, self.name, current_df, self._primary_key, self.schema, if_exists="fail"
            )
        else:
            # Detect and resolve conflicts before executing plan
            plan = self._build_plan_with_conflict_resolution(current_df)

            # Execute the plan (async)
            await async_execute_plan(engine_to_use, self.name, plan, self.schema, self._primary_key)

        # Refresh the table after successful push
        await self.pull()

    async def pull(  # type: ignore[override]
        self, engine: AsyncEngine | None = None, schema: str | None = None
    ) -> None:
        """
        Refresh the table with current database data (async version).

        Args:
            engine: Optional async engine to use
            schema: Optional schema to use
        """
        from pandalchemy.async_operations import _ensure_greenlet_context, async_pull_table
        from pandalchemy.sql_operations import get_primary_key

        # Workaround: Ensure greenlet context before async operations
        # TODO: Remove this once pytest-green-light fixes greenlet context persistence
        await _ensure_greenlet_context()

        if engine is not None:
            object.__setattr__(self, "_async_engine", engine)
        if schema is not None:
            object.__setattr__(self, "schema", schema)

        if self._async_engine and self.name:
            # Get primary key if not set
            if self._primary_key is None:
                # Use sync engine for introspection
                from sqlalchemy import create_engine  # type: ignore[unreachable]

                sync_engine = create_engine(
                    str(self._async_engine.url)
                    .replace("+asyncpg", "")
                    .replace("+aiomysql", "")
                    .replace("+aiosqlite", "")
                )
                self._primary_key = get_primary_key(sync_engine, self.name, self.schema)
                if self._primary_key is None:
                    self._primary_key = "id"

            # Pull table data (async)
            data = await async_pull_table(
                self._async_engine, self.name, self.schema, self._primary_key, set_index=True
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

    def __init__(self, engine: AsyncEngine, lazy: bool = False, schema: str | None = None):
        """
        Initialize an AsyncDataBase instance.

        Args:
            engine: SQLAlchemy async engine
            lazy: If True, tables are loaded only when accessed
            schema: Optional schema name
        """
        self.engine = engine
        self.schema = schema
        self.lazy = lazy

        self.db: dict[str, AsyncTableDataFrame] = {}
        # Note: _load_tables needs to be async, so we'll defer it
        # User should call await db.load_tables() after initialization

    async def load_tables(self) -> None:
        """
        Load or reload all tables from the database (async version).

        This method should be called after initialization to populate tables.
        """
        from pandalchemy.async_operations import _ensure_greenlet_context

        # Workaround: Ensure greenlet context before async operations
        # TODO: Remove this once pytest-green-light fixes greenlet context persistence
        await _ensure_greenlet_context()

        # Clear existing tables
        self.db.clear()

        # Use sync engine for introspection (inspect doesn't have async support yet)
        from sqlalchemy import create_engine

        sync_engine = create_engine(
            str(self.engine.url)
            .replace("+asyncpg", "")
            .replace("+aiomysql", "")
            .replace("+aiosqlite", "")
        )
        inspector = inspect(sync_engine)
        table_names = inspector.get_table_names(schema=self.schema)

        if not self.lazy:
            for name in table_names:
                self.db[name] = AsyncTableDataFrame(
                    name=name, engine=self.engine, db=self, schema=self.schema
                )
                # Load table data asynchronously
                await self.db[name].pull()
        else:
            for name in table_names:
                self.db[name] = None  # type: ignore

    def __getitem__(self, key: str) -> AsyncTableDataFrame:
        """Get a table by name."""
        if self.lazy and self.db.get(key) is None:
            self.db[key] = AsyncTableDataFrame(
                name=key, engine=self.engine, db=self, schema=self.schema
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
        """
        import asyncio

        from pandalchemy.async_operations import _ensure_greenlet_context

        # Workaround: Ensure greenlet context before async operations
        # TODO: Remove this once pytest-green-light fixes greenlet context persistence
        await _ensure_greenlet_context()

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
                    from sqlalchemy import create_engine

                    from pandalchemy.sql_operations import table_exists

                    sync_engine = create_engine(
                        str(table._async_engine.url)
                        .replace("+asyncpg", "")
                        .replace("+aiomysql", "")
                        .replace("+aiosqlite", "")
                    )
                    if not table_exists(sync_engine, table.name, table.schema):
                        should_push = True
                    sync_engine.dispose()

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
            # Execute in parallel using asyncio
            tasks = [table.push() for table in tables_with_changes.values()]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for errors
            errors = [
                (name, result)
                for name, result in zip(tables_with_changes.keys(), results)
                if isinstance(result, Exception)
            ]

            if errors:
                from pandalchemy.exceptions import TransactionError

                error_messages = [f"{name}: {str(e)}" for name, e in errors]
                raise TransactionError(
                    f"Failed to push {len(errors)} table(s): " + "; ".join(error_messages),
                    details={"failed_tables": [name for name, _ in errors]},
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
