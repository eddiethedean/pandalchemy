"""
Async SQL operations for pandalchemy.

This module provides async versions of SQL operations using SQLAlchemy's
async engine and async database drivers.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import text

from pandalchemy.async_connection_manager import (
    check_connection_health,
    get_connection_pool_status,
    get_sync_engine_cached,
)
from pandalchemy.async_retry import (
    AsyncRetryPolicy,
    async_retry,
    is_deadlock_error,
    is_retryable_error,
)
from pandalchemy.exceptions import ConnectionError, TransactionError
from pandalchemy.execution_plan import ExecutionPlan, OperationType, SchemaChange
from pandalchemy.sql_operations import _calculate_batch_size, _chunk_list
from pandalchemy.utils import convert_numpy_types, convert_records_list


class AsyncGreenletContext:
    """
    Context manager to ensure greenlet context is established.

    This ensures greenlet context is available for SQLAlchemy async operations
    that require it (especially aiosqlite).
    """

    async def __aenter__(self) -> None:
        """Establish greenlet context."""
        await _ensure_greenlet_context()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Clean up (no-op)."""
        pass


async def _ensure_greenlet_context() -> None:
    """
    Ensure greenlet context is established before async database operations.

    This is needed for SQLAlchemy async engines (especially aiosqlite) which
    require greenlet context to be established in the same async context where
    connections are made. This function should be called right before any
    `async with engine.begin()` or similar connection operations.

    NOTE: This is a workaround for pytest-green-light not properly persisting
    greenlet context from its fixture to the test's async context. Once
    pytest-green-light is fixed to establish context in the same async context
    where operations run, this can be removed.

    See: GREENLET_CONTEXT_ISSUE.md in pytest-green-light repository
    """
    try:
        from sqlalchemy.util._concurrency_py3k import greenlet_spawn
    except ImportError:
        try:
            from sqlalchemy.util import greenlet_spawn
        except ImportError:
            return  # greenlet_spawn not available, skip

    if greenlet_spawn is not None:

        def _noop() -> None:
            pass

        await greenlet_spawn(_noop)


@async_retry(policy=AsyncRetryPolicy(max_attempts=3))
async def async_pull_table(
    engine: AsyncEngine,
    table_name: str,
    schema: str | None = None,
    primary_key: str | list[str] | None = None,
    set_index: bool = True,
    timeout: float | None = None,
    check_health: bool = True,
) -> pd.DataFrame:
    """
    Pull a table from the database into a DataFrame (async version).

    Args:
        engine: SQLAlchemy async engine
        table_name: Name of the table to pull
        schema: Optional schema name
        primary_key: Primary key column(s) to set as index
        set_index: Whether to set primary key as index (default True)
        timeout: Optional timeout in seconds for the operation
        check_health: Whether to check connection health before operation (default True)

    Returns:
        DataFrame with PK as index if set_index=True and primary_key provided

    Raises:
        ConnectionError: If connection health check fails or operation times out
    """
    dialect = engine.dialect.name
    use_direct_sql = dialect in ("mysql", "postgresql")

    # Check connection health if requested
    if check_health:
        health_timeout = timeout or 5.0
        if not await check_connection_health(engine, timeout=health_timeout):
            pool_status = get_connection_pool_status(engine)
            raise ConnectionError(
                "Connection health check failed",
                connection_pool_status=pool_status,
                error_code="CONNECTION_HEALTH_CHECK_FAILED",
                operation="pull_table",
                table_name=table_name,
            )

    async with AsyncGreenletContext():
        # Apply timeout if specified
        if timeout:
            async with asyncio.timeout(timeout):
                return await _async_pull_table_internal(
                    engine, table_name, schema, primary_key, set_index, use_direct_sql, dialect
                )
        else:
            return await _async_pull_table_internal(
                engine, table_name, schema, primary_key, set_index, use_direct_sql, dialect
            )


async def _async_pull_table_internal(
    engine: AsyncEngine,
    table_name: str,
    schema: str | None,
    primary_key: str | list[str] | None,
    set_index: bool,
    use_direct_sql: bool,
    dialect: str,
) -> pd.DataFrame:
    """Internal implementation of async_pull_table."""
    async with engine.begin() as connection:
        if use_direct_sql:
            # Use read_sql directly for MySQL/PostgreSQL
            # Build safe query with text()
            if dialect == "mysql":
                # MySQL uses backticks
                if schema is None:
                    query = text(f"SELECT * FROM `{table_name}`")
                else:
                    query = text(f"SELECT * FROM `{schema}`.`{table_name}`")
            elif dialect == "postgresql":
                # PostgreSQL uses double quotes
                if schema is None:
                    query = text(f'SELECT * FROM "{table_name}"')
                else:
                    query = text(f'SELECT * FROM "{schema}"."{table_name}"')
            else:
                # SQLite and others
                if schema is None:
                    query = text(f'SELECT * FROM "{table_name}"')
                else:
                    query = text(f'SELECT * FROM "{schema}"."{table_name}"')

            # Execute query and read into DataFrame
            result = await connection.execute(query)
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)
        else:
            # For SQLite, try using pandas directly (pandas supports async via sync_to_async)
            # For now, we'll use direct SQL for consistency
            if schema is None:
                query = text(f'SELECT * FROM "{table_name}"')
            else:
                query = text(f'SELECT * FROM "{schema}"."{table_name}"')
            result = await connection.execute(query)
            rows = result.fetchall()
            columns = result.keys()
            df = pd.DataFrame(rows, columns=columns)

    # Try to infer better dtypes
    try:
        df = df.infer_objects()
        # Convert columns that look like numbers
        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    converted = pd.to_numeric(df[col], errors="coerce")
                    original_na_count = df[col].isna().sum()
                    converted_na_count = converted.isna().sum()
                    if converted_na_count == original_na_count:
                        df[col] = converted
                except (ValueError, TypeError):
                    pass
    except Exception:
        # If type inference fails, use data as-is
        pass

    # Set primary key as index if requested
    if set_index and primary_key:
        from pandalchemy.pk_utils import normalize_primary_key, set_pk_as_index

        pk_cols = normalize_primary_key(primary_key)

        # Check if all PK columns exist
        if all(col in df.columns for col in pk_cols):
            df = set_pk_as_index(df, pk_cols)

    return df


async def async_execute_plan(
    engine: AsyncEngine,
    table_name: str,
    plan: ExecutionPlan,
    schema: str | None = None,
    primary_key: str | list[str] | None = None,
    timeout: float | None = None,
    check_health: bool = True,
    retry_policy: AsyncRetryPolicy | None = None,
    isolation_level: str | None = None,
) -> None:
    """
    Execute a complete execution plan with transaction management (async version).

    All operations are executed within a single transaction. If any operation
    fails, all changes are rolled back automatically.

    Args:
        engine: SQLAlchemy async engine
        table_name: Name of the table to modify
        plan: The ExecutionPlan to execute
        schema: Optional schema name
        primary_key: Name of the primary key column(s)
        timeout: Optional timeout in seconds for the operation
        check_health: Whether to check connection health before operation (default True)
        retry_policy: Custom retry policy for this operation
        isolation_level: Transaction isolation level (e.g., 'READ_COMMITTED', 'SERIALIZABLE')

    Raises:
        TransactionError: If any SQL operation fails
        ConnectionError: If connection health check fails
    """
    if not plan.has_changes():
        return

    start_time = time.time()

    # Check connection health if requested
    if check_health:
        health_timeout = timeout or 5.0 if timeout else 5.0
        if not await check_connection_health(engine, timeout=health_timeout):
            pool_status = get_connection_pool_status(engine)
            raise ConnectionError(
                "Connection health check failed before executing plan",
                connection_pool_status=pool_status,
                error_code="CONNECTION_HEALTH_CHECK_FAILED",
                operation="execute_plan",
                table_name=table_name,
            )

    # Use default retry policy if not provided
    if retry_policy is None:
        retry_policy = AsyncRetryPolicy(max_attempts=3)

    # Separate schema changes from data changes
    schema_changes = [
        step for step in plan.steps if step.operation_type == OperationType.SCHEMA_CHANGE
    ]
    data_changes = [
        step for step in plan.steps if step.operation_type != OperationType.SCHEMA_CHANGE
    ]

    # Execute schema changes - use async where possible
    for step in schema_changes:
        try:
            schema_change = step.data
            # Check if we can use async for this schema change
            if await _async_schema_change_supported(engine, schema_change):
                await _async_execute_schema_change(engine, table_name, schema_change, schema)
            else:
                # Fall back to sync execution
                sync_engine = get_sync_engine_cached(engine)
                from pandalchemy.sql_operations import _execute_schema_change

                _execute_schema_change(sync_engine, table_name, schema_change, schema)
        except Exception as e:
            raise TransactionError(
                f"Failed to execute schema change: {str(e)}",
                details={"table": table_name, "schema": schema, "step": step.description},
                operation="schema_change",
            ) from e

    # Execute data changes within an async transaction
    if data_changes:
        # Ensure retry_policy is not None for the decorator and exception handler
        effective_retry_policy = retry_policy

        @async_retry(policy=effective_retry_policy, retryable_check=is_retryable_error)
        async def _execute_data_changes() -> None:
            async with AsyncGreenletContext(), engine.begin() as connection:
                # Set isolation level if specified
                if isolation_level and engine.dialect.name in ("postgresql", "mysql"):
                    # Set isolation level (database-specific)
                    await connection.execute(
                        text(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                    )
                    # Note: SQLite doesn't support isolation level changes easily

                try:
                    for step in data_changes:
                        if step.operation_type == OperationType.DELETE:
                            await _async_execute_deletes(
                                connection, table_name, step.data, primary_key, schema
                            )
                        elif step.operation_type == OperationType.UPDATE:
                            await _async_execute_updates(
                                connection, table_name, step.data, primary_key, schema
                            )
                        elif step.operation_type == OperationType.INSERT:
                            await _async_execute_inserts(connection, table_name, step.data, schema)
                except Exception as e:
                    # Check if it's a deadlock - deadlocks will be retried by the decorator
                    if is_deadlock_error(e) and effective_retry_policy:
                        # Add extra delay for deadlocks
                        delay = (
                            effective_retry_policy.calculate_delay(0)
                            + (time.time() - start_time) * 0.1
                        )
                        await asyncio.sleep(min(delay, 1.0))  # Cap at 1 second
                    raise TransactionError(
                        f"Failed to execute plan: {str(e)}",
                        details={"table": table_name, "schema": schema},
                        operation="execute_plan",
                    ) from e

        # Apply timeout if specified
        start_time = time.time()
        try:
            if timeout:
                async with asyncio.timeout(timeout):
                    await _execute_data_changes()
            else:
                await _execute_data_changes()
        except asyncio.TimeoutError as e:
            elapsed = time.time() - start_time
            raise TransactionError(
                f"Operation timed out after {elapsed:.2f}s",
                details={"table": table_name, "schema": schema, "timeout": timeout},
                operation="execute_plan",
                error_code="ASYNCIO_TIMEOUT",
            ) from e


async def _async_schema_change_supported(engine: AsyncEngine, schema_change: SchemaChange) -> bool:
    """
    Check if async schema change is supported for this dialect and change type.

    Args:
        engine: Async engine
        schema_change: The schema change to check

    Returns:
        True if async schema change is supported, False otherwise
    """
    dialect = engine.dialect.name

    # PostgreSQL and MySQL support async DDL operations
    # SQLite async support is limited, so we use sync fallback
    return dialect in ("postgresql", "mysql") and schema_change.change_type in (
        "add_column",
        "drop_column",
        "rename_column",
    )


async def _async_execute_schema_change(
    engine: AsyncEngine,
    table_name: str,
    schema_change: SchemaChange,
    schema: str | None = None,
) -> None:
    """
    Execute a schema change operation using async connection.

    This function uses raw SQL for supported dialects (PostgreSQL, MySQL)
    to avoid the need for sync transmutation library.

    Args:
        engine: Async engine
        table_name: Name of the table
        schema_change: The schema change to apply
        schema: Optional schema name

    Raises:
        TransactionError: If schema change fails
    """
    from pandalchemy.sql_operations import _pandas_dtype_to_python_type

    dialect = engine.dialect.name

    async with AsyncGreenletContext(), engine.begin() as connection:
        # Use async connection for schema changes
        # Handle add_column for PostgreSQL and MySQL
        if schema_change.change_type == "add_column" and dialect in ("postgresql", "mysql"):
            column_type = _pandas_dtype_to_python_type(schema_change.column_type)

            # Map pandas/SQLAlchemy types to database-specific types
            is_mysql = dialect == "mysql"
            if is_mysql:
                type_map = {
                    "Integer": "INTEGER",
                    "Float": "REAL",
                    "Double": "DOUBLE",
                    "Boolean": "BOOLEAN",
                    "String": "VARCHAR(255)",
                    "Text": "TEXT",
                }
            else:  # PostgreSQL
                type_map = {
                    "Integer": "INTEGER",
                    "Float": "REAL",
                    "Double": "DOUBLE PRECISION",
                    "Boolean": "BOOLEAN",
                    "String": "VARCHAR",
                    "Text": "TEXT",
                }
            sql_type = type_map.get(column_type.__name__, "VARCHAR(255)" if is_mysql else "VARCHAR")

            # Build ALTER TABLE statement
            if is_mysql:
                schema_prefix = f"`{schema}`." if schema else ""
                alter_sql = text(
                    f"ALTER TABLE {schema_prefix}`{table_name}` ADD COLUMN `{schema_change.column_name}` {sql_type}"
                )
            else:  # PostgreSQL
                schema_prefix = f'"{schema}".' if schema else ""
                alter_sql = text(
                    f'ALTER TABLE {schema_prefix}"{table_name}" ADD COLUMN "{schema_change.column_name}" {sql_type}'
                )

            await connection.execute(alter_sql)
            return

        # Handle drop_column
        if schema_change.change_type == "drop_column" and dialect in ("postgresql", "mysql"):
            is_mysql = dialect == "mysql"
            if is_mysql:
                schema_prefix = f"`{schema}`." if schema else ""
                alter_sql = text(
                    f"ALTER TABLE {schema_prefix}`{table_name}` DROP COLUMN `{schema_change.column_name}`"
                )
            else:  # PostgreSQL
                schema_prefix = f'"{schema}".' if schema else ""
                alter_sql = text(
                    f'ALTER TABLE {schema_prefix}"{table_name}" DROP COLUMN "{schema_change.column_name}"'
                )

            await connection.execute(alter_sql)
            return

        # Handle rename_column for PostgreSQL
        if schema_change.change_type == "rename_column" and dialect == "postgresql":
            if schema_change.new_column_name is None:
                raise ValueError("new_column_name is required for rename_column operation")

            schema_prefix = f'"{schema}".' if schema else ""
            alter_sql = text(
                f'ALTER TABLE {schema_prefix}"{table_name}" RENAME COLUMN "{schema_change.column_name}" TO "{schema_change.new_column_name}"'
            )

            await connection.execute(alter_sql)
            return

        # Handle rename_column for MySQL (requires type, so we need to get it first)
        if schema_change.change_type == "rename_column" and dialect == "mysql":
            if schema_change.new_column_name is None:
                raise ValueError("new_column_name is required for rename_column operation")

            # Get existing column type using sync engine (inspector doesn't support async)
            sync_engine = get_sync_engine_cached(engine)
            from sqlalchemy import inspect as sync_inspect

            def _get_columns_sync() -> list[dict[str, Any]]:
                sync_inspector = sync_inspect(sync_engine)
                return sync_inspector.get_columns(table_name, schema=schema)

            # Use asyncio.to_thread (Python 3.9+) or run_in_executor as fallback
            try:
                columns_info = await asyncio.to_thread(_get_columns_sync)
            except AttributeError:
                # Fallback for Python < 3.9
                loop = asyncio.get_event_loop()
                columns_info = await loop.run_in_executor(None, _get_columns_sync)
            column_info = next(
                (col for col in columns_info if col["name"] == schema_change.column_name), None
            )

            if column_info is None:
                raise ValueError(
                    f"Column '{schema_change.column_name}' not found in table '{table_name}'"
                )

            existing_type = column_info["type"]
            type_str = str(existing_type)

            schema_prefix = f"`{schema}`." if schema else ""
            alter_sql = text(
                f"ALTER TABLE {schema_prefix}`{table_name}` CHANGE COLUMN `{schema_change.column_name}` `{schema_change.new_column_name}` {type_str}"
            )

            await connection.execute(alter_sql)
            return

    # If we get here, the schema change type or dialect is not supported for async
    # This should not happen if _async_schema_change_supported is called first
    raise ValueError(
        f"Async schema change not supported for dialect '{dialect}' and change type '{schema_change.change_type}'"
    )


async def _async_execute_deletes(
    connection: Any,
    table_name: str,
    delete_keys: list[Any],
    primary_key: str | list[str] | None,
    schema: str | None = None,
) -> None:
    """
    Execute delete operations (async version) with batching support.

    Args:
        connection: Async database connection
        table_name: Name of the table
        delete_keys: List of primary key values to delete
        primary_key: Primary key column name(s)
        schema: Optional schema name
    """
    if not delete_keys or not primary_key:
        return

    from sqlalchemy import Column, Integer, MetaData, Table, and_, or_

    clean_keys = [convert_numpy_types(key) for key in delete_keys]

    # Calculate batch size
    batch_size = _calculate_batch_size(OperationType.DELETE, len(clean_keys))
    batches = _chunk_list(clean_keys, batch_size)

    metadata = MetaData()

    if isinstance(primary_key, str):
        pk_col = Column(primary_key, Integer)
        table = Table(table_name, metadata, pk_col, schema=schema)

        # Batch deletes
        for batch in batches:
            if len(batch) == 1:
                # Single delete
                stmt = table.delete().where(table.c[primary_key] == batch[0])
                await connection.execute(stmt)
            else:
                # Batch delete using OR conditions
                conditions = [table.c[primary_key] == key for key in batch]
                stmt = table.delete().where(or_(*conditions))
                await connection.execute(stmt)
    else:
        pk_cols = [Column(pk_name, Integer) for pk_name in primary_key]
        table = Table(table_name, metadata, *pk_cols, schema=schema)

        # Batch deletes for composite keys
        for batch in batches:
            batch_conditions = []
            for key_value in batch:
                if isinstance(key_value, (tuple, list)) and len(key_value) == len(primary_key):
                    conditions = [
                        table.c[pk_col] == val for pk_col, val in zip(primary_key, key_value)
                    ]
                    batch_conditions.append(and_(*conditions))

            if batch_conditions:
                if len(batch_conditions) == 1:
                    stmt = table.delete().where(batch_conditions[0])
                else:
                    stmt = table.delete().where(or_(*batch_conditions))
                await connection.execute(stmt)


async def _async_execute_updates(
    connection: Any,
    table_name: str,
    update_records: list[dict[str, Any]],
    primary_key: str | list[str] | None,
    schema: str | None = None,
) -> None:
    """
    Execute update operations (async version) with batching support.

    Args:
        connection: Async database connection
        table_name: Name of the table
        update_records: List of records to update (dicts with PK and update values)
        primary_key: Primary key column name(s)
        schema: Optional schema name
    """
    if not update_records or not primary_key:
        return

    from sqlalchemy import Column, MetaData, Table, and_

    # Get column info using cached sync engine
    # We need to get the async engine from the connection to create sync engine
    engine_for_inspect = connection.engine if hasattr(connection, "engine") else connection
    if isinstance(engine_for_inspect, AsyncEngine):
        sync_engine = get_sync_engine_cached(engine_for_inspect)
    else:
        # Already sync engine
        sync_engine = engine_for_inspect

    # Use sync engine introspection - must run in sync context
    # Execute sync operations in a thread to avoid greenlet context issues
    def _get_columns_sync() -> list[dict[str, Any]]:
        inspector = inspect(sync_engine)
        return inspector.get_columns(table_name, schema=schema)

    # Use asyncio.to_thread (Python 3.9+) or run_in_executor as fallback
    try:
        columns_info = await asyncio.to_thread(_get_columns_sync)
    except AttributeError:
        # Fallback for Python < 3.9
        loop = asyncio.get_event_loop()
        columns_info = await loop.run_in_executor(None, _get_columns_sync)

    # Build table structure from column info
    metadata = MetaData()
    columns = []
    for col_info in columns_info:
        col_name = col_info["name"]
        col_type = col_info["type"]
        columns.append(Column(col_name, col_type))

    table = Table(table_name, metadata, *columns, schema=schema)

    pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    # Calculate batch size and process in batches
    batch_size = _calculate_batch_size(OperationType.UPDATE, len(update_records))
    batches = _chunk_list(update_records, batch_size)

    for batch in batches:
        for record in batch:
            if len(pk_cols) == 1:
                pk_value = convert_numpy_types(record.get(pk_cols[0]))
                if pk_value is None:
                    continue

                update_values = {
                    k: convert_numpy_types(v) for k, v in record.items() if k != pk_cols[0]
                }

                if update_values:
                    stmt = (
                        table.update()
                        .where(table.c[pk_cols[0]] == pk_value)
                        .values(**update_values)
                    )
                    await connection.execute(stmt)
            else:
                if not all(pk_col in record for pk_col in pk_cols):
                    continue

                conditions = []
                for pk_col in pk_cols:
                    pk_val = convert_numpy_types(record.get(pk_col))
                    if pk_val is None:
                        break
                    conditions.append(table.c[pk_col] == pk_val)

                if not conditions or len(conditions) != len(pk_cols):
                    continue

                update_values = {
                    k: convert_numpy_types(v) for k, v in record.items() if k not in pk_cols
                }

                if update_values:
                    stmt = table.update().where(and_(*conditions)).values(**update_values)
                    await connection.execute(stmt)


async def _async_execute_inserts(
    connection: Any,
    table_name: str,
    insert_records: list[dict[str, Any]],
    schema: str | None = None,
) -> None:
    """
    Execute insert operations (async version) with batching support.

    Args:
        connection: Async database connection
        table_name: Name of the table
        insert_records: List of records to insert
        schema: Optional schema name
    """
    if not insert_records:
        return

    from sqlalchemy import Column, MetaData, Table, insert

    clean_records = convert_records_list(insert_records)

    if not clean_records:
        return

    # Get table structure using cached sync engine
    engine_for_inspect = connection.engine if hasattr(connection, "engine") else connection
    if isinstance(engine_for_inspect, AsyncEngine):
        sync_engine = get_sync_engine_cached(engine_for_inspect)
    else:
        # Already sync engine
        sync_engine = engine_for_inspect

    # Use sync engine introspection - must run in sync context
    # Execute sync operations in a thread to avoid greenlet context issues
    def _get_columns_sync() -> list[dict[str, Any]]:
        inspector = inspect(sync_engine)
        return inspector.get_columns(table_name, schema=schema)

    # Use asyncio.to_thread (Python 3.9+) or run_in_executor as fallback
    try:
        columns_info = await asyncio.to_thread(_get_columns_sync)
    except AttributeError:
        # Fallback for Python < 3.9
        loop = asyncio.get_event_loop()
        columns_info = await loop.run_in_executor(None, _get_columns_sync)

    metadata = MetaData()
    columns = []
    for col_info in columns_info:
        col_name = col_info["name"]
        col_type = col_info["type"]
        columns.append(Column(col_name, col_type))

    table = Table(table_name, metadata, *columns, schema=schema)

    # Calculate batch size and process in batches
    batch_size = _calculate_batch_size(OperationType.INSERT, len(clean_records))
    batches = _chunk_list(clean_records, batch_size)

    # Execute inserts in batches
    for batch in batches:
        await connection.execute(insert(table), batch)
