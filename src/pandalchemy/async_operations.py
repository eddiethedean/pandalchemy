"""
Async SQL operations for pandalchemy.

This module provides async versions of SQL operations using SQLAlchemy's
async engine and async database drivers.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import text

from pandalchemy.exceptions import TransactionError
from pandalchemy.execution_plan import ExecutionPlan, OperationType
from pandalchemy.utils import convert_numpy_types, convert_records_list


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


async def async_pull_table(
    engine: AsyncEngine,
    table_name: str,
    schema: str | None = None,
    primary_key: str | list[str] | None = None,
    set_index: bool = True,
) -> pd.DataFrame:
    """
    Pull a table from the database into a DataFrame (async version).

    Args:
        engine: SQLAlchemy async engine
        table_name: Name of the table to pull
        schema: Optional schema name
        primary_key: Primary key column(s) to set as index
        set_index: Whether to set primary key as index (default True)

    Returns:
        DataFrame with PK as index if set_index=True and primary_key provided
    """
    dialect = engine.dialect.name
    use_direct_sql = dialect in ("mysql", "postgresql")

    # Ensure greenlet context is established before connection
    await _ensure_greenlet_context()

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

    Raises:
        TransactionError: If any SQL operation fails
    """
    if not plan.has_changes():
        return

    # Separate schema changes from data changes
    schema_changes = [
        step for step in plan.steps if step.operation_type == OperationType.SCHEMA_CHANGE
    ]
    data_changes = [
        step for step in plan.steps if step.operation_type != OperationType.SCHEMA_CHANGE
    ]

    # Execute schema changes first
    # Note: Schema changes may need special handling for async
    # For now, we'll execute them synchronously via sync engine if needed
    # This is a limitation - transmutation doesn't have async support yet
    from sqlalchemy import create_engine

    from pandalchemy.sql_operations import _execute_schema_change

    sync_engine = create_engine(
        str(engine.url).replace("+asyncpg", "").replace("+aiomysql", "").replace("+aiosqlite", "")
    )
    for step in schema_changes:
        _execute_schema_change(sync_engine, table_name, step.data, schema)

    # Execute data changes within an async transaction
    if data_changes:
        # Ensure greenlet context is established before connection
        await _ensure_greenlet_context()

        async with engine.begin() as connection:
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
                raise TransactionError(
                    f"Failed to execute plan: {str(e)}",
                    details={"table": table_name, "schema": schema},
                ) from e


async def _async_execute_deletes(
    connection: Any,
    table_name: str,
    delete_keys: list[Any],
    primary_key: str | list[str] | None,
    schema: str | None = None,
) -> None:
    """Execute delete operations (async version)."""
    if not delete_keys or not primary_key:
        return

    from sqlalchemy import Column, Integer, MetaData, Table, and_

    clean_keys = [convert_numpy_types(key) for key in delete_keys]
    metadata = MetaData()

    if isinstance(primary_key, str):
        pk_col = Column(primary_key, Integer)
        table = Table(table_name, metadata, pk_col, schema=schema)
        for key in clean_keys:
            stmt = table.delete().where(table.c[primary_key] == key)
            await connection.execute(stmt)
    else:
        pk_cols = [Column(pk_name, Integer) for pk_name in primary_key]
        table = Table(table_name, metadata, *pk_cols, schema=schema)
        for key_value in clean_keys:
            if isinstance(key_value, (tuple, list)) and len(key_value) == len(primary_key):
                conditions = [table.c[pk_col] == val for pk_col, val in zip(primary_key, key_value)]
                stmt = table.delete().where(and_(*conditions))
                await connection.execute(stmt)


async def _async_execute_updates(
    connection: Any,
    table_name: str,
    update_records: list[dict[str, Any]],
    primary_key: str | list[str] | None,
    schema: str | None = None,
) -> None:
    """Execute update operations (async version)."""
    if not update_records or not primary_key:
        return

    from sqlalchemy import Column, MetaData, Table, and_

    # Get column info using sync engine (inspector doesn't support async yet)
    # Use the connection's engine - for async connections, get the async engine
    engine_for_inspect = connection.engine if hasattr(connection, "engine") else connection

    # Convert async engine URL to sync
    from sqlalchemy import create_engine

    engine_url = str(engine_for_inspect.url)
    sync_url = engine_url.replace("+asyncpg", "").replace("+aiomysql", "").replace("+aiosqlite", "")
    sync_engine = create_engine(sync_url)

    inspector = inspect(sync_engine)
    columns_info = inspector.get_columns(table_name, schema=schema)

    # Build table structure from column info
    metadata = MetaData()
    columns = []
    for col_info in columns_info:
        col_name = col_info["name"]
        col_type = col_info["type"]
        columns.append(Column(col_name, col_type))

    table = Table(table_name, metadata, *columns, schema=schema)

    pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    for record in update_records:
        if len(pk_cols) == 1:
            pk_value = convert_numpy_types(record.get(pk_cols[0]))
            if pk_value is None:
                continue

            update_values = {
                k: convert_numpy_types(v) for k, v in record.items() if k != pk_cols[0]
            }

            stmt = table.update().where(table.c[pk_cols[0]] == pk_value).values(**update_values)
        else:
            if not all(pk_col in record for pk_col in pk_cols):
                continue

            conditions = []
            for pk_col in pk_cols:
                pk_val = convert_numpy_types(record.get(pk_col))
                if pk_val is None:
                    continue
                conditions.append(table.c[pk_col] == pk_val)

            if not conditions or len(conditions) != len(pk_cols):
                continue

            update_values = {
                k: convert_numpy_types(v) for k, v in record.items() if k not in pk_cols
            }

            stmt = table.update().where(and_(*conditions)).values(**update_values)

        await connection.execute(stmt)


async def _async_execute_inserts(
    connection: Any,
    table_name: str,
    insert_records: list[dict[str, Any]],
    schema: str | None = None,
) -> None:
    """Execute insert operations (async version)."""
    if not insert_records:
        return

    from sqlalchemy import Column, MetaData, Table, insert

    clean_records = convert_records_list(insert_records)

    if not clean_records:
        return

    # Get table structure using sync engine for introspection
    engine_for_inspect = connection.engine if hasattr(connection, "engine") else connection

    # Convert async engine URL to sync
    from sqlalchemy import create_engine

    engine_url = str(engine_for_inspect.url)
    sync_url = engine_url.replace("+asyncpg", "").replace("+aiomysql", "").replace("+aiosqlite", "")
    sync_engine = create_engine(sync_url)

    inspector = inspect(sync_engine)
    columns_info = inspector.get_columns(table_name, schema=schema)

    metadata = MetaData()
    columns = []
    for col_info in columns_info:
        col_name = col_info["name"]
        col_type = col_info["type"]
        columns.append(Column(col_name, col_type))

    table = Table(table_name, metadata, *columns, schema=schema)

    # Execute inserts
    await connection.execute(insert(table), clean_records)
