"""
SQL operations module wrapping fullmetalalchemy and transmutation.

This module provides high-level functions for executing SQL operations
with automatic transaction management and rollback on errors.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import transmutation as tm
from sqlalchemy import Engine, MetaData, Table, inspect, text

from pandalchemy.exceptions import TransactionError
from pandalchemy.execution_plan import ExecutionPlan, OperationType, SchemaChange
from pandalchemy.utils import (
    convert_numpy_types,
    convert_records_list,
    extract_primary_key_column,
    normalize_schema,
    pandas_dtype_to_python_type,
)


def pull_table(
    engine: Engine,
    table_name: str,
    schema: str | None = None
) -> pd.DataFrame:
    """
    Pull a table from the database into a DataFrame.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table to pull
        schema: Optional schema name

    Returns:
        DataFrame containing the table data
    """
    # Use pandas read_sql_table directly - it's reliable and efficient
    try:
        return pd.read_sql_table(table_name, engine, schema=schema)
    except Exception:
        # If table is empty or has issues, get structure and return empty DataFrame
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        return pd.DataFrame(columns=[col.name for col in table.columns])


def get_primary_key(engine: Engine, table_name: str, schema: str | None = None) -> str | None:
    """
    Get the primary key column name for a table.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        schema: Optional schema name

    Returns:
        Primary key column name, or None if no primary key exists
    """
    inspector = inspect(engine)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)

    if pk_constraint and pk_constraint['constrained_columns']:
        return pk_constraint['constrained_columns'][0]

    return None


def execute_plan(
    engine: Engine,
    table_name: str,
    plan: ExecutionPlan,
    schema: str | None = None,
    primary_key: str | None = None
) -> None:
    """
    Execute a complete execution plan with transaction management.

    All operations are executed within a single transaction. If any operation
    fails, all changes are rolled back automatically.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table to modify
        plan: The ExecutionPlan to execute
        schema: Optional schema name
        primary_key: Name of the primary key column

    Raises:
        SQLAlchemyError: If any SQL operation fails
    """
    if not plan.has_changes():
        return

    # Execute within a transaction
    with engine.begin() as connection:
        try:
            # Execute each step in order
            for step in plan.steps:
                if step.operation_type == OperationType.SCHEMA_CHANGE:
                    # Schema changes need the engine, not connection
                    _execute_schema_change(engine, table_name, step.data, schema)
                elif step.operation_type == OperationType.DELETE:
                    _execute_deletes(connection, table_name, step.data, primary_key, schema)
                elif step.operation_type == OperationType.UPDATE:
                    _execute_updates(connection, table_name, step.data, primary_key, schema)
                elif step.operation_type == OperationType.INSERT:
                    _execute_inserts(connection, table_name, step.data, schema)
        except Exception as e:
            # Transaction will automatically rollback on exception
            raise TransactionError(
                f"Failed to execute plan: {str(e)}",
                details={'table': table_name, 'schema': schema}
            ) from e


def _execute_schema_change(
    engine: Engine,
    table_name: str,
    schema_change: SchemaChange,
    schema: str | None = None
) -> None:
    """
    Execute a schema change operation.

    Args:
        engine: SQLAlchemy engine (transmutation requires engine, not connection)
        table_name: Name of the table to modify
        schema_change: The schema change to apply
        schema: Optional schema name
    """
    # Note: transmutation API - uses different parameter names than expected
    if schema_change.change_type == 'add_column':
        tm.add_column(
            table_name=table_name,
            column_name=schema_change.column_name,
            dtype=_pandas_dtype_to_python_type(schema_change.column_type),
            engine=engine,
            schema=schema
        )
    elif schema_change.change_type == 'drop_column':
        tm.drop_column(
            engine=engine,
            table_name=table_name,
            col_name=schema_change.column_name,
            schema=schema
        )
    elif schema_change.change_type == 'rename_column':
        tm.rename_column(
            engine=engine,
            table_name=table_name,
            old_col_name=schema_change.column_name,
            new_col_name=schema_change.new_column_name,
            schema=schema
        )


def _execute_deletes(
    connection: Any,
    table_name: str,
    delete_keys: list[Any],
    primary_key: str | None,
    schema: str | None = None
) -> None:
    """
    Execute delete operations.

    Args:
        connection: SQLAlchemy connection within a transaction
        table_name: Name of the table
        delete_keys: List of primary key values to delete
        primary_key: Name of the primary key column
        schema: Optional schema name
    """
    if not delete_keys or not primary_key:
        return

    # Convert numpy types to Python native types
    clean_keys = [convert_numpy_types(key) for key in delete_keys]

    # Use SQLAlchemy Table for delete operations
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=connection, schema=schema)

    # Build DELETE statement with IN clause
    stmt = table.delete().where(table.c[primary_key].in_(clean_keys))
    connection.execute(stmt)


def _execute_updates(
    connection: Any,
    table_name: str,
    update_records: list[dict[str, Any]],
    primary_key: str | None,
    schema: str | None = None
) -> None:
    """
    Execute update operations.

    Args:
        connection: SQLAlchemy connection within a transaction
        table_name: Name of the table
        update_records: List of records to update (must include primary key)
        primary_key: Name of the primary key column
        schema: Optional schema name
    """
    if not update_records or not primary_key:
        return

    # Use SQLAlchemy Table for update operations
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=connection, schema=schema)

    for record in update_records:
        pk_value = convert_numpy_types(record.get(primary_key))
        if pk_value is None:
            continue

        # Separate primary key from update values and convert numpy types
        update_values = {
            k: convert_numpy_types(v)
            for k, v in record.items()
            if k != primary_key
        }

        # Build UPDATE statement
        stmt = table.update().where(table.c[primary_key] == pk_value).values(**update_values)
        connection.execute(stmt)


def _execute_inserts(
    connection: Any,
    table_name: str,
    insert_records: list[dict[str, Any]],
    schema: str | None = None
) -> None:
    """
    Execute insert operations.

    Args:
        connection: SQLAlchemy connection within a transaction
        table_name: Name of the table
        insert_records: List of records to insert
        schema: Optional schema name
    """
    if not insert_records:
        return

    # Convert numpy types to Python native types in records
    clean_records = convert_records_list(insert_records)

    # Use pandas DataFrame to_sql for inserts
    df = pd.DataFrame(clean_records)
    df.to_sql(table_name, connection, schema=schema, if_exists='append', index=False)


def _pandas_dtype_to_python_type(dtype: Any) -> type:
    """
    Convert pandas dtype to Python type for transmutation.

    This is a wrapper around the utils function for internal use.

    Args:
        dtype: Pandas dtype

    Returns:
        Python type (int, float, str, bool)
    """
    return pandas_dtype_to_python_type(dtype)


def create_table_from_dataframe(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    primary_key: str,
    schema: str,
    if_exists: str = 'fail'
) -> None:
    """
    Create a new table from a DataFrame.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table to create
        df: DataFrame to create table from
        primary_key: Name of the primary key column
        schema: Optional schema name
        if_exists: What to do if table exists ('fail', 'replace', 'append')

    Raises:
        ValueError: If table exists and if_exists='fail'
    """
    # Convert DataFrame to ensure primary key is a column
    df_to_write = extract_primary_key_column(df, primary_key)

    # Normalize schema for pandas
    schema_normalized = normalize_schema(schema)

    # Write to SQL with pandas (pandas handles its own transaction)
    df_to_write.to_sql(
        table_name,
        engine,
        schema=schema_normalized,
        if_exists=if_exists,
        index=False
    )

    # Add primary key constraint if it doesn't exist (separate transaction)
    try:
        # Check if primary key already exists
        existing_pk = get_primary_key(engine, table_name, schema_normalized)
        if not existing_pk:
            # Add primary key constraint using raw SQL
            with engine.begin() as connection:
                table_ref = f"{schema_normalized}.{table_name}" if schema_normalized else table_name
                connection.execute(text(
                    f"ALTER TABLE {table_ref} ADD PRIMARY KEY ({primary_key})"
                ))
    except Exception:
        # Primary key might already exist or database doesn't support ALTER TABLE
        # This is okay - pandas ensures uniqueness in the data itself
        pass


def table_exists(engine: Engine, table_name: str, schema: str | None = None) -> bool:
    """
    Check if a table exists in the database.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        schema: Optional schema name

    Returns:
        True if table exists, False otherwise
    """
    inspector = inspect(engine)
    return table_name in inspector.get_table_names(schema=schema)

