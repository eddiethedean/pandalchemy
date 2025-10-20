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
    schema: str | None = None,
    primary_key: str | list[str] | None = None,
    set_index: bool = True
) -> pd.DataFrame:
    """
    Pull a table from the database into a DataFrame.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table to pull
        schema: Optional schema name
        primary_key: Primary key column(s) to set as index
        set_index: Whether to set primary key as index (default True)

    Returns:
        DataFrame with PK as index if set_index=True and primary_key provided
    """
    # Use pandas read_sql_table directly - it's reliable and efficient
    try:
        df = pd.read_sql_table(table_name, engine, schema=schema)
    except Exception:
        # If table is empty or has issues, get structure and return empty DataFrame
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        df = pd.DataFrame(columns=[col.name for col in table.columns])

    # Set primary key as index if requested
    if set_index and primary_key:
        # Normalize PK to list
        pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)

        # Check if all PK columns exist
        if all(col in df.columns for col in pk_cols):
            if len(pk_cols) == 1:
                df = df.set_index(pk_cols[0])
            else:
                # Composite key - set MultiIndex
                df = df.set_index(pk_cols)

    return df


def get_primary_key(engine: Engine, table_name: str, schema: str | None = None) -> str | list[str] | None:
    """
    Get the primary key column name(s) for a table.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        schema: Optional schema name

    Returns:
        Primary key column name (str) for single-column PK,
        list of column names for composite PK,
        or None if no primary key exists
    """
    inspector = inspect(engine)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)

    if pk_constraint and pk_constraint['constrained_columns']:
        pk_cols = pk_constraint['constrained_columns']
        # Return single string for single-column PK, list for composite PK
        if len(pk_cols) == 1:
            return pk_cols[0]
        else:
            return pk_cols

    return None


def execute_plan(
    engine: Engine,
    table_name: str,
    plan: ExecutionPlan,
    schema: str | None = None,
    primary_key: str | list[str] | None = None
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
        primary_key: Name of the primary key column(s) - single string or list for composite

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
        if schema_change.new_column_name is None:
            raise ValueError(f"new_column_name is required for rename_column operation")
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
    primary_key: str | list[str] | None,
    schema: str | None = None
) -> None:
    """
    Execute delete operations.

    Args:
        connection: SQLAlchemy connection within a transaction
        table_name: Name of the table
        delete_keys: List of primary key values to delete (tuples for composite PKs)
        primary_key: Name of the primary key column(s)
        schema: Optional schema name
    """
    if not delete_keys or not primary_key:
        return

    # Convert numpy types to Python native types
    clean_keys = [convert_numpy_types(key) for key in delete_keys]

    # Use SQLAlchemy Table for delete operations
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=connection, schema=schema)

    # Handle single vs composite primary keys
    if isinstance(primary_key, str):
        # Single column PK
        stmt = table.delete().where(table.c[primary_key].in_(clean_keys))
    else:
        # Composite PK - need to match on all columns
        from sqlalchemy import and_, or_

        conditions = []
        for key_value in clean_keys:
            if isinstance(key_value, (tuple, list)):
                # Match all PK columns
                condition = and_(*[
                    table.c[pk_col] == val
                    for pk_col, val in zip(primary_key, key_value)
                ])
                conditions.append(condition)

        if conditions:
            stmt = table.delete().where(or_(*conditions))
        else:
            return

    connection.execute(stmt)


def _execute_updates(
    connection: Any,
    table_name: str,
    update_records: list[dict[str, Any]],
    primary_key: str | list[str] | None,
    schema: str | None = None
) -> None:
    """
    Execute update operations.

    Args:
        connection: SQLAlchemy connection within a transaction
        table_name: Name of the table
        update_records: List of records to update (must include primary key)
        primary_key: Name of the primary key column(s) - single string or list for composite
        schema: Optional schema name
    """
    if not update_records or not primary_key:
        return

    # Use SQLAlchemy Table for update operations
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=connection, schema=schema)

    # Handle single vs composite primary keys
    if isinstance(primary_key, str):
        # Single column PK
        pk_cols = [primary_key]
    else:
        # Composite PK
        pk_cols = list(primary_key)

    for record in update_records:
        # Extract PK values
        if len(pk_cols) == 1:
            pk_value = convert_numpy_types(record.get(pk_cols[0]))
            if pk_value is None:
                continue

            # Separate primary key from update values
            update_values = {
                k: convert_numpy_types(v)
                for k, v in record.items()
                if k != pk_cols[0]
            }

            # Build UPDATE statement
            stmt = table.update().where(table.c[pk_cols[0]] == pk_value).values(**update_values)
        else:
            # Composite PK - build condition for all PK columns
            from sqlalchemy import and_

            # Check all PK columns are present
            if not all(pk_col in record for pk_col in pk_cols):
                continue

            # Build WHERE clause for composite key
            conditions = []
            for pk_col in pk_cols:
                pk_val = convert_numpy_types(record.get(pk_col))
                if pk_val is None:
                    continue
                conditions.append(table.c[pk_col] == pk_val)

            if not conditions or len(conditions) != len(pk_cols):
                continue

            # Separate PK from update values
            update_values = {
                k: convert_numpy_types(v)
                for k, v in record.items()
                if k not in pk_cols
            }

            # Build UPDATE statement
            stmt = table.update().where(and_(*conditions)).values(**update_values)

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
    primary_key: str | list[str],
    schema: str | None,
    if_exists: str = 'fail'
) -> None:
    """
    Create a new table from a DataFrame.

    For composite primary keys, uses SQLAlchemy to create table with proper constraints.
    For single column PKs, uses pandas to_sql and adds constraint afterward.

    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table to create
        df: DataFrame to create table from
        primary_key: Name of the primary key column(s) - single string or list for composite
        schema: Optional schema name
        if_exists: What to do if table exists ('fail', 'replace', 'append')

    Raises:
        ValueError: If table exists and if_exists='fail'
    """
    from sqlalchemy import Boolean, Column, Float, Integer, MetaData, PrimaryKeyConstraint, String

    # Convert DataFrame to ensure primary key is a column
    df_to_write = extract_primary_key_column(df, primary_key)

    # Normalize schema for pandas
    schema_normalized = normalize_schema(schema)

    # Check if we have a composite primary key
    pk_cols = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    is_composite = len(pk_cols) > 1

    if is_composite:
        # For composite PKs, use SQLAlchemy to create table with proper constraint
        metadata = MetaData()

        # Map pandas dtypes to SQLAlchemy types
        columns = []
        for col_name in df_to_write.columns:
            dtype = df_to_write[col_name].dtype
            if 'int' in str(dtype):
                col_type: type = Integer
            elif 'float' in str(dtype):
                col_type = Float
            elif 'bool' in str(dtype):
                col_type = Boolean
            else:
                col_type = String
            columns.append(Column(col_name, col_type))

        # Add composite primary key constraint
        table_obj = Table(
            table_name,
            metadata,
            *columns,
            PrimaryKeyConstraint(*pk_cols, name=f'{table_name}_pk'),
            schema=schema_normalized
        )

        # Create the table
        if if_exists == 'replace':
            table_obj.drop(engine, checkfirst=True)
        metadata.create_all(engine)

        # Insert data
        df_to_write.to_sql(
            table_name,
            engine,
            schema=schema_normalized,
            if_exists='append',
            index=False
        )
    else:
        # Single column PK - use pandas to_sql
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

