"""
SQL operations module wrapping fullmetalalchemy and transmutation.

This module provides high-level functions for executing SQL operations
with automatic transaction management and rollback on errors.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import transmutation as tm
from sqlalchemy import Engine, MetaData, Table, inspect

from pandalchemy.exceptions import TransactionError
from pandalchemy.execution_plan import ExecutionPlan, OperationType, SchemaChange
from pandalchemy.utils import (
    convert_numpy_types,
    convert_records_list,
    extract_primary_key_column,
    normalize_schema,
    pandas_dtype_to_python_type,
)

# Adaptive batch size configuration
# These values can be adjusted based on database type and connection performance
DEFAULT_BATCH_SIZE_INSERT = 1000  # Insert batch size
DEFAULT_BATCH_SIZE_UPDATE = 500  # Update batch size (smaller due to WHERE clauses)
DEFAULT_BATCH_SIZE_DELETE = 1000  # Delete batch size


def _calculate_batch_size(operation_type: OperationType, record_count: int) -> int:
    """
    Calculate optimal batch size based on operation type and record count.

    Uses adaptive sizing: smaller batches for very large operations,
    larger batches for small operations to reduce round trips.

    Args:
        operation_type: Type of operation (INSERT, UPDATE, DELETE)
        record_count: Total number of records to process

    Returns:
        Optimal batch size for chunking
    """
    if operation_type == OperationType.INSERT:
        base_size = DEFAULT_BATCH_SIZE_INSERT
        # For very large inserts (>100k), use smaller batches to avoid timeouts
        if record_count > 100000:
            return base_size // 2
        # For small operations (<1k), use larger batches
        elif record_count < 1000:
            return base_size * 2
        return base_size
    elif operation_type == OperationType.UPDATE:
        base_size = DEFAULT_BATCH_SIZE_UPDATE
        # Updates are slower due to WHERE clauses, so use smaller batches for large ops
        if record_count > 50000:
            return base_size // 2
        elif record_count < 500:
            return base_size * 2
        return base_size
    elif operation_type == OperationType.DELETE:
        base_size = DEFAULT_BATCH_SIZE_DELETE
        # Similar to inserts but can be slower with constraints
        if record_count > 100000:
            return base_size // 2
        elif record_count < 1000:
            return base_size * 2
        return base_size
    else:
        # Default batch size for unknown operations
        return 1000


def _chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    """
    Split a list into chunks of specified size.

    Args:
        items: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def pull_table(
    engine: Engine,
    table_name: str,
    schema: str | None = None,
    primary_key: str | list[str] | None = None,
    set_index: bool = True,
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
    # For MySQL and PostgreSQL, use pd.read_sql directly to avoid potential hangs
    # pd.read_sql_table uses inspect() internally which can hang after schema changes
    dialect = engine.dialect.name
    use_direct_sql = dialect in ("mysql", "postgresql")

    if not use_direct_sql:
        # For SQLite and others, try read_sql_table first for better type inference
        try:
            df = pd.read_sql_table(table_name, engine, schema=schema)
            # Success with read_sql_table, skip to dtype inference
        except (ValueError, TypeError):
            use_direct_sql = True  # Fall through to direct SQL

    if use_direct_sql:
        # Use read_sql directly for MySQL/PostgreSQL to avoid inspect() hangs
        # or as fallback for SQLite when read_sql_table fails
        try:
            # Use read_sql which is more lenient with type conversions
            # Build safe query with text() - table/schema names are from database introspection
            from sqlalchemy import text

            # Build query safely with database-specific quoting
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
            df = pd.read_sql(query, engine)
        except Exception as e:
            # If all else fails, return empty DataFrame with correct structure
            import warnings

            warnings.warn(
                f"Failed to read table '{table_name}' from database, returning empty DataFrame. "
                f"Error: {type(e).__name__}: {str(e)}",
                UserWarning,
                stacklevel=2,
            )
            # Use inspect() instead of autoload_with to avoid potential hangs
            # Similar to PostgreSQL, use explicit connection management
            inspector = inspect(engine)
            columns_info = inspector.get_columns(table_name, schema=schema)
            df = pd.DataFrame(columns=[col["name"] for col in columns_info])
            return df

    # Try to infer better dtypes - convert string representations of numbers
    # This handles cases where schema says FLOAT but data is stored as strings
    try:
        df = df.infer_objects()
        # Convert columns that look like numbers
        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    # Try numeric conversion, keeping NaN for non-numeric values
                    converted = pd.to_numeric(df[col], errors="coerce")
                    # Only apply if we didn't lose information (no new NaNs)
                    original_na_count = df[col].isna().sum()
                    converted_na_count = converted.isna().sum()
                    if converted_na_count == original_na_count:
                        df[col] = converted
                except (ValueError, TypeError):
                    # If it fails, keep as object
                    pass
    except Exception as e:
        # If type inference fails, use data as-is
        import warnings

        warnings.warn(
            f"Type inference failed for table '{table_name}', using data as-is. "
            f"Error: {type(e).__name__}: {str(e)}",
            UserWarning,
            stacklevel=2,
        )

    # Set primary key as index if requested
    if set_index and primary_key:
        from pandalchemy.pk_utils import normalize_primary_key, set_pk_as_index

        # Normalize PK to list
        pk_cols = normalize_primary_key(primary_key)

        # Check if all PK columns exist
        if all(col in df.columns for col in pk_cols):
            df = set_pk_as_index(df, pk_cols)

    return df


def get_primary_key(
    engine: Engine, table_name: str, schema: str | None = None
) -> str | list[str] | None:
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
    # inspect(engine) manages connections internally
    inspector = inspect(engine)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)

    if pk_constraint and pk_constraint["constrained_columns"]:
        pk_cols = pk_constraint["constrained_columns"]
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
    primary_key: str | list[str] | None = None,
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

    # Separate schema changes from data changes
    # Schema changes must execute OUTSIDE any transaction to avoid metadata lock conflicts
    schema_changes = [
        step for step in plan.steps if step.operation_type == OperationType.SCHEMA_CHANGE
    ]
    data_changes = [
        step for step in plan.steps if step.operation_type != OperationType.SCHEMA_CHANGE
    ]

    # Execute schema changes first - use direct connection to ensure immediate commit
    # This prevents PostgreSQL metadata lock conflicts
    for step in schema_changes:
        # Use a fresh connection outside any transaction for schema changes
        # transmutation needs engine but we ensure it gets a clean connection
        conn = engine.connect()
        try:
            # Execute schema change - it will use engine internally but we ensure connection is clean
            _execute_schema_change(engine, table_name, step.data, schema)
            # Explicitly commit any operations on this connection
            conn.commit()
        finally:
            conn.close()

    # Execute data changes within a transaction
    if data_changes:
        with engine.begin() as connection:
            try:
                for step in data_changes:
                    if step.operation_type == OperationType.DELETE:
                        _execute_deletes(connection, table_name, step.data, primary_key, schema)
                    elif step.operation_type == OperationType.UPDATE:
                        _execute_updates(connection, table_name, step.data, primary_key, schema)
                    elif step.operation_type == OperationType.INSERT:
                        _execute_inserts(connection, table_name, step.data, schema)
            except Exception as e:
                # Transaction will automatically rollback on exception
                raise TransactionError(
                    f"Failed to execute plan: {str(e)}",
                    details={"table": table_name, "schema": schema},
                ) from e


def _execute_schema_change(
    engine: Engine, table_name: str, schema_change: SchemaChange, schema: str | None = None
) -> None:
    """
    Execute a schema change operation.

    Args:
        engine: SQLAlchemy engine (transmutation requires engine, not connection)
        table_name: Name of the table to modify
        schema_change: The schema change to apply
        schema: Optional schema name
    """
    from sqlalchemy import text

    # For PostgreSQL and MySQL, use raw SQL to avoid issues:
    # - PostgreSQL: transmutation's inspect() can hang due to metadata locks
    # - MySQL: transmutation doesn't handle VARCHAR length requirement and rename_column requires type
    # This bypasses these issues by using direct SQL
    if engine.dialect.name in ("postgresql", "mysql") and schema_change.change_type == "add_column":
        column_type = _pandas_dtype_to_python_type(schema_change.column_type)

        # Map pandas/SQLAlchemy types to database-specific types
        is_mysql = engine.dialect.name == "mysql"
        if is_mysql:
            type_map = {
                "Integer": "INTEGER",
                "Float": "REAL",
                "Double": "DOUBLE",
                "Boolean": "BOOLEAN",
                "String": "VARCHAR(255)",  # MySQL requires VARCHAR length
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

        # Build ALTER TABLE statement with database-specific quoting
        if is_mysql:
            schema_prefix = f"`{schema}`." if schema else ""
            alter_sql = f"ALTER TABLE {schema_prefix}`{table_name}` ADD COLUMN `{schema_change.column_name}` {sql_type}"
        else:  # PostgreSQL
            schema_prefix = f'"{schema}".' if schema else ""
            alter_sql = f'ALTER TABLE {schema_prefix}"{table_name}" ADD COLUMN "{schema_change.column_name}" {sql_type}'

        # Execute with fresh connection outside any transaction
        # This ensures the schema change commits immediately, avoiding lock conflicts
        conn = engine.connect()
        try:
            conn.execute(text(alter_sql))
            conn.commit()
        finally:
            conn.close()
        return

    # For MySQL rename_column, use raw SQL with existing column type
    # MySQL's CHANGE COLUMN requires the existing column type
    if engine.dialect.name == "mysql" and schema_change.change_type == "rename_column":
        if schema_change.new_column_name is None:
            raise ValueError("new_column_name is required for rename_column operation")

        # Get existing column type from database
        inspector = inspect(engine)
        columns_info = inspector.get_columns(table_name, schema=schema)
        column_info = next(
            (col for col in columns_info if col["name"] == schema_change.column_name), None
        )

        if column_info is None:
            raise ValueError(
                f"Column '{schema_change.column_name}' not found in table '{table_name}'"
            )

        # Convert SQLAlchemy type to MySQL type string
        existing_type = column_info["type"]
        type_str = str(existing_type)

        # MySQL uses backticks for identifiers
        schema_prefix = f"`{schema}`." if schema else ""
        alter_sql = f"ALTER TABLE {schema_prefix}`{table_name}` CHANGE COLUMN `{schema_change.column_name}` `{schema_change.new_column_name}` {type_str}"

        # Execute with fresh connection outside any transaction
        conn = engine.connect()
        try:
            conn.execute(text(alter_sql))
            conn.commit()
        finally:
            conn.close()
        return

    # For PostgreSQL rename_column, use RENAME COLUMN (doesn't require type)
    if engine.dialect.name == "postgresql" and schema_change.change_type == "rename_column":
        if schema_change.new_column_name is None:
            raise ValueError("new_column_name is required for rename_column operation")

        # PostgreSQL uses double quotes for identifiers
        schema_prefix = f'"{schema}".' if schema else ""
        alter_sql = f'ALTER TABLE {schema_prefix}"{table_name}" RENAME COLUMN "{schema_change.column_name}" TO "{schema_change.new_column_name}"'

        # Execute with fresh connection outside any transaction
        conn = engine.connect()
        try:
            conn.execute(text(alter_sql))
            conn.commit()
        finally:
            conn.close()
        return

    # Use transmutation for other databases or change types
    # Wrap transmutation calls to convert ValidationError to TransactionError
    try:
        if schema_change.change_type == "add_column":
            tm.add_column(
                table_name=table_name,
                column_name=schema_change.column_name,
                dtype=_pandas_dtype_to_python_type(schema_change.column_type),
                engine=engine,
                schema=schema,
            )
        elif schema_change.change_type == "drop_column":
            tm.drop_column(
                engine=engine,
                table_name=table_name,
                col_name=schema_change.column_name,
                schema=schema,
            )
        elif schema_change.change_type == "rename_column":
            if schema_change.new_column_name is None:
                raise ValueError("new_column_name is required for rename_column operation")
            tm.rename_column(
                engine=engine,
                table_name=table_name,
                old_col_name=schema_change.column_name,
                new_col_name=schema_change.new_column_name,
                schema=schema,
            )
        elif schema_change.change_type == "alter_column_type":
            # Use transmutation's alter_column with type_ parameter
            tm.alter_column(
                table_name=table_name,
                column_name=schema_change.column_name,
                engine=engine,
                schema=schema,
                type_=_pandas_dtype_to_python_type(schema_change.new_column_type),
            )
    except Exception as e:
        # Convert transmutation ValidationError and other errors to TransactionError
        # This ensures consistent error handling across all schema operations
        if (
            "ValidationError" in type(e).__name__
            or "Column" in str(e)
            and ("does not exist" in str(e) or "not found" in str(e))
        ):
            raise TransactionError(
                f"Schema change failed: {str(e)}",
                details={
                    "table": table_name,
                    "schema": schema,
                    "change_type": schema_change.change_type,
                },
            ) from e
        # Re-raise other exceptions as-is
        raise


def _execute_deletes(
    connection: Any,
    table_name: str,
    delete_keys: list[Any],
    primary_key: str | list[str] | None,
    schema: str | None = None,
) -> None:
    """
    Execute delete operations with adaptive batching.

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

    # Calculate optimal batch size
    batch_size = _calculate_batch_size(OperationType.DELETE, len(clean_keys))

    # Chunk deletes into batches
    chunks = _chunk_list(clean_keys, batch_size)

    # Build a minimal Table object with just the primary key column(s)
    # We know the primary key name(s) from the parameter, so we don't need inspect
    # Use Integer as a generic type - SQLAlchemy will handle the actual comparison
    from sqlalchemy import Column, Integer

    metadata = MetaData()

    if isinstance(primary_key, str):
        # Single column PK
        pk_col = Column(primary_key, Integer)
        table = Table(table_name, metadata, pk_col, schema=schema)
        # Execute DELETE using individual WHERE clauses for each key in batches
        for chunk in chunks:
            for key in chunk:
                stmt = table.delete().where(table.c[primary_key] == key)
                result = connection.execute(stmt)
                # Check if delete actually worked
                if result.rowcount == 0:
                    # Row might not exist - log but continue
                    import warnings

                    warnings.warn(
                        f"DELETE statement for {primary_key}={key} affected 0 rows", stacklevel=3
                    )
    else:
        # Composite PK - build table with all PK columns
        pk_cols = [Column(pk_name, Integer) for pk_name in primary_key]
        table = Table(table_name, metadata, *pk_cols, schema=schema)
        # Execute individual deletes for each composite key in batches
        from sqlalchemy import and_

        for chunk in chunks:
            for key_value in chunk:
                if isinstance(key_value, (tuple, list)) and len(key_value) == len(primary_key):
                    conditions = [
                        table.c[pk_col] == val for pk_col, val in zip(primary_key, key_value)
                    ]
                    stmt = table.delete().where(and_(*conditions))
                    connection.execute(stmt)


def _execute_updates(
    connection: Any,
    table_name: str,
    update_records: list[dict[str, Any]],
    primary_key: str | list[str] | None,
    schema: str | None = None,
) -> None:
    """
    Execute update operations with adaptive batching.

    Args:
        connection: SQLAlchemy connection within a transaction
        table_name: Name of the table
        update_records: List of records to update (must include primary key)
        primary_key: Name of the primary key column(s) - single string or list for composite
        schema: Optional schema name
    """
    if not update_records or not primary_key:
        return

    # Calculate optimal batch size
    batch_size = _calculate_batch_size(OperationType.UPDATE, len(update_records))

    # Chunk updates into batches
    chunks = _chunk_list(update_records, batch_size)

    # Use autoload_with to load table structure from the existing connection
    # This ensures we see the current transaction state, including any schema changes
    # Note: autoload_with uses the connection's transaction context
    table = Table(table_name, MetaData(), autoload_with=connection, schema=schema)

    # Handle single vs composite primary keys
    if isinstance(primary_key, str):
        # Single column PK
        pk_cols = [primary_key]
    else:
        # Composite PK
        pk_cols = list(primary_key)

    # Process updates in batches
    for chunk in chunks:
        for record in chunk:
            # Extract PK values
            if len(pk_cols) == 1:
                pk_value = convert_numpy_types(record.get(pk_cols[0]))
                if pk_value is None:
                    continue

                # Separate primary key from update values
                update_values = {
                    k: convert_numpy_types(v) for k, v in record.items() if k != pk_cols[0]
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
                    k: convert_numpy_types(v) for k, v in record.items() if k not in pk_cols
                }

                # Build UPDATE statement
                stmt = table.update().where(and_(*conditions)).values(**update_values)

            connection.execute(stmt)


def _execute_inserts(
    connection: Any,
    table_name: str,
    insert_records: list[dict[str, Any]],
    schema: str | None = None,
) -> None:
    """
    Execute insert operations with adaptive batching.

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

    if not clean_records:
        return

    # Calculate optimal batch size
    batch_size = _calculate_batch_size(OperationType.INSERT, len(clean_records))

    # Chunk inserts into batches
    chunks = _chunk_list(clean_records, batch_size)

    # Use raw SQLAlchemy insert instead of pandas to_sql to avoid connection issues
    # This is especially important for MySQL and PostgreSQL which can have connection
    # management conflicts with pandas' internal connection handling
    from sqlalchemy import insert

    # Use inspect to get table structure without autoload_with which can hang
    # Similar to PostgreSQL fix - inspect(engine) manages connections safely
    inspector = inspect(connection.engine)
    columns_info = inspector.get_columns(table_name, schema=schema)

    # Build table structure from column info
    from sqlalchemy import Column

    metadata = MetaData()
    columns = []
    for col_info in columns_info:
        col_name = col_info["name"]
        col_type = col_info["type"]
        columns.append(Column(col_name, col_type))

    table = Table(table_name, metadata, *columns, schema=schema)

    # Execute inserts in batches
    for chunk in chunks:
        connection.execute(insert(table), chunk)


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
    if_exists: str = "fail",
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

    from pandalchemy.pk_utils import normalize_primary_key
    from pandalchemy.utils import prepare_primary_key_for_table_creation

    # Prepare primary key (handles index naming and validation)
    df_prepared = prepare_primary_key_for_table_creation(df, primary_key)

    # Convert DataFrame to ensure primary key is a column
    df_to_write = extract_primary_key_column(df_prepared, primary_key)

    # Normalize schema for pandas
    schema_normalized = normalize_schema(schema)

    # Normalize primary key to list format
    pk_cols = normalize_primary_key(primary_key)

    # Use SQLAlchemy Table API for all cases to ensure PRIMARY KEY constraint
    # This avoids the ALTER TABLE limitation in SQLite and other databases
    metadata = MetaData()

    # Map pandas dtypes to SQLAlchemy types
    columns: list[Column] = []
    # MySQL requires VARCHAR length - use a reasonable default
    is_mysql = engine.dialect.name == "mysql"
    string_length = 255 if is_mysql else None

    for col_name in df_to_write.columns:
        dtype = df_to_write[col_name].dtype

        # For empty DataFrames, default to String to avoid type inference issues
        # Empty DataFrames have object dtype, but we don't know what type will be inserted
        # Type annotation allows both type classes and instances (String vs String(length))
        col_type: type[Any] | Any
        if df_to_write.empty:
            col_type = String(string_length) if is_mysql else String
        elif "int" in str(dtype):
            col_type = Integer
        elif "float" in str(dtype):
            col_type = Float
        elif "bool" in str(dtype):
            col_type = Boolean
        elif "datetime" in str(dtype):
            # DateTime columns - use String for SQLite, DateTime for others
            if engine.dialect.name == "sqlite":
                col_type = String(string_length) if is_mysql else String
            else:
                from sqlalchemy import DateTime

                col_type = DateTime
        else:
            # MySQL requires explicit VARCHAR length, others can be None
            col_type = String(string_length) if is_mysql else String
        columns.append(Column(col_name, col_type))

    # Add primary key constraint
    table_obj = Table(
        table_name,
        metadata,
        *columns,
        PrimaryKeyConstraint(*pk_cols, name=f"{table_name}_pk"),
        schema=schema_normalized,
    )

    # Create the table and insert data in explicit transaction
    # This ensures everything commits before any subsequent operations
    with engine.begin() as conn:
        # Create the table structure
        if if_exists == "replace":
            table_obj.drop(bind=conn, checkfirst=True)
        metadata.create_all(bind=conn)

        # Insert data - use raw SQL to avoid pandas to_sql connection issues
        if not df_to_write.empty:
            from sqlalchemy import insert

            table_ref = table_obj
            # Convert records to ensure Timestamp objects are converted for SQLite
            records = convert_records_list(df_to_write.to_dict("records"))
            conn.execute(insert(table_ref), records)


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
    # inspect(engine) manages connections internally
    inspector = inspect(engine)
    return table_name in inspector.get_table_names(schema=schema)
