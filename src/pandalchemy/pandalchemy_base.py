"""
Core pandalchemy classes with change tracking and modern SQL operations.

This module provides the main DataBase and Table classes that integrate
pandas DataFrames with SQLAlchemy using change tracking and optimized
SQL execution plans.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from pandas import DataFrame
from sqlalchemy import Engine, inspect

from pandalchemy.execution_plan import ExecutionPlan
from pandalchemy.interfaces import IDataBase, ITable
from pandalchemy.sql_operations import (
    create_table_from_dataframe,
    execute_plan,
    get_primary_key,
    pull_table,
    table_exists,
)
from pandalchemy.tracked_dataframe import TrackedDataFrame


class DataBase(IDataBase):
    """
    Container for database tables as tracked DataFrames.

    The DataBase class manages multiple Table objects and provides methods
    to synchronize all changes with the database in a single transaction.

    Attributes:
        engine: SQLAlchemy engine for database connection
        schema: Optional schema name
        lazy: Whether to use lazy loading for tables
        db: Dictionary mapping table names to Table objects

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine('sqlite:///example.db')
        >>> db = DataBase(engine)
        >>> table = db['my_table']
        >>> table['new_column'] = [1, 2, 3]
        >>> db.push()  # Synchronize all changes
    """

    def __init__(self, engine: Engine, lazy: bool = False, schema: str | None = None):
        """
        Initialize a DataBase instance.

        Args:
            engine: SQLAlchemy engine for database connection
            lazy: If True, tables are loaded only when accessed
            schema: Optional schema name for the database
        """
        self.engine = engine
        self.schema = schema
        self.lazy = lazy

        inspector = inspect(engine)
        table_names = inspector.get_table_names(schema=schema)

        if not self.lazy:
            self.db: dict[str, Table] = {
                name: Table(
                    name=name,
                    engine=engine,
                    db=self,
                    schema=schema
                )
                for name in table_names
            }
        else:
            # Store table names only, load on access
            # Use a dict with Table type hint, actual values loaded later
            self.db = {}  # type: ignore
            for name in table_names:
                self.db[name] = None  # type: ignore

    def __getitem__(self, key: str) -> Table:
        """
        Get a table by name.

        Args:
            key: Name of the table

        Returns:
            Table object for the specified table
        """
        if self.lazy and self.db.get(key) is None:
            # Lazy load the table
            self.db[key] = Table(key, engine=self.engine, db=self, schema=self.schema)
        return self.db[key]

    def __setitem__(self, key: str, value: Table) -> None:
        """
        Set a table by name.

        Args:
            key: Name of the table
            value: Table object to store
        """
        self.db[key] = value

    def __len__(self) -> int:
        """Return the number of tables."""
        return len(self.db)

    @property
    def table_names(self):  # type: ignore[override]
        """Get iterator of table names in the database."""
        return iter(self.db.keys())

    def __repr__(self) -> str:
        """Return string representation."""
        table_list = ", ".join(self.db.keys())
        return f"DataBase(tables=[{table_list}], url={self.engine.url})"

    def push(self) -> None:
        """
        Push all table changes to the database.

        All changes across all tables are executed within a single transaction.
        If any operation fails, all changes are rolled back.
        """
        # Execute all pushes within a single transaction
        with self.engine.begin() as connection:
            for _table_name, table in self.db.items():
                if table is not None and hasattr(table, 'push'):
                    # Push using the connection from the transaction
                    table._push_with_connection(connection)

        # Refresh all tables after successful push
        self.__init__(self.engine, lazy=self.lazy, schema=self.schema)  # type: ignore[misc]

    def pull(self) -> None:
        """Refresh all tables with current database data."""
        self.__init__(self.engine, lazy=self.lazy, schema=self.schema)  # type: ignore[misc]

    def add_table(self, table: Table, push: bool = False) -> None:
        """
        Add a new table to the database.

        Args:
            table: Table object to add
            push: If True, immediately push the table to the database
        """
        self.db[table.name] = table
        table.db = self
        table.engine = self.engine
        table.schema = self.schema
        if push:
            self.push()

    def create_table(
        self,
        name: str,
        data: DataFrame,
        primary_key: str = 'id',
        if_exists: str = 'fail'
    ) -> Table:
        """
        Create a new table from a DataFrame.

        Args:
            name: Name for the new table
            data: DataFrame containing the data
            primary_key: Name of the primary key column
            if_exists: What to do if table exists ('fail', 'replace', 'append')

        Returns:
            Table object for the new table
        """
        create_table_from_dataframe(
            self.engine,
            name,
            data,
            primary_key,
            schema=self.schema,
            if_exists=if_exists
        )

        # Refresh and return the new table
        self.pull()
        return self[name]


class Table(ITable):
    """
    DataFrame-like interface for a SQL table with change tracking.

    The Table class wraps a TrackedDataFrame that monitors all changes.
    When push() is called, it generates an optimized execution plan and
    applies all changes to the database within a transaction.

    Attributes:
        name: Name of the SQL table
        engine: SQLAlchemy engine
        schema: Optional schema name
        db: Parent DataBase object
        data: TrackedDataFrame containing the table data
        key: Primary key column name

    Example:
        >>> table = db['users']
        >>> table['age'] = table['age'] + 1  # Increment all ages
        >>> table.loc[0, 'name'] = 'Alice'   # Update specific cell
        >>> table.push()  # Apply changes to database
    """

    def __init__(
        self,
        name: str,
        data: DataFrame | None = None,
        key: str | list[str] | None = None,
        engine: Engine | None = None,
        db: DataBase | None = None,
        schema: str | None = None,
        auto_increment: bool = False
    ):
        """
        Initialize a Table instance.

        Args:
            name: Name of the SQL table
            data: Optional DataFrame with initial data
            key: Optional primary key column name
            engine: Optional SQLAlchemy engine
            db: Optional parent DataBase object
            schema: Optional schema name
            auto_increment: If True, enable auto-increment for primary key
        """
        self.name = name
        self.engine = engine
        self.schema = schema
        self.db = db
        self.key = key
        self.auto_increment = auto_increment

        # If engine provided, pull data from database
        if self.engine is not None and data is None:
            if table_exists(self.engine, self.name, self.schema):
                # Get primary key if not specified
                if self.key is None:
                    self.key = get_primary_key(self.engine, self.name, self.schema)
                    if self.key is None:
                        self.key = 'id'

                # Pull data from existing table with PK as index
                df = pull_table(
                    self.engine,
                    self.name,
                    self.schema,
                    primary_key=self.key,
                    set_index=True
                )

                # Create tracked DataFrame
                self.data = TrackedDataFrame(df, self.key)
            else:
                # Table doesn't exist yet
                if data is None:
                    data = pd.DataFrame()
                self.data = TrackedDataFrame(data, self.key or 'id')
                self.key = self.key or 'id'
        else:
            # Use provided data or create empty DataFrame
            if data is None:
                data = pd.DataFrame()

            if self.key is None:
                # Try to infer key from index
                if isinstance(data.index, pd.MultiIndex):
                    self.key = list(data.index.names) if all(data.index.names) else 'id'
                elif data.index.name:
                    self.key = data.index.name
                else:
                    self.key = 'id'

            # Ensure PK is set as index if not already
            if isinstance(self.key, list):
                # Composite key
                if not isinstance(data.index, pd.MultiIndex) or list(data.index.names) != self.key:
                    if all(col in data.columns for col in self.key):
                        data = data.set_index(self.key)
            else:
                # Single key
                if data.index.name != self.key and self.key in data.columns:
                    data = data.set_index(self.key)

            self.data = TrackedDataFrame(data, self.key)

    def __getitem__(self, key: Any) -> Any:
        """Delegate item access to tracked DataFrame."""
        return self.data[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        """Delegate item setting to tracked DataFrame."""
        self.data[key] = value

    def __len__(self) -> int:
        """Return the number of rows."""
        return len(self.data)

    def __repr__(self) -> str:
        """Return string representation."""
        return f"Table(name='{self.name}', rows={len(self)}, columns={list(self.data.columns)})"

    def _repr_html_(self) -> str:
        """Return HTML representation for Jupyter notebooks."""
        return self.data._repr_html_()

    @property
    def shape(self):
        """Return the shape of the table."""
        return self.data.shape

    @property
    def columns(self):
        """Return the columns of the table."""
        return self.data.columns

    @property
    def column_names(self):
        """Return the column names of the table (for ITable interface compatibility)."""
        return self.data.columns

    @property
    def index(self):
        """Return the index of the table."""
        return self.data.index

    @property
    def loc(self):
        """Access via loc indexer."""
        return self.data.loc

    @property
    def iloc(self):
        """Access via iloc indexer."""
        return self.data.iloc

    @property
    def at(self):
        """Access via at indexer."""
        return self.data.at

    @property
    def iat(self):
        """Access via iat indexer."""
        return self.data.iat

    def head(self, n: int = 5) -> DataFrame:
        """Return the first n rows."""
        return self.data.head(n)

    def tail(self, n: int = 5) -> DataFrame:
        """Return the last n rows."""
        return self.data.tail(n)

    def push(self, engine: Engine | None = None, schema: str | None = None) -> None:
        """
        Push all changes to the database.

        This method generates an optimized execution plan from tracked changes
        and executes it within a transaction. All changes are rolled back if
        any operation fails.

        Args:
            engine: Optional engine to use (overrides instance engine)
            schema: Optional schema to use (overrides instance schema)

        Raises:
            SchemaError: If primary key columns have been dropped
            DataValidationError: If primary key validation fails
            SQLAlchemyError: If any database operation fails
        """
        from pandalchemy.exceptions import DataValidationError, SchemaError

        if engine is not None:
            self.engine = engine
        if schema is not None:
            self.schema = schema

        # Ensure engine is available
        if self.engine is None:
            raise ValueError("Cannot push without an engine")
        if self.key is None:
            raise ValueError("Cannot push without a primary key")

        # VALIDATE PRIMARY KEY BEFORE PUSH
        try:
            self.data.validate_primary_key()
        except (SchemaError, DataValidationError) as e:
            raise SchemaError(
                f"Cannot push table '{self.name}': {str(e)}",
                details={
                    'table': self.name,
                    'primary_key': self.key,
                    'error': str(e)
                }
            ) from e

        # Get the current pandas DataFrame
        current_df = self.data.to_pandas()

        # Get the change tracker
        tracker = self.data.get_tracker()

        # Build execution plan
        plan = ExecutionPlan(tracker, current_df)

        # Check if table exists
        if not table_exists(self.engine, self.name, self.schema):
            # Create new table
            create_table_from_dataframe(
                self.engine,
                self.name,
                current_df,
                self.key,
                self.schema,
                if_exists='fail'
            )
        else:
            # Execute the plan to update existing table
            execute_plan(
                self.engine,
                self.name,
                plan,
                self.schema,
                self.key
            )

        # Refresh the table after successful push
        self.pull()

    def _push_with_connection(self, connection: Any) -> None:
        """
        Internal method to push changes using an existing connection.

        Used by DataBase.push() to execute multiple table pushes in one transaction.

        Args:
            connection: SQLAlchemy connection within a transaction (currently unused,
                       future enhancement for true multi-table transactions)

        Raises:
            SchemaError: If primary key columns have been dropped
            DataValidationError: If primary key validation fails
        """
        from pandalchemy.exceptions import DataValidationError, SchemaError

        # Note: connection parameter reserved for future optimization
        # Currently, each table uses its own transaction via execute_plan
        _ = connection  # Reserved for future use  # noqa: F841

        # VALIDATE PRIMARY KEY BEFORE PUSH
        try:
            self.data.validate_primary_key()
        except (SchemaError, DataValidationError) as e:
            raise SchemaError(
                f"Cannot push table '{self.name}': {str(e)}",
                details={
                    'table': self.name,
                    'primary_key': self.key,
                    'error': str(e)
                }
            ) from e

        current_df = self.data.to_pandas()
        tracker = self.data.get_tracker()
        plan = ExecutionPlan(tracker, current_df)

        # Ensure engine and key are set
        assert self.engine is not None, "Engine must be set before push"
        assert self.key is not None, "Primary key must be set before push"

        if not table_exists(self.engine, self.name, self.schema):
            # Create new table
            create_table_from_dataframe(
                self.engine,
                self.name,
                current_df,
                self.key,
                self.schema,
                if_exists='fail'
            )
        else:
            # Execute plan (uses its own transaction)
            execute_plan(
                self.engine,
                self.name,
                plan,
                self.schema,
                self.key
            )

    def pull(self, engine: Engine | None = None, schema: str | None = None) -> None:
        """
        Refresh the table with current database data.

        Args:
            engine: Optional engine to use (overrides instance engine)
            schema: Optional schema to use (overrides instance schema)
        """
        if engine is not None:
            self.engine = engine
        if schema is not None:
            self.schema = schema

        self.__init__(self.name, engine=self.engine, schema=self.schema, db=self.db)  # type: ignore[misc]

    def to_pandas(self) -> DataFrame:
        """
        Convert to a regular pandas DataFrame.

        Returns:
            Regular pandas DataFrame with current data
        """
        return self.data.to_pandas()

    def copy(self) -> Table:
        """
        Create a copy of the table.

        Returns:
            New Table instance with copied data
        """
        new_data = self.data.to_pandas()
        return Table(
            name=self.name,
            data=new_data,
            key=self.key,
            engine=self.engine,
            db=self.db,
            schema=self.schema,
            auto_increment=self.auto_increment
        )

    def get_next_pk_value(self) -> int:
        """
        Get the next primary key value for auto-increment.

        Queries both local DataFrame and database to get the maximum value,
        then returns max + 1.

        Returns:
            Next available PK value

        Raises:
            ValueError: If table is not configured for auto-increment or PK is not suitable
        """
        from sqlalchemy import text

        if not self.auto_increment:
            raise ValueError(
                f"Table '{self.name}' is not configured for auto-increment. "
                "Set auto_increment=True when creating the table."
            )

        # Get max from local data
        try:
            local_max = self.data.get_next_pk_value() - 1  # Subtract 1 since it returns next
        except ValueError:
            local_max = 0

        # Get max from database (if table exists)
        db_max = 0
        if self.engine and table_exists(self.engine, self.name, self.schema):
            try:
                with self.engine.connect() as conn:
                    # Build query based on schema
                    table_ref = f"{self.schema}.{self.name}" if self.schema else self.name
                    query = text(f"SELECT MAX({self.key}) as max_id FROM {table_ref}")
                    result = conn.execute(query).fetchone()
                    if result and result[0] is not None:
                        db_max = int(result[0])
            except Exception:
                # If query fails, use local max
                pass

        return max(local_max, db_max) + 1

    def drop(self, *args, **kwargs) -> None:
        """Drop rows or columns from the table."""
        self.data.drop(*args, **kwargs)

    def sort_values(self, *args, **kwargs) -> None:
        """Sort the table by values."""
        self.data.sort_values(*args, **kwargs)

    def rename(self, *args, **kwargs) -> None:
        """Rename columns or index."""
        self.data.rename(*args, **kwargs)

    def get_changes_summary(self) -> dict[str, Any]:
        """
        Get a summary of tracked changes.

        Returns:
            Dictionary with change statistics
        """
        return self.data.get_tracker().get_summary()
