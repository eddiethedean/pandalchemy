"""
Core pandalchemy classes with change tracking and modern SQL operations.

This module provides the main DataBase and Table classes that integrate
pandas DataFrames with SQLAlchemy using change tracking and optimized
SQL execution plans.
"""

from __future__ import annotations

from pandas import DataFrame
from sqlalchemy import Engine, inspect

from pandalchemy.interfaces import IDataBase
from pandalchemy.sql_operations import (
    create_table_from_dataframe,
)
from pandalchemy.tracked_dataframe import TableDataFrame


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

        self.db: dict[str, TableDataFrame] = {}
        self._load_tables()

    def _load_tables(self) -> None:
        """
        Load or reload all tables from the database.

        This method is used both during initialization and when refreshing
        the database state after push/pull operations.
        """
        # Clear existing tables
        self.db.clear()

        # Use inspect(engine) with explicit connection context for table names
        # This ensures connection is properly closed
        inspector = inspect(self.engine)
        # get_table_names() creates and closes its own connection internally
        table_names = inspector.get_table_names(schema=self.schema)

        if not self.lazy:
            self.db = {
                name: TableDataFrame(name=name, engine=self.engine, db=self, schema=self.schema)
                for name in table_names
            }
        else:
            # Store table names only, load on access
            # Use a dict with TableDataFrame type hint, actual values loaded later
            for name in table_names:
                self.db[name] = None  # type: ignore

    def __getitem__(self, key: str) -> TableDataFrame:
        """
        Get a table by name.

        Args:
            key: Name of the table

        Returns:
            TableDataFrame object for the specified table
        """
        if self.lazy and self.db.get(key) is None:
            # Lazy load the table
            self.db[key] = TableDataFrame(name=key, engine=self.engine, db=self, schema=self.schema)
        return self.db[key]

    def __setitem__(self, key: str, value: TableDataFrame) -> None:
        """
        Set a table by name.

        Args:
            key: Name of the table
            value: TableDataFrame object to store
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

    def push(self, parallel: bool = True, _max_workers: int | None = None) -> None:
        """
        Push all table changes to the database.

        By default, independent tables (tables with no foreign key dependencies)
        are executed in parallel for better performance. Tables with dependencies
        are executed sequentially to maintain referential integrity.

        Args:
            parallel: If True, execute independent tables in parallel (default True)
            max_workers: Maximum number of parallel workers (None = auto-detect CPU count)

        Note:
            All changes across all tables are executed within transactions.
            If any operation fails, all changes are rolled back.

            Note: SQLite does not support multi-threaded writes, so parallel
            execution is automatically disabled for SQLite databases.
        """

        # SQLite doesn't support multi-threaded writes - disable parallel for SQLite
        if self.engine.dialect.name == "sqlite":
            parallel = False

        # Get tables that need to be pushed
        # Build list of tables with changes AND all tables for validation
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

                if should_push:
                    tables_with_changes[name] = table

        # Use tables_with_changes for pushing, but validate all tables
        tables_to_push = tables_with_changes

        # If parallel execution is disabled or only one table, use sequential execution
        # This ensures all changes happen in a single transaction for atomicity
        if not parallel or len(tables_to_push) <= 1:
            # Validate ALL tables before starting transaction (even ones without changes)
            # This ensures SchemaErrors are caught early, even if tracking missed a schema change
            # This is especially important for SQLite where we disable parallel execution
            for _table_name, table in all_tables_for_validation.items():
                if hasattr(table, "validate_primary_key"):
                    try:
                        table.validate_primary_key()
                    except Exception:
                        # Re-raise validation errors immediately, before transaction
                        raise

        # After validation, if no tables have changes, we're done
        if not tables_to_push:
            return

        # Execute all pushes within a single transaction
        try:
            with self.engine.begin() as connection:
                for _table_name, table in tables_to_push.items():
                    table._push_with_connection(connection)
        except Exception:
            # Transaction will auto-rollback, but we need to re-raise the error
            raise

        # Refresh all tables after successful push
        self._load_tables()

    def pull(self) -> None:
        """Refresh all tables with current database data."""
        self._load_tables()

    def add_table(self, table: TableDataFrame, push: bool = False) -> None:
        """
        Add a new table to the database.

        Args:
            table: TableDataFrame object to add
            push: If True, immediately push the table to the database
        """
        self.db[table.name] = table
        object.__setattr__(table, "db", self)
        object.__setattr__(table, "engine", self.engine)
        object.__setattr__(table, "schema", self.schema)
        if push:
            # If push is requested, explicitly push this table even if it has no tracked changes
            # (e.g., for new tables that need to be created)
            table.push()

    def create_table(
        self, name: str, data: DataFrame, primary_key: str = "id", if_exists: str = "fail"
    ) -> TableDataFrame:
        """
        Create a new table from a DataFrame.

        Args:
            name: Name for the new table
            data: DataFrame containing the data
            primary_key: Name of the primary key column
            if_exists: What to do if table exists ('fail', 'replace', 'append')

        Returns:
            TableDataFrame object for the new table
        """
        create_table_from_dataframe(
            self.engine, name, data, primary_key, schema=self.schema, if_exists=if_exists
        )

        # Refresh and return the new table
        self.pull()
        return self[name]
