# Migration Plan: Pandalchemy to Fullmetalalchemy

## Executive Summary

This plan analyzes all SQL operations currently performed in pandalchemy using direct SQLAlchemy calls and identifies gaps in fullmetalalchemy that need to be addressed to enable a complete migration. The goal is to replace all direct SQLAlchemy usage in pandalchemy with fullmetalalchemy functions.

## Current Pandalchemy SQL Operations

### 1. Table Introspection (`sql_operations.py`)
- **`table_exists()`**: Checks if table exists using `inspect(engine).get_table_names()`
- **`get_primary_key()`**: Gets primary key column(s) using `inspect(engine).get_pk_constraint()`
- **`_load_tables()`** (pandalchemy_base.py): Uses `inspect(engine).get_table_names()` to list tables

### 2. Data Operations
- **`pull_table()`**: Reads table into pandas DataFrame using `pd.read_sql_table()` or `pd.read_sql()` with database-specific quoting
- **`create_table_from_dataframe()`**: Creates table from pandas DataFrame with proper type mapping (handles MySQL VARCHAR length requirements)
- **`_execute_inserts()`**: Bulk insert using SQLAlchemy `insert()` statement with connection
- **`_execute_updates()`**: Updates using SQLAlchemy `update()` with `autoload_with=connection` for transaction visibility
- **`_execute_deletes()`**: Deletes using SQLAlchemy `delete()` with composite PK support

### 3. Schema Evolution
- **`_execute_schema_change()`**: 
  - PostgreSQL/MySQL: Uses raw SQL `ALTER TABLE` for `add_column` and `rename_column`
  - Other databases: Uses `transmutation` library
  - Handles MySQL VARCHAR length requirements
  - Handles PostgreSQL rename without type requirement
  - Handles MySQL rename with type requirement

### 4. Transaction Management
- **`execute_plan()`**: Separates schema changes (outside transaction) from data changes (inside transaction)
- Uses `engine.begin()` for transaction context
- Uses separate connections for schema changes to avoid metadata locks

## Fullmetalalchemy Capabilities Analysis

### What Fullmetalalchemy Has ✅

1. **INSERT**: `insert_records()` - inserts records, supports sessions
2. **UPDATE**: `update_records()` - updates by primary key or match columns, supports composite PKs via `match_column_names`
3. **DELETE**: 
   - `delete_records()` - deletes by single column name and values
   - `delete_records_by_values()` - deletes by matching record values (supports composite PKs)
4. **SELECT**: `select_records_all()` - returns list of dicts (not DataFrame)
5. **CREATE**: 
   - `create_table()` - creates from column specs
   - `create_table_from_records()` - creates from list of dicts (not DataFrame)
6. **Primary Key**: `primary_key_names()` - requires Table object
7. **Table Introspection**: `get_table()`, `get_table_names()` - can check existence via `get_table_names()`

### What Fullmetalalchemy Needs ❌

## Gap Analysis: Required Fullmetalalchemy Features

### 1. Table Existence Check
**Priority: High**
- **Gap**: No direct `table_exists(engine, table_name, schema)` function
- **Workaround**: Can use `table_name in get_table_names(engine, schema)` but this is inefficient for single checks
- **Required Function**:
  ```python
  def table_exists(table_name: str, engine: Engine, schema: Optional[str] = None) -> bool:
      """Check if a table exists without loading all table names."""
  ```
- **Location**: `fullmetalalchemy/features.py`

### 2. Primary Key Retrieval from Engine
**Priority: High**
- **Gap**: `primary_key_names()` requires a Table object, but pandalchemy needs it from `(engine, table_name, schema)` tuple
- **Current**: Must call `get_table()` first, which is inefficient
- **Required Function**:
  ```python
  def get_primary_key_names(
      table_name: str, 
      engine: Engine, 
      schema: Optional[str] = None
  ) -> List[str] | None:
      """Get primary key column names without loading full table metadata."""
      # Returns None if no PK, single string if single PK, list if composite PK
  ```
- **Location**: `fullmetalalchemy/features.py`
- **Implementation**: Use `inspect(engine).get_pk_constraint(table_name, schema)` directly

### 3. DataFrame to Table Creation
**Priority: High**
- **Gap**: `create_table_from_records()` takes list of dicts, but pandalchemy has pandas DataFrames
- **Required Function**:
  ```python
  def create_table_from_dataframe(
      table_name: str,
      df: pd.DataFrame,
      primary_key: str | List[str],
      engine: Engine,
      schema: Optional[str] = None,
      if_exists: str = "fail"
  ) -> Table:
      """
      Create table from pandas DataFrame.
      - Handles empty DataFrames with default String type
      - Handles MySQL VARCHAR length requirement (VARCHAR(255))
      - Handles SQLite DateTime -> String conversion
      - Proper type inference from pandas dtypes
      """
  ```
- **Location**: `fullmetalalchemy/create.py`
- **Dependencies**: Need pandas integration

### 4. Table to DataFrame Reading
**Priority: High**
- **Gap**: `select_records_all()` returns list of dicts, not DataFrame
- **Workaround**: Can convert with `pd.DataFrame(select_records_all(...))` but loses type inference
- **Required Function**:
  ```python
  def select_table_as_dataframe(
      table: Union[Table, str],
      engine: Engine,
      schema: Optional[str] = None,
      primary_key: Optional[str | List[str]] = None,
      set_index: bool = True
  ) -> pd.DataFrame:
      """
      Read table into pandas DataFrame with proper type inference.
      - Uses pd.read_sql_table for SQLite (better type inference)
      - Uses pd.read_sql for PostgreSQL/MySQL (avoids inspect() hangs)
      - Handles database-specific quoting (backticks for MySQL, quotes for PostgreSQL)
      - Sets primary key as index if requested
      """
  ```
- **Location**: `fullmetalalchemy/select.py`
- **Dependencies**: Need pandas integration

### 5. Connection-Based Operations
**Priority: Medium-High**
- **Gap**: Fullmetalalchemy operations use sessions or auto-commit, but pandalchemy needs connection-level control for `autoload_with=connection` to see transaction state
- **Current**: Session operations exist but don't support connection objects
- **Required Functions**:
  ```python
  def insert_records_connection(
      table: Union[Table, str],
      records: Sequence[Record],
      connection: Connection  # Not Session
  ) -> None:
      """Insert records using connection (for transaction control)."""
  
  def update_records_connection(
      table: Union[Table, str],
      records: Sequence[Record],
      connection: Connection,
      match_column_names: Optional[Sequence[str]] = None
  ) -> None:
      """Update records using connection with autoload_with support."""
  
  def delete_records_connection(
      table: Union[Table, str],
      records: Sequence[Record] | None,
      column_name: Optional[str] = None,
      values: Optional[Sequence[Any]] = None,
      connection: Connection
  ) -> None:
      """Delete records using connection."""
  ```
- **Location**: `fullmetalalchemy/session.py` or new `fullmetalalchemy/connection.py`
- **Note**: These should use `autoload_with=connection` for UPDATE to see transaction state

### 6. Composite Primary Key Delete Support
**Priority: Medium**
- **Gap**: `delete_records()` only accepts single column name, but pandalchemy needs composite PK support
- **Partial Solution**: `delete_records_by_values()` exists but takes full records (inefficient for PK-only deletes)
- **Required Enhancement**:
  ```python
  def delete_records_by_primary_keys(
      table: Union[Table, str],
      primary_key_values: Sequence[Any | Tuple[Any, ...]],  # Single values or tuples for composite
      connection: Connection
  ) -> None:
      """
      Delete records by primary key values.
      - Single PK: List[Any]
      - Composite PK: List[Tuple[Any, ...]]
      """
  ```
- **Location**: `fullmetalalchemy/delete.py`

### 7. Schema Evolution Support
**Priority: Medium**
- **Gap**: Fullmetalalchemy has no schema evolution functions (add/drop/rename columns, alter types)
- **Current**: Pandalchemy uses `transmutation` library or raw SQL
- **Required Functions**:
  ```python
  def add_column(
      table_name: str,
      column_name: str,
      column_type: type,
      engine: Engine,
      schema: Optional[str] = None,
      default_value: Optional[Any] = None
  ) -> None:
      """
      Add column to table.
      - Database-specific handling (MySQL VARCHAR length, etc.)
      """
  
  def drop_column(
      table_name: str,
      column_name: str,
      engine: Engine,
      schema: Optional[str] = None
  ) -> None:
      """Drop column from table."""
  
  def rename_column(
      table_name: str,
      old_column_name: str,
      new_column_name: str,
      engine: Engine,
      schema: Optional[str] = None
  ) -> None:
      """
      Rename column.
      - PostgreSQL: RENAME COLUMN (no type needed)
      - MySQL: CHANGE COLUMN (requires existing type lookup)
      """
  
  def alter_column_type(
      table_name: str,
      column_name: str,
      new_type: type,
      engine: Engine,
      schema: Optional[str] = None
  ) -> None:
      """Change column type."""
  ```
- **Location**: New `fullmetalalchemy/schema.py` module
- **Dependencies**: May need `transmutation` integration or raw SQL generation

### 8. Efficient Table Introspection
**Priority: Low-Medium**
- **Gap**: `get_table()` uses `autoload_with` which can hang in PostgreSQL after schema changes
- **Required Enhancement**: Option to use `inspect(engine)` instead of `autoload_with`
- **Function**:
  ```python
  def get_table_columns(
      table_name: str,
      engine: Engine,
      schema: Optional[str] = None
  ) -> List[Dict[str, Any]]:
      """
      Get column information without full table reflection.
      Returns list of dicts with 'name', 'type', etc.
      Uses inspect(engine).get_columns() to avoid hangs.
      """
  ```
- **Location**: `fullmetalalchemy/features.py`

## Implementation Priority

1. **Phase 1 (Critical)**: Features needed for basic CRUD migration
   - Table existence check
   - Primary key retrieval from engine
   - DataFrame to/from table functions
   - Connection-based operations

2. **Phase 2 (Important)**: Features for transaction support
   - Composite PK delete enhancement
   - Connection-based operations with autoload_with

3. **Phase 3 (Nice to have)**: Schema evolution
   - Add/drop/rename column functions
   - Type alteration functions

## Files to Modify in Fullmetalalchemy

1. `src/fullmetalalchemy/features.py` - Add `table_exists()`, `get_primary_key_names()`, `get_table_columns()`
2. `src/fullmetalalchemy/create.py` - Add `create_table_from_dataframe()`
3. `src/fullmetalalchemy/select.py` - Add `select_table_as_dataframe()`
4. `src/fullmetalalchemy/session.py` or new `connection.py` - Add connection-based operations
5. `src/fullmetalalchemy/delete.py` - Enhance with `delete_records_by_primary_keys()`
6. New `src/fullmetalalchemy/schema.py` - Schema evolution functions

## Dependencies to Add

- `pandas>=1.5.0` (for DataFrame support) - **NEW DEPENDENCY**
- Consider `transmutation>=0.1.0` (for schema evolution) - **OPTIONAL**

## Testing Requirements

Each new function should have:
- Unit tests for single/composite primary keys
- Integration tests for SQLite, PostgreSQL, MySQL
- Tests for schema parameter support
- Tests for edge cases (empty tables, missing columns, etc.)

## Migration Strategy for Pandalchemy

Once fullmetalalchemy features are implemented:

1. Replace `table_exists()` → `fa.features.table_exists()`
2. Replace `get_primary_key()` → `fa.features.get_primary_key_names()`
3. Replace `pull_table()` → `fa.select.select_table_as_dataframe()`
4. Replace `create_table_from_dataframe()` → `fa.create.create_table_from_dataframe()`
5. Replace `_execute_inserts()` → `fa.connection.insert_records_connection()` (or session version)
6. Replace `_execute_updates()` → `fa.connection.update_records_connection()` (with autoload_with)
7. Replace `_execute_deletes()` → `fa.delete.delete_records_by_primary_keys()` or connection version
8. Replace schema changes → `fa.schema.add_column()`, `drop_column()`, `rename_column()`, etc.

## Notes

- Fullmetalalchemy already has good composite PK support in UPDATE via `match_column_names`
- Fullmetalalchemy's session module provides transaction control, but connection-level is needed for `autoload_with`
- The `transmutation` library handles some schema evolution, but fullmetalalchemy should provide a unified API
- Database-specific handling (MySQL VARCHAR, PostgreSQL metadata locks) should be abstracted in fullmetalalchemy

