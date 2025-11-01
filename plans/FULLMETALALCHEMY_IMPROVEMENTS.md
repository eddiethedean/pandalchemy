# Fullmetalalchemy Improvements Plan

## Overview

This document outlines the specific improvements and new features needed in fullmetalalchemy to support a complete migration of pandalchemy from direct SQLAlchemy usage to fullmetalalchemy APIs. These improvements will enable pandalchemy to replace all its custom SQL operations with fullmetalalchemy functions.

## Implementation Phases

### Phase 1: Core Functionality (Critical)
Features needed for basic CRUD operations migration.

### Phase 2: Advanced Operations (Important)
Features needed for transaction support and composite keys.

### Phase 3: Schema Evolution (Nice to Have)
Features for dynamic schema changes.

---

## Phase 1: Core Functionality

### 1.1 Table Existence Check

**File**: `src/fullmetalalchemy/features.py`

**Function Signature**:
```python
def table_exists(
    table_name: str, 
    engine: Engine, 
    schema: Optional[str] = None
) -> bool:
    """
    Check if a table exists in the database.
    
    More efficient than checking via get_table_names() as it uses
    direct introspection without loading all table names.
    
    Parameters
    ----------
    table_name : str
        Name of the table to check
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name (for databases that support schemas)
    
    Returns
    -------
    bool
        True if table exists, False otherwise
    
    Examples
    --------
    >>> import fullmetalalchemy as fa
    >>> engine = fa.create_engine('sqlite:///test.db')
    >>> fa.features.table_exists('users', engine)
    False
    >>> fa.create.create_table_from_records('users', [...], 'id', engine)
    >>> fa.features.table_exists('users', engine)
    True
    """
```

**Implementation**:
```python
def table_exists(table_name: str, engine: Engine, schema: Optional[str] = None) -> bool:
    """Check if a table exists without loading all table names."""
    inspector = _sa.inspect(engine)
    return table_name in inspector.get_table_names(schema=schema)
```

**Tests Required**:
- Test with SQLite (no schema)
- Test with PostgreSQL (with schema)
- Test with MySQL (with schema)
- Test with non-existent table
- Test with existing table

---

### 1.2 Primary Key Retrieval from Engine

**File**: `src/fullmetalalchemy/features.py`

**Function Signature**:
```python
def get_primary_key_names(
    table_name: str, 
    engine: Engine, 
    schema: Optional[str] = None
) -> Optional[Union[str, List[str]]]:
    """
    Get primary key column name(s) for a table.
    
    This is more efficient than get_table() + primary_key_names() as it
    only queries primary key information without loading full table metadata.
    
    Parameters
    ----------
    table_name : str
        Name of the table
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name
    
    Returns
    -------
    Optional[Union[str, List[str]]]
        - None if no primary key exists
        - str if single-column primary key
        - List[str] if composite primary key
    
    Examples
    --------
    >>> engine = fa.create_engine('sqlite:///test.db')
    >>> fa.features.get_primary_key_names('users', engine)
    'id'
    >>> fa.features.get_primary_key_names('memberships', engine)
    ['user_id', 'org_id']
    >>> fa.features.get_primary_key_names('logs', engine)
    None
    """
```

**Implementation**:
```python
def get_primary_key_names(
    table_name: str, 
    engine: Engine, 
    schema: Optional[str] = None
) -> Optional[Union[str, List[str]]]:
    """Get primary key column names without loading full table metadata."""
    inspector = _sa.inspect(engine)
    pk_constraint = inspector.get_pk_constraint(table_name, schema=schema)
    
    if not pk_constraint or not pk_constraint.get('constrained_columns'):
        return None
    
    pk_cols = pk_constraint['constrained_columns']
    if len(pk_cols) == 1:
        return pk_cols[0]
    return pk_cols
```

**Tests Required**:
- Single-column primary key
- Composite primary key
- No primary key
- With schema parameter
- Non-existent table (should raise appropriate error)

---

### 1.3 Create Table from DataFrame

**File**: `src/fullmetalalchemy/create.py`

**Dependencies**: Add `pandas>=1.5.0` to `pyproject.toml`

**Function Signature**:
```python
def create_table_from_dataframe(
    table_name: str,
    df: pd.DataFrame,
    primary_key: Union[str, List[str]],
    engine: Engine,
    schema: Optional[str] = None,
    if_exists: str = "fail"
) -> Table:
    """
    Create a SQL table from a pandas DataFrame.
    
    Handles:
    - Empty DataFrames (defaults to String type for columns)
    - MySQL VARCHAR length requirements (VARCHAR(255) for strings)
    - SQLite DateTime limitations (converts to String)
    - Proper pandas dtype to SQLAlchemy type mapping
    - Composite primary keys
    
    Parameters
    ----------
    table_name : str
        Name of the table to create
    df : pd.DataFrame
        DataFrame to create table from
    primary_key : Union[str, List[str]]
        Primary key column name(s)
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name
    if_exists : str
        What to do if table exists: 'fail', 'replace', or 'append'
    
    Returns
    -------
    Table
        SQLAlchemy Table object
    
    Raises
    ------
    ValueError
        If table exists and if_exists='fail'
        If primary key column(s) not in DataFrame
    
    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
    >>> table = fa.create.create_table_from_dataframe(
    ...     'users', df, 'id', engine
    ... )
    """
```

**Implementation Notes**:
- Handle empty DataFrames by defaulting columns to String type
- Detect MySQL dialect and use VARCHAR(255) for string columns
- Convert pandas dtypes: int64 -> Integer, float64 -> Float, bool -> Boolean, datetime64 -> DateTime (or String for SQLite)
- Use existing `create_table()` function but with DataFrame input
- Support `if_exists='replace'` using `drop_table()`
- Insert data using `insert_records()` if DataFrame is not empty

**Tests Required**:
- Empty DataFrame
- DataFrame with various dtypes (int, float, bool, string, datetime)
- Single-column primary key
- Composite primary key
- MySQL VARCHAR length requirement
- SQLite DateTime conversion
- `if_exists='replace'` behavior
- `if_exists='append'` behavior (should fail if not implemented)
- Schema parameter

---

### 1.4 Select Table as DataFrame

**File**: `src/fullmetalalchemy/select.py`

**Dependencies**: Add `pandas>=1.5.0` to `pyproject.toml`

**Function Signature**:
```python
def select_table_as_dataframe(
    table: Union[Table, str],
    connection: Optional[SqlConnection] = None,
    schema: Optional[str] = None,
    primary_key: Optional[Union[str, List[str]]] = None,
    set_index: bool = True
) -> pd.DataFrame:
    """
    Read a SQL table into a pandas DataFrame with proper type inference.
    
    Uses optimal method per database:
    - SQLite: pd.read_sql_table (better type inference)
    - PostgreSQL/MySQL: pd.read_sql (avoids inspect() hangs after schema changes)
    
    Parameters
    ----------
    table : Union[Table, str]
        Table object or table name
    connection : Optional[SqlConnection]
        Engine or connection (defaults to table's engine if Table object)
    schema : Optional[str]
        Schema name
    primary_key : Optional[Union[str, List[str]]]
        Primary key to set as DataFrame index
    set_index : bool
        Whether to set primary key as index (default True)
    
    Returns
    -------
    pd.DataFrame
        DataFrame with data from table, optionally indexed by primary key
    
    Examples
    --------
    >>> df = fa.select.select_table_as_dataframe('users', engine, primary_key='id')
    >>> df.index.name
    'id'
    """
```

**Implementation Notes**:
- For SQLite: try `pd.read_sql_table()` first for better type inference
- For PostgreSQL/MySQL: use `pd.read_sql()` with database-specific quoting:
  - MySQL: backticks `` `table_name` ``
  - PostgreSQL: double quotes `"table_name"`
  - SQLite: double quotes `"table_name"`
- Handle type inference failures gracefully (return data as-is with warning)
- If primary_key provided and set_index=True, set it as DataFrame index
- Convert string representations of numbers to numeric types when possible

**Tests Required**:
- SQLite table reading
- PostgreSQL table reading
- MySQL table reading
- With primary key as index
- Without setting index
- Composite primary key as MultiIndex
- Type inference (int, float, bool, datetime, string)
- Empty table
- With schema parameter

---

### 1.5 Connection-Based Operations Module

**File**: New `src/fullmetalalchemy/connection.py`

**Purpose**: Provide connection-level operations (not session-level) for transaction control, especially needed for `autoload_with=connection` to see transaction state.

**Functions to Implement**:

#### 1.5.1 Insert Records Connection

```python
def insert_records_connection(
    table: Union[Table, str],
    records: Sequence[Record],
    connection: Connection
) -> None:
    """
    Insert records using a connection (for transaction control).
    
    This is useful when you need to insert within an existing transaction
    and want to use the connection object directly rather than a session.
    
    Parameters
    ----------
    table : Union[Table, str]
        Table object or name
    records : Sequence[Record]
        Records to insert
    connection : Connection
        SQLAlchemy connection (from engine.connect() or engine.begin())
    
    Examples
    --------
    >>> with engine.begin() as conn:
    ...     fa.connection.insert_records_connection('users', records, conn)
    """
```

#### 1.5.2 Update Records Connection

```python
def update_records_connection(
    table: Union[Table, str],
    records: Sequence[Record],
    connection: Connection,
    match_column_names: Optional[Sequence[str]] = None
) -> None:
    """
    Update records using a connection with autoload_with support.
    
    This is critical for seeing transaction state - uses autoload_with=connection
    so that schema changes within the same transaction are visible.
    
    Parameters
    ----------
    table : Union[Table, str]
        Table object or name
    records : Sequence[Record]
        Records to update (must include match columns)
    connection : Connection
        SQLAlchemy connection
    match_column_names : Optional[Sequence[str]]
        Columns to match on (defaults to primary key)
    
    Examples
    --------
    >>> with engine.begin() as conn:
    ...     fa.connection.update_records_connection(
    ...         'users', 
    ...         [{'id': 1, 'name': 'Alice'}],
    ...         conn
    ...     )
    """
```

**Implementation Note**: Use `Table(table_name, MetaData(), autoload_with=connection, schema=schema)` to ensure transaction visibility.

#### 1.5.3 Delete Records Connection

```python
def delete_records_connection(
    table: Union[Table, str],
    connection: Connection,
    column_name: Optional[str] = None,
    values: Optional[Sequence[Any]] = None,
    records: Optional[Sequence[Record]] = None,
    primary_key_values: Optional[Sequence[Union[Any, Tuple[Any, ...]]]] = None
) -> None:
    """
    Delete records using a connection.
    
    Supports multiple deletion patterns:
    - By column values: column_name + values
    - By record matching: records
    - By primary key: primary_key_values (single or composite)
    
    Parameters
    ----------
    table : Union[Table, str]
        Table object or name
    connection : Connection
        SQLAlchemy connection
    column_name : Optional[str]
        Column name for value-based deletion
    values : Optional[Sequence[Any]]
        Values to match in column_name
    records : Optional[Sequence[Record]]
        Full records to match
    primary_key_values : Optional[Sequence[Union[Any, Tuple[Any, ...]]]]
        Primary key values (single values or tuples for composite)
    
    Examples
    --------
    >>> with engine.begin() as conn:
    ...     # Delete by single PK
    ...     fa.connection.delete_records_connection(
    ...         'users', conn, primary_key_values=[1, 2, 3]
    ...     )
    ...     # Delete by composite PK
    ...     fa.connection.delete_records_connection(
    ...         'memberships', 
    ...         conn, 
    ...         primary_key_values=[(1, 10), (2, 20)]
    ...     )
    """
```

**Tests Required** (for all connection functions):
- Within transaction context
- With autoload_with visibility
- Single primary key operations
- Composite primary key operations
- Error handling and rollback
- Connection vs session behavior differences

---

## Phase 2: Advanced Operations

### 2.1 Enhanced Delete by Primary Keys

**File**: `src/fullmetalalchemy/delete.py`

**Function Signature**:
```python
def delete_records_by_primary_keys(
    table: Union[Table, str],
    primary_key_values: Sequence[Union[Any, Tuple[Any, ...]]],
    connection: Optional[SqlConnection] = None
) -> None:
    """
    Delete records by primary key values (single or composite).
    
    More efficient than delete_records_by_values() when you only have
    primary key values, not full records.
    
    Parameters
    ----------
    table : Union[Table, str]
        Table object or name
    primary_key_values : Sequence[Union[Any, Tuple[Any, ...]]]
        For single PK: List[Any]
        For composite PK: List[Tuple[Any, ...]]
    connection : Optional[SqlConnection]
        Engine or connection
    
    Examples
    --------
    >>> # Single PK
    >>> fa.delete.delete_records_by_primary_keys(
    ...     'users', [1, 2, 3], engine
    ... )
    >>> # Composite PK
    >>> fa.delete.delete_records_by_primary_keys(
    ...     'memberships',
    ...     [(1, 10), (2, 20)],
    ...     engine
    ... )
    """
```

**Implementation**:
- Detect if table has single or composite PK
- Validate that values match PK structure
- Build appropriate WHERE clause
- Execute deletes

**Tests Required**:
- Single primary key
- Composite primary key
- Empty list
- Invalid values (wrong tuple length for composite)
- Non-existent records

---

### 2.2 Get Table Columns (Avoid Hangs)

**File**: `src/fullmetalalchemy/features.py`

**Function Signature**:
```python
def get_table_columns(
    table_name: str,
    engine: Engine,
    schema: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get column information without full table reflection.
    
    Uses inspect(engine).get_columns() instead of autoload_with to avoid
    potential hangs in PostgreSQL after schema changes.
    
    Parameters
    ----------
    table_name : str
        Name of the table
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name
    
    Returns
    -------
    List[Dict[str, Any]]
        List of column information dicts with keys:
        - 'name': str
        - 'type': SQLAlchemy type
        - 'nullable': bool
        - 'default': Any
        - Other column attributes
    
    Examples
    --------
    >>> cols = fa.features.get_table_columns('users', engine)
    >>> cols[0]['name']
    'id'
    >>> cols[0]['type']
    INTEGER()
    """
```

**Tests Required**:
- SQLite columns
- PostgreSQL columns
- MySQL columns
- With schema
- After schema changes (should not hang)
- Non-existent table

---

## Phase 3: Schema Evolution

### 3.1 Schema Evolution Module

**File**: New `src/fullmetalalchemy/schema.py`

**Dependencies**: Consider `transmutation>=0.1.0` as optional dependency

### 3.1.1 Add Column

```python
def add_column(
    table_name: str,
    column_name: str,
    column_type: type,
    engine: Engine,
    schema: Optional[str] = None,
    default_value: Optional[Any] = None,
    nullable: bool = True
) -> None:
    """
    Add a new column to an existing table.
    
    Handles database-specific requirements:
    - MySQL: VARCHAR requires explicit length (defaults to VARCHAR(255))
    - PostgreSQL: Supports all types
    - SQLite: Limited ALTER TABLE support
    
    Parameters
    ----------
    table_name : str
        Name of the table
    column_name : str
        Name of the new column
    column_type : type
        Python type (int, str, float, bool, datetime, etc.)
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name
    default_value : Optional[Any]
        Default value for existing rows
    nullable : bool
        Whether column can be NULL (default True)
    
    Examples
    --------
    >>> fa.schema.add_column('users', 'email', str, engine)
    >>> fa.schema.add_column('users', 'age', int, engine, default_value=0)
    """
```

### 3.1.2 Drop Column

```python
def drop_column(
    table_name: str,
    column_name: str,
    engine: Engine,
    schema: Optional[str] = None
) -> None:
    """
    Drop a column from a table.
    
    Parameters
    ----------
    table_name : str
        Name of the table
    column_name : str
        Name of the column to drop
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name
    
    Examples
    --------
    >>> fa.schema.drop_column('users', 'old_field', engine)
    """
```

### 3.1.3 Rename Column

```python
def rename_column(
    table_name: str,
    old_column_name: str,
    new_column_name: str,
    engine: Engine,
    schema: Optional[str] = None
) -> None:
    """
    Rename a column in a table.
    
    Handles database-specific syntax:
    - PostgreSQL: ALTER TABLE ... RENAME COLUMN ... TO ...
    - MySQL: ALTER TABLE ... CHANGE COLUMN ... ... (requires type lookup)
    - SQLite: Limited support (may need to recreate table)
    
    Parameters
    ----------
    table_name : str
        Name of the table
    old_column_name : str
        Current column name
    new_column_name : str
        New column name
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name
    
    Examples
    --------
    >>> fa.schema.rename_column('users', 'user_name', 'username', engine)
    """
```

**Implementation Notes**:
- For MySQL: Use `CHANGE COLUMN` with existing type (lookup via `get_table_columns()`)
- For PostgreSQL: Use `RENAME COLUMN` (simpler, no type needed)
- For SQLite: May need table recreation (check SQLite version support)

### 3.1.4 Alter Column Type

```python
def alter_column_type(
    table_name: str,
    column_name: str,
    new_type: type,
    engine: Engine,
    schema: Optional[str] = None
) -> None:
    """
    Change the data type of a column.
    
    Parameters
    ----------
    table_name : str
        Name of the table
    column_name : str
        Name of the column
    new_type : type
        New Python type
    engine : Engine
        SQLAlchemy engine
    schema : Optional[str]
        Schema name
    
    Examples
    --------
    >>> fa.schema.alter_column_type('users', 'age', float, engine)
    """
```

**Tests Required** (for all schema functions):
- SQLite schema changes
- PostgreSQL schema changes
- MySQL schema changes
- With schema parameter
- Error handling (column doesn't exist, etc.)
- MySQL VARCHAR length handling
- PostgreSQL rename vs MySQL rename
- Type conversion validation

---

## Implementation Checklist

### Phase 1: Core Functionality
- [ ] Add `table_exists()` to `features.py`
- [ ] Add `get_primary_key_names()` to `features.py`
- [ ] Add `create_table_from_dataframe()` to `create.py`
- [ ] Add `select_table_as_dataframe()` to `select.py`
- [ ] Create new `connection.py` module
- [ ] Add `insert_records_connection()` to `connection.py`
- [ ] Add `update_records_connection()` to `connection.py`
- [ ] Add `delete_records_connection()` to `connection.py`
- [ ] Update `pyproject.toml` to add pandas dependency
- [ ] Update `__init__.py` to export new functions
- [ ] Write unit tests for all Phase 1 functions
- [ ] Write integration tests (SQLite, PostgreSQL, MySQL)

### Phase 2: Advanced Operations
- [ ] Add `delete_records_by_primary_keys()` to `delete.py`
- [ ] Add `get_table_columns()` to `features.py`
- [ ] Write unit tests
- [ ] Write integration tests

### Phase 3: Schema Evolution
- [ ] Create new `schema.py` module
- [ ] Add `add_column()` to `schema.py`
- [ ] Add `drop_column()` to `schema.py`
- [ ] Add `rename_column()` to `schema.py`
- [ ] Add `alter_column_type()` to `schema.py`
- [ ] Update `__init__.py` to export schema functions
- [ ] Write unit tests
- [ ] Write integration tests for all supported databases

## Testing Strategy

### Unit Tests
- Test each function in isolation
- Mock database interactions where appropriate
- Test error handling
- Test edge cases (empty inputs, None values, etc.)

### Integration Tests
- Test with real SQLite database
- Test with real PostgreSQL database (if available)
- Test with real MySQL database (if available)
- Test schema parameter support
- Test transaction behavior
- Test connection vs session differences

### Database-Specific Tests
- MySQL: VARCHAR length requirements
- PostgreSQL: Metadata lock avoidance
- SQLite: Limited ALTER TABLE support
- Cross-database compatibility

## Documentation Requirements

- Update main README with new functions
- Add examples for each new function
- Document pandas dependency
- Document database-specific behavior
- Add migration guide from direct SQLAlchemy

## Dependencies Impact

### New Required Dependency
- `pandas>=1.5.0` (for DataFrame support)

### Optional Dependency
- `transmutation>=0.1.0` (for schema evolution, if not implementing raw SQL)

### Version Updates
- Update `pyproject.toml` with new dependencies
- Update version number (likely minor or major version bump)

## Success Criteria

Fullmetalalchemy improvements are complete when:

1. ✅ All Phase 1 functions implemented and tested
2. ✅ All Phase 2 functions implemented and tested
3. ✅ All Phase 3 functions implemented and tested
4. ✅ All tests passing (unit + integration)
5. ✅ Documentation updated
6. ✅ Pandalchemy can replace all SQLAlchemy usage with fullmetalalchemy calls
7. ✅ No regression in existing fullmetalalchemy functionality
8. ✅ Performance is acceptable (no significant degradation)

## Notes

- Consider making pandas an optional dependency with `[pandas]` extra if not all users need DataFrame support
- Connection-based operations are critical for pandalchemy's transaction model
- Schema evolution can leverage `transmutation` library or implement raw SQL for better control
- Database-specific handling should be abstracted to provide a consistent API

