# Pandalchemy Architecture

## Overview

Pandalchemy 0.2.0 features a completely redesigned architecture centered around automatic change tracking and optimized SQL execution. This document describes the key components and how they work together.

## Core Components

### 1. TrackedDataFrame

**Location**: `src/pandalchemy/tracked_dataframe.py`

The `TrackedDataFrame` class is a wrapper around pandas DataFrame that intercepts all modification operations.

**Key Features**:
- Delegates all attribute access to the underlying pandas DataFrame
- Intercepts mutating methods (`drop`, `rename`, `sort_values`, etc.)
- Tracks column additions, deletions, and modifications
- Provides special indexers (`loc`, `iloc`, `at`, `iat`) that track changes
- Maintains full pandas API compatibility

**How It Works**:
```python
class TrackedDataFrame:
    def __setitem__(self, key, value):
        # Record the operation
        self._tracker.record_operation('__setitem__', key, value)
        
        # Track new columns
        if key not in self._data.columns:
            self._tracker.track_column_addition(key)
        
        # Perform the operation
        self._data[key] = value
        
        # Recompute row changes
        self._tracker.compute_row_changes(self._data)
```

### 2. ChangeTracker

**Location**: `src/pandalchemy/change_tracker.py`

The `ChangeTracker` class monitors changes at two levels:

1. **Operation Level**: Records every method call and operation
2. **Row Level**: Tracks which rows were inserted, updated, or deleted

**Data Structures**:
```python
@dataclass
class RowChange:
    change_type: ChangeType  # INSERT, UPDATE, DELETE
    primary_key_value: Any
    old_data: Optional[Dict[str, Any]]
    new_data: Optional[Dict[str, Any]]
```

**Key Methods**:
- `record_operation()`: Log an operation
- `track_column_addition()`: Mark a column as added
- `track_column_drop()`: Mark a column as dropped
- `compute_row_changes()`: Compare current vs original data to find row changes
- `get_inserts()`, `get_updates()`, `get_deletes()`: Retrieve specific change types

### 3. ExecutionPlan

**Location**: `src/pandalchemy/execution_plan.py`

The `ExecutionPlan` class analyzes tracked changes and generates an optimized sequence of SQL operations.

**Priority System**:
```
Priority 1-3:  Schema changes (rename, drop, add columns)
Priority 10:   Deletes (remove old data first)
Priority 20:   Updates (modify existing data)
Priority 30:   Inserts (add new data last)
```

**Optimization Strategies**:
1. **Batching**: All inserts/updates/deletes of the same type are batched together
2. **Ordering**: Schema changes before data changes; deletes before inserts
3. **Minimization**: Multiple row updates combined into single SQL operations

**Plan Structure**:
```python
@dataclass
class PlanStep:
    operation_type: OperationType
    description: str
    data: Any
    priority: int
```

### 4. SQL Operations

**Location**: `src/pandalchemy/sql_operations.py`

This module provides high-level functions that wrap `fullmetalalchemy` and `transmutation`:

**Key Functions**:
- `execute_plan()`: Execute a complete ExecutionPlan in a transaction
- `_execute_schema_change()`: Apply column add/drop/rename using transmutation
- `_execute_inserts()`: Batch insert using fullmetalalchemy
- `_execute_updates()`: Batch update using fullmetalalchemy
- `_execute_deletes()`: Batch delete using fullmetalalchemy

**Transaction Management**:
```python
def execute_plan(engine, table_name, plan, schema, primary_key):
    with engine.begin() as connection:
        try:
            for step in plan.steps:
                # Execute each step
                ...
        except Exception as e:
            # Automatic rollback on error
            raise SQLAlchemyError(f"Failed: {e}") from e
```

### 5. Table and DataBase

**Location**: `src/pandalchemy/pandalchemy_base.py`

**Table Class**:
- Wraps a `TrackedDataFrame`
- Provides pandas-like interface
- Implements `push()` method that:
  1. Gets current DataFrame state
  2. Gets the ChangeTracker
  3. Builds an ExecutionPlan
  4. Executes the plan
  5. Refreshes from database

**DataBase Class**:
- Manages multiple Table objects
- Provides dictionary-like access to tables
- Implements multi-table `push()` that executes all changes in one transaction

## Data Flow

### 1. User Makes Changes

```
User: table['age'] = table['age'] + 1
  ↓
TrackedDataFrame.__setitem__('age', ...)
  ↓
ChangeTracker.record_operation('__setitem__', ...)
  ↓
ChangeTracker.compute_row_changes(current_data)
```

### 2. Push Operation

```
User: table.push()
  ↓
Table.push()
  ↓
ExecutionPlan(tracker, current_df)
  ↓
Execute plan steps in priority order:
  1. Schema changes (ALTER TABLE)
  2. Deletes (DELETE FROM)
  3. Updates (UPDATE SET)
  4. Inserts (INSERT INTO)
  ↓
Commit transaction or rollback on error
  ↓
Refresh table from database
```

### 3. Change Detection Algorithm

```python
def compute_row_changes(current_data):
    original_keys = set(original_data.index)
    current_keys = set(current_data.index)
    
    # Find inserts
    inserted = current_keys - original_keys
    
    # Find deletes
    deleted = original_keys - current_keys
    
    # Find updates
    common = current_keys & original_keys
    for key in common:
        if not rows_equal(original[key], current[key]):
            mark_as_updated(key)
```

## Integration with External Libraries

### fullmetalalchemy

Used for data operations (insert, update, delete, select):

```python
fa.insert_records(connection, table_name, records)
fa.update_records(connection, table_name, values, where)
fa.delete_records(connection, table_name, where)
fa.select_records(engine, table_name)
```

### transmutation

Used for schema migrations:

```python
tm.add_column(connection, table_name, column_name, column_type)
tm.drop_column(connection, table_name, column_name)
tm.rename_column(connection, table_name, old_name, new_name)
```

## Design Decisions

### Why Wrapper Instead of Subclass?

We chose to wrap pandas DataFrame rather than subclass it because:
1. **Easier Interception**: Can intercept any attribute access via `__getattribute__`
2. **Less Fragile**: Don't have to worry about pandas internal methods
3. **Cleaner Separation**: Clear boundary between pandas and tracking logic
4. **Flexibility**: Can swap out the underlying DataFrame implementation

### Why Two-Level Tracking?

We track at both operation and row levels because:
1. **Operation Level**: Useful for debugging and understanding what user did
2. **Row Level**: Essential for generating correct SQL operations
3. **Redundancy**: Provides validation and fallback mechanisms

### Why Priority-Based Execution?

The priority system ensures:
1. **Correctness**: Schema changes must happen before data changes
2. **Efficiency**: Deletes before inserts to avoid conflicts
3. **Optimization**: Similar operations batched together
4. **Predictability**: Execution order is deterministic

## Performance Considerations

### Memory Usage

- `TrackedDataFrame` maintains two copies: original and current
- For large tables, this doubles memory usage
- Trade-off: Memory for automatic change tracking

### Optimization Opportunities

1. **Lazy Change Detection**: Only compute row changes when needed (on push)
2. **Batch Operations**: All operations of same type combined
3. **Minimal SQL**: Only changed rows are updated
4. **Transaction Efficiency**: All tables pushed in single transaction

### Future Improvements

1. **Incremental Tracking**: Track changes as they happen instead of comparing DataFrames
2. **Smart Batching**: Adaptive batch sizes based on operation type
3. **Parallel Execution**: Multiple tables pushed in parallel where safe
4. **Change Compression**: Merge redundant operations (e.g., update then update)

## Testing Strategy

### Unit Tests

- `test_change_tracker.py`: ChangeTracker functionality
- `test_tracked_dataframe.py`: TrackedDataFrame interception
- `test_execution_plan.py`: Plan generation and optimization

### Integration Tests

- `test_integration.py`: End-to-end workflows with real database
- Tests multiple database backends (SQLite, PostgreSQL)
- Verifies transaction rollback behavior

### Test Coverage

Target: >90% code coverage
Key areas:
- All DataFrame operations
- All change types
- Error conditions
- Transaction rollback

## Extension Points

### Custom Change Types

Add new change types by extending `ChangeType` enum:
```python
class ChangeType(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CUSTOM = "custom"  # Your custom type
```

### Custom SQL Operations

Extend `sql_operations.py` with custom operations:
```python
def custom_operation(connection, table_name, data):
    # Your custom SQL logic
    pass
```

### Custom Optimization

Extend `ExecutionPlan` to add custom optimization strategies:
```python
class CustomExecutionPlan(ExecutionPlan):
    def _build_plan(self):
        super()._build_plan()
        self._apply_custom_optimization()
```

## Backward Compatibility

### Removed Features

- `SubTable`: Use standard DataFrame slicing
- `View`: Use regular Table operations
- `sqlalchemy-migrate`: Replaced by transmutation

### Migration Path

1. Update SQLAlchemy to 2.x
2. Replace SubTable with standard operations
3. Remove manual change tracking
4. Test with new transaction behavior

## Future Roadmap

### Planned Features

1. **Async Support**: AsyncIO-compatible operations
2. **Streaming**: Handle tables larger than memory
3. **Caching**: Cache execution plans for repeated operations
4. **Monitoring**: Built-in performance monitoring and logging
5. **Type Checking**: Full mypy strict mode support

### Under Consideration

1. **Multi-Database**: Synchronize across multiple databases
2. **Conflict Resolution**: Handle concurrent modifications
3. **Query Builder**: Pandas-like query API for SQL generation
4. **Migration Tool**: Automatic database migration generation

