# Plan: Immutable Index & Auto-Increment Primary Keys

## Overview

Implement two key features:
1. **Immutable Index**: Primary key values (stored in index) cannot be modified, only deleted or created
2. **Auto-Increment Support**: Automatically generate next PK value when inserting rows without explicit PK

## Requirements

### Immutability
- Index values are completely immutable
- Cannot update/change primary key values
- Can delete rows (removes from index)
- Can add new rows (adds to index)
- Block any operations that would modify index values

### Auto-Increment
- When `add_row()` called without PK value, auto-generate next value
- For single integer PKs only (like SQL AUTO_INCREMENT/SERIAL)
- Get max PK from current DataFrame AND database table
- Use max + 1 as next value
- Track whether a table uses auto-increment

---

## Implementation Plan

### Phase 1: Immutable Index

#### 1.1 Block Index Setter in TrackedDataFrame
```python
@index.setter
def index(self, value):
    """
    Prevent direct index modification since index contains primary keys.
    
    Raises:
        DataValidationError: Primary keys are immutable
    """
    from pandalchemy.exceptions import DataValidationError
    
    raise DataValidationError(
        "Cannot modify index directly. Primary key values are immutable. "
        "Use add_row() to create or delete_row() to remove records.",
        details={'operation': 'set_index'}
    )
```

#### 1.2 Add Validation to update_row()
```python
def update_row(self, primary_key_value: Any, updates: dict) -> None:
    """Update a row - CANNOT update primary key values."""
    pk_cols = self._get_pk_columns()
    
    # VALIDATE: updates should NOT include PK columns
    pk_in_updates = [col for col in pk_cols if col in updates]
    if pk_in_updates:
        raise DataValidationError(
            f"Cannot update primary key column(s): {pk_in_updates}. "
            "Primary keys are immutable. To change a primary key, "
            "delete the row and insert a new one.",
            details={'attempted_pk_updates': pk_in_updates}
        )
    
    # ... rest of implementation
```

#### 1.3 Review and Fix Tests
- Search for any tests that try to update PK values
- Update them to use delete + insert pattern
- Ensure all tests respect PK immutability

### Phase 2: Auto-Increment Support

#### 2.1 Add auto_increment Attribute to Table
```python
class Table(ITable):
    def __init__(
        self,
        name: str,
        data: DataFrame | None = None,
        key: str | None = None,
        engine: Engine | None = None,
        db: DataBase | None = None,
        schema: str | None = None,
        auto_increment: bool = False  # NEW PARAMETER
    ):
        self.name = name
        self.engine = engine
        self.schema = schema
        self.db = db
        self.key = key
        self.auto_increment = auto_increment  # Track if PK is auto-increment
        
        # ... rest of initialization
```

#### 2.2 Detect Auto-Increment from Database
```python
def _detect_auto_increment(engine: Engine, table_name: str, pk_col: str, schema: str | None) -> bool:
    """
    Detect if a column has auto-increment/SERIAL.
    
    Returns:
        True if column is auto-incrementing
    """
    inspector = inspect(engine)
    
    # SQLite: check for INTEGER PRIMARY KEY (auto-increments)
    # PostgreSQL: check for SERIAL/BIGSERIAL
    # MySQL: check for AUTO_INCREMENT
    
    columns = inspector.get_columns(table_name, schema=schema)
    for col in columns:
        if col['name'] == pk_col:
            # Check if autoincrement
            return col.get('autoincrement', False) or \
                   col.get('default', '').lower().startswith('nextval')
    
    return False
```

#### 2.3 Add get_next_pk_value() Method
```python
def get_next_pk_value(self) -> int:
    """
    Get the next primary key value for auto-increment.
    
    Returns:
        Next available PK value (max + 1)
        
    Raises:
        ValueError: If PK is not auto-incrementable (composite or non-integer)
    """
    pk_cols = self._get_pk_columns()
    
    # Only works for single integer PK
    if len(pk_cols) != 1:
        raise ValueError("Auto-increment only works with single-column primary keys")
    
    pk_col = pk_cols[0]
    
    # Get max value from current DataFrame
    if self._data.index.name == pk_col:
        current_max = self._data.index.max() if len(self._data) > 0 else 0
    elif pk_col in self._data.columns:
        current_max = self._data[pk_col].max() if len(self._data) > 0 else 0
    else:
        current_max = 0
    
    # If we have a database connection, check DB max too
    # (in case DB has newer data)
    db_max = current_max
    if hasattr(self, '_table_ref') and self._table_ref:
        # Query database for max PK
        # This would require access to engine/table
        pass
    
    return max(current_max, db_max) + 1
```

#### 2.4 Update add_row() for Auto-Increment
```python
def add_row(self, row_data: dict | None = None, validate: bool = True, auto_increment: bool = False) -> None:
    """
    Add a new row to the DataFrame with tracking.
    
    Args:
        row_data: Dictionary with column names as keys (PK optional if auto_increment=True)
        validate: Whether to validate primary key uniqueness
        auto_increment: If True and PK missing, auto-generate next value
    """
    from pandalchemy.exceptions import DataValidationError
    
    if row_data is None:
        row_data = {}
    
    pk_cols = self._get_pk_columns()
    
    # Check if PK is missing
    missing_pk = [col for col in pk_cols if col not in row_data]
    
    if missing_pk:
        if auto_increment and len(pk_cols) == 1:
            # Auto-generate PK value
            next_pk = self.get_next_pk_value()
            row_data[pk_cols[0]] = next_pk
        else:
            raise DataValidationError(
                f"Row data missing required primary key column(s): {missing_pk}",
                details={'missing': missing_pk, 'primary_key': pk_cols}
            )
    
    # ... rest of add_row implementation
```

#### 2.5 Add get_next_pk_value to Table Class
```python
class Table:
    def get_next_pk_value(self) -> int:
        """Get next auto-increment primary key value."""
        if not self.auto_increment:
            raise ValueError("Table is not configured for auto-increment")
        
        pk_cols = self._get_pk_columns_from_key()
        if len(pk_cols) != 1:
            raise ValueError("Auto-increment only works with single-column primary keys")
        
        # Get max from current data
        current_df = self.data.to_pandas().reset_index()
        current_max = current_df[self.key].max() if len(current_df) > 0 else 0
        
        # Query database for max (if table exists)
        if self.engine and table_exists(self.engine, self.name, self.schema):
            from sqlalchemy import select, text
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT MAX({self.key}) as max_id FROM {self.name}")
                ).fetchone()
                db_max = result[0] if result and result[0] else 0
                return max(current_max, db_max) + 1
        
        return current_max + 1
```

### Phase 3: Testing & Validation

#### 3.1 Tests for Immutability
- `test_index_setter_raises_error()` - Verify index is immutable
- `test_update_row_with_pk_raises_error()` - Cannot update PK
- `test_delete_and_recreate_works()` - Can delete + insert with new PK
- `test_upsert_with_different_pk_raises_error()` - Upsert cannot change PK

#### 3.2 Tests for Auto-Increment
- `test_add_row_auto_increment_single_pk()` - Auto-gen PK for new row
- `test_get_next_pk_value()` - Correct next value
- `test_auto_increment_respects_db_max()` - Uses DB max, not just local
- `test_auto_increment_fails_for_composite_pk()` - Error for composite
- `test_auto_increment_sequential()` - Multiple inserts increment correctly

#### 3.3 Review Existing Tests
- Search for tests that modify index
- Search for tests that update PK values
- Update to use proper patterns (delete + insert)

---

## Files to Modify

### Core Implementation
1. **src/pandalchemy/tracked_dataframe.py**
   - Block index setter
   - Add PK validation to update_row()
   - Add get_next_pk_value() method
   - Update add_row() with auto_increment support

2. **src/pandalchemy/pandalchemy_base.py**
   - Add auto_increment parameter to Table.__init__()
   - Add auto_increment detection on pull
   - Add get_next_pk_value() to Table class
   - Pass auto_increment context to TrackedDataFrame

3. **src/pandalchemy/sql_operations.py**
   - Add detect_auto_increment() function
   - Add get_max_pk_value() function

### Tests
4. **tests/test_immutable_index.py** (NEW)
   - Comprehensive immutability tests

5. **tests/test_auto_increment.py** (NEW)
   - Auto-increment functionality tests

6. **Review and fix:**
   - tests/test_integration.py
   - tests/test_tracked_dataframe_sql_helpers.py
   - tests/test_edge_cases.py
   - Any other tests that modify PKs

---

## Usage Examples

### Immutable Index
```python
table = db['users']

# ✅ Allowed: Add new row
table.data.add_row({'id': 4, 'name': 'Dave'})

# ✅ Allowed: Delete row
table.data.delete_row(2)

# ✅ Allowed: Update non-PK columns
table.data.update_row(1, {'name': 'Alicia', 'age': 26})

# ❌ NOT Allowed: Update PK value
table.data.update_row(1, {'id': 999})  # Raises DataValidationError

# ❌ NOT Allowed: Set index directly
table.data.index = [10, 20, 30]  # Raises DataValidationError

# ✅ Workaround to "change" PK: delete + insert
table.data.delete_row(1)
table.data.add_row({'id': 999, 'name': 'Alice', 'age': 25})
```

### Auto-Increment
```python
# Create table with auto-increment
table = Table('users', df, 'id', engine, auto_increment=True)

# Add row without specifying PK - auto-generated
table.data.add_row({'name': 'Dave', 'age': 40}, auto_increment=True)
# Automatically gets id = 4 (max existing id + 1)

# Explicit PK still works
table.data.add_row({'id': 100, 'name': 'Eve', 'age': 45})

# Get next PK value manually
next_id = table.get_next_pk_value()  # Returns 101
```

---

## Edge Cases to Handle

1. **Composite Keys**: Cannot auto-increment (raise error)
2. **String PKs**: Cannot auto-increment (raise error)  
3. **Empty Table**: Next PK should be 1
4. **Concurrent Access**: Race conditions (document limitation)
5. **Custom Start Value**: How to set starting value for auto-increment?
6. **DB Out of Sync**: Always check DB max, not just local max

---

## Breaking Changes

**Potential Breaking Changes:**
1. Cannot set `table.data.index = [...]` (now raises error)
2. Cannot update PK values via `update_row()`
3. Need to explicitly enable auto_increment (opt-in)

**Migration:**
- Document that PKs are immutable
- Provide delete + insert pattern for "changing" PKs
- Auto-increment is opt-in per table

---

## Questions for Consideration

1. **Auto-increment detection**: Should we auto-detect from DB schema, or require explicit flag?
2. **Composite keys**: Should we allow auto-increment on first column only?
3. **UUID PKs**: Should we support UUID generation as well?
4. **Custom generators**: Allow custom PK generation functions?
5. **Database sequences**: Should we use database sequences instead of max+1?

---

## Recommended Approach

**Step 1**: Implement immutability (safer, simpler)
**Step 2**: Add basic auto-increment for single integer PKs
**Step 3**: Extend with detection and advanced features
**Step 4**: Document patterns and best practices

This keeps changes incremental and testable.

