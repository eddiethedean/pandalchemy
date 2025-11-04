# Feature Proposal: Enhanced `to_sql` API with Primary Key Support

## Summary

This proposal extends pandas `DataFrame.to_sql()` with two new optional parameters (`primary_key` and `auto_increment`) to enable primary key creation and auto-increment functionality when writing DataFrames to SQL databases. This addresses a frequently requested feature that would eliminate the need for manual ALTER TABLE statements after table creation.

## Motivation and Use Cases

### The Problem

The current `to_sql()` method creates tables without primary keys, which is a significant limitation for real-world database workflows. Users must execute additional SQL statements to add primary key constraints, breaking the simplicity of pandas' one-line table creation.

### Why This Matters

Primary keys are fundamental to database design:
- **Data Integrity**: Enforce uniqueness and prevent duplicate records
- **Performance**: Enable efficient indexing and querying
- **Relationships**: Required for foreign key constraints
- **Best Practices**: Most database schemas require primary keys

### Common Use Cases

1. **Data Pipeline Initialization**: Creating tables from CSV/Excel files with primary keys
2. **ETL Workflows**: Setting up staging tables with proper constraints
3. **Application Development**: Creating database schemas from DataFrame definitions
4. **Data Migration**: Transferring data with primary key relationships intact

### Current Limitations

1. **No Primary Key Support**: Tables created by `to_sql()` have no primary key, requiring manual ALTER TABLE statements
2. **No Auto-Increment Support**: Cannot create tables with auto-incrementing primary keys
3. **No Composite Key Support**: Cannot specify multi-column primary keys
4. **Index Inference**: Index information is written as a regular column but not used as a primary key

### Current Workarounds

Users currently have two options, both with drawbacks:

**Option 1: Create table, then add primary key (two-step process)**
```python
df.to_sql('users', engine)
# Then manually add primary key
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE users ADD PRIMARY KEY (id)"))
    conn.commit()
```

**Option 2: Preemptively create table structure, then insert data**
```python
from sqlalchemy import Column, Integer, String, MetaData, Table, create_engine

# Create table structure with primary key first
metadata = MetaData()
users_table = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(50)),
    Column('age', Integer)
)
metadata.create_all(engine)

# Then use to_sql for data insertion (index=False since schema is pre-defined)
df.to_sql('users', engine, if_exists='append', index=False)
```

Both approaches are error-prone and break the simplicity of pandas' one-line table creation workflow. The proposed enhancement would eliminate the need for these workarounds for common use cases.

## Proposed API

### New Parameters

Add two optional parameters to `DataFrame.to_sql()`:

- `primary_key`: `str | list[str] | None` (default: `None`)
  - Specifies the primary key column(s) for the table
  - If `str`, creates a single-column primary key
  - If `list[str]`, creates a composite primary key
  - If `None`, no primary key is created (maintains current pandas behavior)

- `auto_increment`: `bool` (default: `False`)
  - If `True`, enables database-native auto-increment for the primary key
  - Only applies when creating new tables (`if_exists='fail'` or `'replace'`)
  - Only valid for single-column integer primary keys
  - Ignored when `if_exists='append'`

### Primary Key Behavior

- **When `primary_key=None` (default)**: No primary key is created. This maintains current pandas behavior for full backwards compatibility. No automatic inference from index is performed.
- **When `primary_key` is specified**: The specified column(s) are used as the primary key. The column(s) must exist in the DataFrame (either as columns or in the index if `index=True`).
- **When `if_exists='append'`**: The `primary_key` parameter is ignored when appending to existing tables. The primary key constraint is not modified on existing tables.

### Backwards Compatibility

- **Fully backwards compatible**: Both new parameters default to `None`/`False`
- Existing code continues to work unchanged
- No primary key is created if `primary_key=None` (current behavior)

## Examples

### Example 1: Basic Primary Key Creation

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///example.db')
df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
df.index.name = 'id'

# Create table with primary key
df.to_sql('users', engine, primary_key='id', if_exists='replace')
```

### Example 2: Auto-Increment Primary Key

```python
df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
df.index.name = 'id'

# Create table with auto-increment primary key
df.to_sql('users', engine, primary_key='id', auto_increment=True, if_exists='replace')

# Subsequent inserts will auto-generate IDs
new_df = pd.DataFrame({'name': ['Charlie'], 'age': [35]})
new_df.to_sql('users', engine, index=False, if_exists='append')  # id auto-generated
```

### Example 3: Composite Primary Key

```python
df = pd.DataFrame({
    'user_id': [1, 1, 2],
    'org_id': ['org1', 'org2', 'org1'],
    'role': ['admin', 'member', 'admin']
})

# Create table with composite primary key (index=False since PK is in columns)
df.to_sql('memberships', engine, primary_key=['user_id', 'org_id'], index=False, if_exists='replace')
```

### Example 4: Primary Key from MultiIndex

```python
# Create DataFrame with data and key columns
df = pd.DataFrame({
    'key1': [1, 2],
    'key2': ['a', 'b'],
    'value': [100, 200]
})

# Set MultiIndex from columns (more common pattern)
df = df.set_index(['key1', 'key2'], drop=True)

# Create table with composite primary key from MultiIndex
df.to_sql('data', engine, primary_key=['key1', 'key2'], if_exists='replace')
```

### Example 5: No Primary Key (Default Behavior)

```python
# Default behavior - no primary key created (current pandas behavior)
df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
df.to_sql('users', engine, index=False, if_exists='replace')  # No primary key, fully backwards compatible
```

### Example 6: Append Mode

```python
# Create initial table with primary key
df1 = pd.DataFrame({'name': ['Alice', 'Bob']})
df1.index.name = 'id'
df1.to_sql('users', engine, primary_key='id', if_exists='replace')

# Append new data - primary_key parameter is ignored for append mode
df2 = pd.DataFrame({'name': ['Charlie']})
df2.index.name = 'id'
df2.to_sql('users', engine, if_exists='append')  # Appends to existing table
```

## Implementation Considerations

### Database Support

The implementation should support:
- **SQLite**: Uses `AUTOINCREMENT` keyword
- **PostgreSQL**: Uses `SERIAL` or `GENERATED ALWAYS AS IDENTITY` (SQLAlchemy `Identity()`)
- **MySQL/MariaDB**: Uses `AUTO_INCREMENT` keyword
- **Other databases**: Fall back to SQLAlchemy's `autoincrement=True` parameter

### SQLAlchemy Integration

- Use SQLAlchemy Core API (`MetaData`, `Table`, `Column`, `PrimaryKeyConstraint`)
- Leverage SQLAlchemy's dialect-specific auto-increment handling
- Ensure compatibility with SQLAlchemy 2.0+ patterns

### Type Validation

- Validate `primary_key` parameter (must exist in DataFrame or index)
- Validate `auto_increment` constraints (single integer column only)
- Provide clear error messages for invalid configurations

### API Design Principles

- **Minimal API Surface**: Only two new optional parameters
- **Explicit Behavior**: No automatic inference - users must explicitly specify primary keys
- **Clear Errors**: Explicit error messages when primary key columns are not found
- **Database Agnostic**: Works across all SQLAlchemy-supported databases
- **Backwards Compatible**: Default behavior (`primary_key=None`) matches current pandas

## Error Handling

### New Exceptions

1. **Primary Key Column Not Found**
   ```python
   df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
   df.to_sql('users', engine, primary_key='id', if_exists='replace')
   # Raises ValueError: Primary key column 'id' not found in DataFrame columns or index.
   ```
   
   **Fix**: Ensure the primary key column exists in the DataFrame columns or index:
   ```python
   df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
   df.index.name = 'id'  # Add 'id' to index
   df.to_sql('users', engine, primary_key='id', if_exists='replace')
   ```

2. **Invalid Auto-Increment**
   ```python
   df = pd.DataFrame({
       'user_id': [1, 2],
       'org_id': ['org1', 'org2'],
       'role': ['admin', 'member']
   })
   df.to_sql('memberships', engine, primary_key=['user_id', 'org_id'], 
             auto_increment=True, if_exists='replace')
   # Raises ValueError: auto_increment=True only supported for single-column integer primary keys. 
   # Got composite key: ['user_id', 'org_id']
   ```
   
   **Fix**: Use `auto_increment` only with single-column integer primary keys:
   ```python
   df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
   df.index.name = 'id'
   df.to_sql('users', engine, primary_key='id', auto_increment=True, if_exists='replace')
   ```

3. **Primary Key Not Found (Alternative Scenario)**
   ```python
   df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
   df.to_sql('users', engine, primary_key='invalid_col', if_exists='replace')
   # Raises ValueError: Primary key column 'invalid_col' not found in DataFrame columns or index.
   ```
   
   **Fix**: Use a column or index name that exists in the DataFrame:
   ```python
   df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
   df.to_sql('users', engine, primary_key='name', if_exists='replace')  # 'name' exists
   ```

## Testing Strategy

### Test Cases

1. **Basic Primary Key Creation**
   - Single column primary key
   - Composite primary key
   - Primary key from index
   - Primary key from column

2. **Auto-Increment**
   - SQLite auto-increment
   - PostgreSQL SERIAL/IDENTITY
   - MySQL AUTO_INCREMENT
   - Validation (composite keys, non-integer types)

3. **Primary Key Validation**
   - Column must exist in DataFrame or index
   - Single vs composite key validation
   - Append mode behavior (parameter ignored)

4. **Backwards Compatibility**
   - Existing code continues to work
   - No primary key created when `primary_key=None`

5. **Database Compatibility**
   - SQLite, PostgreSQL, MySQL
   - Edge cases (empty DataFrames, all-NaN columns)

## Proof of Concept and Reference Implementation

A working implementation demonstrating this API has been developed and tested in the [pandalchemy](https://github.com/eddiethedean/pandalchemy) project (v1.5.0+):

### Implementation Status
- **Production Ready**: Fully implemented and tested
- **Test Coverage**: 36 comprehensive tests across SQLite, PostgreSQL, and MySQL
- **Documentation**: Complete API documentation with examples
- **Test Suite**: 986 tests passing, including edge cases

### Reference Implementation Details

The implementation in pandalchemy provides a reference for:
- API parameter design and validation
- Auto-increment handling across database dialects
- Error handling and user feedback

**Note**: The pandalchemy implementation includes primary key inference from index when `primary_key=None` (for convenience), but this proposal recommends a simpler, more explicit API where `primary_key=None` means no primary key is created (no inference). This explicit approach is more aligned with pandas' philosophy of "explicit is better than implicit" and maintains full backwards compatibility.

Key files for reference:
- `src/pandalchemy/tracked_dataframe.py` (lines 1720-1920): Main `to_sql()` implementation
- `src/pandalchemy/sql_operations.py` (lines 709-896): Database-specific table creation logic
- `tests/test_to_sql.py`: Comprehensive test suite covering all scenarios

This proof of concept demonstrates the feasibility of the API design and can serve as a reference for pandas core developers during implementation. The proposed pandas API is a simplified version that prioritizes explicitness and backwards compatibility.

## Alternative Approaches

### Alternative: Preemptive Table Creation

An alternative approach to adding primary key support would be to create the table structure first using SQLAlchemy, then use `to_sql()` for data insertion. This approach already works today:

```python
from sqlalchemy import Column, Integer, String, MetaData, Table, create_engine

engine = create_engine('sqlite:///example.db')
metadata = MetaData()

# Create table structure with primary key
users_table = Table(
    'users',
    metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(50)),
    Column('age', Integer)
)
metadata.create_all(engine)

# Then use to_sql for data insertion
df = pd.DataFrame({'name': ['Alice', 'Bob'], 'age': [30, 25]})
df.to_sql('users', engine, if_exists='append', index=False)
```

**Pros:**
- Already works with current pandas
- Full control over table schema
- No changes to pandas API needed

**Cons:**
- Requires two-step process (table creation + data insertion)
- More verbose - requires SQLAlchemy knowledge
- Breaks the simplicity of one-line table creation
- Requires maintaining separate schema definitions
- More error-prone (table structure and data can get out of sync)

**Comparison:**
The proposed API enhancement provides a more streamlined workflow:
- Single-step table creation with primary keys
- Leverages DataFrame structure for schema definition
- Maintains pandas' simplicity and ease of use
- Reduces boilerplate code for common use cases

Both approaches can coexist - the proposed enhancement doesn't prevent users from continuing to use the preemptive table creation approach when they need more complex schema control.

## Open Questions

1. **Index Handling**: Should `index=False` exclude primary key column if it's in the index?
   - Current proposal: Primary key column must exist in DataFrame columns (if `index=False`, primary key must be a column, not in index)
   - Alternative: Always include primary key column even with `index=False`

2. **Auto-Increment Behavior**: Should auto-increment values be populated in the DataFrame after insertion?
   - Current proposal: No (database handles auto-increment)
   - Alternative: Fetch and populate auto-generated values

3. **Composite Key Auto-Increment**: Should we support auto-increment for one column in a composite key?
   - Current proposal: No (only single-column primary keys)
   - Alternative: Allow specifying which column in composite key should auto-increment

## Timeline

- **Phase 1**: Core implementation (primary key creation without auto-increment)
- **Phase 2**: Auto-increment support
- **Phase 3**: Enhanced inference and error handling
- **Phase 4**: Documentation and examples

## References

- [pandas `to_sql` documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html)
- [SQLAlchemy Core API](https://docs.sqlalchemy.org/en/20/core/)
- [pandalchemy implementation](https://github.com/eddiethedean/pandalchemy) (proof of concept)

## Community Feedback and Discussion

This proposal addresses a frequently requested feature that would significantly improve pandas' database integration capabilities. The API design is:

- **Backwards Compatible**: Default behavior (`primary_key=None`) is identical to current pandas
- **Explicit**: Users must explicitly specify primary keys - no automatic inference
- **Well-Tested**: Proof of concept demonstrates feasibility across multiple databases
- **Community-Driven**: Based on real-world use cases and user requests

### Seeking Community Input

We welcome feedback from both pandas users and core developers on:

1. **API Design**: Are the parameter names and defaults intuitive?
2. **Index Handling**: How should primary keys work when `index=False`?
3. **Error Messages**: Are the proposed error messages clear and helpful?
4. **Database Support**: Are there additional database-specific considerations?
5. **Edge Cases**: What edge cases should be prioritized in testing?

### Implementation Path

For pandas core developers:
- The proof-of-concept implementation can be adapted for pandas' codebase
- Database-specific logic can leverage existing SQLAlchemy integration
- Testing strategy aligns with pandas' existing test patterns
- Backwards compatibility ensures no breaking changes

### Next Steps

1. **Community Review**: Gather feedback on API design and use cases
2. **Core Developer Discussion**: Engage with pandas maintainers on implementation approach
3. **Design Refinement**: Incorporate feedback into final API specification
4. **Implementation Planning**: Coordinate with pandas development workflow

This feature would be a valuable addition to pandas' database integration capabilities, benefiting users across data science, engineering, and analytics workflows.

