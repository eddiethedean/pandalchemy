# pandalchemy

> **Pandas + SQLAlchemy = Smart DataFrames with Automatic Database Sync**

Work with database tables as pandas DataFrames while pandalchemy automatically tracks changes and syncs to your database with optimized SQL operations.

[![Tests](https://img.shields.io/badge/tests-973%20passing-brightgreen)](https://github.com/eddiethedean/pandalchemy)
[![Type Checked](https://img.shields.io/badge/mypy-passing-blue)](https://github.com/eddiethedean/pandalchemy)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org)

## Why pandalchemy?

```python
import pandalchemy as pa

# Connect to your database
db = pa.DataBase(engine)

# Work with tables like DataFrames
users = db['users']
users.loc[users['age'] > 30, 'senior'] = True

# Changes are automatically tracked and synced
db.push()  # One line, optimized transaction
```

**No more manual SQL**. No more tracking what changed. Just work with your data.

---

## Quick Start

### Installation

```bash
pip install pandalchemy
```

### 30-Second Example

```python
from sqlalchemy import create_engine
import pandalchemy as pa

# Connect
engine = create_engine('postgresql://localhost/mydb')
db = pa.DataBase(engine)

# Read, modify, sync
users = db['users']
users['age'] = users['age'] + 1
users.push()  # All changes synced automatically
```

---

## Key Features

### Automatic Change Tracking
```python
users['age'] = users['age'] + 1  # Tracked
users.push()  # All changes synced
```

### Immutable Primary Keys
```python
users.update_row(1, {'name': 'Alice'})  # ✅ Works
users.update_row(1, {'id': 999})  # ❌ Raises error
```

### Composite Primary Keys
```python
memberships.update_row(('user1', 'org1'), {'role': 'admin'})  # Tuple keys
```

### Auto-Increment Support
```python
users.add_row({'name': 'Bob'}, auto_increment=True)  # ID auto-generated
```

### Conditional Updates & Deletes
```python
users.update_where(users._data['age'] > 65, {'senior': True})
users.delete_where(users._data['status'] == 'inactive')
```

### Schema Evolution
```python
users.add_column_with_default('verified', False)
users.rename_column_safe('old_name', 'new_name')
users.drop_column_safe('legacy_field')
users.push()  # Executes ALTER TABLE
```

### Transaction Safety
```python
users['age'] = users['age'] + 1
users.push()  # Atomic transaction with rollback on error
```

---

## Core API

### DataBase
```python
db = pa.DataBase(engine)
users = db['users']  # Access table
db.push()  # Push all changes
db.create_table('products', df, 'id')  # Create new table
```

### TableDataFrame
```python
# Create from DataFrame
df = pd.DataFrame({'name': ['Alice', 'Bob']}, index=[1, 2])
users = pa.TableDataFrame('users', df, 'id', engine, auto_increment=True)
users.push()

# CRUD
users.add_row({'name': 'Charlie'}, auto_increment=True)
users.update_row(1, {'name': 'Alice Updated'})
users.delete_row(2)
users.push()

# Use full pandas API
users._data['age'] = users._data['age'] + 1
filtered = users._data[users._data['age'] > 30]
```

**See [CRUD Operations notebook](https://github.com/eddiethedean/pandalchemy/blob/main/examples/01_crud_operations.ipynb) for comprehensive examples.**

---

## Best Practices

- **Multi-table changes**: Use `db.push()` instead of individual `table.push()` calls
- **Schema changes**: Push schema first, then pull and update data
- **Validation**: Check `has_changes()` and `get_changes_summary()` before push
- **Bulk operations**: Use `update_where()` and `delete_where()` instead of loops

**See [Interactive Examples](#interactive-examples) for detailed patterns and workflows.**

---

## Known Limitations

1. **Boolean columns**: SQLite BOOLEAN doesn't accept NaN - use explicit defaults
2. **Schema changes**: Push schema changes separately from data updates
3. **Primary keys**: Cannot be updated (delete + insert instead)

See [Full Limitations Guide](#full-limitations) for details and workarounds.

---

## Troubleshooting

### Error: "Cannot update primary key"

**Problem**: You're trying to update a primary key column, which is immutable.

**Solution**:
```python
# ❌ This fails
users.update_row(1, {'id': 999})  # Raises DataValidationError

# ✅ Instead, delete and re-insert
old_data = users.get_row(1)
users.delete_row(1)
users.add_row({**old_data, 'id': 999})
users.push()
```

**Enhanced Error Message**: The error now includes detailed context showing which table, operation, and suggested fix.

---

### Error: "Boolean column errors in SQLite"

**Problem**: SQLite doesn't accept NaN values for BOOLEAN columns.

**Solution**:
```python
# ❌ This may fail
users['active'] = None  # Becomes NaN, may fail on push

# ✅ Use explicit False instead
users.add_column_with_default('active', False)
# Or handle None explicitly
users['active'] = users['active'].fillna(False)
```

---

### Issue: "Schema changes not visible after push"

**Problem**: Schema changes (add/drop/rename columns) need to be pushed separately, then you need to pull to refresh.

**Solution**:
```python
# Add column
users.add_column_with_default('email', '')
users.push()  # Push schema change first

# Pull to refresh with new schema
users.pull()

# Now you can update the new column
users['email'] = 'user@example.com'
users.push()  # Push data changes
```

**Best Practice**: Push schema changes and data changes in separate transactions for reliability.

---

### Issue: "Memory issues with large tables"

**Problem**: Working with very large tables can consume significant memory.

**Solutions**:
1. **Use lazy change computation** (already implemented):
   ```python
   # Changes are only computed when needed
   users['age'] = users['age'] + 1  # No computation yet
   if users.has_changes():  # Computes here if needed
       users.push()
   ```

2. **Use bulk operations instead of loops**:
   ```python
   # ✅ Fast - single bulk operation
   users.update_where(users._data['age'] > 65, {'senior': True})
   users.bulk_insert(new_rows)

   # ❌ Slow - many individual operations
   for row in new_rows:
       users.add_row(row)
   ```

3. **Batch your changes**:
   ```python
   # Make all changes first
   users['age'] = users['age'] + 1
   users['status'] = 'active'
   # Then push once
   users.push()  # Single transaction
   ```

---

### Issue: "Transaction rollback not working as expected"

**Problem**: Understanding when transactions rollback and when they commit.

**Solution**: 
- `push()` automatically wraps all changes in a transaction
- If any error occurs, the entire transaction rolls back
- Schema changes and data changes happen in the correct order automatically

```python
try:
    users['age'] = users['age'] + 1
    products['price'] = products['price'] * 1.1
    db.push()  # All changes in one transaction
except Exception as e:
    # All changes rolled back automatically
    print(f"Error: {e}. No changes were committed.")
```

---

### Error: "No row found with primary key value"

**Problem**: Trying to update or delete a row that doesn't exist.

**Solution**: Check if the row exists first, or use `upsert_row()`:
```python
# Check first
if users.row_exists(pk_value):
    users.update_row(pk_value, updates)
else:
    # Row doesn't exist, create it
    users.add_row({**updates, 'id': pk_value})

# Or use upsert (update if exists, insert if not)
users.upsert_row({**updates, 'id': pk_value})
```

**Enhanced Error**: The error message now shows the table name, operation, and suggests using `get_row()` or `row_exists()` to verify.

---

### Issue: "Performance is slow with many updates"

**Problem**: Many individual `update_row()` calls are slow.

**Solution**: Use `update_where()` for bulk conditional updates:
```python
# ✅ Fast - single SQL operation
users.update_where(
    users._data['age'] > 65,
    {'senior': True, 'discount': 0.1}
)

# ❌ Slow - many SQL operations
for idx in old_users.index:
    users.update_row(idx, {'senior': True, 'discount': 0.1})
```

---

### Error: "Column 'X' does not exist"

**Problem**: Trying to access or modify a column that doesn't exist in the DataFrame.

**Enhanced Error**: The error now shows:
- Which table you're working with
- The operation that failed
- Available columns in the table
- Suggested fix

**Solution**: Check column names and use `add_column_with_default()` if you need to add it:
```python
# Check available columns
print(users.columns.tolist())

# Add missing column if needed
if 'new_column' not in users.columns:
    users.add_column_with_default('new_column', default_value=0)
    users.push()  # Push schema change
    users.pull()  # Refresh
```

---

For more detailed troubleshooting, see the [Full Limitations Guide](#full-limitations) below.

---

## Installation & Setup

### Requirements

- Python 3.9+
- pandas >= 1.5.0
- SQLAlchemy >= 2.0.0

### Install

```bash
pip install pandalchemy
```

### Supported Databases

Works with any SQLAlchemy-supported database:
- PostgreSQL (fully tested)
- MySQL/MariaDB (fully tested)
- SQLite
- Oracle
- SQL Server
- And more

**Multi-Database Testing**: pandalchemy is extensively tested on PostgreSQL and MySQL with 534 tests ensuring cross-platform compatibility. Over 150+ tests run on multiple database backends (SQLite, PostgreSQL, MySQL) to validate consistent behavior across platforms.

---

## API Reference

### DataBase Class

```python
db = pa.DataBase(
    engine,           # SQLAlchemy engine
    lazy=False,       # Lazy load tables
    schema=None       # Optional schema
)
```

**Methods:**
- `db['table_name']` - Access table
- `db.push()` - Push all changes
- `db.pull()` - Refresh all tables
- `db.create_table(name, df, primary_key)` - Create new table
- `db.add_table(table, push=False)` - Add existing table
- `db.table_names` - List all tables

### TableDataFrame Class

```python
table = pa.TableDataFrame(
    name,              # Table name (or DataFrame for standalone)
    data=None,         # DataFrame data
    primary_key='id',  # PK column(s)
    engine=None,       # SQLAlchemy engine
    auto_increment=False  # Enable auto-increment
)
```

**CRUD Methods:**
- `add_row(row_data, auto_increment=False)` - Insert row
- `update_row(pk_value, updates)` - Update row
- `delete_row(pk_value)` - Delete row
- `upsert_row(row_data)` - Update or insert
- `get_row(pk_value)` - Get row as dict
- `row_exists(pk_value)` - Check if exists
- `bulk_insert(records)` - Bulk insert

**Conditional Methods:**
- `update_where(condition, updates)` - Bulk conditional update
- `delete_where(condition)` - Bulk conditional delete

**Schema Methods:**
- `add_column_with_default(name, default)` - Add column
- `rename_column_safe(old, new)` - Rename column
- `drop_column_safe(name)` - Drop column
- `convert_column_type(name, new_type)` - Change type
- `set_primary_key(columns)` - Change PK

**Database Methods:**
- `push()` - Sync to database
- `pull()` - Refresh from database
- `get_next_pk_value()` - Next auto-increment value

**Inspection Methods:**
- `has_changes()` - Check if modified
- `get_changes_summary()` - Change statistics
- `validate_data()` - Check data integrity
- `to_pandas()` - Get underlying DataFrame

**pandas Compatibility:**
All standard pandas DataFrame operations work and are automatically tracked.

---

## Interactive Examples

Comprehensive Jupyter notebooks demonstrating all features (with pre-executed outputs):

1. **[CRUD Operations](https://github.com/eddiethedean/pandalchemy/blob/main/examples/01_crud_operations.ipynb)** - Create, read, update, delete
2. **[Change Tracking](https://github.com/eddiethedean/pandalchemy/blob/main/examples/02_change_tracking.ipynb)** - Automatic change detection
3. **[Composite Primary Keys](https://github.com/eddiethedean/pandalchemy/blob/main/examples/03_composite_primary_keys.ipynb)** - Multi-column keys with MultiIndex
4. **[Auto-Increment](https://github.com/eddiethedean/pandalchemy/blob/main/examples/04_auto_increment.ipynb)** - Automatic ID generation
5. **[Conditional Operations](https://github.com/eddiethedean/pandalchemy/blob/main/examples/05_conditional_operations.ipynb)** - Bulk update_where/delete_where
6. **[Schema Evolution](https://github.com/eddiethedean/pandalchemy/blob/main/examples/06_schema_evolution.ipynb)** - Add, drop, rename columns
7. **[Transactions](https://github.com/eddiethedean/pandalchemy/blob/main/examples/07_transactions.ipynb)** - ACID guarantees and rollback
8. **[Index-based PKs](https://github.com/eddiethedean/pandalchemy/blob/main/examples/08_index_based_primary_keys.ipynb)** - Using DataFrame index as primary key ⭐ NEW
9. **[Immutable Primary Keys](https://github.com/eddiethedean/pandalchemy/blob/main/examples/09_immutable_primary_keys.ipynb)** - PK constraints
10. **[Pandas Integration](https://github.com/eddiethedean/pandalchemy/blob/main/examples/10_pandas_integration.ipynb)** - Full pandas API
11. **[E-Commerce System](https://github.com/eddiethedean/pandalchemy/blob/main/examples/11_real_world_ecommerce.ipynb)** - Real-world workflow

All notebooks include working code with outputs and can be run interactively.

---

## Example: Data Cleaning

```python
users = db['users']

# Clean with pandas operations
users._data['email'] = users._data['email'].str.lower().str.strip()
users._data['phone'] = users._data['phone'].str.replace(r'\D', '', regex=True)

# Add derived column
users._data['full_name'] = users._data['first_name'] + ' ' + users._data['last_name']

# Remove invalid rows
deleted = users.delete_where(users._data['email'].isna())

users.push()  # All changes in one transaction
```

**See [Interactive Examples](#interactive-examples) for 11 comprehensive notebooks with live outputs.**

---

## Performance Tips

### Quick Performance Wins

- **Bulk inserts**: Use `bulk_insert()` instead of looping `add_row()`
- **Minimize push()**: Batch all changes, then push once
- **Conditional updates**: Use `update_where()` instead of looping `update_row()`

```python
# ✅ Fast
table.bulk_insert(rows)
users.update_where(users._data['age'] > 65, {'senior': True})

# ❌ Slow
for row in rows: table.add_row(row)
for idx in old_users.index: users.update_row(idx, {'senior': True})
```

### Performance Tuning Guide

#### When to Use `bulk_insert()` vs `add_row()`

Use `bulk_insert()` when inserting multiple rows:
```python
# ✅ Fast - Single SQL operation
users.bulk_insert([
    {'name': 'Alice', 'age': 30},
    {'name': 'Bob', 'age': 25},
    {'name': 'Charlie', 'age': 35}
])

# ❌ Slow - Multiple SQL operations
for row in rows:
    users.add_row(row)
```

**Benchmark**: `bulk_insert()` is typically 10-100x faster for 100+ rows.

#### Batching Strategies for Large Updates

1. **Batch by condition**: Use `update_where()` for conditional bulk updates
```python
# ✅ Process all matching rows in one operation
users.update_where(
    users._data['status'] == 'pending',
    {'processed': True, 'processed_at': datetime.now()}
)
```

2. **Batch by chunks**: For very large datasets, process in chunks
```python
# Process 1000 rows at a time
chunk_size = 1000
for i in range(0, len(users), chunk_size):
    chunk = users.iloc[i:i+chunk_size]
    # Process chunk
    chunk['age'] = chunk['age'] + 1
    chunk.push()  # Push this chunk
```

#### Memory Optimization Tips

1. **Use lazy computation**: Changes are computed only when needed
```python
# Make changes
users['age'] = users['age'] + 1
# Computation hasn't happened yet - no performance cost

# Only computes when you check or push
if users.has_changes():  # Computes here if needed
    users.push()
```

2. **Pull only what you need**: If working with large tables, filter before pulling
```python
# ❌ Pulls entire table into memory
users = db['users']  # Could be millions of rows

# ✅ Pull with filter (if supported by your workflow)
# Or work with specific columns
users = db['users']
subset = users[users['department'] == 'Sales']  # Work with subset
```

3. **Connection pooling**: Use SQLAlchemy connection pooling for better performance
```python
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

engine = create_engine(
    'postgresql://localhost/mydb',
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10
)
```

#### Performance Benchmarks

Typical performance characteristics:
- **bulk_insert()**: ~10,000 rows/second (depends on row size and database)
- **update_where()**: ~5,000-10,000 rows/second (depends on condition complexity)
- **push()**: ~1,000-5,000 operations/second (combines all changes into optimized SQL)

**Note**: Actual performance depends on:
- Database type (PostgreSQL, MySQL, SQLite)
- Network latency (for remote databases)
- Row size and complexity
- Index presence on affected columns

---

## Full Limitations

<details>
<summary><b>Click to expand detailed limitations and workarounds</b></summary>

### Primary Key Operations

**Cannot Update Primary Keys**
```python
# PK values are immutable
users.update_row(1, {'id': 999})  # Raises DataValidationError

# Workaround: delete + insert
old_data = users.get_row(1)
users.delete_row(1)
users.add_row({**old_data, 'id': 999})
```

### Schema Changes

**Column Additions Timing**
```python
# New columns should be pushed before updating
users.add_column_with_default('new_col', 0)
users.push()  # Commit schema
users.pull()  # Refresh
users['new_col'] = 100  # Now safe
users.push()
```

**Renaming Newly Added Columns**
```python
# Can't rename columns added in same transaction
users.add_column_with_default('temp_name', 0)
users.rename_column_safe('temp_name', 'final_name')  # May fail on push

# Workaround: push between operations
users.add_column_with_default('temp_name', 0)
users.push()
users.rename_column_safe('temp_name', 'final_name')
users.push()
```

### Type Constraints

**Boolean NULL Values**
```python
# SQLite BOOLEAN doesn't accept NaN
users['active'] = None  # Becomes NaN, fails on push

# Workaround: use explicit False or create as nullable
users.add_column_with_default('active', False)
```

**String to Numeric Conversion**
```python
# Automatic type inference may fail
# Manually convert if needed:
users['numeric_string'] = pd.to_numeric(users['string_col'], errors='coerce')
```

### Composite Keys

**Auto-Increment with Composite Keys**
```python
# Auto-increment only works with single-column integer PKs
table = pa.TableDataFrame('t', df, ['user_id', 'org_id'], engine, auto_increment=True)
# Raises ValueError

# Workaround: use single-column surrogate key
```

### Data Integrity

**Duplicate Column Names**
```python
# DataFrame with duplicate columns will fail validation
df.columns = ['id', 'name', 'name']  # Fails on push

# Workaround: ensure unique column names
```

**NULL in Primary Keys**
```python
# Primary keys cannot be NULL
users.add_row({'id': None, 'name': 'Test'})  # Raises DataValidationError

# Always provide valid PK values
```

</details>

---

## Comparison with Similar Tools

`pandalchemy` is the only package offering the complete workflow of automatic change tracking, schema evolution, and transaction safety for pandas-to-database synchronization.

### Feature Comparison Matrix

| Feature | pandalchemy | pandas + SQLAlchemy | Pangres | pandabase | pandas-gbq | pandera |
|---------|-------------|---------------------|---------|-----------|------------|---------|
| **Automatic Change Tracking** | ✅ | ❌ Manual | ❌ | ❌ | ❌ | ❌ |
| **Schema Evolution (DDL)** | ✅ | ❌ Manual ALTER | ⚠️ Limited | ❌ | ❌ | ❌ |
| **Transaction Safety & Rollback** | ✅ | ❌ Manual | ❌ | ❌ | ❌ | N/A |
| **Primary Key Immutability** | ✅ Enforced | ❌ | ❌ | ❌ | ❌ | N/A |
| **Composite Primary Keys** | ✅ Full Support | ✅ Manual | ✅ | ⚠️ Limited | ⚠️ Limited | N/A |
| **Auto-Increment PKs** | ✅ | ❌ Manual | ❌ | ❌ | ❌ | N/A |
| **Delete Tracking** | ✅ | ❌ Manual | ❌ | ❌ | ❌ | N/A |
| **Column Type Change Tracking** | ✅ | ❌ Manual | ⚠️ Limited | ❌ | ❌ | N/A |
| **Optimized Execution Plans** | ✅ | ❌ | ⚠️ Upserts only | ❌ | ⚠️ Bulk only | N/A |
| **Change Audit Trail** | ✅ | ❌ | ❌ | ❌ | ❌ | N/A |
| **Conditional Updates** (`update_where`) | ✅ | ❌ Manual | ❌ | ❌ | ❌ | N/A |
| **Full pandas API** | ✅ | ✅ | ✅ | ⚠️ Partial | ✅ | ✅ |
| **Multi-Database Support** | ✅ All SQLAlchemy | ✅ All SQLAlchemy | ✅ Postgres/MySQL/SQLite | ✅ SQLite/Postgres | ❌ BigQuery only | ✅ |
| **Database Integration** | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ Validation only |
| **Active Maintenance** | ✅ | ✅ | ✅ | ❌ Deprecated | ✅ | ✅ |

**Legend:** ✅ = Included | ❌ = Missing | ⚠️ = Partial/Limited | N/A = Not Applicable

### Package Links
- **[pandalchemy](https://github.com/eddiethedean/pandalchemy)** - This package
- **[pandas](https://pandas.pydata.org/) + [SQLAlchemy](https://www.sqlalchemy.org/)** - Manual integration baseline
- **[Pangres](https://github.com/ThibTrip/pangres)** - Upsert-focused alternative
- **[pandabase](https://pypi.org/project/pandabase/)** - Deprecated (use Pangres instead)
- **[pandas-gbq](https://pandas-gbq.readthedocs.io/)** - Google BigQuery integration
- **[pandera](https://pandera.readthedocs.io/)** - DataFrame validation library

**Choose pandalchemy when you need:**
- Automatic tracking of all DataFrame changes (inserts, updates, deletes)
- Schema evolution with automatic DDL generation
- Transaction safety with automatic rollback on errors
- Composite primary key support with immutability enforcement
- Production-ready workflows with minimal boilerplate

---

## Development

### Setup

```bash
git clone https://github.com/eddiethedean/pandalchemy
cd pandalchemy
pip install -e ".[dev]"
```

### Run Tests

```bash
# Run all tests (SQLite only)
pytest tests/

# Run PostgreSQL tests (requires testing.postgresql)
pytest -m postgres

# Run MySQL tests (requires testing.mysqld)
pytest -m mysql

# Run all multi-database tests
pytest -m multidb

# See tests/TESTING_MULTI_DB.md for setup instructions
```

### Code Quality

```bash
# Type checking
mypy src/pandalchemy

# Linting
ruff check src/pandalchemy

# Format
ruff format src/pandalchemy
```

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new features
4. Ensure all tests pass
5. Submit a pull request

See [CONTRIBUTING.rst](CONTRIBUTING.rst) for details.

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

## Links

- **Documentation**: [Read the Docs](#) (coming soon)
- **Source Code**: [GitHub](https://github.com/eddiethedean/pandalchemy)
- **Issue Tracker**: [GitHub Issues](https://github.com/eddiethedean/pandalchemy/issues)
- **PyPI**: [pandalchemy](https://pypi.org/project/pandalchemy/)

---

## Acknowledgments

Built with:
- [pandas](https://pandas.pydata.org/) - Data manipulation
- [SQLAlchemy](https://www.sqlalchemy.org/) - Database toolkit
- [fullmetalalchemy](https://github.com/eddiethedean/fullmetalalchemy) - SQL helpers
- [transmutation](https://github.com/eddiethedean/transmutation) - Schema operations

---

## Version History

### 1.3.0 (Latest) 🎉
- **AsyncIO Support**: Full async/await support with `AsyncDataBase` and `AsyncTableDataFrame` classes for async database operations
- **Enhanced Error Handling**: Detailed error context with error codes, suggested fixes, and structured error information
- **Memory Optimization**: Incremental change tracking mode to reduce memory usage for large datasets
- **Lazy Change Computation**: Changes are computed only when needed (on `has_changes()`, `get_changes_summary()`, or `push()`)
- **Conflict Resolution**: Configurable conflict resolution strategies (last_writer_wins, first_writer_wins, abort, merge, custom) for concurrent modifications
- **Adaptive Batch Sizing**: Dynamic batch sizing for SQL operations based on operation type and record count for optimal performance
- **Improved Validation**: Comprehensive validation before transactions to catch schema errors early
- **Code Quality**: All ruff checks passing (0 errors), all mypy checks passing (0 errors), full code formatting compliance
- **Testing**: 973 tests passing with comprehensive PostgreSQL and MySQL test infrastructure

### 1.2.0
- **Dependency Upgrades**: Upgraded to fullmetalalchemy 2.4.0 and transmutation 1.1.0 for improved SQL operations and schema evolution
- **Code Modernization**: Replaced all SQLAlchemy Core API usage with fullmetalalchemy functions for consistency and better abstraction
- **Type Safety**: Added fast type checking with `ty` (Rust-based type checker) and fixed all type issues for better code quality
- **Improved Schema Operations**: Leveraged transmutation 1.1.0 features including improved column operations, better transaction handling, and MySQL VARCHAR length support
- **Performance**: Optimized MAX aggregation queries using fullmetalalchemy's `select_column_max` for efficient primary key generation
- **Code Quality**: Full ruff formatting and linting compliance, improved type annotations throughout the codebase
- **Testing**: 453 tests passing with improved test coverage and reliability

### 1.1.0
- **Multi-Database Support**: Full PostgreSQL and MySQL compatibility with 534 tests, 150+ running on multiple databases
- **Database-Specific Optimizations**: Raw SQL paths for PostgreSQL/MySQL to avoid metadata lock issues
- **Schema Evolution Improvements**: Proper handling of MySQL VARCHAR length requirements and column rename operations
- **Connection Management**: Improved connection pooling and transaction handling for production databases
- **Transaction Fixes**: Fixed DELETE operations in complex transactions with schema changes
- **Testing Infrastructure**: Added `testing.postgresql` and `testing.mysqld` for isolated test environments
- **Performance**: Optimized table introspection using `inspect(engine)` and `autoload_with` for better transaction visibility
- **Code Quality**: Full ruff and mypy compliance with 0 errors

### 1.0.0
- **Major refactoring**: Merged Table and TrackedDataFrame into unified TableDataFrame
- **New feature**: Column type change tracking with ALTER COLUMN support
- **New methods**: update_where() and delete_where() for conditional operations
- **Code quality**: Eliminated ~185 lines of duplicate code, created pk_utils module
- **Security**: Fixed SQL injection vulnerabilities
- **Type safety**: Full mypy compliance (0 errors)
- **Testing**: 446 comprehensive tests passing
- **Documentation**: Complete README revamp (34% more concise)

### 0.2.0
- Added composite primary key support
- Immutable primary keys as index
- Auto-increment functionality
- Enhanced CRUD operations

### 0.1.0
- Initial release
- Basic change tracking
- Simple CRUD operations

---

Made with ❤️ by [Odos Matthews](https://github.com/eddiethedean)
