# pandalchemy

> **Pandas + SQLAlchemy = Smart DataFrames with Automatic Database Sync**

Work with database tables as pandas DataFrames while pandalchemy automatically tracks changes and syncs to your database with optimized SQL operations.

[![Tests](https://img.shields.io/badge/tests-426%20passing-brightgreen)](https://github.com/eddiethedean/pandalchemy)
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
- PostgreSQL
- MySQL/MariaDB
- SQLite
- Oracle
- SQL Server
- And more

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
pytest tests/
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

### 1.0.0 (Latest) 🎉
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
