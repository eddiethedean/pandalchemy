# pandalchemy: Pandas + SQLAlchemy with Change Tracking

## What is it?

**pandalchemy** is a modern Python package that seamlessly integrates pandas DataFrames with SQL databases through SQLAlchemy. It features intelligent change tracking, optimized SQL operations, and automatic transaction management with rollback support.

## Key Features

Here are the main features of pandalchemy 0.2.0:

- **Automatic Change Tracking**: Every modification to your DataFrame is tracked at both operation and row levels
- **Optimized SQL Execution**: Changes are batched and optimized to minimize database operations
- **Transaction Safety**: All changes execute within transactions with automatic rollback on errors
- **pandas-Compatible API**: Use familiar pandas operations - they're all intercepted and tracked
- **Schema Evolution**: Add, drop, or rename columns with automatic schema migration
- **Type Preservation**: Maintains all data types, primary keys, and indexes
- **Modern Stack**: Built on SQLAlchemy 2.x, fullmetalalchemy, and transmutation

## Installation

```bash
# From PyPI
pip install pandalchemy

# From source
git clone https://github.com/eddiethedean/pandalchemy
cd pandalchemy
pip install -e .
```

## Dependencies

- pandas >= 1.5.0
- sqlalchemy >= 2.0.0
- fullmetalalchemy >= 0.1.0
- transmutation >= 0.1.0
- numpy >= 1.20.0

## Quick Start

### Basic Usage

```python
from sqlalchemy import create_engine
import pandalchemy as pa

# Create a database connection
engine = create_engine('postgresql://user:password@localhost:5432/mydb')

# Initialize a pandalchemy Database
db = pa.DataBase(engine)

# Access a table (returns a Table object with tracked DataFrame)
users = db['users']

# Make changes just like you would with pandas
users['age'] = users['age'] + 1
users.loc[users['age'] > 65, 'senior'] = True
users['email'] = users['name'].str.lower() + '@example.com'

# Push all changes to the database in an optimized transaction
users.push()
```

### Change Tracking in Action

```python
from sqlalchemy import create_engine
import pandas as pd
import pandalchemy as pa

# Create engine
engine = create_engine('sqlite:///example.db')
db = pa.DataBase(engine)

# Create a new table
data = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
table = db.create_table('users', data, primary_key='id')

# Make various changes
db['users']['email'] = ['alice@test.com', 'bob@test.com', 'charlie@test.com']
db['users'].loc[1, 'age'] = 26
db['users'].drop(3, inplace=True)  # Delete a row

# Check what changes are tracked
summary = db['users'].get_changes_summary()
print(summary)
# Output: {
#     'inserts': 0,
#     'updates': 1,
#     'deletes': 1,
#     'columns_added': 1,
#     'has_changes': True
# }

# Push all changes in a single optimized transaction
db['users'].push()
```

### Schema Modifications

```python
# Add a new column
db['users']['phone'] = ['555-0001', '555-0002', '555-0003']

# Rename a column
db['users'].rename(columns={'age': 'years_old'}, inplace=True)

# Drop a column
db['users'].drop('phone', axis=1, inplace=True)

# All schema changes are tracked and applied efficiently
db['users'].push()
```

### Transaction Safety

```python
try:
    # Make multiple changes
    db['users']['salary'] = [50000, 60000, 70000]
    db['users'].loc[1, 'name'] = 'Alicia'
    
    # This will execute all changes in a transaction
    # If any operation fails, ALL changes are rolled back
    db['users'].push()
    
except Exception as e:
    print(f"Changes were rolled back: {e}")
    # Database remains in its original state
```

### Working with Multiple Tables

```python
# Push changes to all tables in a single transaction
db['users']['last_login'] = pd.Timestamp.now()
db['posts']['published'] = True

# This executes all changes across all tables in one transaction
db.push()
```

### Creating New Tables

```python
# Create a new table from a DataFrame
new_data = pd.DataFrame({
    'id': [1, 2, 3],
    'product': ['Widget', 'Gadget', 'Doohickey'],
    'price': [9.99, 19.99, 14.99]
})

products_table = db.create_table(
    'products',
    new_data,
    primary_key='id',
    if_exists='replace'  # or 'fail' or 'append'
)
```

### Lazy Loading

```python
# For large databases, use lazy loading
db = pa.DataBase(engine, lazy=True)

# Tables are only loaded when accessed
users = db['users']  # Loads only the users table
```

### Advanced: Direct Access to Tracking

```python
# Get the change tracker
tracker = db['users'].data.get_tracker()

# Inspect tracked operations
for op in tracker.operations:
    print(f"{op.method_name} with args {op.args}")

# Get row-level changes
inserts = tracker.get_inserts()
updates = tracker.get_updates()
deletes = tracker.get_deletes()
```

## Migration from 0.1.x

If you're upgrading from pandalchemy 0.1.x, here are the key changes:

### What's Changed

- **SQLAlchemy 2.x**: Now requires SQLAlchemy >= 2.0.0
- **New Dependencies**: Uses fullmetalalchemy and transmutation for SQL operations
- **Automatic Tracking**: All changes are now automatically tracked (no manual tracking needed)
- **Transaction-First**: All pushes happen in transactions with automatic rollback

### Migration Steps

1. Update your dependencies:
   ```bash
   pip install --upgrade pandalchemy sqlalchemy
   ```

2. Update SQLAlchemy code to 2.x patterns (if using engine directly)

3. Remove manual change tracking code (now automatic)

4. The API remains largely the same, so most code should work without changes

### Removed Features

- `SubTable` class (use regular slicing with automatic tracking)
- `View` class (use standard Table operations)
- Direct sqlalchemy-migrate usage (replaced by transmutation)

## Architecture

### Components

1. **TrackedDataFrame**: Wraps pandas DataFrame, intercepts all modifications
2. **ChangeTracker**: Records changes at operation and row levels
3. **ExecutionPlan**: Optimizes changes into efficient SQL operations
4. **SQL Operations**: Executes plans using fullmetalalchemy and transmutation

### How It Works

```
User modifies DataFrame
        ↓
TrackedDataFrame intercepts operation
        ↓
ChangeTracker records change
        ↓
User calls push()
        ↓
ExecutionPlan analyzes changes
        ↓
SQL operations execute in transaction
        ↓
Success: Changes committed
Failure: Automatic rollback
```

## Performance Tips

1. **Batch Changes**: Make multiple changes before calling `push()` - they'll be optimized together
2. **Use Lazy Loading**: For large databases with many tables
3. **Primary Keys**: Always ensure tables have primary keys for optimal performance
4. **Check Changes**: Use `get_changes_summary()` to see what will be pushed

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=pandalchemy --cov-report=html
```

### Code Quality

```bash
# Format code
black src/

# Type checking
mypy src/

# Linting
ruff check src/
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## License

MIT License - see LICENSE file for details

## Links

- **GitHub**: https://github.com/eddiethedean/pandalchemy
- **Issues**: https://github.com/eddiethedean/pandalchemy/issues
- **PyPI**: https://pypi.org/project/pandalchemy/

## Acknowledgments

Built with:
- [pandas](https://pandas.pydata.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [fullmetalalchemy](https://github.com/eddiethedean/fullmetalalchemy)
- [transmutation](https://github.com/eddiethedean/transmutation)
