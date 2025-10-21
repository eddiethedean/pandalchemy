# pandalchemy: Pandas + SQLAlchemy with Change Tracking

## What is it?

**pandalchemy** is a modern Python package that seamlessly integrates pandas DataFrames with SQL databases through SQLAlchemy. It features intelligent change tracking, optimized SQL operations, immutable primary keys, and automatic transaction management with rollback support.

## Key Features

Here are the main features of pandalchemy 0.2.0:

- **Automatic Change Tracking**: Every modification to your DataFrame is tracked at both operation and row levels
- **Immutable Primary Keys**: Primary keys are stored as DataFrame index and cannot be modified (only deleted/inserted)
- **Composite Primary Keys**: Full support for multi-column primary keys with automatic detection from database
- **Auto-Increment Support**: Automatic primary key generation for new rows
- **Optimized SQL Execution**: Changes are batched and optimized to minimize database operations
- **Transaction Safety**: All changes execute within transactions with automatic rollback on errors
- **pandas-Compatible API**: Use familiar pandas operations - they're all intercepted and tracked
- **Schema Evolution**: Add, drop, or rename columns with automatic schema migration
- **Enhanced CRUD Operations**: Direct methods for add, update, delete, and upsert operations
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

# Access a table (automatically loads with PK as index)
users = db['users']

# Primary key is now the DataFrame index
print(users.data.to_pandas())
#      name  age
# id             
# 1   Alice   25
# 2   Bob     30
# 3   Charlie 35

# Make changes just like you would with pandas
users['age'] = users['age'] + 1
users.loc[users['age'] > 65, 'senior'] = True

# Push all changes to the database in an optimized transaction
users.push()
```

### Primary Keys as Index

Primary keys are now stored as the DataFrame index, making operations more intuitive and preventing accidental modifications:

```python
from sqlalchemy import create_engine
import pandas as pd
import pandalchemy as pa

engine = create_engine('sqlite:///example.db')

# Create a table - PK automatically becomes the index
data = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})
table = pa.Table('users', data, 'id', engine)

# PK is now in the index
print(table.data.to_pandas())
#      name  age
# id             
# 1   Alice   25
# 2   Bob     30
# 3   Charlie 35

# ✅ Can update non-PK columns
table.data.update_row(1, {'age': 26, 'name': 'Alicia'})
print(table.data.to_pandas())
#      name  age
# id             
# 1   Alicia   26  ← Updated!
# 2   Bob     30
# 3   Charlie 35

# ❌ Cannot update PK values (immutable)
try:
    table.data.update_row(1, {'id': 999})
except Exception as e:
    print(f"Error: {type(e).__name__}")  
    # Output: Error: DataValidationError

# ✅ To "change" a PK: delete old, insert new
table.data.delete_row(1)
table.data.add_row({'id': 100, 'name': 'Alicia', 'age': 26})
print(table.data.to_pandas())
#        name  age
# id               
# 2        Bob   30
# 3    Charlie   35
# 100   Alicia   26  ← New ID!

table.push()
```

### Composite Primary Keys

Full support for tables with multi-column primary keys:

```python
# Create table with composite primary key
memberships = pd.DataFrame({
    'user_id': ['u1', 'u1', 'u2'],
    'org_id': ['org1', 'org2', 'org1'],
    'role': ['admin', 'user', 'user']
})

table = pa.Table('memberships', memberships, ['user_id', 'org_id'], engine)
table.push()

# Composite PK automatically becomes MultiIndex
print(table.data.to_pandas())
#                 role
# user_id org_id       
# u1      org1    admin
#         org2     user
# u2      org1     user

# CRUD operations with composite keys (use tuples)
table.data.add_row({
    'user_id': 'u3',
    'org_id': 'org1',
    'role': 'guest'
})

table.data.update_row(
    ('u1', 'org1'),  # Composite PK as tuple
    {'role': 'superadmin'}
)

table.data.delete_row(('u2', 'org1'))

print(table.data.to_pandas())
#                      role
# user_id org_id            
# u1      org1    superadmin  ← Updated!
#         org2          user
# u3      org1         guest  ← Added!

# Composite PKs are auto-detected when pulling from database
db = pa.DataBase(engine)
memberships = db['memberships']  # Automatically has MultiIndex
```

### Auto-Increment Primary Keys

Automatic generation of primary key values for new rows:

```python
# Enable auto-increment for a table
df = pd.DataFrame({
    'id': [1, 2],
    'name': ['Alice', 'Bob'],
    'age': [25, 30]
})
users = pa.Table('users', df, 'id', engine, auto_increment=True)

# Add rows without specifying PK - automatically generated
users.data.add_row({
    'name': 'Dave',
    'age': 40
}, auto_increment=True)

print(users.data.to_pandas())
#      name  age
# id            
# 1   Alice   25
# 2     Bob   30
# 3    Dave   40  ← Auto-generated ID!

# Still can specify PK explicitly if needed
users.data.add_row({
    'id': 100,
    'name': 'Eve',
    'age': 45
})

print(users.data.to_pandas())
#      name  age
# id             
# 1    Alice   25
# 2      Bob   30
# 3     Dave   40
# 100    Eve   45  ← Explicit ID

users.push()
```

### Enhanced CRUD Operations

Direct methods for common database operations:

```python
db = pa.DataBase(engine)
users = db['users']

# Add a single row
users.data.add_row({
    'id': 4,
    'name': 'David',
    'age': 28
})

# Update a row (PK cannot be changed)
users.data.update_row(4, {
    'age': 29,
    'name': 'Dave'
})

# Upsert (update if exists, insert if not)
users.data.upsert_row({
    'id': 5,
    'name': 'Eve',
    'age': 32
})

# Delete a row
users.data.delete_row(3)

# Bulk insert multiple rows
new_users = [
    {'id': 6, 'name': 'Frank', 'age': 35},
    {'id': 7, 'name': 'Grace', 'age': 27},
    {'id': 8, 'name': 'Henry', 'age': 41}
]
users.data.bulk_insert(new_users)

# Query operations
if users.data.row_exists(4):
    user = users.data.get_row(4)
    print(f"User 4: {user}")

# Push all changes
users.push()
```

#### Conditional Updates with update_where()

For SQL-style conditional updates, use the intuitive `update_where()` method:

```python
from datetime import datetime

# Single column update with lambda
db['users'].data.update_where(
    db['users'].data['age'] > 30,
    {'age': lambda x: x + 1}
)

# Multiple columns at once
db['users'].data.update_where(
    db['users'].data['active'] == True,
    {
        'last_seen': datetime.now(),
        'login_count': lambda x: x + 1
    }
)

# Simple value assignment
db['users'].data.update_where(
    db['users'].data['status'] == 'pending',
    {'status': 'approved'}
)

# Shorthand for single column
db['users'].data.update_where(
    db['users'].data['age'] > 65,
    True,
    column='senior'
)

# Department-wide salary increase
db['employees'].data.update_where(
    db['employees'].data['department'] == 'Engineering',
    {'salary': lambda x: x * 1.15}  # 15% raise
)

# All tracked automatically!
db.push()
```

**Alternative:** You can also use pandas-style `loc` syntax which is fully tracked:

```python
# These are equivalent and both tracked:
db['users'].data.update_where(db['users'].data['age'] > 30, {'age': lambda x: x + 1})
db['users'].data.loc[db['users'].data['age'] > 30, 'age'] += 1
```

#### Conditional Deletions with delete_where()

Similarly, use `delete_where()` for SQL-style conditional deletions:

```python
# Delete old records
deleted = db['logs'].data.delete_where(db['logs'].data['age'] > 65)
print(f"Deleted {deleted} rows")

# Delete inactive users
deleted = db['users'].data.delete_where(db['users'].data['active'] == False)

# Delete by multiple conditions
deleted = db['sessions'].data.delete_where(
    (db['sessions'].data['status'] == 'expired') & 
    (db['sessions'].data['last_activity'] < cutoff_date)
)

# Delete all error logs
deleted = db['logs'].data.delete_where(
    (db['logs'].data['level'] == 'ERROR') | (db['logs'].data['level'] == 'WARNING')
)

# Delete zero balance accounts
deleted = db['accounts'].data.delete_where(db['accounts'].data['balance'] == 0)

# All tracked automatically!
db.push()
```

**Note:** `delete_where()` returns the count of deleted rows for confirmation.

### Schema Helper Methods

Convenient methods for schema modifications:

```python
# Add column with default value
users.data.add_column_with_default('status', 'active')

# Drop column safely (with validation)
users.data.drop_column_safe('temp_field')

# Rename column
users.data.rename_column_safe('age', 'years_old')

# Change column type
users.data.change_column_type('status', str)

# Change primary key (moves columns to index)
users.data.set_primary_key('email')  # Single column
# or
users.data.set_primary_key(['user_id', 'org_id'])  # Composite

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
db['users'].data.update_row(1, {'age': 26})
db['users'].data.delete_row(3)

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

### Transaction Safety

```python
try:
    # Make multiple changes
    db['users'].data.add_row({'id': 10, 'name': 'Test', 'age': 25})
    db['users'].data.update_row(1, {'name': 'Alicia'})
    
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
db['users'].data.update_row(1, {'last_login': pd.Timestamp.now()})
db['posts']['published'] = True

# This executes all changes across all tables in one transaction
db.push() 
```

### Data Validation

```python
# Validate data before pushing
errors = users.data.validate_data()
if errors:
    print(f"Validation errors: {errors}")
else:
    users.push()

# Validate primary key integrity
try:
    users.data.validate_primary_key()
    print("Primary key is valid")
except Exception as e:
    print(f"PK validation failed: {e}")
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

# Create table with auto-increment
users_table = db.create_table(
    'users',
    users_df,
    primary_key='id',
    auto_increment=True
)
```

### Lazy Loading

```python
# For large databases, use lazy loading
db = pa.DataBase(engine, lazy=True)

# Tables are only loaded when accessed
users = db['users']  # Loads only the users table
```

### Working with Indexes

```python
# Get PK back as column if needed (backward compatibility)
df_with_pk = users.data.to_pandas().reset_index()
print('id' in df_with_pk.columns)  # True

# Access index directly (read-only)
print(users.data.index)  # Primary key values

# Cannot modify index directly (PKs are immutable)
try:
    users.data.index = [10, 20, 30]
except Exception as e:
    print(f"Error: {e}")  # Cannot modify index directly
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

# Check if there are any changes
if db['users'].data.has_changes():
    print("Pending changes detected")
```

## Migration from 0.1.x

If you're upgrading from pandalchemy 0.1.x, here are the key changes:

### What's Changed

- **SQLAlchemy 2.x**: Now requires SQLAlchemy >= 2.0.0
- **New Dependencies**: Uses fullmetalalchemy and transmutation for SQL operations
- **Automatic Tracking**: All changes are now automatically tracked (no manual tracking needed)
- **Transaction-First**: All pushes happen in transactions with automatic rollback
- **Immutable Primary Keys**: PKs are now stored as DataFrame index and cannot be modified
- **Composite PK Support**: Full support for multi-column primary keys
- **Auto-Increment**: Optional auto-generation of primary key values

### Breaking Changes

1. **Primary keys are now the DataFrame index**
   - Old: `df['id']` to access PK column
   - New: `df.index` to access PK values, or `df.reset_index()` to get as column

2. **Cannot modify primary key values directly**
   - Old: `df.loc[0, 'id'] = 999`
   - New: Delete and re-insert: `delete_row(old_id)` then `add_row({'id': new_id, ...})`

3. **Index setter is blocked**
   - Old: `df.index = [1, 2, 3]`
   - New: Use `add_row()` and `delete_row()` methods

### Migration Steps

1. Update your dependencies:
   ```bash
   pip install --upgrade pandalchemy sqlalchemy
   ```

2. Update code that modifies primary keys:
   ```python
   # Old way (no longer works)
   df.loc[0, 'id'] = 999
   
   # New way
   df.delete_row(old_id)
   df.add_row({'id': 999, 'name': 'New Name', ...})
   ```

3. Update code that accesses PKs as columns:
   ```python
   # If you need PK as column
   df_with_pk = table.data.to_pandas().reset_index()
   ```

4. Update SQLAlchemy code to 2.x patterns (if using engine directly)

5. Remove manual change tracking code (now automatic)

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
Validates primary key integrity
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
5. **Composite Keys**: Use composite PKs for many-to-many relationships
6. **Auto-Increment**: Enable for tables with sequential integer PKs

## Best Practices

### Primary Key Management

```python
# ✅ Good: Use natural, immutable keys
users = pa.Table('users', df, 'email', engine)

# ✅ Good: Use auto-increment for synthetic keys
users = pa.Table('users', df, 'id', engine, auto_increment=True)

# ✅ Good: Use composite keys for join tables
memberships = pa.Table('memberships', df, ['user_id', 'org_id'], engine)

# ❌ Avoid: Trying to change PK values
# Instead: delete and re-insert
```

### Data Validation

```python
# Always validate before pushing
errors = table.data.validate_data()
if errors:
    print(f"Validation failed: {errors}")
    # Fix errors before pushing
else:
    table.push()
```

### Transaction Management

```python
# Group related changes in one transaction
try:
    # Multiple related changes
    db['users'].data.update_row(1, {'status': 'active'})
    db['logs'].data.add_row({'user_id': 1, 'action': 'activated'})
    
    # Push all together
    db.push()  # Single transaction for both tables
except Exception as e:
    print(f"Transaction rolled back: {e}")
```

## Common Pitfalls

### 1. Trying to Modify Primary Key Values

**Problem:** Attempting to update primary key values directly

```python
# ❌ This will raise DataValidationError
table.data.update_row(1, {'id': 999})

# ❌ This will also fail
table.data.index = [10, 20, 30]
```

**Solution:** Delete the old row and insert a new one

```python
# ✅ Correct way to "change" a PK
old_data = table.data.get_row(1)
table.data.delete_row(1)
table.data.add_row({'id': 999, **old_data})
```

**Why:** Primary keys are immutable in SQL databases. This design enforces that constraint and prevents referential integrity issues.

### 2. Forgetting PKs are in the Index

**Problem:** Trying to access PK as a column when it's in the index

```python
# ❌ This won't work - 'id' is not a column
result = table.data['id']  # KeyError

# ❌ This won't work either
table.data.loc[table.data['id'] > 5]  # KeyError
```

**Solution:** Access the index directly or reset it

```python
# ✅ Access the index
result = table.data.index

# ✅ Filter by index
result = table.data[table.data.index > 5]

# ✅ Or reset index to get PK as column
df_with_pk = table.data.to_pandas().reset_index()
result = df_with_pk[df_with_pk['id'] > 5]
```

### 3. Wrong Composite PK Syntax

**Problem:** Using wrong format for composite primary keys

```python
# ❌ Wrong: passing PK columns separately
table.data.update_row('u1', 'org1', {'role': 'admin'})

# ❌ Wrong: using list instead of tuple
table.data.update_row(['u1', 'org1'], {'role': 'admin'})

# ❌ Wrong: wrong number of values
table.data.delete_row('u1')  # Missing org_id
```

**Solution:** Use tuples for composite PK values

```python
# ✅ Correct: composite PK as tuple
table.data.update_row(('u1', 'org1'), {'role': 'admin'})
table.data.delete_row(('u1', 'org1'))
table.data.get_row(('u1', 'org1'))
```

### 4. Auto-Increment on Wrong PK Types

**Problem:** Trying to use auto-increment with composite or non-integer PKs

```python
# ❌ Won't work: composite PK
table = pa.Table('memberships', df, ['user_id', 'org_id'], engine, auto_increment=True)
table.data.add_row({'role': 'admin'}, auto_increment=True)
# Raises: ValueError - Auto-increment only works with single-column primary keys

# ❌ Won't work: string PK
table = pa.Table('users', df, 'email', engine, auto_increment=True)
table.data.add_row({'name': 'Alice'}, auto_increment=True)
# Raises: ValueError - Primary key must be integer type
```

**Solution:** Only use auto-increment with single integer PKs

```python
# ✅ Correct: single integer PK
table = pa.Table('users', df, 'id', engine, auto_increment=True)
table.data.add_row({'name': 'Alice', 'age': 25}, auto_increment=True)
```

### 5. Forgetting to Call push()

**Problem:** Changes only in memory, not persisted to database

```python
# Make changes
table.data.add_row({'id': 10, 'name': 'Test'})
table.data.update_row(1, {'age': 30})

# ❌ Forgot to call push()
# Changes are tracked but not in database!
```

**Solution:** Always call push() to persist changes

```python
# Make changes
table.data.add_row({'id': 10, 'name': 'Test'})
table.data.update_row(1, {'age': 30})

# ✅ Push to database
table.push()

# Or push all tables at once
db.push()
```

### 6. Modifying Derived DataFrames

**Problem:** Trying to push changes made to derived DataFrames

```python
# Get a subset
subset = table.data.copy()  # Independent tracker!
subset.update_row(1, {'age': 30})

# ❌ This won't affect the original table
table.push()  # Subset changes not included
```

**Solution:** Modify the original table directly

```python
# ✅ Modify original table
table.data.update_row(1, {'age': 30})
table.push()

# Or use pandas operations directly on table.data
table.data.loc[table.data.index == 1, 'age'] = 30
table.push()
```

### 7. Large Tables in Memory

**Problem:** Loading huge tables entirely into memory

```python
# ❌ Problematic for multi-GB tables
db = pa.DataBase(engine)  # Loads ALL tables immediately
huge_table = db['big_table']  # Loads entire table into RAM
```

**Solution:** Use lazy loading and consider chunking

```python
# ✅ Use lazy loading
db = pa.DataBase(engine, lazy=True)
huge_table = db['big_table']  # Only loads when accessed

# ✅ Or use Ibis for read-only queries on large data
# pandalchemy is best for smaller operational tables
```

### 8. Mixing pandas and pandalchemy Operations

**Problem:** Bypassing the tracker by modifying underlying DataFrame

```python
# ❌ This bypasses change tracking
table.data._data['new_column'] = [1, 2, 3]  # Not tracked!
table.push()  # new_column not added to database
```

**Solution:** Use the public API

```python
# ✅ Use public methods that are tracked
table.data['new_column'] = [1, 2, 3]
table.data.add_column_with_default('new_column', 0)

# ✅ Or use pandas methods on table.data directly
table.data['new_column'] = table.data['old_column'] * 2
```

### 9. Type Conversion Issues

**Problem:** Pandas Timestamp or numpy types causing SQL errors

```python
# ❌ Pandas Timestamp might fail with some databases
table.data.add_column_with_default('created_at', pd.Timestamp.now())
table.push()  # May raise InterfaceError
```

**Solution:** Convert to string or use database-native types

```python
# ✅ Convert to string
table.data.add_column_with_default('created_at', str(pd.Timestamp.now()))

# ✅ Or use datetime.datetime
from datetime import datetime
table.data.add_column_with_default('created_at', datetime.now())
```

### 10. Not Validating Before Push

**Problem:** Pushing invalid data and getting cryptic SQL errors

```python
# Make changes that violate constraints
table.data.add_row({'id': 1, 'name': 'Duplicate'})  # ID 1 already exists
table.push()  # Raises confusing SQL error
```

**Solution:** Validate data before pushing

```python
# ✅ Validate first
table.data.add_row({'id': 10, 'name': 'Valid'})

errors = table.data.validate_data()
if errors:
    print(f"Validation errors: {errors}")
    # Fix issues
else:
    table.push()
```

### 11. Incorrect Primary Key Specification

**Problem:** Specifying wrong or missing primary key

```python
# ❌ PK column doesn't exist in DataFrame
table = pa.Table('users', df, 'user_id', engine)  # df has 'id', not 'user_id'

# ❌ Missing PK for composite key
table = pa.Table('memberships', df, 'user_id', engine)  
# Should be ['user_id', 'org_id']
```

**Solution:** Ensure PK columns exist and match database schema

```python
# ✅ Verify PK exists in DataFrame
if 'id' in df.columns or df.index.name == 'id':
    table = pa.Table('users', df, 'id', engine)

# ✅ Use correct composite PK
table = pa.Table('memberships', df, ['user_id', 'org_id'], engine)
```

### 12. Concurrent Modifications

**Problem:** Multiple processes modifying same table

```python
# Process 1
db['users'].data.update_row(1, {'age': 30})

# Process 2 (at same time)
db['users'].data.update_row(1, {'age': 35})

# ❌ Last writer wins, no conflict detection
```

**Solution:** Use database-level locking or application-level coordination

```python
# ✅ Use transactions wisely
# Pull latest data before modifications
db.pull()  # Get fresh data

# Make changes
db['users'].data.update_row(1, {'age': 30})

# Push immediately
db.push()

# Note: For high-concurrency scenarios, consider using
# database-level optimistic locking or row versioning
```

## Known Limitations

While pandalchemy is powerful, there are some limitations to be aware of:

### 1. Schema Changes with Immediate Updates

Adding columns and immediately updating their values in the same transaction may not persist correctly:

```python
# NOT RECOMMENDED: Schema change + data update in same transaction
db['articles'].data.add_column_with_default('reviewed_by', None)
db['articles'].data.update_row(1, {'reviewed_by': 'editor1'})  
db.push()  # reviewed_by update may not persist

# RECOMMENDED: Push schema changes first
db['articles'].data.add_column_with_default('reviewed_by', None)
db['articles'].push()  # Push schema change

db.pull()  # Refresh
db['articles'].data.update_row(1, {'reviewed_by': 'editor1'})
db.push()  # Now update persists
```

### 2. Type Conversion with Empty DataFrames

Creating empty DataFrames without explicit dtypes defaults to float64, which can cause type mismatches:

```python
# PROBLEMATIC: No type specification
enrollments = pd.DataFrame({
    'student_id': [],
    'course_id': [],
    'status': []
})  # All columns default to float64

# BETTER: Specify types explicitly
enrollments = pd.DataFrame({
    'student_id': pd.Series([], dtype='int64'),
    'course_id': pd.Series([], dtype='int64'),
    'status': pd.Series([], dtype='str')
})
```

**Note**: The `pull_table` function includes automatic type inference to handle most type mismatch scenarios, but explicit types are still recommended.

### 3. Renaming Newly Added Columns

You cannot rename a column that was added in the same transaction:

```python
# WILL FAIL: Rename column added in same transaction
db['products'].data.add_column_with_default('price_dollars', 0.0)
db['products'].data.rename_column_safe('price_dollars', 'price')
db.push()  # TransactionError: Column doesn't exist yet in database

# CORRECT: Push first, then rename
db['products'].data.add_column_with_default('price_dollars', 0.0)
db.push()

db.pull()
db['products'].data.rename_column_safe('price_dollars', 'price')
db.push()
```

### 4. Boolean Columns with NULL/NaN Values

SQLite and some databases have strict boolean type checking that doesn't handle NaN:

```python
# PROBLEMATIC: NaN in boolean column
flags = pd.DataFrame({
    'id': [1, 2],
    'active': [True, None]  # None becomes NaN
})
db.create_table('flags', flags, primary_key='id')
db.push()  # May fail: "Not a boolean value: nan"

# BETTER: Use explicit False or handle NaN
flags['active'] = flags['active'].fillna(False)
```

### 5. Database Re-initialization on create_table()

Creating a new table causes the DataBase object to reinitialize, which clears in-memory changes:

```python
# PROBLEMATIC: Creating table after modifications
db['accounts'].data.update_row(1, {'balance': 900.0})
usd_balance = db['accounts'].data.get_row(1)['balance']  # 900.0

# Creating new table reinitializes db
db.create_table('conversions', conversions_df, primary_key='id')

# Previous modification lost!
usd_balance = db['accounts'].data.get_row(1)['balance']  # Back to 1000.0

# CORRECT: Push changes before creating new tables
db['accounts'].data.update_row(1, {'balance': 900.0})
db['accounts'].push()  # Push before creating new table

db.create_table('conversions', conversions_df, primary_key='id')
```

### 6. Manually Adding Tables to db.db

Manually adding Table objects to `db.db` after initialization can cause issues:

```python
# PROBLEMATIC: Manual table addition
profiles_table = Table('user_profiles', profiles, 'user_id', engine)
profiles_table.push()
db.db['user_profiles'] = profiles_table  # May not work correctly

# CORRECT: Use db.create_table or db.add_table
db.create_table('user_profiles', profiles, primary_key='user_id')
# or
table = Table('user_profiles', profiles, 'user_id', engine)
db.add_table(table, push=True)
```

### 7. Merging Tables with Different Primary Keys

When merging data from tables with different PK column names, use pandas merge:

```python
# PROBLEMATIC: Direct iteration with get_row
for user_id in db['users'].data.index:
    profile = db['profiles'].data.get_row(user_id)  # May return None
    # if profiles uses 'user_id' but users uses 'id'

# CORRECT: Use pandas merge
users_df = db['users'].data.to_pandas().reset_index()
profiles_df = db['profiles'].data.to_pandas().reset_index()
merged_df = users_df.merge(profiles_df, left_on='id', right_on='user_id')
```

### 8. get_row() with Newly Added Columns

Newly added columns may not be accessible via `get_row()` until after a push/pull cycle:

```python
# PROBLEMATIC: Accessing new column immediately
db['documents'].data.add_column_with_default('version', 1)
doc = db['documents'].data.get_row(1)
version = doc['version']  # May raise KeyError

# CORRECT: Use direct DataFrame access or push first
version = db['documents'].data._data.loc[1, 'version']
# or
db['documents'].push()
db.pull()
doc = db['documents'].data.get_row(1)
version = doc['version']  # Now works
```

### Best Practices to Avoid Limitations

1. **Push schema changes immediately** before updating data in new columns
2. **Specify dtypes explicitly** when creating empty DataFrames
3. **Push before creating new tables** to avoid losing in-memory changes
4. **Use pandas merge** for complex table joins and denormalization
5. **Handle NULL values** appropriately for boolean and numeric columns
6. **Separate schema operations** from data operations across transactions
7. **Re-instantiate DataBase** after major structural changes

## Development

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=pandalchemy --cov-report=html

# Run specific test file
pytest tests/test_composite_pk_integration.py
```

### Code Quality

```bash
# Type checking
mypy src/

# Linting
ruff check src/

# Run all quality checks
pytest && mypy src/ && ruff check src/
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup

```bash
git clone https://github.com/eddiethedean/pandalchemy
cd pandalchemy
pip install -e ".[dev]"
pytest
```

## Comparison with Similar Tools

### pandalchemy vs Ibis

[Ibis](https://ibis-project.org/) is another popular Python library for working with databases. While both tools integrate pandas with SQL databases, they have fundamentally different philosophies and use cases:

#### Ibis: Query-Focused, Lazy Execution

Ibis is designed for **querying and analyzing data** that lives in databases:

```python
# Ibis example - queries stay in SQL
import ibis

con = ibis.duckdb.connect('example.db')
users = con.table('users')

# Build query expression (not executed yet)
result = users.filter(users.age > 25).select(users.name, users.age)

# Execute and get pandas DataFrame
df = result.execute()
```

**Ibis strengths:**
- ✅ Lazy evaluation - queries only execute when needed
- ✅ Query optimization - complex expressions compiled to efficient SQL
- ✅ Multiple backend support (PostgreSQL, BigQuery, Spark, etc.)
- ✅ Great for large datasets that don't fit in memory
- ✅ Expressive API for complex analytical queries
- ✅ Stays in SQL as long as possible (performance)

**Ibis limitations:**
- ❌ Read-only by default (not designed for writes/updates)
- ❌ No automatic change tracking
- ❌ No transaction management for updates
- ❌ Not designed for CRUD operations
- ❌ Manual DataFrame ↔ SQL synchronization

#### pandalchemy: DataFrame-Focused, Change Tracking

pandalchemy is designed for **bidirectional sync** between pandas and databases:

```python
# pandalchemy example - DataFrame with change tracking
import pandalchemy as pa

db = pa.DataBase(engine)
users = db['users']

# Work with pandas DataFrame locally
users.data.loc[users.data['age'] > 25, 'senior'] = True
users.data.add_row({'id': 10, 'name': 'Dave', 'age': 30})

# All changes are tracked and pushed efficiently
users.push()  # Transaction-safe update to database
```

**pandalchemy strengths:**
- ✅ Automatic change tracking (operations, schema, rows)
- ✅ Transaction-safe updates with rollback
- ✅ Full CRUD operations (add, update, delete, upsert)
- ✅ Schema evolution (add/drop/rename columns)
- ✅ Immutable primary keys with validation
- ✅ Optimized SQL execution plans
- ✅ Works with standard pandas API

**pandalchemy limitations:**
- ❌ Loads full tables into memory (not for huge datasets)
- ❌ Single backend (SQLAlchemy)
- ❌ Eager execution (data loaded immediately)
- ❌ Not optimized for complex analytical queries

#### When to Use Each

**Use Ibis when you need:**
- 📊 Complex analytical queries on large datasets
- 🚀 Query performance and optimization
- 🌐 Multiple database backend support
- 📈 Data that doesn't fit in memory
- 🔍 Exploratory data analysis on remote data
- ⚡ Lazy evaluation and query compilation

**Use pandalchemy when you need:**
- ✏️ CRUD operations (create, read, update, delete)
- 🔄 Bidirectional DataFrame ↔ SQL synchronization
- 📝 Change tracking and audit trails
- 🛡️ Transaction safety with automatic rollback
- 🔧 Schema evolution and migrations
- 🗂️ Working with normalized tables and relationships
- 🎯 Primary key management and validation

#### Combining Both Tools

You can use both libraries together for different purposes:

```python
import ibis
import pandalchemy as pa

# Use Ibis for complex read queries
con = ibis.duckdb.connect('app.db')
large_result = con.table('transactions') \
    .filter(ibis._.amount > 1000) \
    .group_by('user_id') \
    .aggregate(total=ibis._.amount.sum()) \
    .execute()

# Use pandalchemy for updates
db = pa.DataBase(engine)
db['users'].data.update_row(123, {'credit_limit': 5000})
db['users'].push()
```

#### Summary Table

| Feature | Ibis | pandalchemy |
|---------|------|-------------|
| **Primary Use Case** | Querying & Analytics | CRUD & Sync |
| **Execution Model** | Lazy (compiles to SQL) | Eager (loads to memory) |
| **Data Location** | Keeps data in database | Loads into pandas |
| **Write Operations** | Limited | Full CRUD support |
| **Change Tracking** | ❌ No | ✅ Automatic |
| **Transactions** | Manual | ✅ Automatic |
| **Schema Evolution** | ❌ No | ✅ Yes |
| **Multiple Backends** | ✅ Many | SQLAlchemy only |
| **Memory Efficiency** | ✅ Excellent | Moderate (in-memory) |
| **Primary Keys** | Basic support | ✅ Advanced (immutable, composite, auto-increment) |
| **pandas Compatibility** | Similar API | ✅ Full compatibility |
| **Best For** | Large-scale analytics | Application databases |

**Bottom Line:**
- **Ibis**: Choose for data science, analytics, and querying large datasets
- **pandalchemy**: Choose for application development, CRUD operations, and data synchronization

Both are excellent tools - they just solve different problems! Many teams use both in different parts of their data pipeline.

## License

MIT License - see LICENSE file for details

## Links

- **GitHub**: https://github.com/eddiethedean/pandalchemy
- **Issues**: https://github.com/eddiethedean/pandalchemy/issues
- **PyPI**: https://pypi.org/project/pandalchemy/
- **Documentation**: https://github.com/eddiethedean/pandalchemy#readme

## Acknowledgments

Built with:
- [pandas](https://pandas.pydata.org/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [fullmetalalchemy](https://github.com/eddiethedean/fullmetalalchemy)
- [transmutation](https://github.com/eddiethedean/transmutation)

## Changelog

### Version 0.2.0 (Latest)

**Major Features:**
- ✨ Immutable primary keys (stored as DataFrame index)
- ✨ Composite primary key support with auto-detection
- ✨ Auto-increment support for primary keys
- ✨ Enhanced CRUD operations (add_row, update_row, delete_row, upsert_row, bulk_insert)
- ✨ Schema helper methods (add_column_with_default, drop_column_safe, rename_column_safe)
- ✨ Primary key validation before push
- ✨ MultiIndex support for composite keys
- 🔧 Improved transaction handling
- 🔧 Better error messages and validation
- 📝 Comprehensive test coverage (85%+)
- 📝 Updated documentation with new examples

**Breaking Changes:**
- Primary keys now stored as DataFrame index (not columns)
- Cannot modify primary key values directly (use delete + insert)
- Index setter is blocked

### Version 0.1.x

- Initial release with change tracking
- Basic SQL operations
- Transaction support
