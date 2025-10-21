# %% [markdown]
# # 06_schema_evolution
# 
# Schema Evolution
# 
# This example demonstrates schema modification capabilities:
# - Adding columns with defaults
# - Dropping columns safely
# - Renaming columns
# - Changing column types
# - Best practices for schema changes

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %%
# Setup
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# %%
print("Schema Evolution Example")

# %%
# Create initial table
users_data = pd.DataFrame({
    'username': ['alice', 'bob', 'charlie'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
}, index=[1, 2, 3])

users = db.create_table('users', users_data, primary_key='id')

# %% [markdown]
# ### 1. Initial Schema

# %%
print(users.to_pandas())
print(f"\nColumns: {list(users._data.columns)}")
print(f"Dtypes:\n{users._data.dtypes}")

# %% [markdown]
# ### 2. Adding Columns

# %%
# Add column with default value

# %% [markdown]
# ### \n   a) Add 'active' column with default True:

# %%
users.add_column_with_default('active', True)
users.push()

print("      ✓ Column added")
print(users.to_pandas())

# Add multiple columns

# %% [markdown]
# ### \n   b) Add multiple columns:

# %%
users.pull()  # Refresh first
users.add_column_with_default('verified', False)
users.add_column_with_default('signup_date', '2024-01-01')
users.push()

print("      ✓ Added 'verified' and 'signup_date'")
users.pull()
print(users.to_pandas())

# Add column with None/null default

# %% [markdown]
# ### \n   c) Add nullable column:

# %%
users.add_column_with_default('phone', None)
users.push()

print("      ✓ Added 'phone' (nullable)")
users.pull()
print(users.to_pandas())

# %% [markdown]
# ### 3. Renaming Columns

# %%
# Rename single column

# %% [markdown]
# ### \n   a) Rename 'username' to 'name':

# %%
users.rename_column_safe('username', 'name')
users.push()

print("      ✓ Column renamed")
users.pull()
print(users.to_pandas())

# Rename multiple columns

# %% [markdown]
# ### \n   b) Rename 'signup_date' to 'created_at':

# %%
users.rename_column_safe('signup_date', 'created_at')
users.push()
users.pull()

print("      ✓ Column renamed")
print(f"      Columns: {list(users._data.columns)}")

# %% [markdown]
# ### 4. Dropping Columns

# %%
# Drop single column

# %% [markdown]
# ### \n   a) Drop 'phone' column:

# %%
users.drop_column_safe('phone')
users.push()

print("      ✓ Column dropped")
users.pull()
print(users.to_pandas())

# Drop multiple columns in sequence

# %% [markdown]
# ### \n   b) Clean up boolean flags:

# %%
users.drop_column_safe('active')
users.push()
users.pull()
users.drop_column_safe('verified')
users.push()

print("      ✓ Dropped 'active' and 'verified'")
users.pull()
print(users.to_pandas())

# %% [markdown]
# ### 5. Changing Column Types

# %%
# Create table with type change needs
orders_data = pd.DataFrame({
    'order_number': ['1001', '1002', '1003'],  # String but should be int
    'amount': [99.99, 149.50, 299.99],
    'quantity': [1.0, 2.0, 1.0]  # Float but should be int
}, index=[1, 2, 3])

orders = db.create_table('orders', orders_data, primary_key='id')

print("\n   Initial types:")
print(orders._data.dtypes)

# Convert string to int

# %% [markdown]
# ### \n   a) Convert order_number to integer:

# %%
orders.change_column_type('order_number', int)
orders.push()
orders.pull()

print("      ✓ Converted to int")
print(f"      New type: {orders._data['order_number'].dtype}")

# Convert float to int

# %% [markdown]
# ### \n   b) Convert quantity to integer:

# %%
orders.change_column_type('quantity', int)
orders.push()
orders.pull()

print("      ✓ Converted to int")
print(f"      New type: {orders._data['quantity'].dtype}")

print("\n   Final types:")
print(orders._data.dtypes)

# %% [markdown]
# ### 6. Complex Schema Evolution

# %%
# Real-world example: evolving user table
print("\n   Starting schema:")
print(users.to_pandas())

# Step 1: Add new fields
print("\n   Step 1: Add user profile fields")
users.add_column_with_default('first_name', '')
users.add_column_with_default('last_name', '')
users.add_column_with_default('age', 0)
users.push()
users.pull()

# Step 2: Populate from existing data
print("\n   Step 2: Populate from 'name' column")
for idx in users._data.index:
    name = users.get_row(idx)['name']
    # Simple split (in real code, handle edge cases)
    parts = name.split()
    if len(parts) >= 1:
        users.update_row(idx, {'first_name': parts[0].capitalize()})
    if len(parts) >= 2:
        users.update_row(idx, {'last_name': parts[-1].capitalize()})

users.push()
users.pull()
print(users.to_pandas())

# Step 3: Drop old column
print("\n   Step 3: Drop old 'name' column")
users.drop_column_safe('name')
users.push()
users.pull()

print("      ✓ Schema evolution complete")
print(users.to_pandas())

# %% [markdown]
# ### 7. Best Practices

# %%
print("\n   ✓ DO: Push schema changes separately from data changes")
print("      users.add_column_with_default('status', 'active')")
print("      users.push()  # Push schema")
print("      users.pull()  # Refresh")
print("      users.loc[0, 'status'] = 'inactive'")
print("      users.push()  # Push data")
print()
print("   ❌ DON'T: Mix schema and data changes in same push")
print("      users.add_column_with_default('status', 'active')")
print("      users.loc[0, 'status'] = 'inactive'  # May not persist")
print("      users.push()")

# %% [markdown]
# ### 8. Error Handling

# %%
# Try to rename non-existent column

# %% [markdown]
# ### \n   a) Rename non-existent column:

# %%
try:
    users.rename_column_safe('nonexistent', 'newname')
    users.push()
    print("      ❌ Should have failed")
except Exception as e:
    print(f"      ✓ Correctly prevented: {type(e).__name__}")

# Try to drop non-existent column

# %% [markdown]
# ### \n   b) Drop non-existent column:

# %%
try:
    users.drop_column_safe('nonexistent')
    users.push()
    print("      ❌ Should have failed")
except Exception as e:
    print(f"      ✓ Correctly prevented: {type(e).__name__}")

# %% [markdown]
# ### 9. Schema Change Workflow

# %%
print("\n   Recommended workflow for major changes:")
print()
print("   1. Add new columns with defaults")
print("      → Push and verify")
print()
print("   2. Populate new columns from old data")
print("      → Push and verify")
print()
print("   3. Update application to use new columns")
print("      → Deploy and verify")
print()
print("   4. Drop old columns")
print("      → Push and verify")

# %% [markdown]
# ### 10. Tracking Schema Changes

# %%
# Schema changes appear in change summary
products_data = pd.DataFrame({
    'name': ['Widget', 'Gadget'],
    'price': [9.99, 19.99]
}, index=[1, 2])

products = db.create_table('products', products_data, primary_key='id')

# Make schema changes
products.add_column_with_default('stock', 0)
products.rename_column_safe('price', 'unit_price')

# Check change summary
summary = products.get_changes_summary()
print("\n   Change summary:")
for key, value in summary.items():
    if value:
        print(f"      {key}: {value}")

products.push()
print("\n   ✓ Schema changes committed")

print("\n" + "=" * 70)
print("Example Complete!")
print("Key Takeaways:")
print("  • Use add_column_with_default() to add columns")
print("  • Use rename_column_safe() to rename columns")
print("  • Use drop_column_safe() to drop columns")
print("  • Use convert_column_type() to change types")
print("  • Always push schema changes separately")
print("  • Pull after schema push before data changes")
