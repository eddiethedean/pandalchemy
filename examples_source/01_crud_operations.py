# %% [markdown]
# # CRUD Operations
#
# This notebook demonstrates all CRUD operations in pandalchemy:
# - **C**reate: add_row, bulk_insert
# - **R**ead: get_row, row_exists
# - **U**pdate: update_row, upsert_row
# - **D**elete: delete_row

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %% [markdown]
# ## Setup
# Create a database and initial table with auto-increment

# %%
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# Create initial table
initial_data = pd.DataFrame({
    'name': ['Alice', 'Bob'],
    'email': ['alice@example.com', 'bob@example.com'],
    'age': [25, 30]
}, index=[1, 2])

users = pa.TableDataFrame('users', initial_data, 'id', engine, auto_increment=True)
users.push()

users.to_pandas()

# %% [markdown]
# ## 1. CREATE Operations

# %% [markdown]
# ### Add single row with auto-increment

# %%
users.add_row({'name': 'Charlie', 'email': 'charlie@example.com', 'age': 35}, auto_increment=True)
print("✓ Added Charlie (ID will be auto-generated)")

# %% [markdown]
# ### Bulk insert multiple rows

# %%
new_users = [
    {'name': 'Diana', 'email': 'diana@example.com', 'age': 28},
    {'name': 'Eve', 'email': 'eve@example.com', 'age': 32},
    {'name': 'Frank', 'email': 'frank@example.com', 'age': 45}
]

for user in new_users:
    users.add_row(user, auto_increment=True)

users.push()
print(f"✓ Added {len(new_users)} users")
print(f"Total users: {len(users._data)}")

users.to_pandas()

# %% [markdown]
# ## 2. READ Operations

# %% [markdown]
# ### Get row by primary key

# %%
users.get_row(1)

# %% [markdown]
# ### Check if row exists

# %%
print(f"User 1 exists: {users.row_exists(1)}")
print(f"User 999 exists: {users.row_exists(999)}")

# %% [markdown]
# ### Get all data

# %%
df = users.to_pandas()
print(f"Total rows: {len(df)}, Columns: {list(df.columns)}")
df

# %% [markdown]
# ### Filter data using pandas

# %%
seniors = users._data[users._data['age'] > 30]
print(f"Users over 30: {len(seniors)}")
seniors[['name', 'age']]

# %% [markdown]
# ## 3. UPDATE Operations

# %% [markdown]
# ### Update single field

# %%
users.update_row(1, {'age': 26})
print("✓ Updated Alice's age to 26")

# %% [markdown]
# ### Update multiple fields

# %%
users.update_row(2, {'email': 'robert@example.com', 'age': 31})
print("✓ Updated Bob's email and age")

# %% [markdown]
# ### Upsert operation (update or insert)

# %%
# Update existing
users.upsert_row({'id': 2, 'name': 'Robert', 'email': 'robert@example.com', 'age': 31})
print("✓ Upserted user 2 (updated existing)")

# Insert new
users.upsert_row({'id': 100, 'name': 'Grace', 'email': 'grace@example.com', 'age': 29})
print("✓ Upserted user 100 (created new)")

users.push()
users.to_pandas()

# %% [markdown]
# ## 4. DELETE Operations

# %% [markdown]
# ### Delete rows

# %%
users.delete_row(100)
users.delete_row(5)
users.delete_row(6)

users.push()
print(f"✓ Remaining users: {len(users._data)}")
users.to_pandas()

# %% [markdown]
# ## 5. Pandas Integration
# All pandas operations work seamlessly with pandalchemy

# %%
# Update via pandas .loc and .at
users._data.loc[1, 'email'] = 'alice.updated@example.com'
users._data.at[2, 'age'] = 32

# Add column via pandas
users._data['verified'] = [True, False, True, False]

users.push()
users.to_pandas()

# %% [markdown]
# ## 6. Error Handling

# %%
# Try to update non-existent row
try:
    users.update_row(999, {'age': 100})
except Exception as e:
    print(f"✓ Correctly raised: {type(e).__name__}")

# Try to update primary key (immutable)
from pandalchemy.exceptions import DataValidationError
try:
    users.update_row(1, {'id': 999})
except DataValidationError:
    print("✓ Correctly prevented: Primary keys are immutable")

# %% [markdown]
# ## Summary
#
# **Key Takeaways:**
# - `add_row()` with `auto_increment=True` for automatic IDs
# - `get_row()` and `row_exists()` for reading data
# - `update_row()` for updates, `upsert_row()` for update-or-insert
# - `delete_row()` for deletions
# - All pandas DataFrame operations work and are automatically tracked
# - Primary keys are immutable (stored as DataFrame index)

