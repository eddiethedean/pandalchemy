# %% [markdown]
# # Automatic Change Tracking
#
# This notebook demonstrates how pandalchemy automatically tracks all changes:
# - Tracking inserts, updates, and deletes
# - Getting change summaries
# - Understanding tracked operations
# - Inspecting what will be executed

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %% [markdown]
# ## Setup
# Create a database with an employees table

# %%
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

initial_data = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'department': ['Engineering', 'Sales', 'Engineering'],
    'salary': [80000, 60000, 75000]
}, index=[1, 2, 3])

employees = pa.TableDataFrame('employees', initial_data, 'id', engine)
employees.push()

print("Initial state:")
employees.to_pandas()

# %% [markdown]
# ### Check initial state

# %%
print(f"Has changes: {employees.has_changes()}")

# %% [markdown]
# ## Making Changes
# Let's make various types of changes and see how they're tracked

# %% [markdown]
# ### Add new employees (inserts)

# %%
employees.add_row({'id': 4, 'name': 'Diana', 'department': 'Marketing', 'salary': 70000})
employees.add_row({'id': 5, 'name': 'Eve', 'department': 'Engineering', 'salary': 85000})
print("✓ Added Diana and Eve")

# %% [markdown]
# ### Give raises (updates)

# %%
employees.update_row(1, {'salary': 85000})
employees.update_row(3, {'salary': 80000})
print("✓ Updated Alice and Charlie's salaries")

# %% [markdown]
# ### Remove employee (delete)

# %%
employees.delete_row(2)
print("✓ Deleted Bob")

# %% [markdown]
# ### Department reorganization (bulk update via pandas)

# %%
employees._data.loc[employees._data['department'] == 'Engineering', 'department'] = 'Product'
print("✓ Renamed Engineering to Product department")

# %% [markdown]
# ## Change Summary
# Get a summary of all tracked changes

# %%
summary = employees.get_changes_summary()
print(f"Has changes: {employees.has_changes()}\n")
print("Change summary:")
for key, value in summary.items():
    if value:  # Only show non-zero values
        print(f"  {key}: {value}")

# %% [markdown]
# ## Tracked Operations
# Inspect the detailed tracking information

# %%
tracker = employees.get_tracker()

print(f"Total operations tracked: {len(tracker.operations)}")
print(f"Has changes: {employees.has_changes()}\n")

print("Row-level changes:")
print(f"  Inserted rows: {len(tracker.get_inserts())}")
print(f"  Updated rows: {len(tracker.get_updates())}")
print(f"  Deleted rows: {len(tracker.get_deletes())}")

# %% [markdown]
# ## Preview Before Push
# See what the data looks like before committing

# %%
print("Current state (before push):")
employees.to_pandas()

# %% [markdown]
# ### Statistics

# %%
print(f"Total rows in memory: {len(employees._data)}")
print(f"Rows to insert: {summary.get('inserts', 0)}")
print(f"Rows to update: {summary.get('updates', 0)}")
print(f"Rows to delete: {summary.get('deletes', 0)}")

# %% [markdown]
# ## Push Changes
# Commit all changes to the database in one transaction

# %%
employees.push()
print("✓ Changes pushed to database")
print(f"Has changes after push: {employees.has_changes()}")

# Verify from database
employees.pull()
print("\nVerified state from database:")
employees.to_pandas()

# %% [markdown]
# ## More Complex Tracking
# Making multiple changes in sequence

# %% [markdown]
# ### Add another employee

# %%
employees.add_row({'id': 6, 'name': 'Frank', 'department': 'Sales', 'salary': 65000})
print("✓ Added Frank")

# %% [markdown]
# ### Update multiple employees

# %%
# Give raises to Product department
employees.update_where(
    employees._data['department'] == 'Product',
    {'salary': lambda x: x + 5000}
)
print("✓ Gave raises to Product department")

# %% [markdown]
# ### Check changes before pushing

# %%
summary2 = employees.get_changes_summary()
print("Change summary:")
for key, value in summary2.items():
    if value:
        print(f"  {key}: {value}")

# %% [markdown]
# ### Push all changes

# %%
employees.push()
print("✓ All changes committed")

employees.to_pandas()

# %% [markdown]
# ## Change Tracking Lifecycle

# %% [markdown]
# ### After pull, tracker is reset

# %%
employees.pull()
print(f"Has changes after pull: {employees.has_changes()}")
print(f"Change summary: {employees.get_changes_summary()}")

# %% [markdown]
# ### Make new changes

# %%
employees.update_row(1, {'salary': 90000})
print(f"\nAfter new change:")
print(f"  Has changes: {employees.has_changes()}")
print(f"  Changes: {employees.get_changes_summary()}")

# %% [markdown]
# ## Working with Change Details

# %%
# Make multiple operations
employees.update_row(4, {'department': 'Sales'})
employees.update_row(5, {'salary': 88000})
employees.delete_row(6)

tracker = employees.get_tracker()

# Show affected IDs
update_ids = [rc.primary_key_value for rc in tracker.get_updates()]
delete_ids = [rc.primary_key_value for rc in tracker.get_deletes()]

print(f"Inserted rows: {len(tracker.get_inserts())}")
print(f"Updated rows: {len(tracker.get_updates())}")
print(f"Deleted rows: {len(tracker.get_deletes())}")

if update_ids:
    print(f"\nUpdated row IDs: {', '.join(map(str, update_ids))}")
if delete_ids:
    print(f"Deleted row IDs: {', '.join(map(str, delete_ids))}")

employees.push()

# %% [markdown]
# ## Summary
#
# **Key Takeaways:**
# - All DataFrame operations are automatically tracked
# - Use `get_changes_summary()` to see what will be executed
# - Use `has_changes()` to check if push is needed
# - Tracker is reset after `push()` or `pull()`
# - Access detailed change information via `get_tracker()`
# - Changes are organized by type: inserts, updates, deletes, schema changes

