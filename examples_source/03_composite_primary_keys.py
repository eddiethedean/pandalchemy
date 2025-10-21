# %% [markdown]
# # Composite Primary Keys
#
# This notebook demonstrates working with multi-column primary keys:
# - Creating tables with composite PKs
# - Using pandas MultiIndex
# - CRUD operations with tuple keys
# - Real-world many-to-many relationships

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %% [markdown]
# ## Setup

# %%
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# %% [markdown]
# ## Creating Table with Composite PK
# A many-to-many relationship table (users ↔ organizations)

# %%
memberships_data = pd.DataFrame({
    'user_id': [1, 1, 2, 2, 3],
    'org_id': ['org1', 'org2', 'org1', 'org3', 'org1'],
    'role': ['admin', 'member', 'member', 'admin', 'member'],
    'joined_date': ['2024-01-01', '2024-02-15', '2024-01-10', '2024-03-01', '2024-01-05']
})

print("Original DataFrame:")
memberships_data

# %%
# Create table with composite PK
memberships = db.create_table('memberships', memberships_data, 
                               primary_key=['user_id', 'org_id'])

print("✓ Table created with composite primary key")
print(f"Index names: {memberships._data.index.names}")
print(f"Index levels: {memberships._data.index.nlevels}")

memberships.to_pandas()

# %% [markdown]
# ## CRUD with Composite Keys
# Use tuples to access rows with composite keys

# %% [markdown]
# ### Read with composite key

# %%
# Get row by composite key (tuple)
membership = memberships.get_row((1, 'org1'))
print(f"User 1 in org1: {membership}")

# %%
# Check if membership exists
exists = memberships.row_exists((2, 'org1'))
not_exists = memberships.row_exists((999, 'org999'))
print(f"(2, 'org1') exists: {exists}")
print(f"(999, 'org999') exists: {not_exists}")

# %% [markdown]
# ### Create with composite key

# %%
memberships.add_row({
    'user_id': 3,
    'org_id': 'org2',
    'role': 'member',
    'joined_date': '2024-04-01'
})
print("✓ Added user 3 to org2")

# %% [markdown]
# ### Update with composite key

# %%
memberships.update_row((1, 'org2'), {'role': 'admin'})
print("✓ Promoted user 1 in org2 to admin")

# %% [markdown]
# ### Delete with composite key

# %%
memberships.delete_row((2, 'org3'))
print("✓ Removed user 2 from org3")

memberships.push()
memberships.to_pandas()

# %% [markdown]
# ## Querying with MultiIndex

# %% [markdown]
# ### Get all memberships for a user

# %%
user_1_orgs = memberships._data.loc[1]
print("All orgs for user 1:")
user_1_orgs

# %% [markdown]
# ### Get all users in an organization

# %%
org1_users = memberships._data.xs('org1', level='org_id')
print("All users in org1:")
org1_users

# %% [markdown]
# ### Filter by role

# %%
admins = memberships._data[memberships._data['role'] == 'admin']
print("All admin memberships:")
admins

# %% [markdown]
# ## Real-World Example: Student Enrollments

# %%
enrollments_data = pd.DataFrame({
    'student_id': [101, 101, 102, 102, 103, 103],
    'course_id': ['CS101', 'MATH200', 'CS101', 'ENG150', 'MATH200', 'CS101'],
    'grade': ['A', 'B+', 'A-', 'B', 'A', 'A'],
    'semester': ['Fall2024', 'Fall2024', 'Fall2024', 'Fall2024', 'Fall2024', 'Fall2024']
})

enrollments = db.create_table('enrollments', enrollments_data,
                               primary_key=['student_id', 'course_id'])

print("✓ Created enrollments table:")
enrollments.to_pandas()

# %% [markdown]
# ### Student transcript (all courses for a student)

# %%
transcript = enrollments._data.loc[101]
print("Student 101 transcript:")
transcript[['grade', 'semester']]

# %% [markdown]
# ### Course roster (all students in a course)

# %%
roster = enrollments._data.xs('CS101', level='course_id')
print("CS101 roster:")
roster[['grade', 'semester']]

# %% [markdown]
# ### Update a grade

# %%
enrollments.update_row((102, 'CS101'), {'grade': 'A'})
enrollments.push()
print("✓ Grade updated to A")

enrollments.to_pandas()

# %% [markdown]
# ## Three-Column Composite Keys
# Time-series data with server, metric, and timestamp

# %%
metrics_data = pd.DataFrame({
    'server_id': ['srv1', 'srv1', 'srv2', 'srv2', 'srv1', 'srv2'],
    'metric_name': ['cpu', 'memory', 'cpu', 'memory', 'cpu', 'memory'],
    'timestamp': ['2024-01-01 10:00', '2024-01-01 10:00', 
                  '2024-01-01 10:00', '2024-01-01 10:00',
                  '2024-01-01 10:05', '2024-01-01 10:05'],
    'value': [45.2, 62.1, 78.3, 45.8, 48.1, 67.2]
})

metrics = db.create_table('metrics', metrics_data,
                           primary_key=['server_id', 'metric_name', 'timestamp'])

print("✓ Created metrics table with 3-column PK:")
metrics.to_pandas()

# %%
# Access with 3-tuple
cpu_metric = metrics.get_row(('srv1', 'cpu', '2024-01-01 10:00'))
print(f"srv1 CPU at 10:00: {cpu_metric['value']}")

# %%
# Add new metric
metrics.add_row({
    'server_id': 'srv1',
    'metric_name': 'disk',
    'timestamp': '2024-01-01 10:00',
    'value': 82.5
})
metrics.push()

metrics.to_pandas()

# %% [markdown]
# ## Bulk Operations with Composite Keys

# %%
new_memberships = [
    {'user_id': 4, 'org_id': 'org1', 'role': 'member', 'joined_date': '2024-05-01'},
    {'user_id': 4, 'org_id': 'org2', 'role': 'admin', 'joined_date': '2024-05-15'},
    {'user_id': 5, 'org_id': 'org1', 'role': 'member', 'joined_date': '2024-06-01'},
]

for membership in new_memberships:
    memberships.add_row(membership)

memberships.push()
print(f"✓ Added {len(new_memberships)} memberships")
print(f"Total memberships: {len(memberships._data)}")

memberships.to_pandas()

# %% [markdown]
# ## Conditional Operations with Composite Keys

# %%
# Update all members in org1 to contributors
memberships.update_where(
    (memberships._data.index.get_level_values('org_id') == 'org1') & 
    (memberships._data['role'] == 'member'),
    {'role': 'contributor'}
)

print("✓ Promoted all members in org1 to contributors")

# %%
# Delete old memberships
old_count = memberships.delete_where(
    memberships._data['joined_date'] < '2024-02-01'
)
print(f"✓ Deleted {old_count} old memberships")

memberships.push()
memberships.to_pandas()

# %% [markdown]
# ## Summary
#
# **Key Takeaways:**
# - Composite PKs use pandas MultiIndex
# - Access rows with tuples: `(key1, key2)` or `(key1, key2, key3)`
# - Use `.loc[key1]` for single level access
# - Use `.xs(value, level='name')` for cross-sections
# - Perfect for many-to-many relationships
# - All CRUD operations work with tuple keys
# - Conditional operations work with composite keys

