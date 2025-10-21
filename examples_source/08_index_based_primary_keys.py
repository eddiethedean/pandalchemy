# %% [markdown]
# # 08_index_based_primary_keys
# 
# Index-Based Primary Keys
# 
# This example demonstrates the new index-based primary key feature:
# - Using DataFrame index as primary key
# - Automatic index naming
# - Validation and error handling
# - Mixing with column-based PKs

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %%
# Setup
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# %%
print("Index-Based Primary Keys Example")

# %% [markdown]
# ### 1. Basic Index-Based Primary Key

# %%
# Create DataFrame with unnamed index
df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['NYC', 'SF', 'LA']
}, index=[1, 2, 3])  # Index has values but no name

print("\n   DataFrame before:")
print(f"   Index name: {df.index.name}")
print(df)

# Create table - index will be named 'id' and used as PK
users = pa.TableDataFrame('users', df, 'id', engine)

print("\n   After creating TableDataFrame:")
print(f"   Index name: {users._data.index.name}")
print(f"   ✓ Index automatically named 'id'")
print(users.to_pandas())

users.push()
print("\n   ✓ Table created in database")

# %% [markdown]
# ### 2. Index vs Column-Based PKs

# %%
# Index-based (no 'id' column)

# %% [markdown]
# ### \n   a) Index-based PK:

# %%
df_index = pd.DataFrame({
    'product': ['Widget', 'Gadget'],
    'price': [9.99, 19.99]
}, index=[100, 101])

products = pa.TableDataFrame('products', df_index, 'product_id', engine)
print(f"      Index name: {products._data.index.name}")
print(f"      Columns: {list(products._data.columns)}")
print("      ✓ 'product_id' is the index, not a column")

# Column-based (has 'id' column)

# %% [markdown]
# ### \n   b) Column-based PK:

# %%
df_column = pd.DataFrame({
    'id': [1, 2],
    'category': ['Electronics', 'Books'],
    'description': ['Tech items', 'Reading material']
})

categories = pa.TableDataFrame('categories', df_column, 'id', engine)
print(f"      Index name: {categories._data.index.name}")
print(f"      Columns: {list(categories._data.columns)}")
print("      ✓ 'id' was a column, moved to index")

products.push()
categories.push()

# %% [markdown]
# ### 3. Auto-Increment with Index-Based PK

# %%
# Create table with index-based PK and auto-increment
posts_df = pd.DataFrame({
    'title': ['First Post', 'Second Post'],
    'content': ['Content 1', 'Content 2']
}, index=[1, 2])

posts = pa.TableDataFrame('posts', posts_df, 'post_id', engine, 
                           auto_increment=True)
posts.push()

print("\n   Created with index-based PK + auto-increment:")
print(f"   Index name: {posts._data.index.name}")
print(posts.to_pandas())

# Add without specifying ID
print("\n   Adding post without ID:")
posts.add_row({
    'title': 'Third Post',
    'content': 'Auto-generated ID'
}, auto_increment=True)

posts.push()
print(f"   ✓ New post ID: {posts._data.index.max()}")
print(posts.to_pandas())

# %% [markdown]
# ### 4. Named vs Unnamed Index

# %%
# Unnamed index

# %% [markdown]
# ### \n   a) Unnamed index:

# %%
df1 = pd.DataFrame({'value': [10, 20]}, index=[1, 2])
print(f"      Index name before: {df1.index.name}")

t1 = pa.TableDataFrame('test1', df1, 'id', engine)
print(f"      Index name after: {t1._data.index.name}")
print("      ✓ Index automatically named 'id'")

# Already named index (matching)

# %% [markdown]
# ### \n   b) Index already named correctly:

# %%
df2 = pd.DataFrame({'value': [30, 40]}, index=pd.Index([1, 2], name='id'))
print(f"      Index name before: {df2.index.name}")

t2 = pa.TableDataFrame('test2', df2, 'id', engine)
print(f"      Index name after: {t2._data.index.name}")
print("      ✓ Index name preserved")

# %% [markdown]
# ### 5. Error Handling: Index Name Mismatch

# %%
# Index has different name than primary_key parameter

# %% [markdown]
# ### \n   a) Index name doesn't match primary_key:

# %%
df_mismatch = pd.DataFrame({'value': [50, 60]})
df_mismatch.index.name = 'user_id'

try:
    t3 = pa.TableDataFrame('test3', df_mismatch, 'id', engine)
    print("      ❌ Should have raised ValueError")
except ValueError as e:
    print(f"      ✓ Correctly raised ValueError")
    print(f"      Message: {str(e)[:70]}...")

# %% [markdown]
# ### 6. Error Handling: Ambiguous Primary Key

# %%
# PK exists as both column and index name
print("\n   PK name exists in both column and index:")
df_ambiguous = pd.DataFrame({
    'id': [1, 2, 3],
    'value': [100, 200, 300]
})
df_ambiguous.index.name = 'id'  # Now 'id' is both!

try:
    t4 = pa.TableDataFrame('test4', df_ambiguous, 'id', engine)
    print("      ❌ Should have raised ValueError")
except ValueError as e:
    print(f"      ✓ Correctly raised ValueError")
    print(f"      Message: {str(e)[:70]}...")

# %% [markdown]
# ### 7. Real-World Example: Time Series Data

# %%
# Create time series with timestamp as index
dates = pd.date_range('2024-01-01', periods=5, freq='D')
ts_df = pd.DataFrame({
    'temperature': [72, 75, 73, 71, 74],
    'humidity': [65, 68, 70, 67, 66]
}, index=dates)

print("\n   Time series DataFrame:")
print(f"   Index type: {type(ts_df.index).__name__}")
print(f"   Index name: {ts_df.index.name}")
print(ts_df)

# Need to convert DatetimeIndex to something SQL-friendly
ts_df_sql = ts_df.copy()
ts_df_sql.index = ts_df_sql.index.astype(str)

weather = pa.TableDataFrame('weather', ts_df_sql, 'date', engine)
weather.push()

print(f"\n   ✓ Created weather table")
print(f"   Index name: {weather._data.index.name}")
print(weather.to_pandas())

# %% [markdown]
# ### 8. Composite Keys with Index

# %%
# Create MultiIndex DataFrame
arrays = [
    ['Server1', 'Server1', 'Server2', 'Server2'],
    ['CPU', 'Memory', 'CPU', 'Memory']
]
multi_index = pd.MultiIndex.from_arrays(arrays)

metrics_df = pd.DataFrame({
    'value': [45.2, 62.1, 78.3, 45.8],
    'timestamp': ['10:00', '10:00', '10:00', '10:00']
}, index=multi_index)

print("\n   MultiIndex DataFrame:")
print(f"   Index names: {metrics_df.index.names}")
print(metrics_df)

# Name the MultiIndex levels
metrics = pa.TableDataFrame('metrics', metrics_df, 
                              ['server_id', 'metric_name'], engine)

print(f"\n   ✓ Created with composite PK")
print(f"   Index names: {metrics._data.index.names}")
print(metrics.to_pandas())

metrics.push()

# %% [markdown]
# ### 9. Migration Pattern: Column to Index

# %%
# Start with column-based

# %% [markdown]
# ### \n   a) Original (column-based):

# %%
old_df = pd.DataFrame({
    'employee_id': [100, 101, 102],
    'name': ['Alice', 'Bob', 'Charlie'],
    'department': ['Eng', 'Sales', 'Eng']
})
print(old_df)

# Convert to index-based

# %% [markdown]
# ### \n   b) Converted (index-based):

# %%
new_df = old_df.set_index('employee_id')
print(f"   Index name: {new_df.index.name}")
print(new_df)

employees = pa.TableDataFrame('employees', new_df, 'employee_id', engine)
employees.push()

print("\n   ✓ Now employee_id is the index")
print(employees.to_pandas())

# %% [markdown]
# ### 10. Best Practices

# %%
print("\n   ✓ Use index-based PKs when:")
print("      • Data naturally has a unique identifier")
print("      • You want to leverage pandas indexing features")
print("      • Working with time series (date/time index)")
print("      • Migrating from pure pandas code")

print("\n   ✓ Use column-based PKs when:")
print("      • PK is just another attribute")
print("      • You need PK in computations")
print("      • Interfacing with legacy code")

print("\n   ✓ Remember:")
print("      • Unnamed index → named with primary_key parameter")
print("      • Named index → must match primary_key or be unnamed")
print("      • Can't have PK in both index and columns")

print("\n" + "=" * 70)
print("Example Complete!")
print("Key Takeaways:")
print("  • DataFrame index can be used as primary key")
print("  • Unnamed indexes are automatically named")
print("  • Index names must match primary_key parameter")
print("  • Prevents ambiguity (PK can't be both column and index)")
print("  • Works with auto-increment and composite keys")
