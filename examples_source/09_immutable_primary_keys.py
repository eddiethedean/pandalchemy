# %% [markdown]
# # 09_immutable_primary_keys
# 
# Immutable Primary Keys
# 
# This example demonstrates primary key immutability:
# - Why primary keys can't be modified
# - How to handle PK changes
# - Working with PK as index
# - Best practices

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa
from pandalchemy.exceptions import DataValidationError

# %%
# Setup
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# %%
print("Immutable Primary Keys Example")

# %%
# Create sample data
users_data = pd.DataFrame({
    'username': ['alice', 'bob', 'charlie'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
    'status': ['active', 'active', 'inactive']
}, index=[1, 2, 3])

users = db.create_table('users', users_data, primary_key='id')

# %% [markdown]
# ### 1. Primary Key as Index

# %%
print("\n   Table structure:")
print(users.to_pandas())
print(f"\n   Index (Primary Key): {users._data.index.name}")
print(f"   Index values: {list(users._data.index)}")
print(f"   Columns: {list(users._data.columns)}")
print("\n   ✓ Primary key is stored as DataFrame index")

# %% [markdown]
# ### 2. Why Primary Keys Are Immutable

# %%
print("\n   Primary keys identify rows uniquely:")
print("   • Changing PK would break referential integrity")
print("   • Foreign keys would point to wrong rows")
print("   • Database constraints enforce this")
print("   • Pandas index provides natural immutability")

# %% [markdown]
# ### 3. Attempting to Modify Primary Key

# %%
# Try to update PK through update_row

# %% [markdown]
# ### \n   a) Try to update PK via update_row:

# %%
try:
    users.update_row(1, {'id': 999})
    print("      ❌ Should have raised error")
except DataValidationError as e:
    print(f"      ✓ Prevented: {str(e)[:60]}...")

# Try to update PK in update dict

# %% [markdown]
# ### \n   b) Try to include PK in updates:

# %%
try:
    users.update_row(2, {'username': 'robert', 'id': 888})
    print("      ❌ Should have raised error")
except DataValidationError as e:
    print(f"      ✓ Prevented: PK cannot be updated")

# Try to modify index directly

# %% [markdown]
# ### \n   c) Try to modify index directly:

# %%
try:
    users._data.index = [10, 20, 30]
    users.push()
    print("      ❌ Should have raised error")
except Exception as e:
    print(f"      ✓ Prevented: {type(e).__name__}")
finally:
    users.pull()  # Reset

# %% [markdown]
# ### 4. How to 

# %%
print("\n   The correct approach: Delete old row + Insert new row")
print()
print("   Step 1: Get the row data")
old_row = users.get_row(3)
print(f"   Old row: {old_row}")

print("\n   Step 2: Delete the old row")
users.delete_row(3)
print("   ✓ Deleted row with ID=3")

print("\n   Step 3: Insert new row with new ID")
new_row = old_row.copy()
new_row['id'] = 30  # New ID
users.add_row(new_row)
print("   ✓ Inserted row with ID=30")

users.push()
print("\n   Result:")
print(users.to_pandas())
print("   ✓ Effectively 'changed' PK from 3 to 30")

# %% [markdown]
# ### 5. Composite Primary Keys

# %%
# Create table with composite PK
enrollments_data = pd.DataFrame({
    'student_id': [101, 101, 102],
    'course_id': ['CS101', 'MATH200', 'CS101'],
    'grade': ['A', 'B+', 'A-'],
    'semester': ['Fall2024', 'Fall2024', 'Fall2024']
})

enrollments = db.create_table('enrollments', enrollments_data,
                               primary_key=['student_id', 'course_id'])

print("\n   Composite PK table:")
print(enrollments.to_pandas())
print(f"\n   Index names: {enrollments._data.index.names}")
print("   ✓ Both student_id and course_id are in index")

# Try to update composite PK
print("\n   Try to update composite PK:")
try:
    enrollments.update_row((101, 'CS101'), {
        'student_id': 999,
        'grade': 'A+'
    })
    print("      ❌ Should have raised error")
except DataValidationError as e:
    print(f"      ✓ Prevented: Cannot update PK components")

# Correct way for composite PK
print("\n   Correct way: Delete + Insert")
old_enrollment = enrollments.get_row((102, 'CS101'))
enrollments.delete_row((102, 'CS101'))
enrollments.add_row({
    'student_id': 103,  # New student
    'course_id': 'CS101',
    'grade': old_enrollment['grade'],
    'semester': old_enrollment['semester']
})
enrollments.push()

print("   ✓ Changed student from 102 to 103")
print(enrollments.to_pandas())

# %% [markdown]
# ### 6. Working Safely with Primary Keys

# %%
print("\n   ✓ Safe operations (don't modify PK):")
print()
print("   a) Update non-PK columns:")
users.update_row(1, {'username': 'alice_updated', 'status': 'active'})
print("      users.update_row(1, {'username': 'alice_updated'})")

# %% [markdown]
# ### \n   b) Access by PK:

# %%
user = users.get_row(1)
print(f"      user = users.get_row(1)")
print(f"      Result: {user}")

# %% [markdown]
# ### \n   c) Delete by PK:

# %%
print("      users.delete_row(30)")

# %% [markdown]
# ### \n   d) Insert with explicit PK:

# %%
print("      users.add_row({'id': 4, 'username': 'diana', ...})")

users.push()

# %% [markdown]
# ### 7. Primary Key in Calculations

# %%
# Create orders table
orders_data = pd.DataFrame({
    'customer_id': [1, 1, 2, 2],
    'amount': [99.99, 149.50, 299.99, 49.99],
    'status': ['paid', 'paid', 'paid', 'pending']
}, index=[1001, 1002, 1003, 1004])

orders = db.create_table('orders', orders_data, primary_key='order_id')

print("\n   Orders table:")
print(orders.to_pandas())

# Access PK in calculations

# %% [markdown]
# ### \n   a) Use index (PK) in filtering:

# %%
recent_orders = orders._data[orders._data.index > 1002]
print(f"      Orders > 1002: {list(recent_orders.index)}")

# %% [markdown]
# ### \n   b) Access PK values:

# %%
print(f"      All order IDs: {list(orders._data.index)}")

# %% [markdown]
# ### \n   c) Reset index to use PK in computation:

# %%
df_with_id = orders._data.reset_index()
print("      df_with_id = orders._data.reset_index()")
print(f"      Now 'order_id' is a column: {'order_id' in df_with_id.columns}")

# But don't push after reset_index!
print("\n      ⚠ Don't push after reset_index (PK structure changed)")

# %% [markdown]
# ### 8. Foreign Key Relationships

# %%
print("\n   Why PK immutability matters:")

print("\n   Products table:")
products_data = pd.DataFrame({
    'name': ['Widget', 'Gadget', 'Doohickey'],
    'price': [9.99, 19.99, 29.99]
}, index=[100, 101, 102])

products = db.create_table('products', products_data, primary_key='product_id')
print(products.to_pandas())

print("\n   Order Items (references products):")
items_data = pd.DataFrame({
    'order_id': [1001, 1001, 1002],
    'product_id': [100, 101, 102],  # Foreign keys
    'quantity': [2, 1, 3]
}, index=[1, 2, 3])

items = db.create_table('order_items', items_data, primary_key='item_id')
print(items.to_pandas())

print("\n   If we could change product_id=100 to product_id=999:")
print("   • Order items would reference wrong product")
print("   • Data integrity would be broken")
print("   • Hence: PRIMARY KEYs are IMMUTABLE")

# %% [markdown]
# ### 9. Auto-Increment and Immutability

# %%
posts_data = pd.DataFrame({
    'title': ['Post 1', 'Post 2'],
    'content': ['Content 1', 'Content 2']
}, index=[1, 2])

posts = pa.TableDataFrame('posts', posts_data, 'post_id', engine,
                           auto_increment=True)
posts.push()

print("\n   Posts with auto-increment:")
print(posts.to_pandas())

# Add new post
posts.add_row({'title': 'Post 3', 'content': 'Content 3'}, 
              auto_increment=True)
posts.push()

print(f"\n   New post ID: {posts._data.index.max()}")
print("   ✓ Auto-generated IDs are also immutable")
print("   ✓ Can't change auto-generated ID after creation")

# %% [markdown]
# ### 10. Best Practices

# %%
print("\n   ✓ DO:")
print("      • Use surrogate keys (auto-increment) for mutable data")
print("      • Delete + Insert when you need to 'change' a PK")
print("      • Keep PK as index (natural in pandalchemy)")
print("      • Use PK for lookups: table.get_row(pk)")

print("\n   ❌ DON'T:")
print("      • Try to update PK values")
print("      • Use mutable data as natural keys")
print("      • Reset index then push (breaks PK structure)")
print("      • Expect PK to be in columns (it's in index)")

print("\n" + "=" * 70)
print("Example Complete!")
print("Key Takeaways:")
print("  • Primary keys are stored as DataFrame index")
print("  • Primary keys cannot be modified (immutable)")
print("  • To 'change' PK: delete old row + insert new row")
print("  • PK immutability protects referential integrity")
print("  • Works with single and composite keys")
