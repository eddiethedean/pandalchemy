# %% [markdown]
# # Auto-Increment Primary Keys
#
# This notebook demonstrates auto-increment functionality:
# - Setting up auto-increment tables
# - Adding rows without specifying PKs
# - Getting next available PK value
# - Combining manual and auto-generated IDs

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %% [markdown]
# ## Setup

# %%
engine = create_engine('sqlite:///:memory:')

# %% [markdown]
# ## Creating Auto-Increment Table

# %%
initial_data = pd.DataFrame({
    'title': ['First Post', 'Second Post', 'Third Post'],
    'content': ['Content 1', 'Content 2', 'Content 3'],
    'author': ['Alice', 'Bob', 'Alice']
}, index=[1, 2, 3])

posts = pa.TableDataFrame('posts', initial_data, 'id', engine, auto_increment=True)
posts.push()

print("✓ Created posts table with auto_increment=True")
print(f"Current max ID: {posts._data.index.max()}")

posts.to_pandas()

# %% [markdown]
# ## Adding Rows with Auto-Increment

# %% [markdown]
# ### Add without specifying ID

# %%
posts.add_row({
    'title': 'Auto Post 1',
    'content': 'This ID was auto-generated',
    'author': 'Charlie'
}, auto_increment=True)

print("✓ Added (ID will be 4)")

# %% [markdown]
# ### Add multiple rows

# %%
posts.add_row({'title': 'Auto Post 2', 'content': 'Content', 'author': 'Diana'}, 
              auto_increment=True)
posts.add_row({'title': 'Auto Post 3', 'content': 'Content', 'author': 'Eve'}, 
              auto_increment=True)

posts.push()
print("✓ Added posts with IDs 5, 6")

posts.to_pandas()

# %% [markdown]
# ## Getting Next PK Value

# %%
next_id = posts.get_next_pk_value()
print(f"Next available ID: {next_id}")
print(f"If I add 3 more posts, they will have IDs: {next_id}, {next_id+1}, {next_id+2}")

# %% [markdown]
# ## Mixing Manual and Auto IDs

# %% [markdown]
# ### Add with manual ID

# %%
posts.add_row({
    'id': 100,
    'title': 'Manual ID Post',
    'content': 'I chose this ID',
    'author': 'Frank'
})
posts.push()

print("✓ Added with ID 100")

# %% [markdown]
# ### Next auto-increment continues from max

# %%
next_id_after = posts.get_next_pk_value()
print(f"Next ID after manual: {next_id_after}")

posts.add_row({'title': 'After Manual', 'content': 'Content', 'author': 'Grace'}, 
              auto_increment=True)
posts.push()

print(f"✓ New auto ID: {posts._data.index.max()}")
posts.to_pandas()

# %% [markdown]
# ## Bulk Insert with Auto-Increment

# %%
for i in range(5):
    posts.add_row({
        'title': f'Bulk Post {i+1}',
        'content': f'Bulk content {i+1}',
        'author': 'System'
    }, auto_increment=True)

posts.push()
print(f"✓ Added 5 posts, total: {len(posts._data)}")
print(f"ID range: {posts._data.index.min()} - {posts._data.index.max()}")

# %% [markdown]
# ## Real-World Example: Blog with Comments

# %%
db = pa.DataBase(engine)

# Comments table with auto-increment
comments_data = pd.DataFrame({
    'post_id': [1, 1, 2],
    'user': ['Bob', 'Charlie', 'Alice'],
    'comment': ['Great post!', 'Thanks for sharing', 'Interesting read']
}, index=[1, 2, 3])

comments = pa.TableDataFrame('comments', comments_data, 'id', engine, 
                              auto_increment=True)
comments.push()
db.db['comments'] = comments

print("✓ Created comments table")

# %%
# Add new comment with auto ID
comments.add_row({
    'post_id': 1,
    'user': 'Diana',
    'comment': 'Love this!'
}, auto_increment=True)

comments.push()

print("Comments on post 1:")
post_1_comments = comments._data[comments._data['post_id'] == 1]
post_1_comments[['user', 'comment']]

# %% [markdown]
# ## Custom ID Sequences
# Start from a specific ID

# %%
products_data = pd.DataFrame({
    'name': ['Widget', 'Gadget', 'Doohickey'],
    'price': [9.99, 19.99, 29.99]
}, index=[1000, 1001, 1002])  # Start from 1000

products = pa.TableDataFrame('products', products_data, 'id', engine, 
                              auto_increment=True)
products.push()

print("✓ Created products with IDs starting at 1000")
products.to_pandas()

# %%
# Auto-increment continues from max
products.add_row({'name': 'Thingamajig', 'price': 39.99}, auto_increment=True)
products.push()

print(f"New product ID: {products._data.index.max()}")

# %% [markdown]
# ## Error Handling

# %% [markdown]
# ### Try to add duplicate ID

# %%
try:
    posts.add_row({
        'id': 1,  # Already exists
        'title': 'Duplicate',
        'content': 'Should fail',
        'author': 'Test'
    })
    posts.push()
except Exception as e:
    print(f"✓ Correctly prevented: {type(e).__name__}")

# %% [markdown]
# ### Adding without auto_increment flag

# %%
try:
    posts.add_row({
        'title': 'No ID provided',
        'content': 'Missing ID',
        'author': 'Test'
    })  # Note: no auto_increment=True
    posts.push()
except Exception as e:
    print(f"✓ Correctly failed: {type(e).__name__}")

# %% [markdown]
# ## Auto-Increment Requirements
#
# Auto-increment requires:
# - ✓ Single-column primary key (not composite)
# - ✓ Integer-type primary key
# - ✓ `auto_increment=True` in TableDataFrame constructor
# - ✓ `auto_increment=True` in add_row() call
#
# ❌ Won't work with composite keys

# %% [markdown]
# ## Best Practices
#
# - ✓ Use auto-increment for user-generated content (posts, comments, orders)
# - ✓ Use manual IDs for reference data (countries, categories)
# - ✓ Check `get_next_pk_value()` when you need to know the next ID
# - ✓ Don't mix auto and manual IDs unless necessary
# - ✓ Always use `auto_increment=True` flag in add_row()

# %% [markdown]
# ## Summary
#
# **Key Takeaways:**
# - Auto-increment generates IDs automatically
# - Set `auto_increment=True` in TableDataFrame constructor
# - Use `auto_increment=True` in add_row() call
# - `get_next_pk_value()` returns next available ID
# - Can mix manual and auto IDs (auto continues from max)
# - Only works with single-column integer primary keys

