# %% [markdown]
# # E-Commerce System
#
# This notebook demonstrates a complete e-commerce workflow with multi-table transactions

# %%
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import pandalchemy as pa

# %% [markdown]
# ## Setup Database

# %%
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# %% [markdown]
# ### Create Products Table

# %%
products_data = pd.DataFrame({
    'product_id': [1, 2, 3, 4, 5],
    'name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam'],
    'category': ['Computer', 'Accessory', 'Accessory', 'Display', 'Accessory'],
    'price': [999.99, 29.99, 79.99, 299.99, 89.99],
    'stock': [50, 200, 150, 75, 100]
})

products = pa.TableDataFrame('products', products_data, 'product_id', engine)
products.push()
db.db['products'] = products

print("✓ Created products table")
products.to_pandas()

# %% [markdown]
# ### Create Customers Table

# %%
customers_data = pd.DataFrame({
    'customer_id': [101, 102, 103],
    'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown'],
    'email': ['alice@email.com', 'bob@email.com', 'charlie@email.com'],
    'total_spent': [0.0, 0.0, 0.0]
})

customers = pa.TableDataFrame('customers', customers_data, 'customer_id', engine)
customers.push()
db.db['customers'] = customers

print("✓ Created customers table")

# %% [markdown]
# ## Browse Product Catalog

# %%
print("Available products:")
db['products'].to_pandas()[['name', 'category', 'price', 'stock']]

# %% [markdown]
# ## Process New Order

# %%
customer_id = 101
cart = [
    {'product_id': 1, 'quantity': 1},  # Laptop
    {'product_id': 2, 'quantity': 2},  # Mouse x2
    {'product_id': 3, 'quantity': 1},  # Keyboard
]

print(f"Customer {customer_id} shopping cart:")
order_total = 0
for item in cart:
    product = db['products'].get_row(item['product_id'])
    item_total = product['price'] * item['quantity']
    order_total += item_total
    print(f"  {product['name']} x{item['quantity']} @ ${product['price']:.2f} = ${item_total:.2f}")

print(f"\nOrder total: ${order_total:.2f}")

# %% [markdown]
# ### Update inventory

# %%
for item in cart:
    product_id = item['product_id']
    quantity = item['quantity']
    
    product = db['products'].get_row(product_id)
    new_stock = product['stock'] - quantity
    
    db['products'].update_row(product_id, {'stock': new_stock})
    
    print(f"✓ Updated {product['name']}: stock {product['stock']} → {new_stock}")

# %% [markdown]
# ### Update customer total

# %%
customer = db['customers'].get_row(customer_id)
new_total = customer['total_spent'] + order_total

db['customers'].update_row(customer_id, {
    'total_spent': new_total
})

print(f"✓ Customer total: ${customer['total_spent']:.2f} → ${new_total:.2f}")

# %% [markdown]
# ### Commit all changes in one transaction

# %%
db.push()
print("✓ Order processed successfully (all tables updated atomically)")

# %% [markdown]
# ## View Updated State

# %%
db.pull()

print("Updated inventory:")
db['products'].to_pandas()[['name', 'stock']]

# %%
print("Customer analytics:")
db['customers'].to_pandas()[['name', 'total_spent']].sort_values('total_spent', ascending=False)

# %% [markdown]
# ## Summary
#
# **Key Takeaways:**
# - Multi-table transactions with `db.push()`
# - All changes committed atomically (all-or-nothing)
# - Inventory management with stock tracking
# - Customer analytics and lifetime value
# - Real-world business logic implementation
