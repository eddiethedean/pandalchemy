# %% [markdown]
# # Pandas Integration
#
# This notebook demonstrates full pandas API compatibility:
# - Using pandas DataFrame operations
# - Filtering and selection
# - Grouping and aggregation
# - All operations are tracked automatically

# %%
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pandalchemy as pa

# %% [markdown]
# ## Setup

# %%
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# Create sample sales data
np.random.seed(42)
sales_data = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=50, freq='D').astype(str),
    'product': np.random.choice(['Widget', 'Gadget', 'Doohickey'], 50),
    'region': np.random.choice(['North', 'South', 'East', 'West'], 50),
    'quantity': np.random.randint(1, 20, 50),
    'unit_price': np.random.choice([9.99, 19.99, 29.99], 50)
})
sales_data.index = range(1, 51)

sales = db.create_table('sales', sales_data, primary_key='id')

print("Sample data (first 5 rows):")
sales._data.head()

# %% [markdown]
# ## Column Operations

# %% [markdown]
# ### Add calculated column

# %%
# Add total column using proper schema change
sales.add_column_with_default('total', 0.0)
sales.push()
sales.pull()

# Calculate total values
sales._data['total'] = sales._data['quantity'] * sales._data['unit_price']
sales.push()

print("✓ Added total = quantity × unit_price")
sales._data[['quantity', 'unit_price', 'total']].head()

# %% [markdown]
# ### Apply function to categorize

# %%
# Add price_tier column
sales.add_column_with_default('price_tier', 'Medium')
sales.push()
sales.pull()

sales._data['price_tier'] = sales._data['unit_price'].apply(
    lambda x: 'Low' if x < 15 else 'Medium' if x < 25 else 'High'
)
sales.push()

print("✓ Categorized prices into tiers")
sales._data[['unit_price', 'price_tier']].head()

# %% [markdown]
# ## Filtering and Selection

# %% [markdown]
# ### Filter by condition

# %%
high_quantity = sales._data[sales._data['quantity'] > 10]
print(f"Sales with quantity > 10: {len(high_quantity)}")
high_quantity[['product', 'quantity', 'total']].head()

# %% [markdown]
# ### Multiple conditions

# %%
complex_filter = (sales._data['product'] == 'Widget') & (sales._data['region'] == 'North')
filtered = sales._data[complex_filter]
print(f"Widgets sold in North: {len(filtered)}")
filtered[['product', 'region', 'quantity']].head()

# %% [markdown]
# ### Use isin for multiple values

# %%
selected = sales._data[sales._data['product'].isin(['Widget', 'Gadget'])]
print(f"Widget or Gadget sales: {len(selected)}")

# %% [markdown]
# ## Grouping and Aggregation

# %% [markdown]
# ### Group by product

# %%
by_product = sales._data.groupby('product')['total'].sum()
print("Total sales by product:")
by_product

# %% [markdown]
# ### Group by multiple columns

# %%
by_product_region = sales._data.groupby(['product', 'region'])['total'].agg(['sum', 'mean', 'count'])
print("Sales by product and region:")
by_product_region.head(10)

# %% [markdown]
# ### Multiple statistics

# %%
stats = sales._data.groupby('region').agg({
    'total': ['sum', 'mean', 'count'],
    'quantity': ['sum', 'mean']
})
print("Statistics by region:")
stats

# %% [markdown]
# ## Sorting

# %% [markdown]
# ### Top sales

# %%
top_sales = sales._data.nlargest(5, 'total')
print("Top 5 sales by total:")
top_sales[['product', 'region', 'quantity', 'total']]

# %% [markdown]
# ### Sort by multiple columns

# %%
sorted_sales = sales._data.sort_values(['product', 'total'], ascending=[True, False])
print("Sorted by product then total (desc):")
sorted_sales[['product', 'total']].head(10)

# %% [markdown]
# ## Data Transformation

# %% [markdown]
# ### Use assign() to add columns

# %%
sales._data = sales._data.assign(
    discount=0.1,
    final_price=lambda x: x['total'] * 0.9
)
print("✓ Added discount and final_price columns")
sales._data[['total', 'discount', 'final_price']].head()

# %% [markdown]
# ### Transform by groups

# %%
sales._data['pct_of_product_total'] = sales._data.groupby('product')['total'].transform(
    lambda x: x / x.sum() * 100
)
print("✓ Calculated percentage of product total")
sales._data[['product', 'total', 'pct_of_product_total']].head()

# %%
sales.push()

# %% [markdown]
# ## Summary
#
# **Key Takeaways:**
# - Full pandas DataFrame API is available
# - All operations are automatically tracked
# - Use `.loc[]`, `.iloc[]`, `.at[]`, `.iat[]` for indexing
# - `groupby()`, `merge()`, `pivot_table()` all work
# - String and datetime operations supported
# - Changes tracked regardless of operation type
