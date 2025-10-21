# %% [markdown]
# # 05_conditional_operations
# 
# Conditional Updates and Deletes
# 
# This example demonstrates bulk conditional operations:
# - update_where() for conditional updates
# - delete_where() for conditional deletes
# - Working with pandas boolean masks
# - Bulk data transformations

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %%
# Setup
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# %%
print("Conditional Operations Example")

# %%
# Create sample employee data
employees_data = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry'],
    'department': ['Engineering', 'Sales', 'Engineering', 'Marketing', 
                   'Sales', 'Engineering', 'Marketing', 'Sales'],
    'salary': [80000, 60000, 75000, 70000, 62000, 85000, 72000, 58000],
    'years': [5, 3, 4, 2, 3, 6, 2, 1],
    'performance': ['excellent', 'good', 'excellent', 'good', 
                    'average', 'excellent', 'good', 'average']
}, index=range(1, 9))

employees = db.create_table('employees', employees_data, primary_key='id')

# %% [markdown]
# ### 1. Initial Employee Data

# %%
print(employees.to_pandas())

# %% [markdown]
# ### 2. update_where() - Conditional Updates

# %%
# Simple condition update

# %% [markdown]
# ### \n   a) Give 10% raise to excellent performers:

# %%
condition = employees._data['performance'] == 'excellent'
employees.update_where(condition, {
    'salary': lambda x: x * 1.10
})

print(f"✓ Updated {condition.sum()} employees")

# Multiple field update with dict

# %% [markdown]
# ### \n   b) Update Sales department info:

# %%
sales_condition = employees._data['department'] == 'Sales'
employees.update_where(sales_condition, {
    'department': 'Business Development'
})

print(f"      ✓ Renamed department for {sales_condition.sum()} employees")

employees.push()
print("\n   After updates:")
print(employees.to_pandas()[['name', 'department', 'salary', 'performance']])

# %% [markdown]
# ### 3. Complex Conditions

# %%
# Multiple conditions with &, |

# %% [markdown]
# ### a) Bonus for senior excellent performers

# %%
# First add bonus column with default
employees.add_column_with_default('bonus', 0)
employees.push()
employees.pull()

# Now update bonuses
senior_excellent = (employees._data['years'] >= 5) & \
                   (employees._data['performance'] == 'excellent')

employees.update_where(senior_excellent, {'bonus': 5000})

print(f"✓ {senior_excellent.sum()} employees get $5000 bonus")

# Salary-based condition

# %% [markdown]
# ### \n   b) Adjust low salaries:

# %%
low_salary = employees._data['salary'] < 65000
employees.update_where(low_salary, {
    'salary': lambda x: max(x * 1.05, 65000)
})

print(f"✓ Adjusted {low_salary.sum()} salaries to minimum $65,000")

employees.push()
print("\n   Current state:")
print(employees.to_pandas()[['name', 'salary', 'years', 'bonus']])

# %% [markdown]
# ### 4. delete_where() - Conditional Deletes

# %%
# Create temp employee data
temp_data = pd.DataFrame({
    'name': ['Temp1', 'Temp2', 'Temp3'],
    'department': ['Engineering', 'Marketing', 'Engineering'],
    'salary': [45000, 42000, 48000],
    'years': [0.5, 0.3, 0.8],
    'performance': ['average', 'average', 'good'],
    'bonus': [0, 0, 0]
}, index=[20, 21, 22])

# Add temp employees
for idx, row in temp_data.iterrows():
    row_dict = row.to_dict()
    row_dict['id'] = idx  # Add the index as id
    employees.add_row(row_dict)

employees.push()

# %% [markdown]
# ### \n   a) Added temporary employees:

# %%
print(f"      Total employees: {len(employees._data)}")

# Delete based on condition

# %% [markdown]
# ### \n   b) Remove employees with < 1 year experience:

# %%
deleted_count = employees.delete_where(employees._data['years'] < 1)

print(f"      ✓ Deleted {deleted_count} employees")

employees.push()
print(f"      Remaining employees: {len(employees._data)}")

# %% [markdown]
# ### 5. Combining Conditions with Boolean Logic

# %%
# AND condition

# %% [markdown]
# ### \n   a) Update high performers in Engineering:

# %%
condition_and = (employees._data['department'] == 'Engineering') & \
                (employees._data['performance'] == 'excellent')

employees.update_where(condition_and, {
    'bonus': lambda x: x + 3000
})
print(f"✓ {condition_and.sum()} engineers got extra $3000 bonus")

# OR condition

# %% [markdown]
# ### \n   b) Flag employees needing review:

# %%
employees._data['needs_review'] = False
condition_or = (employees._data['performance'] == 'average') | \
               (employees._data['years'] < 2)

employees.update_where(condition_or, {'needs_review': True})
print(f"      ✓ {condition_or.sum()} employees flagged for review")

# NOT condition

# %% [markdown]
# ### \n   c) Update everyone except Marketing:

# %%
not_marketing = ~(employees._data['department'] == 'Marketing')
employees.update_where(not_marketing, {
    'salary': lambda x: x + 1000  # $1000 across-the-board raise
})
print(f"✓ {not_marketing.sum()} employees got $1000 raise")

employees.push()

# %% [markdown]
# ### 6. Real-World Example: Customer Status Updates

# %%
customers_data = pd.DataFrame({
    'name': ['Acme Corp', 'TechStart', 'DataCo', 'SmallBiz', 'MegaCorp'],
    'revenue': [500000, 150000, 800000, 50000, 1200000],
    'signup_date': ['2023-01-15', '2023-06-20', '2022-12-01', 
                    '2024-01-10', '2022-08-15'],
    'status': ['active', 'active', 'active', 'trial', 'active'],
    'support_tier': ['standard', 'standard', 'standard', 'basic', 'standard']
}, index=range(1, 6))

customers = db.create_table('customers', customers_data, primary_key='id')

print("\n   Initial customers:")
print(customers.to_pandas())

# Upgrade high-value customers

# %% [markdown]
# ### \n   a) Upgrade high-revenue customers to premium:

# %%
high_value = customers._data['revenue'] > 500000
customers.update_where(high_value, {'support_tier': 'premium'})
print(f"      ✓ {high_value.sum()} customers upgraded")

# Convert trials to active

# %% [markdown]
# ### \n   b) Convert trial to paid after certain revenue:

# %%
trial_conversion = (customers._data['status'] == 'trial') & \
                   (customers._data['revenue'] > 40000)
customers.update_where(trial_conversion, {'status': 'active'})
print(f"      ✓ {trial_conversion.sum()} trials converted")

# Remove inactive small accounts

# %% [markdown]
# ### \n   c) Remove low-value inactive accounts:

# %%
to_remove = (customers._data['revenue'] < 100000) & \
            (customers._data['status'] != 'active')
removed = customers.delete_where(to_remove)
print(f"      ✓ {removed} accounts removed")

customers.push()
print("\n   Final customers:")
print(customers.to_pandas())

# %% [markdown]
# ### 7. Batch Updates with Calculations

# %%
# Price adjustment example
products_data = pd.DataFrame({
    'name': ['Widget', 'Gadget', 'Doohickey', 'Thingamajig', 'Whatsit'],
    'price': [9.99, 19.99, 29.99, 39.99, 14.99],
    'category': ['A', 'B', 'A', 'C', 'B'],
    'stock': [100, 50, 75, 25, 200]
}, index=range(1, 6))

products = db.create_table('products', products_data, primary_key='id')

print("\n   Initial products:")
print(products.to_pandas())

# Discount low-stock items

# %% [markdown]
# ### \n   a) 10% discount on low-stock items:

# %%
low_stock = products._data['stock'] < 50
products.update_where(low_stock, {
    'price': lambda x: round(x * 0.9, 2)
})

# Category-based pricing

# %% [markdown]
# ### \n   b) Premium pricing for category C:

# %%
cat_c = products._data['category'] == 'C'
products.update_where(cat_c, {
    'price': lambda x: round(x * 1.15, 2)
})

products.push()
print("\n   After pricing updates:")
print(products.to_pandas())

# %% [markdown]
# ### 8. Performance Tip: Single Update vs Loop

# %%
print("\n   ❌ SLOW - Loop with individual updates:")
print("      for idx in df[df['age'] > 65].index:")
print("          table.update_row(idx, {'senior': True})")
print()
print("   ✓ FAST - Single conditional update:")
print("      table.update_where(df['age'] > 65, {'senior': True})")
print()
print("   Difference: One SQL UPDATE vs many individual UPDATEs")

print("\n" + "=" * 70)
print("Example Complete!")
print("Key Takeaways:")
print("  • update_where() for bulk conditional updates")
print("  • delete_where() for bulk conditional deletes")
print("  • Use pandas boolean masks for conditions")
print("  • Combine with & (and), | (or), ~ (not)")
print("  • Much faster than looping with update_row()")
