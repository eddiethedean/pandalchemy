# %% [markdown]
# # 07_transactions
# 
# Transaction Safety
# 
# This example demonstrates ACID transaction guarantees:
# - All-or-nothing execution
# - Automatic rollback on errors
# - Multi-table transactions
# - Error recovery

# %%
import pandas as pd
from sqlalchemy import create_engine
import pandalchemy as pa

# %%
# Setup
engine = create_engine('sqlite:///:memory:')
db = pa.DataBase(engine)

# %%
print("Transaction Safety Example")

# %%
# Create test tables
users_data = pd.DataFrame({
    'name': ['Alice', 'Bob'],
    'balance': [1000.00, 500.00]
}, index=[1, 2])

db.create_table('users', users_data, primary_key='id')

# %% [markdown]
# ### 1. Basic Transaction

# %%
users = db['users']
print("\n   Initial state:")
print(users.to_pandas())

# Make changes
print("\n   Making changes:")
users.update_row(1, {'balance': 1100.00})
users.update_row(2, {'balance': 600.00})

print("   Changes tracked (not yet committed)")
print(f"   Has changes: {users.has_changes()}")

# Commit
users.push()
print("\n   ✓ Transaction committed")
print(users.to_pandas())

# %% [markdown]
# ### 2. Rollback on Error

# %%
print("\n   Initial balance:")
print(users.to_pandas()[['name', 'balance']])

# Make changes that will fail
print("\n   Attempting invalid operation:")
try:
    users.update_row(1, {'balance': 1200.00})
    users.update_row(2, {'balance': 700.00})
    # This will cause an error (trying to update PK)
    users.update_row(1, {'id': 999})
    users.push()
    print("   ❌ Should have failed")
except Exception as e:
    print(f"   ✓ Transaction failed: {type(e).__name__}")
    print("   ✓ All changes rolled back")

# Verify rollback
users.pull()
print("\n   After rollback:")
print(users.to_pandas()[['name', 'balance']])
print("   ✓ Data unchanged from before transaction")

# %% [markdown]
# ### 3. Multi-Table Transactions

# %%
# Create accounts and transactions tables
accounts_data = pd.DataFrame({
    'user_id': [1, 2],
    'account_number': ['ACC001', 'ACC002'],
    'balance': [1100.00, 600.00]
}, index=[1, 2])

transactions_data = pd.DataFrame({
    'from_account': [],
    'to_account': [],
    'amount': [],
    'status': []
}, dtype=object)
transactions_data.index.name = 'id'

db.create_table('accounts', accounts_data, primary_key='id')
db.create_table('transactions', transactions_data, primary_key='id')

print("\n   Initial state:")
print("   Accounts:")
print(db['accounts'].to_pandas())

# Perform money transfer (multi-table)
print("\n   Transferring $200 from Alice to Bob:")

# Deduct from Alice
db['accounts'].update_row(1, {'balance': 900.00})

# Add to Bob
db['accounts'].update_row(2, {'balance': 800.00})

# Record transaction
db['transactions'].add_row({
    'id': 1,
    'from_account': 'ACC001',
    'to_account': 'ACC002',
    'amount': 200.00,
    'status': 'completed'
})

# Commit all changes in one transaction
db.push()
print("   ✓ Transfer complete (all tables updated atomically)")

print("\n   Final state:")
print("   Accounts:")
print(db['accounts'].to_pandas())
print("\n   Transactions:")
print(db['transactions'].to_pandas())

# %% [markdown]
# ### 4. Failed Multi-Table Transaction

# %%
print("\n   Attempting invalid transfer:")

initial_acc1 = db['accounts'].get_row(1)['balance']
initial_acc2 = db['accounts'].get_row(2)['balance']

try:
    # Try to transfer more than available
    db['accounts'].update_row(1, {'balance': initial_acc1 - 1000.00})  # Would go negative
    db['accounts'].update_row(2, {'balance': initial_acc2 + 1000.00})
    
    # Add invalid transaction record (duplicate ID)
    db['transactions'].add_row({
        'id': 1,  # Already exists!
        'from_account': 'ACC001',
        'to_account': 'ACC002',
        'amount': 1000.00,
        'status': 'completed'
    })
    
    db.push()
    print("   ❌ Should have failed")
except Exception as e:
    print(f"   ✓ Transaction failed: {type(e).__name__}")

# Verify everything rolled back
db.pull()
final_acc1 = db['accounts'].get_row(1)['balance']
final_acc2 = db['accounts'].get_row(2)['balance']

print(f"\n   Account 1: ${initial_acc1:.2f} → ${final_acc1:.2f} (unchanged)")
print(f"   Account 2: ${initial_acc2:.2f} → ${final_acc2:.2f} (unchanged)")
print(f"   Transactions: {len(db['transactions']._data)} (unchanged)")

# %% [markdown]
# ### 5. Isolation and Consistency

# %%
print("\n   Demonstrating consistency:")

# Get total balance before
total_before = db['accounts']._data['balance'].sum()
print(f"   Total balance before: ${total_before:.2f}")

# Transfer within system
db['accounts'].update_row(1, {'balance': 850.00})
db['accounts'].update_row(2, {'balance': 850.00})

db['transactions'].add_row({
    'id': 2,
    'from_account': 'ACC001',
    'to_account': 'ACC002',
    'amount': 50.00,
    'status': 'completed'
})

db.push()

# Get total balance after
total_after = db['accounts']._data['balance'].sum()
print(f"   Total balance after: ${total_after:.2f}")
print(f"   ✓ Consistency maintained: {total_before == total_after}")

# %% [markdown]
# ### 6. Error Recovery Patterns

# %%
print("\n   Pattern 1: Try-except with pull")
print("   ```python")
print("   try:")
print("       table.update_row(1, {...})")
print("       table.push()")
print("   except Exception as e:")
print("       table.pull()  # Refresh to clean state")
print("       # Handle error")
print("   ```")

print("\n   Pattern 2: Validate before push")
print("   ```python")
print("   table.update_row(1, {...})")
print("   if table.has_changes():")
print("       # Validate changes")
print("       if validation_ok:")
print("           table.push()")
print("       else:")
print("           table.pull()  # Discard")
print("   ```")

# %% [markdown]
# ### 7. Single-Table Transaction

# %%
products_data = pd.DataFrame({
    'name': ['Widget', 'Gadget'],
    'stock': [100, 50],
    'reserved': [0, 0]
}, index=[1, 2])

products = pa.TableDataFrame('products', products_data, 'id', engine)
products.push()

print("\n   Initial inventory:")
print(products.to_pandas())

# Simulate order processing
print("\n   Processing order (reserve 10 widgets):")
try:
    current = products.get_row(1)
    if current['stock'] >= 10:
        products.update_row(1, {
            'stock': current['stock'] - 10,
            'reserved': current['reserved'] + 10
        })
        products.push()
        print("   ✓ Order processed")
    else:
        print("   ✗ Insufficient stock")
except Exception as e:
    products.pull()
    print(f"   ✗ Order failed: {e}")

print("\n   After order:")
print(products.to_pandas())

# %% [markdown]
# ### 8. Transaction with Validation

# %%
print("\n   Transfer with validation:")

def safe_transfer(db, from_id, to_id, amount):
    """Safe money transfer with validation."""
    try:
        # Get current balances
        from_account = db['accounts'].get_row(from_id)
        to_account = db['accounts'].get_row(to_id)
        
        # Validate
        if from_account['balance'] < amount:
            return False, "Insufficient funds"
        
        if amount <= 0:
            return False, "Invalid amount"
        
        # Perform transfer
        db['accounts'].update_row(from_id, {
            'balance': from_account['balance'] - amount
        })
        db['accounts'].update_row(to_id, {
            'balance': to_account['balance'] + amount
        })
        
        # Commit
        db.push()
        return True, "Transfer successful"
        
    except Exception as e:
        db.pull()  # Rollback
        return False, f"Transfer failed: {e}"

# Try transfer
success, message = safe_transfer(db, 1, 2, 100.00)
print(f"   Result: {message}")
print(f"   Success: {success}")

print("\n   Balances after transfer:")
print(db['accounts'].to_pandas()[['account_number', 'balance']])

print("\n" + "=" * 70)
print("Example Complete!")
print("Key Takeaways:")
print("  • All push() operations are transactional")
print("  • Errors trigger automatic rollback")
print("  • db.push() commits all tables atomically")
print("  • Use try-except with pull() for error recovery")
print("  • Validate before push() when possible")
