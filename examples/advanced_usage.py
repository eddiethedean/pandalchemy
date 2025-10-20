"""
Advanced usage example for pandalchemy 0.2.0

This example demonstrates advanced features including:
- Complex DataFrame operations with tracking
- Composite primary keys and relationships
- Auto-increment and PK management
- Error handling and transaction rollback
- Working with execution plans
- Performance optimization techniques
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import pandalchemy as pa


def demo_composite_keys_relationships():
    """Demonstrate composite primary keys and many-to-many relationships."""
    print("\n" + "=" * 60)
    print("DEMO 1: Composite Keys & Relationships")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    # Create users table
    users = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com']
    })
    db.create_table('users', users, primary_key='id')
    print("✓ Created users table")

    # Create organizations table
    orgs = pd.DataFrame({
        'id': ['org1', 'org2', 'org3'],
        'name': ['Acme Corp', 'TechStart', 'DataCo']
    })
    db.create_table('organizations', orgs, primary_key='id')
    print("✓ Created organizations table")

    # Create membership table with composite PK
    memberships = pd.DataFrame({
        'user_id': [1, 1, 2, 2, 3],
        'org_id': ['org1', 'org2', 'org1', 'org3', 'org1'],
        'role': ['admin', 'member', 'member', 'admin', 'member'],
        'joined_date': pd.date_range('2024-01-01', periods=5)
    })
    db.create_table('memberships', memberships, 
                   primary_key=['user_id', 'org_id'])
    print("✓ Created memberships table with composite PK")
    print()

    print("Memberships (MultiIndex):")
    print(db['memberships'].to_pandas())
    print()

    # Add a new membership
    print("Adding new membership (user 3, org2):")
    db['memberships'].data.add_row({
        'user_id': 3,
        'org_id': 'org2',
        'role': 'member',
        'joined_date': pd.Timestamp.now()
    })

    # Update a membership (using tuple for composite PK)
    print("Promoting user 2 in org1 to admin:")
    db['memberships'].data.update_row(
        (2, 'org1'),  # Composite PK as tuple
        {'role': 'admin'}
    )

    # Remove a membership
    print("Removing user 1 from org2:")
    db['memberships'].data.delete_row((1, 'org2'))

    db['memberships'].push()
    print("\n✓ All relationship changes applied!")
    print("\nUpdated memberships:")
    print(db['memberships'].to_pandas())


def demo_auto_increment_strategies():
    """Demonstrate different auto-increment strategies."""
    print("\n" + "=" * 60)
    print("DEMO 2: Auto-Increment Strategies")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    # Strategy 1: Auto-increment from start
    print("\n1. Auto-increment from empty table:")
    posts = pd.DataFrame({
        'id': [],
        'title': [],
        'content': []
    })
    posts_table = pa.Table('posts', posts, 'id', engine, auto_increment=True)

    # Add posts without specifying ID
    for i, title in enumerate(['First', 'Second', 'Third'], 1):
        posts_table.data.add_row({
            'title': title,
            'content': f'Content for post {i}'
        }, auto_increment=True)
        print(f"   Added '{title}' - ID auto-generated")

    posts_table.push()
    print("   ✓ Auto-increment from empty table worked!")

    # Strategy 2: Auto-increment with existing data
    print("\n2. Auto-increment with existing max ID:")
    comments = pd.DataFrame({
        'id': [10, 20, 30],  # Non-sequential IDs
        'post_id': [1, 1, 2],
        'text': ['Great!', 'Nice', 'Thanks']
    })
    comments_table = pa.Table('comments', comments, 'id', engine, auto_increment=True)

    # New comment gets next ID based on max
    comments_table.data.add_row({
        'post_id': 3,
        'text': 'Auto ID after gap'
    }, auto_increment=True)

    print("   Auto-generated ID after 30 (should be 31)")
    comments_table.push()

    print("\n✓ Auto-increment strategies demonstrated!")


def demo_immutable_pk_patterns():
    """Demonstrate patterns for working with immutable primary keys."""
    print("\n" + "=" * 60)
    print("DEMO 3: Immutable PK Patterns")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    users = pd.DataFrame({
        'id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie'],
        'status': ['active', 'active', 'inactive']
    })
    db.create_table('users', users, primary_key='id')

    print("Initial users:")
    print(db['users'].to_pandas())
    print()

    # Pattern 1: Update non-PK columns (works normally)
    print("Pattern 1: Update non-PK columns")
    db['users'].data.update_row(1, {'status': 'inactive'})
    print("   ✓ Updated user 1 status")
    print()

    # Pattern 2: Cannot update PK directly
    print("Pattern 2: Attempting to update PK (should fail)")
    try:
        db['users'].data.update_row(1, {'id': 999})
        print("   ❌ Should not reach here")
    except Exception as e:
        print(f"   ✓ Correctly prevented: {type(e).__name__}")
    print()

    # Pattern 3: "Change" PK via delete + insert
    print("Pattern 3: 'Migrate' user 2 to new ID (delete + insert)")
    old_user = db['users'].data.get_row(2)
    db['users'].data.delete_row(2)
    db['users'].data.add_row({
        'id': 200,
        'username': old_user['username'],
        'status': old_user['status']
    })
    print("   ✓ User 2 'migrated' to ID 200")
    print()

    # Pattern 4: Upsert (update if exists, insert if not)
    print("Pattern 4: Upsert operation")
    db['users'].data.upsert_row({
        'id': 3,  # Exists - will update
        'username': 'charlie',
        'status': 'active'
    })
    db['users'].data.upsert_row({
        'id': 4,  # New - will insert
        'username': 'dave',
        'status': 'active'
    })
    print("   ✓ Upserted existing and new users")
    print()

    db['users'].push()
    print("Final users:")
    print(db['users'].to_pandas())


def demo_complex_operations():
    """Demonstrate complex DataFrame operations with tracking."""
    print("\n" + "=" * 60)
    print("DEMO 4: Complex DataFrame Operations")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    # Create a sales table
    sales_data = pd.DataFrame({
        'id': range(1, 101),
        'product': np.random.choice(['Widget', 'Gadget', 'Doohickey'], 100),
        'quantity': np.random.randint(1, 20, 100),
        'price': np.random.uniform(10, 100, 100).round(2),
        'date': pd.date_range('2024-01-01', periods=100, freq='D')
    })

    db.create_table('sales', sales_data, primary_key='id')
    print(f"Created sales table with {len(db['sales'])} records")

    # Perform complex operations
    print("\nPerforming complex operations:")

    # 1. Add calculated columns using helper methods
    print("  1. Adding calculated columns")
    db['sales'].data.add_column_with_default('total', 0.0)
    db['sales']['total'] = db['sales']['quantity'] * db['sales']['price']

    # 2. Add discount column based on conditions
    print("  2. Adding conditional discount")
    db['sales'].data.add_column_with_default('discount', 0.0)
    db['sales'].loc[db['sales']['quantity'] > 10, 'discount'] = 0.1
    db['sales'].loc[db['sales']['quantity'] > 15, 'discount'] = 0.15

    # 3. Calculate final price
    print("  3. Calculating final price with discount")
    db['sales']['final_price'] = (
        db['sales']['total'] * (1 - db['sales']['discount'])
    ).round(2)

    # 4. Add category based on price
    print("  4. Categorizing by price range")
    db['sales']['category'] = 'Standard'
    db['sales'].loc[db['sales']['final_price'] > 500, 'category'] = 'Premium'
    db['sales'].loc[db['sales']['final_price'] > 1000, 'category'] = 'Luxury'

    # Check changes before push
    summary = db['sales'].get_changes_summary()
    print(f"\nChanges tracked: {summary['columns_added']} columns added")

    # Push all changes
    print("\nPushing all changes...")
    db['sales'].push()
    print("✓ All complex operations applied successfully!")

    # Show sample results
    print("\nSample results:")
    print(db['sales'].head(5).to_pandas()[['product', 'quantity', 'price', 'final_price', 'category']])


def demo_execution_plan_inspection():
    """Demonstrate execution plan inspection and optimization."""
    print("\n" + "=" * 60)
    print("DEMO 5: Execution Plan Inspection")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    # Create initial data
    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'value': [10, 20, 30, 40, 50]
    })
    db.create_table('demo', data, primary_key='id')

    # Make various types of changes
    print("\nMaking changes:")
    print("  - Adding 'status' column")
    db['demo'].data.add_column_with_default('status', 'active')

    print("  - Updating row 1")
    db['demo'].data.update_row(1, {'name': 'Alicia', 'value': 15})

    print("  - Renaming 'value' to 'score'")
    db['demo'].data.rename_column_safe('value', 'score')

    # Build execution plan without executing
    from pandalchemy.execution_plan import ExecutionPlan

    tracker = db['demo'].data.get_tracker()
    current_df = db['demo'].to_pandas()
    plan = ExecutionPlan(tracker, current_df)

    print("\nExecution Plan:")
    print(plan)

    print("\nPlan Summary:")
    summary = plan.get_summary()
    for key, value in summary.items():
        print(f"  {key}: {value}")

    # Execute the plan
    print("\nExecuting plan...")
    db['demo'].push()
    print("✓ Plan executed successfully!")


def demo_error_handling():
    """Demonstrate error handling and transaction rollback."""
    print("\n" + "=" * 60)
    print("DEMO 6: Error Handling and Rollback")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    # Create a table
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'balance': [100, 200, 300]
    })
    db.create_table('accounts', data, primary_key='id')

    print("Initial state:")
    print(db['accounts'].to_pandas())

    # Make changes
    print("\nMaking changes:")
    db['accounts'].data.update_row(1, {'balance': 150})
    db['accounts'].data.add_column_with_default('status', 'active')

    try:
        print("Attempting to push changes...")
        db['accounts'].push()
        print("✓ Changes applied successfully!")

        print("\nFinal state:")
        print(db['accounts'].to_pandas())

    except Exception as e:
        print(f"✗ Error occurred: {e}")
        print("All changes were rolled back!")

        # Verify rollback by reloading
        db.pull()
        print("\nState after rollback:")
        print(db['accounts'].to_pandas())


def demo_batch_optimization():
    """Demonstrate batch optimization for large datasets."""
    print("\n" + "=" * 60)
    print("DEMO 7: Batch Optimization")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    # Create a large dataset
    n_rows = 1000
    print(f"\nCreating dataset with {n_rows} rows...")
    large_data = pd.DataFrame({
        'id': range(1, n_rows + 1),
        'category': np.random.choice(['A', 'B', 'C'], n_rows),
        'value': np.random.randint(1, 1000, n_rows),
        'active': np.random.choice([True, False], n_rows)
    })

    db.create_table('large_table', large_data, primary_key='id')
    print(f"✓ Table created with {len(db['large_table'])} rows")

    # Make bulk changes
    print("\nMaking bulk changes:")

    print("  1. Updating all 'B' category values")
    mask = db['large_table']['category'] == 'B'
    db['large_table'].loc[mask, 'value'] = db['large_table'].loc[mask, 'value'] * 1.1

    print("  2. Adding 'processed' column")
    db['large_table'].data.add_column_with_default('processed', True)

    print("  3. Deactivating low-value items")
    db['large_table'].loc[db['large_table']['value'] < 100, 'active'] = False

    # Check what will be done
    summary = db['large_table'].get_changes_summary()
    print("\nChanges to apply:")
    print(f"  Updates: {summary['updates']}")
    print(f"  Columns added: {summary['columns_added']}")

    # Push changes (all batched together)
    print("\nPushing changes (batched for efficiency)...")
    import time
    start = time.time()
    db['large_table'].push()
    elapsed = time.time() - start
    print(f"✓ Pushed {n_rows} row changes in {elapsed:.3f} seconds")


def demo_multi_table_transaction():
    """Demonstrate multi-table transactions."""
    print("\n" + "=" * 60)
    print("DEMO 8: Multi-Table Transactions")
    print("=" * 60)

    engine = create_engine('sqlite:///:memory:')
    db = pa.DataBase(engine)

    # Create related tables
    print("\nCreating related tables:")

    users = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com']
    })
    db.create_table('users', users, primary_key='id')
    print("  ✓ Created 'users' table")

    orders = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'user_id': [1, 1, 2, 2, 3],
        'amount': [100.0, 150.0, 200.0, 75.0, 300.0],
        'status': ['pending', 'completed', 'pending', 'completed', 'pending']
    })
    db.create_table('orders', orders, primary_key='id')
    print("  ✓ Created 'orders' table")

    # Make changes to both tables
    print("\nMaking changes to both tables:")

    print("  1. Adding 'verified' column to users")
    db['users'].data.add_column_with_default('verified', True)

    print("  2. Updating order statuses")
    db['orders'].loc[db['orders']['status'] == 'pending', 'status'] = 'processing'

    print("  3. Adding 'total_spent' to users")
    # Calculate total per user
    totals = db['orders'].to_pandas().groupby('user_id')['amount'].sum()
    db['users']['total_spent'] = db['users'].index.map(totals).fillna(0)

    # Push all changes in a single transaction
    print("\nPushing all changes in single transaction...")
    db.push()  # Commits both tables atomically
    print("✓ All tables updated successfully!")

    print("\nFinal state:")
    print("\nUsers:")
    print(db['users'].to_pandas())
    print("\nOrders:")
    print(db['orders'].to_pandas())


def main():
    """Run all demonstrations."""
    print("\n" + "=" * 60)
    print("PANDALCHEMY 0.2.0 - ADVANCED USAGE EXAMPLES")
    print("=" * 60)

    demo_composite_keys_relationships()
    demo_auto_increment_strategies()
    demo_immutable_pk_patterns()
    demo_complex_operations()
    demo_execution_plan_inspection()
    demo_error_handling()
    demo_batch_optimization()
    demo_multi_table_transaction()

    print("\n" + "=" * 60)
    print("ALL DEMONSTRATIONS COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print()


if __name__ == '__main__':
    main()
