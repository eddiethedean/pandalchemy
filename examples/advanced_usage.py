"""
Advanced usage example for pandalchemy 0.2.0

This example demonstrates advanced features including:
- Complex DataFrame operations with tracking
- Error handling and transaction rollback
- Working with execution plans
- Performance optimization techniques
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

import pandalchemy as pa


def demo_complex_operations():
    """Demonstrate complex DataFrame operations with change tracking."""
    print("\n" + "=" * 60)
    print("DEMO 1: Complex DataFrame Operations")
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

    # 1. Add calculated column
    print("  1. Adding 'total' column (quantity * price)")
    db['sales']['total'] = db['sales']['quantity'] * db['sales']['price']

    # 2. Add discount column based on conditions
    print("  2. Adding 'discount' based on quantity")
    db['sales']['discount'] = 0.0
    db['sales'].loc[db['sales']['quantity'] > 10, 'discount'] = 0.1
    db['sales'].loc[db['sales']['quantity'] > 15, 'discount'] = 0.15

    # 3. Calculate final price
    print("  3. Adding 'final_price' with discount applied")
    db['sales']['final_price'] = (
        db['sales']['total'] * (1 - db['sales']['discount'])
    ).round(2)

    # 4. Add category based on price
    print("  4. Adding 'category' based on price range")
    db['sales']['category'] = 'Standard'
    db['sales'].loc[db['sales']['final_price'] > 500, 'category'] = 'Premium'
    db['sales'].loc[db['sales']['final_price'] > 1000, 'category'] = 'Luxury'

    # Check changes before push
    summary = db['sales'].get_changes_summary()
    print(f"\nChanges tracked: {summary['columns_added']} columns added")
    print(f"Operations recorded: {summary['total_operations']}")

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
    print("DEMO 2: Execution Plan Inspection")
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
    print("  - Adding 'doubled' column")
    db['demo']['doubled'] = db['demo']['value'] * 2

    print("  - Updating row 1")
    db['demo'].loc[1, 'name'] = 'Alicia'

    print("  - Dropping 'value' column")
    db['demo'].drop('value', axis=1, inplace=True)

    print("  - Renaming 'doubled' to 'score'")
    db['demo'].rename(columns={'doubled': 'score'}, inplace=True)

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
    print("DEMO 3: Error Handling and Rollback")
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
    db['accounts'].loc[1, 'balance'] = 150
    db['accounts']['status'] = 'active'

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
    print("DEMO 4: Batch Optimization")
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
    db['large_table']['processed'] = True

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
    print("DEMO 5: Multi-Table Transactions")
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
    db['users']['verified'] = True

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

