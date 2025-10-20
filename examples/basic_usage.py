"""
Basic usage example for pandalchemy 0.2.0

This example demonstrates the key features of pandalchemy including:
- Creating a database and tables
- Automatic change tracking
- Immutable primary keys (stored as index)
- CRUD operations
- Schema modifications
- Transaction-safe operations
"""

import pandas as pd
from sqlalchemy import create_engine

import pandalchemy as pa


def main():
    # Create an in-memory SQLite database for demonstration
    engine = create_engine('sqlite:///:memory:')

    print("=" * 70)
    print("Pandalchemy 0.2.0 - Basic Usage Example")
    print("=" * 70)
    print()

    # Initialize the database
    print("1. Creating Database...")
    db = pa.DataBase(engine)
    print(f"   Database created: {db}")
    print()

    # Create a sample table
    print("2. Creating 'users' table...")
    users_data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })

    users_table = db.create_table('users', users_data, primary_key='id')
    print(f"   Table created with {len(users_table)} rows")
    print()
    print("   Note: Primary key 'id' is now the DataFrame index!")
    print(users_table.to_pandas())
    print()

    # Demonstrate primary key as index
    print("3. Primary Key Features...")
    print("   ✓ Primary key is stored as DataFrame index")
    print(f"   ✓ Index name: {db['users'].data.index.name}")
    print(f"   ✓ Index values: {list(db['users'].data.index)}")
    print()

    # CRUD operations
    print("4. CRUD Operations...")
    
    # Add a row
    print("   - Adding a new user (id=4)")
    db['users'].data.add_row({
        'id': 4,
        'name': 'David',
        'age': 28
    })
    
    # Update a row (non-PK columns only)
    print("   - Updating user 1 (Alice) - age to 26")
    db['users'].data.update_row(1, {'age': 26, 'name': 'Alicia'})
    
    # Delete a row
    print("   - Deleting user 3 (Charlie)")
    db['users'].data.delete_row(3)
    
    print(f"   Current state: {len(db['users'])} users")
    print()

    # Demonstrate PK immutability
    print("5. Primary Key Immutability...")
    print("   Primary keys are immutable - cannot be changed!")
    print()
    try:
        db['users'].data.update_row(1, {'id': 999})
        print("   ❌ This should not happen")
    except Exception as e:
        print(f"   ✓ Correctly prevented: {type(e).__name__}")
        print(f"     '{str(e)[:60]}...'")
    print()
    print("   To 'change' a PK: delete old row, insert new row")
    print()

    # Make various changes
    print("6. Schema Modifications...")

    # Add a new column
    print("   - Adding 'email' column")
    db['users'].data.add_column_with_default('email', 'pending@example.com')
    
    # Update email values
    print("   - Updating email values")
    for idx in db['users'].data.index:
        row = db['users'].data.get_row(idx)
        email = f"{row['name'].lower()}@example.com"
        db['users'].data.update_row(idx, {'email': email})

    print()

    # Check what changes are tracked
    print("7. Checking tracked changes...")
    summary = db['users'].get_changes_summary()
    print("   Changes summary:")
    for key, value in summary.items():
        print(f"     {key}: {value}")
    print()

    # Push changes
    print("8. Pushing changes to database...")
    db['users'].push()
    print("   ✓ Changes pushed successfully!")
    print()

    # Verify changes persisted
    print("9. Verifying changes persisted...")
    db.pull()  # Refresh from database
    print(db['users'].to_pandas())
    print()

    # Demonstrate composite primary keys
    print("10. Composite Primary Keys Example...")
    memberships_data = pd.DataFrame({
        'user_id': [1, 1, 2, 4],
        'org_id': ['org1', 'org2', 'org1', 'org1'],
        'role': ['admin', 'user', 'user', 'member']
    })
    
    memberships = db.create_table('memberships', memberships_data, 
                                   primary_key=['user_id', 'org_id'])
    print("    Created 'memberships' table with composite PK")
    print()
    print("    Note: Composite PK becomes MultiIndex!")
    print(memberships.to_pandas())
    print()
    
    # CRUD with composite keys
    print("    - Adding new membership (user_id=4, org_id='org2')")
    memberships.data.add_row({
        'user_id': 4,
        'org_id': 'org2',
        'role': 'guest'
    })
    
    print("    - Updating membership (1, 'org1') role to 'owner'")
    memberships.data.update_row(
        (1, 'org1'),  # Composite PK as tuple
        {'role': 'owner'}
    )
    
    memberships.push()
    print("    ✓ Composite PK operations work seamlessly!")
    print()

    # Demonstrate auto-increment
    print("11. Auto-Increment Example...")
    posts_data = pd.DataFrame({
        'id': [1, 2],
        'user_id': [1, 2],
        'title': ['First Post', 'Second Post']
    })
    
    # Create table with auto-increment using Table constructor
    posts = pa.Table('posts', posts_data, 'id', engine, auto_increment=True)
    
    print("    Created 'posts' table with auto_increment=True")
    print()
    
    # Add without specifying PK - it will be auto-generated
    print("    - Adding post without ID (will auto-generate ID=3)")
    posts.data.add_row({
        'user_id': 4,
        'title': 'Auto-generated ID Post'
    }, auto_increment=True)
    
    print("    - Posts after auto-increment:")
    print("     ", posts.data.to_pandas()[['user_id', 'title']])
    
    posts.push()
    db.db['posts'] = posts
    print("    ✓ Auto-increment working!")
    print()

    # Schema evolution
    print("12. Schema Evolution...")
    
    # Add column
    print("    - Adding 'created_at' column")
    db['posts'].data.add_column_with_default('created_at', str(pd.Timestamp.now()))
    
    # Rename column
    print("    - Renaming 'title' to 'post_title'")
    db['posts'].data.rename_column_safe('title', 'post_title')
    
    db['posts'].push()
    print("    ✓ Schema changes applied!")
    print()

    # Show final state
    print("13. Final Database State:")
    print()
    print("Users table:")
    print(db['users'].to_pandas())
    print()
    print("Memberships table (composite PK):")
    print(db['memberships'].to_pandas())
    print()
    print("Posts table (auto-increment):")
    print(db['posts'].to_pandas())
    print()

    # Demonstrate transaction safety
    print("14. Transaction Safety...")
    print("    All changes happen in transactions.")
    print("    If any error occurs, all changes are rolled back.")
    print()

    # Multi-table transaction
    print("15. Multi-Table Transaction...")
    db['users'].data.update_row(1, {'age': 27})
    db['posts'].data.add_row({
        'id': 100,
        'user_id': 1,
        'post_title': 'Transaction Test',
        'created_at': pd.Timestamp.now()
    })
    
    print("    Pushing changes to both tables in single transaction...")
    db.push()
    print("    ✓ All changes committed atomically!")
    print()

    print("=" * 70)
    print("Example completed successfully!")
    print()
    print("Key Takeaways:")
    print("  ✓ Primary keys are now DataFrame indexes (immutable)")
    print("  ✓ Composite primary keys use MultiIndex")
    print("  ✓ Auto-increment generates IDs automatically")
    print("  ✓ Full CRUD operations with type safety")
    print("  ✓ Schema evolution with helper methods")
    print("  ✓ Automatic change tracking")
    print("  ✓ Transaction safety with rollback")
    print("=" * 70)


if __name__ == '__main__':
    main()
