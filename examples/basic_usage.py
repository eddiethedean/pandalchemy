"""
Basic usage example for pandalchemy 0.2.0

This example demonstrates the key features of pandalchemy including:
- Creating a database and tables
- Automatic change tracking
- Schema modifications
- Transaction-safe operations
"""

import pandas as pd
from sqlalchemy import create_engine

import pandalchemy as pa


def main():
    # Create an in-memory SQLite database for demonstration
    engine = create_engine('sqlite:///:memory:')

    print("=" * 60)
    print("Pandalchemy 0.2.0 - Basic Usage Example")
    print("=" * 60)
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
    print(users_table.to_pandas())
    print()

    # Make various changes
    print("3. Making changes to the table...")

    # Add a new column
    print("   - Adding 'email' column")
    db['users']['email'] = ['alice@example.com', 'bob@example.com', 'charlie@example.com']

    # Update a row
    print("   - Updating Alice's age to 26")
    db['users'].loc[1, 'age'] = 26

    # Add a salary column
    print("   - Adding 'salary' column")
    db['users']['salary'] = [50000, 60000, 70000]

    # Check what changes are tracked
    print()
    print("4. Checking tracked changes...")
    summary = db['users'].get_changes_summary()
    print("   Changes summary:")
    for key, value in summary.items():
        print(f"     {key}: {value}")
    print()

    # Push changes
    print("5. Pushing changes to database...")
    db['users'].push()
    print("   ✓ Changes pushed successfully!")
    print()

    # Verify changes persisted
    print("6. Verifying changes persisted...")
    db.pull()  # Refresh from database
    print(db['users'].to_pandas())
    print()

    # Demonstrate schema changes
    print("7. Demonstrating column operations...")

    # Rename a column
    print("   - Renaming 'salary' to 'annual_salary'")
    db['users'].rename(columns={'salary': 'annual_salary'}, inplace=True)

    # Drop a column
    print("   - Dropping 'age' column")
    db['users'].drop('age', axis=1, inplace=True)

    # Push schema changes
    db['users'].push()
    print("   ✓ Schema changes applied!")
    print()

    # Show final state
    print("8. Final table state:")
    print(db['users'].to_pandas())
    print()

    # Demonstrate transaction safety
    print("9. Transaction Safety Demonstration...")
    print("   All changes happen in transactions.")
    print("   If any error occurs, all changes are rolled back.")
    print()

    # Create another table
    print("10. Working with multiple tables...")
    posts_data = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'user_id': [1, 1, 2, 3],
        'title': ['First Post', 'Second Post', 'Bob\'s Post', 'Charlie\'s Post'],
        'published': [True, True, False, True]
    })

    db.create_table('posts', posts_data, primary_key='id')
    print(f"   Created 'posts' table with {len(db['posts'])} rows")
    print()

    # Make changes to both tables
    print("11. Making changes to multiple tables...")
    db['users'].loc[1, 'email'] = 'alice.smith@example.com'
    db['posts'].loc[3, 'published'] = True

    # Push all changes in a single transaction
    print("    Pushing all changes in single transaction...")
    db.push()
    print("    ✓ All changes pushed!")
    print()

    print("=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == '__main__':
    main()

