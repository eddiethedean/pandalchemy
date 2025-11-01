"""Example test demonstrating multi-database testing.

This test will run on all available database types (SQLite, PostgreSQL, MySQL).
Set TEST_POSTGRES_URL and/or TEST_MYSQL_URL environment variables to enable
testing on those databases.
"""

import pandas as pd
import pytest

from pandalchemy import DataBase


@pytest.mark.multidb
def test_basic_crud_operations(db_engine):
    """Example test that runs on all configured databases."""
    # This test will run once per available database type
    db = DataBase(db_engine)

    # Create a table
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})

    table = db.create_table("users", df, "id")
    table.push()

    # Verify table was created
    assert len(db["users"]) == 3
    assert db["users"].get_row(1)["name"] == "Alice"

    # Update a row
    db["users"].update_row(1, {"age": 26})
    db.push()

    # Verify update
    db.pull()
    assert db["users"].get_row(1)["age"] == 26

    # Delete a row
    db["users"].delete_row(2)
    db.push()

    # Verify deletion
    db.pull()
    assert not db["users"].row_exists(2)
    assert len(db["users"]) == 2


@pytest.mark.multidb
def test_schema_evolution(db_engine):
    """Test schema changes work across all database types."""
    db = DataBase(db_engine)

    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

    table = db.create_table("users", df, "id")
    table.push()

    # Add a new column
    table.add_column_with_default("email", "")
    table.push()

    # Verify new column exists
    table.pull()
    assert "email" in table.columns

    # Update the new column
    table.update_row(1, {"email": "alice@example.com"})
    table.push()

    # Verify update
    table.pull()
    assert table.get_row(1)["email"] == "alice@example.com"


# Example of database-specific tests


@pytest.mark.postgres
def test_postgres_specific_feature(postgres_engine):
    """Test PostgreSQL-specific features."""
    # This test only runs if TEST_POSTGRES_URL is set
    # PostgreSQL-specific test code here
    pass


@pytest.mark.mysql
def test_mysql_specific_feature(mysql_engine):
    """Test MySQL-specific features."""
    # This test only runs if TEST_MYSQL_URL is set
    # MySQL-specific test code here
    pass
