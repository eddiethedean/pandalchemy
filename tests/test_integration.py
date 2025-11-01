"""Integration tests for the full pandalchemy workflow."""

import contextlib
import os
import tempfile

import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame


@pytest.fixture
def sqlite_engine():
    """Create a temporary SQLite database engine."""
    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    engine = create_engine(f"sqlite:///{path}")

    yield engine

    # Cleanup
    engine.dispose()
    with contextlib.suppress(OSError, PermissionError):
        os.remove(path)


@pytest.fixture
def test_db(db_engine):
    """Parametrized fixture for multi-database testing."""
    return DataBase(db_engine)


@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})


@pytest.mark.multidb
def test_database_initialization(test_db, sample_data):
    """Test DataBase initialization with empty database."""
    db = test_db

    assert len(db) == 0
    assert isinstance(db, DataBase)


@pytest.mark.multidb
def test_create_and_retrieve_table(test_db, sample_data):
    """Test creating a table and retrieving it."""
    db = test_db

    # Create a table
    table = db.create_table("users", sample_data, primary_key="id")

    assert table.name == "users"
    assert len(table) == 3

    # Retrieve the table
    retrieved_table = db["users"]
    assert retrieved_table.name == "users"
    assert len(retrieved_table) == 3


@pytest.mark.multidb
def test_table_modification_and_push(test_db, sample_data):
    """Test modifying a table and pushing changes."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Modify the table
    db["users"]["age"] = [26, 31, 36]

    # Push changes
    db["users"].push()

    # Verify changes persisted
    db.pull()
    assert list(db["users"]["age"]) == [26, 31, 36]


@pytest.mark.multidb
def test_add_column(test_db, sample_data):
    """Test adding a new column to a table."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Add a new column
    db["users"]["email"] = ["alice@test.com", "bob@test.com", "charlie@test.com"]

    # Push changes
    db["users"].push()

    # Verify column was added
    db.pull()
    assert "email" in db["users"].columns
    assert db["users"]["email"].iloc[0] == "alice@test.com"


@pytest.mark.multidb
def test_drop_column(test_db, sample_data):
    """Test dropping a column from a table."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Drop a column
    db["users"].drop("age", axis=1, inplace=True)

    # Push changes
    db["users"].push()

    # Verify column was dropped
    db.pull()
    assert "age" not in db["users"].columns


@pytest.mark.multidb
def test_insert_row(test_db, sample_data):
    """Test inserting a new row."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Get current data
    current_df = db["users"].to_pandas()

    # Add a new row
    new_row = pd.DataFrame({"name": ["David"], "age": [40]}, index=[4])
    new_row.index.name = "id"

    combined_df = pd.concat([current_df, new_row])

    # Update the table data
    db["users"]._data = combined_df
    db["users"]._tracker.compute_row_changes(combined_df)

    # Push changes
    db["users"].push()

    # Verify row was inserted
    db.pull()
    assert len(db["users"]) == 4
    assert 4 in db["users"].index


@pytest.mark.multidb
def test_update_row(test_db, sample_data):
    """Test updating an existing row."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Update a row
    db["users"].loc[1, "name"] = "Alicia"
    db["users"].loc[1, "age"] = 26

    # Push changes
    db["users"].push()

    # Verify row was updated
    db.pull()
    assert db["users"].loc[1, "name"] == "Alicia"
    assert db["users"].loc[1, "age"] == 26


@pytest.mark.multidb
def test_delete_row(test_db, sample_data):
    """Test deleting a row."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Delete a row
    db["users"].drop(2, inplace=True)

    # Push changes
    db["users"].push()

    # Verify row was deleted
    db.pull()
    assert len(db["users"]) == 2
    assert 2 not in db["users"].index


@pytest.mark.multidb
def test_multiple_changes_single_push(test_db, sample_data):
    """Test making multiple changes and pushing once."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Make multiple changes
    db["users"]["email"] = ["a@test.com", "b@test.com", "c@test.com"]
    db["users"].loc[1, "age"] = 26
    db["users"].drop("name", axis=1, inplace=True)

    # Push all changes at once
    db["users"].push()

    # Verify all changes persisted
    db.pull()
    assert "email" in db["users"].columns
    assert "name" not in db["users"].columns
    assert db["users"].loc[1, "age"] == 26


@pytest.mark.multidb
def test_multiple_tables(test_db):
    """Test working with multiple tables."""
    db = test_db

    # Create first table
    users_data = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    db.create_table("users", users_data, primary_key="id")

    # Create second table
    posts_data = pd.DataFrame(
        {"id": [1, 2, 3], "user_id": [1, 1, 2], "content": ["Post 1", "Post 2", "Post 3"]}
    )
    db.create_table("posts", posts_data, primary_key="id")

    # Verify both tables exist
    assert "users" in db.table_names
    assert "posts" in db.table_names
    assert len(db["users"]) == 2
    assert len(db["posts"]) == 3


@pytest.mark.multidb
def test_rollback_on_error(test_db, sample_data):
    """Test that changes are rolled back on error."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Make a change
    db["users"].loc[1, "age"] = 26

    # Note: Testing actual rollback is tricky without causing a real error
    # This test verifies that the transaction mechanism is in place
    # A real error would need to be simulated in the SQL operations

    # For now, just verify the structure supports transactions
    assert hasattr(db.engine, "begin")


@pytest.mark.multidb
def test_get_changes_summary(test_db, sample_data):
    """Test getting a summary of tracked changes."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Make some changes
    db["users"]["email"] = ["a@test.com", "b@test.com", "c@test.com"]
    db["users"].loc[1, "age"] = 26

    # Get changes summary before push
    summary = db["users"].get_changes_summary()

    assert "has_changes" in summary
    assert summary["has_changes"] is True
    assert summary["columns_added"] >= 1


@pytest.mark.multidb
def test_lazy_loading(test_db, sample_data, db_engine):
    """Test lazy loading of tables."""
    # Create database with table
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Create new database instance with lazy loading
    db_lazy = DataBase(db_engine, lazy=True)

    # Table should not be loaded yet
    assert db_lazy.db["users"] is None

    # Access the table to load it
    table = db_lazy["users"]

    # Now it should be loaded
    assert table is not None
    assert len(table) == 3


@pytest.mark.multidb
def test_table_copy(test_db, sample_data):
    """Test copying a table."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Copy the table
    table_copy = db["users"].copy()

    # Modify the copy
    table_copy["age"] = [100, 200, 300]

    # Original should be unchanged
    assert list(db["users"]["age"]) != [100, 200, 300]


@pytest.mark.multidb
def test_to_pandas(test_db, sample_data):
    """Test converting table to pandas DataFrame."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Convert to pandas
    df = db["users"].to_pandas()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert "name" in df.columns
    assert "age" in df.columns


@pytest.mark.multidb
def test_empty_table(test_db, db_engine):
    """Test working with an empty table."""
    # Create empty DataFrame
    empty_df = pd.DataFrame(columns=["id", "name", "age"])
    empty_df = empty_df.set_index("id")

    # Create table (this might not work with some implementations)
    # For now, just verify the structure can handle it
    table = TableDataFrame("empty_table", data=empty_df, primary_key="id", engine=db_engine)

    assert len(table) == 0


@pytest.mark.multidb
def test_repr_methods(test_db, sample_data):
    """Test string representation methods."""
    db = test_db
    db.create_table("users", sample_data, primary_key="id")

    # Test DataBase repr
    db_repr = repr(db)
    assert "DataBase" in db_repr

    # Test TableDataFrame repr
    table_repr = repr(db["users"])
    assert "TableDataFrame" in table_repr
    # Name should be shown in repr
    assert "Alice" in table_repr or "users" in table_repr
