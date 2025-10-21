"""Tests for column drop rollback behavior on transaction failure."""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame
from pandalchemy.exceptions import SchemaError, TransactionError


def test_dropped_column_not_restored_after_push_failure(tmp_path):
    """
    Test that demonstrates the current behavior: dropped columns are NOT
    restored in memory after a failed push, even though the database is rolled back.
    
    This is the expected behavior - users must call pull() to restore state.
    """
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)
    
    # Create table with email column
    users_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
    })
    db.create_table('users', users_df, primary_key='id')
    
    # Get table and verify email exists
    users = db['users']
    assert 'email' in users.columns
    original_email_data = users['email'].copy()
    
    # Drop the email column
    users.drop_column_safe('email')
    assert 'email' not in users.columns  # Dropped in memory
    assert users.has_changes()
    
    # Manually corrupt the DataFrame to cause validation error
    # (simulate a scenario that would cause push to fail)
    users._data = users._data.reset_index()
    users._data = users._data.drop(columns=['id'])
    
    # Try to push - should fail due to missing PK
    with pytest.raises(SchemaError, match="Primary key column"):
        users.push()
    
    # Current behavior: Column is still missing in memory
    assert 'email' not in users.columns  # ❌ NOT restored
    assert users.has_changes()  # ❌ Changes still tracked
    
    # Database should not have the column dropped (rollback worked)
    db_fresh = DataBase(engine)
    users_fresh = db_fresh['users']
    assert 'email' in users_fresh.columns  # ✅ Database unchanged
    assert list(users_fresh['email']) == list(original_email_data)


def test_dropped_column_data_preserved_in_database(tmp_path):
    """Test that dropped column data is preserved in database after rollback."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)
    
    # Create table with sensitive data
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie'],
        'password': ['secret1', 'secret2', 'secret3'],
        'balance': [100.0, 200.0, 300.0]
    })
    db.create_table('accounts', data, primary_key='id')
    
    # Drop password column and cause error
    accounts = db['accounts']
    original_passwords = accounts['password'].copy()
    
    accounts.drop_column_safe('password')
    assert 'password' not in accounts.columns
    
    # Cause a different error (corrupt PK)
    accounts._data = accounts._data.reset_index()
    accounts._data = accounts._data.drop(columns=['id'])
    
    # Push fails
    with pytest.raises(SchemaError):
        accounts.push()
    
    # Verify database still has password data
    db_check = DataBase(engine)
    accounts_check = db_check['accounts']
    assert 'password' in accounts_check.columns
    assert list(accounts_check['password']) == list(original_passwords)
    

def test_multiple_columns_dropped_then_error(tmp_path):
    """Test rollback behavior when multiple columns are dropped before error."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)
    
    # Create table with many columns
    data = pd.DataFrame({
        'id': [1, 2],
        'col1': ['a', 'b'],
        'col2': ['c', 'd'],
        'col3': ['e', 'f'],
        'col4': ['g', 'h']
    })
    db.create_table('test', data, primary_key='id')
    
    test = db['test']
    original_col2 = test['col2'].copy()
    original_col3 = test['col3'].copy()
    
    # Drop multiple columns
    test.drop_column_safe('col2')
    test.drop_column_safe('col3')
    assert 'col2' not in test.columns
    assert 'col3' not in test.columns
    
    # Cause error
    test._data = test._data.reset_index()
    test._data = test._data.drop(columns=['id'])
    
    with pytest.raises(SchemaError):
        test.push()
    
    # Columns still dropped in memory (current behavior)
    assert 'col2' not in test.columns
    assert 'col3' not in test.columns
    
    # But database has all columns preserved
    db_check = DataBase(engine)
    test_check = db_check['test']
    assert 'col2' in test_check.columns
    assert 'col3' in test_check.columns
    assert list(test_check['col2']) == list(original_col2)
    assert list(test_check['col3']) == list(original_col3)


def test_add_then_drop_column_with_error(tmp_path):
    """Test behavior when a column is added, then dropped, then error occurs."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)
    
    data = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob']
    })
    db.create_table('users', data, primary_key='id')
    
    users = db['users']
    
    # Add a new column
    users.add_column_with_default('temp_col', 'temp_value')
    assert 'temp_col' in users.columns
    
    # Then drop it
    users.drop_column_safe('temp_col')
    assert 'temp_col' not in users.columns
    
    # Cause error
    users._data = users._data.reset_index()
    users._data = users._data.drop(columns=['id'])
    
    with pytest.raises(SchemaError):
        users.push()
    
    # Column should still not exist (it was added then dropped in same session)
    assert 'temp_col' not in users.columns
    
    # Database should not have the temp column
    db_check = DataBase(engine)
    users_check = db_check['users']
    assert 'temp_col' not in users_check.columns


def test_drop_column_with_composite_key(tmp_path):
    """Test column drop rollback with composite primary keys."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)
    
    # Create table with composite key
    data = pd.DataFrame({
        'user_id': [1, 1, 2],
        'org_id': [10, 20, 10],
        'role': ['admin', 'user', 'admin'],
        'permissions': ['all', 'read', 'all']
    })
    db.create_table('memberships', data, primary_key=['user_id', 'org_id'])
    
    memberships = db['memberships']
    original_permissions = memberships['permissions'].copy()
    
    # Drop non-key column
    memberships.drop_column_safe('permissions')
    assert 'permissions' not in memberships.columns
    
    # Cause error by dropping part of composite key
    memberships._data = memberships._data.reset_index()
    memberships._data = memberships._data.drop(columns=['user_id'])
    
    with pytest.raises(SchemaError):
        memberships.push()
    
    # Permissions column still dropped in memory
    assert 'permissions' not in memberships.columns
    
    # Database has permissions column intact
    db_check = DataBase(engine)
    memberships_check = db_check['memberships']
    assert 'permissions' in memberships_check.columns
    assert list(memberships_check['permissions']) == list(original_permissions)


def test_user_must_pull_to_restore_after_failed_push(tmp_path):
    """
    Test that documents the solution: users must call pull() after a failed
    push to restore the DataFrame to the database state.
    """
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)
    
    data = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'status': ['active', 'inactive']
    })
    db.create_table('users', data, primary_key='id')
    
    users = db['users']
    
    # Drop column
    users.drop_column_safe('status')
    assert 'status' not in users.columns
    
    # Cause error
    users._data = users._data.reset_index()
    users._data = users._data.drop(columns=['id'])
    
    # Push fails
    with pytest.raises(SchemaError):
        users.push()
    
    # Column still missing
    assert 'status' not in users.columns
    
    # Solution: Re-instantiate or pull to restore
    db = DataBase(engine)  # Fresh instance
    users = db['users']
    
    # Now column is back with data
    assert 'status' in users.columns
    assert list(users['status']) == ['active', 'inactive']


def test_drop_column_data_types_preserved(tmp_path):
    """Test that various data types in dropped columns are preserved in database."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)
    
    # Create table with various data types
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'balance': [100.50, 200.75, 300.00],
        'active': [True, False, True]
    })
    db.create_table('users', data, primary_key='id')
    
    users = db['users']
    original_age = users['age'].copy()
    original_balance = users['balance'].copy()
    original_active = users['active'].copy()
    
    # Drop all non-key, non-name columns
    users.drop_column_safe('age')
    users.drop_column_safe('balance')
    users.drop_column_safe('active')
    
    assert 'age' not in users.columns
    assert 'balance' not in users.columns
    assert 'active' not in users.columns
    
    # Cause error by corrupting PK
    users._data = users._data.reset_index()
    users._data = users._data.drop(columns=['id'])
    
    with pytest.raises(SchemaError):
        users.push()
    
    # Verify database preserved all data types correctly
    db_check = DataBase(engine)
    users_check = db_check['users']
    
    assert 'age' in users_check.columns
    assert 'balance' in users_check.columns
    assert 'active' in users_check.columns
    
    assert list(users_check['age']) == list(original_age)
    assert list(users_check['balance']) == list(original_balance)
    assert list(users_check['active']) == list(original_active)

