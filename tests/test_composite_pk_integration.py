"""Integration tests for composite primary key functionality."""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame


def test_composite_pk_full_workflow(tmp_path):
    """Test complete workflow with composite primary keys."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table with composite PK
    df = pd.DataFrame({
        'user_id': ['u1', 'u1', 'u2', 'u2'],
        'org_id': ['org1', 'org2', 'org1', 'org2'],
        'role': ['admin', 'user', 'user', 'admin'],
        'active': [True, True, False, True]
    })

    table = TableDataFrame('memberships', df, ['user_id', 'org_id'], engine)

    # Verify MultiIndex in local data
    pulled_df = table.to_pandas()
    assert isinstance(pulled_df.index, pd.MultiIndex)
    assert list(pulled_df.index.names) == ['user_id', 'org_id']

    # Test CRUD operations with composite PK BEFORE pushing
    # Add a new row
    table.add_row({
        'user_id': 'u3',
        'org_id': 'org1',
        'role': 'guest',
        'active': True
    })
    assert table.row_exists(('u3', 'org1'))

    # Update a row
    table.update_row(('u1', 'org1'), {'role': 'superadmin', 'active': False})
    row = table.get_row(('u1', 'org1'))
    assert row['role'] == 'superadmin'
    assert row['active'] == False

    # Delete a row
    table.delete_row(('u2', 'org2'))
    assert not table.row_exists(('u2', 'org2'))

    # Push changes
    table.push()

    # Verify changes persisted by pulling fresh
    db = DataBase(engine)
    assert db['memberships'].row_exists(('u3', 'org1'))
    assert db['memberships'].get_row(('u1', 'org1'))['role'] == 'superadmin'
    assert not db['memberships'].row_exists(('u2', 'org2'))


def test_composite_pk_immutability(tmp_path):
    """Test that composite PK values cannot be updated."""
    from pandalchemy.exceptions import DataValidationError

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    })

    table = TableDataFrame('memberships', df, ['user_id', 'org_id'], engine)
    table.push()

    # Try to update part of composite PK
    db = DataBase(engine)
    with pytest.raises(DataValidationError, match="Cannot update primary key"):
        db['memberships'].update_row(('u1', 'org1'), {'user_id': 'u999'})

    with pytest.raises(DataValidationError, match="Cannot update primary key"):
        db['memberships'].update_row(('u1', 'org1'), {'org_id': 'org999'})


def test_composite_pk_with_set_primary_key():
    """Test changing to composite PK creates MultiIndex."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'user_id': ['u1', 'u2', 'u3'],
        'org_id': ['org1', 'org1', 'org2'],
        'role': ['admin', 'user', 'admin']
    })

    # Start with single PK
    from pandalchemy import TableDataFrame
    tdf = TableDataFrame(data=df, primary_key='id')

    # Change to composite PK
    tdf.set_primary_key(['user_id', 'org_id'])

    # Verify MultiIndex created
    result = tdf.to_pandas()
    assert isinstance(result.index, pd.MultiIndex)
    assert list(result.index.names) == ['user_id', 'org_id']
    assert 'user_id' not in result.columns
    assert 'org_id' not in result.columns
    assert 'id' in result.columns  # Old PK became column


def test_composite_pk_upsert(tmp_path):
    """Test upsert with composite primary key."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    })

    table = TableDataFrame('memberships', df, ['user_id', 'org_id'], engine)

    # Upsert existing row (should update)
    table.upsert_row({
        'user_id': 'u1',
        'org_id': 'org1',
        'role': 'owner'
    })
    assert table.get_row(('u1', 'org1'))['role'] == 'owner'

    # Upsert new row (should insert)
    table.upsert_row({
        'user_id': 'u3',
        'org_id': 'org2',
        'role': 'guest'
    })
    assert table.row_exists(('u3', 'org2'))

    # Push and verify persistence
    table.push()

    # Reload from DB
    db = DataBase(engine)
    assert db['memberships'].get_row(('u1', 'org1'))['role'] == 'owner'
    assert db['memberships'].row_exists(('u3', 'org2'))


def test_composite_pk_validation(tmp_path):
    """Test validation with composite primary keys."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    })

    table = TableDataFrame('memberships', df, ['user_id', 'org_id'], engine)

    # Valid data should pass
    errors = table.validate_data()
    assert errors == []

    # Dropping part of composite PK should fail validation
    # Need to reset index first to access column
    table._data = table._data.reset_index()
    table.drop_column_safe('org_id')
    errors = table.validate_data()
    assert len(errors) > 0
    assert any('dropped' in error for error in errors)


def test_composite_pk_bulk_operations(tmp_path):
    """Test bulk operations with composite primary keys."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    df = pd.DataFrame({
        'user_id': ['u1', 'u2'],
        'org_id': ['org1', 'org1'],
        'role': ['admin', 'user']
    })

    table = TableDataFrame('memberships', df, ['user_id', 'org_id'], engine)

    # Bulk insert with composite PKs
    new_rows = [
        {'user_id': 'u3', 'org_id': 'org1', 'role': 'guest'},
        {'user_id': 'u3', 'org_id': 'org2', 'role': 'user'},
        {'user_id': 'u4', 'org_id': 'org1', 'role': 'admin'}
    ]
    table.bulk_insert(new_rows)

    # Verify all inserted locally
    assert table.row_exists(('u3', 'org1'))
    assert table.row_exists(('u3', 'org2'))
    assert table.row_exists(('u4', 'org1'))

    # Push and verify persistence
    table.push()

    # Reload from DB
    db = DataBase(engine)
    assert len(db['memberships']) == 5  # 2 original + 3 new
    assert db['memberships'].row_exists(('u3', 'org1'))
    assert db['memberships'].row_exists(('u4', 'org1'))

