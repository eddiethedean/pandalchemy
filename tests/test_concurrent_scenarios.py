"""Tests for concurrent access scenarios."""

import pandas as pd
from sqlalchemy import create_engine

from pandalchemy import DataBase


def test_two_instances_non_overlapping_modifications(tmp_path):
    """Test two database instances modifying non-overlapping rows."""
    db_path = tmp_path / "concurrent.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create initial data
    data = pd.DataFrame({
        'id': range(1, 101),
        'value': [0] * 100,
        'updated_by': ['none'] * 100
    })
    db1 = DataBase(engine)
    db1.create_table('data', data, primary_key='id')

    # Instance 1: Modify rows 1-50
    for i in range(1, 51):
        db1['data'].update_row(i, {'value': i, 'updated_by': 'instance1'})

    db1.push()

    # Instance 2: Modify rows 51-100 (on fresh instance)
    db2 = DataBase(engine)

    for i in range(51, 101):
        db2['data'].update_row(i, {'value': i, 'updated_by': 'instance2'})

    db2.push()

    # Verify no data loss
    db_final = DataBase(engine)
    assert len(db_final['data']) == 100

    # Check instance 1 updates persisted
    assert db_final['data'].get_row(25)['updated_by'] == 'instance1'
    assert db_final['data'].get_row(25)['value'] == 25

    # Check instance 2 updates persisted
    assert db_final['data'].get_row(75)['updated_by'] == 'instance2'
    assert db_final['data'].get_row(75)['value'] == 75


def test_last_writer_wins_scenario(tmp_path):
    """Test last-writer-wins pattern with concurrent modifications."""
    db_path = tmp_path / "last_writer.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Initial state
    users = pd.DataFrame({
        'id': [1],
        'name': ['Alice'],
        'value': [100]
    })
    db1 = DataBase(engine)
    db1.create_table('users', users, primary_key='id')

    # Instance 1: Read and modify
    db1['users'].update_row(1, {'value': 200})

    # Instance 2: Read same data and modify differently
    db2 = DataBase(engine)
    db2['users'].update_row(1, {'value': 300})

    # Instance 1 pushes first
    db1.push()

    # Instance 2 pushes second (overwrites)
    db2.push()

    # Verify last writer wins
    db_final = DataBase(engine)
    assert db_final['users'].get_row(1)['value'] == 300  # Instance 2's value


def test_pull_modify_push_pattern(tmp_path):
    """Test recommended pull → modify → push pattern."""
    db_path = tmp_path / "pmp.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Initial data
    data = pd.DataFrame({
        'id': [1, 2],
        'value': [100, 200]
    })
    db1 = DataBase(engine)
    db1.create_table('data', data, primary_key='id')

    # Instance 1: Modifies
    db1['data'].update_row(1, {'value': 150})
    db1.push()

    # Instance 2: Uses recommended pattern
    db2 = DataBase(engine)
    db2.pull()  # Get fresh data

    # Verify instance 2 sees instance 1's changes
    assert db2['data'].get_row(1)['value'] == 150

    # Make changes
    db2['data'].update_row(2, {'value': 250})
    db2.push()

    # Verify both changes preserved
    db_final = DataBase(engine)
    assert db_final['data'].get_row(1)['value'] == 150
    assert db_final['data'].get_row(2)['value'] == 250


def test_optimistic_locking_with_version_column(tmp_path):
    """Test optimistic locking pattern using version column."""
    db_path = tmp_path / "versioned.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table with version column
    data = pd.DataFrame({
        'id': [1, 2],
        'value': [100, 200],
        'version': [1, 1]
    })
    db1 = DataBase(engine)
    db1.create_table('data', data, primary_key='id')

    # Instance 1: Read version
    row1_v1 = db1['data'].get_row(1)
    original_version = row1_v1['version']

    # Instance 2: Also reads and modifies
    db2 = DataBase(engine)
    row1_v2 = db2['data'].get_row(1)
    db2['data'].update_row(1, {'value': 150, 'version': row1_v2['version'] + 1})
    db2.push()

    # Instance 1: Tries to update with stale version
    # First check if version changed
    db1.pull()
    current_row = db1['data'].get_row(1)

    if current_row['version'] != original_version:
        print("Detected stale data! Re-reading before update.")
        # Stale data detected
        assert current_row['version'] == 2
        assert current_row['value'] == 150

        # Update with current version
        db1['data'].update_row(1, {'value': 175, 'version': current_row['version'] + 1})
        db1.push()

        # Verify final state
        db_final = DataBase(engine)
        assert db_final['data'].get_row(1)['version'] == 3
        assert db_final['data'].get_row(1)['value'] == 175


def test_stale_data_detection(tmp_path):
    """Test detecting stale data before push."""
    db_path = tmp_path / "stale.db"
    engine = create_engine(f"sqlite:///{db_path}")

    data = pd.DataFrame({
        'id': [1],
        'value': [100],
        'checksum': ['abc123']
    })
    db1 = DataBase(engine)
    db1.create_table('data', data, primary_key='id')

    # Instance 1: Read data
    original_checksum = db1['data'].get_row(1)['checksum']

    # Instance 2: Modify and push
    db2 = DataBase(engine)
    db2['data'].update_row(1, {'value': 200, 'checksum': 'def456'})
    db2.push()

    # Instance 1: Before pushing, verify checksum
    db1.pull()
    current_checksum = db1['data'].get_row(1)['checksum']

    if current_checksum != original_checksum:
        print("Data was modified by another process!")
        assert current_checksum == 'def456'
        # Handle conflict (e.g., merge, abort, or retry)


def test_sequential_pushes_consistency(tmp_path):
    """Test consistency with sequential pushes from multiple instances."""
    db_path = tmp_path / "sequential.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Initial data
    counter = pd.DataFrame({
        'id': [1],
        'count': [0]
    })
    db1 = DataBase(engine)
    db1.create_table('counter', counter, primary_key='id')

    # Simulate 10 processes incrementing counter
    for _i in range(10):
        db = DataBase(engine)
        current = db['counter'].get_row(1)['count']
        db['counter'].update_row(1, {'count': current + 1})
        db.push()

    # Verify final count
    db_final = DataBase(engine)
    assert db_final['counter'].get_row(1)['count'] == 10


def test_concurrent_bulk_inserts_different_tables(tmp_path):
    """Test concurrent bulk inserts to different tables."""
    db_path = tmp_path / "bulk_concurrent.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create empty tables
    db1 = DataBase(engine)
    db1.create_table('table_a', pd.DataFrame({'id': [], 'value': []}), primary_key='id')
    db1.create_table('table_b', pd.DataFrame({'id': [], 'value': []}), primary_key='id')

    # Instance 1: Bulk insert to table_a
    rows_a = [{'id': i, 'value': i * 10} for i in range(1, 1001)]
    db1['table_a'].bulk_insert(rows_a)
    db1.push()

    # Instance 2: Bulk insert to table_b
    db2 = DataBase(engine)
    rows_b = [{'id': i, 'value': i * 20} for i in range(1, 1001)]
    db2['table_b'].bulk_insert(rows_b)
    db2.push()

    # Verify both tables populated
    db_final = DataBase(engine)
    assert len(db_final['table_a']) == 1000
    assert len(db_final['table_b']) == 1000
    assert db_final['table_a'].get_row(500)['value'] == 5000
    assert db_final['table_b'].get_row(500)['value'] == 10000


def test_concurrent_schema_changes_same_table(tmp_path):
    """Test concurrent schema changes to same table."""
    db_path = tmp_path / "schema_concurrent.db"
    engine = create_engine(f"sqlite:///{db_path}")

    data = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob']
    })
    db1 = DataBase(engine)
    db1.create_table('users', data, primary_key='id')

    # Instance 1: Add column_a
    db1['users'].add_column_with_default('column_a', 'value_a')
    db1.push()

    # Instance 2: Add column_b (after instance 1 pushed)
    db2 = DataBase(engine)
    db2.pull()  # Get latest schema
    db2['users'].add_column_with_default('column_b', 'value_b')
    db2.push()

    # Verify both columns exist
    db_final = DataBase(engine)
    assert 'column_a' in db_final['users'].columns
    assert 'column_b' in db_final['users'].columns

