"""Performance tests with realistic dataset sizes."""

import time

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame


def test_bulk_operations_reasonable_scale(tmp_path):
    """Test bulk operations at reasonable scale (1000 rows)."""
    db_path = tmp_path / "bulk.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create table with 1000 rows
    data = pd.DataFrame({
        'id': range(1, 1001),
        'category': np.random.choice(['A', 'B', 'C'], 1000),
        'value': np.random.randint(1, 1000, 1000),
        'active': [True] * 1000
    })
    db.create_table('data', data, primary_key='id')

    start = time.time()

    # Bulk update using vectorized operations
    db['data'].loc[db['data']['category'] == 'A', 'value'] = \
        db['data'].loc[db['data']['category'] == 'A', 'value'] * 2

    # Add column
    db['data'].add_column_with_default('processed', True)

    # Push
    db.push()

    elapsed = time.time() - start

    # Should be fast with vectorized operations
    assert elapsed < 5.0, f"Took {elapsed:.2f}s, should be <5s"

    # Verify
    db.pull()
    assert 'processed' in db['data'].columns


def test_chunked_processing_pattern(tmp_path):
    """Test chunked processing pattern for large updates."""
    db_path = tmp_path / "chunks.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table with 5000 rows
    data = pd.DataFrame({
        'id': range(1, 5001),
        'value': range(1, 5001),
        'processed': [False] * 5000
    })
    table = TableDataFrame('data', data, 'id', engine)
    table.push()

    # Process in 5 chunks of 1000
    chunk_size = 1000

    for i in range(5):
        # Reload fresh for each chunk
        db = DataBase(engine)

        # Select chunk (vectorized)
        start_id = i * chunk_size + 1
        end_id = (i + 1) * chunk_size + 1
        chunk_mask = (db['data'].index >= start_id) & (db['data'].index < end_id)

        # Update chunk (vectorized)
        db['data'].loc[chunk_mask, 'value'] = \
            db['data'].loc[chunk_mask, 'value'] * 2
        db['data'].loc[chunk_mask, 'processed'] = True

        # Push chunk
        db.push()

    # Verify all chunks processed
    db = DataBase(engine)
    assert len(db['data'][db['data']['processed'] == True]) == 5000


def test_chunked_migration_pattern(tmp_path):
    """Test data migration in chunks."""
    db_path = tmp_path / "migration.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Source table
    source = pd.DataFrame({
        'id': range(1, 1001),
        'full_name': [f'User {i}' for i in range(1, 1001)],
        'value': range(1, 1001)
    })
    db.create_table('source', source, primary_key='id')

    # Destination table
    dest = pd.DataFrame({'id': [], 'first_name': [], 'last_name': [], 'value': []})
    db.create_table('dest', dest, primary_key='id')

    # Migrate in 4 chunks of 250 using vectorized operations
    chunk_size = 250

    for i in range(4):
        db = DataBase(engine)

        start_id = i * chunk_size + 1
        end_id = (i + 1) * chunk_size + 1

        # Get chunk (vectorized)
        chunk = db['source'].to_pandas().loc[start_id:end_id-1]

        # Transform (vectorized)
        chunk['first_name'] = chunk['full_name'].str.split(' ').str[0]
        chunk['last_name'] = chunk['full_name'].str.split(' ').str[1]
        chunk = chunk.drop('full_name', axis=1)

        # Bulk insert into destination
        chunk_records = chunk.reset_index().to_dict('records')
        db['dest'].bulk_insert(chunk_records)

        db['dest'].push()

    # Verify
    db = DataBase(engine)
    assert len(db['dest']) == 1000


def test_many_columns_operations(tmp_path):
    """Test operations on table with many columns."""
    db_path = tmp_path / "wide.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create table with 50 columns
    data_dict = {'id': [1, 2, 3]}
    for i in range(1, 51):
        data_dict[f'col_{i}'] = [i, i*2, i*3]

    data = pd.DataFrame(data_dict)
    db.create_table('wide_table', data, primary_key='id')

    # Add row (vectorized across columns)
    new_row = {'id': 4}
    for i in range(1, 51):
        new_row[f'col_{i}'] = i * 4
    db['wide_table'].add_row(new_row)

    # Update all rows for specific columns (vectorized)
    db['wide_table'].loc[:, 'col_25'] = 999

    db.push()

    # Verify
    db.pull()
    assert db['wide_table'].get_row(1)['col_25'] == 999


def test_many_tables_transaction(tmp_path):
    """Test transaction across many tables."""
    db_path = tmp_path / "many.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create 10 tables
    for i in range(1, 11):
        data = pd.DataFrame({
            'id': [1, 2],
            'value': [i * 10, i * 20]
        })
        db.create_table(f'table_{i}', data, primary_key='id')

    # Modify all (vectorized)
    for i in range(1, 11):
        db[f'table_{i}']['value'] = db[f'table_{i}']['value'] * 2

    # Push all at once
    start = time.time()
    db.push()
    elapsed = time.time() - start

    assert elapsed < 5.0

    # Verify
    db.pull()
    assert db['table_1'].get_row(1)['value'] == 20  # 10 * 2


def test_execution_plan_batching(tmp_path):
    """Test that execution plan batches efficiently."""
    db_path = tmp_path / "batching.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': range(1, 101),
        'value': [0] * 100
    })
    db.create_table('data', data, primary_key='id')

    # Update all rows (vectorized)
    db['data']['value'] = db['data'].index * 2

    # Check execution plan
    from pandalchemy.execution_plan import ExecutionPlan
    tracker = db['data'].get_tracker()
    plan = ExecutionPlan(tracker, db['data'].to_pandas())

    summary = plan.get_summary()
    # Should batch efficiently
    assert summary['total_steps'] < 10

    db.push()
    db.pull()
    assert db['data'].get_row(50)['value'] == 100

