"""Tests for primary key as index behavior."""

import pandas as pd
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame


def test_single_pk_becomes_index_on_pull(tmp_path):
    """Test that single column PK becomes index when pulling from database."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create and push a table
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'age': [25, 30, 35]})
    table = TableDataFrame('users', df, 'id', engine)
    table.push()

    # Pull and verify PK is index
    db = DataBase(engine)
    assert db['users'].to_pandas().index.name == 'id'
    assert 'id' not in db['users'].columns
    assert list(db['users'].to_pandas().index) == [1, 2, 3]


def test_composite_pk_becomes_multiindex_on_pull(tmp_path):
    """Test that composite PK becomes MultiIndex when pulling from database."""
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table with composite PK
    df = pd.DataFrame({
        'user_id': ['u1', 'u1', 'u2'],
        'org_id': ['org1', 'org2', 'org1'],
        'role': ['admin', 'user', 'user']
    })

    table = TableDataFrame('memberships', df, ['user_id', 'org_id'], engine)
    table.push()

    # Pull and verify composite PK is MultiIndex
    db = DataBase(engine)
    pulled_df = db['memberships'].to_pandas()

    assert isinstance(pulled_df.index, pd.MultiIndex)
    assert list(pulled_df.index.names) == ['user_id', 'org_id']
    assert 'user_id' not in pulled_df.columns
    assert 'org_id' not in pulled_df.columns


def test_set_primary_key_moves_column_to_index():
    """Test that set_primary_key moves column to index."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'user_id': ['u1', 'u2', 'u3'],
        'name': ['Alice', 'Bob', 'Charlie']
    })

    tdf = TableDataFrame(data=df, primary_key='id')

    # Initially id might be in columns
    tdf.to_pandas()

    # Change PK to user_id
    tdf.set_primary_key('user_id')

    # Verify user_id is now the index
    result_df = tdf.to_pandas()
    assert result_df.index.name == 'user_id'
    assert 'user_id' not in result_df.columns
    assert list(result_df.index) == ['u1', 'u2', 'u3']


def test_set_primary_key_creates_multiindex():
    """Test that set_primary_key with list creates MultiIndex."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'user_id': ['u1', 'u2', 'u3'],
        'org_id': ['org1', 'org1', 'org2'],
        'role': ['admin', 'user', 'admin']
    })

    tdf = TableDataFrame(data=df, primary_key='id')

    # Change to composite PK
    tdf.set_primary_key(['user_id', 'org_id'])

    # Verify MultiIndex was created
    result_df = tdf.to_pandas()
    assert isinstance(result_df.index, pd.MultiIndex)
    assert list(result_df.index.names) == ['user_id', 'org_id']
    assert 'user_id' not in result_df.columns
    assert 'org_id' not in result_df.columns
    assert 'role' in result_df.columns  # Non-PK column remains


def test_set_primary_key_resets_old_index():
    """Test that set_primary_key resets the old index to columns."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'user_id': ['u1', 'u2', 'u3'],
        'name': ['Alice', 'Bob', 'Charlie']
    })

    # Start with id as PK (in index)
    df_indexed = df.set_index('id')
    tdf = TableDataFrame(data=df_indexed, primary_key='id')

    # Verify id is the index
    assert tdf.to_pandas().index.name == 'id'
    assert 'id' not in tdf.to_pandas().columns

    # Change PK to user_id
    tdf.set_primary_key('user_id')

    # Verify: user_id is now index, id is back as column
    result_df = tdf.to_pandas()
    assert result_df.index.name == 'user_id'
    assert 'id' in result_df.columns  # Old index became column
    assert 'user_id' not in result_df.columns


def test_crud_operations_work_with_index_based_pk(tmp_path):
    """Test that CRUD operations work when PK is in index."""
    db_path = tmp_path / "test.db"
    create_engine(f"sqlite:///{db_path}")

    # Create table with PK in index
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }).set_index('id')

    tdf = TableDataFrame(data=df, primary_key='id')

    # Test add_row
    tdf.add_row({'id': 4, 'name': 'Dave', 'age': 40})
    assert tdf.row_exists(4)

    # Test update_row
    tdf.update_row(2, {'age': 31})
    assert tdf.get_row(2)['age'] == 31

    # Test delete_row
    tdf.delete_row(3)
    assert not tdf.row_exists(3)

    # Test get_row
    row = tdf.get_row(1)
    assert row['name'] == 'Alice'


def test_pull_table_with_composite_key(tmp_path):
    """Test pull_table function with composite primary key."""
    from pandalchemy.sql_operations import pull_table

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table
    df = pd.DataFrame({
        'user_id': ['u1', 'u2', 'u3'],
        'org_id': ['org1', 'org1', 'org2'],
        'role': ['admin', 'user', 'admin']
    })
    df.to_sql('memberships', engine, index=False, if_exists='replace')

    # Pull with composite PK as index
    result = pull_table(
        engine,
        'memberships',
        primary_key=['user_id', 'org_id'],
        set_index=True
    )

    # Verify MultiIndex
    assert isinstance(result.index, pd.MultiIndex)
    assert list(result.index.names) == ['user_id', 'org_id']
    assert 'user_id' not in result.columns
    assert 'org_id' not in result.columns


def test_pull_table_with_set_index_false(tmp_path):
    """Test pull_table with set_index=False keeps PK as column."""
    from pandalchemy.sql_operations import pull_table

    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")

    # Create table
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
    df.to_sql('users', engine, index=False, if_exists='replace')

    # Pull WITHOUT setting as index
    result = pull_table(engine, 'users', primary_key='id', set_index=False)

    # Verify PK is still in columns
    assert 'id' in result.columns
    assert result.index.name is None


def test_reset_index_provides_pk_as_column():
    """Test that reset_index() can convert index back to columns."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    }).set_index('id')

    tdf = TableDataFrame(data=df, primary_key='id')

    # Get as regular DataFrame with PK as column
    df_with_pk = tdf.to_pandas().reset_index()

    assert 'id' in df_with_pk.columns
    assert df_with_pk.index.name is None
    assert list(df_with_pk['id']) == [1, 2, 3]


def test_table_operations_preserve_index():
    """Test that table operations preserve PK in index."""
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }).set_index('id')

    tdf = TableDataFrame(data=df, primary_key='id')

    # Add a column
    tdf['email'] = ['a@test.com', 'b@test.com', 'c@test.com']

    # Verify index is still 'id'
    assert tdf.to_pandas().index.name == 'id'
    assert 'id' not in tdf.columns
    assert 'email' in tdf.columns

