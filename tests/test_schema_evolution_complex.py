"""Tests for complex schema evolution scenarios."""

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, TableDataFrame


def test_split_table_normalization(tmp_path):
    """Test splitting one table into two normalized tables."""
    db_path = tmp_path / "normalize.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Original denormalized table
    users = pd.DataFrame({
        'id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie'],
        'email': ['alice@test.com', 'bob@test.com', 'charlie@test.com'],
        'address_street': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'address_city': ['NYC', 'LA', 'Chicago'],
        'address_zip': ['10001', '90001', '60601']
    })
    db.create_table('users_old', users, primary_key='id')

    # Create normalized tables
    users_new = pd.DataFrame({
        'id': [],
        'username': [],
        'email': []
    })
    db.create_table('users', users_new, primary_key='id')

    addresses = pd.DataFrame({
        'id': [],
        'user_id': [],
        'street': [],
        'city': [],
        'zip_code': []
    })
    db.create_table('addresses', addresses, primary_key='id')

    # Migrate data
    for user_id in db['users_old'].index:
        user = db['users_old'].get_row(user_id)

        # Insert into users
        db['users'].add_row({
            'id': user_id,
            'username': user['username'],
            'email': user['email']
        })

        # Insert into addresses
        db['addresses'].add_row({
            'id': user_id,
            'user_id': user_id,
            'street': user['address_street'],
            'city': user['address_city'],
            'zip_code': user['address_zip']
        })

    db.push()

    # Verify normalization
    db.pull()
    assert len(db['users']) == 3
    assert len(db['addresses']) == 3
    assert 'address_street' not in db['users'].columns
    assert db['addresses'].get_row(1)['street'] == '123 Main St'


def test_merge_tables_denormalization(tmp_path):
    """Test that demonstrates proper way to merge data from different tables."""
    db_path = tmp_path / "denormalize.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Separate tables
    users = pd.DataFrame({
        'id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie']
    })
    db.create_table('users', users, primary_key='id')

    profiles = pd.DataFrame({
        'user_id': [1, 2, 3],
        'full_name': ['Alice Smith', 'Bob Jones', 'Charlie Brown'],
        'bio': ['Dev', 'PM', 'Designer']
    })
    db.create_table('profiles', profiles, primary_key='user_id')

    # Best approach: use pandas merge for denormalization
    users_df = db['users'].to_pandas().reset_index()
    profiles_df = db['profiles'].to_pandas().reset_index()

    # Merge in memory
    merged_df = users_df.merge(profiles_df, left_on='id', right_on='user_id')
    merged_df = merged_df[['id', 'username', 'full_name', 'bio']]
    merged_df = merged_df.set_index('id')

    # Create new table from merged data
    from pandalchemy import TableDataFrame
    merged_table = TableDataFrame('users_merged', merged_df, 'id', engine)
    merged_table.push()

    # Verify merge
    db_verify = DataBase(engine)
    assert len(db_verify['users_merged']) == 3
    merged_user = db_verify['users_merged'].get_row(1)
    assert merged_user['username'] == 'alice'
    assert merged_user['full_name'] == 'Alice Smith'
    assert merged_user['bio'] == 'Dev'


def test_column_splitting_transformation(tmp_path):
    """Test splitting a column into multiple columns."""
    db_path = tmp_path / "split.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Table with full_name
    users = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'full_name': ['Alice Smith', 'Bob Jones', 'Charlie Brown', 'Diana Prince'],
        'email': ['a@test.com', 'b@test.com', 'c@test.com', 'd@test.com']
    })
    db.create_table('users', users, primary_key='id')

    # Add first_name and last_name columns
    db['users'].add_column_with_default('first_name', '')
    db['users'].add_column_with_default('last_name', '')

    # Split full_name
    for user_id in db['users'].index:
        user = db['users'].get_row(user_id)
        names = user['full_name'].split(' ', 1)
        first = names[0]
        last = names[1] if len(names) > 1 else ''

        db['users'].update_row(user_id, {
            'first_name': first,
            'last_name': last
        })

    # Drop old column
    db['users'].drop_column_safe('full_name')

    db.push()

    # Verify split
    db.pull()
    assert 'full_name' not in db['users'].columns
    assert db['users'].get_row(1)['first_name'] == 'Alice'
    assert db['users'].get_row(1)['last_name'] == 'Smith'


def test_column_combining_transformation(tmp_path):
    """Test combining multiple columns into one."""
    db_path = tmp_path / "combine.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Table with split names
    users = pd.DataFrame({
        'id': [1, 2, 3],
        'first_name': ['Alice', 'Bob', 'Charlie'],
        'last_name': ['Smith', 'Jones', 'Brown'],
        'email': ['a@test.com', 'b@test.com', 'c@test.com']
    })
    db.create_table('users', users, primary_key='id')

    # Add combined column
    db['users'].add_column_with_default('full_name', '')

    # Combine names
    for user_id in db['users'].index:
        user = db['users'].get_row(user_id)
        full_name = f"{user['first_name']} {user['last_name']}"
        db['users'].update_row(user_id, {'full_name': full_name})

    # Drop old columns
    db['users'].drop_column_safe('first_name')
    db['users'].drop_column_safe('last_name')

    db.push()

    # Verify combination
    db.pull()
    assert 'first_name' not in db['users'].columns
    assert 'last_name' not in db['users'].columns
    assert db['users'].get_row(1)['full_name'] == 'Alice Smith'


def test_change_from_single_to_composite_pk(tmp_path):
    """Test changing from single PK to composite PK."""
    db_path = tmp_path / "pk_change.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Original table with single PK
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'user_id': [1, 1, 2],
        'org_id': ['org1', 'org2', 'org1'],
        'role': ['admin', 'user', 'user']
    })
    db.create_table('memberships', data, primary_key='id')

    # Create new table with composite PK
    memberships_new = pd.DataFrame({
        'user_id': [],
        'org_id': [],
        'role': []
    })
    new_table = TableDataFrame('memberships_new', memberships_new, ['user_id', 'org_id'], engine)

    # Migrate data (excluding old PK)
    for row_id in db['memberships'].index:
        row = db['memberships'].get_row(row_id)
        new_table.add_row({
            'user_id': row['user_id'],
            'org_id': row['org_id'],
            'role': row['role']
        })

    new_table.push()

    # Verify new structure
    db = DataBase(engine)
    assert isinstance(db['memberships_new'].index, pd.MultiIndex)
    assert list(db['memberships_new'].index.names) == ['user_id', 'org_id']


def test_change_from_composite_to_single_pk(tmp_path):
    """Test changing from composite PK to single PK."""
    db_path = tmp_path / "pk_change2.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Original table with composite PK
    data = pd.DataFrame({
        'user_id': [1, 1, 2],
        'org_id': ['org1', 'org2', 'org1'],
        'role': ['admin', 'user', 'user']
    })
    db.create_table('memberships', data, primary_key=['user_id', 'org_id'])

    # Create new table with synthetic PK
    memberships_new = pd.DataFrame({
        'id': [],
        'user_id': [],
        'org_id': [],
        'role': []
    })
    new_table = TableDataFrame('memberships_new', memberships_new, 'id', engine, auto_increment=True)

    # Migrate data with new synthetic PK
    for idx, (user_id, org_id) in enumerate(db['memberships'].index, 1):
        row = db['memberships'].get_row((user_id, org_id))
        new_table.add_row({
            'id': idx,
            'user_id': user_id,
            'org_id': org_id,
            'role': row['role']
        })

    new_table.push()

    # Verify new structure
    db = DataBase(engine)
    assert db['memberships_new'].index.name == 'id'
    assert 'user_id' in db['memberships_new'].columns
    assert 'org_id' in db['memberships_new'].columns


def test_add_computed_columns_from_existing_data(tmp_path):
    """Test adding computed columns based on existing data."""
    db_path = tmp_path / "computed.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Sales data
    sales = pd.DataFrame({
        'id': range(1, 101),
        'quantity': np.random.randint(1, 100, 100),
        'unit_price': np.random.uniform(10, 100, 100).round(2)
    })
    db.create_table('sales', sales, primary_key='id')

    # Add computed columns
    db['sales'].add_column_with_default('total', 0.0)
    db['sales'].add_column_with_default('tax', 0.0)
    db['sales'].add_column_with_default('grand_total', 0.0)

    # Calculate values
    tax_rate = 0.08
    for sale_id in db['sales'].index:
        sale = db['sales'].get_row(sale_id)
        total = sale['quantity'] * sale['unit_price']
        tax = total * tax_rate
        grand_total = total + tax

        db['sales'].update_row(sale_id, {
            'total': round(total, 2),
            'tax': round(tax, 2),
            'grand_total': round(grand_total, 2)
        })

    db.push()

    # Verify calculations
    db.pull()
    sample = db['sales'].get_row(1)
    expected = sample['quantity'] * sample['unit_price']
    assert abs(sample['total'] - expected) < 0.01


def test_progressive_schema_migration(tmp_path):
    """Test progressive schema migration with multiple steps."""
    db_path = tmp_path / "progressive.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Version 1: Initial schema
    users_v1 = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'email': ['alice@old.com', 'bob@old.com']
    })
    db.create_table('users', users_v1, primary_key='id')

    # Version 2: Add status column
    db['users'].add_column_with_default('status', 'active')
    db.push()

    # Version 3: Split name into first/last
    db.pull()
    db['users'].add_column_with_default('first_name', '')
    db['users'].add_column_with_default('last_name', '')

    for user_id in db['users'].index:
        user = db['users'].get_row(user_id)
        db['users'].update_row(user_id, {
            'first_name': user['name'],
            'last_name': ''
        })

    db['users'].drop_column_safe('name')
    db.push()

    # Version 4: Add timestamps
    db.pull()
    db['users'].add_column_with_default('created_at', str(pd.Timestamp.now()))
    db['users'].add_column_with_default('updated_at', str(pd.Timestamp.now()))
    db.push()

    # Verify final schema
    db.pull()
    assert 'name' not in db['users'].columns
    assert 'first_name' in db['users'].columns
    assert 'status' in db['users'].columns
    assert 'created_at' in db['users'].columns


def test_reorder_composite_pk_columns(tmp_path):
    """Test reordering composite PK columns."""
    db_path = tmp_path / "reorder.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Original: (org_id, user_id)
    data = pd.DataFrame({
        'org_id': ['org1', 'org2', 'org1'],
        'user_id': [1, 1, 2],
        'role': ['admin', 'user', 'user']
    })
    db.create_table('memberships_old', data, primary_key=['org_id', 'user_id'])

    # New: (user_id, org_id) - different order
    memberships_new = pd.DataFrame({
        'user_id': [],
        'org_id': [],
        'role': []
    })
    new_table = TableDataFrame('memberships', memberships_new, ['user_id', 'org_id'], engine)

    # Migrate with reordered PK
    for idx in db['memberships_old'].index:
        org_id, user_id = idx  # Original order
        row = db['memberships_old'].get_row(idx)

        new_table.add_row({
            'user_id': user_id,  # Swapped order
            'org_id': org_id,
            'role': row['role']
        })

    new_table.push()

    # Verify new order
    db = DataBase(engine)
    assert db['memberships'].index.names == ['user_id', 'org_id']
    # Access with new order
    assert db['memberships'].row_exists((1, 'org1'))


def test_type_migration_with_data_transformation(tmp_path):
    """Test that renaming a column added in same transaction raises error."""
    from pandalchemy.exceptions import TransactionError

    db_path = tmp_path / "type_migration.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Prices stored as integers (cents)
    products = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Widget', 'Gadget', 'Doohickey'],
        'price_cents': [1999, 2999, 3999]  # In cents
    })
    db.create_table('products', products, primary_key='id')

    # Add a new column
    db['products'].add_column_with_default('price_dollars', 0.0)

    # Transform cents to dollars
    for product_id in db['products'].index:
        product = db['products'].get_row(product_id)
        price_dollars = product['price_cents'] / 100.0
        db['products'].update_row(product_id, {'price_dollars': price_dollars})

    # Drop old column
    db['products'].drop_column_safe('price_cents')

    # Try to rename the newly added column in the same transaction
    # This should fail because the column doesn't exist in the database yet
    db['products'].rename_column_safe('price_dollars', 'price')

    # This should raise TransactionError because you can't rename a column
    # that was just added in the same transaction
    with pytest.raises(TransactionError, match="does not exist"):
        db.push()


def test_add_versioning_to_existing_table(tmp_path):
    """Test adding versioning columns to existing table with best practices."""
    db_path = tmp_path / "versioning.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Existing data without versioning
    documents = pd.DataFrame({
        'id': [1, 2, 3],
        'title': ['Doc 1', 'Doc 2', 'Doc 3'],
        'content': ['Content 1', 'Content 2', 'Content 3']
    })
    db.create_table('documents', documents, primary_key='id')

    # Best practice: Add columns and push immediately
    timestamp = str(pd.Timestamp.now())
    db['documents'].add_column_with_default('version', 1)
    db['documents'].add_column_with_default('updated_at', timestamp)
    db['documents'].add_column_with_default('updated_by', 'system')
    db['documents'].push()

    # Now create history table and populate it
    # Re-instantiate to get fresh schema
    db = DataBase(engine)

    # Create history table
    history_data = []
    for doc_id in db['documents'].index:
        doc_data = db['documents']._data.loc[doc_id]
        history_data.append({
            'id': doc_id,
            'document_id': doc_id,
            'version': int(doc_data['version']),
            'title': doc_data['title'],
            'content': doc_data['content'],
            'updated_at': doc_data['updated_at'],
            'updated_by': doc_data['updated_by']
        })

    history_df = pd.DataFrame(history_data)
    from pandalchemy import TableDataFrame
    history_table = TableDataFrame('document_history', history_df, 'id', engine)
    history_table.push()

    # Verify versioning added
    db = DataBase(engine)
    assert 'version' in db['documents'].columns
    assert 'updated_at' in db['documents'].columns
    assert len(db['document_history']) == 3

    # Verify the history data
    assert db['document_history'].get_row(1)['version'] == 1
    assert db['document_history'].get_row(1)['title'] == 'Doc 1'


def test_denormalize_for_read_performance(tmp_path):
    """Test denormalizing data for read performance."""
    db_path = tmp_path / "denorm_perf.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Normalized: users and posts
    users = pd.DataFrame({
        'id': [1, 2],
        'username': ['alice', 'bob']
    })
    db.create_table('users', users, primary_key='id')

    posts = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'user_id': [1, 1, 2, 2],
        'title': ['Post 1', 'Post 2', 'Post 3', 'Post 4']
    })
    db.create_table('posts', posts, primary_key='id')

    # Add denormalized username to posts for faster reads
    db['posts'].add_column_with_default('username', '')

    # Populate username
    for post_id in db['posts'].index:
        post = db['posts'].get_row(post_id)
        user = db['users'].get_row(post['user_id'])
        db['posts'].update_row(post_id, {'username': user['username']})

    db.push()

    # Verify denormalization
    db.pull()
    assert db['posts'].get_row(1)['username'] == 'alice'
    assert db['posts'].get_row(3)['username'] == 'bob'

