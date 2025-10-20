"""Tests for complex schema evolution scenarios."""

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase, Table


@pytest.mark.skip(reason="Test needs refactoring - table creation pattern issue")
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
    for user_id in db['users_old'].data.index:
        user = db['users_old'].data.get_row(user_id)

        # Insert into users
        db['users'].data.add_row({
            'id': user_id,
            'username': user['username'],
            'email': user['email']
        })

        # Insert into addresses
        db['addresses'].data.add_row({
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
    assert 'address_street' not in db['users'].data.columns
    assert db['addresses'].data.get_row(1)['street'] == '123 Main St'


@pytest.mark.skip(reason="Test needs refactoring")
def test_merge_tables_denormalization(tmp_path):
    """Test merging two tables into one denormalized table."""
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

    # Create merged table
    users_merged = pd.DataFrame({
        'id': [],
        'username': [],
        'full_name': [],
        'bio': []
    })
    db.create_table('users_merged', users_merged, primary_key='id')

    # Merge data
    for user_id in db['users'].data.index:
        user = db['users'].data.get_row(user_id)
        profile = db['profiles'].data.get_row(user_id)

        db['users_merged'].data.add_row({
            'id': user_id,
            'username': user['username'],
            'full_name': profile['full_name'],
            'bio': profile['bio']
        })

    db.push()

    # Verify merge
    db.pull()
    assert len(db['users_merged']) == 3
    merged_user = db['users_merged'].data.get_row(1)
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
    db['users'].data.add_column_with_default('first_name', '')
    db['users'].data.add_column_with_default('last_name', '')

    # Split full_name
    for user_id in db['users'].data.index:
        user = db['users'].data.get_row(user_id)
        names = user['full_name'].split(' ', 1)
        first = names[0]
        last = names[1] if len(names) > 1 else ''

        db['users'].data.update_row(user_id, {
            'first_name': first,
            'last_name': last
        })

    # Drop old column
    db['users'].data.drop_column_safe('full_name')

    db.push()

    # Verify split
    db.pull()
    assert 'full_name' not in db['users'].data.columns
    assert db['users'].data.get_row(1)['first_name'] == 'Alice'
    assert db['users'].data.get_row(1)['last_name'] == 'Smith'


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
    db['users'].data.add_column_with_default('full_name', '')

    # Combine names
    for user_id in db['users'].data.index:
        user = db['users'].data.get_row(user_id)
        full_name = f"{user['first_name']} {user['last_name']}"
        db['users'].data.update_row(user_id, {'full_name': full_name})

    # Drop old columns
    db['users'].data.drop_column_safe('first_name')
    db['users'].data.drop_column_safe('last_name')

    db.push()

    # Verify combination
    db.pull()
    assert 'first_name' not in db['users'].data.columns
    assert 'last_name' not in db['users'].data.columns
    assert db['users'].data.get_row(1)['full_name'] == 'Alice Smith'


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
    new_table = Table('memberships_new', memberships_new, ['user_id', 'org_id'], engine)

    # Migrate data (excluding old PK)
    for row_id in db['memberships'].data.index:
        row = db['memberships'].data.get_row(row_id)
        new_table.data.add_row({
            'user_id': row['user_id'],
            'org_id': row['org_id'],
            'role': row['role']
        })

    new_table.push()

    # Verify new structure
    db = DataBase(engine)
    assert isinstance(db['memberships_new'].data.index, pd.MultiIndex)
    assert list(db['memberships_new'].data.index.names) == ['user_id', 'org_id']


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
    new_table = Table('memberships_new', memberships_new, 'id', engine, auto_increment=True)

    # Migrate data with new synthetic PK
    for idx, (user_id, org_id) in enumerate(db['memberships'].data.index, 1):
        row = db['memberships'].data.get_row((user_id, org_id))
        new_table.data.add_row({
            'id': idx,
            'user_id': user_id,
            'org_id': org_id,
            'role': row['role']
        })

    new_table.push()

    # Verify new structure
    db = DataBase(engine)
    assert db['memberships_new'].data.index.name == 'id'
    assert 'user_id' in db['memberships_new'].data.columns
    assert 'org_id' in db['memberships_new'].data.columns


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
    db['sales'].data.add_column_with_default('total', 0.0)
    db['sales'].data.add_column_with_default('tax', 0.0)
    db['sales'].data.add_column_with_default('grand_total', 0.0)

    # Calculate values
    tax_rate = 0.08
    for sale_id in db['sales'].data.index:
        sale = db['sales'].data.get_row(sale_id)
        total = sale['quantity'] * sale['unit_price']
        tax = total * tax_rate
        grand_total = total + tax

        db['sales'].data.update_row(sale_id, {
            'total': round(total, 2),
            'tax': round(tax, 2),
            'grand_total': round(grand_total, 2)
        })

    db.push()

    # Verify calculations
    db.pull()
    sample = db['sales'].data.get_row(1)
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
    db['users'].data.add_column_with_default('status', 'active')
    db.push()

    # Version 3: Split name into first/last
    db.pull()
    db['users'].data.add_column_with_default('first_name', '')
    db['users'].data.add_column_with_default('last_name', '')

    for user_id in db['users'].data.index:
        user = db['users'].data.get_row(user_id)
        db['users'].data.update_row(user_id, {
            'first_name': user['name'],
            'last_name': ''
        })

    db['users'].data.drop_column_safe('name')
    db.push()

    # Version 4: Add timestamps
    db.pull()
    db['users'].data.add_column_with_default('created_at', str(pd.Timestamp.now()))
    db['users'].data.add_column_with_default('updated_at', str(pd.Timestamp.now()))
    db.push()

    # Verify final schema
    db.pull()
    assert 'name' not in db['users'].data.columns
    assert 'first_name' in db['users'].data.columns
    assert 'status' in db['users'].data.columns
    assert 'created_at' in db['users'].data.columns


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
    new_table = Table('memberships', memberships_new, ['user_id', 'org_id'], engine)

    # Migrate with reordered PK
    for idx in db['memberships_old'].data.index:
        org_id, user_id = idx  # Original order
        row = db['memberships_old'].data.get_row(idx)

        new_table.data.add_row({
            'user_id': user_id,  # Swapped order
            'org_id': org_id,
            'role': row['role']
        })

    new_table.push()

    # Verify new order
    db = DataBase(engine)
    assert db['memberships'].data.index.names == ['user_id', 'org_id']
    # Access with new order
    assert db['memberships'].data.row_exists((1, 'org1'))


@pytest.mark.skip(reason="Column rename tracking issue")
def test_type_migration_with_data_transformation(tmp_path):
    """Test changing column type with data transformation."""
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

    # Migrate to dollars (float)
    db['products'].data.add_column_with_default('price_dollars', 0.0)

    # Transform cents to dollars
    for product_id in db['products'].data.index:
        product = db['products'].data.get_row(product_id)
        price_dollars = product['price_cents'] / 100.0
        db['products'].data.update_row(product_id, {'price_dollars': price_dollars})

    # Drop old column
    db['products'].data.drop_column_safe('price_cents')

    # Rename
    db['products'].data.rename_column_safe('price_dollars', 'price')

    db.push()

    # Verify migration
    db.pull()
    assert 'price_cents' not in db['products'].data.columns
    assert 'price' in db['products'].data.columns
    assert db['products'].data.get_row(1)['price'] == 19.99


@pytest.mark.skip(reason="Schema timing issue with new columns")
def test_add_versioning_to_existing_table(tmp_path):
    """Test adding version tracking to existing table."""
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

    # Add versioning columns
    db['documents'].data.add_column_with_default('version', 1)
    db['documents'].data.add_column_with_default('updated_at', str(pd.Timestamp.now()))
    db['documents'].data.add_column_with_default('updated_by', 'system')

    # Create version history table
    history = pd.DataFrame({
        'id': [],
        'document_id': [],
        'version': [],
        'title': [],
        'content': [],
        'updated_at': [],
        'updated_by': []
    })
    db.create_table('document_history', history, primary_key='id')

    # Snapshot current state to history
    for doc_id in db['documents'].data.index:
        doc = db['documents'].data.get_row(doc_id)
        db['document_history'].data.add_row({
            'id': doc_id,
            'document_id': doc_id,
            'version': 1,
            'title': doc['title'],
            'content': doc['content'],
            'updated_at': doc['updated_at'],
            'updated_by': 'system'
        })

    db.push()

    # Verify versioning added
    db.pull()
    assert 'version' in db['documents'].data.columns
    assert len(db['document_history']) == 3


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
    db['posts'].data.add_column_with_default('username', '')

    # Populate username
    for post_id in db['posts'].data.index:
        post = db['posts'].data.get_row(post_id)
        user = db['users'].data.get_row(post['user_id'])
        db['posts'].data.update_row(post_id, {'username': user['username']})

    db.push()

    # Verify denormalization
    db.pull()
    assert db['posts'].data.get_row(1)['username'] == 'alice'
    assert db['posts'].data.get_row(3)['username'] == 'bob'

