"""Tests for data integrity and edge cases."""

import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase
from pandalchemy.exceptions import DataValidationError, SchemaError


def test_null_handling_various_patterns(tmp_path):
    """Test handling of nulls in various scenarios."""
    db_path = tmp_path / "nulls.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create table with mixed null/non-null data
    data = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Alice', None, 'Charlie', ''],
        'age': [25, 30, None, 40],
        'email': [None, 'bob@test.com', None, 'd@test.com']
    })
    db.create_table('users', data, primary_key='id')

    # Add row with nulls
    db['users'].add_row({
        'id': 5,
        'name': None,
        'age': None,
        'email': None
    })

    # Update null to value
    db['users'].update_row(2, {'name': 'Bob'})

    # Update value to null
    db['users'].update_row(4, {'email': None})

    db.push()

    # Verify null handling
    db.pull()
    assert db['users'].get_row(2)['name'] == 'Bob'
    assert pd.isna(db['users'].get_row(4)['email'])
    assert pd.isna(db['users'].get_row(5)['name'])


def test_null_in_composite_pk_fails(tmp_path):
    """Test that null in composite PK fails validation."""
    db_path = tmp_path / "nullpk.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'user_id': [1, 2],
        'org_id': ['org1', 'org2'],
        'role': ['admin', 'user']
    })
    db.create_table('memberships', data, primary_key=['user_id', 'org_id'])

    # Try to add row with null in PK
    with pytest.raises(DataValidationError):
        db['memberships'].add_row({
            'user_id': None,
            'org_id': 'org3',
            'role': 'guest'
        })


def test_duplicate_pk_detection_single_column(tmp_path):
    """Test duplicate primary key detection for single column."""
    db_path = tmp_path / "dup.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    db.create_table('users', data, primary_key='id')

    # Try to add duplicate PK
    with pytest.raises(DataValidationError, match="already exists"):
        db['users'].add_row({
            'id': 2,  # Duplicate!
            'name': 'Bob2'
        })


def test_duplicate_pk_detection_composite(tmp_path):
    """Test duplicate primary key detection for composite keys."""
    db_path = tmp_path / "dup_comp.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'user_id': [1, 1, 2],
        'org_id': ['org1', 'org2', 'org1'],
        'role': ['admin', 'user', 'user']
    })
    db.create_table('memberships', data, primary_key=['user_id', 'org_id'])

    # Try to add duplicate composite PK
    with pytest.raises(DataValidationError, match="already exists"):
        db['memberships'].add_row({
            'user_id': 1,
            'org_id': 'org1',  # Duplicate combination!
            'role': 'guest'
        })


def test_bulk_insert_with_duplicates_fails(tmp_path):
    """Test that bulk insert with duplicates fails."""
    db_path = tmp_path / "bulk_dup.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob']
    })
    db.create_table('users', data, primary_key='id')

    # Try bulk insert with internal duplicates
    rows = [
        {'id': 3, 'name': 'Charlie'},
        {'id': 4, 'name': 'Diana'},
        {'id': 3, 'name': 'Charlie2'}  # Duplicate in batch!
    ]

    with pytest.raises(DataValidationError, match="duplicate"):
        db['users'].bulk_insert(rows)


def test_unicode_and_special_characters(tmp_path):
    """Test handling of unicode and special characters."""
    db_path = tmp_path / "unicode.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': [
            'Alice Smith',
            '王小明',  # Chinese characters
            'José García',  # Spanish accents
            'Σωκράτης',  # Greek
            '🎉 Party! 🎊'  # Emojis
        ],
        'description': [
            'Normal text',
            'Special chars: @#$%^&*()',
            "Quotes: 'single' \"double\"",
            'Newline\nTab\tChars',
            'Very long text ' + 'x' * 1000
        ]
    })
    db.create_table('users', data, primary_key='id')

    # Add more unicode
    db['users'].add_row({
        'id': 6,
        'name': 'Владимир',  # Cyrillic
        'description': '→←↑↓'  # Arrows
    })

    db.push()

    # Verify unicode preserved
    db.pull()
    assert db['users'].get_row(2)['name'] == '王小明'
    assert db['users'].get_row(5)['name'] == '🎉 Party! 🎊'
    assert db['users'].get_row(6)['name'] == 'Владимир'


def test_very_large_numbers(tmp_path):
    """Test handling of very large numbers."""
    db_path = tmp_path / "numbers.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2, 3],
        'small_int': [1, -1, 0],
        'large_int': [2**31 - 1, -(2**31), 2**30],
        'huge_int': [2**63 - 1, -(2**63), 0],
        'small_float': [0.000001, -0.000001, 1e-10],
        'large_float': [1e10, -1e10, 1e15]
    })
    db.create_table('numbers', data, primary_key='id')

    # Add row with extreme values
    db['numbers'].add_row({
        'id': 4,
        'small_int': 127,
        'large_int': 2**31 - 2,
        'huge_int': 2**62,
        'small_float': 1e-15,
        'large_float': 1e20
    })

    db.push()

    # Verify large numbers preserved
    db.pull()
    assert db['numbers'].get_row(1)['large_int'] == 2**31 - 1
    assert db['numbers'].get_row(2)['huge_int'] == -(2**63)


def test_empty_strings_vs_nulls(tmp_path):
    """Test distinction between empty strings and nulls."""
    db_path = tmp_path / "strings.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'value': ['', None, 'text', '   ']  # Empty, null, normal, whitespace
    })
    db.create_table('strings', data, primary_key='id')

    # Add various string edge cases
    db['strings'].add_row({'id': 5, 'value': ''})
    db['strings'].add_row({'id': 6, 'value': None})

    # Update empty to null and vice versa
    db['strings'].update_row(1, {'value': None})
    db['strings'].update_row(2, {'value': ''})

    db.push()

    # Verify distinction maintained
    db.pull()
    assert pd.isna(db['strings'].get_row(1)['value'])
    assert db['strings'].get_row(2)['value'] == ''
    assert pd.isna(db['strings'].get_row(6)['value'])


def test_boolean_type_handling(tmp_path):
    """Test boolean type handling - demonstrates NaN limitation."""
    from pandalchemy.exceptions import TransactionError

    db_path = tmp_path / "bools.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create table with proper boolean types (no None for initial data)
    data = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'active': [True, False, True, False],
        'verified': [False, False, True, True],
        'premium': [False, True, False, False]
    })
    db.create_table('flags', data, primary_key='id')

    # Toggle boolean values - this works fine
    db['flags'].update_row(1, {'active': False})
    db['flags'].update_row(2, {'active': True})
    db['flags'].update_row(4, {'active': True})

    db.push()

    # Verify boolean handling
    db.pull()
    assert db['flags'].get_row(1)['active'] == False
    assert db['flags'].get_row(2)['active'] == True
    assert db['flags'].get_row(4)['active'] == True

    # Now demonstrate the NaN limitation
    # Setting boolean to None creates NaN which SQLAlchemy rejects
    db['flags'].update_row(3, {'premium': None})

    # This should raise TransactionError due to NaN in boolean column
    with pytest.raises(TransactionError, match="Not a boolean value"):
        db.push()


def test_dropping_pk_column_fails_validation(tmp_path):
    """Test that dropping PK column fails validation."""
    db_path = tmp_path / "drop_pk.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    db.create_table('users', data, primary_key='id')

    # Try to drop PK column (need to reset index first to access it)
    db['users']._data = db['users']._data.reset_index()
    db['users'].drop_column_safe('id')

    # Validation should fail
    errors = db['users'].validate_data()
    assert len(errors) > 0
    assert any('dropped' in err.lower() for err in errors)

    # Push should fail
    with pytest.raises(SchemaError, match="Primary key"):
        db['users'].push()


def test_upsert_duplicate_handling(tmp_path):
    """Test upsert with duplicate scenarios."""
    db_path = tmp_path / "upsert.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie'],
        'score': [100, 200, 300]
    })
    db.create_table('users', data, primary_key='id')

    # Upsert existing (should update)
    db['users'].upsert_row({
        'id': 2,
        'username': 'bob_updated',
        'score': 250
    })

    # Upsert new (should insert)
    db['users'].upsert_row({
        'id': 4,
        'username': 'diana',
        'score': 400
    })

    db.push()

    # Verify
    db.pull()
    assert db['users'].get_row(2)['username'] == 'bob_updated'
    assert db['users'].get_row(2)['score'] == 250
    assert db['users'].row_exists(4)
    assert db['users'].get_row(4)['username'] == 'diana'


def test_data_type_conversions_edge_cases(tmp_path):
    """Test edge cases in data type conversions."""
    db_path = tmp_path / "types.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create table with various types
    data = pd.DataFrame({
        'id': [1],
        'int_val': [42],
        'float_val': [3.14159],
        'str_val': ['hello'],
        'bool_val': [True]
    })
    db.create_table('types', data, primary_key='id')

    # Change column types via data replacement
    db['types'].add_row({
        'id': 2,
        'int_val': 999,
        'float_val': 2.71828,
        'str_val': '12345',  # String that looks like number
        'bool_val': False
    })

    # Add row with type coercion edge cases
    db['types'].add_row({
        'id': 3,
        'int_val': 0,
        'float_val': 0.0,
        'str_val': '',
        'bool_val': False
    })

    db.push()

    # Verify types preserved
    db.pull()
    assert isinstance(db['types'].get_row(2)['int_val'], (int, pd.Int64Dtype))
    assert db['types'].get_row(2)['str_val'] == '12345'
    assert db['types'].get_row(3)['int_val'] == 0
    assert db['types'].get_row(3)['float_val'] == 0.0


def test_constraint_violation_scenarios(tmp_path):
    """Test various constraint violation scenarios."""
    db_path = tmp_path / "constraints.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create users
    users = pd.DataFrame({
        'id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie'],
        'email': ['a@test.com', 'b@test.com', 'c@test.com']
    })
    db.create_table('users', users, primary_key='id')

    # Create posts referencing users
    posts = pd.DataFrame({
        'id': [1, 2],
        'user_id': [1, 2],
        'title': ['Post 1', 'Post 2']
    })
    db.create_table('posts', posts, primary_key='id')

    # Creating orphaned reference (no FK constraint enforced by pandalchemy)
    # This would violate FK in a real DB with constraints
    db['posts'].add_row({
        'id': 3,
        'user_id': 999,  # Non-existent user
        'title': 'Orphaned Post'
    })

    # This succeeds in pandalchemy (no FK enforcement)
    # but documents the pattern
    db.push()

    db.pull()
    assert db['posts'].row_exists(3)
    assert db['posts'].get_row(3)['user_id'] == 999


def test_partial_composite_pk_null_fails(tmp_path):
    """Test that partial null in composite PK fails."""
    db_path = tmp_path / "partial_null.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'user_id': [1, 2],
        'org_id': ['org1', 'org2'],
        'role': ['admin', 'user']
    })
    db.create_table('memberships', data, primary_key=['user_id', 'org_id'])

    # Try to add with one PK column null
    with pytest.raises(DataValidationError):
        db['memberships'].add_row({
            'user_id': 3,
            'org_id': None,  # Null in PK!
            'role': 'guest'
        })


def test_string_length_edge_cases(tmp_path):
    """Test very long strings and edge cases."""
    db_path = tmp_path / "strings.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1],
        'short': ['a'],
        'medium': ['x' * 100],
        'long': ['y' * 10000],
        'empty': ['']
    })
    db.create_table('strings', data, primary_key='id')

    # Add very long string
    db['strings'].add_row({
        'id': 2,
        'short': '',
        'medium': 'z' * 50,
        'long': 'w' * 50000,  # 50K characters
        'empty': ''
    })

    db.push()

    # Verify length preserved
    db.pull()
    assert len(db['strings'].get_row(1)['long']) == 10000
    assert len(db['strings'].get_row(2)['long']) == 50000


def test_date_edge_cases(tmp_path):
    """Test date/datetime edge cases."""
    db_path = tmp_path / "dates.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'event_date': [
            '1900-01-01',  # Very old date
            '2099-12-31',  # Far future
            '2024-02-29',  # Leap year
            str(pd.Timestamp.now())
        ]
    })
    db.create_table('events', data, primary_key='id')

    # Add edge case dates
    db['events'].add_row({
        'id': 5,
        'event_date': '1970-01-01'  # Unix epoch
    })

    db.push()

    # Verify dates
    db.pull()
    assert db['events'].get_row(1)['event_date'] == '1900-01-01'
    assert db['events'].get_row(5)['event_date'] == '1970-01-01'


def test_mixed_type_columns(tmp_path):
    """Test columns with mixed types (object dtype)."""
    db_path = tmp_path / "mixed.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create with mixed types (will be object dtype)
    data = pd.DataFrame({
        'id': [1, 2, 3],
        'value': [42, 'text', True]  # Mixed types!
    })
    db.create_table('mixed', data, primary_key='id')

    db.push()

    # Verify mixed types handled (all become strings in object dtype)
    db.pull()
    assert str(db['mixed'].get_row(1)['value']) == '42'
    assert db['mixed'].get_row(2)['value'] == 'text'


def test_composite_pk_uniqueness_validation(tmp_path):
    """Test composite PK uniqueness across all columns."""
    db_path = tmp_path / "comp_unique.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    data = pd.DataFrame({
        'user_id': [1, 1, 2, 2],
        'org_id': ['org1', 'org2', 'org1', 'org2'],
        'role': ['admin', 'user', 'user', 'admin']
    })
    db.create_table('memberships', data, primary_key=['user_id', 'org_id'])

    # Same user_id but different org_id should work
    db['memberships'].add_row({
        'user_id': 1,
        'org_id': 'org3',  # New org
        'role': 'guest'
    })

    # Same org_id but different user_id should work
    db['memberships'].add_row({
        'user_id': 3,  # New user
        'org_id': 'org1',
        'role': 'member'
    })

    db.push()

    # Verify both added
    db.pull()
    assert db['memberships'].row_exists((1, 'org3'))
    assert db['memberships'].row_exists((3, 'org1'))


def test_cascading_nulls_in_updates(tmp_path):
    """Test updating related tables when setting values to null."""
    db_path = tmp_path / "cascade_null.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    users = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'manager_id': [None, 1]  # Bob reports to Alice
    })
    db.create_table('users', users, primary_key='id')

    # Remove manager relationship
    db['users'].update_row(2, {'manager_id': None})

    db.push()

    # Verify null set
    db.pull()
    assert pd.isna(db['users'].get_row(2)['manager_id'])

