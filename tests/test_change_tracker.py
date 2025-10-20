"""Tests for the ChangeTracker class."""

import numpy as np
import pandas as pd
import pytest

from pandalchemy.change_tracker import ChangeTracker, ChangeType


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }).set_index('id')


def test_tracker_initialization(sample_dataframe):
    """Test ChangeTracker initialization."""
    tracker = ChangeTracker('id', sample_dataframe)

    assert tracker.primary_key == 'id'
    assert len(tracker.operations) == 0
    assert len(tracker.row_changes) == 0
    assert len(tracker.added_columns) == 0
    assert len(tracker.dropped_columns) == 0


def test_record_operation(sample_dataframe):
    """Test operation recording."""
    tracker = ChangeTracker('id', sample_dataframe)

    tracker.record_operation('test_method', 'arg1', kwarg1='value1')

    assert len(tracker.operations) == 1
    assert tracker.operations[0].method_name == 'test_method'
    assert tracker.operations[0].args == ('arg1',)
    assert tracker.operations[0].kwargs == {'kwarg1': 'value1'}


def test_track_column_addition(sample_dataframe):
    """Test column addition tracking."""
    tracker = ChangeTracker('id', sample_dataframe)

    tracker.track_column_addition('email')

    assert 'email' in tracker.added_columns
    assert 'email' not in tracker.dropped_columns


def test_track_column_drop(sample_dataframe):
    """Test column drop tracking."""
    tracker = ChangeTracker('id', sample_dataframe)

    tracker.track_column_drop('age')

    assert 'age' in tracker.dropped_columns
    assert 'age' not in tracker.added_columns


def test_track_column_rename(sample_dataframe):
    """Test column rename tracking."""
    tracker = ChangeTracker('id', sample_dataframe)

    tracker.track_column_rename('name', 'full_name')

    assert tracker.renamed_columns['name'] == 'full_name'


def test_compute_row_changes_insert(sample_dataframe):
    """Test detecting row insertions."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Add a new row
    modified_df = sample_dataframe.copy()
    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40]
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])

    tracker.compute_row_changes(modified_df)

    inserts = tracker.get_inserts()
    assert len(inserts) == 1
    assert inserts[0].primary_key_value == 4
    assert inserts[0].change_type == ChangeType.INSERT


def test_compute_row_changes_delete(sample_dataframe):
    """Test detecting row deletions."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Remove a row
    modified_df = sample_dataframe.copy()
    modified_df = modified_df.drop(2)

    tracker.compute_row_changes(modified_df)

    deletes = tracker.get_deletes()
    assert len(deletes) == 1
    assert deletes[0].primary_key_value == 2
    assert deletes[0].change_type == ChangeType.DELETE


def test_compute_row_changes_update(sample_dataframe):
    """Test detecting row updates."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Modify a row
    modified_df = sample_dataframe.copy()
    modified_df.loc[1, 'age'] = 26

    tracker.compute_row_changes(modified_df)

    updates = tracker.get_updates()
    assert len(updates) == 1
    assert updates[0].primary_key_value == 1
    assert updates[0].change_type == ChangeType.UPDATE
    assert updates[0].new_data['age'] == 26


def test_has_changes_empty(sample_dataframe):
    """Test has_changes with no changes."""
    tracker = ChangeTracker('id', sample_dataframe)

    assert not tracker.has_changes()


def test_has_changes_with_changes(sample_dataframe):
    """Test has_changes with tracked changes."""
    tracker = ChangeTracker('id', sample_dataframe)

    tracker.track_column_addition('email')

    assert tracker.has_changes()


def test_reset(sample_dataframe):
    """Test resetting the tracker."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Make some changes
    tracker.track_column_addition('email')
    tracker.record_operation('test_method')

    # Create new DataFrame
    new_df = pd.DataFrame({
        'id': [10, 20],
        'value': [100, 200]
    }).set_index('id')

    # Reset
    tracker.reset(new_df)

    assert len(tracker.operations) == 0
    assert len(tracker.row_changes) == 0
    assert len(tracker.added_columns) == 0
    assert not tracker.has_changes()


def test_get_summary(sample_dataframe):
    """Test getting change summary."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Make various changes
    tracker.track_column_addition('email')
    tracker.track_column_drop('age')
    tracker.record_operation('test_method')

    # Add a row
    modified_df = sample_dataframe.copy()
    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40]
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])
    tracker.compute_row_changes(modified_df)

    summary = tracker.get_summary()

    assert summary['total_operations'] == 1
    assert summary['inserts'] == 1
    assert summary['columns_added'] == 1
    assert summary['columns_dropped'] == 1
    assert summary['has_changes'] is True


def test_compute_row_changes_with_nan(sample_dataframe):
    """Test row change detection with NaN values."""
    df_with_nan = sample_dataframe.copy()
    df_with_nan.loc[2, 'age'] = np.nan

    tracker = ChangeTracker('id', df_with_nan)

    # DataFrame with same NaN should not show as update
    modified_df = df_with_nan.copy()
    tracker.compute_row_changes(modified_df)

    updates = tracker.get_updates()
    assert len(updates) == 0


def test_multiple_row_changes(sample_dataframe):
    """Test tracking multiple types of row changes."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Create modified DataFrame with all types of changes
    modified_df = sample_dataframe.copy()

    # Delete row 1
    modified_df = modified_df.drop(1)

    # Update row 2
    modified_df.loc[2, 'age'] = 31

    # Insert row 4
    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40]
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])

    tracker.compute_row_changes(modified_df)

    assert len(tracker.get_inserts()) == 1
    assert len(tracker.get_updates()) == 1
    assert len(tracker.get_deletes()) == 1
    assert tracker.has_changes()

