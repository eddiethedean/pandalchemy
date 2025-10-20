"""Tests for the ExecutionPlan class."""

import pandas as pd
import pytest

from pandalchemy.change_tracker import ChangeTracker
from pandalchemy.execution_plan import ExecutionPlan, OperationType


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }).set_index('id')


def test_execution_plan_no_changes(sample_dataframe):
    """Test execution plan with no changes."""
    tracker = ChangeTracker('id', sample_dataframe)
    plan = ExecutionPlan(tracker, sample_dataframe)

    assert not plan.has_changes()
    assert len(plan.steps) == 0


def test_execution_plan_with_insert(sample_dataframe):
    """Test execution plan with row insertion."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Add a new row
    modified_df = sample_dataframe.copy()
    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40]
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])

    plan = ExecutionPlan(tracker, modified_df)

    assert plan.has_changes()
    insert_steps = plan.get_steps_by_type(OperationType.INSERT)
    assert len(insert_steps) == 1
    assert len(insert_steps[0].data) == 1  # One insert record


def test_execution_plan_with_update(sample_dataframe):
    """Test execution plan with row update."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Modify a row
    modified_df = sample_dataframe.copy()
    modified_df.loc[1, 'age'] = 26

    plan = ExecutionPlan(tracker, modified_df)

    assert plan.has_changes()
    update_steps = plan.get_steps_by_type(OperationType.UPDATE)
    assert len(update_steps) == 1
    assert len(update_steps[0].data) == 1  # One update record


def test_execution_plan_with_delete(sample_dataframe):
    """Test execution plan with row deletion."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Remove a row
    modified_df = sample_dataframe.copy()
    modified_df = modified_df.drop(2)

    plan = ExecutionPlan(tracker, modified_df)

    assert plan.has_changes()
    delete_steps = plan.get_steps_by_type(OperationType.DELETE)
    assert len(delete_steps) == 1
    assert 2 in delete_steps[0].data  # Primary key 2 was deleted


def test_execution_plan_with_column_add(sample_dataframe):
    """Test execution plan with column addition."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Add a new column
    modified_df = sample_dataframe.copy()
    modified_df['email'] = ['a@test.com', 'b@test.com', 'c@test.com']
    tracker.track_column_addition('email')

    plan = ExecutionPlan(tracker, modified_df)

    assert plan.has_changes()
    schema_steps = plan.get_steps_by_type(OperationType.SCHEMA_CHANGE)
    assert len(schema_steps) >= 1

    # Find the add column step
    add_steps = [s for s in schema_steps if s.data.change_type == 'add_column']
    assert len(add_steps) == 1
    assert add_steps[0].data.column_name == 'email'


def test_execution_plan_with_column_drop(sample_dataframe):
    """Test execution plan with column drop."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Drop a column
    modified_df = sample_dataframe.copy()
    modified_df = modified_df.drop('age', axis=1)
    tracker.track_column_drop('age')

    plan = ExecutionPlan(tracker, modified_df)

    assert plan.has_changes()
    schema_steps = plan.get_steps_by_type(OperationType.SCHEMA_CHANGE)
    assert len(schema_steps) >= 1

    # Find the drop column step
    drop_steps = [s for s in schema_steps if s.data.change_type == 'drop_column']
    assert len(drop_steps) == 1
    assert drop_steps[0].data.column_name == 'age'


def test_execution_plan_with_column_rename(sample_dataframe):
    """Test execution plan with column rename."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Rename a column
    modified_df = sample_dataframe.copy()
    modified_df = modified_df.rename(columns={'name': 'full_name'})
    tracker.track_column_rename('name', 'full_name')

    plan = ExecutionPlan(tracker, modified_df)

    assert plan.has_changes()
    schema_steps = plan.get_steps_by_type(OperationType.SCHEMA_CHANGE)
    assert len(schema_steps) >= 1

    # Find the rename column step
    rename_steps = [s for s in schema_steps if s.data.change_type == 'rename_column']
    assert len(rename_steps) == 1
    assert rename_steps[0].data.column_name == 'name'
    assert rename_steps[0].data.new_column_name == 'full_name'


def test_execution_plan_priority_order(sample_dataframe):
    """Test that steps are ordered by priority."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Make multiple types of changes
    modified_df = sample_dataframe.copy()

    # Schema change
    modified_df['email'] = ['a@test.com', 'b@test.com', 'c@test.com']
    tracker.track_column_addition('email')

    # Delete
    modified_df = modified_df.drop(1)

    # Insert
    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40],
        'email': ['d@test.com']
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])

    # Update
    modified_df.loc[2, 'age'] = 31

    plan = ExecutionPlan(tracker, modified_df)

    # Verify steps are in correct order
    # Schema changes should come first (priority 1-3)
    # Deletes next (priority 10)
    # Updates next (priority 20)
    # Inserts last (priority 30)

    priorities = [step.priority for step in plan.steps]
    assert priorities == sorted(priorities)

    # Check that schema changes come before data changes
    if len(plan.steps) > 1:
        # At least one of the early steps should be schema change
        assert any(s.operation_type == OperationType.SCHEMA_CHANGE
                  for s in plan.steps[:len(plan.steps)//2 + 1])


def test_execution_plan_summary(sample_dataframe):
    """Test execution plan summary."""
    tracker = ChangeTracker('id', sample_dataframe)

    # Make various changes
    modified_df = sample_dataframe.copy()
    modified_df['email'] = ['a@test.com', 'b@test.com', 'c@test.com']
    tracker.track_column_addition('email')

    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40],
        'email': ['d@test.com']
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])

    plan = ExecutionPlan(tracker, modified_df)
    summary = plan.get_summary()

    assert 'total_steps' in summary
    assert 'schema_changes' in summary
    assert 'insert_operations' in summary
    assert summary['total_steps'] > 0


def test_execution_plan_repr(sample_dataframe):
    """Test execution plan string representation."""
    tracker = ChangeTracker('id', sample_dataframe)

    modified_df = sample_dataframe.copy()
    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40]
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])

    plan = ExecutionPlan(tracker, modified_df)

    repr_str = repr(plan)
    assert 'ExecutionPlan' in repr_str
    assert 'insert' in repr_str.lower()


def test_execution_plan_multiple_operations(sample_dataframe):
    """Test execution plan with multiple operations of each type."""
    tracker = ChangeTracker('id', sample_dataframe)

    modified_df = sample_dataframe.copy()

    # Add multiple columns
    modified_df['email'] = ['a@test.com', 'b@test.com', 'c@test.com']
    modified_df['phone'] = ['111', '222', '333']
    tracker.track_column_addition('email')
    tracker.track_column_addition('phone')

    # Delete multiple rows
    modified_df = modified_df.drop([1, 2])

    # Insert multiple rows
    new_rows = pd.DataFrame({
        'name': ['David', 'Eve'],
        'age': [40, 45],
        'email': ['d@test.com', 'e@test.com'],
        'phone': ['444', '555']
    }, index=[4, 5])
    new_rows.index.name = 'id'
    modified_df = pd.concat([modified_df, new_rows])

    plan = ExecutionPlan(tracker, modified_df)

    # Verify all operations are captured
    summary = plan.get_summary()
    assert summary['schema_changes'] >= 2  # At least 2 column additions
    assert summary['insert_operations'] >= 1
    assert summary['delete_operations'] >= 1


def test_get_steps_by_type(sample_dataframe):
    """Test filtering steps by operation type."""
    tracker = ChangeTracker('id', sample_dataframe)

    modified_df = sample_dataframe.copy()
    modified_df['email'] = ['a@test.com', 'b@test.com', 'c@test.com']
    tracker.track_column_addition('email')

    new_row = pd.DataFrame({
        'name': ['David'],
        'age': [40],
        'email': ['d@test.com']
    }, index=[4])
    new_row.index.name = 'id'
    modified_df = pd.concat([modified_df, new_row])

    plan = ExecutionPlan(tracker, modified_df)

    # Get schema changes
    schema_steps = plan.get_steps_by_type(OperationType.SCHEMA_CHANGE)
    assert all(s.operation_type == OperationType.SCHEMA_CHANGE for s in schema_steps)

    # Get inserts
    insert_steps = plan.get_steps_by_type(OperationType.INSERT)
    assert all(s.operation_type == OperationType.INSERT for s in insert_steps)

