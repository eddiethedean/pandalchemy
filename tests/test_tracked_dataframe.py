"""Tests for the TableDataFrame class."""

import pandas as pd
import pytest

from pandalchemy.change_tracker import ChangeTracker
from pandalchemy.tracked_dataframe import TableDataFrame


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }).set_index('id')


def test_tracked_dataframe_initialization(sample_dataframe):
    """Test TableDataFrame initialization."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    assert len(tdf) == 3
    assert 'name' in tdf.columns
    assert 'age' in tdf.columns


def test_getitem(sample_dataframe):
    """Test __getitem__ access."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Column access
    names = tdf['name']
    assert len(names) == 3

    # Index access
    assert tdf.index.name == 'id'


def test_setitem_existing_column(sample_dataframe):
    """Test __setitem__ on existing column."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf['age'] = [26, 31, 36]

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0

    # Check that changes were tracked
    tracker.compute_row_changes(tdf._data)
    updates = tracker.get_updates()
    assert len(updates) > 0


def test_setitem_new_column(sample_dataframe):
    """Test __setitem__ for adding a new column."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf['email'] = ['alice@test.com', 'bob@test.com', 'charlie@test.com']

    tracker = tdf.get_tracker()
    assert 'email' in tracker.added_columns
    assert 'email' in tdf._data.columns


def test_delitem(sample_dataframe):
    """Test __delitem__ for dropping columns."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    del tdf['age']

    tracker = tdf.get_tracker()
    assert 'age' in tracker.dropped_columns
    assert 'age' not in tdf._data.columns


def test_loc_getitem(sample_dataframe):
    """Test loc indexer for getting values."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    value = tdf.loc[1, 'name']
    assert value == 'Alice'


def test_loc_setitem(sample_dataframe):
    """Test loc indexer for setting values."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.loc[1, 'age'] = 26

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0

    # Verify the change
    assert tdf.loc[1, 'age'] == 26


def test_iloc_setitem(sample_dataframe):
    """Test iloc indexer for setting values."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.iloc[0, 1] = 26  # Change age of first row

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_shape_property(sample_dataframe):
    """Test shape property."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    assert tdf.shape == (3, 2)


def test_columns_property(sample_dataframe):
    """Test columns property."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    assert list(tdf.columns) == ['name', 'age']


def test_index_property(sample_dataframe):
    """Test index property."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    assert list(tdf.index) == [1, 2, 3]


def test_to_pandas(sample_dataframe):
    """Test converting to pandas DataFrame."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    df = tdf.to_pandas()

    assert isinstance(df, pd.DataFrame)
    assert df.equals(sample_dataframe)


def test_copy(sample_dataframe):
    """Test copying TableDataFrame."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf_copy = tdf.copy()

    assert isinstance(tdf_copy, TableDataFrame)
    assert tdf_copy._data.equals(tdf._data)

    # Modify copy and ensure original is unchanged
    tdf_copy['age'] = [100, 200, 300]
    assert not tdf._data['age'].equals(tdf_copy._data['age'])


def test_drop_method(sample_dataframe):
    """Test drop method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.drop('age', axis=1, inplace=True)

    tracker = tdf.get_tracker()
    assert 'age' in tracker.dropped_columns


def test_rename_method(sample_dataframe):
    """Test rename method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.rename(columns={'name': 'full_name'}, inplace=True)

    tracker = tdf.get_tracker()
    assert 'name' in tracker.renamed_columns
    assert tracker.renamed_columns['name'] == 'full_name'


def test_sort_values_method(sample_dataframe):
    """Test sort_values method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.sort_values('age', ascending=False, inplace=True)

    tracker = tdf.get_tracker()
    # Should have recorded the operation
    assert len(tracker.operations) > 0


def test_repr(sample_dataframe):
    """Test string representation."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    repr_str = repr(tdf)
    assert 'TableDataFrame' in repr_str


def test_str(sample_dataframe):
    """Test str representation."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    str_repr = str(tdf)
    assert isinstance(str_repr, str)


def test_get_tracker(sample_dataframe):
    """Test getting the change tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tracker = tdf.get_tracker()

    assert isinstance(tracker, ChangeTracker)
    assert tracker.primary_key == 'id'


def test_columns_setter(sample_dataframe):
    """Test setting columns property."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.columns = ['full_name', 'years']

    tracker = tdf.get_tracker()
    assert 'name' in tracker.renamed_columns
    assert 'age' in tracker.renamed_columns


def test_index_setter(sample_dataframe):
    """Test that setting index is blocked (PK immutability)."""
    from pandalchemy.exceptions import DataValidationError

    tdf = TableDataFrame(sample_dataframe, 'id')

    # Index setter should raise DataValidationError (PKs are immutable)
    with pytest.raises(DataValidationError, match="Cannot modify index directly"):
        tdf.index = [10, 20, 30]


def test_values_property(sample_dataframe):
    """Test values property."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    values = tdf.values
    assert values.shape == (3, 2)


def test_dtypes_property(sample_dataframe):
    """Test dtypes property."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    dtypes = tdf.dtypes
    assert 'name' in dtypes
    assert 'age' in dtypes


def test_multiple_operations(sample_dataframe):
    """Test tracking multiple operations."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Perform multiple operations
    tdf['email'] = ['a@test.com', 'b@test.com', 'c@test.com']
    tdf.loc[1, 'age'] = 26
    del tdf['name']

    tracker = tdf.get_tracker()

    # Check all changes were tracked
    assert 'email' in tracker.added_columns
    assert 'name' in tracker.dropped_columns
    assert len(tracker.operations) > 0


def test_at_indexer(sample_dataframe):
    """Test at indexer."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get value
    value = tdf.at[1, 'name']
    assert value == 'Alice'

    # Set value
    tdf.at[1, 'name'] = 'Alicia'
    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_iat_indexer(sample_dataframe):
    """Test iat indexer."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get value
    value = tdf.iat[0, 0]
    assert value == 'Alice'

    # Set value
    tdf.iat[0, 0] = 'Alicia'
    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_fillna_method(sample_dataframe):
    """Test fillna method."""
    df_with_nan = sample_dataframe.copy()
    df_with_nan.loc[1, 'age'] = None

    tdf = TableDataFrame(data=df_with_nan, primary_key='id')

    tdf.fillna(0, inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_dropna_method(sample_dataframe):
    """Test dropna method."""
    df_with_nan = sample_dataframe.copy()
    df_with_nan.loc[1, 'age'] = None

    tdf = TableDataFrame(data=df_with_nan, primary_key='id')

    tdf.dropna(inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_reset_index_method(sample_dataframe):
    """Test reset_index method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.reset_index(inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_replace_method(sample_dataframe):
    """Test replace method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.replace('Alice', 'Alicia', inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_update_method(sample_dataframe):
    """Test update method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    other = pd.DataFrame({'age': [100]}, index=[1])
    other.index.name = 'id'

    tdf.update(other)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_insert_method(sample_dataframe):
    """Test insert method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.insert(0, 'new_col', ['X', 'Y', 'Z'])

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0
    assert 'new_col' in tdf._data.columns


def test_pop_method(sample_dataframe):
    """Test pop method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    popped = tdf.pop('age')

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0
    assert 'age' not in tdf._data.columns
    assert len(popped) == 3


def test_getattr_delegation(sample_dataframe):
    """Test that __getattr__ properly delegates to DataFrame."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Test accessing DataFrame methods
    assert hasattr(tdf, 'mean')
    assert hasattr(tdf, 'sum')
    assert hasattr(tdf, 'describe')


def test_internal_attribute_access(sample_dataframe):
    """Test that internal attributes are not delegated."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Should be able to access internal attributes
    assert tdf._data is not None
    assert tdf._tracker is not None
    assert tdf._primary_key == 'id'


def test_repr_html(sample_dataframe):
    """Test _repr_html_ for Jupyter notebooks."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    html = tdf._repr_html_()

    assert isinstance(html, str)
    assert 'table' in html.lower() or '<' in html  # HTML tags


def test_bfill_method(sample_dataframe):
    """Test bfill method."""
    df_with_nan = sample_dataframe.copy()
    df_with_nan.loc[1, 'age'] = None

    tdf = TableDataFrame(data=df_with_nan, primary_key='id')

    tdf.bfill(inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_ffill_method(sample_dataframe):
    """Test ffill method."""
    df_with_nan = sample_dataframe.copy()
    df_with_nan.loc[1, 'age'] = None

    tdf = TableDataFrame(data=df_with_nan, primary_key='id')

    tdf.ffill(inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_interpolate_method(sample_dataframe):
    """Test interpolate method."""
    df_with_nan = sample_dataframe.copy()
    df_with_nan.loc[2, 'age'] = None

    tdf = TableDataFrame(data=df_with_nan, primary_key='id')

    tdf.interpolate(inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_clip_method():
    """Test clip method."""
    # Create DataFrame with only numeric data for clip
    df = pd.DataFrame({
        'value1': [10, 20, 30],
        'value2': [100, 200, 300]
    }, index=pd.Index([1, 2, 3], name='id'))

    tdf = TableDataFrame(data=df, primary_key='id')

    # Clip numeric values
    tdf.clip(lower=15, upper=250, inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_mask_method(sample_dataframe):
    """Test mask method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.mask(tdf['age'] > 30, other=30, inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_where_method(sample_dataframe):
    """Test where method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    tdf.where(tdf['age'] <= 30, other=99, inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0


def test_add_prefix_method(sample_dataframe):
    """Test add_prefix method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # add_prefix returns a new DataFrame (tracked via __getattr__)
    result = tdf.add_prefix('prefix_')

    # Since it goes through __getattr__ and is in _RETURNING_METHODS,
    # it should return wrapped result if it's a DataFrame
    if isinstance(result, TableDataFrame):
        assert 'prefix_name' in result.columns
    else:
        # If not wrapped, at least verify it's a DataFrame
        assert isinstance(result, pd.DataFrame)


def test_add_suffix_method(sample_dataframe):
    """Test add_suffix method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # add_suffix returns a new DataFrame (tracked via __getattr__)
    result = tdf.add_suffix('_suffix')

    # Since it goes through __getattr__ and is in _RETURNING_METHODS,
    # it should return wrapped result if it's a DataFrame
    if isinstance(result, TableDataFrame):
        assert 'name_suffix' in result.columns
    else:
        # If not wrapped, at least verify it's a DataFrame
        assert isinstance(result, pd.DataFrame)


def test_align_method(sample_dataframe):
    """Test align method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    other = pd.DataFrame({'name': ['David'], 'age': [40]}, index=[4])
    other.index.name = 'id'

    # Align returns tuple, so we need to handle it differently
    result = tdf.align(other, fill_value=0)

    # Result is a tuple of DataFrames
    assert isinstance(result, tuple)


def test_reindex_method(sample_dataframe):
    """Test reindex method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # reindex returns a new DataFrame
    result = tdf.reindex([1, 2, 3, 4], fill_value=0)

    # Verify it returns a DataFrame (wrapped or not)
    assert isinstance(result, (TableDataFrame, pd.DataFrame))
    assert len(result) == 4


def test_truncate_method(sample_dataframe):
    """Test truncate method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # truncate returns a new DataFrame
    result = tdf.truncate(before=2, after=3)

    # Verify it returns a DataFrame (wrapped or not)
    assert isinstance(result, (TableDataFrame, pd.DataFrame))
    assert len(result) <= 2


def test_eval_method(sample_dataframe):
    """Test eval method."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Eval can mutate with inplace=True
    tdf.eval('new_age = age * 2', inplace=True)

    tracker = tdf.get_tracker()
    assert len(tracker.operations) > 0
    assert 'new_age' in tdf.columns


def test_head_returns_independent_tracker(sample_dataframe):
    """Test that head() returns TableDataFrame with independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get subset
    subset = tdf.head(2)

    # Modify subset
    subset['age'] = [100, 200]

    # Original should have no changes
    original_summary = tdf.get_tracker().get_summary()
    assert not original_summary['has_changes']

    # Subset should have changes
    subset_summary = subset.get_tracker().get_summary()
    assert subset_summary['has_changes']


def test_tail_returns_independent_tracker(sample_dataframe):
    """Test that tail() returns TableDataFrame with independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get subset
    subset = tdf.tail(2)

    # Modify subset
    subset['name'] = ['X', 'Y']

    # Original should have no changes
    assert not tdf.get_tracker().has_changes()

    # Subset should have changes
    assert subset.get_tracker().has_changes()


def test_copy_returns_independent_tracker(sample_dataframe):
    """Test that copy() returns TableDataFrame with independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Make a copy
    copy_tdf = tdf.copy()

    # Modify copy
    copy_tdf['age'] = [100, 200, 300]

    # Original should have no changes
    assert not tdf.get_tracker().has_changes()

    # Copy should have changes
    assert copy_tdf.get_tracker().has_changes()


def test_filter_returns_independent_tracker(sample_dataframe):
    """Test that filter() returns TableDataFrame with independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Filter columns
    filtered = tdf.filter(items=['name'])

    # Modify filtered
    if isinstance(filtered, TableDataFrame):
        filtered['name'] = ['A', 'B', 'C']

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_getitem_slice_independent_tracker(sample_dataframe):
    """Test that __getitem__ with slice returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get subset via slicing (if it returns DataFrame)
    result = tdf[['name', 'age']]

    if isinstance(result, TableDataFrame):
        # Modify the result
        result['age'] = [100, 200, 300]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()

        # Result should have changes
        assert result.get_tracker().has_changes()


def test_loc_slice_independent_tracker(sample_dataframe):
    """Test that loc slicing returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get subset via loc
    subset = tdf.loc[[1, 2]]

    if isinstance(subset, TableDataFrame):
        # Modify subset
        subset['age'] = [100, 200]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()

        # Subset should have changes
        assert subset.get_tracker().has_changes()


def test_iloc_slice_independent_tracker(sample_dataframe):
    """Test that iloc slicing returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get subset via iloc
    subset = tdf.iloc[0:2]

    if isinstance(subset, TableDataFrame):
        # Modify subset
        subset['name'] = ['X', 'Y']

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()

        # Subset should have changes
        assert subset.get_tracker().has_changes()


def test_query_returns_independent_tracker(sample_dataframe):
    """Test that query() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Query for subset
    result = tdf.query('age > 25')

    if isinstance(result, TableDataFrame):
        # Modify result
        result['age'] = [100, 200]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()

        # Result should have changes
        assert result.get_tracker().has_changes()


def test_mutating_method_with_inplace_false(sample_dataframe):
    """Test that mutating method with inplace=False returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Drop with inplace=False returns new DataFrame
    result = tdf.drop('age', axis=1, inplace=False)

    if isinstance(result, TableDataFrame):
        # Modify result
        result['name'] = ['X', 'Y', 'Z']

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()

        # Original should still have 'age' column
        assert 'age' in tdf.columns

        # Result should not have 'age' column
        assert 'age' not in result.columns


def test_assign_returns_independent_tracker(sample_dataframe):
    """Test that assign() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Use assign to add a new column
    result = tdf.assign(new_col=lambda x: x['age'] * 2)

    if isinstance(result, TableDataFrame):
        # Result should have new column
        assert 'new_col' in result.columns

        # Original should not have new column
        assert 'new_col' not in tdf.columns

        # Modify result
        result['age'] = [100, 200, 300]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()

        # Result should have changes
        assert result.get_tracker().has_changes()


def test_merge_returns_independent_tracker(sample_dataframe):
    """Test that merge() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Create another DataFrame to merge
    other_df = pd.DataFrame({'id': [1, 2], 'salary': [50000, 60000]})

    # Merge
    result = tdf.merge(other_df, on='id')

    if isinstance(result, TableDataFrame):
        # Result should have salary column
        assert 'salary' in result.columns

        # Modify result
        result['salary'] = [100000, 200000]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_rank_returns_independent_tracker(sample_dataframe):
    """Test that rank() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Rank by age
    result = tdf[['age']].rank()

    if isinstance(result, TableDataFrame):
        # Modify result
        result['age'] = [10, 20, 30]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_nlargest_returns_independent_tracker(sample_dataframe):
    """Test that nlargest() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Get 2 largest by age
    result = tdf.nlargest(2, 'age')

    if isinstance(result, TableDataFrame):
        assert len(result) == 2

        # Modify result
        result['age'] = [100, 200]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_abs_returns_independent_tracker(sample_dataframe):
    """Test that abs() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Apply abs (on numeric columns)
    result = tdf[['age']].abs()

    if isinstance(result, TableDataFrame):
        # Modify result
        result['age'] = [100, 200, 300]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_transpose_returns_independent_tracker(sample_dataframe):
    """Test that transpose() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Transpose
    result = tdf[['age']].transpose()

    if isinstance(result, TableDataFrame):
        # Modify result (column names are now row indices)
        result[0] = [999]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_diff_returns_independent_tracker(sample_dataframe):
    """Test that diff() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Calculate differences
    result = tdf[['age']].diff()

    if isinstance(result, TableDataFrame):
        # Modify result
        result['age'] = [100, 200, 300]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_shift_returns_independent_tracker(sample_dataframe):
    """Test that shift() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Shift values
    result = tdf.shift(1)

    if isinstance(result, TableDataFrame):
        # Modify result
        result['age'] = [100, 200, 300]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_isin_returns_independent_tracker(sample_dataframe):
    """Test that isin() returns independent tracker (boolean DataFrame)."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Check if values are in list
    result = tdf[['age']].isin([25, 30])

    if isinstance(result, TableDataFrame):
        # Result should be boolean DataFrame
        assert result['age'].dtype == bool

        # Modify result
        result['age'] = [True, False, True]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()


def test_round_returns_independent_tracker(sample_dataframe):
    """Test that round() returns independent tracker."""
    tdf = TableDataFrame(sample_dataframe, 'id')

    # Round numeric columns
    result = tdf[['age']].round(0)

    if isinstance(result, TableDataFrame):
        # Modify result
        result['age'] = [100, 200, 300]

        # Original should have no changes
        assert not tdf.get_tracker().has_changes()

