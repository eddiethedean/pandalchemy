"""Tests for type conversion utilities."""

import numpy as np
import pandas as pd

from pandalchemy.utils import (
    convert_numpy_types,
    convert_record_types,
    convert_records_list,
    extract_primary_key_column,
    get_table_reference,
    normalize_schema,
    pandas_dtype_to_python_type,
)


def test_convert_numpy_int():
    """Test converting numpy integer types."""
    assert convert_numpy_types(np.int8(5)) == 5
    assert convert_numpy_types(np.int16(10)) == 10
    assert convert_numpy_types(np.int32(20)) == 20
    assert convert_numpy_types(np.int64(30)) == 30
    assert isinstance(convert_numpy_types(np.int64(30)), int)


def test_convert_numpy_float():
    """Test converting numpy float types."""
    assert convert_numpy_types(np.float16(1.5)) == 1.5
    assert convert_numpy_types(np.float32(2.5)) == 2.5
    assert convert_numpy_types(np.float64(3.5)) == 3.5
    assert isinstance(convert_numpy_types(np.float64(3.5)), float)


def test_convert_numpy_bool():
    """Test converting numpy boolean types."""
    assert convert_numpy_types(np.bool_(True)) is True
    assert convert_numpy_types(np.bool_(False)) is False
    assert isinstance(convert_numpy_types(np.bool_(True)), bool)


def test_convert_non_numpy_types():
    """Test that non-numpy types pass through unchanged."""
    assert convert_numpy_types(5) == 5
    assert convert_numpy_types(3.14) == 3.14
    assert convert_numpy_types("hello") == "hello"
    assert convert_numpy_types(True) is True
    assert convert_numpy_types(None) is None


def test_convert_record_types():
    """Test converting all types in a dictionary."""
    record = {
        'int_val': np.int64(100),
        'float_val': np.float64(2.5),
        'str_val': 'hello',
        'bool_val': np.bool_(True),
        'none_val': None
    }

    result = convert_record_types(record)

    assert result['int_val'] == 100
    assert isinstance(result['int_val'], int)
    assert result['float_val'] == 2.5
    assert isinstance(result['float_val'], float)
    assert result['str_val'] == 'hello'
    assert result['bool_val'] is True
    assert isinstance(result['bool_val'], bool)
    assert result['none_val'] is None


def test_convert_records_list():
    """Test converting list of records."""
    records = [
        {'id': np.int64(1), 'value': np.float64(10.5)},
        {'id': np.int64(2), 'value': np.float64(20.5)},
        {'id': np.int64(3), 'value': np.float64(30.5)}
    ]

    result = convert_records_list(records)

    assert len(result) == 3
    for _i, rec in enumerate(result):
        assert isinstance(rec['id'], int)
        assert isinstance(rec['value'], float)


def test_pandas_dtype_to_python_type():
    """Test pandas dtype to Python type conversion."""
    # Integer types
    assert pandas_dtype_to_python_type(pd.Int64Dtype()) == int
    assert pandas_dtype_to_python_type('int64') == int
    assert pandas_dtype_to_python_type('int32') == int

    # Float types
    assert pandas_dtype_to_python_type('float64') == float
    assert pandas_dtype_to_python_type('float32') == float

    # String types
    assert pandas_dtype_to_python_type('object') == str
    assert pandas_dtype_to_python_type('string') == str

    # Boolean
    assert pandas_dtype_to_python_type('bool') == bool
    assert pandas_dtype_to_python_type('boolean') == bool

    # Datetime
    assert pandas_dtype_to_python_type('datetime64[ns]') == str

    # None
    assert pandas_dtype_to_python_type(None) == str


def test_normalize_schema():
    """Test schema normalization."""
    assert normalize_schema(None) is None
    assert normalize_schema('') is None
    assert normalize_schema('public') == 'public'
    assert normalize_schema('myschema') == 'myschema'


def test_get_table_reference():
    """Test table reference string generation."""
    assert get_table_reference('users', None) == 'users'
    assert get_table_reference('users', 'public') == 'public.users'
    assert get_table_reference('my_table', 'my_schema') == 'my_schema.my_table'


def test_extract_primary_key_column_from_index():
    """Test extracting primary key when it's the index."""
    df = pd.DataFrame({
        'name': ['Alice', 'Bob'],
        'age': [25, 30]
    }, index=pd.Index([1, 2], name='id'))

    result = extract_primary_key_column(df, 'id')

    assert 'id' in result.columns
    assert list(result['id']) == [1, 2]
    assert 'name' in result.columns


def test_extract_primary_key_column_already_column():
    """Test extracting primary key when it's already a column."""
    df = pd.DataFrame({
        'id': [1, 2],
        'name': ['Alice', 'Bob'],
        'age': [25, 30]
    })

    result = extract_primary_key_column(df, 'id')

    assert 'id' in result.columns
    assert result.equals(df)  # Should be a copy but same data


def test_numpy_array_in_record():
    """Test handling numpy arrays in records."""
    record = {
        'id': np.array([1])[0],  # numpy.int64
        'values': [1, 2, 3],  # regular list
        'data': np.array([10.5])[0]  # numpy.float64
    }

    result = convert_record_types(record)

    assert isinstance(result['id'], (int, np.integer))
    assert result['values'] == [1, 2, 3]


def test_mixed_types_in_records_list():
    """Test converting records with mixed types."""
    records = [
        {'id': 1, 'value': np.int64(10)},
        {'id': np.int64(2), 'value': 20},
        {'id': 3, 'value': np.float64(30.5)}
    ]

    result = convert_records_list(records)

    assert result[0]['value'] == 10
    assert result[1]['id'] == 2
    assert result[2]['value'] == 30.5


def test_empty_record_conversion():
    """Test converting empty record."""
    assert convert_record_types({}) == {}
    assert convert_records_list([]) == []


def test_pandas_series_to_dict_conversion():
    """Test converting pandas Series (which contains numpy types)."""
    series = pd.Series({
        'id': np.int64(1),
        'value': np.float64(10.5),
        'name': 'test'
    })

    record = series.to_dict()
    result = convert_record_types(record)

    assert isinstance(result['id'], int)
    assert isinstance(result['value'], float)
    assert result['name'] == 'test'

