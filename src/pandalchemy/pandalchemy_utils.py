import pandas as pd
import sqlalchemy as sa
import numpy as np

from sqlalchemy import Integer, String, DateTime, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Float, Boolean


def to_sql_k(df, name, con, if_exists='fail', index=True,
             index_label=None, schema=None, chunksize=None,
             dtype=None, **kwargs):
    pandas_sql = pd.io.sql.pandasSQL_builder(con, schema=schema)

    if dtype is not None:
        from sqlalchemy.types import to_instance, TypeEngine
        for col, my_type in dtype.items():
            if not isinstance(to_instance(my_type), TypeEngine):
                raise ValueError('The type of %s is not a SQLAlchemy '
                                 'type ' % col)

    table = pd.io.sql.SQLTable(name, pandas_sql, frame=df, index=index,
                               if_exists=if_exists, index_label=index_label,
                               schema=schema, dtype=dtype, **kwargs)
    table.create()
    table.insert(chunksize)


def tables_data_equal(t1, t2):
    """Check if tables have same table_name,
       columns, relationships, and data"""
    # data
    df1 = pd.read_sql(t1.name, t1.metadata.bind)
    df2 = pd.read_sql(t2.name, t2.metadata.bind)
    if not df1.equals(df2):
        return False
    return True


def tables_metadata_equal(t1, t2):
    """Check if tables have same table_name,
       columns, relationships, and data"""
    # table_name
    if t1.name != t2.name:
        return False
    # columns
    # Check if each column name exists and has same data_type, attributes
    col_names1 = set(get_col_names(t1))
    col_names2 = set(get_col_names(t2))
    if col_names1 != col_names2:
        return False
    for name in col_names1:
        if t1.columns[name].type is t2.columns[name].type:
            return False
    return True


def get_col_names(table):
    return [c.name for c in table.columns]


def get_type(df, col_name):
    # return sqlalcheymy type based on DataFrame col type
    pd_type = df[col_name].dtype
    if pd_type == np.int64:
        return Integer
    elif pd_type == np.float64:
        return Float
    elif pd_type == np.bool:
        return Boolean
    elif pd_type == np.datetime64:
        return DateTime
    return String


def get_class(name, engine):
    metadata = sa.MetaData(engine)
    metadata.reflect(engine, only=[name])
    Base = automap_base(metadata=metadata)
    Base.prepare()
    return Base.classes[name]


def get_col_types(name, engine):
    """Returns dict of table column names:data_type"""
    md = sa.MetaData()
    table = sa.Table(name, md, autoload=True, autoload_with=engine)
    return {c.name: c.type for c in table.c}


def list_of_tables(engine):
    return [(name, pd.read_sql(name, con=engine))
            for name in engine.table_names()]


def primary_key(table_name, engine):
    meta = sa.MetaData()
    table = sa.Table(table_name, meta, autoload=True, autoload_with=engine)
    k = table.primary_key.columns.values()
    if len(k) == 0:
        return 'index'
    return k[0].name


def get_table(name, engine):
    metadata = MetaData(engine)
    return sa.Table(name, metadata, autoload=True, autoload_with=engine)


def get_column(table, column_name):
    return table.c[column_name]


def col_name_exists(engine, table_name, col_name):
    return col_name in get_col_names(get_table(table_name, engine))
