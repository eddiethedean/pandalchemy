import pandas as pd
import sqlalchemy as sa
import numpy as np

from sqlalchemy import Integer, String, DateTime, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Float, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, case


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


def to_sql_indexkey(df, name, con, if_exists='fail',
             schema=None, chunksize=None,
             dtype=None, **kwargs):
    # push DataFrame to database and set primary key to match DataFrame index
    to_sql_k(df=df, name=name, con=con, if_exists=if_exists, index=True,
             index_label=df.index.name, schema=schema, chunksize=chunksize,
             dtype=dtype, kwargs)


def from_sql_keyindex(table_name, con, key, schema=None,
                      coerce_float=True, parse_dates=None,
                      columns=None, chunksize=None):
    # pull sql table into a DataFrame with index of table's primary key
    return pd.read_sql_table(table_name=table_name, con=con, schema=schema,
                             index_col=key, coerce_float=coerce_float,
                             parse_dates=parse_dates, columns=columns,
                             chunksize=chunksize)

def tables_data_equal(t1, t2):
    """Check if tables have same table_name,
       columns, relationships, and data"""
    # data
    df1 = pd.read_sql(t1.name, t1.metadata.bind)
    df2 = pd.read_sql(t2.name, t2.metadata.bind)
    if not df1.equals(df2):
        return False
    return True
    

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


def has_primary_key(table_name, engine):
    meta = sa.MetaData()
    table = sa.Table(table_name, meta, autoload=True, autoload_with=engine)
    k = table.primary_key.columns.values()
    if len(k) == 0:
        return False
    return True

 
def primary_key(table_name, engine):
    meta = sa.MetaData()
    table = sa.Table(table_name, meta, autoload=True, autoload_with=engine)
    k = table.primary_key.columns.values()
    if has_primary_key(table_name, engine):
        return k[0].name
    return 'index'


def get_table(name, engine):
    metadata = MetaData(engine)
    return sa.Table(name, metadata, autoload=True, autoload_with=engine)


def get_column(table, column_name):
    return table.c[column_name]


def col_name_exists(engine, table_name, col_name):
    return col_name in get_col_names(get_table(table_name, engine))


def get_column_values(engine, table_name, column_name):
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine)
    vals = session.query(tbl.c[column_name]).all()
    return [val[0] for val in vals]


def check_vals_exist(engine, table_name, column_name, vals):
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine)
    col = tbl.c[column_name]
    
    out = list()
    for val in vals:
        my_case_stmt = case(
            [
                (col.in_([val]), True)
            ],
            else_=False
        )
    
        score = session.query(func.sum(my_case_stmt)).scalar()
        out.append(score)
    session.close()
    return out


def check_val_exist(engine, table_name, column_name, val):
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine)
    col = tbl.c[column_name]
    my_case_stmt = case(
        [
            (col.in_([val]), True)
        ]
    )

    score = session.query(func.sum(my_case_stmt)).scalar()
    session.close()
    if score:
        return score
    else:
        return False


def delete_rows(table_name, engine, col_name, vals):
    tbl = get_table(table_name, engine)
    conn = engine.connect()
    for val in vals:
        stmt = tbl.delete().where(tbl.c[col_name] == val)
        conn.execute(stmt)
    conn.close()


def update_table(df, table_name, engine, key, index=False):
    matches_bool = check_vals_exist(engine,
                                     table_name,
                                     key,
                                     df[key])
    matches = df[key][matches_bool]
    delete_rows(table_name, engine, key, matches)
    df.to_sql(table_name, engine, if_exists='append', index=index)