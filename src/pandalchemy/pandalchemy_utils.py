import pandas as pd
import sqlalchemy as sa
import numpy as np

from sqlalchemy import Integer, String, DateTime, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Float, Boolean, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert


def to_sql_k(df, name, con, if_exists='fail', index=True,
             index_label=None, schema=None, chunksize=None,
             dtype=None, **kwargs):
    """
    """
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
                    dtype=None):
    """Push DataFrame to database and set primary key to match DataFrame index
    """
    to_sql_k(df=df, name=name, con=con, if_exists=if_exists, index=True,
             index_label=df.index.name, schema=schema, chunksize=chunksize,
             dtype=dtype, keys=df.index.name)


def from_sql_keyindex(table_name, con, schema=None,
                      coerce_float=True, parse_dates=None,
                      columns=None, chunksize=None):
    """Pull sql table into a DataFrame with index of table's primary key
    """
    key = primary_key(table_name, con, schema=schema)
    return pd.read_sql_table(table_name=table_name, con=con, schema=schema,
                             index_col=key, coerce_float=coerce_float,
                             parse_dates=parse_dates, columns=columns,
                             chunksize=chunksize)


def tables_data_equal(t1, t2, t1_schema=None, t2_schema=None):
    """Check if tables have same table_name,
    columns, relationships, and data
    """
    # data
    df1 = pd.read_sql(t1.name, t1.metadata.bind, schema=t1_schema)
    df2 = pd.read_sql(t2.name, t2.metadata.bind, schema=t2_schema)
    if not df1.equals(df2):
        return False
    return True
    

def tables_metadata_equal(t1, t2, t1_schema=None, t2_schema=None):
    """Check if tables have same table_name,
    columns, relationships, and data
    """
    # table_name
    if t1.name != t2.name:
        return False
    # columns
    # Check if each column name exists and has same data_type, attributes
    col_names1 = set(get_col_names(t1, schema=t1_schema))
    col_names2 = set(get_col_names(t2, schema=t2_schema))
    if col_names1 != col_names2:
        return False
    for name in col_names1:
        if t1.columns[name].type is t2.columns[name].type:
            return False
    return True


def get_col_names(table, engine=None, name=None, schema=None):
    """
    """
    if type(table) is str:
        name = table
    if name and engine:
        table = get_table(name, engine, schema=schema)
    return [c.name for c in table.columns]


def get_type(df, col_name):
    """return sqlalcheymy type based on DataFrame col type
    """
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


def get_class(name, engine, schema=None):
    """
    """
    metadata = sa.MetaData(engine, schema=schema)
    metadata.reflect(engine, only=[name])
    Base = automap_base(metadata=metadata)
    Base.prepare()
    return Base.classes[name]


def get_col_types(name, engine, schema=None):
    """Returns dict of table column names:data_type
    """
    md = sa.MetaData()
    table = sa.Table(name, md, autoload=True, autoload_with=engine, schema=schema)
    return {c.name: c.type for c in table.c}


def list_of_tables(engine, schema=None):
    """
    """
    return [(name, pd.read_sql(name, con=engine, schema=schema))
            for name in engine.table_names(schema=schema)]


def has_primary_key(table_name, engine, schema=None):
    """
    """
    meta = sa.MetaData(bind=engine, schema=schema)
    table = sa.Table(table_name, meta, autoload=True, autoload_with=engine, schema=schema)
    k = table.primary_key.columns.values()
    if len(k) == 0:
        return False
    return True

 
def primary_key(table_name, engine, schema=None):
    """
    """
    meta = sa.MetaData(bind=engine, schema=schema, )
    table = sa.Table(table_name, meta, autoload=True, autoload_with=engine, schema=schema)
    k = table.primary_key.columns.values()
    if has_primary_key(table_name, engine, schema=schema):
        return k[0].name
    return None


def get_table(name, engine, schema=None):
    """
    """
    metadata = MetaData(bind=engine, schema=schema)
    return sa.Table(name, metadata, autoload=True, autoload_with=engine, schema=schema)


def get_column(table, column_name, engine=None, schema=None):
    """
    """
    if type(table) is str:
        table = get_table(table, engine, schema)
    return table.c[column_name]


def col_name_exists(engine, table_name, col_name, schema=None):
    """
    """
    return col_name in get_col_names(get_table(table_name, engine, schema=schema), schema=schema)


def get_column_values(engine, table_name, column_name, schema=None):
    """
    """
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine, schema=schema)
    vals = session.query(tbl.c[column_name]).all()
    #vals = engine.execute(f'select {column_name} from {table_name}').fetchall()
    return [val[0] for val in vals]


def check_val_exist(engine, table_name, column_name, val, schema=None):
    """
    """
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine, schema=schema)
    col = tbl.c[column_name]

    my_case_stmt = select([col]).where(col.in_([val]))
    out = session.execute(my_case_stmt).fetchall()
    session.close()
    
    out = [r[0] for r in out]
    return val in out


def check_vals_exist(engine, table_name, column_name, vals,
                     return_vals=False, schema=None):
    """
    """
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine, schema=schema)
    col = tbl.c[column_name]

    my_case_stmt = select([col]).where(col.in_(vals))
    out = session.execute(my_case_stmt).fetchall()
    session.close()
    
    out = [r[0] for r in out]
    if return_vals:
        return out
    return [True if val in out else False for val in vals]


def delete_rows(table_name, engine, col_name, vals, schema=None):
    """
    """
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine, schema=schema)
    col = tbl.c[col_name]
    session.query(tbl).filter(col.in_(vals)).delete(synchronize_session=False)
    session.commit()
    session.close()


def update_table(df, table_name, engine, key, index=False, schema=None):
    """
    """
    matches_bool = check_vals_exist(engine,
                                     table_name,
                                     key,
                                     df[key],
                                     schema=schema)
    matches = df[key][matches_bool]
    delete_rows(table_name, engine, key, matches, schema=schema)
    df.to_sql(table_name, engine, if_exists='append', index=index, schema=schema)


def copy_table(src_engine, src_name, dest_name, dest_engine=None, schema=None, dest_schema=None):
    """
    """
    if dest_engine is None:
        dest_engine = src_engine

    if dest_engine is None:
        dest_engine = schema

    # reflect existing columns, and create table object for oldTable
    src_engine._metadata = MetaData(bind=src_engine, schema=schema)
    src_engine._metadata.reflect(src_engine) # get columns from existing table
    srcTable = sa.Table(src_name, src_engine._metadata, schema=schema)

    # create engine and table object for newTable
    dest_engine._metadata = MetaData(bind=dest_engine, schema=dest_schema)
    destTable = sa.Table(dest_name, dest_engine._metadata, schema=dest_schema)

    # copy schema and create newTable from oldTable
    for column in srcTable.columns:
        destTable.append_column(column.copy())
    destTable.create()  

    SrcSession = sessionmaker(src_engine)
    session = SrcSession()
    query = session.query(srcTable).all()

    DestSession = sessionmaker(dest_engine)
    dest_session = DestSession()
    for row in query:
        dest_session.execute(destTable.insert(row))
    dest_session.commit()
    session.close()
    dest_session.close()


def add_primary_key(table_name, engine, column_name, schema=None):
    """
    """
    # reflect existing columns, and create table object for oldTable
    engine._metadata = MetaData(bind=engine, schema=schema)
    engine._metadata.reflect(engine) # get columns from existing table
    srcTable = sa.Table(table_name, engine._metadata, schema=schema)

    temp_name = table_name + '__temp__'

    # create engine and table object for newTable
    engine._metadata = MetaData(bind=engine, schema=schema)
    destTable = sa.Table(temp_name, engine._metadata, schema=schema)

    # copy schema and create newTable from oldTable
    for column in srcTable.columns:
        destTable.append_column(column.copy())
    destTable.append_column(sa.PrimaryKeyConstraint(column_name, name=column_name))
    destTable.create()

    SrcSession = sessionmaker(engine)
    session = SrcSession()
    query = session.query(srcTable).all()
    session.close()

    DestSession = sessionmaker(engine)
    dest_session = DestSession()
    for row in query:
        dest_session.execute(destTable.insert(row))
    dest_session.commit()
    dest_session.close()

    # delete old table
    get_table(table_name, engine, schema=schema).drop()
    # copy new table to old table name
    copy_table(engine, temp_name, table_name, schema=schema)
    # delete temp table
    get_table(temp_name, engine, schema=schema).drop()


def get_row_count(table_name, engine, schema=None):
    """
    """
    Session = sessionmaker(engine)
    session = Session()
    tbl = get_table(table_name, engine, schema=schema)
    cols = get_col_names(tbl,schema=schema)
    col = get_column(tbl, cols[0])
    return session.query(func.count(col)).scalar()


def df_sql_check(df):
    """
    """
    if not df.index.is_unique:
        return False
    if not df.columns.is_unique:
        return False
    return True


def get_table_rows(table_name, engine, key, key_matches,
                   coerce_float=True, params=None,
                   parse_dates=None, chunksize=None,
                   column_names=None, schema=None):
    """
    """
    if column_names is None:
        column_names = '*'
    else:
        column_names = ', '.join(x for x in column_names)
    return pd.read_sql_query(f'''SELECT {column_names}
                                 FROM {table_name}
                                 WHERE {key}
                                 IN {tuple(key_matches)}''',
                             engine,
                             index_col=key,
                             coerce_float=coerce_float,
                             params=params,
                             parse_dates=parse_dates,
                             chunksize=chunksize,
                             schema=schema)


def key_chunks(engine, table_name, column_name, chunksize, schema=None):
    """
    """
    vals = get_column_values(engine, table_name, column_name, schema=schema)
    i = 0
    while i < len(vals):
        yield vals[i:i+chunksize]
        i += chunksize


def table_chunks(engine, table_name, key, chunksize, column_names=None,
                 coerce_float=True, params=None, parse_dates=None, schema=None):
    """Generator function -> [pd.DataFrame]
    Pulls pandas DataFrame chunks from sql table
    Doesn't lock up sqlite database
    """
    for keys in key_chunks(engine, table_name, key, chunksize, schema=schema):
        yield get_table_rows(table_name, engine, key, keys,
                             column_names=column_names,
                             coerce_float=coerce_float,
                             params=params,
                             parse_dates=parse_dates,
                             schema=schema)


def filter_list(a_list: list, bool_list: list):
    # filter down a list by a True/False list
    return [val for val, b in zip(a_list, bool_list) if b]


def reverse_filter(a_list: list, bool_list: list):
    bool_list = [False if x else True for x in bool_list]
    return filter_list(a_list, bool_list)


def update_insert(table_name, engine, records, schema=None):
    """Updates any key matched records
       Inserts any new key records
       Table must have primary key
       records must all have table primary key entries
       records is a list of dictionaries for each table row
    """
    key = primary_key(table_name, engine, schema=schema)
    if key is None:
        raise AttributeError('table has no primary key')
        
    # get key values from records
    key_vals = [record[key] for record in records]
    
    # find matches in table
    bool_matches = check_vals_exist(engine, table_name, key, key_vals, schema=schema)
    matches_keys = filter_list(key_vals, bool_matches)
    new_records_keys = reverse_filter(key_vals, bool_matches)
    
    match_records = [x for x in records if x[key] in matches_keys]
    new_records = [x for x in records if x[key] in new_records_keys]
    
    Session = sa.orm.sessionmaker(engine)
    session = Session()
    mapper =  sa.inspect(get_class(table_name, engine, schema=schema))
    session.bulk_update_mappings(mapper, match_records)
    session.bulk_insert_mappings(mapper, new_records)
    session.commit()
    session.close()


def update_insert_df(table_name, engine, df, index_key=False, schema=None):
    """Updates and inserts rows from DataFrame into table
       Must have column with same name as table primary key
       If the DataFrame index is the primary key set index_key=True and
       the DataFrame index name must match the primary key name
    """
    if index_key:
        df = df.copy()
        df[df.index.name] = df.index.values

    records = df.to_dict('records')
    update_insert(table_name, engine, records, schema=schema)


def df_to_sql_on_conflict_do_nothing(df, engine, table_name, primary_key, schema=None):
    insert_values = df.to_dict(orient='records')
    table = get_table(table_name, engine, schema)
    insert_statement = insert(table).values(insert_values)
    do_nothing_statement = insert_statement.on_conflict_do_nothing(index_elements=[primary_key])
    return engine.execute(do_nothing_statement)


def divide_chunks(l, n):  
    for i in range(0, len(l), n):  
        yield l[i:i + n] 


def insert_df(df, engine, table_name, schema=None, chunk_size=500):
    '''Table and columns must already exist.
       Use this if table has no primary key.'''
    records = df.to_dict('records')
    table = get_table(table_name, engine, schema=schema)
    for chunk in divide_chunks(records, chunk_size):
        sql = table.insert().values(chunk)
        engine.execute(sql)


def insert_df_k(df, engine, table_name, schema=None):
    '''Table and columns must already exist.
       Table MUST have primary key.
       Faster than insert_df because of primary key.'''
    records = df.to_dict('records')
    Session = sa.orm.sessionmaker(engine)
    session = Session()
    mapper = sa.inspect(get_class(table_name, engine, schema=schema))
    session.bulk_insert_mappings(mapper, records)
    session.commit()
    session.close()