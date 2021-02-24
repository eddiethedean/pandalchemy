import sqlalchemy as sa

from pandalchemy.migration import add_column, delete_column
from pandalchemy.pandalchemy_utils import get_table, get_type, get_class, has_primary_key, primary_key, to_sql_k
from pandalchemy.pandalchemy_utils import col_name_exists, add_primary_key


def to_sql(df, name, engine, schema=None):
    """Drops all rows then uses bulk_insert_mappings to add data back
    """
    df = df.copy()
    key = df.index.name
    if key is None:
        key = 'index'
    df[key] = df.index

    Session = sa.orm.sessionmaker(bind=engine)
    session = Session()
    metadata = sa.MetaData(engine)
    tbl = sa.Table(name, metadata, autoload=True, autoload_with=engine, schema=schema)

    # If table has no primary key, add it to column
    if not has_primary_key(name, engine, schema=schema):
        # If table primary key col doesn't exist
        if not col_name_exists(engine, name, key, schema=schema):
            add_column(tbl,
                       key,
                       get_type(df, key)
                       )

    metadata = sa.MetaData(engine)
    tbl = sa.Table(name, metadata, autoload=True, autoload_with=engine, schema=schema)

    # Delete data, leave table columns
    engine.execute(tbl.delete(None))
    # get old column names
    old_names = set(tbl.columns.keys())
    # get new column names
    new_names = set(df.columns)
    # Add any new columns
    new_to_add = new_names - old_names
    if len(new_to_add) > 0:
        for col_name in new_to_add:
            # Need to calculate type based on DataFrame col type
            add_column(get_table(name, engine, schema=schema),
                       col_name,
                       get_type(df, col_name))
    # Delete any missing columns
    old_to_delete = old_names - new_names
    if len(old_to_delete) > 0:
        for col_name in old_to_delete:
            delete_column(get_table(name, engine), col_name)
    # Bulk upload all the rows into the table
    # to_sql_k(df, name, engine, index=False, if_exists='replace', keys=key)
    #df.to_sql(name, engine, index=False, if_exists='append', keys=key)
    if not has_primary_key(name, engine):
        tbl = sa.Table(name, metadata, autoload=True, autoload_with=engine, schema=schema)
        add_primary_key(tbl, engine, key, schema=schema)
    session.bulk_insert_mappings(get_class(name, engine, schema=schema),
                                 df.to_dict(orient="records")
                                 )
    session.commit()
    
