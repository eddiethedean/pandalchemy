from pandalchemy.migration import add_column, delete_column
from pandalchemy.pandalchemy_utils import get_table, get_type, has_primary_key
from pandalchemy.pandalchemy_utils import add_primary_key, get_table, primary_key


def update_sql_with_df(df, name, engine, schema=None, index_is_key=True, key=None):
    """Drops all rows then push DataFrame to add data back
       Creates any new columns and deletes any missing columns
    """
    df = df.copy()

    if index_is_key:
        key = df.index.name
        if key is None:
            key = 'id'
        df[key] = df.index

    with engine.begin() as conn:
        start_key = primary_key(name, conn, schema=schema)
        start_key_deleted = False
        tbl = get_table(name, conn, schema=schema)
        # Delete data, leave table columns
        conn.execute(tbl.delete(None))
        # get old column names
        old_names = set(tbl.columns.keys())
        # get new column names
        new_names = set(df.columns)
        # Add any new columns
        new_to_add = new_names - old_names
        for col_name in new_to_add:
            add_column(get_table(name, conn, schema=schema),
                       col_name, get_type(df, col_name))

        # Delete any missing columns
        old_to_delete = old_names - new_names
        if len(old_to_delete) > 0:
            for col_name in old_to_delete:
                if col_name == start_key:
                    start_key_deleted = True
                delete_column(get_table(name, conn, schema=schema), col_name)
        
        if not has_primary_key(name, conn, schema=schema) or start_key_deleted:
            add_primary_key(name, conn, key, schema=None)

        df.to_sql(name, conn, index=False, if_exists='append', schema=schema)
    
