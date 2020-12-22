import sqlalchemy as sa

from pandalchemy.migration import add_column, delete_column, add_primary_key
from pandalchemy.pandalchemy_utils import get_table, get_type, get_class, has_primary_key, primary_key


def to_sql(df, name, engine):
    """Drops all rows then uses bulk_insert_mappings to add data back"""
    Session = sa.orm.sessionmaker(bind=engine)
    session = Session()
    metadata = sa.MetaData(engine)
    tbl = sa.Table(name, metadata, autoload=True, autoload_with=engine)
    # If table has no primary key, add it to column
    if not has_primary_key(name, engine):
        p_key = df.index.name
        add_primary_key(tbl, p_key)
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
            add_column(get_table(name, engine),
                       col_name,
                       get_type(df, col_name))
    # Delete any missing columns
    old_to_delete = old_names - new_names
    if len(old_to_delete) > 0:
        for col_name in old_to_delete:
            delete_column(get_table(name, engine), col_name)
    # Bulk upload all the rows into the table
    session.bulk_insert_mappings(get_class(name, engine),
                                 df.to_dict(orient="records"))
    session.commit()
