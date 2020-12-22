from pandalchemy.pandalchemy_utils import list_of_tables, primary_key, get_col_types, to_sql_k
from pandalchemy.pandalchemy_utils import from_sql_keyindex, to_sql_indexkey
from pandalchemy.magration_functions import to_sql
from pandalchemy.interfaces import IDataBase, ITable

from pandas import read_sql_table
from sqlalchemy.engine.base import Engine


class DataBase(IDataBase):
    """Holds the different database tables as DataFrames
       Needs to connect to a database to push changes
       push method changes database with sql
    """
    def __init__(self, engine):
        self.engine = engine
        self.db = {name: Table(name,
                               engine,
                               )
                   for name in engine.table_names()
                   }

    def __getitem__(self, key):
        return self.db[key]

    def __setitem__(self, key, value):
        self.db[key] = value

    def __len__(self):
        return len(self.db)

    @property
    def table_names(self):
        return self.db.keys()

    def __repr__(self):
        return f'DataBase({", ".join(repr(tbl) for tbl in self.db.values())})'

    def push(self):
        # Push each table to the database
        for tbl in self.db.values():
            tbl.push(self.engine)

    def pull(self):
        # updates DataBase object with current database data
        self.__init__(self.engine)

    # TODO add_table method
    # TODO drop_table method


class Table(ITable):
    """Pandas DataFrame used to change database tables
    """
    def __init__(self, name, data=None, key=None, f_keys=[], types=dict(), engine=None):
        self.name = name
        self.key = key
        self.f_keys = f_keys
        self.types = types
        self.engine = engine
        self.data = data

        if isinstance(data, Engine):
            self.engine = data
            self.data = from_sql_keyindex(self.name, self.engine, key=self.key)
            
        elif (data is None and engine is not None):
            self.data = from_sql_keyindex(self.name, self.engine, key=self.key)
        
        if self.data is not None:
            self.key = self.data.index.name

    def __repr__(self):
        return f"""Table(name={self.name}, key={self.key},
        {repr(self.data)})"""

    def __len__(self):
        return len(self.data)

    def __setitem__(self, key, value):
        if type(key) == int:
            self.data.iloc[key] = value
        else:
            self.data[key] = value

    def push(self, engine=None):
        if engine is not None:
            self.engine = engine

        if self.name in self.engine.table_names():
            to_sql(self.data,
                   self.name,
                   self.engine)
        else:
            to_sql_indexkey(self.data,
                     self.name,
                     self.engine
                     )
        self.__init__(self.name, engine=self.engine)
            
    @property
    def column_names(self):
        return self.data.columns

    def __getitem__(self, key):
        if type(key) == int:
            return self.data.iloc[key]
        else:
            return self.data[key]

    def drop(self, *args, **kwargs):
        self.data.drop(*args, **kwargs)

    # TODO: add/delete primary key

    # TODO: add/delete foreign key



if __name__ == '__main__':
    import pandas as pd
    import sqlalchemy as sa
    engine = sa.create_engine('sqlite:///tests/table_test.db')
    df = pd.DataFrame({'name':['Josh', 'Odos', 'Alec', 'Olivia', 'Max'], 'id':[1, 2, 3, 4, 5]})
    to_sql_k(df, 'people', engine, index=False, if_exists='replace', keys='id')
    tbl = Table('people', engine)
    tbl['name'] = [x+'_' for x in tbl['name']]
    tbl.push()
    print(Table('people', engine=engine))