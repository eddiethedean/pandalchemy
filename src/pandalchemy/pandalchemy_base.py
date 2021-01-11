from pandalchemy.pandalchemy_utils import primary_key, to_sql_k, update_table, table_chunks
from pandalchemy.pandalchemy_utils import from_sql_keyindex, copy_table, get_col_names
from pandalchemy.magration_functions import to_sql
from pandalchemy.interfaces import IDataBase, ITable

from pandas import DataFrame
from sqlalchemy.engine.base import Engine


class DataBase(IDataBase):
    """Holds the different database tables as DataFrames
       Needs to connect to a database to push changes
       push method changes database with sql
    """
    def __init__(self, engine, lazy=False):
        self.engine = engine
        # lazy loading stops all tables from getting loaded into memory
        # until table is accessed
        self.lazy = lazy
        if not self.lazy:
            self.db = {name: Table(name,
                                   engine=engine,
                                   db=self
                                   )
                       for name in engine.table_names()
                      }
        else:
            self.db = {name:None for name in engine.table_names()}

    def __getitem__(self, key):
        if self.lazy:
            # load table into memory
            self.db[key] = Table(key, engine=self.engine)
        return self.db[key]

    def __setitem__(self, key, value):
        self.db[key] = value

    def __len__(self):
        return len(self.db)

    @property
    def table_names(self):
        return self.db.keys()

    def __repr__(self):
        if self.lazy:
            names = self.table_names
            cols = [', '.join(get_col_names(name, self.engine)) for name in names]
            keys = [primary_key(name, self.engine) for name in names]
            tables = [f"Table(name={name}, cols=[{c_names}], key={key})\n" for name, c_names,
                      key in zip(names, cols, keys)]
            return f'DataBase({"       , ".join(tables)}, lazy=True, url={self.engine.url})'
        return f'DataBase({", ".join(repr(tbl) for tbl in self.db.values())}, url={self.engine.url})'

    def push(self):
        # Push each table to the database
        for tbl in self.db.values():
            if tbl is not None:
                tbl.push(self.engine)
        self.__init__(self.engine, lazy=self.lazy)

    def pull(self):
        # updates DataBase object with current database data
        self.__init__(self.engine, lazy=self.lazy)

    # TODO drop_table method
    def add_table(self, table, push=False):
        self.db[table.name] = table
        table.db = self
        table.engine = self.engine
        if push:
            self.push()


class BaseTable(ITable):
    """Pandas DataFrame like object used to change sql database tables
    """
    def __init__(self, name, data=None, key=None,
                 f_keys=[], types=dict(), engine=None,
                 db=None):
        self.name = name
        self.key = key
        self.f_keys = f_keys
        self.types = types
        self.engine = engine
        self.data = data
        self.db = db

        if isinstance(self.data, Engine):
            self.engine = self.data
            self.data = None
        
        if isinstance(self.engine, Engine):
            # If engine provided and no key: set key to existing table key
            if self.key is None:
                if self.name in self.engine.table_names():
                    self.key = primary_key(self.name, self.engine)
            else:
                pass # 
            # If engine and data provided: 
            if self.data is not None:
                pass # table probably doesn't already exist?
            else:
                # pull data down from table
                self.data = from_sql_keyindex(self.name,
                                              self.engine)
        # If no engine provided but data is:
        elif self.data is not None:
            
            if isinstance(self.data, dict):
                self.data = DataFrame(data)

            if isinstance(self.data, DataFrame):
                self.key = self.data.index.name
            else:
                raise TypeError('data can only be DataFrame or dict')

    def __len__(self):
        return len(self.data.index)

    def __setitem__(self, key, value):
        if type(key) == int:
            self.data.iloc[key] = value
        else:
            self.data[key] = value
            
    @property
    def column_names(self):
        return self.data.columns

    def _init(self, data):
        return {'name':self.name,
                'data':data.copy(),
                'key':self.key,
                'f_keys':self.f_keys,
                'types':self.types,
                'engine':self.engine,
                'db':self.db}

    @property
    def shape(self):
        return self.data.shape

    @property
    def index(self):
        return self.data.index

    @property
    def columns(self):
        return self.data.columns

    @property
    def info(self):
        return self.data.info

    @property
    def count(self):
        return self.data.count

    def __getitem__(self, key):
        # Slice into SubTable
        if isinstance(key, slice):
            start, stop, step = key.start, key.stop, key.step
            df = self.data.loc[start:stop:step, :]
            return SubTable(**self._init(df))
        if type(key) == int:
            return self.data.iloc[key]
        else:
            return self.data[key]

    def drop(self, *args, **kwargs):
        self.data.drop(inplace=True, *args, **kwargs)

    def drop_col(self, col_name):
        self.drop(col_name, axis=1)

    def head(self, n=5):
        return SubTable(**self._init(self.data.head(n)))

    def tail(self, n=5):
        return SubTable(**self._init(self.data.tail(n)))

    # TODO: add/delete primary key
    # TODO: add/delete foreign key

    def sort_values(self, *args, **kwargs):
        self.data.sort_values(inplace=True, *args, **kwargs)

    
class Table(BaseTable):
    """
    This class maps to an entire sql table.
    Any changes to DataFrame will get pushed to sql table with push method.
    
    """
    def __repr__(self):
        return f"""Table(name={self.name}, key={self.key},
        {repr(self.data)})"""

    # TODO: add lazy loading feature

    def push(self, engine=None):
        # Check data for sql table rules
        if not self.data.index.is_unique:
            raise AttributeError(f'Table({self.name}) data index must have unique values')
        if not self.data.columns.is_unique:
            raise AttributeError(f'Table({self.name}) data columns must have unique values')

        if engine is not None:
            self.engine = engine

        if self.name in self.engine.table_names():
            to_sql(self.data,
                   self.name,
                   self.engine,
                   )
        else:
            self.key = self.data.index.name
            if self.key is None:
                to_sql_k(self.data, self.name, engine, keys='index')
            else:
                to_sql_k(self.data, self.name, engine, keys=self.key)

        self.__init__(self.name, engine=self.engine)

    def pull(self, engine=None):
        if engine is not None:
            self.engine = engine

        self.__init__(self.name, self.data, self.key, self.f_keys,
                      self.types, self.engine, self.db)

    def copy_push(self, new_name, engine=None):
        if engine is not None:
            target_engine = engine
        else:
            target_engine = self.engine
        copy_table(self.engine, self.name, new_name, target_engine)

    def copy(self, new_name, engine=None):
        if engine is not None:
            target_engine = engine
        else:
            target_engine = self.engine
        self.copy_push(new_name, engine=target_engine)
        return Table(new_name, engine=target_engine)

    def drop_col(self, col_name):
        self.drop(col_name, axis=1)


class SubTable(BaseTable):
    """
    Acts as a Table but does not have to be all rows
    of a sql table.
    Must have a primary key column in order to compare
    to full table when changes are pushed.
    Push Updates matching primary key rows and
    appends new primary key rows.
    """
    # TODO: make __init__ set index as key or index
    def __init__(self, name, data=None, key=None, f_keys=[],
                 types=dict(), engine=None, db=None):
        BaseTable.__init__(self, name, data=data, key=key,
                           f_keys=f_keys, types=types,
                           engine=engine, db=db)

    def __repr__(self):
        return f"""SubTable(name={self.name}, key={self.key},
                            {repr(self.data)})"""
    # TODO: add copy_push method
    # TODO: add copy method
    
    def push(self, engine=None):
        if engine is not None:
            self.engine = engine

        self.data[self.index.name] = self.data.index 
        # TODO: Check for missing columns
        #col_names = get_col_names(self.name, engine)
        #missing = set(col_names) - set(self.data.columns) 
        #if missing:
            #raise AttributeError('Pushing less columns than sql table not allowed')
        # TODO: Check for extra columns
        #extra = set(self.data.columns) - set(col_names)
        #if extra:
            #raise AttributeError('Pushing more columns than sql table not allowed')

        # Push any updates to table
        update_table(self.data,
                     self.name,
                     self.engine,
                     self.key,
                     index=False)

        self.__init__(self.name, engine=self.engine)
        # update parent Table with SubTable changes
        #if self.db is not None and self.name in self.db:
            #self.db[self.name].pull(self.engine)
        #elif self.parent is not None:
            #self.parent.pull()
        #else:
            #pass
        # TODO: figure out how to automatically update parent table


def sub_tables(table, chunksize, column_names=None, coerce_float=True,
                              params=None, parse_dates=None):
    for chunk in table_chunks(table.engine, table.name, table.key, chunksize,
                              column_names=column_names, coerce_float=coerce_float,
                              params=params, parse_dates=parse_dates):
        yield SubTable(**table._init(chunk))