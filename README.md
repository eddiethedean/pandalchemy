
# pandalchemy: an intuitive combination of Pandas and sqlalchemy to manipulate sql databases with pandas

## What is it?

**pandalchemy** is a Python package that lets Data Scientists create and manipulte sql databases with the Pandas package 
that they know and love without needing to learn the ins and outs of sqlalchemy.

## Main Features
Here are just a few of the things that pandalchemy does:

  - Pulls down any sql table as a Pandas DataFrame with sqlalchemy and maintains all data types, keys, and indexes
    after you push your changes.
  - Make changes to Pandas DataFrame as you normally would then push any changes (new columns, delete columns, new rows, updated rows) to sql database.
  - Adds a primary key to a new sql table, something pandas to_sql method does not do.
  - Add or delete columns in a database table thanks to sqlalchemy-migrate.

## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/eddiethedean/pandalchemy

```sh
# PyPI
pip install pandalchemy
```

## Dependencies
- [pandas](https://pandas.pydata.org/)
- [sqlalchemy==1.3.18](https://pypi.org/project/SQLAlchemy/1.3.18/)
- [sqlalchemy-migrate](https://sqlalchemy-migrate.readthedocs.io/en/latest/)
- [numpy](https://numpy.org/)
- [tabulate] 
        
# Example code
```sh
from sqlalchemy import create_engine 
import pandalchemy as ba 
        
# Use sqlalchemy to create an engine to connect to existing database 
engine = create_engine('postgresql://scott:tiger@localhost:5432/mydatabase') 
        
# Initialize a pandalchemy DataBase object 
db = ba.DataBase(engine) 
        
# Accessing a table by name gives you a DataFrame like Table object 
tbl = db['test_table'] 
        
# Make changes to the Table just like you would a pandas DataFrame 
tbl['age'] = [11, 12, 13, 14, 15] 
        
# Use the push method to push all your changes to your database 
db.push() 
```




