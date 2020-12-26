from pandalchemy.pandalchemy_base import DataBase
import sqlalchemy as sa
from pandalchemy.generate_code import generate_code
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd


t = []

t.append(('sqlalchemy version', sa.__version__ == '1.3.18'))

engine = sa.create_engine('sqlite:///', echo=False)
Base = declarative_base()


class TestTable(Base):
    __tablename__ = 'test_table'
    id = Column(Integer, primary_key=True)
    key = Column(String, nullable=False)
    val = Column(String)


class SecondTable(Base):
    __tablename__ = 'second_table'
    id = Column(Integer, primary_key=True)
    val = Column(String, nullable=False)
    key = Column(ForeignKey('test_table.key'))


Base.metadata.create_all(bind=engine)

t.append(('first code gen', generate_code(engine) == (
    ("# coding: utf-8\nfrom sqlalchemy import Column, ForeignKey, "
     "Integer, String\nfrom sqlalchemy.orm import relationship\n"
     "from sqlalchemy.ext.declarative import declarative_base\n\n\n"
     "Base = declarative_base()\nmetadata = Base.metadata\n\n\n"
     "class SecondTable(Base):\n    __tablename__ = 'second_table'\n"
     "\n    id = Column(Integer, primary_key=True)\n    val = Column"
     "(String, nullable=False)\n    key = Column(ForeignKey("
     "'test_table.key'))\n\n    test_table = relationship('TestTable')"
     "\n\n\nclass TestTable(Base):\n    __tablename__ = 'test_table'"
     "\n\n    id = Column(Integer, primary_key=True)\n    key = Column"
     "(String, nullable=False)\n    val = Column(String)\n"))))

# Setup session
Session = sessionmaker(bind=engine)
session = Session()
# Add items
session.add(TestTable(key='21', val='Thing'))
session.add(TestTable(key='25', val='Person'))
session.add(SecondTable(key='21', val='Chair'))
session.commit()
# create DataBase object
db = DataBase(engine)

t.append(('add three TestTable rows print db object', str(db) == (
    'DataBase(Table(name=second_table, key=id,\n           id    val '
    'key\n0   1  Chair  21), Table(name=test_table, key=id,\n         '
    '  id key     val\n0   1  21   Thing\n1   2  25  Person))')))

tbl = db['test_table']
tbl['age'] = [18, 19]
db.push()

t.append(('add age column, print db object', str(db) == (
    'DataBase(Table(name=second_table, key=id,\n           '
    'id    val key\n0   1  Chair  21), Table(name=test_table, '
    'key=id,\n           id key     val  age\n0   1  21   Thing'
    '   18\n1   2  25  Person   19))')))


t.append(('col types test', str(tbl.types) == (
    "{'id': INTEGER(), 'key': VARCHAR(), 'val': VARCHAR()}")))

new_db = DataBase(engine)

t.append(('str types test', str(new_db) == (
    'DataBase(Table(name=second_table, key=id,\n           '
    'id    val key\n0   1  Chair  21), Table(name=test_table, '
    'key=id,\n           id key     val  age\n0   1  21   '
    'Thing   18\n1   2  25  Person   19))')))

t.append(('col types test', str(new_db['test_table'].types) == (
    "{'id': INTEGER(), 'key': VARCHAR(), 'val': "
    "VARCHAR(), 'age': INTEGER()}")))

t.append(('col types test codegen', generate_code(engine) == (
    "# coding: utf-8\nfrom sqlalchemy import Column, ForeignKey, "
    "Integer, String\nfrom sqlalchemy.orm import relationship\n"
    "from sqlalchemy.ext.declarative import declarative_base\n\n\n"
    "Base = declarative_base()\nmetadata = Base.metadata\n\n\n"
    "class SecondTable(Base):\n    __tablename__ = 'second_table'"
    "\n\n    id = Column(Integer, primary_key=True)\n    val = "
    "Column(String, nullable=False)\n    key = Column(ForeignKey("
    "'test_table.key'))\n\n    test_table = relationship("
    "'TestTable')\n\n\nclass TestTable(Base):\n    __tablename__ = "
    "'test_table'\n\n    id = Column(Integer, primary_key=True)"
    "\n    key = Column(String, nullable=False)\n    val = "
    "Column(String)\n    age = Column(Integer)\n")))


tbl2 = db['second_table']
tbl2.drop(['val'], axis=1, inplace=True)

t.append(('str after col drop', str(db) == (
    'DataBase(Table(name=second_table, key=id,\n           id key\n0   '
    '1  21), Table(name=test_table, key=id,\n           id key     val  '
    'age\n0   1  21   Thing   18\n1   2  25  Person   19))')))

t.append(('codegen after col drop', generate_code(engine) == (
    "# coding: utf-8\nfrom sqlalchemy import Column, ForeignKey, Integer, "
    "String\nfrom sqlalchemy.orm import relationship\nfrom sqlalchemy.ext."
    "declarative import declarative_base\n\n\nBase = declarative_base()\n"
    "metadata = Base.metadata\n\n\nclass SecondTable(Base):\n    "
    "__tablename__ = 'second_table'\n\n    id = Column(Integer, primary_key"
    "=True)\n    val = Column(String, nullable=False)\n    key = Column("
    "ForeignKey('test_table.key'))\n\n    test_table = relationship("
    "'TestTable')\n\n\nclass TestTable(Base):\n    __tablename__ = "
    "'test_table'\n\n    id = Column(Integer, primary_key=True)\n    "
    "key = Column(String, nullable=False)\n    val = Column(String)\n    "
    "age = Column(Integer)\n")))


print(len(t), 'tests performed.')
print(sum(1 for x in t if x[1] is False), 'tests failed.')

for test in t:
    if test[1] is False:
        print(test[0], 'failed')
