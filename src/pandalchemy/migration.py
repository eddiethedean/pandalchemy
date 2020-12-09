from sqlalchemy import Column
from migrate import *


def add_column(table, name, type):
    col = Column(name, type)
    col.create(table, populate_default=True)


def delete_column(table, name, engine):
    col = table.c[name]
    col.drop(table)

