from sqlalchemy import Column
from migrate import *
from migrate.changeset.constraint import PrimaryKeyConstraint
from pandalchemy.pandalchemy_utils import has_primary_key


def add_column(table, name, type):
    col = Column(name, type)
    col.create(table, populate_default=True)


def delete_column(table, name):
    col = table.c[name]
    col.drop(table)


def add_primary_key(table, name, engine):
    #col = table.c[name]
    #cons = PrimaryKeyConstraint(col)
    cons = PrimaryKeyConstraint(name, table=table)
    cons.create()