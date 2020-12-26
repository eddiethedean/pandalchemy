from sqlalchemy import Column
from migrate import *
from migrate.changeset.constraint import PrimaryKeyConstraint


def add_column(table, name, type):
    col = Column(name, type)
    col.create(table, populate_default=True)


def delete_column(table, name):
    col = table.c[name]
    col.drop(table)


def add_primary_key(table, name):
    col = table.c[name]
    cons = PrimaryKeyConstraint(col)
    cons.create()