
Changelog
=========

1.6.0 (2025-01-XX)
------------------

**Code Quality Release - Remove Raw SQL**

Improvements
~~~~~~~~~~~~

* **Eliminated Raw SQL**: All raw SQL statements have been replaced with SQLAlchemy ORM/Table API methods
  * SELECT queries now use SQLAlchemy's `select()` and `Table` API instead of raw SQL strings
  * Schema changes (ALTER TABLE) now use transmutation library functions with proper database-specific parameters
  * Health checks use SQLAlchemy's `select(1)` instead of raw SQL
  * Transaction isolation level uses SQLAlchemy's `DDL()` construct
  * Improved type safety and database portability
  * Better maintainability and code consistency
* **Enhanced Transmutation Integration**: All schema changes now use transmutation library with proper parameters
  * PostgreSQL: Uses `verify=False` to avoid metadata lock issues
  * MySQL: Uses `default_varchar_length=255` for VARCHAR columns
  * Consistent behavior across all database dialects

1.5.0 (2025-01-XX)
------------------

**Feature Release - pandas `to_sql` Compatibility**

New Features
~~~~~~~~~~~~

* **pandas `to_sql` Compatibility**: Full pandas-compatible `to_sql` method with enhanced features
  * Primary key creation and auto-increment support for new tables
  * Automatic primary key inference from DataFrame index (named or MultiIndex)
  * Support for all pandas `to_sql` parameters (index, index_label, chunksize, dtype, if_exists)
  * Works seamlessly with existing TableDataFrame change tracking
* **Async `to_sql` Support**: `AsyncTableDataFrame.to_sql()` for async database operations
* **Composite Primary Key Support in `create_table`**: `DataBase.create_table()` now accepts `str | list[str]` for composite keys

Improvements
~~~~~~~~~~~~

* **Improved Type Safety**: Fixed type annotations for composite primary keys throughout the codebase
* **Enhanced MySQL Async Support**: Fixed async URL conversion and sync engine caching for MySQL databases
* **Testing**: 986 tests passing with comprehensive `to_sql` coverage across SQLite, PostgreSQL, and MySQL

0.2.0 (2025-01-20)
------------------

**Major Release - Complete Architectural Overhaul**

New Features
~~~~~~~~~~~~

* **Automatic Change Tracking**: All DataFrame modifications are now automatically tracked
  at both operation and row levels through the new TrackedDataFrame wrapper.
* **Optimized SQL Execution Plans**: Changes are analyzed and batched into efficient SQL
  operations (inserts, updates, deletes, schema changes).
* **Transaction Safety**: All push operations execute within transactions with automatic
  rollback on errors, powered by fullmetalalchemy.
* **Schema Evolution**: Automatic handling of column additions, deletions, and renames
  using transmutation for schema migrations.
* **Modern Dependencies**: Updated to SQLAlchemy 2.x, fullmetalalchemy, and transmutation.

Breaking Changes
~~~~~~~~~~~~~~~~

* **SQLAlchemy Version**: Now requires SQLAlchemy >= 2.0.0 (was 1.3.18)
* **Python Version**: Now requires Python >= 3.9 (was >= 3.6)
* **Removed Classes**: SubTable and View classes have been removed
* **Removed Dependency**: sqlalchemy-migrate replaced by transmutation

New Modules
~~~~~~~~~~~

* ``change_tracker.py``: ChangeTracker class for monitoring DataFrame modifications
* ``tracked_dataframe.py``: TrackedDataFrame wrapper with automatic change detection
* ``execution_plan.py``: ExecutionPlan class for optimizing SQL operations
* ``sql_operations.py``: Wrapper functions for fullmetalalchemy and transmutation

Improvements
~~~~~~~~~~~~

* Comprehensive pytest test suite with >90% coverage
* Type hints throughout the codebase (Python 3.9+ syntax)
* Modern packaging with updated pyproject.toml
* Improved documentation with migration guide
* Better error handling and transaction management

Migration Notes
~~~~~~~~~~~~~~~

* Update SQLAlchemy imports to 2.x patterns
* Remove manual change tracking code (now automatic)
* Replace SubTable usage with standard DataFrame operations
* Update minimum Python version to 3.9+

0.1.x Series
------------

See previous releases for 0.1.x changelog entries.