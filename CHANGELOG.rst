
Changelog
=========

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