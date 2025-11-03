Troubleshooting
===============

This guide covers common issues and their solutions when using pandalchemy.

Common Errors
-------------

Cannot Update Primary Key
^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: You're trying to update a primary key column, which is immutable.

**Error Message**: 
::

    DataValidationError: Cannot update primary key 'id' (value: 123 → 456)
      Table: 'users'
      Operation: update_row(123, {'id': 456})
      Error Code: PK_IMMUTABLE
      Fix: Primary keys are immutable. Delete and re-insert instead:
          old_data = users.get_row(123)
          users.delete_row(123)
          users.add_row({**old_data, 'id': 456})

**Solution**:
.. code-block:: python

    # ❌ This fails
    users.update_row(1, {'id': 999})  # Raises DataValidationError

    # ✅ Instead, delete and re-insert
    old_data = users.get_row(1)
    users.delete_row(1)
    users.add_row({**old_data, 'id': 999})
    users.push()

The enhanced error message includes the table name, operation, and a suggested fix with code example.

Boolean Column Errors in SQLite
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: SQLite doesn't accept NaN values for BOOLEAN columns.

**Solution**:
.. code-block:: python

    # ❌ This may fail
    users['active'] = None  # Becomes NaN, may fail on push

    # ✅ Use explicit False instead
    users.add_column_with_default('active', False)
    # Or handle None explicitly
    users['active'] = users['active'].fillna(False)

No Row Found with Primary Key Value
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Trying to update or delete a row that doesn't exist.

**Enhanced Error Message**: The error now shows the table name, operation, and suggests using ``get_row()`` or ``row_exists()`` to verify.

**Solution**: 
.. code-block:: python

    # Check first
    if users.row_exists(pk_value):
        users.update_row(pk_value, updates)
    else:
        # Row doesn't exist, create it
        users.add_row({**updates, 'id': pk_value})

    # Or use upsert (update if exists, insert if not)
    users.upsert_row({**updates, 'id': pk_value})

Column Does Not Exist
^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Trying to access or modify a column that doesn't exist in the DataFrame.

**Enhanced Error Message**: The error shows:
- Which table you're working with
- The operation that failed
- Available columns in the table
- Suggested fix

**Solution**:
.. code-block:: python

    # Check available columns
    print(users.columns.tolist())

    # Add missing column if needed
    if 'new_column' not in users.columns:
        users.add_column_with_default('new_column', default_value=0)
        users.push()  # Push schema change
        users.pull()  # Refresh

Common Issues
-------------

Schema Changes Not Visible After Push
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Schema changes (add/drop/rename columns) need to be pushed separately, then you need to pull to refresh.

**Solution**:
.. code-block:: python

    # Add column
    users.add_column_with_default('email', '')
    users.push()  # Push schema change first

    # Pull to refresh with new schema
    users.pull()

    # Now you can update the new column
    users['email'] = 'user@example.com'
    users.push()  # Push data changes

**Best Practice**: Push schema changes and data changes in separate transactions for reliability.

Memory Issues with Large Tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Working with very large tables can consume significant memory.

**Solutions**:

1. **Use lazy change computation** (already implemented):
   .. code-block:: python

       # Changes are only computed when needed
       users['age'] = users['age'] + 1  # No computation yet
       if users.has_changes():  # Computes here if needed
           users.push()

2. **Use bulk operations instead of loops**:
   .. code-block:: python

       # ✅ Fast - single bulk operation
       users.update_where(users._data['age'] > 65, {'senior': True})
       users.bulk_insert(new_rows)

       # ❌ Slow - many individual operations
       for row in new_rows:
           users.add_row(row)

3. **Batch your changes**:
   .. code-block:: python

       # Make all changes first
       users['age'] = users['age'] + 1
       users['status'] = 'active'
       # Then push once
       users.push()  # Single transaction

Transaction Rollback Not Working as Expected
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Understanding when transactions rollback and when they commit.

**Solution**: 
- ``push()`` automatically wraps all changes in a transaction
- If any error occurs, the entire transaction rolls back
- Schema changes and data changes happen in the correct order automatically

.. code-block:: python

    try:
        users['age'] = users['age'] + 1
        products['price'] = products['price'] * 1.1
        db.push()  # All changes in one transaction
    except Exception as e:
        # All changes rolled back automatically
        print(f"Error: {e}. No changes were committed.")

Performance is Slow with Many Updates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Problem**: Many individual ``update_row()`` calls are slow.

**Solution**: Use ``update_where()`` for bulk conditional updates:
.. code-block:: python

    # ✅ Fast - single SQL operation
    users.update_where(
        users._data['age'] > 65,
        {'senior': True, 'discount': 0.1}
    )

    # ❌ Slow - many SQL operations
    for idx in old_users.index:
        users.update_row(idx, {'senior': True, 'discount': 0.1})

Getting Help
------------

If you encounter an issue not covered here:

1. Check the error message - enhanced error messages now provide detailed context and suggested fixes
2. Review the :doc:`performance` guide for optimization tips
3. Check the :doc:`usage` guide for examples
4. Review the :doc:`../README` for limitations and known issues
5. Open an issue on `GitHub <https://github.com/eddiethedean/pandalchemy/issues>`_

