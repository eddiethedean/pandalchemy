Performance Tuning
==================

This guide covers performance optimization strategies for pandalchemy.

Quick Performance Wins
----------------------

- **Bulk inserts**: Use ``bulk_insert()`` instead of looping ``add_row()``
- **Minimize push()**: Batch all changes, then push once
- **Conditional updates**: Use ``update_where()`` instead of looping ``update_row()``

.. code-block:: python

    # ✅ Fast
    table.bulk_insert(rows)
    users.update_where(users._data['age'] > 65, {'senior': True})

    # ❌ Slow
    for row in rows: table.add_row(row)
    for idx in old_users.index: users.update_row(idx, {'senior': True})

Choosing the Right Operation
----------------------------

When to Use bulk_insert() vs add_row()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use ``bulk_insert()`` when inserting multiple rows:

.. code-block:: python

    # ✅ Fast - Single SQL operation
    users.bulk_insert([
        {'name': 'Alice', 'age': 30},
        {'name': 'Bob', 'age': 25},
        {'name': 'Charlie', 'age': 35}
    ])

    # ❌ Slow - Multiple SQL operations
    for row in rows:
        users.add_row(row)

**Benchmark**: ``bulk_insert()`` is typically 10-100x faster for 100+ rows.

Batching Strategies for Large Updates
--------------------------------------

Batch by Condition
^^^^^^^^^^^^^^^^^^^

Use ``update_where()`` for conditional bulk updates:

.. code-block:: python

    # ✅ Process all matching rows in one operation
    users.update_where(
        users._data['status'] == 'pending',
        {'processed': True, 'processed_at': datetime.now()}
    )

Batch by Chunks
^^^^^^^^^^^^^^^

For very large datasets, process in chunks:

.. code-block:: python

    # Process 1000 rows at a time
    chunk_size = 1000
    for i in range(0, len(users), chunk_size):
        chunk = users.iloc[i:i+chunk_size]
        # Process chunk
        chunk['age'] = chunk['age'] + 1
        chunk.push()  # Push this chunk

Memory Optimization
--------------------

Lazy Change Computation
^^^^^^^^^^^^^^^^^^^^^^^

Changes are computed only when needed:

.. code-block:: python

    # Make changes
    users['age'] = users['age'] + 1
    # Computation hasn't happened yet - no performance cost

    # Only computes when you check or push
    if users.has_changes():  # Computes here if needed
        users.push()

This means you can make many operations without performance cost until you actually need to know what changed.

Pull Only What You Need
^^^^^^^^^^^^^^^^^^^^^^^^

If working with large tables, filter before pulling:

.. code-block:: python

    # ❌ Pulls entire table into memory
    users = db['users']  # Could be millions of rows

    # ✅ Work with specific columns or subsets
    users = db['users']
    subset = users[users['department'] == 'Sales']  # Work with subset
    subset['bonus'] = subset['salary'] * 0.1
    subset.push()

Connection Pooling
^^^^^^^^^^^^^^^^^^

Use SQLAlchemy connection pooling for better performance:

.. code-block:: python

    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool

    engine = create_engine(
        'postgresql://localhost/mydb',
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10
    )

This reuses database connections, reducing connection overhead.

Performance Benchmarks
-----------------------

Typical performance characteristics:

- **bulk_insert()**: ~10,000 rows/second (depends on row size and database)
- **update_where()**: ~5,000-10,000 rows/second (depends on condition complexity)
- **push()**: ~1,000-5,000 operations/second (combines all changes into optimized SQL)

**Note**: Actual performance depends on:

- Database type (PostgreSQL, MySQL, SQLite)
- Network latency (for remote databases)
- Row size and complexity
- Index presence on affected columns

Performance Best Practices
---------------------------

1. **Batch operations**: Group related changes and push once
2. **Use bulk methods**: Prefer ``bulk_insert()`` and ``update_where()`` over loops
3. **Minimize push() calls**: Make all changes, then push once
4. **Use lazy computation**: Changes are computed only when needed
5. **Connection pooling**: Configure appropriate connection pool sizes
6. **Index optimization**: Ensure database indexes exist on frequently queried columns

Example: Optimized Workflow
----------------------------

.. code-block:: python

    import pandalchemy as pa
    from sqlalchemy import create_engine

    # Setup with connection pooling
    engine = create_engine(
        'postgresql://localhost/mydb',
        pool_size=5,
        max_overflow=10
    )
    db = pa.DataBase(engine)

    # Load data
    users = db['users']
    
    # Make all changes first (computation is lazy)
    users['age'] = users['age'] + 1
    users['last_updated'] = datetime.now()
    
    # Bulk operations for new data
    new_users = [
        {'name': f'User {i}', 'age': 20 + i}
        for i in range(100)
    ]
    users.bulk_insert(new_users)
    
    # Conditional bulk updates
    users.update_where(
        users._data['age'] > 65,
        {'senior': True, 'discount': 0.1}
    )
    
    # Push once - all changes in one transaction
    # Computation happens here
    db.push()

This approach minimizes database round-trips and maximizes efficiency.

