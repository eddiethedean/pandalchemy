# Schema Evolution Hang Analysis

## Problem
The `test_schema_evolution[postgres]` test hangs when calling `table.push()` after adding a column with `table.add_column_with_default()`. This works fine with SQLite but hangs indefinitely with PostgreSQL.

## Investigation Findings

### Where It Hangs
Based on debugging with subprocess timeouts, the hang occurs at:
- **STEP 6**: `table.push()` (with schema change) - this is where it hangs
- The hang happens BEFORE `table.pull()` is called
- Specifically, it hangs inside `execute_plan()` when calling `_execute_schema_change()`

### Root Cause Hypothesis
1. **Transaction Lock Conflict**: When `table.push()` calls `execute_plan()`, it creates a transaction via `engine.begin()`
2. **Schema Change Execution**: `_execute_schema_change()` uses `transmutation.add_column()` which requires an `engine`
3. **Metadata Lock**: `transmutation` likely uses `inspect(engine)` internally to check table structure
4. **Lock Conflict**: PostgreSQL holds metadata locks after ALTER TABLE operations, and when `transmutation` tries to inspect the table using a different connection, it waits for the lock that's held by the transaction

### Why SQLite Works But PostgreSQL Doesn't
- **SQLite**: Doesn't have the same metadata locking mechanism - multiple connections can inspect metadata simultaneously
- **PostgreSQL**: Uses AccessShareLock on system catalogs (pg_attribute, pg_class) which conflicts when:
  - A transaction has modified the table (ALTER TABLE)
  - Another connection tries to inspect the same table metadata
  - The inspecting connection waits indefinitely for the lock

## Potential Solutions

### Solution 1: Execute Schema Changes Outside Transaction (Current Attempt)
- Separated schema changes to execute before data changes
- Use explicit connection management
- **Status**: Still hangs - transmutation might be creating its own connection

### Solution 2: Use Connection for Schema Changes
- Modify `_execute_schema_change()` to accept and use a connection instead of engine
- Requires checking if transmutation supports connection parameter

### Solution 3: Commit Schema Changes Before Inspect
- Execute schema changes in separate transaction that commits before any inspect() calls
- Add small delay or retry logic after schema changes

### Solution 4: Cache Metadata After Schema Changes
- Avoid calling inspect() immediately after schema changes
- Use cached primary key if already known
- Only inspect when absolutely necessary

## Current Code Flow

```
table.push()
  -> execute_plan(engine, table_name, plan, ...)
     -> Separate schema_changes from data_changes
     -> For each schema_change:
        -> conn = engine.connect()  # New connection
        -> _execute_schema_change(engine, ...)  # Uses transmutation with engine
           -> tm.add_column(engine=engine, ...)  # transmutation uses engine
              -> [INTERNAL] transmutation likely calls inspect(engine)
                 -> [HANG] Waiting for metadata lock
```

## Solution Implemented

**Root Cause**: `transmutation.add_column()` uses `inspect(engine)` internally to check table metadata. After an ALTER TABLE operation in PostgreSQL, metadata locks are held on system catalogs (pg_attribute, pg_class). When transmutation tries to inspect the table using a different connection, it waits indefinitely for these locks.

**Fix**: Use raw SQL for PostgreSQL `ADD COLUMN` operations instead of transmutation. This:
- Avoids transmutation's inspect() calls
- Executes schema changes with explicit connection management
- Commits immediately, preventing lock conflicts

**Implementation**: Modified `_execute_schema_change()` in `sql_operations.py` to detect PostgreSQL and use raw `ALTER TABLE` SQL instead of `transmutation.add_column()` for add_column operations.

**Status**: ✅ FIXED - All PostgreSQL tests now pass, including schema evolution tests.

